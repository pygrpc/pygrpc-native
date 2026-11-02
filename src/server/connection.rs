// SPDX-License-Identifier: Apache-2.0

use std::future::poll_fn;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::{watch, Notify};

use super::fallback::Fallback;
use super::listener::stopped;
use super::order::Sequencer;
use super::stream::serve_stream;
use super::{Keepalive, Shared, CALL_DRAIN_TIMEOUT, CONNECTION_DRAIN_TIMEOUT};

// Fixed windows instead of BDP probing. A stream that is not read holds up
// at most STREAM_WINDOW; the connection window is the protocol maximum so such
// streams never stall the others sharing the connection, which grpc-core
// reaches by growing its window.
pub(super) const STREAM_WINDOW: u32 = 4 << 20;

pub(super) const CONNECTION_WINDOW: u32 = (1 << 31) - 1;

// grpc-core's default; a connection that never finishes the preface is dropped.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(120);

// grpc-core's default ping timeout: the longest a keepalive ping goes
// unanswered, even while the client keeps sending.
const PING_TIMEOUT: Duration = Duration::from_secs(60);

/// Lets close() wait for every connection's GOAWAY.
struct OpenConnection(Arc<Shared>);

impl OpenConnection {
    fn new(shared: Arc<Shared>) -> Self {
        shared.connections.fetch_add(1, Ordering::AcqRel);
        OpenConnection(shared)
    }
}

impl Drop for OpenConnection {
    fn drop(&mut self) {
        if self.0.connections.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.0.connections_done.notify_waiters();
        }
    }
}

pub(crate) async fn serve_connection<IO>(
    io: IO,
    peer: Arc<str>,
    shared: Arc<Shared>,
    mut shutdown: watch::Receiver<bool>,
) where
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let _open = OpenConnection::new(shared.clone());
    let reads = Arc::new(LastRead::new());
    let io = Watched {
        io,
        reads: reads.clone(),
    };
    let handshake = h2::server::Builder::new()
        .initial_window_size(STREAM_WINDOW)
        .initial_connection_window_size(CONNECTION_WINDOW)
        .max_concurrent_streams(shared.connection.max_concurrent_streams)
        .max_header_list_size(shared.connection.max_metadata_size)
        .handshake::<_, Bytes>(io);
    let mut connection = tokio::select! {
        result = tokio::time::timeout(HANDSHAKE_TIMEOUT, handshake) => match result {
            Ok(Ok(connection)) => connection,
            Ok(Err(err)) => {
                log::debug!("gRPC handshake failed: {err}");
                return;
            }
            Err(_) => {
                log::debug!("gRPC handshake timed out");
                return;
            }
        },
        _ = stopped(&mut shutdown) => return,
    };
    let fallback = Arc::new(Fallback::default());
    let calls = Arc::new(Calls::default());
    let order = shared.ordered.then(|| Arc::new(Sequencer::default()));
    let keepalive = keepalive(
        shared.connection.keepalive,
        connection.ping_pong(),
        calls.clone(),
        reads,
    );
    tokio::pin!(keepalive);
    let spawn = |request, respond| {
        let call = ActiveCall::new(calls.clone());
        let stream = serve_stream(
            shared.clone(),
            peer.clone(),
            fallback.clone(),
            request,
            respond,
            order.as_ref().map(Sequencer::issue),
        );
        tokio::spawn(async move {
            let _call = call;
            stream.await;
        });
    };
    let mut stopping = false;
    loop {
        tokio::select! {
            accepted = connection.accept() => match accepted {
                Some(Ok((request, respond))) => spawn(request, respond),
                Some(Err(err)) => {
                    log::debug!("gRPC connection ended with error: {err}");
                    return;
                }
                None => return,
            },
            _ = &mut keepalive => break,
            _ = stopped(&mut shutdown) => {
                stopping = true;
                break;
            }
        }
    }
    if stopping {
        // abrupt_shutdown() drops what the calls haven't written yet.
        let finished = calls.finished();
        tokio::pin!(finished);
        let _ = tokio::time::timeout(CALL_DRAIN_TIMEOUT, async {
            loop {
                tokio::select! {
                    accepted = connection.accept() => match accepted {
                        Some(Ok((request, respond))) => spawn(request, respond),
                        _ => return,
                    },
                    _ = &mut finished => break,
                }
            }
            poll_fn(|cx| {
                while let Poll::Ready(Some(Ok((request, respond)))) = connection.poll_accept(cx) {
                    spawn(request, respond);
                }
                Poll::Ready(())
            })
            .await;
        })
        .await;
    }
    // Unlike a graceful GOAWAY, which waits for a PING an idle grpcio client
    // does not answer, this names the last stream served, so the client
    // retries any later call on a new connection. grpcio servers do the same
    // on stop().
    connection.abrupt_shutdown(h2::Reason::NO_ERROR);
    let _ = tokio::time::timeout(
        CONNECTION_DRAIN_TIMEOUT,
        poll_fn(|cx| connection.poll_closed(cx)),
    )
    .await;
}

/// A connection's calls in flight, for keepalive and for closing.
#[derive(Default)]
struct Calls {
    count: AtomicUsize,
    closing: AtomicBool,
    finished: Notify,
}

impl Calls {
    async fn finished(&self) {
        self.closing.store(true, Ordering::SeqCst);
        loop {
            let finished = self.finished.notified();
            if self.count.load(Ordering::SeqCst) == 0 {
                return;
            }
            finished.await;
        }
    }
}

struct ActiveCall(Arc<Calls>);

impl ActiveCall {
    fn new(calls: Arc<Calls>) -> Self {
        calls.count.fetch_add(1, Ordering::Relaxed);
        ActiveCall(calls)
    }
}

impl Drop for ActiveCall {
    fn drop(&mut self) {
        // SeqCst on both sides, so finished() can't miss the last call.
        if self.0.count.fetch_sub(1, Ordering::SeqCst) == 1 && self.0.closing.load(Ordering::SeqCst)
        {
            self.0.finished.notify_waiters();
        }
    }
}

/// When a connection last read anything, as an offset from when it opened.
struct LastRead {
    opened: Instant,
    micros: AtomicU64,
}

impl LastRead {
    fn new() -> Self {
        LastRead {
            opened: Instant::now(),
            micros: AtomicU64::new(0),
        }
    }

    fn now(&self) -> Duration {
        self.opened.elapsed()
    }

    fn mark(&self) {
        self.micros
            .store(self.now().as_micros() as u64, Ordering::Relaxed);
    }

    fn last(&self) -> Duration {
        Duration::from_micros(self.micros.load(Ordering::Relaxed))
    }
}

/// The connection's IO, noting each read for keepalive.
struct Watched<IO> {
    io: IO,
    reads: Arc<LastRead>,
}

impl<IO: AsyncRead + Unpin> AsyncRead for Watched<IO> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        let polled = Pin::new(&mut self.io).poll_read(cx, buf);
        if buf.filled().len() > before {
            self.reads.mark();
        }
        polled
    }
}

impl<IO: AsyncWrite + Unpin> AsyncWrite for Watched<IO> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.io).poll_write(cx, buf)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.io).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.io.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.io).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(cx)
    }
}

/// Resolves once the peer stops answering keepalive pings.
async fn keepalive(
    config: Keepalive,
    ping_pong: Option<h2::PingPong>,
    calls: Arc<Calls>,
    reads: Arc<LastRead>,
) {
    let (Some(time), Some(mut ping_pong)) = (config.time, ping_pong) else {
        return std::future::pending().await;
    };
    let mut next = time;
    loop {
        tokio::time::sleep_until((reads.opened + next).into()).await;
        // Like grpc-core, ping only after the keepalive time passes without
        // anything read, and an idle connection only when allowed.
        let quiet_until = reads.last() + time;
        if quiet_until > reads.now() {
            next = quiet_until;
            continue;
        }
        if config.without_calls || calls.count.load(Ordering::Relaxed) > 0 {
            let sent = reads.now();
            let pong = ping_pong.ping(h2::Ping::opaque());
            tokio::pin!(pong);
            let answered = match tokio::time::timeout(config.timeout, &mut pong).await {
                Ok(result) => result.is_ok(),
                // grpc-core gives a client that is still sending until its
                // ping timeout to answer.
                Err(_) if reads.last() > sent => matches!(
                    tokio::time::timeout(PING_TIMEOUT.saturating_sub(config.timeout), pong).await,
                    Ok(Ok(_))
                ),
                Err(_) => false,
            };
            if !answered {
                log::debug!("gRPC peer missed a keepalive ping; closing the connection");
                return;
            }
        }
        next = reads.now() + time;
    }
}
