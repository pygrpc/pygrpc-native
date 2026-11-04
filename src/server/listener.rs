// SPDX-License-Identifier: Apache-2.0

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::watch;
use tokio_rustls::TlsAcceptor;

use super::connection::{serve_connection, HANDSHAKE_TIMEOUT};
use super::tls::PeerAuth;
use super::{Peer, Shared};

// Unix sockets have no peer address, so each connection gets its own name.
static NEXT_UNIX_PEER: AtomicU64 = AtomicU64::new(1);

pub(super) async fn stopped(shutdown: &mut watch::Receiver<bool>) {
    let _ = shutdown.wait_for(|stop| *stop).await;
}

pub enum Listener {
    Tcp(tokio::net::TcpListener),
    Unix(tokio::net::UnixListener),
}

pub async fn accept_loop(
    listener: Listener,
    shared: Arc<Shared>,
    mut shutdown: watch::Receiver<bool>,
    tls: Option<TlsAcceptor>,
) {
    loop {
        let accepted = tokio::select! {
            _ = stopped(&mut shutdown) => return,
            accepted = accept(&listener) => accepted,
        };
        match accepted {
            Ok(Accepted::Tcp(stream, peer)) => {
                let _ = stream.set_nodelay(true);
                spawn_connection(stream, peer, shared.clone(), shutdown.clone(), &tls);
            }
            Ok(Accepted::Unix(stream, peer)) => {
                spawn_connection(stream, peer, shared.clone(), shutdown.clone(), &tls);
            }
            Err(err) => {
                // Out of file descriptors (EMFILE): back off instead of spinning.
                log::warn!("gRPC accept failed: {err}");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

fn spawn_connection<IO>(
    io: IO,
    peer: Arc<str>,
    shared: Arc<Shared>,
    shutdown: watch::Receiver<bool>,
    tls: &Option<TlsAcceptor>,
) where
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    match tls {
        None => {
            let peer = Arc::new(Peer {
                address: peer,
                auth: None,
            });
            tokio::spawn(serve_connection(io, peer, shared, shutdown));
        }
        Some(tls) => {
            tokio::spawn(serve_tls_connection(
                tls.clone(),
                io,
                peer,
                shared,
                shutdown,
            ));
        }
    }
}

// On the connection's own task, so a slow client can't hold up the accept loop.
async fn serve_tls_connection<IO>(
    tls: TlsAcceptor,
    io: IO,
    peer: Arc<str>,
    shared: Arc<Shared>,
    mut shutdown: watch::Receiver<bool>,
) where
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let stream = tokio::select! {
        result = tokio::time::timeout(HANDSHAKE_TIMEOUT, tls.accept(io)) => match result {
            Ok(Ok(stream)) => stream,
            Ok(Err(err)) => {
                log::debug!("TLS handshake failed: {err}");
                return;
            }
            Err(_) => {
                log::debug!("TLS handshake timed out");
                return;
            }
        },
        _ = stopped(&mut shutdown) => return,
    };
    let certificates = stream.get_ref().1.peer_certificates();
    let auth = certificates.and_then(|chain| match PeerAuth::new(chain) {
        Ok(auth) => Some(Arc::new(auth)),
        // Its calls still go through, showing no identity.
        Err(err) => {
            log::warn!("can't read the certificate of {peer}: {err}");
            None
        }
    });
    let peer = Arc::new(Peer {
        address: peer,
        auth,
    });
    serve_connection(stream, peer, shared, shutdown).await;
}

enum Accepted {
    Tcp(tokio::net::TcpStream, Arc<str>),
    Unix(tokio::net::UnixStream, Arc<str>),
}

async fn accept(listener: &Listener) -> std::io::Result<Accepted> {
    match listener {
        Listener::Tcp(listener) => {
            let (stream, addr) = listener.accept().await?;
            let peer = match addr {
                std::net::SocketAddr::V4(addr) => format!("ipv4:{addr}"),
                std::net::SocketAddr::V6(addr) => {
                    format!("ipv6:%5B{}%5D:{}", addr.ip(), addr.port())
                }
            };
            Ok(Accepted::Tcp(stream, peer.into()))
        }
        Listener::Unix(listener) => {
            let (stream, _addr) = listener.accept().await?;
            let id = NEXT_UNIX_PEER.fetch_add(1, Ordering::Relaxed);
            Ok(Accepted::Unix(stream, format!("unix:{id}").into()))
        }
    }
}
