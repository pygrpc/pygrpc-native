//! One HTTP/2 stream: finding its method, running the call, and writing the response.

// SPDX-License-Identifier: Apache-2.0

use std::future::{poll_fn, Future};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

use bytes::Bytes;
use http_body::{Body as _, Frame};
use tokio::sync::oneshot;
use tonic::{Code, Status};

use super::calls::{
    call_details, call_timeout, deadline_exceeded, shutting_down, ExtraTrailers, StreamingCall,
    UnaryCall,
};
use super::codec::grpc;
use super::fallback::Fallback;
use super::order::Turn;
use super::{Event, NativeMethod, Resolved, Shared, GRPC_HEADER_LEN};

async fn route(
    shared: Arc<Shared>,
    peer: Arc<str>,
    fallback: Arc<Fallback>,
    mut request: http::Request<RequestBody>,
    call: Option<&CallMetrics>,
) -> http::Response<tonic::body::Body> {
    let lookup = |request: &http::Request<RequestBody>| {
        shared
            .methods
            .get()
            .and_then(|methods| methods.get(request.uri().path()))
            .copied()
    };
    // Before the method lookup, so callers without a token can't tell which
    // methods exist, and before the fallback, so it is covered too.
    if let Some(auth) = &shared.bearer_auth {
        if !auth.accepts(request.headers()) {
            if let Some(call) = call {
                call.start(lookup(&request).map(|(id, _)| id));
            }
            return Status::unauthenticated(auth.message.clone()).into_http();
        }
    }
    // The deadline runs from when the headers arrive, so it also covers a
    // request body that is slow to come.
    let deadline = call_timeout(request.headers()).map(|t| tokio::time::Instant::now() + t);
    let mut method = lookup(&request);
    if method.is_none() && shared.resolve_unknown.load(Ordering::Relaxed) {
        let resolve = resolve(&shared, &peer, &request, deadline);
        let resolved = match deadline {
            Some(deadline) => tokio::time::timeout_at(deadline, resolve)
                .await
                .unwrap_or_else(|_| Err(deadline_exceeded())),
            None => resolve.await,
        };
        method = match resolved {
            Ok(method) => method,
            Err(status) => {
                if let Some(call) = call {
                    call.start(None);
                }
                return status.into_http();
            }
        };
    }
    let Some((method_id, streaming)) = method else {
        return match shared.fallback.get() {
            // The fallback server counts these calls itself.
            Some(path) => {
                // A forwarded call can stay open; it must not hold up the next ones.
                request.extensions_mut().remove::<Turn>();
                fallback.forward(path, request).await
            }
            None => {
                if let Some(call) = call {
                    call.start(None);
                }
                Status::new(Code::Unimplemented, "Method not found!").into_http()
            }
        };
    };
    if let Some(call) = call {
        call.start(Some(method_id));
    }
    let sizes = call.map(|call| call.sizes.clone());
    let response = async {
        if streaming {
            let call = StreamingCall {
                shared,
                method_id,
                peer,
                deadline,
            };
            grpc(sizes).server_streaming(call, request).await
        } else {
            let call = UnaryCall {
                shared,
                method_id,
                peer,
                deadline,
            };
            with_extra_trailers(grpc(sizes).unary(call, request).await)
        }
    };
    match deadline {
        Some(deadline) => tokio::time::timeout_at(deadline, response)
            .await
            .unwrap_or_else(|_| deadline_exceeded().into_http()),
        None => response.await,
    }
}

/// How a method missing from the table is served, asking Python the first
/// time; None is the fallback.
async fn resolve(
    shared: &Arc<Shared>,
    peer: &Arc<str>,
    request: &http::Request<RequestBody>,
    deadline: Option<tokio::time::Instant>,
) -> Result<Option<NativeMethod>, Status> {
    let path = request.uri().path();
    if let Some(resolved) = shared.resolved.read().unwrap().get(path) {
        return Ok(match *resolved {
            Resolved::Native(id, streaming) => Some((id, streaming)),
            Resolved::Fallback => None,
        });
    }
    let resolve_id = shared.next_call_id();
    let (sender, answer) = oneshot::channel();
    shared
        .resolving
        .lock()
        .unwrap()
        .insert(resolve_id, (path.to_owned(), sender));
    if shared.is_closed() {
        shared.resolving.lock().unwrap().remove(&resolve_id);
        return Err(shutting_down());
    }
    let metadata = tonic::metadata::MetadataMap::from_headers(request.headers().clone());
    let _ = shared.events.send(Event::Resolve {
        resolve_id,
        path: path.to_owned(),
        details: call_details(shared, &metadata, peer, deadline),
    });
    answer.await.map_err(|_| shutting_down())
}

pub(super) async fn serve_stream(
    shared: Arc<Shared>,
    peer: Arc<str>,
    fallback: Arc<Fallback>,
    mut request: http::Request<h2::RecvStream>,
    mut respond: h2::server::SendResponse<Bytes>,
    turn: Option<Turn>,
) {
    if let Some(turn) = turn {
        request.extensions_mut().insert(turn);
    }
    // Dropped without finish() (client reset, shutdown), it records CANCELLED.
    let call = shared
        .metrics_enabled()
        .then(|| CallMetrics::new(shared.clone()));
    let request = request.map(|recv| RequestBody {
        recv,
        data_done: false,
        sizes: call.as_ref().map(|call| call.sizes.clone()),
    });
    let route = route(shared, peer, fallback, request, call.as_ref());
    tokio::pin!(route);
    // A reset (cancel, deadline on the client) drops the call.
    let response = poll_fn(|cx| {
        if respond.poll_reset(cx).is_ready() {
            return Poll::Ready(None);
        }
        route.as_mut().poll(cx).map(Some)
    })
    .await;
    let Some(response) = response else {
        return;
    };
    let (parts, body) = response.into_parts();
    let end_of_stream = body.is_end_stream();
    // A trailers-only response carries its status in the headers.
    let code = match &call {
        Some(_) if end_of_stream => grpc_status(&parts.headers),
        _ => Code::Ok as i32,
    };
    let Ok(mut send) = respond.send_response(http::Response::from_parts(parts, ()), end_of_stream)
    else {
        return;
    };
    let code = if end_of_stream {
        code
    } else {
        match send_body(&mut send, body, call.is_some()).await {
            Some(code) => code,
            None => return,
        }
    };
    if let Some(call) = &call {
        call.finish(code);
    }
}

/// Records one call when metrics are on: the start once the method is known,
/// and the status and duration once it ends.
struct CallMetrics {
    shared: Arc<Shared>,
    since: Instant,
    slot: AtomicUsize,
    done: AtomicBool,
    sizes: Arc<CallSizes>,
}

/// Message bytes of one call, counted as they go through.
#[derive(Default)]
pub(super) struct CallSizes {
    pub(super) sent: AtomicU64,
    // DATA bytes, so a compressed request counts at its compressed size.
    pub(super) received: AtomicU64,
}

impl CallMetrics {
    const NO_SLOT: usize = usize::MAX;

    fn new(shared: Arc<Shared>) -> Self {
        CallMetrics {
            shared,
            since: Instant::now(),
            slot: AtomicUsize::new(Self::NO_SLOT),
            done: AtomicBool::new(false),
            sizes: Arc::default(),
        }
    }

    fn start(&self, method_id: Option<u32>) {
        if let Some(metrics) = self.shared.metrics() {
            let slot = metrics.slot(method_id);
            metrics.started(slot);
            self.slot.store(slot, Ordering::Relaxed);
        }
    }

    pub(super) fn finish(&self, code: i32) {
        if self.done.swap(true, Ordering::Relaxed) {
            return;
        }
        let slot = self.slot.load(Ordering::Relaxed);
        if slot == Self::NO_SLOT {
            return;
        }
        if let Some(metrics) = self.shared.metrics() {
            let sent = self.sizes.sent.load(Ordering::Relaxed);
            // The natively served methods take one request message; drop
            // its frame header.
            let received = self.sizes.received.load(Ordering::Relaxed);
            let rcvd = received.saturating_sub(GRPC_HEADER_LEN);
            metrics.finished(slot, code, self.since.elapsed(), sent, rcvd);
        }
    }
}

impl Drop for CallMetrics {
    fn drop(&mut self) {
        self.finish(Code::Cancelled as i32);
    }
}

fn grpc_status(headers: &http::HeaderMap) -> i32 {
    headers
        .get("grpc-status")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok())
        .unwrap_or(Code::Unknown as i32)
}

/// Send a response body within HTTP/2 flow control. Returns the call's status
/// (read from the trailers only when `status` is set), or None when the
/// client resets the stream, which drops the body.
async fn send_body(
    send: &mut h2::SendStream<Bytes>,
    body: tonic::body::Body,
    status: bool,
) -> Option<i32> {
    tokio::pin!(body);
    loop {
        let frame = poll_fn(|cx| {
            if send.poll_reset(cx).is_ready() {
                return Poll::Ready(None);
            }
            body.as_mut().poll_frame(cx).map(Some)
        })
        .await;
        let frame = match frame {
            None => return None,
            Some(None) => {
                let _ = send.send_data(Bytes::new(), true);
                return Some(Code::Unknown as i32);
            }
            Some(Some(Err(err))) => {
                // Only a forwarded response can fail here (the out-of-tree
                // server went away); end it as a lost connection would.
                log::debug!("gRPC response body failed: {err}");
                let mut trailers = http::HeaderMap::new();
                match Status::unavailable(err.to_string()).add_header(&mut trailers) {
                    Ok(()) => {
                        let _ = send.send_trailers(trailers);
                    }
                    Err(_) => send.send_reset(h2::Reason::INTERNAL_ERROR),
                }
                return Some(Code::Unavailable as i32);
            }
            Some(Some(Ok(frame))) => frame,
        };
        let frame = match frame.into_data() {
            Ok(mut data) => {
                while !data.is_empty() {
                    send.reserve_capacity(data.len());
                    let capacity = poll_fn(|cx| {
                        if send.poll_reset(cx).is_ready() {
                            return Poll::Ready(None);
                        }
                        send.poll_capacity(cx)
                    })
                    .await;
                    let granted = match capacity {
                        Some(Ok(granted)) => granted,
                        _ => return None,
                    };
                    let chunk = data.split_to(granted.min(data.len()));
                    if send.send_data(chunk, false).is_err() {
                        return None;
                    }
                }
                continue;
            }
            Err(frame) => frame,
        };
        if let Ok(trailers) = frame.into_trailers() {
            let code = if status {
                grpc_status(&trailers)
            } else {
                Code::Ok as i32
            };
            let _ = send.send_trailers(trailers);
            return Some(code);
        }
    }
}

/// A request body read from an HTTP/2 stream, handing flow-control credit
/// back as it is read.
fn with_extra_trailers(
    mut response: http::Response<tonic::body::Body>,
) -> http::Response<tonic::body::Body> {
    let Some(ExtraTrailers(extra)) = response.extensions_mut().remove::<ExtraTrailers>() else {
        return response;
    };
    response.map(|inner| {
        tonic::body::Body::new(TrailersBody {
            inner,
            extra: Some(extra),
        })
    })
}

struct TrailersBody {
    inner: tonic::body::Body,
    extra: Option<http::HeaderMap>,
}

impl http_body::Body for TrailersBody {
    type Data = Bytes;
    type Error = Status;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Status>>> {
        let frame = match Pin::new(&mut self.inner).poll_frame(cx) {
            Poll::Ready(Some(Ok(frame))) => frame,
            other => return other,
        };
        match frame.into_trailers() {
            Ok(mut trailers) => {
                if let Some(extra) = self.extra.take() {
                    trailers.extend(extra);
                }
                Poll::Ready(Some(Ok(Frame::trailers(trailers))))
            }
            Err(frame) => Poll::Ready(Some(Ok(frame))),
        }
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}

pub(crate) struct RequestBody {
    recv: h2::RecvStream,
    data_done: bool,
    sizes: Option<Arc<CallSizes>>,
}

impl http_body::Body for RequestBody {
    type Data = Bytes;
    type Error = h2::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, h2::Error>>> {
        if !self.data_done {
            match self.recv.poll_data(cx) {
                Poll::Ready(Some(Ok(data))) => {
                    let _ = self.recv.flow_control().release_capacity(data.len());
                    if let Some(sizes) = &self.sizes {
                        sizes
                            .received
                            .fetch_add(data.len() as u64, Ordering::Relaxed);
                    }
                    return Poll::Ready(Some(Ok(Frame::data(data))));
                }
                Poll::Ready(Some(Err(err))) => return Poll::Ready(Some(Err(err))),
                Poll::Ready(None) => self.data_done = true,
                Poll::Pending => return Poll::Pending,
            }
        }
        match self.recv.poll_trailers(cx) {
            Poll::Ready(Ok(Some(trailers))) => Poll::Ready(Some(Ok(Frame::trailers(trailers)))),
            Poll::Ready(Ok(None)) => Poll::Ready(None),
            Poll::Ready(Err(err)) => Poll::Ready(Some(Err(err))),
            Poll::Pending => Poll::Pending,
        }
    }

    fn is_end_stream(&self) -> bool {
        self.recv.is_end_stream()
    }
}
