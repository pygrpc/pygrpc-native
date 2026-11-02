//! Calls handed to Python, and how their replies reach the client.

// SPDX-License-Identifier: Apache-2.0

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use futures_core::Stream;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, oneshot, Semaphore, TryAcquireError};
use tonic::server::{ServerStreamingService, UnaryService};
use tonic::Status;

use super::order::turn;
use super::{CallDetails, ClientKey, Event, MetadataValue, Shared};

// Bytes a stream queues before the writer waits; a larger message goes alone.
const STREAM_BUDGET: usize = 256 * 1024;

const STREAM_QUEUE: usize = 64;

/// Response metadata the trailers of a unary OK response must carry, which
/// tonic's unary encoder has no room for.
#[derive(Clone)]
pub(super) struct ExtraTrailers(pub(super) http::HeaderMap);

/// How a server-streaming call ends once its message channel is drained.
/// Only `stream_end` may end it with OK; a channel that closes without an
/// end recorded (server shutdown) must not look like a complete stream.
pub(super) enum StreamEnd {
    Open,
    Ok,
    // Also used for OK with trailing metadata: tonic writes any status
    // yielded by the stream as the trailers.
    Status(Status),
}

#[derive(Clone)]
pub struct StreamSender {
    sender: mpsc::Sender<Bytes>,
    room: Arc<Semaphore>,
}

fn room_for(message: &Bytes) -> u32 {
    message.len().clamp(1, STREAM_BUDGET) as u32
}

impl StreamSender {
    /// Waits while the stream is full; false once it has ended.
    pub fn blocking_send(&self, runtime: &tokio::runtime::Handle, message: Bytes) -> bool {
        let room = room_for(&message);
        match self.room.try_acquire_many(room) {
            Ok(permit) => permit.forget(),
            Err(TryAcquireError::Closed) => return false,
            Err(TryAcquireError::NoPermits) => {
                match runtime.block_on(self.room.acquire_many(room)) {
                    Ok(permit) => permit.forget(),
                    Err(_) => return false,
                }
            }
        }
        self.sender.blocking_send(message).is_ok()
    }

    pub fn try_send(&self, message: Bytes) -> Result<(), TrySendError<Bytes>> {
        let room = room_for(&message);
        match self.room.try_acquire_many(room) {
            Ok(permit) => permit.forget(),
            Err(TryAcquireError::NoPermits) => return Err(TrySendError::Full(message)),
            Err(TryAcquireError::Closed) => return Err(TrySendError::Closed(message)),
        }
        self.sender.try_send(message).inspect_err(|_| {
            self.room.add_permits(room as usize);
        })
    }

    pub async fn send(&self, message: Bytes) -> bool {
        match self.room.acquire_many(room_for(&message)).await {
            Ok(permit) => permit.forget(),
            Err(_) => return false,
        }
        self.sender.send(message).await.is_ok()
    }
}

pub(crate) struct StreamSlot {
    pub(super) sender: StreamSender,
    pub(super) end: Arc<Mutex<StreamEnd>>,
    // With delay_stream_headers, releases the response headers; an Err ends
    // the call before any message, as a trailers-only response.
    pub(super) start: Option<oneshot::Sender<Result<tonic::metadata::MetadataMap, Status>>>,
}

impl StreamSlot {
    pub(super) fn finish(&self, end: StreamEnd) {
        let mut current = self.end.lock().unwrap();
        if matches!(*current, StreamEnd::Open) {
            *current = end;
        }
    }
}

pub(super) fn shutting_down() -> Status {
    Status::unavailable("gRPC server is shutting down")
}

pub(super) fn deadline_exceeded() -> Status {
    Status::deadline_exceeded("Deadline Exceeded")
}

/// The call's `grpc-timeout`, as the gRPC HTTP/2 spec encodes it.
pub(super) fn call_timeout(headers: &http::HeaderMap) -> Option<Duration> {
    let value = headers.get("grpc-timeout")?.to_str().ok()?;
    if value.len() < 2 || value.len() > 9 {
        return None;
    }
    let (amount, unit) = value.split_at(value.len() - 1);
    let amount: u64 = amount.parse().ok()?;
    Some(match unit {
        "H" => Duration::from_secs(amount.saturating_mul(3600)),
        "M" => Duration::from_secs(amount.saturating_mul(60)),
        "S" => Duration::from_secs(amount),
        "m" => Duration::from_millis(amount),
        "u" => Duration::from_micros(amount),
        "n" => Duration::from_nanos(amount),
        _ => return None,
    })
}

fn client_key(
    metadata: &tonic::metadata::MetadataMap,
    key: Option<&str>,
    peer: &Arc<str>,
) -> ClientKey {
    let id = key.and_then(|key| {
        if key.ends_with("-bin") {
            metadata
                .get_bin(key)
                .and_then(|value| value.to_bytes().ok())
        } else {
            metadata
                .get(key)
                .map(|value| Bytes::copy_from_slice(value.as_encoded_bytes()))
        }
    });
    match id {
        Some(id) => ClientKey::Id(id),
        None => ClientKey::Peer(peer.clone()),
    }
}

// Transport headers grpcio leaves out of invocation_metadata().
const TRANSPORT_HEADERS: [&str; 5] = [
    "te",
    "content-type",
    "grpc-timeout",
    "grpc-encoding",
    "grpc-accept-encoding",
];

pub(super) fn call_details(
    shared: &Shared,
    metadata: &tonic::metadata::MetadataMap,
    peer: &Arc<str>,
    deadline: Option<tokio::time::Instant>,
) -> Option<Box<CallDetails>> {
    if !shared.call_details {
        return None;
    }
    let metadata = metadata
        .iter()
        .filter_map(|entry| match entry {
            tonic::metadata::KeyAndValueRef::Ascii(key, value) => {
                if TRANSPORT_HEADERS.contains(&key.as_str()) {
                    return None;
                }
                let value = Bytes::copy_from_slice(value.as_encoded_bytes());
                Some((key.as_str().to_owned(), MetadataValue::Ascii(value)))
            }
            tonic::metadata::KeyAndValueRef::Binary(key, value) => {
                let value = value.to_bytes().ok()?;
                Some((key.as_str().to_owned(), MetadataValue::Binary(value)))
            }
        })
        .collect();
    Some(Box::new(CallDetails {
        metadata,
        peer: peer.clone(),
        deadline,
    }))
}

/// Removes a unary call from the table if its future is dropped first.
struct UnaryGuard {
    shared: Arc<Shared>,
    call_id: u64,
}

impl Drop for UnaryGuard {
    fn drop(&mut self) {
        let unanswered = self
            .shared
            .unary
            .lock()
            .unwrap()
            .remove(&self.call_id)
            .is_some();
        // Only with call_details, so 0.1 callers never see cancels for unary calls.
        if unanswered && self.shared.call_details && !self.shared.is_closed() {
            let _ = self.shared.events.send(Event::Cancel {
                call_id: self.call_id,
            });
        }
    }
}

#[derive(Clone)]
pub(super) struct UnaryCall {
    pub(super) shared: Arc<Shared>,
    pub(super) method_id: u32,
    pub(super) peer: Arc<str>,
    pub(super) deadline: Option<tokio::time::Instant>,
}

impl UnaryService<Bytes> for UnaryCall {
    type Response = Bytes;
    type Future = Pin<Box<dyn Future<Output = Result<tonic::Response<Bytes>, Status>> + Send>>;

    fn call(&mut self, request: tonic::Request<Bytes>) -> Self::Future {
        let shared = self.shared.clone();
        let method_id = self.method_id;
        let client_key = client_key(
            request.metadata(),
            shared.client_key_metadata.as_deref(),
            &self.peer,
        );
        let details = call_details(&shared, request.metadata(), &self.peer, self.deadline);
        let turn = turn(&shared, &request);
        Box::pin(async move {
            if let Some(turn) = &turn {
                turn.wait().await;
            }
            let call_id = shared.next_call_id();
            let (sender, receiver) = oneshot::channel();
            shared.unary.lock().unwrap().insert(call_id, sender);
            let _guard = UnaryGuard {
                shared: shared.clone(),
                call_id,
            };
            if shared.is_closed() {
                return Err(shutting_down());
            }
            let event = Event::Call {
                call_id,
                method_id,
                payload: request.into_inner(),
                client_key,
                details,
            };
            if shared.events.send(event).is_err() {
                return Err(shutting_down());
            }
            drop(turn);
            match receiver.await {
                Ok(reply) => reply.map(|answer| {
                    let mut response = tonic::Response::new(answer.payload);
                    if let Some(metadata) = answer.initial_metadata {
                        *response.metadata_mut() = metadata;
                    }
                    if let Some(trailers) = answer.trailing_metadata {
                        response.extensions_mut().insert(ExtraTrailers(trailers));
                    }
                    response
                }),
                Err(_) => Err(shutting_down()),
            }
        })
    }
}

/// Response stream of a server-streaming call. Dropping it (client gone,
/// connection closed) tells Python to close the handler's stream.
pub(crate) struct ResponseStream {
    receiver: mpsc::Receiver<Bytes>,
    room: Arc<Semaphore>,
    end: Arc<Mutex<StreamEnd>>,
    shared: Arc<Shared>,
    call_id: u64,
    deadline: Option<Pin<Box<tokio::time::Sleep>>>,
    done: bool,
}

impl Stream for ResponseStream {
    type Item = Result<Bytes, Status>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Bytes, Status>>> {
        if self.done {
            return Poll::Ready(None);
        }
        if let Some(deadline) = self.deadline.as_mut() {
            if deadline.as_mut().poll(cx).is_ready() {
                self.done = true;
                return Poll::Ready(Some(Err(deadline_exceeded())));
            }
        }
        match self.receiver.poll_recv(cx) {
            Poll::Ready(Some(message)) => {
                self.room.add_permits(room_for(&message) as usize);
                Poll::Ready(Some(Ok(message)))
            }
            Poll::Ready(None) => {
                self.done = true;
                let end = std::mem::replace(&mut *self.end.lock().unwrap(), StreamEnd::Ok);
                Poll::Ready(match end {
                    StreamEnd::Ok => None,
                    StreamEnd::Status(status) => Some(Err(status)),
                    StreamEnd::Open => Some(Err(shutting_down())),
                })
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for ResponseStream {
    fn drop(&mut self) {
        // Wakes senders waiting for room.
        self.room.close();
        let was_open = self
            .shared
            .streams
            .lock()
            .unwrap()
            .remove(&self.call_id)
            .is_some();
        if was_open && !self.shared.is_closed() {
            let _ = self.shared.events.send(Event::Cancel {
                call_id: self.call_id,
            });
        }
    }
}

#[derive(Clone)]
pub(super) struct StreamingCall {
    pub(super) shared: Arc<Shared>,
    pub(super) method_id: u32,
    pub(super) peer: Arc<str>,
    pub(super) deadline: Option<tokio::time::Instant>,
}

impl ServerStreamingService<Bytes> for StreamingCall {
    type Response = Bytes;
    type ResponseStream = ResponseStream;
    type Future =
        Pin<Box<dyn Future<Output = Result<tonic::Response<ResponseStream>, Status>> + Send>>;

    fn call(&mut self, request: tonic::Request<Bytes>) -> Self::Future {
        let shared = self.shared.clone();
        let method_id = self.method_id;
        let client_key = client_key(
            request.metadata(),
            shared.client_key_metadata.as_deref(),
            &self.peer,
        );
        let details = call_details(&shared, request.metadata(), &self.peer, self.deadline);
        let deadline = self
            .deadline
            .map(|deadline| Box::pin(tokio::time::sleep_until(deadline)));
        let turn = turn(&shared, &request);
        Box::pin(async move {
            if let Some(turn) = &turn {
                turn.wait().await;
            }
            let call_id = shared.next_call_id();
            let (sender, receiver) = mpsc::channel(STREAM_QUEUE);
            let room = Arc::new(Semaphore::new(STREAM_BUDGET));
            let end = Arc::new(Mutex::new(StreamEnd::Open));
            let (start, started) = if shared.delay_stream_headers {
                let (start, started) = oneshot::channel();
                (Some(start), Some(started))
            } else {
                (None, None)
            };
            let slot = StreamSlot {
                sender: StreamSender {
                    sender,
                    room: room.clone(),
                },
                end: end.clone(),
                start,
            };
            shared.streams.lock().unwrap().insert(call_id, slot);
            let stream = ResponseStream {
                receiver,
                room,
                end,
                shared: shared.clone(),
                call_id,
                deadline,
                done: false,
            };
            if shared.is_closed() {
                return Err(shutting_down());
            }
            let event = Event::Call {
                call_id,
                method_id,
                payload: request.into_inner(),
                client_key,
                details,
            };
            if shared.events.send(event).is_err() {
                return Err(shutting_down());
            }
            drop(turn);
            let Some(started) = started else {
                return Ok(tonic::Response::new(stream));
            };
            match started.await {
                Ok(Ok(metadata)) => {
                    let mut response = tonic::Response::new(stream);
                    *response.metadata_mut() = metadata;
                    Ok(response)
                }
                Ok(Err(status)) => Err(status),
                Err(_) => Err(shutting_down()),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn timeout_of(value: &str) -> Option<Duration> {
        let mut headers = http::HeaderMap::new();
        headers.insert("grpc-timeout", value.parse().unwrap());
        call_timeout(&headers)
    }

    #[test]
    fn parses_every_grpc_timeout_unit() {
        assert_eq!(timeout_of("2H"), Some(Duration::from_secs(7200)));
        assert_eq!(timeout_of("3M"), Some(Duration::from_secs(180)));
        assert_eq!(timeout_of("4S"), Some(Duration::from_secs(4)));
        assert_eq!(timeout_of("250m"), Some(Duration::from_millis(250)));
        assert_eq!(timeout_of("7u"), Some(Duration::from_micros(7)));
        assert_eq!(timeout_of("9n"), Some(Duration::from_nanos(9)));
    }

    #[test]
    fn ignores_malformed_grpc_timeouts() {
        // The spec allows at most 8 digits.
        assert_eq!(timeout_of("123456789S"), None);
        assert_eq!(timeout_of("S"), None);
        assert_eq!(timeout_of("10x"), None);
        assert_eq!(timeout_of("-1S"), None);
        assert_eq!(call_timeout(&http::HeaderMap::new()), None);
    }
}
