// SPDX-License-Identifier: Apache-2.0
//! HTTP/2 and gRPC framing for the native gRPC front.
//!
//! Calls are received on a small tokio runtime without the GIL and handed to
//! Python as `(call_id, method_id, payload, client_key)` events. Python answers
//! each call by id. Nothing in this file touches Python objects.

use std::collections::HashMap;
use std::future::{poll_fn, Future};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::{Buf, BufMut, Bytes};
use futures_core::Stream;
use http_body::{Body as _, Frame};
use hyper::client::conn::http2 as client_http2;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{mpsc, oneshot, watch, Notify};
use tonic::codec::{Codec, CompressionEncoding, DecodeBuf, Decoder, EncodeBuf, Encoder};
use tonic::server::{Grpc, ServerStreamingService, UnaryService};
use tonic::{Code, Status};

const STREAM_READ_AHEAD: usize = 1;
// Enough to flush GOAWAY. grpcio clients keep an idle connection open after
// it, and like grpcio's stop(grace=None) we do not wait for them.
pub const CONNECTION_DRAIN_TIMEOUT: Duration = Duration::from_millis(100);
// Fixed windows instead of BDP probing. A stream that is not read holds up
// at most STREAM_WINDOW; the connection window is the protocol maximum so such
// streams never stall the others sharing the connection, which grpc-core
// reaches by growing its window.
const STREAM_WINDOW: u32 = 4 << 20;
const CONNECTION_WINDOW: u32 = (1 << 31) - 1;
// Bounds what one client can make the server hold; a client opening more
// streams waits for one to finish. hyper's server default is 200, but an
// engine keeps lookups and transfers in flight on one connection.
const MAX_CONCURRENT_STREAMS: u32 = 1024;
// grpc-core's limit on request metadata.
const MAX_HEADER_LIST_SIZE: u32 = 16 << 10;
// grpc-core's default; a connection that never finishes the preface is dropped.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(120);
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(2 * 60 * 60);
const KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(20);

// Unix sockets have no peer address, so each connection gets its own name.
static NEXT_UNIX_PEER: AtomicU64 = AtomicU64::new(1);

pub enum ClientKey {
    Id(Bytes),
    Peer(Arc<str>),
}

/// A request metadata value: text, or the decoded bytes of a `-bin` key.
pub enum MetadataValue {
    Ascii(Bytes),
    Binary(Bytes),
}

/// What a call carries besides its payload, collected only when the server
/// is created with `call_details`.
pub struct CallDetails {
    pub metadata: Vec<(String, MetadataValue)>,
    pub peer: Arc<str>,
    pub deadline: Option<tokio::time::Instant>,
}

pub enum Event {
    Call {
        call_id: u64,
        method_id: u32,
        payload: Bytes,
        client_key: ClientKey,
        details: Option<Box<CallDetails>>,
    },
    /// The client went away before a server-streaming call finished.
    Cancel {
        call_id: u64,
    },
    Closed,
}

pub struct Answer {
    pub payload: Bytes,
    pub initial_metadata: Option<tonic::metadata::MetadataMap>,
    pub trailing_metadata: Option<http::HeaderMap>,
}

pub type Reply = Result<Answer, Status>;

/// Response metadata the trailers of a unary OK response must carry, which
/// tonic's unary encoder has no room for.
#[derive(Clone)]
struct ExtraTrailers(http::HeaderMap);

/// How a server-streaming call ends once its message channel is drained.
/// Only `stream_end` may end it with OK; a channel that closes without an
/// end recorded (server shutdown) must not look like a complete stream.
enum StreamEnd {
    Open,
    Ok,
    // Also used for OK with trailing metadata: tonic writes any status
    // yielded by the stream as the trailers.
    Status(Status),
}

pub(crate) struct StreamSlot {
    sender: mpsc::Sender<Bytes>,
    end: Arc<Mutex<StreamEnd>>,
    // With delay_stream_headers, releases the response headers; an Err ends
    // the call before any message, as a trailers-only response.
    start: Option<oneshot::Sender<Result<tonic::metadata::MetadataMap, Status>>>,
}

impl StreamSlot {
    fn finish(&self, end: StreamEnd) {
        let mut current = self.end.lock().unwrap();
        if matches!(*current, StreamEnd::Open) {
            *current = end;
        }
    }
}

pub enum AddMethodsError {
    Started,
    Duplicate(String),
}

pub struct Shared {
    // Registered until start(), then frozen so routing needs no lock.
    pending_methods: Mutex<Vec<(String, bool)>>,
    methods: OnceLock<HashMap<String, (u32, bool)>>,
    events: crossbeam_channel::Sender<Event>,
    unary: Mutex<HashMap<u64, oneshot::Sender<Reply>>>,
    streams: Mutex<HashMap<u64, StreamSlot>>,
    next_id: AtomicU64,
    closed: AtomicBool,
    connections: AtomicUsize,
    connections_done: Notify,
    fallback: OnceLock<PathBuf>,
    client_key_metadata: Option<String>,
    call_details: bool,
    delay_stream_headers: bool,
}

impl Shared {
    pub fn new(
        methods: Vec<(String, bool)>,
        events: crossbeam_channel::Sender<Event>,
        client_key_metadata: Option<String>,
        call_details: bool,
        delay_stream_headers: bool,
    ) -> Self {
        Shared {
            pending_methods: Mutex::new(methods),
            methods: OnceLock::new(),
            events,
            unary: Mutex::new(HashMap::new()),
            streams: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(1),
            closed: AtomicBool::new(false),
            connections: AtomicUsize::new(0),
            connections_done: Notify::new(),
            fallback: OnceLock::new(),
            client_key_metadata,
            call_details,
            delay_stream_headers,
        }
    }

    pub fn add_methods(&self, methods: Vec<(String, bool)>) -> Result<Vec<u32>, AddMethodsError> {
        let mut pending = self.pending_methods.lock().unwrap();
        if self.methods.get().is_some() {
            return Err(AddMethodsError::Started);
        }
        for (path, _) in &methods {
            if pending.iter().any(|(known, _)| known == path) {
                return Err(AddMethodsError::Duplicate(path.clone()));
            }
        }
        let first = pending.len() as u32;
        pending.extend(methods);
        Ok((first..pending.len() as u32).collect())
    }

    pub fn freeze_methods(&self) {
        let pending = self.pending_methods.lock().unwrap();
        let methods = pending
            .iter()
            .enumerate()
            .map(|(i, (path, streaming))| (path.clone(), (i as u32, *streaming)))
            .collect();
        let _ = self.methods.set(methods);
    }

    pub fn set_fallback(&self, path: PathBuf) -> bool {
        self.fallback.set(path).is_ok()
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    pub fn respond(&self, call_id: u64, reply: Reply) -> bool {
        let sender = self.unary.lock().unwrap().remove(&call_id);
        match sender {
            Some(sender) => sender.send(reply).is_ok(),
            None => false,
        }
    }

    pub fn stream_sender(&self, call_id: u64) -> Option<mpsc::Sender<Bytes>> {
        let streams = self.streams.lock().unwrap();
        streams.get(&call_id).map(|slot| slot.sender.clone())
    }

    /// The sender for a stream's next message, releasing its headers first.
    pub fn message_sender(&self, call_id: u64) -> Option<mpsc::Sender<Bytes>> {
        let mut streams = self.streams.lock().unwrap();
        let slot = streams.get_mut(&call_id)?;
        if let Some(start) = slot.start.take() {
            let _ = start.send(Ok(tonic::metadata::MetadataMap::new()));
        }
        Some(slot.sender.clone())
    }

    pub fn send_initial_metadata(
        &self,
        call_id: u64,
        metadata: tonic::metadata::MetadataMap,
    ) -> bool {
        let mut streams = self.streams.lock().unwrap();
        let start = streams.get_mut(&call_id).and_then(|slot| slot.start.take());
        start.is_some_and(|start| start.send(Ok(metadata)).is_ok())
    }

    /// End a stream; it closes once the messages already queued are sent.
    pub fn end_stream(&self, call_id: u64, status: Option<Status>) -> bool {
        let Some(mut slot) = self.streams.lock().unwrap().remove(&call_id) else {
            return false;
        };
        if let Some(start) = slot.start.take() {
            let status = status.clone().unwrap_or_else(|| Status::ok(""));
            let _ = start.send(Err(status));
        }
        slot.finish(match status {
            None => StreamEnd::Ok,
            Some(status) => StreamEnd::Status(status),
        });
        true
    }

    pub fn is_unary(&self, call_id: u64) -> bool {
        self.unary.lock().unwrap().contains_key(&call_id)
    }

    pub fn close(&self) {
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        let _ = self.events.send(Event::Closed);
        let pending: Vec<_> = self.unary.lock().unwrap().drain().collect();
        for (_, sender) in pending {
            let _ = sender.send(Err(shutting_down()));
        }
        let streams: Vec<_> = self.streams.lock().unwrap().drain().collect();
        for (_, slot) in streams {
            slot.finish(StreamEnd::Status(shutting_down()));
        }
    }

    pub async fn wait_connections_closed(&self, timeout: Duration) {
        let drained = async {
            loop {
                let notified = self.connections_done.notified();
                if self.connections.load(Ordering::Acquire) == 0 {
                    return;
                }
                notified.await;
            }
        };
        let _ = tokio::time::timeout(timeout, drained).await;
    }

    fn next_call_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }
}

fn shutting_down() -> Status {
    Status::unavailable("gRPC server is shutting down")
}

fn deadline_exceeded() -> Status {
    Status::deadline_exceeded("Deadline Exceeded")
}

/// The call's `grpc-timeout`, as the gRPC HTTP/2 spec encodes it.
fn call_timeout(headers: &http::HeaderMap) -> Option<Duration> {
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

/// The connection to the grpcio server that serves the methods this front
/// does not know (out-of-tree services). Each client connection gets its own,
/// as it would talking to grpcio directly, so a client that stops reading only
/// stalls its own calls.
#[derive(Default)]
struct Fallback {
    sender: tokio::sync::Mutex<Option<client_http2::SendRequest<RequestBody>>>,
}

impl Fallback {
    async fn sender(&self, path: &Path) -> std::io::Result<client_http2::SendRequest<RequestBody>> {
        let mut sender = self.sender.lock().await;
        if let Some(open) = sender.as_ref().filter(|open| !open.is_closed()) {
            return Ok(open.clone());
        }
        let stream = tokio::net::UnixStream::connect(path).await?;
        // The same windows as the client side, so a stalled client holds up
        // at most as much here.
        let (open, connection) = client_http2::Builder::new(TokioExecutor::new())
            .timer(TokioTimer::new())
            .initial_stream_window_size(STREAM_WINDOW)
            .initial_connection_window_size(CONNECTION_WINDOW)
            .handshake(TokioIo::new(stream))
            .await
            .map_err(std::io::Error::other)?;
        tokio::spawn(async move {
            if let Err(err) = connection.await {
                log::debug!("fallback gRPC connection ended with error: {err}");
            }
        });
        *sender = Some(open.clone());
        Ok(open)
    }

    async fn forward(
        &self,
        path: &Path,
        mut request: http::Request<RequestBody>,
    ) -> http::Response<tonic::body::Body> {
        let Some(path_and_query) = request.uri().path_and_query().cloned() else {
            return Status::new(Code::Unimplemented, "Method not found!").into_http();
        };
        let uri = http::Uri::builder()
            .scheme("http")
            .authority("localhost")
            .path_and_query(path_and_query)
            .build();
        match uri {
            Ok(uri) => *request.uri_mut() = uri,
            Err(err) => return Status::internal(err.to_string()).into_http(),
        }
        let mut sender = match self.sender(path).await {
            Ok(sender) => sender,
            Err(err) => {
                return Status::unavailable(format!("out-of-tree gRPC services: {err}")).into_http()
            }
        };
        match sender.send_request(request).await {
            Ok(response) => response.map(tonic::body::Body::new),
            Err(err) => {
                Status::unavailable(format!("out-of-tree gRPC services: {err}")).into_http()
            }
        }
    }
}

/// Passes message bytes through untouched; Python owns the protobuf codec.
#[derive(Clone, Copy, Default)]
struct BytesCodec;

impl Codec for BytesCodec {
    type Encode = Bytes;
    type Decode = Bytes;
    type Encoder = BytesCodec;
    type Decoder = BytesCodec;

    fn encoder(&mut self) -> Self::Encoder {
        BytesCodec
    }

    fn decoder(&mut self) -> Self::Decoder {
        BytesCodec
    }
}

impl Encoder for BytesCodec {
    type Item = Bytes;
    type Error = Status;

    fn encode(&mut self, item: Bytes, dst: &mut EncodeBuf<'_>) -> Result<(), Status> {
        dst.put(item);
        Ok(())
    }
}

impl Decoder for BytesCodec {
    type Item = Bytes;
    type Error = Status;

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<Bytes>, Status> {
        let len = src.remaining();
        Ok(Some(src.copy_to_bytes(len)))
    }
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

fn call_details(
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
        self.shared.unary.lock().unwrap().remove(&self.call_id);
    }
}

#[derive(Clone)]
struct UnaryCall {
    shared: Arc<Shared>,
    method_id: u32,
    peer: Arc<str>,
    deadline: Option<tokio::time::Instant>,
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
        Box::pin(async move {
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
            Poll::Ready(Some(message)) => Poll::Ready(Some(Ok(message))),
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
struct StreamingCall {
    shared: Arc<Shared>,
    method_id: u32,
    peer: Arc<str>,
    deadline: Option<tokio::time::Instant>,
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
        Box::pin(async move {
            let call_id = shared.next_call_id();
            let (sender, receiver) = mpsc::channel(STREAM_READ_AHEAD);
            let end = Arc::new(Mutex::new(StreamEnd::Open));
            let (start, started) = if shared.delay_stream_headers {
                let (start, started) = oneshot::channel();
                (Some(start), Some(started))
            } else {
                (None, None)
            };
            let slot = StreamSlot {
                sender,
                end: end.clone(),
                start,
            };
            shared.streams.lock().unwrap().insert(call_id, slot);
            let stream = ResponseStream {
                receiver,
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

fn grpc() -> Grpc<BytesCodec> {
    // Like grpcio servers: take gzip requests, but answer uncompressed.
    Grpc::new(BytesCodec)
        .accept_compressed(CompressionEncoding::Gzip)
        .max_decoding_message_size(usize::MAX)
        .max_encoding_message_size(usize::MAX)
}

async fn route(
    shared: Arc<Shared>,
    peer: Arc<str>,
    fallback: Arc<Fallback>,
    request: http::Request<RequestBody>,
) -> http::Response<tonic::body::Body> {
    let method = shared
        .methods
        .get()
        .and_then(|methods| methods.get(request.uri().path()));
    let Some(&(method_id, streaming)) = method else {
        return match shared.fallback.get() {
            Some(path) => fallback.forward(path, request).await,
            None => Status::new(Code::Unimplemented, "Method not found!").into_http(),
        };
    };
    // The deadline runs from when the headers arrive, so it also covers a
    // request body that is slow to come.
    let deadline = call_timeout(request.headers()).map(|t| tokio::time::Instant::now() + t);
    let response = async {
        if streaming {
            let call = StreamingCall {
                shared,
                method_id,
                peer,
                deadline,
            };
            grpc().server_streaming(call, request).await
        } else {
            let call = UnaryCall {
                shared,
                method_id,
                peer,
                deadline,
            };
            with_extra_trailers(grpc().unary(call, request).await)
        }
    };
    match deadline {
        Some(deadline) => tokio::time::timeout_at(deadline, response)
            .await
            .unwrap_or_else(|_| deadline_exceeded().into_http()),
        None => response.await,
    }
}

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
    let handshake = h2::server::Builder::new()
        .initial_window_size(STREAM_WINDOW)
        .initial_connection_window_size(CONNECTION_WINDOW)
        .max_concurrent_streams(MAX_CONCURRENT_STREAMS)
        .max_header_list_size(MAX_HEADER_LIST_SIZE)
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
    let keepalive = keepalive(connection.ping_pong());
    tokio::pin!(keepalive);
    loop {
        tokio::select! {
            accepted = connection.accept() => match accepted {
                Some(Ok((request, respond))) => {
                    tokio::spawn(serve_stream(
                        shared.clone(),
                        peer.clone(),
                        fallback.clone(),
                        request,
                        respond,
                    ));
                }
                Some(Err(err)) => {
                    log::debug!("gRPC connection ended with error: {err}");
                    return;
                }
                None => return,
            },
            _ = &mut keepalive => break,
            _ = stopped(&mut shutdown) => break,
        }
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

/// Resolves once the peer stops answering keepalive pings.
async fn keepalive(ping_pong: Option<h2::PingPong>) {
    let Some(mut ping_pong) = ping_pong else {
        return std::future::pending().await;
    };
    loop {
        tokio::time::sleep(KEEPALIVE_INTERVAL).await;
        let pong = tokio::time::timeout(KEEPALIVE_TIMEOUT, ping_pong.ping(h2::Ping::opaque()));
        if !matches!(pong.await, Ok(Ok(_))) {
            log::debug!("gRPC peer missed a keepalive ping; closing the connection");
            return;
        }
    }
}

async fn serve_stream(
    shared: Arc<Shared>,
    peer: Arc<str>,
    fallback: Arc<Fallback>,
    request: http::Request<h2::RecvStream>,
    mut respond: h2::server::SendResponse<Bytes>,
) {
    let request = request.map(|recv| RequestBody {
        recv,
        data_done: false,
    });
    let route = route(shared, peer, fallback, request);
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
    let Ok(mut send) = respond.send_response(http::Response::from_parts(parts, ()), end_of_stream)
    else {
        return;
    };
    if !end_of_stream {
        send_body(&mut send, body).await;
    }
}

/// Send a response body within HTTP/2 flow control. Returns early when the
/// client resets the stream, which drops the body.
async fn send_body(send: &mut h2::SendStream<Bytes>, body: tonic::body::Body) {
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
            None => return,
            Some(None) => {
                let _ = send.send_data(Bytes::new(), true);
                return;
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
                return;
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
                        _ => return,
                    };
                    let chunk = data.split_to(granted.min(data.len()));
                    if send.send_data(chunk, false).is_err() {
                        return;
                    }
                }
                continue;
            }
            Err(frame) => frame,
        };
        if let Ok(trailers) = frame.into_trailers() {
            let _ = send.send_trailers(trailers);
            return;
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

async fn stopped(shutdown: &mut watch::Receiver<bool>) {
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
) {
    loop {
        let accepted = tokio::select! {
            _ = stopped(&mut shutdown) => return,
            accepted = accept(&listener) => accepted,
        };
        match accepted {
            Ok(Accepted::Tcp(stream, peer)) => {
                let _ = stream.set_nodelay(true);
                tokio::spawn(serve_connection(
                    stream,
                    peer,
                    shared.clone(),
                    shutdown.clone(),
                ));
            }
            Ok(Accepted::Unix(stream, peer)) => {
                tokio::spawn(serve_connection(
                    stream,
                    peer,
                    shared.clone(),
                    shutdown.clone(),
                ));
            }
            Err(err) => {
                // Out of file descriptors (EMFILE): back off instead of spinning.
                log::warn!("gRPC accept failed: {err}");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
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
                std::net::SocketAddr::V6(addr) => format!("ipv6:{addr}"),
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
