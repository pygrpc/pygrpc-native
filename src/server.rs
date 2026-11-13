//! HTTP/2 and gRPC framing for the native gRPC front.
//!
//! Calls are received on a small tokio runtime without the GIL and handed to
//! Python as `(call_id, method_id, payload, client_key)` events. Python answers
//! each call by id. Nothing in this module touches Python objects.

mod auth;
mod calls;
mod codec;
mod connection;
mod fallback;
mod listener;
mod order;
mod stream;
pub(crate) mod tls;

// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::io::{Read, Write};
use std::os::unix::io::{AsRawFd, RawFd};
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::sync::atomic::{fence, AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::{oneshot, Notify};
use tonic::Status;

use crate::metrics::{self, Metrics};
pub use auth::BearerAuth;
use calls::{shutting_down, StreamEnd, StreamSender, StreamSlot};
pub use listener::{accept_loop, Listener};

// The compressed flag and length in front of every gRPC message.
const GRPC_HEADER_LEN: u64 = 5;

// Enough to flush GOAWAY. grpcio clients keep an idle connection open after
// it, and like grpcio's stop(grace=None) we do not wait for them.
pub const CONNECTION_DRAIN_TIMEOUT: Duration = Duration::from_millis(100);

// How long a closing connection waits for its calls to finish sending.
pub const CALL_DRAIN_TIMEOUT: Duration = Duration::from_millis(250);

/// Per-connection HTTP/2 settings.
#[derive(Clone, Copy, Debug)]
pub struct ConnectionSettings {
    pub keepalive: Keepalive,
    /// How many streams a client may open at once; it waits for one to
    /// finish before opening more.
    pub max_concurrent_streams: u32,
    /// The largest header list a request may have.
    pub max_metadata_size: u32,
    /// The largest request message to a native method, as sent.
    pub max_receive_message_length: Option<usize>,
}

/// When the server pings a connection to check that the client is still
/// there, like grpc-core's keepalive arguments.
#[derive(Clone, Copy, Debug)]
pub struct Keepalive {
    /// Time between pings; None never pings.
    pub time: Option<Duration>,
    /// How long to wait for the answer before closing the connection.
    pub timeout: Duration,
    /// Whether to ping a connection that has no calls in flight.
    pub without_calls: bool,
}

/// Who a connection's calls come from.
pub struct Peer {
    pub address: Arc<str>,
    /// The client's certificate, on a connection that presented one.
    pub auth: Option<Arc<tls::PeerAuth>>,
}

pub enum ClientKey {
    Id(Bytes),
    Peer(Arc<Peer>),
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
    pub peer: Arc<Peer>,
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
    /// A message held by `_stream_send_or_park` went out, or its stream ended.
    Writable {
        call_id: u64,
        sent: bool,
    },
    /// A call refused for its message size; see `report_refused_calls`.
    Refused {
        call_id: u64,
        method_id: u32,
        details: Option<Box<CallDetails>>,
    },
    /// A call to a method not in the table; see `resolve_unknown_methods`.
    Resolve {
        resolve_id: u64,
        path: String,
        details: Option<Box<CallDetails>>,
    },
    Closed,
}

pub const MAX_RESOLVED: usize = 1024;

/// `(method_id, server_streaming)` of a method served natively.
type NativeMethod = (u32, bool);

/// A call's method path and where to send its answer.
type Resolving = (String, oneshot::Sender<Option<NativeMethod>>);

#[derive(Clone, Copy)]
enum Resolved {
    Native(u32, bool),
    Fallback,
}

pub struct Answer {
    pub payload: Bytes,
    pub initial_metadata: Option<tonic::metadata::MetadataMap>,
    pub trailing_metadata: Option<http::HeaderMap>,
}

pub type Reply = Result<Answer, Status>;

pub enum AddMethodsError {
    Started,
    Duplicate(String),
}

/// The event queue, and a socket an event loop can watch for new events.
pub struct EventSender {
    tx: crossbeam_channel::Sender<Event>,
    // (reader, writer)
    wake: OnceLock<(UnixStream, UnixStream)>,
    armed: AtomicBool,
}

impl EventSender {
    pub fn send(&self, event: Event) -> Result<(), crossbeam_channel::SendError<Event>> {
        let sent = self.tx.send(event);
        if let Some((_, writer)) = self.wake.get() {
            if !self.armed.swap(true, Ordering::SeqCst) {
                let _ = (&*writer).write(&[1]);
            }
        }
        sent
    }
}

pub struct Shared {
    // Registered until start(), then frozen so routing needs no lock.
    pending_methods: Mutex<Vec<(String, bool)>>,
    methods: OnceLock<HashMap<String, (u32, bool)>>,
    events: EventSender,
    unary: Mutex<HashMap<u64, oneshot::Sender<Reply>>>,
    streams: Mutex<HashMap<u64, StreamSlot>>,
    next_id: AtomicU64,
    closed: AtomicBool,
    connections: AtomicUsize,
    connections_done: Notify,
    fallback: OnceLock<PathBuf>,
    resolve_unknown: AtomicBool,
    report_refused: AtomicBool,
    resolved: RwLock<HashMap<String, Resolved>>,
    resolving: Mutex<HashMap<u64, Resolving>>,
    client_key_metadata: Option<String>,
    call_details: bool,
    delay_stream_headers: bool,
    bearer_auth: Option<BearerAuth>,
    connection: ConnectionSettings,
    // Shard count when metrics are on; the metrics are built at start().
    metrics_shards: Option<usize>,
    ordered: bool,
    metrics: OnceLock<Metrics>,
    metrics_since: u64,
}

impl Shared {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        methods: Vec<(String, bool)>,
        events: crossbeam_channel::Sender<Event>,
        client_key_metadata: Option<String>,
        call_details: bool,
        delay_stream_headers: bool,
        bearer_auth: Option<BearerAuth>,
        connection: ConnectionSettings,
        metrics_shards: Option<usize>,
        ordered: bool,
    ) -> Self {
        Shared {
            pending_methods: Mutex::new(methods),
            methods: OnceLock::new(),
            events: EventSender {
                tx: events,
                wake: OnceLock::new(),
                armed: AtomicBool::new(false),
            },
            unary: Mutex::new(HashMap::new()),
            streams: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(1),
            closed: AtomicBool::new(false),
            connections: AtomicUsize::new(0),
            connections_done: Notify::new(),
            fallback: OnceLock::new(),
            resolve_unknown: AtomicBool::new(false),
            report_refused: AtomicBool::new(false),
            resolved: RwLock::new(HashMap::new()),
            resolving: Mutex::new(HashMap::new()),
            client_key_metadata,
            call_details,
            delay_stream_headers,
            bearer_auth,
            connection,
            metrics_shards,
            ordered,
            metrics: OnceLock::new(),
            metrics_since: metrics::unix_nanos(),
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
        if let Some(shards) = self.metrics_shards {
            let names = pending.iter().map(|(path, _)| path.clone()).collect();
            let _ = self.metrics.set(Metrics::new(names, shards));
        }
    }

    pub fn resolve_unknown_methods(&self) -> bool {
        let _pending = self.pending_methods.lock().unwrap();
        if self.methods.get().is_some() {
            return false;
        }
        self.resolve_unknown.store(true, Ordering::Relaxed);
        true
    }

    pub fn report_refused_calls(&self) -> bool {
        let _pending = self.pending_methods.lock().unwrap();
        if self.methods.get().is_some() {
            return false;
        }
        self.report_refused.store(true, Ordering::Relaxed);
        true
    }

    pub fn resolved(
        &self,
        resolve_id: u64,
        method: Option<NativeMethod>,
        remember: bool,
    ) -> Result<bool, String> {
        if let Some((id, _)) = method {
            let first = self.methods.get().map_or(0, HashMap::len);
            if (id as usize) < first || id as usize >= first + MAX_RESOLVED {
                return Err(format!(
                    "method id {id} is outside {first}..{}",
                    first + MAX_RESOLVED
                ));
            }
        }
        let Some((path, sender)) = self.resolving.lock().unwrap().remove(&resolve_id) else {
            return Ok(false);
        };
        if remember || method.is_some() {
            let route = match method {
                Some((id, streaming)) => Resolved::Native(id, streaming),
                None => Resolved::Fallback,
            };
            self.resolved.write().unwrap().insert(path, route);
        }
        Ok(sender.send(method).is_ok())
    }

    pub fn metrics_enabled(&self) -> bool {
        self.metrics_shards.is_some()
    }

    /// None until start(), or when metrics are off.
    pub fn metrics(&self) -> Option<&Metrics> {
        self.metrics.get()
    }

    pub fn metrics_since(&self) -> u64 {
        self.metrics_since
    }

    pub fn set_fallback(&self, path: PathBuf) -> bool {
        self.fallback.set(path).is_ok()
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    pub fn wake_fd(&self) -> std::io::Result<RawFd> {
        if self.events.wake.get().is_none() {
            let (reader, writer) = UnixStream::pair()?;
            reader.set_nonblocking(true)?;
            writer.set_nonblocking(true)?;
            let _ = self.events.wake.set((reader, writer));
        }
        Ok(self.events.wake.get().expect("set above").0.as_raw_fd())
    }

    /// Lets the next event write to the wake socket again.
    pub fn rearm(&self) {
        if let Some((reader, _)) = self.events.wake.get() {
            let mut buf = [0u8; 64];
            while matches!((&*reader).read(&mut buf), Ok(n) if n > 0) {}
            self.events.armed.store(false, Ordering::SeqCst);
            // Pairs with the swap in send(): an event queued before this is
            // either in the caller's next try_recv or writes again.
            fence(Ordering::SeqCst);
        }
    }

    pub fn respond(&self, call_id: u64, reply: Reply) -> bool {
        let sender = self.unary.lock().unwrap().remove(&call_id);
        match sender {
            Some(sender) => sender.send(reply).is_ok(),
            None => false,
        }
    }

    pub fn stream_sender(&self, call_id: u64) -> Option<StreamSender> {
        let streams = self.streams.lock().unwrap();
        streams.get(&call_id).map(|slot| slot.sender.clone())
    }

    /// The sender for a stream's next message, releasing its headers first.
    pub fn message_sender(&self, call_id: u64) -> Option<StreamSender> {
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

    pub fn writable(&self, call_id: u64, sent: bool) {
        let _ = self.events.send(Event::Writable { call_id, sent });
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
        self.resolving.lock().unwrap().clear();
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
