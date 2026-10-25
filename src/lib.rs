// SPDX-License-Identifier: Apache-2.0
//! A native gRPC front for Python servers.
//!
//! The grpcio Python server spends most of each small RPC in its own Python
//! layer, all under the GIL. This module does the HTTP/2 and gRPC framing on
//! tokio threads instead and hands Python each call as an event: an id, the
//! method, the request bytes and who sent it.
//! Python still decodes the protobuf and runs the handler, so the wire format,
//! the clients and the handlers stay exactly as they are.

mod server;

use std::net::ToSocketAddrs;
use std::os::unix::fs::FileTypeExt;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use crossbeam_channel::{Receiver, RecvTimeoutError};
use pyo3::exceptions::{PyOSError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedBytes;
use pyo3::types::{PyBytes, PyList, PyTuple};
use tokio::runtime::Runtime;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::watch;
use tonic::metadata::{
    AsciiMetadataKey, AsciiMetadataValue, BinaryMetadataKey, BinaryMetadataValue, MetadataMap,
};
use tonic::{Code, Status};

use server::{
    accept_loop, AddMethodsError, Answer, BearerAuth, CallDetails, ClientKey, Event, Listener,
    MetadataValue, Shared, CONNECTION_DRAIN_TIMEOUT,
};

const RUNTIME_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);

const EVENT_CALL: u8 = 0;
const EVENT_CANCEL: u8 = 1;
const STREAM_SENT: u8 = 0;
const STREAM_FULL: u8 = 1;
const STREAM_CLOSED: u8 = 2;

struct Running {
    runtime: Runtime,
    listeners: Vec<Listener>,
    shutdown: watch::Sender<bool>,
}

/// A gRPC server whose calls are answered from Python by call id.
///
/// Args:
///     target: ``host:port``, ``unix:path`` or ``unix:///path``. Port 0 picks
///         a free port. A host is bound on every address it resolves to.
///     methods: ``(path, server_streaming)`` for every method, where path is
///         ``/package.Service/Method``. The method id passed to Python is the
///         index in this list.
///     io_threads: Number of tokio threads doing HTTP/2 work.
///     client_key_metadata: Request metadata key whose value is passed to
///         Python as the client key, or None to always pass the peer address.
///         Calls without it also get the peer address.
///     call_details: Collect each call's request metadata, peer and deadline
///         for ``recv_batch_details``, and also send a cancel event when a
///         unary call ends (client cancel or deadline) before it is answered.
///     delay_stream_headers: Send a stream's response headers with its first
///         message or ``stream_send_initial_metadata``, as grpcio does,
///         instead of when the call starts.
///     bearer_token_sha256: SHA-256 digests of the accepted bearer tokens.
///         When set, a call without ``authorization: Bearer <token>`` for
///         one of them ends with UNAUTHENTICATED before it reaches Python or
///         the fallback.
///     unauthenticated_message: The status message of those calls.
///
/// Raises:
///     OSError: If the address cannot be bound.
///     ValueError: If ``client_key_metadata`` is not a valid metadata key, or
///         ``bearer_token_sha256`` is empty or has a digest that isn't 32 bytes.
#[pyclass(module = "pygrpc_native", frozen)]
struct NativeGrpcFront {
    shared: Arc<Shared>,
    events: Receiver<Event>,
    running: Mutex<Option<Running>>,
    bound_port: u16,
    socket_path: Option<PathBuf>,
}

#[pymethods]
impl NativeGrpcFront {
    #[new]
    #[pyo3(signature = (
        target,
        methods,
        io_threads=2,
        client_key_metadata=None,
        call_details=false,
        delay_stream_headers=false,
        bearer_token_sha256=None,
        unauthenticated_message="Unauthenticated".to_owned(),
    ))]
    // Python keyword arguments.
    #[allow(clippy::too_many_arguments)]
    fn new(
        target: &str,
        methods: Vec<(String, bool)>,
        io_threads: usize,
        client_key_metadata: Option<String>,
        call_details: bool,
        delay_stream_headers: bool,
        bearer_token_sha256: Option<Vec<Vec<u8>>>,
        unauthenticated_message: String,
    ) -> PyResult<Self> {
        if io_threads == 0 {
            return Err(PyValueError::new_err("io_threads must be greater than 0"));
        }
        if let Some(key) = &client_key_metadata {
            // Metadata keys arrive lowercased, so an uppercase key never matches.
            let valid =
                http::HeaderName::from_bytes(key.as_bytes()).is_ok_and(|name| name.as_str() == key);
            if !valid {
                return Err(PyValueError::new_err(format!(
                    "invalid client_key_metadata: {key:?}"
                )));
            }
        }
        let bearer_auth = match bearer_token_sha256 {
            None => None,
            Some(digests) if digests.is_empty() => {
                return Err(PyValueError::new_err(
                    "bearer_token_sha256 must not be empty; pass None to accept every call",
                ));
            }
            Some(digests) => {
                let digests = digests
                    .into_iter()
                    .map(|digest| <[u8; 32]>::try_from(digest.as_slice()))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|_| {
                        PyValueError::new_err("bearer_token_sha256 digests must be 32 bytes")
                    })?;
                Some(BearerAuth::new(digests, unauthenticated_message))
            }
        };
        let os_error = |err: std::io::Error| {
            PyOSError::new_err((err.raw_os_error().unwrap_or(0), format!("{target}: {err}")))
        };
        // Bind before starting any thread, so a failed bind leaves none behind.
        let bound = bind(target).map_err(os_error)?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(io_threads)
            .thread_name("grpc-native-io")
            .enable_all()
            .build()
            .map_err(|err| PyRuntimeError::new_err(format!("tokio runtime: {err}")))?;
        let listeners = {
            let _entered = runtime.enter();
            bound
                .listeners
                .into_iter()
                .map(StdListener::into_tokio)
                .collect::<std::io::Result<Vec<_>>>()
                .map_err(os_error)?
        };
        let (sender, events) = crossbeam_channel::unbounded();
        let (shutdown, _) = watch::channel(false);
        Ok(NativeGrpcFront {
            shared: Arc::new(Shared::new(
                methods,
                sender,
                client_key_metadata,
                call_details,
                delay_stream_headers,
                bearer_auth,
            )),
            events,
            running: Mutex::new(Some(Running {
                runtime,
                listeners,
                shutdown,
            })),
            bound_port: bound.port,
            socket_path: bound.socket_path,
        })
    }

    /// The bound TCP port; 1 for a Unix socket, as grpcio reports it.
    #[getter]
    fn bound_port(&self) -> u16 {
        self.bound_port
    }

    /// Forward calls to methods not in ``methods`` to a grpcio server
    /// listening on the Unix socket ``path``. Call before ``start()``.
    ///
    /// Raises:
    ///     RuntimeError: If a fallback is already set.
    fn set_fallback(&self, path: PathBuf) -> PyResult<()> {
        if self.shared.set_fallback(path) {
            Ok(())
        } else {
            Err(PyRuntimeError::new_err("fallback already set"))
        }
    }

    /// Register more ``(path, server_streaming)`` methods before ``start()``
    /// and return their ids, which follow the ones already registered.
    ///
    /// Raises RuntimeError after ``start()`` or ``close()``, and ValueError
    /// for a path that is already registered.
    fn add_methods(&self, methods: Vec<(String, bool)>) -> PyResult<Vec<u32>> {
        if self.running.lock().unwrap().is_none() {
            return Err(PyRuntimeError::new_err("server is closed"));
        }
        self.shared.add_methods(methods).map_err(|err| match err {
            AddMethodsError::Started => PyRuntimeError::new_err("server already started"),
            AddMethodsError::Duplicate(path) => {
                PyValueError::new_err(format!("method {path} is already registered"))
            }
        })
    }

    /// Start accepting connections.
    ///
    /// Raises:
    ///     RuntimeError: If the server was already started or closed.
    fn start(&self) -> PyResult<()> {
        let mut running = self.running.lock().unwrap();
        let running = running
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("server is closed"))?;
        if running.listeners.is_empty() {
            return Err(PyRuntimeError::new_err("server already started"));
        }
        self.shared.freeze_methods();
        for listener in running.listeners.drain(..) {
            running.runtime.spawn(accept_loop(
                listener,
                self.shared.clone(),
                running.shutdown.subscribe(),
            ));
        }
        Ok(())
    }

    /// Wait for calls and cancellations.
    ///
    /// The GIL is released while waiting. Returns up to ``max_events`` events
    /// as ``(kind, call_id, method_id, payload, client_key)`` tuples, where
    /// kind is 0 for a call and 1 for a cancelled stream, and client_key is
    /// the client's id bytes or its peer string. Without ``timeout_ms`` it
    /// waits until an event arrives; otherwise it returns an empty list on
    /// timeout. Returns None once the server is closed and every event before
    /// the close has been returned.
    #[pyo3(signature = (max_events, timeout_ms=None))]
    fn recv_batch<'py>(
        &self,
        py: Python<'py>,
        max_events: usize,
        timeout_ms: Option<u64>,
    ) -> PyResult<Option<Bound<'py, PyList>>> {
        self.recv(py, max_events, timeout_ms, event_tuple)
    }

    /// Like ``recv_batch``, with a sixth item on every event:
    /// ``(metadata, peer, time_remaining)`` for a call made on a server
    /// created with ``call_details=True``, and None otherwise. metadata is a
    /// tuple of ``(key, value)`` pairs in the order received, without the
    /// transport headers, where value is bytes for ``-bin`` keys and str
    /// otherwise. time_remaining is the seconds left before the deadline, or
    /// None without one.
    #[pyo3(signature = (max_events, timeout_ms=None))]
    fn recv_batch_details<'py>(
        &self,
        py: Python<'py>,
        max_events: usize,
        timeout_ms: Option<u64>,
    ) -> PyResult<Option<Bound<'py, PyList>>> {
        self.recv(py, max_events, timeout_ms, event_details_tuple)
    }

    /// Send the serialized response of a unary call, with optional response
    /// metadata as ``(key, value)`` pairs (bytes values for ``-bin`` keys).
    ///
    /// Returns False if the call no longer exists (client cancelled).
    #[pyo3(signature = (call_id, payload, initial_metadata=None, trailing_metadata=None))]
    fn respond(
        &self,
        call_id: u64,
        payload: PyBackedBytes,
        initial_metadata: Option<Vec<(String, Bound<'_, PyAny>)>>,
        trailing_metadata: Option<Vec<(String, Bound<'_, PyAny>)>>,
    ) -> PyResult<bool> {
        let answer = Answer {
            payload: Bytes::from_owner(payload),
            initial_metadata: initial_metadata.map(metadata_map).transpose()?,
            trailing_metadata: trailing_metadata
                .map(|items| metadata_map(items).map(MetadataMap::into_headers))
                .transpose()?,
        };
        Ok(self.shared.respond(call_id, Ok(answer)))
    }

    /// End a unary or streaming call with a non-OK status. Messages a stream
    /// already queued are still sent first. Never blocks.
    ///
    /// Returns False if the call no longer exists.
    ///
    /// Raises:
    ///     ValueError: If ``code`` is 0 (OK).
    #[pyo3(signature = (call_id, code, message, trailing_metadata=None))]
    fn fail(
        &self,
        call_id: u64,
        code: i32,
        message: &str,
        trailing_metadata: Option<Vec<(String, Bound<'_, PyAny>)>>,
    ) -> PyResult<bool> {
        if code == 0 {
            return Err(PyValueError::new_err(
                "fail() needs a non-OK status; use respond() or stream_end()",
            ));
        }
        let mut items = trailing_metadata.unwrap_or_default();
        // grpcio's abort_with_status() sends the rich status this way.
        let details = match items.iter().position(|(key, _)| key == STATUS_DETAILS_KEY) {
            Some(i) => {
                let value: Vec<u8> = items.remove(i).1.extract()?;
                Bytes::from(value)
            }
            None => Bytes::new(),
        };
        let status = Status::with_details_and_metadata(
            Code::from_i32(code),
            message,
            details,
            metadata_map(items)?,
        );
        if self.shared.is_unary(call_id) {
            return Ok(self.shared.respond(call_id, Err(status)));
        }
        Ok(self.shared.end_stream(call_id, Some(status)))
    }

    /// Send one message of a server-streaming call.
    ///
    /// Blocks, with the GIL released, while the client is not reading (HTTP/2
    /// flow control). Returns False once the client is gone.
    fn stream_send(&self, py: Python<'_>, call_id: u64, payload: PyBackedBytes) -> bool {
        let Some(sender) = self.shared.message_sender(call_id) else {
            return false;
        };
        let payload = Bytes::from_owner(payload);
        py.detach(|| sender.blocking_send(payload).is_ok())
    }

    /// ``stream_send`` without blocking, for callers on an event loop.
    ///
    /// Returns STREAM_SENT, STREAM_FULL if the client hasn't read enough yet
    /// (the message is not queued), or STREAM_CLOSED once the client is gone.
    fn stream_try_send(&self, call_id: u64, payload: PyBackedBytes) -> u8 {
        let Some(sender) = self.shared.message_sender(call_id) else {
            return STREAM_CLOSED;
        };
        match sender.try_send(Bytes::from_owner(payload)) {
            Ok(()) => STREAM_SENT,
            Err(TrySendError::Full(_)) => STREAM_FULL,
            Err(TrySendError::Closed(_)) => STREAM_CLOSED,
        }
    }

    /// Send a stream's response headers now, with this metadata. Only for a
    /// server created with ``delay_stream_headers=True``.
    ///
    /// Returns False if the headers already went out or the call is gone.
    fn stream_send_initial_metadata(
        &self,
        call_id: u64,
        metadata: Vec<(String, Bound<'_, PyAny>)>,
    ) -> PyResult<bool> {
        Ok(self
            .shared
            .send_initial_metadata(call_id, metadata_map(metadata)?))
    }

    /// Finish a server-streaming call with an OK status.
    #[pyo3(signature = (call_id, trailing_metadata=None))]
    fn stream_end(
        &self,
        call_id: u64,
        trailing_metadata: Option<Vec<(String, Bound<'_, PyAny>)>>,
    ) -> PyResult<()> {
        let status = trailing_metadata
            .map(|items| metadata_map(items).map(|md| Status::with_metadata(Code::Ok, "", md)))
            .transpose()?;
        self.shared.end_stream(call_id, status);
        Ok(())
    }

    /// Whether a server-streaming call is still open.
    fn stream_open(&self, call_id: u64) -> bool {
        self.shared.stream_sender(call_id).is_some()
    }

    /// Stop the server without waiting for calls in flight. Safe to call twice.
    ///
    /// Clients of calls still running get UNAVAILABLE, and ``recv_batch``
    /// returns None.
    fn close(&self, py: Python<'_>) {
        let running = self.running.lock().unwrap().take();
        self.shared.close();
        if let Some(running) = running {
            let _ = running.shutdown.send(true);
            let shared = self.shared.clone();
            py.detach(move || {
                // Let connections send GOAWAY before the runtime goes away.
                running
                    .runtime
                    .block_on(shared.wait_connections_closed(CONNECTION_DRAIN_TIMEOUT));
                running.runtime.shutdown_timeout(RUNTIME_SHUTDOWN_TIMEOUT);
            });
            self.remove_socket_file();
        }
    }
}

impl NativeGrpcFront {
    fn remove_socket_file(&self) {
        if let Some(path) = &self.socket_path {
            let _ = std::fs::remove_file(path);
        }
    }
}

impl Drop for NativeGrpcFront {
    fn drop(&mut self) {
        self.shared.close();
        if let Some(running) = self.running.lock().unwrap().take() {
            let _ = running.shutdown.send(true);
            running.runtime.shutdown_background();
            self.remove_socket_file();
        }
    }
}

impl NativeGrpcFront {
    fn recv<'py>(
        &self,
        py: Python<'py>,
        max_events: usize,
        timeout_ms: Option<u64>,
        convert: fn(Python<'py>, Event) -> PyResult<Bound<'py, PyTuple>>,
    ) -> PyResult<Option<Bound<'py, PyList>>> {
        if max_events == 0 {
            return Err(PyValueError::new_err("max_events must be greater than 0"));
        }
        let (batch, closed) = py.detach(|| {
            let mut batch = Vec::new();
            // The close event may have been taken with the previous batch.
            if self.shared.is_closed() && self.events.is_empty() {
                return (batch, true);
            }
            let first = match timeout_ms {
                Some(ms) => self.events.recv_timeout(Duration::from_millis(ms)),
                None => self
                    .events
                    .recv()
                    .map_err(|_| RecvTimeoutError::Disconnected),
            };
            match first {
                Ok(Event::Closed) | Err(RecvTimeoutError::Disconnected) => return (batch, true),
                Ok(event) => batch.push(event),
                Err(RecvTimeoutError::Timeout) => return (batch, false),
            }
            while batch.len() < max_events {
                match self.events.try_recv() {
                    Ok(Event::Closed) => return (batch, true),
                    Ok(event) => batch.push(event),
                    Err(_) => break,
                }
            }
            (batch, false)
        });
        if closed && batch.is_empty() {
            return Ok(None);
        }
        let items = PyList::empty(py);
        for event in batch {
            items.append(convert(py, event)?)?;
        }
        Ok(Some(items))
    }
}

const STATUS_DETAILS_KEY: &str = "grpc-status-details-bin";

fn metadata_map(items: Vec<(String, Bound<'_, PyAny>)>) -> PyResult<MetadataMap> {
    let mut metadata = MetadataMap::with_capacity(items.len());
    for (key, value) in items {
        if key.starts_with("grpc-") || key.starts_with(':') {
            return Err(PyValueError::new_err(format!(
                "metadata key {key:?} is reserved"
            )));
        }
        let invalid = || PyValueError::new_err(format!("invalid metadata {key:?}"));
        // Keys arrive lowercased on the wire, so grpcio rejects uppercase ones.
        if key.bytes().any(|b| b.is_ascii_uppercase()) {
            return Err(invalid());
        }
        if key.ends_with("-bin") {
            let key = BinaryMetadataKey::from_bytes(key.as_bytes()).map_err(|_| invalid())?;
            let value: &[u8] = value.extract().map_err(|_| invalid())?;
            metadata.append_bin(key, BinaryMetadataValue::from_bytes(value));
        } else {
            let key = AsciiMetadataKey::from_bytes(key.as_bytes()).map_err(|_| invalid())?;
            let value: &str = value.extract().map_err(|_| invalid())?;
            let value = AsciiMetadataValue::try_from(value).map_err(|_| invalid())?;
            metadata.append(key, value);
        }
    }
    Ok(metadata)
}

fn event_tuple<'py>(py: Python<'py>, event: Event) -> PyResult<Bound<'py, PyTuple>> {
    match event {
        Event::Call {
            call_id,
            method_id,
            payload,
            client_key,
            ..
        } => {
            let key: Bound<'py, PyAny> = match client_key {
                ClientKey::Id(id) => PyBytes::new(py, &id).into_any(),
                ClientKey::Peer(peer) => peer.as_ref().into_pyobject(py)?.into_any(),
            };
            (
                EVENT_CALL,
                call_id,
                method_id,
                PyBytes::new(py, &payload),
                key,
            )
                .into_pyobject(py)
        }
        Event::Cancel { call_id } => {
            (EVENT_CANCEL, call_id, 0u32, py.None(), py.None()).into_pyobject(py)
        }
        Event::Closed => Err(PyRuntimeError::new_err("unexpected close event")),
    }
}

fn event_details_tuple<'py>(py: Python<'py>, event: Event) -> PyResult<Bound<'py, PyTuple>> {
    let details = match &event {
        Event::Call {
            details: Some(details),
            ..
        } => details_tuple(py, details)?.into_any(),
        _ => py.None().into_bound(py),
    };
    let base = event_tuple(py, event)?;
    let mut items: Vec<Bound<'py, PyAny>> = base.iter().collect();
    items.push(details);
    PyTuple::new(py, items)
}

fn details_tuple<'py>(py: Python<'py>, details: &CallDetails) -> PyResult<Bound<'py, PyTuple>> {
    let mut metadata = Vec::with_capacity(details.metadata.len());
    for (key, value) in &details.metadata {
        let value: Bound<'py, PyAny> = match value {
            MetadataValue::Ascii(text) => {
                String::from_utf8_lossy(text).into_pyobject(py)?.into_any()
            }
            MetadataValue::Binary(bytes) => PyBytes::new(py, bytes).into_any(),
        };
        metadata.push((key.as_str(), value).into_pyobject(py)?);
    }
    let remaining = details.deadline.map(|deadline| {
        deadline
            .saturating_duration_since(tokio::time::Instant::now())
            .as_secs_f64()
    });
    (
        PyTuple::new(py, metadata)?,
        details.peer.as_ref(),
        remaining,
    )
        .into_pyobject(py)
}

enum StdListener {
    Tcp(std::net::TcpListener),
    Unix(std::os::unix::net::UnixListener),
}

impl StdListener {
    /// Needs a tokio runtime context.
    fn into_tokio(self) -> std::io::Result<Listener> {
        match self {
            StdListener::Tcp(listener) => {
                Ok(Listener::Tcp(tokio::net::TcpListener::from_std(listener)?))
            }
            StdListener::Unix(listener) => Ok(Listener::Unix(tokio::net::UnixListener::from_std(
                listener,
            )?)),
        }
    }
}

struct BoundSockets {
    listeners: Vec<StdListener>,
    port: u16,
    socket_path: Option<PathBuf>,
}

fn bind(target: &str) -> std::io::Result<BoundSockets> {
    let unix_path = target
        .strip_prefix("unix://")
        .or_else(|| target.strip_prefix("unix:"));
    if let Some(path) = unix_path {
        let path = PathBuf::from(path);
        // Like grpc-core: a socket file left by a server that is gone would
        // otherwise make every restart fail.
        if std::fs::symlink_metadata(&path).is_ok_and(|meta| meta.file_type().is_socket()) {
            std::fs::remove_file(&path)?;
        }
        let listener = std::os::unix::net::UnixListener::bind(&path)?;
        listener.set_nonblocking(true)?;
        return Ok(BoundSockets {
            listeners: vec![StdListener::Unix(listener)],
            port: 1,
            socket_path: Some(path),
        });
    }
    // Like grpc-core, bind every address the host resolves to (localhost is
    // both 127.0.0.1 and ::1), all on the port the first one got. std's bind
    // never sets SO_REUSEPORT, so a port already in use fails.
    let mut addrs: Vec<std::net::SocketAddr> = Vec::new();
    for addr in target.to_socket_addrs()? {
        if !addrs.contains(&addr) {
            addrs.push(addr);
        }
    }
    let mut listeners = Vec::new();
    let mut port = 0;
    let mut first_error = None;
    for mut addr in addrs {
        if port != 0 {
            addr.set_port(port);
        }
        match std::net::TcpListener::bind(addr) {
            Ok(listener) => {
                listener.set_nonblocking(true)?;
                port = listener.local_addr()?.port();
                listeners.push(StdListener::Tcp(listener));
            }
            Err(err) => {
                log::warn!("gRPC server could not bind {addr}: {err}");
                first_error.get_or_insert(err);
            }
        }
    }
    if listeners.is_empty() {
        return Err(
            first_error.unwrap_or_else(|| std::io::Error::other("host resolved to no address"))
        );
    }
    Ok(BoundSockets {
        listeners,
        port,
        socket_path: None,
    })
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let _ = pyo3_log::try_init();
    m.add_class::<NativeGrpcFront>()?;
    m.add("EVENT_CALL", EVENT_CALL)?;
    m.add("EVENT_CANCEL", EVENT_CANCEL)?;
    m.add("STREAM_SENT", STREAM_SENT)?;
    m.add("STREAM_FULL", STREAM_FULL)?;
    m.add("STREAM_CLOSED", STREAM_CLOSED)?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
