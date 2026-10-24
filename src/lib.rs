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
use tokio::sync::watch;
use tonic::{Code, Status};

use server::{accept_loop, ClientKey, Event, Listener, Shared, CONNECTION_DRAIN_TIMEOUT};

const RUNTIME_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);

const EVENT_CALL: u8 = 0;
const EVENT_CANCEL: u8 = 1;

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
///
/// Raises:
///     OSError: If the address cannot be bound.
///     ValueError: If ``client_key_metadata`` is not a valid metadata key.
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
    #[pyo3(signature = (target, methods, io_threads=2, client_key_metadata=None))]
    fn new(
        target: &str,
        methods: Vec<(String, bool)>,
        io_threads: usize,
        client_key_metadata: Option<String>,
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
            shared: Arc::new(Shared::new(methods, sender, client_key_metadata)),
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
            items.append(event_tuple(py, event)?)?;
        }
        Ok(Some(items))
    }

    /// Send the serialized response of a unary call.
    ///
    /// Returns False if the call no longer exists (client cancelled).
    fn respond(&self, call_id: u64, payload: PyBackedBytes) -> bool {
        self.shared.respond(call_id, Ok(Bytes::from_owner(payload)))
    }

    /// End a unary or streaming call with a non-OK status. Messages a stream
    /// already queued are still sent first. Never blocks.
    ///
    /// Returns False if the call no longer exists.
    ///
    /// Raises:
    ///     ValueError: If ``code`` is 0 (OK).
    fn fail(&self, call_id: u64, code: i32, message: &str) -> PyResult<bool> {
        if code == 0 {
            return Err(PyValueError::new_err(
                "fail() needs a non-OK status; use respond() or stream_end()",
            ));
        }
        let status = Status::new(Code::from_i32(code), message);
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
        let Some(sender) = self.shared.stream_sender(call_id) else {
            return false;
        };
        let payload = Bytes::from_owner(payload);
        py.detach(|| sender.blocking_send(payload).is_ok())
    }

    /// Finish a server-streaming call with an OK status.
    fn stream_end(&self, call_id: u64) {
        self.shared.end_stream(call_id, None);
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

fn event_tuple<'py>(py: Python<'py>, event: Event) -> PyResult<Bound<'py, PyTuple>> {
    match event {
        Event::Call {
            call_id,
            method_id,
            payload,
            client_key,
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
fn pygrpc_native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let _ = pyo3_log::try_init();
    m.add_class::<NativeGrpcFront>()?;
    m.add("EVENT_CALL", EVENT_CALL)?;
    m.add("EVENT_CANCEL", EVENT_CANCEL)?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
