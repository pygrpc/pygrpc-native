import collections
import logging
import os
import shutil
import sys
import tempfile
import threading
import time
from collections.abc import Callable, Iterable, Sequence
from concurrent.futures import Executor
from typing import Any

import grpc

from pygrpc_native._native import EVENT_CALL, EVENT_CANCEL, NativeGrpcFront

_LOGGER = logging.getLogger(__name__)

_Metadatum = collections.namedtuple("_Metadatum", ("key", "value"))

_DEFAULT_RECEIVE_LIMIT = 4 * 1024 * 1024
# grpcio reports this when the call has no deadline.
_NO_DEADLINE = float(sys.maxsize)
_AUTH_CONTEXT = {
    "transport_security_type": [b"insecure"],
    "security_level": [b"TSI_SECURITY_NONE"],
}

# Options grpcio servers take that need no work here: the server never limits
# client pings.
_PING_OPTIONS = frozenset(
    {
        "grpc.http2.min_ping_interval_without_data_ms",
        "grpc.http2.max_ping_strikes",
        "grpc.http2.max_pings_without_data",
        "grpc.keepalive_permit_without_calls",
    }
)


def _code(code: grpc.StatusCode) -> int:
    return code.value[0]


def _limit(value: Any) -> int | None:
    value = int(value)
    return None if value < 0 else value


def _parse_options(
    options: Iterable[tuple[str, Any]] | None,
) -> tuple[int | None, int | None]:
    receive: int | None = _DEFAULT_RECEIVE_LIMIT
    send: int | None = None
    for key, value in options or ():
        if key == "grpc.max_receive_message_length":
            receive = _limit(value)
        elif key == "grpc.max_send_message_length":
            send = _limit(value)
        elif key in _PING_OPTIONS:
            pass
        elif key == "grpc.so_reuseport" and not value:
            pass
        else:
            raise ValueError(f"unsupported server option {key!r}")
    return receive, send


def _split_handlers(
    registered: dict[str, grpc.RpcMethodHandler],
    generic: Sequence[grpc.GenericRpcHandler],
) -> tuple[
    dict[str, grpc.RpcMethodHandler],
    dict[str, dict[str, grpc.RpcMethodHandler]],
    list[grpc.GenericRpcHandler],
]:
    """Splits handlers into the methods served natively and the ones left to
    grpcio: streaming requests, and generic handlers of services that weren't
    also registered (older generated code, hand-written handlers)."""
    native: dict[str, grpc.RpcMethodHandler] = {}
    streamed: dict[str, dict[str, grpc.RpcMethodHandler]] = {}
    for path, handler in registered.items():
        if handler.request_streaming:
            service, name = path[1:].split("/")
            streamed.setdefault(service, {})[name] = handler
        else:
            native[path] = handler
    services = {path[1:].split("/")[0] for path in registered}
    rest = [
        handler
        for handler in generic
        if not (
            isinstance(handler, grpc.ServiceRpcHandler)
            and handler.service_name() in services
        )
    ]
    return native, streamed, rest


def _fallback_socket() -> tuple[str, str]:
    directory = tempfile.mkdtemp(prefix="pygrpc-native-")
    return directory, os.path.join(directory, "fallback.sock")


class _Abort(Exception):
    pass


class _Call:
    def __init__(
        self,
        call_id: int,
        handler: grpc.RpcMethodHandler,
        metadata: tuple[tuple[str, str | bytes], ...],
        peer: str,
        time_remaining: float | None,
    ) -> None:
        self.call_id = call_id
        self.handler = handler
        self.metadata = metadata
        self.peer = peer
        self.deadline = (
            None if time_remaining is None else time.monotonic() + time_remaining
        )
        self.lock = threading.Lock()
        self.active = True
        self.callbacks: list[Callable[[], None]] = []

    def terminate(self) -> None:
        with self.lock:
            if not self.active:
                return
            self.active = False
            callbacks, self.callbacks = self.callbacks, []
        for callback in callbacks:
            try:
                callback()
            except Exception:
                _LOGGER.exception("Exception calling callback")


class _Context(grpc.ServicerContext):
    def __init__(self, front: NativeGrpcFront, call: _Call) -> None:
        self._front = front
        self._call = call
        self._code: grpc.StatusCode | None = None
        self._details: str | None = None
        self._initial_metadata: Sequence[tuple[str, str | bytes]] | None = None
        self._initial_sent = False
        self._trailing_metadata: Sequence[tuple[str, str | bytes]] = ()

    def invocation_metadata(self) -> tuple[_Metadatum, ...]:
        return tuple(_Metadatum(key, value) for key, value in self._call.metadata)

    def peer(self) -> str:
        return self._call.peer

    def peer_identities(self) -> None:
        return None

    def peer_identity_key(self) -> None:
        return None

    def auth_context(self) -> dict[str, list[bytes]]:
        return _AUTH_CONTEXT

    def is_active(self) -> bool:
        return self._call.active

    def time_remaining(self) -> float:
        if self._call.deadline is None:
            return _NO_DEADLINE
        return max(0.0, self._call.deadline - time.monotonic())

    def cancel(self) -> None:
        self._front.fail(
            self._call.call_id, _code(grpc.StatusCode.CANCELLED), "CANCELLED"
        )
        self._call.terminate()

    def add_callback(self, callback: Callable[[], None]) -> bool:
        with self._call.lock:
            if not self._call.active:
                return False
            self._call.callbacks.append(callback)
            return True

    def send_initial_metadata(
        self, initial_metadata: Sequence[tuple[str, str | bytes]]
    ) -> None:
        if self._initial_sent:
            raise ValueError("Initial metadata no longer allowed!")
        self._initial_sent = True
        if self._call.handler.response_streaming:
            self._front.stream_send_initial_metadata(
                self._call.call_id, list(initial_metadata)
            )
        else:
            self._initial_metadata = initial_metadata

    def set_trailing_metadata(
        self, trailing_metadata: Sequence[tuple[str, str | bytes]]
    ) -> None:
        self._trailing_metadata = trailing_metadata

    def trailing_metadata(self) -> Sequence[tuple[str, str | bytes]]:
        return self._trailing_metadata

    def abort(self, code: grpc.StatusCode, details: str) -> None:
        if code == grpc.StatusCode.OK:
            _LOGGER.error("abort() called with StatusCode.OK; returning UNKNOWN")
            code, details = grpc.StatusCode.UNKNOWN, ""
        self._code, self._details = code, details
        raise _Abort()

    def abort_with_status(self, status: grpc.Status) -> None:
        self.set_trailing_metadata(status.trailing_metadata)
        self.abort(status.code, status.details)

    def set_code(self, code: grpc.StatusCode) -> None:
        self._code = code

    def code(self) -> grpc.StatusCode | None:
        return self._code

    def set_details(self, details: str) -> None:
        self._details = details

    def details(self) -> str | None:
        return self._details

    def set_compression(self, compression: grpc.Compression) -> None:
        pass

    def disable_next_message_compression(self) -> None:
        pass


class Server(grpc.Server):
    """A grpc.Server whose HTTP/2 and gRPC work runs in Rust.

    Serves unary and server-streaming methods added with
    ``add_registered_method_handlers``, as generated code does, on one
    insecure port. Other methods are served by a grpcio server behind it.
    """

    def __init__(
        self,
        thread_pool: Executor,
        handlers: Sequence[grpc.GenericRpcHandler] | None = None,
        interceptors: Sequence[grpc.ServerInterceptor] | None = None,
        options: Iterable[tuple[str, Any]] | None = None,
        maximum_concurrent_rpcs: int | None = None,
        compression: grpc.Compression | None = None,
        io_threads: int = 2,
    ) -> None:
        if interceptors:
            raise NotImplementedError("interceptors aren't supported yet")
        if maximum_concurrent_rpcs is not None:
            raise NotImplementedError("maximum_concurrent_rpcs isn't supported yet")
        if compression not in (None, grpc.Compression.NoCompression):
            raise NotImplementedError("response compression isn't supported")
        self._options = list(options or ())
        self._receive_limit, self._send_limit = _parse_options(self._options)
        self._executor = thread_pool
        self._io_threads = io_threads
        self._generic_handlers = list(handlers or ())
        self._registered: dict[str, grpc.RpcMethodHandler] = {}
        self._front: NativeGrpcFront | None = None
        self._fallback: grpc.Server | None = None
        self._fallback_dir: str | None = None
        self._table: list[grpc.RpcMethodHandler] = []
        self._calls: dict[int, _Call] = {}
        self._lock = threading.Lock()
        self._started = False
        self._stopping = False
        self._terminated = threading.Event()

    def add_generic_rpc_handlers(
        self, generic_rpc_handlers: Iterable[grpc.GenericRpcHandler]
    ) -> None:
        self._generic_handlers.extend(generic_rpc_handlers)

    def add_registered_method_handlers(
        self, service_name: str, method_handlers: dict[str, grpc.RpcMethodHandler]
    ) -> None:
        for name, handler in method_handlers.items():
            self._registered[f"/{service_name}/{name}"] = handler

    def add_insecure_port(self, address: str) -> int:
        if self._front is not None:
            raise RuntimeError("only one port is supported")
        try:
            self._front = NativeGrpcFront(
                address,
                [],
                io_threads=self._io_threads,
                call_details=True,
                delay_stream_headers=True,
            )
        except OSError as exc:
            raise RuntimeError(f"Failed to bind to address {address}") from exc
        return self._front.bound_port

    def add_secure_port(
        self, address: str, server_credentials: grpc.ServerCredentials
    ) -> int:
        raise NotImplementedError("TLS isn't supported yet")

    def start(self) -> None:
        with self._lock:
            if self._started:
                raise ValueError("Cannot start already-started server!")
            if self._front is None:
                raise RuntimeError("add a port with add_insecure_port() before start()")
            native, streamed, generic = _split_handlers(
                self._registered, self._generic_handlers
            )
            if streamed or generic:
                self._start_fallback(streamed, generic)
            paths = list(native)
            self._front.add_methods(
                [(path, native[path].response_streaming) for path in paths]
            )
            self._table = [native[path] for path in paths]
            self._front.start()
            self._started = True
        threading.Thread(
            target=self._dispatch, name="pygrpc-native-dispatch", daemon=True
        ).start()

    def _start_fallback(
        self,
        streamed: dict[str, dict[str, grpc.RpcMethodHandler]],
        generic: list[grpc.GenericRpcHandler],
    ) -> None:
        assert self._front is not None
        self._fallback_dir, path = _fallback_socket()
        fallback = grpc.server(self._executor, options=self._options)
        fallback.add_generic_rpc_handlers(generic)
        for service, handlers in streamed.items():
            fallback.add_registered_method_handlers(service, handlers)
        fallback.add_insecure_port(f"unix:{path}")
        fallback.start()
        self._fallback = fallback
        self._front.set_fallback(path)

    def stop(self, grace: float | None) -> threading.Event:
        done = threading.Event()
        with self._lock:
            if self._stopping or not self._started:
                self._stopping = True
                self._shutdown(done)
                return done
            self._stopping = True
        fallback_done = None if self._fallback is None else self._fallback.stop(grace)

        def finish() -> None:
            deadline = time.monotonic() + (grace or 0)
            while self._calls and time.monotonic() < deadline:
                time.sleep(0.01)
            if fallback_done is not None:
                fallback_done.wait(max(0.0, deadline - time.monotonic()))
            self._shutdown(done)

        if grace:
            threading.Thread(target=finish, daemon=True).start()
        else:
            finish()
        return done

    def wait_for_termination(self, timeout: float | None = None) -> bool:
        return not self._terminated.wait(timeout)

    def _shutdown(self, done: threading.Event) -> None:
        if self._front is not None:
            self._front.close()
        if self._fallback is not None:
            self._fallback.stop(None)
        if self._fallback_dir is not None:
            shutil.rmtree(self._fallback_dir, ignore_errors=True)
        for call in list(self._calls.values()):
            call.terminate()
        self._terminated.set()
        done.set()

    def _dispatch(self) -> None:
        front = self._front
        assert front is not None
        while (events := front.recv_batch_details(64)) is not None:
            for kind, call_id, method_id, payload, _, details in events:
                try:
                    if kind == EVENT_CANCEL:
                        call = self._calls.get(call_id)
                        if call is not None:
                            call.terminate()
                        continue
                    if self._stopping:
                        front.fail(
                            call_id,
                            _code(grpc.StatusCode.UNAVAILABLE),
                            "Server is shutting down",
                        )
                        continue
                    assert (
                        kind == EVENT_CALL
                        and payload is not None
                        and details is not None
                    )
                    handler = self._table[method_id]
                    call = _Call(call_id, handler, *details)
                    self._calls[call_id] = call
                    run = (
                        self._run_stream
                        if handler.response_streaming
                        else self._run_unary
                    )
                    self._executor.submit(run, call, payload)
                except Exception:
                    _LOGGER.exception("Error dispatching call %d", call_id)
                    self._calls.pop(call_id, None)
                    if kind == EVENT_CALL:
                        front.fail(
                            call_id,
                            _code(grpc.StatusCode.INTERNAL),
                            "Error dispatching call",
                        )

    def _request(self, call: _Call, payload: bytes) -> tuple[bool, Any]:
        front = self._front
        assert front is not None
        if self._receive_limit is not None and len(payload) > self._receive_limit:
            front.fail(
                call.call_id,
                _code(grpc.StatusCode.RESOURCE_EXHAUSTED),
                f"SERVER: Received message larger than max ({len(payload)} vs. {self._receive_limit})",
            )
            return False, None
        deserializer = call.handler.request_deserializer
        try:
            return True, payload if deserializer is None else deserializer(payload)
        except Exception:
            _LOGGER.exception("Exception deserializing request!")
            front.fail(
                call.call_id,
                _code(grpc.StatusCode.INTERNAL),
                "Exception deserializing request!",
            )
            return False, None

    def _serialize(self, call: _Call, response: Any) -> bytes | None:
        front = self._front
        assert front is not None
        serializer = call.handler.response_serializer
        try:
            data = response if serializer is None else serializer(response)
            if not isinstance(data, bytes):
                raise TypeError(f"expected bytes, got {type(data).__name__}")
        except Exception:
            _LOGGER.exception("Failed to serialize response!")
            front.fail(
                call.call_id,
                _code(grpc.StatusCode.INTERNAL),
                "Failed to serialize response!",
            )
            return None
        if self._send_limit is not None and len(data) > self._send_limit:
            front.fail(
                call.call_id,
                _code(grpc.StatusCode.RESOURCE_EXHAUSTED),
                f"SERVER: Sent message larger than max ({len(data)} vs. {self._send_limit})",
            )
            return None
        return data

    def _fail(
        self, call: _Call, context: _Context, code: grpc.StatusCode, details: str
    ) -> None:
        front = self._front
        assert front is not None
        code = context._code or code
        if code == grpc.StatusCode.OK:
            code = grpc.StatusCode.UNKNOWN
        details = details if context._details is None else context._details
        front.fail(
            call.call_id,
            _code(code),
            details,
            trailing_metadata=list(context._trailing_metadata),
        )

    def _run_unary(self, call: _Call, payload: bytes) -> None:
        try:
            ok, request = self._request(call, payload)
            if not ok:
                return
            context = _Context(self._front, call)  # type: ignore[arg-type]
            try:
                response = call.handler.unary_unary(request, context)
            except _Abort:
                self._fail(call, context, grpc.StatusCode.UNKNOWN, "RPC Aborted")
                return
            except Exception as exc:
                _LOGGER.exception("Exception calling application: %s", exc)
                self._fail(
                    call,
                    context,
                    grpc.StatusCode.UNKNOWN,
                    f"Exception calling application: {exc}",
                )
                return
            if context._code not in (None, grpc.StatusCode.OK):
                self._fail(call, context, grpc.StatusCode.UNKNOWN, "")
                return
            data = self._serialize(call, response)
            if data is not None:
                self._front.respond(  # type: ignore[union-attr]
                    call.call_id,
                    data,
                    initial_metadata=None
                    if context._initial_metadata is None
                    else list(context._initial_metadata),
                    trailing_metadata=list(context._trailing_metadata),
                )
        finally:
            self._calls.pop(call.call_id, None)
            call.terminate()

    def _run_stream(self, call: _Call, payload: bytes) -> None:
        front = self._front
        assert front is not None
        responses = None
        try:
            ok, request = self._request(call, payload)
            if not ok:
                return
            context = _Context(front, call)
            try:
                responses = call.handler.unary_stream(request, context)
                for response in responses:
                    data = self._serialize(call, response)
                    if data is None:
                        return
                    context._initial_sent = True
                    if not front.stream_send(call.call_id, data):
                        return
            except _Abort:
                self._fail(call, context, grpc.StatusCode.UNKNOWN, "RPC Aborted")
                return
            except Exception as exc:
                _LOGGER.exception("Exception iterating responses: %s", exc)
                self._fail(
                    call,
                    context,
                    grpc.StatusCode.UNKNOWN,
                    f"Exception iterating responses: {exc}",
                )
                return
            if context._code not in (None, grpc.StatusCode.OK):
                self._fail(call, context, grpc.StatusCode.UNKNOWN, "")
            else:
                front.stream_end(
                    call.call_id, trailing_metadata=list(context._trailing_metadata)
                )
        finally:
            close = getattr(responses, "close", None)
            if close is not None:
                close()
            self._calls.pop(call.call_id, None)
            call.terminate()


def server(
    thread_pool: Executor,
    handlers: Sequence[grpc.GenericRpcHandler] | None = None,
    interceptors: Sequence[grpc.ServerInterceptor] | None = None,
    options: Iterable[tuple[str, Any]] | None = None,
    maximum_concurrent_rpcs: int | None = None,
    compression: grpc.Compression | None = None,
) -> Server:
    """Like ``grpc.server()``, on the native front."""
    return Server(
        thread_pool,
        handlers=handlers,
        interceptors=interceptors,
        options=options,
        maximum_concurrent_rpcs=maximum_concurrent_rpcs,
        compression=compression,
    )
