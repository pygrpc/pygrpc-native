import collections
import contextvars
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

from pygrpc_native._native import (
    EVENT_CALL,
    EVENT_CANCEL,
    _EVENT_RESOLVE,
    _MAX_RESOLVED,
    NativeGrpcFront,
)

_LOGGER = logging.getLogger(__name__)

_Metadatum = collections.namedtuple("_Metadatum", ("key", "value"))


class _HandlerCallDetails(
    collections.namedtuple("_HandlerCallDetails", ("method", "invocation_metadata")),
    grpc.HandlerCallDetails,
):
    pass


_DEFAULT_RECEIVE_LIMIT = 4 * 1024 * 1024
# grpcio reports this when the call has no deadline.
_NO_DEADLINE = float(sys.maxsize)
_AUTH_CONTEXT = {
    "transport_security_type": [b"insecure"],
    "security_level": [b"TSI_SECURITY_NONE"],
}

# Options a grpcio server acts on in ways clients notice, which this server
# can't do: closing connections by age or idleness, limiting connections, and
# compressing responses. Any other option it doesn't take is ignored, as
# grpcio ignores the many that only clients read.
_UNSUPPORTED_OPTIONS = frozenset(
    {
        "grpc.default_compression_algorithm",
        "grpc.default_compression_level",
        "grpc.max_allowed_incoming_connections",
        "grpc.max_connection_age_grace_ms",
        "grpc.max_connection_age_ms",
        "grpc.max_connection_idle_ms",
    }
)
# grpc-core's limit on request metadata when no option sets it.
_DEFAULT_METADATA_LIMIT = 16 * 1024
# NativeGrpcFront arguments that differ from its defaults for grpcio
# compatibility: grpc-core servers only ping connections with calls.
_FRONT_DEFAULTS = {"keepalive_permit_without_calls": False}


def _code(code: grpc.StatusCode) -> int:
    # Like grpcio, send UNKNOWN for anything that isn't a StatusCode.
    if isinstance(code, grpc.StatusCode):
        return code.value[0]
    return grpc.StatusCode.UNKNOWN.value[0]


def _limit(value: int) -> int | None:
    return None if value < 0 else value


def _parse_options(
    options: Iterable[tuple[str, Any]] | None,
) -> tuple[int | None, int | None, dict[str, Any]]:
    """Returns the receive and send limits and the arguments for
    NativeGrpcFront."""
    receive: int | None = _DEFAULT_RECEIVE_LIMIT
    send: int | None = None
    soft_metadata = hard_metadata = -1
    front: dict[str, Any] = dict(_FRONT_DEFAULTS)
    for key, value in options or ():
        # grpc-core ignores a value that isn't an integer where it reads one.
        integer = isinstance(value, int)
        if key == "grpc.max_receive_message_length":
            if integer:
                receive = _limit(value)
        elif key == "grpc.max_send_message_length":
            if integer:
                send = _limit(value)
        elif key in ("grpc.keepalive_time_ms", "grpc.keepalive_timeout_ms"):
            if integer:
                front[key[len("grpc.") :]] = max(1, value)
        elif key == "grpc.keepalive_permit_without_calls":
            if integer:
                front["keepalive_permit_without_calls"] = bool(value)
        elif key == "grpc.max_concurrent_streams":
            if integer and value >= 0:
                front["max_concurrent_streams"] = min(value, 2**32 - 1)
        elif key == "grpc.max_metadata_size":
            if integer:
                soft_metadata = value
        elif key == "grpc.absolute_max_metadata_size":
            if integer:
                hard_metadata = value
        elif key == "grpc.so_reuseport":
            if integer:
                front["reuse_port"] = bool(value)
        elif key in _UNSUPPORTED_OPTIONS and integer and value:
            raise ValueError(f"unsupported server option {key!r}")
    if hard_metadata >= 0 or soft_metadata >= 0:
        # grpc-core refuses metadata over the hard limit, which is
        # absolute_max_metadata_size or else 1.25 times max_metadata_size.
        # It also refuses some calls between the two limits at random;
        # this server doesn't.
        if hard_metadata < 0:
            hard_metadata = max(int(soft_metadata * 1.25), _DEFAULT_METADATA_LIMIT)
        front["max_metadata_size"] = min(hard_metadata, 2**32 - 1)
    return receive, send, front


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


def _use_handler(
    front: NativeGrpcFront, call: Any, handler: grpc.RpcMethodHandler | None
) -> bool:
    """Sets the handler the interceptors returned, or fails the call as grpcio
    does. A unary handler can answer a stream with one message, as
    interceptors that reject every call often return."""
    if handler is None:
        front.fail(
            call.call_id, _code(grpc.StatusCode.UNIMPLEMENTED), "Method not found!"
        )
        return False
    if handler.request_streaming or (handler.response_streaming and not call.streaming):
        _LOGGER.error(
            "An interceptor returned a handler of another kind for %s", call.method
        )
        front.fail(
            call.call_id, _code(grpc.StatusCode.INTERNAL), "Error in service handler!"
        )
        return False
    call.handler = handler
    return True


class _Methods:
    """The methods a server serves natively: `_table` holds
    `(path, registered handler, server streaming)` by method id, with None for
    a method only generic handlers know, which is looked up on every call."""

    _front: NativeGrpcFront | None
    _registered: dict[str, grpc.RpcMethodHandler]
    _generic: list[grpc.GenericRpcHandler]
    _table: list[tuple[str, grpc.RpcMethodHandler | None, bool]]
    # Paths answered for good: their method id, or None for the fallback.
    _resolved: dict[str, int | None]

    def _lookup(self, details: grpc.HandlerCallDetails) -> grpc.RpcMethodHandler | None:
        for generic in self._generic:
            handler = generic.service(details)
            if handler is not None:
                return handler
        return None

    def _resolve(self, resolve_id: int, path: str, metadata: Any) -> None:
        """Answers the front's first call to a method it doesn't know."""
        assert self._front is not None
        remember = True
        if path in self._resolved:
            method_id = self._resolved[path]
        else:
            method_id = None
            # Registered client-streaming methods come first, as in grpcio.
            if path not in self._registered:
                details = _HandlerCallDetails(
                    path, tuple(_Metadatum(key, value) for key, value in metadata)
                )
                try:
                    handler = contextvars.Context().run(self._lookup, details)
                except Exception:
                    # The fallback server reports it.
                    handler = None
                if handler is None:
                    remember = False
                elif not handler.request_streaming:
                    method_id = len(self._table)
            if len(self._resolved) >= _MAX_RESOLVED:
                method_id, remember = None, False
            if remember:
                self._resolved[path] = method_id
                if method_id is not None:
                    self._table.append((path, None, handler.response_streaming))
        self._front._resolved(
            resolve_id,
            method_id,
            method_id is not None and self._table[method_id][2],
            remember,
        )


class _Abort(Exception):
    pass


class _ConcurrencyLimit:
    def __init__(self, maximum: int | None) -> None:
        self.maximum = maximum
        self._active = 0
        self._lock = threading.Lock()

    def admit(self) -> bool:
        if self.maximum is None:
            return True
        with self._lock:
            if self._active >= self.maximum:
                return False
            self._active += 1
            return True

    def release(self) -> None:
        if self.maximum is None:
            return
        with self._lock:
            self._active -= 1


_LIMIT_EXCEEDED = "Concurrent RPC limit exceeded!"


def _behavior(handler: grpc.RpcMethodHandler) -> Callable[..., Any]:
    kind = (
        f"{'stream' if handler.request_streaming else 'unary'}_"
        f"{'stream' if handler.response_streaming else 'unary'}"
    )
    return getattr(handler, kind)


def _with_behavior(
    handler: grpc.RpcMethodHandler, behavior: Callable[..., Any]
) -> grpc.RpcMethodHandler:
    make = {
        (False, False): grpc.unary_unary_rpc_method_handler,
        (False, True): grpc.unary_stream_rpc_method_handler,
        (True, False): grpc.stream_unary_rpc_method_handler,
        (True, True): grpc.stream_stream_rpc_method_handler,
    }[(handler.request_streaming, handler.response_streaming)]
    return make(
        behavior,
        request_deserializer=handler.request_deserializer,
        response_serializer=handler.response_serializer,
    )


def _counted(
    limit: _ConcurrencyLimit, behavior: Callable[..., Any], streaming: bool
) -> Callable[..., Any]:
    """Wraps a synchronous behavior so that it holds a place in `limit` while
    it runs. A call grpcio never hands to its behavior holds none."""
    if streaming:

        def counted_stream(request: Any, context: grpc.ServicerContext) -> Any:
            if not limit.admit():
                context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, _LIMIT_EXCEEDED)
            try:
                yield from behavior(request, context)
            finally:
                limit.release()

        return counted_stream

    def counted(request: Any, context: grpc.ServicerContext) -> Any:
        if not limit.admit():
            context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, _LIMIT_EXCEEDED)
        try:
            return behavior(request, context)
        finally:
            limit.release()

    return counted


class _FallbackLimit(grpc.ServerInterceptor):
    """Counts the fallback server's calls against the front's limit."""

    def __init__(self, limit: _ConcurrencyLimit) -> None:
        self._limit = limit

    def intercept_service(
        self,
        continuation: Callable[[grpc.HandlerCallDetails], grpc.RpcMethodHandler | None],
        handler_call_details: grpc.HandlerCallDetails,
    ) -> grpc.RpcMethodHandler | None:
        handler = continuation(handler_call_details)
        if handler is None:
            return None
        behavior = _counted(self._limit, _behavior(handler), handler.response_streaming)
        return _with_behavior(handler, behavior)


class _Call:
    def __init__(
        self,
        call_id: int,
        method: str,
        handler: grpc.RpcMethodHandler | None,
        streaming: bool,
        metadata: tuple[tuple[str, str | bytes], ...],
        peer: str,
        time_remaining: float | None,
    ) -> None:
        self.call_id = call_id
        self.method = method
        self.handler = handler
        # Whether the front serves the method as a stream; an interceptor
        # can swap the handler, but not this.
        self.streaming = streaming
        self.metadata = metadata
        self.peer = peer
        self.deadline = (
            None if time_remaining is None else time.monotonic() + time_remaining
        )
        self.lock = threading.Lock()
        self.admitted = True
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
        if self._call.streaming:
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


class _Codec:
    """Request decoding and response encoding with grpcio's limits and errors."""

    _front: NativeGrpcFront | None
    _receive_limit: int | None
    _send_limit: int | None

    def _request(self, call: Any, payload: bytes) -> tuple[bool, Any]:
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

    def _serialize(self, call: Any, response: Any) -> bytes | None:
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
        return self._within_send_limit(call, data)

    def _within_send_limit(self, call: Any, data: bytes) -> bytes | None:
        if self._send_limit is not None and len(data) > self._send_limit:
            self._front.fail(  # type: ignore[union-attr]
                call.call_id,
                _code(grpc.StatusCode.RESOURCE_EXHAUSTED),
                f"SERVER: Sent message larger than max ({len(data)} vs. {self._send_limit})",
            )
            return None
        return data


class Server(_Codec, _Methods, grpc.Server):
    """A grpc.Server whose HTTP/2 and gRPC work runs in Rust.

    Serves unary and server-streaming methods on one insecure port. Other
    methods are served by a grpcio server behind it.
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
        if compression not in (None, grpc.Compression.NoCompression):
            raise NotImplementedError("response compression isn't supported")
        self._limit = _ConcurrencyLimit(maximum_concurrent_rpcs)
        self._options = list(options or ())
        self._receive_limit, self._send_limit, self._front_args = _parse_options(
            self._options
        )
        self._executor = thread_pool
        self._interceptors = tuple(interceptors or ())
        self._io_threads = io_threads
        self._generic_handlers = list(handlers or ())
        self._registered: dict[str, grpc.RpcMethodHandler] = {}
        self._front: NativeGrpcFront | None = None
        self._fallback: grpc.Server | None = None
        self._fallback_dir: str | None = None
        self._generic: list[grpc.GenericRpcHandler] = []
        self._table: list[tuple[str, grpc.RpcMethodHandler | None, bool]] = []
        self._resolved: dict[str, int | None] = {}
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
                **self._front_args,
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
            # Interceptors also see calls to unknown methods, as in grpcio, and
            # only the fallback server can run them for those.
            if streamed or generic or self._interceptors:
                self._start_fallback(streamed, generic)
            if generic:
                self._generic = generic
                self._front._resolve_unknown_methods()
            self._table = [
                (path, handler, handler.response_streaming)
                for path, handler in native.items()
            ]
            self._front.add_methods(
                [(path, streaming) for path, _, streaming in self._table]
            )
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
        interceptors = self._interceptors
        if self._limit.maximum is not None:
            # Last: grpcio applies its limit after the interceptors.
            interceptors += (_FallbackLimit(self._limit),)
        fallback = grpc.server(
            self._executor, interceptors=interceptors, options=self._options
        )
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
                    if kind == _EVENT_RESOLVE:
                        self._resolve(call_id, payload, details[0])
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
                    call = _Call(call_id, *self._table[method_id], *details)
                    if self._limit.maximum is not None and not self._limit.admit():
                        # Not on a worker, so busy workers can't delay it.
                        call.admitted = False
                        contextvars.Context().run(self._reject_over_limit, call)
                        continue
                    self._calls[call_id] = call
                    run = self._run_stream if call.streaming else self._run_unary
                    # Each call gets its own context, which its interceptors
                    # and handler share, as in grpcio.
                    future = self._executor.submit(
                        contextvars.Context().run, run, call, payload
                    )
                    if self._limit.maximum is not None:
                        future.add_done_callback(
                            lambda _, call=call: self._release(call)
                        )
                except Exception:
                    _LOGGER.exception("Error dispatching call %d", call_id)
                    call = self._calls.pop(call_id, None)
                    if call is not None:
                        self._release(call)
                    if kind == _EVENT_RESOLVE:
                        front._resolved(call_id, None, remember=False)
                    elif kind == EVENT_CALL:
                        front.fail(
                            call_id,
                            _code(grpc.StatusCode.INTERNAL),
                            "Error dispatching call",
                        )

    def _intercept(self, call: _Call) -> bool:
        """Runs the interceptors, which may replace the call's handler, and
        looks up the handler of a method only generic handlers know."""
        if not self._interceptors and call.handler is not None:
            return True
        front = self._front
        assert front is not None
        registered = call.handler
        details = _HandlerCallDetails(
            call.method,
            tuple(_Metadatum(key, value) for key, value in call.metadata),
        )

        def intercept_at(index: int, details: grpc.HandlerCallDetails) -> Any:
            if index == len(self._interceptors):
                return registered if registered is not None else self._lookup(details)
            return self._interceptors[index].intercept_service(
                lambda details: intercept_at(index + 1, details), details
            )

        try:
            handler = intercept_at(0, details)
        except Exception as exc:
            _LOGGER.exception("Exception servicing handler: %s", exc)
            front.fail(
                call.call_id,
                _code(grpc.StatusCode.UNKNOWN),
                "Error in service handler!",
            )
            return False
        return _use_handler(front, call, handler)

    def _reject_over_limit(self, call: _Call) -> None:
        try:
            if self._intercept(call):
                self._front.fail(  # type: ignore[union-attr]
                    call.call_id,
                    _code(grpc.StatusCode.RESOURCE_EXHAUSTED),
                    _LIMIT_EXCEEDED,
                )
        finally:
            call.terminate()

    def _release(self, call: _Call) -> None:
        with call.lock:
            admitted, call.admitted = call.admitted, False
        if admitted:
            self._limit.release()

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
            if not self._intercept(call):
                self._release(call)
                return
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
            if not self._intercept(call):
                self._release(call)
                return
            ok, request = self._request(call, payload)
            if not ok:
                return
            context = _Context(front, call)
            try:
                if call.handler.response_streaming:
                    responses = call.handler.unary_stream(request, context)
                else:
                    responses = iter((call.handler.unary_unary(request, context),))
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
                what = (
                    "iterating responses"
                    if call.handler.response_streaming
                    else "calling application"
                )
                _LOGGER.exception("Exception %s: %s", what, exc)
                self._fail(
                    call, context, grpc.StatusCode.UNKNOWN, f"Exception {what}: {exc}"
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
