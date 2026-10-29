"""An asyncio server on the native front, like ``grpc.aio.server()``."""

import asyncio
import inspect
import logging
import shutil
import threading
import time
from collections.abc import Callable, Iterable, Sequence
from concurrent.futures import Executor
from typing import Any, NoReturn

import grpc
from grpc import aio

from pygrpc_native._native import (
    EVENT_CANCEL,
    STREAM_CLOSED,
    STREAM_FULL,
    NativeGrpcFront,
)
from pygrpc_native._server import (
    _AUTH_CONTEXT,
    _LIMIT_EXCEEDED,
    _NO_DEADLINE,
    _Codec,
    _code,
    _behavior,
    _ConcurrencyLimit,
    _counted,
    _fallback_socket,
    _HandlerCallDetails,
    _Metadatum,
    _parse_options,
    _split_handlers,
    _use_handler,
    _with_behavior,
)

__all__ = ["Server", "server"]

_LOGGER = logging.getLogger(__name__)

_END = object()


class _Call:
    def __init__(
        self,
        call_id: int,
        method: str,
        handler: grpc.RpcMethodHandler,
        metadata: tuple[tuple[str, str | bytes], ...],
        peer: str,
        time_remaining: float | None,
    ) -> None:
        self.call_id = call_id
        self.method = method
        self.handler = handler
        # Whether the front serves the method as a stream; an interceptor
        # can swap the handler, but not this.
        self.streaming = handler.response_streaming
        self.metadata = metadata
        self.peer = peer
        self.deadline = (
            None if time_remaining is None else time.monotonic() + time_remaining
        )
        self.task: asyncio.Task[None] | None = None
        self.admitted = True
        # The client left or the deadline passed.
        self.cancelled = False
        self.done = False
        self.callbacks: list[Callable[[Any], None]] = []

    def cancel(self) -> None:
        self.cancelled = True
        if self.task is not None:
            self.task.cancel()

    def finish(self, context: "_Context") -> None:
        self.done = True
        callbacks, self.callbacks = self.callbacks, []
        for callback in callbacks:
            try:
                callback(context)
            except Exception:
                _LOGGER.exception("Exception calling done callback")


class _Context(aio.ServicerContext):
    def __init__(self, server: "Server", call: _Call) -> None:
        self._server = server
        self._call = call
        self._code: grpc.StatusCode | None = None
        self._details: str | None = None
        self._initial_metadata: Sequence[tuple[str, str | bytes]] | None = None
        self._initial_sent = False
        self._trailing_metadata: Sequence[tuple[str, str | bytes]] = ()
        # A write() found the stream gone or failed to encode the message.
        self._closed = False
        self._aborted = False

    async def read(self) -> NoReturn:
        raise RuntimeError("read() is only for client-streaming methods")

    async def write(self, message: Any) -> None:
        if not self._call.handler.response_streaming:
            raise RuntimeError("write() is only for server-streaming methods")
        if not self._closed and not await self._server._send(self._call, self, message):
            self._closed = True

    async def send_initial_metadata(
        self, initial_metadata: Sequence[tuple[str, str | bytes]]
    ) -> None:
        if self._initial_sent:
            raise ValueError("Initial metadata no longer allowed!")
        self._initial_sent = True
        if self._call.streaming:
            front = self._server._front
            assert front is not None
            front.stream_send_initial_metadata(
                self._call.call_id, list(initial_metadata)
            )
        else:
            self._initial_metadata = initial_metadata

    async def abort(
        self,
        code: grpc.StatusCode,
        details: str = "",
        trailing_metadata: Sequence[tuple[str, str | bytes]] = (),
    ) -> NoReturn:
        if code == grpc.StatusCode.OK:
            _LOGGER.error("abort() called with StatusCode.OK; returning UNKNOWN")
            code, details = grpc.StatusCode.UNKNOWN, ""
        self._code, self._details = code, details
        if trailing_metadata:
            self._trailing_metadata = trailing_metadata
        self._aborted = True
        raise aio.AbortError("Locally aborted.")

    async def abort_with_status(self, status: grpc.Status) -> NoReturn:
        await self.abort(status.code, status.details, status.trailing_metadata)

    def set_trailing_metadata(
        self, trailing_metadata: Sequence[tuple[str, str | bytes]]
    ) -> None:
        self._trailing_metadata = trailing_metadata

    def trailing_metadata(self) -> Sequence[tuple[str, str | bytes]]:
        return self._trailing_metadata

    def invocation_metadata(self) -> tuple[_Metadatum, ...]:
        return tuple(_Metadatum(key, value) for key, value in self._call.metadata)

    def set_code(self, code: grpc.StatusCode) -> None:
        self._code = code

    def code(self) -> grpc.StatusCode | None:
        return self._code

    def set_details(self, details: str) -> None:
        self._details = details

    def details(self) -> str:
        return self._details or ""

    def set_compression(self, compression: grpc.Compression) -> None:
        pass

    def disable_next_message_compression(self) -> None:
        pass

    def peer(self) -> str:
        return self._call.peer

    def peer_identities(self) -> None:
        return None

    def peer_identity_key(self) -> None:
        return None

    def auth_context(self) -> dict[str, list[bytes]]:
        return _AUTH_CONTEXT

    def time_remaining(self) -> float:
        if self._call.deadline is None:
            return _NO_DEADLINE
        return max(0.0, self._call.deadline - time.monotonic())

    def cancelled(self) -> bool:
        return self._call.cancelled

    def done(self) -> bool:
        return self._call.done

    def add_done_callback(self, callback: Callable[[Any], None]) -> None:
        if self._call.done:
            callback(self)
        else:
            self._call.callbacks.append(callback)


def _to_bytes(serializer: Callable[[Any], bytes] | None, response: Any) -> bytes:
    """Encodes a response as grpc.aio does: text as UTF-8 and None as an empty
    message. Unlike the sync server, a failure here is an unexpected error of
    the handler."""
    if isinstance(response, str):
        response = response.encode("utf-8")
    data = serializer(response) if serializer else response
    if data is None:
        return b""
    if not isinstance(data, bytes):
        raise TypeError(f"Expected bytes, got {type(data).__name__}")
    return data


class _SyncContext:
    """What a handler that isn't a coroutine gets instead of the context, with
    the same methods as grpcio's: they run on the event loop from the handler's
    thread."""

    def __init__(self, context: _Context, loop: asyncio.AbstractEventLoop) -> None:
        self._context = context
        self._loop = loop
        self._callbacks: list[Callable[[], None]] = []

    def abort(
        self,
        code: grpc.StatusCode,
        details: str = "",
        trailing_metadata: Sequence[tuple[str, str | bytes]] = (),
    ) -> None:
        # As in grpcio, this doesn't raise: the call ends with the status
        # once the handler returns.
        asyncio.run_coroutine_threadsafe(
            self._context.abort(code, details, trailing_metadata), self._loop
        ).exception()

    def send_initial_metadata(
        self, metadata: Sequence[tuple[str, str | bytes]]
    ) -> None:
        asyncio.run_coroutine_threadsafe(
            self._context.send_initial_metadata(metadata), self._loop
        ).result()

    def set_trailing_metadata(
        self, metadata: Sequence[tuple[str, str | bytes]]
    ) -> None:
        self._context.set_trailing_metadata(metadata)

    def invocation_metadata(self) -> tuple[_Metadatum, ...]:
        return self._context.invocation_metadata()

    def set_code(self, code: grpc.StatusCode) -> None:
        self._context.set_code(code)

    def set_details(self, details: str) -> None:
        self._context.set_details(details)

    def set_compression(self, compression: grpc.Compression) -> None:
        self._context.set_compression(compression)

    def disable_next_message_compression(self) -> None:
        self._context.disable_next_message_compression()

    def add_callback(self, callback: Callable[[], None]) -> None:
        self._callbacks.append(callback)

    def peer(self) -> str:
        return self._context.peer()

    def peer_identities(self) -> None:
        return self._context.peer_identities()

    def peer_identity_key(self) -> None:
        return self._context.peer_identity_key()

    def auth_context(self) -> dict[str, list[bytes]]:
        return self._context.auth_context()

    def time_remaining(self) -> float:
        return self._context.time_remaining()

    def __getattr__(self, name: str) -> Any:
        # Methods grpcio's sync context lacks still reach the context, as
        # they did before this class existed.
        return getattr(self._context, name)


class _FallbackLimit(aio.ServerInterceptor):
    """Counts the fallback server's calls against the front's limit.

    A call holds a place while its behavior runs. grpc.aio picks how to run a
    behavior by its type, so the wrapper keeps that type.
    """

    def __init__(self, limit: _ConcurrencyLimit) -> None:
        self._limit = limit

    async def intercept_service(
        self,
        continuation: Callable[[grpc.HandlerCallDetails], Any],
        handler_call_details: grpc.HandlerCallDetails,
    ) -> grpc.RpcMethodHandler | None:
        handler = await continuation(handler_call_details)
        if handler is None:
            return None
        behavior, limit = _behavior(handler), self._limit
        if inspect.isasyncgenfunction(behavior):

            async def counted_stream(request: Any, context: Any) -> Any:
                if not limit.admit():
                    await context.abort(
                        grpc.StatusCode.RESOURCE_EXHAUSTED, _LIMIT_EXCEEDED
                    )
                try:
                    async for response in behavior(request, context):
                        yield response
                finally:
                    limit.release()

            return _with_behavior(handler, counted_stream)
        if inspect.iscoroutinefunction(behavior):

            async def counted(request: Any, context: Any) -> Any:
                if not limit.admit():
                    await context.abort(
                        grpc.StatusCode.RESOURCE_EXHAUSTED, _LIMIT_EXCEEDED
                    )
                try:
                    return await behavior(request, context)
                finally:
                    limit.release()

            return _with_behavior(handler, counted)
        return _with_behavior(
            handler, _counted(limit, behavior, handler.response_streaming)
        )


class Server(_Codec, aio.Server):
    """A grpc.aio.Server whose HTTP/2 and gRPC work runs in Rust.

    Handlers run on the event loop that calls ``start()``; handlers that
    aren't coroutines or async generators run in ``migration_thread_pool``.
    Unary and server-streaming methods are served natively on one insecure
    port, and other methods by a grpc.aio server behind it.
    """

    def __init__(
        self,
        migration_thread_pool: Executor | None = None,
        handlers: Sequence[grpc.GenericRpcHandler] | None = None,
        interceptors: Sequence[Any] | None = None,
        options: Iterable[tuple[str, Any]] | None = None,
        maximum_concurrent_rpcs: int | None = None,
        compression: grpc.Compression | None = None,
        io_threads: int = 2,
    ) -> None:
        invalid = [
            interceptor
            for interceptor in interceptors or ()
            if not isinstance(interceptor, aio.ServerInterceptor)
        ]
        if invalid:
            raise ValueError(
                "Interceptor must be ServerInterceptor, "
                f"the following are invalid: {invalid}"
            )
        if maximum_concurrent_rpcs is not None and maximum_concurrent_rpcs <= 0:
            raise ValueError("maximum_concurrent_rpcs should be a positive integer")
        if compression not in (None, grpc.Compression.NoCompression):
            raise NotImplementedError("response compression isn't supported")
        self._limit = _ConcurrencyLimit(maximum_concurrent_rpcs)
        self._options = list(options or ())
        self._receive_limit, self._send_limit, self._front_args = _parse_options(
            self._options
        )
        self._executor = migration_thread_pool
        self._interceptors = tuple(interceptors or ())
        self._io_threads = io_threads
        self._generic_handlers = list(handlers or ())
        self._registered: dict[str, grpc.RpcMethodHandler] = {}
        self._front: NativeGrpcFront | None = None
        self._fallback: aio.Server | None = None
        self._fallback_dir: str | None = None
        self._table: list[tuple[str, grpc.RpcMethodHandler]] = []
        self._calls: dict[int, _Call] = {}
        self._loop: asyncio.AbstractEventLoop | None = None
        self._started = False
        self._stopping = False
        self._terminated = asyncio.Event()

    def add_generic_rpc_handlers(
        self, generic_rpc_handlers: Sequence[grpc.GenericRpcHandler]
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

    async def start(self) -> None:
        if self._started:
            raise ValueError("Cannot start already-started server!")
        if self._front is None:
            raise RuntimeError("add a port with add_insecure_port() before start()")
        self._started = True
        self._loop = asyncio.get_running_loop()
        native, streamed, generic = _split_handlers(
            self._registered, self._generic_handlers
        )
        # Interceptors also see calls to unknown methods, as in grpcio, and
        # only the fallback server can run them for those.
        if streamed or generic or self._interceptors:
            await self._start_fallback(streamed, generic)
        paths = list(native)
        self._front.add_methods(
            [(path, native[path].response_streaming) for path in paths]
        )
        self._table = [(path, native[path]) for path in paths]
        self._front.start()
        threading.Thread(
            target=self._dispatch, name="pygrpc-native-dispatch", daemon=True
        ).start()

    async def _start_fallback(
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
        fallback = aio.server(
            self._executor, interceptors=interceptors, options=self._options
        )
        # grpc.aio ignores add_registered_method_handlers() before grpcio 1.84,
        # so these go in as generic handlers, ahead of the others.
        fallback.add_generic_rpc_handlers(
            [
                grpc.method_handlers_generic_handler(service, handlers)
                for service, handlers in streamed.items()
            ]
            + list(generic)
        )
        fallback.add_insecure_port(f"unix:{path}")
        await fallback.start()
        self._fallback = fallback
        self._front.set_fallback(path)

    async def stop(self, grace: float | None) -> None:
        if self._stopping:
            await self._terminated.wait()
            return
        self._stopping = True
        fallback = None
        if self._fallback is not None:
            fallback = asyncio.ensure_future(self._fallback.stop(grace))
        tasks = [call.task for call in self._calls.values() if call.task is not None]
        if grace and tasks:
            await asyncio.wait(tasks, timeout=grace)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if fallback is not None:
            await fallback
        if self._front is not None:
            await asyncio.get_running_loop().run_in_executor(None, self._front.close)
        if self._fallback_dir is not None:
            shutil.rmtree(self._fallback_dir, ignore_errors=True)
        self._terminated.set()

    async def wait_for_termination(self, timeout: float | None = None) -> bool:
        try:
            await asyncio.wait_for(self._terminated.wait(), timeout)
        except asyncio.TimeoutError:
            return True
        return False

    def _dispatch(self) -> None:
        front, loop = self._front, self._loop
        assert front is not None and loop is not None
        while (events := front.recv_batch_details(64)) is not None:
            try:
                loop.call_soon_threadsafe(self._on_events, events)
            except RuntimeError:
                # The loop was closed without stop().
                front.close()
                return

    def _on_events(self, events: list[tuple[Any, ...]]) -> None:
        front, loop = self._front, self._loop
        assert front is not None and loop is not None
        for kind, call_id, method_id, payload, _, details in events:
            if kind == EVENT_CANCEL:
                call = self._calls.get(call_id)
                if call is not None:
                    call.cancel()
                continue
            if self._stopping:
                front.fail(
                    call_id,
                    _code(grpc.StatusCode.UNAVAILABLE),
                    "Server is shutting down",
                )
                continue
            method, handler = self._table[method_id]
            call = _Call(call_id, method, handler, *details)
            limited = self._limit.maximum is not None
            if limited:
                call.admitted = self._limit.admit()
            self._calls[call_id] = call
            call.task = loop.create_task(self._run(call, payload))
            if limited and call.admitted:
                # A task cancelled before it starts never runs its finally.
                call.task.add_done_callback(self._release_limit)

    def _release_limit(self, task: asyncio.Task[None]) -> None:
        self._limit.release()

    async def _run(self, call: _Call, payload: bytes) -> None:
        context = _Context(self, call)
        try:
            if self._interceptors and not await self._intercept(call):
                return
            if not call.admitted:
                self._front.fail(  # type: ignore[union-attr]
                    call.call_id,
                    _code(grpc.StatusCode.RESOURCE_EXHAUSTED),
                    _LIMIT_EXCEEDED,
                )
                return
            ok, request = self._request(call, payload)
            if not ok:
                return
            if call.streaming:
                await self._run_stream(call, context, request)
            else:
                await self._run_unary(call, context, request)
        except asyncio.CancelledError:
            # Only an RPC the handler cancelled itself still has a client.
            if not call.cancelled and not self._stopping:
                self._front.fail(  # type: ignore[union-attr]
                    call.call_id, _code(grpc.StatusCode.CANCELLED), "Cancelled"
                )
        finally:
            self._calls.pop(call.call_id, None)
            call.finish(context)

    async def _intercept(self, call: _Call) -> bool:
        """Runs the interceptors, which may replace the call's handler."""
        front = self._front
        assert front is not None
        registered = call.handler
        details = _HandlerCallDetails(
            call.method,
            tuple(_Metadatum(key, value) for key, value in call.metadata),
        )

        async def intercept_at(index: int, details: grpc.HandlerCallDetails) -> Any:
            if index == len(self._interceptors):
                return registered
            return await self._interceptors[index].intercept_service(
                lambda details: intercept_at(index + 1, details), details
            )

        try:
            handler = await intercept_at(0, details)
        except Exception as exc:
            _LOGGER.exception("Unexpected error in interceptor: %s", exc)
            front.fail(
                call.call_id,
                _code(grpc.StatusCode.UNKNOWN),
                f"Unexpected {type(exc)}: {exc}",
            )
            return False
        return _use_handler(front, call, handler)

    def _fail(self, call: _Call, context: _Context, details: str | None) -> None:
        code = context._code or grpc.StatusCode.UNKNOWN
        if code == grpc.StatusCode.OK:
            code = grpc.StatusCode.UNKNOWN
        if details is None:
            details = context._details or ""
        self._front.fail(  # type: ignore[union-attr]
            call.call_id,
            _code(code),
            details,
            trailing_metadata=list(context._trailing_metadata),
        )

    def _failed(self, call: _Call, context: _Context, exc: Exception) -> None:
        if isinstance(exc, aio.AbortError):
            self._fail(call, context, None)
        else:
            _LOGGER.exception("Unexpected error in RPC handler: %s", exc)
            self._fail(call, context, f"Unexpected {type(exc)}: {exc}")

    async def _unary_response(
        self, call: _Call, context: _Context, request: Any
    ) -> tuple[bool, Any]:
        handler = call.handler.unary_unary
        try:
            if inspect.iscoroutinefunction(handler):
                response = await handler(request, context)
            else:
                loop = asyncio.get_running_loop()
                sync_context = _SyncContext(context, loop)
                response = await loop.run_in_executor(
                    self._executor, handler, request, sync_context
                )
                for callback in sync_context._callbacks:
                    callback()
        except Exception as exc:
            self._failed(call, context, exc)
            return False, None
        if context._code not in (None, grpc.StatusCode.OK):
            self._fail(call, context, None)
            return False, None
        return True, response

    async def _run_unary(self, call: _Call, context: _Context, request: Any) -> None:
        ok, response = await self._unary_response(call, context, request)
        if not ok:
            return
        data = self._encode(call, context, response)
        if data is not None:
            self._front.respond(  # type: ignore[union-attr]
                call.call_id,
                data,
                initial_metadata=None
                if context._initial_metadata is None
                else list(context._initial_metadata),
                trailing_metadata=list(context._trailing_metadata),
            )

    async def _run_stream(self, call: _Call, context: _Context, request: Any) -> None:
        if not call.handler.response_streaming:
            # A unary handler from an interceptor answers with one message.
            ok, response = await self._unary_response(call, context, request)
            if ok and await self._send(call, context, response):
                self._front.stream_end(  # type: ignore[union-attr]
                    call.call_id, trailing_metadata=list(context._trailing_metadata)
                )
            return
        handler = call.handler.unary_stream
        try:
            if inspect.isasyncgenfunction(handler):
                responses = handler(request, context)
                try:
                    async for response in responses:
                        if not await self._send(call, context, response):
                            return
                finally:
                    await responses.aclose()
            elif inspect.iscoroutinefunction(handler):
                await handler(request, context)
                if context._closed:
                    return
            elif not await self._iterate(call, context, handler, request):
                return
        except Exception as exc:
            self._failed(call, context, exc)
            return
        if context._code not in (None, grpc.StatusCode.OK):
            self._fail(call, context, None)
        else:
            self._front.stream_end(  # type: ignore[union-attr]
                call.call_id, trailing_metadata=list(context._trailing_metadata)
            )

    async def _iterate(
        self, call: _Call, context: _Context, handler: Any, request: Any
    ) -> bool:
        loop = asyncio.get_running_loop()
        responses = await loop.run_in_executor(
            self._executor, handler, request, _SyncContext(context, loop)
        )
        try:
            while (
                response := await loop.run_in_executor(
                    self._executor, next, responses, _END
                )
            ) is not _END:
                # Nothing more goes out once the handler aborted.
                if context._aborted:
                    break
                if not await self._send(call, context, response):
                    return False
        finally:
            close = getattr(responses, "close", None)
            if close is not None:
                close()
        return True

    def _encode(self, call: _Call, context: _Context, response: Any) -> bytes | None:
        try:
            data = _to_bytes(call.handler.response_serializer, response)
        except Exception as exc:
            self._failed(call, context, exc)
            return None
        return self._within_send_limit(call, data)

    async def _send(self, call: _Call, context: _Context, response: Any) -> bool:
        front = self._front
        assert front is not None
        data = self._encode(call, context, response)
        if data is None:
            return False
        context._initial_sent = True
        sent = front.stream_try_send(call.call_id, data)
        if sent == STREAM_FULL:
            # The client is behind; wait for room off the loop.
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(
                None, front.stream_send, call.call_id, data
            )
        return sent != STREAM_CLOSED


def server(
    migration_thread_pool: Executor | None = None,
    handlers: Sequence[grpc.GenericRpcHandler] | None = None,
    interceptors: Sequence[Any] | None = None,
    options: Iterable[tuple[str, Any]] | None = None,
    maximum_concurrent_rpcs: int | None = None,
    compression: grpc.Compression | None = None,
) -> Server:
    """Like ``grpc.aio.server()``, on the native front."""
    return Server(
        migration_thread_pool,
        handlers=handlers,
        interceptors=interceptors,
        options=options,
        maximum_concurrent_rpcs=maximum_concurrent_rpcs,
        compression=compression,
    )
