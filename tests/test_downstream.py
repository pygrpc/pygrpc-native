"""Servers built the way downstream projects build theirs.

Each profile copies a project's server options, interceptors and address from
its source, so a change that would stop one of them from switching to
pygrpc-native fails here. Every profile runs on grpcio and on the native
server, and the results must match.
"""

import asyncio
import hmac
import inspect
import os
import tempfile
from collections.abc import AsyncIterator, Callable, Iterator
from concurrent import futures
from typing import Any

import grpc
import pytest
from grpc import aio

import pygrpc_native
import pygrpc_native.aio

MB = 1024 * 1024
TOKEN = "secret"


def _echo(request: bytes, context: grpc.ServicerContext) -> bytes:
    if request == b"raise":
        raise ValueError("handler failed")
    return request


def _echo_stream(request: bytes, context: grpc.ServicerContext) -> Iterator[bytes]:
    yield request
    yield request


async def _echo_async(request: bytes, context: aio.ServicerContext) -> bytes:
    if request == b"raise":
        raise ValueError("handler failed")
    return request


async def _echo_stream_async(
    request: bytes, context: aio.ServicerContext
) -> AsyncIterator[bytes]:
    yield request
    yield request


# LocalAI backend/python/common/grpc_auth.py: a hand-built unary handler that
# rejects any call, streaming ones included, from sync and aio servers alike.
class _AbortHandler(grpc.RpcMethodHandler):
    def __init__(self) -> None:
        self.request_streaming = False
        self.response_streaming = False
        self.request_deserializer = None
        self.response_serializer = None
        self.unary_unary = self._abort
        self.unary_stream = None
        self.stream_unary = None
        self.stream_stream = None

    @staticmethod
    def _abort(request: Any, context: Any) -> None:
        context.abort(grpc.StatusCode.UNAUTHENTICATED, "invalid token")


def _token_ok(details: grpc.HandlerCallDetails) -> bool:
    auth = dict(details.invocation_metadata).get("authorization", "")
    return hmac.compare_digest(auth, "Bearer " + TOKEN)


class LocalAiAuth(grpc.ServerInterceptor):
    def intercept_service(
        self, continuation: Callable[..., Any], details: grpc.HandlerCallDetails
    ) -> Any:
        return continuation(details) if _token_ok(details) else _AbortHandler()


class LocalAiAsyncAuth(aio.ServerInterceptor):
    async def intercept_service(
        self, continuation: Callable[..., Any], details: grpc.HandlerCallDetails
    ) -> Any:
        if not _token_ok(details):
            return _AbortHandler()
        return await continuation(details)


def _rebuild(handler: Any, behavior: Callable[..., Any]) -> Any:
    if handler.response_streaming:
        return grpc.unary_stream_rpc_method_handler(
            behavior,
            request_deserializer=handler.request_deserializer,
            response_serializer=handler.response_serializer,
        )
    return grpc.unary_unary_rpc_method_handler(
        behavior,
        request_deserializer=handler.request_deserializer,
        response_serializer=handler.response_serializer,
    )


# LocalAI backend/python/common/model_identity.py, which every backend
# installs ahead of the token check. It rebuilds the handler around a guard.
class LocalAiModelIdentity(grpc.ServerInterceptor):
    def intercept_service(
        self, continuation: Callable[..., Any], details: grpc.HandlerCallDetails
    ) -> Any:
        handler = continuation(details)
        if handler is None:
            return None
        if handler.response_streaming:
            original = handler.unary_stream
        else:
            original = handler.unary_unary

        def guard(request: Any, context: Any) -> Any:
            return original(request, context)

        return _rebuild(handler, guard)


# On aio it runs a sync behavior, such as the token check's abort handler, in
# a thread with the async context, and awaits whatever that returns.
class LocalAiAsyncModelIdentity(aio.ServerInterceptor):
    async def intercept_service(
        self, continuation: Callable[..., Any], details: grpc.HandlerCallDetails
    ) -> Any:
        handler = await continuation(details)
        if handler is None:
            return None
        if handler.response_streaming:
            original_stream = handler.unary_stream

            async def guard_stream(request: Any, context: Any) -> AsyncIterator[Any]:
                async for response in original_stream(request, context):
                    yield response

            return _rebuild(handler, guard_stream)
        original = handler.unary_unary

        async def guard(request: Any, context: Any) -> Any:
            if inspect.iscoroutinefunction(original):
                return await original(request, context)
            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(None, original, request, context)
            if inspect.isawaitable(result):
                result = await result
            return result

        return _rebuild(handler, guard)


# KServe: a logging interceptor that passes every call on.
class LoggingInterceptor(aio.ServerInterceptor):
    async def intercept_service(
        self, continuation: Callable[..., Any], details: grpc.HandlerCallDetails
    ) -> Any:
        return await continuation(details)


def _kserve_interceptors() -> list[Any]:
    grpc_interceptor = pytest.importorskip("grpc_interceptor")
    otel = pytest.importorskip("opentelemetry.instrumentation.grpc")
    return [
        LoggingInterceptor(),
        grpc_interceptor.AsyncExceptionToStatusInterceptor(),
        otel.aio_server_interceptor(),
    ]


# name: (aio, server kwargs, interceptors, address kind)
PROFILES: dict[str, tuple[bool, dict[str, Any], Callable[[], list[Any]], str]] = {
    # tensorrt_llm/grpc/openengine/server.py, one frontend
    "tensorrt-llm-openengine": (
        True,
        {
            "options": [
                ("grpc.max_receive_message_length", 64 * MB),
                ("grpc.max_send_message_length", 64 * MB),
                ("grpc.keepalive_time_ms", 30000),
                ("grpc.keepalive_timeout_ms", 10000),
                ("grpc.keepalive_permit_without_calls", 1),
                ("grpc.http2.max_pings_without_data", 0),
                ("grpc.http2.min_ping_interval_without_data_ms", 10000),
                ("grpc.so_reuseport", 0),
            ]
        },
        list,
        "tcp",
    ),
    # tensorrt_llm/grpc/smg/server.py
    "tensorrt-llm-smg": (
        True,
        {
            "options": [
                ("grpc.max_send_message_length", 32 * MB),
                ("grpc.max_receive_message_length", 32 * MB),
                ("grpc.keepalive_time_ms", 30000),
                ("grpc.keepalive_timeout_ms", 10000),
                ("grpc.keepalive_permit_without_calls", True),
                ("grpc.http2.min_recv_ping_interval_without_data_ms", 10000),
            ]
        },
        list,
        "tcp",
    ),
    # vllm/entrypoints/launchers/grpc_server.py
    "vllm": (
        True,
        {
            "options": [
                ("grpc.max_send_message_length", -1),
                ("grpc.max_receive_message_length", -1),
                ("grpc.http2.max_ping_strikes", 0),
                ("grpc.http2.min_ping_interval_without_data_ms", 10000),
                ("grpc.keepalive_permit_without_calls", True),
            ]
        },
        list,
        "tcp",
    ),
    # smg grpc_servicer/smg_grpc_servicer/sglang/server.py
    "sglang": (
        True,
        {
            "migration_thread_pool": futures.ThreadPoolExecutor(max_workers=10),
            "options": [
                ("grpc.max_send_message_length", 256 * MB),
                ("grpc.max_receive_message_length", 256 * MB),
                ("grpc.http2.min_ping_interval_without_data_ms", 10000),
                ("grpc.keepalive_permit_without_calls", True),
            ],
        },
        list,
        "tcp",
    ),
    # LocalAI backend/python/rerankers/backend.py
    "localai-sync": (
        False,
        {
            "thread_pool": futures.ThreadPoolExecutor(max_workers=1),
            "options": [
                ("grpc.max_message_length", 50 * MB),
                ("grpc.max_send_message_length", 50 * MB),
                ("grpc.max_receive_message_length", 50 * MB),
            ],
        },
        lambda: [LocalAiModelIdentity(), LocalAiAuth()],
        "tcp",
    ),
    # LocalAI backend/python/vllm/backend.py
    "localai-aio": (
        True,
        {
            "migration_thread_pool": futures.ThreadPoolExecutor(max_workers=1),
            "options": [
                ("grpc.max_message_length", 50 * MB),
                ("grpc.max_send_message_length", 50 * MB),
                ("grpc.max_receive_message_length", 50 * MB),
            ],
        },
        lambda: [LocalAiAsyncModelIdentity(), LocalAiAsyncAuth()],
        "tcp",
    ),
    # kserve/protocol/grpc/server.py, with the default message limits
    "kserve": (
        True,
        {
            "migration_thread_pool": futures.ThreadPoolExecutor(max_workers=4),
            "options": [
                ("grpc.max_send_message_length", 8388608),
                ("grpc.max_receive_message_length", 8388608),
            ],
        },
        _kserve_interceptors,
        "ipv6-any",
    ),
    # llm-d-kv-cache services/uds_tokenizer/tokenizer_grpc_service.py
    "llm-d": (
        True,
        {
            "options": [
                ("grpc.max_send_message_length", 100 * MB),
                ("grpc.max_receive_message_length", 100 * MB),
                ("grpc.keepalive_time_ms", 7200000),
                ("grpc.keepalive_timeout_ms", 20000),
                ("grpc.keepalive_permit_without_calls", 1),
                ("grpc.http2.max_pings_without_data", 0),
                ("grpc.http2.min_time_between_pings_ms", 10000),
                ("grpc.http2.min_ping_interval_without_data_ms", 10000),
                ("grpc.http2.max_frame_size", 8192),
            ]
        },
        list,
        "uds",
    ),
}


def _calls(target: str) -> dict[str, Any]:
    results: dict[str, Any] = {}
    with grpc.insecure_channel(target) as channel:
        for name, metadata in (
            ("auth", [("authorization", "Bearer " + TOKEN)]),
            ("no-auth", []),
        ):
            for request in (b"x", b"raise"):
                key = f"{name} unary {request.decode()}"
                try:
                    results[key] = channel.unary_unary("/t.S/U")(
                        request, metadata=metadata, timeout=10
                    )
                except grpc.RpcError as error:
                    results[key] = (error.code(), error.details())
            try:
                results[f"{name} stream"] = list(
                    channel.unary_stream("/t.S/S")(b"s", metadata=metadata, timeout=10)
                )
            except grpc.RpcError as error:
                results[f"{name} stream"] = (error.code(), error.details())
    return results


def _address(kind: str, directory: str, name: str) -> tuple[str, Callable[[int], str]]:
    if kind == "uds":
        path = os.path.join(directory, f"{name}.sock")
        return f"unix://{path}", lambda port: f"unix://{path}"
    if kind == "ipv6-any":
        return "[::]:0", lambda port: f"[::1]:{port}"
    return "127.0.0.1:0", lambda port: f"127.0.0.1:{port}"


def _run(name: str, native: bool) -> dict[str, Any]:
    is_aio, kwargs, interceptors, kind = PROFILES[name]
    kwargs = dict(kwargs, interceptors=interceptors() or None)
    with tempfile.TemporaryDirectory() as directory:
        bind, target = _address(kind, directory, f"{name}-{native}")
        if not is_aio:
            factory = pygrpc_native.server if native else grpc.server
            server = factory(**kwargs)
            server.add_registered_method_handlers(
                "t.S",
                {
                    "U": grpc.unary_unary_rpc_method_handler(_echo),
                    "S": grpc.unary_stream_rpc_method_handler(_echo_stream),
                },
            )
            port = server.add_insecure_port(bind)
            server.start()
            try:
                return _calls(target(port))
            finally:
                server.stop(None)

        async def serve() -> dict[str, Any]:
            factory = pygrpc_native.aio.server if native else aio.server
            server = factory(**kwargs)
            server.add_registered_method_handlers(
                "t.S",
                {
                    "U": grpc.unary_unary_rpc_method_handler(_echo_async),
                    "S": grpc.unary_stream_rpc_method_handler(_echo_stream_async),
                },
            )
            port = server.add_insecure_port(bind)
            await server.start()
            try:
                return await asyncio.get_running_loop().run_in_executor(
                    None, _calls, target(port)
                )
            finally:
                await server.stop(None)

        return asyncio.run(serve())


@pytest.mark.parametrize("name", PROFILES)
def test_downstream_server_matches_grpcio(name: str) -> None:
    native = _run(name, native=True)
    assert native == _run(name, native=False)
    assert native["auth unary x"] == b"x"
    assert native["auth stream"] == [b"s", b"s"]
