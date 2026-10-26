"""Server interceptors on server() and aio.server(), against grpcio."""

import asyncio
import contextvars
import importlib
import sys
from collections.abc import AsyncIterator, Callable, Iterator
from concurrent import futures
from typing import Any

import grpc
import pytest
from grpc import aio

import pygrpc_native
import pygrpc_native.aio

grpc_tools = pytest.importorskip("grpc_tools.protoc")

PROTO = """
syntax = "proto3";
package i;
service S {
  rpc Unary(Msg) returns (Msg);
  rpc Stream(Msg) returns (stream Msg);
  rpc Chat(stream Msg) returns (stream Msg);
}
message Msg { string s = 1; }
"""

USER = contextvars.ContextVar("user", default="")


@pytest.fixture(scope="module")
def pb(tmp_path_factory: pytest.TempPathFactory) -> tuple[Any, Any]:
    out = tmp_path_factory.mktemp("proto")
    (out / "i.proto").write_text(PROTO)
    assert (
        grpc_tools.main(
            [
                "",
                f"-I{out}",
                f"--python_out={out}",
                f"--grpc_python_out={out}",
                str(out / "i.proto"),
            ]
        )
        == 0
    )
    sys.path.insert(0, str(out))
    try:
        return importlib.import_module("i_pb2"), importlib.import_module("i_pb2_grpc")
    finally:
        sys.path.remove(str(out))


def _token(details: grpc.HandlerCallDetails) -> str | None:
    for key, value in details.invocation_metadata:
        if key == "authorization":
            return value
    return None


def _deny(request: Any, context: grpc.ServicerContext) -> Any:
    context.abort(grpc.StatusCode.UNAUTHENTICATED, "denied")


def _deny_stream(request: Any, context: grpc.ServicerContext) -> Iterator[Any]:
    context.abort(grpc.StatusCode.UNAUTHENTICATED, "denied")
    yield


async def _deny_async(request: Any, context: aio.ServicerContext) -> Any:
    await context.abort(grpc.StatusCode.UNAUTHENTICATED, "denied")


async def _deny_async_stream(
    request: Any, context: aio.ServicerContext
) -> AsyncIterator[Any]:
    await context.abort(grpc.StatusCode.UNAUTHENTICATED, "denied")
    yield


def _rejecting(handler: grpc.RpcMethodHandler, is_aio: bool) -> Any:
    """A handler of the same kind that rejects the call without decoding it."""
    unary, stream = (
        (_deny_async, _deny_async_stream) if is_aio else (_deny, _deny_stream)
    )
    if handler.request_streaming and handler.response_streaming:
        return grpc.stream_stream_rpc_method_handler(stream)
    if handler.response_streaming:
        return grpc.unary_stream_rpc_method_handler(stream)
    return grpc.unary_unary_rpc_method_handler(unary)


def _check(
    details: grpc.HandlerCallDetails, handler: Any, is_aio: bool, same_kind: bool
) -> Any:
    token = _token(details)
    if details.method.endswith("/Unary") and token == "missing":
        return None
    if token == "raise":
        raise ValueError("bad token")
    if token == "ok":
        USER.set("alice")
        return handler
    if same_kind:
        return _rejecting(handler, is_aio)
    # Many interceptors reject every call with one unary handler.
    return grpc.unary_unary_rpc_method_handler(_deny_async if is_aio else _deny)


class Auth(grpc.ServerInterceptor):
    def __init__(self, same_kind: bool) -> None:
        self.same_kind = same_kind

    def intercept_service(
        self, continuation: Callable[..., Any], details: grpc.HandlerCallDetails
    ) -> Any:
        return _check(details, continuation(details), False, self.same_kind)


class AioAuth(aio.ServerInterceptor):
    def __init__(self, same_kind: bool) -> None:
        self.same_kind = same_kind

    async def intercept_service(
        self, continuation: Callable[..., Any], details: grpc.HandlerCallDetails
    ) -> Any:
        return _check(details, await continuation(details), True, self.same_kind)


class Order(grpc.ServerInterceptor):
    def __init__(self, name: str, seen: list[str]) -> None:
        self.name, self.seen = name, seen

    def intercept_service(
        self, continuation: Callable[..., Any], details: grpc.HandlerCallDetails
    ) -> Any:
        self.seen.append(f"{self.name} {details.method}")
        return continuation(details)


class AioOrder(aio.ServerInterceptor):
    def __init__(self, name: str, seen: list[str]) -> None:
        self.name, self.seen = name, seen

    async def intercept_service(
        self, continuation: Callable[..., Any], details: grpc.HandlerCallDetails
    ) -> Any:
        self.seen.append(f"{self.name} {details.method}")
        return await continuation(details)


def _calls(pb: tuple[Any, Any], port: int) -> dict[str, Any]:
    i_pb2, i_pb2_grpc = pb
    results: dict[str, Any] = {}
    with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
        stub = i_pb2_grpc.SStub(channel)
        for token in ("ok", "bad", "missing", "raise"):
            metadata = [("authorization", token)]
            calls = {
                "unary": lambda: stub.Unary(i_pb2.Msg(s="x"), metadata=metadata).s,
                "stream": lambda: [
                    m.s for m in stub.Stream(i_pb2.Msg(s="x"), metadata=metadata)
                ],
                "chat": lambda: [
                    m.s for m in stub.Chat(iter([i_pb2.Msg(s="x")]), metadata=metadata)
                ],
            }
            for name, call in calls.items():
                try:
                    results[f"{name} {token}"] = call()
                except grpc.RpcError as error:
                    results[f"{name} {token}"] = (error.code(), error.details())
    return results


def _serve_sync(pb: tuple[Any, Any], kind: str, interceptors: list[Any]) -> Any:
    i_pb2, i_pb2_grpc = pb

    class Servicer(i_pb2_grpc.SServicer):
        def Unary(self, request: Any, context: grpc.ServicerContext) -> Any:
            return i_pb2.Msg(s=USER.get())

        def Stream(self, request: Any, context: grpc.ServicerContext) -> Any:
            yield i_pb2.Msg(s=USER.get())

        def Chat(self, requests: Any, context: grpc.ServicerContext) -> Any:
            for _ in requests:
                yield i_pb2.Msg(s=USER.get())

    factory = {"grpcio": grpc.server, "native": pygrpc_native.server}[kind]
    server = factory(futures.ThreadPoolExecutor(4), interceptors=interceptors)
    i_pb2_grpc.add_SServicer_to_server(Servicer(), server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    return server, port


async def _serve_aio(pb: tuple[Any, Any], kind: str, interceptors: list[Any]) -> Any:
    i_pb2, i_pb2_grpc = pb

    class Servicer(i_pb2_grpc.SServicer):
        async def Unary(self, request: Any, context: aio.ServicerContext) -> Any:
            return i_pb2.Msg(s=USER.get())

        async def Stream(
            self, request: Any, context: aio.ServicerContext
        ) -> AsyncIterator[Any]:
            yield i_pb2.Msg(s=USER.get())

        async def Chat(
            self, requests: AsyncIterator[Any], context: aio.ServicerContext
        ) -> AsyncIterator[Any]:
            async for _ in requests:
                yield i_pb2.Msg(s=USER.get())

    factory = {"grpcio": aio.server, "native": pygrpc_native.aio.server}[kind]
    server = factory(interceptors=interceptors)
    i_pb2_grpc.add_SServicer_to_server(Servicer(), server)
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    return server, port


def _sync_results(pb: tuple[Any, Any], kind: str, interceptors: list[Any]) -> Any:
    server, port = _serve_sync(pb, kind, interceptors)
    try:
        return _calls(pb, port)
    finally:
        server.stop(None)


def _aio_results(pb: tuple[Any, Any], kind: str, interceptors: list[Any]) -> Any:
    async def run() -> Any:
        server, port = await _serve_aio(pb, kind, interceptors)
        try:
            return await asyncio.get_running_loop().run_in_executor(
                None, _calls, pb, port
            )
        finally:
            await server.stop(None)

    return asyncio.run(run())


DENIED = (grpc.StatusCode.UNAUTHENTICATED, "denied")
NOT_FOUND = (grpc.StatusCode.UNIMPLEMENTED, "Method not found!")


@pytest.mark.parametrize("same_kind", [True, False])
def test_sync_interceptors_match_grpcio(pb: tuple[Any, Any], same_kind: bool) -> None:
    results = _sync_results(pb, "native", [Auth(same_kind)])
    assert results == _sync_results(pb, "grpcio", [Auth(same_kind)])
    assert results["unary ok"] == "alice"
    assert results["stream ok"] == ["alice"]
    assert results["chat ok"] == ["alice"]
    assert results["unary bad"] == DENIED
    assert results["stream bad"] == DENIED
    assert results["unary missing"] == NOT_FOUND
    assert results["unary raise"] == (
        grpc.StatusCode.UNKNOWN,
        "Error in service handler!",
    )


@pytest.mark.parametrize("same_kind", [True, False])
def test_aio_interceptors_match_grpcio(pb: tuple[Any, Any], same_kind: bool) -> None:
    results = _aio_results(pb, "native", [AioAuth(same_kind)])
    assert results == _aio_results(pb, "grpcio", [AioAuth(same_kind)])
    assert results["unary ok"] == "alice"
    assert results["stream ok"] == ["alice"]
    assert results["chat ok"] == ["alice"]
    assert results["unary bad"] == DENIED
    assert results["stream bad"] == DENIED
    assert results["unary missing"] == NOT_FOUND
    assert results["unary raise"] == (
        grpc.StatusCode.UNKNOWN,
        "Unexpected <class 'ValueError'>: bad token",
    )


def test_interceptors_run_in_order(pb: tuple[Any, Any]) -> None:
    seen: list[str] = []
    server, port = _serve_sync(pb, "native", [Order("a", seen), Order("b", seen)])
    try:
        _calls(pb, port)
    finally:
        server.stop(None)
    assert seen[:2] == ["a /i.S/Unary", "b /i.S/Unary"]

    seen.clear()

    async def run() -> None:
        server, port = await _serve_aio(
            pb, "native", [AioOrder("a", seen), AioOrder("b", seen)]
        )
        try:
            await asyncio.get_running_loop().run_in_executor(None, _calls, pb, port)
        finally:
            await server.stop(None)

    asyncio.run(run())
    assert seen[:2] == ["a /i.S/Unary", "b /i.S/Unary"]


def test_aio_rejects_sync_interceptors() -> None:
    with pytest.raises(ValueError, match="Interceptor must be ServerInterceptor"):
        pygrpc_native.aio.server(interceptors=[Auth(True)])


def _unknown_method_results(port: int) -> dict[str, Any]:
    results: dict[str, Any] = {}
    with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
        for path in ("/i.S/Unary", "/i.S/Missing"):
            for token in ("ok", "bad"):
                try:
                    results[f"{path} {token}"] = channel.unary_unary(path)(
                        b"", metadata=[("authorization", token)], timeout=5
                    )
                except grpc.RpcError as error:
                    results[f"{path} {token}"] = (error.code(), error.details())
    return results


def test_sync_interceptors_see_unknown_methods() -> None:
    # Without streaming methods there would be no fallback server.
    def unary(request: bytes, context: grpc.ServicerContext) -> bytes:
        return USER.get().encode()

    def results(factory: Any) -> dict[str, Any]:
        server = factory(futures.ThreadPoolExecutor(2), interceptors=[Auth(False)])
        server.add_registered_method_handlers(
            "i.S", {"Unary": grpc.unary_unary_rpc_method_handler(unary)}
        )
        port = server.add_insecure_port("127.0.0.1:0")
        server.start()
        try:
            return _unknown_method_results(port)
        finally:
            server.stop(None)

    native = results(pygrpc_native.server)
    assert native == results(grpc.server)
    assert native["/i.S/Missing bad"] == DENIED
    assert native["/i.S/Missing ok"] == NOT_FOUND


def test_aio_interceptors_see_unknown_methods() -> None:
    async def unary(request: bytes, context: aio.ServicerContext) -> bytes:
        return USER.get().encode()

    async def results(factory: Any) -> dict[str, Any]:
        server = factory(interceptors=[AioAuth(False)])
        server.add_registered_method_handlers(
            "i.S", {"Unary": grpc.unary_unary_rpc_method_handler(unary)}
        )
        port = server.add_insecure_port("127.0.0.1:0")
        await server.start()
        try:
            return await asyncio.get_running_loop().run_in_executor(
                None, _unknown_method_results, port
            )
        finally:
            await server.stop(None)

    native = asyncio.run(results(pygrpc_native.aio.server))
    assert native == asyncio.run(results(aio.server))
    assert native["/i.S/Missing bad"] == DENIED
    assert native["/i.S/Missing ok"] == NOT_FOUND


class Replace(grpc.ServerInterceptor):
    def __init__(self, handler: grpc.RpcMethodHandler) -> None:
        self.handler = handler

    def intercept_service(
        self, continuation: Callable[..., Any], details: grpc.HandlerCallDetails
    ) -> Any:
        return self.handler


def _replaced(factory: Any, handler: grpc.RpcMethodHandler, path: str) -> Any:
    server = factory(futures.ThreadPoolExecutor(2), interceptors=[Replace(handler)])
    server.add_registered_method_handlers(
        "i.S",
        {
            "Unary": grpc.unary_unary_rpc_method_handler(lambda r, c: r),
            "Stream": grpc.unary_stream_rpc_method_handler(lambda r, c: iter([r])),
        },
    )
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    try:
        with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
            if path.endswith("Stream"):
                return list(channel.unary_stream(path)(b"x", timeout=5))
            return channel.unary_unary(path)(b"x", timeout=5)
    except grpc.RpcError as error:
        return (error.code(), error.details())
    finally:
        server.stop(None)


def test_unary_handler_for_a_stream_fails_like_grpcio() -> None:
    def fail(request: bytes, context: grpc.ServicerContext) -> bytes:
        raise ValueError("broken")

    handler = grpc.unary_unary_rpc_method_handler(fail)
    native = _replaced(pygrpc_native.server, handler, "/i.S/Stream")
    assert native == _replaced(grpc.server, handler, "/i.S/Stream")
    assert native == (grpc.StatusCode.UNKNOWN, "Exception calling application: broken")


def test_streaming_request_handler_is_rejected() -> None:
    handler = grpc.stream_unary_rpc_method_handler(lambda requests, c: b"")
    assert _replaced(pygrpc_native.server, handler, "/i.S/Unary") == (
        grpc.StatusCode.INTERNAL,
        "Error in service handler!",
    )
