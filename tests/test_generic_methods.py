"""Unary and server-streaming methods that only generic handlers know are
served natively, and answer as grpcio does."""

import asyncio
from collections.abc import AsyncIterator, Callable, Iterator
from concurrent import futures
from typing import Any, cast

import grpc
import pytest
from grpc import aio

import pygrpc_native
import pygrpc_native.aio


def _peer(context: Any) -> bytes:
    # "ipv4" from the client itself, "unix" through the fallback server.
    return context.peer().split(":")[0].encode()


class _Broken(grpc.GenericRpcHandler):
    def service(self, details: grpc.HandlerCallDetails) -> None:
        if details.method == "/b.S/Call":
            raise ValueError("broken")


def _sync_handlers() -> list[grpc.GenericRpcHandler]:
    def unary(request: bytes, context: Any) -> bytes:
        return _peer(context)

    def stream(request: bytes, context: Any) -> Iterator[bytes]:
        yield _peer(context)
        yield request

    def up(requests: Iterator[bytes], context: Any) -> bytes:
        return b"".join(requests)

    return [
        _Broken(),
        grpc.method_handlers_generic_handler(
            "g.S",
            {
                "Unary": grpc.unary_unary_rpc_method_handler(unary),
                "Stream": grpc.unary_stream_rpc_method_handler(stream),
                "Up": grpc.stream_unary_rpc_method_handler(up),
            },
        ),
    ]


def _aio_handlers() -> list[grpc.GenericRpcHandler]:
    async def unary(request: bytes, context: Any) -> bytes:
        return _peer(context)

    async def stream(request: bytes, context: Any) -> AsyncIterator[bytes]:
        yield _peer(context)
        yield request

    async def up(requests: AsyncIterator[bytes], context: Any) -> bytes:
        return b"".join([request async for request in requests])

    return [
        _Broken(),
        grpc.method_handlers_generic_handler(
            "g.S",
            {
                "Unary": grpc.unary_unary_rpc_method_handler(unary),
                "Stream": grpc.unary_stream_rpc_method_handler(stream),
                "Up": grpc.stream_unary_rpc_method_handler(up),
            },
        ),
    ]


def _outcome(call: Callable[[], Any]) -> Any:
    try:
        return call()
    except grpc.RpcError as error:
        error = cast(grpc.Call, error)
        return error.code(), error.details()


def _client(target: str) -> Any:
    with grpc.insecure_channel(target) as channel:
        unary = channel.unary_unary("/g.S/Unary")
        # The first calls arrive together, before the method is known.
        first = [unary.future(b"x", timeout=5) for _ in range(8)]
        return (
            [call.result() for call in first],
            unary(b"x", timeout=5),
            list(channel.unary_stream("/g.S/Stream")(b"x", timeout=5)),
            channel.stream_unary("/g.S/Up")(iter([b"a", b"b"]), timeout=5),
            _outcome(lambda: channel.unary_unary("/g.S/Nope")(b"x", timeout=5)),
            _outcome(lambda: channel.unary_unary("/b.S/Call")(b"x", timeout=5)),
        )


def _serve_sync(factory: Callable, client: Callable[[str], Any]) -> Any:
    server = factory(futures.ThreadPoolExecutor(8))
    server.add_generic_rpc_handlers(_sync_handlers())
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    try:
        return client(f"127.0.0.1:{port}")
    finally:
        server.stop(None)


def _serve_aio(factory: Callable, client: Callable[[str], Any]) -> Any:
    async def serve() -> Any:
        server = factory()
        server.add_generic_rpc_handlers(_aio_handlers())
        port = server.add_insecure_port("127.0.0.1:0")
        await server.start()
        try:
            return await asyncio.get_running_loop().run_in_executor(
                None, client, f"127.0.0.1:{port}"
            )
        finally:
            await server.stop(None)

    return asyncio.run(serve())


@pytest.mark.parametrize(
    "serve, native, reference",
    [
        (_serve_sync, pygrpc_native.server, grpc.server),
        (_serve_aio, pygrpc_native.aio.server, aio.server),
    ],
    ids=["sync", "aio"],
)
def test_generic_methods_answer_as_grpcio(
    serve: Callable, native: Callable, reference: Callable
) -> None:
    got = serve(native, _client)
    assert got == serve(reference, _client)
    assert got[1] == b"ipv4"


class _CatchAll(grpc.GenericRpcHandler):
    def service(self, details: grpc.HandlerCallDetails) -> grpc.RpcMethodHandler:
        return grpc.unary_unary_rpc_method_handler(lambda request, context: b"any")


@pytest.mark.parametrize("factory", [pygrpc_native.server, grpc.server])
def test_registered_streaming_methods_come_before_generic_ones(
    factory: Callable,
) -> None:
    server = factory(futures.ThreadPoolExecutor(4))
    server.add_registered_method_handlers(
        "r.S",
        {
            "Up": grpc.stream_unary_rpc_method_handler(
                lambda requests, _: b"".join(requests)
            )
        },
    )
    server.add_generic_rpc_handlers([_CatchAll()])
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    try:
        with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
            up = channel.stream_unary("/r.S/Up")(iter([b"a", b"b"]), timeout=5)
            other = channel.unary_unary("/r.S/Other")(b"x", timeout=5)
    finally:
        server.stop(None)
    assert (up, other) == (b"ab", b"any")
