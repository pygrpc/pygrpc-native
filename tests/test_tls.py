"""TLS on `server()` and `aio.server()`, with grpcio's server as the reference."""

import asyncio
from collections.abc import AsyncIterator, Iterator
from concurrent import futures
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import grpc
import pytest
from grpc import aio

import pygrpc_native
import pygrpc_native.aio

CERTS = Path(__file__).parent / "tls_certs"


def pem(name: str) -> bytes:
    return (CERTS / name).read_bytes()


def ident(data: bytes) -> bytes:
    return data


def handlers() -> dict[str, grpc.RpcMethodHandler]:
    def unary(request: bytes, context: Any) -> bytes:
        return request

    def stream(request: bytes, context: Any) -> Iterator[bytes]:
        yield request
        yield request

    def bidi(requests: Iterator[bytes], context: Any) -> Iterator[bytes]:
        yield from requests

    return {
        "U": grpc.unary_unary_rpc_method_handler(unary, ident, ident),
        "S": grpc.unary_stream_rpc_method_handler(stream, ident, ident),
        "B": grpc.stream_stream_rpc_method_handler(bidi, ident, ident),
    }


def credentials(root: bytes | None = None, require: bool = False) -> Any:
    return pygrpc_native.ssl_server_credentials(
        [(pem("server.key"), pem("server.pem"))], root, require
    )


@contextmanager
def serving(kind: str, server_credentials: Any) -> Iterator[int]:
    with futures.ThreadPoolExecutor(8) as pool:
        make = grpc.server if kind == "grpcio" else pygrpc_native.server
        server = make(pool)
        server.add_registered_method_handlers("t.S", handlers())
        port = server.add_secure_port("localhost:0", server_credentials)
        server.start()
        try:
            yield port
        finally:
            server.stop(None)


def client(port: int, key: str | None = None, cert: str | None = None) -> grpc.Channel:
    return grpc.secure_channel(
        f"localhost:{port}",
        grpc.ssl_channel_credentials(
            pem("ca.pem"),
            pem(key) if key else None,
            pem(cert) if cert else None,
        ),
    )


@pytest.fixture(params=["grpcio", "native"])
def kind(request: pytest.FixtureRequest) -> str:
    return request.param


def test_calls_over_tls(kind: str) -> None:
    with serving(kind, credentials()) as port, client(port) as channel:
        assert channel.unary_unary("/t.S/U", ident, ident)(b"a", timeout=5) == b"a"
        stream = channel.unary_stream("/t.S/S", ident, ident)(b"b", timeout=5)
        assert list(stream) == [b"b", b"b"]
        # Served by the fallback behind the front.
        bidi = channel.stream_stream("/t.S/B", ident, ident)(iter([b"c"]), timeout=5)
        assert list(bidi) == [b"c"]


@pytest.mark.parametrize(
    ("require", "key", "cert", "accepted"),
    [
        (True, "client.key", "client.pem", True),
        (True, None, None, False),
        (True, "other.key", "other.pem", False),
        (False, "client.key", "client.pem", True),
        (False, "other.key", "other.pem", True),
        (False, None, None, True),
    ],
)
def test_client_certificates(
    kind: str, require: bool, key: str | None, cert: str | None, accepted: bool
) -> None:
    server_credentials = credentials(pem("ca.pem"), require)
    with serving(kind, server_credentials) as port, client(port, key, cert) as channel:
        call = channel.unary_unary("/t.S/U", ident, ident)
        if accepted:
            assert call(b"a", timeout=5) == b"a"
        else:
            with pytest.raises(grpc.RpcError) as failure:
                call(b"a", timeout=5)
            assert failure.value.code() == grpc.StatusCode.UNAVAILABLE


def test_plaintext_client_is_refused(kind: str) -> None:
    with (
        serving(kind, credentials()) as port,
        grpc.insecure_channel(f"localhost:{port}") as channel,
    ):
        with pytest.raises(grpc.RpcError) as failure:
            channel.unary_unary("/t.S/U", ident, ident)(b"a", timeout=5)
        assert failure.value.code() == grpc.StatusCode.UNAVAILABLE


def test_aio_server_over_tls() -> None:
    async def unary(request: bytes, context: Any) -> bytes:
        return request

    async def stream(request: bytes, context: Any) -> AsyncIterator[bytes]:
        yield request

    async def run() -> tuple[bytes, list[bytes]]:
        server = pygrpc_native.aio.server()
        server.add_registered_method_handlers(
            "t.S",
            {
                "U": grpc.unary_unary_rpc_method_handler(unary, ident, ident),
                "S": grpc.unary_stream_rpc_method_handler(stream, ident, ident),
            },
        )
        port = server.add_secure_port("localhost:0", credentials())
        await server.start()
        try:
            async with aio.secure_channel(
                f"localhost:{port}", grpc.ssl_channel_credentials(pem("ca.pem"))
            ) as channel:
                answer = await channel.unary_unary("/t.S/U", ident, ident)(b"a")
                call = channel.unary_stream("/t.S/S", ident, ident)(b"b")
                return answer, [message async for message in call]
        finally:
            await server.stop(None)

    assert asyncio.run(run()) == (b"a", [b"b"])


def test_credentials_the_front_cant_use() -> None:
    server = pygrpc_native.server(futures.ThreadPoolExecutor(1))
    plain = grpc.ssl_server_credentials([(pem("server.key"), pem("server.pem"))])
    with pytest.raises(NotImplementedError, match="ssl_server_credentials"):
        server.add_secure_port("localhost:0", plain)
    bad = pygrpc_native.ssl_server_credentials([(b"not a key", pem("server.pem"))])
    with pytest.raises(ValueError, match="invalid tls"):
        server.add_secure_port("localhost:0", bad)
