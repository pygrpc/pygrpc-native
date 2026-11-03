"""The runnable examples accept a plain grpcio client."""

import asyncio

import grpc

from examples.aio_server import make_server as make_aio_server
from examples.sync_server import make_server as make_sync_server

METHOD_NAME = "/examples.Greeter/Greet"


def _call_grpcio_client(port: int) -> str:
    with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
        greet = channel.unary_unary(
            METHOD_NAME,
            request_serializer=str.encode,
            response_deserializer=bytes.decode,
        )
        return greet("world", timeout=5)


def test_sync_server_accepts_grpcio_client() -> None:
    server, port = make_sync_server("127.0.0.1:0")
    server.start()
    try:
        assert _call_grpcio_client(port) == "Hello, world!"
    finally:
        server.stop(None)


def test_aio_server_accepts_grpcio_client() -> None:
    async def exercise() -> None:
        server, port = make_aio_server("127.0.0.1:0")
        await server.start()
        try:
            result = await asyncio.to_thread(_call_grpcio_client, port)
            assert result == "Hello, world!"
        finally:
            await server.stop(None)

    asyncio.run(exercise())


def test_health_server_check() -> None:
    from grpc_health.v1 import health_pb2, health_pb2_grpc

    from examples.health_server import make_server

    server, port, health_servicer = make_server("127.0.0.1:0")
    server.start()
    try:
        with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
            stub = health_pb2_grpc.HealthStub(channel)
            # Initial Check should return SERVING
            response = stub.Check(health_pb2.HealthCheckRequest(service=""), timeout=5)
            assert response.status == health_pb2.HealthCheckResponse.SERVING
            # Change to NOT_SERVING and Check again
            health_servicer.set("", health_pb2.HealthCheckResponse.NOT_SERVING)
            response = stub.Check(health_pb2.HealthCheckRequest(service=""), timeout=5)
            assert response.status == health_pb2.HealthCheckResponse.NOT_SERVING
    finally:
        server.stop(None)
