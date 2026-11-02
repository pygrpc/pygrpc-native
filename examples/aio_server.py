"""Serve the example greeting with pygrpc_native.aio.server()."""

import asyncio

import grpc
import pygrpc_native.aio

from examples.greeter import greeting, make_handler


async def greet(name: bytes, context: grpc.aio.ServicerContext) -> bytes:
    return greeting(name)


def make_server(address: str = "127.0.0.1:50051"):
    server = pygrpc_native.aio.server(handlers=[make_handler(greet)])
    port = server.add_insecure_port(address)
    return server, port


async def main() -> None:
    server, port = make_server()
    await server.start()
    print(f"Asyncio server listening on 127.0.0.1:{port}", flush=True)
    try:
        await server.wait_for_termination()
    finally:
        await server.stop(None)


if __name__ == "__main__":
    asyncio.run(main())
