"""Serve the example greeting with pygrpc_native.aio.server()."""

import asyncio

import grpc
import pygrpc_native.aio


async def greet(name: bytes, context: grpc.aio.ServicerContext) -> bytes:
    return f"Hello, {name.decode('utf-8')}!".encode("utf-8")


handler = grpc.method_handlers_generic_handler(
    "examples.Greeter",
    {"Greet": grpc.unary_unary_rpc_method_handler(greet)},
)


def make_server(address: str = "127.0.0.1:50051"):
    # was grpc.aio.server()
    server = pygrpc_native.aio.server()
    server.add_generic_rpc_handlers((handler,))
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
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
