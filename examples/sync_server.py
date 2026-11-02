"""Serve the example greeting with pygrpc_native.server()."""

from concurrent.futures import ThreadPoolExecutor

import grpc
import pygrpc_native

from examples.greeter import greeting, make_handler


def greet(name: bytes, context: grpc.ServicerContext) -> bytes:
    return greeting(name)


def make_server(address: str = "127.0.0.1:50051"):
    server = pygrpc_native.server(
        ThreadPoolExecutor(max_workers=4), handlers=[make_handler(greet)]
    )
    port = server.add_insecure_port(address)
    return server, port


def main() -> None:
    server, port = make_server()
    server.start()
    print(f"Sync server listening on 127.0.0.1:{port}", flush=True)
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        pass
    finally:
        server.stop(None)


if __name__ == "__main__":
    main()
