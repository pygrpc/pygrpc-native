"""Serve the example greeting with pygrpc_native.server()."""

from concurrent.futures import ThreadPoolExecutor

import grpc
import pygrpc_native


def greet(name: bytes, context: grpc.ServicerContext) -> bytes:
    return f"Hello, {name.decode('utf-8')}!".encode("utf-8")


handler = grpc.method_handlers_generic_handler(
    "examples.Greeter",
    {"Greet": grpc.unary_unary_rpc_method_handler(greet)},
)


def make_server(address: str = "127.0.0.1:50051"):
    # was grpc.server(...)
    server = pygrpc_native.server(ThreadPoolExecutor(max_workers=4))
    server.add_generic_rpc_handlers((handler,))
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
