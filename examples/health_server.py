"""Serve gRPC health checks with pygrpc_native.server()."""

from concurrent.futures import ThreadPoolExecutor

import grpc
from grpc_health.v1 import health, health_pb2, health_pb2_grpc
import pygrpc_native


def make_server(address: str = "127.0.0.1:50051"):
    server = pygrpc_native.server(ThreadPoolExecutor(max_workers=4))
    health_servicer = health.HealthServicer()
    health_pb2_grpc.add_HealthServicer_to_server(health_servicer, server)
    # Set a service to SERVING by default
    health_servicer.set("", health_pb2.HealthCheckResponse.SERVING)
    port = server.add_insecure_port(address)
    return server, port, health_servicer


def main() -> None:
    server, port, _ = make_server()
    server.start()
    print(f"Health server listening on 127.0.0.1:{port}", flush=True)
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        pass
    finally:
        server.stop(None)


if __name__ == "__main__":
    main()
