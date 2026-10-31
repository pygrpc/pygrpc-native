"""Serves the interop TestService on grpcio or pygrpc-native.

python interop/server.py --impl grpcio|native --mode sync|aio --port PORT
"""

import argparse
import asyncio
from concurrent import futures

import grpc
from grpc import aio

import _protos

_protos.load()

from src.proto.grpc.testing import test_pb2_grpc  # noqa: E402

from service import AsyncTestService, TestService  # noqa: E402


def _make_server(impl: str, mode: str):
    if impl == "grpcio":
        return (
            grpc.server(futures.ThreadPoolExecutor(max_workers=10))
            if mode == "sync"
            else aio.server()
        )
    import pygrpc_native
    import pygrpc_native.aio

    if mode == "sync":
        return pygrpc_native.server(futures.ThreadPoolExecutor(max_workers=10))
    return pygrpc_native.aio.server()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--impl", choices=["grpcio", "native"], required=True)
    parser.add_argument("--mode", choices=["sync", "aio"], required=True)
    parser.add_argument("--port", type=int, required=True)
    args = parser.parse_args()

    def build():
        server = _make_server(args.impl, args.mode)
        service = TestService() if args.mode == "sync" else AsyncTestService()
        test_pb2_grpc.add_TestServiceServicer_to_server(service, server)
        server.add_insecure_port(f"127.0.0.1:{args.port}")
        return server

    if args.mode == "sync":
        server = build()
        server.start()
        server.wait_for_termination()
        return

    async def serve() -> None:
        server = build()
        await server.start()
        await server.wait_for_termination()

    asyncio.run(serve())


if __name__ == "__main__":
    main()
