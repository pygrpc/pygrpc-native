"""Methods the native front doesn't serve go to a grpcio server behind it."""

import importlib
import sys
from collections.abc import Iterator
from concurrent import futures
from typing import Any, cast

import grpc
import pytest

import pygrpc_native

grpc_tools = pytest.importorskip("grpc_tools.protoc")

PROTO = """
syntax = "proto3";
package f;
service S {
  rpc Unary(Msg) returns (Msg);
  rpc Up(stream Msg) returns (Msg);
  rpc Chat(stream Msg) returns (stream Msg);
}
message Msg { string s = 1; }
"""


@pytest.fixture(scope="module")
def pb(tmp_path_factory: pytest.TempPathFactory) -> tuple[Any, Any]:
    out = tmp_path_factory.mktemp("proto")
    (out / "f.proto").write_text(PROTO)
    assert (
        grpc_tools.main(
            [
                "",
                f"-I{out}",
                f"--python_out={out}",
                f"--grpc_python_out={out}",
                str(out / "f.proto"),
            ]
        )
        == 0
    )
    sys.path.insert(0, str(out))
    try:
        return importlib.import_module("f_pb2"), importlib.import_module("f_pb2_grpc")
    finally:
        sys.path.remove(str(out))


def _servicer(pb: tuple[Any, Any]) -> Any:
    f_pb2, f_pb2_grpc = pb

    class Servicer(f_pb2_grpc.SServicer):
        def Unary(self, request: Any, context: grpc.ServicerContext) -> Any:
            return f_pb2.Msg(s=context.peer().split(":")[0])

        def Up(self, requests: Iterator[Any], context: grpc.ServicerContext) -> Any:
            return f_pb2.Msg(s="".join(r.s for r in requests))

        def Chat(
            self, requests: Iterator[Any], context: grpc.ServicerContext
        ) -> Iterator[Any]:
            for request in requests:
                if request.s == "stop":
                    context.abort(grpc.StatusCode.FAILED_PRECONDITION, "stopped")
                yield f_pb2.Msg(s=request.s.upper())

    return Servicer()


def test_streaming_requests_go_to_grpcio(pb: tuple[Any, Any]) -> None:
    f_pb2, f_pb2_grpc = pb
    server = pygrpc_native.server(futures.ThreadPoolExecutor(4))
    f_pb2_grpc.add_SServicer_to_server(_servicer(pb), server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    try:
        with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
            stub = f_pb2_grpc.SStub(channel)
            assert stub.Unary(f_pb2.Msg(), timeout=5).s == "ipv4"
            up = stub.Up(iter([f_pb2.Msg(s="a"), f_pb2.Msg(s="b")]), timeout=5)
            assert up.s == "ab"
            chat = stub.Chat(iter([f_pb2.Msg(s="x"), f_pb2.Msg(s="y")]), timeout=5)
            assert [m.s for m in chat] == ["X", "Y"]
            chat = stub.Chat(iter([f_pb2.Msg(s="x"), f_pb2.Msg(s="stop")]), timeout=5)
            with pytest.raises(grpc.RpcError) as info:
                list(chat)
            error = cast(grpc.Call, info.value)
            assert error.code() is grpc.StatusCode.FAILED_PRECONDITION
            assert error.details() == "stopped"
    finally:
        assert server.stop(1).wait(5)


def test_reflection_and_health(pb: tuple[Any, Any]) -> None:
    reflection = pytest.importorskip("grpc_reflection.v1alpha.reflection")
    reflection_pb2 = pytest.importorskip("grpc_reflection.v1alpha.reflection_pb2")
    reflection_pb2_grpc = pytest.importorskip(
        "grpc_reflection.v1alpha.reflection_pb2_grpc"
    )
    health = pytest.importorskip("grpc_health.v1.health")
    health_pb2 = pytest.importorskip("grpc_health.v1.health_pb2")
    health_pb2_grpc = pytest.importorskip("grpc_health.v1.health_pb2_grpc")
    f_pb2, f_pb2_grpc = pb

    server = pygrpc_native.server(futures.ThreadPoolExecutor(4))
    f_pb2_grpc.add_SServicer_to_server(_servicer(pb), server)
    health_pb2_grpc.add_HealthServicer_to_server(health.HealthServicer(), server)
    names = ("f.S", "grpc.health.v1.Health", reflection.SERVICE_NAME)
    reflection.enable_server_reflection(names, server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    try:
        with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
            check = health_pb2_grpc.HealthStub(channel).Check(
                health_pb2.HealthCheckRequest(), timeout=5
            )
            assert check.status == health_pb2.HealthCheckResponse.SERVING
            stub = reflection_pb2_grpc.ServerReflectionStub(channel)
            request = reflection_pb2.ServerReflectionRequest(list_services="")
            (response,) = stub.ServerReflectionInfo(iter([request]), timeout=5)
            listed = {s.name for s in response.list_services_response.service}
            assert listed == set(names)
    finally:
        server.stop(None)
