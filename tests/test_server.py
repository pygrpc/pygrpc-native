"""pygrpc_native.server() against grpc.server(), with the same generated code."""

import importlib
import sys
import threading
import time
from collections.abc import Callable, Iterator
from concurrent import futures
from typing import Any, cast

import grpc
import pytest

import pygrpc_native

grpc_tools = pytest.importorskip("grpc_tools.protoc")

PROTO = """
syntax = "proto3";
package t;
service S {
  rpc Unary(Msg) returns (Msg);
  rpc Stream(Msg) returns (stream Msg);
}
message Msg { string s = 1; }
"""


@pytest.fixture(scope="module")
def pb(tmp_path_factory: pytest.TempPathFactory) -> tuple[Any, Any]:
    out = tmp_path_factory.mktemp("proto")
    (out / "t.proto").write_text(PROTO)
    assert (
        grpc_tools.main(
            [
                "",
                f"-I{out}",
                f"--python_out={out}",
                f"--grpc_python_out={out}",
                str(out / "t.proto"),
            ]
        )
        == 0
    )
    sys.path.insert(0, str(out))
    try:
        return importlib.import_module("t_pb2"), importlib.import_module("t_pb2_grpc")
    finally:
        sys.path.remove(str(out))


Behavior = Callable[[Any, grpc.ServicerContext], Any]
SERVERS = {"grpcio": grpc.server, "native": pygrpc_native.server}


def _serve(
    pb: tuple[Any, Any],
    kind: str,
    unary: Behavior | None = None,
    stream: Behavior | None = None,
    **kwargs: Any,
) -> tuple[grpc.Server, int]:
    t_pb2, t_pb2_grpc = pb

    class Servicer(t_pb2_grpc.SServicer):
        def Unary(self, request: Any, context: grpc.ServicerContext) -> Any:
            return unary(request, context) if unary else request

        def Stream(self, request: Any, context: grpc.ServicerContext) -> Any:
            return stream(request, context) if stream else iter([request])

    server = SERVERS[kind](futures.ThreadPoolExecutor(8), **kwargs)
    t_pb2_grpc.add_SServicer_to_server(Servicer(), server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    return server, port


def _outcome(
    error: grpc.RpcError,
) -> tuple[grpc.StatusCode, str, list[tuple[str, Any]]]:
    call = cast(grpc.Call, error)
    return (
        call.code(),
        call.details(),
        [(m.key, m.value) for m in call.trailing_metadata() or ()],
    )


@pytest.fixture(params=["grpcio", "native"])
def kind(request: pytest.FixtureRequest) -> str:
    return request.param


def _unary(
    pb: tuple[Any, Any],
    kind: str,
    behavior: Behavior,
    request: str = "x",
    channel_options: list[tuple[str, Any]] | None = None,
    **kwargs: Any,
) -> Any:
    t_pb2, t_pb2_grpc = pb
    server, port = _serve(pb, kind, unary=behavior, **kwargs)
    try:
        with grpc.insecure_channel(
            f"127.0.0.1:{port}", options=channel_options
        ) as channel:
            stub = t_pb2_grpc.SStub(channel)
            try:
                response, call = stub.Unary.with_call(
                    t_pb2.Msg(s=request), timeout=5, metadata=[("x-in", "1")]
                )
                return (
                    "OK",
                    response.s,
                    [
                        (m.key, m.value)
                        for m in call.initial_metadata()
                        if m.key.startswith("x-")
                    ],
                    [
                        (m.key, m.value)
                        for m in call.trailing_metadata()
                        if m.key.startswith("x-")
                    ],
                )
            except grpc.RpcError as error:
                return _outcome(error)
    finally:
        server.stop(None)


def _stream(pb: tuple[Any, Any], kind: str, behavior: Behavior) -> Any:
    t_pb2, t_pb2_grpc = pb
    server, port = _serve(pb, kind, stream=behavior)
    try:
        with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
            call = t_pb2_grpc.SStub(channel).Stream(t_pb2.Msg(s="x"), timeout=5)
            received = []
            try:
                for message in call:
                    received.append(message.s)
                return (
                    "OK",
                    received,
                    [
                        (m.key, m.value)
                        for m in call.trailing_metadata()
                        if m.key.startswith("x-")
                    ],
                )
            except grpc.RpcError as error:
                return received, *_outcome(error)
    finally:
        server.stop(None)


def _abort_with_status(context: grpc.ServicerContext) -> None:
    class Status(grpc.Status):
        code = grpc.StatusCode.UNAVAILABLE
        details = "busy"
        trailing_metadata = (("x-t", "1"), ("grpc-status-details-bin", b"\x01"))

    context.abort_with_status(Status())


def _set_code(
    context: grpc.ServicerContext, code: grpc.StatusCode, details: str
) -> None:
    context.set_code(code)
    context.set_details(details)


def _metadata(request: Any, context: grpc.ServicerContext) -> Any:
    context.send_initial_metadata((("x-init", "1"),))
    context.set_trailing_metadata((("x-trail", "2"), ("x-trail-bin", b"\x00")))
    metadata = [
        (m.key, m.value)
        for m in context.invocation_metadata()
        if m.key.startswith("x-")
    ]
    request.s = repr(metadata)
    return request


UNARY_CASES: dict[str, Behavior] = {
    "echo": lambda request, context: request,
    "metadata": _metadata,
    "raise": lambda request, context: (_ for _ in ()).throw(ValueError("boom")),
    "abort": lambda request, context: context.abort(
        grpc.StatusCode.PERMISSION_DENIED, "no"
    ),
    "abort_ok": lambda request, context: context.abort(grpc.StatusCode.OK, "ok?"),
    "abort_with_status": lambda request, context: _abort_with_status(context),
    "set_code_and_return": lambda request, context: (
        _set_code(context, grpc.StatusCode.NOT_FOUND, "nf"),
        request,
    )[1],
    "set_code_then_raise": lambda request, context: (
        _set_code(context, grpc.StatusCode.ABORTED, "a"),
        1 / 0,
    ),
    "return_none": lambda request, context: None,
    "cancel": lambda request, context: (context.cancel(), request)[1],
}


@pytest.mark.parametrize("case", UNARY_CASES)
def test_unary_matches_grpcio(pb: tuple[Any, Any], case: str) -> None:
    expected = _unary(pb, "grpcio", UNARY_CASES[case])
    assert _unary(pb, "native", UNARY_CASES[case]) == expected


def _stream_raise(request: Any, context: grpc.ServicerContext) -> Iterator[Any]:
    yield request
    raise RuntimeError("mid")


def _stream_set_code(request: Any, context: grpc.ServicerContext) -> Iterator[Any]:
    yield request
    _set_code(context, grpc.StatusCode.DATA_LOSS, "dl")


def _stream_metadata(request: Any, context: grpc.ServicerContext) -> Iterator[Any]:
    context.set_trailing_metadata((("x-trail", "1"),))
    yield request
    yield request


def _stream_abort_first(request: Any, context: grpc.ServicerContext) -> Iterator[Any]:
    context.abort(grpc.StatusCode.NOT_FOUND, "nothing")
    yield request


STREAM_CASES: dict[str, Behavior] = {
    "two": lambda request, context: iter([request, request]),
    "empty": lambda request, context: iter([]),
    "raise": _stream_raise,
    "set_code": _stream_set_code,
    "metadata": _stream_metadata,
    "abort_first": _stream_abort_first,
}


@pytest.mark.parametrize("case", STREAM_CASES)
def test_stream_matches_grpcio(pb: tuple[Any, Any], case: str) -> None:
    expected = _stream(pb, "grpcio", STREAM_CASES[case])
    assert _stream(pb, "native", STREAM_CASES[case]) == expected


def test_message_limits_match_grpcio(pb: tuple[Any, Any]) -> None:
    echo = UNARY_CASES["echo"]
    options = [("grpc.max_receive_message_length", 10)]
    assert _unary(pb, "native", echo, "x" * 20, options=options) == _unary(
        pb, "grpcio", echo, "x" * 20, options=options
    )
    big = "x" * (4 * 1024 * 1024 + 1)
    assert (
        _unary(pb, "native", echo, big)[0]
        == _unary(pb, "grpcio", echo, big)[0]
        == grpc.StatusCode.RESOURCE_EXHAUSTED
    )
    unlimited = [("grpc.max_receive_message_length", -1)]
    client = [("grpc.max_receive_message_length", -1)]
    assert (
        _unary(pb, "native", echo, big, channel_options=client, options=unlimited)[0]
        == "OK"
    )


def test_context_details(pb: tuple[Any, Any], kind: str) -> None:
    seen: dict[str, Any] = {}

    def look(request: Any, context: grpc.ServicerContext) -> Any:
        seen["active"] = context.is_active()
        seen["remaining"] = context.time_remaining()
        seen["peer"] = context.peer()
        seen["auth"] = context.auth_context()
        seen["identities"] = context.peer_identities()
        seen["code"] = context.code()
        return request

    assert _unary(pb, kind, look)[0] == "OK"
    assert seen["active"] is True
    assert 0 < seen["remaining"] <= 5.5
    assert seen["peer"].startswith("ipv4:127.0.0.1:")
    assert seen["auth"] == {
        "transport_security_type": [b"insecure"],
        "security_level": [b"TSI_SECURITY_NONE"],
    }
    assert seen["identities"] is None and seen["code"] is None


def test_client_cancel_runs_callbacks(pb: tuple[Any, Any], kind: str) -> None:
    t_pb2, t_pb2_grpc = pb
    ran = threading.Event()
    seen: dict[str, Any] = {}

    def slow(request: Any, context: grpc.ServicerContext) -> Any:
        seen["registered"] = context.add_callback(ran.set)
        time.sleep(0.6)
        seen["active"] = context.is_active()
        seen["late"] = context.add_callback(lambda: None)
        return request

    server, port = _serve(pb, kind, unary=slow)
    try:
        with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
            future = t_pb2_grpc.SStub(channel).Unary.future(
                t_pb2.Msg(s="x"), timeout=10
            )
            time.sleep(0.2)
            future.cancel()
            assert ran.wait(5)
            time.sleep(0.6)
    finally:
        server.stop(None)
    assert seen == {"registered": True, "active": False, "late": False}


def test_initial_metadata_twice_is_rejected(pb: tuple[Any, Any], kind: str) -> None:
    errors = []

    def twice(request: Any, context: grpc.ServicerContext) -> Any:
        context.send_initial_metadata((("x-a", "1"),))
        try:
            context.send_initial_metadata((("x-b", "2"),))
        except ValueError as exc:
            errors.append(str(exc))
        return request

    assert _unary(pb, kind, twice)[0] == "OK"
    assert errors == ["Initial metadata no longer allowed!"]


def test_lifecycle(pb: tuple[Any, Any]) -> None:
    server, port = _serve(pb, "native")
    assert isinstance(server, grpc.Server) and port > 0
    with pytest.raises(ValueError, match="already-started"):
        server.start()
    assert server.wait_for_termination(0.05) is True
    event = server.stop(1)
    assert isinstance(event, threading.Event) and event.wait(5)
    assert server.wait_for_termination(0.05) is False


def test_stop_with_grace_finishes_running_calls(pb: tuple[Any, Any]) -> None:
    t_pb2, t_pb2_grpc = pb
    started = threading.Event()

    def slow(request: Any, context: grpc.ServicerContext) -> Any:
        started.set()
        time.sleep(0.5)
        return request

    server, port = _serve(pb, "native", unary=slow)
    with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
        future = t_pb2_grpc.SStub(channel).Unary.future(t_pb2.Msg(s="x"), timeout=10)
        assert started.wait(5)
        done = server.stop(5)
        assert future.result().s == "x"
        assert done.wait(5)


def test_unsupported_features_are_rejected(pb: tuple[Any, Any]) -> None:
    pool = futures.ThreadPoolExecutor(1)
    with pytest.raises(ValueError, match="unsupported server option"):
        pygrpc_native.server(pool, options=[("grpc.max_connection_idle_ms", 1000)])
    server = pygrpc_native.server(pool)
    with pytest.raises(NotImplementedError):
        server.add_secure_port("127.0.0.1:0", object())  # type: ignore[arg-type]
    server.add_insecure_port("127.0.0.1:0")
    with pytest.raises(RuntimeError, match="one port"):
        server.add_insecure_port("127.0.0.1:0")
    server.stop(None)


def test_frequent_client_pings_are_allowed(pb: tuple[Any, Any]) -> None:
    t_pb2, t_pb2_grpc = pb

    def slow(request: Any, context: grpc.ServicerContext) -> Any:
        time.sleep(3)
        return request

    options = [
        ("grpc.http2.max_ping_strikes", 0),
        ("grpc.keepalive_permit_without_calls", 1),
    ]
    server, port = _serve(pb, "native", unary=slow, options=options)
    client_options = [
        ("grpc.keepalive_time_ms", 500),
        ("grpc.keepalive_permit_without_calls", 1),
        ("grpc.http2.max_pings_without_data", 0),
    ]
    try:
        with grpc.insecure_channel(
            f"127.0.0.1:{port}", options=client_options
        ) as channel:
            assert (
                t_pb2_grpc.SStub(channel).Unary(t_pb2.Msg(s="x"), timeout=10).s == "x"
            )
    finally:
        server.stop(None)


def test_rejected_call_does_not_hold_up_stop(pb: tuple[Any, Any]) -> None:
    t_pb2, t_pb2_grpc = pb
    pool = futures.ThreadPoolExecutor(1)
    server = pygrpc_native.server(pool)
    t_pb2_grpc.add_SServicer_to_server(t_pb2_grpc.SServicer(), server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    pool.shutdown()
    with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
        with pytest.raises(grpc.RpcError) as info:
            t_pb2_grpc.SStub(channel).Unary(t_pb2.Msg(s="x"), timeout=5)
    assert cast(grpc.Call, info.value).code() is grpc.StatusCode.INTERNAL
    start = time.monotonic()
    assert server.stop(5).wait(10)
    assert time.monotonic() - start < 1


def test_a_client_that_stops_reading_stops_the_handler() -> None:
    produced = 0

    def endless(request: bytes, context: Any) -> Iterator[bytes]:
        nonlocal produced
        while context.is_active():
            produced += 1
            yield b"x" * (64 << 10)

    server = pygrpc_native.server(futures.ThreadPoolExecutor(2))
    server.add_registered_method_handlers(
        "t.S", {"S": grpc.unary_stream_rpc_method_handler(endless)}
    )
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    try:
        with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
            call = channel.unary_stream("/t.S/S")(b"", timeout=10)
            next(call)
            time.sleep(0.6)
            stalled = produced
            time.sleep(0.6)
            assert produced == stalled
            call.cancel()
    finally:
        server.stop(None)
