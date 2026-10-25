"""delay_stream_headers holds a stream's headers back the way grpcio does."""

import threading
from collections.abc import Iterator
from contextlib import contextmanager
from typing import cast

import grpc
import pytest

from pygrpc_native import EVENT_CALL, EVENT_CANCEL, NativeGrpcFront


@contextmanager
def front(delay: bool = True) -> Iterator[tuple[NativeGrpcFront, grpc.Channel]]:
    server = NativeGrpcFront(
        "127.0.0.1:0", [("/t.S/S", True)], delay_stream_headers=delay
    )
    server.start()
    channel = grpc.insecure_channel(f"127.0.0.1:{server.bound_port}")
    try:
        yield server, channel
    finally:
        channel.close()
        server.close()


def _next_event(server: NativeGrpcFront, kind: int = EVENT_CALL) -> int:
    for _ in range(50):
        for event in server.recv_batch(16, 100) or []:
            if event[0] == kind:
                return event[1]
    raise AssertionError("no event")


def _headers_arrived(call: grpc.Call, wait: float) -> bool:
    done = threading.Event()
    threading.Thread(
        target=lambda: (call.initial_metadata(), done.set()), daemon=True
    ).start()
    return done.wait(wait)


def test_headers_wait_for_the_first_message() -> None:
    with front() as (server, channel):
        call = channel.unary_stream("/t.S/S")(b"", timeout=10)
        call_id = _next_event(server)
        assert not _headers_arrived(call, 0.3)
        assert server.stream_send(call_id, b"a")
        assert _headers_arrived(call, 5)
        server.stream_end(call_id)
        assert list(call) == [b"a"]


def test_headers_go_out_at_once_by_default() -> None:
    with front(delay=False) as (server, channel):
        call = channel.unary_stream("/t.S/S")(b"", timeout=10)
        call_id = _next_event(server)
        assert _headers_arrived(call, 5)
        assert server.stream_send_initial_metadata(call_id, [("x-a", "1")]) is False
        server.stream_end(call_id)


def test_initial_metadata_releases_the_headers() -> None:
    with front() as (server, channel):
        call = channel.unary_stream("/t.S/S")(b"", timeout=10)
        call_id = _next_event(server)
        assert server.stream_send_initial_metadata(call_id, [("x-a", "1")]) is True
        assert _headers_arrived(call, 5)
        assert ("x-a", "1") in [(m.key, m.value) for m in call.initial_metadata()]
        assert server.stream_send_initial_metadata(call_id, [("x-b", "2")]) is False
        server.stream_send(call_id, b"a")
        server.stream_end(call_id)
        assert list(call) == [b"a"]


def test_ending_before_any_message() -> None:
    with front() as (server, channel):
        ok = channel.unary_stream("/t.S/S")(b"", timeout=10)
        server.stream_end(_next_event(server), trailing_metadata=[("x-t", "1")])
        assert list(ok) == []
        assert ok.code() is grpc.StatusCode.OK
        assert ("x-t", "1") in [(m.key, m.value) for m in ok.trailing_metadata()]

        failed = channel.unary_stream("/t.S/S")(b"", timeout=10)
        server.fail(_next_event(server), grpc.StatusCode.NOT_FOUND.value[0], "gone")
        with pytest.raises(grpc.RpcError) as info:
            list(failed)
        assert cast(grpc.Call, info.value).code() is grpc.StatusCode.NOT_FOUND


def test_deadline_and_close_while_waiting() -> None:
    with front() as (server, channel):
        call = channel.unary_stream("/t.S/S")(b"", timeout=0.3)
        _next_event(server)
        with pytest.raises(grpc.RpcError) as info:
            list(call)
        assert cast(grpc.Call, info.value).code() is grpc.StatusCode.DEADLINE_EXCEEDED
        _next_event(server, EVENT_CANCEL)

        call = channel.unary_stream("/t.S/S")(b"", timeout=10)
        _next_event(server)
        server.close()
        with pytest.raises(grpc.RpcError) as info:
            list(call)
        assert cast(grpc.Call, info.value).code() is grpc.StatusCode.UNAVAILABLE
