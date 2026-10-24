"""Tests for the pygrpc_native binding."""

from typing import cast
import threading
import time

import grpc
import pytest

from pygrpc_native import NativeGrpcFront


def _code(error: BaseException) -> grpc.StatusCode:
    return cast(grpc.Call, error).code()


def test_close_reported_after_events_queued_before_it() -> None:
    """recv_batch returns the calls queued before close(), then None."""
    front = NativeGrpcFront("127.0.0.1:0", [("/t.S/Echo", False)])
    front.start()
    channel = grpc.insecure_channel(f"127.0.0.1:{front.bound_port}")
    try:
        echo = channel.unary_unary("/t.S/Echo")
        futures = [echo.future(b"x", timeout=10) for _ in range(4)]
        assert len(front.recv_batch(1, 5000)) == 1
        # Let the other calls queue up unanswered. Without the wait the
        # outcome is the same, the close just comes first.
        time.sleep(0.2)
        front.close()
        for _ in range(50):
            if front.recv_batch(64, 100) is None:
                break
        else:
            pytest.fail("recv_batch never reported the close")
        for future in futures:
            assert _code(future.exception(5)) is grpc.StatusCode.UNAVAILABLE
    finally:
        channel.close()
        front.close()


def test_close_ends_a_backed_up_stream_with_an_error() -> None:
    """A stream cut short by close() must not look complete to the client."""
    front = NativeGrpcFront("127.0.0.1:0", [("/t.S/Stream", True)])
    front.start()
    sends: list[bool] = []

    def serve() -> None:
        events = front.recv_batch(1, 5000)
        assert events
        call_id = events[0][1]
        for _ in range(200):
            sends.append(front.stream_send(call_id, b"x" * (1 << 20)))
            if not sends[-1]:
                return
        front.stream_end(call_id)

    server_thread = threading.Thread(target=serve)
    server_thread.start()
    channel = grpc.insecure_channel(
        f"127.0.0.1:{front.bound_port}",
        options=[("grpc.max_receive_message_length", -1)],
    )
    try:
        call = channel.unary_stream("/t.S/Stream")(b"", timeout=30)
        next(call)
        # The client stops reading here, so the sender backs up.
        time.sleep(0.2)
        front.close()
        with pytest.raises(grpc.RpcError) as info:
            for _ in call:
                pass
        assert _code(info.value) is grpc.StatusCode.UNAVAILABLE
        server_thread.join(10)
        assert sends[-1] is False
    finally:
        channel.close()
        front.close()


def test_start_twice_is_rejected() -> None:
    """A front starts once."""
    front = NativeGrpcFront("127.0.0.1:0", [])
    try:
        front.start()
        with pytest.raises(RuntimeError, match="already started"):
            front.start()
    finally:
        front.close()


def test_invalid_arguments_are_rejected() -> None:
    """fail() needs a non-OK code and recv_batch() a positive batch size."""
    front = NativeGrpcFront("127.0.0.1:0", [])
    try:
        with pytest.raises(ValueError):
            front.fail(1, 0, "")
        with pytest.raises(ValueError):
            front.recv_batch(0, 0)
    finally:
        front.close()
