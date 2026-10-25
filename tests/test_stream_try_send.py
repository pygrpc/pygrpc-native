"""stream_try_send never blocks and reports why a message wasn't queued."""

import threading
import time

import grpc

from pygrpc_native import (
    EVENT_CALL,
    STREAM_CLOSED,
    STREAM_FULL,
    STREAM_SENT,
    NativeGrpcFront,
)

CHUNK = b"x" * (1 << 20)


def _call(front: NativeGrpcFront) -> int:
    (event,) = front.recv_batch(1, 5000)
    assert event[0] == EVENT_CALL
    return event[1]


def test_backed_up_stream_reports_full_then_resumes() -> None:
    front = NativeGrpcFront("127.0.0.1:0", [("/t.S/S", True)])
    front.start()
    channel = grpc.insecure_channel(
        f"127.0.0.1:{front.bound_port}",
        options=[("grpc.max_receive_message_length", -1)],
    )
    try:
        responses = channel.unary_stream("/t.S/S")(b"", timeout=30)
        call_id = _call(front)
        sent = 0
        start = time.monotonic()
        while (result := front.stream_try_send(call_id, CHUNK)) == STREAM_SENT:
            sent += 1
            assert sent < 64, "the client never pushed back"
        assert result == STREAM_FULL
        assert time.monotonic() - start < 5

        received: list[bytes] = []
        reader = threading.Thread(target=lambda: received.extend(responses))
        reader.start()
        deadline = time.monotonic() + 10
        while front.stream_try_send(call_id, CHUNK) != STREAM_SENT:
            assert time.monotonic() < deadline
            time.sleep(0.01)
        front.stream_end(call_id)
        reader.join(10)
        assert len(received) == sent + 1
    finally:
        channel.close()
        front.close()


def test_closed_streams() -> None:
    front = NativeGrpcFront("127.0.0.1:0", [("/t.S/S", True)])
    front.start()
    try:
        assert front.stream_try_send(12345, b"x") == STREAM_CLOSED
        with grpc.insecure_channel(f"127.0.0.1:{front.bound_port}") as channel:
            responses = channel.unary_stream("/t.S/S")(b"", timeout=30)
            call_id = _call(front)
            assert front.stream_try_send(call_id, b"a") == STREAM_SENT
            assert next(responses) == b"a"
            responses.cancel()
            deadline = time.monotonic() + 10
            while front.stream_try_send(call_id, b"b") != STREAM_CLOSED:
                assert time.monotonic() < deadline
                time.sleep(0.01)
    finally:
        front.close()
