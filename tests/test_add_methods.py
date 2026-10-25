"""Methods registered after the port is bound."""

import threading

import grpc
import pytest

from pygrpc_native import EVENT_CALL, NativeGrpcFront


def test_methods_added_before_start_are_served() -> None:
    front = NativeGrpcFront("127.0.0.1:0", [("/t.S/A", False)])
    port = front.bound_port
    assert front.add_methods([("/t.S/B", False), ("/t.S/C", True)]) == [1, 2]
    assert front.add_methods([("/t.S/D", False)]) == [3]
    front.start()
    seen: list[int] = []

    def serve() -> None:
        while (batch := front.recv_batch(16, 100)) is not None:
            for kind, call_id, method_id, payload, _ in batch:
                if kind != EVENT_CALL:
                    continue
                seen.append(method_id)
                if method_id == 2:
                    front.stream_send(call_id, payload)
                    front.stream_end(call_id)
                else:
                    front.respond(call_id, payload)

    thread = threading.Thread(target=serve)
    thread.start()
    try:
        with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
            assert channel.unary_unary("/t.S/A")(b"a", timeout=5) == b"a"
            assert channel.unary_unary("/t.S/B")(b"b", timeout=5) == b"b"
            assert list(channel.unary_stream("/t.S/C")(b"c", timeout=5)) == [b"c"]
            assert channel.unary_unary("/t.S/D")(b"d", timeout=5) == b"d"
    finally:
        front.close()
        thread.join(10)
    assert seen == [0, 1, 2, 3]


def test_starting_with_no_methods_then_adding() -> None:
    front = NativeGrpcFront("127.0.0.1:0", [])
    try:
        assert front.add_methods([("/t.S/A", False)]) == [0]
        front.start()
        with grpc.insecure_channel(f"127.0.0.1:{front.bound_port}") as channel:
            future = channel.unary_unary("/t.S/A").future(b"x", timeout=5)
            (event,) = front.recv_batch(1, 5000)
            front.respond(event[1], event[3])
            assert future.result() == b"x"
    finally:
        front.close()


def test_duplicate_path_is_rejected() -> None:
    front = NativeGrpcFront("127.0.0.1:0", [("/t.S/A", False)])
    try:
        with pytest.raises(ValueError, match="already registered"):
            front.add_methods([("/t.S/A", True)])
        assert front.add_methods([("/t.S/B", False)]) == [1]
    finally:
        front.close()


def test_rejected_after_start_or_close() -> None:
    front = NativeGrpcFront("127.0.0.1:0", [])
    front.start()
    with pytest.raises(RuntimeError, match="already started"):
        front.add_methods([("/t.S/A", False)])
    front.close()
    with pytest.raises(RuntimeError, match="closed"):
        front.add_methods([("/t.S/A", False)])
