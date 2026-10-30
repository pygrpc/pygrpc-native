"""Tests for the pygrpc_native binding."""

from collections.abc import Iterator
from concurrent import futures
from typing import cast
import shutil
import tempfile
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


def _ordered_front(**kwargs: object) -> tuple[NativeGrpcFront, list[bytes]]:
    front = NativeGrpcFront("127.0.0.1:0", [("/t.S/U", False)], ordered=True, **kwargs)
    front.start()
    seen: list[bytes] = []

    def serve() -> None:
        while (events := front.recv_batch(64)) is not None:
            for _, call_id, _, payload, _ in events:
                seen.append(payload)
                front.respond(call_id, b"")

    threading.Thread(target=serve, daemon=True).start()
    return front, seen


def test_ordered_keeps_a_connections_calls_in_order() -> None:
    front, seen = _ordered_front(io_threads=4)
    try:
        with grpc.insecure_channel(f"127.0.0.1:{front.bound_port}") as channel:
            call = channel.unary_unary("/t.S/U")
            # Calls queued before the channel connects go out in any order.
            call(b"", timeout=5)
            for _ in range(10):
                seen.clear()
                sent = [str(i).encode() for i in range(200)]
                for future in [call.future(p, timeout=10) for p in sent]:
                    future.result()
                assert seen == sent
    finally:
        front.close()


def test_ordered_call_waits_only_while_the_one_before_is_pending() -> None:
    front, seen = _ordered_front()
    stalled = threading.Event()

    def unfinished() -> Iterator[bytes]:
        yield b"a"
        stalled.wait(10)

    try:
        with grpc.insecure_channel(f"127.0.0.1:{front.bound_port}") as channel:
            channel.unary_unary("/t.S/U")(b"", timeout=5)
            # A request whose body never ends holds up the call after it,
            # until the client gives up on it.
            first = channel.stream_unary("/t.S/U").future(unfinished(), timeout=10)
            second = channel.unary_unary("/t.S/U").future(b"b", timeout=10)
            time.sleep(0.3)
            assert not second.done()
            first.cancel()
            assert second.result(timeout=5) == b""
            stalled.set()
    finally:
        front.close()


def test_ordered_calls_do_not_wait_on_an_open_forwarded_call() -> None:
    def chat(
        requests: Iterator[bytes], context: grpc.ServicerContext
    ) -> Iterator[bytes]:
        yield from requests

    fallback = grpc.server(futures.ThreadPoolExecutor(2))
    fallback.add_generic_rpc_handlers(
        [
            grpc.method_handlers_generic_handler(
                "t.F", {"Chat": grpc.stream_stream_rpc_method_handler(chat)}
            )
        ]
    )
    directory = tempfile.mkdtemp()
    socket = f"{directory}/fallback.sock"
    fallback.add_insecure_port(f"unix:{socket}")
    fallback.start()
    front = NativeGrpcFront("127.0.0.1:0", [("/t.S/U", False)], ordered=True)
    front.set_fallback(socket)
    front.start()
    done = threading.Event()

    def serve() -> None:
        while (events := front.recv_batch(64)) is not None:
            for _, call_id, _, _, _ in events:
                front.respond(call_id, b"u")

    def held() -> Iterator[bytes]:
        yield b"a"
        done.wait(10)

    threading.Thread(target=serve, daemon=True).start()
    try:
        with grpc.insecure_channel(f"127.0.0.1:{front.bound_port}") as channel:
            stream = channel.stream_stream("/t.F/Chat")(held(), timeout=10)
            assert next(stream) == b"a"
            assert channel.unary_unary("/t.S/U")(b"x", timeout=5) == b"u"
            done.set()
    finally:
        front.close()
        fallback.stop(None)
        shutil.rmtree(directory, ignore_errors=True)
