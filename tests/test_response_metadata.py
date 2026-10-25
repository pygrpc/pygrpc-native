"""Response metadata as a grpcio client sees it."""

import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import cast

import grpc
import pytest

from pygrpc_native import EVENT_CALL, NativeGrpcFront

Answer = Callable[[NativeGrpcFront, int, bytes], None]


@contextmanager
def serving(answer: Answer, streaming: bool = False) -> Iterator[grpc.Channel]:
    front = NativeGrpcFront("127.0.0.1:0", [("/t.S/M", streaming)])
    front.start()

    def loop() -> None:
        while (batch := front.recv_batch(16, 100)) is not None:
            for kind, call_id, _, payload, _ in batch:
                if kind == EVENT_CALL:
                    answer(front, call_id, payload)

    thread = threading.Thread(target=loop)
    thread.start()
    try:
        with grpc.insecure_channel(f"127.0.0.1:{front.bound_port}") as channel:
            yield channel
    finally:
        front.close()
        thread.join(10)


def test_unary_initial_and_trailing_metadata() -> None:
    def answer(front: NativeGrpcFront, call_id: int, payload: bytes) -> None:
        front.respond(
            call_id,
            payload,
            initial_metadata=[("x-init", "1"), ("x-init-bin", b"\x00")],
            trailing_metadata=[("x-trail", "2"), ("x-trail", "3")],
        )

    with serving(answer) as channel:
        response, call = channel.unary_unary("/t.S/M").with_call(b"x", timeout=5)
    assert response == b"x"
    initial = [(m.key, m.value) for m in call.initial_metadata()]
    assert ("x-init", "1") in initial and ("x-init-bin", b"\x00") in initial
    trailing = [(m.key, m.value) for m in call.trailing_metadata()]
    assert [kv for kv in trailing if kv[0] == "x-trail"] == [
        ("x-trail", "2"),
        ("x-trail", "3"),
    ]
    assert call.code() is grpc.StatusCode.OK


def test_fail_with_trailing_metadata_and_status_details() -> None:
    def answer(front: NativeGrpcFront, call_id: int, payload: bytes) -> None:
        front.fail(
            call_id,
            grpc.StatusCode.FAILED_PRECONDITION.value[0],
            "nope",
            trailing_metadata=[
                ("x-why", "busy"),
                ("grpc-status-details-bin", b"\x08\x09"),
            ],
        )

    with serving(answer) as channel:
        with pytest.raises(grpc.RpcError) as info:
            channel.unary_unary("/t.S/M")(b"x", timeout=5)
    error = cast(grpc.Call, info.value)
    assert error.code() is grpc.StatusCode.FAILED_PRECONDITION
    assert error.details() == "nope"
    trailing = dict((m.key, m.value) for m in error.trailing_metadata())
    assert trailing["x-why"] == "busy"
    assert trailing["grpc-status-details-bin"] == b"\x08\x09"


def test_stream_end_with_trailing_metadata() -> None:
    def answer(front: NativeGrpcFront, call_id: int, payload: bytes) -> None:
        front.stream_send(call_id, payload)
        front.stream_end(call_id, trailing_metadata=[("x-count", "1")])

    with serving(answer, streaming=True) as channel:
        call = channel.unary_stream("/t.S/M")(b"s", timeout=5)
        assert list(call) == [b"s"]
        assert ("x-count", "1") in [(m.key, m.value) for m in call.trailing_metadata()]
        assert call.code() is grpc.StatusCode.OK


@pytest.mark.parametrize(
    "metadata",
    [
        [("grpc-foo", "x")],
        [(":path", "x")],
        [("X-Upper", "x")],
        [("x-text", b"bytes")],
        [("x-bin", "text")],
        [("x-bad", "line\nbreak")],
    ],
)
def test_invalid_metadata_is_rejected(metadata: list[tuple[str, object]]) -> None:
    front = NativeGrpcFront("127.0.0.1:0", [])
    try:
        with pytest.raises(ValueError):
            front.respond(1, b"", trailing_metadata=metadata)  # type: ignore[arg-type]
    finally:
        front.close()
