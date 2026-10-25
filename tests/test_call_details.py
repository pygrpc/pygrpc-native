"""recv_batch_details reports what grpcio's ServicerContext would."""

import threading
from collections.abc import Iterator
from contextlib import contextmanager

import grpc
import pytest

from pygrpc_native import EVENT_CALL, EVENT_CANCEL, NativeGrpcFront

METHODS = [("/t.S/U", False), ("/t.S/S", True)]


@contextmanager
def serving(call_details: bool, block_stream: threading.Event | None = None) -> Iterator[tuple[NativeGrpcFront, list[tuple]]]:
    front = NativeGrpcFront("127.0.0.1:0", METHODS, call_details=call_details)
    front.start()
    events: list[tuple] = []

    def loop() -> None:
        while (batch := front.recv_batch_details(16, 100)) is not None:
            for event in batch:
                events.append(event)
                kind, call_id, method_id, payload, _, _ = event
                if kind != EVENT_CALL:
                    continue
                if method_id == 0:
                    front.respond(call_id, payload)
                elif block_stream is None:
                    front.stream_send(call_id, payload)
                    front.stream_end(call_id)

    thread = threading.Thread(target=loop)
    thread.start()
    try:
        yield front, events
    finally:
        front.close()
        thread.join(10)


def test_metadata_peer_and_deadline_match_grpcio() -> None:
    sent = [("x-a", "1"), ("x-a", "2"), ("x-bin", b"\x00\x01"), ("authorization", "Bearer t")]
    with serving(True) as (front, events):
        with grpc.insecure_channel(f"127.0.0.1:{front.bound_port}", compression=grpc.Compression.Gzip) as channel:
            channel.unary_unary("/t.S/U")(b"x", timeout=5, metadata=sent)
    (event,) = events
    assert len(event) == 6
    metadata, peer, remaining = event[5]
    keys = [key for key, _ in metadata]
    assert keys[0] == "user-agent"
    assert list(metadata[1:]) == sent
    for transport in ("te", "content-type", "grpc-timeout", "grpc-encoding", "grpc-accept-encoding"):
        assert transport not in keys
    assert peer.startswith("ipv4:127.0.0.1:")
    assert 0 < remaining <= 5


def test_no_deadline_and_streaming_calls() -> None:
    with serving(True) as (front, events):
        with grpc.insecure_channel(f"127.0.0.1:{front.bound_port}") as channel:
            assert list(channel.unary_stream("/t.S/S")(b"s")) == [b"s"]
    (event,) = events
    metadata, _, remaining = event[5]
    assert [key for key, _ in metadata] == ["user-agent"]
    assert remaining is None


def test_details_are_none_unless_enabled() -> None:
    with serving(False) as (front, events):
        with grpc.insecure_channel(f"127.0.0.1:{front.bound_port}") as channel:
            channel.unary_unary("/t.S/U")(b"x", timeout=5, metadata=[("x-a", "1")])
    (event,) = events
    assert len(event) == 6 and event[5] is None


def test_cancel_events_have_no_details() -> None:
    hold = threading.Event()
    with serving(True, block_stream=hold) as (front, events):
        with grpc.insecure_channel(f"127.0.0.1:{front.bound_port}") as channel:
            with pytest.raises(grpc.RpcError):
                list(channel.unary_stream("/t.S/S")(b"s", timeout=0.3))
            for _ in range(50):
                if any(e[0] == EVENT_CANCEL for e in events):
                    break
                threading.Event().wait(0.1)
    cancel = next(e for e in events if e[0] == EVENT_CANCEL)
    assert cancel[3:] == (None, None, None)


def test_recv_batch_keeps_five_items_with_details_on() -> None:
    front = NativeGrpcFront("127.0.0.1:0", METHODS, call_details=True)
    front.start()
    try:
        with grpc.insecure_channel(f"127.0.0.1:{front.bound_port}") as channel:
            future = channel.unary_unary("/t.S/U").future(b"x", timeout=5)
            (event,) = front.recv_batch(1, 5000)
            assert len(event) == 5
            front.respond(event[1], event[3])
            assert future.result() == b"x"
    finally:
        front.close()
