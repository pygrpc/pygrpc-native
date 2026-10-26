"""metrics=True counts calls per method and status, as gRFC A66 does."""

import hashlib
import threading
import time
from collections.abc import Iterator
from concurrent import futures
from contextlib import contextmanager
from typing import Any

import grpc
import pytest

from pygrpc_native import EVENT_CALL, NativeGrpcFront

UNARY, FAIL, STREAM, SLOW = "/t.S/U", "/t.S/F", "/t.S/S", "/t.S/Slow"
METHODS = [(UNARY, False), (FAIL, False), (STREAM, True), (SLOW, False)]


@contextmanager
def serving(**kwargs: Any) -> Iterator[NativeGrpcFront]:
    front = NativeGrpcFront("127.0.0.1:0", METHODS, metrics=True, **kwargs)
    front.start()

    def loop() -> None:
        while (batch := front.recv_batch(16, 100)) is not None:
            for kind, call_id, method_id, payload, _ in batch:
                if kind != EVENT_CALL:
                    continue
                if method_id == 0:
                    front.respond(call_id, payload)
                elif method_id == 1:
                    front.fail(call_id, grpc.StatusCode.NOT_FOUND.value[0], "no")
                elif method_id == 2:
                    front.stream_send(call_id, payload)
                    front.stream_end(call_id)
                # SLOW is never answered.

    thread = threading.Thread(target=loop)
    thread.start()
    try:
        yield front
    finally:
        front.close()
        thread.join(10)


def _settled(front: NativeGrpcFront) -> dict[str, Any]:
    # The status is recorded once the trailers go out, just after the client
    # gets them.
    for _ in range(100):
        metrics = front.metrics()
        assert metrics is not None
        if all(
            m["started"] == sum(s["count"] for s in m["status"].values())
            for m in metrics["methods"].values()
        ):
            return metrics
        time.sleep(0.01)
    raise AssertionError(metrics)


def _counts(metrics: dict[str, Any], method: str) -> dict[int, int]:
    return {
        code: s["count"] for code, s in metrics["methods"][method]["status"].items()
    }


def test_counts_calls_by_method_and_status() -> None:
    with serving() as front:
        with grpc.insecure_channel(f"127.0.0.1:{front.bound_port}") as channel:
            unary = channel.unary_unary(UNARY)
            with futures.ThreadPoolExecutor(8) as pool:
                list(pool.map(lambda _: unary(b"x", timeout=5), range(200)))
            assert list(channel.unary_stream(STREAM)(b"x", timeout=5)) == [b"x"]
            for method in (FAIL, "/t.S/Missing"):
                with pytest.raises(grpc.RpcError):
                    channel.unary_unary(method)(b"x", timeout=5)
            with pytest.raises(grpc.RpcError) as deadline:
                channel.unary_unary(SLOW)(b"x", timeout=0.2)
            assert deadline.value.code() == grpc.StatusCode.DEADLINE_EXCEEDED
            cancelled = channel.unary_unary(SLOW).future(b"x", timeout=5)
            time.sleep(0.2)
            cancelled.cancel()
        metrics = _settled(front)

    assert len(metrics["bounds"]) == 41
    assert metrics["start_time_unix_nano"] > 0
    assert _counts(metrics, UNARY) == {0: 200}
    assert _counts(metrics, STREAM) == {0: 1}
    assert _counts(metrics, FAIL) == {grpc.StatusCode.NOT_FOUND.value[0]: 1}
    assert _counts(metrics, "other") == {grpc.StatusCode.UNIMPLEMENTED.value[0]: 1}
    assert _counts(metrics, SLOW) == {
        grpc.StatusCode.DEADLINE_EXCEEDED.value[0]: 1,
        grpc.StatusCode.CANCELLED.value[0]: 1,
    }
    ok = metrics["methods"][UNARY]["status"][0]
    assert sum(ok["buckets"]) == ok["count"] == 200
    assert len(ok["buckets"]) == len(metrics["bounds"]) + 1
    assert 0 < ok["min"] <= ok["sum"] / ok["count"] <= ok["max"]


def test_rejected_calls_count_under_their_method() -> None:
    digest = hashlib.sha256(b"key").digest()
    with serving(bearer_token_sha256=[digest]) as front:
        with grpc.insecure_channel(f"127.0.0.1:{front.bound_port}") as channel:
            with pytest.raises(grpc.RpcError):
                channel.unary_unary(UNARY)(b"x", timeout=5)
        metrics = _settled(front)
    assert _counts(metrics, UNARY) == {grpc.StatusCode.UNAUTHENTICATED.value[0]: 1}


def test_off_by_default() -> None:
    front = NativeGrpcFront("127.0.0.1:0", METHODS)
    try:
        assert front.metrics() is None
    finally:
        front.close()
