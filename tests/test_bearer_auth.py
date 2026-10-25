"""bearer_token_sha256 rejects calls without an accepted token in Rust."""

import hashlib
import shutil
import tempfile
import threading
from collections.abc import Iterator
from concurrent import futures
from contextlib import contextmanager
from typing import cast

import grpc
import pytest

from pygrpc_native import EVENT_CALL, NativeGrpcFront

METHODS = [("/t.S/U", False), ("/t.S/S", True)]
TOKENS = ("current", "next")


def _sha256(token: str) -> bytes:
    return hashlib.sha256(token.encode()).digest()


@contextmanager
def serving(**kwargs: object) -> Iterator[tuple[NativeGrpcFront, list[tuple]]]:
    front = NativeGrpcFront("127.0.0.1:0", METHODS, **kwargs)  # type: ignore[arg-type]
    front.start()
    events: list[tuple] = []

    def loop() -> None:
        while (batch := front.recv_batch(16, 100)) is not None:
            for event in batch:
                events.append(event)
                kind, call_id, method_id, payload, _ = event
                if kind != EVENT_CALL:
                    continue
                if method_id == 0:
                    front.respond(call_id, payload)
                else:
                    front.stream_send(call_id, payload)
                    front.stream_end(call_id)

    thread = threading.Thread(target=loop)
    thread.start()
    try:
        yield front, events
    finally:
        front.close()
        thread.join(10)


def _code(call: object) -> grpc.StatusCode:
    return cast(grpc.Call, call).code()


@pytest.mark.parametrize(
    "authorization",
    [None, "Bearer wrong", "current", "Basic current", "Bearer "],
)
def test_calls_without_an_accepted_token_never_reach_python(
    authorization: str | None,
) -> None:
    metadata = None if authorization is None else [("authorization", authorization)]
    digests = [_sha256(token) for token in TOKENS]
    with serving(
        bearer_token_sha256=digests,
        unauthenticated_message="Invalid or missing API key",
    ) as (front, events):
        with grpc.insecure_channel(f"127.0.0.1:{front.bound_port}") as channel:
            for call in (
                lambda: channel.unary_unary("/t.S/U")(
                    b"x" * (1 << 20), timeout=5, metadata=metadata
                ),
                lambda: list(
                    channel.unary_stream("/t.S/S")(b"x", timeout=5, metadata=metadata)
                ),
                lambda: channel.unary_unary("/t.S/Missing")(
                    b"x", timeout=5, metadata=metadata
                ),
            ):
                with pytest.raises(grpc.RpcError) as info:
                    call()
                assert _code(info.value) is grpc.StatusCode.UNAUTHENTICATED
                assert (
                    cast(grpc.Call, info.value).details()
                    == "Invalid or missing API key"
                )
    assert events == []


@pytest.mark.parametrize("token", TOKENS)
def test_accepted_tokens_are_served(token: str) -> None:
    metadata = [("authorization", f"Bearer {token}")]
    with serving(bearer_token_sha256=[_sha256(t) for t in TOKENS]) as (front, _):
        with grpc.insecure_channel(f"127.0.0.1:{front.bound_port}") as channel:
            assert (
                channel.unary_unary("/t.S/U")(b"u", timeout=5, metadata=metadata)
                == b"u"
            )
            stream = channel.unary_stream("/t.S/S")(b"s", timeout=5, metadata=metadata)
            assert list(stream) == [b"s"]


def test_without_tokens_every_call_is_served() -> None:
    with serving() as (front, _):
        with grpc.insecure_channel(f"127.0.0.1:{front.bound_port}") as channel:
            assert channel.unary_unary("/t.S/U")(b"u", timeout=5) == b"u"


def test_fallback_calls_are_checked_too() -> None:
    def echo(requests: Iterator[bytes], context: grpc.ServicerContext) -> bytes:
        return b"".join(requests)

    fallback = grpc.server(futures.ThreadPoolExecutor(2))
    fallback.add_generic_rpc_handlers(
        [
            grpc.method_handlers_generic_handler(
                "t.F", {"Up": grpc.stream_unary_rpc_method_handler(echo)}
            )
        ]
    )
    # pytest's tmp_path is too long for a Unix socket on macOS.
    directory = tempfile.mkdtemp()
    socket = f"{directory}/fallback.sock"
    fallback.add_insecure_port(f"unix:{socket}")
    fallback.start()
    front = NativeGrpcFront(
        "127.0.0.1:0", METHODS, bearer_token_sha256=[_sha256("current")]
    )
    front.set_fallback(socket)
    front.start()
    try:
        with grpc.insecure_channel(f"127.0.0.1:{front.bound_port}") as channel:
            up = channel.stream_unary("/t.F/Up")
            with pytest.raises(grpc.RpcError) as info:
                up(iter([b"a", b"b"]), timeout=5)
            assert _code(info.value) is grpc.StatusCode.UNAUTHENTICATED
            metadata = [("authorization", "Bearer current")]
            assert up(iter([b"a", b"b"]), timeout=5, metadata=metadata) == b"ab"
    finally:
        front.close()
        fallback.stop(None)
        shutil.rmtree(directory, ignore_errors=True)


def test_bad_digests_are_rejected() -> None:
    with pytest.raises(ValueError, match="must not be empty"):
        NativeGrpcFront("127.0.0.1:0", METHODS, bearer_token_sha256=[])
    with pytest.raises(ValueError, match="32 bytes"):
        NativeGrpcFront("127.0.0.1:0", METHODS, bearer_token_sha256=[b"short"])
