"""Calls served through pygrpc_native from a Python thread."""

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import cast
import os
import tempfile
import threading

import grpc
import pytest

import pygrpc_native
from pygrpc_native import EVENT_CALL, EVENT_CANCEL, NativeGrpcFront

Handler = Callable[[NativeGrpcFront, int, int, bytes, object], None]


def _code(error: BaseException) -> grpc.StatusCode:
    return cast(grpc.Call, error).code()


@contextmanager
def serving(
    methods: list[tuple[str, bool]],
    handler: Handler,
    target: str = "127.0.0.1:0",
    **kwargs: object,
) -> Iterator[tuple[NativeGrpcFront, list[int]]]:
    """Run a front whose calls go to handler; yields it and cancelled ids."""
    front = NativeGrpcFront(target, methods, **kwargs)
    front.start()
    cancelled: list[int] = []

    def loop() -> None:
        while (events := front.recv_batch(64, 100)) is not None:
            for kind, call_id, method_id, payload, client_key in events:
                if kind == EVENT_CANCEL:
                    cancelled.append(call_id)
                else:
                    assert kind == EVENT_CALL and payload is not None
                    threading.Thread(
                        target=handler,
                        args=(front, call_id, method_id, payload, client_key),
                    ).start()

    thread = threading.Thread(target=loop)
    thread.start()
    try:
        yield front, cancelled
    finally:
        front.close()
        thread.join(10)


def echo(front: NativeGrpcFront, call_id: int, *args: object) -> None:
    front.respond(call_id, cast(bytes, args[1]))


def test_version() -> None:
    assert pygrpc_native.__version__


def test_unary_echo() -> None:
    with serving([("/t.S/Echo", False)], echo) as (front, _):
        with grpc.insecure_channel(f"127.0.0.1:{front.bound_port}") as channel:
            assert channel.unary_unary("/t.S/Echo")(b"abc", timeout=5) == b"abc"


def test_large_messages() -> None:
    payload = os.urandom(16 << 20)
    with serving([("/t.S/Echo", False)], echo) as (front, _):
        options = [
            ("grpc.max_send_message_length", -1),
            ("grpc.max_receive_message_length", -1),
        ]
        with grpc.insecure_channel(
            f"127.0.0.1:{front.bound_port}", options=options
        ) as channel:
            assert channel.unary_unary("/t.S/Echo")(payload, timeout=30) == payload


def test_gzip_request() -> None:
    with serving([("/t.S/Echo", False)], echo) as (front, _):
        with grpc.insecure_channel(
            f"127.0.0.1:{front.bound_port}", compression=grpc.Compression.Gzip
        ) as channel:
            assert channel.unary_unary("/t.S/Echo")(b"z" * 4096, timeout=5) == (
                b"z" * 4096
            )


def test_server_streaming() -> None:
    def count(front: NativeGrpcFront, call_id: int, *_: object) -> None:
        for i in range(5):
            assert front.stream_send(call_id, bytes([i]))
        front.stream_end(call_id)

    with serving([("/t.S/Count", True)], count) as (front, _):
        with grpc.insecure_channel(f"127.0.0.1:{front.bound_port}") as channel:
            replies = list(channel.unary_stream("/t.S/Count")(b"", timeout=5))
    assert replies == [bytes([i]) for i in range(5)]


def test_fail_sets_status() -> None:
    def reject(front: NativeGrpcFront, call_id: int, *_: object) -> None:
        front.fail(call_id, grpc.StatusCode.INVALID_ARGUMENT.value[0], "bad input")

    with serving([("/t.S/Echo", False)], reject) as (front, _):
        with grpc.insecure_channel(f"127.0.0.1:{front.bound_port}") as channel:
            with pytest.raises(grpc.RpcError) as info:
                channel.unary_unary("/t.S/Echo")(b"", timeout=5)
    assert _code(info.value) is grpc.StatusCode.INVALID_ARGUMENT
    assert cast(grpc.Call, info.value).details() == "bad input"


def test_unknown_method_is_unimplemented() -> None:
    with serving([("/t.S/Echo", False)], echo) as (front, _):
        with grpc.insecure_channel(f"127.0.0.1:{front.bound_port}") as channel:
            with pytest.raises(grpc.RpcError) as info:
                channel.unary_unary("/t.S/Other")(b"", timeout=5)
    assert _code(info.value) is grpc.StatusCode.UNIMPLEMENTED


def test_deadline_cancels_stream() -> None:
    started = threading.Event()

    def hang(front: NativeGrpcFront, call_id: int, *_: object) -> None:
        started.set()

    with serving([("/t.S/Count", True)], hang) as (front, cancelled):
        with grpc.insecure_channel(f"127.0.0.1:{front.bound_port}") as channel:
            with pytest.raises(grpc.RpcError) as info:
                list(channel.unary_stream("/t.S/Count")(b"", timeout=0.5))
            assert _code(info.value) is grpc.StatusCode.DEADLINE_EXCEEDED
            assert started.wait(5)
            for _ in range(50):
                if cancelled:
                    break
                threading.Event().wait(0.1)
            assert len(cancelled) == 1


@pytest.mark.parametrize(
    ("key", "metadata", "expected"),
    [
        ("x-client-id-bin", [("x-client-id-bin", b"\x00\x01")], b"\x00\x01"),
        ("x-client-id", [("x-client-id", "engine-1")], b"engine-1"),
        ("x-client-id", [], None),
        (None, [("x-client-id", "engine-1")], None),
    ],
)
def test_client_key(
    key: str | None, metadata: list[tuple[str, object]], expected: bytes | None
) -> None:
    keys: list[object] = []

    def record(front: NativeGrpcFront, call_id: int, *args: object) -> None:
        keys.append(args[2])
        front.respond(call_id, b"")

    with serving([("/t.S/Echo", False)], record, client_key_metadata=key) as (
        front,
        _,
    ):
        with grpc.insecure_channel(f"127.0.0.1:{front.bound_port}") as channel:
            channel.unary_unary("/t.S/Echo")(b"", metadata=metadata, timeout=5)
    if expected is None:
        # Without the metadata the client key is the peer address.
        assert isinstance(keys[0], str) and "127.0.0.1" in keys[0]
    else:
        assert keys == [expected]


@pytest.mark.parametrize("key", ["X-Client-Id", "bad key", ""])
def test_invalid_client_key_metadata(key: str) -> None:
    with pytest.raises(ValueError):
        NativeGrpcFront("127.0.0.1:0", [], client_key_metadata=key)


def test_unix_socket() -> None:
    with tempfile.TemporaryDirectory(dir="/tmp") as tmp:
        path = os.path.join(tmp, "s.sock")
        with serving([("/t.S/Echo", False)], echo, target=f"unix:{path}") as (
            front,
            _,
        ):
            assert front.bound_port == 1
            # h2 rejects the default :authority grpcio sends over a Unix socket.
            with grpc.insecure_channel(
                f"unix:{path}", options=[("grpc.default_authority", "localhost")]
            ) as channel:
                assert channel.unary_unary("/t.S/Echo")(b"u", timeout=5) == b"u"
        assert not os.path.exists(path)


def test_bind_failure_raises_oserror() -> None:
    front = NativeGrpcFront("127.0.0.1:0", [])
    try:
        with pytest.raises(OSError):
            NativeGrpcFront(f"127.0.0.1:{front.bound_port}", [])
    finally:
        front.close()
