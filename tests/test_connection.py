"""HTTP/2 connection settings: keepalive pings, stream and frame limits, and
the server options grpcio takes for them."""

import socket
import struct
import time
from collections.abc import Iterator
from concurrent import futures
from typing import Any

import grpc
import pytest

import pygrpc_native
from pygrpc_native import NativeGrpcFront

PREFACE = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
SETTINGS, PING, GOAWAY = 0x4, 0x6, 0x7
FAST = {"keepalive_time_ms": 100, "keepalive_timeout_ms": 100}


def _frame(kind: int, flags: int, stream: int, payload: bytes = b"") -> bytes:
    return (
        struct.pack(">I", len(payload))[1:]
        + struct.pack(">BBI", kind, flags, stream)
        + payload
    )


def _literal(name: str, value: str) -> bytes:
    # HPACK literal header field without indexing, new name, no Huffman.
    return (
        b"\x00"
        + bytes([len(name)])
        + name.encode()
        + bytes([len(value)])
        + value.encode()
    )


def _start_call(path: str) -> bytes:
    """HEADERS for a call whose request body never comes, so it stays open."""
    block = b"".join(
        _literal(name, value)
        for name, value in [
            (":method", "POST"),
            (":scheme", "http"),
            (":path", path),
            (":authority", "x"),
            ("content-type", "application/grpc"),
            ("te", "trailers"),
        ]
    )
    return _frame(0x1, 0x4, 1, block)  # END_HEADERS


class Peer:
    """A client that speaks just enough HTTP/2 and never answers a ping."""

    def __init__(self, port: int) -> None:
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=5)
        self.sock.sendall(PREFACE + _frame(SETTINGS, 0, 0))
        self.buffer = b""

    def frames(self, seconds: float) -> Iterator[int]:
        """Yields frame types for up to `seconds`; stops when the server closes."""
        deadline = time.monotonic() + seconds
        while (left := deadline - time.monotonic()) > 0:
            while len(self.buffer) >= 9:
                length = int.from_bytes(self.buffer[:3], "big")
                if len(self.buffer) < 9 + length:
                    break
                kind = self.buffer[3]
                self.buffer = self.buffer[9 + length :]
                yield kind
            self.sock.settimeout(left)
            try:
                data = self.sock.recv(65536)
            except socket.timeout:
                return
            if not data:
                yield -1  # closed
                return
            self.buffer += data

    def settings(self) -> dict[int, int]:
        """The server's first SETTINGS frame, as {identifier: value}."""
        while True:
            while len(self.buffer) < 9 or len(self.buffer) < 9 + int.from_bytes(
                self.buffer[:3], "big"
            ):
                self.buffer += self.sock.recv(65536)
            length = int.from_bytes(self.buffer[:3], "big")
            kind, flags = self.buffer[3], self.buffer[4]
            payload = self.buffer[9 : 9 + length]
            self.buffer = self.buffer[9 + length :]
            if kind == SETTINGS and not flags & 0x1:
                return {
                    int.from_bytes(payload[i : i + 2], "big"): int.from_bytes(
                        payload[i + 2 : i + 6], "big"
                    )
                    for i in range(0, length, 6)
                }

    def close(self) -> None:
        self.sock.close()


def _front(**kwargs: Any) -> NativeGrpcFront:
    front = NativeGrpcFront("127.0.0.1:0", [("/t.S/U", False)], **kwargs)
    front.start()
    return front


def test_unanswered_pings_close_the_connection() -> None:
    front = _front(**FAST)
    peer = Peer(front.bound_port)
    try:
        seen = list(peer.frames(5))
    finally:
        peer.close()
        front.close()
    assert PING in seen
    assert seen[-2:] == [GOAWAY, -1]


def test_idle_connections_are_pinged_only_when_permitted() -> None:
    front = _front(**FAST, keepalive_permit_without_calls=False)
    peer = Peer(front.bound_port)
    try:
        assert PING not in list(peer.frames(0.6))
        peer.sock.sendall(_start_call("/t.S/U"))
        seen = list(peer.frames(5))
    finally:
        peer.close()
        front.close()
    assert PING in seen
    assert seen[-1] == -1


def test_reads_put_off_the_ping() -> None:
    # grpc-core only pings a connection that has read nothing for the
    # keepalive time.
    front = _front(keepalive_time_ms=1000, keepalive_timeout_ms=100)
    peer = Peer(front.bound_port)
    window_update = _frame(0x8, 0, 0, struct.pack(">I", 1))
    try:
        for _ in range(25):
            peer.sock.sendall(window_update)
            assert PING not in list(peer.frames(0.1))
        seen = list(peer.frames(5))
    finally:
        peer.close()
        front.close()
    assert PING in seen
    assert seen[-1] == -1


def test_anything_read_after_a_ping_answers_it() -> None:
    # As in grpc-core, a client still sending isn't cut off for a late answer.
    front = _front(keepalive_time_ms=200, keepalive_timeout_ms=1000)
    peer = Peer(front.bound_port)
    try:
        assert any(kind == PING for kind in peer.frames(5))
        peer.sock.sendall(_frame(0x8, 0, 0, struct.pack(">I", 1)))
        assert -1 not in list(peer.frames(1.5))
    finally:
        peer.close()
        front.close()


def test_keepalive_can_be_turned_off() -> None:
    front = _front(keepalive_time_ms=2**31 - 1, keepalive_timeout_ms=100)
    peer = Peer(front.bound_port)
    try:
        assert PING not in list(peer.frames(0.5))
    finally:
        peer.close()
        front.close()


def test_keepalive_times_must_be_positive() -> None:
    for kwargs in ({"keepalive_time_ms": 0}, {"keepalive_timeout_ms": 0}):
        with pytest.raises(ValueError, match="keepalive"):
            NativeGrpcFront("127.0.0.1:0", [], **kwargs)


def _echo_server(options: list[tuple[str, Any]]) -> tuple[Any, int]:
    def stream(request: bytes, context: grpc.ServicerContext) -> Iterator[bytes]:
        for _ in range(5):
            time.sleep(0.2)
            yield request

    server = pygrpc_native.server(futures.ThreadPoolExecutor(4), options=options)
    server.add_registered_method_handlers(
        "t.S",
        {
            "U": grpc.unary_unary_rpc_method_handler(lambda request, context: request),
            "S": grpc.unary_stream_rpc_method_handler(stream),
        },
    )
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    return server, port


def test_server_takes_grpc_keepalive_options() -> None:
    options = [
        ("grpc.keepalive_time_ms", 100),
        ("grpc.keepalive_timeout_ms", 100),
        ("grpc.keepalive_permit_without_calls", 1),
    ]
    server, port = _echo_server(options)
    try:
        # A grpcio client answers the pings, so a call can outlive many of them.
        with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
            assert list(channel.unary_stream("/t.S/S")(b"x", timeout=10)) == [b"x"] * 5
        peer = Peer(port)
        try:
            seen = list(peer.frames(5))
        finally:
            peer.close()
    finally:
        server.stop(None)
    assert seen[-2:] == [GOAWAY, -1]


def test_server_pings_idle_connections_only_when_permitted() -> None:
    server, port = _echo_server(
        [("grpc.keepalive_time_ms", 100), ("grpc.keepalive_timeout_ms", 100)]
    )
    peer = Peer(port)
    try:
        assert PING not in list(peer.frames(0.6))
    finally:
        peer.close()
        server.stop(None)


def _bind_twice(options: list[tuple[str, Any]]) -> bool:
    first = pygrpc_native.server(futures.ThreadPoolExecutor(1), options=options)
    second = pygrpc_native.server(futures.ThreadPoolExecutor(1), options=options)
    try:
        port = first.add_insecure_port("127.0.0.1:0")
        try:
            second.add_insecure_port(f"127.0.0.1:{port}")
        except RuntimeError:
            return False
        return True
    finally:
        first.stop(None)
        second.stop(None)


def test_options_grpc_core_ignores_are_accepted() -> None:
    # grpc-core doesn't read grpc.max_message_length, so grpcio doesn't limit
    # anything with it, and it reads integers only, so a string value is the
    # same as leaving the option out.
    options = [
        ("grpc.max_message_length", 10),
        ("grpc.max_receive_message_length", "10"),
        ("grpc.keepalive_time_ms", "100"),
        ("grpc.keepalive_permit_without_calls", 1),
        ("grpc.so_reuseport", "1"),
    ]
    server, port = _echo_server(options)
    try:
        with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
            assert channel.unary_unary("/t.S/U")(b"x" * 100, timeout=5) == b"x" * 100
        peer = Peer(port)
        try:
            assert PING not in list(peer.frames(0.3))
        finally:
            peer.close()
    finally:
        server.stop(None)
    # Unlike grpcio, the server only sets SO_REUSEPORT when asked to.
    assert not _bind_twice([])
    assert not _bind_twice([("grpc.so_reuseport", 0)])
    assert not _bind_twice([("grpc.so_reuseport", "0")])
    assert _bind_twice([("grpc.so_reuseport", 1)])


def test_grpcio_ignores_max_message_length() -> None:
    server = grpc.server(
        futures.ThreadPoolExecutor(1), options=[("grpc.max_message_length", 10)]
    )
    server.add_registered_method_handlers(
        "t.S", {"U": grpc.unary_unary_rpc_method_handler(lambda r, c: r)}
    )
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    try:
        with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
            assert channel.unary_unary("/t.S/U")(b"x" * 100, timeout=5) == b"x" * 100
    finally:
        server.stop(None)


MAX_CONCURRENT_STREAMS, MAX_FRAME_SIZE, MAX_HEADER_LIST_SIZE = 0x3, 0x5, 0x6


def _settings(port: int) -> dict[int, int]:
    peer = Peer(port)
    try:
        return peer.settings()
    finally:
        peer.close()


def test_stream_limit() -> None:
    front = _front()
    try:
        defaults = _settings(front.bound_port)
    finally:
        front.close()
    assert defaults[MAX_CONCURRENT_STREAMS] == 1024
    assert defaults.get(MAX_FRAME_SIZE, 16384) == 16384
    # grpcio advertises the same frame size whatever this option says.
    server, port = _echo_server(
        [("grpc.max_concurrent_streams", 7), ("grpc.http2.max_frame_size", 8192)]
    )
    try:
        settings = _settings(port)
    finally:
        server.stop(None)
    assert settings[MAX_CONCURRENT_STREAMS] == 7
    assert settings.get(MAX_FRAME_SIZE, 16384) == 16384


def test_options_the_server_cant_honor_are_the_only_ones_rejected() -> None:
    # grpcio servers take all of these: a misspelling some servers carry, one
    # grpc-core no longer reads, options only clients read (code often passes
    # the same list to both), and HTTP/2 tuning.
    ignored = [
        ("grpc.http2.min_recv_ping_interval_without_data_ms", 10000),
        ("grpc.http2.min_time_between_pings_ms", 10000),
        ("grpc.enable_retries", 1),
        ("grpc.service_config", '{"methodConfig": []}'),
        ("grpc.enable_http_proxy", 0),
        ("grpc.http2.bdp_probe", 0),
        ("my.own.option", 1),
    ]
    server, port = _echo_server(ignored)
    try:
        with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
            assert channel.unary_unary("/t.S/U")(b"x", timeout=5) == b"x"
    finally:
        server.stop(None)
    for option in [
        ("grpc.max_connection_idle_ms", 1000),
        ("grpc.default_compression_algorithm", 2),
    ]:
        with pytest.raises(ValueError, match="unsupported server option"):
            pygrpc_native.server(futures.ThreadPoolExecutor(1), options=[option])


@pytest.mark.parametrize(
    "options",
    [
        [],
        [("grpc.max_metadata_size", 32768)],
        [("grpc.max_metadata_size", 4096)],
        [("grpc.absolute_max_metadata_size", 8192)],
    ],
)
def test_metadata_limit_matches_grpcio(options: list[tuple[str, Any]]) -> None:
    server, port = _echo_server(options)
    try:
        native = _settings(port)
    finally:
        server.stop(None)
    grpcio = grpc.server(futures.ThreadPoolExecutor(1), options=options)
    port = grpcio.add_insecure_port("127.0.0.1:0")
    grpcio.start()
    try:
        expected = _settings(port)
    finally:
        grpcio.stop(None)
    assert native[MAX_HEADER_LIST_SIZE] == expected[MAX_HEADER_LIST_SIZE]
