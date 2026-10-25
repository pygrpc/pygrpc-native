"""The 0.1 API that existing users call. New parameters may be added after
the existing ones with a default; nothing here may change or go away."""

import inspect
import threading

import grpc

import pygrpc_native
from pygrpc_native import EVENT_CALL, EVENT_CANCEL, NativeGrpcFront


_NO_DEFAULT = inspect.Parameter.empty

# (name, default) of every 0.1 parameter, in order.
SIGNATURES = {
    "__init__": [
        ("target", _NO_DEFAULT),
        ("methods", _NO_DEFAULT),
        ("io_threads", 2),
        ("client_key_metadata", None),
    ],
    "set_fallback": [("path", _NO_DEFAULT)],
    "start": [],
    "recv_batch": [("max_events", _NO_DEFAULT), ("timeout_ms", None)],
    "respond": [("call_id", _NO_DEFAULT), ("payload", _NO_DEFAULT)],
    "fail": [("call_id", _NO_DEFAULT), ("code", _NO_DEFAULT), ("message", _NO_DEFAULT)],
    "stream_send": [("call_id", _NO_DEFAULT), ("payload", _NO_DEFAULT)],
    "stream_end": [("call_id", _NO_DEFAULT)],
    "stream_open": [("call_id", _NO_DEFAULT)],
    "close": [],
}


def test_module_exports() -> None:
    assert isinstance(pygrpc_native.__version__, str)
    assert (EVENT_CALL, EVENT_CANCEL) == (0, 1)


def test_signatures() -> None:
    """0.1 parameters keep their order and defaults; new ones need a default."""
    for name, expected in SIGNATURES.items():
        func = NativeGrpcFront if name == "__init__" else getattr(NativeGrpcFront, name)
        params = [
            p for p in inspect.signature(func).parameters.values() if p.name != "self"
        ]
        actual = [(p.name, p.default) for p in params]
        assert actual[: len(expected)] == expected, name
        assert all(
            default is not _NO_DEFAULT for _, default in actual[len(expected) :]
        ), name


def test_events_and_return_values() -> None:
    front = NativeGrpcFront("127.0.0.1:0", [("/t.S/U", False), ("/t.S/S", True)])
    assert isinstance(front.bound_port, int)
    front.start()
    events: list[tuple] = []

    def serve() -> None:
        while (batch := front.recv_batch(16, 100)) is not None:
            for event in batch:
                events.append(event)
                kind, call_id, method_id, payload, _ = event
                if kind != EVENT_CALL:
                    continue
                if method_id == 0:
                    assert front.respond(call_id, payload) is True
                    assert front.respond(call_id, payload) is False
                else:
                    assert front.stream_open(call_id) is True
                    assert front.stream_send(call_id, payload) is True
                    front.stream_end(call_id)
                    assert front.stream_open(call_id) is False

    thread = threading.Thread(target=serve)
    thread.start()
    try:
        with grpc.insecure_channel(f"127.0.0.1:{front.bound_port}") as channel:
            assert channel.unary_unary("/t.S/U")(b"u", timeout=5) == b"u"
            assert list(channel.unary_stream("/t.S/S")(b"s", timeout=5)) == [b"s"]
    finally:
        front.close()
        thread.join(10)
    assert front.recv_batch(1, 0) is None
    assert all(len(event) == 5 for event in events)
    calls = [e for e in events if e[0] == EVENT_CALL]
    assert len(calls) == 2
    for _, call_id, method_id, payload, client_key in calls:
        assert isinstance(call_id, int) and isinstance(method_id, int)
        assert isinstance(payload, bytes)
        assert isinstance(client_key, str)
    assert front.fail(10**9, 2, "gone") is False
