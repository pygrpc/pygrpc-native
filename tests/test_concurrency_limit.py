"""maximum_concurrent_rpcs: calls over the limit get RESOURCE_EXHAUSTED, the
same way grpcio answers them."""

import asyncio
import collections
import threading
import time
from collections.abc import Callable, Iterator
from concurrent import futures
from typing import Any

import grpc
import pytest
from grpc import aio

import pygrpc_native
import pygrpc_native.aio
from pygrpc_native._native import EVENT_CALL, EVENT_CANCEL

EXCEEDED = (grpc.StatusCode.RESOURCE_EXHAUSTED, "Concurrent RPC limit exceeded!")

Client = Callable[[str, "_Gate", list[str]], Any]


class _Gate:
    """Holds the slow calls until the test opens it."""

    def __init__(self) -> None:
        self.entered = threading.Semaphore(0)
        self.open = threading.Event()

    def wait(self) -> None:
        self.entered.release()
        assert self.open.wait(10)


class _Seen(grpc.ServerInterceptor):
    """Records each call, and fails the ones to InterceptorBoom."""

    def __init__(self, seen: list[str]) -> None:
        self.seen = seen

    def intercept_service(self, continuation: Callable, details: Any) -> Any:
        self.seen.append(details.method)
        if details.method == "/l.S/InterceptorBoom":
            raise ValueError("boom")
        return continuation(details)


class _AioSeen(aio.ServerInterceptor):
    def __init__(self, seen: list[str]) -> None:
        self.seen = seen

    async def intercept_service(self, continuation: Callable, details: Any) -> Any:
        self.seen.append(details.method)
        if details.method == "/l.S/InterceptorBoom":
            raise ValueError("boom")
        return await continuation(details)


def _bad_request(data: bytes) -> bytes:
    raise ValueError("can't decode")


def _sync_handlers(gate: _Gate) -> dict[str, grpc.RpcMethodHandler]:
    def slow(request: bytes, context: Any) -> bytes:
        gate.wait()
        return request

    def slow_stream(request: bytes, context: Any) -> Iterator[bytes]:
        yield request
        gate.wait()
        yield request

    def chat(requests: Iterator[bytes], context: Any) -> Iterator[bytes]:
        for request in requests:
            yield request
            gate.wait()

    def boom(request: bytes, context: Any) -> bytes:
        raise RuntimeError("boom")

    def abort(request: bytes, context: Any) -> bytes:
        context.abort(grpc.StatusCode.PERMISSION_DENIED, "no")

    echo = grpc.unary_unary_rpc_method_handler(lambda request, context: request)
    return {
        "Fast": echo,
        "InterceptorBoom": echo,
        "Slow": grpc.unary_unary_rpc_method_handler(slow),
        "SlowStream": grpc.unary_stream_rpc_method_handler(slow_stream),
        "Chat": grpc.stream_stream_rpc_method_handler(chat),
        "Boom": grpc.unary_unary_rpc_method_handler(boom),
        "Abort": grpc.unary_unary_rpc_method_handler(abort),
        "BadRequest": grpc.unary_unary_rpc_method_handler(
            lambda request, context: request, request_deserializer=_bad_request
        ),
    }


def _aio_handlers(gate: _Gate) -> dict[str, grpc.RpcMethodHandler]:
    async def wait() -> None:
        await asyncio.get_running_loop().run_in_executor(None, gate.wait)

    async def fast(request: bytes, context: Any) -> bytes:
        return request

    async def slow(request: bytes, context: Any) -> bytes:
        await wait()
        return request

    async def slow_stream(request: bytes, context: Any) -> Any:
        yield request
        await wait()
        yield request

    async def chat(requests: Any, context: Any) -> Any:
        async for request in requests:
            yield request
            await wait()

    async def boom(request: bytes, context: Any) -> bytes:
        raise RuntimeError("boom")

    async def abort(request: bytes, context: Any) -> bytes:
        await context.abort(grpc.StatusCode.PERMISSION_DENIED, "no")

    echo = grpc.unary_unary_rpc_method_handler(fast)
    return {
        "Fast": echo,
        "InterceptorBoom": echo,
        "Slow": grpc.unary_unary_rpc_method_handler(slow),
        "SlowStream": grpc.unary_stream_rpc_method_handler(slow_stream),
        "Chat": grpc.stream_stream_rpc_method_handler(chat),
        "Boom": grpc.unary_unary_rpc_method_handler(boom),
        "Abort": grpc.unary_unary_rpc_method_handler(abort),
        "BadRequest": grpc.unary_unary_rpc_method_handler(
            fast, request_deserializer=_bad_request
        ),
    }


def _add(server: Any, handlers: dict[str, grpc.RpcMethodHandler]) -> None:
    # Both, as generated code does: grpc.aio ignores registered handlers
    # before grpcio 1.84.
    server.add_generic_rpc_handlers(
        (grpc.method_handlers_generic_handler("l.S", handlers),)
    )
    server.add_registered_method_handlers("l.S", handlers)


def _outcome(call: Callable[[], Any]) -> Any:
    try:
        result = call()
        return list(result) if isinstance(result, Iterator) else result
    except grpc.RpcError as error:
        return (error.code(), error.details())


def _eventually(call: Callable[[], Any], want: Any) -> Any:
    """A finished call's place is given back just after its response is
    sent, so the next call may briefly still be over the limit."""
    for _ in range(50):
        out = _outcome(call)
        if out == want:
            break
        time.sleep(0.05)
    return out


def _serve_sync(
    factory: Callable,
    client: Client,
    workers: int = 8,
    intercept: bool = True,
    **kwargs: Any,
) -> Any:
    gate, seen = _Gate(), []
    server = factory(
        futures.ThreadPoolExecutor(workers),
        interceptors=[_Seen(seen)] if intercept else None,
        **kwargs,
    )
    _add(server, _sync_handlers(gate))
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    try:
        return client(f"127.0.0.1:{port}", gate, seen)
    finally:
        gate.open.set()
        server.stop(None)


def _serve_aio(
    factory: Callable,
    client: Client,
    workers: int = 8,
    intercept: bool = True,
    **kwargs: Any,
) -> Any:
    gate, seen = _Gate(), []

    async def serve() -> Any:
        server = factory(interceptors=[_AioSeen(seen)] if intercept else None, **kwargs)
        _add(server, _aio_handlers(gate))
        port = server.add_insecure_port("127.0.0.1:0")
        await server.start()
        try:
            return await asyncio.get_running_loop().run_in_executor(
                None, client, f"127.0.0.1:{port}", gate, seen
            )
        finally:
            gate.open.set()
            await server.stop(None)

    return asyncio.run(serve())


SERVERS = pytest.mark.parametrize(
    "serve, native, reference",
    [
        (_serve_sync, pygrpc_native.server, grpc.server),
        (_serve_aio, pygrpc_native.aio.server, aio.server),
    ],
    ids=["sync", "aio"],
)


def _hold(channel: grpc.Channel, held: str) -> Any:
    if held == "unary":
        return channel.unary_unary("/l.S/Slow").future(b"a", timeout=10)
    if held == "stream":
        first = channel.unary_stream("/l.S/SlowStream")(b"a", timeout=10)
    else:
        first = channel.stream_stream("/l.S/Chat")(iter([b"a"]), timeout=10)
    assert next(first) == b"a"
    return first


def _over_the_limit(held: str) -> Client:
    """With a limit of 1, holds one call of kind `held` open, sends calls of
    the same kind past the limit, then lets the held call finish and sends
    one more."""

    def client(target: str, gate: _Gate, seen: list[str]) -> dict[str, Any]:
        out: dict[str, Any] = {}
        with grpc.insecure_channel(target) as channel:
            fast = channel.unary_unary("/l.S/Fast")
            first = _hold(channel, held)
            assert gate.entered.acquire(timeout=10)
            if held == "bidi":
                # Methods passed to the fallback server are counted there.
                out["bidi over"] = _outcome(
                    lambda: channel.stream_stream("/l.S/Chat")(iter([b"x"]), timeout=10)
                )
            else:
                out["unary over"] = _outcome(lambda: fast(b"x", timeout=10))
                out["stream over"] = _outcome(
                    lambda: channel.unary_stream("/l.S/SlowStream")(b"x", timeout=10)
                )
            out["seen"] = collections.Counter(seen)
            gate.open.set()
            out["held"] = _outcome(
                lambda: first.result() if held == "unary" else list(first)
            )
            out["after"] = _eventually(lambda: fast(b"y", timeout=10), b"y")
        return out

    return client


@pytest.mark.parametrize("intercept", [True, False], ids=["intercepted", "plain"])
@pytest.mark.parametrize("held", ["unary", "stream", "bidi"])
@SERVERS
def test_calls_over_the_limit_match_grpcio(
    serve: Callable, native: Callable, reference: Callable, held: str, intercept: bool
) -> None:
    got = serve(
        native, _over_the_limit(held), intercept=intercept, maximum_concurrent_rpcs=1
    )
    expected = serve(
        reference, _over_the_limit(held), intercept=intercept, maximum_concurrent_rpcs=1
    )
    assert got == expected
    for key in ("unary over", "stream over", "bidi over"):
        if key in got:
            assert got[key] == EXCEEDED
    assert got["after"] == b"y"


def _rejected_while_workers_are_busy(target: str, gate: _Gate, seen: list[str]) -> Any:
    with grpc.insecure_channel(target) as channel:
        first = _hold(channel, "unary")
        assert gate.entered.acquire(timeout=10)
        start = time.monotonic()
        out = _outcome(lambda: channel.unary_unary("/l.S/Fast")(b"x", timeout=5))
        elapsed = time.monotonic() - start
        gate.open.set()
        first.result()
    return out, elapsed


@pytest.mark.parametrize("factory", [pygrpc_native.server, grpc.server])
def test_sync_rejects_without_waiting_for_a_worker(factory: Callable) -> None:
    # The only worker is busy with the held call.
    out, elapsed = _serve_sync(
        factory, _rejected_while_workers_are_busy, workers=1, maximum_concurrent_rpcs=1
    )
    assert out == EXCEEDED
    assert elapsed < 1


def _native_while_bidi_is_held(target: str, gate: _Gate, seen: list[str]) -> Any:
    with grpc.insecure_channel(target) as channel:
        first = _hold(channel, "bidi")
        assert gate.entered.acquire(timeout=10)
        out = _outcome(lambda: channel.unary_unary("/l.S/Fast")(b"x", timeout=5))
        gate.open.set()
        list(first)
    return out


@pytest.mark.parametrize(
    "serve, factory",
    [(_serve_sync, pygrpc_native.server), (_serve_aio, pygrpc_native.aio.server)],
    ids=["sync", "aio"],
)
def test_native_and_fallback_methods_are_counted_apart(
    serve: Callable, factory: Callable
) -> None:
    # grpcio counts both together and would reject this call.
    assert serve(factory, _native_while_bidi_is_held, maximum_concurrent_rpcs=1) == b"x"


def _failures_then_fast(target: str, gate: _Gate, seen: list[str]) -> Any:
    out = []
    with grpc.insecure_channel(target) as channel:
        for _ in range(3):
            for method in ("Boom", "Abort", "InterceptorBoom", "BadRequest"):
                out.append(
                    _outcome(
                        lambda: channel.unary_unary(f"/l.S/{method}")(b"x", timeout=5)
                    )[0]
                )
                out.append(
                    _eventually(
                        lambda: channel.unary_unary("/l.S/Fast")(b"x", timeout=5), b"x"
                    )
                )
    return out


@pytest.mark.parametrize(
    "serve, factory",
    [(_serve_sync, pygrpc_native.server), (_serve_aio, pygrpc_native.aio.server)],
    ids=["sync", "aio"],
)
def test_failed_calls_give_their_place_back(serve: Callable, factory: Callable) -> None:
    # Not compared with grpcio: grpc.aio 1.64 decides before a call arrives,
    # so it sometimes rejects the call right after a failure.
    out = serve(factory, _failures_then_fast, maximum_concurrent_rpcs=1)
    assert grpc.StatusCode.OK not in out[0::2]
    assert out[1::2] == [b"x"] * 12


def _deadline_then_fast(target: str, gate: _Gate, seen: list[str]) -> Any:
    with grpc.insecure_channel(target) as channel:
        expired = _outcome(lambda: channel.unary_unary("/l.S/Slow")(b"a", timeout=0.3))
        gate.open.set()
        fast = channel.unary_unary("/l.S/Fast")
        return expired[0], _eventually(lambda: fast(b"x", timeout=5), b"x")


@pytest.mark.parametrize(
    "serve, factory",
    [(_serve_sync, pygrpc_native.server), (_serve_aio, pygrpc_native.aio.server)],
    ids=["sync", "aio"],
)
def test_call_past_its_deadline_gives_its_place_back(
    serve: Callable, factory: Callable
) -> None:
    got = serve(factory, _deadline_then_fast, maximum_concurrent_rpcs=1)
    assert got == (grpc.StatusCode.DEADLINE_EXCEEDED, b"x")


def _three_held_at_once(target: str, gate: _Gate, seen: list[str]) -> Any:
    with grpc.insecure_channel(target) as channel:
        held = [_hold(channel, "unary") for _ in range(3)]
        entered = all(gate.entered.acquire(timeout=10) for _ in held)
        gate.open.set()
        return entered, [call.result() for call in held]


@pytest.mark.parametrize(
    "serve, factory",
    [(_serve_sync, pygrpc_native.server), (_serve_aio, pygrpc_native.aio.server)],
    ids=["sync", "aio"],
)
def test_no_limit_by_default(serve: Callable, factory: Callable) -> None:
    assert serve(factory, _three_held_at_once) == (True, [b"a", b"a", b"a"])


def test_aio_rejects_a_limit_below_one() -> None:
    async def build(factory: Callable) -> None:
        factory(maximum_concurrent_rpcs=0)

    for factory in (aio.server, pygrpc_native.aio.server):
        # Older grpcio misspells "positive" in this message.
        with pytest.raises(ValueError, match="maximum_concurrent_rpcs should be a"):
            asyncio.run(build(factory))


def test_sync_limit_of_zero_rejects_every_call() -> None:
    def fast(target: str, gate: _Gate, seen: list[str]) -> Any:
        with grpc.insecure_channel(target) as channel:
            return _outcome(lambda: channel.unary_unary("/l.S/Fast")(b"x", timeout=10))

    got = _serve_sync(pygrpc_native.server, fast, maximum_concurrent_rpcs=0)
    assert got == _serve_sync(grpc.server, fast, maximum_concurrent_rpcs=0) == EXCEEDED


def test_aio_call_cancelled_in_the_same_batch_gives_its_place_back() -> None:
    """A cancel that arrives with its call cancels the task before it ever
    runs, so nothing in the task's body can give the place back."""

    async def serve() -> int:
        server = pygrpc_native.aio.server(maximum_concurrent_rpcs=1)
        _add(server, _aio_handlers(_Gate()))
        server.add_insecure_port("127.0.0.1:0")
        await server.start()
        try:
            method_id = [path for path, _ in server._table].index("/l.S/Fast")
            details = ((), "ipv4:127.0.0.1:1", None)
            server._on_events(
                [
                    (EVENT_CALL, 10**9, method_id, b"x", None, details),
                    (EVENT_CANCEL, 10**9, 0, None, None, None),
                ]
            )
            await asyncio.sleep(0.1)
            return server._limit._active
        finally:
            await server.stop(None)

    assert asyncio.run(serve()) == 0
