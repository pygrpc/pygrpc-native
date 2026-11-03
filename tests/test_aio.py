"""pygrpc_native.aio.server() against grpc.aio.server(), with the same generated code."""

import asyncio
import importlib
import subprocess
import sys
import time
from collections.abc import AsyncIterator, Callable, Iterator
from concurrent import futures
from typing import Any, cast

import grpc
import pytest
from grpc import aio

import pygrpc_native
import pygrpc_native.aio

grpc_tools = pytest.importorskip("grpc_tools.protoc")

PROTO = """
syntax = "proto3";
package a;
service S {
  rpc Unary(Msg) returns (Msg);
  rpc Stream(Msg) returns (stream Msg);
  rpc Chat(stream Msg) returns (stream Msg);
}
message Msg { string s = 1; }
"""


@pytest.fixture(scope="module")
def pb(tmp_path_factory: pytest.TempPathFactory) -> tuple[Any, Any]:
    out = tmp_path_factory.mktemp("proto")
    (out / "a.proto").write_text(PROTO)
    assert (
        grpc_tools.main(
            [
                "",
                f"-I{out}",
                f"--python_out={out}",
                f"--grpc_python_out={out}",
                str(out / "a.proto"),
            ]
        )
        == 0
    )
    sys.path.insert(0, str(out))
    try:
        return importlib.import_module("a_pb2"), importlib.import_module("a_pb2_grpc")
    finally:
        sys.path.remove(str(out))


Behavior = Callable[[Any, aio.ServicerContext], Any]
SERVERS = {"grpcio": aio.server, "native": pygrpc_native.aio.server}


async def _serve(
    pb: tuple[Any, Any],
    kind: str,
    unary: Behavior | None = None,
    stream: Behavior | None = None,
    host: str = "127.0.0.1",
    **kwargs: Any,
) -> tuple[aio.Server, int]:
    a_pb2, a_pb2_grpc = pb

    class Servicer(a_pb2_grpc.SServicer):
        async def Unary(self, request: Any, context: aio.ServicerContext) -> Any:
            return await unary(request, context) if unary else request

        async def Stream(
            self, request: Any, context: aio.ServicerContext
        ) -> AsyncIterator[Any]:
            if stream is None:
                yield request
                return
            async for response in stream(request, context):
                yield response

        async def Chat(
            self, requests: AsyncIterator[Any], context: aio.ServicerContext
        ) -> AsyncIterator[Any]:
            async for request in requests:
                yield a_pb2.Msg(s=request.s.upper())

    server = SERVERS[kind](**kwargs)
    a_pb2_grpc.add_SServicer_to_server(Servicer(), server)
    port = server.add_insecure_port(f"{host}:0")
    await server.start()
    return server, port


def _outcome(error: aio.AioRpcError) -> tuple[Any, ...]:
    return (
        error.code(),
        error.details(),
        list(error.trailing_metadata() or ()),
    )


def _picked(metadata: Any) -> list[tuple[str, Any]]:
    return [(key, value) for key, value in metadata or () if key.startswith("x-")]


async def _unary(
    pb: tuple[Any, Any],
    kind: str,
    behavior: Behavior,
    host: str = "127.0.0.1",
) -> Any:
    a_pb2, a_pb2_grpc = pb
    server, port = await _serve(pb, kind, unary=behavior, host=host)
    try:
        async with aio.insecure_channel(f"{host}:{port}") as channel:
            call = a_pb2_grpc.SStub(channel).Unary(
                a_pb2.Msg(s="x"), timeout=5, metadata=[("x-in", "1")]
            )
            try:
                response = await call
            except aio.AioRpcError as error:
                return _outcome(error)
            return (
                "OK",
                response.s,
                _picked(await call.initial_metadata()),
                _picked(await call.trailing_metadata()),
            )
    finally:
        await server.stop(None)


async def _stream(pb: tuple[Any, Any], kind: str, behavior: Behavior) -> Any:
    a_pb2, a_pb2_grpc = pb
    server, port = await _serve(pb, kind, stream=behavior)
    try:
        async with aio.insecure_channel(f"127.0.0.1:{port}") as channel:
            call = a_pb2_grpc.SStub(channel).Stream(a_pb2.Msg(s="x"), timeout=5)
            received = []
            try:
                async for message in call:
                    received.append(message.s)
            except aio.AioRpcError as error:
                return received, *_outcome(error)
            return (
                "OK",
                received,
                _picked(await call.initial_metadata()),
                _picked(await call.trailing_metadata()),
            )
    finally:
        await server.stop(None)


async def _metadata(request: Any, context: aio.ServicerContext) -> Any:
    await context.send_initial_metadata((("x-init", "1"),))
    context.set_trailing_metadata((("x-trail", "2"), ("x-trail-bin", b"\x00")))
    request.s = repr(_picked(context.invocation_metadata()))
    return request


async def _raise(request: Any, context: aio.ServicerContext) -> Any:
    raise ValueError("boom")


async def _abort(request: Any, context: aio.ServicerContext) -> Any:
    await context.abort(grpc.StatusCode.PERMISSION_DENIED, "no")


async def _abort_with_metadata(request: Any, context: aio.ServicerContext) -> Any:
    await context.abort(grpc.StatusCode.NOT_FOUND, "nf", (("x-t", "1"),))


async def _abort_with_status(request: Any, context: aio.ServicerContext) -> Any:
    class Status(grpc.Status):
        code = grpc.StatusCode.UNAVAILABLE
        details = "busy"
        trailing_metadata = (("x-t", "1"), ("grpc-status-details-bin", b"\x01"))

    await context.abort_with_status(Status())


async def _catch_abort(request: Any, context: aio.ServicerContext) -> Any:
    try:
        await context.abort(grpc.StatusCode.INTERNAL, "kept")
    except aio.AbortError:
        return request


async def _set_code(request: Any, context: aio.ServicerContext) -> Any:
    context.set_code(grpc.StatusCode.NOT_FOUND)
    context.set_details("nf")
    return request


async def _set_code_then_raise(request: Any, context: aio.ServicerContext) -> Any:
    context.set_code(grpc.StatusCode.ABORTED)
    context.set_details("a")
    return 1 / 0


def _set_code_int(code: int) -> Behavior:
    async def behavior(request: Any, context: aio.ServicerContext) -> Any:
        context.set_code(code)  # type: ignore[arg-type]
        context.set_details("nf")
        return request

    return behavior


async def _abort_int(request: Any, context: aio.ServicerContext) -> Any:
    await context.abort(5, "nf")  # type: ignore[arg-type]


async def _echo(request: Any, context: aio.ServicerContext) -> Any:
    return request


async def _none(request: Any, context: aio.ServicerContext) -> Any:
    return None


UNARY_CASES: dict[str, Behavior] = {
    "echo": _echo,
    "metadata": _metadata,
    "raise": _raise,
    "abort": _abort,
    "abort_with_metadata": _abort_with_metadata,
    "abort_with_status": _abort_with_status,
    "catch_abort": _catch_abort,
    "set_code": _set_code,
    "set_code_then_raise": _set_code_then_raise,
    "set_code_int": _set_code_int(5),
    "set_code_int_zero": _set_code_int(0),
    "set_code_int_out_of_range": _set_code_int(16),
    "abort_int": _abort_int,
    "none": _none,
}


@pytest.mark.parametrize("case", UNARY_CASES)
def test_unary_matches_grpcio(pb: tuple[Any, Any], case: str) -> None:
    expected = asyncio.run(_unary(pb, "grpcio", UNARY_CASES[case]))
    assert asyncio.run(_unary(pb, "native", UNARY_CASES[case])) == expected


async def _two(request: Any, context: aio.ServicerContext) -> AsyncIterator[Any]:
    yield request
    yield request


async def _empty(request: Any, context: aio.ServicerContext) -> AsyncIterator[Any]:
    return
    yield


async def _stream_raise(
    request: Any, context: aio.ServicerContext
) -> AsyncIterator[Any]:
    yield request
    raise RuntimeError("mid")


async def _stream_set_code(
    request: Any, context: aio.ServicerContext
) -> AsyncIterator[Any]:
    yield request
    context.set_code(grpc.StatusCode.DATA_LOSS)
    context.set_details("dl")


async def _stream_set_code_int(
    request: Any, context: aio.ServicerContext
) -> AsyncIterator[Any]:
    yield request
    context.set_code(15)  # type: ignore[arg-type]
    context.set_details("dl")


async def _stream_abort_first(
    request: Any, context: aio.ServicerContext
) -> AsyncIterator[Any]:
    await context.abort(grpc.StatusCode.NOT_FOUND, "nothing")
    yield request


async def _stream_metadata(
    request: Any, context: aio.ServicerContext
) -> AsyncIterator[Any]:
    await context.send_initial_metadata((("x-init", "1"),))
    context.set_trailing_metadata((("x-trail", "1"),))
    yield request


async def _stream_none(
    request: Any, context: aio.ServicerContext
) -> AsyncIterator[Any]:
    yield request
    yield None


async def _headers_only(
    request: Any, context: aio.ServicerContext
) -> AsyncIterator[Any]:
    await context.send_initial_metadata(())
    await asyncio.sleep(0.05)
    yield request


STREAM_CASES: dict[str, Behavior] = {
    "two": _two,
    "empty": _empty,
    "raise": _stream_raise,
    "set_code": _stream_set_code,
    "set_code_int": _stream_set_code_int,
    "abort_first": _stream_abort_first,
    "metadata": _stream_metadata,
    "headers_first": _headers_only,
    "none": _stream_none,
}


@pytest.mark.parametrize("case", STREAM_CASES)
def test_stream_matches_grpcio(pb: tuple[Any, Any], case: str) -> None:
    expected = asyncio.run(_stream(pb, "grpcio", STREAM_CASES[case]))
    assert asyncio.run(_stream(pb, "native", STREAM_CASES[case])) == expected


@pytest.fixture(params=["grpcio", "native"])
def kind(request: pytest.FixtureRequest) -> str:
    return request.param


def _servers(pb: tuple[Any, Any], kind: str, servicer: Any, **kwargs: Any) -> Any:
    async def start() -> tuple[aio.Server, int]:
        server = SERVERS[kind](**kwargs)
        pb[1].add_SServicer_to_server(servicer, server)
        port = server.add_insecure_port("127.0.0.1:0")
        await server.start()
        return server, port

    return start()


def test_write_and_sync_handlers(pb: tuple[Any, Any], kind: str) -> None:
    a_pb2, a_pb2_grpc = pb

    class Servicer(a_pb2_grpc.SServicer):
        def Unary(self, request: Any, context: Any) -> Any:
            return a_pb2.Msg(s=request.s + "!")

        async def Stream(self, request: Any, context: aio.ServicerContext) -> None:
            await context.write(a_pb2.Msg(s="a"))
            await context.write(a_pb2.Msg(s="b"))

    class SyncStream(a_pb2_grpc.SServicer):
        def Stream(self, request: Any, context: Any) -> Iterator[Any]:
            yield a_pb2.Msg(s="1")
            yield a_pb2.Msg(s="2")

    async def run() -> tuple[str, list[str], list[str]]:
        pool = futures.ThreadPoolExecutor(2)
        server, port = await _servers(pb, kind, Servicer(), migration_thread_pool=pool)
        sync_server, sync_port = await _servers(
            pb, kind, SyncStream(), migration_thread_pool=pool
        )
        try:
            async with aio.insecure_channel(f"127.0.0.1:{port}") as channel:
                stub = a_pb2_grpc.SStub(channel)
                unary = (await stub.Unary(a_pb2.Msg(s="u"), timeout=5)).s
                written = [m.s async for m in stub.Stream(a_pb2.Msg(), timeout=5)]
            async with aio.insecure_channel(f"127.0.0.1:{sync_port}") as channel:
                stub = a_pb2_grpc.SStub(channel)
                iterated = [m.s async for m in stub.Stream(a_pb2.Msg(), timeout=5)]
            return unary, written, iterated
        finally:
            await server.stop(None)
            await sync_server.stop(None)

    assert asyncio.run(run()) == ("u!", ["a", "b"], ["1", "2"])


def test_client_cancel_cancels_the_handler(pb: tuple[Any, Any], kind: str) -> None:
    a_pb2, a_pb2_grpc = pb
    seen: dict[str, Any] = {}

    class Servicer(a_pb2_grpc.SServicer):
        async def Unary(self, request: Any, context: aio.ServicerContext) -> Any:
            seen["done_before"] = context.done()
            context.add_done_callback(
                lambda c: seen.setdefault("callback", c is context)
            )
            try:
                await asyncio.sleep(5)
            except asyncio.CancelledError:
                seen["unary"] = "cancelled"
                raise
            return request

        async def Stream(
            self, request: Any, context: aio.ServicerContext
        ) -> AsyncIterator[Any]:
            try:
                yield request
                await asyncio.sleep(5)
                yield request
            except asyncio.CancelledError:
                seen["stream"] = "cancelled"
                raise

    async def run() -> None:
        server, port = await _servers(pb, kind, Servicer())
        try:
            async with aio.insecure_channel(f"127.0.0.1:{port}") as channel:
                stub = a_pb2_grpc.SStub(channel)
                call = stub.Unary(a_pb2.Msg(s="x"), timeout=10)
                await asyncio.sleep(0.2)
                call.cancel()
                stream = stub.Stream(a_pb2.Msg(s="x"), timeout=10)
                await stream.read()
                stream.cancel()
                deadline = stub.Unary(a_pb2.Msg(s="x"), timeout=0.3)
                with pytest.raises(aio.AioRpcError) as info:
                    await deadline
                assert info.value.code() is grpc.StatusCode.DEADLINE_EXCEEDED
                await asyncio.sleep(0.3)
        finally:
            await server.stop(None)

    asyncio.run(run())
    assert seen == {
        "done_before": False,
        "callback": True,
        "unary": "cancelled",
        "stream": "cancelled",
    }


def test_context_details(pb: tuple[Any, Any], kind: str) -> None:
    seen: dict[str, Any] = {}

    async def look(request: Any, context: aio.ServicerContext) -> Any:
        seen["remaining"] = context.time_remaining()
        seen["peer"] = context.peer()
        seen["auth"] = context.auth_context()
        seen["code"] = context.code()
        seen["cancelled"] = context.cancelled()
        return request

    assert asyncio.run(_unary(pb, kind, look))[0] == "OK"
    assert 0 < seen["remaining"] <= 5.5
    assert seen["peer"].startswith("ipv4:127.0.0.1:")
    assert seen["auth"] == {
        "transport_security_type": [b"insecure"],
        "security_level": [b"TSI_SECURITY_NONE"],
    }
    assert seen["code"] is None and seen["cancelled"] is False
    assert asyncio.run(_unary(pb, kind, look, host="[::1]"))[0] == "OK"
    assert seen["peer"].startswith("ipv6:%5B::1%5D:")


def test_initial_metadata_twice_is_rejected(pb: tuple[Any, Any], kind: str) -> None:
    errors = []

    async def twice(request: Any, context: aio.ServicerContext) -> Any:
        await context.send_initial_metadata((("x-a", "1"),))
        try:
            await context.send_initial_metadata((("x-b", "2"),))
        except aio.UsageError as exc:
            errors.append(str(exc))
        return request

    assert asyncio.run(_unary(pb, kind, twice))[0] == "OK"
    assert errors == ["Send initial metadata failed: already sent"]


def test_start_on_a_running_or_stopped_server(pb: tuple[Any, Any], kind: str) -> None:
    async def run() -> None:
        server, _ = await _serve(pb, kind)
        await server.start()
        await server.stop(None)
        with pytest.raises(aio.UsageError, match="Server not in ready state"):
            await server.start()

    asyncio.run(run())


def test_stop_with_grace(pb: tuple[Any, Any], kind: str) -> None:
    a_pb2, a_pb2_grpc = pb

    started: list[str] = []

    class Servicer(a_pb2_grpc.SServicer):
        async def Unary(self, request: Any, context: aio.ServicerContext) -> Any:
            started.append(request.s)
            await asyncio.sleep(float(request.s))
            return request

    async def run() -> tuple[str, grpc.StatusCode, bool, bool]:
        server, port = await _servers(pb, kind, Servicer())
        async with aio.insecure_channel(f"127.0.0.1:{port}") as channel:
            stub = a_pb2_grpc.SStub(channel)
            quick = stub.Unary(a_pb2.Msg(s="0.2"), timeout=10)
            slow = stub.Unary(a_pb2.Msg(s="5"), timeout=10)
            while len(started) < 2:
                await asyncio.sleep(0.01)
            assert await server.wait_for_termination(0.01) is True
            await server.stop(0.5)
            with pytest.raises(aio.AioRpcError) as info:
                await slow
            return (
                (await quick).s,
                info.value.code(),
                await server.wait_for_termination(0.01),
                isinstance(server, aio.Server),
            )

    quick, code, timed_out, is_aio = asyncio.run(run())
    assert (quick, timed_out, is_aio) == ("0.2", False, True)
    assert code in (grpc.StatusCode.CANCELLED, grpc.StatusCode.UNAVAILABLE)


def test_reflection_and_health(pb: tuple[Any, Any]) -> None:
    reflection = pytest.importorskip("grpc_reflection.v1alpha.reflection")
    reflection_pb2 = pytest.importorskip("grpc_reflection.v1alpha.reflection_pb2")
    reflection_pb2_grpc = pytest.importorskip(
        "grpc_reflection.v1alpha.reflection_pb2_grpc"
    )
    health = pytest.importorskip("grpc_health.v1.health")
    health_pb2 = pytest.importorskip("grpc_health.v1.health_pb2")
    health_pb2_grpc = pytest.importorskip("grpc_health.v1.health_pb2_grpc")
    a_pb2, a_pb2_grpc = pb

    names = ("a.S", "grpc.health.v1.Health", reflection.SERVICE_NAME)

    async def run() -> None:
        server = pygrpc_native.aio.server()
        a_pb2_grpc.add_SServicer_to_server(a_pb2_grpc.SServicer(), server)
        health_pb2_grpc.add_HealthServicer_to_server(
            health.aio.HealthServicer(), server
        )
        reflection.enable_server_reflection(names, server)
        port = server.add_insecure_port("127.0.0.1:0")
        await server.start()
        try:
            async with aio.insecure_channel(f"127.0.0.1:{port}") as channel:
                check = await health_pb2_grpc.HealthStub(channel).Check(
                    health_pb2.HealthCheckRequest(), timeout=5
                )
                assert check.status == health_pb2.HealthCheckResponse.SERVING
                stub = reflection_pb2_grpc.ServerReflectionStub(channel)
                request = reflection_pb2.ServerReflectionRequest(list_services="")
                responses = [
                    r
                    async for r in stub.ServerReflectionInfo(iter([request]), timeout=5)
                ]
                listed = {s.name for s in responses[0].list_services_response.service}
                assert listed == set(names)
        finally:
            await server.stop(None)

    asyncio.run(run())


def test_bidi_through_the_fallback(pb: tuple[Any, Any]) -> None:
    a_pb2, a_pb2_grpc = pb

    async def run() -> list[str]:
        server, port = await _serve(pb, "native")
        try:
            async with aio.insecure_channel(f"127.0.0.1:{port}") as channel:
                chat = a_pb2_grpc.SStub(channel).Chat(
                    iter([a_pb2.Msg(s="p"), a_pb2.Msg(s="q")]), timeout=5
                )
                return [m.s async for m in chat]
        finally:
            await server.stop(None)

    assert asyncio.run(run()) == ["P", "Q"]


def test_uvloop(pb: tuple[Any, Any]) -> None:
    uvloop = pytest.importorskip("uvloop")
    a_pb2, a_pb2_grpc = pb

    async def run() -> list[str]:
        server, port = await _serve(pb, "native", stream=_two)
        try:
            async with aio.insecure_channel(f"127.0.0.1:{port}") as channel:
                stub = a_pb2_grpc.SStub(channel)
                unary = await stub.Unary(a_pb2.Msg(s="u"), timeout=5)
                streamed = [m.s async for m in stub.Stream(a_pb2.Msg(s="s"), timeout=5)]
                return [unary.s, *streamed]
        finally:
            await server.stop(None)

    assert uvloop.run(run()) == ["u", "s", "s"]


def test_unsupported_features_are_rejected() -> None:
    with pytest.raises(ValueError, match="unsupported server option"):
        pygrpc_native.aio.server(options=[("grpc.max_connection_idle_ms", 1000)])
    server = pygrpc_native.aio.server()
    with pytest.raises(NotImplementedError):
        server.add_secure_port("127.0.0.1:0", cast(Any, object()))
    server.add_insecure_port("127.0.0.1:0")
    with pytest.raises(RuntimeError, match="one port"):
        server.add_insecure_port("127.0.0.1:0")
    asyncio.run(server.stop(None))


def test_module_attribute() -> None:
    assert pygrpc_native.aio.Server.__module__ == "pygrpc_native.aio"
    assert isinstance(pygrpc_native.aio.server(), aio.Server)


@pytest.mark.parametrize(
    "code",
    ["from pygrpc_native import aio", "import pygrpc_native; pygrpc_native.aio"],
)
def test_aio_reached_from_the_package(code: str) -> None:
    # A fresh interpreter: this one has imported pygrpc_native.aio already.
    subprocess.run([sys.executable, "-c", code], check=True, timeout=60)


def test_slow_reader_gets_every_message(pb: tuple[Any, Any]) -> None:
    a_pb2, a_pb2_grpc = pb
    chunk = "x" * (1 << 20)

    async def many(request: Any, context: aio.ServicerContext) -> AsyncIterator[Any]:
        for i in range(16):
            yield a_pb2.Msg(s=f"{i}{chunk}")

    async def run() -> list[str]:
        server, port = await _serve(pb, "native", stream=many)
        try:
            async with aio.insecure_channel(f"127.0.0.1:{port}") as channel:
                received = []
                call = a_pb2_grpc.SStub(channel).Stream(a_pb2.Msg(), timeout=10)
                async for message in call:
                    received.append(message.s[: -len(chunk)])
                    await asyncio.sleep(0.02)
                return received
        finally:
            await server.stop(None)

    assert asyncio.run(run()) == [str(i) for i in range(16)]


def test_streams_written_together_keep_their_order(pb: tuple[Any, Any]) -> None:
    a_pb2, a_pb2_grpc = pb
    # Every third message is larger than a stream may queue, so writes have
    # to wait for room while messages written after a pause are held with
    # them until the end of the loop pass.
    big = "x" * (300 << 10)

    async def numbered(
        request: Any, context: aio.ServicerContext
    ) -> AsyncIterator[Any]:
        for i in range(60):
            if i % 4 == 1:
                await asyncio.sleep(0.002)
            yield a_pb2.Msg(s=f"{i}:{big if i % 3 == 0 else ''}")

    async def read(stub: Any, slow: bool) -> list[str]:
        received = []
        async for message in stub.Stream(a_pb2.Msg(), timeout=20):
            received.append(message.s.split(":")[0])
            if slow:
                await asyncio.sleep(0.005)
        return received

    async def run() -> list[list[str]]:
        server, port = await _serve(pb, "native", stream=numbered)
        try:
            async with aio.insecure_channel(f"127.0.0.1:{port}") as channel:
                stub = a_pb2_grpc.SStub(channel)
                return await asyncio.gather(*(read(stub, i % 2 == 0) for i in range(8)))
        finally:
            await server.stop(None)

    expected = [str(i) for i in range(60)]
    assert asyncio.run(run()) == [expected] * 8


def test_held_message_goes_out_before_a_failed_send(pb: tuple[Any, Any]) -> None:
    a_pb2, a_pb2_grpc = pb

    async def then_too_big(
        request: Any, context: aio.ServicerContext
    ) -> AsyncIterator[Any]:
        yield a_pb2.Msg(s="0")
        # Written after a pause, so it's held when the next one fails.
        await asyncio.sleep(0.01)
        yield a_pb2.Msg(s="1")
        yield a_pb2.Msg(s="x" * 1000)

    async def run() -> tuple[list[str], Any]:
        server, port = await _serve(
            pb,
            "native",
            stream=then_too_big,
            options=[("grpc.max_send_message_length", 100)],
        )
        received = []
        try:
            async with aio.insecure_channel(f"127.0.0.1:{port}") as channel:
                call = a_pb2_grpc.SStub(channel).Stream(a_pb2.Msg(), timeout=10)
                try:
                    async for message in call:
                        received.append(message.s)
                except aio.AioRpcError as e:
                    return received, e.code()
        finally:
            await server.stop(None)
        return received, None

    assert asyncio.run(run()) == (["0", "1"], grpc.StatusCode.RESOURCE_EXHAUSTED)


def test_client_leaving_a_blocked_stream_cancels_the_handler(
    pb: tuple[Any, Any], kind: str
) -> None:
    a_pb2, a_pb2_grpc = pb
    chunk = "x" * (1 << 20)
    ended = asyncio.Event()

    async def endless(request: Any, context: aio.ServicerContext) -> AsyncIterator[Any]:
        try:
            while True:
                yield a_pb2.Msg(s=chunk)
        finally:
            ended.set()

    async def run() -> None:
        server, port = await _serve(pb, kind, stream=endless)
        try:
            async with aio.insecure_channel(f"127.0.0.1:{port}") as channel:
                call = a_pb2_grpc.SStub(channel).Stream(a_pb2.Msg(), timeout=10)
                await call.read()
                # Let the unread writes back up.
                await asyncio.sleep(0.3)
                call.cancel()
            await asyncio.wait_for(ended.wait(), 5)
        finally:
            await server.stop(None)

    asyncio.run(run())


def test_cancelling_blocked_streams_keeps_serving(pb: tuple[Any, Any]) -> None:
    a_pb2, a_pb2_grpc = pb
    chunk = "x" * (1 << 20)

    async def endless(request: Any, context: aio.ServicerContext) -> AsyncIterator[Any]:
        while True:
            yield a_pb2.Msg(s=chunk)

    async def run() -> list[Any]:
        errors: list[Any] = []
        asyncio.get_running_loop().set_exception_handler(
            lambda loop, context: errors.append(context)
        )
        server, port = await _serve(pb, "native", stream=endless)
        try:
            async with aio.insecure_channel(f"127.0.0.1:{port}") as channel:
                stub = a_pb2_grpc.SStub(channel)
                calls = [stub.Stream(a_pb2.Msg(), timeout=10) for _ in range(16)]
                for call in calls:
                    await call.read()
                # Let the unread writes back up, then leave them all at once,
                # so the cancels and the parked writes' events come together.
                await asyncio.sleep(0.3)
                for call in calls:
                    call.cancel()
                await asyncio.sleep(0.3)
                await stub.Unary(a_pb2.Msg(s="still"), timeout=5)
        finally:
            await server.stop(None)
        return errors

    assert asyncio.run(run()) == []


def test_sync_handlers_get_grpcio_sync_context() -> None:
    """Handlers that aren't coroutines get grpcio's sync context: abort()
    returns, and the call ends with its status once the handler does."""
    called: list[str] = []

    def abort(request: bytes, context: Any) -> bytes:
        context.abort(grpc.StatusCode.PERMISSION_DENIED, "denied")
        return b"ignored"

    def status(request: bytes, context: Any) -> bytes:
        context.set_code(grpc.StatusCode.NOT_FOUND)
        context.set_details("missing")
        return b""

    def metadata(request: bytes, context: Any) -> bytes:
        context.send_initial_metadata([("x-a", "1")])
        context.set_trailing_metadata([("x-b", "2")])
        context.add_callback(lambda: called.append("done"))
        return request

    def extra(request: bytes, context: Any) -> bytes:
        # Not on grpcio's sync context, but it worked here before.
        return str(context.cancelled()).encode()

    def stream_abort(request: bytes, context: Any) -> Iterator[bytes]:
        yield b"1"
        context.abort(grpc.StatusCode.ABORTED, "stop")
        yield b"2"
        yield b"3"

    handlers = {
        "Abort": grpc.unary_unary_rpc_method_handler(abort),
        "Status": grpc.unary_unary_rpc_method_handler(status),
        "Metadata": grpc.unary_unary_rpc_method_handler(metadata),
        "StreamAbort": grpc.unary_stream_rpc_method_handler(stream_abort),
        "Extra": grpc.unary_unary_rpc_method_handler(extra),
    }

    def calls(port: int, stream_abort: bool) -> dict[str, Any]:
        out: dict[str, Any] = {}
        with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
            for name in ("Abort", "Status"):
                try:
                    out[name] = channel.unary_unary(f"/s.S/{name}")(b"x", timeout=5)
                except grpc.RpcError as error:
                    out[name] = (error.code(), error.details())
            response, call = channel.unary_unary("/s.S/Metadata").with_call(
                b"x", timeout=5
            )
            out["Metadata"] = (
                response,
                [m for m in call.initial_metadata() if m[0].startswith("x-")],
                [m for m in call.trailing_metadata() if m[0].startswith("x-")],
            )
            if stream_abort:
                out["Extra"] = channel.unary_unary("/s.S/Extra")(b"x", timeout=5)
                received: list[bytes] = []
                try:
                    for message in channel.unary_stream("/s.S/StreamAbort")(
                        b"x", timeout=5
                    ):
                        received.append(message)
                except grpc.RpcError as error:
                    out["StreamAbort"] = (received, error.code(), error.details())
        return out

    async def run(factory: Any, stream_abort: bool) -> dict[str, Any]:
        server = factory(futures.ThreadPoolExecutor(2))
        # Both, as generated code does: grpc.aio ignores registered handlers
        # before grpcio 1.84.
        server.add_generic_rpc_handlers(
            (grpc.method_handlers_generic_handler("s.S", handlers),)
        )
        server.add_registered_method_handlers("s.S", handlers)
        port = server.add_insecure_port("127.0.0.1:0")
        await server.start()
        try:
            return await asyncio.get_running_loop().run_in_executor(
                None, calls, port, stream_abort
            )
        finally:
            await server.stop(None)

    native = asyncio.run(run(pygrpc_native.aio.server, stream_abort=True))
    native_called, called[:] = list(called), []
    # grpcio 1.84 never ends a sync stream that aborts: its client waits for
    # the deadline and the handler's thread can hang, so only check it here.
    assert native.pop("StreamAbort") == ([b"1"], grpc.StatusCode.ABORTED, "stop")
    assert native.pop("Extra") == b"False"
    assert native == asyncio.run(run(aio.server, stream_abort=False))
    assert native_called == called == ["done"]
    assert native["Abort"] == (grpc.StatusCode.PERMISSION_DENIED, "denied")


BAD_METADATA = (("X-Upper", "x"),)


async def _bad_initial(request: Any, context: aio.ServicerContext) -> Any:
    await context.send_initial_metadata(BAD_METADATA)
    return request


async def _bad_trailing(request: Any, context: aio.ServicerContext) -> Any:
    context.set_trailing_metadata(BAD_METADATA)
    return request


async def _bad_trailing_abort(request: Any, context: aio.ServicerContext) -> Any:
    context.set_trailing_metadata(BAD_METADATA)
    await context.abort(grpc.StatusCode.NOT_FOUND, "missing")


async def _bad_stream_initial(
    request: Any, context: aio.ServicerContext
) -> AsyncIterator[Any]:
    await context.send_initial_metadata(BAD_METADATA)
    yield request


async def _bad_stream_trailing(
    request: Any, context: aio.ServicerContext
) -> AsyncIterator[Any]:
    context.set_trailing_metadata(BAD_METADATA)
    yield request


@pytest.mark.parametrize("behavior", [_bad_initial, _bad_trailing, _bad_trailing_abort])
def test_unary_with_invalid_metadata_fails(
    pb: tuple[Any, Any], behavior: Behavior
) -> None:
    # Recent grpcio fails these calls the same way; grpcio 1.64 hangs on them.
    assert asyncio.run(_unary(pb, "native", behavior)) == (
        grpc.StatusCode.INTERNAL,
        "Invalid metadata",
        [],
    )


@pytest.mark.parametrize(
    ("behavior", "received"),
    [(_bad_stream_initial, []), (_bad_stream_trailing, ["x"])],
)
def test_stream_with_invalid_metadata_fails(
    pb: tuple[Any, Any], behavior: Behavior, received: list[str]
) -> None:
    assert asyncio.run(_stream(pb, "native", behavior)) == (
        received,
        grpc.StatusCode.INTERNAL,
        "Invalid metadata",
        [],
    )


def test_ascii_bytes_on_a_text_key_go_out_as_text(
    pb: tuple[Any, Any], kind: str
) -> None:
    async def trailing(request: Any, context: aio.ServicerContext) -> Any:
        context.set_trailing_metadata((("x-bytes", b"abc"),))
        return request

    outcome = asyncio.run(_unary(pb, kind, trailing))
    assert outcome == ("OK", "x", [], [("x-bytes", "abc")])


def _echo_server() -> tuple[pygrpc_native.aio.Server, int]:
    async def echo(request: bytes, context: aio.ServicerContext) -> bytes:
        return request

    server = pygrpc_native.aio.server()
    server.add_registered_method_handlers(
        "t.S", {"U": grpc.unary_unary_rpc_method_handler(echo)}
    )
    return server, server.add_insecure_port("127.0.0.1:0")


def test_calls_queued_past_one_poll_are_all_answered() -> None:
    async def main() -> None:
        server, port = _echo_server()
        await server.start()
        try:
            with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
                call = channel.unary_unary("/t.S/U")
                pending = [call.future(bytes([i])) for i in range(200)]
                # Block the loop so the calls queue up behind it.
                time.sleep(1)
                loop = asyncio.get_running_loop()
                replies = await asyncio.gather(
                    *(loop.run_in_executor(None, f.result, 10) for f in pending)
                )
            assert replies == [bytes([i]) for i in range(200)]
        finally:
            await server.stop(None)

    asyncio.run(main())


def test_a_server_whose_loop_closes_without_stop_stops_serving() -> None:
    server, port = _echo_server()
    loop = asyncio.new_event_loop()
    loop.run_until_complete(server.start())
    loop.close()
    with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
        with pytest.raises(grpc.RpcError) as info:
            channel.unary_unary("/t.S/U")(b"x", timeout=10)
    assert info.value.code() == grpc.StatusCode.UNAVAILABLE
