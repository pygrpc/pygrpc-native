"""Echo benchmark for grpc sync, grpc aio and pygrpc-native.

Usage: python benchmarks/echo.py <sync|aio|native> <procs> <threads>

DUR sets the seconds measured (default 5) and SIZE the payload bytes
(default 64).
"""

import multiprocessing as mp
import os
import socket
import sys
import threading
import time

import grpc

PAYLOAD = os.urandom(int(os.environ.get("SIZE", "64")))
DUR = float(os.environ.get("DUR", "5"))
METHOD = "/bench.Echo/Unary"


def ident(b: bytes) -> bytes:
    return b


def serve(kind: str, port: int) -> None:
    target = f"127.0.0.1:{port}"
    if kind == "native":
        from pygrpc_native import EVENT_CALL, NativeGrpcFront

        front = NativeGrpcFront(target, [(METHOD, False)])
        front.start()
        while (events := front.recv_batch(256)) is not None:
            for ev_kind, call_id, _, payload, _ in events:
                if ev_kind == EVENT_CALL:
                    front.respond(call_id, payload)
        return
    if kind == "aio":
        import asyncio

        async def main() -> None:
            async def unary(req: bytes, ctx: object) -> bytes:
                return req

            h = grpc.method_handlers_generic_handler(
                "bench.Echo",
                {"Unary": grpc.unary_unary_rpc_method_handler(unary, ident, ident)},
            )
            server = grpc.aio.server()
            server.add_generic_rpc_handlers((h,))
            server.add_insecure_port(target)
            await server.start()
            await server.wait_for_termination()

        asyncio.run(main())
        return
    from concurrent.futures import ThreadPoolExecutor

    def unary(req: bytes, ctx: object) -> bytes:
        return req

    handler = grpc.method_handlers_generic_handler(
        "bench.Echo",
        {"Unary": grpc.unary_unary_rpc_method_handler(unary, ident, ident)},
    )
    server = grpc.server(ThreadPoolExecutor(64))
    server.add_generic_rpc_handlers((handler,))
    server.add_insecure_port(target)
    server.start()
    server.wait_for_termination()


def client(port: int, threads: int, out: mp.Queue, ready: mp.Queue) -> None:
    channel = grpc.insecure_channel(f"127.0.0.1:{port}")
    grpc.channel_ready_future(channel).result(30)
    call = channel.unary_unary(METHOD, ident, ident)
    counts = [0] * threads
    stop = threading.Event()

    def work(i: int) -> None:
        while not stop.is_set():
            call(PAYLOAD)
            counts[i] += 1

    workers = [
        threading.Thread(target=work, args=(i,), daemon=True) for i in range(threads)
    ]
    call(PAYLOAD)
    ready.put(1)
    cpu0, t0 = time.process_time(), time.perf_counter()
    for w in workers:
        w.start()
    time.sleep(DUR)
    stop.set()
    for w in workers:
        w.join()
    out.put((sum(counts), time.perf_counter() - t0, time.process_time() - cpu0))
    out.close()
    out.join_thread()
    os._exit(0)


def main() -> None:
    kind, procs, threads = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    ctx = mp.get_context("spawn")
    server = ctx.Process(target=serve, args=(kind, port), daemon=True)
    server.start()
    out, ready = ctx.Queue(), ctx.Queue()
    clients = [
        ctx.Process(target=client, args=(port, threads, out, ready))
        for _ in range(procs)
    ]
    for c in clients:
        c.start()
    for _ in clients:
        ready.get(timeout=60)
    results = [out.get(timeout=DUR + 60) for _ in clients]
    server.kill()
    calls = sum(r[0] for r in results)
    rate = sum(r[0] / r[1] for r in results)
    cpu_us = 1e6 * sum(r[2] for r in results) / calls
    print(
        f"{kind:7s} {procs}x{threads}: {rate:9,.0f} calls/s  client CPU {cpu_us:6.1f} us/call",
        flush=True,
    )


if __name__ == "__main__":
    main()
