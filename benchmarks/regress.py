"""Compare two pygrpc-native builds on the same machine.

Usage: python benchmarks/regress.py --base PYTHON --head PYTHON
           [--rounds N] [--json FILE] [--summary FILE]

Each PYTHON is an interpreter with one build installed. The clients run on
this interpreter, so both builds face the same grpcio client.

For every scenario, each round starts a fresh server per build, in random
order, and records server CPU time and voluntary context switches per call
over a fixed number of calls. A scenario regresses when the median head/base
ratio is over its threshold and head is worse in all but at most one round.
"""

import argparse
import asyncio
import glob
import json
import os
import random
import shutil
import socket
import statistics
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

UNARY = "/bench.Echo/Unary"
STREAM = "/bench.Echo/Stream"
STREAM_MESSAGES = 16
PAYLOAD = b"x" * 64
CLIENT_PROCS = 2
CLIENT_THREADS = 4
WARMUP_CALLS = 50

# scenario: (server kind, method, calls per client thread)
SCENARIOS = {
    "low-level unary": ("lowlevel", UNARY, 1500),
    "server() unary": ("sync", UNARY, 1500),
    "aio.server() unary": ("aio", UNARY, 1500),
    "server() stream": ("sync", STREAM, 300),
    "aio.server() stream": ("aio", STREAM, 300),
}

# Smallest change reported as a regression or an improvement.
THRESHOLDS = {"cpu_us": 0.10, "vcs": 0.05}
METRICS = {"cpu_us": "CPU µs/call", "vcs": "context switches/call"}


def serve(kind: str, port: int) -> None:
    import grpc

    target = f"127.0.0.1:{port}"
    if kind == "lowlevel":
        from pygrpc_native import EVENT_CALL, NativeGrpcFront

        front = NativeGrpcFront(target, [(UNARY, False)])
        front.start()
        print("ready", flush=True)
        while (events := front.recv_batch(256)) is not None:
            for event, call_id, _, payload, _ in events:
                if event == EVENT_CALL:
                    front.respond(call_id, payload)
        return

    if kind == "aio":
        import pygrpc_native.aio

        async def unary(request, context):
            return request

        async def stream(request, context):
            for _ in range(STREAM_MESSAGES):
                yield request

        async def main() -> None:
            server = pygrpc_native.aio.server()
            server.add_generic_rpc_handlers([_handler(grpc, unary, stream)])
            server.add_insecure_port(target)
            await server.start()
            print("ready", flush=True)
            await server.wait_for_termination()

        asyncio.run(main())
        return

    import pygrpc_native

    def sync_unary(request, context):
        return request

    def sync_stream(request, context):
        for _ in range(STREAM_MESSAGES):
            yield request

    server = pygrpc_native.server(ThreadPoolExecutor(8))
    server.add_generic_rpc_handlers([_handler(grpc, sync_unary, sync_stream)])
    server.add_insecure_port(target)
    server.start()
    print("ready", flush=True)
    server.wait_for_termination()


def _handler(grpc, unary, stream):
    return grpc.method_handlers_generic_handler(
        "bench.Echo",
        {
            "Unary": grpc.unary_unary_rpc_method_handler(unary),
            "Stream": grpc.unary_stream_rpc_method_handler(stream),
        },
    )


def client(method: str, port: int, calls: int, threads: int) -> None:
    import grpc

    channels = [grpc.insecure_channel(f"127.0.0.1:{port}") for _ in range(threads)]
    for channel in channels:
        grpc.channel_ready_future(channel).result(30)
    if method == STREAM:
        rpcs = [ch.unary_stream(method) for ch in channels]

        def call(rpc):
            for _ in rpc(PAYLOAD):
                pass
    else:
        rpcs = [ch.unary_unary(method) for ch in channels]

        def call(rpc):
            rpc(PAYLOAD)

    def run(rpc, n):
        for _ in range(n):
            call(rpc)

    for rpc in rpcs:
        run(rpc, WARMUP_CALLS)
    print("warm", flush=True)
    sys.stdin.readline()
    workers = [threading.Thread(target=run, args=(rpc, calls)) for rpc in rpcs]
    for w in workers:
        w.start()
    for w in workers:
        w.join()
    print("done", flush=True)


def _cpu_seconds(pid: int) -> float:
    with open(f"/proc/{pid}/stat") as f:
        fields = f.read().rsplit(")", 1)[1].split()
    return (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK")


def _voluntary_switches(pid: int) -> int:
    # Summed over threads: /proc/<pid>/status only counts the main thread.
    total = 0
    for path in glob.glob(f"/proc/{pid}/task/*/status"):
        try:
            with open(path) as f:
                for line in f:
                    if line.startswith("voluntary_ctxt_switches"):
                        total += int(line.split()[1])
        except FileNotFoundError:
            pass
    return total


def _pin(cores: str) -> list[str]:
    if shutil.which("taskset") and (os.cpu_count() or 0) >= 4:
        return ["taskset", "-c", cores]
    return []


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def measure(python: str, scenario: str) -> dict:
    kind, method, calls = SCENARIOS[scenario]
    port = _free_port()
    server = subprocess.Popen(
        [*_pin("0,1"), python, __file__, "serve", kind, str(port)],
        stdout=subprocess.PIPE,
        text=True,
    )
    clients = []
    try:
        line = _readline(server.stdout, 60)
        if line != "ready":
            raise RuntimeError(f"{python}: server for {scenario} didn't start")
        clients = [
            subprocess.Popen(
                [*_pin("2,3"), sys.executable, __file__, "client", method]
                + [str(port), str(calls), str(CLIENT_THREADS)],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                text=True,
            )
            for _ in range(CLIENT_PROCS)
        ]
        for c in clients:
            if _readline(c.stdout, 60) != "warm":
                raise RuntimeError(f"{python}: client for {scenario} didn't warm up")
        cpu0, vcs0 = _cpu_seconds(server.pid), _voluntary_switches(server.pid)
        for c in clients:
            c.stdin.write("go\n")
            c.stdin.flush()
        for c in clients:
            if _readline(c.stdout, 300) != "done":
                raise RuntimeError(f"{python}: client for {scenario} didn't finish")
        cpu1, vcs1 = _cpu_seconds(server.pid), _voluntary_switches(server.pid)
    finally:
        for p in [*clients, server]:
            p.kill()
            p.wait()
    total = CLIENT_PROCS * CLIENT_THREADS * calls
    return {"cpu_us": (cpu1 - cpu0) / total * 1e6, "vcs": (vcs1 - vcs0) / total}


def _readline(stream, timeout: float) -> str:
    result: list[str] = []
    reader = threading.Thread(target=lambda: result.append(stream.readline()))
    reader.daemon = True
    reader.start()
    reader.join(timeout)
    return result[0].strip() if result else ""


def verdict(ratios: list[float], threshold: float) -> str:
    median = statistics.median(ratios)
    worse = sum(r > 1 for r in ratios)
    better = sum(r < 1 for r in ratios)
    agree = len(ratios) - 1
    if median > 1 + threshold:
        return "regression" if worse >= agree else "suspect"
    if median < 1 - threshold:
        return "improvement" if better >= agree else "suspect"
    if worse >= agree and median > 1 + threshold / 2:
        return "suspect"
    return "no change"


def compare(base: str, head: str, rounds: int, seed: int) -> dict:
    rng = random.Random(seed)
    results = {}
    for scenario in SCENARIOS:
        runs: dict[str, list[dict]] = {"base": [], "head": []}
        for _ in range(rounds):
            order = [("base", base), ("head", head)]
            rng.shuffle(order)
            for name, python in order:
                runs[name].append(measure(python, scenario))
        metrics = {}
        for metric in METRICS:
            b = [r[metric] for r in runs["base"]]
            h = [r[metric] for r in runs["head"]]
            ratios = [y / x if x else float("inf") for x, y in zip(b, h)]
            metrics[metric] = {
                "base": statistics.median(b),
                "head": statistics.median(h),
                "change": statistics.median(ratios) - 1,
                "worse": sum(r > 1 for r in ratios),
                "verdict": verdict(ratios, THRESHOLDS[metric]),
                "base_runs": b,
                "head_runs": h,
            }
        results[scenario] = metrics
        print(f"{scenario}: done", file=sys.stderr, flush=True)
    return results


def _environment() -> dict:
    model = ""
    try:
        with open("/proc/cpuinfo") as f:
            model = next(
                (line.split(":", 1)[1].strip() for line in f if "model name" in line),
                "",
            )
    except OSError:
        pass
    import grpc

    return {
        "cpu": model,
        "cpus": os.cpu_count(),
        "pinned": bool(_pin("0")),
        "grpcio": grpc.__version__,
        "load": f"{CLIENT_PROCS} client processes x {CLIENT_THREADS} channels",
    }


def summary(results: dict, env: dict, rounds: int) -> str:
    lines = [
        "## Server cost per call, base vs head",
        "",
        f"{rounds} rounds in random order on one runner ({env['cpus']} CPUs, "
        f"{env['cpu']}), {env['load']}, grpcio {env['grpcio']}.",
        "",
        "| Scenario | Metric | Base | Head | Change | Head worse | Verdict |",
        "|:--|:--|--:|--:|--:|--:|:--|",
    ]
    for scenario, metrics in results.items():
        for metric, m in metrics.items():
            lines.append(
                f"| {scenario} | {METRICS[metric]} | {m['base']:.2f} | {m['head']:.2f} "
                f"| {m['change']:+.1%} | {m['worse']}/{rounds} | {m['verdict']} |"
            )
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", required=True)
    parser.add_argument("--head", required=True)
    parser.add_argument("--rounds", type=int, default=10)
    parser.add_argument("--seed", type=int, default=int(time.time()))
    parser.add_argument("--json")
    parser.add_argument("--summary")
    args = parser.parse_args()

    env = _environment()
    results = compare(args.base, args.head, args.rounds, args.seed)
    text = summary(results, env, args.rounds)
    print(text)
    if args.summary:
        with open(args.summary, "a") as f:
            f.write(text)
    if args.json:
        with open(args.json, "w") as f:
            json.dump({"environment": env, "seed": args.seed, "results": results}, f)
    if os.environ.get("GITHUB_ACTIONS"):
        for scenario, metrics in results.items():
            for metric, m in metrics.items():
                if m["verdict"] in ("regression", "suspect"):
                    print(
                        f"::warning title=Performance::{scenario}: {METRICS[metric]} "
                        f"{m['change']:+.1%} ({m['verdict']})"
                    )


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "serve":
        serve(sys.argv[2], int(sys.argv[3]))
    elif len(sys.argv) > 1 and sys.argv[1] == "client":
        client(sys.argv[2], int(sys.argv[3]), int(sys.argv[4]), int(sys.argv[5]))
    else:
        main()
