"""Runs the gRPC interop cases with grpc-go's and grpcio's clients against grpcio
and pygrpc-native servers. A case that passes on grpcio has to pass on
pygrpc-native too, unless expected_diffs.toml lists it.

    python interop/run.py --go-client PATH [--table table.md]
"""

import argparse
import socket
import subprocess
import sys
import tempfile
import time
import tomllib
from pathlib import Path

HERE = Path(__file__).parent
CASE_TIMEOUT = 20

# Client-streaming and bidi methods go to the grpcio fallback server.
CASES = {
    "empty_unary": "native",
    "large_unary": "native",
    "server_streaming": "native",
    "status_code_and_message": "native + fallback",
    "special_status_message": "native",
    "custom_metadata": "native + fallback",
    "unimplemented_method": "native",
    "unimplemented_service": "fallback",
    "client_streaming": "fallback",
    "ping_pong": "fallback",
    "empty_stream": "fallback",
    "cancel_after_begin": "fallback",
    "cancel_after_first_response": "fallback",
    "timeout_on_sleeping_server": "fallback",
}
MODES = ["sync", "aio"]
CLIENTS = ["go", "python"]


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _wait_ready(port: int, server: subprocess.Popen, log: Path) -> None:
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if server.poll() is not None:
            raise RuntimeError(
                f"server exited with {server.returncode}:\n{log.read_text()}"
            )
        try:
            socket.create_connection(("127.0.0.1", port), timeout=1).close()
            return
        except OSError:
            time.sleep(0.1)
    raise RuntimeError("server did not start")


def _client_command(client: str, go_client: str, port: int, case: str) -> list[str]:
    if client == "go":
        return [
            go_client,
            "-server_host=127.0.0.1",
            f"-server_port={port}",
            f"-test_case={case}",
        ]
    return [
        sys.executable,
        str(HERE / "python_client.py"),
        f"--port={port}",
        f"--test_case={case}",
    ]


def _run_case(command: list[str]) -> dict:
    try:
        done = subprocess.run(
            command, capture_output=True, text=True, timeout=CASE_TIMEOUT
        )
    except subprocess.TimeoutExpired:
        return {"ok": False, "error": f"timed out after {CASE_TIMEOUT} s"}
    if done.returncode == 0:
        return {"ok": True}
    lines = (done.stderr or done.stdout).strip().splitlines()
    return {
        "ok": False,
        "error": lines[-1][:300] if lines else f"exit {done.returncode}",
    }


def run(go_client: str) -> dict:
    results: dict = {}
    for impl in ["grpcio", "native"]:
        for mode in MODES:
            port = _free_port()
            log = Path(
                tempfile.mkstemp(prefix=f"interop-{impl}-{mode}-", suffix=".log")[1]
            )
            with open(log, "wb") as out:
                server = subprocess.Popen(
                    [
                        sys.executable,
                        str(HERE / "server.py"),
                        f"--impl={impl}",
                        f"--mode={mode}",
                        f"--port={port}",
                    ],
                    stdout=out,
                    stderr=subprocess.STDOUT,
                )
            try:
                _wait_ready(port, server, log)
                for client in CLIENTS:
                    for case in CASES:
                        result = _run_case(
                            _client_command(client, go_client, port, case)
                        )
                        results[f"{impl}/{mode}/{client}/{case}"] = result
                        print(
                            f"{impl:6} {mode:4} {client:6} {case:28} {'ok' if result['ok'] else 'FAIL'}",
                            flush=True,
                        )
            finally:
                server.kill()
                server.wait()
    return results


def _expected_diffs() -> dict:
    with open(HERE / "expected_diffs.toml", "rb") as f:
        diffs = tomllib.load(f).get("diff", [])
    return {
        (mode, client, d["case"]): d
        for d in diffs
        for mode in d.get("modes", MODES)
        for client in d.get("clients", CLIENTS)
    }


def compare(results: dict) -> list[str]:
    expected = _expected_diffs()
    problems = []
    for mode in MODES:
        for client in CLIENTS:
            for case in CASES:
                base = results[f"grpcio/{mode}/{client}/{case}"]
                native = results[f"native/{mode}/{client}/{case}"]
                differs = base["ok"] and not native["ok"]
                listed = (mode, client, case) in expected
                if differs and not listed:
                    problems.append(
                        f"{mode}/{client}/{case}: fails on pygrpc-native: {native['error']}"
                    )
                elif listed and not differs:
                    problems.append(
                        f"{mode}/{client}/{case}: listed in expected_diffs.toml but no longer differs"
                    )
    return problems


def table(results: dict) -> str:
    expected = _expected_diffs()
    lines = [
        "| Case | Path | "
        + " | ".join(f"{c} client, {m}" for c in CLIENTS for m in MODES)
        + " |",
        "|---|---|" + "---|" * (len(CLIENTS) * len(MODES)),
    ]
    for case, path in CASES.items():
        cells = []
        for client in CLIENTS:
            for mode in MODES:
                base = results[f"grpcio/{mode}/{client}/{case}"]["ok"]
                native = results[f"native/{mode}/{client}/{case}"]["ok"]
                if native:
                    cells.append("pass")
                elif not base:
                    cells.append("fails on grpcio too")
                else:
                    issue = expected.get((mode, client, case), {}).get("issue")
                    cells.append(f"fail (#{issue})" if issue else "fail")
        lines.append(f"| `{case}` | {path} | " + " | ".join(cells) + " |")
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--go-client", required=True)
    parser.add_argument("--table")
    args = parser.parse_args()

    results = run(args.go_client)
    if args.table:
        Path(args.table).write_text(table(results))
    problems = compare(results)
    for problem in problems:
        print(problem)
    sys.exit(1 if problems else 0)


if __name__ == "__main__":
    main()
