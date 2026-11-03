"""Posts the results of a Perf run on its pull request, or updates the comment
an earlier run posted. Run by perf-comment.yml, next to the run's results.json
and pr.json.

Both files come from a job that ran the pull request's code, so only numbers
and known names are read from them, and the comment only goes to a pull
request whose head is the commit the run tested.
"""

import json
import os
import re
import subprocess
import sys

MARKER = "<!-- perf-results -->"
METRICS = {"cpu_us": "CPU µs/call", "vcs": "context switches/call"}
VERDICTS = {"no change", "improvement", "suspect", "regression"}
FLAGGED = {"suspect", "regression"}
SCENARIO = re.compile(r"[\w .()-]{1,60}")
SHA = re.compile(r"[0-9a-f]{40}")
VERSION = re.compile(r"[0-9][0-9a-z.]{0,20}")


def gh(*args: str, body: dict | None = None) -> str:
    cmd = ["gh", "api", *args]
    if body is not None:
        cmd += ["--input", "-"]
    return subprocess.run(
        cmd,
        input=None if body is None else json.dumps(body),
        capture_output=True,
        text=True,
        check=True,
    ).stdout


def render(results: dict, base: str, head: str, run_url: str) -> str:
    env = results.get("environment", {})
    cpus = env.get("cpus")
    grpcio = str(env.get("grpcio", ""))
    rows, flagged, rounds = [], [], 0
    for scenario, metrics in results["results"].items():
        if not SCENARIO.fullmatch(scenario):
            raise ValueError(f"unexpected scenario name: {scenario!r}")
        for key, label in METRICS.items():
            m = metrics[key]
            b, h, change = float(m["base"]), float(m["head"]), float(m["change"])
            worse, verdict = int(m["worse"]), m["verdict"]
            if verdict not in VERDICTS:
                raise ValueError(f"unexpected verdict: {verdict!r}")
            rounds = len(m["head_runs"])
            if verdict in FLAGGED:
                flagged.append(f"{scenario}, {label} {change:+.1%} ({verdict})")
                verdict = f"**{verdict}**"
            rows.append(
                f"| {scenario} | {label} | {b:.2f} | {h:.2f} | {change:+.1%} "
                f"| {worse}/{rounds} | {verdict} |"
            )
    where = f"{cpus} CPUs" if isinstance(cpus, int) else "one runner"
    if VERSION.fullmatch(grpcio):
        where += f", grpcio {grpcio}"
    lines = [
        MARKER,
        "### Perf: server cost per call",
        "",
        f"Head `{head[:7]}` against base `{base[:7]}`, {rounds} rounds in random "
        f"order on one runner ({where}). [Run]({run_url})",
        "",
    ]
    if flagged:
        lines += ["Worth a look:", *(f"- {f}" for f in flagged), ""]
    lines += [
        "| Scenario | Metric | Base | Head | Change | Head worse | Verdict |",
        "|:--|:--|--:|--:|--:|--:|:--|",
        *rows,
    ]
    return "\n".join(lines) + "\n"


def main() -> None:
    repo, head, run_url = (
        os.environ["REPO"],
        os.environ["HEAD_SHA"],
        os.environ["RUN_URL"],
    )
    with open("pr.json") as f:
        meta = json.load(f)
    pr, base = meta.get("pr"), str(meta.get("base", ""))
    if not isinstance(pr, int) or not SHA.fullmatch(head) or not SHA.fullmatch(base):
        sys.exit("pr.json doesn't hold a pull request number and a base commit")
    if json.loads(gh(f"repos/{repo}/pulls/{pr}"))["head"]["sha"] != head:
        print(f"#{pr} isn't at {head[:7]} any more, so the results are stale")
        return
    with open("results.json") as f:
        body = render(json.load(f), base, head, run_url)
    ids = gh(
        "--paginate",
        f"repos/{repo}/issues/{pr}/comments",
        "--jq",
        '.[] | select(.user.login == "github-actions[bot]" '
        f'and (.body | startswith("{MARKER}"))) | .id',
    ).split()
    if ids:
        gh("-X", "PATCH", f"repos/{repo}/issues/comments/{ids[0]}", body={"body": body})
    else:
        gh("-X", "POST", f"repos/{repo}/issues/{pr}/comments", body={"body": body})


if __name__ == "__main__":
    main()
