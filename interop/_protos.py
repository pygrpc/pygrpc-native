"""Generates the interop test protos into a temp directory and puts it on sys.path."""

import sys
import tempfile
from pathlib import Path

from grpc_tools import protoc

PROTOS = Path(__file__).parent / "protos"


def load() -> None:
    out = tempfile.mkdtemp(prefix="pygrpc-interop-")
    files = [str(p.relative_to(PROTOS)) for p in sorted(PROTOS.rglob("*.proto"))]
    args = ["protoc", f"-I{PROTOS}", f"--python_out={out}", f"--grpc_python_out={out}"]
    if protoc.main(args + files) != 0:
        raise RuntimeError("protoc failed")
    sys.path.insert(0, out)
