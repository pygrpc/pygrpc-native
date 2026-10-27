import importlib
from typing import TYPE_CHECKING

from pygrpc_native._native import (
    EVENT_CALL,
    EVENT_CANCEL,
    STREAM_CLOSED,
    STREAM_FULL,
    STREAM_SENT,
    NativeGrpcFront,
    __version__,
)

if TYPE_CHECKING:
    from pygrpc_native import aio
    from pygrpc_native._server import Server, server

__all__ = [
    "Server",
    "EVENT_CALL",
    "EVENT_CANCEL",
    "STREAM_CLOSED",
    "STREAM_FULL",
    "STREAM_SENT",
    "NativeGrpcFront",
    "__version__",
    "aio",
    "server",
]


def __getattr__(name: str):
    # The grpcio-compatible servers need grpcio; the low-level API doesn't.
    # import_module, not "from pygrpc_native import ...": that looks the name
    # up on this module first and lands back here.
    if name in ("Server", "server"):
        return getattr(importlib.import_module("pygrpc_native._server"), name)
    if name == "aio":
        return importlib.import_module("pygrpc_native.aio")
    raise AttributeError(f"module 'pygrpc_native' has no attribute {name!r}")
