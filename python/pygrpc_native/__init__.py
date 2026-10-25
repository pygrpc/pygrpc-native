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
    if name in ("Server", "server"):
        from pygrpc_native import _server

        return getattr(_server, name)
    if name == "aio":
        from pygrpc_native import aio

        return aio
    raise AttributeError(f"module 'pygrpc_native' has no attribute {name!r}")
