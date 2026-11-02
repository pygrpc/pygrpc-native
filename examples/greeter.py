"""Shared generic gRPC handler and wire format for the runnable examples."""

from collections.abc import Callable
from typing import Any

import grpc

SERVICE_NAME = "examples.Greeter"
METHOD_NAME = f"/{SERVICE_NAME}/Greet"


def serialize(value: str) -> bytes:
    return value.encode("utf-8")


def deserialize(value: bytes) -> str:
    return value.decode("utf-8")


def greeting(name: bytes) -> bytes:
    return f"Hello, {name.decode('utf-8')}!".encode("utf-8")


def make_handler(greet: Callable[..., Any]) -> grpc.GenericRpcHandler:
    return grpc.method_handlers_generic_handler(
        SERVICE_NAME,
        {
            "Greet": grpc.unary_unary_rpc_method_handler(greet)
        },
    )
