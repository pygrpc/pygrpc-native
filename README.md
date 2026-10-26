# pygrpc-native

A gRPC server for Python whose HTTP/2 and gRPC work runs in Rust, outside the
GIL. Your handlers stay in Python.

grpcio runs its own Python code for every call, under the same GIL as your
handlers. pygrpc-native does the framing, flow control, deadlines and
cancellation on a tokio runtime instead, and gives Python each call as an
event: an id, the method, the request bytes and who sent it. You decode the
protobuf, run the handler and send back the encoded response. Clients and the
wire format are the same as with grpcio.

```
grpc client ──HTTP/2──▶ tokio threads (no GIL)
                          │ path → method id
                          ▼
                    event queue ──▶ recv_batch() in Python
                          ◀── respond(call_id, bytes) from any thread
```

Calls reach Python in batches, so a burst takes one trip across the GIL
rather than one per call.

## Install

```bash
pip install pygrpc-native
```

Wheels are published for Linux (x86_64, aarch64) and macOS (arm64,
x86_64) and work on CPython 3.10 and later. To build from a checkout:

```bash
pip install maturin
maturin develop --release
```

## With grpcio servicers

`pygrpc_native.server()` takes the same arguments as `grpc.server()` and
serves servicers from grpcio-tools generated code (1.62 or later) unchanged:

```python
from concurrent import futures
import pygrpc_native

server = pygrpc_native.server(futures.ThreadPoolExecutor(16))
greeter_pb2_grpc.add_GreeterServicer_to_server(Greeter(), server)
server.add_insecure_port("[::]:50051")
server.start()
server.wait_for_termination()
```

Status codes, error messages, metadata, deadlines, cancellation and
`ServicerContext` behave as they do in grpcio.

Unary and server-streaming methods are served natively on one insecure port.
The rest, such as client-streaming and bidirectional methods, server
reflection, or handlers added only with `add_generic_rpc_handlers`, go to a
grpcio server on a Unix socket behind it. They work, but they aren't any
faster, and `peer()` in them is the socket.

Interceptors run as in grpcio, on the methods served here and on the fallback
server. Of the server options, the message size limits and the keepalive
settings (`grpc.keepalive_time_ms`, `grpc.keepalive_timeout_ms` and
`grpc.keepalive_permit_without_calls`) work as in grpcio, the limits on client
pings are accepted but not enforced, and what grpc-core ignores, such as
`grpc.max_message_length` or a value that isn't an integer, is ignored here
too. `maximum_concurrent_rpcs`, TLS, response compression and other options
raise an error rather than being ignored.

There are two known differences. If a unary handler calls
`send_initial_metadata()` and then fails, the metadata goes out with the
status, so the client sees it as trailing metadata. And a connection whose
client doesn't answer a keepalive ping is closed after
`grpc.keepalive_timeout_ms`, as gRPC's keepalive documentation describes,
while grpcio waits for `grpc.http2.ping_timeout_ms`, a minute by default.

`pygrpc_native.aio.server()` does the same for `grpc.aio.server()`:

```python
import pygrpc_native.aio

server = pygrpc_native.aio.server()
greeter_pb2_grpc.add_GreeterServicer_to_server(Greeter(), server)
server.add_insecure_port("[::]:50051")
await server.start()
await server.wait_for_termination()
```

Handlers run on the event loop that called `start()`, uvloop included.
`abort()` raises `grpc.aio.AbortError`, and a handler whose client cancels or
whose deadline passes gets `asyncio.CancelledError`, as in grpc.aio. Streams
can yield from an async generator or call `context.write()`, and handlers
that aren't coroutines run in `migration_thread_pool`. The fallback,
the unsupported options and the known differences are the same as
for the sync server.

## Low-level API

`NativeGrpcFront` is the layer under `server()`. Each call reaches Python as
an event that you answer by id:

```python
import threading
from pygrpc_native import EVENT_CALL, NativeGrpcFront

front = NativeGrpcFront("0.0.0.0:50051", [("/echo.Echo/Say", False)])
front.start()

def serve():
    while (events := front.recv_batch(64)) is not None:
        for kind, call_id, method_id, payload, client_key in events:
            if kind == EVENT_CALL:
                front.respond(call_id, payload)

threading.Thread(target=serve).start()
```

Streaming, call details, response metadata, bearer tokens and metrics are
covered in [docs/low-level-api.md](https://github.com/pygrpc/pygrpc-native/blob/main/docs/low-level-api.md).

## Compatibility

pygrpc-native follows semantic versioning, and the low-level API stays
compatible within 0.x as well. `tests/test_api.py` pins the 0.1 names,
signatures and event tuples of `NativeGrpcFront`: they won't change or go
away, and new parameters only come after the existing ones, with a default.
The keys `metrics()` returns won't be renamed or removed within 0.x either,
though new ones may be added. `server()` and `aio.server()` follow grpcio's
API, and any difference from grpcio's behavior is a bug unless it is listed in
this README. Changes are listed in [CHANGELOG.md](CHANGELOG.md).

## Limitations

- `NativeGrpcFront` serves unary and server-streaming methods only; other
  kinds can go through the fallback server. `server()` and `aio.server()`
  send them there on their own.
- No TLS yet. `NativeGrpcFront` checks bearer tokens itself; `server()` and
  `aio.server()` run grpcio interceptors instead.
- Like grpc-core, binding a Unix socket path replaces any socket file
  already there.
- The server never sets `SO_REUSEPORT`, so binding a port another server is
  using fails, where grpcio's default lets both bind it. `grpc.so_reuseport`
  set to 0 is accepted; other integers raise an error.
- Per connection: at most 1024 concurrent streams and 16 KiB of request
  metadata. A connection that hasn't finished the HTTP/2 preface after 120 s
  is dropped.

## Benchmarks

With an empty handler on an Apple M4 Pro, `pygrpc_native.server()` answers
about 1.5x the calls per second of `grpc.server()` from one client thread and
about 5x from 32. On a 40-vCPU Linux container the low-level API answered
about 12x at 32 threads. See [benchmarks](https://github.com/pygrpc/pygrpc-native/blob/main/benchmarks/README.md) for the
numbers and how they were measured.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Every commit needs a DCO sign-off
(`git commit -s`).

## License

Apache-2.0
