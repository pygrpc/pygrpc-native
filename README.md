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
`ServicerContext` behave as they do in grpcio. Unary and server-streaming
methods are served natively on one insecure port. Everything else, such as
client-streaming and bidirectional methods, server reflection or handlers
added only with `add_generic_rpc_handlers`, is passed to a grpcio server on a
Unix socket behind it, so it works but doesn't get faster, and `peer()` there
is the socket. Interceptors, `maximum_concurrent_rpcs`, TLS, response
compression and server options other than the message size limits and client
ping settings raise an error instead of being ignored. One difference
remains: a unary handler that calls `send_initial_metadata()` and then fails
sends the metadata with the status instead of before it, so the client sees
it as trailing metadata.

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
the unsupported options and the unary metadata difference are the same as
for the sync server.

## Low-level example

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

Any grpcio client can call it:

```python
import grpc

with grpc.insecure_channel("localhost:50051") as channel:
    print(channel.unary_unary("/echo.Echo/Say")(b"hello"))
```

In a real server `payload` is a serialized protobuf message: decode it with
the generated class, run the handler (in a thread pool, if it blocks) and
pass the serialized reply to `respond`.

## API

`NativeGrpcFront(target, methods, io_threads=2, client_key_metadata=None, call_details=False, delay_stream_headers=False, bearer_token_sha256=None, unauthenticated_message="Unauthenticated", metrics=False)`
binds `host:port` (every address the host resolves to), `unix:path` or
`unix:///path`. Port 0 picks a free port. `methods` lists
`(path, server_streaming)` pairs, where a path is `/package.Service/Method`
and a method's id is its index in the list. `add_methods` appends more
before `start()`, so the port can be bound before the methods are known.

| Member | Purpose |
|---|---|
| `bound_port` | the bound TCP port; 1 for a Unix socket, as grpcio reports it |
| `add_methods(methods)` | register more methods before `start()`; returns their ids |
| `set_fallback(path)` | forward unknown methods to a grpcio server on a Unix socket |
| `start()` | start accepting connections |
| `recv_batch(max_events, timeout_ms=None)` | wait for events with the GIL released; `None` once closed |
| `recv_batch_details(max_events, timeout_ms=None)` | the same, with call details as a sixth item |
| `respond(call_id, payload, initial_metadata=None, trailing_metadata=None)` | answer a unary call |
| `fail(call_id, code, message, trailing_metadata=None)` | end any call with a non-OK status, without blocking |
| `stream_send(call_id, payload)` | send one streamed message; blocks while the client is not reading |
| `stream_try_send(call_id, payload)` | the same without blocking: `STREAM_SENT`, `STREAM_FULL` (not queued) or `STREAM_CLOSED` |
| `stream_send_initial_metadata(call_id, metadata)` | send a held-back stream's headers now |
| `stream_end(call_id, trailing_metadata=None)` | finish a stream with OK; a stream cut short by `close()` ends with UNAVAILABLE |
| `stream_open(call_id)` | whether the client is still reading a stream |
| `metrics()` | call counts and latencies per method and status; `None` without `metrics=True` |
| `close()` | stop without waiting for calls in flight; their clients get UNAVAILABLE |

`recv_batch` returns `(kind, call_id, method_id, payload, client_key)`
tuples. `kind` is `EVENT_CALL` for a new call and `EVENT_CANCEL` when the
client left a stream before it ended (`payload` and `client_key` are then
`None`). `client_key` is the value of the `client_key_metadata` request
metadata when it is set and present, and the peer address otherwise, so
calls from the same client can be routed to the same worker.

With `call_details=True`, `recv_batch_details` adds
`(metadata, peer, time_remaining)` to each call, matching grpcio's
`invocation_metadata()`, `peer()` and `time_remaining()`: the request metadata
in order without the transport headers, with `-bin` values as bytes, the peer
as `ipv4:host:port`, and the seconds left before the deadline or `None`.
Without it the sixth item is `None`, and nothing extra is collected per call.
The option also sends `EVENT_CANCEL` for a unary call that ends before it is
answered, as grpcio lets a handler see through `is_active()`.

A stream sends its response headers as soon as the call starts. With
`delay_stream_headers=True` they wait, as in grpcio, for the first message or
`stream_send_initial_metadata`, which is how a stream gets initial metadata; a
stream that ends before either is answered with trailers only.

Response metadata is a sequence of `(key, value)` pairs, with bytes values
for `-bin` keys and str otherwise. Keys must be lowercase and can't start with
`grpc-`, except that `fail` accepts `grpc-status-details-bin` for a rich
status, as grpcio's `abort_with_status` sends it.

With `bearer_token_sha256`, a list of SHA-256 digests of the accepted
tokens, every call must send `authorization: Bearer <token>` for one of them.
Other calls end with UNAUTHENTICATED and `unauthenticated_message` on the
tokio side, before the method lookup, so they never reach Python or the
fallback server. The scheme is matched in any case, and the server only keeps
the digests:

```python
import hashlib

front = NativeGrpcFront(
    "0.0.0.0:50051",
    methods,
    bearer_token_sha256=[hashlib.sha256(key.encode()).digest() for key in keys],
)
```

`set_fallback` lets one port serve methods you don't list: run a regular
`grpc.Server` on a Unix socket and its methods are proxied there, with its
interceptors.

With `metrics=True`, the front counts every call it serves the way gRFC A66's
`grpc.server.call.started` and `grpc.server.call.duration` do: per method, and
per method and status once the trailers go out. A call the client resets
counts as CANCELLED, so one the client gives up on at its deadline counts as
CANCELLED or DEADLINE_EXCEEDED, whichever side notices first. Paths you don't list count under `"other"`, so unknown
paths can't add entries, and calls forwarded to the fallback are left to the
fallback server. Each tokio thread records into its own counters, which costs
about 70 ns per call, and `metrics()` adds them up when you read them:

```python
{
    "start_time_unix_nano": 1761436800000000000,
    "bounds": [0.0, 1e-05, ..., 100.0],  # A66's latency buckets, in seconds
    "methods": {
        "/echo.Echo/Say": {
            "started": 1042,  # still running: started minus the counts
            "status": {
                0: {"count": 1040, "sum": 0.21, "min": 0.00009, "max": 0.0031,
                    "buckets": [0, 0, 0, 12, ...]},  # one more than bounds
            },
        },
    },
}
```

The totals keep growing from `start_time_unix_nano`, as a cumulative
OpenTelemetry histogram or a Prometheus histogram does, so an exporter can
pass them on as they are.

## Compatibility

pygrpc-native follows semantic versioning, and the low-level API stays
compatible within 0.x as well. `tests/test_api.py` pins the 0.1 names,
signatures and event tuples of `NativeGrpcFront`: they won't change or go
away, and new parameters only come after the existing ones, with a default.
`server()` and `aio.server()` follow grpcio's API, and any difference from
grpcio's behavior is a bug unless it is listed in this README. Changes are
listed in [CHANGELOG.md](CHANGELOG.md).

## Limitations

- `NativeGrpcFront` serves unary and server-streaming methods only; other
  kinds can go through the fallback server. `server()` and `aio.server()`
  send them there on their own.
- No TLS yet, and grpcio interceptors don't run on the listed methods. Bearer
  tokens are the only authentication, and only on `NativeGrpcFront`.
- Clients of a Unix socket server must set `grpc.default_authority`, since
  the socket path grpc-core sends as `:authority` is not valid HTTP/2.
- Like grpc-core, binding a Unix socket path replaces any socket file
  already there.
- Per connection: at most 1024 concurrent streams and 16 KiB of request
  metadata. A connection that hasn't finished the HTTP/2 preface after 120 s
  is dropped.

## Benchmarks

With an empty handler on an Apple M4 Pro, `pygrpc_native.server()` answers
about 1.5x the calls per second of `grpc.server()` from one client thread and
about 5x from 32. On a 40-vCPU Linux container the low-level API answered
about 12x at 32 threads. See [benchmarks](benchmarks/README.md) for the
numbers and how they were measured.

## Tests

```bash
cargo test --locked --no-default-features
maturin develop --release
pytest tests
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Every commit needs a DCO sign-off
(`git commit -s`).

## License

Apache-2.0
