# pygrpc-native

A gRPC server for Python whose HTTP/2 and gRPC work runs in Rust, outside the
GIL. Your handlers stay in Python.

With grpcio, every call runs grpcio's own Python layer under the same GIL as
your handlers, and on a busy server that layer is often where most of the
time goes. pygrpc-native does the framing, flow control, deadlines and
cancellation on a small tokio runtime and hands Python each call as an event:
an id, the method, the request bytes and who sent it. You decode the
protobuf, run the handler and send back the encoded response, so clients and
the wire format stay exactly as they are with grpcio.

```
grpc client ──HTTP/2──▶ tokio threads (no GIL)
                          │ path → method id
                          ▼
                    event queue ──▶ recv_batch() in Python
                          ◀── respond(call_id, bytes) from any thread
```

Python takes calls in batches, so a burst of requests costs one trip across
the GIL instead of one per call.

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

## Example

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

`NativeGrpcFront(target, methods, io_threads=2, client_key_metadata=None)`
binds `host:port` (every address the host resolves to), `unix:path` or
`unix:///path`. Port 0 picks a free port. `methods` lists
`(path, server_streaming)` pairs, where a path is `/package.Service/Method`
and a method's id is its index in the list.

| Member | Purpose |
|---|---|
| `bound_port` | the bound TCP port; 1 for a Unix socket, as grpcio reports it |
| `set_fallback(path)` | forward unknown methods to a grpcio server on a Unix socket |
| `start()` | start accepting connections |
| `recv_batch(max_events, timeout_ms=None)` | wait for events with the GIL released; `None` once closed |
| `respond(call_id, payload)` | answer a unary call |
| `fail(call_id, code, message)` | end any call with a non-OK status, without blocking |
| `stream_send(call_id, payload)` | send one streamed message; blocks while the client is not reading |
| `stream_end(call_id)` | finish a stream with OK; a stream cut short by `close()` ends with UNAVAILABLE |
| `stream_open(call_id)` | whether the client is still reading a stream |
| `close()` | stop without waiting for calls in flight; their clients get UNAVAILABLE |

`recv_batch` returns `(kind, call_id, method_id, payload, client_key)`
tuples. `kind` is `EVENT_CALL` for a new call and `EVENT_CANCEL` when the
client left a stream before it ended (`payload` and `client_key` are then
`None`). `client_key` is the value of the `client_key_metadata` request
metadata when it is set and present, and the peer address otherwise, so
calls from the same client can be routed to the same worker.

`set_fallback` lets one port serve methods you don't list: run a regular
`grpc.Server` on a Unix socket and its methods are proxied there, with its
interceptors.

## Limitations

- Unary and server-streaming methods only. Other kinds can go through the
  fallback server.
- No TLS or authentication yet, and grpcio interceptors don't run on the
  listed methods.
- Clients of a Unix socket server must set `grpc.default_authority`, since
  the socket path grpc-core sends as `:authority` is not valid HTTP/2.
- Like grpc-core, binding a Unix socket path replaces any socket file
  already there.
- Per connection: at most 1024 concurrent streams and 16 KiB of request
  metadata. A connection that hasn't finished the HTTP/2 preface after 120 s
  is dropped.

## Benchmarks

With an empty handler, pygrpc-native answers about 1.6x the calls per second
of grpcio's servers from one client thread, and 4x to 12x from 16 or 32. See
[benchmarks](benchmarks/README.md).

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
