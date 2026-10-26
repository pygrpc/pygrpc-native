# Low-level API

`NativeGrpcFront` is the layer under `pygrpc_native.server()`. Each call
reaches Python as an event, and you answer it by id, so you choose how calls
are batched, which threads run the handlers, and how calls from the same
client are routed.

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

## Reference

`NativeGrpcFront(target, methods, io_threads=2, client_key_metadata=None, call_details=False, delay_stream_headers=False, bearer_token_sha256=None, unauthenticated_message="Unauthenticated", metrics=False, keepalive_time_ms=7200000, keepalive_timeout_ms=20000, keepalive_permit_without_calls=True)`
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

Like grpc-core, the server pings a connection that has received nothing for
`keepalive_time_ms` and closes it if the client doesn't answer within
`keepalive_timeout_ms`, which is how it notices a client that went away
without closing the connection. A client that is still sending gets up to a
minute to answer. A `keepalive_time_ms` of 2147483647 (INT_MAX, as in
grpc-core) or more turns the pings off. With `keepalive_permit_without_calls=False`, as grpc-core
servers do by default, a connection is only pinged while it has calls in
flight; `server()` and `aio.server()` default to that.

`set_fallback` lets one port serve methods you don't list: run a regular
`grpc.Server` on a Unix socket and its methods are proxied there, with its
interceptors.

## Metrics

With `metrics=True`, the front counts calls like gRFC A66's
`grpc.server.call.started`, `grpc.server.call.duration`,
`grpc.server.call.sent_total_compressed_message_size` and
`grpc.server.call.rcvd_total_compressed_message_size`. It counts each call
when it starts, and records its status, duration and message bytes when the
trailers go out. A call the client resets counts as CANCELLED, so one the client drops at
its deadline can show up as CANCELLED or DEADLINE_EXCEEDED, depending on which
side notices first. Calls to paths you didn't list count under `"other"`, so
unknown paths can't add entries. Calls forwarded to the fallback aren't
counted here; the fallback server counts them. Each tokio thread keeps its own
counters, which costs about 70 ns per call, and `metrics()` adds them up when
you call it:

```python
{
    "start_time_unix_nano": 1761436800000000000,
    "bounds": [0.0, 1e-05, ..., 100.0],  # A66's latency buckets, in seconds
    "size_bounds": [0, 1024, ..., 4294967296],  # A66's size buckets, in bytes
    "methods": {
        "/echo.Echo/Say": {
            "started": 1042,  # still running: started minus the counts
            "status": {
                0: {"count": 1040, "sum": 0.21, "min": 0.00009, "max": 0.0031,
                    "buckets": [0, 0, 0, 12, ...],  # one more than bounds
                    "sent": {"sum": 66560, "buckets": [0, 1040, 0, ...]},
                    "rcvd": {"sum": 66560, "buckets": [0, 1040, 0, ...]}},
            },
        },
    },
}
```

Each entry in `buckets` is the number of calls in that bucket, not a running
total. A call goes in the first bucket whose bound is at least its duration,
and the last bucket holds anything slower than the last bound. OpenTelemetry
histograms use the same layout, while Prometheus `le` buckets are cumulative,
so add them up from the first one. Nothing resets: the totals count up from
`start_time_unix_nano`. The status keys are gRPC status code numbers, with 0
for OK. A66 labels statuses by name instead.

`sent` and `rcvd` hold each call's total message bytes, without the 5-byte
gRPC frame headers, in the same layout over `size_bounds`. A compressed
request counts at its compressed size, as A66 asks. Responses aren't
compressed.
