# Changelog

All notable changes are listed here. The project follows
[semantic versioning](https://semver.org); see
[Compatibility](README.md#compatibility) for what that covers.

## Unreleased

### Added

- `server()` and `aio.server()` take grpcio server interceptors.
- `server()` and `aio.server()` take the `grpc.keepalive_time_ms`,
  `grpc.keepalive_timeout_ms` and `grpc.keepalive_permit_without_calls`
  options, and `NativeGrpcFront` the matching `keepalive_*` arguments.
- `server()` and `aio.server()` take `grpc.max_concurrent_streams`, and
  `NativeGrpcFront` a matching `max_concurrent_streams` argument.
- `server()` and `aio.server()` take `grpc.max_metadata_size` and
  `grpc.absolute_max_metadata_size`, and `NativeGrpcFront` a matching
  `max_metadata_size` argument.
- `server()` and `aio.server()` ignore options they don't take, as grpcio
  does, such as options only clients read, `grpc.max_message_length` and
  `grpc.http2.max_frame_size`, and values that aren't integers. Only options
  asking for something the server can't do, such as closing connections by
  age or compressing responses, still raise an error.
- `metrics()` also reports each call's sent and received message bytes,
  like gRFC A66's `grpc.server.call.sent_total_compressed_message_size` and
  `rcvd_total_compressed_message_size`, under the new keys `size_bounds`,
  `sent` and `rcvd`.

### Changed

- Like grpc-core, the keepalive ping now waits until a connection has
  received nothing for the keepalive time, so busy connections aren't pinged.

### Fixed

- Handlers that aren't coroutines get grpcio's sync context on
  `aio.server()`, so calling `abort()` from them ends the call with that
  status instead of INTERNAL.
- `aio.server()` encodes responses as grpc.aio does: a handler's `None` goes
  out as an empty message when there's no serializer, and a response that
  fails to serialize ends the call with UNKNOWN, like any other error in the
  handler, instead of INTERNAL.
- `server()` and `aio.server()` ignore a message size limit that isn't an
  integer, as grpcio does, instead of converting it.
- grpcio clients can call a Unix socket server without setting
  `grpc.default_authority`. This uses a patched copy of the `http` crate
  until hyperium/http#836 is fixed upstream.

## 0.3.0 (2026-10-26)

Everything here is an addition, so code written for 0.1 and 0.2 keeps working.

### Added

- `NativeGrpcFront(metrics=True)` and `metrics()`: call counts and latencies
  per method and status, recorded in Rust without the GIL. The keys
  `metrics()` returns are covered by the compatibility promise. (#29, #31)

### Changed

- The low-level API reference moved from the README to
  [docs/low-level-api.md](docs/low-level-api.md). (#31)

## 0.2.0 (2026-10-25)

Everything here is an addition, so code written for 0.1 keeps working.

### Added

- `pygrpc_native.server()` and `pygrpc_native.aio.server()` take the same
  arguments as `grpc.server()` and `grpc.aio.server()` and serve grpcio-tools
  generated servicers unchanged. Status codes, metadata, deadlines and
  cancellation behave as in grpcio, and the aio server runs on uvloop too.
  (#17, #18, #20)
- Methods the front doesn't serve itself, such as client-streaming and
  bidirectional methods or server reflection, go to a grpcio server behind it.
  (#19)
- `NativeGrpcFront` can require a bearer token (`bearer_token_sha256`,
  `unauthenticated_message`). Calls without one are rejected in Rust before
  they reach Python. (#23)
- `call_details` and `recv_batch_details()` report each call's request
  metadata, peer and deadline. (#10)
- `add_methods()` registers methods after the front is bound. (#11)
- `stream_try_send()` for callers that can't block, such as an event loop.
  (#12)
- `respond()`, `fail()` and `stream_end()` take response metadata, and `fail()`
  can send `grpc-status-details-bin`. (#13)
- `delay_stream_headers` and `stream_send_initial_metadata()` hold a stream's
  headers back until its first message. (#14)
- With `call_details`, a cancelled unary call or one that misses its deadline
  is reported as an `EVENT_CANCEL`. (#16)

### Changed

- The extension module moved to `pygrpc_native._native`, so the
  `pygrpc_native.pygrpc_native` attribute is gone. Import names from
  `pygrpc_native` as before. (#15)

## 0.1.0 (2026-10-24)

First release: `NativeGrpcFront`, a gRPC server whose HTTP/2 and gRPC work
runs in Rust, outside the GIL. Unary and server-streaming methods, gzip
requests, deadlines and batched calls to Python. abi3 wheels for CPython 3.10
and later on Linux (x86_64, aarch64) and macOS (arm64, x86_64).
