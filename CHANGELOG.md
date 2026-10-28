# Changelog

All notable changes are listed here. The project follows
[semantic versioning](https://semver.org); see
[Compatibility](README.md#compatibility) for what that covers.

## 0.5.2 (2026-10-28)

### Fixed

- On grpcio 1.64 through 1.83, the aio server answered client-streaming and
  bidirectional methods of generated code with `UNIMPLEMENTED`. They now reach
  the grpcio fallback. (#69)

## 0.5.1 (2026-10-27)

### Fixed

- `from pygrpc_native import aio`, and `pygrpc_native.aio` after a plain
  `import pygrpc_native`, no longer fail with `RecursionError`. (#61)

## 0.5.0 (2026-10-27)

Everything here is an addition, so code written for 0.4 keeps working.

### Added

- `server()` and `aio.server()` take `grpc.so_reuseport` set to 1, so several
  processes can serve one port, and `NativeGrpcFront` a matching `reuse_port`
  argument. It stays off unless asked for. (#57)

## 0.4.0 (2026-10-27)

`server()` and `aio.server()` now take the interceptors and options grpcio
servers commonly pass. Code written for 0.3 keeps working; the changes in
behavior below bring the servers closer to grpcio.

### Added

- `server()` and `aio.server()` take grpcio server interceptors. (#39)
- `server()` and `aio.server()` take the `grpc.keepalive_time_ms`,
  `grpc.keepalive_timeout_ms` and `grpc.keepalive_permit_without_calls`
  options, and `NativeGrpcFront` the matching `keepalive_*` arguments. (#47)
- `server()` and `aio.server()` take `grpc.max_concurrent_streams`, and
  `NativeGrpcFront` a matching `max_concurrent_streams` argument. (#51)
- `server()` and `aio.server()` take `grpc.max_metadata_size` and
  `grpc.absolute_max_metadata_size`, and `NativeGrpcFront` a matching
  `max_metadata_size` argument. (#51)
- `server()` and `aio.server()` ignore options they don't take, as grpcio
  does, such as options only clients read, `grpc.max_message_length` and
  `grpc.http2.max_frame_size`, and values that aren't integers. Only options
  asking for something the server can't do, such as closing connections by
  age or compressing responses, still raise an error. (#47, #51)
- `metrics()` also reports each call's sent and received message bytes,
  like gRFC A66's `grpc.server.call.sent_total_compressed_message_size` and
  `rcvd_total_compressed_message_size`, under the new keys `size_bounds`,
  `sent` and `rcvd`. (#37)

### Changed

- Like grpc-core, the keepalive ping now waits until a connection has
  received nothing for the keepalive time, so busy connections aren't
  pinged. (#47)

### Fixed

- Handlers that aren't coroutines get grpcio's sync context on
  `aio.server()`, so calling `abort()` from them ends the call with that
  status instead of INTERNAL. (#51)
- `aio.server()` encodes responses as grpc.aio does: a handler's `None` goes
  out as an empty message when there's no serializer, and a response that
  fails to serialize ends the call with UNKNOWN, like any other error in the
  handler, instead of INTERNAL. (#51)
- `server()` and `aio.server()` ignore a message size limit that isn't an
  integer, as grpcio does, instead of converting it. (#47)
- grpcio clients can call a Unix socket server without setting
  `grpc.default_authority`. This uses a patched copy of the `http` crate
  until hyperium/http#836 is fixed upstream. (#35)

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
