# Benchmarks

`echo.py` starts a server with empty unary and server-streaming handlers
and calls it from grpcio clients for a few seconds.

```bash
pip install grpcio pygrpc-native
python benchmarks/echo.py native-aio 8 4    # server kind, client processes, threads each
```

The kinds are `sync` and `aio` for grpcio's servers, `native-sync` and
`native-aio` for `pygrpc_native.server()` and `pygrpc_native.aio.server()`,
and `native` for the low-level API. `DUR` (default 5) and `SIZE` (default 64)
change the seconds measured and the payload bytes.
`METHOD` (unary or stream) and `STREAM_COUNT` (default 10) configure the
streaming benchmark. With `ITL_MS`, handlers send each message on the next
tick of a clock with that period, the way an LLM engine emits one token for
every running request at each decode step. The server and the clients share
the machine. On Linux the server's CPU per call or message is printed too.

```bash
METHOD=stream python benchmarks/echo.py native-aio 8 4
# 1024 streams, each sending a message every 20 ms, all on the same tick
METHOD=stream ITL_MS=20 STREAM_COUNT=128 SIZE=24 python benchmarks/echo.py native-aio 8 128
```

Calls/s with the client CPU per call in brackets.

Apple M4 Pro (14 cores), macOS 27.0.1, Python 3.12.15, grpcio 1.85.1,
pygrpc-native 0.8.0, median of five runs:

| server | 1x1 | 4x4 | 8x4 |
|---|---|---|---|
| grpc sync | 7,449 (81 us) | 11,301 (126 us) | 11,192 (135 us) |
| grpc aio | 4,766 (80 us) | 10,408 (108 us) | 10,214 (118 us) |
| pygrpc_native.server() | 10,790 (79 us) | 47,118 (146 us) | 56,834 (159 us) |
| pygrpc_native.aio.server() | 9,736 (79 us) | 50,429 (153 us) | 60,869 (162 us) |
| low-level API | 12,191 (78 us) | 50,710 (163 us) | 62,664 (169 us) |

Modal CPU container (40 vCPUs, gVisor), Python 3.12.10, grpcio 1.78.0,
pygrpc-native 0.1.0 (low-level API only), median of three runs:

| server | 1x1 | 4x4 | 8x4 |
|---|---|---|---|
| grpc sync | 1,378 (234 us) | 2,354 (311 us) | 2,341 (319 us) |
| grpc aio | 1,343 (250 us) | 4,274 (316 us) | 4,235 (312 us) |
| pygrpc-native | 2,294 (208 us) | 16,137 (357 us) | 28,493 (342 us) |

The clients are grpcio in every row, so at 1x1 they cost about the same
whichever server they call. With pygrpc-native at 4x4 and 8x4 the clients make
several times more calls on the same cores, which is why their CPU per call
goes up.

### More on the Apple M4 Pro

Same machine and versions, median of five runs.

Payload size, unary at 8x4, calls/s:

| server | 64 B | 1 KiB | 64 KiB | 1 MiB |
|---|---|---|---|---|
| grpc sync | 11,335 | 11,154 | 10,565 | 5,779 |
| grpc aio | 10,329 | 10,307 | 9,577 | 3,910 |
| pygrpc_native.server() | 55,531 | 55,199 | 42,717 | 4,496 |
| pygrpc_native.aio.server() | 60,710 | 59,130 | 44,987 | 4,531 |
| low-level API | 62,485 | 60,958 | 45,579 | 4,556 |

At 1 MiB grpc sync was faster than every pygrpc-native server, in all five
runs.

Server streaming with 10 messages per stream, msgs/s. `echo.py` doesn't run
the low-level API with streams.

| server | 64 B, 1x1 | 64 B, 4x4 | 64 B, 8x4 | 64 KiB, 1x1 | 64 KiB, 4x4 | 64 KiB, 8x4 |
|---|---|---|---|---|---|---|
| grpc sync | 23,993 | 23,902 | 23,308 | 21,410 | 22,882 | 22,558 |
| grpc aio | 15,667 | 50,755 | 55,373 | 14,441 | 39,521 | 38,722 |
| pygrpc_native.server() | 44,339 | 62,042 | 69,571 | 28,367 | 56,359 | 64,746 |
| pygrpc_native.aio.server() | 41,487 | 64,666 | 79,456 | 27,316 | 57,172 | 73,575 |

The token streaming command above (1024 streams, a message every 20 ms) can
reach at most 51,200 msgs/s:

| server | msgs/s |
|---|---|
| grpc sync | 3,218 |
| grpc aio | 48,602 |
| pygrpc_native.server() | 3,318 |
| pygrpc_native.aio.server() | 50,358 |

The sync servers in `echo.py` use a 64-thread pool, and each stream holds a
thread for 2.56 s, so both stop near 3,200 msgs/s.

With `DUR=30`, the unary 8x4 medians were within 1.2% of the 5-second ones.

## Comparing two builds

`regress.py` compares the server cost per call of two builds on the same
machine. The `Perf` workflow runs it on pull requests, and it can be run by
hand against another ref, such as the last release.

```bash
python benchmarks/regress.py --base base-venv/bin/python --head head-venv/bin/python
```

Each interpreter needs its build of pygrpc-native installed, and the one
running the script needs grpcio. It runs on Linux only.
