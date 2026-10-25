# Benchmarks

`echo.py` starts a server with an empty unary handler and calls it from
grpcio clients for a few seconds.

```bash
pip install grpcio pygrpc-native
python benchmarks/echo.py native-aio 8 4    # server kind, client processes, threads each
```

The kinds are `sync` and `aio` for grpcio's servers, `native-sync` and
`native-aio` for `pygrpc_native.server()` and `pygrpc_native.aio.server()`,
and `native` for the low-level API. `DUR` (default 5) and `SIZE` (default 64) change the seconds measured and the
payload bytes. The server and the clients share the machine.

Median of three runs, calls/s with the client CPU per call in brackets.

Apple M4 Pro (14 cores), Python 3.12.13, grpcio 1.84.1, pygrpc-native main:

| server | 1x1 | 4x4 | 8x4 |
|---|---|---|---|
| grpc sync | 7,739 (78 us) | 11,165 (124 us) | 10,955 (134 us) |
| grpc aio | 4,804 (78 us) | 10,488 (105 us) | 10,322 (115 us) |
| pygrpc_native.server() | 11,428 (74 us) | 47,614 (146 us) | 57,419 (161 us) |
| pygrpc_native.aio.server() | 9,444 (75 us) | 47,723 (140 us) | 58,073 (150 us) |
| low-level API | 12,835 (75 us) | 50,016 (168 us) | 63,413 (163 us) |

Modal CPU container (40 vCPUs, gVisor), Python 3.12.10, grpcio 1.78.0,
pygrpc-native 0.1.0 (low-level API only):

| server | 1x1 | 4x4 | 8x4 |
|---|---|---|---|
| grpc sync | 1,378 (234 us) | 2,354 (311 us) | 2,341 (319 us) |
| grpc aio | 1,343 (250 us) | 4,274 (316 us) | 4,235 (312 us) |
| pygrpc-native | 2,294 (208 us) | 16,137 (357 us) | 28,493 (342 us) |

The clients are grpcio in every row, so at 1x1 they cost about the same
whichever server they call. With pygrpc-native at 4x4 and 8x4 the clients make
several times more calls on the same cores, which is why their CPU per call
goes up.
