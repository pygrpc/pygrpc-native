# Benchmarks

`echo.py` starts a server with an empty unary handler and calls it from
grpcio clients for a few seconds.

```bash
pip install grpcio pygrpc-native
python benchmarks/echo.py native 8 4    # sync, aio or native, client processes, threads each
```

`DUR` (default 5) and `SIZE` (default 64) change the seconds measured and the
payload bytes. The server and the clients share the machine.

Median of three runs, calls/s with the client CPU per call in brackets.

Apple M4 Pro (14 cores), Python 3.12.13, grpcio 1.84.1, pygrpc-native 0.1.0:

| server | 1x1 | 4x4 | 8x4 |
|---|---|---|---|
| grpc sync | 7,675 (78 us) | 11,249 (123 us) | 11,048 (133 us) |
| grpc aio | 4,678 (81 us) | 10,431 (106 us) | 10,199 (116 us) |
| pygrpc-native | 12,596 (76 us) | 50,068 (164 us) | 64,492 (156 us) |

Modal CPU container (40 vCPUs, gVisor), Python 3.12.10, grpcio 1.78.0,
pygrpc-native 0.1.0:

| server | 1x1 | 4x4 | 8x4 |
|---|---|---|---|
| grpc sync | 1,378 (234 us) | 2,354 (311 us) | 2,341 (319 us) |
| grpc aio | 1,343 (250 us) | 4,274 (316 us) | 4,235 (312 us) |
| pygrpc-native | 2,294 (208 us) | 16,137 (357 us) | 28,493 (342 us) |

The clients are grpcio in every row, so at 1x1 they cost about the same
whichever server they call. With pygrpc-native at 4x4 and 8x4 the clients make
several times more calls on the same cores, which is why their CPU per call
goes up.
