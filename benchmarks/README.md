# Benchmarks

## Echo

`echo.py` measures how many calls per second each server answers when the
handler does nothing, so the number is the cost of the server itself. Every
server answers `/bench.Echo/Unary` with the request bytes (64 B by default),
and grpcio client processes call it in a loop.

```bash
pip install grpcio pygrpc-native
python benchmarks/echo.py sync 8 4     # grpc.server with 64 threads
python benchmarks/echo.py aio 8 4      # grpc.aio.server
python benchmarks/echo.py native 8 4   # pygrpc-native, answered on the receiving thread
```

The arguments are client processes and threads per process. `DUR` sets the
seconds measured (default 5) and `SIZE` the payload bytes. Server and clients
run on the same machine, so the busiest configurations also compete for CPU.

The script also prints the client CPU per call. The clients are grpcio in every
case, and at 1x1 their cost is about the same whichever server they call:
pygrpc-native makes the server cheaper, not the client.

### Results

Median of three interleaved runs, calls/s (client CPU per call in brackets).

macOS, Apple M4 Pro (14 cores), Python 3.12.13, grpcio 1.84.1, pygrpc-native 0.1.0:

| server | 1x1 | 4x4 | 8x4 |
|---|---|---|---|
| grpc sync | 7,675 (78 us) | 11,249 (123 us) | 11,048 (133 us) |
| grpc aio | 4,678 (81 us) | 10,431 (106 us) | 10,199 (116 us) |
| pygrpc-native | 12,596 (76 us) | 50,068 (164 us) | 64,492 (156 us) |

Linux, Modal CPU container (40 vCPUs, gVisor 4.19, so loopback is slow and
absolute numbers are low), Python 3.12.10, grpcio 1.78.0, pygrpc-native 0.1.0:

| server | 1x1 | 4x4 | 8x4 |
|---|---|---|---|
| grpc sync | 1,378 (234 us) | 2,354 (311 us) | 2,341 (319 us) |
| grpc aio | 1,343 (250 us) | 4,274 (316 us) | 4,235 (312 us) |
| pygrpc-native | 2,294 (208 us) | 16,137 (357 us) | 28,493 (342 us) |

At 4x4 and 8x4 the client CPU per call goes up with pygrpc-native because the
clients run several times more calls on the same cores, not because a call to
it costs the client more.
