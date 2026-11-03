# Runnable examples

The examples use `grpc.method_handlers_generic_handler` with a UTF-8 string
payload, so they do not need a `.proto` file or generated code. The same
grpcio client works with either server.

Install the package and grpcio:

```bash
pip install pygrpc-native grpcio
```

To build from source, see [CONTRIBUTING.md](../CONTRIBUTING.md).

From the repository root, start either server in one terminal:

```bash
python -m examples.sync_server
# or
python -m examples.aio_server
```

In another terminal, run the grpcio client:

```bash
python -m examples.client
```

The client prints `Hello, world!`. Stop the server with Ctrl-C before starting
the other one; both listen on `127.0.0.1:50051`.

## Health checking example

The `health_server` example demonstrates that gRPC health checking (from
`grpcio-health-checking`) runs natively with `pygrpc_native.server()`.

Install the extra dependency:

```bash
pip install grpcio-health-checking
```

Then start the server:

```bash
python -m examples.health_server
```

It registers a `HealthServicer`, sets the default service to `SERVING`, and
listens on `127.0.0.1:50051`. You can call `Check` and `Watch` against it with
a standard grpcio health client.
