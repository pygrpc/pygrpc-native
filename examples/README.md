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
