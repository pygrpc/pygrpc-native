# Runnable examples

The examples use `grpc.method_handlers_generic_handler` with a UTF-8 string
payload, so they do not need a `.proto` file or generated code. The same
grpcio client works with either server.

From the repository root, create and activate a virtual environment, then
build the checkout into it:

```bash
python -m venv .venv
source .venv/bin/activate  # Windows PowerShell: .venv\Scripts\Activate.ps1
python -m pip install maturin grpcio
maturin develop --release
```

In one terminal, start either server:

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
