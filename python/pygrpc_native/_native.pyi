from collections.abc import Sequence
from typing import Any

Metadata = Sequence[tuple[str, str | bytes]]

__version__: str
EVENT_CALL: int
EVENT_CANCEL: int
_EVENT_RESOLVE: int
_MAX_RESOLVED: int
_EVENT_WRITABLE: int
_EVENT_REFUSED: int
_STREAM_PARKED: int
STREAM_SENT: int
STREAM_FULL: int
STREAM_CLOSED: int

# (kind, call_id, method_id, payload, client_key); payload and client_key are
# None for EVENT_CANCEL and _EVENT_REFUSED. payload is the method path for
# _EVENT_RESOLVE, and whether the held message went out for _EVENT_WRITABLE.
Event = tuple[int, int, int, bytes | str | bool | None, bytes | str | None]

# (metadata, peer, time_remaining); metadata values are bytes for -bin keys.
# A call from a client with a certificate adds a PeerAuth.
CallDetails = (
    tuple[tuple[tuple[str, str | bytes], ...], str, float | None]
    | tuple[tuple[tuple[str, str | bytes], ...], str, float | None, PeerAuth]
)
DetailedEvent = tuple[
    int, int, int, bytes | str | bool | None, bytes | str | None, CallDetails | None
]

class PeerAuth:
    def auth_context(self) -> dict[str, list[bytes]]: ...
    def peer_identity_key(self) -> str: ...

class NativeGrpcFront:
    def __init__(
        self,
        target: str,
        methods: Sequence[tuple[str, bool]],
        io_threads: int = 2,
        client_key_metadata: str | None = None,
        call_details: bool = False,
        delay_stream_headers: bool = False,
        bearer_token_sha256: Sequence[bytes] | None = None,
        unauthenticated_message: str = "Unauthenticated",
        metrics: bool = False,
        keepalive_time_ms: int = 7_200_000,
        keepalive_timeout_ms: int = 20_000,
        keepalive_permit_without_calls: bool = True,
        max_concurrent_streams: int = 1024,
        max_metadata_size: int = 16384,
        reuse_port: bool = False,
        ordered: bool = False,
        tls: tuple[bytes, bytes, bytes | None, bool] | None = None,
        max_receive_message_length: int | None = None,
    ) -> None: ...
    @property
    def bound_port(self) -> int: ...
    def add_methods(self, methods: Sequence[tuple[str, bool]]) -> list[int]: ...
    def set_fallback(self, path: str) -> None: ...
    def _resolve_unknown_methods(self) -> None: ...
    def _report_refused_calls(self) -> None: ...
    def _drain(self) -> None: ...
    def _wake_fd(self) -> int: ...
    def _poll_batch_details(self, max_events: int) -> list[DetailedEvent] | None: ...
    def _resolved(
        self,
        resolve_id: int,
        method_id: int | None,
        server_streaming: bool = False,
        remember: bool = True,
    ) -> bool: ...
    def start(self) -> None: ...
    def recv_batch(
        self, max_events: int, timeout_ms: int | None = None
    ) -> list[Event] | None: ...
    def recv_batch_details(
        self, max_events: int, timeout_ms: int | None = None
    ) -> list[DetailedEvent] | None: ...
    def respond(
        self,
        call_id: int,
        payload: bytes,
        initial_metadata: Metadata | None = None,
        trailing_metadata: Metadata | None = None,
    ) -> bool: ...
    def fail(
        self,
        call_id: int,
        code: int,
        message: str,
        trailing_metadata: Metadata | None = None,
    ) -> bool: ...
    def stream_send(self, call_id: int, payload: bytes) -> bool: ...
    def stream_try_send(self, call_id: int, payload: bytes) -> int: ...
    def _stream_send_or_park(self, call_id: int, payload: bytes) -> int: ...
    def stream_send_initial_metadata(
        self, call_id: int, metadata: Metadata
    ) -> bool: ...
    def stream_end(
        self, call_id: int, trailing_metadata: Metadata | None = None
    ) -> None: ...
    def stream_open(self, call_id: int) -> bool: ...
    def metrics(self) -> dict[str, Any] | None: ...
    def close(self) -> None: ...
