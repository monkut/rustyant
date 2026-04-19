"""
rustyant adapters for `redis-py`.

Two `redis.connection.AbstractConnection` subclasses so that the stock
`redis.Redis` client works against a rustyant deployment without any
bespoke API:

    from redis import ConnectionPool, Redis
    from rustyant import RustyAntWSConnection

    pool = ConnectionPool(
        connection_class=RustyAntWSConnection,
        url="wss://abc.execute-api.us-east-1.amazonaws.com/prod",
    )
    r = Redis(connection_pool=pool)
    r.set("hello", "world")
    assert r.get("hello") == b"world"

Or via the convenience helpers:

    from rustyant import connect_ws, connect_http
    r = connect_ws("wss://abc.execute-api.us-east-1.amazonaws.com/prod")

`connect_http` points at a Lambda Function URL / API Gateway HTTP API.

Both adapters bypass redis-py's socket layer entirely: `send_packed_command`
ships bytes over the transport, `read_response` parses the reply through
`hiredis`, and `on_connect` is a no-op (no HELLO / AUTH / CLIENT SETINFO /
SELECT DB — rustyant has none of those).
"""

from __future__ import annotations

from typing import Any

import hiredis
import redis
import redis.connection
import redis.exceptions
import requests
import websocket

# An empty DriverInfo's formatted_name and lib_version are both "" (falsy),
# which the `AbstractConnection.on_connect()` code treats as "don't send
# CLIENT SETINFO". rustyant doesn't implement CLIENT, so this is essential
# — otherwise connection setup blocks on unknown-command replies.
try:
    from redis.connection import DriverInfo  # redis-py 7.x+
    _SUPPRESSED_DRIVER_INFO: Any = DriverInfo(name="", lib_version="")
except ImportError:  # pragma: no cover — redis-py <7 (no driver_info concept)
    _SUPPRESSED_DRIVER_INFO = None


# ---------------------------------------------------------------------------
# Shared base
# ---------------------------------------------------------------------------


class _RustyAntConnectionBase(redis.connection.AbstractConnection):
    """Common wiring for the rustyant → redis-py adapters.

    Subclasses must implement:
      * `_open_transport(self)`  — open the wire connection / session
      * `_close_transport(self)` — tear it down (idempotent)
      * `_send_bytes(self, data: bytes)` — ship a packed command
      * `_recv_reply(self) -> Any`       — return the next parsed reply
                                            (bytes / int / list / None, or
                                            a `hiredis.ReplyError`)
    """

    def __init__(self, url: str, **kwargs: Any) -> None:
        # redis-py's `ConnectionPool` forwards `host` / `port` by default when
        # the pool was constructed with them; strip them — we don't use them.
        kwargs.pop("host", None)
        kwargs.pop("port", None)
        # Suppress the CLIENT SETINFO LIB-NAME / LIB-VER handshake.
        # rustyant doesn't implement CLIENT, and the base class blocks on
        # the expected +OK reply; passing an empty DriverInfo makes both
        # CLIENT SETINFO calls no-op.
        if _SUPPRESSED_DRIVER_INFO is not None:
            kwargs.setdefault("driver_info", _SUPPRESSED_DRIVER_INFO)
            # The deprecated lib_name/lib_version kwargs emit DeprecationWarning
            # even when None in redis-py 7.x — don't pass them at all.
            kwargs.pop("lib_name", None)
            kwargs.pop("lib_version", None)
        else:
            # redis-py <7 path — set explicit Nones to skip CLIENT SETINFO.
            kwargs.setdefault("lib_name", None)
            kwargs.setdefault("lib_version", None)
        # Force RESP2. RESP3 would trigger a HELLO exchange rustyant doesn't
        # handle.
        kwargs.setdefault("protocol", 2)
        super().__init__(**kwargs)
        self._url = url

    # ---- lifecycle -----------------------------------------------------

    def _connect(self) -> Any:
        # Called by AbstractConnection.connect(); return a truthy sentinel so
        # the base class stores it in self._sock and sees the connection as
        # "alive". Nothing ever reads from this object — we override every
        # method that would.
        self._open_transport()
        return _NullSock()

    def _host_error(self) -> str:
        return f"rustyant[{self._url}]"

    def disconnect(self, *_args: Any) -> None:
        try:
            self._close_transport()
        finally:
            self._sock = None

    def on_connect(self) -> None:
        # rustyant has no AUTH, no HELLO, no CLIENT SETINFO, no SELECT DB.
        # Override redis-py's default handshake to skip all of that.
        pass

    # ---- command I/O ---------------------------------------------------

    def send_packed_command(self, command: Any, check_health: bool = True) -> None:  # noqa: ARG002
        # `command` can be a single `bytes` or a `list[bytes]` containing
        # one OR MORE packed RESP2 frames concatenated (pipelines hit this
        # path via `connection.send_packed_command(all_cmds)`). Split on
        # frame boundaries so the transport sends exactly one command per
        # WS message / HTTP POST — rustyant's Lambda handler parses one
        # frame per delivered payload.
        if self._sock is None:
            self.connect()
        packed = b"".join(command) if isinstance(command, list) else command
        for frame in _split_resp2_frames(packed):
            self._send_one_frame(frame)

    def send_command(self, *args: Any, **kwargs: Any) -> None:  # noqa: ARG002
        self.send_packed_command(self.pack_command(*args))

    def read_response(
        self,
        disable_decoding: bool = False,
        *,
        disconnect_on_error: bool = True,  # noqa: ARG002
        push_request: bool = False,  # noqa: ARG002
    ) -> Any:
        try:
            reply = self._recv_reply()
        except redis.exceptions.RedisError:
            raise
        except Exception as e:
            raise redis.exceptions.ConnectionError(f"rustyant transport: {e}") from e
        if isinstance(reply, hiredis.ReplyError):
            raise redis.exceptions.ResponseError(str(reply))
        if self.encoder.decode_responses and not disable_decoding:
            reply = self.encoder.decode(reply)
        return reply

    def can_read(self, timeout: float | None = 0) -> bool:  # noqa: ARG002
        # Pipelines and pubsub call this to poll readiness. rustyant's
        # transports are always-request-reply; there's never a spontaneous
        # push, so "nothing to read unless a command is in flight" is the
        # safe answer.
        return False


class _NullSock:
    """Stand-in object stored in `self._sock` so base-class connected
    checks pass. Only `close()` is ever called on it by redis-py's
    error-path cleanup."""

    def close(self) -> None:
        pass


# ---------------------------------------------------------------------------
# WebSocket transport
# ---------------------------------------------------------------------------


class RustyAntWSConnection(_RustyAntConnectionBase):
    """Connect to a rustyant-ws Lambda via API Gateway WebSocket.

    `url` is the full wss:// endpoint including stage — for the SAM template
    this is the `WebSocketUrl` output, e.g.
    `wss://abc123.execute-api.us-east-1.amazonaws.com/prod`.
    """

    def _open_transport(self) -> None:
        self._ws = websocket.create_connection(self._url, timeout=self.socket_timeout)
        self._ws_reader = hiredis.Reader()

    def _close_transport(self) -> None:
        ws = getattr(self, "_ws", None)
        if ws is not None:
            try:
                ws.close()
            finally:
                self._ws = None

    def _send_one_frame(self, frame: bytes) -> None:
        self._ws.send_binary(frame)

    def _recv_reply(self) -> Any:
        while True:
            reply = self._ws_reader.gets()
            if reply is not False:
                return reply
            raw = self._ws.recv()
            if isinstance(raw, str):
                raw = raw.encode()
            if not raw:
                raise redis.exceptions.ConnectionError("rustyant WebSocket closed mid-reply")
            self._ws_reader.feed(raw)


# ---------------------------------------------------------------------------
# HTTP transport
# ---------------------------------------------------------------------------


class RustyAntHttpConnection(_RustyAntConnectionBase):
    """Connect to a rustyant Lambda via Lambda Function URL or API Gateway
    HTTP API.

    Each command becomes one HTTP POST; replies are parsed out of the
    response body. Pipelined sequences of `send_command` / `read_response`
    work but are serialized — one HTTP round-trip per command.
    """

    def __init__(self, url: str, *, session: requests.Session | None = None, **kwargs: Any) -> None:
        super().__init__(url, **kwargs)
        self._user_session = session
        self._owns_session = session is None

    def _open_transport(self) -> None:
        self._session = self._user_session or requests.Session()
        self._http_reply_queue: list[bytes] = []

    def _close_transport(self) -> None:
        sess = getattr(self, "_session", None)
        if sess is not None and self._owns_session:
            sess.close()
        self._session = None

    def _send_one_frame(self, frame: bytes) -> None:
        resp = self._session.post(
            self._url,
            data=frame,
            headers={"Content-Type": "application/resp"},
            timeout=self.socket_timeout,
        )
        self._http_reply_queue.append(resp.content)

    def _recv_reply(self) -> Any:
        if not self._http_reply_queue:
            raise redis.exceptions.ConnectionError("rustyant HTTP: no pending reply")
        raw = self._http_reply_queue.pop(0)
        reader = hiredis.Reader()
        reader.feed(raw)
        reply = reader.gets()
        if reply is False:
            raise redis.exceptions.ConnectionError("rustyant HTTP: truncated reply body")
        return reply


# ---------------------------------------------------------------------------
# Convenience constructors
# ---------------------------------------------------------------------------


def _split_resp2_frames(buf: bytes) -> list[bytes]:
    """Split a RESP2 byte sequence into individual top-level frames.

    rustyant commands are always arrays of bulk strings, but the splitter
    handles every RESP2 type so it works on pipelined replies too. Runs in
    O(n) and allocates only the output list.
    """
    frames: list[bytes] = []
    pos = 0
    while pos < len(buf):
        consumed = _frame_len(buf, pos)
        if consumed == 0:
            # Incomplete / unparsable tail — refuse to send it so the
            # caller sees the truncation rather than silently losing data.
            raise redis.exceptions.ConnectionError(
                f"rustyant: outgoing RESP2 stream not frame-aligned at byte {pos}"
            )
        frames.append(bytes(buf[pos : pos + consumed]))
        pos += consumed
    return frames


def _frame_len(buf: bytes, start: int) -> int:
    if start >= len(buf):
        return 0
    prefix = buf[start : start + 1]
    if prefix in (b"+", b"-", b":"):
        crlf = buf.find(b"\r\n", start)
        return 0 if crlf == -1 else crlf + 2 - start
    if prefix == b"$":
        crlf = buf.find(b"\r\n", start)
        if crlf == -1:
            return 0
        n = int(buf[start + 1 : crlf])
        if n < 0:
            return crlf + 2 - start
        end = crlf + 2 + n + 2
        return 0 if end > len(buf) else end - start
    if prefix == b"*":
        crlf = buf.find(b"\r\n", start)
        if crlf == -1:
            return 0
        n = int(buf[start + 1 : crlf])
        pos = crlf + 2 - start
        if n < 0:
            return pos
        for _ in range(n):
            item = _frame_len(buf, start + pos)
            if item == 0:
                return 0
            pos += item
        return pos
    return 0


def connect_ws(url: str, **kwargs: Any) -> redis.Redis:
    """Shorthand for a `redis.Redis` client pre-wired with
    `RustyAntWSConnection`."""
    pool = redis.ConnectionPool(connection_class=RustyAntWSConnection, url=url, **kwargs)
    return redis.Redis(connection_pool=pool)


def connect_http(url: str, **kwargs: Any) -> redis.Redis:
    """Shorthand for a `redis.Redis` client pre-wired with
    `RustyAntHttpConnection`."""
    pool = redis.ConnectionPool(connection_class=RustyAntHttpConnection, url=url, **kwargs)
    return redis.Redis(connection_pool=pool)
