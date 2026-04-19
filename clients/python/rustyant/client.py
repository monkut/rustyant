"""
Rustyant WebSocket client — persistent connection, RESP2 framing over WS.

Connects to an API Gateway WebSocket endpoint fronting the rustyant-ws Lambda
and issues Redis-style commands against the S3-backed key/value store.

Example:

    from rustyant import Client

    c = Client("wss://abc123.execute-api.us-east-1.amazonaws.com/prod")
    c.set("hello", "world")
    assert c.get("hello") == b"world"
    c.close()
"""

from __future__ import annotations

from typing import Any

import hiredis
import websocket

from rustyant._base import RustyAntError, _Arg, _RedisShapedClient, _encode_command


class Client(_RedisShapedClient):
    """Blocking rustyant WebSocket client. Thread-unsafe — one Client per thread."""

    def __init__(
        self,
        url: str,
        *,
        timeout: float = 10.0,
        auto_connect: bool = True,
    ) -> None:
        if not url.startswith(("ws://", "wss://")):
            raise ValueError(f"expected ws:// or wss:// URL, got {url!r}")
        self._url = url
        self._timeout = timeout
        self._ws: websocket.WebSocket | None = None
        self._reader: hiredis.Reader = hiredis.Reader()
        if auto_connect:
            self._connect()

    # ---- connection management -----------------------------------------

    def _connect(self) -> None:
        self._ws = websocket.create_connection(self._url, timeout=self._timeout)

    def close(self) -> None:
        if self._ws is not None:
            try:
                self._ws.close()
            finally:
                self._ws = None

    def __enter__(self) -> "Client":
        if self._ws is None:
            self._connect()
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    # ---- transport -----------------------------------------------------

    def _call(self, *args: _Arg) -> Any:
        if self._ws is None:
            self._connect()
        assert self._ws is not None  # help the type-checker

        packed = _encode_command(*args)
        self._ws.send_binary(packed)

        # Drain until a full reply is parsed. API Gateway delivers each
        # `post_to_connection` payload as a single WS frame, so one recv
        # normally suffices — but we loop defensively.
        while True:
            reply = self._reader.gets()
            if reply is not False:
                break
            raw = self._ws.recv()
            if isinstance(raw, str):
                raw = raw.encode()
            if not raw:
                raise RustyAntError("connection closed while awaiting reply")
            self._reader.feed(raw)

        if isinstance(reply, hiredis.ReplyError):
            raise RustyAntError(str(reply))
        return reply
