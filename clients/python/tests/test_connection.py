"""Tests for the rustyant redis-py adapters.

Mocks the transport (WebSocket / HTTP) and drives real `redis.Redis()`
through our `Connection` subclasses. Verifies:
  * redis-py's `pack_command` bytes arrive at our transport unchanged
  * RESP2 reply bytes we feed back are parsed by redis-py's pipeline and
    surface as the expected native Python types
  * server-side `-ERR` is raised as `redis.exceptions.ResponseError`
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from unittest.mock import patch

import pytest
import redis
import redis.exceptions

from rustyant import RustyAntHttpConnection, RustyAntWSConnection, connect_http, connect_ws


# ---------------------------------------------------------------------------
# Fake WebSocket
# ---------------------------------------------------------------------------


@dataclass
class _FakeWS:
    replies: list[bytes] = field(default_factory=list)
    sent: list[bytes] = field(default_factory=list)
    closed: bool = False

    def send_binary(self, data: bytes) -> None:
        self.sent.append(bytes(data))

    def recv(self) -> bytes:
        if not self.replies:
            raise AssertionError("FakeWS.recv() called with no canned reply")
        return self.replies.pop(0)

    def close(self) -> None:
        self.closed = True


def _patch_ws(ws: _FakeWS):
    return patch("rustyant.connection.websocket.create_connection", return_value=ws)


# ---------------------------------------------------------------------------
# Fake HTTP session
# ---------------------------------------------------------------------------


@dataclass
class _FakeResponse:
    content: bytes
    status_code: int = 200


class _FakeSession:
    def __init__(self, replies: list[bytes]) -> None:
        self._replies = list(replies)
        self.sent: list[bytes] = []

    def post(self, url: str, *, data: bytes, headers: dict[str, str], timeout: float | None) -> _FakeResponse:  # noqa: ARG002
        assert url.startswith("http")
        assert headers.get("Content-Type") == "application/resp"
        self.sent.append(bytes(data))
        if not self._replies:
            raise AssertionError("FakeSession.post() called with no canned reply")
        return _FakeResponse(self._replies.pop(0))

    def close(self) -> None:
        pass


# ---------------------------------------------------------------------------
# WebSocket adapter tests
# ---------------------------------------------------------------------------


def test_ws_ping_returns_true() -> None:
    ws = _FakeWS(replies=[b"+PONG\r\n"])
    with _patch_ws(ws):
        r = connect_ws("wss://example/ws")
        assert r.ping() is True
    assert ws.sent == [b"*1\r\n$4\r\nPING\r\n"]


def test_ws_set_then_get_roundtrip() -> None:
    ws = _FakeWS(replies=[b"+OK\r\n", b"$5\r\nworld\r\n"])
    with _patch_ws(ws):
        r = connect_ws("wss://example/ws")
        assert r.set("hello", "world") is True
        assert r.get("hello") == b"world"
    assert ws.sent == [
        b"*3\r\n$3\r\nSET\r\n$5\r\nhello\r\n$5\r\nworld\r\n",
        b"*2\r\n$3\r\nGET\r\n$5\r\nhello\r\n",
    ]


def test_ws_incr_returns_int() -> None:
    ws = _FakeWS(replies=[b":1\r\n", b":2\r\n"])
    with _patch_ws(ws):
        r = connect_ws("wss://example/ws")
        assert r.incr("counter") == 1
        assert r.incr("counter") == 2


def test_ws_hgetall_returns_dict() -> None:
    ws = _FakeWS(replies=[b"*4\r\n$1\r\na\r\n$2\r\nv1\r\n$1\r\nb\r\n$2\r\nv2\r\n"])
    with _patch_ws(ws):
        r = connect_ws("wss://example/ws")
        assert r.hgetall("h") == {b"a": b"v1", b"b": b"v2"}


def test_ws_server_error_raises_response_error() -> None:
    ws = _FakeWS(replies=[b"-WRONGTYPE op on wrong kind\r\n"])
    with _patch_ws(ws):
        r = connect_ws("wss://example/ws")
        with pytest.raises(redis.exceptions.ResponseError) as exc:
            r.get("k")
    assert "WRONGTYPE" in str(exc.value)


def test_ws_pipeline_preserves_order() -> None:
    ws = _FakeWS(replies=[b"+OK\r\n", b":1\r\n", b":2\r\n", b"$1\r\n2\r\n"])
    with _patch_ws(ws):
        r = connect_ws("wss://example/ws")
        with r.pipeline(transaction=False) as p:
            p.set("k", "1")
            p.incr("k")
            p.incr("k")
            p.get("k")
            out = p.execute()
    assert out == [True, 1, 2, b"2"]


def test_ws_decode_responses_gives_str() -> None:
    ws = _FakeWS(replies=[b"+OK\r\n", b"$5\r\nworld\r\n"])
    with _patch_ws(ws):
        pool = redis.ConnectionPool(
            connection_class=RustyAntWSConnection,
            url="wss://example/ws",
            decode_responses=True,
        )
        r = redis.Redis(connection_pool=pool)
        r.set("hello", "world")
        assert r.get("hello") == "world"  # str, not bytes


def test_ws_url_stored_on_host_error() -> None:
    # Direct construction path — skip the pool/Redis layer.
    conn = RustyAntWSConnection(url="wss://example/ws")
    assert conn._host_error() == "rustyant[wss://example/ws]"


# ---------------------------------------------------------------------------
# HTTP adapter tests
# ---------------------------------------------------------------------------


def test_http_ping_returns_true() -> None:
    sess = _FakeSession(replies=[b"+PONG\r\n"])
    r = connect_http("https://example/", session=sess)  # type: ignore[arg-type]
    assert r.ping() is True
    assert sess.sent == [b"*1\r\n$4\r\nPING\r\n"]


def test_http_set_get_roundtrip() -> None:
    sess = _FakeSession(replies=[b"+OK\r\n", b"$5\r\nworld\r\n"])
    r = connect_http("https://example/", session=sess)  # type: ignore[arg-type]
    r.set("hello", "world")
    assert r.get("hello") == b"world"


def test_http_missing_key_returns_none() -> None:
    sess = _FakeSession(replies=[b"$-1\r\n"])
    r = connect_http("https://example/", session=sess)  # type: ignore[arg-type]
    assert r.get("missing") is None


def test_http_server_error_raises_response_error() -> None:
    sess = _FakeSession(replies=[b"-WRONGTYPE op on wrong kind\r\n"])
    r = connect_http("https://example/", session=sess)  # type: ignore[arg-type]
    with pytest.raises(redis.exceptions.ResponseError) as exc:
        r.get("k")
    assert "WRONGTYPE" in str(exc.value)


def test_http_pipeline_preserves_order() -> None:
    sess = _FakeSession(replies=[b"+OK\r\n", b":1\r\n", b":2\r\n", b"$1\r\n2\r\n"])
    r = connect_http("https://example/", session=sess)  # type: ignore[arg-type]
    with r.pipeline(transaction=False) as p:
        p.set("k", "1")
        p.incr("k")
        p.incr("k")
        p.get("k")
        out = p.execute()
    assert out == [True, 1, 2, b"2"]
