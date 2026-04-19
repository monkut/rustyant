"""Tests for the HTTP transport client.

No network — a fake `requests.Session` intercepts POSTs and returns
canned RESP replies. Combined with the encoder tests, this proves the
HTTP client produces the bytes the rustyant handler accepts and parses
the bytes the handler returns.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from rustyant import HttpClient, RustyAntError


@dataclass
class _FakeResponse:
    content: bytes
    status_code: int = 200


class _FakeSession:
    """Captures POSTs and plays back a programmed queue of replies."""

    def __init__(self, replies: list[bytes]) -> None:
        self._replies = list(replies)
        self.sent: list[bytes] = []

    def post(self, url: str, *, data: bytes, headers: dict[str, str], timeout: float) -> _FakeResponse:
        assert url == "https://example/"
        assert headers["Content-Type"] == "application/resp"
        assert timeout > 0
        self.sent.append(data)
        if not self._replies:
            raise AssertionError("no more canned replies")
        return _FakeResponse(self._replies.pop(0))

    def close(self) -> None:
        pass


def _client(replies: list[bytes]) -> tuple[HttpClient, _FakeSession]:
    sess = _FakeSession(replies)
    return HttpClient("https://example/", session=sess), sess  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# URL validation
# ---------------------------------------------------------------------------


def test_http_client_rejects_non_http_url() -> None:
    with pytest.raises(ValueError):
        HttpClient("wss://example/")
    with pytest.raises(ValueError):
        HttpClient("redis://example/")


# ---------------------------------------------------------------------------
# Round trips — bytes sent match encoder, replies parse via hiredis
# ---------------------------------------------------------------------------


def test_ping_returns_pong() -> None:
    c, sess = _client([b"+PONG\r\n"])
    assert c.ping() == b"PONG"
    assert sess.sent == [b"*1\r\n$4\r\nPING\r\n"]


def test_set_and_get_roundtrip() -> None:
    c, sess = _client([b"+OK\r\n", b"$5\r\nhello\r\n"])
    assert c.set("k", "hello") == b"OK"
    assert c.get("k") == b"hello"
    assert sess.sent == [
        b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nhello\r\n",
        b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n",
    ]


def test_get_missing_returns_none() -> None:
    c, _ = _client([b"$-1\r\n"])
    assert c.get("missing") is None


def test_incr_returns_int() -> None:
    c, _ = _client([b":5\r\n"])
    assert c.incr("counter") == 5


def test_hgetall_returns_dict() -> None:
    # HGETALL replies as a flat array which the base class zips into a dict.
    c, _ = _client([b"*4\r\n$1\r\na\r\n$2\r\nv1\r\n$1\r\nb\r\n$2\r\nv2\r\n"])
    assert c.hgetall("h") == {b"a": b"v1", b"b": b"v2"}


def test_mget_returns_list_with_nil() -> None:
    c, _ = _client([b"*3\r\n$1\r\n1\r\n$-1\r\n$1\r\n3\r\n"])
    assert c.mget("a", "b", "c") == [b"1", None, b"3"]


# ---------------------------------------------------------------------------
# Error propagation
# ---------------------------------------------------------------------------


def test_server_error_reply_raises() -> None:
    c, _ = _client([b"-WRONGTYPE oops\r\n"])
    with pytest.raises(RustyAntError) as exc:
        c.get("k")
    assert "WRONGTYPE" in str(exc.value)


def test_empty_body_raises() -> None:
    c, _ = _client([b""])
    with pytest.raises(RustyAntError):
        c.ping()


# ---------------------------------------------------------------------------
# Resource management
# ---------------------------------------------------------------------------


def test_context_manager_closes_only_owned_session() -> None:
    # Caller passed a session — close() must not close it.
    sess = _FakeSession([b"+PONG\r\n"])
    with HttpClient("https://example/", session=sess) as c:  # type: ignore[arg-type]
        c.ping()
    # Nothing broken if sess.close is called again explicitly:
    sess.close()
