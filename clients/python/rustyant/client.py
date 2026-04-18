"""
Rustyant client — WebSocket transport, RESP2 wire format.

Connects to an API Gateway WebSocket endpoint fronting the rustyant Lambda
and issues Redis-style commands against the S3-backed key/value store. The
method names mirror `redis-py` where the semantics line up, so porting small
amounts of application code is usually one import change.

Example:

    from rustyant import Client

    c = Client("wss://abc123.execute-api.us-east-1.amazonaws.com/prod")
    c.set("hello", "world")
    assert c.get("hello") == b"world"
    c.hset("profile", "name", "alice", "age", "30")
    assert c.hget("profile", "name") == b"alice"
    c.close()
"""

from __future__ import annotations

from typing import Any, Iterable, Union

import hiredis
import websocket


class RustyAntError(Exception):
    """Raised when the server returns a RESP `-ERR` reply."""


_Arg = Union[str, bytes, int, float, bool]


def _encode_command(*args: _Arg) -> bytes:
    """Serialize a command tuple into a RESP2 array frame."""
    crlf = b"\r\n"
    parts = [b"*" + str(len(args)).encode() + crlf]
    for arg in args:
        if isinstance(arg, bytes):
            value = arg
        elif isinstance(arg, bool):
            # Match redis-py: True→b"1", False→b"0".
            value = b"1" if arg else b"0"
        elif isinstance(arg, (int, float)):
            value = repr(arg).encode()
        else:
            value = str(arg).encode("utf-8")
        parts.append(b"$" + str(len(value)).encode() + crlf + value + crlf)
    return b"".join(parts)


class Client:
    """Blocking rustyant client. Thread-unsafe — one Client per thread."""

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

    # ---- low-level dispatch --------------------------------------------

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

    # ---- Redis-style command surface -----------------------------------

    def ping(self) -> bytes:
        return self._call("PING")

    def set(
        self,
        key: _Arg,
        value: _Arg,
        *,
        ex: int | None = None,
        px: int | None = None,
    ) -> bytes:
        args: list[_Arg] = ["SET", key, value]
        if ex is not None:
            args += ["EX", ex]
        if px is not None:
            args += ["PX", px]
        return self._call(*args)

    def get(self, key: _Arg) -> bytes | None:
        return self._call("GET", key)

    def delete(self, *keys: _Arg) -> int:
        return self._call("DEL", *keys)

    def exists(self, *keys: _Arg) -> int:
        return self._call("EXISTS", *keys)

    def expire(self, key: _Arg, seconds: int) -> int:
        return self._call("EXPIRE", key, seconds)

    def ttl(self, key: _Arg) -> int:
        return self._call("TTL", key)

    def incr(self, key: _Arg) -> int:
        return self._call("INCR", key)

    def incrby(self, key: _Arg, delta: int) -> int:
        return self._call("INCRBY", key, delta)

    def hset(self, key: _Arg, *field_value_pairs: _Arg) -> int:
        if len(field_value_pairs) < 2 or len(field_value_pairs) % 2 != 0:
            raise ValueError("hset requires an even number of field/value args")
        return self._call("HSET", key, *field_value_pairs)

    def hget(self, key: _Arg, field: _Arg) -> bytes | None:
        return self._call("HGET", key, field)

    def hdel(self, key: _Arg, *fields: _Arg) -> int:
        return self._call("HDEL", key, *fields)

    def hgetall(self, key: _Arg) -> dict[bytes, bytes]:
        flat: list[bytes] = self._call("HGETALL", key) or []
        return dict(zip(flat[0::2], flat[1::2]))

    def lpush(self, key: _Arg, *values: _Arg) -> int:
        return self._call("LPUSH", key, *values)

    def rpush(self, key: _Arg, *values: _Arg) -> int:
        return self._call("RPUSH", key, *values)

    def lpop(self, key: _Arg, count: int | None = None) -> Any:
        if count is None:
            return self._call("LPOP", key)
        return self._call("LPOP", key, count)

    def rpop(self, key: _Arg, count: int | None = None) -> Any:
        if count is None:
            return self._call("RPOP", key)
        return self._call("RPOP", key, count)

    def lrange(self, key: _Arg, start: int, stop: int) -> list[bytes]:
        return self._call("LRANGE", key, start, stop)

    def sadd(self, key: _Arg, *members: _Arg) -> int:
        return self._call("SADD", key, *members)

    def zadd(self, key: _Arg, mapping: dict[str, float] | Iterable[tuple[float, str]]) -> int:
        """Accept either {member: score} or an iterable of (score, member) tuples."""
        flat: list[_Arg] = []
        if isinstance(mapping, dict):
            for member, score in mapping.items():
                flat += [score, member]
        else:
            for score, member in mapping:
                flat += [score, member]
        if not flat:
            raise ValueError("zadd requires at least one score/member pair")
        return self._call("ZADD", key, *flat)

    def zrange(self, key: _Arg, start: int, stop: int) -> list[bytes]:
        return self._call("ZRANGE", key, start, stop)
