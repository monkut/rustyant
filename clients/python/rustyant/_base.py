"""
Shared redis-py-shaped API surface used by both transports.

Subclasses implement `_call(*args) -> Any` (and connection lifecycle);
this base handles argument packing and the Redis-style command methods.
Keeping the command surface in one place means the WebSocket and HTTP
clients can't drift in what they expose.
"""

from __future__ import annotations

from typing import Any, Iterable, Union

_Arg = Union[str, bytes, int, float, bool]


class RustyAntError(Exception):
    """Raised when the server returns a RESP `-ERR` reply, or the transport
    closes unexpectedly mid-command."""


def _encode_command(*args: _Arg) -> bytes:
    """Serialize a command tuple into a RESP2 array frame."""
    crlf = b"\r\n"
    parts = [b"*" + str(len(args)).encode() + crlf]
    for arg in args:
        if isinstance(arg, bytes):
            value = arg
        elif isinstance(arg, bool):
            value = b"1" if arg else b"0"
        elif isinstance(arg, (int, float)):
            value = repr(arg).encode()
        else:
            value = str(arg).encode("utf-8")
        parts.append(b"$" + str(len(value)).encode() + crlf + value + crlf)
    return b"".join(parts)


class _RedisShapedClient:
    """Mixin providing the redis-py-shaped command surface.

    Subclasses must implement `_call(*args) -> Any` which serializes the
    command via `_encode_command`, ships the bytes over the wire, and
    returns the parsed RESP reply (raising `RustyAntError` on server-side
    `-ERR` replies).
    """

    def _call(self, *args: _Arg) -> Any:  # noqa: D401
        raise NotImplementedError

    # ---- Server --------------------------------------------------------

    def ping(self) -> bytes:
        return self._call("PING")

    # ---- Strings -------------------------------------------------------

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

    def setnx(self, key: _Arg, value: _Arg) -> int:
        return self._call("SETNX", key, value)

    def setex(self, key: _Arg, seconds: int, value: _Arg) -> bytes:
        return self._call("SETEX", key, seconds, value)

    def get(self, key: _Arg) -> bytes | None:
        return self._call("GET", key)

    def mget(self, *keys: _Arg) -> list[bytes | None]:
        return self._call("MGET", *keys)

    def mset(self, *key_value_pairs: _Arg) -> bytes:
        if len(key_value_pairs) < 2 or len(key_value_pairs) % 2 != 0:
            raise ValueError("mset requires an even number of key/value args")
        return self._call("MSET", *key_value_pairs)

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

    # ---- Hashes --------------------------------------------------------

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

    def hlen(self, key: _Arg) -> int:
        return self._call("HLEN", key)

    def hkeys(self, key: _Arg) -> list[bytes]:
        return self._call("HKEYS", key)

    def hvals(self, key: _Arg) -> list[bytes]:
        return self._call("HVALS", key)

    def hexists(self, key: _Arg, field: _Arg) -> int:
        return self._call("HEXISTS", key, field)

    def hmget(self, key: _Arg, *fields: _Arg) -> list[bytes | None]:
        return self._call("HMGET", key, *fields)

    def hincrby(self, key: _Arg, field: _Arg, delta: int) -> int:
        return self._call("HINCRBY", key, field, delta)

    # ---- Lists ---------------------------------------------------------

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

    def llen(self, key: _Arg) -> int:
        return self._call("LLEN", key)

    # ---- Sets ----------------------------------------------------------

    def sadd(self, key: _Arg, *members: _Arg) -> int:
        return self._call("SADD", key, *members)

    def srem(self, key: _Arg, *members: _Arg) -> int:
        return self._call("SREM", key, *members)

    def smembers(self, key: _Arg) -> list[bytes]:
        return self._call("SMEMBERS", key)

    def sismember(self, key: _Arg, member: _Arg) -> int:
        return self._call("SISMEMBER", key, member)

    def scard(self, key: _Arg) -> int:
        return self._call("SCARD", key)

    # ---- Sorted sets ---------------------------------------------------

    def zadd(
        self,
        key: _Arg,
        mapping: dict[str, float] | Iterable[tuple[float, str]],
    ) -> int:
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

    def zrem(self, key: _Arg, *members: _Arg) -> int:
        return self._call("ZREM", key, *members)

    def zincrby(self, key: _Arg, delta: float, member: _Arg) -> bytes:
        return self._call("ZINCRBY", key, delta, member)

    def zrange(self, key: _Arg, start: int, stop: int) -> list[bytes]:
        return self._call("ZRANGE", key, start, stop)

    def zscore(self, key: _Arg, member: _Arg) -> bytes | None:
        return self._call("ZSCORE", key, member)

    def zcard(self, key: _Arg) -> int:
        return self._call("ZCARD", key)
