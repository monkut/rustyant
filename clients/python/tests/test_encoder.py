"""Encoder tests — byte-level wire-format compatibility checks.

These tests have zero network dependencies and zero floci dependencies.
They assert that `_encode_command` produces the exact RESP2 bytes the
rustyant handler's integration tests are known to accept.
"""

from __future__ import annotations

import pytest

from rustyant.client import _encode_command


@pytest.mark.parametrize(
    ("args", "expected"),
    [
        (("PING",), b"*1\r\n$4\r\nPING\r\n"),
        (("SET", "hello", "world"),
         b"*3\r\n$3\r\nSET\r\n$5\r\nhello\r\n$5\r\nworld\r\n"),
        (("SET", "k", "v", "EX", 60),
         b"*5\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n$2\r\nEX\r\n$2\r\n60\r\n"),
        (("HSET", "h", "f1", "v1", "f2", "v2"),
         b"*6\r\n$4\r\nHSET\r\n$1\r\nh\r\n$2\r\nf1\r\n$2\r\nv1\r\n$2\r\nf2\r\n$2\r\nv2\r\n"),
        (("LPUSH", "l", "a", "b", "c"),
         b"*5\r\n$5\r\nLPUSH\r\n$1\r\nl\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n"),
        (("INCRBY", "counter", 10),
         b"*3\r\n$6\r\nINCRBY\r\n$7\r\ncounter\r\n$2\r\n10\r\n"),
    ],
)
def test_encode_matches_rustyant_wire_format(args: tuple[object, ...], expected: bytes) -> None:
    assert _encode_command(*args) == expected


def test_bytes_args_pass_through_unchanged() -> None:
    packed = _encode_command("SET", b"\xff\x00\xaa", b"\x01\x02")
    assert packed == b"*3\r\n$3\r\nSET\r\n$3\r\n\xff\x00\xaa\r\n$2\r\n\x01\x02\r\n"


def test_bool_encodes_as_one_or_zero() -> None:
    assert _encode_command("SET", "k", True) == b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\n1\r\n"
    assert _encode_command("SET", "k", False) == b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\n0\r\n"
