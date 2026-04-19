"""
Rustyant HTTP client — one POST per command, RESP2 in body/response.

Alternative to the WebSocket `Client` when the rustyant Lambda is fronted by
an HTTP endpoint (Lambda Function URL, API Gateway HTTP API, or the legacy
REST API). Both HTTP and WebSocket deployments are VPC-less and have
equivalent infrastructure footprint — the distinction is developer-facing:
the HTTP path is one Lambda behind one HTTPS route, while the WebSocket path
needs `$connect`/`$disconnect`/`$default` routes and the
`execute-api:ManageConnections` IAM grant so the Lambda can reply via the
management API. Trade-off: HTTP pays a per-command TLS handshake; WebSocket
pays the handshake once per connection.

Example:

    from rustyant import HttpClient

    c = HttpClient("https://abc123.lambda-url.us-east-1.on.aws")
    c.set("hello", "world")
    assert c.get("hello") == b"world"
"""

from __future__ import annotations

from typing import Any

import hiredis
import requests

from rustyant._base import RustyAntError, _Arg, _RedisShapedClient, _encode_command


class HttpClient(_RedisShapedClient):
    """Blocking rustyant HTTP client. Thread-unsafe — one HttpClient per thread."""

    def __init__(
        self,
        url: str,
        *,
        timeout: float = 10.0,
        session: requests.Session | None = None,
    ) -> None:
        if not url.startswith(("http://", "https://")):
            raise ValueError(f"expected http:// or https:// URL, got {url!r}")
        self._url = url
        self._timeout = timeout
        self._session = session or requests.Session()
        self._owns_session = session is None

    def close(self) -> None:
        if self._owns_session:
            self._session.close()

    def __enter__(self) -> "HttpClient":
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    # ---- transport -----------------------------------------------------

    def _call(self, *args: _Arg) -> Any:
        packed = _encode_command(*args)
        resp = self._session.post(
            self._url,
            data=packed,
            headers={"Content-Type": "application/resp"},
            timeout=self._timeout,
        )
        body = resp.content
        if not body:
            raise RustyAntError(f"empty response from {self._url} (status {resp.status_code})")

        # One POST = one RESP reply; use a fresh parser per call so a prior
        # short read can't corrupt the next one.
        reader = hiredis.Reader()
        reader.feed(body)
        reply = reader.gets()
        if reply is False:
            raise RustyAntError(f"incomplete RESP reply from {self._url}")
        if isinstance(reply, hiredis.ReplyError):
            raise RustyAntError(str(reply))
        return reply
