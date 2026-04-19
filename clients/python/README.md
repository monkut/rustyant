# rustyant — redis-py adapters

`redis-py` adapters for [rustyant](https://github.com/monkut/rustyant). The package exposes two `redis.connection.AbstractConnection` subclasses so the stock `redis.Redis(...)` client works against a rustyant deployment without any bespoke API.

## Install

```bash
pip install rustyant
# or
uv pip install rustyant
```

Brings in `redis>=5.0`, `hiredis`, `requests`, and `websocket-client`.

## Usage

Two transport classes; both plug into `redis.Redis` via a `ConnectionPool`. Convenience helpers wrap the boilerplate.

### WebSocket transport (API Gateway WebSocket API)

```python
from rustyant import connect_ws

r = connect_ws("wss://abc123.execute-api.us-east-1.amazonaws.com/prod")

r.set("hello", "world")
assert r.get("hello") == b"world"

r.hset("profile", mapping={"name": "alice", "age": 30})
assert r.hgetall("profile") == {b"name": b"alice", b"age": b"30"}

r.zadd("scores", {"alice": 10, "bob": 5})
assert r.zrange("scores", 0, -1) == [b"bob", b"alice"]
```

### HTTP transport (Lambda Function URL / API Gateway HTTP API)

```python
from rustyant import connect_http

r = connect_http("https://abc123.lambda-url.us-east-1.on.aws")

r.set("hello", "world")
assert r.get("hello") == b"world"
```

### Explicit `ConnectionPool`

The helpers are sugar; under the hood they construct a pool. Use the classes directly when you want custom pool settings:

```python
from redis import ConnectionPool, Redis
from rustyant import RustyAntWSConnection

pool = ConnectionPool(
    connection_class=RustyAntWSConnection,
    url="wss://abc123.execute-api.us-east-1.amazonaws.com/prod",
    max_connections=4,
    socket_timeout=10,
)
r = Redis(connection_pool=pool)
```

## What works, what doesn't

Everything that maps to rustyant's command surface works through the standard `redis-py` API — `get`, `set` (with `ex`/`px`), `mget`, `mset`, `setnx`, `incr`, `hset` (mapping or positional), `hmget`, `hincrby`, `lpush`, `lrange`, `sadd`, `zadd`, `zincrby`, `zrange`, pipelines (`r.pipeline(transaction=False)`), etc. `redis.Redis(decode_responses=True)` returns `str` instead of `bytes` as usual.

Not supported — rustyant doesn't implement these, so calling them raises `redis.exceptions.ResponseError`:
- `MULTI` / `EXEC` transactions (rustyant has no transaction log)
- `WATCH` / `UNWATCH`
- `SUBSCRIBE` / `PUBLISH` / `PSUBSCRIBE` (no pub/sub)
- `EVAL` / scripting
- Streams (`XADD`, `XREAD`, ...)
- Geo (`GEOADD`, `GEOSEARCH`, ...)
- `CLIENT SETINFO` — suppressed automatically at connection setup via an empty `DriverInfo`, so you don't see these errors
- `HELLO` — suppressed; the adapter forces `protocol=2` (RESP2)

## Architecture

Both adapter classes bypass `redis-py`'s socket layer:

- `_connect()` opens the WebSocket / HTTP session and stores a sentinel in `self._sock` so `redis-py`'s is-connected checks pass
- `send_packed_command(command)` splits the RESP2 bytes into individual frames (pipelines arrive as one concatenated blob) and ships each frame as one WS binary message / HTTP POST
- `read_response()` parses each reply through `hiredis.Reader` and surfaces server `-ERR` as `redis.exceptions.ResponseError`
- `on_connect()` is a no-op — no AUTH, no SELECT DB, no CLIENT SETINFO, no HELLO

## Why a Connection subclass instead of a bespoke client

Earlier iterations of this package shipped a stand-alone `Client` class that mimicked `redis-py`'s method names. That's the wrong shape: users already have `redis-py`, and ORMs / libraries that consume a `redis.Redis` instance can't accept a drop-in replacement with a different class. The `AbstractConnection` subclass slots into the existing `redis-py` machinery — all of the library's features (response callbacks, exceptions, pipelines, custom encodings, connection pooling, retry policies) work unchanged.

## License

See the parent [LICENSE](../../LICENSE) in the rustyant repository.
