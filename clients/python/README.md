# rustyant — Python client

Python client for [rustyant](https://github.com/monkut/rustyant) — a Redis-compatible key-value store served over AWS API Gateway WebSocket + Lambda + S3.

## Install

```bash
pip install rustyant
# or
uv pip install rustyant
```

## Usage

```python
from rustyant import Client

c = Client("wss://abc123.execute-api.us-east-1.amazonaws.com/prod")

c.set("hello", "world")
assert c.get("hello") == b"world"

c.hset("profile", "name", "alice", "age", "30")
assert c.hget("profile", "name") == b"alice"
assert c.hgetall("profile") == {b"name": b"alice", b"age": b"30"}

c.rpush("queue", "a", "b", "c")
assert c.lrange("queue", 0, -1) == [b"a", b"b", b"c"]

c.zadd("scores", {"alice": 10, "bob": 5})
assert c.zrange("scores", 0, -1) == [b"bob", b"alice"]

c.close()
```

Context-manager form auto-closes the WebSocket:

```python
with Client("wss://…") as c:
    c.set("k", "v")
```

## Command surface

| Group       | Methods                                                                  |
| ----------- | ------------------------------------------------------------------------ |
| Server      | `ping`                                                                   |
| Strings     | `get`, `set` (`ex=`, `px=`), `delete`, `exists`, `expire`, `ttl`, `incr`, `incrby` |
| Hashes      | `hset`, `hget`, `hdel`, `hgetall`                                        |
| Lists       | `lpush`, `rpush`, `lpop` (optional `count`), `rpop` (optional `count`), `lrange`  |
| Sets        | `sadd`                                                                   |
| Sorted sets | `zadd`, `zrange`                                                         |

Return values mirror `redis-py` defaults: bytes for bulk-string replies, `None` for nil, `int` for integer replies, `dict[bytes, bytes]` for `hgetall`. Server-side errors are raised as `RustyAntError`.

## Requirements

- Python ≥ 3.9
- `websocket-client` ≥ 1.7 (TCP WebSocket transport)
- `hiredis` ≥ 2.0 (RESP2 parser)

## Transport

Each command is sent as one binary WebSocket frame carrying a RESP2 array. The server replies on the same connection via the API Gateway Management API. Multi-frame pipelines work but are serialized (one in flight per connection) — use multiple clients to parallelize.

Unsupported vs. real Redis: `MULTI`/`EXEC`, `SUBSCRIBE`/`PUBLISH`, scripting, streams, geo.

## License

See the parent [LICENSE](../../LICENSE) in the rustyant repository.
