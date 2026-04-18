# rustyant

RustyAnt is a Lambda front end providing a Redis-compatible (RESP-over-HTTP) key-value store backed by S3.

Sibling project to [rustyhip](https://github.com/monkut/rustyhip) (SQLite-over-S3). Same architectural wedge — your data is just files in your S3 bucket — applied to Redis semantics.

## Protocol

RESP commands are sent as HTTP POST bodies to the Lambda URL. The response body is a RESP reply.

```
POST /  HTTP/1.1
Content-Type: application/resp

*3\r\n$3\r\nSET\r\n$5\r\nhello\r\n$5\r\nworld\r\n
```

Response:

```
HTTP/1.1 200 OK
Content-Type: application/resp

+OK\r\n
```

This is not a drop-in replacement for a real Redis client — pipelining, MULTI/EXEC, and PUB/SUB are not supported by the HTTP transport. Suited for agent tool calls, batch KV reads/writes, and serverless workloads.

## Command Surface

Implemented:

| Group | Commands |
|---|---|
| Server | `PING` |
| Strings | `GET`, `SET` (+ `EX` / `PX` options), `DEL`, `EXISTS`, `EXPIRE`, `TTL`, `INCR`, `INCRBY` |
| Hashes | `HSET`, `HGET`, `HDEL`, `HGETALL` |
| Lists | `LPUSH`, `RPUSH`, `LPOP` (+ count), `RPOP` (+ count), `LRANGE` |
| Sets | `SADD` |
| Sorted Sets | `ZADD`, `ZRANGE` |

Not implemented (PRs welcome): `GETSET`, `MGET`, `MSET`, `SETNX`, `SETEX`, `PERSIST`, `KEYS`, `SCAN`, `HEXISTS`, `HKEYS`, `HVALS`, `HLEN`, `HINCRBY`, `LLEN`, `LINDEX`, `LSET`, `LREM`, `SREM`, `SMEMBERS`, `SISMEMBER`, `SCARD`, `ZREM`, `ZSCORE`, `ZINCRBY`, `ZRANGEBYSCORE`, `ZCARD`, and all pub/sub, transactions, scripting, streams, geo.

### Concurrency caveat

Every read-modify-write (INCR, HSET, LPUSH, SADD, ZADD, HDEL, LPOP/RPOP) is inherently racy across concurrent Lambda invocations — S3 PUT is last-writer-wins. A production deployment needs S3 conditional writes (`If-Match` on ETag) or a DynamoDB optimistic-lock layer. Not addressed in this scaffold.

## Architecture

```
┌────────────┐   RESP-over-HTTP   ┌─────────────────┐   put/get       ┌──────────┐
│  client    │ ─────────────────> │  Lambda         │ ──────────────> │   S3     │
│ (CLI/SDK)  │ <───────────────── │  rustyant       │ <────────────── │  bucket  │
└────────────┘                    └─────────────────┘                 └──────────┘
```

Each Redis key maps to one S3 object under `${KEY_PREFIX}${key}`. The object body is JSON with a tagged union discriminating string/hash/list/set/zset.

## Local Development

Rust: `1.85+` (edition `2024`), toolchain pinned via `rust-toolchain.toml`.

```bash
rustup show               # install toolchain
cargo fetch               # pull dependencies
just check                # fmt + clippy
just test                 # cargo-nextest
```

### Environment

- `BUCKET` (required) — S3 bucket holding the key objects.
- `KEY_PREFIX` (default `rustyant/`) — prefix prepended to every key.
- `AWS_REGION`, `AWS_ENDPOINT_URL` — standard AWS env; `AWS_ENDPOINT_URL` points at a local S3 emulator.

### Local S3 (floci)

Same pattern as the sibling [rustyhip](https://github.com/monkut/rustyhip) project — a docker-hosted S3 emulator ([floci](https://github.com/floci-io/floci)) on `http://localhost:4566`. Requires `docker` and the `aws` CLI. Storage is in-memory — restarting floci wipes buckets.

```bash
just floci-up               # start the emulator (container: rustyant-floci)
just floci-seed             # create the rustyant-dev bucket
just rustyant-dev           # cargo lambda watch against floci on :9000
just floci-down             # tear down
```

All recipes take overridable parameters, e.g. `just floci-seed BUCKET=my-bucket` or `just rustyant-dev BUCKET=my-bucket KEY_PREFIX=tenant42/`.

Once `rustyant-dev` is running, fire a RESP command at it:

```bash
# SET hello world
printf '*3\r\n$3\r\nSET\r\n$5\r\nhello\r\n$5\r\nworld\r\n' | \
  curl -s --data-binary @- http://localhost:9000/lambda-url/rustyant/
```

## Status

Working scaffold: RESP-over-HTTP transport, full string/hash/list/set/zset command dispatch, S3-backed storage with per-key TTL. 8 RESP round-trip tests passing. No integration tests against a real Lambda runtime yet; no CI, no deny.toml, no pre-commit config (see the sibling rustyhip repo for the full tooling template).
