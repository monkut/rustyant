# rustyant

RustyAnt is a Lambda front end providing a Redis-compatible key-value store backed by S3.

Sibling project to [rustyhip](https://github.com/monkut/rustyhip) (SQLite-over-S3). Same architectural wedge — your data is just files in your S3 bucket — applied to Redis semantics.

## Transports

Two Lambda binaries ship from the same library:

| Binary | Transport | AWS front door | Use when |
|---|---|---|---|
| `rustyant` | RESP-over-HTTP | Lambda URL / API Gateway HTTP | One-shot batch calls, curl/CI, fewer moving parts |
| `rustyant-ws` | RESP-over-WebSocket | API Gateway WebSocket API | redis-py-style client usage, persistent connection, pipelining |

### HTTP transport

RESP commands as HTTP POST bodies to the Lambda URL; reply is the response body.

```
POST /  HTTP/1.1
Content-Type: application/resp

*3\r\n$3\r\nSET\r\n$5\r\nhello\r\n$5\r\nworld\r\n
```

```
HTTP/1.1 200 OK
Content-Type: application/resp

+OK\r\n
```

### WebSocket transport

API Gateway WS API routes `$connect` / `$disconnect` / `$default` events to the `rustyant-ws` Lambda. Each inbound WebSocket frame carries one RESP2 command; the Lambda posts the reply back on the same connection via the API Gateway Management API. Persistent connection → no per-command HTTP handshake, pipelining works, lower tail latency.

Use the [Python client](clients/python/README.md) for a `redis-py`-shaped API. Two transport classes share one method surface:

```python
# WebSocket — persistent connection, pipelining
from rustyant import Client
with Client("wss://abc.execute-api.us-east-1.amazonaws.com/prod") as c:
    c.set("k", "v"); c.get("k")  # b"v"

# HTTP — one POST per command, Lambda Function URL works
from rustyant import HttpClient
with HttpClient("https://abc.lambda-url.us-east-1.on.aws") as c:
    c.set("k", "v"); c.get("k")  # b"v"
```

Neither transport supports `MULTI`/`EXEC`, `SUBSCRIBE`/`PUBLISH`, or streams.

## Command Surface

Implemented:

| Group | Commands |
|---|---|
| Server | `PING` |
| Strings | `GET`, `SET` (+ `EX` / `PX` options), `SETNX`, `SETEX`, `MGET`, `MSET`, `DEL`, `EXISTS`, `EXPIRE`, `TTL`, `INCR`, `INCRBY` |
| Hashes | `HSET`, `HGET`, `HDEL`, `HGETALL`, `HLEN`, `HKEYS`, `HVALS`, `HEXISTS`, `HMGET`, `HINCRBY` |
| Lists | `LPUSH`, `RPUSH`, `LPOP` (+ count), `RPOP` (+ count), `LRANGE`, `LLEN` |
| Sets | `SADD`, `SREM`, `SMEMBERS`, `SISMEMBER`, `SCARD` |
| Sorted Sets | `ZADD`, `ZREM`, `ZINCRBY`, `ZRANGE`, `ZSCORE`, `ZCARD` |

Not implemented (PRs welcome): `GETSET`, `PERSIST`, `KEYS`, `SCAN`, `LINDEX`, `LSET`, `LREM`, `ZRANGEBYSCORE`, and all pub/sub, transactions, scripting, streams, geo.

`MSET` is **not atomic across keys** — a failure partway through leaves earlier keys set. Real Redis is atomic here; rustyant's S3 backing makes the all-or-none semantic expensive, and the fire-and-forget variant is fine for most workloads.

### Concurrency

Read-modify-write commands (INCR, HSET, HDEL, LPUSH, RPUSH, LPOP, RPOP, SADD, ZADD, EXPIRE) use S3 conditional writes (`If-Match` on `ETag`) with bounded retry. Each `load → compute → save` goes through `If-Match: <etag>` on create-over-existing, or `If-None-Match: *` on first write. On HTTP 412 (precondition failed → concurrent modification) the operation backs off (10/20/40/80/160 ms) and re-reads; after 5 unsuccessful attempts the handler returns RESP `-ERR too much contention — retries exhausted`.

Known gaps:
- When HDEL / LPOP / RPOP removes the last element and the key becomes empty, the underlying `DeleteObject` is unconditional (S3 doesn't support conditional delete). A narrow race window exists where a concurrent writer can be clobbered by the empty-collection cleanup.
- The floci S3 emulator does **not** enforce conditional-write headers — it returns 200 on every PUT regardless of `If-Match`. The test `s3_concurrent_incr_converges` in `tests/floci.rs` is gated behind `RUSTYANT_S3_CAS=1` and only validates the retry loop against real AWS S3.

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

## Deploying the WebSocket API

The [`infra/template.yaml`](infra/template.yaml) SAM template provisions the API Gateway WebSocket API, the `rustyant-ws` Lambda, the S3 bucket, and the IAM policy granting `execute-api:ManageConnections`. Deployment requires the `sam` CLI, `cargo-lambda`, and AWS credentials for the target account.

```bash
just ws-template-validate                       # sam validate --lint
just ws-template-build                          # sam build (invokes cargo-lambda)
just ws-template-deploy BUCKET=my-kv-bucket     # creates stack rant-rustyant-ws
```

Outputs include `WebSocketUrl`, which is the `wss://…` URL to hand to the Python client:

```python
from rustyant import Client
c = Client("wss://abc123.execute-api.ap-northeast-1.amazonaws.com/prod")
c.set("hello", "world")
```

The HTTP variant (Lambda URL fronting the `rustyant` binary) is deployed separately via `just lambda-deploy` — not provisioned by this template.

## Status

Working: RESP over HTTP and WebSocket, full string/hash/list/set/zset command dispatch, S3-backed storage with per-key TTL, 56+ Rust tests (8 RESP units + 8 WS units + 34 handler integration + 6 floci-gated S3), 8 Python client encoder tests, CI on GitHub Actions with floci as a service container, SAM template for the WebSocket stack.

Not wired: no CI step for `sam validate`; no end-to-end test driving a real WebSocket connection against the deployed binary. No conditional-write concurrency control — read-modify-write commands (INCR, HSET, etc.) are last-writer-wins under concurrency.
