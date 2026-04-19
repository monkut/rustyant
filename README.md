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

Use the [redis-py adapters](clients/python/README.md) — `redis.Redis(...)` works directly:

```python
from rustyant import connect_ws, connect_http

r = connect_ws("wss://abc.execute-api.us-east-1.amazonaws.com/prod")
r.set("k", "v"); r.get("k")  # b"v"

r = connect_http("https://abc.lambda-url.us-east-1.on.aws")
r.set("k", "v"); r.get("k")  # b"v"
```

The `connect_ws` / `connect_http` helpers return a `redis.Redis` instance backed by a `RustyAntWSConnection` / `RustyAntHttpConnection` — so anything that consumes a `redis.Redis` (ORMs, session stores, rate-limiters, third-party libs) works unchanged.

Neither transport supports `MULTI`/`EXEC`, `SUBSCRIBE`/`PUBLISH`, or streams.

## Command Surface

Implemented:

| Group | Commands |
|---|---|
| Server | `PING` |
| Keyspace | `KEYS`, `SCAN` (+ `MATCH` / `COUNT` options), `TYPE` |
| Strings | `GET`, `SET` (+ `EX` / `PX` options), `GETSET`, `SETNX`, `SETEX`, `MGET`, `MSET`, `DEL`, `EXISTS`, `EXPIRE`, `EXPIREAT`, `PEXPIREAT`, `PERSIST`, `TTL`, `INCR`, `INCRBY` |
| Hashes | `HSET`, `HGET`, `HDEL`, `HGETALL`, `HLEN`, `HKEYS`, `HVALS`, `HEXISTS`, `HMGET`, `HINCRBY` |
| Lists | `LPUSH`, `RPUSH`, `LPOP` (+ count), `RPOP` (+ count), `LRANGE`, `LLEN`, `LINDEX`, `LSET`, `LREM` |
| Sets | `SADD`, `SREM`, `SMEMBERS`, `SISMEMBER`, `SCARD` |
| Sorted Sets | `ZADD`, `ZREM`, `ZINCRBY`, `ZRANGE`, `ZRANGEBYSCORE`, `ZSCORE`, `ZCARD` |

`KEYS` paginates through `ListObjectsV2` in full and filters by the wildmatch pattern — O(n) over the keyspace, safe at low cardinality, proportionally slower for larger buckets. `SCAN` delegates the page boundary to S3 via a continuation token, returning one `ListObjectsV2` page per call; `MATCH` is applied client-side, so the per-page yield may be smaller than `COUNT` when a pattern is narrow.

Not implemented (PRs welcome): pub/sub, transactions, scripting, streams, geo.

`MSET` is **not atomic across keys** — a failure partway through leaves earlier keys set. Real Redis is atomic here; rustyant's S3 backing makes the all-or-none semantic expensive, and the fire-and-forget variant is fine for most workloads.

### Concurrency

Read-modify-write commands (INCR, HSET, HDEL, LPUSH, RPUSH, LPOP, RPOP, SADD, ZADD, EXPIRE) use S3 conditional writes (`If-Match` on `ETag`) with bounded retry. Each `load → compute → save` goes through `If-Match: <etag>` on create-over-existing, or `If-None-Match: *` on first write. When a mutation empties a collection (last field/element removed), the cleanup `DeleteObject` is also conditional on `If-Match: <etag>`, so a concurrent writer's new value isn't clobbered. On HTTP 412 (precondition failed → concurrent modification) the operation backs off (10/20/40/80/160 ms) and re-reads; after 5 unsuccessful attempts the handler returns RESP `-ERR too much contention — retries exhausted`.

Known gaps:
- The floci S3 emulator does **not** enforce conditional-write headers — it returns 200 on every PUT/DELETE regardless of `If-Match`. The test `s3_concurrent_incr_converges` in `tests/floci.rs` is gated behind `RUSTYANT_S3_CAS=1` and only validates the retry loop against real AWS S3.

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
- `RUSTYANT_EMF_NAMESPACE` (optional) — when set, each dispatched command emits a CloudWatch Embedded Metric Format line to stdout with `DispatchCount` and `DispatchLatency` under the given namespace, dimensioned by `{Command, Outcome}`. Unset in local dev so the terminal stays clean; the SAM template sets it to `rustyant` for deployed Lambdas.

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
from rustyant import connect_ws
r = connect_ws("wss://abc123.execute-api.ap-northeast-1.amazonaws.com/prod")
r.set("hello", "world")
```

The HTTP variant (Lambda URL fronting the `rustyant` binary) is deployed separately via `just lambda-deploy` — not provisioned by this template.

## Observability

The `rustyant` and `rustyant-ws` binaries emit structured JSON logs via `tracing`; each dispatched command produces one log line with `command`, `argc`, `outcome` (ok / wrong_type / contention / s3 / …), and `duration_ms`. When `RUSTYANT_EMF_NAMESPACE` is set (the SAM template sets it to `rustyant` by default), each dispatch also emits a CloudWatch Embedded Metric Format line — CloudWatch Logs auto-extracts `DispatchCount` and `DispatchLatency` under that namespace, dimensioned by `{Command, Outcome}`, so dashboards can slice by command and failure mode without SDK calls.

## Status

Working: RESP over HTTP and WebSocket, full string/hash/list/set/zset command dispatch plus `KEYS` / `SCAN`, S3-backed storage with per-key TTL and conditional-write CAS on every read-modify-write, 123 Rust tests across 5 suites (18 lib units + 81 HTTP integration + 11 redis-py compat + 6 WebSocket E2E + 7 encoder) and 13 Python client tests, structured logs and CloudWatch EMF metrics, CI on GitHub Actions with floci as a service container, SAM template validated in CI.

Not wired: no end-to-end test driving a real WebSocket connection against a deployed binary in AWS; the `s3_concurrent_incr_converges` CAS test is gated behind `RUSTYANT_S3_CAS=1` because floci doesn't enforce `If-Match` headers.
