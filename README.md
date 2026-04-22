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
| Server | `PING`, `ECHO`, `TIME`, `INFO` (+ section filter), `COMMAND` (`COUNT` / `LIST` / `INFO`), `HELLO` (+ `AUTH` / `SETNAME`), `CLIENT` (`SETINFO` / `SETNAME` / `GETNAME` / `ID` / `INFO` / `LIST` / `NO-EVICT` / `REPLY` / `TRACKING` / `PAUSE` / `UNPAUSE`), `AUTH`, `WAIT`, `RESET`, `SAVE`, `BGSAVE` (+ `SCHEDULE`), `BGREWRITEAOF`, `LASTSAVE`, `LATENCY` (`RESET` / `HISTORY` / `LATEST` / `GRAPH` / `DOCTOR`), `DEBUG SLEEP`, `MULTI`, `EXEC`, `DISCARD`, `WATCH`, `UNWATCH`, `SUBSCRIBE`, `PSUBSCRIBE`, `UNSUBSCRIBE`, `PUNSUBSCRIBE`, `PUBLISH`, `PUBSUB` (`CHANNELS` / `NUMSUB` / `NUMPAT`), `DBSIZE`, `FLUSHDB`, `FLUSHALL` |
| Keyspace | `KEYS`, `SCAN` (+ `MATCH` / `COUNT` options), `TYPE`, `RENAME`, `RENAMENX`, `RANDOMKEY`, `UNLINK`, `COPY` (+ `REPLACE` / `DB 0`), `EXPIRETIME`, `PEXPIRETIME` |
| Strings | `GET`, `GETEX` (+ `EX` / `PX` / `EXAT` / `PXAT` / `PERSIST`), `SET` (+ `EX` / `PX`), `GETSET`, `GETDEL`, `GETRANGE`, `SETRANGE`, `SETNX`, `SETEX`, `MGET`, `MSET`, `MSETNX`, `APPEND`, `STRLEN`, `DEL`, `EXISTS`, `EXPIRE`, `EXPIREAT`, `PEXPIRE`, `PEXPIREAT`, `PERSIST`, `TTL`, `PTTL`, `INCR`, `INCRBY`, `INCRBYFLOAT`, `DECR`, `DECRBY`, `GETBIT`, `SETBIT`, `BITCOUNT` (+ `BYTE` / `BIT`), `BITPOS` (+ `BYTE` / `BIT`), `BITOP` (`AND` / `OR` / `XOR` / `NOT`) |
| Hashes | `HSET`, `HSETNX`, `HGET`, `HDEL`, `HGETALL`, `HLEN`, `HKEYS`, `HVALS`, `HEXISTS`, `HSTRLEN`, `HMGET`, `HINCRBY`, `HSCAN` (+ `MATCH` / `COUNT`) |
| Lists | `LPUSH`, `RPUSH`, `LPUSHX`, `RPUSHX`, `LPOP` (+ count), `RPOP` (+ count), `LRANGE`, `LLEN`, `LINDEX`, `LSET`, `LREM`, `LINSERT`, `LTRIM`, `LMOVE`, `RPOPLPUSH`, `LPOS` (+ `RANK` / `COUNT` / `MAXLEN`) |
| Sets | `SADD`, `SREM`, `SMEMBERS`, `SISMEMBER`, `SMISMEMBER`, `SCARD`, `SINTER`, `SUNION`, `SDIFF`, `SPOP` (+ count), `SRANDMEMBER` (+ count), `SSCAN` (+ `MATCH` / `COUNT`) |
| Sorted Sets | `ZADD`, `ZREM`, `ZINCRBY`, `ZRANGE`, `ZREVRANGE`, `ZRANGEBYSCORE`, `ZREVRANGEBYSCORE`, `ZREMRANGEBYRANK`, `ZREMRANGEBYSCORE`, `ZPOPMIN` (+ count), `ZPOPMAX` (+ count), `ZSCORE`, `ZMSCORE`, `ZCARD`, `ZCOUNT`, `ZRANK`, `ZREVRANK`, `ZSCAN` (+ `MATCH` / `COUNT`) |
| Geo | `GEOADD` (+ `NX` / `XX` / `CH`), `GEOPOS`, `GEODIST` (+ `m` / `km` / `ft` / `mi`), `GEOHASH` |

`KEYS` paginates through `ListObjectsV2` in full and filters by the wildmatch pattern — O(n) over the keyspace, safe at low cardinality, proportionally slower for larger buckets. `SCAN` delegates the page boundary to S3 via a continuation token, returning one `ListObjectsV2` page per call; `MATCH` is applied client-side, so the per-page yield may be smaller than `COUNT` when a pattern is narrow.

`HSCAN` / `SSCAN` / `ZSCAN` paginate inside a single collection. The cursor is an integer offset into the caller's iteration and `0` means "start / done", matching Redis. Because each collection is one S3 object, every call loads the full value — pagination is a client-side ergonomic, not a server-side cost saving. `MATCH` is applied after the batch is sliced (Redis semantics), so a narrow pattern can yield fewer than `COUNT` items per page.

`DBSIZE` and `RANDOMKEY` walk `ListObjectsV2` pagination — O(n) in the number of live keys. Recently-expired keys that haven't been GC'd yet still count toward `DBSIZE`, matching Redis's lazy-expiry semantics. `FLUSHDB` and `FLUSHALL` are aliases here — rustyant exposes one logical namespace — and batch-delete a page (up to 1000 objects) per `DeleteObjects` call. The optional `ASYNC` / `SYNC` modifier is accepted but ignored: the flush is always synchronous over S3. `UNLINK` shares the synchronous `DEL` path; rustyant has no background freer thread.

Not implemented (PRs welcome): scripting, streams, geo search (`GEOSEARCH` / `GEOSEARCHSTORE`). Transactions (`MULTI` / `EXEC` / `DISCARD` / `WATCH`) and subscribe-side pub/sub (`SUBSCRIBE` / `PSUBSCRIBE` / `UNSUBSCRIBE` / `PUNSUBSCRIBE`) return explicit errors — rustyant processes one command per HTTP request with no connection-level state, so cross-request queueing, optimistic CAS, and server-pushed pub/sub messages cannot be honored honestly. `UNWATCH`, `PUBLISH`, and the `PUBSUB` introspection subcommands do reply successfully: clearing a never-populated watch set is a trivial no-op; `PUBLISH` returns `:0` because zero subscribers is the literal truth on a no-substrate server; `PUBSUB CHANNELS` / `NUMSUB` / `NUMPAT` return correspondingly empty / zero results. The deprecated `GEORADIUS` / `GEORADIUSBYMEMBER` family is intentionally not surfaced — `GEOSEARCH` is the supported replacement in Redis 7+, and any follow-up work should target it directly.

`MSET` is **not atomic across keys** — a failure partway through leaves earlier keys set. Real Redis is atomic here; rustyant's S3 backing makes the all-or-none semantic expensive, and the fire-and-forget variant is fine for most workloads. `MSETNX`, `RENAME` / `RENAMENX`, and `COPY` are similarly best-effort: a concurrent writer landing between the existence check and the write can leak past the `NX` guard. `RENAMENX` and `COPY` (without `REPLACE`) use `If-None-Match: *` on the destination to shrink that window, so the failure mode is "operation reports 0 / error" rather than data loss.

Bit operations follow Redis's bit numbering: bit 0 is the most significant bit of byte 0. `SETBIT` zero-pads the underlying string to fit the requested offset and runs under the same CAS as other read-modify-write commands. `BITPOS` keeps Redis's asymmetric "infinite trailing zeros" behavior — when searching for a 0 bit without an explicit end, an all-ones string returns `strlen * 8` rather than `-1`; pinning an explicit end suppresses that fiction. `BITOP` reads each source sequentially, pads shorter sources to the longest with zero bytes, and stores the result; an empty result removes the destination instead of writing an empty-string entry.

`LMOVE` / `RPOPLPUSH` are fully atomic when source and destination are the same key (single CAS). Cross-key moves pop from source first, then push to destination — same best-effort guarantee as `RENAME` / `COPY` on the S3 backend. A type-mismatch on the destination is detected before the pop so the source stays intact; a concurrent writer swapping the destination's type between the check and the push can still surface an error after the element has been removed from source. `LPOS` follows Redis semantics: without `COUNT` the reply is the first matching index (`nil` when absent), with `COUNT` it is always an array. `RANK` may be negative (tail→head search) but not zero; `MAXLEN 0` scans the whole list.

`INFO` emits `# Server`, `# Clients`, `# Stats`, and `# Keyspace` sections. `uptime_in_seconds` is measured from the container's cold start, so it resets on every Lambda cold boot rather than tracking a long-lived server process. `connected_clients` is a fixed `1` and `total_commands_processed` is a fixed `0` — there is no cross-invocation counter to report. `# Keyspace` uses `keyspace_stats`, which counts every live S3 object; `expires` is always `0` on the S3 backend because computing it exactly would require a GET per key (future backends can override). `COMMAND INFO` / `COMMAND LIST` / `COMMAND COUNT` return the classic 6-tuple metadata (`name`, `arity`, `flags`, `first_key`, `last_key`, `step`) for every implemented command; `COMMAND DOCS` and `COMMAND GETKEYS` are not implemented.

`GETEX` resolves `EX` / `PX` / `EXAT` / `PXAT` to an absolute epoch-ms on the handler side, then runs one CAS against the key — so a concurrent writer can't race the expiry change with a write. `PERSIST` clears any existing TTL. `EXPIRETIME` / `PEXPIRETIME` return the absolute expiry (seconds / ms); `-1` for no TTL, `-2` for missing keys, matching Redis.

`HELLO` accepts protover `2` and returns the standard info map (`server`, `version`, `proto`, `id`, `mode`, `role`, `modules`); protover `3` returns `-NOPROTO` so clients fall back to RESP2 cleanly. `AUTH` and `SETNAME` are accepted syntactically but ignored — rustyant has no auth backend and no per-connection client tracking. `CLIENT` subcommands are stubbed quietly (`+OK` for `SETINFO` / `SETNAME` / connection-config variants; fixed-value replies for `ID` / `GETNAME` / `INFO` / `LIST`) so redis-py's connection setup doesn't log "unknown command" on every connect. `RESET` returns `+RESET` with no state to clear.

Standalone `AUTH` follows the same "accept-and-ignore" pattern as the HELLO option; there is no credential backend. `WAIT` returns `0` immediately — rustyant has no replication model, and zero replicas is the honest answer. `SAVE` / `BGSAVE` / `BGREWRITEAOF` acknowledge with the same simple strings real Redis does (so monitoring clients parse them unchanged), but the acknowledgment is all there is: every SET is already durable on S3 and there is no AOF. `LASTSAVE` reports the container's cold-start epoch — a reasonable proxy, given the above. `LATENCY` is a stub surface (`RESET` returns `0`, `HISTORY` / `LATEST` return empty arrays, `DOCTOR` returns a bland all-clear string); real latency signals live in CloudWatch EMF (see below). `DEBUG SLEEP <seconds>` actually sleeps — useful for probing client timeout handling — capped at 5s so it can't burn a full Lambda invocation window. All other `DEBUG` subcommands return an explicit error, since the engine-internal state they expose (encoding, memory layout, active-expire toggles) doesn't exist on S3.

Transaction commands follow the same "error explicitly where a lie would hide real misbehavior" rule. `MULTI` returns `-ERR ... not supported ...` rather than `+OK`, because silently accepting it and then failing at `EXEC` would break any client expecting atomic batching. `EXEC` and `DISCARD` return Redis's standard `EXEC without MULTI` / `DISCARD without MULTI` errors (the only honest reply when no transaction can ever have been opened). `WATCH` returns `-ERR ... not supported ...` — optimistic CAS across a subsequent `EXEC` needs connection-level state that rustyant doesn't carry. `UNWATCH` returns `+OK`: clearing a never-populated watch set is trivially successful, no contract is violated.

Pub/sub follows the same rule with a split: the subscribe surface (`SUBSCRIBE` / `PSUBSCRIBE` / `UNSUBSCRIBE` / `PUNSUBSCRIBE`) errors explicitly because returning `+OK` would fool client libraries into entering push-read mode on an HTTP connection that will never deliver a server-initiated frame. `PUBLISH` returns `:0` — literally "zero clients received this message", which is the same reply real Redis gives against an idle server with no subscribers, so fan-out callers that only fire-and-forget get a truthful no-op reply. `PUBSUB CHANNELS` returns an empty array; `PUBSUB NUMSUB` returns `(channel, :0)` pairs for each channel named (none omitted); `PUBSUB NUMPAT` returns `:0`. Introspection tooling sees an honest empty pub/sub plane; no workflow gets silently broken.

Geo commands (`GEOADD` / `GEOPOS` / `GEODIST` / `GEOHASH`) are layered directly on sorted sets: each member's score is a 52-bit interleaved geohash integer with longitude over `[-180, 180]` and latitude clamped to Redis's Mercator band `[-85.05112878, 85.05112878]`, matching Redis's wire format so external geo tooling interoperates. `GEOADD` accepts `NX` / `XX` / `CH` with standard `ZADD` semantics; out-of-range coordinates return an explicit error rather than silently clamping. `GEODIST` uses the Haversine formula with Redis's Earth-radius constant (`6_372_797.560856` m) and formats replies to four decimal places — against the canonical Sicily example (Palermo, Catania) rustyant reports `166274.1516` m identically. `GEOHASH` decodes the internal score back to `(lon, lat)` and re-encodes with the standard latitude range to produce the 11-character base32 string Redis emits (`sqc8b49rny0` / `sqdtr74hyu0` for the same example). The search surface (`GEOSEARCH` / `GEOSEARCHSTORE`) is left for a follow-up; the deprecated `GEORADIUS*` family is explicitly not planned.

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

**Storage is always S3-compatible — there is no in-memory mode.** Lambda instances are ephemeral, so an in-memory backend would lose data on every cold start. The `Storage` trait has exactly one production implementation (`S3Storage`). Integration tests run against [floci](https://github.com/floci-io/floci), a local S3 emulator — see `just floci-up` below.

## Local Development

Rust: `1.85+` (edition `2024`), toolchain pinned via `rust-toolchain.toml`.

```bash
rustup show               # install toolchain
cargo fetch               # pull dependencies
just check                # fmt + clippy
just test                 # cargo-nextest — auto-starts floci
```

`just test` brings up the floci emulator on `http://localhost:4566` (via `docker compose`), creates the test bucket, then runs the full suite. Requires `docker` and `aws` CLI.

### Environment

- `BUCKET` (required) — S3 bucket holding the key objects.
- `KEY_PREFIX` (default `rustyant/`) — prefix prepended to every key.
- `AWS_REGION`, `AWS_ENDPOINT_URL` — standard AWS env; `AWS_ENDPOINT_URL` points at a local S3 emulator.
- `RUSTYANT_EMF_NAMESPACE` (optional) — when set, each dispatched command emits a CloudWatch Embedded Metric Format line to stdout with `DispatchCount` and `DispatchLatency` under the given namespace, dimensioned by `{Command, Outcome}`. Unset in local dev so the terminal stays clean; the SAM template sets it to `rustyant` for deployed Lambdas.

### Local S3 (floci)

Same pattern as the sibling [rustyhip](https://github.com/monkut/rustyhip) project — a docker-hosted S3 emulator ([floci](https://github.com/floci-io/floci)) on `http://localhost:4566`. Requires `docker` and the `aws` CLI. Floci runs in memory mode, so restarting the container wipes buckets.

```bash
just floci-up               # start the emulator (container: rustyant-floci)
just floci-seed             # create the rustyant-dev bucket
just rustyant-dev           # cargo lambda watch against floci on :9000
just floci-down             # tear down
```

`just test` invokes `floci-up` + `floci-seed` automatically, so in most workflows you only need these recipes when running the `cargo lambda watch` dev loop.

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

Working: RESP over HTTP and WebSocket, full string/hash/list/set/zset command dispatch plus `KEYS` / `SCAN` / `HSCAN` / `SSCAN` / `ZSCAN`, server introspection and administration surface (`INFO` / `COMMAND` / `HELLO` / `CLIENT` / `AUTH` / `WAIT` / `SAVE` / `BGSAVE` / `BGREWRITEAOF` / `LASTSAVE` / `LATENCY` / `DEBUG SLEEP` / `MULTI` / `EXEC` / `DISCARD` / `WATCH` / `UNWATCH` / `SUBSCRIBE` / `PSUBSCRIBE` / `UNSUBSCRIBE` / `PUNSUBSCRIBE` / `PUBLISH` / `PUBSUB`), Core 4 geo commands (`GEOADD` / `GEOPOS` / `GEODIST` / `GEOHASH`) layered on sorted sets with Redis-compatible encoding, S3-backed storage with per-key TTL and conditional-write CAS on every read-modify-write, 432 Rust tests across 5 suites (26 lib units + 379 HTTP integration + 14 redis-py compat + 6 WebSocket E2E + 7 floci/S3) and 13 Python client tests, structured logs and CloudWatch EMF metrics, CI on GitHub Actions with floci as a service container, SAM template validated in CI.

Not wired: no end-to-end test driving a real WebSocket connection against a deployed binary in AWS; the `s3_concurrent_incr_converges` CAS test is gated behind `RUSTYANT_S3_CAS=1` because floci doesn't enforce `If-Match` headers.
