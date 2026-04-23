use std::time::{Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use tracing::info;

use crate::error::RustyAntError;
use crate::geo::{self, GeoUnit};
use crate::metrics;
use crate::resp::RespReply;
use crate::state::State;
use crate::storage::{GetExOp, ScoreBound, TtlResult, ZAddFlags, bit_at, now_ms};

/// Coarse classification of a dispatch result, emitted as a structured log
/// field so `CloudWatch` queries can count/alert by outcome without string
/// parsing error messages.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum Outcome {
    Ok,
    UnknownCommand,
    WrongArity,
    WrongType,
    Parse,
    RespParse,
    Contention,
    S3,
    Config,
    Io,
    Serde,
}

impl Outcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::UnknownCommand => "unknown_command",
            Self::WrongArity => "wrong_arity",
            Self::WrongType => "wrong_type",
            Self::Parse => "parse",
            Self::RespParse => "resp_parse",
            Self::Contention => "contention",
            Self::S3 => "s3",
            Self::Config => "config",
            Self::Io => "io",
            Self::Serde => "serde",
        }
    }

    const fn classify(result: &Result<RespReply, RustyAntError>) -> Self {
        match result {
            Ok(_) => Self::Ok,
            Err(RustyAntError::UnknownCommand(_)) => Self::UnknownCommand,
            Err(RustyAntError::WrongArity { .. }) => Self::WrongArity,
            Err(RustyAntError::WrongType { .. }) => Self::WrongType,
            Err(RustyAntError::Parse(_)) => Self::Parse,
            Err(RustyAntError::RespParse(_)) => Self::RespParse,
            Err(RustyAntError::Contention) => Self::Contention,
            Err(RustyAntError::S3(_)) => Self::S3,
            Err(RustyAntError::Config(_)) => Self::Config,
            Err(RustyAntError::Io(_)) => Self::Io,
            Err(RustyAntError::Serde(_)) => Self::Serde,
        }
    }
}

pub async fn dispatch(state: &State, command_tokens: Vec<Bytes>) -> RespReply {
    let cmd_name = command_tokens
        .first()
        .and_then(|b| std::str::from_utf8(b).ok())
        .map_or_else(|| "?".to_string(), str::to_ascii_uppercase);
    let argc = command_tokens.len();
    let start = Instant::now();
    let result = run(state, command_tokens).await;
    let outcome = Outcome::classify(&result);
    let duration_ms = u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX);
    info!(
        command = %cmd_name,
        argc,
        outcome = outcome.as_str(),
        duration_ms,
        "command dispatched",
    );
    if let Some(ns) = state.settings.emf_namespace.as_deref() {
        metrics::emit_command_metrics(ns, &cmd_name, outcome.as_str(), duration_ms);
    }
    match result {
        Ok(reply) => reply,
        Err(e) => RespReply::err(format!("ERR {e}")),
    }
}

#[allow(clippy::large_stack_frames, clippy::too_many_lines)]
async fn run(state: &State, tokens: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    let mut iter = tokens.into_iter();
    let cmd_bytes = iter.next().ok_or_else(|| RustyAntError::RespParse("empty command array".into()))?;
    let cmd = std::str::from_utf8(&cmd_bytes)
        .map_err(|_| RustyAntError::RespParse("command not utf8".into()))?
        .to_ascii_uppercase();
    let args: Vec<Bytes> = iter.collect();

    match cmd.as_str() {
        "PING" => Ok(RespReply::SimpleString("PONG".into())),
        "ECHO" => handle_echo(&args),
        "TIME" => handle_time(&args),
        "INFO" => handle_info(state, args).await,
        "COMMAND" => handle_command(&args),
        "HELLO" => handle_hello(&args),
        "CLIENT" => handle_client(&args),
        "RESET" => Ok(RespReply::SimpleString("RESET".into())),
        "AUTH" => handle_auth(&args),
        "WAIT" => handle_wait(&args),
        "SAVE" => {
            arity("SAVE", args.is_empty())?;
            Ok(RespReply::ok())
        }
        "BGSAVE" => handle_bgsave(&args),
        "BGREWRITEAOF" => {
            arity("BGREWRITEAOF", args.is_empty())?;
            Ok(RespReply::SimpleString("Background append only file rewriting started".into()))
        }
        "LASTSAVE" => {
            arity("LASTSAVE", args.is_empty())?;
            // Honest stub: report container cold-start seconds. rustyant
            // never does a "save" — every SET is durable on S3 on the spot.
            Ok(RespReply::Integer(state.started_at_ms / 1000))
        }
        "LATENCY" => handle_latency(&args),
        "DEBUG" => handle_debug(&args).await,
        "MULTI" => handle_multi(&args),
        "EXEC" => handle_exec(&args),
        "DISCARD" => handle_discard(&args),
        "WATCH" => handle_watch(&args),
        "UNWATCH" => handle_unwatch(&args),
        "SUBSCRIBE" => handle_subscribe(&args),
        "PSUBSCRIBE" => handle_psubscribe(&args),
        "UNSUBSCRIBE" => handle_unsubscribe(&args),
        "PUNSUBSCRIBE" => handle_punsubscribe(&args),
        "PUBLISH" => handle_publish(&args),
        "PUBSUB" => handle_pubsub(&args),
        "GEOADD" => handle_geoadd(state, args).await,
        "GEOPOS" => handle_geopos(state, args).await,
        "GEODIST" => handle_geodist(state, args).await,
        "GEOHASH" => handle_geohash(state, args).await,
        "GEOSEARCH" => handle_geosearch(state, args).await,
        "GEOSEARCHSTORE" => handle_geosearchstore(state, args).await,
        "DBSIZE" => handle_dbsize(state, args).await,
        "FLUSHDB" | "FLUSHALL" => handle_flushall(state, args, &cmd).await,
        "RANDOMKEY" => handle_randomkey(state, args).await,
        // UNLINK is Redis's lazy-delete variant; rustyant has no background
        // freer thread, so it folds into the same synchronous DEL path.
        "DEL" | "UNLINK" => handle_del(state, args).await,
        "COPY" => handle_copy(state, args).await,
        // Bit ops on Strings
        "GETBIT" => handle_getbit(state, args).await,
        "SETBIT" => handle_setbit(state, args).await,
        "BITCOUNT" => handle_bitcount(state, args).await,
        "BITPOS" => handle_bitpos(state, args).await,
        "BITOP" => handle_bitop(state, args).await,
        // Strings
        "GET" => handle_get(state, args).await,
        "GETEX" => handle_getex(state, args).await,
        "GETSET" => handle_getset(state, args).await,
        "GETDEL" => handle_getdel(state, args).await,
        "GETRANGE" => handle_getrange(state, args).await,
        "SETRANGE" => handle_setrange(state, args).await,
        "STRLEN" => handle_strlen(state, args).await,
        "APPEND" => handle_append(state, args).await,
        "SET" => handle_set(state, args).await,
        "SETNX" => handle_setnx(state, args).await,
        "SETEX" => handle_setex(state, args).await,
        "MGET" => handle_mget(state, args).await,
        "MSET" => handle_mset(state, args).await,
        "MSETNX" => handle_msetnx(state, args).await,
        "EXISTS" => handle_exists(state, args).await,
        "EXPIRE" => handle_expire(state, args).await,
        "EXPIREAT" => handle_expireat(state, args).await,
        "PEXPIRE" => handle_pexpire(state, args).await,
        "PEXPIREAT" => handle_pexpireat(state, args).await,
        "PERSIST" => handle_persist(state, args).await,
        "TTL" => handle_ttl(state, args).await,
        "PTTL" => handle_pttl(state, args).await,
        "EXPIRETIME" => handle_expiretime(state, args).await,
        "PEXPIRETIME" => handle_pexpiretime(state, args).await,
        "RENAME" => handle_rename(state, args).await,
        "RENAMENX" => handle_renamenx(state, args).await,
        "KEYS" => handle_keys(state, args).await,
        "SCAN" => handle_scan(state, args).await,
        "TYPE" => handle_type(state, args).await,
        "INCR" => handle_incrby(state, args, 1).await,
        "INCRBY" => {
            let delta = parse_delta(&args)?;
            handle_incrby(state, args, delta).await
        }
        "INCRBYFLOAT" => handle_incrbyfloat(state, args).await,
        "DECR" => handle_incrby(state, args, -1).await,
        "DECRBY" => {
            let delta = parse_delta(&args)?;
            let neg = delta.checked_neg().ok_or_else(|| RustyAntError::Parse("decrement overflow".into()))?;
            handle_incrby(state, args, neg).await
        }
        // Hashes
        "HSET" => handle_hset(state, args).await,
        "HSETNX" => handle_hsetnx(state, args).await,
        "HGET" => handle_hget(state, args).await,
        "HDEL" => handle_hdel(state, args).await,
        "HGETALL" => handle_hgetall(state, args).await,
        "HLEN" => handle_hlen(state, args).await,
        "HKEYS" => handle_hkeys(state, args).await,
        "HVALS" => handle_hvals(state, args).await,
        "HEXISTS" => handle_hexists(state, args).await,
        "HSTRLEN" => handle_hstrlen(state, args).await,
        "HMGET" => handle_hmget(state, args).await,
        "HINCRBY" => handle_hincrby(state, args).await,
        "HINCRBYFLOAT" => handle_hincrbyfloat(state, args).await,
        "HSCAN" => handle_hscan(state, args).await,
        // Lists
        "LPUSH" => handle_push(state, args, true).await,
        "RPUSH" => handle_push(state, args, false).await,
        "LPUSHX" => handle_pushx(state, args, true).await,
        "RPUSHX" => handle_pushx(state, args, false).await,
        "LPOP" => handle_pop(state, args, true).await,
        "RPOP" => handle_pop(state, args, false).await,
        "LRANGE" => handle_lrange(state, args).await,
        "LLEN" => handle_llen(state, args).await,
        "LINDEX" => handle_lindex(state, args).await,
        "LSET" => handle_lset(state, args).await,
        "LREM" => handle_lrem(state, args).await,
        "LINSERT" => handle_linsert(state, args).await,
        "LTRIM" => handle_ltrim(state, args).await,
        "LMOVE" => handle_lmove(state, args).await,
        "RPOPLPUSH" => handle_rpoplpush(state, args).await,
        "LPOS" => handle_lpos(state, args).await,
        // Sets
        "SADD" => handle_sadd(state, args).await,
        "SREM" => handle_srem(state, args).await,
        "SMEMBERS" => handle_smembers(state, args).await,
        "SISMEMBER" => handle_sismember(state, args).await,
        "SMISMEMBER" => handle_smismember(state, args).await,
        "SCARD" => handle_scard(state, args).await,
        "SINTER" => handle_sinter(state, args).await,
        "SUNION" => handle_sunion(state, args).await,
        "SDIFF" => handle_sdiff(state, args).await,
        "SINTERSTORE" => handle_sstore(state, args, SetOp::Inter).await,
        "SUNIONSTORE" => handle_sstore(state, args, SetOp::Union).await,
        "SDIFFSTORE" => handle_sstore(state, args, SetOp::Diff).await,
        "SPOP" => handle_spop(state, args).await,
        "SRANDMEMBER" => handle_srandmember(state, args).await,
        "SSCAN" => handle_sscan(state, args).await,
        // Sorted sets
        "ZADD" => handle_zadd(state, args).await,
        "ZINTERSTORE" => handle_zstore(state, args, ZStoreOp::Inter).await,
        "ZUNIONSTORE" => handle_zstore(state, args, ZStoreOp::Union).await,
        "ZDIFFSTORE" => handle_zstore(state, args, ZStoreOp::Diff).await,
        "ZREM" => handle_zrem(state, args).await,
        "ZINCRBY" => handle_zincrby(state, args).await,
        "ZRANGE" => handle_zrange(state, args).await,
        "ZREVRANGE" => handle_zrevrange(state, args).await,
        "ZRANGEBYSCORE" => handle_zrangebyscore(state, args).await,
        "ZREVRANGEBYSCORE" => handle_zrevrangebyscore(state, args).await,
        "ZREMRANGEBYRANK" => handle_zremrangebyrank(state, args).await,
        "ZREMRANGEBYSCORE" => handle_zremrangebyscore(state, args).await,
        "ZPOPMIN" => handle_zpop(state, args, false).await,
        "ZPOPMAX" => handle_zpop(state, args, true).await,
        "ZSCORE" => handle_zscore(state, args).await,
        "ZCARD" => handle_zcard(state, args).await,
        "ZRANK" => handle_zrank(state, args, false).await,
        "ZREVRANK" => handle_zrank(state, args, true).await,
        "ZCOUNT" => handle_zcount(state, args).await,
        "ZMSCORE" => handle_zmscore(state, args).await,
        "ZSCAN" => handle_zscan(state, args).await,
        other => Err(RustyAntError::UnknownCommand(other.to_string())),
    }
}

fn arg_as_str(arg: &Bytes) -> Result<&str, RustyAntError> {
    std::str::from_utf8(arg).map_err(|_| RustyAntError::RespParse("argument not utf8".into()))
}

fn arg_as_string(arg: &Bytes) -> Result<String, RustyAntError> {
    arg_as_str(arg).map(std::string::ToString::to_string)
}

fn arity(command: &str, expected: bool) -> Result<(), RustyAntError> {
    if expected { Ok(()) } else { Err(RustyAntError::WrongArity { command: command.to_string() }) }
}

fn parse_i64(arg: &Bytes, label: &str) -> Result<i64, RustyAntError> {
    arg_as_str(arg)?.parse::<i64>().map_err(|_| RustyAntError::Parse(format!("{label} is not an integer")))
}

fn parse_f64(arg: &Bytes, label: &str) -> Result<f64, RustyAntError> {
    arg_as_str(arg)?.parse::<f64>().map_err(|_| RustyAntError::Parse(format!("{label} is not a float")))
}

fn parse_delta(args: &[Bytes]) -> Result<i64, RustyAntError> {
    if args.len() != 2 {
        return Err(RustyAntError::WrongArity { command: "INCRBY".into() });
    }
    parse_i64(&args[1], "increment")
}

// ---- Strings --------------------------------------------------------------

async fn handle_get(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("GET", args.len() == 1)?;
    let key = arg_as_str(&args[0])?;
    Ok(state.storage.get_string(key).await?.map_or(RespReply::Nil, |v| RespReply::BulkString(Some(v))))
}

async fn handle_getex(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("GETEX", !args.is_empty() && args.len() <= 3)?;
    let key = arg_as_string(&args[0])?;
    let op = parse_getex_op(&args[1..])?;
    Ok(state.storage.get_string_with_ttl(&key, op).await?.map_or(RespReply::Nil, |v| RespReply::BulkString(Some(v))))
}

fn parse_getex_op(opts: &[Bytes]) -> Result<GetExOp, RustyAntError> {
    if opts.is_empty() {
        return Ok(GetExOp::Leave);
    }
    let name = arg_as_str(&opts[0])?.to_ascii_uppercase();
    if name == "PERSIST" {
        if opts.len() != 1 {
            return Err(RustyAntError::Parse("PERSIST takes no value".into()));
        }
        return Ok(GetExOp::Persist);
    }
    let value = opts.get(1).ok_or_else(|| RustyAntError::Parse(format!("{name} requires a value")))?;
    if opts.len() > 2 {
        return Err(RustyAntError::Parse(format!("GETEX accepts at most one option; extra args after {name}")));
    }
    let n = parse_i64(value, name.as_str())?;
    let at_ms = match name.as_str() {
        "EX" => now_ms() + n.saturating_mul(1000),
        "PX" => now_ms() + n,
        "EXAT" => n.saturating_mul(1000),
        "PXAT" => n,
        other => return Err(RustyAntError::Parse(format!("unsupported GETEX option: {other}"))),
    };
    Ok(GetExOp::SetExpireAtMs(at_ms))
}

async fn handle_set(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("SET", args.len() >= 2)?;
    let key = arg_as_string(&args[0])?;
    let value = args[1].clone();
    let mut expires_at_ms: Option<i64> = None;
    let mut i = 2;
    while i < args.len() {
        let opt = arg_as_str(&args[i])?.to_ascii_uppercase();
        match opt.as_str() {
            "EX" => {
                let secs = parse_i64(
                    args.get(i + 1).ok_or_else(|| RustyAntError::Parse("EX requires a value".into()))?,
                    "EX",
                )?;
                expires_at_ms = Some(now_ms() + secs * 1000);
                i += 2;
            }
            "PX" => {
                let ms = parse_i64(
                    args.get(i + 1).ok_or_else(|| RustyAntError::Parse("PX requires a value".into()))?,
                    "PX",
                )?;
                expires_at_ms = Some(now_ms() + ms);
                i += 2;
            }
            other => {
                return Err(RustyAntError::Parse(format!("unsupported SET option: {other}")));
            }
        }
    }
    state.storage.set_string(&key, value, expires_at_ms).await?;
    Ok(RespReply::ok())
}

async fn handle_del(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("DEL", !args.is_empty())?;
    let mut deleted: i64 = 0;
    for arg in &args {
        let key = arg_as_str(arg)?;
        if state.storage.delete(key).await? {
            deleted += 1;
        }
    }
    Ok(RespReply::Integer(deleted))
}

async fn handle_exists(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("EXISTS", !args.is_empty())?;
    let mut count: i64 = 0;
    for arg in &args {
        let key = arg_as_str(arg)?;
        if state.storage.exists(key).await? {
            count += 1;
        }
    }
    Ok(RespReply::Integer(count))
}

async fn handle_expire(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("EXPIRE", args.len() == 2)?;
    let key = arg_as_str(&args[0])?;
    let secs = parse_i64(&args[1], "seconds")?;
    let set = state.storage.expire_at(key, now_ms() + secs * 1000).await?;
    Ok(RespReply::Integer(i64::from(set)))
}

async fn handle_expireat(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("EXPIREAT", args.len() == 2)?;
    let key = arg_as_str(&args[0])?;
    let unix_secs = parse_i64(&args[1], "unix-time-seconds")?;
    let set = state.storage.expire_at(key, unix_secs.saturating_mul(1000)).await?;
    Ok(RespReply::Integer(i64::from(set)))
}

async fn handle_pexpireat(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("PEXPIREAT", args.len() == 2)?;
    let key = arg_as_str(&args[0])?;
    let unix_ms = parse_i64(&args[1], "unix-time-milliseconds")?;
    let set = state.storage.expire_at(key, unix_ms).await?;
    Ok(RespReply::Integer(i64::from(set)))
}

async fn handle_ttl(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("TTL", args.len() == 1)?;
    let key = arg_as_str(&args[0])?;
    Ok(match state.storage.ttl_ms(key).await? {
        TtlResult::NoKey => RespReply::Integer(-2),
        TtlResult::NoExpire => RespReply::Integer(-1),
        TtlResult::Ms(ms) => RespReply::Integer((ms + 999) / 1000),
    })
}

async fn handle_incrby(state: &State, args: Vec<Bytes>, delta: i64) -> Result<RespReply, RustyAntError> {
    arity("INCR", !args.is_empty())?;
    let key = arg_as_str(&args[0])?;
    let new = state.storage.incr_by(key, delta).await?;
    Ok(RespReply::Integer(new))
}

// ---- Hashes ---------------------------------------------------------------

async fn handle_hset(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    if args.len() < 3 || (args.len() - 1) % 2 != 0 {
        return Err(RustyAntError::WrongArity { command: "HSET".into() });
    }
    let key = arg_as_string(&args[0])?;
    let mut pairs: Vec<(String, Bytes)> = Vec::with_capacity((args.len() - 1) / 2);
    let mut i = 1;
    while i + 1 < args.len() {
        let field = arg_as_string(&args[i])?;
        let value = args[i + 1].clone();
        pairs.push((field, value));
        i += 2;
    }
    let new_fields = state.storage.hset(&key, pairs).await?;
    Ok(RespReply::Integer(new_fields))
}

async fn handle_hget(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("HGET", args.len() == 2)?;
    let key = arg_as_str(&args[0])?;
    let field = arg_as_str(&args[1])?;
    Ok(state.storage.hget(key, field).await?.map_or(RespReply::Nil, |v| RespReply::BulkString(Some(v))))
}

async fn handle_hdel(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("HDEL", args.len() >= 2)?;
    let key = arg_as_str(&args[0])?;
    let fields: Vec<String> = args.iter().skip(1).map(arg_as_string).collect::<Result<_, _>>()?;
    let removed = state.storage.hdel(key, &fields).await?;
    Ok(RespReply::Integer(removed))
}

async fn handle_hgetall(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("HGETALL", args.len() == 1)?;
    let key = arg_as_str(&args[0])?;
    let pairs = state.storage.hgetall(key).await?;
    let mut flat: Vec<RespReply> = Vec::with_capacity(pairs.len() * 2);
    for (k, v) in pairs {
        flat.push(RespReply::BulkString(Some(Bytes::from(k.into_bytes()))));
        flat.push(RespReply::BulkString(Some(v)));
    }
    Ok(RespReply::Array(flat))
}

// ---- Lists ----------------------------------------------------------------

async fn handle_push(state: &State, args: Vec<Bytes>, left: bool) -> Result<RespReply, RustyAntError> {
    let cmd = if left { "LPUSH" } else { "RPUSH" };
    arity(cmd, args.len() >= 2)?;
    let key = arg_as_string(&args[0])?;
    let values: Vec<Bytes> = args.into_iter().skip(1).collect();
    let len = state.storage.list_push(&key, values, left).await?;
    Ok(RespReply::Integer(len))
}

async fn handle_pop(state: &State, args: Vec<Bytes>, left: bool) -> Result<RespReply, RustyAntError> {
    let cmd = if left { "LPOP" } else { "RPOP" };
    arity(cmd, !args.is_empty() && args.len() <= 2)?;
    let key = arg_as_str(&args[0])?;
    if args.len() == 2 {
        let count = parse_i64(&args[1], "count")?;
        if count < 0 {
            return Err(RustyAntError::Parse("count must be >= 0".into()));
        }
        let count_usize = usize::try_from(count).unwrap_or(0);
        let popped = state.storage.list_pop(key, count_usize, left).await?;
        if popped.is_empty() {
            Ok(RespReply::Nil)
        } else {
            Ok(RespReply::Array(popped.into_iter().map(|b| RespReply::BulkString(Some(b))).collect()))
        }
    } else {
        let mut popped = state.storage.list_pop(key, 1, left).await?;
        Ok(popped.pop().map_or(RespReply::Nil, |b| RespReply::BulkString(Some(b))))
    }
}

async fn handle_lrange(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("LRANGE", args.len() == 3)?;
    let key = arg_as_str(&args[0])?;
    let start = parse_i64(&args[1], "start")?;
    let stop = parse_i64(&args[2], "stop")?;
    let items = state.storage.lrange(key, start, stop).await?;
    Ok(RespReply::Array(items.into_iter().map(|b| RespReply::BulkString(Some(b))).collect()))
}

// ---- Sets -----------------------------------------------------------------

async fn handle_sadd(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("SADD", args.len() >= 2)?;
    let key = arg_as_string(&args[0])?;
    let members: Vec<String> = args.iter().skip(1).map(arg_as_string).collect::<Result<_, _>>()?;
    let added = state.storage.sadd(&key, members).await?;
    Ok(RespReply::Integer(added))
}

// ---- Sorted Sets ----------------------------------------------------------

async fn handle_zadd(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    if args.len() < 3 {
        return Err(RustyAntError::WrongArity { command: "ZADD".into() });
    }
    let key = arg_as_string(&args[0])?;
    let mut flags = ZAddFlags::default();
    let mut incr = false;
    let mut idx = 1;
    while idx < args.len() {
        let token = arg_as_str(&args[idx])?;
        match token.to_ascii_uppercase().as_str() {
            "NX" => {
                flags.nx = true;
                idx += 1;
            }
            "XX" => {
                flags.xx = true;
                idx += 1;
            }
            "GT" => {
                flags.gt = true;
                idx += 1;
            }
            "LT" => {
                flags.lt = true;
                idx += 1;
            }
            "CH" => {
                flags.ch = true;
                idx += 1;
            }
            "INCR" => {
                incr = true;
                idx += 1;
            }
            _ => break,
        }
    }
    if flags.nx && flags.xx {
        return Err(RustyAntError::Parse("XX and NX options at the same time are not compatible".into()));
    }
    if flags.gt && flags.lt {
        return Err(RustyAntError::Parse("GT, LT, and/or NX options at the same time are not compatible".into()));
    }
    if flags.nx && (flags.gt || flags.lt) {
        return Err(RustyAntError::Parse("GT, LT, and/or NX options at the same time are not compatible".into()));
    }
    // Remaining args must be score/member pairs.
    let remaining = args.len() - idx;
    if remaining < 2 || remaining % 2 != 0 {
        return Err(RustyAntError::WrongArity { command: "ZADD".into() });
    }
    let mut pairs: Vec<(f64, String)> = Vec::with_capacity(remaining / 2);
    while idx < args.len() {
        let score = parse_f64(&args[idx], "score")?;
        let member = arg_as_string(&args[idx + 1])?;
        pairs.push((score, member));
        idx += 2;
    }
    if incr {
        if pairs.len() != 1 {
            return Err(RustyAntError::Parse("INCR option supports a single increment-element pair".into()));
        }
        let (delta, member) = pairs.into_iter().next().expect("len == 1 checked above");
        let new_score = state.storage.zadd_ext_incr(&key, delta, &member, flags).await?;
        return Ok(new_score
            .map_or(RespReply::Nil, |s| RespReply::BulkString(Some(Bytes::from(format_score(s).into_bytes())))));
    }
    // Fast path: no flags at all → original zadd (preserves call-site shape
    // for the common case).
    let has_any_flag = flags.nx || flags.xx || flags.gt || flags.lt || flags.ch;
    let count = if has_any_flag {
        state.storage.zadd_ext(&key, pairs, flags).await?
    } else {
        state.storage.zadd(&key, pairs).await?
    };
    Ok(RespReply::Integer(count))
}

async fn handle_zrange(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("ZRANGE", args.len() == 3)?;
    let key = arg_as_str(&args[0])?;
    let start = parse_i64(&args[1], "start")?;
    let stop = parse_i64(&args[2], "stop")?;
    let members = state.storage.zrange(key, start, stop).await?;
    Ok(RespReply::Array(
        members.into_iter().map(|m| RespReply::BulkString(Some(Bytes::from(m.into_bytes())))).collect(),
    ))
}

// ---- Additional read-only commands ----------------------------------------

async fn handle_hlen(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("HLEN", args.len() == 1)?;
    let n = state.storage.hlen(arg_as_str(&args[0])?).await?;
    Ok(RespReply::Integer(n))
}

async fn handle_hkeys(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("HKEYS", args.len() == 1)?;
    let keys = state.storage.hkeys(arg_as_str(&args[0])?).await?;
    Ok(RespReply::Array(keys.into_iter().map(|k| RespReply::BulkString(Some(Bytes::from(k.into_bytes())))).collect()))
}

async fn handle_hvals(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("HVALS", args.len() == 1)?;
    let vals = state.storage.hvals(arg_as_str(&args[0])?).await?;
    Ok(RespReply::Array(vals.into_iter().map(|v| RespReply::BulkString(Some(v))).collect()))
}

async fn handle_hexists(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("HEXISTS", args.len() == 2)?;
    let present = state.storage.hexists(arg_as_str(&args[0])?, arg_as_str(&args[1])?).await?;
    Ok(RespReply::Integer(i64::from(present)))
}

async fn handle_hsetnx(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("HSETNX", args.len() == 3)?;
    let key = arg_as_str(&args[0])?;
    let field = arg_as_str(&args[1])?;
    let set = state.storage.hsetnx(key, field, args[2].clone()).await?;
    Ok(RespReply::Integer(i64::from(set)))
}

async fn handle_hstrlen(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("HSTRLEN", args.len() == 2)?;
    let len = state.storage.hstrlen(arg_as_str(&args[0])?, arg_as_str(&args[1])?).await?;
    Ok(RespReply::Integer(len))
}

async fn handle_hmget(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("HMGET", args.len() >= 2)?;
    let key = arg_as_str(&args[0])?;
    let fields: Vec<String> = args.iter().skip(1).map(arg_as_string).collect::<Result<_, _>>()?;
    let vals = state.storage.hmget(key, &fields).await?;
    Ok(RespReply::Array(
        vals.into_iter().map(|v| v.map_or(RespReply::Nil, |b| RespReply::BulkString(Some(b)))).collect(),
    ))
}

async fn handle_llen(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("LLEN", args.len() == 1)?;
    let n = state.storage.llen(arg_as_str(&args[0])?).await?;
    Ok(RespReply::Integer(n))
}

async fn handle_smembers(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("SMEMBERS", args.len() == 1)?;
    let members = state.storage.smembers(arg_as_str(&args[0])?).await?;
    Ok(RespReply::Array(
        members.into_iter().map(|m| RespReply::BulkString(Some(Bytes::from(m.into_bytes())))).collect(),
    ))
}

async fn handle_sismember(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("SISMEMBER", args.len() == 2)?;
    let present = state.storage.sismember(arg_as_str(&args[0])?, arg_as_str(&args[1])?).await?;
    Ok(RespReply::Integer(i64::from(present)))
}

async fn handle_scard(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("SCARD", args.len() == 1)?;
    let n = state.storage.scard(arg_as_str(&args[0])?).await?;
    Ok(RespReply::Integer(n))
}

async fn handle_zscore(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("ZSCORE", args.len() == 2)?;
    let score = state.storage.zscore(arg_as_str(&args[0])?, arg_as_str(&args[1])?).await?;
    Ok(score.map_or(RespReply::Nil, |s| {
        // Redis returns scores as bulk strings using the canonical float format.
        RespReply::BulkString(Some(Bytes::from(format_score(s).into_bytes())))
    }))
}

async fn handle_zcard(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("ZCARD", args.len() == 1)?;
    let n = state.storage.zcard(arg_as_str(&args[0])?).await?;
    Ok(RespReply::Integer(n))
}

async fn handle_zrank(state: &State, args: Vec<Bytes>, reverse: bool) -> Result<RespReply, RustyAntError> {
    let cmd = if reverse { "ZREVRANK" } else { "ZRANK" };
    arity(cmd, args.len() == 2)?;
    let key = arg_as_str(&args[0])?;
    let member = arg_as_str(&args[1])?;
    let rank =
        if reverse { state.storage.zrevrank(key, member).await? } else { state.storage.zrank(key, member).await? };
    Ok(rank.map_or(RespReply::Nil, RespReply::Integer))
}

async fn handle_zcount(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("ZCOUNT", args.len() == 3)?;
    let key = arg_as_str(&args[0])?;
    let min = ScoreBound::parse(arg_as_str(&args[1])?)?;
    let max = ScoreBound::parse(arg_as_str(&args[2])?)?;
    let n = state.storage.zcount(key, min, max).await?;
    Ok(RespReply::Integer(n))
}

async fn handle_zmscore(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("ZMSCORE", args.len() >= 2)?;
    let key = arg_as_str(&args[0])?;
    let members: Vec<String> = args.iter().skip(1).map(arg_as_string).collect::<Result<_, _>>()?;
    let scores = state.storage.zmscore(key, &members).await?;
    Ok(RespReply::Array(
        scores
            .into_iter()
            .map(|s| {
                s.map_or(RespReply::Nil, |v| RespReply::BulkString(Some(Bytes::from(format_score(v).into_bytes()))))
            })
            .collect(),
    ))
}

// ---- String multi-key + NX/EX + GETSET + PERSIST --------------------------

async fn handle_getset(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("GETSET", args.len() == 2)?;
    let key = arg_as_str(&args[0])?;
    let old = state.storage.getset(key, args[1].clone()).await?;
    Ok(old.map_or(RespReply::Nil, |v| RespReply::BulkString(Some(v))))
}

async fn handle_getdel(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("GETDEL", args.len() == 1)?;
    let key = arg_as_str(&args[0])?;
    let old = state.storage.get_and_delete(key).await?;
    Ok(old.map_or(RespReply::Nil, |v| RespReply::BulkString(Some(v))))
}

async fn handle_strlen(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("STRLEN", args.len() == 1)?;
    let key = arg_as_str(&args[0])?;
    let len = state.storage.strlen(key).await?;
    Ok(RespReply::Integer(len))
}

async fn handle_append(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("APPEND", args.len() == 2)?;
    let key = arg_as_str(&args[0])?;
    let len = state.storage.append(key, args[1].clone()).await?;
    Ok(RespReply::Integer(len))
}

async fn handle_persist(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("PERSIST", args.len() == 1)?;
    let key = arg_as_str(&args[0])?;
    let cleared = state.storage.persist(key).await?;
    Ok(RespReply::Integer(i64::from(cleared)))
}

async fn handle_keys(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("KEYS", args.len() == 1)?;
    let pattern = arg_as_str(&args[0])?;
    let keys = state.storage.keys(pattern).await?;
    Ok(RespReply::Array(keys.into_iter().map(|k| RespReply::BulkString(Some(Bytes::from(k.into_bytes())))).collect()))
}

async fn handle_type(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("TYPE", args.len() == 1)?;
    let key = arg_as_str(&args[0])?;
    let kind = state.storage.kind(key).await?.unwrap_or("none");
    Ok(RespReply::SimpleString(kind.into()))
}

async fn handle_scan(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("SCAN", !args.is_empty())?;
    let cursor_arg = arg_as_str(&args[0])?;
    // Redis convention: "0" means start / done.
    let cursor: Option<String> = if cursor_arg == "0" { None } else { Some(cursor_arg.to_string()) };
    let (pattern, count) = parse_scan_opts(&args, 1, "SCAN")?;

    let (keys, next) = state.storage.scan(cursor.as_deref(), pattern.as_deref(), count).await?;
    let cursor_out = next.unwrap_or_else(|| "0".to_string());
    Ok(RespReply::Array(vec![
        RespReply::BulkString(Some(Bytes::from(cursor_out.into_bytes()))),
        RespReply::Array(keys.into_iter().map(|k| RespReply::BulkString(Some(Bytes::from(k.into_bytes())))).collect()),
    ]))
}

/// Parse the trailing `[MATCH pattern] [COUNT n]` options common to SCAN /
/// HSCAN / SSCAN / ZSCAN. `from` is the arg index to start at (past the
/// command's fixed prefix — `1` for SCAN, `2` for collection scans).
fn parse_scan_opts(args: &[Bytes], from: usize, cmd: &str) -> Result<(Option<String>, usize), RustyAntError> {
    let mut pattern: Option<String> = None;
    let mut count: usize = 10;
    let mut i = from;
    while i < args.len() {
        let opt = arg_as_str(&args[i])?.to_ascii_uppercase();
        match opt.as_str() {
            "MATCH" => {
                let v = args.get(i + 1).ok_or_else(|| RustyAntError::Parse("MATCH requires a pattern".into()))?;
                pattern = Some(arg_as_string(v)?);
                i += 2;
            }
            "COUNT" => {
                let v = args.get(i + 1).ok_or_else(|| RustyAntError::Parse("COUNT requires a value".into()))?;
                let c = parse_i64(v, "COUNT")?;
                if c <= 0 {
                    return Err(RustyAntError::Parse("COUNT must be positive".into()));
                }
                count = usize::try_from(c).unwrap_or(10);
                i += 2;
            }
            other => {
                return Err(RustyAntError::Parse(format!("unsupported {cmd} option: {other}")));
            }
        }
    }
    Ok((pattern, count))
}

fn parse_scan_cursor(arg: &Bytes) -> Result<u64, RustyAntError> {
    let n = parse_i64(arg, "cursor")?;
    u64::try_from(n).map_err(|_| RustyAntError::Parse("cursor must be >= 0".into()))
}

fn scan_reply(next_cursor: u64, items: Vec<RespReply>) -> RespReply {
    RespReply::Array(vec![
        RespReply::BulkString(Some(Bytes::from(next_cursor.to_string().into_bytes()))),
        RespReply::Array(items),
    ])
}

async fn handle_hscan(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("HSCAN", args.len() >= 2)?;
    let key = arg_as_str(&args[0])?;
    let cursor = parse_scan_cursor(&args[1])?;
    let (pattern, count) = parse_scan_opts(&args, 2, "HSCAN")?;
    let (next, pairs) = state.storage.hscan(key, cursor, pattern.as_deref(), count).await?;
    let mut flat: Vec<RespReply> = Vec::with_capacity(pairs.len() * 2);
    for (field, value) in pairs {
        flat.push(RespReply::BulkString(Some(Bytes::from(field.into_bytes()))));
        flat.push(RespReply::BulkString(Some(value)));
    }
    Ok(scan_reply(next, flat))
}

async fn handle_sscan(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("SSCAN", args.len() >= 2)?;
    let key = arg_as_str(&args[0])?;
    let cursor = parse_scan_cursor(&args[1])?;
    let (pattern, count) = parse_scan_opts(&args, 2, "SSCAN")?;
    let (next, members) = state.storage.sscan(key, cursor, pattern.as_deref(), count).await?;
    let items: Vec<RespReply> =
        members.into_iter().map(|m| RespReply::BulkString(Some(Bytes::from(m.into_bytes())))).collect();
    Ok(scan_reply(next, items))
}

async fn handle_zscan(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("ZSCAN", args.len() >= 2)?;
    let key = arg_as_str(&args[0])?;
    let cursor = parse_scan_cursor(&args[1])?;
    let (pattern, count) = parse_scan_opts(&args, 2, "ZSCAN")?;
    let (next, pairs) = state.storage.zscan(key, cursor, pattern.as_deref(), count).await?;
    let mut flat: Vec<RespReply> = Vec::with_capacity(pairs.len() * 2);
    for (member, score) in pairs {
        flat.push(RespReply::BulkString(Some(Bytes::from(member.into_bytes()))));
        flat.push(RespReply::BulkString(Some(Bytes::from(format_score(score).into_bytes()))));
    }
    Ok(scan_reply(next, flat))
}

async fn handle_lindex(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("LINDEX", args.len() == 2)?;
    let key = arg_as_str(&args[0])?;
    let index = parse_i64(&args[1], "index")?;
    let elem = state.storage.lindex(key, index).await?;
    Ok(elem.map_or(RespReply::Nil, |v| RespReply::BulkString(Some(v))))
}

async fn handle_lset(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("LSET", args.len() == 3)?;
    let key = arg_as_str(&args[0])?;
    let index = parse_i64(&args[1], "index")?;
    state.storage.lset(key, index, args[2].clone()).await?;
    Ok(RespReply::ok())
}

async fn handle_lrem(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("LREM", args.len() == 3)?;
    let key = arg_as_str(&args[0])?;
    let count = parse_i64(&args[1], "count")?;
    let value = args[2].clone();
    let removed = state.storage.lrem(key, count, value).await?;
    Ok(RespReply::Integer(removed))
}

async fn handle_linsert(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("LINSERT", args.len() == 4)?;
    let key = arg_as_str(&args[0])?;
    let before = match arg_as_str(&args[1])?.to_ascii_uppercase().as_str() {
        "BEFORE" => true,
        "AFTER" => false,
        other => return Err(RustyAntError::Parse(format!("LINSERT direction must be BEFORE or AFTER, got {other}"))),
    };
    let pivot = args[2].clone();
    let value = args[3].clone();
    let new_len = state.storage.linsert(key, before, pivot, value).await?;
    Ok(RespReply::Integer(new_len))
}

async fn handle_ltrim(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("LTRIM", args.len() == 3)?;
    let key = arg_as_str(&args[0])?;
    let start = parse_i64(&args[1], "start")?;
    let stop = parse_i64(&args[2], "stop")?;
    state.storage.ltrim(key, start, stop).await?;
    Ok(RespReply::ok())
}

async fn handle_pushx(state: &State, args: Vec<Bytes>, left: bool) -> Result<RespReply, RustyAntError> {
    let cmd = if left { "LPUSHX" } else { "RPUSHX" };
    arity(cmd, args.len() >= 2)?;
    let key = arg_as_string(&args[0])?;
    let values: Vec<Bytes> = args.into_iter().skip(1).collect();
    let len = state.storage.list_pushx(&key, values, left).await?;
    Ok(RespReply::Integer(len))
}

fn parse_side(arg: &Bytes, label: &str) -> Result<bool, RustyAntError> {
    match arg_as_str(arg)?.to_ascii_uppercase().as_str() {
        "LEFT" => Ok(true),
        "RIGHT" => Ok(false),
        other => Err(RustyAntError::Parse(format!("{label} must be LEFT or RIGHT, got {other}"))),
    }
}

async fn handle_lmove(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("LMOVE", args.len() == 4)?;
    let from = arg_as_str(&args[0])?;
    let to = arg_as_str(&args[1])?;
    let from_left = parse_side(&args[2], "LMOVE source side")?;
    let to_left = parse_side(&args[3], "LMOVE destination side")?;
    let moved = state.storage.list_move(from, to, from_left, to_left).await?;
    Ok(moved.map_or(RespReply::Nil, |b| RespReply::BulkString(Some(b))))
}

async fn handle_rpoplpush(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("RPOPLPUSH", args.len() == 2)?;
    let from = arg_as_str(&args[0])?;
    let to = arg_as_str(&args[1])?;
    // RPOPLPUSH = LMOVE src dst RIGHT LEFT.
    let moved = state.storage.list_move(from, to, false, true).await?;
    Ok(moved.map_or(RespReply::Nil, |b| RespReply::BulkString(Some(b))))
}

async fn handle_lpos(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("LPOS", args.len() >= 2)?;
    let key = arg_as_str(&args[0])?;
    let element = args[1].clone();
    let mut rank: i64 = 1;
    let mut count: Option<usize> = None;
    let mut maxlen: usize = 0;
    let mut i = 2;
    while i < args.len() {
        let opt = arg_as_str(&args[i])?.to_ascii_uppercase();
        match opt.as_str() {
            "RANK" => {
                let v = args.get(i + 1).ok_or_else(|| RustyAntError::Parse("RANK requires a value".into()))?;
                rank = parse_i64(v, "RANK")?;
                if rank == 0 {
                    return Err(RustyAntError::Parse("RANK can't be zero".into()));
                }
                i += 2;
            }
            "COUNT" => {
                let v = args.get(i + 1).ok_or_else(|| RustyAntError::Parse("COUNT requires a value".into()))?;
                let n = parse_i64(v, "COUNT")?;
                if n < 0 {
                    return Err(RustyAntError::Parse("COUNT can't be negative".into()));
                }
                count = Some(usize::try_from(n).unwrap_or(0));
                i += 2;
            }
            "MAXLEN" => {
                let v = args.get(i + 1).ok_or_else(|| RustyAntError::Parse("MAXLEN requires a value".into()))?;
                let n = parse_i64(v, "MAXLEN")?;
                if n < 0 {
                    return Err(RustyAntError::Parse("MAXLEN can't be negative".into()));
                }
                maxlen = usize::try_from(n).unwrap_or(0);
                i += 2;
            }
            other => return Err(RustyAntError::Parse(format!("unsupported LPOS option: {other}"))),
        }
    }
    let hits = state.storage.lpos(key, &element, rank, count, maxlen).await?;
    // Without COUNT: first match as integer, or nil when none found.
    // With COUNT: always an array (possibly empty).
    if count.is_some() {
        Ok(RespReply::Array(hits.into_iter().map(RespReply::Integer).collect()))
    } else {
        Ok(hits.into_iter().next().map_or(RespReply::Nil, RespReply::Integer))
    }
}

async fn handle_smismember(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("SMISMEMBER", args.len() >= 2)?;
    let key = arg_as_str(&args[0])?;
    let members: Vec<String> = args.iter().skip(1).map(arg_as_string).collect::<Result<_, _>>()?;
    let flags = state.storage.smismember(key, &members).await?;
    Ok(RespReply::Array(flags.into_iter().map(|b| RespReply::Integer(i64::from(b))).collect()))
}

async fn handle_sinter(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("SINTER", !args.is_empty())?;
    let keys: Vec<String> = args.iter().map(arg_as_string).collect::<Result<_, _>>()?;
    let members = state.storage.sinter(&keys).await?;
    Ok(RespReply::Array(
        members.into_iter().map(|m| RespReply::BulkString(Some(Bytes::from(m.into_bytes())))).collect(),
    ))
}

async fn handle_sunion(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("SUNION", !args.is_empty())?;
    let keys: Vec<String> = args.iter().map(arg_as_string).collect::<Result<_, _>>()?;
    let members = state.storage.sunion(&keys).await?;
    Ok(RespReply::Array(
        members.into_iter().map(|m| RespReply::BulkString(Some(Bytes::from(m.into_bytes())))).collect(),
    ))
}

async fn handle_sdiff(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("SDIFF", !args.is_empty())?;
    let keys: Vec<String> = args.iter().map(arg_as_string).collect::<Result<_, _>>()?;
    let members = state.storage.sdiff(&keys).await?;
    Ok(RespReply::Array(
        members.into_iter().map(|m| RespReply::BulkString(Some(Bytes::from(m.into_bytes())))).collect(),
    ))
}

// ---------------------------------------------------------------------------
// SINTERSTORE / SUNIONSTORE / SDIFFSTORE — set aggregates into a destination.
//
// Inputs must all be SET (or missing); any other kind is a WRONGTYPE error.
// Destination is overwritten unconditionally (no WRONGTYPE check, matching
// Redis). An empty result deletes the destination.
// ---------------------------------------------------------------------------

#[derive(Debug, Copy, Clone)]
enum SetOp {
    Inter,
    Union,
    Diff,
}

async fn handle_sstore(state: &State, args: Vec<Bytes>, op: SetOp) -> Result<RespReply, RustyAntError> {
    if args.len() < 2 {
        return Err(RustyAntError::WrongArity {
            command: match op {
                SetOp::Inter => "SINTERSTORE",
                SetOp::Union => "SUNIONSTORE",
                SetOp::Diff => "SDIFFSTORE",
            }
            .into(),
        });
    }
    let dest = arg_as_string(&args[0])?;
    let src_keys: Vec<String> = args.iter().skip(1).map(arg_as_string).collect::<Result<_, _>>()?;
    let members = match op {
        SetOp::Inter => state.storage.sinter(&src_keys).await?,
        SetOp::Union => state.storage.sunion(&src_keys).await?,
        SetOp::Diff => state.storage.sdiff(&src_keys).await?,
    };
    state.storage.delete(&dest).await?;
    if members.is_empty() {
        return Ok(RespReply::Integer(0));
    }
    let count = state.storage.sadd(&dest, members).await?;
    Ok(RespReply::Integer(count))
}

// ---------------------------------------------------------------------------
// ZINTERSTORE / ZUNIONSTORE / ZDIFFSTORE — sorted-set aggregates into a
// destination, with optional WEIGHTS and AGGREGATE for Inter/Union.
//
// Inputs can be SET or ZSET (or missing); a SET contributes each member
// with score 1.0 before weight multiplication. Destination is overwritten
// unconditionally. An empty result deletes the destination.
// ---------------------------------------------------------------------------

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum ZStoreOp {
    Inter,
    Union,
    Diff,
}

#[derive(Debug, Copy, Clone)]
enum Aggregate {
    Sum,
    Min,
    Max,
}

async fn handle_zstore(state: &State, args: Vec<Bytes>, op: ZStoreOp) -> Result<RespReply, RustyAntError> {
    let cmd_name: &str = match op {
        ZStoreOp::Inter => "ZINTERSTORE",
        ZStoreOp::Union => "ZUNIONSTORE",
        ZStoreOp::Diff => "ZDIFFSTORE",
    };
    // Minimum: dest numkeys key = 3 tokens.
    if args.len() < 3 {
        return Err(RustyAntError::WrongArity { command: cmd_name.into() });
    }
    let dest = arg_as_string(&args[0])?;
    let numkeys_i = parse_i64(&args[1], "numkeys")?;
    if numkeys_i <= 0 {
        return Err(RustyAntError::Parse("at least 1 input key is needed for this command".into()));
    }
    let numkeys = usize::try_from(numkeys_i).unwrap_or(0);
    if args.len() < 2 + numkeys {
        return Err(RustyAntError::Parse("Number of keys can't be greater than number of args".into()));
    }
    let src_keys: Vec<String> = args.iter().skip(2).take(numkeys).map(arg_as_string).collect::<Result<_, _>>()?;

    // Trailing options: WEIGHTS w1 w2 ... / AGGREGATE SUM|MIN|MAX.
    // ZDIFFSTORE does not accept either — surface an error per Redis.
    let tail = &args[2 + numkeys..];
    let (weights, aggregate) = if op == ZStoreOp::Diff {
        if !tail.is_empty() {
            return Err(RustyAntError::Parse(format!("syntax error near '{}'", arg_as_str(&tail[0])?)));
        }
        (vec![1.0_f64; numkeys], Aggregate::Sum)
    } else {
        parse_weights_aggregate(tail, numkeys)?
    };

    // Load each source, applying its weight to produce (member, weighted score).
    let mut per_input: Vec<Vec<(String, f64)>> = Vec::with_capacity(numkeys);
    for (k, w) in src_keys.iter().zip(weights.iter()) {
        let items = load_zset_or_set(state, k).await?;
        per_input.push(items.into_iter().map(|(m, s)| (m, s * w)).collect());
    }

    let pairs = match op {
        ZStoreOp::Inter => aggregate_intersection(&per_input, aggregate),
        ZStoreOp::Union => aggregate_union(per_input, aggregate),
        ZStoreOp::Diff => aggregate_difference(per_input),
    };

    state.storage.delete(&dest).await?;
    if pairs.is_empty() {
        return Ok(RespReply::Integer(0));
    }
    let count = state.storage.zadd(&dest, pairs).await?;
    Ok(RespReply::Integer(count))
}

fn parse_weights_aggregate(tail: &[Bytes], numkeys: usize) -> Result<(Vec<f64>, Aggregate), RustyAntError> {
    let mut weights = vec![1.0_f64; numkeys];
    let mut aggregate = Aggregate::Sum;
    let mut i = 0;
    while i < tail.len() {
        let token = arg_as_str(&tail[i])?.to_ascii_uppercase();
        match token.as_str() {
            "WEIGHTS" => {
                if i + numkeys >= tail.len() {
                    return Err(RustyAntError::Parse("WEIGHTS needs one value per input key".into()));
                }
                for (slot, arg) in weights.iter_mut().zip(tail[i + 1..i + 1 + numkeys].iter()) {
                    *slot = parse_f64(arg, "weight")?;
                }
                i += 1 + numkeys;
            }
            "AGGREGATE" => {
                if i + 1 >= tail.len() {
                    return Err(RustyAntError::Parse("AGGREGATE requires SUM, MIN, or MAX".into()));
                }
                aggregate = match arg_as_str(&tail[i + 1])?.to_ascii_uppercase().as_str() {
                    "SUM" => Aggregate::Sum,
                    "MIN" => Aggregate::Min,
                    "MAX" => Aggregate::Max,
                    other => return Err(RustyAntError::Parse(format!("unsupported AGGREGATE mode: {other}"))),
                };
                i += 2;
            }
            other => return Err(RustyAntError::Parse(format!("syntax error near '{other}'"))),
        }
    }
    Ok((weights, aggregate))
}

async fn load_zset_or_set(state: &State, key: &str) -> Result<Vec<(String, f64)>, RustyAntError> {
    match state.storage.kind(key).await? {
        Some("zset") => state.storage.zitems(key).await,
        Some("set") => {
            let members = state.storage.smembers(key).await?;
            Ok(members.into_iter().map(|m| (m, 1.0)).collect())
        }
        Some(_) => Err(RustyAntError::WrongType { key: key.to_string() }),
        None => Ok(Vec::new()),
    }
}

fn aggregate_intersection(per_input: &[Vec<(String, f64)>], agg: Aggregate) -> Vec<(f64, String)> {
    use std::collections::HashMap;
    let Some(first) = per_input.first() else {
        return Vec::new();
    };
    // Start with the first input; subsequent inputs shrink the candidate set.
    let mut current: HashMap<String, f64> = first.iter().cloned().collect();
    for input in &per_input[1..] {
        if current.is_empty() {
            return Vec::new();
        }
        let next: HashMap<String, f64> = input.iter().cloned().collect();
        current.retain(|member, existing| {
            next.get(member).is_some_and(|incoming| {
                *existing = combine_scores(*existing, *incoming, agg);
                true
            })
        });
    }
    current.into_iter().map(|(m, s)| (s, m)).collect()
}

fn aggregate_union(per_input: Vec<Vec<(String, f64)>>, agg: Aggregate) -> Vec<(f64, String)> {
    use std::collections::HashMap;
    let mut acc: HashMap<String, f64> = HashMap::new();
    for input in per_input {
        for (member, score) in input {
            acc.entry(member).and_modify(|existing| *existing = combine_scores(*existing, score, agg)).or_insert(score);
        }
    }
    acc.into_iter().map(|(m, s)| (s, m)).collect()
}

fn aggregate_difference(per_input: Vec<Vec<(String, f64)>>) -> Vec<(f64, String)> {
    use std::collections::HashSet;
    if per_input.is_empty() {
        return Vec::new();
    }
    // ZDIFFSTORE: members in first set that don't appear in any other.
    // Scores from the first set are preserved (Redis doesn't aggregate —
    // there's only one source of truth for each surviving member).
    let mut drop: HashSet<String> = HashSet::new();
    for input in per_input.iter().skip(1) {
        for (member, _) in input {
            drop.insert(member.clone());
        }
    }
    per_input
        .into_iter()
        .next()
        .unwrap_or_default()
        .into_iter()
        .filter(|(m, _)| !drop.contains(m))
        .map(|(m, s)| (s, m))
        .collect()
}

fn combine_scores(a: f64, b: f64, agg: Aggregate) -> f64 {
    match agg {
        Aggregate::Sum => a + b,
        // Redis uses partial_cmp ordering for these; NaN falls through to
        // the first operand, matching Redis's "nan-pessimistic" behaviour.
        Aggregate::Min => {
            if b < a {
                b
            } else {
                a
            }
        }
        Aggregate::Max => {
            if b > a {
                b
            } else {
                a
            }
        }
    }
}

async fn handle_spop(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("SPOP", !args.is_empty() && args.len() <= 2)?;
    let key = arg_as_str(&args[0])?;
    if args.len() == 2 {
        let count = parse_i64(&args[1], "count")?;
        if count < 0 {
            return Err(RustyAntError::Parse("count must be >= 0".into()));
        }
        let count_usize = usize::try_from(count).unwrap_or(0);
        let popped = state.storage.spop(key, count_usize).await?;
        Ok(RespReply::Array(
            popped.into_iter().map(|m| RespReply::BulkString(Some(Bytes::from(m.into_bytes())))).collect(),
        ))
    } else {
        let mut popped = state.storage.spop(key, 1).await?;
        Ok(popped.pop().map_or(RespReply::Nil, |m| RespReply::BulkString(Some(Bytes::from(m.into_bytes())))))
    }
}

async fn handle_srandmember(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("SRANDMEMBER", !args.is_empty() && args.len() <= 2)?;
    let key = arg_as_str(&args[0])?;
    if args.len() == 2 {
        let count = parse_i64(&args[1], "count")?;
        let allow_duplicates = count < 0;
        let abs_count = count.checked_abs().ok_or_else(|| RustyAntError::Parse("count overflow".into()))?;
        let picked = state.storage.srandmember(key, abs_count, allow_duplicates).await?;
        Ok(RespReply::Array(
            picked.into_iter().map(|m| RespReply::BulkString(Some(Bytes::from(m.into_bytes())))).collect(),
        ))
    } else {
        let mut picked = state.storage.srandmember(key, 1, false).await?;
        Ok(picked.pop().map_or(RespReply::Nil, |m| RespReply::BulkString(Some(Bytes::from(m.into_bytes())))))
    }
}

async fn handle_zrangebyscore(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("ZRANGEBYSCORE", args.len() == 3)?;
    let key = arg_as_str(&args[0])?;
    let min = ScoreBound::parse(arg_as_str(&args[1])?)?;
    let max = ScoreBound::parse(arg_as_str(&args[2])?)?;
    let members = state.storage.zrangebyscore(key, min, max).await?;
    Ok(RespReply::Array(
        members.into_iter().map(|m| RespReply::BulkString(Some(Bytes::from(m.into_bytes())))).collect(),
    ))
}

async fn handle_setnx(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("SETNX", args.len() == 2)?;
    let key = arg_as_str(&args[0])?;
    let set = state.storage.set_string_nx(key, args[1].clone(), None).await?;
    Ok(RespReply::Integer(i64::from(set)))
}

async fn handle_setex(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("SETEX", args.len() == 3)?;
    let key = arg_as_str(&args[0])?;
    let secs = parse_i64(&args[1], "seconds")?;
    if secs <= 0 {
        return Err(RustyAntError::Parse("SETEX seconds must be positive".into()));
    }
    let expires_at_ms = Some(now_ms() + secs * 1000);
    state.storage.set_string(key, args[2].clone(), expires_at_ms).await?;
    Ok(RespReply::ok())
}

async fn handle_mget(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("MGET", !args.is_empty())?;
    let keys: Vec<String> = args.iter().map(arg_as_string).collect::<Result<_, _>>()?;
    let vals = state.storage.mget(&keys).await?;
    Ok(RespReply::Array(
        vals.into_iter().map(|v| v.map_or(RespReply::Nil, |b| RespReply::BulkString(Some(b)))).collect(),
    ))
}

async fn handle_mset(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    if args.is_empty() || args.len() % 2 != 0 {
        return Err(RustyAntError::WrongArity { command: "MSET".into() });
    }
    let mut pairs: Vec<(String, Bytes)> = Vec::with_capacity(args.len() / 2);
    let mut i = 0;
    while i < args.len() {
        pairs.push((arg_as_string(&args[i])?, args[i + 1].clone()));
        i += 2;
    }
    state.storage.mset(pairs).await?;
    Ok(RespReply::ok())
}

// ---- Additional mutating commands -----------------------------------------

async fn handle_hincrby(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("HINCRBY", args.len() == 3)?;
    let key = arg_as_str(&args[0])?;
    let field = arg_as_str(&args[1])?;
    let delta = parse_i64(&args[2], "increment")?;
    let new_val = state.storage.hincr_by(key, field, delta).await?;
    Ok(RespReply::Integer(new_val))
}

/// Redis `HINCRBYFLOAT key field increment`. Reply is the new value as a
/// bulk string, matching Redis. Rejects NaN / infinity deltas and results.
async fn handle_hincrbyfloat(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("HINCRBYFLOAT", args.len() == 3)?;
    let key = arg_as_str(&args[0])?;
    let field = arg_as_str(&args[1])?;
    let delta = parse_f64(&args[2], "increment")?;
    if delta.is_nan() || delta.is_infinite() {
        return Err(RustyAntError::Parse("increment would produce NaN or infinity".into()));
    }
    let new_val = state.storage.hincr_by_float(key, field, delta).await?;
    Ok(RespReply::BulkString(Some(Bytes::from(format_score(new_val).into_bytes()))))
}

async fn handle_srem(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("SREM", args.len() >= 2)?;
    let key = arg_as_str(&args[0])?;
    let members: Vec<String> = args.iter().skip(1).map(arg_as_string).collect::<Result<_, _>>()?;
    let removed = state.storage.srem(key, &members).await?;
    Ok(RespReply::Integer(removed))
}

async fn handle_zrem(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("ZREM", args.len() >= 2)?;
    let key = arg_as_str(&args[0])?;
    let members: Vec<String> = args.iter().skip(1).map(arg_as_string).collect::<Result<_, _>>()?;
    let removed = state.storage.zrem(key, &members).await?;
    Ok(RespReply::Integer(removed))
}

async fn handle_zincrby(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("ZINCRBY", args.len() == 3)?;
    let key = arg_as_str(&args[0])?;
    let delta = parse_f64(&args[1], "increment")?;
    let member = arg_as_str(&args[2])?;
    let new_score = state.storage.zincr_by(key, member, delta).await?;
    Ok(RespReply::BulkString(Some(Bytes::from(format_score(new_score).into_bytes()))))
}

/// Match Redis's score formatting: integers as `"42"`, finite floats via
/// their shortest round-trippable decimal, special values spelled out.
fn format_score(s: f64) -> String {
    if s.is_nan() {
        return "nan".to_string();
    }
    if s.is_infinite() {
        return (if s > 0.0 { "inf" } else { "-inf" }).to_string();
    }
    // Integer-valued scores inside i64 range → render without a decimal point.
    // Casting through the safe integer window first keeps clippy's truncation
    // lint satisfied.
    if s.fract() == 0.0 && s.abs() < 9.007_199_254_740_992e15 {
        #[allow(clippy::cast_possible_truncation)] // fract==0 && range checked
        let as_int = s as i64;
        return as_int.to_string();
    }
    format!("{s}")
}

// ---- New keyspace commands -------------------------------------------------

async fn handle_pexpire(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("PEXPIRE", args.len() == 2)?;
    let key = arg_as_str(&args[0])?;
    let ms = parse_i64(&args[1], "milliseconds")?;
    let set = state.storage.expire_at(key, now_ms() + ms).await?;
    Ok(RespReply::Integer(i64::from(set)))
}

async fn handle_pttl(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("PTTL", args.len() == 1)?;
    let key = arg_as_str(&args[0])?;
    Ok(match state.storage.ttl_ms(key).await? {
        TtlResult::NoKey => RespReply::Integer(-2),
        TtlResult::NoExpire => RespReply::Integer(-1),
        TtlResult::Ms(ms) => RespReply::Integer(ms),
    })
}

async fn handle_expiretime(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("EXPIRETIME", args.len() == 1)?;
    let key = arg_as_str(&args[0])?;
    Ok(match state.storage.expire_time_ms(key).await? {
        TtlResult::NoKey => RespReply::Integer(-2),
        TtlResult::NoExpire => RespReply::Integer(-1),
        TtlResult::Ms(ms) => RespReply::Integer(ms / 1000),
    })
}

async fn handle_pexpiretime(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("PEXPIRETIME", args.len() == 1)?;
    let key = arg_as_str(&args[0])?;
    Ok(match state.storage.expire_time_ms(key).await? {
        TtlResult::NoKey => RespReply::Integer(-2),
        TtlResult::NoExpire => RespReply::Integer(-1),
        TtlResult::Ms(ms) => RespReply::Integer(ms),
    })
}

async fn handle_rename(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("RENAME", args.len() == 2)?;
    let from = arg_as_str(&args[0])?;
    let to = arg_as_str(&args[1])?;
    state.storage.rename(from, to).await?;
    Ok(RespReply::ok())
}

async fn handle_renamenx(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("RENAMENX", args.len() == 2)?;
    let from = arg_as_str(&args[0])?;
    let to = arg_as_str(&args[1])?;
    let renamed = state.storage.renamenx(from, to).await?;
    Ok(RespReply::Integer(i64::from(renamed)))
}

// ---- New string commands ---------------------------------------------------

async fn handle_incrbyfloat(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("INCRBYFLOAT", args.len() == 2)?;
    let key = arg_as_str(&args[0])?;
    let delta = parse_f64(&args[1], "increment")?;
    let new = state.storage.incr_by_float(key, delta).await?;
    Ok(RespReply::BulkString(Some(Bytes::from(format_score(new).into_bytes()))))
}

async fn handle_getrange(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("GETRANGE", args.len() == 3)?;
    let key = arg_as_str(&args[0])?;
    let start = parse_i64(&args[1], "start")?;
    let end = parse_i64(&args[2], "end")?;
    let slice = state.storage.getrange(key, start, end).await?;
    Ok(RespReply::BulkString(Some(slice)))
}

async fn handle_setrange(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("SETRANGE", args.len() == 3)?;
    let key = arg_as_str(&args[0])?;
    let offset = parse_i64(&args[1], "offset")?;
    if offset < 0 {
        return Err(RustyAntError::Parse("offset must be >= 0".into()));
    }
    let offset_u = usize::try_from(offset).unwrap_or(0);
    let len = state.storage.setrange(key, offset_u, args[2].clone()).await?;
    Ok(RespReply::Integer(len))
}

async fn handle_msetnx(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    if args.is_empty() || args.len() % 2 != 0 {
        return Err(RustyAntError::WrongArity { command: "MSETNX".into() });
    }
    let mut pairs: Vec<(String, Bytes)> = Vec::with_capacity(args.len() / 2);
    let mut i = 0;
    while i < args.len() {
        pairs.push((arg_as_string(&args[i])?, args[i + 1].clone()));
        i += 2;
    }
    let all_set = state.storage.msetnx(pairs).await?;
    Ok(RespReply::Integer(i64::from(all_set)))
}

// ---- New sorted-set commands ----------------------------------------------

async fn handle_zrevrange(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("ZREVRANGE", args.len() == 3)?;
    let key = arg_as_str(&args[0])?;
    let start = parse_i64(&args[1], "start")?;
    let stop = parse_i64(&args[2], "stop")?;
    let members = state.storage.zrevrange(key, start, stop).await?;
    Ok(RespReply::Array(
        members.into_iter().map(|m| RespReply::BulkString(Some(Bytes::from(m.into_bytes())))).collect(),
    ))
}

async fn handle_zrevrangebyscore(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("ZREVRANGEBYSCORE", args.len() == 3)?;
    let key = arg_as_str(&args[0])?;
    // Note: argument order is (max, min) — the reverse of ZRANGEBYSCORE —
    // to match Redis's CLI convention.
    let max = ScoreBound::parse(arg_as_str(&args[1])?)?;
    let min = ScoreBound::parse(arg_as_str(&args[2])?)?;
    let members = state.storage.zrevrangebyscore(key, max, min).await?;
    Ok(RespReply::Array(
        members.into_iter().map(|m| RespReply::BulkString(Some(Bytes::from(m.into_bytes())))).collect(),
    ))
}

async fn handle_zremrangebyrank(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("ZREMRANGEBYRANK", args.len() == 3)?;
    let key = arg_as_str(&args[0])?;
    let start = parse_i64(&args[1], "start")?;
    let stop = parse_i64(&args[2], "stop")?;
    let removed = state.storage.zremrangebyrank(key, start, stop).await?;
    Ok(RespReply::Integer(removed))
}

async fn handle_zremrangebyscore(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("ZREMRANGEBYSCORE", args.len() == 3)?;
    let key = arg_as_str(&args[0])?;
    let min = ScoreBound::parse(arg_as_str(&args[1])?)?;
    let max = ScoreBound::parse(arg_as_str(&args[2])?)?;
    let removed = state.storage.zremrangebyscore(key, min, max).await?;
    Ok(RespReply::Integer(removed))
}

async fn handle_zpop(state: &State, args: Vec<Bytes>, from_max: bool) -> Result<RespReply, RustyAntError> {
    let cmd = if from_max { "ZPOPMAX" } else { "ZPOPMIN" };
    arity(cmd, !args.is_empty() && args.len() <= 2)?;
    let key = arg_as_str(&args[0])?;
    let count = if args.len() == 2 {
        let c = parse_i64(&args[1], "count")?;
        if c < 0 {
            return Err(RustyAntError::Parse("count must be >= 0".into()));
        }
        usize::try_from(c).unwrap_or(0)
    } else {
        1
    };
    let popped =
        if from_max { state.storage.zpopmax(key, count).await? } else { state.storage.zpopmin(key, count).await? };
    // Flatten [(member, score), ...] into a flat RESP array matching Redis's
    // `member, score, member, score, ...` encoding.
    let mut flat: Vec<RespReply> = Vec::with_capacity(popped.len() * 2);
    for (m, s) in popped {
        flat.push(RespReply::BulkString(Some(Bytes::from(m.into_bytes()))));
        flat.push(RespReply::BulkString(Some(Bytes::from(format_score(s).into_bytes()))));
    }
    Ok(RespReply::Array(flat))
}

// ---- Server / keyspace housekeeping ---------------------------------------

fn handle_echo(args: &[Bytes]) -> Result<RespReply, RustyAntError> {
    arity("ECHO", args.len() == 1)?;
    Ok(RespReply::BulkString(Some(args[0].clone())))
}

fn handle_time(args: &[Bytes]) -> Result<RespReply, RustyAntError> {
    arity("TIME", args.is_empty())?;
    let dur = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    let secs = dur.as_secs().to_string();
    let micros = dur.subsec_micros().to_string();
    Ok(RespReply::Array(vec![
        RespReply::BulkString(Some(Bytes::from(secs.into_bytes()))),
        RespReply::BulkString(Some(Bytes::from(micros.into_bytes()))),
    ]))
}

async fn handle_dbsize(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("DBSIZE", args.is_empty())?;
    let n = state.storage.dbsize().await?;
    Ok(RespReply::Integer(n))
}

/// Redis `INFO` — multi-section bulk string, CRLF-separated.
///
/// rustyant emits four sections (`server`, `clients`, `stats`, `keyspace`).
/// An optional single-section argument filters the output; `everything` /
/// `default` / `all` are accepted as synonyms for "all sections" (Redis 7).
async fn handle_info(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    use std::fmt::Write as _;

    let section = match args.len() {
        0 => None,
        1 => Some(arg_as_str(&args[0])?.to_ascii_lowercase()),
        _ => return Err(RustyAntError::WrongArity { command: "INFO".into() }),
    };
    let mut out = String::new();
    if info_section_included(section.as_deref(), "server") {
        let uptime = (now_ms().saturating_sub(state.started_at_ms)) / 1000;
        out.push_str("# Server\r\n");
        // Report a real Redis version so clients that gate on it (redis-py
        // HELLO, etc.) treat rustyant as compatible. Actual package version
        // lives under `rustyant_version`.
        out.push_str("redis_version:7.4.0\r\n");
        let _ = writeln!(out, "rustyant_version:{}\r", env!("CARGO_PKG_VERSION"));
        let _ = writeln!(out, "process_id:{}\r", std::process::id());
        let _ = writeln!(out, "uptime_in_seconds:{uptime}\r");
        out.push_str("os:Linux-lambda\r\n");
        out.push_str("arch_bits:64\r\n");
        out.push_str("\r\n");
    }
    if info_section_included(section.as_deref(), "clients") {
        // Lambda serves one request per invocation — there's no persistent
        // client pool to report on. Fixed at 1 so clients that check this
        // field don't trip on a zero.
        out.push_str("# Clients\r\n");
        out.push_str("connected_clients:1\r\n");
        out.push_str("\r\n");
    }
    if info_section_included(section.as_deref(), "stats") {
        // No cross-invocation counter to report; Lambda cold-starts reset it.
        // Emit the fields so tools don't explode on missing keys, with zeros.
        out.push_str("# Stats\r\n");
        out.push_str("total_connections_received:0\r\n");
        out.push_str("total_commands_processed:0\r\n");
        out.push_str("instantaneous_ops_per_sec:0\r\n");
        out.push_str("\r\n");
    }
    if info_section_included(section.as_deref(), "keyspace") {
        let ks = state.storage.keyspace_stats().await?;
        out.push_str("# Keyspace\r\n");
        if ks.total_keys > 0 {
            let _ = writeln!(out, "db0:keys={},expires={},avg_ttl=0\r", ks.total_keys, ks.keys_with_expire);
        }
        out.push_str("\r\n");
    }
    Ok(RespReply::BulkString(Some(Bytes::from(out))))
}

fn info_section_included(requested: Option<&str>, section: &str) -> bool {
    requested.is_none_or(|r| r == section || matches!(r, "everything" | "default" | "all"))
}

async fn handle_flushall(state: &State, args: Vec<Bytes>, cmd: &str) -> Result<RespReply, RustyAntError> {
    // Accept the optional ASYNC / SYNC modifier (Redis 4+) but ignore it —
    // rustyant's flush is always synchronous over S3.
    if args.len() > 1 {
        return Err(RustyAntError::WrongArity { command: cmd.to_string() });
    }
    if let Some(mode) = args.first() {
        match arg_as_str(mode)?.to_ascii_uppercase().as_str() {
            "ASYNC" | "SYNC" => {}
            other => return Err(RustyAntError::Parse(format!("unsupported {cmd} option: {other}"))),
        }
    }
    state.storage.flushall().await?;
    Ok(RespReply::ok())
}

async fn handle_randomkey(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("RANDOMKEY", args.is_empty())?;
    Ok(state
        .storage
        .random_key()
        .await?
        .map_or(RespReply::Nil, |k| RespReply::BulkString(Some(Bytes::from(k.into_bytes())))))
}

// ---- Bit ops on Strings ---------------------------------------------------

fn parse_bit_offset(arg: &Bytes) -> Result<u64, RustyAntError> {
    let n = parse_i64(arg, "offset")?;
    u64::try_from(n).map_err(|_| RustyAntError::Parse("offset must be >= 0".into()))
}

async fn handle_getbit(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("GETBIT", args.len() == 2)?;
    let key = arg_as_str(&args[0])?;
    let offset = parse_bit_offset(&args[1])?;
    let v = state.storage.getbit(key, offset).await?;
    Ok(RespReply::Integer(v))
}

async fn handle_setbit(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("SETBIT", args.len() == 3)?;
    let key = arg_as_str(&args[0])?;
    let offset = parse_bit_offset(&args[1])?;
    let bit = match parse_i64(&args[2], "value")? {
        0 => false,
        1 => true,
        _ => return Err(RustyAntError::Parse("bit value must be 0 or 1".into())),
    };
    let prev = state.storage.setbit(key, offset, bit).await?;
    Ok(RespReply::Integer(prev))
}

/// Resolve a Redis-style inclusive byte/bit range against `len_units`,
/// returning `(start, end_inclusive)` clamped into bounds. `None` means
/// the requested range collapses to empty (e.g. start past end).
fn resolve_inclusive_range(start: i64, end: i64, len_units: usize) -> Option<(usize, usize)> {
    if len_units == 0 {
        return None;
    }
    let len_i = i64::try_from(len_units).unwrap_or(i64::MAX);
    let s = if start < 0 { (len_i + start).max(0) } else { start };
    let e = if end < 0 { len_i + end } else { end.min(len_i - 1) };
    if s >= len_i || e < 0 || s > e {
        return None;
    }
    Some((usize::try_from(s).unwrap_or(0), usize::try_from(e).unwrap_or(0)))
}

/// Parse the optional trailing `BYTE` / `BIT` keyword on `BITCOUNT` / `BITPOS`.
/// Defaults to byte-wise when omitted, matching Redis.
fn parse_range_unit(arg: Option<&Bytes>) -> Result<bool, RustyAntError> {
    let Some(arg) = arg else { return Ok(false) };
    match arg_as_str(arg)?.to_ascii_uppercase().as_str() {
        "BYTE" => Ok(false),
        "BIT" => Ok(true),
        other => Err(RustyAntError::Parse(format!("range unit must be BYTE or BIT, got {other}"))),
    }
}

/// Count set bits across `data[start..=end]` (byte indices, both inclusive).
fn count_bits_byte_range(data: &[u8], start: usize, end: usize) -> i64 {
    let total: u32 = data[start..=end].iter().map(|b| b.count_ones()).sum();
    i64::from(total)
}

/// Count set bits across the bit range `[start..=end]` (bit indices, both
/// inclusive). Walks bit by bit at the boundaries; the fully-covered
/// interior bytes are summed via `count_ones` for speed.
fn count_bits_bit_range(data: &[u8], start: u64, end: u64) -> i64 {
    let mut count: i64 = 0;
    for bit in start..=end {
        count += i64::from(bit_at(data, bit));
    }
    count
}

async fn handle_bitcount(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    if args.is_empty() || args.len() == 2 || args.len() > 4 {
        return Err(RustyAntError::WrongArity { command: "BITCOUNT".into() });
    }
    let key = arg_as_str(&args[0])?;
    let Some(data) = state.storage.get_string(key).await? else {
        return Ok(RespReply::Integer(0));
    };
    if args.len() == 1 {
        let total: u32 = data.iter().map(|b| b.count_ones()).sum();
        return Ok(RespReply::Integer(i64::from(total)));
    }
    let start = parse_i64(&args[1], "start")?;
    let end = parse_i64(&args[2], "end")?;
    let in_bits = parse_range_unit(args.get(3))?;
    let len_units = if in_bits { data.len().saturating_mul(8) } else { data.len() };
    let Some((s, e)) = resolve_inclusive_range(start, end, len_units) else {
        return Ok(RespReply::Integer(0));
    };
    let count =
        if in_bits { count_bits_bit_range(&data, s as u64, e as u64) } else { count_bits_byte_range(&data, s, e) };
    Ok(RespReply::Integer(count))
}

/// Find the first bit `target` (0 or 1) in `data[start_bit..=end_bit]`,
/// returning the absolute bit index, or `None` if not found.
fn find_first_bit(data: &[u8], target: u8, start_bit: u64, end_bit: u64) -> Option<u64> {
    (start_bit..=end_bit).find(|&bit| bit_at(data, bit) == target)
}

async fn handle_bitpos(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    // BITPOS key bit [start [end [BYTE|BIT]]]
    if args.len() < 2 || args.len() > 5 {
        return Err(RustyAntError::WrongArity { command: "BITPOS".into() });
    }
    let key = arg_as_str(&args[0])?;
    let target = match parse_i64(&args[1], "bit")? {
        0 => 0u8,
        1 => 1u8,
        _ => return Err(RustyAntError::Parse("bit must be 0 or 1".into())),
    };
    let data = state.storage.get_string(key).await?.unwrap_or_default();
    if data.is_empty() {
        // Empty / missing key: an all-zeros search lands at bit 0; an
        // all-ones search has nothing to find.
        return Ok(RespReply::Integer(if target == 0 { 0 } else { -1 }));
    }
    let total_bits = u64::try_from(data.len()).unwrap_or(u64::MAX).saturating_mul(8);
    let end_explicit = args.len() >= 4;
    let in_bits = parse_range_unit(args.get(4))?;

    // Resolve byte- or bit-based range against the right unit count.
    let (start_bit, end_bit) = if args.len() >= 3 {
        let start = parse_i64(&args[2], "start")?;
        let end_unit = if end_explicit { parse_i64(&args[3], "end")? } else { i64::MAX };
        let len_units = if in_bits { data.len().saturating_mul(8) } else { data.len() };
        let Some((s, e)) = resolve_inclusive_range(start, end_unit, len_units) else {
            return Ok(RespReply::Integer(-1));
        };
        if in_bits { (s as u64, e as u64) } else { ((s as u64) * 8, ((e as u64) + 1) * 8 - 1) }
    } else {
        (0u64, total_bits - 1)
    };

    if let Some(pos) = find_first_bit(&data, target, start_bit, end_bit) {
        return Ok(RespReply::Integer(i64::try_from(pos).unwrap_or(i64::MAX)));
    }
    // Asymmetry in Redis: when looking for a 0 bit and the user did NOT pin
    // an explicit end, the trailing bits are treated as "infinite zeros" —
    // return the position just past the end of the string. With an explicit
    // end (or when looking for a 1), -1 is the right answer.
    if target == 0 && !end_explicit {
        return Ok(RespReply::Integer(i64::try_from(total_bits).unwrap_or(i64::MAX)));
    }
    Ok(RespReply::Integer(-1))
}

#[derive(Debug, Copy, Clone)]
enum BitOp {
    And,
    Or,
    Xor,
    Not,
}

impl BitOp {
    fn parse(s: &str) -> Result<Self, RustyAntError> {
        match s.to_ascii_uppercase().as_str() {
            "AND" => Ok(Self::And),
            "OR" => Ok(Self::Or),
            "XOR" => Ok(Self::Xor),
            "NOT" => Ok(Self::Not),
            other => Err(RustyAntError::Parse(format!("BITOP operation must be AND/OR/XOR/NOT, got {other}"))),
        }
    }
}

/// Combine `sources` byte-wise under `op`. AND/OR/XOR pad missing key bytes
/// to zero up to the longest source's length; NOT inverts a single source.
fn apply_bitop(op: BitOp, sources: &[Vec<u8>]) -> Vec<u8> {
    if matches!(op, BitOp::Not) {
        return sources.first().map(|s| s.iter().map(|b| !b).collect()).unwrap_or_default();
    }
    let max_len = sources.iter().map(Vec::len).max().unwrap_or(0);
    let init = match op {
        BitOp::And => 0xFFu8,
        _ => 0x00u8,
    };
    let mut out = vec![init; max_len];
    let mut first = true;
    for src in sources {
        for (i, slot) in out.iter_mut().enumerate() {
            let b = src.get(i).copied().unwrap_or(0);
            if first {
                *slot = b;
            } else {
                match op {
                    BitOp::And => *slot &= b,
                    BitOp::Or => *slot |= b,
                    BitOp::Xor => *slot ^= b,
                    BitOp::Not => unreachable!(),
                }
            }
        }
        first = false;
    }
    out
}

async fn handle_bitop(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    if args.len() < 3 {
        return Err(RustyAntError::WrongArity { command: "BITOP".into() });
    }
    let op = BitOp::parse(arg_as_str(&args[0])?)?;
    let dest = arg_as_string(&args[1])?;
    let src_keys: Vec<String> = args.iter().skip(2).map(arg_as_string).collect::<Result<_, _>>()?;
    if matches!(op, BitOp::Not) && src_keys.len() != 1 {
        return Err(RustyAntError::Parse("BITOP NOT takes exactly one source key".into()));
    }
    let mut sources: Vec<Vec<u8>> = Vec::with_capacity(src_keys.len());
    for k in &src_keys {
        let bytes = state.storage.get_string(k).await?.map_or_else(Vec::new, |b| b.to_vec());
        sources.push(bytes);
    }
    let result = apply_bitop(op, &sources);
    let len = i64::try_from(result.len()).unwrap_or(i64::MAX);
    if result.is_empty() {
        // Mirror Redis: empty result removes the destination instead of
        // creating an empty-string entry.
        state.storage.delete(&dest).await?;
    } else {
        state.storage.set_string(&dest, Bytes::from(result), None).await?;
    }
    Ok(RespReply::Integer(len))
}

async fn handle_copy(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    if args.len() < 2 {
        return Err(RustyAntError::WrongArity { command: "COPY".into() });
    }
    let from = arg_as_str(&args[0])?;
    let to = arg_as_str(&args[1])?;
    let mut replace = false;
    let mut i = 2;
    while i < args.len() {
        let opt = arg_as_str(&args[i])?.to_ascii_uppercase();
        match opt.as_str() {
            "REPLACE" => {
                replace = true;
                i += 1;
            }
            "DB" => {
                // Single-DB rustyant — only DB 0 is acceptable.
                let v = args.get(i + 1).ok_or_else(|| RustyAntError::Parse("DB requires a value".into()))?;
                let db = parse_i64(v, "DB")?;
                if db != 0 {
                    return Err(RustyAntError::Parse("DB must be 0 — rustyant exposes a single namespace".into()));
                }
                i += 2;
            }
            other => return Err(RustyAntError::Parse(format!("unsupported COPY option: {other}"))),
        }
    }
    let copied = state.storage.copy(from, to, replace).await?;
    Ok(RespReply::Integer(i64::from(copied)))
}

// ---------------------------------------------------------------------------
// Connection handshake — HELLO / CLIENT / RESET.
//
// redis-py issues these on every connection (HELLO for protocol negotiation,
// CLIENT SETINFO to register library metadata). Returning `unknown command`
// forces redis-py into error-and-retry paths and clutters production logs;
// accepting them quietly is the friendlier default.
// ---------------------------------------------------------------------------

/// Redis `HELLO` — protocol handshake.
///
/// `HELLO [protover] [AUTH user password] [SETNAME name]`. rustyant only
/// speaks RESP2: `protover=2` (or omitted) succeeds with the info map;
/// `protover=3` returns `-NOPROTO` so the client falls back cleanly. AUTH
/// and SETNAME are accepted syntactically but ignored — rustyant has no
/// auth model and no per-connection client tracking (Lambda is one-shot).
fn handle_hello(args: &[Bytes]) -> Result<RespReply, RustyAntError> {
    let mut i = 0;
    if let Some(first) = args.first() {
        // First positional arg, if present, must be the protover — anything
        // else (AUTH/SETNAME with no protover) is a syntax error per Redis.
        let proto_str = arg_as_str(first)?;
        let proto: i64 = proto_str.parse().map_err(|_| {
            RustyAntError::Parse(format!("Protocol version is not an integer or out of range: {proto_str}"))
        })?;
        if proto != 2 {
            return Err(RustyAntError::Parse(
                "NOPROTO unsupported protocol version — rustyant speaks RESP2 only".into(),
            ));
        }
        i += 1;
    }
    while i < args.len() {
        let opt = arg_as_str(&args[i])?.to_ascii_uppercase();
        match opt.as_str() {
            "AUTH" => {
                // AUTH requires two more args (user, password). Consume them
                // without validating — rustyant has no auth backend.
                if args.len() < i + 3 {
                    return Err(RustyAntError::Parse("AUTH requires username and password".into()));
                }
                i += 3;
            }
            "SETNAME" => {
                if args.len() < i + 2 {
                    return Err(RustyAntError::Parse("SETNAME requires a value".into()));
                }
                i += 2;
            }
            other => return Err(RustyAntError::Parse(format!("unsupported HELLO option: {other}"))),
        }
    }
    Ok(hello_info_reply())
}

/// Flat key/value array describing the server, returned to a successful
/// `HELLO [2]`. Matches the shape Redis itself emits for RESP2.
fn hello_info_reply() -> RespReply {
    let bulk = |s: &str| RespReply::BulkString(Some(Bytes::copy_from_slice(s.as_bytes())));
    RespReply::Array(vec![
        bulk("server"),
        bulk("rustyant"),
        bulk("version"),
        bulk(env!("CARGO_PKG_VERSION")),
        bulk("proto"),
        RespReply::Integer(2),
        bulk("id"),
        RespReply::Integer(1),
        bulk("mode"),
        bulk("standalone"),
        bulk("role"),
        bulk("master"),
        bulk("modules"),
        RespReply::Array(Vec::new()),
    ])
}

/// Redis `CLIENT` — per-connection configuration.
///
/// rustyant has no persistent connection state (Lambda), so every subcommand
/// returns a sensible canned reply. The subcommands that clients actually
/// call on connect (SETINFO, SETNAME, ID, GETNAME) are accepted quietly;
/// the rest return `+OK` too, with an explicit error only for genuinely
/// unknown subcommand names.
fn handle_client(args: &[Bytes]) -> Result<RespReply, RustyAntError> {
    let sub = args.first().ok_or_else(|| RustyAntError::WrongArity { command: "CLIENT".into() })?;
    let sub = arg_as_str(sub)?.to_ascii_uppercase();
    match sub.as_str() {
        "SETINFO" | "SETNAME" | "NO-EVICT" | "NO-TOUCH" | "REPLY" | "UNPAUSE" | "PAUSE" | "TRACKING"
        | "TRACKINGINFO" => Ok(RespReply::ok()),
        "ID" => Ok(RespReply::Integer(1)),
        "GETNAME" => Ok(RespReply::BulkString(Some(Bytes::new()))),
        // INFO and LIST report the single "connection" rustyant ever has
        // (Lambda is one-shot). redis-py and redis-cli both parse this line.
        "INFO" | "LIST" => {
            Ok(RespReply::BulkString(Some(Bytes::from_static(b"id=1 addr=lambda name= age=0 idle=0 flags=N db=0\r\n"))))
        }
        other => Err(RustyAntError::Parse(format!("unsupported CLIENT subcommand: {other}"))),
    }
}

/// Redis `AUTH` — `AUTH password` or `AUTH username password`.
///
/// rustyant has no auth backend; accept the call so clients that send
/// credentials on connect (matching HELLO's AUTH-is-ignored behavior)
/// don't hit an error. Arity is validated so genuine malformed requests
/// still surface.
fn handle_auth(args: &[Bytes]) -> Result<RespReply, RustyAntError> {
    arity("AUTH", matches!(args.len(), 1 | 2))?;
    Ok(RespReply::ok())
}

/// Redis `WAIT numreplicas timeout`.
///
/// rustyant has no replication model — returning `0` immediately is the
/// honest answer: zero replicas have acknowledged. The two args are
/// parsed for syntactic validation but the call does not block.
fn handle_wait(args: &[Bytes]) -> Result<RespReply, RustyAntError> {
    arity("WAIT", args.len() == 2)?;
    let _ = parse_i64(&args[0], "numreplicas")?;
    let _ = parse_i64(&args[1], "timeout")?;
    Ok(RespReply::Integer(0))
}

/// Redis `BGSAVE [SCHEDULE]`.
///
/// No actual background save — every SET is already durable on S3. Reply
/// with the same simple string Redis does so monitoring clients parse the
/// acknowledgment unchanged.
fn handle_bgsave(args: &[Bytes]) -> Result<RespReply, RustyAntError> {
    if args.len() > 1 {
        return Err(RustyAntError::WrongArity { command: "BGSAVE".into() });
    }
    if let Some(opt) = args.first() {
        match arg_as_str(opt)?.to_ascii_uppercase().as_str() {
            "SCHEDULE" => {}
            other => return Err(RustyAntError::Parse(format!("unsupported BGSAVE option: {other}"))),
        }
    }
    Ok(RespReply::SimpleString("Background saving started".into()))
}

/// Redis `LATENCY` — monitoring subcommand router.
///
/// rustyant emits EMF metrics for latency observation (see `README.md`);
/// the `LATENCY` command family itself is stubbed. `HISTORY` / `LATEST` /
/// `GRAPH` return empty results, `RESET` returns `0`, `DOCTOR` returns a
/// bland all-clear string.
fn handle_latency(args: &[Bytes]) -> Result<RespReply, RustyAntError> {
    let sub = args.first().ok_or_else(|| RustyAntError::WrongArity { command: "LATENCY".into() })?;
    let sub = arg_as_str(sub)?.to_ascii_uppercase();
    match sub.as_str() {
        "RESET" => Ok(RespReply::Integer(0)),
        "HISTORY" | "GRAPH" => {
            // Both require a single event-name arg but rustyant tracks none.
            if args.len() != 2 {
                return Err(RustyAntError::WrongArity { command: format!("LATENCY {sub}") });
            }
            Ok(if sub == "GRAPH" { RespReply::BulkString(Some(Bytes::new())) } else { RespReply::Array(Vec::new()) })
        }
        "LATEST" => Ok(RespReply::Array(Vec::new())),
        "DOCTOR" => Ok(RespReply::BulkString(Some(Bytes::from_static(
            b"Dave, I have observed the system for a while and I have no latency issues to report.\n",
        )))),
        other => Err(RustyAntError::Parse(format!("unsupported LATENCY subcommand: {other}"))),
    }
}

/// Redis `DEBUG` — developer subcommand router, mostly unsupported.
///
/// The only genuinely useful subcommand outside Redis internals is
/// `DEBUG SLEEP <seconds>`, which callers use to probe client timeout
/// handling. Everything else (OBJECT, SEGFAULT, RELOAD, SET-ACTIVE-EXPIRE,
/// etc.) touches server-specific state rustyant doesn't have, so they
/// return an explicit error rather than a silent lie.
async fn handle_debug(args: &[Bytes]) -> Result<RespReply, RustyAntError> {
    let sub = args.first().ok_or_else(|| RustyAntError::WrongArity { command: "DEBUG".into() })?;
    let sub = arg_as_str(sub)?.to_ascii_uppercase();
    match sub.as_str() {
        "SLEEP" => {
            if args.len() != 2 {
                return Err(RustyAntError::WrongArity { command: "DEBUG SLEEP".into() });
            }
            let secs = arg_as_str(&args[1])?
                .parse::<f64>()
                .map_err(|_| RustyAntError::Parse("DEBUG SLEEP takes a number".into()))?;
            if !secs.is_finite() || secs < 0.0 {
                return Err(RustyAntError::Parse("DEBUG SLEEP takes a non-negative number".into()));
            }
            // Cap at the test-loop-friendly 5 seconds. Lambda hard-timeouts
            // past this anyway; a longer sleep is almost always a mistake.
            // The cast is safe: the value is finite, non-negative, and <= 5000.
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            let ms = (secs.min(5.0) * 1000.0) as u64;
            tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
            Ok(RespReply::ok())
        }
        other => Err(RustyAntError::Parse(format!(
            "DEBUG {other} is not supported on rustyant (S3-backed; no engine-internal state to inspect)"
        ))),
    }
}

// ---------------------------------------------------------------------------
// Transport-gated commands (MULTI / WATCH / SUBSCRIBE / PSUBSCRIBE /
// UNSUBSCRIBE / PUNSUBSCRIBE) share one policy and one error message: rustyant
// dispatches one command per request with no cross-request connection state
// and no server-initiated push, so any command that needs either (transaction
// queueing, optimistic CAS, long-lived subscriber channel) must error
// explicitly rather than silently return +OK and leave the client hung.
//
// EXEC / DISCARD keep Redis's canonical `EXEC without MULTI` / `DISCARD
// without MULTI` replies — returning the shape real Redis returns is positive
// compat signal for clients that probe the error string.
//
// UNWATCH / PUBLISH / PUBSUB are honest no-ops, not errors: clearing an empty
// watch set is trivially successful; `:0` subscribers is literally the truth
// on a no-substrate server; empty/zero replies for PUBSUB introspection let
// monitoring tools confirm a redis-shaped server without lying.
// ---------------------------------------------------------------------------

/// Shared error for commands rustyant cannot honestly satisfy given its
/// one-command-per-request transport (no connection-level state, no
/// server-initiated push). Consolidated so all six call sites carry the same
/// policy statement rather than six near-duplicate sentences.
fn not_supported_on_this_transport(cmd: &str) -> RustyAntError {
    RustyAntError::Parse(format!(
        "{cmd} is not supported on rustyant: requires connection-level state or server-initiated push, which rustyant's one-command-per-request transport does not provide"
    ))
}

fn handle_multi(args: &[Bytes]) -> Result<RespReply, RustyAntError> {
    arity("MULTI", args.is_empty())?;
    Err(not_supported_on_this_transport("MULTI"))
}

fn handle_exec(args: &[Bytes]) -> Result<RespReply, RustyAntError> {
    arity("EXEC", args.is_empty())?;
    Err(RustyAntError::Parse("EXEC without MULTI".into()))
}

fn handle_discard(args: &[Bytes]) -> Result<RespReply, RustyAntError> {
    arity("DISCARD", args.is_empty())?;
    Err(RustyAntError::Parse("DISCARD without MULTI".into()))
}

fn handle_watch(args: &[Bytes]) -> Result<RespReply, RustyAntError> {
    // Redis requires at least one key; enforce arity before rejecting so a
    // malformed `WATCH` still surfaces as a clearer "wrong arity" error.
    arity("WATCH", !args.is_empty())?;
    Err(not_supported_on_this_transport("WATCH"))
}

fn handle_unwatch(args: &[Bytes]) -> Result<RespReply, RustyAntError> {
    arity("UNWATCH", args.is_empty())?;
    // Matches real Redis's "UNWATCH outside MULTI" behavior — trivially-
    // successful no-op when no keys are watched.
    Ok(RespReply::ok())
}

fn handle_subscribe(args: &[Bytes]) -> Result<RespReply, RustyAntError> {
    arity("SUBSCRIBE", !args.is_empty())?;
    Err(not_supported_on_this_transport("SUBSCRIBE"))
}

fn handle_psubscribe(args: &[Bytes]) -> Result<RespReply, RustyAntError> {
    arity("PSUBSCRIBE", !args.is_empty())?;
    Err(not_supported_on_this_transport("PSUBSCRIBE"))
}

fn handle_unsubscribe(_args: &[Bytes]) -> Result<RespReply, RustyAntError> {
    // Redis allows UNSUBSCRIBE with zero args ("unsubscribe from all"); we
    // still error because the command only makes sense inside a subscribed
    // session rustyant never enters.
    Err(not_supported_on_this_transport("UNSUBSCRIBE"))
}

fn handle_punsubscribe(_args: &[Bytes]) -> Result<RespReply, RustyAntError> {
    Err(not_supported_on_this_transport("PUNSUBSCRIBE"))
}

fn handle_publish(args: &[Bytes]) -> Result<RespReply, RustyAntError> {
    arity("PUBLISH", args.len() == 2)?;
    // Zero subscribers received the message. Honest on a no-pubsub substrate
    // — and matches what Redis returns on an idle server with no one
    // subscribed to the channel.
    Ok(RespReply::Integer(0))
}

fn handle_pubsub(args: &[Bytes]) -> Result<RespReply, RustyAntError> {
    let sub = args.first().ok_or_else(|| RustyAntError::WrongArity { command: "PUBSUB".into() })?;
    let sub = arg_as_str(sub)?.to_ascii_uppercase();
    match sub.as_str() {
        "CHANNELS" => {
            // Optional pattern argument; ignored because there are no
            // channels to filter.
            if args.len() > 2 {
                return Err(RustyAntError::WrongArity { command: "PUBSUB CHANNELS".into() });
            }
            Ok(RespReply::Array(Vec::new()))
        }
        "NUMSUB" => {
            // Redis returns `(channel, count)` pairs in the order requested.
            // All counts are 0 here.
            let pairs = args[1..]
                .iter()
                .flat_map(|name| [RespReply::BulkString(Some(Bytes::copy_from_slice(name))), RespReply::Integer(0)])
                .collect();
            Ok(RespReply::Array(pairs))
        }
        "NUMPAT" => {
            if args.len() != 1 {
                return Err(RustyAntError::WrongArity { command: "PUBSUB NUMPAT".into() });
            }
            Ok(RespReply::Integer(0))
        }
        other => Err(RustyAntError::Parse(format!("unsupported PUBSUB subcommand: {other}"))),
    }
}

// ---------------------------------------------------------------------------
// GEOADD / GEOPOS / GEODIST / GEOHASH — geo surface layered on ZSET scores.
//
// Each geo member is stored as a ZSET entry whose score is a 52-bit
// interleaved geohash integer (see `geo::encode_score`). GEOADD is ZADD with
// computed scores plus NX/XX/CH semantics; the other commands read the score
// and decode it. Only the Redis 7+ surface — deprecated GEORADIUS* is not
// implemented and won't be added.
// ---------------------------------------------------------------------------

async fn handle_geoadd(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    if args.len() < 4 {
        return Err(RustyAntError::WrongArity { command: "GEOADD".into() });
    }
    let key = arg_as_string(&args[0])?;
    let mut flags = ZAddFlags::default();
    let mut idx = 1;
    while idx < args.len() {
        let token = arg_as_str(&args[idx])?;
        match token.to_ascii_uppercase().as_str() {
            "NX" => {
                flags.nx = true;
                idx += 1;
            }
            "XX" => {
                flags.xx = true;
                idx += 1;
            }
            "CH" => {
                flags.ch = true;
                idx += 1;
            }
            _ => break,
        }
    }
    if flags.nx && flags.xx {
        return Err(RustyAntError::Parse("XX and NX options at the same time are not compatible".into()));
    }
    // Remaining args must be triples: lon lat member [lon lat member ...]
    let remaining = args.len() - idx;
    if remaining < 3 || remaining % 3 != 0 {
        return Err(RustyAntError::WrongArity { command: "GEOADD".into() });
    }
    let mut pairs: Vec<(f64, String)> = Vec::with_capacity(remaining / 3);
    while idx < args.len() {
        let lon = parse_f64(&args[idx], "longitude")?;
        let lat = parse_f64(&args[idx + 1], "latitude")?;
        geo::validate_lon_lat(lon, lat)?;
        let member = arg_as_string(&args[idx + 2])?;
        #[allow(clippy::cast_precision_loss)]
        let score = geo::encode_score(lon, lat) as f64;
        pairs.push((score, member));
        idx += 3;
    }
    let count = state.storage.zadd_ext(&key, pairs, flags).await?;
    Ok(RespReply::Integer(count))
}

async fn handle_geopos(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    if args.len() < 2 {
        return Err(RustyAntError::WrongArity { command: "GEOPOS".into() });
    }
    let key = arg_as_str(&args[0])?;
    let members: Vec<String> = args.iter().skip(1).map(arg_as_string).collect::<Result<_, _>>()?;
    let scores = state.storage.zmscore(key, &members).await?;
    let replies = scores
        .into_iter()
        .map(|s| {
            s.map_or(RespReply::Nil, |score| {
                let (lon, lat) = geo::decode_score(geo::score_to_u64(score));
                RespReply::Array(vec![
                    RespReply::BulkString(Some(Bytes::from(format_lon_lat(lon)))),
                    RespReply::BulkString(Some(Bytes::from(format_lon_lat(lat)))),
                ])
            })
        })
        .collect();
    Ok(RespReply::Array(replies))
}

async fn handle_geodist(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    if !(3..=4).contains(&args.len()) {
        return Err(RustyAntError::WrongArity { command: "GEODIST".into() });
    }
    let key = arg_as_str(&args[0])?;
    let m1 = arg_as_string(&args[1])?;
    let m2 = arg_as_string(&args[2])?;
    let unit = args.get(3).map_or(Ok(GeoUnit::Meters), |b| arg_as_str(b).and_then(GeoUnit::parse))?;
    let scores = state.storage.zmscore(key, &[m1, m2]).await?;
    let (Some(s1), Some(s2)) = (scores[0], scores[1]) else {
        return Ok(RespReply::Nil);
    };
    let (lon1, lat1) = geo::decode_score(geo::score_to_u64(s1));
    let (lon2, lat2) = geo::decode_score(geo::score_to_u64(s2));
    let meters = geo::haversine_meters(lon1, lat1, lon2, lat2);
    let converted = meters / unit.to_meters();
    // Redis formats GEODIST replies with four decimal places.
    Ok(RespReply::BulkString(Some(Bytes::from(format!("{converted:.4}").into_bytes()))))
}

async fn handle_geohash(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    if args.len() < 2 {
        return Err(RustyAntError::WrongArity { command: "GEOHASH".into() });
    }
    let key = arg_as_str(&args[0])?;
    let members: Vec<String> = args.iter().skip(1).map(arg_as_string).collect::<Result<_, _>>()?;
    let scores = state.storage.zmscore(key, &members).await?;
    let replies = scores
        .into_iter()
        .map(|s| {
            s.map_or(RespReply::Nil, |score| {
                RespReply::BulkString(Some(Bytes::from(geo::geohash_string(score).into_bytes())))
            })
        })
        .collect();
    Ok(RespReply::Array(replies))
}

/// Redis formats `GEOPOS` coordinates as `%.17Lf` (17 decimal places). The
/// round-trip through a 26-bit cell means the tail digits reflect the cell
/// centre; clients that care stringify them back into doubles anyway.
fn format_lon_lat(v: f64) -> Vec<u8> {
    format!("{v:.17}").into_bytes()
}

// ---------------------------------------------------------------------------
// GEOSEARCH / GEOSEARCHSTORE — spatial search over a geo-populated ZSET.
//
// The implementation is a linear scan: load every `(member, score)` pair,
// decode to `(lon, lat)`, filter by the requested circle or box, then
// optionally sort by distance and limit. Redis accelerates this with a
// geohash prefix walk; rustyant loads the whole ZSET in one S3 object per
// request regardless, so the walk wouldn't change the cost profile and
// would add significant code.
// ---------------------------------------------------------------------------

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum GeoSort {
    Asc,
    Desc,
}

#[derive(Debug, Clone)]
enum GeoCentre {
    Member(String),
    LonLat(f64, f64),
}

#[derive(Debug, Copy, Clone)]
enum GeoShape {
    Radius(f64),            // metres
    Box { w: f64, h: f64 }, // metres
}

#[derive(Debug, Clone)]
#[allow(clippy::struct_excessive_bools)] // mirrors Redis's flag layout
struct GeoSearchOpts {
    centre: GeoCentre,
    shape: GeoShape,
    unit: GeoUnit,
    sort: Option<GeoSort>,
    count: Option<(usize, bool)>, // (count, any)
    with_coord: bool,
    with_dist: bool,
    with_hash: bool,
    store_dist: bool, // GEOSEARCHSTORE only
}

#[derive(Debug, Clone)]
struct GeoMatch {
    member: String,
    score: f64,
    lon: f64,
    lat: f64,
    distance_m: f64,
}

async fn handle_geosearch(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    // Minimum: key + FROMMEMBER m + BYRADIUS n unit = 6 tokens.
    if args.len() < 6 {
        return Err(RustyAntError::WrongArity { command: "GEOSEARCH".into() });
    }
    let key = arg_as_string(&args[0])?;
    let opts = parse_geosearch_opts(&args[1..], false)?;
    let matches = run_geo_search(state, &key, &opts).await?;
    Ok(format_geosearch_reply(&matches, &opts))
}

async fn handle_geosearchstore(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    // Minimum: dst + src + FROMMEMBER m + BYRADIUS n unit = 7 tokens.
    if args.len() < 7 {
        return Err(RustyAntError::WrongArity { command: "GEOSEARCHSTORE".into() });
    }
    let destination = arg_as_string(&args[0])?;
    let source = arg_as_string(&args[1])?;
    let opts = parse_geosearch_opts(&args[2..], true)?;
    let matches = run_geo_search(state, &source, &opts).await?;
    // Redis overwrites the destination unconditionally (no WRONGTYPE check).
    state.storage.delete(&destination).await?;
    if matches.is_empty() {
        return Ok(RespReply::Integer(0));
    }
    let pairs: Vec<(f64, String)> = matches
        .iter()
        .map(|m| {
            let score = if opts.store_dist { m.distance_m / opts.unit.to_meters() } else { m.score };
            (score, m.member.clone())
        })
        .collect();
    let _ = state.storage.zadd(&destination, pairs).await?;
    Ok(RespReply::Integer(i64::try_from(matches.len()).unwrap_or(i64::MAX)))
}

fn parse_geosearch_opts(args: &[Bytes], is_store: bool) -> Result<GeoSearchOpts, RustyAntError> {
    let mut centre: Option<GeoCentre> = None;
    let mut shape: Option<(GeoShape, GeoUnit)> = None;
    let mut sort: Option<GeoSort> = None;
    let mut count: Option<(usize, bool)> = None;
    let mut with_coord = false;
    let mut with_dist = false;
    let mut with_hash = false;
    let mut store_dist = false;

    let mut i = 0;
    while i < args.len() {
        let token = arg_as_str(&args[i])?.to_ascii_uppercase();
        match token.as_str() {
            "FROMMEMBER" => {
                if i + 1 >= args.len() {
                    return Err(RustyAntError::Parse("FROMMEMBER requires a member".into()));
                }
                centre = Some(GeoCentre::Member(arg_as_string(&args[i + 1])?));
                i += 2;
            }
            "FROMLONLAT" => {
                if i + 2 >= args.len() {
                    return Err(RustyAntError::Parse("FROMLONLAT requires lon and lat".into()));
                }
                let lon = parse_f64(&args[i + 1], "longitude")?;
                let lat = parse_f64(&args[i + 2], "latitude")?;
                geo::validate_lon_lat(lon, lat)?;
                centre = Some(GeoCentre::LonLat(lon, lat));
                i += 3;
            }
            "BYRADIUS" => {
                if i + 2 >= args.len() {
                    return Err(RustyAntError::Parse("BYRADIUS requires radius and unit".into()));
                }
                let radius = parse_f64(&args[i + 1], "radius")?;
                if !radius.is_finite() || radius < 0.0 {
                    return Err(RustyAntError::Parse("radius must be a non-negative number".into()));
                }
                let unit = GeoUnit::parse(arg_as_str(&args[i + 2])?)?;
                shape = Some((GeoShape::Radius(radius * unit.to_meters()), unit));
                i += 3;
            }
            "BYBOX" => {
                if i + 3 >= args.len() {
                    return Err(RustyAntError::Parse("BYBOX requires width, height and unit".into()));
                }
                let w = parse_f64(&args[i + 1], "width")?;
                let h = parse_f64(&args[i + 2], "height")?;
                if !w.is_finite() || !h.is_finite() || w < 0.0 || h < 0.0 {
                    return Err(RustyAntError::Parse("BYBOX width and height must be non-negative".into()));
                }
                let unit = GeoUnit::parse(arg_as_str(&args[i + 3])?)?;
                let m = unit.to_meters();
                shape = Some((GeoShape::Box { w: w * m, h: h * m }, unit));
                i += 4;
            }
            "ASC" => {
                sort = Some(GeoSort::Asc);
                i += 1;
            }
            "DESC" => {
                sort = Some(GeoSort::Desc);
                i += 1;
            }
            "COUNT" => {
                if i + 1 >= args.len() {
                    return Err(RustyAntError::Parse("COUNT requires a value".into()));
                }
                let n = parse_i64(&args[i + 1], "COUNT")?;
                if n <= 0 {
                    return Err(RustyAntError::Parse("COUNT must be positive".into()));
                }
                let count_val = usize::try_from(n).unwrap_or(usize::MAX);
                // Optional trailing ANY keyword.
                let any = i + 2 < args.len() && arg_as_str(&args[i + 2]).is_ok_and(|s| s.eq_ignore_ascii_case("ANY"));
                count = Some((count_val, any));
                i += if any { 3 } else { 2 };
            }
            "WITHCOORD" if !is_store => {
                with_coord = true;
                i += 1;
            }
            "WITHDIST" if !is_store => {
                with_dist = true;
                i += 1;
            }
            "WITHHASH" if !is_store => {
                with_hash = true;
                i += 1;
            }
            "STOREDIST" if is_store => {
                store_dist = true;
                i += 1;
            }
            other => return Err(RustyAntError::Parse(format!("unsupported GEOSEARCH option: {other}"))),
        }
    }

    let centre =
        centre.ok_or_else(|| RustyAntError::Parse("exactly one of FROMMEMBER | FROMLONLAT is required".into()))?;
    let (shape, unit) =
        shape.ok_or_else(|| RustyAntError::Parse("exactly one of BYRADIUS | BYBOX is required".into()))?;
    Ok(GeoSearchOpts { centre, shape, unit, sort, count, with_coord, with_dist, with_hash, store_dist })
}

async fn run_geo_search(state: &State, key: &str, opts: &GeoSearchOpts) -> Result<Vec<GeoMatch>, RustyAntError> {
    // Resolve the search centre. FROMMEMBER must exist in the source ZSET;
    // a missing member is an explicit error to match Redis.
    let (centre_lon, centre_lat) = match &opts.centre {
        GeoCentre::LonLat(lon, lat) => (*lon, *lat),
        GeoCentre::Member(m) => {
            let score = state.storage.zscore(key, m).await?;
            let score =
                score.ok_or_else(|| RustyAntError::Parse(format!("could not decode requested zset member '{m}'")))?;
            geo::decode_score(geo::score_to_u64(score))
        }
    };
    let items = state.storage.zitems(key).await?;
    let mut matches: Vec<GeoMatch> = Vec::with_capacity(items.len());
    for (member, score) in items {
        let (lon, lat) = geo::decode_score(geo::score_to_u64(score));
        let distance_m = match opts.shape {
            GeoShape::Radius(r) => {
                let d = geo::haversine_meters(centre_lon, centre_lat, lon, lat);
                if d > r {
                    continue;
                }
                d
            }
            GeoShape::Box { w, h } => match geo::point_in_box(centre_lon, centre_lat, w, h, lon, lat) {
                Some(d) => d,
                None => continue,
            },
        };
        matches.push(GeoMatch { member, score, lon, lat, distance_m });
    }

    // Sort (unless ANY mode with a COUNT limit, which may skip ordering
    // precision — rustyant still sorts for determinism; the ANY flag is
    // parsed and documented as advisory).
    match opts.sort {
        Some(GeoSort::Asc) => {
            matches.sort_by(|a, b| a.distance_m.partial_cmp(&b.distance_m).unwrap_or(std::cmp::Ordering::Equal));
        }
        Some(GeoSort::Desc) => {
            matches.sort_by(|a, b| b.distance_m.partial_cmp(&a.distance_m).unwrap_or(std::cmp::Ordering::Equal));
        }
        None => {}
    }
    if let Some((n, _any)) = opts.count {
        matches.truncate(n);
    }
    Ok(matches)
}

fn format_geosearch_reply(matches: &[GeoMatch], opts: &GeoSearchOpts) -> RespReply {
    let augmented = opts.with_coord || opts.with_dist || opts.with_hash;
    if !augmented {
        return RespReply::Array(
            matches.iter().map(|m| RespReply::BulkString(Some(Bytes::from(m.member.clone().into_bytes())))).collect(),
        );
    }
    let unit_div = opts.unit.to_meters();
    let entries = matches
        .iter()
        .map(|m| {
            let mut parts = Vec::with_capacity(4);
            parts.push(RespReply::BulkString(Some(Bytes::from(m.member.clone().into_bytes()))));
            if opts.with_dist {
                let d = m.distance_m / unit_div;
                parts.push(RespReply::BulkString(Some(Bytes::from(format!("{d:.4}").into_bytes()))));
            }
            if opts.with_hash {
                #[allow(clippy::cast_possible_wrap)]
                let hash_i = geo::score_to_u64(m.score) as i64;
                parts.push(RespReply::Integer(hash_i));
            }
            if opts.with_coord {
                parts.push(RespReply::Array(vec![
                    RespReply::BulkString(Some(Bytes::from(format_lon_lat(m.lon)))),
                    RespReply::BulkString(Some(Bytes::from(format_lon_lat(m.lat)))),
                ]));
            }
            RespReply::Array(parts)
        })
        .collect();
    RespReply::Array(entries)
}

// ---------------------------------------------------------------------------
// COMMAND — minimal server introspection for redis-py's discovery path.
// Scope is `COUNT` / `LIST` / `INFO`; `DOCS` and `GETKEYS` are out of scope
// because redis-py does not require them.
// ---------------------------------------------------------------------------

/// `(name, arity, flags, first_key, last_key, step)` — the classic Redis
/// 6-tuple shape that powers `COMMAND INFO`.
///
/// Arity is positive for exact argument count (including the command name)
/// and negative for "at least |n|". `first_key` / `last_key` / `step`
/// describe key extraction in RESP terms: `0` means no keys (server
/// commands); `-1` as `last_key` means "to the last argument".
type CommandMeta = (&'static str, i64, &'static [&'static str], i64, i64, i64);

const COMMAND_META: &[CommandMeta] = &[
    // Connection / server
    ("ping", -1, &["fast"], 0, 0, 0),
    ("echo", 2, &["fast"], 0, 0, 0),
    ("time", 1, &["fast", "loading", "stale"], 0, 0, 0),
    ("info", -1, &["loading", "stale"], 0, 0, 0),
    ("command", -1, &["loading", "stale"], 0, 0, 0),
    ("hello", -1, &["fast", "loading", "stale"], 0, 0, 0),
    ("client", -2, &["admin", "noscript"], 0, 0, 0),
    ("reset", 1, &["fast", "loading", "stale"], 0, 0, 0),
    ("auth", -2, &["fast", "loading", "stale"], 0, 0, 0),
    ("wait", 3, &["admin"], 0, 0, 0),
    ("save", 1, &["admin", "noscript"], 0, 0, 0),
    ("bgsave", -1, &["admin"], 0, 0, 0),
    ("bgrewriteaof", 1, &["admin"], 0, 0, 0),
    ("lastsave", 1, &["readonly", "fast", "admin"], 0, 0, 0),
    ("latency", -2, &["admin", "noscript", "loading", "stale"], 0, 0, 0),
    ("debug", -2, &["admin", "noscript"], 0, 0, 0),
    // Transaction surface — stubs that error (MULTI/EXEC/DISCARD/WATCH) or
    // no-op quietly (UNWATCH); see `handle_multi` et al. for rationale.
    ("multi", 1, &["noscript", "loading", "stale", "fast"], 0, 0, 0),
    ("exec", 1, &["noscript", "loading", "stale", "skip_monitor", "skip_slowlog"], 0, 0, 0),
    ("discard", 1, &["noscript", "loading", "stale", "fast"], 0, 0, 0),
    ("watch", -2, &["noscript", "loading", "stale", "fast"], 1, -1, 1),
    ("unwatch", 1, &["noscript", "loading", "stale", "fast"], 0, 0, 0),
    // Pub/sub surface — SUBSCRIBE / PSUBSCRIBE / UNSUBSCRIBE / PUNSUBSCRIBE
    // error explicitly; PUBLISH and PUBSUB report honest zeros/empties.
    ("subscribe", -2, &["pubsub", "loading", "stale", "fast"], 0, 0, 0),
    ("psubscribe", -2, &["pubsub", "loading", "stale", "fast"], 0, 0, 0),
    ("unsubscribe", -1, &["pubsub", "loading", "stale", "fast"], 0, 0, 0),
    ("punsubscribe", -1, &["pubsub", "loading", "stale", "fast"], 0, 0, 0),
    ("publish", 3, &["pubsub", "loading", "stale", "fast"], 0, 0, 0),
    ("pubsub", -2, &["pubsub", "admin", "loading", "stale"], 0, 0, 0),
    // Geo commands — layered on ZSET scores. Core 4 (Redis 7+); deprecated
    // GEORADIUS* family is intentionally not surfaced.
    ("geoadd", -5, &["write", "denyoom"], 1, 1, 1),
    ("geopos", -3, &["readonly"], 1, 1, 1),
    ("geodist", -4, &["readonly"], 1, 1, 1),
    ("geohash", -3, &["readonly"], 1, 1, 1),
    ("geosearch", -7, &["readonly"], 1, 1, 1),
    ("geosearchstore", -8, &["write", "denyoom"], 1, 2, 1),
    ("dbsize", 1, &["readonly", "fast"], 0, 0, 0),
    ("flushdb", -1, &["write"], 0, 0, 0),
    ("flushall", -1, &["write"], 0, 0, 0),
    ("randomkey", 1, &["readonly"], 0, 0, 0),
    // Generic / keyspace
    ("del", -2, &["write"], 1, -1, 1),
    ("unlink", -2, &["write", "fast"], 1, -1, 1),
    ("copy", -3, &["write"], 1, 2, 1),
    ("exists", -2, &["readonly", "fast"], 1, -1, 1),
    ("expire", -3, &["write", "fast"], 1, 1, 1),
    ("expireat", -3, &["write", "fast"], 1, 1, 1),
    ("pexpire", -3, &["write", "fast"], 1, 1, 1),
    ("pexpireat", -3, &["write", "fast"], 1, 1, 1),
    ("persist", 2, &["write", "fast"], 1, 1, 1),
    ("ttl", 2, &["readonly", "fast"], 1, 1, 1),
    ("pttl", 2, &["readonly", "fast"], 1, 1, 1),
    ("expiretime", 2, &["readonly", "fast"], 1, 1, 1),
    ("pexpiretime", 2, &["readonly", "fast"], 1, 1, 1),
    ("keys", 2, &["readonly"], 0, 0, 0),
    ("scan", -2, &["readonly"], 0, 0, 0),
    ("type", 2, &["readonly", "fast"], 1, 1, 1),
    ("rename", 3, &["write"], 1, 2, 1),
    ("renamenx", 3, &["write", "fast"], 1, 2, 1),
    // Strings
    ("get", 2, &["readonly", "fast"], 1, 1, 1),
    ("getex", -2, &["write", "fast"], 1, 1, 1),
    ("getset", 3, &["write", "fast"], 1, 1, 1),
    ("getdel", 2, &["write", "fast"], 1, 1, 1),
    ("getrange", 4, &["readonly"], 1, 1, 1),
    ("setrange", 4, &["write", "denyoom"], 1, 1, 1),
    ("strlen", 2, &["readonly", "fast"], 1, 1, 1),
    ("append", 3, &["write", "denyoom", "fast"], 1, 1, 1),
    ("set", -3, &["write", "denyoom"], 1, 1, 1),
    ("setnx", 3, &["write", "denyoom", "fast"], 1, 1, 1),
    ("setex", 4, &["write", "denyoom"], 1, 1, 1),
    ("mget", -2, &["readonly", "fast"], 1, -1, 1),
    ("mset", -3, &["write", "denyoom"], 1, -1, 2),
    ("msetnx", -3, &["write", "denyoom"], 1, -1, 2),
    ("incr", 2, &["write", "denyoom", "fast"], 1, 1, 1),
    ("incrby", 3, &["write", "denyoom", "fast"], 1, 1, 1),
    ("incrbyfloat", 3, &["write", "denyoom", "fast"], 1, 1, 1),
    ("decr", 2, &["write", "denyoom", "fast"], 1, 1, 1),
    ("decrby", 3, &["write", "denyoom", "fast"], 1, 1, 1),
    // Bit ops
    ("getbit", 3, &["readonly", "fast"], 1, 1, 1),
    ("setbit", 4, &["write", "denyoom"], 1, 1, 1),
    ("bitcount", -2, &["readonly"], 1, 1, 1),
    ("bitpos", -3, &["readonly"], 1, 1, 1),
    ("bitop", -4, &["write", "denyoom"], 2, -1, 1),
    // Hashes
    ("hset", -4, &["write", "denyoom", "fast"], 1, 1, 1),
    ("hsetnx", 4, &["write", "denyoom", "fast"], 1, 1, 1),
    ("hget", 3, &["readonly", "fast"], 1, 1, 1),
    ("hdel", -3, &["write", "fast"], 1, 1, 1),
    ("hexists", 3, &["readonly", "fast"], 1, 1, 1),
    ("hgetall", 2, &["readonly"], 1, 1, 1),
    ("hincrby", 4, &["write", "denyoom", "fast"], 1, 1, 1),
    ("hkeys", 2, &["readonly"], 1, 1, 1),
    ("hlen", 2, &["readonly", "fast"], 1, 1, 1),
    ("hmget", -3, &["readonly", "fast"], 1, 1, 1),
    ("hscan", -3, &["readonly"], 1, 1, 1),
    ("hstrlen", 3, &["readonly", "fast"], 1, 1, 1),
    ("hvals", 2, &["readonly"], 1, 1, 1),
    // Lists
    ("lpush", -3, &["write", "denyoom", "fast"], 1, 1, 1),
    ("lpushx", -3, &["write", "denyoom", "fast"], 1, 1, 1),
    ("rpush", -3, &["write", "denyoom", "fast"], 1, 1, 1),
    ("rpushx", -3, &["write", "denyoom", "fast"], 1, 1, 1),
    ("lpop", -2, &["write", "fast"], 1, 1, 1),
    ("rpop", -2, &["write", "fast"], 1, 1, 1),
    ("lrange", 4, &["readonly"], 1, 1, 1),
    ("lindex", 3, &["readonly"], 1, 1, 1),
    ("llen", 2, &["readonly", "fast"], 1, 1, 1),
    ("lset", 4, &["write", "denyoom"], 1, 1, 1),
    ("ltrim", 4, &["write"], 1, 1, 1),
    ("linsert", 5, &["write", "denyoom"], 1, 1, 1),
    ("lrem", 4, &["write"], 1, 1, 1),
    ("lmove", 5, &["write"], 1, 2, 1),
    ("rpoplpush", 3, &["write", "denyoom"], 1, 2, 1),
    ("lpos", -3, &["readonly"], 1, 1, 1),
    // Sets
    ("sadd", -3, &["write", "denyoom", "fast"], 1, 1, 1),
    ("srem", -3, &["write", "fast"], 1, 1, 1),
    ("scard", 2, &["readonly", "fast"], 1, 1, 1),
    ("sismember", 3, &["readonly", "fast"], 1, 1, 1),
    ("smismember", -3, &["readonly", "fast"], 1, 1, 1),
    ("smembers", 2, &["readonly"], 1, 1, 1),
    ("srandmember", -2, &["readonly"], 1, 1, 1),
    ("spop", -2, &["write", "fast"], 1, 1, 1),
    ("sinter", -2, &["readonly"], 1, -1, 1),
    ("sunion", -2, &["readonly"], 1, -1, 1),
    ("sdiff", -2, &["readonly"], 1, -1, 1),
    ("sinterstore", -3, &["write", "denyoom"], 1, -1, 1),
    ("sunionstore", -3, &["write", "denyoom"], 1, -1, 1),
    ("sdiffstore", -3, &["write", "denyoom"], 1, -1, 1),
    ("sscan", -3, &["readonly"], 1, 1, 1),
    // Sorted sets
    ("zadd", -4, &["write", "denyoom", "fast"], 1, 1, 1),
    ("zrem", -3, &["write", "fast"], 1, 1, 1),
    ("zcard", 2, &["readonly", "fast"], 1, 1, 1),
    ("zcount", 4, &["readonly", "fast"], 1, 1, 1),
    ("zrange", -4, &["readonly"], 1, 1, 1),
    ("zrevrange", -4, &["readonly"], 1, 1, 1),
    ("zrangebyscore", -4, &["readonly"], 1, 1, 1),
    ("zrevrangebyscore", -4, &["readonly"], 1, 1, 1),
    ("zscore", 3, &["readonly", "fast"], 1, 1, 1),
    ("zmscore", -3, &["readonly", "fast"], 1, 1, 1),
    ("zrank", -3, &["readonly", "fast"], 1, 1, 1),
    ("zrevrank", -3, &["readonly", "fast"], 1, 1, 1),
    ("zincrby", 4, &["write", "denyoom", "fast"], 1, 1, 1),
    ("zpopmin", -2, &["write", "fast"], 1, 1, 1),
    ("zpopmax", -2, &["write", "fast"], 1, 1, 1),
    ("zremrangebyrank", 4, &["write"], 1, 1, 1),
    ("zremrangebyscore", 4, &["write"], 1, 1, 1),
    // *STORE aggregates. Redis's first_key/last_key/step describe the
    // destination-key-then-source-keys layout; rustyant reuses delete+zadd
    // under the hood so the layout is purely informational here.
    ("zinterstore", -4, &["write", "denyoom"], 1, 1, 1),
    ("zunionstore", -4, &["write", "denyoom"], 1, 1, 1),
    ("zdiffstore", -4, &["write", "denyoom"], 1, 1, 1),
    ("zscan", -3, &["readonly"], 1, 1, 1),
];

fn handle_command(args: &[Bytes]) -> Result<RespReply, RustyAntError> {
    if args.is_empty() {
        // Plain `COMMAND` returns metadata for every known command — same as
        // `COMMAND INFO` with no filter. redis-cli relies on this.
        return Ok(RespReply::Array(COMMAND_META.iter().map(command_meta_reply).collect()));
    }
    let sub = arg_as_str(&args[0])?.to_ascii_uppercase();
    match sub.as_str() {
        "COUNT" => {
            if args.len() != 1 {
                return Err(RustyAntError::WrongArity { command: "COMMAND COUNT".into() });
            }
            Ok(RespReply::Integer(i64::try_from(COMMAND_META.len()).unwrap_or(i64::MAX)))
        }
        "LIST" => {
            if args.len() != 1 {
                return Err(RustyAntError::WrongArity { command: "COMMAND LIST".into() });
            }
            Ok(RespReply::Array(
                COMMAND_META
                    .iter()
                    .map(|(name, ..)| RespReply::BulkString(Some(Bytes::copy_from_slice(name.as_bytes()))))
                    .collect(),
            ))
        }
        "INFO" => {
            // `COMMAND INFO` with no filter → all; otherwise filter by name
            // (case-insensitive). Unknown names return Nil at the matching
            // array position, matching Redis.
            if args.len() == 1 {
                return Ok(RespReply::Array(COMMAND_META.iter().map(command_meta_reply).collect()));
            }
            let mut out = Vec::with_capacity(args.len() - 1);
            for want in args.iter().skip(1) {
                let want_str = arg_as_str(want)?.to_ascii_lowercase();
                match COMMAND_META.iter().find(|(name, ..)| *name == want_str) {
                    Some(meta) => out.push(command_meta_reply(meta)),
                    None => out.push(RespReply::Nil),
                }
            }
            Ok(RespReply::Array(out))
        }
        other => Err(RustyAntError::Parse(format!("unsupported COMMAND subcommand: {other}"))),
    }
}

fn command_meta_reply(meta: &CommandMeta) -> RespReply {
    let (name, arity, flags, first_key, last_key, step) = *meta;
    RespReply::Array(vec![
        RespReply::BulkString(Some(Bytes::copy_from_slice(name.as_bytes()))),
        RespReply::Integer(arity),
        RespReply::Array(
            flags.iter().map(|f| RespReply::BulkString(Some(Bytes::copy_from_slice(f.as_bytes())))).collect(),
        ),
        RespReply::Integer(first_key),
        RespReply::Integer(last_key),
        RespReply::Integer(step),
    ])
}
