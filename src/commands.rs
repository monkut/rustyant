use std::time::{Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use tracing::info;

use crate::error::RustyAntError;
use crate::metrics;
use crate::resp::RespReply;
use crate::state::State;
use crate::storage::{ScoreBound, TtlResult, bit_at, now_ms};

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
        "SPOP" => handle_spop(state, args).await,
        "SRANDMEMBER" => handle_srandmember(state, args).await,
        "SSCAN" => handle_sscan(state, args).await,
        // Sorted sets
        "ZADD" => handle_zadd(state, args).await,
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
    if args.len() < 3 || (args.len() - 1) % 2 != 0 {
        return Err(RustyAntError::WrongArity { command: "ZADD".into() });
    }
    let key = arg_as_string(&args[0])?;
    let mut pairs: Vec<(f64, String)> = Vec::with_capacity((args.len() - 1) / 2);
    let mut i = 1;
    while i + 1 < args.len() {
        let score = parse_f64(&args[i], "score")?;
        let member = arg_as_string(&args[i + 1])?;
        pairs.push((score, member));
        i += 2;
    }
    let added = state.storage.zadd(&key, pairs).await?;
    Ok(RespReply::Integer(added))
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
