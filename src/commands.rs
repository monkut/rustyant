use std::time::Instant;

use bytes::Bytes;
use tracing::info;

use crate::error::RustyAntError;
use crate::metrics;
use crate::resp::RespReply;
use crate::state::State;
use crate::storage::{ScoreBound, TtlResult, now_ms};

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

#[allow(clippy::large_stack_frames)]
async fn run(state: &State, tokens: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    let mut iter = tokens.into_iter();
    let cmd_bytes = iter.next().ok_or_else(|| RustyAntError::RespParse("empty command array".into()))?;
    let cmd = std::str::from_utf8(&cmd_bytes)
        .map_err(|_| RustyAntError::RespParse("command not utf8".into()))?
        .to_ascii_uppercase();
    let args: Vec<Bytes> = iter.collect();

    match cmd.as_str() {
        "PING" => Ok(RespReply::SimpleString("PONG".into())),
        // Strings
        "GET" => handle_get(state, args).await,
        "GETSET" => handle_getset(state, args).await,
        "SET" => handle_set(state, args).await,
        "SETNX" => handle_setnx(state, args).await,
        "SETEX" => handle_setex(state, args).await,
        "MGET" => handle_mget(state, args).await,
        "MSET" => handle_mset(state, args).await,
        "DEL" => handle_del(state, args).await,
        "EXISTS" => handle_exists(state, args).await,
        "EXPIRE" => handle_expire(state, args).await,
        "EXPIREAT" => handle_expireat(state, args).await,
        "PEXPIREAT" => handle_pexpireat(state, args).await,
        "PERSIST" => handle_persist(state, args).await,
        "TTL" => handle_ttl(state, args).await,
        "KEYS" => handle_keys(state, args).await,
        "SCAN" => handle_scan(state, args).await,
        "TYPE" => handle_type(state, args).await,
        "INCR" => handle_incrby(state, args, 1).await,
        "INCRBY" => {
            let delta = parse_delta(&args)?;
            handle_incrby(state, args, delta).await
        }
        "DECR" => handle_incrby(state, args, -1).await,
        "DECRBY" => {
            let delta = parse_delta(&args)?;
            let neg = delta.checked_neg().ok_or_else(|| RustyAntError::Parse("decrement overflow".into()))?;
            handle_incrby(state, args, neg).await
        }
        // Hashes
        "HSET" => handle_hset(state, args).await,
        "HGET" => handle_hget(state, args).await,
        "HDEL" => handle_hdel(state, args).await,
        "HGETALL" => handle_hgetall(state, args).await,
        "HLEN" => handle_hlen(state, args).await,
        "HKEYS" => handle_hkeys(state, args).await,
        "HVALS" => handle_hvals(state, args).await,
        "HEXISTS" => handle_hexists(state, args).await,
        "HMGET" => handle_hmget(state, args).await,
        "HINCRBY" => handle_hincrby(state, args).await,
        // Lists
        "LPUSH" => handle_push(state, args, true).await,
        "RPUSH" => handle_push(state, args, false).await,
        "LPOP" => handle_pop(state, args, true).await,
        "RPOP" => handle_pop(state, args, false).await,
        "LRANGE" => handle_lrange(state, args).await,
        "LLEN" => handle_llen(state, args).await,
        "LINDEX" => handle_lindex(state, args).await,
        "LSET" => handle_lset(state, args).await,
        "LREM" => handle_lrem(state, args).await,
        // Sets
        "SADD" => handle_sadd(state, args).await,
        "SREM" => handle_srem(state, args).await,
        "SMEMBERS" => handle_smembers(state, args).await,
        "SISMEMBER" => handle_sismember(state, args).await,
        "SCARD" => handle_scard(state, args).await,
        // Sorted sets
        "ZADD" => handle_zadd(state, args).await,
        "ZREM" => handle_zrem(state, args).await,
        "ZINCRBY" => handle_zincrby(state, args).await,
        "ZRANGE" => handle_zrange(state, args).await,
        "ZRANGEBYSCORE" => handle_zrangebyscore(state, args).await,
        "ZSCORE" => handle_zscore(state, args).await,
        "ZCARD" => handle_zcard(state, args).await,
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

// ---- String multi-key + NX/EX + GETSET + PERSIST --------------------------

async fn handle_getset(state: &State, args: Vec<Bytes>) -> Result<RespReply, RustyAntError> {
    arity("GETSET", args.len() == 2)?;
    let key = arg_as_str(&args[0])?;
    let old = state.storage.getset(key, args[1].clone()).await?;
    Ok(old.map_or(RespReply::Nil, |v| RespReply::BulkString(Some(v))))
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

    let mut pattern: Option<String> = None;
    let mut count: usize = 10; // Redis default
    let mut i = 1;
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
                return Err(RustyAntError::Parse(format!("unsupported SCAN option: {other}")));
            }
        }
    }

    let (keys, next) = state.storage.scan(cursor.as_deref(), pattern.as_deref(), count).await?;
    let cursor_out = next.unwrap_or_else(|| "0".to_string());
    Ok(RespReply::Array(vec![
        RespReply::BulkString(Some(Bytes::from(cursor_out.into_bytes()))),
        RespReply::Array(keys.into_iter().map(|k| RespReply::BulkString(Some(Bytes::from(k.into_bytes())))).collect()),
    ]))
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
