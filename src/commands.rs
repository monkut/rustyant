use bytes::Bytes;

use crate::error::RustyAntError;
use crate::resp::RespReply;
use crate::state::State;
use crate::storage::{TtlResult, now_ms};

pub async fn dispatch(state: &State, command_tokens: Vec<Bytes>) -> RespReply {
    match run(state, command_tokens).await {
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
        "SET" => handle_set(state, args).await,
        "DEL" => handle_del(state, args).await,
        "EXISTS" => handle_exists(state, args).await,
        "EXPIRE" => handle_expire(state, args).await,
        "TTL" => handle_ttl(state, args).await,
        "INCR" => handle_incrby(state, args, 1).await,
        "INCRBY" => {
            let delta = parse_delta(&args)?;
            handle_incrby(state, args, delta).await
        }
        // Hashes
        "HSET" => handle_hset(state, args).await,
        "HGET" => handle_hget(state, args).await,
        "HDEL" => handle_hdel(state, args).await,
        "HGETALL" => handle_hgetall(state, args).await,
        // Lists
        "LPUSH" => handle_push(state, args, true).await,
        "RPUSH" => handle_push(state, args, false).await,
        "LPOP" => handle_pop(state, args, true).await,
        "RPOP" => handle_pop(state, args, false).await,
        "LRANGE" => handle_lrange(state, args).await,
        // Sets
        "SADD" => handle_sadd(state, args).await,
        // Sorted sets
        "ZADD" => handle_zadd(state, args).await,
        "ZRANGE" => handle_zrange(state, args).await,
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
