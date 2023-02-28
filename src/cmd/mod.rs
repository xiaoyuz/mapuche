mod get;

use futures::future::BoxFuture;
pub use get::Get;

mod publish;
pub use publish::Publish;

mod set;
pub use set::Set;

mod subscribe;
pub use subscribe::{Subscribe, Unsubscribe};

mod ping;
pub use ping::Ping;

mod unknown;
pub use unknown::Unknown;

mod mget;
pub use mget::Mget;

mod mset;
pub use mset::Mset;

mod strlen;
pub use strlen::Strlen;

mod cmdtype;
pub use cmdtype::Type;

mod exists;
pub use exists::Exists;

mod incrdecr;
pub use incrdecr::IncrDecr;

mod expire;
pub use expire::Expire;

mod ttl;
pub use ttl::TTL;

mod del;
pub use del::Del;

mod scan;
pub use scan::Scan;

mod sadd;
pub use sadd::Sadd;

mod scard;
pub use scard::Scard;

mod sismember;
pub use sismember::Sismember;

mod smismember;
pub use smismember::Smismember;

mod srandmember;
pub use srandmember::Srandmember;

mod smembers;
pub use smembers::Smembers;

mod srem;
pub use srem::Srem;

mod spop;
pub use spop::Spop;

mod push;
pub use push::Push;

mod pop;
pub use pop::Pop;

mod ltrim;
pub use ltrim::Ltrim;

mod lrange;
pub use lrange::Lrange;

mod llen;
pub use llen::Llen;

mod lindex;
pub use lindex::Lindex;

mod lset;
pub use lset::Lset;

mod linsert;
pub use linsert::Linsert;

mod lrem;
pub use lrem::Lrem;

mod hset;
pub use hset::Hset;

mod hget;
pub use hget::Hget;

mod hstrlen;
pub use hstrlen::Hstrlen;

mod hexists;
pub use hexists::Hexists;

mod hmget;
pub use hmget::Hmget;

mod hlen;
pub use hlen::Hlen;

mod hgetall;
pub use hgetall::Hgetall;

mod hkeys;
pub use hkeys::Hkeys;

mod hvals;
pub use hvals::Hvals;

mod hdel;
pub use hdel::Hdel;

mod hincrby;
pub use hincrby::Hincrby;

mod zadd;
pub use zadd::Zadd;

mod zcard;
pub use zcard::Zcard;

mod zscore;
pub use zscore::Zscore;

mod zcount;
pub use zcount::Zcount;

mod zrange;
pub use zrange::Zrange;

mod zrevrange;
pub use zrevrange::Zrevrange;

mod zrangebyscore;
pub use zrangebyscore::Zrangebyscore;

mod zpop;
pub use zpop::Zpop;

mod zrank;
pub use zrank::Zrank;

mod zincrby;
pub use zincrby::Zincrby;

mod zrem;
pub use zrem::Zrem;

mod zremrangebyrank;
pub use zremrangebyrank::Zremrangebyrank;

mod zremrangebyscore;
pub use zremrangebyscore::Zremrangebyscore;

mod keys;
pub use keys::Keys;

mod auth;
pub use auth::Auth;

use crate::config::txn_retry_count;
use crate::metrics::TXN_RETRY_COUNTER;
use crate::{Connection, Db, Frame, Parse, ParseError, Shutdown};

use crate::rocks::Result as RocksResult;

/// Enumeration of supported Redis commands.
///
/// Methods called on `Command` are delegated to the command implementation.
#[derive(Debug)]
pub enum Command {
    Get(Get),
    Mget(Mget),
    Mset(Mset),
    Publish(Publish),
    Set(Set),
    Subscribe(Subscribe),
    Unsubscribe(Unsubscribe),
    Del(Del),
    Ping(Ping),
    Strlen(Strlen),
    Type(Type),
    Exists(Exists),
    Incr(IncrDecr),
    Decr(IncrDecr),
    Expire(Expire),
    ExpireAt(Expire),
    Pexpire(Expire),
    PexpireAt(Expire),
    TTL(TTL),
    PTTL(TTL),
    Scan(Scan),
    Keys(Keys),

    // set
    Sadd(Sadd),
    Scard(Scard),
    Sismember(Sismember),
    Smismember(Smismember),
    Smembers(Smembers),
    Srandmember(Srandmember),
    Spop(Spop),
    Srem(Srem),

    // list
    Lpush(Push),
    Rpush(Push),
    Lpop(Pop),
    Rpop(Pop),
    Lrange(Lrange),
    Ltrim(Ltrim),
    Llen(Llen),
    Lindex(Lindex),
    Lset(Lset),
    Lrem(Lrem),
    Linsert(Linsert),

    // hash
    Hset(Hset),
    Hmset(Hset),
    Hsetnx(Hset),
    Hget(Hget),
    Hmget(Hmget),
    Hlen(Hlen),
    Hgetall(Hgetall),
    Hdel(Hdel),
    Hkeys(Hkeys),
    Hvals(Hvals),
    Hincrby(Hincrby),
    Hexists(Hexists),
    Hstrlen(Hstrlen),

    // sorted set
    Zadd(Zadd),
    Zcard(Zcard),
    Zscore(Zscore),
    Zrem(Zrem),
    Zremrangebyscore(Zremrangebyscore),
    Zremrangebyrank(Zremrangebyrank),
    Zrange(Zrange),
    Zrevrange(Zrevrange),
    Zrangebyscore(Zrangebyscore),
    Zrevrangebyscore(Zrangebyscore),
    Zcount(Zcount),
    Zpopmin(Zpop),
    Zpopmax(Zpop),
    Zrank(Zrank),
    Zincrby(Zincrby),

    Auth(Auth),

    Unknown(Unknown),
}

impl Command {
    /// Parse a command from a received frame.
    ///
    /// The `Frame` must represent a Redis command supported by `mapuche` and
    /// be the array variant.
    ///
    /// # Returns
    ///
    /// On success, the command value is returned, otherwise, `Err` is returned.
    pub fn from_frame(frame: Frame) -> crate::Result<Command> {
        // The frame  value is decorated with `Parse`. `Parse` provides a
        // "cursor" like API which makes parsing the command easier.
        //
        // The frame value must be an array variant. Any other frame variants
        // result in an error being returned.
        let mut parse = Parse::new(frame)?;

        // All redis commands begin with the command name as a string. The name
        // is read and converted to lower cases in order to do case sensitive
        // matching.
        let command_name = parse.next_string()?.to_lowercase();

        // Match the command name, delegating the rest of the parsing to the
        // specific command.
        let command = match &command_name[..] {
            "get" => Command::Get(Get::parse_frames(&mut parse)?),
            "mget" => Command::Mget(transform_parse(Mget::parse_frames(&mut parse), &mut parse)),
            "mset" => Command::Mset(transform_parse(Mset::parse_frames(&mut parse), &mut parse)),
            "publish" => Command::Publish(Publish::parse_frames(&mut parse)?),
            "set" => Command::Set(Set::parse_frames(&mut parse)?),
            "subscribe" => Command::Subscribe(Subscribe::parse_frames(&mut parse)?),
            "unsubscribe" => Command::Unsubscribe(Unsubscribe::parse_frames(&mut parse)?),
            "del" => Command::Del(transform_parse(Del::parse_frames(&mut parse), &mut parse)),
            "ping" => Command::Ping(Ping::parse_frames(&mut parse)?),
            "strlen" => Command::Strlen(transform_parse(
                Strlen::parse_frames(&mut parse),
                &mut parse,
            )),
            "type" => Command::Type(transform_parse(Type::parse_frames(&mut parse), &mut parse)),
            "exists" => Command::Exists(transform_parse(
                Exists::parse_frames(&mut parse),
                &mut parse,
            )),
            "incr" => Command::Incr(transform_parse(
                IncrDecr::parse_frames(&mut parse, true),
                &mut parse,
            )),
            "decr" => Command::Decr(transform_parse(
                IncrDecr::parse_frames(&mut parse, true),
                &mut parse,
            )),
            "expire" => Command::Expire(transform_parse(
                Expire::parse_frames(&mut parse),
                &mut parse,
            )),
            "expireat" => Command::ExpireAt(transform_parse(
                Expire::parse_frames(&mut parse),
                &mut parse,
            )),
            "pexpire" => Command::Pexpire(transform_parse(
                Expire::parse_frames(&mut parse),
                &mut parse,
            )),
            "pexpireat" => Command::PexpireAt(transform_parse(
                Expire::parse_frames(&mut parse),
                &mut parse,
            )),
            "ttl" => Command::TTL(transform_parse(TTL::parse_frames(&mut parse), &mut parse)),
            "pttl" => Command::PTTL(transform_parse(TTL::parse_frames(&mut parse), &mut parse)),
            "scan" => Command::Scan(transform_parse(Scan::parse_frames(&mut parse), &mut parse)),
            "keys" => Command::Keys(transform_parse(Keys::parse_frames(&mut parse), &mut parse)),
            "sadd" => Command::Sadd(transform_parse(Sadd::parse_frames(&mut parse), &mut parse)),
            "scard" => Command::Scard(transform_parse(Scard::parse_frames(&mut parse), &mut parse)),
            "sismember" => Command::Sismember(transform_parse(
                Sismember::parse_frames(&mut parse),
                &mut parse,
            )),
            "smismember" => Command::Smismember(transform_parse(
                Smismember::parse_frames(&mut parse),
                &mut parse,
            )),
            "smembers" => Command::Smembers(transform_parse(
                Smembers::parse_frames(&mut parse),
                &mut parse,
            )),
            "srandmember" => Command::Srandmember(transform_parse(
                Srandmember::parse_frames(&mut parse),
                &mut parse,
            )),
            "spop" => Command::Spop(transform_parse(Spop::parse_frames(&mut parse), &mut parse)),
            "srem" => Command::Srem(transform_parse(Srem::parse_frames(&mut parse), &mut parse)),
            "lpush" => Command::Lpush(transform_parse(Push::parse_frames(&mut parse), &mut parse)),
            "rpush" => Command::Rpush(transform_parse(Push::parse_frames(&mut parse), &mut parse)),
            "lpop" => Command::Lpop(transform_parse(Pop::parse_frames(&mut parse), &mut parse)),
            "rpop" => Command::Rpop(transform_parse(Pop::parse_frames(&mut parse), &mut parse)),
            "lrange" => Command::Lrange(transform_parse(
                Lrange::parse_frames(&mut parse),
                &mut parse,
            )),
            "ltrim" => Command::Ltrim(transform_parse(Ltrim::parse_frames(&mut parse), &mut parse)),
            "llen" => Command::Llen(transform_parse(Llen::parse_frames(&mut parse), &mut parse)),
            "lindex" => Command::Lindex(transform_parse(
                Lindex::parse_frames(&mut parse),
                &mut parse,
            )),
            "lset" => Command::Lset(transform_parse(Lset::parse_frames(&mut parse), &mut parse)),
            "lrem" => Command::Lrem(transform_parse(Lrem::parse_frames(&mut parse), &mut parse)),
            "linsert" => Command::Linsert(transform_parse(
                Linsert::parse_frames(&mut parse),
                &mut parse,
            )),
            "hset" => Command::Hset(transform_parse(Hset::parse_frames(&mut parse), &mut parse)),
            "hsetnx" => {
                Command::Hsetnx(transform_parse(Hset::parse_frames(&mut parse), &mut parse))
            }
            "hmset" => Command::Hmset(transform_parse(Hset::parse_frames(&mut parse), &mut parse)),
            "hget" => Command::Hget(transform_parse(Hget::parse_frames(&mut parse), &mut parse)),
            "hmget" => Command::Hmget(transform_parse(Hmget::parse_frames(&mut parse), &mut parse)),
            "hlen" => Command::Hlen(transform_parse(Hlen::parse_frames(&mut parse), &mut parse)),
            "hgetall" => Command::Hgetall(transform_parse(
                Hgetall::parse_frames(&mut parse),
                &mut parse,
            )),
            "hdel" => Command::Hdel(transform_parse(Hdel::parse_frames(&mut parse), &mut parse)),
            "hkeys" => Command::Hkeys(transform_parse(Hkeys::parse_frames(&mut parse), &mut parse)),
            "hvals" => Command::Hvals(transform_parse(Hvals::parse_frames(&mut parse), &mut parse)),
            "hincrby" => Command::Hincrby(transform_parse(
                Hincrby::parse_frames(&mut parse),
                &mut parse,
            )),
            "hexists" => Command::Hexists(transform_parse(
                Hexists::parse_frames(&mut parse),
                &mut parse,
            )),
            "hstrlen" => Command::Hstrlen(transform_parse(
                Hstrlen::parse_frames(&mut parse),
                &mut parse,
            )),
            "zadd" => Command::Zadd(transform_parse(Zadd::parse_frames(&mut parse), &mut parse)),
            "zcard" => Command::Zcard(transform_parse(Zcard::parse_frames(&mut parse), &mut parse)),
            "zscore" => Command::Zscore(transform_parse(
                Zscore::parse_frames(&mut parse),
                &mut parse,
            )),
            "zrem" => Command::Zrem(transform_parse(Zrem::parse_frames(&mut parse), &mut parse)),
            "zremrangebyscore" => Command::Zremrangebyscore(transform_parse(
                Zremrangebyscore::parse_frames(&mut parse),
                &mut parse,
            )),
            "zremrangebyrank" => Command::Zremrangebyrank(transform_parse(
                Zremrangebyrank::parse_frames(&mut parse),
                &mut parse,
            )),
            "zrange" => Command::Zrange(transform_parse(
                Zrange::parse_frames(&mut parse),
                &mut parse,
            )),
            "zrevrange" => Command::Zrevrange(transform_parse(
                Zrevrange::parse_frames(&mut parse),
                &mut parse,
            )),
            "zrangebyscore" => Command::Zrangebyscore(transform_parse(
                Zrangebyscore::parse_frames(&mut parse),
                &mut parse,
            )),
            "zrevrangebyscore" => Command::Zrevrangebyscore(transform_parse(
                Zrangebyscore::parse_frames(&mut parse),
                &mut parse,
            )),
            "zcount" => Command::Zcount(transform_parse(
                Zcount::parse_frames(&mut parse),
                &mut parse,
            )),
            "zpopmin" => {
                Command::Zpopmin(transform_parse(Zpop::parse_frames(&mut parse), &mut parse))
            }
            "zpopmax" => {
                Command::Zpopmax(transform_parse(Zpop::parse_frames(&mut parse), &mut parse))
            }
            "zrank" => Command::Zrank(transform_parse(Zrank::parse_frames(&mut parse), &mut parse)),
            "zincrby" => Command::Zincrby(transform_parse(
                Zincrby::parse_frames(&mut parse),
                &mut parse,
            )),
            "auth" => Command::Auth(transform_parse(Auth::parse_frames(&mut parse), &mut parse)),

            _ => {
                // The command is not recognized and an Unknown command is
                // returned.
                //
                // `return` is called here to skip the `finish()` call below. As
                // the command is not recognized, there is most likely
                // unconsumed fields remaining in the `Parse` instance.
                return Ok(Command::Unknown(Unknown::new(command_name)));
            }
        };

        // Check if there is any remaining unconsumed fields in the `Parse`
        // value. If fields remain, this indicates an unexpected frame format
        // and an error is returned.
        parse.finish()?;

        // The command has been successfully parsed
        Ok(command)
    }

    /// Apply the command to the specified `Db` instance.
    ///
    /// The response is written to `dst`. This is called by the server in order
    /// to execute a received command.
    pub(crate) async fn apply(
        mut self,
        db: &Db,
        dst: &mut Connection,
        shutdown: &mut Shutdown,
    ) -> crate::Result<()> {
        use Command::*;

        match &mut self {
            Get(cmd) => cmd.apply(dst).await,
            Mget(cmd) => cmd.apply(dst).await,
            Mset(cmd) => cmd.apply(dst).await,
            Publish(cmd) => cmd.apply(db, dst).await,
            Set(cmd) => cmd.apply(dst).await,
            Subscribe(cmd) => cmd.apply(db, dst, shutdown).await,
            Del(cmd) => cmd.apply(dst).await,
            Ping(cmd) => cmd.apply(dst).await,
            Strlen(cmd) => cmd.apply(dst).await,
            Type(cmd) => cmd.apply(dst).await,
            Exists(cmd) => cmd.apply(dst).await,
            Incr(cmd) => cmd.apply(dst, true).await,
            Decr(cmd) => cmd.apply(dst, false).await,
            Expire(cmd) => cmd.apply(dst, false, false).await,
            ExpireAt(cmd) => cmd.apply(dst, false, true).await,
            Pexpire(cmd) => cmd.apply(dst, true, false).await,
            PexpireAt(cmd) => cmd.apply(dst, true, true).await,
            TTL(cmd) => cmd.apply(dst, false).await,
            PTTL(cmd) => cmd.apply(dst, true).await,
            Scan(cmd) => cmd.apply(dst).await,
            Keys(cmd) => cmd.apply(dst).await,
            Sadd(cmd) => cmd.apply(dst).await,
            Scard(cmd) => cmd.apply(dst).await,
            Sismember(cmd) => cmd.apply(dst).await,
            Smismember(cmd) => cmd.apply(dst).await,
            Smembers(cmd) => cmd.apply(dst).await,
            Srandmember(cmd) => cmd.apply(dst).await,
            Spop(cmd) => cmd.apply(dst).await,
            Srem(cmd) => cmd.apply(dst).await,
            Lpush(cmd) => cmd.apply(dst, true).await,
            Rpush(cmd) => cmd.apply(dst, false).await,
            Lpop(cmd) => cmd.apply(dst, true).await,
            Rpop(cmd) => cmd.apply(dst, false).await,
            Lrange(cmd) => cmd.apply(dst).await,
            Ltrim(cmd) => cmd.apply(dst).await,
            Llen(cmd) => cmd.apply(dst).await,
            Lindex(cmd) => cmd.apply(dst).await,
            Lset(cmd) => cmd.apply(dst).await,
            Lrem(cmd) => cmd.apply(dst).await,
            Linsert(cmd) => cmd.apply(dst).await,
            Hset(cmd) => cmd.apply(dst, false, false).await,
            Hmset(cmd) => cmd.apply(dst, true, false).await,
            Hsetnx(cmd) => cmd.apply(dst, false, true).await,
            Hget(cmd) => cmd.apply(dst).await,
            Hmget(cmd) => cmd.apply(dst).await,
            Hlen(cmd) => cmd.apply(dst).await,
            Hgetall(cmd) => cmd.apply(dst).await,
            Hdel(cmd) => cmd.apply(dst).await,
            Hkeys(cmd) => cmd.apply(dst).await,
            Hvals(cmd) => cmd.apply(dst).await,
            Hincrby(cmd) => cmd.apply(dst).await,
            Hexists(cmd) => cmd.apply(dst).await,
            Hstrlen(cmd) => cmd.apply(dst).await,
            Zadd(cmd) => cmd.apply(dst).await,
            Zcard(cmd) => cmd.apply(dst).await,
            Zscore(cmd) => cmd.apply(dst).await,
            Zrem(cmd) => cmd.apply(dst).await,
            Zremrangebyscore(cmd) => cmd.apply(dst).await,
            Zremrangebyrank(cmd) => cmd.apply(dst).await,
            Zrange(cmd) => cmd.apply(dst).await,
            Zrevrange(cmd) => cmd.apply(dst).await,
            Zrangebyscore(cmd) => cmd.apply(dst, false).await,
            Zrevrangebyscore(cmd) => cmd.apply(dst, true).await,
            Zcount(cmd) => cmd.apply(dst).await,
            Zpopmin(cmd) => cmd.apply(dst, true).await,
            Zpopmax(cmd) => cmd.apply(dst, false).await,
            Zrank(cmd) => cmd.apply(dst).await,
            Zincrby(cmd) => cmd.apply(dst).await,

            Unknown(cmd) => cmd.apply(dst).await,
            // `Unsubscribe` cannot be applied. It may only be received from the
            // context of a `Subscribe` command.
            Unsubscribe(_) => Err("`Unsubscribe` is unsupported in this context".into()),

            _ => Ok(()),
        }
    }

    /// Returns the command name
    pub(crate) fn get_name(&self) -> &str {
        match self {
            Command::Get(_) => "get",
            Command::Mget(_) => "mget",
            Command::Mset(_) => "mset",
            Command::Publish(_) => "pub",
            Command::Set(_) => "set",
            Command::Subscribe(_) => "subscribe",
            Command::Unsubscribe(_) => "unsubscribe",
            Command::Del(_) => "del",
            Command::Ping(_) => "ping",
            Command::Strlen(_) => "strlen",
            Command::Type(_) => "type",
            Command::Exists(_) => "exists",
            Command::Incr(_) => "incr",
            Command::Decr(_) => "decr",
            Command::Expire(_) => "expire",
            Command::ExpireAt(_) => "expireat",
            Command::Pexpire(_) => "pexpire",
            Command::PexpireAt(_) => "pexpireat",
            Command::TTL(_) => "ttl",
            Command::PTTL(_) => "pttl",
            Command::Scan(_) => "scan",
            Command::Keys(_) => "keys",
            Command::Sadd(_) => "sadd",
            Command::Scard(_) => "scard",
            Command::Sismember(_) => "sismember",
            Command::Smismember(_) => "smismember",
            Command::Smembers(_) => "smembers",
            Command::Srandmember(_) => "srandmember",
            Command::Spop(_) => "spop",
            Command::Srem(_) => "srem",
            Command::Lpush(_) => "lpush",
            Command::Rpush(_) => "rpush",
            Command::Lpop(_) => "lpop",
            Command::Rpop(_) => "rpop",
            Command::Lrange(_) => "lrange",
            Command::Ltrim(_) => "ltrim",
            Command::Llen(_) => "llen",
            Command::Lindex(_) => "lindex",
            Command::Lset(_) => "lset",
            Command::Lrem(_) => "lrem",
            Command::Linsert(_) => "linsert",
            Command::Hset(_) => "hset",
            Command::Hmset(_) => "hmset",
            Command::Hsetnx(_) => "hsetnx",
            Command::Hget(_) => "hget",
            Command::Hmget(_) => "hmget",
            Command::Hlen(_) => "hlen",
            Command::Hgetall(_) => "hgetall",
            Command::Hdel(_) => "hdel",
            Command::Hkeys(_) => "hkeys",
            Command::Hvals(_) => "hvals",
            Command::Hincrby(_) => "hincrby",
            Command::Hexists(_) => "hexists",
            Command::Hstrlen(_) => "hstrlen",
            Command::Zadd(_) => "zadd",
            Command::Zcard(_) => "zcard",
            Command::Zscore(_) => "zscore",
            Command::Zrem(_) => "zrem",
            Command::Zremrangebyscore(_) => "zremrangebyscore",
            Command::Zremrangebyrank(_) => "zremrangebyrank",
            Command::Zrange(_) => "zrange",
            Command::Zrevrange(_) => "zrevrange",
            Command::Zrangebyscore(_) => "zrangebyscore",
            Command::Zrevrangebyscore(_) => "zrevrangebyscore",
            Command::Zcount(_) => "zcount",
            Command::Zpopmin(_) => "zpopmin",
            Command::Zpopmax(_) => "zpopmax",
            Command::Zrank(_) => "zrank",
            Command::Zincrby(_) => "zincrby",
            Command::Auth(_) => "auth",

            Command::Unknown(cmd) => cmd.get_name(),
        }
    }
}

/// All commands should be implement new_invalid() for invalid check
pub trait Invalid {
    fn new_invalid() -> Self;
}

fn transform_parse<T: Invalid>(parse_res: crate::Result<T>, parse: &mut Parse) -> T {
    match parse_res {
        Ok(cmd) => {
            if parse.check_finish() {
                cmd
            } else {
                T::new_invalid()
            }
        }
        Err(_) => T::new_invalid(),
    }
}

async fn retry_call<'a, F>(mut f: F) -> RocksResult<Frame>
where
    F: FnMut() -> BoxFuture<'a, RocksResult<Frame>> + Copy,
{
    let mut retry = txn_retry_count();
    let mut res = Frame::Null;
    while retry > 0 {
        res = f().await?;
        if let Frame::TxnFailed(_) = res {
            retry -= 1;
            TXN_RETRY_COUNTER.inc();
            continue;
        }
        return Ok(res);
    }
    Ok(res)
}
