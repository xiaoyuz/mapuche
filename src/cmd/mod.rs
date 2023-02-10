mod get;
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
pub use smismember:: Smismember;

use crate::{Connection, Db, Frame, Parse, ParseError, Shutdown};

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

    // set
    Sadd(Sadd),
    Scard(Scard),
    Sismember(Sismember),
    Smismember(Smismember),

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
        self,
        db: &Db,
        dst: &mut Connection,
        shutdown: &mut Shutdown,
    ) -> crate::Result<()> {
        use Command::*;

        match self {
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
            Sadd(cmd) => cmd.apply(dst).await,
            Scard(cmd) => cmd.apply(dst).await,
            Sismember(cmd) => cmd.apply(dst).await,
            Smismember(cmd) => cmd.apply(dst).await,

            Unknown(cmd) => cmd.apply(dst).await,
            // `Unsubscribe` cannot be applied. It may only be received from the
            // context of a `Subscribe` command.
            Unsubscribe(_) => Err("`Unsubscribe` is unsupported in this context".into()),
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
            Command::Sadd(_) => "sadd",
            Command::Scard(_) => "scard",
            Command::Sismember(_) => "sismember",
            Command::Smismember(_) => "smismember",
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