//! Real Redis-backed session service and target builder for perk/v1.
//!
//! [`RedisFactory`] serializes connection-form values into `redis:`-
//! labelled `redis://` targets (the host strips the label before
//! [`SessionFactory::open`]; direct `redis://` targets reach `open`
//! unchanged) and opens real sessions: one Tokio connection manager per
//! session, `INFO server` at open, and raw command forwarding for
//! execute/validate. Host-generated virtual-table SELECTs
//! (`SELECT * FROM "keys" ...`) route to the keys browse instead of
//! Redis. The virtual schema exposes one fixed `keys` table
//! over the selected logical database; `perk/v1/row_write` inserts,
//! updates, and deletes rows of that table (document writes are not
//! advertised).

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::dto::capabilities::{FormValues, WorkspaceViewScope};
use crate::dto::request::{
    AddColumnRequest, BrowseTableRequest, BuildTargetResult, ColumnChangeRequest, DropRequest,
    EmptyRequest, ForeignKeyChangeRequest, IndexChangeRequest, ReplaceForeignKeyRequest,
    ReplaceIndexRequest, StatementRequest, TableRequest, WorkspaceViewRequest,
};
use crate::dto::service::{
    ColumnInfo, DatabaseInfo, ForeignKeyInfo, IndexInfo, QueryResult, ReferencingForeignKeyInfo,
    SchemaObject, StatementMetadata,
};
use crate::dto::write::{RowValue, RowWriteRequest, RowWriteResponse, RowsAffected};
use crate::service::{OpenFuture, ServiceError, ServiceFuture, SessionFactory, SessionService};

/// Display rows are capped here; `truncated`/`has_more` report the cut.
const MAX_ROWS: usize = 500;
/// Display cells are capped at this many Unicode scalar values with a
/// trailing ellipsis (U+2026); full values stay in `untruncated_rows`.
const MAX_CELL: usize = 300;
/// Bounded readable preview bounds for `keys` browse.
const PREVIEW_BYTES: usize = 1024;
const PREVIEW_ELEMENTS: i64 = 64;

/// The single fixed virtual table.
const KEYS_TABLE: &str = "keys";

/// INFO sections the `server` workspace view reads. Each section is
/// fetched with its own `INFO <section>` command so the view works on
/// every supported Redis version (multi-section INFO is newer); only
/// allowlisted fields of these sections ever reach the wire.
const SERVER_VIEW_SECTIONS: [&str; 3] = ["server", "memory", "keyspace"];

/// The conservative allowlist of INFO server fields exposed by the
/// `server` workspace view: stable identity and runtime facts only.
const SERVER_VIEW_SERVER_FIELDS: [&str; 6] = [
    "redis_version",
    "redis_mode",
    "arch_bits",
    "uptime_in_seconds",
    "uptime_in_days",
    "tcp_port",
];

/// The conservative allowlist of INFO memory fields: runtime numeric
/// usage and fragmentation metrics only. Configuration policy fields
/// (e.g. `maxmemory_policy`) are never exposed.
const SERVER_VIEW_MEMORY_FIELDS: [&str; 8] = [
    "used_memory",
    "used_memory_human",
    "used_memory_rss",
    "used_memory_peak",
    "used_memory_peak_human",
    "maxmemory",
    "maxmemory_human",
    "mem_fragmentation_ratio",
];

/// The keyspace fields reported for the session's selected logical
/// database (its `dbN:` line), when Redis reports one.
const SERVER_VIEW_KEYSPACE_FIELDS: [&str; 3] = ["keys", "expires", "avg_ttl"];

/// Commands `execute_read_only` forwards; anything else is rejected
/// before any Redis I/O. Case-insensitive match on the first token.
const READ_ONLY_COMMANDS: [&str; 20] = [
    "PING", "GET", "MGET", "EXISTS", "TYPE", "TTL", "PTTL", "DBSIZE", "SCAN", "KEYS", "HGET",
    "HGETALL", "HLEN", "SMEMBERS", "SCARD", "ZRANGE", "ZCARD", "LRANGE", "LLEN", "INFO",
];

/// The fixed primary-key index exposed over the virtual `keys` table.
fn primary_index() -> IndexInfo {
    IndexInfo {
        name: "PRIMARY".to_string(),
        unique: true,
        primary_key: true,
        columns: vec!["key".to_string()],
    }
}

/// The `keys` table columns: key (primary), type, value.
fn keys_columns() -> Vec<ColumnInfo> {
    vec![
        ColumnInfo {
            name: "key".to_string(),
            type_: "string".to_string(),
            attributes: String::new(),
            nullable: false,
            default_value: None,
            primary_key: 1,
            indexes: vec![1],
        },
        ColumnInfo {
            name: "type".to_string(),
            type_: "string".to_string(),
            attributes: String::new(),
            nullable: false,
            default_value: None,
            primary_key: 0,
            indexes: vec![],
        },
        ColumnInfo {
            name: "value".to_string(),
            type_: "string".to_string(),
            attributes: String::new(),
            nullable: true,
            default_value: None,
            primary_key: 0,
            indexes: vec![],
        },
    ]
}

/// One parsed statement: either a native Redis command line (the default
/// execute/validate surface) or a virtual-table SELECT over the fixed
/// `keys` table.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Statement {
    /// A native Redis command line, kept tokenized as typed.
    Redis(Vec<String>),
    /// `SELECT * FROM "keys" [LIMIT n] [OFFSET m]`: a browse over the
    /// virtual keys table, paged like `browse_table`.
    SelectKeys { offset: usize, limit: Option<usize> },
}

/// Parses one statement with shell quoting rules. Blank statements and
/// malformed quoting are operation errors; a parseable statement is
/// accepted without any Redis I/O (validate semantics). Statements
/// whose first token is `SELECT *` are parsed against the virtual keys
/// table grammar; everything else stays a native Redis command line.
fn parse_statement(statement: &str) -> Result<Statement, ServiceError> {
    if statement.trim().is_empty() {
        return Err(ServiceError::validation("empty statement"));
    }
    let tokens = shell_words::split(statement)
        .map_err(|e| ServiceError::validation(format!("invalid statement: {e}")))?;
    // `SELECT *` is unambiguous: Redis's SELECT takes a database index,
    // never `*`, so only a virtual-table query can start this way. Any
    // other SELECT (e.g. the native `SELECT 2`) stays a Redis command.
    if tokens
        .first()
        .is_some_and(|t| t.eq_ignore_ascii_case("SELECT"))
        && tokens.get(1).is_some_and(|t| t == "*")
    {
        return parse_select(&tokens);
    }
    Ok(Statement::Redis(tokens))
}

/// Parses the host-generated virtual-table SELECT grammar
/// `SELECT * FROM <table> [LIMIT <n>] [OFFSET <m>]`, optionally ended
/// by a single `;` (whitespace-separated or glued to the last token).
/// The table identifier is matched case-insensitively against the fixed
/// `keys` table whether double-quoted or bare; LIMIT and OFFSET are
/// non-negative integers in either order.
fn parse_select(tokens: &[String]) -> Result<Statement, ServiceError> {
    let bad = |message: String| ServiceError::validation(format!("invalid statement: {message}"));
    // A `;` glued to the final token is the statement terminator
    // (`LIMIT 25;`); anywhere else it stays part of the token so a
    // terminator can never hide trailing clauses. A standalone final
    // `;` is handled by the clause loop below.
    let mut tokens = tokens.to_vec();
    if let Some(last) = tokens.last_mut()
        && let Some(stripped) = last.strip_suffix(';')
        && !stripped.is_empty()
    {
        *last = stripped.to_string();
    }
    if tokens
        .get(2)
        .is_none_or(|t| !t.eq_ignore_ascii_case("FROM"))
    {
        return Err(bad("expected FROM after SELECT *".to_string()));
    }
    let Some(table) = tokens.get(3) else {
        return Err(bad("expected a table name after FROM".to_string()));
    };
    if !table.eq_ignore_ascii_case(KEYS_TABLE) {
        return Err(bad(format!("unknown table: {table}")));
    }
    let mut offset = 0usize;
    let mut limit = None;
    let mut offset_seen = false;
    let mut index = 4;
    while let Some(token) = tokens.get(index) {
        if token == ";" {
            if tokens.len() != index + 1 {
                return Err(bad("unsupported clause after ;".to_string()));
            }
            break;
        }
        match token.to_ascii_uppercase().as_str() {
            "LIMIT" => {
                let value = parse_page_size(tokens.get(index + 1), "LIMIT", &bad)?;
                if limit.replace(value).is_some() {
                    return Err(bad("duplicate LIMIT clause".to_string()));
                }
                index += 2;
            }
            "OFFSET" => {
                let value = parse_page_size(tokens.get(index + 1), "OFFSET", &bad)?;
                if offset_seen {
                    return Err(bad("duplicate OFFSET clause".to_string()));
                }
                offset_seen = true;
                offset = value;
                index += 2;
            }
            other => return Err(bad(format!("unsupported SELECT clause: {other}"))),
        }
    }
    Ok(Statement::SelectKeys { offset, limit })
}

/// Parses one LIMIT/OFFSET value as a non-negative usize.
fn parse_page_size(
    value: Option<&String>,
    clause: &str,
    bad: &dyn Fn(String) -> ServiceError,
) -> Result<usize, ServiceError> {
    let Some(value) = value else {
        return Err(bad(format!("{clause} requires a value")));
    };
    value.parse::<usize>().map_err(|_| {
        bad(format!(
            "{clause} requires a non-negative integer, got {value}"
        ))
    })
}

fn is_read_only(verb: &str) -> bool {
    READ_ONLY_COMMANDS
        .iter()
        .any(|command| command.eq_ignore_ascii_case(verb))
}

// --- statement metadata -------------------------------------------------

/// Commands whose statement embeds a value, payload, credential, or
/// script, so the text must never be stored verbatim: the host redacts
/// it and forces the entry non-replayable. Covers credential
/// configuration (AUTH, HELLO AUTH, ACL SETUSER, CONFIG, MIGRATE AUTH)
/// and every value-bearing write; anything else unknown or module-level
/// defaults to sensitive as well (see [`command_metadata`]).
const SENSITIVE_COMMANDS: &[&str] = &[
    "ACL",
    "APPEND",
    "AUTH",
    "BITFIELD",
    "BLMOVE",
    "CONFIG",
    "DEBUG",
    "DECRBY",
    "ECHO",
    "EVAL",
    "EVALSHA",
    "FCALL",
    "FCALL_RO",
    "FUNCTION",
    "GEOADD",
    "GETSET",
    "HELLO",
    "HINCRBY",
    "HINCRBYFLOAT",
    "HMSET",
    "HSET",
    "HSETNX",
    "INCRBY",
    "INCRBYFLOAT",
    "LINSERT",
    "LMOVE",
    "LPUSH",
    "LPUSHX",
    "LSET",
    "MIGRATE",
    "MSET",
    "MSETNX",
    "PFADD",
    "PSETEX",
    "PUBLISH",
    "RESTORE",
    "RPUSH",
    "RPUSHX",
    "SADD",
    "SCRIPT",
    "SET",
    "SETBIT",
    "SETEX",
    "SETNX",
    "SETRANGE",
    "SMOVE",
    "SREM",
    "SPUBLISH",
    "XADD",
    "XCLAIM",
    "XAUTOCLAIM",
    "XSETID",
    "ZADD",
    "ZINCRBY",
];

/// Key-only writes with no payload: pasting the statement into the
/// plugin's editor reproduces the operation, and no value ever appears
/// in the text, so they stay non-sensitive and replayable.
const SAFE_WRITE_COMMANDS: &[&str] = &[
    "COPY",
    "DEL",
    "DISCARD",
    "EXEC",
    "EXPIRE",
    "EXPIREAT",
    "FLUSHALL",
    "FLUSHDB",
    "MOVE",
    "MULTI",
    "PERSIST",
    "PEXPIRE",
    "PEXPIREAT",
    "QUIT",
    "RENAME",
    "RENAMENX",
    "RESET",
    "SELECT",
    "SWAPDB",
    "TOUCH",
    "UNLINK",
    "UNWATCH",
    "WATCH",
];

/// Classifies one native Redis command for statement metadata.
/// `replayable` is true only when pasting the statement into this
/// plugin's editor reproduces the operation; `sensitive` marks
/// statements that embed a value/payload and must never be stored
/// verbatim. Read commands and key-only writes are benign; value-bearing
/// writes, credential configuration, and unknown/module commands
/// (which may embed payloads) default to sensitive.
///
/// The read-only allowlist was audited argument by argument: every entry
/// takes only keys, patterns, numeric ranges/limits, or an INFO section
/// name — locators and query parameters, never stored values. The one
/// exception is PING's optional message argument, an arbitrary payload
/// the server echoes back, so it is special-cased below.
fn command_metadata(tokens: &[String]) -> StatementMetadata {
    let Some(verb) = tokens.first().map(String::as_str) else {
        // Unreachable: empty token lists are rejected before execution.
        return StatementMetadata::redis(false, true);
    };
    let verb = verb.to_ascii_uppercase();
    // PING carries an optional arbitrary message: `PING <payload>`
    // embeds that payload in the statement, so any argument makes it
    // sensitive and non-replayable; a bare PING is a health check and
    // stays benign.
    if verb == "PING" {
        return StatementMetadata::redis(tokens.len() == 1, tokens.len() > 1);
    }
    if is_read_only(&verb) {
        StatementMetadata::redis(true, false)
    } else if SENSITIVE_COMMANDS.contains(&verb.as_str()) {
        StatementMetadata::redis(false, true)
    } else if SAFE_WRITE_COMMANDS.contains(&verb.as_str()) {
        StatementMetadata::redis(true, false)
    } else {
        // Unknown or module commands may embed payloads: conservative.
        StatementMetadata::redis(false, true)
    }
}

/// The exact pseudo-command `execute` replays for one virtual keys
/// browse: the host-generated `SELECT * FROM "keys"` grammar with the
/// effective paging clauses (the host always sends `LIMIT n OFFSET m`,
/// so an explicit zero offset with a limit keeps its clause), replayable
/// and non-sensitive.
fn select_statement(offset: usize, limit: Option<usize>) -> String {
    let mut statement = format!("SELECT * FROM \"{KEYS_TABLE}\"");
    if let Some(limit) = limit {
        statement.push_str(&format!(" LIMIT {limit}"));
    }
    if offset > 0 || limit.is_some() {
        statement.push_str(&format!(" OFFSET {offset}"));
    }
    statement
}

// --- error classification -----------------------------------------------

/// Maps one redis crate error onto the stable error kinds: credential
/// failures (AUTH handshake, `NOAUTH`/`WRONGPASS`/`NOPERM` replies) ->
/// `authentication`; I/O and dropped connections -> `connection`;
/// client-side configuration (unparseable URL) -> `validation`; every
/// other server or unexpected error -> `operation`. The message keeps
/// the caller's safe prefix; nothing beyond it enters the error text.
fn map_redis_error(e: &redis::RedisError, prefix: &str) -> ServiceError {
    let message = format!("{prefix}{e}");
    let auth_code = matches!(e.code(), Some("NOAUTH" | "WRONGPASS" | "NOPERM"));
    // Real server replies carry the code; synthetic and extended errors
    // surface it in the text instead. Either way the classification is
    // deterministic and testable.
    let auth_text = e.to_string().contains("NOAUTH") || e.to_string().contains("WRONGPASS");
    match e.kind() {
        redis::ErrorKind::AuthenticationFailed => ServiceError::authentication(message),
        redis::ErrorKind::Io => ServiceError::connection(message),
        redis::ErrorKind::InvalidClientConfig => ServiceError::validation(message),
        _ if auth_code || auth_text => ServiceError::authentication(message),
        _ => ServiceError::new(message),
    }
}

/// Commands whose first argument is a single key, for WRONGTYPE
/// guidance, mapped to the type family they accept. Conservative by
/// design: only unambiguous single-key commands are listed, so
/// multi-key commands (MGET, DEL, EXISTS, ...) and keyless commands
/// never get guidance; commands that accept any type (TTL, EXPIRE,
/// TYPE, ...) are absent because they cannot produce WRONGTYPE. The
/// family names the hint's "accepts" clause; the key's actual type is
/// inspected separately.
fn wrongtype_verb(verb: &str) -> Option<&'static str> {
    match verb {
        "GET" | "SET" | "SETNX" | "SETEX" | "PSETEX" | "GETSET" | "GETEX" | "GETDEL" | "APPEND"
        | "STRLEN" | "GETRANGE" | "SETRANGE" | "INCR" | "INCRBY" | "INCRBYFLOAT" | "DECR"
        | "DECRBY" => Some("strings"),
        "HSET" | "HSETNX" | "HGET" | "HGETALL" | "HDEL" | "HEXISTS" | "HLEN" | "HKEYS"
        | "HVALS" | "HMGET" | "HMSET" | "HINCRBY" | "HINCRBYFLOAT" | "HSTRLEN" => Some("hashes"),
        "LPUSH" | "LPUSHX" | "RPUSH" | "RPUSHX" | "LPOP" | "RPOP" | "LLEN" | "LRANGE"
        | "LINDEX" | "LSET" | "LREM" | "LTRIM" | "LINSERT" | "LPOS" => Some("lists"),
        "SADD" | "SREM" | "SMEMBERS" | "SISMEMBER" | "SCARD" | "SPOP" | "SRANDMEMBER"
        | "SMISMEMBER" => Some("sets"),
        "ZADD" | "ZREM" | "ZRANGE" | "ZCARD" | "ZSCORE" | "ZRANK" | "ZREVRANK" | "ZINCRBY"
        | "ZCOUNT" | "ZRANGEBYSCORE" | "ZREVRANGE" => Some("sorted sets"),
        _ => None,
    }
}

/// The single key argument of a listed command: the first token after
/// the verb, nonblank. Commands outside [`wrongtype_verb`] get no key.
fn wrongtype_key<'a>(verb: &str, tokens: &'a [String]) -> Option<&'a str> {
    wrongtype_verb(verb)?;
    tokens
        .get(1)
        .map(String::as_str)
        .filter(|key| !key.is_empty())
}

/// The canonical read command for one Redis type, plus the article the
/// hint uses for what the key actually is. Streams and other types get
/// no suggestion; the failing command's family (what it accepts) comes
/// from [`wrongtype_verb`], not from the inspected type.
fn type_read_command(actual: &str) -> Option<(&'static str, &'static str)> {
    match actual {
        "string" => Some(("GET", "a string")),
        "hash" => Some(("HGETALL", "a hash")),
        "list" => Some(("LRANGE", "a list")),
        "set" => Some(("SMEMBERS", "a set")),
        "zset" => Some(("ZRANGE", "a sorted set")),
        _ => None,
    }
}

// --- row writes ---------------------------------------------------------

/// The atomic update script, shared verbatim by execution and the
/// reported EVAL statement so the two cannot drift. Re-checks existence,
/// type, and destination inside Lua, compares the validated current
/// value, and performs rename + SET together so a concurrent change
/// aborts the whole update instead of partially applying it.
const UPDATE_SCRIPT: &str = r#"local src = KEYS[1]
local dst = ARGV[1]
local want_value = ARGV[2]
local expected = ARGV[3]
local new_value = ARGV[4]
if redis.call('EXISTS', src) == 0 then
  return redis.error_reply('key not found: ' .. src)
end
if want_value == '1' then
  local t = redis.call('TYPE', src).ok
  if t ~= 'string' then
    return redis.error_reply('cannot edit value: ' .. src .. ' is not a string (type ' .. t .. ')')
  end
  if redis.call('GET', src) ~= expected then
    return redis.error_reply('cannot edit value: ' .. src .. ' changed concurrently')
  end
end
if dst ~= src and redis.call('EXISTS', dst) == 1 then
  return redis.error_reply('destination key already exists: ' .. dst)
end
if dst ~= src then
  redis.call('RENAME', src, dst)
end
if want_value == '1' then
  redis.call('SET', dst, new_value)
end
return 1
"#;

/// Renders one native Redis command line from its raw tokens with the
/// same shell quoting [`parse_statement`] decodes, so the statement
/// round-trips through the plugin's own parser to exactly the executed
/// command. Empty tokens are quoted as `''`: `shell_words::quote` alone
/// leaves them bare, which would collapse an empty key/argument into the
/// neighbouring token when split back. Row writes return this as the
/// wire `statement` for the host's query log.
fn render_command<S: AsRef<str>>(tokens: &[S]) -> String {
    tokens
        .iter()
        .map(|token| {
            let token = token.as_ref();
            if token.is_empty() {
                "''".to_string()
            } else {
                shell_words::quote(token).into_owned()
            }
        })
        .collect::<Vec<String>>()
        .join(" ")
}

/// One parsed row-write request over the virtual `keys` table, fully
/// validated against the fixed schema before any Redis I/O.
#[derive(Debug, Clone, PartialEq, Eq)]
enum RowWrite {
    /// `DEL` the identified key; reports DEL's actual 0/1 count.
    Delete { key: String },
    /// Optional rename (when `key` is among the changed columns) plus an
    /// optional value replacement.
    Update {
        key: String,
        rename_to: Option<String>,
        value: Option<String>,
    },
    /// `SET key value NX` from an explicit insert form.
    Insert { key: String, value: String },
}

/// Validates a row-write request against the fixed `keys` schema and
/// normalizes it. Everything is checked here — operation string, table,
/// column names, cell kinds, key presence and uniqueness — before any
/// Redis I/O happens.
fn parse_row_write(request: &RowWriteRequest) -> Result<RowWrite, ServiceError> {
    if request.table != KEYS_TABLE {
        return Err(ServiceError::unsupported(format!(
            "unknown table: {}",
            request.table
        )));
    }
    match request.operation.as_str() {
        "delete" => {
            if let Some(values) = &request.values
                && !values.is_empty()
            {
                return Err(ServiceError::validation(
                    "delete does not accept a values payload",
                ));
            }
            Ok(RowWrite::Delete {
                key: parse_identity(request.key.as_deref())?,
            })
        }
        "update" => {
            let key = parse_identity(request.key.as_deref())?;
            let values = request
                .values
                .as_deref()
                .ok_or_else(|| ServiceError::validation("update requires a values payload"))?;
            let (rename_to, value) = parse_update_values(values)?;
            Ok(RowWrite::Update {
                key,
                rename_to,
                value,
            })
        }
        "insert" => {
            if request.key.is_some() {
                return Err(ServiceError::validation(
                    "insert does not accept a key identity",
                ));
            }
            let values = request
                .values
                .as_deref()
                .ok_or_else(|| ServiceError::validation("insert requires a values payload"))?;
            let (key, value) = parse_insert_values(values)?;
            Ok(RowWrite::Insert { key, value })
        }
        other => Err(ServiceError::validation(format!(
            "unsupported row_write operation: {other}"
        ))),
    }
}

/// The primary-key cell list: exactly one string `key` cell. An empty
/// string is a valid Redis key identity; only the rename destination is
/// rejected as empty.
fn parse_identity(cells: Option<&[RowValue]>) -> Result<String, ServiceError> {
    let Some(cells) = cells else {
        return Err(ServiceError::validation("missing key fields"));
    };
    let mut found: Option<String> = None;
    for cell in cells {
        if cell.name != "key" {
            return Err(ServiceError::validation(format!(
                "unknown key column: {}",
                cell.name
            )));
        }
        if found.is_some() {
            return Err(ServiceError::validation("duplicate key fields"));
        }
        found = Some(require_string(cell)?);
    }
    found.ok_or_else(|| ServiceError::validation("missing key fields"))
}

/// One cell payload must be a plain string; every other Value kind is
/// rejected (typed, default, null, collections).
fn require_string(cell: &RowValue) -> Result<String, ServiceError> {
    if cell.value.kind != "string" {
        return Err(ServiceError::validation(format!(
            "column {} must be a string value (got {} kind)",
            cell.name, cell.value.kind
        )));
    }
    match &cell.value.string {
        Some(s) => Ok(s.clone()),
        None => Err(ServiceError::validation(format!(
            "column {}: string kind without a payload",
            cell.name
        ))),
    }
}

/// The changed-column list of an update: optional `key` (the rename
/// destination) and `value` (the new string). `type` is immutable and any
/// other column is unknown; at least one change is required.
fn parse_update_values(
    cells: &[RowValue],
) -> Result<(Option<String>, Option<String>), ServiceError> {
    let mut rename_to: Option<String> = None;
    let mut value: Option<String> = None;
    for cell in cells {
        match cell.name.as_str() {
            "key" => {
                if rename_to.is_some() {
                    return Err(ServiceError::validation("duplicate column: key"));
                }
                let destination = require_string(cell)?;
                if destination.is_empty() {
                    return Err(ServiceError::validation(
                        "rename destination must not be empty",
                    ));
                }
                rename_to = Some(destination);
            }
            "value" => {
                if value.is_some() {
                    return Err(ServiceError::validation("duplicate column: value"));
                }
                value = Some(require_string(cell)?);
            }
            "type" => {
                return Err(ServiceError::validation("column type is immutable"));
            }
            other => {
                return Err(ServiceError::validation(format!("unknown column: {other}")));
            }
        }
    }
    if rename_to.is_none() && value.is_none() {
        return Err(ServiceError::validation(
            "update requires at least one column change",
        ));
    }
    Ok((rename_to, value))
}

/// The insert cells: required `key`, optional `type` (string, or blank
/// which defaults to string) and `value` (defaults to the empty string).
fn parse_insert_values(cells: &[RowValue]) -> Result<(String, String), ServiceError> {
    let mut key: Option<String> = None;
    let mut value: Option<String> = None;
    let mut type_seen = false;
    for cell in cells {
        match cell.name.as_str() {
            "key" => {
                if key.is_some() {
                    return Err(ServiceError::validation("duplicate column: key"));
                }
                key = Some(require_string(cell)?);
            }
            "value" => {
                if value.is_some() {
                    return Err(ServiceError::validation("duplicate column: value"));
                }
                value = Some(require_string(cell)?);
            }
            "type" => {
                if type_seen {
                    return Err(ServiceError::validation("duplicate column: type"));
                }
                type_seen = true;
                // Only an explicit string type is accepted: "string" or a
                // blank string, which defaults to string. The host omits
                // untouched insert fields, so a DEFAULT kind here is a
                // malformed payload, rejected like every other non-string
                // kind.
                let type_ = match cell.value.kind.as_str() {
                    "string" => cell.value.string.clone().ok_or_else(|| {
                        ServiceError::validation("column type: string kind without a payload")
                    })?,
                    other => {
                        return Err(ServiceError::validation(format!(
                            "column type must be a string value (got {other} kind)"
                        )));
                    }
                };
                if !type_.is_empty() && type_ != "string" {
                    return Err(ServiceError::validation(format!(
                        "cannot insert: type {type_} is not supported (only string)"
                    )));
                }
            }
            other => {
                return Err(ServiceError::validation(format!("unknown column: {other}")));
            }
        }
    }
    let key = key.ok_or_else(|| ServiceError::validation("insert requires a key column"))?;
    Ok((key, value.unwrap_or_default()))
}

// --- redis::Value reply conversion -------------------------------------

/// Converts one reply into `(columns, full rows)` exactly:
/// scalar/status/integer/bool/null replies become one nullable value
/// row; arrays/sets/pushes become `index, value` rows; maps become
/// `key, value` rows; nested values stringify as compact JSON-compatible
/// text.
fn reply_rows(reply: &redis::Value) -> (Vec<String>, Vec<Vec<Option<String>>>) {
    match reply {
        redis::Value::Array(items) | redis::Value::Set(items) => (
            vec!["index".to_string(), "value".to_string()],
            items
                .iter()
                .enumerate()
                .map(|(i, v)| vec![Some(i.to_string()), cell(v)])
                .collect(),
        ),
        redis::Value::Push { data, .. } => (
            vec!["index".to_string(), "value".to_string()],
            data.iter()
                .enumerate()
                .map(|(i, v)| vec![Some(i.to_string()), cell(v)])
                .collect(),
        ),
        redis::Value::Map(pairs) => (
            vec!["key".to_string(), "value".to_string()],
            pairs
                .iter()
                .map(|(k, v)| vec![Some(cell(k).unwrap_or_else(|| "null".to_string())), cell(v)])
                .collect(),
        ),
        redis::Value::Attribute { data, .. } => reply_rows(data),
        _ => (vec!["value".to_string()], vec![vec![cell(reply)]]),
    }
}

/// One display cell: `None` for null, raw text for scalars, compact JSON
/// for compound values.
fn cell(v: &redis::Value) -> Option<String> {
    match v {
        redis::Value::Nil => None,
        redis::Value::Int(i) => Some(i.to_string()),
        redis::Value::BulkString(b) => Some(String::from_utf8_lossy(b).into_owned()),
        redis::Value::SimpleString(s) => Some(s.clone()),
        redis::Value::Okay => Some("OK".to_string()),
        redis::Value::Double(f) => Some(f.to_string()),
        redis::Value::Boolean(b) => Some(b.to_string()),
        redis::Value::VerbatimString { text, .. } => Some(text.clone()),
        redis::Value::BigNumber(b) => Some(b.to_string()),
        redis::Value::ServerError(e) => Some(e.to_string()),
        _ => Some(to_json(v).to_string()),
    }
}

/// Compact JSON-compatible text for nested values.
fn to_json(v: &redis::Value) -> serde_json::Value {
    match v {
        redis::Value::Nil => serde_json::Value::Null,
        redis::Value::Int(i) => serde_json::Value::Number((*i).into()),
        redis::Value::BulkString(b) => {
            serde_json::Value::String(String::from_utf8_lossy(b).into_owned())
        }
        redis::Value::SimpleString(s) => serde_json::Value::String(s.clone()),
        redis::Value::Okay => serde_json::Value::String("OK".to_string()),
        redis::Value::Double(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        redis::Value::Boolean(b) => serde_json::Value::Bool(*b),
        redis::Value::VerbatimString { text, .. } => serde_json::Value::String(text.clone()),
        redis::Value::BigNumber(b) => serde_json::Value::String(b.to_string()),
        redis::Value::ServerError(e) => serde_json::Value::String(e.to_string()),
        redis::Value::Array(items) | redis::Value::Set(items) => {
            serde_json::Value::Array(items.iter().map(to_json).collect())
        }
        redis::Value::Push { data, .. } => {
            serde_json::Value::Array(data.iter().map(to_json).collect())
        }
        redis::Value::Map(pairs) => {
            let mut obj = serde_json::Map::new();
            for (k, v) in pairs {
                let key = cell(k).unwrap_or_else(|| "null".to_string());
                obj.insert(key, to_json(v));
            }
            serde_json::Value::Object(obj)
        }
        redis::Value::Attribute { data, .. } => to_json(data),
        // redis::Value is non_exhaustive; unknown variants render as null.
        _ => serde_json::Value::Null,
    }
}

/// Caps one display cell at [`MAX_CELL`] Unicode scalar values in total —
/// the trailing ellipsis (U+2026) counts toward the cap, so a truncated
/// cell never exceeds [`MAX_CELL`] runes.
fn cap_cell(s: &str) -> String {
    if s.chars().count() > MAX_CELL {
        let mut out: String = s.chars().take(MAX_CELL - 1).collect();
        out.push('\u{2026}');
        out
    } else {
        s.to_string()
    }
}

/// Builds the wire Result: display rows capped at [`MAX_ROWS`] with cells
/// capped at [`MAX_CELL`], full values preserved in `untruncated_rows`,
/// plus the exact accepted statement and its metadata.
fn finalize(
    columns: Vec<String>,
    full_rows: Vec<Vec<Option<String>>>,
    duration: Duration,
    has_more: bool,
    statement: Option<String>,
    statement_metadata: Option<StatementMetadata>,
) -> QueryResult {
    let truncated = full_rows.len() > MAX_ROWS;
    let shown: &[Vec<Option<String>>] = if truncated {
        &full_rows[..MAX_ROWS]
    } else {
        &full_rows
    };
    let rows: Vec<Vec<Option<String>>> = shown
        .iter()
        .map(|row| {
            row.iter()
                .map(|c| c.as_ref().map(|s| cap_cell(s)))
                .collect()
        })
        .collect();
    let column_types = vec!["string".to_string(); columns.len()];
    QueryResult {
        columns,
        column_types,
        rows,
        untruncated_rows: shown.to_vec(),
        rows_affected: 0,
        has_more,
        duration_ns: duration.as_nanos() as u64,
        truncated,
        document_ids: None,
        statement,
        statement_metadata,
    }
}

// --- target building ----------------------------------------------------

/// Serializes one connection form into a `redis://` URI with url::Url
/// setters so optional username/password are percent-encoded. Blank
/// host/port/database default to 127.0.0.1/6379/0; a non-integer or
/// negative database (or an invalid port) yields `None`.
fn build_redis_uri(values: &FormValues) -> Option<String> {
    let host = values
        .host
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or("127.0.0.1");
    let port_str = values
        .port
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or("6379");
    let database_str = values
        .database
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or("0");

    let port: u16 = port_str.parse().ok()?;
    if port == 0 {
        return None;
    }
    let database: i64 = database_str.parse().ok()?;
    if database < 0 {
        return None;
    }

    let mut url = Url::parse("redis://localhost/").ok()?;
    // The form delivers IPv6 hosts unbracketed; url wants brackets.
    let host = if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]")
    } else {
        host.to_string()
    };
    url.set_host(Some(&host)).ok()?;
    url.set_port(Some(port)).ok()?;
    if let Some(user) = values
        .user
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        url.set_username(user).ok()?;
    }
    if let Some(pass) = values
        .pass
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        url.set_password(Some(pass)).ok()?;
    }
    url.set_path(&format!("/{database}"));
    Some(url.to_string())
}

/// The host strips the `redis:` label before `open`; direct `redis://`
/// targets reach `open` unchanged. Defensively strip the label here too
/// so both routes land on the plain URI.
fn normalize_target(target: &str) -> Result<String, ServiceError> {
    let trimmed = target.trim();
    if trimmed.starts_with("redis://") {
        Ok(trimmed.to_string())
    } else if let Some(rest) = trimmed.strip_prefix("redis:") {
        let rest = rest.trim();
        if rest.is_empty() {
            Err(ServiceError::validation(
                "invalid target: missing redis:// URI",
            ))
        } else {
            Ok(rest.to_string())
        }
    } else {
        Err(ServiceError::validation(
            "invalid target: expected a redis:// URI",
        ))
    }
}

/// Validates the logical database encoded in the URL path: default 0
/// when absent, otherwise a non-negative integer.
fn parse_database(url: &Url) -> Result<i64, ServiceError> {
    let path = url.path().trim_matches('/');
    if path.is_empty() {
        return Ok(0);
    }
    match path.parse::<i64>() {
        Ok(db) if db >= 0 => Ok(db),
        _ => Err(ServiceError::validation(format!(
            "invalid target: invalid database number: {path}"
        ))),
    }
}

// --- session service ----------------------------------------------------

/// One Redis session: a shared Tokio connection manager plus the
/// selected logical database. All Redis I/O goes through the manager
/// mutex so concurrent handlers are serialized on one connection.
#[derive(Clone)]
pub struct RedisService {
    conn: Arc<Mutex<Option<redis::aio::ConnectionManager>>>,
    closed: Arc<AtomicBool>,
    database: i64,
}

impl RedisService {
    fn new(conn: redis::aio::ConnectionManager, database: i64) -> Self {
        RedisService {
            conn: Arc::new(Mutex::new(Some(conn))),
            closed: Arc::new(AtomicBool::new(false)),
            database,
        }
    }

    /// Runs one command and maps Redis errors onto the stable kinds:
    /// credential failures become authentication, dropped connections
    /// become connection, and server command errors stay operation. A
    /// closed session rejects commands without touching the network.
    async fn query<T: redis::FromRedisValue>(&self, cmd: &redis::Cmd) -> Result<T, ServiceError> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(ServiceError::connection("session closed"));
        }
        let mut guard = self.conn.lock().await;
        let conn = guard
            .as_mut()
            .ok_or_else(|| ServiceError::connection("session closed"))?;
        cmd.query_async::<T>(conn)
            .await
            .map_err(|e| map_redis_error(&e, "redis: "))
    }

    /// Forwards one parsed statement as a raw `redis::Cmd` and returns
    /// the elapsed duration alongside the reply.
    async fn run_command<S: AsRef<str>>(
        &self,
        tokens: &[S],
    ) -> Result<(Duration, redis::Value), ServiceError> {
        let mut cmd = redis::Cmd::new();
        for token in tokens {
            cmd.arg(token.as_ref());
        }
        let start = Instant::now();
        let reply = self.query::<redis::Value>(&cmd).await;
        let elapsed = start.elapsed();
        reply.map(|value| (elapsed, value))
    }

    /// Runs one native command, attaching advisory WRONGTYPE guidance to
    /// the failure when the command has a determinable key and the key's
    /// actual type can be inspected. The original failure is never
    /// replaced or reworded.
    async fn run_command_guided(
        &self,
        tokens: &[String],
    ) -> Result<(Duration, redis::Value), ServiceError> {
        match self.run_command(tokens).await {
            Ok(ok) => Ok(ok),
            Err(err) => Err(self.attach_wrongtype_guidance(tokens, err).await),
        }
    }

    /// Attaches advisory guidance to a WRONGTYPE failure: a hint
    /// explaining what the command accepts (its family, from the
    /// failing verb) and what the key actually is, plus a suggested
    /// read command for the key's type. TYPE is inspected only after a
    /// WRONGTYPE reply and only for commands with a determinable key;
    /// any inspection failure or unmapped type keeps the original
    /// error unchanged. Suggestions are rendered through
    /// [`render_command`], so arbitrary keys stay valid and
    /// value-bearing arguments never enter the guidance.
    async fn attach_wrongtype_guidance(
        &self,
        tokens: &[String],
        err: ServiceError,
    ) -> ServiceError {
        if !err.message.contains("WRONGTYPE") {
            return err;
        }
        let Some(verb) = tokens.first() else {
            return err;
        };
        let verb = verb.to_ascii_uppercase();
        let Some(accepts) = wrongtype_verb(&verb) else {
            return err;
        };
        let Some(key) = wrongtype_key(&verb, tokens) else {
            return err;
        };
        let Some((article, suggested)) = self.wrongtype_guidance(key).await else {
            return err;
        };
        let hint = format!("{verb} accepts {accepts}, but {key} is {article}");
        err.with_guidance(hint, suggested)
    }

    /// Inspects one key's actual type after a WRONGTYPE failure and
    /// maps it onto the canonical read command for that type. Returns
    /// None — and the caller keeps the original failure — when the
    /// inspection itself fails or the type has no conservative
    /// mapping.
    async fn wrongtype_guidance(&self, key: &str) -> Option<(String, String)> {
        let actual: String = self.query(redis::cmd("TYPE").arg(key)).await.ok()?;
        let (read_command, article) = type_read_command(&actual)?;
        let suggested = match read_command {
            "LRANGE" => render_command(&[read_command, key, "0", "-1"]),
            "ZRANGE" => render_command(&[read_command, key, "0", "-1", "WITHSCORES"]),
            _ => render_command(&[read_command, key]),
        };
        Some((article.to_string(), suggested))
    }

    /// Executes a virtual-table SELECT over the fixed `keys` table,
    /// paged exactly like `browse_table`: sorted keys with type and
    /// bounded value previews, `has_more` when the page is not the
    /// last, display rows capped at [`MAX_ROWS`]. The result reports the
    /// exact pseudo-command `execute` replays, non-sensitive and
    /// replayable.
    async fn run_select(
        &self,
        offset: usize,
        limit: Option<usize>,
    ) -> Result<QueryResult, ServiceError> {
        let start = Instant::now();
        let (full_rows, total) = self.browse_page(offset, limit).await?;
        let elapsed = start.elapsed();
        let shown = full_rows.len().min(MAX_ROWS);
        // Saturating: a huge parsed OFFSET must stay an empty page, not
        // overflow the end-of-page check.
        let has_more = offset.saturating_add(shown) < total;
        Ok(finalize(
            vec!["key".to_string(), "type".to_string(), "value".to_string()],
            full_rows,
            elapsed,
            has_more,
            Some(select_statement(offset, limit)),
            Some(StatementMetadata::redis(true, false)),
        ))
    }

    /// Collects every key in the selected database via SCAN, sorts, and
    /// pages with `offset`/`limit`. Returns the page rows plus the total
    /// key count.
    async fn browse_page(
        &self,
        offset: usize,
        limit: Option<usize>,
    ) -> Result<(Vec<Vec<Option<String>>>, usize), ServiceError> {
        let mut keys = Vec::new();
        let mut cursor: i64 = 0;
        loop {
            let reply = self
                .query::<redis::Value>(redis::cmd("SCAN").arg(cursor).arg("COUNT").arg(1000))
                .await?;
            let mut next = 0i64;
            if let redis::Value::Array(items) = reply {
                // RESP2 delivers the cursor as a bulk string; RESP3 as an
                // integer. Missing either shape stops the scan safely.
                if let Some(first) = items.first() {
                    next = match first {
                        redis::Value::Int(c) => *c,
                        redis::Value::BulkString(b) => {
                            String::from_utf8_lossy(b).parse().unwrap_or(0)
                        }
                        _ => 0,
                    };
                }
                if let Some(redis::Value::Array(batch)) = items.get(1) {
                    for key in batch {
                        if let redis::Value::BulkString(b) = key {
                            keys.push(String::from_utf8_lossy(b).into_owned());
                        }
                    }
                }
            }
            cursor = next;
            if cursor == 0 {
                break;
            }
        }
        keys.sort();
        let total = keys.len();
        let page: Vec<&String> = match limit {
            Some(l) => keys.iter().skip(offset).take(l).collect(),
            None => keys.iter().skip(offset).collect(),
        };
        let mut rows = Vec::with_capacity(page.len());
        for key in page {
            let (type_, preview) = self.fetch_preview(key).await;
            rows.push(vec![Some(key.clone()), Some(type_), preview]);
        }
        Ok((rows, total))
    }

    /// TYPE plus a bounded readable preview per key. Preview failures
    /// degrade to null cells rather than failing the whole page.
    async fn fetch_preview(&self, key: &str) -> (String, Option<String>) {
        let Ok(type_) = self.query::<String>(redis::cmd("TYPE").arg(key)).await else {
            return ("unknown".to_string(), None);
        };
        let preview = match type_.as_str() {
            "string" => self.preview_string(key).await,
            "hash" => self.preview_hash(key).await,
            "list" => self.preview_list(key).await,
            "set" => self.preview_set(key).await,
            "zset" => self.preview_zset(key).await,
            _ => None,
        };
        (type_, preview)
    }

    /// String preview: a bounded byte prefix cut at a char boundary.
    async fn preview_string(&self, key: &str) -> Option<String> {
        let got: Vec<u8> = self
            .query::<Vec<u8>>(
                redis::cmd("GETRANGE")
                    .arg(key)
                    .arg(0)
                    .arg((PREVIEW_BYTES - 1) as i64),
            )
            .await
            .ok()?;
        let s = String::from_utf8_lossy(&got);
        let mut end = PREVIEW_BYTES.min(s.len());
        while !s.is_char_boundary(end) {
            end -= 1;
        }
        Some(s[..end].to_string())
    }

    /// Hash preview: the first fields from HSCAN as a compact JSON object.
    async fn preview_hash(&self, key: &str) -> Option<String> {
        let reply = self
            .query::<redis::Value>(
                redis::cmd("HSCAN")
                    .arg(key)
                    .arg(0)
                    .arg("COUNT")
                    .arg(PREVIEW_ELEMENTS),
            )
            .await
            .ok()?;
        let mut obj = serde_json::Map::new();
        for (field, value) in pairs_from_scan(&reply) {
            obj.insert(field, to_json(&value));
        }
        serde_json::to_string(&serde_json::Value::Object(obj)).ok()
    }

    /// List preview: the first elements from LRANGE as a compact JSON array.
    async fn preview_list(&self, key: &str) -> Option<String> {
        let reply = self
            .query::<redis::Value>(
                redis::cmd("LRANGE")
                    .arg(key)
                    .arg(0)
                    .arg(PREVIEW_ELEMENTS - 1),
            )
            .await
            .ok()?;
        if let redis::Value::Array(items) = reply {
            serde_json::to_string(&serde_json::Value::Array(
                items.iter().map(to_json).collect(),
            ))
            .ok()
        } else {
            None
        }
    }

    /// Set preview: the first members from SSCAN, sorted, as a compact
    /// JSON array.
    async fn preview_set(&self, key: &str) -> Option<String> {
        let reply = self
            .query::<redis::Value>(
                redis::cmd("SSCAN")
                    .arg(key)
                    .arg(0)
                    .arg("COUNT")
                    .arg(PREVIEW_ELEMENTS),
            )
            .await
            .ok()?;
        let mut members: Vec<serde_json::Value> = if let redis::Value::Array(items) = reply {
            if let Some(redis::Value::Array(members)) = items.get(1) {
                members.iter().map(to_json).collect()
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        };
        members.sort_by_key(|a| a.to_string());
        serde_json::to_string(&serde_json::Value::Array(members)).ok()
    }

    /// Zset preview: the first members from ZRANGE WITHSCORES as a
    /// compact JSON object of member -> score.
    async fn preview_zset(&self, key: &str) -> Option<String> {
        let reply = self
            .query::<redis::Value>(
                redis::cmd("ZRANGE")
                    .arg(key)
                    .arg(0)
                    .arg(PREVIEW_ELEMENTS - 1)
                    .arg("WITHSCORES"),
            )
            .await
            .ok()?;
        let mut obj = serde_json::Map::new();
        for (member, score) in pairs_from_scan(&reply) {
            obj.insert(member, to_json(&score));
        }
        serde_json::to_string(&serde_json::Value::Object(obj)).ok()
    }

    // --- row writes ----------------------------------------------------

    /// `DEL` one key; the actual 0/1 deletion count is the rows
    /// affected. Returns the count plus the exact executed command.
    async fn delete_row(&self, key: &str) -> Result<(u64, String), ServiceError> {
        let tokens = ["DEL", key];
        let (_, reply) = self.run_command(&tokens).await?;
        let deleted = match reply {
            redis::Value::Int(count) => count.max(0) as u64,
            other => {
                return Err(ServiceError::new(format!(
                    "unexpected DEL reply: {other:?}"
                )));
            }
        };
        Ok((deleted, render_command(&tokens)))
    }

    /// `SET key value NX`: creates the string only when the key is
    /// absent, so an explicit insert can never overwrite anything.
    /// Returns the affected count plus the exact executed command.
    async fn insert_row(&self, key: &str, value: &str) -> Result<(u64, String), ServiceError> {
        let tokens = ["SET", key, value, "NX"];
        let (_, reply) = self.run_command(&tokens).await?;
        match reply {
            redis::Value::Nil => Err(ServiceError::new(format!("key already exists: {key}"))),
            _ => Ok((1, render_command(&tokens))),
        }
    }

    /// Applies one validated update. Reads validate everything first —
    /// source existence, string type, complete UTF-8 value, 300-rune
    /// display fit, destination availability — then one Lua script
    /// re-checks the invariants and performs rename + SET together, so a
    /// concurrent change aborts the whole update instead of partially
    /// applying it. Returns the affected count plus the exact executed
    /// EVAL statement, rendered from the same script and arguments.
    async fn update_row(
        &self,
        key: &str,
        rename_to: Option<&str>,
        value: Option<&str>,
    ) -> Result<(u64, String), ServiceError> {
        let destination = rename_to.unwrap_or(key);
        let mut expected: Option<String> = None;
        if value.is_some() {
            // Only an existing string whose complete value fits the
            // 300-rune display cell is editable: a bounded preview must
            // never replace a larger value, a non-UTF-8 blob, or a
            // hash/list/set/zset.
            let type_: String = self.query(redis::cmd("TYPE").arg(key)).await?;
            if type_ != "string" {
                return Err(ServiceError::new(format!(
                    "cannot edit value: {key} is a {type_}, not a string"
                )));
            }
            let reply = self
                .query::<redis::Value>(redis::cmd("GET").arg(key))
                .await?;
            let current = match reply {
                redis::Value::BulkString(bytes) => bytes,
                redis::Value::Nil => {
                    return Err(ServiceError::new(format!("key not found: {key}")));
                }
                other => {
                    return Err(ServiceError::new(format!(
                        "unexpected GET reply for {key}: {other:?}"
                    )));
                }
            };
            let current = String::from_utf8(current).map_err(|_| {
                ServiceError::new(format!(
                    "cannot edit value: the current value of {key} is not valid UTF-8"
                ))
            })?;
            let runes = current.chars().count();
            if runes > MAX_CELL {
                return Err(ServiceError::new(format!(
                    "cannot edit value: the current value of {key} has {runes} \
                     characters, more than the {MAX_CELL} the workbench displays; \
                     use SET for large strings"
                )));
            }
            expected = Some(current);
        }
        if destination != key {
            let exists: i64 = self.query(redis::cmd("EXISTS").arg(destination)).await?;
            if exists > 0 {
                return Err(ServiceError::new(format!(
                    "destination key already exists: {destination}"
                )));
            }
        }
        // One atomic script (shared with the reported statement):
        // existence, type, and destination are re-checked inside; the
        // validated current value is compared so a value changed by
        // another client after validation aborts the whole update.
        // Rename and SET never split across commands.
        let args: [&str; 4] = [
            destination,
            if value.is_some() { "1" } else { "0" },
            expected.as_deref().unwrap_or(""),
            value.unwrap_or(""),
        ];
        let statement = render_command(&[
            "EVAL",
            UPDATE_SCRIPT,
            "1",
            key,
            args[0],
            args[1],
            args[2],
            args[3],
        ]);
        if self.closed.load(Ordering::Relaxed) {
            return Err(ServiceError::connection("session closed"));
        }
        let mut guard = self.conn.lock().await;
        let conn = guard
            .as_mut()
            .ok_or_else(|| ServiceError::connection("session closed"))?;
        let affected: i64 = redis::Script::new(UPDATE_SCRIPT)
            .key(key)
            .arg(args[0])
            .arg(args[1])
            .arg(args[2])
            .arg(args[3])
            .invoke_async(conn)
            .await
            .map_err(|e| map_redis_error(&e, "redis: "))?;
        Ok((affected.max(0) as u64, statement))
    }
}

/// Extracts `(member, value)` pairs from a scan-style reply, tolerating
/// both the RESP2 flat `[a, 1, b, 2, ...]` shape and the RESP3 nested
/// `[[a, 1], [b, 2], ...]` shape.
fn pairs_from_scan(reply: &redis::Value) -> Vec<(String, redis::Value)> {
    let mut out = Vec::new();
    let redis::Value::Array(items) = reply else {
        return out;
    };
    let Some(redis::Value::Array(pairs)) = items.get(1) else {
        return out;
    };
    if pairs.iter().all(|p| matches!(p, redis::Value::Array(_))) {
        for pair in pairs {
            if let redis::Value::Array(pair) = pair
                && let [member, value] = pair.as_slice()
            {
                let member = cell(member).unwrap_or_else(|| "null".to_string());
                out.push((member, value.clone()));
            }
        }
    } else {
        for chunk in pairs.chunks(2) {
            if let [member, value] = chunk {
                let member = cell(member).unwrap_or_else(|| "null".to_string());
                out.push((member, value.clone()));
            }
        }
    }
    out
}

// --- workspace views ---------------------------------------------------

impl RedisService {
    /// The `server` workspace view: a deterministic two-column Field/Value
    /// table of a conservative allowlist of non-secret server/keyspace/
    /// memory metrics from native INFO sections. Only exact allowlisted
    /// fields of the three sections are taken — never credentials, raw
    /// config, the command line, paths, arbitrary module fields, or
    /// unbounded INFO content — and rows are sorted by field name. The
    /// keyspace fields report the session's selected logical database only
    /// (`dbN:` line; an empty database reports no keyspace fields). The
    /// result follows the bounded table conventions: display cells capped
    /// at [`MAX_CELL`] runes and rows at [`MAX_ROWS`]. Cancellation aborts
    /// the collection between sections.
    async fn server_view(&self, cancel: &CancellationToken) -> Result<QueryResult, ServiceError> {
        let start = Instant::now();
        let mut fields: HashMap<String, String> = HashMap::new();
        for section in SERVER_VIEW_SECTIONS {
            let mut cmd = redis::cmd("INFO");
            cmd.arg(section);
            let reply = tokio::select! {
                r = self.query::<String>(&cmd) => r,
                _ = cancel.cancelled() => {
                    return Err(ServiceError::canceled("request canceled"));
                }
            }?;
            match section {
                "server" => {
                    fields.extend(parse_info_section(
                        &reply,
                        "server",
                        &SERVER_VIEW_SERVER_FIELDS,
                    ));
                }
                "memory" => {
                    fields.extend(parse_info_section(
                        &reply,
                        "memory",
                        &SERVER_VIEW_MEMORY_FIELDS,
                    ));
                }
                "keyspace" => {
                    fields.extend(parse_keyspace_line(&reply, self.database));
                }
                _ => unreachable!("fixed section list"),
            }
        }
        let mut rows: Vec<(String, String)> = fields
            .into_iter()
            // Allowlisted values are inherently small, but cap every value
            // here — not only in the display rows — so both `rows` and
            // `untruncated_rows` meet the 300-rune cell bound on the wire.
            .map(|(field, value)| (field, cap_cell(&value)))
            .collect();
        rows.sort();
        Ok(finalize(
            vec!["Field".to_string(), "Value".to_string()],
            rows.into_iter()
                .map(|(field, value)| vec![Some(field), Some(value)])
                .collect(),
            start.elapsed(),
            false,
            // A workspace view is not a statement: no wire statement or
            // metadata is attached.
            None,
            None,
        ))
    }
}

/// Takes the allowlisted `field: value` lines of one INFO section reply,
/// prefixed with the section name (`server.redis_version`). Every other
/// line and field of the section is dropped, so the section's remaining
/// content (credentials, config, paths, module fields) never crosses
/// the wire.
fn parse_info_section(reply: &str, prefix: &str, fields: &[&str]) -> HashMap<String, String> {
    let mut out = HashMap::new();
    for line in reply.lines() {
        let Some((field, value)) = line.split_once(':') else {
            continue;
        };
        if fields.contains(&field) {
            out.insert(format!("{prefix}.{field}"), value.trim().to_string());
        }
    }
    out
}

/// Takes the allowlisted fields of one logical database's `dbN:` line in
/// the INFO keyspace section. An absent line (an empty database) yields
/// no keyspace fields at all.
fn parse_keyspace_line(reply: &str, database: i64) -> HashMap<String, String> {
    let mut out = HashMap::new();
    let Some(line) = reply
        .lines()
        .find(|line| line.starts_with(&format!("db{database}:")))
    else {
        return out;
    };
    let Some((_, values)) = line.split_once(':') else {
        return out;
    };
    for part in values.split(',') {
        if let Some((field, value)) = part.split_once('=')
            && SERVER_VIEW_KEYSPACE_FIELDS.contains(&field)
        {
            out.insert(format!("keyspace.{field}"), value.trim().to_string());
        }
    }
    out
}

impl SessionService for RedisService {
    fn execute(
        &self,
        request: StatementRequest,
        _cancel: CancellationToken,
    ) -> ServiceFuture<'static, QueryResult> {
        let this = self.clone();
        Box::pin(async move {
            match parse_statement(&request.statement)? {
                Statement::Redis(tokens) => {
                    if tokens.is_empty() {
                        // e.g. a comment-only statement parses to zero tokens.
                        return Err(ServiceError::validation("empty statement"));
                    }
                    // The exact command accepted by the plugin, rendered
                    // from the parsed tokens so it round-trips through
                    // the plugin's own parser.
                    let statement = render_command(&tokens);
                    let metadata = command_metadata(&tokens);
                    let (duration, reply) = this.run_command_guided(&tokens).await?;
                    let (columns, full_rows) = reply_rows(&reply);
                    let truncated = full_rows.len() > MAX_ROWS;
                    Ok(finalize(
                        columns,
                        full_rows,
                        duration,
                        truncated,
                        Some(statement),
                        Some(metadata),
                    ))
                }
                Statement::SelectKeys { offset, limit } => this.run_select(offset, limit).await,
            }
        })
    }

    fn execute_read_only(
        &self,
        request: StatementRequest,
        _cancel: CancellationToken,
    ) -> ServiceFuture<'static, QueryResult> {
        let this = self.clone();
        Box::pin(async move {
            match parse_statement(&request.statement)? {
                // A virtual-table SELECT only issues read commands over
                // the keys browse, so it is allowed on the read-only
                // path regardless of the allowlist.
                Statement::SelectKeys { offset, limit } => this.run_select(offset, limit).await,
                Statement::Redis(tokens) => {
                    if tokens.is_empty() {
                        // e.g. a comment-only statement parses to zero tokens.
                        return Err(ServiceError::validation("empty statement"));
                    }
                    let verb = tokens[0].to_ascii_uppercase();
                    if !is_read_only(&verb) {
                        return Err(ServiceError::unsupported(format!(
                            "read-only: {verb} is not allowed"
                        )));
                    }
                    let statement = render_command(&tokens);
                    let metadata = command_metadata(&tokens);
                    let (duration, reply) = this.run_command_guided(&tokens).await?;
                    let (columns, full_rows) = reply_rows(&reply);
                    let truncated = full_rows.len() > MAX_ROWS;
                    Ok(finalize(
                        columns,
                        full_rows,
                        duration,
                        truncated,
                        Some(statement),
                        Some(metadata),
                    ))
                }
            }
        })
    }

    fn validate(
        &self,
        request: StatementRequest,
        _cancel: CancellationToken,
    ) -> ServiceFuture<'static, ()> {
        Box::pin(async move {
            parse_statement(&request.statement).map(|_| ()) // no Redis I/O
        })
    }

    fn list_schema(
        &self,
        _request: EmptyRequest,
        _cancel: CancellationToken,
    ) -> ServiceFuture<'static, Vec<SchemaObject>> {
        let this = self.clone();
        Box::pin(async move {
            let dbsize: i64 = this.query(&redis::cmd("DBSIZE")).await?;
            let database = format!("db{}", this.database);
            // The database root makes the virtual table reachable in the
            // host sidebar (roots come only from database-type objects);
            // the host renders it expanded for non-relational products.
            Ok(vec![
                SchemaObject {
                    database: database.clone(),
                    type_: "database".to_string(),
                    name: database.clone(),
                    row_count: Some(dbsize.max(0) as u64),
                },
                SchemaObject {
                    database,
                    type_: "table".to_string(),
                    name: KEYS_TABLE.to_string(),
                    row_count: Some(dbsize.max(0) as u64),
                },
            ])
        })
    }

    fn table_info(
        &self,
        request: TableRequest,
        _cancel: CancellationToken,
    ) -> ServiceFuture<'static, Vec<ColumnInfo>> {
        Box::pin(async move {
            if request.table != KEYS_TABLE {
                return Err(ServiceError::unsupported(format!(
                    "unknown table: {}",
                    request.table
                )));
            }
            Ok(keys_columns())
        })
    }

    fn list_indexes(
        &self,
        request: TableRequest,
        _cancel: CancellationToken,
    ) -> ServiceFuture<'static, Vec<IndexInfo>> {
        Box::pin(async move {
            if request.table != KEYS_TABLE {
                return Err(ServiceError::unsupported(format!(
                    "unknown table: {}",
                    request.table
                )));
            }
            Ok(vec![primary_index()])
        })
    }

    fn create_index(
        &self,
        _request: IndexChangeRequest,
        _cancel: CancellationToken,
    ) -> ServiceFuture<'static, ()> {
        Box::pin(async { Err(fixed_schema_error()) })
    }

    fn replace_index(
        &self,
        _request: ReplaceIndexRequest,
        _cancel: CancellationToken,
    ) -> ServiceFuture<'static, ()> {
        Box::pin(async { Err(fixed_schema_error()) })
    }

    fn drop_index(
        &self,
        _request: DropRequest,
        _cancel: CancellationToken,
    ) -> ServiceFuture<'static, ()> {
        Box::pin(async { Err(fixed_schema_error()) })
    }

    fn list_foreign_keys(
        &self,
        _request: TableRequest,
        _cancel: CancellationToken,
    ) -> ServiceFuture<'static, Vec<ForeignKeyInfo>> {
        Box::pin(async { Ok(vec![]) })
    }

    fn list_referencing_foreign_keys(
        &self,
        _request: TableRequest,
        _cancel: CancellationToken,
    ) -> ServiceFuture<'static, Vec<ReferencingForeignKeyInfo>> {
        Box::pin(async { Ok(vec![]) })
    }

    fn list_foreign_keys_all(
        &self,
        _request: EmptyRequest,
        _cancel: CancellationToken,
    ) -> ServiceFuture<'static, HashMap<String, Vec<ForeignKeyInfo>>> {
        Box::pin(async { Ok(HashMap::new()) })
    }

    fn list_indexes_all(
        &self,
        _request: EmptyRequest,
        _cancel: CancellationToken,
    ) -> ServiceFuture<'static, HashMap<String, Vec<IndexInfo>>> {
        Box::pin(async {
            let mut all = HashMap::new();
            all.insert(KEYS_TABLE.to_string(), vec![primary_index()]);
            Ok(all)
        })
    }

    fn create_foreign_key(
        &self,
        _request: ForeignKeyChangeRequest,
        _cancel: CancellationToken,
    ) -> ServiceFuture<'static, ()> {
        Box::pin(async { Err(fixed_schema_error()) })
    }

    fn replace_foreign_key(
        &self,
        _request: ReplaceForeignKeyRequest,
        _cancel: CancellationToken,
    ) -> ServiceFuture<'static, ()> {
        Box::pin(async { Err(fixed_schema_error()) })
    }

    fn drop_foreign_key(
        &self,
        _request: DropRequest,
        _cancel: CancellationToken,
    ) -> ServiceFuture<'static, ()> {
        Box::pin(async { Err(fixed_schema_error()) })
    }

    fn alter_column(
        &self,
        _request: ColumnChangeRequest,
        _cancel: CancellationToken,
    ) -> ServiceFuture<'static, ()> {
        Box::pin(async { Err(fixed_schema_error()) })
    }

    fn drop_column(
        &self,
        _request: DropRequest,
        _cancel: CancellationToken,
    ) -> ServiceFuture<'static, ()> {
        Box::pin(async { Err(fixed_schema_error()) })
    }

    fn add_column(
        &self,
        _request: AddColumnRequest,
        _cancel: CancellationToken,
    ) -> ServiceFuture<'static, ()> {
        Box::pin(async { Err(fixed_schema_error()) })
    }

    fn browse_table(
        &self,
        request: BrowseTableRequest,
        _cancel: CancellationToken,
    ) -> ServiceFuture<'static, QueryResult> {
        let this = self.clone();
        Box::pin(async move {
            if request.table != KEYS_TABLE {
                return Err(ServiceError::unsupported(format!(
                    "unknown table: {}",
                    request.table
                )));
            }
            let offset = request.options.offset.unwrap_or(0) as usize;
            let limit = request.options.limit.map(|l| l as usize);
            this.run_select(offset, limit).await
        })
    }

    fn workspace_view(
        &self,
        request: WorkspaceViewRequest,
        cancel: CancellationToken,
    ) -> ServiceFuture<'static, QueryResult> {
        let this = self.clone();
        Box::pin(async move {
            if request.view_id != "server" {
                return Err(ServiceError::unsupported(format!(
                    "unknown view: {}",
                    request.view_id
                )));
            }
            // The view is advertised for database and table scopes only;
            // a schema target is not a Redis scope.
            match request.target.kind {
                WorkspaceViewScope::Database | WorkspaceViewScope::Table => {}
                WorkspaceViewScope::Schema => {
                    return Err(ServiceError::unsupported(
                        "view server is not available for schema targets",
                    ));
                }
            }
            this.server_view(&cancel).await
        })
    }

    fn row_write(
        &self,
        request: RowWriteRequest,
        _cancel: CancellationToken,
    ) -> ServiceFuture<'static, RowWriteResponse> {
        let this = self.clone();
        Box::pin(async move {
            let (rows_affected, statement, metadata) = match parse_row_write(&request)? {
                // Key-only DEL: the statement embeds no value, so it is
                // replayable and non-sensitive.
                RowWrite::Delete { key } => {
                    let (affected, statement) = this.delete_row(&key).await?;
                    (affected, statement, StatementMetadata::redis(true, false))
                }
                RowWrite::Update {
                    key,
                    rename_to,
                    value,
                } => {
                    let (affected, statement) = this
                        .update_row(&key, rename_to.as_deref(), value.as_deref())
                        .await?;
                    // A rename-only update is RENAME-equivalent: the
                    // reported EVAL embeds keys only. A value change
                    // embeds the new value in the statement, so it is
                    // sensitive and non-replayable.
                    let metadata = if value.is_none() {
                        StatementMetadata::redis(true, false)
                    } else {
                        StatementMetadata::redis(false, true)
                    };
                    (affected, statement, metadata)
                }
                RowWrite::Insert { key, value } => {
                    let (affected, statement) = this.insert_row(&key, &value).await?;
                    // SET <key> <value> NX embeds the value: sensitive,
                    // never stored verbatim, never replayed.
                    (affected, statement, StatementMetadata::redis(false, true))
                }
            };
            Ok(RowWriteResponse {
                result: RowsAffected::with_statement(rows_affected, statement, metadata),
            })
        })
    }

    fn close(&self) {
        self.closed.store(true, Ordering::Relaxed);
        if let Ok(mut guard) = self.conn.try_lock() {
            // Nothing in flight: release the connection immediately.
            *guard = None;
        }
    }
}

fn fixed_schema_error() -> ServiceError {
    ServiceError::unsupported("Redis keys has a fixed virtual schema")
}

// --- factory ------------------------------------------------------------

/// Builds `redis:`-labelled targets from connection forms and opens real
/// Redis sessions.
#[derive(Default)]
pub struct RedisFactory {}

impl SessionFactory for RedisFactory {
    fn build_target(&self, values: &FormValues) -> BuildTargetResult {
        match build_redis_uri(values) {
            Some(uri) => BuildTargetResult {
                target: format!("redis:{uri}"),
                ok: true,
            },
            None => BuildTargetResult {
                target: String::new(),
                ok: false,
            },
        }
    }

    fn open<'a>(&'a self, target: &'a str) -> OpenFuture<'a> {
        Box::pin(async move {
            let uri = normalize_target(target)?;
            let parsed = Url::parse(&uri)
                .map_err(|e| ServiceError::validation(format!("invalid target: {e}")))?;
            if parsed.scheme() != "redis" {
                return Err(ServiceError::validation(
                    "invalid target: only plain TCP redis:// targets are supported (no TLS)",
                ));
            }
            if parsed
                .query_pairs()
                .any(|(k, v)| k == "tls" && matches!(v.as_ref(), "true" | "1" | "secure"))
            {
                return Err(ServiceError::validation(
                    "invalid target: only plain TCP redis:// targets are supported (no TLS)",
                ));
            }
            let database = parse_database(&parsed)?;

            let client =
                redis::Client::open(uri).map_err(|e| map_redis_error(&e, "invalid target: "))?;
            let mut manager = redis::aio::ConnectionManager::new(client)
                .await
                .map_err(|e| map_redis_error(&e, "connect failed: "))?;

            let info: String = redis::cmd("INFO")
                .arg("server")
                .query_async(&mut manager)
                .await
                .map_err(|e| map_redis_error(&e, "connect failed: "))?;
            let version = info
                .lines()
                .find_map(|line| line.strip_prefix("redis_version:").map(str::trim))
                .unwrap_or("unknown")
                .to_string();

            let service: Box<dyn SessionService> = Box::new(RedisService::new(manager, database));
            Ok((
                DatabaseInfo {
                    product: "Redis".to_string(),
                    version,
                },
                service,
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dto::capabilities::QueryCommand;
    use crate::dto::write::Value;
    use crate::protocol::ErrorKind;

    #[test]
    fn build_target_uses_defaults_for_blank_fields() {
        let factory = RedisFactory::default();
        let result = factory.build_target(&FormValues::default());
        assert!(result.ok);
        assert_eq!(result.target, "redis:redis://127.0.0.1:6379/0");
    }

    #[test]
    fn build_target_trims_and_applies_form_values() {
        let values = FormValues {
            host: Some("  db.example.com  ".to_string()),
            port: Some(" 6380 ".to_string()),
            database: Some(" 2 ".to_string()),
            ..Default::default()
        };
        let result = RedisFactory::default().build_target(&values);
        assert!(result.ok);
        assert_eq!(result.target, "redis:redis://db.example.com:6380/2");
    }

    #[test]
    fn build_target_percent_encodes_credentials() {
        let values = FormValues {
            user: Some("alice".to_string()),
            pass: Some("p@ss:w/rd".to_string()),
            ..Default::default()
        };
        let result = RedisFactory::default().build_target(&values);
        assert!(result.ok);
        assert_eq!(
            result.target,
            "redis:redis://alice:p%40ss%3Aw%2Frd@127.0.0.1:6379/0"
        );
    }

    #[test]
    fn build_target_omits_blank_credentials() {
        let values = FormValues {
            user: Some("  ".to_string()),
            pass: Some("".to_string()),
            ..Default::default()
        };
        let result = RedisFactory::default().build_target(&values);
        assert!(result.ok);
        assert_eq!(result.target, "redis:redis://127.0.0.1:6379/0");
    }

    #[test]
    fn build_target_brackets_ipv6_hosts() {
        let values = FormValues {
            host: Some("::1".to_string()),
            ..Default::default()
        };
        let result = RedisFactory::default().build_target(&values);
        assert!(result.ok);
        assert_eq!(result.target, "redis:redis://[::1]:6379/0");
    }

    #[test]
    fn build_target_rejects_invalid_database() {
        for bad in ["abc", "-1", "1.5", "0x10", "1_000"] {
            let values = FormValues {
                database: Some(bad.to_string()),
                ..Default::default()
            };
            let result = RedisFactory::default().build_target(&values);
            assert!(!result.ok, "database {bad:?} must be rejected");
        }
    }

    #[test]
    fn build_target_rejects_invalid_port() {
        for bad in ["0", "70000", "abc", "-1"] {
            let values = FormValues {
                port: Some(bad.to_string()),
                ..Default::default()
            };
            let result = RedisFactory::default().build_target(&values);
            assert!(!result.ok, "port {bad:?} must be rejected");
        }
    }

    #[tokio::test]
    async fn open_rejects_non_tcp_targets_without_io() {
        let factory = RedisFactory::default();
        for target in [
            "not-a-url",
            "redis:",
            "rediss://example.com:6379/0",
            "unix:///tmp/redis.sock",
            "redis://example.com:6379/abc",
            "redis://example.com:6379/-1",
            "redis://example.com:6379/0?tls=true",
        ] {
            let err = match factory.open(target).await {
                Err(e) => e,
                Ok(_) => panic!("target {target:?} must be rejected"),
            };
            assert!(
                err.message.contains("invalid target"),
                "unexpected error for {target:?}: {}",
                err.message
            );
        }
    }

    #[test]
    fn parse_statement_accepts_shell_quoting() {
        assert_eq!(
            parse_statement("SET key \"hello world\"").unwrap(),
            Statement::Redis(vec!["SET".into(), "key".into(), "hello world".into()])
        );
        assert_eq!(
            parse_statement("GET 'single quoted'").unwrap(),
            Statement::Redis(vec!["GET".into(), "single quoted".into()])
        );
        // A native Redis SELECT (database switch) is not a virtual-table
        // query and stays on the Redis path.
        assert_eq!(
            parse_statement("SELECT 2").unwrap(),
            Statement::Redis(vec!["SELECT".into(), "2".into()])
        );
    }

    #[test]
    fn parse_statement_rejects_blank_and_malformed() {
        for bad in ["", "   ", "\t\n", "SET unclosed 'quote", "\"dangling"] {
            assert!(
                parse_statement(bad).is_err(),
                "statement {bad:?} must be an operation error"
            );
        }
        // Comment-only statements parse to zero tokens: execute must
        // reject them rather than indexing an empty vec.
        assert_eq!(
            parse_statement("# comment").unwrap(),
            Statement::Redis(Vec::<String>::new())
        );
    }

    #[test]
    fn parse_statement_routes_virtual_table_selects() {
        // The exact host-generated browse statement, quoted and bare.
        assert_eq!(
            parse_statement(r#"SELECT * FROM "keys" LIMIT 25 OFFSET 0"#).unwrap(),
            Statement::SelectKeys {
                offset: 0,
                limit: Some(25)
            }
        );
        assert_eq!(
            parse_statement("SELECT * FROM keys").unwrap(),
            Statement::SelectKeys {
                offset: 0,
                limit: None
            }
        );
        // Case-insensitive keywords, OFFSET before LIMIT, trailing
        // semicolon, and zero paging values all parse.
        assert_eq!(
            parse_statement("select * from keys offset 5 limit 10;").unwrap(),
            Statement::SelectKeys {
                offset: 5,
                limit: Some(10)
            }
        );
        assert_eq!(
            parse_statement("SELECT * FROM \"keys\" LIMIT 0 OFFSET 0").unwrap(),
            Statement::SelectKeys {
                offset: 0,
                limit: Some(0)
            }
        );
    }

    #[test]
    fn parse_statement_rejects_malformed_selects() {
        // Unmatched quoting anywhere in the statement stays a shell
        // quoting error, exactly like native commands.
        for bad in [
            r#"SELECT * FROM "keys"#,
            "SELECT * FROM 'keys LIMIT 1",
            "SELECT * FROM keys LIMIT \"25",
        ] {
            let err = parse_statement(bad).expect_err("unclosed quote must fail");
            assert!(
                err.message.contains("missing closing quote"),
                "statement {bad:?}: {}",
                err.message
            );
        }
        // Well-formed shell syntax but outside the virtual-table grammar.
        for (bad, expected) in [
            ("SELECT *", "expected FROM"),
            ("SELECT * FROM", "table name"),
            (r#"SELECT * FROM "nope""#, "unknown table"),
            ("SELECT * FROM keys LIMIT", "LIMIT requires a value"),
            ("SELECT * FROM keys LIMIT -1", "non-negative integer"),
            ("SELECT * FROM keys LIMIT 1 LIMIT 2", "duplicate LIMIT"),
            ("SELECT * FROM keys OFFSET 1 OFFSET 0", "duplicate OFFSET"),
            (
                "SELECT * FROM keys WHERE 1 = 1",
                "unsupported SELECT clause",
            ),
            // A terminator glued to a non-final token cannot hide
            // trailing clauses.
            ("SELECT * FROM keys LIMIT 1; DROP", "non-negative integer"),
            ("SELECT * FROM keys; LIMIT 1", "unknown table"),
            ("SELECT * FROM keys LIMIT 1 ; DROP", "unsupported clause"),
        ] {
            let err = parse_statement(bad).expect_err("must be rejected");
            assert!(
                err.message.contains(expected),
                "statement {bad:?}: expected {expected:?}, got {}",
                err.message
            );
        }
    }

    #[test]
    fn read_only_allowlist_is_exact_and_case_insensitive() {
        assert!(is_read_only("PING"));
        assert!(is_read_only("get"));
        assert!(is_read_only("HGetAll"));
        assert!(is_read_only("INFO"));
        for verb in [
            "SET", "DEL", "DUMP", "GETRANGE", "SELECT", "FLUSHDB", "INCR",
        ] {
            assert!(!is_read_only(verb), "{verb} must not be read-only");
        }
    }

    #[test]
    fn scalar_replies_become_one_value_row() {
        let (columns, rows) = reply_rows(&redis::Value::BulkString(b"hello".to_vec()));
        assert_eq!(columns, vec!["value"]);
        assert_eq!(rows, vec![vec![Some("hello".to_string())]]);
        let (_, rows) = reply_rows(&redis::Value::Nil);
        assert_eq!(rows, vec![vec![None]]);
        let (_, rows) = reply_rows(&redis::Value::Int(42));
        assert_eq!(rows, vec![vec![Some("42".to_string())]]);
        let (_, rows) = reply_rows(&redis::Value::Okay);
        assert_eq!(rows, vec![vec![Some("OK".to_string())]]);
        let (_, rows) = reply_rows(&redis::Value::Boolean(true));
        assert_eq!(rows, vec![vec![Some("true".to_string())]]);
    }

    #[test]
    fn array_replies_become_index_value_rows() {
        let reply = redis::Value::Array(vec![
            redis::Value::BulkString(b"a".to_vec()),
            redis::Value::Int(2),
        ]);
        let (columns, rows) = reply_rows(&reply);
        assert_eq!(columns, vec!["index", "value"]);
        assert_eq!(
            rows,
            vec![
                vec![Some("0".to_string()), Some("a".to_string())],
                vec![Some("1".to_string()), Some("2".to_string())]
            ]
        );
    }

    #[test]
    fn map_replies_become_key_value_rows() {
        let reply = redis::Value::Map(vec![
            (
                redis::Value::BulkString(b"f1".to_vec()),
                redis::Value::BulkString(b"v1".to_vec()),
            ),
            (redis::Value::BulkString(b"f2".to_vec()), redis::Value::Nil),
        ]);
        let (columns, rows) = reply_rows(&reply);
        assert_eq!(columns, vec!["key", "value"]);
        assert_eq!(
            rows,
            vec![
                vec![Some("f1".to_string()), Some("v1".to_string())],
                vec![Some("f2".to_string()), None]
            ]
        );
    }

    #[test]
    fn nested_values_stringify_as_compact_json() {
        let reply = redis::Value::Array(vec![
            redis::Value::Int(0),
            redis::Value::Array(vec![
                redis::Value::BulkString(b"k1".to_vec()),
                redis::Value::BulkString(b"k2".to_vec()),
            ]),
        ]);
        let (_, rows) = reply_rows(&reply);
        assert_eq!(rows[0][0].as_deref(), Some("0"));
        assert_eq!(rows[0][1].as_deref(), Some("0"));
        assert_eq!(rows[1][1].as_deref(), Some("[\"k1\",\"k2\"]"));
    }

    #[test]
    fn display_cells_are_capped_at_300_with_ellipsis() {
        let long = "x".repeat(400);
        let capped = cap_cell(&long);
        // The ellipsis counts toward the 300-rune cap: a truncated cell
        // is at most MAX_CELL runes, never MAX_CELL + 1.
        assert_eq!(capped.chars().count(), 300);
        assert!(capped.ends_with('\u{2026}'));
        assert_eq!(cap_cell("short"), "short");
        let exactly = "y".repeat(300);
        assert_eq!(cap_cell(&exactly), exactly);
    }

    fn string_cell(name: &str, value: &str) -> RowValue {
        RowValue {
            name: name.to_string(),
            value: Value {
                kind: "string".to_string(),
                string: Some(value.to_string()),
                bool_: None,
                integer: None,
                float: None,
                bytes: None,
                decimal: None,
                timestamp: None,
                array: None,
                object: None,
            },
        }
    }

    fn value_of_kind(kind: &str) -> Value {
        Value {
            kind: kind.to_string(),
            string: None,
            bool_: None,
            integer: None,
            float: None,
            bytes: None,
            decimal: None,
            timestamp: None,
            array: None,
            object: None,
        }
    }

    fn request(operation: &str, key: Vec<RowValue>, values: Vec<RowValue>) -> RowWriteRequest {
        RowWriteRequest {
            operation: operation.to_string(),
            table: KEYS_TABLE.to_string(),
            key: (!key.is_empty()).then_some(key),
            values: (!values.is_empty()).then_some(values),
        }
    }

    #[test]
    fn parse_row_write_accepts_each_operation() {
        let delete =
            parse_row_write(&request("delete", vec![string_cell("key", "k1")], vec![])).unwrap();
        assert_eq!(delete, RowWrite::Delete { key: "k1".into() });

        // An empty string is a valid Redis key identity: delete runs DEL
        // and reports its actual count. Only the rename destination is
        // rejected as empty.
        let delete_empty =
            parse_row_write(&request("delete", vec![string_cell("key", "")], vec![])).unwrap();
        assert_eq!(delete_empty, RowWrite::Delete { key: String::new() });

        let update = parse_row_write(&request(
            "update",
            vec![string_cell("key", "k1")],
            vec![string_cell("key", "k2"), string_cell("value", "new value")],
        ))
        .unwrap();
        assert_eq!(
            update,
            RowWrite::Update {
                key: "k1".into(),
                rename_to: Some("k2".into()),
                value: Some("new value".into())
            }
        );

        let insert = parse_row_write(&request(
            "insert",
            vec![],
            vec![
                string_cell("key", "k1"),
                string_cell("type", "string"),
                string_cell("value", "v"),
            ],
        ))
        .unwrap();
        assert_eq!(
            insert,
            RowWrite::Insert {
                key: "k1".into(),
                value: "v".into()
            }
        );

        // A blank string type means string; a missing value is empty.
        let blank = parse_row_write(&request(
            "insert",
            vec![],
            vec![string_cell("key", "k2"), string_cell("type", "")],
        ))
        .unwrap();
        assert_eq!(
            blank,
            RowWrite::Insert {
                key: "k2".into(),
                value: String::new()
            }
        );

        // An empty key is an explicit string key: SET "" NX semantics.
        let empty_key =
            parse_row_write(&request("insert", vec![], vec![string_cell("key", "")])).unwrap();
        assert_eq!(
            empty_key,
            RowWrite::Insert {
                key: String::new(),
                value: String::new()
            }
        );
    }

    #[test]
    fn parse_row_write_rejects_malformed_payloads() {
        let mut bad = vec![
            (request("delete", vec![], vec![]), "missing key fields"),
            (
                request(
                    "delete",
                    vec![string_cell("key", "k"), string_cell("key", "k2")],
                    vec![],
                ),
                "duplicate key fields",
            ),
            (
                request("delete", vec![string_cell("nope", "k")], vec![]),
                "unknown key column",
            ),
            (
                request(
                    "delete",
                    vec![string_cell("key", "k")],
                    vec![string_cell("value", "v")],
                ),
                "delete does not accept a values payload",
            ),
            (
                request("update", vec![], vec![string_cell("value", "v")]),
                "missing key fields",
            ),
            (
                request("update", vec![string_cell("key", "k")], vec![]),
                "update requires a values payload",
            ),
            (
                request(
                    "update",
                    vec![string_cell("key", "k")],
                    vec![string_cell("type", "string")],
                ),
                "column type is immutable",
            ),
            (
                request(
                    "update",
                    vec![string_cell("key", "k")],
                    vec![string_cell("value", "v"), string_cell("value", "v2")],
                ),
                "duplicate column: value",
            ),
            (
                request(
                    "update",
                    vec![string_cell("key", "k")],
                    vec![string_cell("key", "")],
                ),
                "rename destination must not be empty",
            ),
            (
                request(
                    "update",
                    vec![string_cell("key", "k")],
                    vec![string_cell("other", "x")],
                ),
                "unknown column: other",
            ),
            (
                request(
                    "update",
                    vec![string_cell("key", "k")],
                    vec![string_cell("key", "k2"), string_cell("key", "k3")],
                ),
                "duplicate column: key",
            ),
            (
                request("insert", vec![string_cell("key", "k")], vec![]),
                "insert does not accept a key identity",
            ),
            (
                request("insert", vec![], vec![]),
                "insert requires a values payload",
            ),
            (
                request("insert", vec![], vec![string_cell("value", "v")]),
                "insert requires a key column",
            ),
            (
                request(
                    "insert",
                    vec![],
                    vec![string_cell("key", "k"), string_cell("type", "hash")],
                ),
                "only string",
            ),
            (
                RowWriteRequest {
                    operation: "insert".to_string(),
                    table: KEYS_TABLE.to_string(),
                    key: None,
                    values: Some(vec![
                        string_cell("key", "k"),
                        RowValue {
                            name: "type".to_string(),
                            value: value_of_kind("default"),
                        },
                    ]),
                },
                "must be a string value",
            ),
            (
                RowWriteRequest {
                    operation: "insert".to_string(),
                    table: KEYS_TABLE.to_string(),
                    key: None,
                    values: Some(vec![
                        string_cell("key", "k"),
                        RowValue {
                            name: "type".to_string(),
                            value: Value {
                                kind: "string".to_string(),
                                string: None,
                                bool_: None,
                                integer: None,
                                float: None,
                                bytes: None,
                                decimal: None,
                                timestamp: None,
                                array: None,
                                object: None,
                            },
                        },
                    ]),
                },
                "without a payload",
            ),
            (
                request(
                    "insert",
                    vec![],
                    vec![string_cell("key", "k"), string_cell("type", "STRING")],
                ),
                "only string",
            ),
            (
                request(
                    "insert",
                    vec![],
                    vec![string_cell("key", "k"), string_cell("nope", "x")],
                ),
                "unknown column: nope",
            ),
            (
                request("upsert", vec![string_cell("key", "k")], vec![]),
                "unsupported row_write operation: upsert",
            ),
            (
                RowWriteRequest {
                    operation: "insert".to_string(),
                    table: "nope".to_string(),
                    key: None,
                    values: Some(vec![string_cell("key", "k")]),
                },
                "unknown table: nope",
            ),
        ];
        for (case, expected) in bad.drain(..) {
            let err = parse_row_write(&case).expect_err("must be rejected");
            assert!(
                err.message.contains(expected),
                "payload {case:?}: expected {expected:?}, got {}",
                err.message
            );
        }
    }

    #[test]
    fn parse_row_write_rejects_non_string_cell_kinds() {
        for (label, cells) in [
            (
                "integer",
                vec![RowValue {
                    name: "value".into(),
                    value: value_of_kind("integer"),
                }],
            ),
            (
                "bool",
                vec![RowValue {
                    name: "value".into(),
                    value: value_of_kind("bool"),
                }],
            ),
            (
                "null",
                vec![RowValue {
                    name: "value".into(),
                    value: value_of_kind("null"),
                }],
            ),
            (
                "default",
                vec![RowValue {
                    name: "value".into(),
                    value: value_of_kind("default"),
                }],
            ),
            (
                "object",
                vec![RowValue {
                    name: "value".into(),
                    value: value_of_kind("object"),
                }],
            ),
            (
                "string without payload",
                vec![RowValue {
                    name: "value".into(),
                    value: Value {
                        kind: "string".into(),
                        string: None,
                        bool_: None,
                        integer: None,
                        float: None,
                        bytes: None,
                        decimal: None,
                        timestamp: None,
                        array: None,
                        object: None,
                    },
                }],
            ),
        ] {
            let case = request("update", vec![string_cell("key", "k")], cells);
            let err = parse_row_write(&case).expect_err("must be rejected");
            assert!(
                err.message.contains("must be a string value")
                    || err.message.contains("without a payload"),
                "kind {label}: {}",
                err.message
            );
        }
    }

    #[test]
    fn render_command_round_trips_hostile_tokens() {
        let hostile = [
            "hello world",
            "it's",
            "say \"hi\"",
            r"back\slash",
            "line1\nline2",
            "tab\there",
            "",
            "$HOME",
            "`cmd`",
            "*?[#~=%",
            "päth/ünicode ✓",
        ];
        let statement = render_command(&hostile);
        // The exact executed tokens survive the plugin's own parser...
        assert_eq!(
            parse_statement(&statement).unwrap(),
            Statement::Redis(hostile.iter().map(|s| s.to_string()).collect()),
            "statement: {statement:?}"
        );
        // ...and plain shell_words splitting agrees token for token.
        assert_eq!(
            shell_words::split(&statement).unwrap(),
            hostile,
            "statement: {statement:?}"
        );
    }

    #[test]
    fn render_command_quotes_empty_tokens_explicitly() {
        // `SET '' v NX`: an empty key must stay an empty token. Bare
        // quoting would collapse it into the neighbouring argument and
        // silently change the executed command.
        assert_eq!(render_command(&["SET", "", "v", "NX"]), "SET '' v NX");
        assert_eq!(
            parse_statement("SET '' v NX").unwrap(),
            Statement::Redis(vec!["SET".into(), String::new(), "v".into(), "NX".into()])
        );
        assert_eq!(render_command(&["DEL", ""]), "DEL ''");
    }

    #[test]
    fn render_command_leaves_simple_tokens_bare() {
        assert_eq!(
            render_command(&["SET", "user:2", "v1", "NX"]),
            "SET user:2 v1 NX"
        );
        assert_eq!(render_command(&["DEL", "user:2"]), "DEL user:2");
    }

    #[test]
    fn update_statements_are_exact_eval_of_the_shared_script() {
        // A pure rename reports the exact EVAL of the atomic script, not
        // a plain RENAME whose overwrite semantics would differ.
        let statement =
            render_command(&["EVAL", UPDATE_SCRIPT, "1", "user:2", "user:3", "0", "", ""]);
        assert!(statement.starts_with("EVAL '"), "statement: {statement:?}");
        assert_eq!(
            shell_words::split(&statement).unwrap(),
            vec![
                "EVAL".to_string(),
                UPDATE_SCRIPT.to_string(),
                "1".to_string(),
                "user:2".to_string(),
                "user:3".to_string(),
                "0".to_string(),
                String::new(),
                String::new(),
            ]
        );
        // Hostile rename + value args survive the round trip through the
        // plugin's own parser.
        let hostile = [
            "EVAL",
            UPDATE_SCRIPT,
            "1",
            "user:2",
            "user 3's \"new\"\nname",
            "1",
            "old 'value'",
            r"back\slash",
        ];
        let statement = render_command(&hostile);
        assert_eq!(
            parse_statement(&statement).unwrap(),
            Statement::Redis(hostile.iter().map(|s| s.to_string()).collect()),
            "statement: {statement:?}"
        );
        assert_eq!(shell_words::split(&statement).unwrap(), hostile);
    }

    // --- statement metadata ------------------------------------------------

    fn tokens_of(statement: &str) -> Vec<String> {
        shell_words::split(statement).unwrap()
    }

    #[test]
    fn command_metadata_reads_are_replayable_and_benign() {
        for statement in [
            "PING",
            "GET user:1",
            "MGET user:1 user:2",
            "HGETALL user:1",
            "INFO server",
            "SCAN 0",
            "KEYS *",
            "TTL user:1",
            "get user:1", // case-insensitive
        ] {
            let metadata = command_metadata(&tokens_of(statement));
            assert!(metadata.replayable, "{statement:?} must be replayable");
            assert!(!metadata.sensitive, "{statement:?} must not be sensitive");
            assert_eq!(metadata.language, "redis");
        }
    }

    #[test]
    fn command_metadata_ping_message_is_sensitive_but_bare_ping_is_benign() {
        // Bare PING is a health check: benign and replayable.
        let bare = command_metadata(&tokens_of("PING"));
        assert!(bare.replayable, "bare PING must be replayable");
        assert!(!bare.sensitive, "bare PING must not be sensitive");

        // PING with any argument embeds an arbitrary payload the server
        // echoes back: sensitive, never stored verbatim, never replayed
        // — case-insensitive like every other verb.
        for statement in [
            "PING hunter2",
            "ping secret-message",
            "PING \"hello world\"",
            "PING a b",
        ] {
            let metadata = command_metadata(&tokens_of(statement));
            assert!(!metadata.replayable, "{statement:?} must be non-replayable");
            assert!(metadata.sensitive, "{statement:?} must be sensitive");
        }
    }

    #[test]
    fn command_metadata_ping_discriminates_in_serialized_metadata() {
        // The serialized statement metadata the host consumes must
        // discriminate the two PING shapes: the secret payload is
        // flagged sensitive so the host redacts the text and forces the
        // entry non-replayable, while the bare health check keeps the
        // benign defaults.
        let secret = command_metadata(&tokens_of("PING hunter2"));
        assert_eq!(
            serde_json::to_string(&secret).unwrap(),
            r#"{"language":"redis","replayable":false,"sensitive":true}"#
        );
        let bare = command_metadata(&tokens_of("PING"));
        assert_eq!(
            serde_json::to_string(&bare).unwrap(),
            r#"{"language":"redis","replayable":true,"sensitive":false}"#
        );
    }

    #[test]
    fn read_only_allowlist_arguments_are_locators_or_query_parameters() {
        // Audit guard: every read-only command takes only keys,
        // patterns, numeric ranges/limits, or an INFO section name —
        // never a stored value. PING is exercised separately because
        // its optional message argument is a payload.
        for statement in [
            "GET user:1",
            "MGET user:1 user:2 user:3",
            "EXISTS user:1 user:2",
            "TYPE user:1",
            "TTL user:1",
            "PTTL user:1",
            "DBSIZE",
            "SCAN 0 MATCH user:* COUNT 100 TYPE string",
            "KEYS user:*",
            "HGET user:1 field-name",
            "HGETALL user:1",
            "HLEN user:1",
            "SMEMBERS user:1",
            "SCARD user:1",
            "ZRANGE user:1 0 -1 BYSCORE REV LIMIT 0 10 WITHSCORES",
            "ZCARD user:1",
            "LRANGE user:1 0 -1",
            "LLEN user:1",
            "INFO server",
            "info replication",
        ] {
            let metadata = command_metadata(&tokens_of(statement));
            assert!(
                metadata.replayable && !metadata.sensitive,
                "{statement:?} must stay benign: {metadata:?}"
            );
        }
    }

    #[test]
    fn command_metadata_key_only_writes_are_replayable_and_benign() {
        for statement in [
            "DEL user:1",
            "RENAME user:1 user:2",
            "RENAMENX user:1 user:2",
            "UNLINK user:1",
            "EXPIRE user:1 60",
            "PERSIST user:1",
            "TOUCH user:1",
            "COPY user:1 user:2",
            "FLUSHDB",
            "SELECT 2",
            "WATCH user:1",
        ] {
            let metadata = command_metadata(&tokens_of(statement));
            assert!(metadata.replayable, "{statement:?} must be replayable");
            assert!(!metadata.sensitive, "{statement:?} must not be sensitive");
        }
    }

    #[test]
    fn command_metadata_value_bearing_writes_are_sensitive_and_non_replayable() {
        // The host redacts sensitive statements and forces them
        // non-replayable: secrets never survive in serialized statement
        // metadata paths.
        for statement in [
            "SET user:1 hunter2",
            "SET user:1 hunter2 NX",
            "MSET user:1 a user:2 b",
            "APPEND user:1 more",
            "HSET user:1 field value",
            "LPUSH queue:jobs payload",
            "SADD tags:demo secret-tag",
            "ZADD leaderboard:demo 1.5 member",
            "XADD stream:events * field value",
            "INCRBY counter 5",
            "SETBIT bitmap 7 1",
            "PFADD visitors alice",
            "GEOADD geo 13.36 38.11 palermo",
            "PUBLISH channel secret-message",
            "RESTORE user:1 0 \"payload\"",
            "ECHO secret",
        ] {
            let metadata = command_metadata(&tokens_of(statement));
            assert!(!metadata.replayable, "{statement:?} must be non-replayable");
            assert!(metadata.sensitive, "{statement:?} must be sensitive");
        }
    }

    #[test]
    fn command_metadata_credentials_are_sensitive_and_non_replayable() {
        for statement in [
            "AUTH hunter2",
            "AUTH alice hunter2",
            "HELLO 3 AUTH alice hunter2",
            "ACL SETUSER alice on >hunter2",
            "ACL SETUSER alice ~keys:* +@all",
            "CONFIG SET requirepass hunter2",
            "CONFIG SET masterauth hunter2",
            "MIGRATE 10.0.0.1 6379 user:1 0 1000 AUTH hunter2",
            "MIGRATE 10.0.0.1 6379 \"\" 0 1000 AUTH2 alice hunter2",
            "EVAL \"return redis.call('SET', KEYS[1], ARGV[1])\" 1 k v",
            "SCRIPT LOAD \"return 1\"",
            "FUNCTION LOAD \"#!lua name=mylib\nredis.register_function('f', function() return 1 end)\"",
        ] {
            let metadata = command_metadata(&tokens_of(statement));
            assert!(!metadata.replayable, "{statement:?} must be non-replayable");
            assert!(metadata.sensitive, "{statement:?} must be sensitive");
        }
    }

    #[test]
    fn command_metadata_unknown_commands_default_sensitive() {
        // Unknown/module commands may embed payloads: the conservative
        // default is sensitive and non-replayable unless explicitly
        // classified safe.
        for statement in [
            "JSON.SET user:1 $ \"{\\\"a\\\":1}\"",
            "FT.CREATE idx ON HASH PREFIX 1 doc:",
            "BF.ADD bloom member",
            "TS.ADD sensor:1 * 42",
            "XINFO STREAM s",
            "OBJECT ENCODING user:1",
            "DUMP user:1",
            "HELLO 3",
        ] {
            let metadata = command_metadata(&tokens_of(statement));
            assert!(!metadata.replayable, "{statement:?} must be non-replayable");
            assert!(metadata.sensitive, "{statement:?} must default sensitive");
        }
    }

    #[test]
    fn classification_lists_are_disjoint_and_reads_stay_benign() {
        for verb in SENSITIVE_COMMANDS {
            assert!(
                !is_read_only(verb) && !SAFE_WRITE_COMMANDS.contains(verb),
                "{verb} must live in exactly one classification"
            );
        }
        for verb in SAFE_WRITE_COMMANDS {
            assert!(
                !is_read_only(verb) && !SENSITIVE_COMMANDS.contains(verb),
                "{verb} must live in exactly one classification"
            );
        }
        for verb in READ_ONLY_COMMANDS {
            assert!(
                !SENSITIVE_COMMANDS.contains(&verb) && !SAFE_WRITE_COMMANDS.contains(&verb),
                "{verb} must live in exactly one classification"
            );
        }
    }

    #[test]
    fn select_statement_is_the_exact_replayable_pseudo_command() {
        assert_eq!(select_statement(0, None), r#"SELECT * FROM "keys""#);
        assert_eq!(
            select_statement(0, Some(25)),
            r#"SELECT * FROM "keys" LIMIT 25 OFFSET 0"#
        );
        // The host-generated form with both clauses parses to exactly
        // the browse the plugin replays.
        assert_eq!(
            parse_statement(r#"SELECT * FROM "keys" LIMIT 25 OFFSET 0"#).unwrap(),
            Statement::SelectKeys {
                offset: 0,
                limit: Some(25)
            }
        );
        assert_eq!(
            select_statement(5, Some(25)),
            r#"SELECT * FROM "keys" LIMIT 25 OFFSET 5"#
        );
        assert_eq!(
            select_statement(5, None),
            r#"SELECT * FROM "keys" OFFSET 5"#
        );
        // Every pseudo-command round-trips through the plugin's own
        // parser as the same virtual browse.
        for (offset, limit) in [(0, None), (0, Some(25)), (5, Some(25)), (5, None)] {
            let statement = select_statement(offset, limit);
            assert_eq!(
                parse_statement(&statement).unwrap(),
                Statement::SelectKeys { offset, limit },
                "statement: {statement:?}"
            );
        }
    }

    #[test]
    fn advertised_query_language_examples_all_parse() {
        // Every example the initialize handshake advertises must be
        // accepted by the plugin's own parser.
        let capabilities = crate::server::redis_capabilities();
        let examples = capabilities
            .query_language
            .as_ref()
            .expect("query_language must be advertised")
            .examples
            .clone()
            .expect("examples must be advertised");
        assert!(
            (2..=4).contains(&examples.len()),
            "2-4 examples expected, got {}",
            examples.len()
        );
        for example in &examples {
            parse_statement(example)
                .unwrap_or_else(|e| panic!("advertised example {example:?} must parse: {e:?}"));
        }
    }

    #[test]
    fn advertised_command_catalog_is_valid() {
        // The static completion catalog the handshake advertises must
        // cover the required conservative surface, stay within the host
        // bounds (512 entries; 64/256/256 runes for name/usage/summary),
        // carry no control characters, and keep names unique
        // case-insensitively — the host rejects anything else, so this
        // test pins what the editor will actually see.
        let capabilities = crate::server::redis_capabilities();
        let commands = capabilities
            .query_language
            .as_ref()
            .expect("query_language must be advertised")
            .commands
            .clone()
            .expect("commands must be advertised");

        let required = [
            "PING", "TYPE", "GET", "SET", "DEL", "EXISTS", "HGETALL", "HGET", "HSET", "LRANGE",
            "LPUSH", "SMEMBERS", "SADD", "ZRANGE", "ZADD", "SCAN", "SELECT",
        ];
        let mut by_name: std::collections::HashMap<String, &QueryCommand> =
            std::collections::HashMap::new();
        for command in &commands {
            assert!(
                !command.name.trim().is_empty(),
                "command name must be nonblank"
            );
            assert!(
                !command.usage.trim().is_empty(),
                "usage of {} must be nonblank",
                command.name
            );
            assert!(
                !command.summary.trim().is_empty(),
                "summary of {} must be nonblank",
                command.name
            );
            assert!(
                command.name.chars().count() <= 64,
                "name {} exceeds the 64-rune bound",
                command.name
            );
            assert!(
                command.usage.chars().count() <= 256,
                "usage of {} exceeds the 256-rune bound",
                command.name
            );
            assert!(
                command.summary.chars().count() <= 256,
                "summary of {} exceeds the 256-rune bound",
                command.name
            );
            for (label, text) in [
                ("name", command.name.as_str()),
                ("usage", command.usage.as_str()),
                ("summary", command.summary.as_str()),
            ] {
                assert!(
                    !text.chars().any(char::is_control),
                    "{label} of {} contains a control character",
                    command.name
                );
            }
            let key = command.name.to_ascii_lowercase();
            assert!(
                by_name.insert(key, command).is_none(),
                "command name {} repeats case-insensitively",
                command.name
            );
        }
        assert!(
            commands.len() <= 512,
            "catalog has {} entries, over the 512 cap",
            commands.len()
        );

        for name in required {
            assert!(
                by_name.contains_key(&name.to_ascii_lowercase()),
                "catalog must advertise {name}"
            );
        }
        let select = by_name["select"];
        assert!(
            select.usage.contains("SELECT * FROM"),
            "SELECT usage must cover the virtual-table syntax: {}",
            select.usage
        );
        // The virtual-table form in the usage is the exact grammar the
        // plugin parses (minus the bracket placeholders).
        for example in [
            "SELECT * FROM \"keys\" LIMIT 25",
            "SELECT * FROM \"keys\" OFFSET 5 LIMIT 10",
        ] {
            parse_statement(example)
                .unwrap_or_else(|e| panic!("catalog SELECT form {example:?} must parse: {e:?}"));
        }
    }

    // --- error classification ---------------------------------------------

    #[test]
    fn map_redis_error_classifies_auth_connection_and_operation() {
        // AUTH handshake failure: the crate's dedicated kind.
        let auth: redis::RedisError =
            (redis::ErrorKind::AuthenticationFailed, "password mismatch").into();
        let err = map_redis_error(&auth, "connect failed: ");
        assert_eq!(err.kind, ErrorKind::Authentication);
        assert!(
            err.message.starts_with("connect failed: "),
            "the safe prefix is preserved: {}",
            err.message
        );

        // A server NOAUTH/WRONGPASS reply: extension code in the text.
        let noauth: redis::RedisError = (
            redis::ErrorKind::Extension,
            "NOAUTH Authentication required.",
        )
            .into();
        assert_eq!(
            map_redis_error(&noauth, "redis: ").kind,
            ErrorKind::Authentication
        );
        let wrongpass: redis::RedisError = (
            redis::ErrorKind::Extension,
            "WRONGPASS invalid username-password pair",
        )
            .into();
        assert_eq!(
            map_redis_error(&wrongpass, "redis: ").kind,
            ErrorKind::Authentication
        );

        // Connection refused / dropped connection: I/O.
        let io: redis::RedisError = (redis::ErrorKind::Io, "connection refused").into();
        assert_eq!(
            map_redis_error(&io, "connect failed: ").kind,
            ErrorKind::Connection
        );

        // Unparseable client config (bad URL): validation.
        let config: redis::RedisError = (
            redis::ErrorKind::InvalidClientConfig,
            "Redis URL did not parse",
        )
            .into();
        assert_eq!(
            map_redis_error(&config, "invalid target: ").kind,
            ErrorKind::Validation
        );

        // A plain server command error stays an operation error, never
        // protocol or plugin_crash.
        let response: redis::RedisError = (
            redis::ErrorKind::Server(redis::ServerErrorKind::ResponseError),
            "ERR unknown command 'FOO'",
        )
            .into();
        assert_eq!(
            map_redis_error(&response, "redis: ").kind,
            ErrorKind::Operation
        );

        // Client-side misuse also falls back to operation.
        let client: redis::RedisError = (redis::ErrorKind::Client, "client misuse").into();
        assert_eq!(
            map_redis_error(&client, "redis: ").kind,
            ErrorKind::Operation
        );
    }

    // --- WRONGTYPE advisory guidance --------------------------------------

    #[test]
    fn wrongtype_verb_maps_families_and_rejects_ambiguous_commands() {
        assert_eq!(wrongtype_verb("GET"), Some("strings"));
        assert_eq!(wrongtype_verb("SET"), Some("strings"));
        assert_eq!(wrongtype_verb("HGETALL"), Some("hashes"));
        assert_eq!(wrongtype_verb("LRANGE"), Some("lists"));
        assert_eq!(wrongtype_verb("SMEMBERS"), Some("sets"));
        assert_eq!(wrongtype_verb("ZRANGE"), Some("sorted sets"));
        // Multi-key and keyless commands get no guidance.
        for verb in [
            "MGET", "DEL", "EXISTS", "MSET", "KEYS", "SCAN", "DBSIZE", "PING", "INFO", "FLUSHDB",
            "EVAL", "SORT", "BITOP", "TTL", "EXPIRE", "TYPE", "OBJECT",
        ] {
            assert_eq!(wrongtype_verb(verb), None, "{verb} must not get guidance");
        }
    }

    #[test]
    fn wrongtype_key_requires_a_listed_command_and_a_blank_key() {
        let tokens = vec!["get".to_string(), "user:1".to_string()];
        assert_eq!(wrongtype_key("GET", &tokens), Some("user:1"));
        // The key argument is the first token after the verb.
        assert_eq!(
            wrongtype_key(
                "LRANGE",
                &[
                    "LRANGE".to_string(),
                    "k".to_string(),
                    "0".to_string(),
                    "-1".to_string()
                ]
            ),
            Some("k")
        );
        // Missing key, blank key, and unlisted commands yield None.
        assert_eq!(wrongtype_key("GET", &["GET".to_string()]), None);
        assert_eq!(
            wrongtype_key("GET", &["GET".to_string(), "".to_string()]),
            None
        );
        assert_eq!(
            wrongtype_key("MGET", &["MGET".to_string(), "user:1".to_string()]),
            None
        );
    }

    #[test]
    fn type_read_command_maps_the_five_types_and_nothing_else() {
        for (actual, command, article) in [
            ("string", "GET", "a string"),
            ("hash", "HGETALL", "a hash"),
            ("list", "LRANGE", "a list"),
            ("set", "SMEMBERS", "a set"),
            ("zset", "ZRANGE", "a sorted set"),
        ] {
            let (got_command, got_article) =
                type_read_command(actual).unwrap_or_else(|| panic!("{actual} must map"));
            assert_eq!(got_command, command);
            assert_eq!(got_article, article);
        }
        for actual in ["none", "stream", "module", "json"] {
            assert_eq!(type_read_command(actual), None, "{actual} must not map");
        }
    }

    #[test]
    fn wrongtype_suggestions_render_through_the_command_renderer() {
        // Full round-trip through the parser: each suggestion must parse
        // back to exactly the command it describes, and hostile keys
        // stay valid shell-quoted tokens.
        for (actual, key, want) in [
            ("string", "user:1", "GET user:1"),
            ("hash", "user:1", "HGETALL user:1"),
            ("list", "user:1", "LRANGE user:1 0 -1"),
            ("set", "user:1", "SMEMBERS user:1"),
            ("zset", "user:1", "ZRANGE user:1 0 -1 WITHSCORES"),
            ("hash", "odd key", "HGETALL 'odd key'"),
            ("hash", "it's", "HGETALL 'it'\\''s'"),
        ] {
            let (read_command, _) = type_read_command(actual).unwrap();
            let suggested = match read_command {
                "LRANGE" => render_command(&[read_command, key, "0", "-1"]),
                "ZRANGE" => render_command(&[read_command, key, "0", "-1", "WITHSCORES"]),
                _ => render_command(&[read_command, key]),
            };
            assert_eq!(suggested, want, "{actual} key {key:?}");
            // The suggestion round-trips through the plugin's parser.
            let tokens = shell_words::split(&suggested).expect("suggestion must parse");
            assert_eq!(tokens[0], read_command);
            assert_eq!(tokens[1], key);
        }
    }

    // --- workspace views --------------------------------------------------

    #[test]
    fn advertised_workspace_keeps_columns_and_omits_relational_tabs() {
        // The workspace advertisement pins the exact wire shape: the
        // Columns standard tab only (a key-value store has no indexes,
        // foreign keys, or diagram) plus the driver-owned `server` view
        // for database and table scopes.
        let capabilities = crate::server::redis_capabilities();
        let workspace = capabilities
            .workspace
            .as_ref()
            .expect("workspace must be advertised");
        let wire = serde_json::to_value(workspace).unwrap();
        assert_eq!(
            wire,
            serde_json::json!({
                "standard_tabs": ["columns"],
                "custom_views": [
                    {"id": "server", "label": "Server", "scopes": ["database", "table"]}
                ]
            }),
            "workspace advertisement must match the contract exactly"
        );
        let text = serde_json::to_string(workspace).unwrap();
        for forbidden in ["indexes", "foreign_keys", "diagram"] {
            assert!(
                !text.contains(forbidden),
                "advertisement must not contain {forbidden:?}: {text}"
            );
        }
        // The view id/label/scopes satisfy the host's registration
        // bounds: nonblank, control-free, bounded runes, unique, valid
        // scopes without duplicates.
        for view in workspace.custom_views.as_ref().unwrap() {
            assert!(!view.id.trim().is_empty() && view.id.chars().count() <= 64);
            assert!(!view.label.trim().is_empty() && view.label.chars().count() <= 32);
            assert!(!view.id.chars().any(char::is_control));
            assert!(!view.label.chars().any(char::is_control));
            assert!(view.scopes.len() <= 3);
        }
    }

    #[test]
    fn parse_info_section_takes_only_allowlisted_fields() {
        // A hostile INFO server reply: allowlisted fields survive,
        // everything else — credentials, config, paths, command lines,
        // module fields — is dropped before it can reach the wire.
        let reply = "\
# Server\r
redis_version:7.4.7\r
redis_mode:standalone\r
arch_bits:64\r
uptime_in_seconds:1234\r
uptime_in_days:0\r
tcp_port:6380\r
config_file:/etc/redis/redis.conf\r
executable:/usr/local/bin/redis-server\r
run_id:0123456789abcdef\r
process_id:42\r
os:Linux 5.15.0\r
server_name:secret-host\r
\r
# Modules\r
module:name=mysecretmodule,ver=1\r
";
        let fields = parse_info_section(reply, "server", &SERVER_VIEW_SERVER_FIELDS);
        assert_eq!(
            fields,
            std::collections::HashMap::from([
                ("server.redis_version".to_string(), "7.4.7".to_string()),
                ("server.redis_mode".to_string(), "standalone".to_string()),
                ("server.arch_bits".to_string(), "64".to_string()),
                ("server.uptime_in_seconds".to_string(), "1234".to_string()),
                ("server.uptime_in_days".to_string(), "0".to_string()),
                ("server.tcp_port".to_string(), "6380".to_string()),
            ]),
            "only allowlisted server fields may survive"
        );
        // The memory section keeps runtime metrics and drops policy.
        let memory = parse_info_section(
            "\
used_memory:1048576\r
used_memory_human:1.00M\r
maxmemory:0\r
maxmemory_human:0B\r
maxmemory_policy:noeviction\r
mem_fragmentation_ratio:1.05\r
mem_allocator:jemalloc-5.3.0\r
",
            "memory",
            &SERVER_VIEW_MEMORY_FIELDS,
        );
        assert!(
            !memory.contains_key("memory.maxmemory_policy"),
            "configuration policy fields must never be exposed"
        );
        assert_eq!(memory["memory.mem_fragmentation_ratio"], "1.05");
        assert!(
            !memory.contains_key("memory.mem_allocator"),
            "allocator detail is not on the allowlist"
        );
        assert_eq!(memory["memory.maxmemory"], "0");
    }

    #[test]
    fn parse_keyspace_line_reports_the_selected_database_only() {
        let reply = "\
# Keyspace\r
db0:keys=1,expires=0,avg_ttl=0\r
db2:keys=6,expires=2,avg_ttl=12345\r
db7:keys=99,expires=99,avg_ttl=99999\r
";
        let fields = parse_keyspace_line(reply, 2);
        assert_eq!(
            fields,
            std::collections::HashMap::from([
                ("keyspace.keys".to_string(), "6".to_string()),
                ("keyspace.expires".to_string(), "2".to_string()),
                ("keyspace.avg_ttl".to_string(), "12345".to_string()),
            ]),
            "only the selected database's line may be reported"
        );
        // An empty database has no dbN line: no keyspace fields at all.
        assert!(
            parse_keyspace_line(reply, 3).is_empty(),
            "an absent dbN line must yield no keyspace fields"
        );
        // Unknown fields on the line are dropped.
        assert!(!parse_keyspace_line(reply, 2).contains_key("keyspace.something"));
    }
}
