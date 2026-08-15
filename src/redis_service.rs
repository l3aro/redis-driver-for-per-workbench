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
//! over the selected logical database; the service never advertises row
//! or document writes.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::dto::capabilities::FormValues;
use crate::dto::request::{
    AddColumnRequest, BrowseTableRequest, BuildTargetResult, ColumnChangeRequest, DropRequest,
    EmptyRequest, ForeignKeyChangeRequest, IndexChangeRequest, ReplaceForeignKeyRequest,
    ReplaceIndexRequest, StatementRequest, TableRequest,
};
use crate::dto::service::{
    ColumnInfo, DatabaseInfo, ForeignKeyInfo, IndexInfo, QueryResult, ReferencingForeignKeyInfo,
    SchemaObject,
};
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
        return Err(ServiceError::new("empty statement"));
    }
    let tokens = shell_words::split(statement)
        .map_err(|e| ServiceError::new(format!("invalid statement: {e}")))?;
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
    let bad = |message: String| ServiceError::new(format!("invalid statement: {message}"));
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

/// Caps one display cell at [`MAX_CELL`] Unicode scalar values, appending
/// an ellipsis (U+2026) when truncated.
fn cap_cell(s: &str) -> String {
    if s.chars().count() > MAX_CELL {
        let mut out: String = s.chars().take(MAX_CELL).collect();
        out.push('\u{2026}');
        out
    } else {
        s.to_string()
    }
}

/// Builds the wire Result: display rows capped at [`MAX_ROWS`] with cells
/// capped at [`MAX_CELL`], full values preserved in `untruncated_rows`.
fn finalize(
    columns: Vec<String>,
    full_rows: Vec<Vec<Option<String>>>,
    duration: Duration,
    has_more: bool,
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
            Err(ServiceError::new("invalid target: missing redis:// URI"))
        } else {
            Ok(rest.to_string())
        }
    } else {
        Err(ServiceError::new("invalid target: expected a redis:// URI"))
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
        _ => Err(ServiceError::new(format!(
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

    /// Runs one command and maps Redis errors to operation errors. A
    /// closed session rejects commands without touching the network.
    async fn query<T: redis::FromRedisValue>(&self, cmd: &redis::Cmd) -> Result<T, ServiceError> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(ServiceError::new("session closed"));
        }
        let mut guard = self.conn.lock().await;
        let conn = guard
            .as_mut()
            .ok_or_else(|| ServiceError::new("session closed"))?;
        cmd.query_async::<T>(conn)
            .await
            .map_err(|e| ServiceError::new(format!("redis: {e}")))
    }

    /// Forwards one parsed statement as a raw `redis::Cmd` and returns
    /// the elapsed duration alongside the reply.
    async fn run_command(
        &self,
        tokens: &[String],
    ) -> Result<(Duration, redis::Value), ServiceError> {
        let mut cmd = redis::Cmd::new();
        for token in tokens {
            cmd.arg(token.as_str());
        }
        let start = Instant::now();
        let reply = self.query::<redis::Value>(&cmd).await;
        let elapsed = start.elapsed();
        reply.map(|value| (elapsed, value))
    }

    /// Executes a virtual-table SELECT over the fixed `keys` table,
    /// paged exactly like `browse_table`: sorted keys with type and
    /// bounded value previews, `has_more` when the page is not the
    /// last, display rows capped at [`MAX_ROWS`].
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
                        return Err(ServiceError::new("empty statement"));
                    }
                    let (duration, reply) = this.run_command(&tokens).await?;
                    let (columns, full_rows) = reply_rows(&reply);
                    let truncated = full_rows.len() > MAX_ROWS;
                    Ok(finalize(columns, full_rows, duration, truncated))
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
                        return Err(ServiceError::new("empty statement"));
                    }
                    let verb = tokens[0].to_ascii_uppercase();
                    if !is_read_only(&verb) {
                        return Err(ServiceError::new(format!(
                            "read-only: {verb} is not allowed"
                        )));
                    }
                    let (duration, reply) = this.run_command(&tokens).await?;
                    let (columns, full_rows) = reply_rows(&reply);
                    let truncated = full_rows.len() > MAX_ROWS;
                    Ok(finalize(columns, full_rows, duration, truncated))
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
            Ok(vec![SchemaObject {
                database: format!("db{}", this.database),
                type_: "table".to_string(),
                name: KEYS_TABLE.to_string(),
                row_count: Some(dbsize.max(0) as u64),
            }])
        })
    }

    fn table_info(
        &self,
        request: TableRequest,
        _cancel: CancellationToken,
    ) -> ServiceFuture<'static, Vec<ColumnInfo>> {
        Box::pin(async move {
            if request.table != KEYS_TABLE {
                return Err(ServiceError::new(format!(
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
                return Err(ServiceError::new(format!(
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
                return Err(ServiceError::new(format!(
                    "unknown table: {}",
                    request.table
                )));
            }
            let offset = request.options.offset.unwrap_or(0) as usize;
            let limit = request.options.limit.map(|l| l as usize);
            this.run_select(offset, limit).await
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
    ServiceError::new("Redis keys has a fixed virtual schema")
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
            let parsed =
                Url::parse(&uri).map_err(|e| ServiceError::new(format!("invalid target: {e}")))?;
            if parsed.scheme() != "redis" {
                return Err(ServiceError::new(
                    "invalid target: only plain TCP redis:// targets are supported (no TLS)",
                ));
            }
            if parsed
                .query_pairs()
                .any(|(k, v)| k == "tls" && matches!(v.as_ref(), "true" | "1" | "secure"))
            {
                return Err(ServiceError::new(
                    "invalid target: only plain TCP redis:// targets are supported (no TLS)",
                ));
            }
            let database = parse_database(&parsed)?;

            let client = redis::Client::open(uri)
                .map_err(|e| ServiceError::new(format!("invalid target: {e}")))?;
            let mut manager = redis::aio::ConnectionManager::new(client)
                .await
                .map_err(|e| ServiceError::new(format!("connect failed: {e}")))?;

            let info: String = redis::cmd("INFO")
                .arg("server")
                .query_async(&mut manager)
                .await
                .map_err(|e| ServiceError::new(format!("connect failed: {e}")))?;
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

    #[test]
    fn build_target_uses_defaults_for_blank_fields() {
        let factory = RedisFactory::default();
        let result = factory.build_target(&FormValues::default());
        assert!(result.ok);
        assert_eq!(result.target, "redis:redis://127.0.0.1:6379/0");
    }

    #[test]
    fn build_target_trims_and_applies_form_values() {
        let mut values = FormValues::default();
        values.host = Some("  db.example.com  ".to_string());
        values.port = Some(" 6380 ".to_string());
        values.database = Some(" 2 ".to_string());
        let result = RedisFactory::default().build_target(&values);
        assert!(result.ok);
        assert_eq!(result.target, "redis:redis://db.example.com:6380/2");
    }

    #[test]
    fn build_target_percent_encodes_credentials() {
        let mut values = FormValues::default();
        values.user = Some("alice".to_string());
        values.pass = Some("p@ss:w/rd".to_string());
        let result = RedisFactory::default().build_target(&values);
        assert!(result.ok);
        assert_eq!(
            result.target,
            "redis:redis://alice:p%40ss%3Aw%2Frd@127.0.0.1:6379/0"
        );
    }

    #[test]
    fn build_target_omits_blank_credentials() {
        let mut values = FormValues::default();
        values.user = Some("  ".to_string());
        values.pass = Some("".to_string());
        let result = RedisFactory::default().build_target(&values);
        assert!(result.ok);
        assert_eq!(result.target, "redis:redis://127.0.0.1:6379/0");
    }

    #[test]
    fn build_target_brackets_ipv6_hosts() {
        let mut values = FormValues::default();
        values.host = Some("::1".to_string());
        let result = RedisFactory::default().build_target(&values);
        assert!(result.ok);
        assert_eq!(result.target, "redis:redis://[::1]:6379/0");
    }

    #[test]
    fn build_target_rejects_invalid_database() {
        for bad in ["abc", "-1", "1.5", "0x10", "1_000"] {
            let mut values = FormValues::default();
            values.database = Some(bad.to_string());
            let result = RedisFactory::default().build_target(&values);
            assert!(!result.ok, "database {bad:?} must be rejected");
        }
    }

    #[test]
    fn build_target_rejects_invalid_port() {
        for bad in ["0", "70000", "abc", "-1"] {
            let mut values = FormValues::default();
            values.port = Some(bad.to_string());
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
        assert_eq!(capped.chars().count(), 301);
        assert!(capped.ends_with('\u{2026}'));
        assert_eq!(cap_cell("short"), "short");
        let exactly = "y".repeat(300);
        assert_eq!(cap_cell(&exactly), exactly);
    }
}
