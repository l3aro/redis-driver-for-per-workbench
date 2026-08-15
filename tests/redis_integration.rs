//! Integration tests against a live Redis server.
//!
//! These tests require the `REDIS_URL` environment variable to point at a
//! disposable Redis instance, e.g.
//! `redis://:workbench-demo@127.0.0.1:6380/2`. When `REDIS_URL` is unset
//! every test skips. Each test runs inside a global lock and flushes the
//! selected logical database first, so runs are deterministic and
//! isolated from each other.

use std::sync::LazyLock;

use redis::AsyncCommands;
use tokio::sync::Mutex as AsyncMutex;
use tokio_util::sync::CancellationToken;

use perk_redis::dto::request::{BrowseTableRequest, EmptyRequest, StatementRequest, TableRequest};
use perk_redis::dto::service::{BrowseOptions, DatabaseInfo, QueryResult, StatementMetadata};
use perk_redis::dto::write::{RowValue, RowWriteRequest, RowWriteResponse, Value};
use perk_redis::protocol::ErrorKind;
use perk_redis::redis_service::RedisFactory;
use perk_redis::service::{ServiceError, SessionFactory, SessionService};

fn redis_url() -> Option<String> {
    std::env::var("REDIS_URL")
        .ok()
        .filter(|v| !v.trim().is_empty())
}

/// The logical database encoded in REDIS_URL's path (default 0).
fn database_from_url(url: &str) -> i64 {
    let parsed = url::Url::parse(url).expect("REDIS_URL must parse as a URL");
    parsed.path().trim_matches('/').parse().unwrap_or(0)
}

/// Serializes integration tests so FLUSHDB-based isolation is sound.
static SERIAL: LazyLock<AsyncMutex<()>> = LazyLock::new(|| AsyncMutex::new(()));

async fn open_session(url: &str) -> (DatabaseInfo, Box<dyn SessionService>) {
    RedisFactory::default()
        .open(url)
        .await
        .expect("open must succeed against the test server")
}

async fn exec_async(
    svc: &dyn SessionService,
    statement: &str,
) -> Result<QueryResult, ServiceError> {
    svc.execute(
        StatementRequest {
            statement: statement.to_string(),
        },
        CancellationToken::new(),
    )
    .await
}

async fn exec_ok(svc: &dyn SessionService, statement: &str) -> QueryResult {
    exec_async(svc, statement)
        .await
        .unwrap_or_else(|e| panic!("execute {statement:?} failed: {e:?}"))
}

async fn read_only(svc: &dyn SessionService, statement: &str) -> Result<QueryResult, ServiceError> {
    svc.execute_read_only(
        StatementRequest {
            statement: statement.to_string(),
        },
        CancellationToken::new(),
    )
    .await
}

async fn validate(svc: &dyn SessionService, statement: &str) -> Result<(), ServiceError> {
    svc.validate(
        StatementRequest {
            statement: statement.to_string(),
        },
        CancellationToken::new(),
    )
    .await
}

fn str_val(s: &str) -> Value {
    Value {
        kind: "string".to_string(),
        string: Some(s.to_string()),
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

fn cell(name: &str, value: &str) -> RowValue {
    RowValue {
        name: name.to_string(),
        value: str_val(value),
    }
}

fn insert_req(key: &str, value: &str) -> RowWriteRequest {
    RowWriteRequest {
        operation: "insert".to_string(),
        table: "keys".to_string(),
        key: None,
        values: Some(vec![cell("key", key), cell("value", value)]),
    }
}

fn update_req(key: &str, changes: Vec<RowValue>) -> RowWriteRequest {
    RowWriteRequest {
        operation: "update".to_string(),
        table: "keys".to_string(),
        key: Some(vec![cell("key", key)]),
        values: Some(changes),
    }
}

fn delete_req(key: &str) -> RowWriteRequest {
    RowWriteRequest {
        operation: "delete".to_string(),
        table: "keys".to_string(),
        key: Some(vec![cell("key", key)]),
        values: None,
    }
}

async fn row_write(
    svc: &dyn SessionService,
    request: RowWriteRequest,
) -> Result<RowWriteResponse, ServiceError> {
    svc.row_write(request, CancellationToken::new()).await
}

async fn row_write_ok(svc: &dyn SessionService, request: RowWriteRequest) -> u64 {
    row_write(svc, request)
        .await
        .unwrap_or_else(|e| panic!("row_write failed: {e:?}"))
        .result
        .rows_affected
}

/// Runs a row write and returns `(rows_affected, statement)`, where
/// `statement` is the native Redis command the host logs for it.
async fn row_write_statement(svc: &dyn SessionService, request: RowWriteRequest) -> (u64, String) {
    let response = row_write(svc, request)
        .await
        .unwrap_or_else(|e| panic!("row_write failed: {e:?}"));
    (response.result.rows_affected, response.result.statement)
}

/// Asserts an update statement is the exact EVAL of the shared atomic
/// script with the given keys and arguments: shell-parseable into
/// `EVAL <script> 1 <key> <dst> <want> <expected> <new_value>` with the
/// real atomic script embedded, never a simpler substitute whose
/// effects (e.g. overwriting a colliding destination) would differ.
fn assert_update_statement(
    statement: &str,
    key: &str,
    dst: &str,
    want: &str,
    expected: &str,
    new_value: &str,
) {
    assert!(statement.starts_with("EVAL "), "statement: {statement:?}");
    let tokens = shell_words::split(statement).expect("statement must be shell-parseable");
    assert_eq!(tokens.len(), 8, "statement: {statement:?}");
    assert_eq!(tokens[0], "EVAL", "statement: {statement:?}");
    assert!(
        tokens[1].contains("local src = KEYS[1]")
            && tokens[1].contains("redis.call('RENAME', src, dst)"),
        "embedded script must be the shared atomic update script, got: {}",
        tokens[1]
    );
    assert_eq!(tokens[2], "1", "statement: {statement:?}");
    assert_eq!(
        &tokens[3..],
        [key, dst, want, expected, new_value],
        "statement: {statement:?}"
    );
}

#[tokio::test]
async fn open_reports_redis_info_and_selects_the_database() {
    let Some(url) = redis_url() else {
        eprintln!("skipping: REDIS_URL is not set");
        return;
    };
    let _guard = SERIAL.lock().await;
    let (info, svc) = open_session(&url).await;

    assert_eq!(info.product, "Redis");
    assert_ne!(
        info.version, "unknown",
        "INFO server must expose redis_version"
    );

    exec_ok(&*svc, "FLUSHDB").await;

    // The selected logical database is recorded in the schema name.
    let schema = svc
        .list_schema(EmptyRequest {}, CancellationToken::new())
        .await
        .expect("list_schema must succeed");
    assert_eq!(schema.len(), 1);
    assert_eq!(schema[0].database, format!("db{}", database_from_url(&url)));
    assert_eq!(schema[0].type_, "table");
    assert_eq!(schema[0].name, "keys");
    assert_eq!(schema[0].row_count, Some(0));

    // A round trip proves the authenticated database is really selected.
    exec_ok(&*svc, "SET selection-probe v").await;
    let row_count = exec_ok(&*svc, "DBSIZE").await;
    assert_eq!(row_count.rows, vec![vec![Some("1".to_string())]]);

    svc.close();
}

#[tokio::test]
async fn validate_checks_parse_only_and_never_touches_redis() {
    let Some(url) = redis_url() else {
        eprintln!("skipping: REDIS_URL is not set");
        return;
    };
    let _guard = SERIAL.lock().await;
    let (_info, svc) = open_session(&url).await;
    exec_ok(&*svc, "FLUSHDB").await;

    // A parseable statement validates without any Redis I/O: the key is
    // never created. Even a Redis-invalid command parses fine.
    validate(&*svc, "SET validate-probe v")
        .await
        .expect("valid statement must validate");
    validate(&*svc, "THISCOMMANDDOESNOTEXIST x")
        .await
        .expect("parseable statements validate without Redis");
    let got = exec_ok(&*svc, "GET validate-probe").await;
    assert_eq!(got.rows, vec![vec![None]], "validate must not mutate state");

    // Blank and malformed statements are operation errors.
    for bad in ["", "   ", "SET unclosed 'quote", "\"dangling"] {
        let err = validate(&*svc, bad).await.expect_err("must be rejected");
        assert!(
            !err.message.is_empty() && err.code.is_none(),
            "operation error expected: {err:?}"
        );
    }

    svc.close();
}

#[tokio::test]
async fn quoted_arguments_are_parsed_and_preserved() {
    let Some(url) = redis_url() else {
        eprintln!("skipping: REDIS_URL is not set");
        return;
    };
    let _guard = SERIAL.lock().await;
    let (_info, svc) = open_session(&url).await;
    exec_ok(&*svc, "FLUSHDB").await;

    exec_ok(&*svc, "SET quoted-key \"hello world\"").await;
    let got = exec_ok(&*svc, "GET quoted-key").await;
    assert_eq!(got.rows, vec![vec![Some("hello world".to_string())]]);

    exec_ok(&*svc, "SET quoted-single 'single quoted'").await;
    let got = exec_ok(&*svc, "GET quoted-single").await;
    assert_eq!(got.rows, vec![vec![Some("single quoted".to_string())]]);

    // A quoted empty argument survives as an empty value, not a dropped arg.
    exec_ok(&*svc, "SET quoted-empty \"\"").await;
    let got = exec_ok(&*svc, "GET quoted-empty").await;
    assert_eq!(got.rows, vec![vec![Some(String::new())]]);

    svc.close();
}

#[tokio::test]
async fn replies_convert_exactly_for_set_get_arrays_and_maps() {
    let Some(url) = redis_url() else {
        eprintln!("skipping: REDIS_URL is not set");
        return;
    };
    let _guard = SERIAL.lock().await;
    let (_info, svc) = open_session(&url).await;
    exec_ok(&*svc, "FLUSHDB").await;

    // Status reply: one "value" row.
    let set = exec_ok(&*svc, "SET conv-k v").await;
    assert_eq!(set.columns, vec!["value"]);
    assert_eq!(set.rows, vec![vec![Some("OK".to_string())]]);
    assert_eq!(set.rows, set.untruncated_rows);
    assert!(!set.truncated && !set.has_more);
    assert!(set.duration_ns > 0, "duration_ns must be measured");

    // Scalar and null replies.
    let got = exec_ok(&*svc, "GET conv-k").await;
    assert_eq!(got.rows, vec![vec![Some("v".to_string())]]);
    let missing = exec_ok(&*svc, "GET conv-missing").await;
    assert_eq!(missing.rows, vec![vec![None]]);
    assert_eq!(missing.untruncated_rows, vec![vec![None]]);

    // Integer reply.
    let incr = exec_ok(&*svc, "INCR conv-n").await;
    assert_eq!(incr.rows, vec![vec![Some("1".to_string())]]);

    // Array reply: index, value rows.
    exec_ok(&*svc, "RPUSH conv-list a b c").await;
    let list = exec_ok(&*svc, "LRANGE conv-list 0 -1").await;
    assert_eq!(list.columns, vec!["index", "value"]);
    assert_eq!(
        list.rows,
        vec![
            vec![Some("0".to_string()), Some("a".to_string())],
            vec![Some("1".to_string()), Some("b".to_string())],
            vec![Some("2".to_string()), Some("c".to_string())],
        ]
    );

    // HGETALL over the default RESP2 connection arrives as a flat
    // field/value array: index, value rows. (With a `?protocol=3` URL
    // the server sends a map and the conversion yields key, value rows;
    // both shapes are covered by unit tests.)
    exec_ok(&*svc, "HSET conv-h f1 v1 f2 v2").await;
    let hash = exec_ok(&*svc, "HGETALL conv-h").await;
    assert_eq!(hash.columns, vec!["index", "value"]);
    assert_eq!(
        hash.rows,
        vec![
            vec![Some("0".to_string()), Some("f1".to_string())],
            vec![Some("1".to_string()), Some("v1".to_string())],
            vec![Some("2".to_string()), Some("f2".to_string())],
            vec![Some("3".to_string()), Some("v2".to_string())],
        ]
    );

    // Nested arrays stringify as compact JSON: the SCAN reply is
    // [cursor, keys], so the JSON cell is the second row's value.
    let scan = exec_ok(&*svc, "SCAN 0 COUNT 100").await;
    assert_eq!(scan.columns, vec!["index", "value"]);
    assert_eq!(scan.rows[0][0].as_deref(), Some("0"), "cursor row");
    let nested = scan.rows[1][1].as_deref().expect("nested cell");
    let parsed: serde_json::Value = serde_json::from_str(nested).expect("nested cell is JSON");
    assert!(parsed.is_array(), "SCAN value cell must be a JSON array");

    // Long scalar cells: display capped at 300 + ellipsis, full value
    // preserved in untruncated_rows.
    let long = "x".repeat(400);
    exec_ok(&*svc, &format!("SET conv-long {long}")).await;
    let got = exec_ok(&*svc, "GET conv-long").await;
    let display = got.rows[0][0].as_deref().expect("long cell");
    assert_eq!(display.chars().count(), 301);
    assert!(display.ends_with('\u{2026}'));
    assert_eq!(got.untruncated_rows[0][0].as_deref(), Some(long.as_str()));

    svc.close();
}

#[tokio::test]
async fn read_only_rejects_mutations_before_reaching_redis() {
    let Some(url) = redis_url() else {
        eprintln!("skipping: REDIS_URL is not set");
        return;
    };
    let _guard = SERIAL.lock().await;
    let (_info, svc) = open_session(&url).await;
    exec_ok(&*svc, "FLUSHDB").await;

    let err = read_only(&*svc, "SET ro-key v")
        .await
        .expect_err("SET must be rejected as read-only");
    assert!(
        err.message.contains("read-only"),
        "message: {}",
        err.message
    );
    assert!(err.message.contains("SET"));

    // The rejection happened before Redis: the key was never created.
    let got = exec_ok(&*svc, "GET ro-key").await;
    assert_eq!(got.rows, vec![vec![None]]);

    // Non-allowlisted read commands are also rejected before Redis.
    let err = read_only(&*svc, "DUMP ro-key")
        .await
        .expect_err("DUMP must be rejected as read-only");
    assert!(err.message.contains("read-only"));

    // Allowlisted reads pass, case-insensitively, and execute still
    // mutates afterwards.
    read_only(&*svc, "ping").await.expect("PING is read-only");
    read_only(&*svc, "get ro-key")
        .await
        .expect("GET is read-only");
    read_only(&*svc, "SCAN 0").await.expect("SCAN is read-only");
    read_only(&*svc, "INFO server")
        .await
        .expect("INFO is read-only");
    exec_ok(&*svc, "SET ro-key v").await;
    let got = exec_ok(&*svc, "GET ro-key").await;
    assert_eq!(got.rows, vec![vec![Some("v".to_string())]]);

    svc.close();
}

#[tokio::test]
async fn virtual_keys_schema_and_browse_paging() {
    let Some(url) = redis_url() else {
        eprintln!("skipping: REDIS_URL is not set");
        return;
    };
    let _guard = SERIAL.lock().await;
    let (_info, svc) = open_session(&url).await;
    exec_ok(&*svc, "FLUSHDB").await;

    exec_ok(&*svc, "SET b-key bval").await;
    exec_ok(&*svc, "SET a-key aval").await;
    exec_ok(&*svc, "SET c-key cval").await;
    exec_ok(&*svc, "HSET h-key f1 v1").await;
    exec_ok(&*svc, "RPUSH l-key x y").await;

    let schema = svc
        .list_schema(EmptyRequest {}, CancellationToken::new())
        .await
        .expect("list_schema");
    assert_eq!(schema.len(), 1);
    assert_eq!(schema[0].database, format!("db{}", database_from_url(&url)));
    assert_eq!(schema[0].name, "keys");
    assert_eq!(schema[0].type_, "table");
    assert_eq!(schema[0].row_count, Some(5));

    let columns = svc
        .table_info(
            TableRequest {
                table: "keys".to_string(),
            },
            CancellationToken::new(),
        )
        .await
        .expect("table_info(keys)");
    assert_eq!(columns.len(), 3);
    assert_eq!(columns[0].name, "key");
    assert_eq!(columns[0].primary_key, 1);
    assert_eq!(columns[0].indexes, vec![1]);
    assert_eq!(columns[1].name, "type");
    assert_eq!(columns[2].name, "value");

    let indexes = svc
        .list_indexes(
            TableRequest {
                table: "keys".to_string(),
            },
            CancellationToken::new(),
        )
        .await
        .expect("list_indexes(keys)");
    assert_eq!(indexes.len(), 1);
    assert_eq!(indexes[0].name, "PRIMARY");
    assert!(indexes[0].unique && indexes[0].primary_key);
    assert_eq!(indexes[0].columns, vec!["key"]);

    let all_indexes = svc
        .list_indexes_all(EmptyRequest {}, CancellationToken::new())
        .await
        .expect("list_indexes_all");
    assert_eq!(all_indexes.len(), 1);
    assert_eq!(all_indexes["keys"][0].name, "PRIMARY");

    // Foreign keys are always empty.
    assert!(
        svc.list_foreign_keys(
            TableRequest {
                table: "keys".to_string(),
            },
            CancellationToken::new(),
        )
        .await
        .expect("list_foreign_keys")
        .is_empty()
    );
    assert!(
        svc.list_foreign_keys_all(EmptyRequest {}, CancellationToken::new())
            .await
            .expect("list_foreign_keys_all")
            .is_empty()
    );

    // Unknown tables are operation errors.
    let unknown = TableRequest {
        table: "nope".to_string(),
    };
    assert!(
        svc.table_info(unknown.clone(), CancellationToken::new())
            .await
            .is_err()
    );
    assert!(
        svc.browse_table(
            BrowseTableRequest {
                table: "nope".to_string(),
                options: Default::default(),
            },
            CancellationToken::new(),
        )
        .await
        .is_err()
    );

    // Paging over the sorted keys: a-key, b-key, c-key, h-key, l-key.
    let page = svc
        .browse_table(
            BrowseTableRequest {
                table: "keys".to_string(),
                options: perk_redis::dto::service::BrowseOptions {
                    offset: Some(1),
                    limit: Some(2),
                    ..Default::default()
                },
            },
            CancellationToken::new(),
        )
        .await
        .expect("browse page");
    assert_eq!(page.columns, vec!["key", "type", "value"]);
    assert_eq!(page.rows.len(), 2);
    assert_eq!(page.rows[0][0].as_deref(), Some("b-key"));
    assert_eq!(page.rows[0][1].as_deref(), Some("string"));
    assert_eq!(page.rows[0][2].as_deref(), Some("bval"));
    assert_eq!(page.rows[1][0].as_deref(), Some("c-key"));
    assert!(page.has_more);
    assert!(!page.truncated);

    // The last page reports no more rows.
    let tail = svc
        .browse_table(
            BrowseTableRequest {
                table: "keys".to_string(),
                options: perk_redis::dto::service::BrowseOptions {
                    offset: Some(4),
                    limit: Some(5),
                    ..Default::default()
                },
            },
            CancellationToken::new(),
        )
        .await
        .expect("browse tail");
    assert_eq!(tail.rows.len(), 1);
    assert_eq!(tail.rows[0][0].as_deref(), Some("l-key"));
    assert_eq!(tail.rows[0][1].as_deref(), Some("list"));
    assert_eq!(tail.rows[0][2].as_deref(), Some("[\"x\",\"y\"]"));
    assert!(!tail.has_more);

    // Full browse: everything, sorted, with bounded previews per type.
    let all = svc
        .browse_table(
            BrowseTableRequest {
                table: "keys".to_string(),
                options: Default::default(),
            },
            CancellationToken::new(),
        )
        .await
        .expect("browse all");
    assert_eq!(all.rows.len(), 5);
    assert!(!all.has_more && !all.truncated);
    assert_eq!(all.rows[0][0].as_deref(), Some("a-key"));
    let hash_row = all
        .rows
        .iter()
        .find(|r| r[0].as_deref() == Some("h-key"))
        .expect("h-key row");
    assert_eq!(hash_row[1].as_deref(), Some("hash"));
    assert_eq!(hash_row[2].as_deref(), Some("{\"f1\":\"v1\"}"));

    // Schema mutation methods are operation errors about the fixed schema.
    let err = svc
        .create_index(
            perk_redis::dto::request::IndexChangeRequest {
                table: "keys".to_string(),
                change: perk_redis::dto::service::IndexInfo {
                    name: "ix".to_string(),
                    unique: false,
                    primary_key: false,
                    columns: vec!["value".to_string()],
                },
            },
            CancellationToken::new(),
        )
        .await
        .expect_err("create_index must fail");
    assert!(
        err.message.contains("fixed virtual schema"),
        "{}",
        err.message
    );
    assert!(
        svc.alter_column(
            perk_redis::dto::request::ColumnChangeRequest {
                table: "keys".to_string(),
                change: perk_redis::dto::service::ColumnChange {
                    previous_name: "value".to_string(),
                    name: "value".to_string(),
                    type_: "string".to_string(),
                    nullable: true,
                    default_value: None,
                    attributes: None,
                },
            },
            CancellationToken::new(),
        )
        .await
        .is_err()
    );

    svc.close();
}

#[tokio::test]
async fn virtual_table_select_is_accepted_through_sql_execute() {
    let Some(url) = redis_url() else {
        eprintln!("skipping: REDIS_URL is not set");
        return;
    };
    let _guard = SERIAL.lock().await;
    let (_info, svc) = open_session(&url).await;
    exec_ok(&*svc, "FLUSHDB").await;

    exec_ok(&*svc, "SET b-key bval").await;
    exec_ok(&*svc, "SET a-key aval").await;
    exec_ok(&*svc, "SET c-key cval").await;
    exec_ok(&*svc, "HSET h-key f1 v1").await;
    exec_ok(&*svc, "RPUSH l-key x y").await;

    // The exact host-generated browse statement must be accepted through
    // the SQL execute path too, returning the same virtual-table rows as
    // browse_table: sorted keys with type and bounded value previews.
    let stmt = r#"SELECT * FROM "keys" LIMIT 25 OFFSET 0"#;
    validate(&*svc, stmt)
        .await
        .expect("the quoted virtual-table SELECT must validate");
    let result = exec_ok(&*svc, stmt).await;
    assert_eq!(result.columns, vec!["key", "type", "value"]);
    let keys: Vec<&str> = result
        .rows
        .iter()
        .map(|row| row[0].as_deref().expect("key cell"))
        .collect();
    assert_eq!(keys, vec!["a-key", "b-key", "c-key", "h-key", "l-key"]);
    assert_eq!(result.rows[0][1].as_deref(), Some("string"));
    assert_eq!(result.rows[0][2].as_deref(), Some("aval"));
    assert!(!result.has_more && !result.truncated);

    // The read-only path serves the same virtual-table SELECT.
    let ro = read_only(&*svc, stmt)
        .await
        .expect("virtual-table SELECT is read-only");
    assert_eq!(ro.rows.len(), 5);

    // LIMIT/OFFSET paging through SQL mirrors browse paging.
    let page = exec_ok(&*svc, "SELECT * FROM keys LIMIT 2 OFFSET 1").await;
    assert_eq!(page.rows.len(), 2);
    assert_eq!(page.rows[0][0].as_deref(), Some("b-key"));
    assert_eq!(page.rows[1][0].as_deref(), Some("c-key"));
    assert!(page.has_more);

    // A huge OFFSET must stay an empty page, not overflow the
    // end-of-page check.
    let far = exec_ok(
        &*svc,
        "SELECT * FROM keys LIMIT 25 OFFSET 18446744073709551615",
    )
    .await;
    assert_eq!(far.rows.len(), 0);
    assert!(!far.has_more && !far.truncated);

    // Malformed quoting and unknown tables stay operation errors.
    for bad in [
        r#"SELECT * FROM "keys"#,
        "SELECT * FROM 'keys LIMIT 1",
        r#"SELECT * FROM "nope" LIMIT 25 OFFSET 0"#,
        "SELECT * FROM",
        "SELECT * FROM keys LIMIT nope",
    ] {
        let err = exec_async(&*svc, bad)
            .await
            .expect_err("malformed SELECT must be rejected");
        assert!(
            err.message.starts_with("invalid statement"),
            "unexpected error for {bad:?}: {}",
            err.message
        );
    }

    // Native Redis commands keep working unchanged.
    exec_ok(&*svc, "PING").await;
    let got = exec_ok(&*svc, "GET a-key").await;
    assert_eq!(got.rows, vec![vec![Some("aval".to_string())]]);

    svc.close();
}

#[tokio::test]
async fn command_errors_do_not_terminate_the_session() {
    let Some(url) = redis_url() else {
        eprintln!("skipping: REDIS_URL is not set");
        return;
    };
    let _guard = SERIAL.lock().await;
    let (_info, svc) = open_session(&url).await;
    exec_ok(&*svc, "FLUSHDB").await;

    // A WRONGTYPE command error surfaces as an operation error.
    exec_ok(&*svc, "HSET err-h f v").await;
    let err = exec_async(&*svc, "INCR err-h")
        .await
        .expect_err("WRONGTYPE must surface as an operation error");
    assert!(
        err.message.contains("WRONGTYPE"),
        "message: {}",
        err.message
    );

    let err = exec_async(&*svc, "GET")
        .await
        .expect_err("wrong arity must surface as an operation error");
    assert!(!err.message.is_empty());

    // The same session keeps serving afterwards.
    exec_ok(&*svc, "PING").await;
    exec_ok(&*svc, "SET after-error ok").await;
    let got = exec_ok(&*svc, "GET after-error").await;
    assert_eq!(got.rows, vec![vec![Some("ok".to_string())]]);

    svc.close();
}

#[tokio::test]
async fn browse_collects_all_keys_across_scan_batches() {
    let Some(url) = redis_url() else {
        eprintln!("skipping: REDIS_URL is not set");
        return;
    };
    let _guard = SERIAL.lock().await;
    let (_info, svc) = open_session(&url).await;
    exec_ok(&*svc, "FLUSHDB").await;

    // More keys than one SCAN COUNT 1000 batch: the cursor must be
    // followed across batches (RESP2 returns it as a bulk string). The
    // display caps pages at 500 rows, so page through the whole set.
    for i in 0..1200 {
        exec_ok(&*svc, &format!("SET batch-key-{i:04} v")).await;
    }

    let mut collected: Vec<String> = Vec::new();
    let mut offset = 0u64;
    loop {
        let page = svc
            .browse_table(
                BrowseTableRequest {
                    table: "keys".to_string(),
                    options: perk_redis::dto::service::BrowseOptions {
                        offset: Some(offset),
                        limit: Some(500),
                        ..Default::default()
                    },
                },
                CancellationToken::new(),
            )
            .await
            .expect("browse page");
        assert_eq!(page.rows.len(), page.untruncated_rows.len());
        for row in &page.rows {
            collected.push(row[0].clone().expect("key cell"));
        }
        if !page.has_more {
            break;
        }
        offset += 500;
    }
    assert_eq!(collected.len(), 1200, "every key must be collected");
    assert_eq!(
        collected.first().map(String::as_str),
        Some("batch-key-0000")
    );
    assert_eq!(collected.last().map(String::as_str), Some("batch-key-1199"));
    assert_eq!(
        collected.get(500).map(String::as_str),
        Some("batch-key-0500"),
        "sorted by key across pages"
    );

    svc.close();
}

#[tokio::test]
async fn close_is_idempotent_and_new_sessions_are_fresh() {
    let Some(url) = redis_url() else {
        eprintln!("skipping: REDIS_URL is not set");
        return;
    };
    let _guard = SERIAL.lock().await;
    let (_info, svc) = open_session(&url).await;
    exec_ok(&*svc, "FLUSHDB").await;

    svc.close();
    svc.close(); // idempotent

    // A closed session rejects further work.
    let err = exec_async(&*svc, "PING").await.expect_err("closed session");
    assert!(!err.message.is_empty());

    // A fresh session opens and serves independently.
    let (_info2, svc2) = open_session(&url).await;
    exec_ok(&*svc2, "PING").await;
    exec_ok(&*svc2, "SET fresh-probe v").await;
    let got = exec_ok(&*svc2, "GET fresh-probe").await;
    assert_eq!(got.rows, vec![vec![Some("v".to_string())]]);
    svc2.close();
}

#[tokio::test]
async fn row_write_insert_creates_strings_and_rejects_collisions() {
    let Some(url) = redis_url() else {
        eprintln!("skipping: REDIS_URL is not set");
        return;
    };
    let _guard = SERIAL.lock().await;
    let (_info, svc) = open_session(&url).await;
    exec_ok(&*svc, "FLUSHDB").await;

    assert_eq!(
        row_write_statement(&*svc, insert_req("rw-ins", "v1")).await,
        (1, "SET rw-ins v1 NX".to_string())
    );
    let got = exec_ok(&*svc, "GET rw-ins").await;
    assert_eq!(got.rows, vec![vec![Some("v1".to_string())]]);
    let type_ = exec_ok(&*svc, "TYPE rw-ins").await;
    assert_eq!(type_.rows, vec![vec![Some("string".to_string())]]);

    // Explicit `type` "string" and a blank type both insert strings; a
    // missing `value` inserts the empty string.
    let mut with_type = insert_req("rw-ins2", "v2");
    with_type
        .values
        .as_mut()
        .unwrap()
        .push(cell("type", "string"));
    assert_eq!(
        row_write_statement(&*svc, with_type).await,
        (1, "SET rw-ins2 v2 NX".to_string())
    );
    let mut blank_type = insert_req("rw-ins3", "v3");
    blank_type.values.as_mut().unwrap().push(cell("type", ""));
    assert_eq!(
        row_write_statement(&*svc, blank_type).await,
        (1, "SET rw-ins3 v3 NX".to_string())
    );
    let mut no_value = insert_req("rw-ins4", "");
    no_value.values.as_mut().unwrap().pop();
    // The empty value stays an explicit empty token on the wire.
    assert_eq!(
        row_write_statement(&*svc, no_value).await,
        (1, "SET rw-ins4 '' NX".to_string())
    );
    assert_eq!(
        exec_ok(&*svc, "GET rw-ins4").await.rows,
        vec![vec![Some(String::new())]]
    );
    assert_eq!(
        exec_ok(&*svc, "DBSIZE").await.rows,
        vec![vec![Some("4".to_string())]]
    );

    // A collision is rejected and the stored value is untouched.
    let err = row_write(&*svc, insert_req("rw-ins", "v1b"))
        .await
        .expect_err("collision must be rejected");
    assert!(err.message.contains("already exists"), "{}", err.message);
    let got = exec_ok(&*svc, "GET rw-ins").await;
    assert_eq!(got.rows, vec![vec![Some("v1".to_string())]]);

    // Collection types are rejected on insert; nothing is created.
    let mut hash_type = insert_req("rw-hash", "v");
    hash_type
        .values
        .as_mut()
        .unwrap()
        .push(cell("type", "hash"));
    let err = row_write(&*svc, hash_type)
        .await
        .expect_err("hash type must be rejected");
    assert!(err.message.contains("only string"), "{}", err.message);
    assert_eq!(
        exec_ok(&*svc, "EXISTS rw-hash").await.rows,
        vec![vec![Some("0".to_string())]]
    );

    svc.close();
}

#[tokio::test]
async fn row_write_update_edits_strings_and_renames() {
    let Some(url) = redis_url() else {
        eprintln!("skipping: REDIS_URL is not set");
        return;
    };
    let _guard = SERIAL.lock().await;
    let (_info, svc) = open_session(&url).await;
    exec_ok(&*svc, "FLUSHDB").await;

    exec_ok(&*svc, "SET rw-up v").await;

    // Value edit: the complete string is replaced with SET.
    let (affected, statement) =
        row_write_statement(&*svc, update_req("rw-up", vec![cell("value", "edited")])).await;
    assert_eq!(affected, 1);
    assert_update_statement(&statement, "rw-up", "rw-up", "1", "v", "edited");
    let got = exec_ok(&*svc, "GET rw-up").await;
    assert_eq!(got.rows, vec![vec![Some("edited".to_string())]]);

    // The guard is on the current value, not the new one: a 400-rune
    // explicit replacement of a short string is an accepted edit.
    let huge_new = "z".repeat(400);
    let (affected, statement) =
        row_write_statement(&*svc, update_req("rw-up", vec![cell("value", &huge_new)])).await;
    assert_eq!(affected, 1);
    assert_update_statement(&statement, "rw-up", "rw-up", "1", "edited", &huge_new);
    let got = exec_ok(&*svc, "GET rw-up").await;
    assert_eq!(got.untruncated_rows, vec![vec![Some(huge_new)]]);

    // Updating a missing key is an error.
    let err = row_write(&*svc, update_req("rw-missing", vec![cell("value", "x")]))
        .await
        .expect_err("missing key must be rejected");
    assert!(err.message.contains("rw-missing"), "{}", err.message);

    // Rename: the `key` column change moves the existing key.
    let (affected, statement) =
        row_write_statement(&*svc, update_req("rw-up", vec![cell("key", "rw-renamed")])).await;
    assert_eq!(affected, 1);
    assert_update_statement(&statement, "rw-up", "rw-renamed", "0", "", "");
    assert_eq!(
        exec_ok(&*svc, "EXISTS rw-up").await.rows,
        vec![vec![Some("0".to_string())]]
    );
    let got = exec_ok(&*svc, "GET rw-renamed").await;
    assert_eq!(got.untruncated_rows, vec![vec![Some("z".repeat(400))]]);

    // A rename onto an existing key is rejected; nothing moves.
    exec_ok(&*svc, "SET rw-other other").await;
    let err = row_write(
        &*svc,
        update_req("rw-renamed", vec![cell("key", "rw-other")]),
    )
    .await
    .expect_err("destination collision must be rejected");
    assert!(err.message.contains("already exists"), "{}", err.message);
    assert_eq!(
        exec_ok(&*svc, "GET rw-renamed").await.untruncated_rows[0][0].as_deref(),
        Some("z".repeat(400).as_str())
    );
    assert_eq!(
        exec_ok(&*svc, "GET rw-other").await.rows,
        vec![vec![Some("other".to_string())]]
    );

    // Same-name rename is a successful no-op.
    let (affected, statement) = row_write_statement(
        &*svc,
        update_req("rw-renamed", vec![cell("key", "rw-renamed")]),
    )
    .await;
    assert_eq!(affected, 1);
    assert_update_statement(&statement, "rw-renamed", "rw-renamed", "0", "", "");

    // Rename works on non-string types: the type travels unchanged.
    exec_ok(&*svc, "HSET rw-h f v").await;
    let (affected, statement) =
        row_write_statement(&*svc, update_req("rw-h", vec![cell("key", "rw-h2")])).await;
    assert_eq!(affected, 1);
    assert_update_statement(&statement, "rw-h", "rw-h2", "0", "", "");
    assert_eq!(
        exec_ok(&*svc, "TYPE rw-h2").await.rows,
        vec![vec![Some("hash".to_string())]]
    );
    assert_eq!(
        exec_ok(&*svc, "HGETALL rw-h2").await.rows,
        vec![
            vec![Some("0".to_string()), Some("f".to_string())],
            vec![Some("1".to_string()), Some("v".to_string())],
        ]
    );

    svc.close();
}

#[tokio::test]
async fn row_write_update_rejects_unsafe_value_edits_without_mutation() {
    let Some(url) = redis_url() else {
        eprintln!("skipping: REDIS_URL is not set");
        return;
    };
    let _guard = SERIAL.lock().await;
    let (_info, svc) = open_session(&url).await;
    exec_ok(&*svc, "FLUSHDB").await;

    // Collection value edits are rejected and the collections stay intact.
    exec_ok(&*svc, "HSET rw-colh f v").await;
    let err = row_write(
        &*svc,
        update_req("rw-colh", vec![cell("value", "hijacked")]),
    )
    .await
    .expect_err("hash value edit must be rejected");
    assert!(err.message.contains("not a string"), "{}", err.message);
    assert_eq!(
        exec_ok(&*svc, "HGETALL rw-colh").await.rows,
        vec![
            vec![Some("0".to_string()), Some("f".to_string())],
            vec![Some("1".to_string()), Some("v".to_string())],
        ]
    );

    exec_ok(&*svc, "RPUSH rw-coll a b").await;
    let err = row_write(&*svc, update_req("rw-coll", vec![cell("value", "x")]))
        .await
        .expect_err("list value edit must be rejected");
    assert!(err.message.contains("not a string"), "{}", err.message);
    assert_eq!(exec_ok(&*svc, "LRANGE rw-coll 0 -1").await.rows.len(), 2);

    // A value over the 300-rune display cell is rejected and the stored
    // value stays byte-for-byte unchanged.
    let long = "x".repeat(400);
    exec_ok(&*svc, &format!("SET rw-long {long}")).await;
    let err = row_write(&*svc, update_req("rw-long", vec![cell("value", "short")]))
        .await
        .expect_err("long value edit must be rejected");
    assert!(err.message.contains("300"), "{}", err.message);
    let got = exec_ok(&*svc, "GET rw-long").await;
    assert_eq!(got.untruncated_rows, vec![vec![Some(long.clone())]]);

    // Exactly 300 runes is representable without truncation: editable.
    let exact = "y".repeat(300);
    exec_ok(&*svc, &format!("SET rw-exact {exact}")).await;
    let (affected, statement) =
        row_write_statement(&*svc, update_req("rw-exact", vec![cell("value", "ok")])).await;
    assert_eq!(affected, 1);
    assert_update_statement(&statement, "rw-exact", "rw-exact", "1", &exact, "ok");
    assert_eq!(
        exec_ok(&*svc, "GET rw-exact").await.rows,
        vec![vec![Some("ok".to_string())]]
    );

    // A non-UTF-8 value is rejected; the raw bytes are untouched (checked
    // outside the plugin's lossy display path).
    let client = redis::Client::open(url.clone()).expect("open raw client");
    let mut conn = client
        .get_connection_manager()
        .await
        .expect("connect raw client");
    let bad = vec![0xffu8, 0xfe, 0x00, 0x41];
    conn.set::<_, _, ()>("rw-bad", bad.clone())
        .await
        .expect("set raw bytes");
    let err = row_write(&*svc, update_req("rw-bad", vec![cell("value", "x")]))
        .await
        .expect_err("non-UTF-8 edit must be rejected");
    assert!(err.message.contains("UTF-8"), "{}", err.message);
    let raw: Vec<u8> = conn.get("rw-bad").await.expect("get raw bytes");
    assert_eq!(raw, bad, "stored bytes must be unchanged");

    // Type is immutable: any `type` change is rejected before mutation.
    exec_ok(&*svc, "SET rw-typ v").await;
    for type_value in ["hash", "string", ""] {
        let err = row_write(&*svc, update_req("rw-typ", vec![cell("type", type_value)]))
            .await
            .expect_err("type change must be rejected");
        assert!(err.message.contains("immutable"), "{}", err.message);
    }
    assert_eq!(
        exec_ok(&*svc, "TYPE rw-typ").await.rows,
        vec![vec![Some("string".to_string())]]
    );

    svc.close();
}

#[tokio::test]
async fn row_write_update_rename_plus_value_is_atomic() {
    let Some(url) = redis_url() else {
        eprintln!("skipping: REDIS_URL is not set");
        return;
    };
    let _guard = SERIAL.lock().await;
    let (_info, svc) = open_session(&url).await;
    exec_ok(&*svc, "FLUSHDB").await;

    // Rename and value change apply together: one row, moved and replaced.
    exec_ok(&*svc, "SET rw-src old").await;
    let (affected, statement) = row_write_statement(
        &*svc,
        update_req("rw-src", vec![cell("key", "rw-dst"), cell("value", "new")]),
    )
    .await;
    assert_eq!(affected, 1);
    assert_update_statement(&statement, "rw-src", "rw-dst", "1", "old", "new");
    assert_eq!(
        exec_ok(&*svc, "EXISTS rw-src").await.rows,
        vec![vec![Some("0".to_string())]]
    );
    let got = exec_ok(&*svc, "GET rw-dst").await;
    assert_eq!(got.rows, vec![vec![Some("new".to_string())]]);

    // Destination collision with a value change: nothing mutates at all.
    exec_ok(&*svc, "SET rw-src2 v").await;
    exec_ok(&*svc, "SET rw-dst2 occupied").await;
    let err = row_write(
        &*svc,
        update_req("rw-src2", vec![cell("key", "rw-dst2"), cell("value", "w")]),
    )
    .await
    .expect_err("destination collision must be rejected");
    assert!(err.message.contains("already exists"), "{}", err.message);
    assert_eq!(
        exec_ok(&*svc, "GET rw-src2").await.rows,
        vec![vec![Some("v".to_string())]]
    );
    assert_eq!(
        exec_ok(&*svc, "GET rw-dst2").await.rows,
        vec![vec![Some("occupied".to_string())]]
    );

    // A hash source with a combined change: the value part is rejected
    // before any rename happens.
    exec_ok(&*svc, "HSET rw-hsrc f v").await;
    let err = row_write(
        &*svc,
        update_req("rw-hsrc", vec![cell("key", "rw-hdst"), cell("value", "w")]),
    )
    .await
    .expect_err("hash value change must be rejected");
    assert!(err.message.contains("not a string"), "{}", err.message);
    assert_eq!(
        exec_ok(&*svc, "TYPE rw-hsrc").await.rows,
        vec![vec![Some("hash".to_string())]]
    );
    assert_eq!(
        exec_ok(&*svc, "EXISTS rw-hdst").await.rows,
        vec![vec![Some("0".to_string())]]
    );
    assert_eq!(
        exec_ok(&*svc, "HGETALL rw-hsrc").await.rows,
        vec![
            vec![Some("0".to_string()), Some("f".to_string())],
            vec![Some("1".to_string()), Some("v".to_string())],
        ]
    );

    svc.close();
}

#[tokio::test]
async fn row_write_delete_reports_actual_rows_affected() {
    let Some(url) = redis_url() else {
        eprintln!("skipping: REDIS_URL is not set");
        return;
    };
    let _guard = SERIAL.lock().await;
    let (_info, svc) = open_session(&url).await;
    exec_ok(&*svc, "FLUSHDB").await;

    exec_ok(&*svc, "SET rw-del v").await;
    assert_eq!(
        row_write_statement(&*svc, delete_req("rw-del")).await,
        (1, "DEL rw-del".to_string())
    );
    assert_eq!(
        exec_ok(&*svc, "EXISTS rw-del").await.rows,
        vec![vec![Some("0".to_string())]]
    );
    // DEL's actual count: a missing key reports 0, not an error. The
    // statement still logs the exact DEL command.
    assert_eq!(
        row_write_statement(&*svc, delete_req("rw-del")).await,
        (0, "DEL rw-del".to_string())
    );
    assert_eq!(
        row_write_statement(&*svc, delete_req("rw-never")).await,
        (0, "DEL rw-never".to_string())
    );

    // An empty-string key is a valid Redis key: delete runs DEL and
    // reports its actual count, logging the empty key explicitly.
    exec_ok(&*svc, "SET \"\" empty-key-value").await;
    assert_eq!(
        row_write_statement(&*svc, delete_req("")).await,
        (1, "DEL ''".to_string())
    );
    assert_eq!(row_write_ok(&*svc, delete_req("")).await, 0);

    svc.close();
}

#[tokio::test]
async fn row_write_statements_are_native_replayable_commands() {
    let Some(url) = redis_url() else {
        eprintln!("skipping: REDIS_URL is not set");
        return;
    };
    let _guard = SERIAL.lock().await;
    let (_info, svc) = open_session(&url).await;
    exec_ok(&*svc, "FLUSHDB").await;

    // Insert: the logged command is the exact SET ... NX.
    let (affected, insert_statement) = row_write_statement(&*svc, insert_req("user:2", "v1")).await;
    assert_eq!(affected, 1);
    assert_eq!(insert_statement, "SET user:2 v1 NX");

    // Pure key rename user:2 -> user:3 logs an executable EVAL of the
    // atomic script — never a generic Table/Key/Changes preview.
    let (affected, rename_statement) =
        row_write_statement(&*svc, update_req("user:2", vec![cell("key", "user:3")])).await;
    assert_eq!(affected, 1);
    assert_update_statement(&rename_statement, "user:2", "user:3", "0", "", "");

    // Value-only update and combined rename + value update.
    let (affected, value_statement) =
        row_write_statement(&*svc, update_req("user:3", vec![cell("value", "v2")])).await;
    assert_eq!(affected, 1);
    assert_update_statement(&value_statement, "user:3", "user:3", "1", "v1", "v2");
    let (affected, combined_statement) = row_write_statement(
        &*svc,
        update_req("user:3", vec![cell("key", "user:4"), cell("value", "v3")]),
    )
    .await;
    assert_eq!(affected, 1);
    assert_update_statement(&combined_statement, "user:3", "user:4", "1", "v2", "v3");

    // Delete: the logged command is the exact DEL.
    let (affected, delete_statement) = row_write_statement(&*svc, delete_req("user:4")).await;
    assert_eq!(affected, 1);
    assert_eq!(delete_statement, "DEL user:4");

    // Every captured statement replays through the plugin's normal
    // execute parser against Redis: run them in sequence from a clean
    // database and observe the same end state each row write produced.
    exec_ok(&*svc, "FLUSHDB").await;
    exec_ok(&*svc, &insert_statement).await;
    assert_eq!(
        exec_ok(&*svc, "GET user:2").await.rows,
        vec![vec![Some("v1".to_string())]]
    );
    exec_ok(&*svc, &rename_statement).await;
    assert_eq!(
        exec_ok(&*svc, "EXISTS user:2").await.rows,
        vec![vec![Some("0".to_string())]]
    );
    assert_eq!(
        exec_ok(&*svc, "GET user:3").await.rows,
        vec![vec![Some("v1".to_string())]]
    );
    exec_ok(&*svc, &value_statement).await;
    assert_eq!(
        exec_ok(&*svc, "GET user:3").await.rows,
        vec![vec![Some("v2".to_string())]]
    );
    exec_ok(&*svc, &combined_statement).await;
    assert_eq!(
        exec_ok(&*svc, "EXISTS user:3").await.rows,
        vec![vec![Some("0".to_string())]]
    );
    assert_eq!(
        exec_ok(&*svc, "GET user:4").await.rows,
        vec![vec![Some("v3".to_string())]]
    );
    exec_ok(&*svc, &delete_statement).await;
    assert_eq!(
        exec_ok(&*svc, "EXISTS user:4").await.rows,
        vec![vec![Some("0".to_string())]]
    );

    // Hostile tokens: the statement the host logs round-trips through
    // the plugin's own parser and replays verbatim, including spaces,
    // quotes, backslashes, and a newline.
    let (affected, hostile_statement) =
        row_write_statement(&*svc, insert_req("user:5 a", "it's \"quoted\"\nline\\end")).await;
    assert_eq!(affected, 1);
    assert_eq!(
        shell_words::split(&hostile_statement).unwrap(),
        vec![
            "SET".to_string(),
            "user:5 a".to_string(),
            "it's \"quoted\"\nline\\end".to_string(),
            "NX".to_string(),
        ]
    );
    validate(&*svc, &hostile_statement)
        .await
        .expect("hostile statement must pass the plugin's own parser");
    exec_ok(&*svc, "FLUSHDB").await;
    exec_ok(&*svc, &hostile_statement).await;
    assert_eq!(
        exec_ok(&*svc, "GET \"user:5 a\"").await.rows,
        vec![vec![Some("it's \"quoted\"\nline\\end".to_string())]]
    );

    svc.close();
}

#[tokio::test]
async fn execution_results_report_statement_and_metadata() {
    let Some(url) = redis_url() else {
        eprintln!("skipping: REDIS_URL is not set");
        return;
    };
    let _guard = SERIAL.lock().await;
    let (_info, svc) = open_session(&url).await;
    exec_ok(&*svc, "FLUSHDB").await;

    // A benign read reports the exact command, replayable and benign.
    let result = exec_ok(&*svc, "PING").await;
    assert_eq!(result.statement.as_deref(), Some("PING"));
    assert_eq!(
        result.statement_metadata,
        Some(StatementMetadata::redis(true, false))
    );

    exec_ok(&*svc, "SET exec-meta v").await;
    let result = exec_ok(&*svc, "GET exec-meta").await;
    assert_eq!(result.statement.as_deref(), Some("GET exec-meta"));
    assert_eq!(
        result.statement_metadata,
        Some(StatementMetadata::redis(true, false))
    );

    // A value-bearing write is flagged sensitive: the host redacts the
    // statement and forces the entry non-replayable, so the secret
    // never survives in serialized metadata paths.
    let result = exec_ok(&*svc, "SET exec-meta hunter2").await;
    assert_eq!(result.statement.as_deref(), Some("SET exec-meta hunter2"));
    assert_eq!(
        result.statement_metadata,
        Some(StatementMetadata::redis(false, true))
    );

    // The read-only path reports the same statement and metadata.
    let result = read_only(&*svc, "GET exec-meta")
        .await
        .expect("read-only GET");
    assert_eq!(result.statement.as_deref(), Some("GET exec-meta"));
    assert_eq!(
        result.statement_metadata,
        Some(StatementMetadata::redis(true, false))
    );

    // A write through the read-only surface is unsupported.
    let err = read_only(&*svc, "SET exec-meta x")
        .await
        .expect_err("read-only SET must be rejected");
    assert_eq!(err.kind, ErrorKind::Unsupported);

    // An unknown command is a server operation error, never protocol or
    // plugin_crash.
    let err = exec_async(&*svc, "THISCMDDOESNOTEXIST x")
        .await
        .expect_err("unknown command must fail");
    assert_eq!(err.kind, ErrorKind::Operation);

    svc.close();
}

#[tokio::test]
async fn browse_and_virtual_select_report_the_replayable_pseudo_command() {
    let Some(url) = redis_url() else {
        eprintln!("skipping: REDIS_URL is not set");
        return;
    };
    let _guard = SERIAL.lock().await;
    let (_info, svc) = open_session(&url).await;
    exec_ok(&*svc, "FLUSHDB").await;
    for key in ["b:1", "a:1", "c:1"] {
        exec_ok(&*svc, &format!("SET {key} v")).await;
    }

    // browse_table reports the exact pseudo-command execute can replay.
    let request = BrowseTableRequest {
        table: "keys".to_string(),
        options: BrowseOptions {
            offset: Some(0),
            limit: Some(2),
            ..Default::default()
        },
    };
    let result = svc
        .browse_table(request, CancellationToken::new())
        .await
        .expect("browse must succeed");
    assert_eq!(
        result.statement.as_deref(),
        Some(r#"SELECT * FROM "keys" LIMIT 2 OFFSET 0"#)
    );
    assert_eq!(
        result.statement_metadata,
        Some(StatementMetadata::redis(true, false))
    );

    // Executing the host-generated statement reports the same
    // pseudo-command with its clauses, replayable and benign.
    let result = exec_ok(&*svc, r#"SELECT * FROM "keys" LIMIT 25 OFFSET 0"#).await;
    assert_eq!(
        result.statement.as_deref(),
        Some(r#"SELECT * FROM "keys" LIMIT 25 OFFSET 0"#)
    );
    assert_eq!(
        result.statement_metadata,
        Some(StatementMetadata::redis(true, false))
    );
    assert_eq!(result.rows.len(), 3);

    svc.close();
}

/// Runs a row write and returns `(statement, metadata)`.
async fn row_write_metadata(
    svc: &dyn SessionService,
    request: RowWriteRequest,
) -> (String, StatementMetadata) {
    let response = row_write(svc, request)
        .await
        .unwrap_or_else(|e| panic!("row_write failed: {e:?}"));
    (
        response.result.statement,
        response
            .result
            .statement_metadata
            .expect("every row write pairs its statement with metadata"),
    )
}

#[tokio::test]
async fn row_write_statements_pair_metadata_by_sensitivity() {
    let Some(url) = redis_url() else {
        eprintln!("skipping: REDIS_URL is not set");
        return;
    };
    let _guard = SERIAL.lock().await;
    let (_info, svc) = open_session(&url).await;
    exec_ok(&*svc, "FLUSHDB").await;

    // Insert embeds the value: sensitive, non-replayable.
    let (statement, metadata) = row_write_metadata(&*svc, insert_req("meta-ins", "secret")).await;
    assert_eq!(statement, "SET meta-ins secret NX");
    assert_eq!(metadata, StatementMetadata::redis(false, true));

    // A value edit embeds the new value in the EVAL statement.
    let (_, metadata) = row_write_metadata(
        &*svc,
        update_req("meta-ins", vec![cell("value", "secret2")]),
    )
    .await;
    assert_eq!(metadata, StatementMetadata::redis(false, true));

    // A rename-only update is RENAME-equivalent: keys only, replayable.
    let (_, metadata) = row_write_metadata(
        &*svc,
        update_req("meta-ins", vec![cell("key", "meta-renamed")]),
    )
    .await;
    assert_eq!(metadata, StatementMetadata::redis(true, false));

    // Key-only DEL: replayable and benign.
    let (statement, metadata) = row_write_metadata(&*svc, delete_req("meta-renamed")).await;
    assert_eq!(statement, "DEL meta-renamed");
    assert_eq!(metadata, StatementMetadata::redis(true, false));

    svc.close();
}

#[tokio::test]
async fn error_kinds_map_auth_connection_validation_and_unsupported() {
    let Some(url) = redis_url() else {
        eprintln!("skipping: REDIS_URL is not set");
        return;
    };
    let _guard = SERIAL.lock().await;

    // Wrong credentials: authentication, before any session exists.
    let mut bad = url::Url::parse(&url).expect("REDIS_URL must parse");
    bad.set_password(Some("wrong-password"))
        .expect("set password");
    let err = match RedisFactory::default().open(bad.as_str()).await {
        Err(e) => e,
        Ok(_) => panic!("wrong password must fail open"),
    };
    assert_eq!(err.kind, ErrorKind::Authentication, "{}", err.message);

    // An unreachable port: connection.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind probe");
    let port = listener.local_addr().expect("probe address").port();
    drop(listener);
    let err = match RedisFactory::default()
        .open(&format!("redis://127.0.0.1:{port}/0"))
        .await
    {
        Err(e) => e,
        Ok(_) => panic!("closed port must fail open"),
    };
    assert_eq!(err.kind, ErrorKind::Connection, "{}", err.message);

    let (_info, svc) = open_session(&url).await;
    exec_ok(&*svc, "FLUSHDB").await;

    // Malformed statements and malformed row-write input: validation.
    let err = validate(&*svc, "SET unclosed 'quote")
        .await
        .expect_err("malformed statement must be rejected");
    assert_eq!(err.kind, ErrorKind::Validation, "{}", err.message);
    let err = row_write(
        &*svc,
        RowWriteRequest {
            operation: "upsert".to_string(),
            table: "keys".to_string(),
            key: None,
            values: None,
        },
    )
    .await
    .expect_err("unknown operation must be rejected");
    assert_eq!(err.kind, ErrorKind::Validation, "{}", err.message);

    // Unknown table on a schema RPC: unsupported.
    let err = svc
        .table_info(
            TableRequest {
                table: "nope".to_string(),
            },
            CancellationToken::new(),
        )
        .await
        .expect_err("unknown table must be rejected");
    assert_eq!(err.kind, ErrorKind::Unsupported, "{}", err.message);
    let err = row_write(&*svc, insert_req("x", "v"))
        .await
        .expect("collision-free insert") // inserted fine; now target another table
        .result
        .rows_affected;
    assert_eq!(err, 1);
    let mut bad_table = insert_req("x2", "v");
    bad_table.table = "nope".to_string();
    let err = row_write(&*svc, bad_table)
        .await
        .expect_err("unknown write table must be rejected");
    assert_eq!(err.kind, ErrorKind::Unsupported, "{}", err.message);

    // A closed session reports connection errors.
    svc.close();
    let err = exec_async(&*svc, "PING")
        .await
        .expect_err("closed session must fail");
    assert_eq!(err.kind, ErrorKind::Connection, "{}", err.message);
}
