//! Integration tests against a live Redis server.
//!
//! These tests require the `REDIS_URL` environment variable to point at a
//! disposable Redis instance, e.g.
//! `redis://:workbench-demo@127.0.0.1:6380/2`. When `REDIS_URL` is unset
//! every test skips. Each test runs inside a global lock and flushes the
//! selected logical database first, so runs are deterministic and
//! isolated from each other.

use std::sync::LazyLock;

use tokio::sync::Mutex as AsyncMutex;
use tokio_util::sync::CancellationToken;

use perk_redis::dto::request::{BrowseTableRequest, EmptyRequest, StatementRequest, TableRequest};
use perk_redis::dto::service::{DatabaseInfo, QueryResult};
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
