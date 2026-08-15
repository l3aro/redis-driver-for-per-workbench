//! Session service contract and the in-memory test service that proves
//! routing. The real Redis adapter replaces `MemoryFactory` in a later
//! change; the trait below is the seam it plugs into.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio_util::sync::CancellationToken;

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
use crate::protocol::ERR_CANCELED;

pub type ServiceFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, ServiceError>> + Send + 'a>>;

/// Handler error. `code: None` becomes the JSON-RPC internal error
/// (-32603); a present code is used as-is (e.g. -32800 canceled).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceError {
    pub code: Option<i32>,
    pub message: String,
}

impl ServiceError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            code: None,
            message: message.into(),
        }
    }

    pub fn with_code(code: i32, message: impl Into<String>) -> Self {
        Self {
            code: Some(code),
            message: message.into(),
        }
    }

    pub fn canceled(message: impl Into<String>) -> Self {
        Self {
            code: Some(ERR_CANCELED),
            message: message.into(),
        }
    }

    pub fn jsonrpc_code(&self) -> i32 {
        self.code.unwrap_or(crate::protocol::ERR_INTERNAL)
    }
}

/// The 20 mandatory session handlers plus the close hook. Every method
/// observes cancellation through `cancel`; a canceled handler must return
/// a `-32800` error. Handlers are `Send` and may run concurrently.
pub trait SessionService: Send + Sync {
    fn execute(
        &self,
        request: StatementRequest,
        cancel: CancellationToken,
    ) -> ServiceFuture<'static, QueryResult>;
    fn execute_read_only(
        &self,
        request: StatementRequest,
        cancel: CancellationToken,
    ) -> ServiceFuture<'static, QueryResult>;
    fn validate(
        &self,
        request: StatementRequest,
        cancel: CancellationToken,
    ) -> ServiceFuture<'static, ()>;
    fn list_schema(
        &self,
        request: EmptyRequest,
        cancel: CancellationToken,
    ) -> ServiceFuture<'static, Vec<SchemaObject>>;
    fn table_info(
        &self,
        request: TableRequest,
        cancel: CancellationToken,
    ) -> ServiceFuture<'static, Vec<ColumnInfo>>;
    fn list_indexes(
        &self,
        request: TableRequest,
        cancel: CancellationToken,
    ) -> ServiceFuture<'static, Vec<IndexInfo>>;
    fn create_index(
        &self,
        request: IndexChangeRequest,
        cancel: CancellationToken,
    ) -> ServiceFuture<'static, ()>;
    fn replace_index(
        &self,
        request: ReplaceIndexRequest,
        cancel: CancellationToken,
    ) -> ServiceFuture<'static, ()>;
    fn drop_index(
        &self,
        request: DropRequest,
        cancel: CancellationToken,
    ) -> ServiceFuture<'static, ()>;
    fn list_foreign_keys(
        &self,
        request: TableRequest,
        cancel: CancellationToken,
    ) -> ServiceFuture<'static, Vec<ForeignKeyInfo>>;
    fn list_referencing_foreign_keys(
        &self,
        request: TableRequest,
        cancel: CancellationToken,
    ) -> ServiceFuture<'static, Vec<ReferencingForeignKeyInfo>>;
    fn list_foreign_keys_all(
        &self,
        request: EmptyRequest,
        cancel: CancellationToken,
    ) -> ServiceFuture<'static, HashMap<String, Vec<ForeignKeyInfo>>>;
    fn list_indexes_all(
        &self,
        request: EmptyRequest,
        cancel: CancellationToken,
    ) -> ServiceFuture<'static, HashMap<String, Vec<IndexInfo>>>;
    fn create_foreign_key(
        &self,
        request: ForeignKeyChangeRequest,
        cancel: CancellationToken,
    ) -> ServiceFuture<'static, ()>;
    fn replace_foreign_key(
        &self,
        request: ReplaceForeignKeyRequest,
        cancel: CancellationToken,
    ) -> ServiceFuture<'static, ()>;
    fn drop_foreign_key(
        &self,
        request: DropRequest,
        cancel: CancellationToken,
    ) -> ServiceFuture<'static, ()>;
    fn alter_column(
        &self,
        request: ColumnChangeRequest,
        cancel: CancellationToken,
    ) -> ServiceFuture<'static, ()>;
    fn drop_column(
        &self,
        request: DropRequest,
        cancel: CancellationToken,
    ) -> ServiceFuture<'static, ()>;
    fn add_column(
        &self,
        request: AddColumnRequest,
        cancel: CancellationToken,
    ) -> ServiceFuture<'static, ()>;
    fn browse_table(
        &self,
        request: BrowseTableRequest,
        cancel: CancellationToken,
    ) -> ServiceFuture<'static, QueryResult>;
    /// Close hook. The server removes the session before calling this, so
    /// a failing hook can never resurrect the session.
    fn close(&self);
}

/// Creates sessions and serializes connection forms. The Redis adapter
/// atom replaces the memory-backed implementation.
pub trait SessionFactory: Send + Sync {
    fn build_target(&self, values: &FormValues) -> BuildTargetResult;
    fn open(&self, target: &str) -> Result<(DatabaseInfo, Box<dyn SessionService>), ServiceError>;
}

fn stub_result(
    columns: &[&str],
    rows: Vec<Vec<Option<String>>>,
    rows_affected: u64,
) -> QueryResult {
    QueryResult {
        columns: columns.iter().map(|s| s.to_string()).collect(),
        column_types: columns.iter().map(|_| "string".to_string()).collect(),
        rows: rows.clone(),
        untruncated_rows: rows,
        rows_affected,
        has_more: false,
        duration_ns: 0,
        truncated: false,
        document_ids: None,
    }
}

/// In-memory key-value session service: proves full perk/v1 routing with
/// SET/GET/DEL statements and a blocking SLEEP that aborts on cancellation.
pub struct MemoryService {
    name: String,
    store: Arc<Mutex<HashMap<String, String>>>,
}

impl MemoryService {
    pub fn new(name: String) -> Self {
        MemoryService {
            name,
            store: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

async fn execute_statement(
    statement: &str,
    store: &Arc<Mutex<HashMap<String, String>>>,
    cancel: &CancellationToken,
    writable: bool,
) -> Result<QueryResult, ServiceError> {
    let mut parts = statement.split_whitespace();
    let verb = parts.next().unwrap_or("").to_ascii_uppercase();
    match verb.as_str() {
        "SET" => {
            if !writable {
                return Err(ServiceError::new("read-only: SET is not allowed"));
            }
            let key = parts
                .next()
                .ok_or_else(|| ServiceError::new("SET requires a key"))?;
            let value = parts.collect::<Vec<_>>().join(" ");
            store.lock().unwrap().insert(key.to_string(), value);
            Ok(stub_result(&[], vec![], 1))
        }
        "GET" => {
            let key = parts
                .next()
                .ok_or_else(|| ServiceError::new("GET requires a key"))?;
            let value = store.lock().unwrap().get(key).cloned();
            let rows = match value {
                Some(v) => vec![vec![Some(key.to_string()), Some(v)]],
                None => vec![],
            };
            Ok(stub_result(&["key", "value"], rows, 0))
        }
        "DEL" => {
            if !writable {
                return Err(ServiceError::new("read-only: DEL is not allowed"));
            }
            let key = parts
                .next()
                .ok_or_else(|| ServiceError::new("DEL requires a key"))?;
            let removed = store.lock().unwrap().remove(key).is_some();
            Ok(stub_result(&[], vec![], u64::from(removed)))
        }
        "SLEEP" => {
            // Blocking statement: cancellation aborts it through the token.
            let ms = parts
                .next()
                .and_then(|p| p.parse::<u64>().ok())
                .unwrap_or(0);
            tokio::select! {
                _ = cancel.cancelled() => Err(ServiceError::canceled("request canceled")),
                _ = tokio::time::sleep(Duration::from_millis(ms)) => {
                    Ok(stub_result(&["slept"], vec![vec![Some(ms.to_string())]], 0))
                }
            }
        }
        _ => Err(ServiceError::new(format!(
            "unsupported statement: {statement}"
        ))),
    }
}

impl SessionService for MemoryService {
    fn execute(
        &self,
        request: StatementRequest,
        cancel: CancellationToken,
    ) -> ServiceFuture<'static, QueryResult> {
        let store = self.store.clone();
        Box::pin(async move { execute_statement(&request.statement, &store, &cancel, true).await })
    }

    fn execute_read_only(
        &self,
        request: StatementRequest,
        cancel: CancellationToken,
    ) -> ServiceFuture<'static, QueryResult> {
        let store = self.store.clone();
        Box::pin(async move { execute_statement(&request.statement, &store, &cancel, false).await })
    }

    fn validate(
        &self,
        _request: StatementRequest,
        _cancel: CancellationToken,
    ) -> ServiceFuture<'static, ()> {
        Box::pin(async { Ok(()) })
    }

    fn list_schema(
        &self,
        _request: EmptyRequest,
        _cancel: CancellationToken,
    ) -> ServiceFuture<'static, Vec<SchemaObject>> {
        let name = self.name.clone();
        let row_count = self.store.lock().unwrap().len() as u64;
        Box::pin(async move {
            Ok(vec![SchemaObject {
                database: name,
                type_: "table".to_string(),
                name: "kv".to_string(),
                row_count: Some(row_count),
            }])
        })
    }

    fn table_info(
        &self,
        _request: TableRequest,
        _cancel: CancellationToken,
    ) -> ServiceFuture<'static, Vec<ColumnInfo>> {
        Box::pin(async {
            Ok(vec![
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
                    name: "value".to_string(),
                    type_: "string".to_string(),
                    attributes: String::new(),
                    nullable: true,
                    default_value: None,
                    primary_key: 0,
                    indexes: vec![],
                },
            ])
        })
    }

    fn list_indexes(
        &self,
        _request: TableRequest,
        _cancel: CancellationToken,
    ) -> ServiceFuture<'static, Vec<IndexInfo>> {
        Box::pin(async {
            Ok(vec![IndexInfo {
                name: "PRIMARY".to_string(),
                unique: true,
                primary_key: true,
                columns: vec!["key".to_string()],
            }])
        })
    }

    fn create_index(
        &self,
        _request: IndexChangeRequest,
        _cancel: CancellationToken,
    ) -> ServiceFuture<'static, ()> {
        Box::pin(async {
            Err(ServiceError::new(
                "the demo store has a fixed primary index",
            ))
        })
    }

    fn replace_index(
        &self,
        _request: ReplaceIndexRequest,
        _cancel: CancellationToken,
    ) -> ServiceFuture<'static, ()> {
        Box::pin(async {
            Err(ServiceError::new(
                "the demo store has a fixed primary index",
            ))
        })
    }

    fn drop_index(
        &self,
        _request: DropRequest,
        _cancel: CancellationToken,
    ) -> ServiceFuture<'static, ()> {
        Box::pin(async {
            Err(ServiceError::new(
                "the demo store has a fixed primary index",
            ))
        })
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
            all.insert(
                "kv".to_string(),
                vec![IndexInfo {
                    name: "PRIMARY".to_string(),
                    unique: true,
                    primary_key: true,
                    columns: vec!["key".to_string()],
                }],
            );
            Ok(all)
        })
    }

    fn create_foreign_key(
        &self,
        _request: ForeignKeyChangeRequest,
        _cancel: CancellationToken,
    ) -> ServiceFuture<'static, ()> {
        Box::pin(async { Err(ServiceError::new("the demo store has no foreign keys")) })
    }

    fn replace_foreign_key(
        &self,
        _request: ReplaceForeignKeyRequest,
        _cancel: CancellationToken,
    ) -> ServiceFuture<'static, ()> {
        Box::pin(async { Err(ServiceError::new("the demo store has no foreign keys")) })
    }

    fn drop_foreign_key(
        &self,
        _request: DropRequest,
        _cancel: CancellationToken,
    ) -> ServiceFuture<'static, ()> {
        Box::pin(async { Err(ServiceError::new("the demo store has no foreign keys")) })
    }

    fn alter_column(
        &self,
        _request: ColumnChangeRequest,
        _cancel: CancellationToken,
    ) -> ServiceFuture<'static, ()> {
        Box::pin(async { Err(ServiceError::new("the demo store has a fixed schema")) })
    }

    fn drop_column(
        &self,
        _request: DropRequest,
        _cancel: CancellationToken,
    ) -> ServiceFuture<'static, ()> {
        Box::pin(async { Err(ServiceError::new("the demo store has a fixed schema")) })
    }

    fn add_column(
        &self,
        _request: AddColumnRequest,
        _cancel: CancellationToken,
    ) -> ServiceFuture<'static, ()> {
        Box::pin(async { Err(ServiceError::new("the demo store has a fixed schema")) })
    }

    fn browse_table(
        &self,
        request: BrowseTableRequest,
        _cancel: CancellationToken,
    ) -> ServiceFuture<'static, QueryResult> {
        let store = self.store.clone();
        Box::pin(async move {
            let entries: Vec<(String, String)> = {
                let lock = store.lock().unwrap();
                let mut v: Vec<(String, String)> = lock
                    .iter()
                    .map(|(k, val)| (k.clone(), val.clone()))
                    .collect();
                v.sort();
                v
            };
            let offset = request.options.offset.unwrap_or(0) as usize;
            let limit = request
                .options
                .limit
                .map(|l| l as usize)
                .unwrap_or(entries.len());
            let page: Vec<Vec<Option<String>>> = entries
                .iter()
                .skip(offset)
                .take(limit)
                .map(|(k, v)| vec![Some(k.clone()), Some(v.clone())])
                .collect();
            Ok(stub_result(&["key", "value"], page, 0))
        })
    }

    fn close(&self) {}
}

/// Builds targets and opens sessions against the in-memory test store.
/// Replaced by the Redis adapter in a later change.
#[derive(Default)]
pub struct MemoryFactory {}

impl SessionFactory for MemoryFactory {
    fn build_target(&self, values: &FormValues) -> BuildTargetResult {
        let host = values
            .host
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or("127.0.0.1");
        let port = values
            .port
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or("6379");
        let database = values
            .database
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or("0");
        BuildTargetResult {
            target: format!("redis:{host}:{port}/{database}"),
            ok: true,
        }
    }

    fn open(&self, target: &str) -> Result<(DatabaseInfo, Box<dyn SessionService>), ServiceError> {
        Ok((
            DatabaseInfo {
                product: "Redis (stub)".to_string(),
                version: "0.0.0".to_string(),
            },
            Box::new(MemoryService::new(target.trim().to_string())),
        ))
    }
}
