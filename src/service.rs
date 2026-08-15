//! Session service contract, plus the in-memory service used as a
//! transport test double. Production sessions come from
//! [`crate::redis_service::RedisFactory`]; the trait below is the seam
//! both plug into.

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
use crate::dto::write::{RowValue, RowWriteRequest, RowWriteResponse, RowsAffected};
use crate::protocol::{ERR_CANCELED, ERR_INVALID_PARAMS, ERR_METHOD_NOT_FOUND, ErrorKind};

pub type ServiceFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, ServiceError>> + Send + 'a>>;

/// The future returned by [`SessionFactory::open`]. Boxed so the trait
/// stays dyn-compatible (the server holds `Arc<dyn SessionFactory>`).
pub type OpenFuture<'a> = Pin<
    Box<
        dyn Future<Output = Result<(DatabaseInfo, Box<dyn SessionService>), ServiceError>>
            + Send
            + 'a,
    >,
>;

/// Handler error. `code: None` becomes the JSON-RPC internal error
/// (-32603); a present code is used as-is (e.g. -32800 canceled). Every
/// error carries a normalized [`ErrorKind`], serialized into the
/// structured `data` provenance of the response (kind + plugin + wire
/// method); the kind never leaks into the message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceError {
    pub code: Option<i32>,
    pub message: String,
    pub kind: ErrorKind,
}

impl ServiceError {
    /// Generic operation failure: the fallback kind.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            code: None,
            message: message.into(),
            kind: ErrorKind::Operation,
        }
    }

    /// The operation was rejected as invalid (parse, params, schema,
    /// or row-write input).
    pub fn validation(message: impl Into<String>) -> Self {
        Self {
            code: None,
            message: message.into(),
            kind: ErrorKind::Validation,
        }
    }

    /// Credentials were missing or rejected.
    pub fn authentication(message: impl Into<String>) -> Self {
        Self {
            code: None,
            message: message.into(),
            kind: ErrorKind::Authentication,
        }
    }

    /// The backend connection failed or dropped.
    pub fn connection(message: impl Into<String>) -> Self {
        Self {
            code: None,
            message: message.into(),
            kind: ErrorKind::Connection,
        }
    }

    /// The backend does not support the operation.
    pub fn unsupported(message: impl Into<String>) -> Self {
        Self {
            code: None,
            message: message.into(),
            kind: ErrorKind::Unsupported,
        }
    }

    /// The operation was canceled: -32800, the perk/v1 cancellation code.
    pub fn canceled(message: impl Into<String>) -> Self {
        Self {
            code: Some(ERR_CANCELED),
            message: message.into(),
            kind: ErrorKind::Cancelled,
        }
    }

    /// Invalid params: -32602 plus the validation kind.
    pub fn invalid_params(message: impl Into<String>) -> Self {
        Self {
            code: Some(ERR_INVALID_PARAMS),
            message: message.into(),
            kind: ErrorKind::Validation,
        }
    }

    /// Method not found: -32601 plus the unsupported kind.
    pub fn method_not_found(message: impl Into<String>) -> Self {
        Self {
            code: Some(ERR_METHOD_NOT_FOUND),
            message: message.into(),
            kind: ErrorKind::Unsupported,
        }
    }

    pub fn jsonrpc_code(&self) -> i32 {
        self.code.unwrap_or(crate::protocol::ERR_INTERNAL)
    }
}

/// The 20 mandatory session handlers, the optional row_write handler
/// (advertised by `write_capabilities.row_writer`), and the close hook.
/// Every method observes cancellation through `cancel`; a canceled handler
/// must return a `-32800` error. Handlers are `Send` and may run
/// concurrently.
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
    /// Optional `perk/v1/row_write` handler: insert/update/delete one row
    /// of a virtual table. Services that do not advertise
    /// `write_capabilities.row_writer` may stub it; the transport never
    /// routes to it then.
    fn row_write(
        &self,
        request: RowWriteRequest,
        cancel: CancellationToken,
    ) -> ServiceFuture<'static, RowWriteResponse>;
    /// Close hook. The server removes the session before calling this, so
    /// a failing hook can never resurrect the session.
    fn close(&self);
}

/// Creates sessions and serializes connection forms. The Redis adapter
/// (see [`crate::redis_service`]) is the production implementation;
/// [`MemoryFactory`] is the transport test double.
pub trait SessionFactory: Send + Sync {
    fn build_target(&self, values: &FormValues) -> BuildTargetResult;
    /// Opens one session. The target is the connection target with a
    /// stripped label prefix (or a whole URL-scheme target).
    fn open<'a>(&'a self, target: &'a str) -> OpenFuture<'a>;
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
        statement: None,
        statement_metadata: None,
    }
}

/// In-memory key-value session service: the transport test double. Proves
/// full perk/v1 routing with SET/GET/DEL statements and a blocking SLEEP
/// that aborts on cancellation, with no Redis required.
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
                return Err(ServiceError::unsupported("read-only: SET is not allowed"));
            }
            let key = parts
                .next()
                .ok_or_else(|| ServiceError::validation("SET requires a key"))?;
            let value = parts.collect::<Vec<_>>().join(" ");
            store.lock().unwrap().insert(key.to_string(), value);
            Ok(stub_result(&[], vec![], 1))
        }
        "GET" => {
            let key = parts
                .next()
                .ok_or_else(|| ServiceError::validation("GET requires a key"))?;
            let value = store.lock().unwrap().get(key).cloned();
            let rows = match value {
                Some(v) => vec![vec![Some(key.to_string()), Some(v)]],
                None => vec![],
            };
            Ok(stub_result(&["key", "value"], rows, 0))
        }
        "DEL" => {
            if !writable {
                return Err(ServiceError::unsupported("read-only: DEL is not allowed"));
            }
            let key = parts
                .next()
                .ok_or_else(|| ServiceError::validation("DEL requires a key"))?;
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
        _ => Err(ServiceError::unsupported(format!(
            "unsupported statement: {statement}"
        ))),
    }
}

/// One cell of the memory double must be a plain string; every other
/// Value kind is rejected, mirroring the Redis service's rules.
fn memory_string_cell(cell: &RowValue) -> Result<String, ServiceError> {
    if cell.value.kind != "string" {
        return Err(ServiceError::validation(format!(
            "column {} must be a string value (got {} kind)",
            cell.name, cell.value.kind
        )));
    }
    cell.value.string.clone().ok_or_else(|| {
        ServiceError::validation(format!(
            "column {}: string kind without a payload",
            cell.name
        ))
    })
}

/// The primary-key identity of a memory-double row write: exactly one
/// string `key` cell. An empty string is a valid key; only the rename
/// destination is rejected as empty.
fn memory_identity(cells: Option<&[RowValue]>) -> Result<String, ServiceError> {
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
        found = Some(memory_string_cell(cell)?);
    }
    found.ok_or_else(|| ServiceError::validation("missing key fields"))
}

/// The changed-column list of a memory-double update: optional `key`
/// rename destination and `value`; at least one change is required.
fn memory_update_fields(
    cells: Option<&[RowValue]>,
) -> Result<(Option<String>, Option<String>), ServiceError> {
    let Some(cells) = cells else {
        return Err(ServiceError::validation("update requires a values payload"));
    };
    let mut rename_to: Option<String> = None;
    let mut value: Option<String> = None;
    for cell in cells {
        match cell.name.as_str() {
            "key" => {
                if rename_to.is_some() {
                    return Err(ServiceError::validation("duplicate column: key"));
                }
                let dst = memory_string_cell(cell)?;
                if dst.is_empty() {
                    return Err(ServiceError::validation(
                        "rename destination must not be empty",
                    ));
                }
                rename_to = Some(dst);
            }
            "value" => {
                if value.is_some() {
                    return Err(ServiceError::validation("duplicate column: value"));
                }
                value = Some(memory_string_cell(cell)?);
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

/// The insert cells of a memory-double insert: required `key`, optional
/// `value` (defaults to the empty string).
fn memory_insert_fields(cells: Option<&[RowValue]>) -> Result<(String, String), ServiceError> {
    let Some(cells) = cells else {
        return Err(ServiceError::validation("insert requires a values payload"));
    };
    let mut key: Option<String> = None;
    let mut value: Option<String> = None;
    for cell in cells {
        match cell.name.as_str() {
            "key" => {
                if key.is_some() {
                    return Err(ServiceError::validation("duplicate column: key"));
                }
                key = Some(memory_string_cell(cell)?);
            }
            "value" => {
                if value.is_some() {
                    return Err(ServiceError::validation("duplicate column: value"));
                }
                value = Some(memory_string_cell(cell)?);
            }
            other => {
                return Err(ServiceError::validation(format!("unknown column: {other}")));
            }
        }
    }
    let key = key.ok_or_else(|| ServiceError::validation("insert requires a key column"))?;
    Ok((key, value.unwrap_or_default()))
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
            Err(ServiceError::unsupported(
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
            Err(ServiceError::unsupported(
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
            Err(ServiceError::unsupported(
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
        Box::pin(async {
            Err(ServiceError::unsupported(
                "the demo store has no foreign keys",
            ))
        })
    }

    fn replace_foreign_key(
        &self,
        _request: ReplaceForeignKeyRequest,
        _cancel: CancellationToken,
    ) -> ServiceFuture<'static, ()> {
        Box::pin(async {
            Err(ServiceError::unsupported(
                "the demo store has no foreign keys",
            ))
        })
    }

    fn drop_foreign_key(
        &self,
        _request: DropRequest,
        _cancel: CancellationToken,
    ) -> ServiceFuture<'static, ()> {
        Box::pin(async {
            Err(ServiceError::unsupported(
                "the demo store has no foreign keys",
            ))
        })
    }

    fn alter_column(
        &self,
        _request: ColumnChangeRequest,
        _cancel: CancellationToken,
    ) -> ServiceFuture<'static, ()> {
        Box::pin(async {
            Err(ServiceError::unsupported(
                "the demo store has a fixed schema",
            ))
        })
    }

    fn drop_column(
        &self,
        _request: DropRequest,
        _cancel: CancellationToken,
    ) -> ServiceFuture<'static, ()> {
        Box::pin(async {
            Err(ServiceError::unsupported(
                "the demo store has a fixed schema",
            ))
        })
    }

    fn add_column(
        &self,
        _request: AddColumnRequest,
        _cancel: CancellationToken,
    ) -> ServiceFuture<'static, ()> {
        Box::pin(async {
            Err(ServiceError::unsupported(
                "the demo store has a fixed schema",
            ))
        })
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

    fn row_write(
        &self,
        request: RowWriteRequest,
        _cancel: CancellationToken,
    ) -> ServiceFuture<'static, RowWriteResponse> {
        let store = self.store.clone();
        Box::pin(async move {
            if request.table != "kv" {
                return Err(ServiceError::unsupported(format!(
                    "unknown table: {}",
                    request.table
                )));
            }
            let affected = match request.operation.as_str() {
                "insert" => {
                    let (key, value) = memory_insert_fields(request.values.as_deref())?;
                    let mut lock = store.lock().unwrap();
                    if lock.contains_key(&key) {
                        return Err(ServiceError::new(format!("key already exists: {key}")));
                    }
                    lock.insert(key, value);
                    1
                }
                "update" => {
                    let identity = memory_identity(request.key.as_deref())?;
                    let (rename_to, value) = memory_update_fields(request.values.as_deref())?;
                    let mut lock = store.lock().unwrap();
                    if !lock.contains_key(&identity) {
                        return Err(ServiceError::new(format!("key not found: {identity}")));
                    }
                    let destination = match &rename_to {
                        Some(dst) => {
                            if *dst != identity && lock.contains_key(dst) {
                                return Err(ServiceError::new(format!(
                                    "destination key already exists: {dst}"
                                )));
                            }
                            dst.clone()
                        }
                        None => identity.clone(),
                    };
                    // Move the row to its destination: a rename keeps the
                    // old value, a value change replaces it, and a
                    // same-name update removes and reinserts in place.
                    let old_value = lock.remove(&identity);
                    if let Some(stored) = value.or(old_value) {
                        lock.insert(destination, stored);
                    }
                    1
                }
                "delete" => {
                    let identity = memory_identity(request.key.as_deref())?;
                    u64::from(store.lock().unwrap().remove(&identity).is_some())
                }
                other => {
                    return Err(ServiceError::validation(format!(
                        "unsupported row_write operation: {other}"
                    )));
                }
            };
            Ok(RowWriteResponse {
                result: RowsAffected {
                    rows_affected: affected,
                    // The in-memory double is a transport fixture, not a
                    // real backend: no native statement to report.
                    statement: String::new(),
                    statement_metadata: None,
                },
            })
        })
    }

    fn close(&self) {}
}

/// Builds targets and opens sessions against the in-memory test store.
/// Transport tests pass this factory into [`crate::server::run`]; the
/// production binary uses [`crate::redis_service::RedisFactory`].
#[derive(Default)]
pub struct MemoryFactory {}

impl SessionFactory for MemoryFactory {
    fn build_target(&self, values: &FormValues) -> BuildTargetResult {
        // Mirrors the real factory's serialization so the transport test
        // exercises the production wire format (credentials are ignored
        // by the in-memory double).
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
            target: format!("redis:redis://{host}:{port}/{database}"),
            ok: true,
        }
    }

    fn open<'a>(&'a self, target: &'a str) -> OpenFuture<'a> {
        let service: Box<dyn SessionService> =
            Box::new(MemoryService::new(target.trim().to_string()));
        Box::pin(async move {
            Ok((
                DatabaseInfo {
                    product: "Redis (stub)".to_string(),
                    version: "0.0.0".to_string(),
                },
                service,
            ))
        })
    }
}
