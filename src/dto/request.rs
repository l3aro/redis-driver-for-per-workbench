//! Request/response DTOs for lifecycle methods and session handlers.
//! Session handler shapes are the wire params **minus** `session_id`
//! (the server strips it before dispatch).

use serde::{Deserialize, Serialize};

use super::service::{
    BrowseOptions, ColumnChange, ColumnDef, DatabaseInfo, ForeignKeyChange, IndexChange,
};

/// `perk/v1/open` params.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct OpenRequest {
    pub target: String,
}

/// `perk/v1/close` params.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct CloseRequest {
    pub session_id: u64,
}

/// `perk/v1/cancel` notification params: the original request id.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct CancelRequest {
    pub id: u64,
}

/// `perk/v1/build_target` result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BuildTargetResult {
    pub target: String,
    pub ok: bool,
}

/// `perk/v1/open` result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OpenResult {
    pub session_id: u64,
    pub info: DatabaseInfo,
}

// --- Session handler requests (params minus session_id) ---

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct StatementRequest {
    pub statement: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct TableRequest {
    pub table: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct IndexChangeRequest {
    pub table: String,
    pub change: IndexChange,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct ReplaceIndexRequest {
    pub table: String,
    pub old_name: String,
    pub change: IndexChange,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct DropRequest {
    pub table: String,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct ForeignKeyChangeRequest {
    pub table: String,
    pub change: ForeignKeyChange,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct ReplaceForeignKeyRequest {
    pub table: String,
    pub old_name: String,
    pub change: ForeignKeyChange,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct ColumnChangeRequest {
    pub table: String,
    pub change: ColumnChange,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct AddColumnRequest {
    pub table: String,
    pub def: ColumnDef,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct BrowseTableRequest {
    pub table: String,
    pub options: BrowseOptions,
}

/// Session methods that carry only `session_id`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct EmptyRequest {}
