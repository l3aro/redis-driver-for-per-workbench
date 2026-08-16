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

/// The active structured target of one `perk/v1/workspace_view` request:
/// the scope kind plus the identifiers the kind needs. `kind` is one of
/// `database`/`schema`/`table`; the identifier fields carry the scope's
/// names — `database`/`schema` for the scope kinds, and for a table
/// target the qualified `table` plus the table's `database`/`schema`
/// identifiers preserved at selection time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceViewTarget {
    pub kind: crate::dto::capabilities::WorkspaceViewScope,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub database: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub schema: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub table: String,
}

/// One `perk/v1/workspace_view` handler request (params minus
/// `session_id`): an advertised custom view id and the active structured
/// target. The result reuses the bounded table-result conventions of
/// `QueryResult` (500 rows / 300 runes per cell).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct WorkspaceViewRequest {
    pub view_id: String,
    pub target: WorkspaceViewTarget,
}

/// Session methods that carry only `session_id`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct EmptyRequest {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dto::capabilities::WorkspaceViewScope;

    #[test]
    fn workspace_view_request_decodes_the_exact_handler_shape() {
        // The canonical wire params minus session_id: the host strips
        // `session_id` before dispatch, leaving exactly this object.
        let request: WorkspaceViewRequest = serde_json::from_str(
            r#"{
                "view_id": "server",
                "target": {
                    "kind": "table",
                    "database": "db2",
                    "table": "keys"
                }
            }"#,
        )
        .unwrap();
        assert_eq!(request.view_id, "server");
        assert_eq!(request.target.kind, WorkspaceViewScope::Table);
        assert_eq!(request.target.database, "db2");
        assert_eq!(request.target.table, "keys");
        assert!(request.target.schema.is_empty());
    }

    #[test]
    fn workspace_view_request_accepts_database_and_schema_targets() {
        let database: WorkspaceViewRequest = serde_json::from_str(
            r#"{"view_id":"server","target":{"kind":"database","database":"db2"}}"#,
        )
        .unwrap();
        assert_eq!(database.target.kind, WorkspaceViewScope::Database);
        assert_eq!(database.target.database, "db2");

        let schema: WorkspaceViewRequest = serde_json::from_str(
            r#"{"view_id":"server","target":{"kind":"schema","database":"db2","schema":"public"}}"#,
        )
        .unwrap();
        assert_eq!(schema.target.kind, WorkspaceViewScope::Schema);
        assert_eq!(schema.target.schema, "public");
    }

    #[test]
    fn workspace_view_request_rejects_missing_view_id_or_target() {
        for (label, wire) in [
            ("missing view_id", r#"{"target":{"kind":"database"}}"#),
            ("missing target", r#"{"view_id":"server"}"#),
            ("missing kind", r#"{"view_id":"server","target":{}}"#),
        ] {
            assert!(
                serde_json::from_str::<WorkspaceViewRequest>(wire).is_err(),
                "{label} must not decode"
            );
        }
    }
}
