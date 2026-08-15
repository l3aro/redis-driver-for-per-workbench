//! Shared service DTOs: database info, schema objects, query results,
//! table/index/foreign-key shapes, and browse options.

use serde::{Deserialize, Serialize};

use super::write::DocumentPayload;

/// Display-only product identity; the workbench never branches on it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatabaseInfo {
    pub product: String,
    pub version: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaObject {
    pub database: String,
    #[serde(rename = "type")]
    pub type_: String,
    pub name: String,
    /// Estimate where the engine exposes one; null/absent when unknown.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row_count: Option<u64>,
}

/// Execution and browse result (`perk/v1/execute`, `execute_read_only`,
/// `browse_table`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub column_types: Vec<String>,
    /// Display cells; null is SQL NULL.
    pub rows: Vec<Vec<Option<String>>>,
    /// Full cell values, parallel to `rows`, for the cell viewer.
    pub untruncated_rows: Vec<Vec<Option<String>>>,
    pub rows_affected: u64,
    pub has_more: bool,
    /// Integer nanoseconds.
    pub duration_ns: u64,
    pub truncated: bool,
    /// One stable document identity per row; null/absent when not
    /// document-capable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub document_ids: Option<Vec<DocumentPayload>>,
    /// The backend-native statement for the operation that produced this
    /// result (the exact command accepted by this plugin's parser, or
    /// the pseudo-command `execute` can replay for a browse). Omitted
    /// when absent; the host logs it in place of the generic preview and
    /// never executes it itself.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub statement: Option<String>,
    /// Structured metadata for `statement`; meaningful only when
    /// `statement` is nonblank, so it is always paired with one. Omitted
    /// (or null) keeps the legacy defaults (replayable, not sensitive).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub statement_metadata: Option<StatementMetadata>,
}

/// Structured metadata for a backend-native `statement`. All three
/// fields are required whenever the object is present; the object is
/// only ever emitted together with a nonblank `statement`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatementMetadata {
    /// The backend statement language, carried on query-log entries.
    pub language: String,
    /// Whether pasting the statement into this plugin's editor
    /// reproduces the operation; `false` disables copy/re-run/explain.
    pub replayable: bool,
    /// Whether the statement embeds values that must never be stored
    /// verbatim; the host persists a redacted marker and forces the
    /// entry non-replayable.
    pub sensitive: bool,
}

impl StatementMetadata {
    /// Redis-language metadata: `replayable` when pasting the statement
    /// into the plugin's editor reproduces the operation, `sensitive`
    /// when the statement embeds a value/payload that must never be
    /// stored verbatim.
    pub fn redis(replayable: bool, sensitive: bool) -> Self {
        StatementMetadata {
            language: "redis".to_string(),
            replayable,
            sensitive,
        }
    }
}

/// Index kind enum values: 1 primary key, 2 unique, 3 regular.
pub type IndexKind = u8;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    #[serde(rename = "type")]
    pub type_: String,
    pub attributes: String,
    pub nullable: bool,
    /// Serialized as null when unknown.
    pub default_value: Option<String>,
    /// Position in the primary key; 0 = not part of it.
    pub primary_key: u32,
    pub indexes: Vec<IndexKind>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    #[serde(rename = "type")]
    pub type_: String,
    pub nullable: bool,
    pub default_value: Option<String>,
    pub attributes: Option<String>,
}

/// Renames when `previous_name != name`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnChange {
    pub previous_name: String,
    pub name: String,
    #[serde(rename = "type")]
    pub type_: String,
    pub nullable: bool,
    pub default_value: Option<String>,
    pub attributes: Option<String>,
}

/// `IndexInfo` and `IndexChange` share one shape; `change` carries the new name.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexInfo {
    pub name: String,
    pub unique: bool,
    pub primary_key: bool,
    pub columns: Vec<String>,
}

pub type IndexChange = IndexInfo;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForeignKeyInfo {
    pub id: String,
    pub columns: Vec<String>,
    pub reference_table: String,
    pub reference_columns: Vec<String>,
    pub on_delete: String,
    pub on_update: String,
}

/// `ForeignKeyInfo` plus the table that declares the foreign key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReferencingForeignKeyInfo {
    pub id: String,
    pub columns: Vec<String>,
    pub reference_table: String,
    pub reference_columns: Vec<String>,
    pub on_delete: String,
    pub on_update: String,
    pub table: String,
}

/// Column counts must match between `columns` and `reference_columns`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForeignKeyChange {
    pub columns: Vec<String>,
    pub reference_table: String,
    pub reference_columns: Vec<String>,
    pub on_delete: String,
    pub on_update: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowseFilter {
    pub column: String,
    pub operator: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowseSort {
    pub column: String,
    pub descending: bool,
}

/// `offset`/`limit` default to 0 / unbounded.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowseOptions {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub columns: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filters: Option<Vec<BrowseFilter>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sorts: Option<Vec<BrowseSort>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A minimal result with every optional field absent: the exact
    /// legacy wire shape before statement metadata existed.
    fn legacy_result() -> QueryResult {
        QueryResult {
            columns: vec!["key".to_string()],
            column_types: vec!["string".to_string()],
            rows: vec![vec![Some("k".to_string())]],
            untruncated_rows: vec![vec![Some("k".to_string())]],
            rows_affected: 0,
            has_more: false,
            duration_ns: 0,
            truncated: false,
            document_ids: None,
            statement: None,
            statement_metadata: None,
        }
    }

    fn sample_result() -> QueryResult {
        QueryResult {
            statement: Some("GET user:1".to_string()),
            statement_metadata: Some(StatementMetadata::redis(true, false)),
            ..legacy_result()
        }
    }

    #[test]
    fn query_result_omits_statement_and_metadata_when_absent() {
        let plain = serde_json::to_string(&legacy_result()).unwrap();
        assert!(
            !plain.contains("statement"),
            "legacy shape must not carry statement keys: {plain}"
        );
        let value: serde_json::Value = serde_json::from_str(&plain).unwrap();
        assert!(value.get("statement").is_none());
        assert!(value.get("statement_metadata").is_none());
    }

    #[test]
    fn query_result_round_trips_statement_with_metadata() {
        let result = sample_result();
        let json = serde_json::to_string(&result).unwrap();
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(value["statement"], "GET user:1");
        assert_eq!(
            value["statement_metadata"],
            serde_json::json!({"language": "redis", "replayable": true, "sensitive": false})
        );
        let back: QueryResult = serde_json::from_str(&json).unwrap();
        assert_eq!(back, result);
    }

    #[test]
    fn query_result_legacy_decode_defaults_statement_fields() {
        // A legacy result without the new keys decodes with both absent.
        let mut legacy = legacy_result();
        legacy.statement = None;
        legacy.statement_metadata = None;
        let json = serde_json::to_string(&legacy).unwrap();
        let back: QueryResult = serde_json::from_str(&json).unwrap();
        assert_eq!(back.statement, None);
        assert_eq!(back.statement_metadata, None);
    }

    #[test]
    fn statement_metadata_redis_requires_all_three_fields() {
        let metadata = StatementMetadata::redis(false, true);
        assert_eq!(
            serde_json::to_string(&metadata).unwrap(),
            r#"{"language":"redis","replayable":false,"sensitive":true}"#
        );
        // All three fields are required on decode too: a present object
        // never loses one.
        let decoded: StatementMetadata =
            serde_json::from_str(r#"{"language":"redis","replayable":false,"sensitive":true}"#)
                .unwrap();
        assert_eq!(decoded, metadata);
    }

    #[test]
    fn query_result_statement_and_metadata_travel_together() {
        // The plugin only ever emits metadata next to a nonblank
        // statement (the host rejects the orphan shape at its
        // boundary), so the serialized pair round-trips as one object.
        let result = QueryResult {
            statement: Some("DEL user:1".to_string()),
            statement_metadata: Some(StatementMetadata::redis(true, false)),
            ..legacy_result()
        };
        let json = serde_json::to_string(&result).unwrap();
        let back: QueryResult = serde_json::from_str(&json).unwrap();
        assert_eq!(back.statement.as_deref(), Some("DEL user:1"));
        assert!(back.statement_metadata.is_some());
    }
}
