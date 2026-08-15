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
