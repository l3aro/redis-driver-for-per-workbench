//! Row and document write DTOs. The Redis plugin advertises
//! `row_writer: false` and no document capability, so these types are
//! part of the wire contract but are never produced or accepted by the
//! session service in this atom; the Redis adapter constructs them later.
#![allow(dead_code)]

use serde::{Deserialize, Serialize};

/// Tagged cell payload. Exactly the payload matching `kind` is meaningful;
/// the others are omitted.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Value {
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub string: Option<String>,
    #[serde(rename = "bool", default, skip_serializing_if = "Option::is_none")]
    pub bool_: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub integer: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub float: Option<f64>,
    /// Base64 JSON string.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bytes: Option<String>,
    /// Exact decimal text.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decimal: Option<String>,
    /// RFC 3339 string.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub array: Option<Vec<Value>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object: Option<Vec<NamedValue>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NamedValue {
    pub name: String,
    pub value: Value,
}

/// One column of a row write; ordering is the caller's and must be preserved.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RowValue {
    pub name: String,
    pub value: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RowWriteRequest {
    pub operation: String,
    pub table: String,
    /// Row identity for update/delete.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<Vec<RowValue>>,
    /// Insert/update payload.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub values: Option<Vec<RowValue>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowsAffected {
    pub rows_affected: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowWriteResponse {
    pub result: RowsAffected,
}

/// Document bytes; `data` is base64.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocumentPayload {
    pub format: String,
    pub data: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DocumentWriteRequest {
    pub operation: String,
    pub collection: String,
    /// Serialized as null when unset.
    #[serde(default)]
    pub id: Option<DocumentPayload>,
    /// Serialized as null when unset.
    #[serde(default)]
    pub document: Option<DocumentPayload>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DocumentWriteResponse {
    pub result: RowsAffected,
    /// Set for read operations; serialized as null when unset.
    #[serde(default)]
    pub document: Option<DocumentPayload>,
}
