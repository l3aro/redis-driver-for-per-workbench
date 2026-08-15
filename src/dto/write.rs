//! Row and document write DTOs. The Redis plugin advertises
//! `row_writer: true` and serves `perk/v1/row_write` over the virtual
//! `keys` table; document writes are not advertised, so the document
//! request/response types below are part of the wire contract but are
//! never produced or accepted.

use serde::{Deserialize, Serialize};

use super::service::StatementMetadata;

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
    /// Optional backend-native command that produced this result (e.g.
    /// `SET user:2 v NX`). The host logs it in place of the generic
    /// preview and never executes it itself; omitted from the wire when
    /// empty so older hosts see the prior shape. Replayability and
    /// sensitivity are described by `statement_metadata`.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub statement: String,
    /// Structured metadata for `statement`; meaningful only when
    /// `statement` is nonblank, so it is only ever emitted next to one.
    /// Omitted (or null) keeps the legacy defaults.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub statement_metadata: Option<StatementMetadata>,
}

impl RowsAffected {
    /// Reports one native statement with its metadata. Metadata is only
    /// meaningful with a nonblank statement, so a blank statement never
    /// carries metadata: the pairing invariant holds by construction.
    pub fn with_statement(
        rows_affected: u64,
        statement: String,
        metadata: StatementMetadata,
    ) -> Self {
        let statement_metadata = (!statement.trim().is_empty()).then_some(metadata);
        RowsAffected {
            rows_affected,
            statement,
            statement_metadata,
        }
    }
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

/// Document write RPCs are not advertised by this plugin
/// (`write_capabilities.document` is absent), so these wire DTOs are
/// never produced or accepted here.
#[allow(dead_code)]
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

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DocumentWriteResponse {
    pub result: RowsAffected,
    /// Set for read operations; serialized as null when unset.
    #[serde(default)]
    pub document: Option<DocumentPayload>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dto::service::StatementMetadata;

    #[test]
    fn rows_affected_omits_empty_statement_and_round_trips() {
        // Empty statement: the exact prior wire shape.
        let plain = RowWriteResponse {
            result: RowsAffected {
                rows_affected: 2,
                statement: String::new(),
                statement_metadata: None,
            },
        };
        assert_eq!(
            serde_json::to_string(&plain).unwrap(),
            r#"{"result":{"rows_affected":2}}"#
        );

        // A non-empty statement rides inside `result` and survives a
        // round trip.
        let native = RowWriteResponse {
            result: RowsAffected {
                rows_affected: 1,
                statement: "SET user:2 v NX".to_string(),
                statement_metadata: None,
            },
        };
        assert_eq!(
            serde_json::to_string(&native).unwrap(),
            r#"{"result":{"rows_affected":1,"statement":"SET user:2 v NX"}}"#
        );
        let back: RowWriteResponse =
            serde_json::from_str(r#"{"result":{"rows_affected":1,"statement":"SET user:2 v NX"}}"#)
                .unwrap();
        assert_eq!(back, native);

        // A legacy response without the key decodes with an empty
        // statement.
        let legacy: RowWriteResponse =
            serde_json::from_str(r#"{"result":{"rows_affected":3}}"#).unwrap();
        assert_eq!(legacy.result.rows_affected, 3);
        assert!(legacy.result.statement.is_empty());
        assert_eq!(legacy.result.statement_metadata, None);
    }

    #[test]
    fn rows_affected_pairs_statement_with_metadata() {
        let native = RowWriteResponse {
            result: RowsAffected::with_statement(
                1,
                "DEL user:2".to_string(),
                StatementMetadata::redis(true, false),
            ),
        };
        assert_eq!(
            serde_json::to_string(&native).unwrap(),
            r#"{"result":{"rows_affected":1,"statement":"DEL user:2","statement_metadata":{"language":"redis","replayable":true,"sensitive":false}}}"#
        );
        let back: RowWriteResponse =
            serde_json::from_str(r#"{"result":{"rows_affected":1,"statement":"DEL user:2","statement_metadata":{"language":"redis","replayable":true,"sensitive":false}}}"#)
                .unwrap();
        assert_eq!(back, native);

        // Legacy decode of the new shape: metadata absent -> None.
        let legacy: RowWriteResponse =
            serde_json::from_str(r#"{"result":{"rows_affected":1,"statement":"DEL user:2"}}"#)
                .unwrap();
        assert_eq!(legacy.result.statement_metadata, None);
    }

    #[test]
    fn with_statement_never_emits_metadata_for_a_blank_statement() {
        // The pairing invariant holds by construction: metadata is only
        // meaningful with a nonblank statement, so a blank statement
        // drops it.
        let blank =
            RowsAffected::with_statement(0, String::new(), StatementMetadata::redis(true, false));
        assert_eq!(blank.statement_metadata, None);
        assert_eq!(
            serde_json::to_string(&blank).unwrap(),
            r#"{"rows_affected":0}"#
        );
    }
}
