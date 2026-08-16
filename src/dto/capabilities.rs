//! Capabilities, connection form, and handshake DTOs.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// A target pattern addressed to this driver. Label prefixes end in `:`
/// and are stripped from the target before `open`; scheme prefixes
/// (`keep_target: true`) are passed to `open` whole.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TargetPattern {
    pub prefix: String,
    /// Omitted on the wire when false.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keep_target: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FormOption {
    pub label: String,
    pub value: String,
}

/// One connection-form field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FormField {
    pub key: String,
    pub title: String,
    /// 0 input, 1 password, 2 select.
    pub kind: u8,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub placeholder: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub options: Option<Vec<FormOption>>,
    /// 0 none, 1 required, 2 port (blank or 1-65535).
    pub validate: u8,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Connection form. `prefix` is prepended to the serialized target so the
/// host routes it back to this driver.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FormSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    pub fields: Vec<FormField>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocumentWriteCapability {
    pub format: String,
    pub text: bool,
}

/// Gates the optional `perk/v1/row_write` / `perk/v1/document_write` RPCs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriteCapabilities {
    pub row_writer: bool,
    /// Omitted (or null) when the driver has no document support.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub document: Option<DocumentWriteCapability>,
}

/// One static command entry of the query language advertisement: the
/// canonical command name, a Redis-native usage line, and a concise
/// summary. All three must be nonblank, bounded, and control-free; names
/// must be unique case-insensitively and the list is capped, so a plugin
/// can never force an unbounded completion list or handshake frame.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryCommand {
    pub name: String,
    pub usage: String,
    pub summary: String,
}

/// How the query editor presents this driver's statements: the language
/// name, the editor tab label, the input placeholder, an optional lexer
/// hint, optional example statements the driver's parser already
/// accepts, and an optional static command catalog for completion.
/// `name`, `editor_label`, and `placeholder` must be nonblank; `lexer`,
/// `examples`, and `commands` are omitted on the wire when absent
/// (Redis advertises no lexer: statements are commands, not SQL).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryLanguage {
    pub name: String,
    pub editor_label: String,
    pub placeholder: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lexer: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub examples: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commands: Option<Vec<QueryCommand>>,
}

/// One standard workspace tab a driver may explicitly advertise support
/// for: columns, indexes, foreign_keys, or diagram. Query and Browse are
/// never part of the advertisement — they keep their per-scope policy at
/// every driver. The wire values are the canonical fixed set; the host
/// rejects duplicates and unknown values at registration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceStandardTab {
    Columns,
    Indexes,
    ForeignKeys,
    Diagram,
}

/// The structured-target scope kinds a custom workspace view may serve.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceViewScope {
    Database,
    Schema,
    Table,
}

/// One plain-data workspace tab a driver advertises: a stable nonblank
/// `id` (echoed back on every workspace_view request), a human `label`
/// rendered in the workspace tab row, and the `scopes` it serves (one or
/// more of database/schema/table). It carries no code and no UI: the
/// host owns lifecycle, rendering, input, and cancellation; the driver
/// only answers bounded table data for the view. IDs and labels must be
/// nonblank, bounded, control-free, and unique case-insensitively
/// within the list; scopes must be nonempty and duplicate-free; the
/// list is capped at 8 entries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CustomWorkspaceView {
    pub id: String,
    pub label: String,
    pub scopes: Vec<WorkspaceViewScope>,
}

/// The optional workspace tab advertisement of a driver: the subset of
/// standard tabs it supports (columns, indexes, foreign_keys, diagram)
/// and its ordered custom plain-data views. Omitted (or null) keeps the
/// host's legacy per-product tab policy exactly, so plugins written
/// before this field existed load unchanged; a present-but-invalid
/// advertisement is rejected at registration. When present it is
/// authoritative: standard tabs are filtered by the explicit
/// advertisement, and custom views are appended after them in
/// advertised order, filtered by their scopes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceCapability {
    /// Omitted on the wire when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub standard_tabs: Option<Vec<WorkspaceStandardTab>>,
    /// Omitted on the wire when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub custom_views: Option<Vec<CustomWorkspaceView>>,
}

/// Driver advertisement returned by `perk/v1/initialize`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Capabilities {
    pub name: String,
    pub display: String,
    /// Omitted for target-only drivers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub targets: Option<Vec<TargetPattern>>,
    /// Omitted for target-only drivers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub form: Option<FormSpec>,
    /// Omitted when the driver does not advertise one (the host then
    /// falls back to the legacy SQL default).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_language: Option<QueryLanguage>,
    pub write_capabilities: WriteCapabilities,
    /// Omitted (or null) when the driver has no workspace tab
    /// advertisement; the host then keeps its legacy per-product tab
    /// policy. Gates the optional `perk/v1/workspace_view` RPC.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace: Option<WorkspaceCapability>,
}

/// `perk/v1/initialize` result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InitializeResult {
    pub protocol_version: u32,
    pub capabilities: Capabilities,
}

/// Driver-facing view of the connection form (`perk/v1/build_target` params).
/// Keys outside the fixed set land in `extras`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FormValues {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pass: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub database: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extras: Option<HashMap<String, String>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workspace_capability_serializes_the_exact_wire_shape() {
        // Mirrors the canonical initialize fixture: standard tab keys
        // use the fixed snake_case values, custom views carry id, label,
        // and scopes in order.
        let workspace = WorkspaceCapability {
            standard_tabs: Some(vec![WorkspaceStandardTab::Columns]),
            custom_views: Some(vec![CustomWorkspaceView {
                id: "server".to_string(),
                label: "Server".to_string(),
                scopes: vec![WorkspaceViewScope::Database, WorkspaceViewScope::Table],
            }]),
        };
        assert_eq!(
            serde_json::to_string(&workspace).unwrap(),
            r#"{"standard_tabs":["columns"],"custom_views":[{"id":"server","label":"Server","scopes":["database","table"]}]}"#
        );
    }

    #[test]
    fn workspace_capability_omits_absent_parts_and_decodes_legacy() {
        let empty = WorkspaceCapability {
            standard_tabs: None,
            custom_views: None,
        };
        assert_eq!(
            serde_json::to_string(&empty).unwrap(),
            "{}",
            "absent parts must be omitted, never null"
        );
        // An advertisement with only custom views keeps the same shape
        // on decode.
        let decoded: WorkspaceCapability = serde_json::from_str(
            r#"{"custom_views":[{"id":"server","label":"Server","scopes":["table"]}]}"#,
        )
        .unwrap();
        assert!(decoded.standard_tabs.is_none());
        assert_eq!(decoded.custom_views.unwrap()[0].id, "server");
    }

    #[test]
    fn standard_tab_and_scope_wires_are_the_fixed_values() {
        for (tab, wire) in [
            (WorkspaceStandardTab::Columns, "columns"),
            (WorkspaceStandardTab::Indexes, "indexes"),
            (WorkspaceStandardTab::ForeignKeys, "foreign_keys"),
            (WorkspaceStandardTab::Diagram, "diagram"),
        ] {
            assert_eq!(serde_json::to_string(&tab).unwrap(), format!("\"{wire}\""));
            assert_eq!(
                serde_json::from_str::<WorkspaceStandardTab>(&format!("\"{wire}\"")).unwrap(),
                tab
            );
        }
        for (scope, wire) in [
            (WorkspaceViewScope::Database, "database"),
            (WorkspaceViewScope::Schema, "schema"),
            (WorkspaceViewScope::Table, "table"),
        ] {
            assert_eq!(
                serde_json::to_string(&scope).unwrap(),
                format!("\"{wire}\"")
            );
        }
        // Unknown values never decode: the host rejects them too.
        assert!(serde_json::from_str::<WorkspaceStandardTab>("\"relations\"").is_err());
        assert!(serde_json::from_str::<WorkspaceViewScope>("\"collection\"").is_err());
    }

    #[test]
    fn capabilities_omits_workspace_when_absent() {
        let caps = Capabilities {
            name: "redis".to_string(),
            display: "Redis".to_string(),
            targets: None,
            form: None,
            query_language: None,
            write_capabilities: WriteCapabilities {
                row_writer: false,
                document: None,
            },
            workspace: None,
        };
        let text = serde_json::to_string(&caps).unwrap();
        assert!(
            !text.contains("workspace"),
            "absent workspace must not appear on the wire: {text}"
        );
    }
}
