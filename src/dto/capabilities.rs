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
    pub write_capabilities: WriteCapabilities,
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
