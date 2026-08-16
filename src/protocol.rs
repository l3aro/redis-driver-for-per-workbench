//! perk/v1 framing and JSON-RPC 2.0 envelope.

use std::io;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;
use tokio::io::{AsyncBufRead, AsyncBufReadExt};

/// Maximum frame size in bytes, **including** the trailing newline (16 MiB).
pub const MAX_FRAME: usize = 16 * 1024 * 1024;

/// JSON-RPC error codes used by the protocol, plus the perk/v1 cancellation code.
pub const ERR_INVALID_REQUEST: i32 = -32600;
pub const ERR_METHOD_NOT_FOUND: i32 = -32601;
pub const ERR_INVALID_PARAMS: i32 = -32602;
pub const ERR_INTERNAL: i32 = -32603;
pub const ERR_CANCELED: i32 = -32800;

/// Stable operation-error kinds, mirrored from the canonical perk/v1
/// contract (the Go host's `plugin.Kind` and the Node SDK's
/// `ErrorKind`). Serialized exactly as the wire strings below; unknown
/// or blank kinds on decode normalize to [`ErrorKind::Operation`], so an
/// impossible kind can never be produced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// The operation was rejected as invalid.
    Validation,
    /// Credentials were missing or rejected.
    Authentication,
    /// The backend connection failed or dropped.
    Connection,
    /// Generic operation failure (the default).
    Operation,
    /// The backend does not support the operation.
    Unsupported,
    /// The operation was canceled.
    Cancelled,
    /// Protocol-level failure inside the plugin.
    Protocol,
    /// The plugin's own runtime crashed or was killed.
    PluginCrash,
}

impl ErrorKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            ErrorKind::Validation => "validation",
            ErrorKind::Authentication => "authentication",
            ErrorKind::Connection => "connection",
            ErrorKind::Operation => "operation",
            ErrorKind::Unsupported => "unsupported",
            ErrorKind::Cancelled => "cancelled",
            ErrorKind::Protocol => "protocol",
            ErrorKind::PluginCrash => "plugin_crash",
        }
    }
}

impl Serialize for ErrorKind {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for ErrorKind {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let kind = String::deserialize(deserializer)?;
        Ok(match kind.as_str() {
            "validation" => ErrorKind::Validation,
            "authentication" => ErrorKind::Authentication,
            "connection" => ErrorKind::Connection,
            "operation" => ErrorKind::Operation,
            "unsupported" => ErrorKind::Unsupported,
            "cancelled" => ErrorKind::Cancelled,
            "protocol" => ErrorKind::Protocol,
            "plugin_crash" => ErrorKind::PluginCrash,
            // Unknown or blank kinds normalize to operation, mirroring
            // the host's normalization.
            _ => ErrorKind::Operation,
        })
    }
}

/// Structured error provenance: the stable failure kind, the advisory
/// plugin identity, and the advisory wire method, plus optional
/// non-control advisory guidance. The host treats `plugin` and `method`
/// as advisory only — it overrides them with its own handshake identity
/// and the actual request method, so the method renders exactly once.
/// `hint` and `suggested_statement` are advisory too: the host renders
/// them separately from the error and never executes a suggested
/// statement; empty strings are omitted from the wire. Never carries
/// targets, credentials, statements, or values beyond the advisory text.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorData {
    pub kind: ErrorKind,
    pub plugin: String,
    pub method: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub hint: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub suggested_statement: String,
}

/// Terminal protocol violations. Either side treats these as fatal: the
/// connection is broken and the process shuts down.
#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("frame exceeds the {MAX_FRAME}-byte limit (16 MiB including newline)")]
    OversizedFrame,
    #[error("malformed frame: {0}")]
    Malformed(String),
}

#[derive(Debug, Error)]
pub enum ServerError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Protocol(#[from] ProtocolError),
}

/// Reads one newline-delimited frame. Returns `None` on clean EOF.
/// A frame that cannot fit within [`MAX_FRAME`] bytes (newline included)
/// is a terminal [`ProtocolError::OversizedFrame`].
pub async fn read_frame<R: AsyncBufRead + Unpin>(
    reader: &mut R,
) -> Result<Option<Vec<u8>>, ServerError> {
    let mut buf = Vec::new();
    loop {
        let (consume_len, done): (usize, bool) = {
            let available = reader.fill_buf().await?;
            if available.is_empty() {
                // EOF. A trailing partial frame (no final newline) is still
                // parsed as a frame, mirroring the SDK line reader.
                return Ok(if buf.is_empty() { None } else { Some(buf) });
            }
            match available.iter().position(|&b| b == b'\n') {
                Some(pos) => {
                    if buf.len() + pos + 1 > MAX_FRAME {
                        return Err(ProtocolError::OversizedFrame.into());
                    }
                    buf.extend_from_slice(&available[..pos]);
                    (pos + 1, true)
                }
                None => {
                    if buf.len() + available.len() >= MAX_FRAME {
                        return Err(ProtocolError::OversizedFrame.into());
                    }
                    buf.extend_from_slice(available);
                    (available.len(), false)
                }
            }
        };
        reader.consume(consume_len);
        if done {
            return Ok(Some(buf));
        }
    }
}

/// JSON-RPC error object. `data` is the optional structured provenance
/// object; it is omitted — never null — when there is no provenance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorObject {
    pub code: i32,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<ErrorData>,
}

impl ErrorObject {
    /// An error without provenance: no `data` member on the wire.
    pub fn plain(code: i32, message: impl Into<String>) -> Self {
        ErrorObject {
            code,
            message: message.into(),
            data: None,
        }
    }

    /// An error with the full structured provenance: stable kind, the
    /// plugin identity, and the actual wire method, exactly once, and
    /// no advisory guidance.
    pub fn with_provenance(
        code: i32,
        message: impl Into<String>,
        kind: ErrorKind,
        plugin: &str,
        method: &str,
    ) -> Self {
        Self::with_guidance(code, message, kind, plugin, method, "", "")
    }

    /// An error with the full structured provenance plus optional
    /// advisory guidance: a `hint` explaining the failure and a
    /// `suggested_statement` the user may try instead. Both are
    /// non-control — the host renders them separately and never
    /// executes a suggestion — and empty strings are omitted from the
    /// wire.
    pub fn with_guidance(
        code: i32,
        message: impl Into<String>,
        kind: ErrorKind,
        plugin: &str,
        method: &str,
        hint: &str,
        suggested_statement: &str,
    ) -> Self {
        ErrorObject {
            code,
            message: message.into(),
            data: Some(ErrorData {
                kind,
                plugin: plugin.to_string(),
                method: method.to_string(),
                hint: hint.to_string(),
                suggested_statement: suggested_statement.to_string(),
            }),
        }
    }
}

/// One response envelope. Exactly one of `result` / `error` is set.
/// `id` echoes the request id; it serializes as `null` for requests whose
/// id was invalid.
#[derive(Debug, Clone, Serialize)]
pub struct Response {
    pub jsonrpc: &'static str,
    pub id: Option<serde_json::Number>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorObject>,
}

impl Response {
    pub fn result(id: Option<serde_json::Number>, result: serde_json::Value) -> Self {
        Response {
            jsonrpc: "2.0",
            id,
            result: Some(result),
            error: None,
        }
    }

    /// Builds one error response from a complete [`ErrorObject`], so
    /// callers compose the provenance (kind, plugin, actual wire method)
    /// at the site that knows the method — a plain `(code, message)`
    /// pair can no longer be passed here accidentally.
    pub fn error(id: Option<serde_json::Number>, error: ErrorObject) -> Self {
        Response {
            jsonrpc: "2.0",
            id,
            result: None,
            error: Some(error),
        }
    }
}

/// Serializes one response into a single newline-terminated frame.
pub fn frame_bytes(response: &Response) -> Vec<u8> {
    let mut out = serde_json::to_string(response).expect("response serialization cannot fail");
    out.push('\n');
    out.into_bytes()
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn error_kind_serializes_to_stable_strings() {
        for (kind, wire) in [
            (ErrorKind::Validation, "validation"),
            (ErrorKind::Authentication, "authentication"),
            (ErrorKind::Connection, "connection"),
            (ErrorKind::Operation, "operation"),
            (ErrorKind::Unsupported, "unsupported"),
            (ErrorKind::Cancelled, "cancelled"),
            (ErrorKind::Protocol, "protocol"),
            (ErrorKind::PluginCrash, "plugin_crash"),
        ] {
            assert_eq!(serde_json::to_string(&kind).unwrap(), format!("\"{wire}\""));
            assert_eq!(kind.as_str(), wire);
        }
    }

    #[test]
    fn error_kind_decodes_and_normalizes_unknown_kinds() {
        for (wire, expected) in [
            ("validation", ErrorKind::Validation),
            ("authentication", ErrorKind::Authentication),
            ("connection", ErrorKind::Connection),
            ("operation", ErrorKind::Operation),
            ("unsupported", ErrorKind::Unsupported),
            ("cancelled", ErrorKind::Cancelled),
            ("protocol", ErrorKind::Protocol),
            ("plugin_crash", ErrorKind::PluginCrash),
        ] {
            let decoded: ErrorKind = serde_json::from_str(&format!("\"{wire}\"")).unwrap();
            assert_eq!(decoded, expected, "kind {wire:?}");
        }
        // Unknown or blank kinds normalize to operation: an impossible
        // kind can never be produced by the type.
        for unknown in ["frobnicate", "", "VALIDATION", "null", "42"] {
            let decoded: ErrorKind = serde_json::from_str(&format!("\"{unknown}\"")).unwrap();
            assert_eq!(decoded, ErrorKind::Operation, "kind {unknown:?}");
        }
    }

    #[test]
    fn error_object_with_provenance_emits_exact_data() {
        let error = ErrorObject::with_provenance(
            ERR_INVALID_PARAMS,
            "invalid params: missing statement",
            ErrorKind::Validation,
            "redis",
            "perk/v1/execute",
        );
        assert_eq!(
            serde_json::to_string(&error).unwrap(),
            r#"{"code":-32602,"message":"invalid params: missing statement","data":{"kind":"validation","plugin":"redis","method":"perk/v1/execute"}}"#
        );
    }

    #[test]
    fn error_object_with_guidance_emits_advisory_fields_and_omits_empty() {
        let guided = ErrorObject::with_guidance(
            ERR_INTERNAL,
            "redis: WRONGTYPE Operation against a key holding the wrong kind of value",
            ErrorKind::Operation,
            "redis",
            "perk/v1/execute",
            "GET accepts strings, but user:1 is a hash",
            "HGETALL user:1",
        );
        assert_eq!(
            serde_json::to_string(&guided).unwrap(),
            r#"{"code":-32603,"message":"redis: WRONGTYPE Operation against a key holding the wrong kind of value","data":{"kind":"operation","plugin":"redis","method":"perk/v1/execute","hint":"GET accepts strings, but user:1 is a hash","suggested_statement":"HGETALL user:1"}}"#
        );
        // Empty advisory strings serialize as absent — never as "" members.
        let empty = ErrorObject::with_guidance(
            ERR_INTERNAL,
            "boom",
            ErrorKind::Operation,
            "redis",
            "perk/v1/execute",
            "",
            "",
        );
        assert_eq!(
            serde_json::to_string(&empty).unwrap(),
            r#"{"code":-32603,"message":"boom","data":{"kind":"operation","plugin":"redis","method":"perk/v1/execute"}}"#,
            "empty advisories must be omitted from the wire"
        );
        // Legacy decode without the new members stays equal to the
        // no-guidance object.
        let decoded: ErrorObject =
            serde_json::from_str(r#"{"code":-32603,"message":"boom","data":{"kind":"operation","plugin":"redis","method":"perk/v1/execute"}}"#)
                .unwrap();
        assert_eq!(decoded, empty);
    }

    #[test]
    fn error_object_plain_omits_data_and_legacy_decode_keeps_it_absent() {
        let plain = ErrorObject::plain(ERR_INTERNAL, "boom");
        assert_eq!(
            serde_json::to_string(&plain).unwrap(),
            r#"{"code":-32603,"message":"boom"}"#
        );
        // Legacy decode: an error without data keeps data absent, never
        // a null member.
        let decoded: ErrorObject =
            serde_json::from_str(r#"{"code":-32603,"message":"boom"}"#).unwrap();
        assert_eq!(decoded, plain);
        assert_eq!(decoded.data, None);
        // A null data member is also accepted and treated as absent.
        let nulled: ErrorObject =
            serde_json::from_str(r#"{"code":-32603,"message":"boom","data":null}"#).unwrap();
        assert_eq!(nulled.data, None);
    }

    #[test]
    fn response_error_serializes_full_envelope_without_duplicated_prefix() {
        let response = Response::error(
            Some(serde_json::Number::from(7)),
            ErrorObject::with_provenance(
                ERR_METHOD_NOT_FOUND,
                "method not found: perk/v1/frobnicate",
                ErrorKind::Unsupported,
                "redis",
                "perk/v1/frobnicate",
            ),
        );
        assert_eq!(
            serde_json::to_string(&response).unwrap(),
            r#"{"jsonrpc":"2.0","id":7,"error":{"code":-32601,"message":"method not found: perk/v1/frobnicate","data":{"kind":"unsupported","plugin":"redis","method":"perk/v1/frobnicate"}}}"#,
            "the method renders exactly once, with one perk/v1 prefix"
        );
        assert!(response.result.is_none());
        assert!(response.error.is_some());
    }

    #[test]
    fn response_result_has_no_error_member() {
        let response = Response::result(Some(serde_json::Number::from(1)), json!({"ok": true}));
        assert_eq!(
            serde_json::to_string(&response).unwrap(),
            r#"{"jsonrpc":"2.0","id":1,"result":{"ok":true}}"#
        );
    }
}
