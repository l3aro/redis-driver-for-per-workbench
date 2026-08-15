//! perk/v1 framing and JSON-RPC 2.0 envelope.

use std::io;

use serde::Serialize;
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

/// JSON-RPC error object.
#[derive(Debug, Clone, Serialize)]
pub struct ErrorObject {
    pub code: i32,
    pub message: String,
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

    pub fn error(id: Option<serde_json::Number>, code: i32, message: impl Into<String>) -> Self {
        Response {
            jsonrpc: "2.0",
            id,
            result: None,
            error: Some(ErrorObject {
                code,
                message: message.into(),
            }),
        }
    }
}

/// Serializes one response into a single newline-terminated frame.
pub fn frame_bytes(response: &Response) -> Vec<u8> {
    let mut out = serde_json::to_string(response).expect("response serialization cannot fail");
    out.push('\n');
    out.into_bytes()
}
