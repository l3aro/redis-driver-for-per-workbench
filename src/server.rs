//! perk/v1 server: frame dispatch, session lifecycle, cancellation, and
//! the single serialized stdout writer.
//!
//! Transport rules:
//! - Frames are newline-delimited JSON-RPC 2.0 on stdin/stdout.
//! - Malformed frames (invalid UTF-8, unparseable JSON, non-object, or
//!   oversized) terminate the server without writing further frames.
//! - Requests that parse but are invalid get -32600/-32601/-32602 errors.
//! - `perk/v1/cancel` is an id-less notification mapping the original
//!   request id to a `CancellationToken`; a canceled handler answers -32800.
//! - On stdin EOF or a terminal error, in-flight requests are aborted and
//!   the output is closed: no further frames are written.

use std::collections::HashMap;
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use serde_json::{Number, Value};
use tokio::io::{AsyncBufRead, AsyncWrite, AsyncWriteExt};
use tokio::sync::RwLock;
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use tokio_util::sync::CancellationToken;

use crate::dto::capabilities::{
    Capabilities, FormField, FormSpec, TargetPattern, WriteCapabilities,
};
use crate::dto::request::{CancelRequest, CloseRequest, OpenRequest, OpenResult};
use crate::protocol::{
    ERR_INVALID_PARAMS, ERR_INVALID_REQUEST, ERR_METHOD_NOT_FOUND, ProtocolError, Response,
    ServerError, frame_bytes, read_frame,
};
use crate::service::{MemoryFactory, ServiceError, SessionFactory, SessionService};

/// The 20 mandatory session methods.
const SESSION_METHODS: [&str; 20] = [
    "perk/v1/execute",
    "perk/v1/execute_read_only",
    "perk/v1/validate",
    "perk/v1/list_schema",
    "perk/v1/table_info",
    "perk/v1/list_indexes",
    "perk/v1/create_index",
    "perk/v1/replace_index",
    "perk/v1/drop_index",
    "perk/v1/list_foreign_keys",
    "perk/v1/list_referencing_foreign_keys",
    "perk/v1/list_foreign_keys_all",
    "perk/v1/list_indexes_all",
    "perk/v1/create_foreign_key",
    "perk/v1/replace_foreign_key",
    "perk/v1/drop_foreign_key",
    "perk/v1/alter_column",
    "perk/v1/drop_column",
    "perk/v1/add_column",
    "perk/v1/browse_table",
];

/// The Redis plugin advertisement for the perk/v1 handshake.
pub fn redis_capabilities() -> Capabilities {
    Capabilities {
        name: "redis".to_string(),
        display: "Redis (Rust plugin)".to_string(),
        targets: Some(vec![
            TargetPattern {
                prefix: "redis://".to_string(),
                keep_target: Some(true),
            },
            TargetPattern {
                prefix: "redis:".to_string(),
                keep_target: None,
            },
        ]),
        form: Some(FormSpec {
            prefix: Some("redis:".to_string()),
            fields: vec![
                FormField {
                    key: "host".to_string(),
                    title: "Host".to_string(),
                    kind: 0,
                    placeholder: None,
                    default: Some("127.0.0.1".to_string()),
                    options: None,
                    validate: 1,
                    error: None,
                },
                FormField {
                    key: "port".to_string(),
                    title: "Port".to_string(),
                    kind: 0,
                    placeholder: None,
                    default: Some("6379".to_string()),
                    options: None,
                    validate: 2,
                    error: Some("port must be between 1 and 65535".to_string()),
                },
                FormField {
                    key: "username".to_string(),
                    title: "Username".to_string(),
                    kind: 0,
                    placeholder: None,
                    default: None,
                    options: None,
                    validate: 0,
                    error: None,
                },
                FormField {
                    key: "password".to_string(),
                    title: "Password".to_string(),
                    kind: 1,
                    placeholder: None,
                    default: None,
                    options: None,
                    validate: 0,
                    error: None,
                },
                FormField {
                    key: "database".to_string(),
                    title: "Database".to_string(),
                    kind: 0,
                    placeholder: None,
                    default: Some("0".to_string()),
                    options: None,
                    validate: 0,
                    error: None,
                },
            ],
        }),
        write_capabilities: WriteCapabilities {
            row_writer: false,
            document: None,
        },
    }
}

/// Serialized stdout writer. Frames are queued here and written one at a
/// time by a single writer task; `close()` gates all further output so a
/// terminal condition never emits frames.
#[derive(Clone)]
struct Sink {
    tx: UnboundedSender<Vec<u8>>,
    open: Arc<AtomicBool>,
}

impl Sink {
    fn send(&self, frame: Vec<u8>) {
        if self.open.load(Ordering::Relaxed) {
            let _ = self.tx.send(frame);
        }
    }

    fn close(&self) {
        self.open.store(false, Ordering::Relaxed);
    }
}

/// In-flight request registry: original request id -> cancellation token.
#[derive(Clone, Default)]
struct Registry {
    inflight: Arc<Mutex<HashMap<u64, CancellationToken>>>,
}

impl Registry {
    fn insert(&self, id: u64, token: CancellationToken) {
        self.inflight.lock().unwrap().insert(id, token);
    }

    fn cancel(&self, id: u64) {
        if let Some(token) = self.inflight.lock().unwrap().get(&id) {
            token.cancel();
        }
    }

    fn cancel_all(&self) {
        for token in self.inflight.lock().unwrap().values() {
            token.cancel();
        }
    }

    fn remove(&self, id: u64) {
        self.inflight.lock().unwrap().remove(&id);
    }
}

struct Server {
    factory: Arc<dyn SessionFactory>,
    initialized: bool,
    registry: Registry,
    sessions: RwLock<HashMap<u64, Arc<dyn SessionService>>>,
    next_session_id: u64,
}

impl Server {
    fn new(factory: Arc<dyn SessionFactory>) -> Self {
        Server {
            factory,
            initialized: false,
            registry: Registry::default(),
            sessions: RwLock::new(HashMap::new()),
            next_session_id: 1,
        }
    }

    async fn serve<R: AsyncBufRead + Unpin>(
        &mut self,
        reader: &mut R,
        sink: &Sink,
    ) -> Result<(), ServerError> {
        while let Some(frame) = read_frame(reader).await? {
            let message: Value = match serde_json::from_slice(&frame) {
                Ok(v) => v,
                Err(e) => {
                    return Err(ProtocolError::Malformed(format!("invalid JSON: {e}")).into());
                }
            };
            self.handle_message(message, sink).await?;
        }
        Ok(())
    }

    async fn handle_message(&mut self, message: Value, sink: &Sink) -> Result<(), ServerError> {
        let Some(obj) = message.as_object() else {
            return Err(ProtocolError::Malformed("frame is not a JSON object".into()).into());
        };
        let jsonrpc = obj.get("jsonrpc").and_then(Value::as_str);
        let id = obj.get("id").cloned();
        let method = obj.get("method").and_then(Value::as_str);
        let params = obj.get("params").cloned();
        let is_request = id.is_some();

        // A request id must be an integer; anything else answers -32600
        // with id null.
        let request_id: Option<Number> = match &id {
            None => None,
            Some(Value::Number(n)) if n.is_i64() || n.is_u64() => Some(n.clone()),
            Some(_) => {
                sink.send(frame_bytes(&Response::error(
                    None,
                    ERR_INVALID_REQUEST,
                    "invalid request: id must be an integer",
                )));
                return Ok(());
            }
        };

        if jsonrpc != Some("2.0") {
            if is_request {
                sink.send(frame_bytes(&Response::error(
                    request_id,
                    ERR_INVALID_REQUEST,
                    "invalid request: jsonrpc must be \"2.0\"",
                )));
            }
            return Ok(());
        }

        let Some(method) = method else {
            if is_request {
                sink.send(frame_bytes(&Response::error(
                    request_id,
                    ERR_INVALID_REQUEST,
                    "invalid request: missing method",
                )));
            }
            return Ok(());
        };

        // Notifications: only perk/v1/cancel is defined; others are ignored.
        if !is_request {
            if method == "perk/v1/cancel" {
                self.handle_cancel(params, sink);
            }
            return Ok(());
        }

        // Requests below always carry a valid integer id.
        let id = request_id.expect("request id validated above");

        if !self.initialized {
            if method == "perk/v1/initialize" {
                self.handle_initialize(params, id, sink);
            } else {
                sink.send(frame_bytes(&Response::error(
                    Some(id),
                    ERR_INVALID_REQUEST,
                    "request before initialization",
                )));
            }
            return Ok(());
        }

        match method {
            "perk/v1/initialize" => sink.send(frame_bytes(&Response::error(
                Some(id),
                ERR_INVALID_REQUEST,
                "initialize already called",
            ))),
            "perk/v1/build_target" => self.handle_build_target(params, id, sink),
            "perk/v1/open" => self.handle_open(params, id, sink).await,
            "perk/v1/close" => self.handle_close(params, id, sink).await,
            "perk/v1/row_write" | "perk/v1/document_write" => {
                sink.send(frame_bytes(&Response::error(
                    Some(id),
                    ERR_METHOD_NOT_FOUND,
                    "method not found: not advertised by write_capabilities",
                )));
            }
            _ if SESSION_METHODS.contains(&method) => {
                self.spawn_session_method(method, params, id, sink).await;
            }
            _ => sink.send(frame_bytes(&Response::error(
                Some(id),
                ERR_METHOD_NOT_FOUND,
                format!("method not found: {method}"),
            ))),
        }
        Ok(())
    }

    fn handle_initialize(&mut self, params: Option<Value>, id: Number, sink: &Sink) {
        #[derive(serde::Deserialize)]
        struct InitializeParams {
            protocol_version: u64,
            /// Present for deserialization validation only.
            #[allow(dead_code)]
            workbench_version: String,
        }
        let parsed = params.and_then(|p| serde_json::from_value::<InitializeParams>(p).ok());
        let Some(parsed) = parsed else {
            sink.send(frame_bytes(&Response::error(
                Some(id),
                ERR_INVALID_PARAMS,
                "invalid params: initialize requires {\"protocol_version\":1,\"workbench_version\":string}",
            )));
            return;
        };
        if parsed.protocol_version != 1 {
            sink.send(frame_bytes(&Response::error(
                Some(id),
                ERR_INVALID_REQUEST,
                "unsupported protocol_version",
            )));
            return;
        }
        self.initialized = true;
        let result = serde_json::to_value(crate::dto::capabilities::InitializeResult {
            protocol_version: 1,
            capabilities: redis_capabilities(),
        })
        .expect("initialize result serialization cannot fail");
        sink.send(frame_bytes(&Response::result(Some(id), result)));
    }

    fn handle_cancel(&self, params: Option<Value>, _sink: &Sink) {
        // Unknown cancel ids are ignored (the request already answered).
        if let Some(params) = params {
            if let Ok(request) = serde_json::from_value::<CancelRequest>(params) {
                self.registry.cancel(request.id);
            }
        }
    }

    fn handle_build_target(&self, params: Option<Value>, id: Number, sink: &Sink) {
        let values = match decode_params::<crate::dto::capabilities::FormValues>(params) {
            Ok(v) => v,
            Err(message) => {
                sink.send(frame_bytes(&Response::error(
                    Some(id),
                    ERR_INVALID_PARAMS,
                    message,
                )));
                return;
            }
        };
        let result = serde_json::to_value(self.factory.build_target(&values))
            .expect("build_target result serialization cannot fail");
        sink.send(frame_bytes(&Response::result(Some(id), result)));
    }

    async fn handle_open(&mut self, params: Option<Value>, id: Number, sink: &Sink) {
        let request = match decode_params::<OpenRequest>(params) {
            Ok(r) => r,
            Err(message) => {
                sink.send(frame_bytes(&Response::error(
                    Some(id),
                    ERR_INVALID_PARAMS,
                    message,
                )));
                return;
            }
        };
        let (info, service) = match self.factory.open(&request.target) {
            Ok(v) => v,
            Err(e) => {
                sink.send(frame_bytes(&error_response(Some(id), &e)));
                return;
            }
        };
        let session_id = self.next_session_id;
        self.next_session_id += 1;
        self.sessions
            .write()
            .await
            .insert(session_id, Arc::from(service));
        let result = serde_json::to_value(OpenResult { session_id, info })
            .expect("open result serialization cannot fail");
        sink.send(frame_bytes(&Response::result(Some(id), result)));
    }

    async fn handle_close(&mut self, params: Option<Value>, id: Number, sink: &Sink) {
        let request = match decode_params::<CloseRequest>(params) {
            Ok(r) => r,
            Err(message) => {
                sink.send(frame_bytes(&Response::error(
                    Some(id),
                    ERR_INVALID_PARAMS,
                    message,
                )));
                return;
            }
        };
        // The session is removed before the close hook runs: a failing
        // hook can never resurrect it, and a second close answers -32602.
        let session = self.sessions.write().await.remove(&request.session_id);
        match session {
            Some(session) => {
                session.close();
                sink.send(frame_bytes(&Response::result(Some(id), Value::Null)));
            }
            None => {
                sink.send(frame_bytes(&Response::error(
                    Some(id),
                    ERR_INVALID_PARAMS,
                    "invalid params: unknown session_id",
                )));
            }
        }
    }

    /// Validates the session inline, then runs the handler in its own task
    /// so a blocking statement never stalls the frame loop. Cancellation
    /// reaches the handler through the request's token; a canceled handler
    /// answers -32800.
    async fn spawn_session_method(
        &mut self,
        method: &str,
        params: Option<Value>,
        id: Number,
        sink: &Sink,
    ) {
        let Some(obj) = params.as_ref().and_then(Value::as_object) else {
            sink.send(frame_bytes(&Response::error(
                Some(id),
                ERR_INVALID_PARAMS,
                "invalid params: expected an object",
            )));
            return;
        };
        let Some(session_id) = obj.get("session_id").and_then(Value::as_u64) else {
            sink.send(frame_bytes(&Response::error(
                Some(id),
                ERR_INVALID_PARAMS,
                "invalid params: missing or non-integer session_id",
            )));
            return;
        };
        let Some(session) = self.sessions.read().await.get(&session_id).cloned() else {
            sink.send(frame_bytes(&Response::error(
                Some(id),
                ERR_INVALID_PARAMS,
                "invalid params: unknown session_id",
            )));
            return;
        };

        // Handler params are the wire params minus session_id.
        let mut rest = params.expect("params object validated above");
        rest.as_object_mut()
            .expect("params object validated above")
            .remove("session_id");

        let key = id
            .as_u64()
            .or_else(|| id.as_i64().map(|i| i as u64))
            .unwrap_or(0);
        let token = CancellationToken::new();
        self.registry.insert(key, token.clone());

        let session = session.clone();
        let sink = sink.clone();
        let registry = self.registry.clone();
        let method = method.to_string();
        let id = id.clone();
        tokio::spawn(async move {
            let outcome = run_session_method(&method, rest, session, token.clone()).await;
            // A handler that finishes (successfully or not) after the
            // cancel fired is answered -32800 regardless.
            let outcome = if token.is_cancelled() {
                Err(ServiceError::canceled("request canceled"))
            } else {
                outcome
            };
            let response = match outcome {
                Ok(value) => Response::result(Some(id.clone()), value),
                Err(e) => error_response(Some(id), &e),
            };
            sink.send(frame_bytes(&response));
            registry.remove(key);
        });
    }
}

/// Decodes params into a request DTO; malformed params become -32602.
fn decode_params<T: serde::de::DeserializeOwned>(params: Option<Value>) -> Result<T, String> {
    match params {
        Some(p) => serde_json::from_value(p).map_err(|e| format!("invalid params: {e}")),
        None => Err("invalid params: missing params object".to_string()),
    }
}

fn error_response(id: Option<Number>, error: &ServiceError) -> Response {
    Response::error(id, error.jsonrpc_code(), error.message.clone())
}

/// Decodes the handler request, runs the trait method, and races it
/// against cancellation. Returns the serializable result value.
async fn run_session_method(
    method: &str,
    rest: Value,
    session: Arc<dyn SessionService>,
    token: CancellationToken,
) -> Result<Value, ServiceError> {
    macro_rules! call {
        ($req:ty, $trait_method:ident) => {{
            let request: $req = serde_json::from_value(rest.clone()).map_err(|e| {
                ServiceError::with_code(ERR_INVALID_PARAMS, format!("invalid params: {e}"))
            })?;
            let mut fut = session.$trait_method(request, token.clone());
            tokio::select! {
                r = &mut fut => r,
                _ = token.cancelled() => Err(ServiceError::canceled("request canceled")),
            }
            .map(|out| serde_json::to_value(out).expect("result serialization cannot fail"))
        }};
    }

    use crate::dto::request::{
        AddColumnRequest, BrowseTableRequest, ColumnChangeRequest, DropRequest, EmptyRequest,
        ForeignKeyChangeRequest, IndexChangeRequest, ReplaceForeignKeyRequest, ReplaceIndexRequest,
        StatementRequest, TableRequest,
    };
    match method {
        "perk/v1/execute" => call!(StatementRequest, execute),
        "perk/v1/execute_read_only" => call!(StatementRequest, execute_read_only),
        "perk/v1/validate" => call!(StatementRequest, validate),
        "perk/v1/list_schema" => call!(EmptyRequest, list_schema),
        "perk/v1/table_info" => call!(TableRequest, table_info),
        "perk/v1/list_indexes" => call!(TableRequest, list_indexes),
        "perk/v1/create_index" => call!(IndexChangeRequest, create_index),
        "perk/v1/replace_index" => call!(ReplaceIndexRequest, replace_index),
        "perk/v1/drop_index" => call!(DropRequest, drop_index),
        "perk/v1/list_foreign_keys" => call!(TableRequest, list_foreign_keys),
        "perk/v1/list_referencing_foreign_keys" => {
            call!(TableRequest, list_referencing_foreign_keys)
        }
        "perk/v1/list_foreign_keys_all" => call!(EmptyRequest, list_foreign_keys_all),
        "perk/v1/list_indexes_all" => call!(EmptyRequest, list_indexes_all),
        "perk/v1/create_foreign_key" => call!(ForeignKeyChangeRequest, create_foreign_key),
        "perk/v1/replace_foreign_key" => {
            call!(ReplaceForeignKeyRequest, replace_foreign_key)
        }
        "perk/v1/drop_foreign_key" => call!(DropRequest, drop_foreign_key),
        "perk/v1/alter_column" => call!(ColumnChangeRequest, alter_column),
        "perk/v1/drop_column" => call!(DropRequest, drop_column),
        "perk/v1/add_column" => call!(AddColumnRequest, add_column),
        "perk/v1/browse_table" => call!(BrowseTableRequest, browse_table),
        _ => Err(ServiceError::with_code(
            ERR_METHOD_NOT_FOUND,
            format!("method not found: {method}"),
        )),
    }
}

/// Blocking direct-to-fd stdout adapter for the single serialized writer
/// task.
///
/// `tokio::io::Stdout` dispatches every write and flush through the
/// runtime's blocking pool, which stalled in production (a flushed frame
/// could sit unwritten for hundreds of milliseconds while the frame loop
/// was parked on a blocking stdin read). The protocol writer is the only
/// writer and emits one small frame at a time, so a plain blocking
/// `write(2)` on a dedicated worker costs microseconds and is
/// deterministic. Diagnostics still go to stderr via `eprintln!`.
pub struct DirectStdout(std::io::Stdout);

impl DirectStdout {
    pub fn new() -> Self {
        DirectStdout(std::io::stdout())
    }
}

impl tokio::io::AsyncWrite for DirectStdout {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(std::io::Write::write(&mut self.0.lock(), buf))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(std::io::Write::flush(&mut self.0.lock()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

/// Runs the server until stdin EOF or a terminal protocol error. On EOF
/// the server exits cleanly; on a terminal error it returns the violation
/// after aborting in-flight requests. In both cases no further frames are
/// written.
pub async fn run<R, W>(reader: R, output: W) -> Result<(), ServerError>
where
    R: AsyncBufRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let (tx, mut rx) = unbounded_channel::<Vec<u8>>();
    let open = Arc::new(AtomicBool::new(true));
    let sink = Sink {
        tx,
        open: open.clone(),
    };
    let writer_sink = sink.clone();
    let _writer = tokio::spawn(async move {
        let mut out = output;
        while let Some(frame) = rx.recv().await {
            if !open.load(Ordering::Relaxed) {
                continue; // shutdown: drain and discard queued frames
            }
            out.write_all(&frame).await?;
            out.flush().await?;
        }
        Ok::<(), std::io::Error>(())
    });

    let mut server = Server::new(Arc::new(MemoryFactory::default()));
    let mut reader = reader;
    let result = server.serve(&mut reader, &writer_sink).await;

    // Terminal: abort in-flight handlers, gate all further output, and
    // stop the writer so nothing queued can still reach stdout.
    server.registry.cancel_all();
    writer_sink.close();
    drop(writer_sink);
    _writer.abort();

    result
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::sync::Mutex as StdMutex;
    use std::task::{Context, Poll};
    use std::time::Duration;

    use serde_json::{Value, json};
    use tokio::io::{AsyncWrite, AsyncWriteExt, BufReader, DuplexStream};
    use tokio::task::JoinHandle;
    use tokio::time::{sleep, timeout};

    use super::*;
    use crate::protocol::ERR_CANCELED;

    /// In-memory output buffer implementing `AsyncWrite`. `take_line`
    /// drains the readable buffer; `transcript` keeps an append-only copy
    /// of every byte ever written for whole-stream assertions.
    #[derive(Clone, Default)]
    struct SharedBuf {
        buf: Arc<StdMutex<Vec<u8>>>,
        transcript: Arc<StdMutex<Vec<u8>>>,
    }

    impl SharedBuf {
        fn take_line(&self) -> Option<Vec<u8>> {
            let mut buf = self.buf.lock().unwrap();
            if let Some(pos) = buf.iter().position(|&b| b == b'\n') {
                Some(buf.drain(..=pos).collect())
            } else {
                None
            }
        }

        fn transcript(&self) -> Vec<u8> {
            self.transcript.lock().unwrap().clone()
        }
    }

    impl AsyncWrite for SharedBuf {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            self.buf.lock().unwrap().extend_from_slice(buf);
            self.transcript.lock().unwrap().extend_from_slice(buf);
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    /// Raw framed in-memory transport: the test writes frames into
    /// `input`, the server writes response frames into `output`.
    struct Harness {
        input: DuplexStream,
        output: SharedBuf,
        task: JoinHandle<Result<(), ServerError>>,
    }

    impl Harness {
        async fn start() -> Self {
            let (client, server_side) = tokio::io::duplex(1 << 16);
            let output = SharedBuf::default();
            let task = tokio::spawn(run(BufReader::new(server_side), output.clone()));
            Harness {
                input: client,
                output,
                task,
            }
        }

        async fn send(&mut self, frame: &str) {
            let mut bytes = frame.as_bytes().to_vec();
            bytes.push(b'\n'); // every frame is newline-terminated
            self.input
                .write_all(&bytes)
                .await
                .expect("write frame into transport");
        }

        /// Waits for the next response frame and parses it.
        async fn response(&mut self) -> Value {
            timeout(Duration::from_secs(5), async {
                loop {
                    if let Some(line) = self.output.take_line() {
                        return serde_json::from_slice(&line).expect("response must be JSON");
                    }
                    sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .expect("timed out waiting for a response frame")
        }

        /// Closes stdin (EOF) and waits for the server to exit.
        async fn finish(self) -> Result<(), ServerError> {
            drop(self.input);
            timeout(Duration::from_secs(5), self.task)
                .await
                .expect("server did not exit on EOF")
                .expect("server task panicked")
        }

        /// Asserts the server terminated on a protocol violation.
        async fn finish_terminal(self) -> ServerError {
            drop(self.input);
            timeout(Duration::from_secs(5), self.task)
                .await
                .expect("server did not terminate")
                .expect("server task panicked")
                .expect_err("expected a terminal protocol error")
        }
    }

    fn assert_error(response: &Value, expected_code: i32) {
        assert_eq!(response["jsonrpc"], "2.0", "envelope: {response}");
        assert!(
            response["result"].is_null(),
            "no result expected: {response}"
        );
        assert_eq!(
            response["error"]["code"].as_i64(),
            Some(expected_code as i64),
            "error code: {response}"
        );
        assert!(
            response["error"]["message"].is_string(),
            "message: {response}"
        );
    }

    #[tokio::test]
    async fn initialize_reports_exact_capabilities() {
        let mut h = Harness::start().await;
        h.send(r#"{"jsonrpc":"2.0","id":1,"method":"perk/v1/initialize","params":{"protocol_version":1,"workbench_version":"perk-workbench 0.1.0"}}"#)
            .await;
        let response = h.response().await;

        assert_eq!(response["jsonrpc"], "2.0");
        assert_eq!(response["id"], json!(1));
        assert!(response["error"].is_null(), "no error expected: {response}");
        assert_eq!(
            response["result"],
            json!({
                "protocol_version": 1,
                "capabilities": {
                    "name": "redis",
                    "display": "Redis (Rust plugin)",
                    "targets": [
                        {"prefix": "redis://", "keep_target": true},
                        {"prefix": "redis:"}
                    ],
                    "form": {
                        "prefix": "redis:",
                        "fields": [
                            {"key": "host", "title": "Host", "kind": 0, "default": "127.0.0.1", "validate": 1},
                            {"key": "port", "title": "Port", "kind": 0, "default": "6379", "validate": 2, "error": "port must be between 1 and 65535"},
                            {"key": "username", "title": "Username", "kind": 0, "validate": 0},
                            {"key": "password", "title": "Password", "kind": 1, "validate": 0},
                            {"key": "database", "title": "Database", "kind": 0, "default": "0", "validate": 0}
                        ]
                    },
                    "write_capabilities": {"row_writer": false}
                }
            }),
            "initialize result must match the contract exactly"
        );
        h.finish().await.expect("clean EOF exit");
    }

    #[tokio::test]
    async fn request_before_initialization_is_invalid() {
        let mut h = Harness::start().await;
        h.send(
            r#"{"jsonrpc":"2.0","id":7,"method":"perk/v1/list_schema","params":{"session_id":1}}"#,
        )
        .await;
        let response = h.response().await;
        assert_eq!(response["id"], json!(7), "id must be echoed: {response}");
        assert_error(&response, ERR_INVALID_REQUEST);
        h.finish().await.expect("clean EOF exit");
    }

    #[tokio::test]
    async fn double_initialize_is_invalid() {
        let mut h = Harness::start().await;
        h.send(r#"{"jsonrpc":"2.0","id":1,"method":"perk/v1/initialize","params":{"protocol_version":1,"workbench_version":"x"}}"#)
            .await;
        h.response().await;
        h.send(r#"{"jsonrpc":"2.0","id":2,"method":"perk/v1/initialize","params":{"protocol_version":1,"workbench_version":"x"}}"#)
            .await;
        let response = h.response().await;
        assert_eq!(response["id"], json!(2));
        assert_error(&response, ERR_INVALID_REQUEST);
        h.finish().await.expect("clean EOF exit");
    }

    #[tokio::test]
    async fn unsupported_protocol_version_is_invalid() {
        let mut h = Harness::start().await;
        h.send(r#"{"jsonrpc":"2.0","id":1,"method":"perk/v1/initialize","params":{"protocol_version":2,"workbench_version":"x"}}"#)
            .await;
        let response = h.response().await;
        assert_error(&response, ERR_INVALID_REQUEST);
        h.finish().await.expect("clean EOF exit");
    }

    #[tokio::test]
    async fn unknown_method_is_method_not_found() {
        let mut h = Harness::start().await;
        h.send(r#"{"jsonrpc":"2.0","id":1,"method":"perk/v1/initialize","params":{"protocol_version":1,"workbench_version":"x"}}"#)
            .await;
        h.response().await;
        h.send(r#"{"jsonrpc":"2.0","id":2,"method":"perk/v1/frobnicate","params":{}}"#)
            .await;
        let response = h.response().await;
        assert_eq!(response["id"], json!(2));
        assert_error(&response, ERR_METHOD_NOT_FOUND);
        h.finish().await.expect("clean EOF exit");
    }

    #[tokio::test]
    async fn malformed_params_are_invalid_params() {
        let mut h = Harness::start().await;
        h.send(r#"{"jsonrpc":"2.0","id":1,"method":"perk/v1/initialize","params":{"protocol_version":1,"workbench_version":"x"}}"#)
            .await;
        h.response().await;
        // Non-object params.
        h.send(r#"{"jsonrpc":"2.0","id":2,"method":"perk/v1/execute","params":42}"#)
            .await;
        let response = h.response().await;
        assert_error(&response, ERR_INVALID_PARAMS);
        // Wrong-typed statement field.
        h.send(r#"{"jsonrpc":"2.0","id":3,"method":"perk/v1/execute","params":{"session_id":1,"statement":123}}"#)
            .await;
        let response = h.response().await;
        assert_error(&response, ERR_INVALID_PARAMS);
        h.finish().await.expect("clean EOF exit");
    }

    #[tokio::test]
    async fn unknown_or_missing_session_is_invalid_params() {
        let mut h = Harness::start().await;
        h.send(r#"{"jsonrpc":"2.0","id":1,"method":"perk/v1/initialize","params":{"protocol_version":1,"workbench_version":"x"}}"#)
            .await;
        h.response().await;
        h.send(r#"{"jsonrpc":"2.0","id":2,"method":"perk/v1/open","params":{"target":"redis://localhost:6379/0"}}"#)
            .await;
        let response = h.response().await;
        assert_eq!(
            response["result"]["session_id"],
            json!(1),
            "first session id is 1"
        );
        // Unknown session id.
        h.send(r#"{"jsonrpc":"2.0","id":3,"method":"perk/v1/execute","params":{"session_id":999,"statement":"GET x"}}"#)
            .await;
        let response = h.response().await;
        assert_error(&response, ERR_INVALID_PARAMS);
        // Missing session_id.
        h.send(
            r#"{"jsonrpc":"2.0","id":4,"method":"perk/v1/execute","params":{"statement":"GET x"}}"#,
        )
        .await;
        let response = h.response().await;
        assert_error(&response, ERR_INVALID_PARAMS);
        // Closing an unknown session is also -32602.
        h.send(r#"{"jsonrpc":"2.0","id":5,"method":"perk/v1/close","params":{"session_id":999}}"#)
            .await;
        let response = h.response().await;
        assert_error(&response, ERR_INVALID_PARAMS);
        h.finish().await.expect("clean EOF exit");
    }

    #[tokio::test]
    async fn close_is_idempotent_and_removes_before_hook() {
        let mut h = Harness::start().await;
        h.send(r#"{"jsonrpc":"2.0","id":1,"method":"perk/v1/initialize","params":{"protocol_version":1,"workbench_version":"x"}}"#)
            .await;
        h.response().await;
        h.send(r#"{"jsonrpc":"2.0","id":2,"method":"perk/v1/open","params":{"target":"redis://localhost:6379/0"}}"#)
            .await;
        h.response().await;
        // First close answers null; the second sees an unknown session.
        h.send(r#"{"jsonrpc":"2.0","id":3,"method":"perk/v1/close","params":{"session_id":1}}"#)
            .await;
        let response = h.response().await;
        assert_eq!(
            response["result"],
            Value::Null,
            "close result is null: {response}"
        );
        h.send(r#"{"jsonrpc":"2.0","id":4,"method":"perk/v1/close","params":{"session_id":1}}"#)
            .await;
        let response = h.response().await;
        assert_error(&response, ERR_INVALID_PARAMS);
        // A closed session no longer serves methods.
        h.send(r#"{"jsonrpc":"2.0","id":5,"method":"perk/v1/execute","params":{"session_id":1,"statement":"GET x"}}"#)
            .await;
        let response = h.response().await;
        assert_error(&response, ERR_INVALID_PARAMS);
        h.finish().await.expect("clean EOF exit");
    }

    #[tokio::test]
    async fn cancel_aborts_a_blocking_handler_with_32800() {
        let mut h = Harness::start().await;
        h.send(r#"{"jsonrpc":"2.0","id":1,"method":"perk/v1/initialize","params":{"protocol_version":1,"workbench_version":"x"}}"#)
            .await;
        h.response().await;
        h.send(r#"{"jsonrpc":"2.0","id":2,"method":"perk/v1/open","params":{"target":"redis://localhost:6379/0"}}"#)
            .await;
        h.response().await;
        // Blocking handler: SLEEP 60000 would take a minute to finish.
        h.send(r#"{"jsonrpc":"2.0","id":3,"method":"perk/v1/execute","params":{"session_id":1,"statement":"SLEEP 60000"}}"#)
            .await;
        // Cancel carries the original request id as a notification.
        h.send(r#"{"jsonrpc":"2.0","method":"perk/v1/cancel","params":{"id":3}}"#)
            .await;
        let response = h.response().await;
        assert_eq!(response["id"], json!(3), "canceled request id: {response}");
        assert_error(&response, ERR_CANCELED);
        // The transport stays live after cancellation: close the session.
        h.send(r#"{"jsonrpc":"2.0","id":4,"method":"perk/v1/close","params":{"session_id":1}}"#)
            .await;
        let response = h.response().await;
        assert_eq!(response["result"], Value::Null);
        h.finish().await.expect("clean EOF exit");
    }

    #[tokio::test]
    async fn stdout_carries_only_newline_delimited_response_objects() {
        let mut h = Harness::start().await;
        // Send and read one at a time: the transport must stay live until
        // every spawned-handler response is written.
        let frames = [
            (
                1,
                r#"{"jsonrpc":"2.0","id":1,"method":"perk/v1/initialize","params":{"protocol_version":1,"workbench_version":"x"}}"#,
            ),
            (
                2,
                r#"{"jsonrpc":"2.0","id":2,"method":"perk/v1/open","params":{"target":"t"}}"#,
            ),
            (
                3,
                r#"{"jsonrpc":"2.0","id":3,"method":"perk/v1/execute","params":{"session_id":1,"statement":"SET k v"}}"#,
            ),
            (
                4,
                r#"{"jsonrpc":"2.0","id":4,"method":"perk/v1/execute","params":{"session_id":1,"statement":"GET k"}}"#,
            ),
            (
                5,
                r#"{"jsonrpc":"2.0","id":5,"method":"perk/v1/list_schema","params":{"session_id":1}}"#,
            ),
            (
                6,
                r#"{"jsonrpc":"2.0","id":6,"method":"perk/v1/browse_table","params":{"session_id":1,"table":"kv","options":{}}}"#,
            ),
            (
                7,
                r#"{"jsonrpc":"2.0","id":7,"method":"perk/v1/close","params":{"session_id":1}}"#,
            ),
        ];
        for (expected_id, frame) in frames {
            h.send(frame).await;
            let response = h.response().await;
            assert_eq!(response["jsonrpc"], "2.0", "envelope: {response}");
            assert_eq!(response["id"], json!(expected_id), "one response per id");
            assert!(
                response["result"].is_object()
                    || response["result"].is_array()
                    || response["result"].is_null()
                    || response["error"].is_object(),
                "result or error set: {response}"
            );
        }
        let raw = h.output.transcript();
        h.finish().await.expect("clean EOF exit");

        // Every stdout byte is one JSON response object per line.
        let text = String::from_utf8(raw).expect("stdout must be UTF-8");
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 7, "one frame per response: {text:?}");
        for line in &lines {
            let value: Value = serde_json::from_str(line).expect("each line is JSON");
            assert!(value.is_object(), "each frame is an object: {line}");
            assert_eq!(value["jsonrpc"], "2.0");
            assert!(value["id"].is_number());
            assert!(
                value.get("result").is_some() != value.get("error").is_some(),
                "exactly one of result/error: {line}"
            );
        }
    }

    #[tokio::test]
    async fn malformed_frame_terminates_without_output() {
        let mut h = Harness::start().await;
        h.send("this is not json\n").await;
        let raw = h.output.transcript();
        let error = h.finish_terminal().await;
        assert!(
            matches!(error, ServerError::Protocol(ProtocolError::Malformed(_))),
            "unexpected error: {error}"
        );
        assert!(raw.is_empty(), "no frames after terminal error");
    }

    #[tokio::test]
    async fn invalid_utf8_terminates() {
        let mut h = Harness::start().await;
        h.input
            .write_all(b"\xff\xfe\x00\x01\n")
            .await
            .expect("write garbage");
        let error = h.finish_terminal().await;
        assert!(
            matches!(error, ServerError::Protocol(ProtocolError::Malformed(_))),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn non_object_frame_terminates() {
        let mut h = Harness::start().await;
        h.send("[1,2,3]\n").await;
        let error = h.finish_terminal().await;
        assert!(
            matches!(error, ServerError::Protocol(ProtocolError::Malformed(_))),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn oversized_frame_terminates() {
        let mut h = Harness::start().await;
        // 16 MiB of content without a newline already exceeds the limit
        // (the newline counts toward the 16 MiB maximum).
        let huge = "a".repeat(crate::protocol::MAX_FRAME);
        h.input
            .write_all(huge.as_bytes())
            .await
            .expect("write oversized frame");
        let error = h.finish_terminal().await;
        assert!(
            matches!(error, ServerError::Protocol(ProtocolError::OversizedFrame)),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn build_target_serializes_form_values() {
        let mut h = Harness::start().await;
        h.send(r#"{"jsonrpc":"2.0","id":1,"method":"perk/v1/initialize","params":{"protocol_version":1,"workbench_version":"x"}}"#)
            .await;
        h.response().await;
        h.send(r#"{"jsonrpc":"2.0","id":2,"method":"perk/v1/build_target","params":{"host":"db.example.com","port":"6380","database":"2"}}"#)
            .await;
        let response = h.response().await;
        assert_eq!(
            response["result"],
            json!({"target": "redis:db.example.com:6380/2", "ok": true})
        );
        h.finish().await.expect("clean EOF exit");
    }
}
