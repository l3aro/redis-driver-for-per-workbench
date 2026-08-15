//! perk-redis: a perk/v1 database plugin for perk-workbench.
//!
//! Speaks JSON-RPC 2.0 over newline-delimited UTF-8 JSON on stdin/stdout.
//! stdout carries protocol frames only; diagnostics go to stderr. The
//! process exits success on stdin EOF and non-zero on a terminal protocol
//! violation.

use std::sync::Arc;

use tokio::io::BufReader;

use perk_redis::redis_service::RedisFactory;
use perk_redis::server::{self, DirectStdout};
use perk_redis::service::SessionFactory;

#[tokio::main]
async fn main() {
    let stdin = tokio::io::stdin();
    let stdout = DirectStdout::new();
    let factory: Arc<dyn SessionFactory> = Arc::new(RedisFactory::default());
    match server::run(BufReader::new(stdin), stdout, factory).await {
        Ok(()) => {}
        Err(e) => {
            eprintln!("[perk-redis] fatal: {e}");
            std::process::exit(1);
        }
    }
}
