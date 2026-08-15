//! perk-redis: a perk/v1 database plugin for perk-workbench.
//!
//! Speaks JSON-RPC 2.0 over newline-delimited UTF-8 JSON on stdin/stdout.
//! stdout carries protocol frames only; diagnostics go to stderr. The
//! process exits success on stdin EOF and non-zero on a terminal protocol
//! violation.

mod dto;
mod protocol;
mod server;
mod service;

use tokio::io::BufReader;

#[tokio::main]
async fn main() {
    let stdin = tokio::io::stdin();
    let stdout = server::DirectStdout::new();
    match server::run(BufReader::new(stdin), stdout).await {
        Ok(()) => {}
        Err(e) => {
            eprintln!("[perk-redis] fatal: {e}");
            std::process::exit(1);
        }
    }
}
