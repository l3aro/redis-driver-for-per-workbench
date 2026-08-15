//! perk-redis: a perk/v1 database plugin for perk-workbench.
//!
//! Speaks JSON-RPC 2.0 over newline-delimited UTF-8 JSON on stdin/stdout
//! (see [`server`]). The real Redis adapter lives in [`redis_service`];
//! the transport tests keep using the in-memory service from [`service`]
//! as a test double.

pub mod dto;
pub mod protocol;
pub mod redis_service;
pub mod server;
pub mod service;
