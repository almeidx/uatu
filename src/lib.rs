//! Uatu: observe cron jobs without replacing cron.
//!
//! Library form of the `uatu` binary so unit and integration tests can reach
//! internals. The CLI surface lives in `main.rs` / `commands`.

pub mod capture;
pub mod commands;
pub mod config;
pub mod db;
pub mod events;
pub mod identity;
pub mod liveness;
pub mod lock;
pub mod oplog;
pub mod prompt;
pub mod reconcile;
pub mod redact;
pub mod report;
pub mod state;
pub mod util;
