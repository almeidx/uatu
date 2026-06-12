//! Command implementations and the shared state-opening helper.

pub mod flush;
pub mod inspect;
pub mod maintain;
pub mod run;

use std::path::Path;
use std::sync::Arc;

use crate::config::{self, Config};
use crate::db::Db;
use crate::oplog::OpLog;
use crate::redact::Redactor;
use crate::state::{self, Paths};

pub struct Opened {
    pub config: Config,
    pub paths: Paths,
    pub db: Db,
    pub oplog: OpLog,
}

/// Open config + state for inspection/maintenance commands. Unlike `run`,
/// these commands error out when state is unavailable (SPEC §7, §10).
/// `quiet_config` suppresses stderr config warnings (flush runs from cron).
pub fn open_for_inspection(
    config_path: Option<&Path>,
    data_dir: Option<&Path>,
    quiet_config: bool,
) -> Result<Opened, String> {
    let loaded = config::load_runtime(config_path);
    if !quiet_config {
        for w in &loaded.warnings {
            eprintln!("uatu: warning: {w}");
        }
        if let Some(e) = &loaded.invalid {
            eprintln!("uatu: warning: {e}; using defaults");
        }
    }
    let cfg = loaded.config;
    let state_dir = config::resolve_state_dir(data_dir, &cfg);
    let paths = state::prepare(&state_dir)
        .map_err(|e| format!("cannot prepare state dir {}: {e}", state_dir.display()))?;
    let redactor = Redactor::new(
        &cfg.redaction.literals,
        &cfg.redaction.regex,
        &cfg.auto_secrets(),
    )
    .map(Arc::new)
    .unwrap_or_else(|_| Arc::new(Redactor::empty()));
    let oplog = OpLog::new(
        config::resolve_log_path(&cfg, &paths.state_dir),
        config::log_max_bytes(&cfg),
        redactor,
    );
    let db = Db::open(&paths.db).map_err(|e| {
        oplog.error("state_unavailable", &e.to_string(), &[]);
        e.to_string()
    })?;
    Ok(Opened {
        config: cfg,
        paths,
        db,
        oplog,
    })
}
