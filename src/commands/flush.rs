//! `uatu flush` (SPEC §3): reconcile stale runs, requeue orphans, retry due
//! deliveries, prune — all under the advisory flush lock.

use std::path::PathBuf;
use std::time::Duration;

use crate::commands::open_for_inspection;
use crate::lock;
use crate::report::{self, DeliverCtx, Sender};
use crate::{liveness, reconcile};

pub struct FlushArgs {
    pub config: Option<PathBuf>,
    pub data_dir: Option<PathBuf>,
}

pub fn cmd_flush(args: FlushArgs) -> i32 {
    let opened = match open_for_inspection(args.config.as_deref(), args.data_dir.as_deref(), true) {
        Ok(o) => o,
        Err(e) => {
            eprintln!("uatu: error: {e}");
            return 1;
        }
    };

    // Serialize with other flushers/pruners; if somebody else holds the lock
    // for the whole wait, they are doing the work — exit 0 with a notice.
    let guard = match lock::acquire_wait(&opened.paths.lock, Duration::from_secs(10)) {
        Ok(Some(g)) => g,
        Ok(None) => {
            println!("uatu: flush lock is held by another process; assuming it is doing the work");
            return 0;
        }
        Err(e) => {
            eprintln!("uatu: error: cannot acquire flush lock: {e}");
            return 1;
        }
    };

    let db = opened.db;
    let cfg = opened.config;
    let oplog = opened.oplog;
    let redactor = opened.redactor;
    reconcile::reconcile(&db, &cfg, &oplog);

    match Sender::new() {
        Ok(sender) => {
            let ctx = DeliverCtx {
                db: &db,
                cfg: &cfg,
                oplog: &oplog,
                sender: &sender,
                host: cfg.host_name(),
                redactor: &redactor,
            };
            let me = liveness::current();
            report::deliver_due(&ctx, &me, None);
        }
        Err(e) => eprintln!("uatu: warning: {e}; queued deliveries left for next flush"),
    }

    match crate::db::prune(
        &db,
        &opened.paths.output,
        cfg.retention_max_age(),
        cfg.retention_max_bytes(),
        false,
    ) {
        Ok(r) if !r.is_empty() => oplog.info(
            "prune_completed",
            &format!(
                "pruned {} aged runs, {} output dirs, {} freed",
                r.aged_runs.len(),
                r.output_pruned_runs.len(),
                crate::util::format_bytes(r.bytes_freed)
            ),
            &[],
        ),
        Ok(_) => {}
        Err(e) => eprintln!("uatu: warning: prune failed: {e}"),
    }

    drop(guard);
    0
}
