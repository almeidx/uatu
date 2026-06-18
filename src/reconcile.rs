//! Stale-run reconciliation (SPEC §6) and orphaned-delivery requeue (SPEC §8).
//!
//! Inspecting commands call this and only ENQUEUE events; delivery happens in
//! `run` and `flush` exclusively (SPEC §3 global rules).

use crate::config::Config;
use crate::db::Db;
use crate::events::{self, Event};
use crate::liveness;
use crate::oplog::OpLog;
use crate::util::now_ms;

#[derive(Debug, Default)]
pub struct ReconcileOutcome {
    pub stale_run_ids: Vec<String>,
    pub orphans_requeued: usize,
}

pub fn reconcile(db: &Db, cfg: &Config, oplog: &OpLog) -> ReconcileOutcome {
    let mut outcome = ReconcileOutcome::default();
    let now = now_ms();

    // Active runs whose wrapper is dead → stale + enqueue stale event.
    if let Ok(active) = db.runs_with_status("active") {
        for run in active {
            if liveness::is_alive(
                run.wrapper_pid as i32,
                run.wrapper_start_ticks as u64,
                &run.boot_id,
            ) {
                continue;
            }
            if db.mark_stale(&run.run_id, now).is_err() {
                continue;
            }
            oplog.warn(
                "reconcile_marked_stale",
                &format!(
                    "run of job {} marked stale (wrapper pid {} is dead)",
                    run.job_id, run.wrapper_pid
                ),
                &[
                    ("run_id", serde_json::json!(run.run_id)),
                    ("job_id", serde_json::json!(run.job_id)),
                ],
            );
            for reporter in events::reporters_for_event(cfg, &run.job_id, Event::Stale, false) {
                let _ = db.insert_delivery(
                    &run.run_id,
                    &run.job_id,
                    Event::Stale.as_str(),
                    &reporter,
                    "queued",
                    now,
                    Some(now),
                    None,
                );
            }
            crate::report::queue_digest_for_run(db, cfg, &run.run_id, &run.job_id, now);
            outcome.stale_run_ids.push(run.run_id);
        }
    }

    // `sending` rows owned by a dead wrapper → requeue (SPEC §8 orphan requeue).
    if let Ok(sending) = db.sending_deliveries() {
        for d in sending {
            let alive = match (d.owner_pid, d.owner_start_ticks, d.owner_boot_id.as_deref()) {
                (Some(pid), Some(ticks), Some(boot)) => {
                    liveness::is_alive(pid as i32, ticks as u64, boot)
                }
                _ => false,
            };
            if alive {
                continue;
            }
            if db.delivery_requeue(d.id, now).is_ok() {
                oplog.warn(
                    "queue_orphan_requeued",
                    &format!(
                        "orphaned sending delivery {} via {} requeued",
                        d.event, d.reporter
                    ),
                    &[("run_id", serde_json::json!(d.run_id))],
                );
                outcome.orphans_requeued += 1;
            }
        }
    }

    outcome
}
