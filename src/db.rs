//! SQLite state (SPEC §7): run metadata, liveness identity, capture state,
//! immediate delivery rows, durable aggregate-digest cohorts and membership,
//! and retention pruning. WAL mode, busy_timeout 5000ms, forward-only
//! migrations via `user_version`.

use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};

use crate::util::now_ms;

pub const SCHEMA_VERSION: i64 = 4;

/// Delivery rows stop protecting aged run metadata after the same seven-day
/// horizon used by the delivery retry state machine. Keeping the value here
/// avoids making retention depend on the network reporter module.
pub const DELIVERY_RETRY_MAX_AGE_MS: i64 = 7 * 86_400_000;

/// Run statuses (SPEC §7). Stored as snake_case strings.
pub const STATUSES: [&str; 6] = [
    "active",
    "success",
    "failure",
    "timeout",
    "stale",
    "start_failed",
];

#[derive(Debug)]
pub enum StateError {
    /// Database written by a newer uatu (SPEC §7 migrations): degrade safely.
    NewerSchema(i64),
    /// SQLITE_BUSY/LOCKED — retryable, unlike other state failures.
    Busy(String),
    Other(String),
}

impl std::fmt::Display for StateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StateError::NewerSchema(v) => write!(
                f,
                "uatu is older than its database (db schema v{v}, binary supports v{SCHEMA_VERSION}); upgrade uatu"
            ),
            StateError::Busy(e) | StateError::Other(e) => write!(f, "{e}"),
        }
    }
}

impl From<rusqlite::Error> for StateError {
    fn from(e: rusqlite::Error) -> Self {
        use rusqlite::ErrorCode::{DatabaseBusy, DatabaseLocked};
        match e.sqlite_error_code() {
            Some(DatabaseBusy | DatabaseLocked) => StateError::Busy(e.to_string()),
            _ => StateError::Other(e.to_string()),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct CaptureMeta {
    pub path: Option<String>,
    pub bytes_total: u64,
    pub bytes_stored: u64,
    pub bytes_omitted: u64,
    pub reason: Option<String>,
}

#[derive(Clone, Debug)]
pub struct RunRow {
    pub run_id: String,
    pub job_id: String,
    pub job_id_inferred: bool,
    pub inferred_basename: Option<String>,
    pub mode: String,
    pub argv_json: Option<String>,
    pub shell_cmd: Option<String>,
    pub cwd: Option<String>,
    pub env_names_json: Option<String>,
    pub host: String,
    pub schedule_label: Option<String>,
    pub status: String,
    pub start_ms: i64,
    pub end_ms: Option<i64>,
    pub end_is_detection: bool,
    pub exit_code: Option<i64>,
    pub signal_no: Option<i64>,
    pub timeout_fired: bool,
    pub interrupted_by: Option<String>,
    pub start_error: Option<String>,
    pub wrapper_pid: i64,
    pub wrapper_start_ticks: i64,
    pub boot_id: String,
    pub child_pid: Option<i64>,
    pub expected_duration_ms: Option<i64>,
    pub long_run_fired: bool,
    pub detached_children: bool,
    pub stdout: CaptureMeta,
    pub stderr: CaptureMeta,
    pub output_pruned_ms: Option<i64>,
}

impl RunRow {
    pub fn duration_ms(&self) -> Option<i64> {
        self.end_ms.map(|e| (e - self.start_ms).max(0))
    }
}

#[derive(Clone, Debug)]
pub struct DeliveryRow {
    pub id: i64,
    pub run_id: String,
    pub job_id: String,
    pub event: String,
    pub reporter: String,
    pub state: String,
    pub attempt_count: i64,
    pub created_ms: i64,
    pub next_attempt_ms: Option<i64>,
    pub delivered_ms: Option<i64>,
    pub last_error: Option<String>,
    pub owner_pid: Option<i64>,
    pub owner_start_ticks: Option<i64>,
    pub owner_boot_id: Option<String>,
    pub digest_period: Option<String>,
    pub digest_start_ms: Option<i64>,
    pub digest_end_ms: Option<i64>,
    pub digest_cohort_id: Option<i64>,
}

#[derive(Clone, Debug)]
pub struct DeliveryDigest {
    pub period: String,
    pub start_ms: i64,
    pub end_ms: i64,
}

pub const DIGEST_JOB_SUMMARY_LIMIT: usize = 64;
pub const DIGEST_PROBLEM_DETAIL_LIMIT: usize = 128;
pub const DIGEST_SUCCESS_DETAIL_LIMIT: usize = 64;

#[derive(Clone, Debug)]
pub struct DigestCohortRow {
    pub id: i64,
    pub event: String,
    pub reporter: String,
    pub host: String,
    pub period: String,
    pub start_ms: i64,
    pub end_ms: i64,
    pub state: String,
    /// Number of memberships accepted while this cohort was queued or
    /// sending. Terminal-suppressed late rows are intentionally excluded.
    pub member_count: i64,
    pub attempt_count: i64,
    pub created_ms: i64,
    pub next_attempt_ms: Option<i64>,
    pub delivered_ms: Option<i64>,
    pub last_error: Option<String>,
    pub owner_pid: Option<i64>,
    pub owner_start_ticks: Option<i64>,
    pub owner_boot_id: Option<String>,
}

/// Compact identity for one claimed digest attempt. Membership remains in
/// SQLite and is read through bounded aggregate queries.
#[derive(Clone, Debug)]
pub struct DigestClaim {
    pub cohort_id: i64,
    pub event: String,
    pub reporter: String,
    pub host: String,
    pub period: String,
    pub start_ms: i64,
    pub end_ms: i64,
    pub member_count: i64,
    pub attempt_count: i64,
    pub created_ms: i64,
}

#[derive(Clone, Debug, Default)]
pub struct DigestStatusTotals {
    pub success: u64,
    pub failure: u64,
    pub timeout: u64,
    pub stale: u64,
    pub start_failed: u64,
    pub active: u64,
}

#[derive(Clone, Debug)]
pub struct DigestDurationAggregate {
    pub average_ms: u64,
    pub max_ms: u64,
}

#[derive(Clone, Debug)]
pub struct DigestLatestAggregate {
    pub status: String,
    pub start_ms: i64,
    pub duration_ms: Option<u64>,
    pub schedule_label: Option<String>,
}

#[derive(Clone, Debug)]
pub struct DigestJobAggregate {
    pub job_id: String,
    pub total_executions: u64,
    pub statuses: DigestStatusTotals,
    pub durations: Option<DigestDurationAggregate>,
    pub latest: DigestLatestAggregate,
}

#[derive(Clone, Debug)]
pub struct DigestExecutionAggregate {
    pub job_id: String,
    pub run_id: String,
    pub status: String,
    pub start_ms: i64,
    pub duration_ms: Option<u64>,
}

#[derive(Clone, Debug, Default)]
pub struct DigestAggregate {
    pub total_jobs: u64,
    pub total_executions: u64,
    pub statuses: DigestStatusTotals,
    pub total_problem_executions: u64,
    pub total_success_executions: u64,
    pub job_summaries: Vec<DigestJobAggregate>,
    pub problem_details: Vec<DigestExecutionAggregate>,
    pub success_details: Vec<DigestExecutionAggregate>,
}

pub struct Db {
    pub conn: Connection,
}

impl Db {
    pub fn open(path: &Path) -> Result<Db, StateError> {
        // Pre-create the db file 0600 so SQLite (and its -wal/-shm siblings,
        // which inherit the db's mode) never exposes captured data.
        if !path.exists() {
            let _ = std::fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .write(true)
                .mode(0o600)
                .open(path);
        }
        // The first WAL switch on a fresh db needs a SHARED→EXCLUSIVE lock
        // upgrade; when two processes race it, SQLite returns BUSY immediately
        // (deadlock avoidance) without consulting the busy handler, so
        // busy_timeout alone cannot cover the top-of-the-hour burst on a fresh
        // state dir (SPEC §7). Retry the whole open with the same 5s budget.
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(5000);
        let mut delay = std::time::Duration::from_millis(5);
        loop {
            match Self::open_once(path) {
                Err(StateError::Busy(_)) if std::time::Instant::now() < deadline => {
                    std::thread::sleep(delay);
                    delay = (delay * 2).min(std::time::Duration::from_millis(100));
                }
                result => return result,
            }
        }
    }

    /// Open state for the child-critical `run` path. Lock waits and SQLite VM
    /// work share one deadline; an interrupted migration is rolled back when
    /// its connection is dropped. Successful connections regain the normal
    /// five-second busy timeout and have no lingering progress handler.
    pub fn open_bounded(path: &Path, budget: std::time::Duration) -> Result<Db, StateError> {
        let deadline = std::time::Instant::now()
            .checked_add(budget)
            .ok_or_else(|| StateError::Other("state-open budget is too large".to_string()))?;
        if !path.exists() {
            let _ = std::fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .write(true)
                .mode(0o600)
                .open(path);
        }

        let mut delay = std::time::Duration::from_millis(5);
        loop {
            if std::time::Instant::now() >= deadline {
                return Err(Self::open_deadline_error());
            }
            match Self::open_once_bounded(path, deadline) {
                Ok(db) => return Ok(db),
                Err(_) if std::time::Instant::now() >= deadline => {
                    return Err(Self::open_deadline_error());
                }
                Err(StateError::Busy(_)) => {
                    let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                    let sleep_for = delay.min(remaining);
                    if sleep_for.is_zero() {
                        return Err(Self::open_deadline_error());
                    }
                    std::thread::sleep(sleep_for);
                    delay = (delay * 2).min(std::time::Duration::from_millis(100));
                }
                Err(e) => return Err(e),
            }
        }
    }

    fn open_once(path: &Path) -> Result<Db, StateError> {
        let conn = Connection::open(path)?;
        // busy_timeout FIRST: the WAL switch and migration below take write
        // locks, and concurrent wrappers racing on a fresh database (the
        // top-of-the-hour burst, SPEC §7) must retry instead of failing.
        conn.pragma_update(None, "busy_timeout", 5000)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        let db = Db { conn };
        db.migrate()?;
        Ok(db)
    }

    fn open_once_bounded(path: &Path, deadline: std::time::Instant) -> Result<Db, StateError> {
        let conn = Connection::open(path)?;
        let db = Db { conn };
        db.set_remaining_busy_timeout(deadline)?;
        db.conn
            .progress_handler(1_000, Some(move || std::time::Instant::now() >= deadline))?;
        db.set_remaining_busy_timeout(deadline)?;
        db.conn.pragma_update(None, "journal_mode", "WAL")?;
        db.set_remaining_busy_timeout(deadline)?;
        db.conn.pragma_update(None, "synchronous", "NORMAL")?;
        db.migrate_inner(Some(deadline))?;
        if std::time::Instant::now() >= deadline {
            return Err(Self::open_deadline_error());
        }
        db.conn.progress_handler(0, None::<fn() -> bool>)?;
        db.conn
            .busy_timeout(std::time::Duration::from_millis(5_000))?;
        Ok(db)
    }

    fn set_remaining_busy_timeout(&self, deadline: std::time::Instant) -> Result<(), StateError> {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining < std::time::Duration::from_millis(1) {
            return Err(Self::open_deadline_error());
        }
        self.conn.busy_timeout(remaining)?;
        Ok(())
    }

    fn open_deadline_error() -> StateError {
        StateError::Other("state initialization deadline exceeded".to_string())
    }

    fn migrate(&self) -> Result<bool, StateError> {
        self.migrate_inner(None)
    }

    fn migrate_inner(&self, deadline: Option<std::time::Instant>) -> Result<bool, StateError> {
        if let Some(deadline) = deadline {
            self.set_remaining_busy_timeout(deadline)?;
        }
        let mut v: i64 = self
            .conn
            .query_row("PRAGMA user_version", [], |r| r.get(0))?;
        if v > SCHEMA_VERSION {
            return Err(StateError::NewerSchema(v));
        }
        if v == SCHEMA_VERSION {
            return Ok(false);
        }
        if v == 1 {
            if let Some(deadline) = deadline {
                self.set_remaining_busy_timeout(deadline)?;
            }
            self.conn.execute_batch(
                r#"
BEGIN EXCLUSIVE;
ALTER TABLE deliveries ADD COLUMN digest_period TEXT;
ALTER TABLE deliveries ADD COLUMN digest_start_ms INTEGER;
ALTER TABLE deliveries ADD COLUMN digest_end_ms INTEGER;
PRAGMA user_version = 2;
COMMIT;
"#,
            )?;
            v = 2;
        }
        if matches!(v, 2 | 3) {
            if let Some(deadline) = deadline {
                self.set_remaining_busy_timeout(deadline)?;
            }
            self.conn.execute_batch(
                r#"
BEGIN EXCLUSIVE;
DROP INDEX IF EXISTS idx_deliv_digest;
ALTER TABLE deliveries ADD COLUMN digest_cohort_id INTEGER;
CREATE TABLE IF NOT EXISTS runs (
  run_id TEXT PRIMARY KEY,
  job_id TEXT NOT NULL,
  job_id_inferred INTEGER NOT NULL DEFAULT 0,
  inferred_basename TEXT,
  mode TEXT NOT NULL,
  argv_json TEXT,
  shell_cmd TEXT,
  cwd TEXT,
  env_names_json TEXT,
  host TEXT NOT NULL DEFAULT '',
  schedule_label TEXT,
  status TEXT NOT NULL,
  start_ms INTEGER NOT NULL,
  end_ms INTEGER,
  end_is_detection INTEGER NOT NULL DEFAULT 0,
  exit_code INTEGER,
  signal_no INTEGER,
  timeout_fired INTEGER NOT NULL DEFAULT 0,
  interrupted_by TEXT,
  start_error TEXT,
  wrapper_pid INTEGER NOT NULL DEFAULT 0,
  wrapper_start_ticks INTEGER NOT NULL DEFAULT 0,
  boot_id TEXT NOT NULL DEFAULT '',
  child_pid INTEGER,
  expected_duration_ms INTEGER,
  long_run_fired INTEGER NOT NULL DEFAULT 0,
  detached_children INTEGER NOT NULL DEFAULT 0,
  stdout_path TEXT,
  stdout_bytes_total INTEGER NOT NULL DEFAULT 0,
  stdout_bytes_stored INTEGER NOT NULL DEFAULT 0,
  stdout_bytes_omitted INTEGER NOT NULL DEFAULT 0,
  stdout_reason TEXT,
  stderr_path TEXT,
  stderr_bytes_total INTEGER NOT NULL DEFAULT 0,
  stderr_bytes_stored INTEGER NOT NULL DEFAULT 0,
  stderr_bytes_omitted INTEGER NOT NULL DEFAULT 0,
  stderr_reason TEXT,
  output_pruned_ms INTEGER
);
CREATE TABLE digest_cohorts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  event TEXT NOT NULL,
  reporter TEXT NOT NULL,
  host TEXT NOT NULL,
  digest_period TEXT NOT NULL,
  digest_start_ms INTEGER NOT NULL,
  digest_end_ms INTEGER NOT NULL,
  state TEXT NOT NULL,
  member_count INTEGER NOT NULL DEFAULT 0 CHECK(member_count >= 0),
  attempt_count INTEGER NOT NULL DEFAULT 0,
  created_ms INTEGER NOT NULL,
  next_attempt_ms INTEGER,
  delivered_ms INTEGER,
  last_error TEXT,
  owner_pid INTEGER,
  owner_start_ticks INTEGER,
  owner_boot_id TEXT,
  UNIQUE(event, reporter, host, digest_period, digest_start_ms, digest_end_ms)
);
CREATE INDEX IF NOT EXISTS idx_runs_job_start ON runs(job_id, start_ms);
CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status);
CREATE INDEX IF NOT EXISTS idx_runs_start ON runs(start_ms);
CREATE INDEX IF NOT EXISTS idx_deliv_state ON deliveries(state, next_attempt_ms);
CREATE INDEX IF NOT EXISTS idx_deliv_run ON deliveries(run_id);
CREATE INDEX idx_digest_cohort_due ON digest_cohorts(state, next_attempt_ms);
CREATE INDEX idx_digest_cohort_prune ON digest_cohorts(digest_end_ms, state);
CREATE INDEX idx_deliv_cohort_state ON deliveries(digest_cohort_id, state);

INSERT INTO digest_cohorts (
  event, reporter, host, digest_period, digest_start_ms, digest_end_ms,
  state, member_count, attempt_count, created_ms, next_attempt_ms, delivered_ms, last_error
)
SELECT
  d.event,
  d.reporter,
  COALESCE((SELECT r.host FROM runs AS r WHERE r.run_id=d.run_id), ''),
  d.digest_period,
  d.digest_start_ms,
  d.digest_end_ms,
  CASE
    WHEN SUM(CASE WHEN d.state IN ('queued', 'sending') THEN 1 ELSE 0 END) > 0 THEN 'queued'
    WHEN SUM(CASE WHEN d.state='delivered' THEN 1 ELSE 0 END) > 0 THEN 'delivered'
    ELSE 'expired'
  END,
  CASE
    WHEN SUM(CASE WHEN d.state IN ('queued', 'sending') THEN 1 ELSE 0 END) > 0
      THEN SUM(CASE WHEN d.state IN ('queued', 'sending') THEN 1 ELSE 0 END)
    ELSE COUNT(*)
  END,
  CASE
    WHEN SUM(CASE WHEN d.state IN ('queued', 'sending') THEN 1 ELSE 0 END) > 0
      THEN COALESCE(MAX(CASE WHEN d.state IN ('queued', 'sending') THEN d.attempt_count END), 0)
    ELSE COALESCE(MAX(d.attempt_count), 0)
  END,
  CASE
    WHEN SUM(CASE WHEN d.state IN ('queued', 'sending') THEN 1 ELSE 0 END) > 0
      THEN MIN(CASE WHEN d.state IN ('queued', 'sending') THEN d.created_ms END)
    ELSE MIN(d.created_ms)
  END,
  MAX(CASE WHEN d.state IN ('queued', 'sending')
    THEN COALESCE(d.next_attempt_ms, d.digest_end_ms, d.created_ms) END),
  CASE
    WHEN SUM(CASE WHEN d.state IN ('queued', 'sending') THEN 1 ELSE 0 END) > 0 THEN NULL
    ELSE MAX(d.delivered_ms)
  END,
  CASE
    WHEN SUM(CASE WHEN d.state IN ('queued', 'sending') THEN 1 ELSE 0 END) > 0
      THEN MAX(CASE WHEN d.state IN ('queued', 'sending') THEN d.last_error END)
    ELSE MAX(d.last_error)
  END
FROM deliveries AS d
WHERE d.digest_period IS NOT NULL
  AND d.digest_start_ms IS NOT NULL
  AND d.digest_end_ms IS NOT NULL
GROUP BY d.event, d.reporter,
  COALESCE((SELECT r.host FROM runs AS r WHERE r.run_id=d.run_id), ''),
  d.digest_period, d.digest_start_ms, d.digest_end_ms;

UPDATE deliveries AS d
SET digest_cohort_id=(
  SELECT c.id FROM digest_cohorts AS c
  WHERE c.event=d.event AND c.reporter=d.reporter
    AND c.host=COALESCE((SELECT r.host FROM runs AS r WHERE r.run_id=d.run_id), '')
    AND c.digest_period=d.digest_period
    AND c.digest_start_ms=d.digest_start_ms
    AND c.digest_end_ms=d.digest_end_ms
)
WHERE d.digest_period IS NOT NULL
  AND d.digest_start_ms IS NOT NULL
  AND d.digest_end_ms IS NOT NULL;

UPDATE deliveries
SET state='expired', owner_pid=NULL, owner_start_ticks=NULL, owner_boot_id=NULL,
    last_error=COALESCE(last_error, 'legacy digest cohort already terminal')
WHERE digest_cohort_id IN (
  SELECT id FROM digest_cohorts WHERE state IN ('delivered', 'expired')
) AND state IN ('queued', 'sending');

UPDATE deliveries
SET state='queued',
    next_attempt_ms=(SELECT c.next_attempt_ms FROM digest_cohorts AS c WHERE c.id=digest_cohort_id),
    owner_pid=NULL, owner_start_ticks=NULL, owner_boot_id=NULL
WHERE digest_cohort_id IN (SELECT id FROM digest_cohorts WHERE state='queued')
  AND state IN ('queued', 'sending');

PRAGMA user_version = 4;
COMMIT;
"#,
            )?;
            return Ok(true);
        }
        // Forward-only migration inside an exclusive transaction (SPEC §7).
        if let Some(deadline) = deadline {
            self.set_remaining_busy_timeout(deadline)?;
        }
        self.conn.execute_batch(
            r#"
BEGIN EXCLUSIVE;
CREATE TABLE IF NOT EXISTS runs (
  run_id TEXT PRIMARY KEY,
  job_id TEXT NOT NULL,
  job_id_inferred INTEGER NOT NULL DEFAULT 0,
  inferred_basename TEXT,
  mode TEXT NOT NULL,
  argv_json TEXT,
  shell_cmd TEXT,
  cwd TEXT,
  env_names_json TEXT,
  host TEXT NOT NULL DEFAULT '',
  schedule_label TEXT,
  status TEXT NOT NULL,
  start_ms INTEGER NOT NULL,
  end_ms INTEGER,
  end_is_detection INTEGER NOT NULL DEFAULT 0,
  exit_code INTEGER,
  signal_no INTEGER,
  timeout_fired INTEGER NOT NULL DEFAULT 0,
  interrupted_by TEXT,
  start_error TEXT,
  wrapper_pid INTEGER NOT NULL DEFAULT 0,
  wrapper_start_ticks INTEGER NOT NULL DEFAULT 0,
  boot_id TEXT NOT NULL DEFAULT '',
  child_pid INTEGER,
  expected_duration_ms INTEGER,
  long_run_fired INTEGER NOT NULL DEFAULT 0,
  detached_children INTEGER NOT NULL DEFAULT 0,
  stdout_path TEXT,
  stdout_bytes_total INTEGER NOT NULL DEFAULT 0,
  stdout_bytes_stored INTEGER NOT NULL DEFAULT 0,
  stdout_bytes_omitted INTEGER NOT NULL DEFAULT 0,
  stdout_reason TEXT,
  stderr_path TEXT,
  stderr_bytes_total INTEGER NOT NULL DEFAULT 0,
  stderr_bytes_stored INTEGER NOT NULL DEFAULT 0,
  stderr_bytes_omitted INTEGER NOT NULL DEFAULT 0,
  stderr_reason TEXT,
  output_pruned_ms INTEGER
);
CREATE INDEX IF NOT EXISTS idx_runs_job_start ON runs(job_id, start_ms);
CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status);
CREATE INDEX IF NOT EXISTS idx_runs_start ON runs(start_ms);
CREATE TABLE IF NOT EXISTS deliveries (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL,
  job_id TEXT NOT NULL,
  event TEXT NOT NULL,
  reporter TEXT NOT NULL,
  state TEXT NOT NULL,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  created_ms INTEGER NOT NULL,
  next_attempt_ms INTEGER,
  delivered_ms INTEGER,
  last_error TEXT,
  owner_pid INTEGER,
  owner_start_ticks INTEGER,
  owner_boot_id TEXT,
  digest_period TEXT,
  digest_start_ms INTEGER,
  digest_end_ms INTEGER,
  digest_cohort_id INTEGER
);
CREATE TABLE IF NOT EXISTS digest_cohorts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  event TEXT NOT NULL,
  reporter TEXT NOT NULL,
  host TEXT NOT NULL,
  digest_period TEXT NOT NULL,
  digest_start_ms INTEGER NOT NULL,
  digest_end_ms INTEGER NOT NULL,
  state TEXT NOT NULL,
  member_count INTEGER NOT NULL DEFAULT 0 CHECK(member_count >= 0),
  attempt_count INTEGER NOT NULL DEFAULT 0,
  created_ms INTEGER NOT NULL,
  next_attempt_ms INTEGER,
  delivered_ms INTEGER,
  last_error TEXT,
  owner_pid INTEGER,
  owner_start_ticks INTEGER,
  owner_boot_id TEXT,
  UNIQUE(event, reporter, host, digest_period, digest_start_ms, digest_end_ms)
);
CREATE INDEX IF NOT EXISTS idx_deliv_state ON deliveries(state, next_attempt_ms);
CREATE INDEX IF NOT EXISTS idx_deliv_run ON deliveries(run_id);
CREATE INDEX IF NOT EXISTS idx_deliv_cohort_state ON deliveries(digest_cohort_id, state);
CREATE INDEX IF NOT EXISTS idx_digest_cohort_due ON digest_cohorts(state, next_attempt_ms);
CREATE INDEX IF NOT EXISTS idx_digest_cohort_prune ON digest_cohorts(digest_end_ms, state);
PRAGMA user_version = 4;
COMMIT;
"#,
        )?;
        Ok(true)
    }

    // ----- runs -----

    pub fn insert_run(&self, r: &RunRow) -> Result<(), StateError> {
        self.conn.execute(
            r#"INSERT INTO runs (
run_id, job_id, job_id_inferred, inferred_basename, mode, argv_json, shell_cmd,
cwd, env_names_json, host, schedule_label, status, start_ms,
wrapper_pid, wrapper_start_ticks, boot_id, child_pid, expected_duration_ms
) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18)"#,
            params![
                r.run_id,
                r.job_id,
                r.job_id_inferred,
                r.inferred_basename,
                r.mode,
                r.argv_json,
                r.shell_cmd,
                r.cwd,
                r.env_names_json,
                r.host,
                r.schedule_label,
                r.status,
                r.start_ms,
                r.wrapper_pid,
                r.wrapper_start_ticks,
                r.boot_id,
                r.child_pid,
                r.expected_duration_ms,
            ],
        )?;
        Ok(())
    }

    pub fn set_child_pid(&self, run_id: &str, pid: i64) -> Result<(), StateError> {
        self.conn.execute(
            "UPDATE runs SET child_pid=?2 WHERE run_id=?1",
            params![run_id, pid],
        )?;
        Ok(())
    }

    pub fn set_long_run_fired(&self, run_id: &str) -> Result<(), StateError> {
        self.conn.execute(
            "UPDATE runs SET long_run_fired=1 WHERE run_id=?1",
            params![run_id],
        )?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn finish_run(
        &self,
        run_id: &str,
        status: &str,
        end_ms: i64,
        exit_code: Option<i64>,
        signal_no: Option<i64>,
        timeout_fired: bool,
        interrupted_by: Option<&str>,
        start_error: Option<&str>,
        detached_children: bool,
        stdout: &CaptureMeta,
        stderr: &CaptureMeta,
    ) -> Result<(), StateError> {
        self.conn.execute(
            r#"UPDATE runs SET
status=?2, end_ms=?3, exit_code=?4, signal_no=?5, timeout_fired=?6,
interrupted_by=?7, start_error=?8, detached_children=?9,
stdout_path=?10, stdout_bytes_total=?11, stdout_bytes_stored=?12, stdout_bytes_omitted=?13, stdout_reason=?14,
stderr_path=?15, stderr_bytes_total=?16, stderr_bytes_stored=?17, stderr_bytes_omitted=?18, stderr_reason=?19
WHERE run_id=?1"#,
            params![
                run_id,
                status,
                end_ms,
                exit_code,
                signal_no,
                timeout_fired,
                interrupted_by,
                start_error,
                detached_children,
                stdout.path,
                stdout.bytes_total as i64,
                stdout.bytes_stored as i64,
                stdout.bytes_omitted as i64,
                stdout.reason,
                stderr.path,
                stderr.bytes_total as i64,
                stderr.bytes_stored as i64,
                stderr.bytes_omitted as i64,
                stderr.reason,
            ],
        )?;
        Ok(())
    }

    pub fn mark_stale(&self, run_id: &str, now: i64) -> Result<(), StateError> {
        self.conn.execute(
            "UPDATE runs SET status='stale', end_ms=?2, end_is_detection=1 WHERE run_id=?1 AND status='active'",
            params![run_id, now],
        )?;
        Ok(())
    }

    fn row_to_run(row: &rusqlite::Row<'_>) -> rusqlite::Result<RunRow> {
        Ok(RunRow {
            run_id: row.get("run_id")?,
            job_id: row.get("job_id")?,
            job_id_inferred: row.get("job_id_inferred")?,
            inferred_basename: row.get("inferred_basename")?,
            mode: row.get("mode")?,
            argv_json: row.get("argv_json")?,
            shell_cmd: row.get("shell_cmd")?,
            cwd: row.get("cwd")?,
            env_names_json: row.get("env_names_json")?,
            host: row.get("host")?,
            schedule_label: row.get("schedule_label")?,
            status: row.get("status")?,
            start_ms: row.get("start_ms")?,
            end_ms: row.get("end_ms")?,
            end_is_detection: row.get("end_is_detection")?,
            exit_code: row.get("exit_code")?,
            signal_no: row.get("signal_no")?,
            timeout_fired: row.get("timeout_fired")?,
            interrupted_by: row.get("interrupted_by")?,
            start_error: row.get("start_error")?,
            wrapper_pid: row.get("wrapper_pid")?,
            wrapper_start_ticks: row.get("wrapper_start_ticks")?,
            boot_id: row.get("boot_id")?,
            child_pid: row.get("child_pid")?,
            expected_duration_ms: row.get("expected_duration_ms")?,
            long_run_fired: row.get("long_run_fired")?,
            detached_children: row.get("detached_children")?,
            stdout: CaptureMeta {
                path: row.get("stdout_path")?,
                bytes_total: row.get::<_, i64>("stdout_bytes_total")? as u64,
                bytes_stored: row.get::<_, i64>("stdout_bytes_stored")? as u64,
                bytes_omitted: row.get::<_, i64>("stdout_bytes_omitted")? as u64,
                reason: row.get("stdout_reason")?,
            },
            stderr: CaptureMeta {
                path: row.get("stderr_path")?,
                bytes_total: row.get::<_, i64>("stderr_bytes_total")? as u64,
                bytes_stored: row.get::<_, i64>("stderr_bytes_stored")? as u64,
                bytes_omitted: row.get::<_, i64>("stderr_bytes_omitted")? as u64,
                reason: row.get("stderr_reason")?,
            },
            output_pruned_ms: row.get("output_pruned_ms")?,
        })
    }

    pub fn get_run(&self, run_id: &str) -> Result<Option<RunRow>, StateError> {
        Ok(self
            .conn
            .query_row(
                "SELECT * FROM runs WHERE run_id=?1",
                params![run_id],
                Self::row_to_run,
            )
            .optional()?)
    }

    /// Resolve a run id or unique prefix (≥4 chars) — SPEC §3 `show`.
    pub fn resolve_run_prefix(
        &self,
        prefix: &str,
    ) -> Result<Result<String, Vec<String>>, StateError> {
        let upper = prefix.to_ascii_uppercase();
        let mut stmt = self.conn.prepare(
            "SELECT run_id FROM runs WHERE run_id LIKE ?1 || '%' ORDER BY run_id LIMIT 10",
        )?;
        let ids: Vec<String> = stmt
            .query_map(params![upper], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?;
        Ok(match ids.len() {
            1 => Ok(ids.into_iter().next().unwrap()),
            _ => Err(ids),
        })
    }

    pub fn history(
        &self,
        limit: usize,
        job: Option<&str>,
        status: Option<&str>,
    ) -> Result<Vec<RunRow>, StateError> {
        let mut sql = String::from("SELECT * FROM runs WHERE 1=1");
        let mut args: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();
        if let Some(j) = job {
            sql.push_str(" AND job_id = ?");
            args.push(Box::new(j.to_string()));
        }
        if let Some(s) = status {
            sql.push_str(" AND status = ?");
            args.push(Box::new(s.to_string()));
        }
        sql.push_str(" ORDER BY start_ms DESC, run_id DESC LIMIT ?");
        args.push(Box::new(limit as i64));
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt
            .query_map(
                rusqlite::params_from_iter(args.iter().map(|b| b.as_ref())),
                Self::row_to_run,
            )?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    pub fn runs_with_status(&self, status: &str) -> Result<Vec<RunRow>, StateError> {
        let mut stmt = self
            .conn
            .prepare("SELECT * FROM runs WHERE status=?1 ORDER BY start_ms ASC")?;
        let rows = stmt
            .query_map(params![status], Self::row_to_run)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    /// Most recent prior terminal status for recovery derivation (SPEC §8):
    /// ordered by start time, terminal = not `active`.
    pub fn last_terminal_status_before(
        &self,
        job_id: &str,
        before_start_ms: i64,
        exclude_run_id: &str,
    ) -> Result<Option<String>, StateError> {
        Ok(self
            .conn
            .query_row(
                r#"SELECT status FROM runs
WHERE job_id=?1 AND run_id != ?2 AND status != 'active' AND start_ms <= ?3
ORDER BY start_ms DESC, run_id DESC LIMIT 1"#,
                params![job_id, exclude_run_id, before_start_ms],
                |r| r.get(0),
            )
            .optional()?)
    }

    /// Identity-fragmentation hint (SPEC §5): basenames with more than
    /// `threshold` distinct inferred ids in the last 30 days.
    pub fn fragmentation_hints(
        &self,
        now: i64,
        threshold: i64,
    ) -> Result<Vec<(String, i64)>, StateError> {
        let cutoff = now - 30 * 86_400_000;
        let mut stmt = self.conn.prepare(
            r#"SELECT inferred_basename, COUNT(DISTINCT job_id) AS c FROM runs
WHERE job_id_inferred=1 AND inferred_basename IS NOT NULL AND start_ms >= ?1
GROUP BY inferred_basename HAVING c > ?2 ORDER BY c DESC"#,
        )?;
        let rows = stmt
            .query_map(params![cutoff, threshold], |r| Ok((r.get(0)?, r.get(1)?)))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    // ----- deliveries -----

    #[allow(clippy::too_many_arguments)]
    pub fn insert_delivery(
        &self,
        run_id: &str,
        job_id: &str,
        event: &str,
        reporter: &str,
        state: &str,
        created_ms: i64,
        next_attempt_ms: Option<i64>,
        owner: Option<&crate::liveness::Liveness>,
    ) -> Result<i64, StateError> {
        self.insert_delivery_inner(
            run_id,
            job_id,
            event,
            reporter,
            state,
            created_ms,
            next_attempt_ms,
            owner,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn insert_digest_delivery(
        &self,
        run_id: &str,
        job_id: &str,
        event: &str,
        reporter: &str,
        _state: &str,
        created_ms: i64,
        next_attempt_ms: Option<i64>,
        _owner: Option<&crate::liveness::Liveness>,
        digest: &DeliveryDigest,
    ) -> Result<i64, StateError> {
        self.insert_digest_deliveries_inner(
            run_id,
            job_id,
            event,
            std::iter::once(reporter),
            created_ms,
            next_attempt_ms,
            digest,
            None,
        )?
        .into_iter()
        .next()
        .ok_or_else(|| StateError::Other("digest membership was not inserted".to_string()))
    }

    /// Atomically queue one digest membership for each reporter. The returned
    /// ids follow the input order and include truthful terminal-suppressed
    /// membership rows.
    #[allow(clippy::too_many_arguments)]
    pub fn insert_digest_deliveries(
        &self,
        run_id: &str,
        job_id: &str,
        event: &str,
        reporters: &[String],
        created_ms: i64,
        next_attempt_ms: Option<i64>,
        digest: &DeliveryDigest,
    ) -> Result<Vec<i64>, StateError> {
        if reporters.is_empty() {
            return Ok(Vec::new());
        }
        self.insert_digest_deliveries_inner(
            run_id,
            job_id,
            event,
            reporters.iter().map(String::as_str),
            created_ms,
            next_attempt_ms,
            digest,
            None,
        )
    }

    /// Atomically queue one digest membership for each reporter without
    /// exceeding the supplied child-path deadline. If the deadline expires
    /// before commit, every cohort and membership change in the batch is
    /// rolled back. The returned ids have the same input-order and terminal
    /// tombstone semantics as [`Db::insert_digest_deliveries`].
    #[allow(clippy::too_many_arguments)]
    pub fn insert_digest_deliveries_bounded(
        &self,
        run_id: &str,
        job_id: &str,
        event: &str,
        reporters: &[String],
        created_ms: i64,
        next_attempt_ms: Option<i64>,
        digest: &DeliveryDigest,
        deadline: std::time::Instant,
    ) -> Result<Vec<i64>, StateError> {
        if reporters.is_empty() {
            return Ok(Vec::new());
        }
        Self::ensure_digest_queue_deadline(Some(deadline))?;
        let previous_busy_timeout_ms: i64 =
            self.conn
                .query_row("PRAGMA busy_timeout", [], |row| row.get(0))?;
        let result = self.insert_digest_deliveries_inner(
            run_id,
            job_id,
            event,
            reporters.iter().map(String::as_str),
            created_ms,
            next_attempt_ms,
            digest,
            Some(deadline),
        );
        let restore = self.conn.busy_timeout(std::time::Duration::from_millis(
            previous_busy_timeout_ms.max(0) as u64,
        ));
        match result {
            Ok(ids) => {
                restore?;
                Ok(ids)
            }
            Err(_) if std::time::Instant::now() >= deadline => {
                let _ = restore;
                Err(Self::digest_queue_deadline_error())
            }
            Err(error) => {
                let _ = restore;
                Err(error)
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn insert_digest_deliveries_inner<'a>(
        &self,
        run_id: &str,
        job_id: &str,
        event: &str,
        reporters: impl IntoIterator<Item = &'a str>,
        created_ms: i64,
        next_attempt_ms: Option<i64>,
        digest: &DeliveryDigest,
        deadline: Option<std::time::Instant>,
    ) -> Result<Vec<i64>, StateError> {
        // Reserve the single WAL writer before reading the run host. A
        // deferred read followed by a write can otherwise fail immediately
        // on lock upgrade when cron wrappers queue the same window together.
        self.set_digest_queue_busy_timeout(deadline)?;
        let tx = Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)?;
        Self::ensure_digest_queue_deadline(deadline)?;
        Self::set_digest_queue_tx_timeout(&tx, deadline)?;
        let host = tx
            .query_row(
                "SELECT host FROM runs WHERE run_id=?1",
                params![run_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .unwrap_or_default();
        Self::ensure_digest_queue_deadline(deadline)?;
        let mut ids = Vec::new();
        for reporter in reporters {
            Self::set_digest_queue_tx_timeout(&tx, deadline)?;
            ids.push(Self::insert_digest_membership_tx(
                &tx,
                run_id,
                job_id,
                event,
                reporter,
                &host,
                created_ms,
                next_attempt_ms,
                digest,
            )?);
            Self::ensure_digest_queue_deadline(deadline)?;
        }
        Self::set_digest_queue_tx_timeout(&tx, deadline)?;
        tx.commit()?;
        Ok(ids)
    }

    fn set_digest_queue_busy_timeout(
        &self,
        deadline: Option<std::time::Instant>,
    ) -> Result<(), StateError> {
        Self::ensure_digest_queue_deadline(deadline)?;
        if let Some(deadline) = deadline {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining < std::time::Duration::from_millis(1) {
                return Err(Self::digest_queue_deadline_error());
            }
            self.conn.busy_timeout(remaining)?;
        }
        Ok(())
    }

    fn set_digest_queue_tx_timeout(
        tx: &Transaction<'_>,
        deadline: Option<std::time::Instant>,
    ) -> Result<(), StateError> {
        Self::ensure_digest_queue_deadline(deadline)?;
        if let Some(deadline) = deadline {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining < std::time::Duration::from_millis(1) {
                return Err(Self::digest_queue_deadline_error());
            }
            tx.busy_timeout(remaining)?;
        }
        Ok(())
    }

    fn ensure_digest_queue_deadline(
        deadline: Option<std::time::Instant>,
    ) -> Result<(), StateError> {
        if deadline.is_some_and(|deadline| std::time::Instant::now() >= deadline) {
            return Err(Self::digest_queue_deadline_error());
        }
        Ok(())
    }

    fn digest_queue_deadline_error() -> StateError {
        StateError::Other("digest queue deadline exceeded".to_string())
    }

    #[allow(clippy::too_many_arguments)]
    fn insert_digest_membership_tx(
        tx: &Transaction<'_>,
        run_id: &str,
        job_id: &str,
        event: &str,
        reporter: &str,
        host: &str,
        created_ms: i64,
        next_attempt_ms: Option<i64>,
        digest: &DeliveryDigest,
    ) -> Result<i64, StateError> {
        let due = next_attempt_ms.unwrap_or(digest.end_ms);
        tx.execute(
            r#"INSERT INTO digest_cohorts (
event, reporter, host, digest_period, digest_start_ms, digest_end_ms,
state, member_count, attempt_count, created_ms, next_attempt_ms
) VALUES (?1,?2,?3,?4,?5,?6,'queued',0,0,?7,?8)
ON CONFLICT(event, reporter, host, digest_period, digest_start_ms, digest_end_ms)
DO UPDATE SET
  created_ms=MIN(digest_cohorts.created_ms, excluded.created_ms),
  next_attempt_ms=CASE WHEN digest_cohorts.state='queued'
    THEN MAX(COALESCE(digest_cohorts.next_attempt_ms, excluded.next_attempt_ms), excluded.next_attempt_ms)
    ELSE digest_cohorts.next_attempt_ms END"#,
            params![
                event,
                reporter,
                host,
                digest.period,
                digest.start_ms,
                digest.end_ms,
                created_ms,
                due,
            ],
        )?;
        let (cohort_id, cohort_state, cohort_due): (i64, String, Option<i64>) = tx.query_row(
            r#"SELECT id, state, next_attempt_ms FROM digest_cohorts
WHERE event=?1 AND reporter=?2 AND host=?3 AND digest_period=?4
  AND digest_start_ms=?5 AND digest_end_ms=?6"#,
            params![
                event,
                reporter,
                host,
                digest.period,
                digest.start_ms,
                digest.end_ms,
            ],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        let (member_state, member_error) = match cohort_state.as_str() {
            "queued" | "sending" => ("queued", None),
            "delivered" => (
                "expired",
                Some("digest cohort was already delivered; late membership suppressed"),
            ),
            _ => (
                "expired",
                Some("digest cohort was already expired; late membership suppressed"),
            ),
        };
        tx.execute(
            r#"INSERT INTO deliveries (
run_id, job_id, event, reporter, state, attempt_count, created_ms,
next_attempt_ms, last_error, digest_period, digest_start_ms, digest_end_ms,
digest_cohort_id
) VALUES (?1,?2,?3,?4,?5,0,?6,?7,?8,?9,?10,?11,?12)"#,
            params![
                run_id,
                job_id,
                event,
                reporter,
                member_state,
                created_ms,
                cohort_due.or(next_attempt_ms).or(Some(digest.end_ms)),
                member_error,
                digest.period,
                digest.start_ms,
                digest.end_ms,
                cohort_id,
            ],
        )?;
        let id = tx.last_insert_rowid();
        if matches!(cohort_state.as_str(), "queued" | "sending") {
            let changed = tx.execute(
                r#"UPDATE digest_cohorts SET member_count=member_count+1
WHERE id=?1 AND state IN ('queued', 'sending')"#,
                params![cohort_id],
            )?;
            debug_assert_eq!(changed, 1);
        }
        Ok(id)
    }

    #[allow(clippy::too_many_arguments)]
    fn insert_delivery_inner(
        &self,
        run_id: &str,
        job_id: &str,
        event: &str,
        reporter: &str,
        state: &str,
        created_ms: i64,
        next_attempt_ms: Option<i64>,
        owner: Option<&crate::liveness::Liveness>,
        digest: Option<&DeliveryDigest>,
    ) -> Result<i64, StateError> {
        self.conn.execute(
            r#"INSERT INTO deliveries
(run_id, job_id, event, reporter, state, attempt_count, created_ms, next_attempt_ms, owner_pid, owner_start_ticks, owner_boot_id, digest_period, digest_start_ms, digest_end_ms)
VALUES (?1,?2,?3,?4,?5,0,?6,?7,?8,?9,?10,?11,?12,?13)"#,
            params![
                run_id,
                job_id,
                event,
                reporter,
                state,
                created_ms,
                next_attempt_ms,
                owner.map(|o| o.pid as i64),
                owner.map(|o| o.start_ticks as i64),
                owner.map(|o| o.boot_id.clone()),
                digest.map(|d| d.period.clone()),
                digest.map(|d| d.start_ms),
                digest.map(|d| d.end_ms),
            ],
        )?;
        Ok(self.conn.last_insert_rowid())
    }

    fn row_to_delivery(row: &rusqlite::Row<'_>) -> rusqlite::Result<DeliveryRow> {
        Ok(DeliveryRow {
            id: row.get("id")?,
            run_id: row.get("run_id")?,
            job_id: row.get("job_id")?,
            event: row.get("event")?,
            reporter: row.get("reporter")?,
            state: row.get("state")?,
            attempt_count: row.get("attempt_count")?,
            created_ms: row.get("created_ms")?,
            next_attempt_ms: row.get("next_attempt_ms")?,
            delivered_ms: row.get("delivered_ms")?,
            last_error: row.get("last_error")?,
            owner_pid: row.get("owner_pid")?,
            owner_start_ticks: row.get("owner_start_ticks")?,
            owner_boot_id: row.get("owner_boot_id")?,
            digest_period: row.get("digest_period")?,
            digest_start_ms: row.get("digest_start_ms")?,
            digest_end_ms: row.get("digest_end_ms")?,
            digest_cohort_id: row.get("digest_cohort_id")?,
        })
    }

    pub fn get_delivery(&self, id: i64) -> Result<Option<DeliveryRow>, StateError> {
        Ok(self
            .conn
            .query_row(
                "SELECT * FROM deliveries WHERE id=?1",
                params![id],
                Self::row_to_delivery,
            )
            .optional()?)
    }

    pub fn due_deliveries(&self, now: i64) -> Result<Vec<DeliveryRow>, StateError> {
        let mut stmt = self.conn.prepare(
            r#"SELECT * FROM deliveries
WHERE state='queued' AND next_attempt_ms <= ?1 AND digest_cohort_id IS NULL
ORDER BY next_attempt_ms ASC, id ASC"#,
        )?;
        let rows = stmt
            .query_map(params![now], Self::row_to_delivery)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    pub fn due_deliveries_limited(
        &self,
        now: i64,
        limit: usize,
    ) -> Result<Vec<DeliveryRow>, StateError> {
        let mut stmt = self.conn.prepare(
            r#"SELECT * FROM deliveries
WHERE state='queued' AND next_attempt_ms <= ?1 AND digest_cohort_id IS NULL
ORDER BY next_attempt_ms ASC, id ASC LIMIT ?2"#,
        )?;
        let rows = stmt
            .query_map(params![now, limit as i64], Self::row_to_delivery)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    fn row_to_digest_cohort(row: &rusqlite::Row<'_>) -> rusqlite::Result<DigestCohortRow> {
        Ok(DigestCohortRow {
            id: row.get("id")?,
            event: row.get("event")?,
            reporter: row.get("reporter")?,
            host: row.get("host")?,
            period: row.get("digest_period")?,
            start_ms: row.get("digest_start_ms")?,
            end_ms: row.get("digest_end_ms")?,
            state: row.get("state")?,
            member_count: row.get("member_count")?,
            attempt_count: row.get("attempt_count")?,
            created_ms: row.get("created_ms")?,
            next_attempt_ms: row.get("next_attempt_ms")?,
            delivered_ms: row.get("delivered_ms")?,
            last_error: row.get("last_error")?,
            owner_pid: row.get("owner_pid")?,
            owner_start_ticks: row.get("owner_start_ticks")?,
            owner_boot_id: row.get("owner_boot_id")?,
        })
    }

    pub fn get_digest_cohort(&self, id: i64) -> Result<Option<DigestCohortRow>, StateError> {
        Ok(self
            .conn
            .query_row(
                "SELECT * FROM digest_cohorts WHERE id=?1",
                params![id],
                Self::row_to_digest_cohort,
            )
            .optional()?)
    }

    pub fn due_digest_cohorts(&self, now: i64) -> Result<Vec<DigestCohortRow>, StateError> {
        let mut stmt = self.conn.prepare(
            r#"SELECT * FROM digest_cohorts
WHERE state='queued' AND next_attempt_ms <= ?1
ORDER BY next_attempt_ms ASC, id ASC"#,
        )?;
        let rows = stmt
            .query_map(params![now], Self::row_to_digest_cohort)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    pub fn due_digest_cohorts_limited(
        &self,
        now: i64,
        limit: usize,
        max_members: i64,
    ) -> Result<Vec<DigestCohortRow>, StateError> {
        let mut stmt = self.conn.prepare(
            r#"SELECT * FROM digest_cohorts
WHERE state='queued' AND next_attempt_ms <= ?1 AND member_count <= ?3
ORDER BY next_attempt_ms ASC, id ASC LIMIT ?2"#,
        )?;
        let rows = stmt
            .query_map(
                params![now, limit as i64, max_members],
                Self::row_to_digest_cohort,
            )?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    /// Atomically claim a queued row for a synchronous attempt.
    pub fn claim_delivery(
        &self,
        id: i64,
        owner: &crate::liveness::Liveness,
    ) -> Result<bool, StateError> {
        let n = self.conn.execute(
            "UPDATE deliveries SET state='sending', owner_pid=?2, owner_start_ticks=?3, owner_boot_id=?4 WHERE id=?1 AND state='queued'",
            params![id, owner.pid as i64, owner.start_ticks as i64, owner.boot_id],
        )?;
        Ok(n == 1)
    }

    pub fn claim_digest_cohort(
        &self,
        cohort_id: i64,
        owner: &crate::liveness::Liveness,
        now: i64,
    ) -> Result<Option<DigestClaim>, StateError> {
        let tx = self.conn.unchecked_transaction()?;
        let changed = tx.execute(
            r#"UPDATE digest_cohorts
SET state='sending', owner_pid=?2, owner_start_ticks=?3, owner_boot_id=?4
WHERE id=?1 AND state='queued' AND next_attempt_ms <= ?5"#,
            params![
                cohort_id,
                owner.pid as i64,
                owner.start_ticks as i64,
                owner.boot_id,
                now,
            ],
        )?;
        if changed == 0 {
            tx.commit()?;
            return Ok(None);
        }
        // Once the cohort-wide backoff is due, every current queued member is
        // part of this attempt even if a legacy row carries a later timestamp.
        tx.execute(
            r#"UPDATE deliveries
SET state='sending', owner_pid=?2, owner_start_ticks=?3, owner_boot_id=?4
WHERE digest_cohort_id=?1 AND state='queued'"#,
            params![
                cohort_id,
                owner.pid as i64,
                owner.start_ticks as i64,
                owner.boot_id,
            ],
        )?;
        let claim = tx.query_row(
            r#"SELECT id, event, reporter, host, digest_period, digest_start_ms,
digest_end_ms, member_count, attempt_count, created_ms
FROM digest_cohorts WHERE id=?1"#,
            params![cohort_id],
            |row| {
                Ok(DigestClaim {
                    cohort_id: row.get(0)?,
                    event: row.get(1)?,
                    reporter: row.get(2)?,
                    host: row.get(3)?,
                    period: row.get(4)?,
                    start_ms: row.get(5)?,
                    end_ms: row.get(6)?,
                    member_count: row.get(7)?,
                    attempt_count: row.get(8)?,
                    created_ms: row.get(9)?,
                })
            },
        )?;
        tx.commit()?;
        Ok(Some(claim))
    }

    pub fn load_digest_aggregate(
        &self,
        claim: &DigestClaim,
        owner: &crate::liveness::Liveness,
    ) -> Result<DigestAggregate, StateError> {
        let member_cte = r#"WITH member_runs AS (
  SELECT r.job_id, r.run_id, r.status, r.start_ms, r.end_ms,
         r.end_is_detection, r.schedule_label,
         CASE WHEN r.end_ms IS NOT NULL AND r.end_is_detection=0
              THEN MAX(r.end_ms-r.start_ms, 0) END AS duration_ms
  FROM runs AS r
  JOIN (
    SELECT DISTINCT run_id FROM deliveries
    WHERE digest_cohort_id=?1 AND state='sending'
      AND owner_pid=?2 AND owner_start_ticks=?3 AND owner_boot_id=?4
  ) AS members ON members.run_id=r.run_id
)"#;
        let owner_params = params![
            claim.cohort_id,
            owner.pid as i64,
            owner.start_ticks as i64,
            owner.boot_id,
        ];
        let totals_sql = format!(
            r#"{member_cte}
SELECT COUNT(DISTINCT job_id), COUNT(*),
  COALESCE(SUM(status='success'),0), COALESCE(SUM(status='failure'),0),
  COALESCE(SUM(status='timeout'),0), COALESCE(SUM(status='stale'),0),
  COALESCE(SUM(status='start_failed'),0), COALESCE(SUM(status='active'),0),
  COALESCE(SUM(status!='success'),0)
FROM member_runs"#
        );
        let mut aggregate = self.conn.query_row(&totals_sql, owner_params, |row| {
            let total_success: i64 = row.get(2)?;
            Ok(DigestAggregate {
                total_jobs: nonnegative_u64(row.get(0)?),
                total_executions: nonnegative_u64(row.get(1)?),
                statuses: DigestStatusTotals {
                    success: nonnegative_u64(total_success),
                    failure: nonnegative_u64(row.get(3)?),
                    timeout: nonnegative_u64(row.get(4)?),
                    stale: nonnegative_u64(row.get(5)?),
                    start_failed: nonnegative_u64(row.get(6)?),
                    active: nonnegative_u64(row.get(7)?),
                },
                total_problem_executions: nonnegative_u64(row.get(8)?),
                total_success_executions: nonnegative_u64(total_success),
                ..DigestAggregate::default()
            })
        })?;

        let jobs_sql = format!(
            r#"{member_cte}, ranked AS (
  SELECT *, ROW_NUMBER() OVER (
    PARTITION BY job_id ORDER BY start_ms DESC, run_id ASC
  ) AS latest_rank
  FROM member_runs
)
SELECT job_id, COUNT(*),
  COALESCE(SUM(status='success'),0), COALESCE(SUM(status='failure'),0),
  COALESCE(SUM(status='timeout'),0), COALESCE(SUM(status='stale'),0),
  COALESCE(SUM(status='start_failed'),0), COALESCE(SUM(status='active'),0),
  CAST(AVG(duration_ms) AS INTEGER), MAX(duration_ms),
  MAX(CASE WHEN latest_rank=1 THEN status END),
  MAX(CASE WHEN latest_rank=1 THEN start_ms END),
  MAX(CASE WHEN latest_rank=1 THEN duration_ms END),
  MAX(CASE WHEN latest_rank=1 THEN schedule_label END),
  MAX(status!='success') AS has_problem
FROM ranked GROUP BY job_id
ORDER BY has_problem DESC, job_id ASC
LIMIT {}"#,
            DIGEST_JOB_SUMMARY_LIMIT
        );
        let mut stmt = self.conn.prepare(&jobs_sql)?;
        aggregate.job_summaries = stmt
            .query_map(
                params![
                    claim.cohort_id,
                    owner.pid as i64,
                    owner.start_ticks as i64,
                    owner.boot_id,
                ],
                |row| {
                    let average_ms: Option<i64> = row.get(8)?;
                    let max_ms: Option<i64> = row.get(9)?;
                    Ok(DigestJobAggregate {
                        job_id: row.get(0)?,
                        total_executions: nonnegative_u64(row.get(1)?),
                        statuses: DigestStatusTotals {
                            success: nonnegative_u64(row.get(2)?),
                            failure: nonnegative_u64(row.get(3)?),
                            timeout: nonnegative_u64(row.get(4)?),
                            stale: nonnegative_u64(row.get(5)?),
                            start_failed: nonnegative_u64(row.get(6)?),
                            active: nonnegative_u64(row.get(7)?),
                        },
                        durations: average_ms.zip(max_ms).map(|(average_ms, max_ms)| {
                            DigestDurationAggregate {
                                average_ms: nonnegative_u64(average_ms),
                                max_ms: nonnegative_u64(max_ms),
                            }
                        }),
                        latest: DigestLatestAggregate {
                            status: row.get(10)?,
                            start_ms: row.get(11)?,
                            duration_ms: row.get::<_, Option<i64>>(12)?.map(nonnegative_u64),
                            schedule_label: row.get(13)?,
                        },
                    })
                },
            )?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        aggregate.problem_details =
            self.digest_execution_details(claim, owner, false, DIGEST_PROBLEM_DETAIL_LIMIT)?;
        aggregate.success_details =
            self.digest_execution_details(claim, owner, true, DIGEST_SUCCESS_DETAIL_LIMIT)?;
        Ok(aggregate)
    }

    fn digest_execution_details(
        &self,
        claim: &DigestClaim,
        owner: &crate::liveness::Liveness,
        successes: bool,
        limit: usize,
    ) -> Result<Vec<DigestExecutionAggregate>, StateError> {
        let status_predicate = if successes {
            "member_runs.status='success'"
        } else {
            "member_runs.status!='success'"
        };
        let order = if successes {
            "member_runs.start_ms DESC, member_runs.job_id ASC, member_runs.run_id ASC"
        } else {
            "member_runs.job_id ASC, member_runs.start_ms DESC, member_runs.run_id ASC"
        };
        let sql = format!(
            r#"WITH member_runs AS (
  SELECT r.job_id, r.run_id, r.status, r.start_ms,
         CASE WHEN r.end_ms IS NOT NULL AND r.end_is_detection=0
              THEN MAX(r.end_ms-r.start_ms, 0) END AS duration_ms
  FROM runs AS r
  JOIN (
    SELECT DISTINCT run_id FROM deliveries
    WHERE digest_cohort_id=?1 AND state='sending'
      AND owner_pid=?2 AND owner_start_ticks=?3 AND owner_boot_id=?4
  ) AS members ON members.run_id=r.run_id
)
SELECT member_runs.job_id, member_runs.run_id, member_runs.status,
       member_runs.start_ms, member_runs.duration_ms
FROM member_runs
WHERE {status_predicate}
ORDER BY {order} LIMIT {limit}"#
        );
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt
            .query_map(
                params![
                    claim.cohort_id,
                    owner.pid as i64,
                    owner.start_ticks as i64,
                    owner.boot_id,
                ],
                |row| {
                    Ok(DigestExecutionAggregate {
                        job_id: row.get(0)?,
                        run_id: row.get(1)?,
                        status: row.get(2)?,
                        start_ms: row.get(3)?,
                        duration_ms: row.get::<_, Option<i64>>(4)?.map(nonnegative_u64),
                    })
                },
            )?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    /// Mark one claimed digest cohort delivered and tombstone members that
    /// arrived after its membership snapshot.
    pub fn digest_group_delivered(
        &self,
        claim: &DigestClaim,
        owner: &crate::liveness::Liveness,
        now: i64,
    ) -> Result<usize, StateError> {
        let tx = self.conn.unchecked_transaction()?;
        let changed = tx.execute(
            r#"UPDATE digest_cohorts
SET state='delivered', delivered_ms=?5, attempt_count=attempt_count+1,
    next_attempt_ms=NULL, last_error=NULL,
    owner_pid=NULL, owner_start_ticks=NULL, owner_boot_id=NULL
WHERE id=?1 AND state='sending'
  AND owner_pid=?2 AND owner_start_ticks=?3 AND owner_boot_id=?4"#,
            params![
                claim.cohort_id,
                owner.pid as i64,
                owner.start_ticks as i64,
                owner.boot_id,
                now,
            ],
        )?;
        if changed == 0 {
            tx.commit()?;
            return Ok(0);
        }
        let delivered = tx.execute(
            r#"UPDATE deliveries
SET state='delivered', delivered_ms=?5, attempt_count=attempt_count+1,
    last_error=NULL, owner_pid=NULL, owner_start_ticks=NULL, owner_boot_id=NULL
WHERE digest_cohort_id=?1 AND state='sending'
  AND owner_pid=?2 AND owner_start_ticks=?3 AND owner_boot_id=?4"#,
            params![
                claim.cohort_id,
                owner.pid as i64,
                owner.start_ticks as i64,
                owner.boot_id,
                now,
            ],
        )?;
        tx.execute(
            r#"UPDATE deliveries
SET state='expired', last_error='digest cohort delivered before late membership could be included',
    owner_pid=NULL, owner_start_ticks=NULL, owner_boot_id=NULL
WHERE digest_cohort_id=?1 AND state='queued'"#,
            params![claim.cohort_id],
        )?;
        tx.commit()?;
        Ok(delivered)
    }

    /// Queue every row in one claimed digest group after a failed attempt.
    pub fn digest_group_queued(
        &self,
        claim: &DigestClaim,
        owner: &crate::liveness::Liveness,
        next_attempt_ms: i64,
        error: &str,
    ) -> Result<usize, StateError> {
        let tx = self.conn.unchecked_transaction()?;
        let changed = tx.execute(
            r#"UPDATE digest_cohorts
SET state='queued', attempt_count=attempt_count+1, next_attempt_ms=?5,
    last_error=?6, owner_pid=NULL, owner_start_ticks=NULL, owner_boot_id=NULL
WHERE id=?1 AND state='sending'
  AND owner_pid=?2 AND owner_start_ticks=?3 AND owner_boot_id=?4"#,
            params![
                claim.cohort_id,
                owner.pid as i64,
                owner.start_ticks as i64,
                owner.boot_id,
                next_attempt_ms,
                error,
            ],
        )?;
        if changed == 0 {
            tx.commit()?;
            return Ok(0);
        }
        let members = tx.execute(
            r#"UPDATE deliveries
SET attempt_count=attempt_count+CASE WHEN state='sending' THEN 1 ELSE 0 END,
    state='queued', next_attempt_ms=?2, last_error=?3,
    owner_pid=NULL, owner_start_ticks=NULL, owner_boot_id=NULL
WHERE digest_cohort_id=?1 AND state IN ('queued', 'sending')"#,
            params![claim.cohort_id, next_attempt_ms, error,],
        )?;
        tx.commit()?;
        Ok(members)
    }

    /// Requeue a claimed digest group without counting an attempt.
    pub fn digest_group_requeue(
        &self,
        claim: &DigestClaim,
        owner: &crate::liveness::Liveness,
        next_attempt_ms: i64,
    ) -> Result<usize, StateError> {
        self.requeue_digest_cohort_owner(
            claim.cohort_id,
            owner.pid as i64,
            owner.start_ticks as i64,
            &owner.boot_id,
            next_attempt_ms,
        )
    }

    /// Expire every row in one claimed digest group in one statement.
    pub fn digest_group_expired(
        &self,
        claim: &DigestClaim,
        owner: &crate::liveness::Liveness,
        error: &str,
    ) -> Result<usize, StateError> {
        let tx = self.conn.unchecked_transaction()?;
        let changed = tx.execute(
            r#"UPDATE digest_cohorts
SET state='expired', next_attempt_ms=NULL, last_error=?5,
    owner_pid=NULL, owner_start_ticks=NULL, owner_boot_id=NULL
WHERE id=?1 AND state='sending'
  AND owner_pid=?2 AND owner_start_ticks=?3 AND owner_boot_id=?4"#,
            params![
                claim.cohort_id,
                owner.pid as i64,
                owner.start_ticks as i64,
                owner.boot_id,
                error,
            ],
        )?;
        if changed == 0 {
            tx.commit()?;
            return Ok(0);
        }
        let members = tx.execute(
            r#"UPDATE deliveries
SET state='expired', last_error=?2,
    owner_pid=NULL, owner_start_ticks=NULL, owner_boot_id=NULL
WHERE digest_cohort_id=?1 AND state IN ('queued', 'sending')"#,
            params![claim.cohort_id, error],
        )?;
        tx.commit()?;
        Ok(members)
    }

    /// Idempotently requeue an orphaned digest cohort from any one of its
    /// sending membership rows.
    pub fn digest_cohort_requeue_orphan(
        &self,
        cohort: &DigestCohortRow,
        next_attempt_ms: i64,
    ) -> Result<usize, StateError> {
        let (Some(pid), Some(ticks), Some(boot_id)) = (
            cohort.owner_pid,
            cohort.owner_start_ticks,
            cohort.owner_boot_id.as_deref(),
        ) else {
            return Ok(0);
        };
        self.requeue_digest_cohort_owner(cohort.id, pid, ticks, boot_id, next_attempt_ms)
    }

    fn requeue_digest_cohort_owner(
        &self,
        cohort_id: i64,
        owner_pid: i64,
        owner_start_ticks: i64,
        owner_boot_id: &str,
        next_attempt_ms: i64,
    ) -> Result<usize, StateError> {
        let tx = self.conn.unchecked_transaction()?;
        let changed = tx.execute(
            r#"UPDATE digest_cohorts
SET state='queued', next_attempt_ms=?5,
    owner_pid=NULL, owner_start_ticks=NULL, owner_boot_id=NULL
WHERE id=?1 AND state='sending'
  AND owner_pid=?2 AND owner_start_ticks=?3 AND owner_boot_id=?4"#,
            params![
                cohort_id,
                owner_pid,
                owner_start_ticks,
                owner_boot_id,
                next_attempt_ms,
            ],
        )?;
        if changed == 0 {
            tx.commit()?;
            return Ok(0);
        }
        let members = tx.execute(
            r#"UPDATE deliveries
SET state='queued', next_attempt_ms=?2,
    owner_pid=NULL, owner_start_ticks=NULL, owner_boot_id=NULL
WHERE digest_cohort_id=?1 AND state IN ('queued', 'sending')"#,
            params![cohort_id, next_attempt_ms],
        )?;
        tx.commit()?;
        Ok(members)
    }

    pub fn delivery_delivered(&self, id: i64, now: i64) -> Result<(), StateError> {
        self.conn.execute(
            "UPDATE deliveries SET state='delivered', delivered_ms=?2, attempt_count=attempt_count+1, last_error=NULL WHERE id=?1",
            params![id, now],
        )?;
        Ok(())
    }

    pub fn delivery_queued(
        &self,
        id: i64,
        next_attempt_ms: i64,
        error: &str,
    ) -> Result<(), StateError> {
        self.conn.execute(
            "UPDATE deliveries SET state='queued', attempt_count=attempt_count+1, next_attempt_ms=?2, last_error=?3 WHERE id=?1",
            params![id, next_attempt_ms, error],
        )?;
        Ok(())
    }

    /// Re-queue without counting an attempt (overall budget ran out before
    /// this row was tried, or an orphan was reclaimed).
    pub fn delivery_requeue(&self, id: i64, next_attempt_ms: i64) -> Result<(), StateError> {
        self.conn.execute(
            "UPDATE deliveries SET state='queued', next_attempt_ms=?2 WHERE id=?1",
            params![id, next_attempt_ms],
        )?;
        Ok(())
    }

    pub fn delivery_expired(&self, id: i64, error: &str) -> Result<(), StateError> {
        self.conn.execute(
            "UPDATE deliveries SET state='expired', last_error=?2 WHERE id=?1",
            params![id, error],
        )?;
        Ok(())
    }

    pub fn deliveries_for_run(&self, run_id: &str) -> Result<Vec<DeliveryRow>, StateError> {
        let mut stmt = self
            .conn
            .prepare("SELECT * FROM deliveries WHERE run_id=?1 ORDER BY id ASC")?;
        let rows = stmt
            .query_map(params![run_id], Self::row_to_delivery)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    pub fn sending_deliveries(&self) -> Result<Vec<DeliveryRow>, StateError> {
        let mut stmt = self.conn.prepare(
            "SELECT * FROM deliveries WHERE state='sending' AND digest_cohort_id IS NULL",
        )?;
        let rows = stmt
            .query_map([], Self::row_to_delivery)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    /// Compact owner records for orphan reconciliation: one row per digest
    /// cohort rather than one full delivery row per membership.
    pub fn sending_digest_cohorts(&self) -> Result<Vec<DigestCohortRow>, StateError> {
        let mut stmt = self
            .conn
            .prepare("SELECT * FROM digest_cohorts WHERE state='sending' ORDER BY id ASC")?;
        let rows = stmt
            .query_map([], Self::row_to_digest_cohort)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    // ----- retention (SPEC §7) -----

    /// Runs whose row (and output, and deliveries) may age out entirely.
    /// Queued/sending delivery membership protects metadata until seven days
    /// after the event (or digest window end), so a 31-day monthly digest is
    /// not destroyed by the default 30-day run-retention pass.
    pub fn runs_older_than(&self, cutoff_ms: i64, now_ms: i64) -> Result<Vec<RunRow>, StateError> {
        let expiry_cutoff_ms = now_ms.saturating_sub(DELIVERY_RETRY_MAX_AGE_MS);
        let mut stmt = self.conn.prepare(
            r#"SELECT r.* FROM runs AS r
WHERE r.status != 'active' AND r.start_ms < ?1
  AND NOT EXISTS (
    SELECT 1 FROM deliveries AS d
    WHERE d.run_id=r.run_id AND d.state IN ('queued', 'sending')
      AND COALESCE(d.digest_end_ms, d.created_ms) >= ?2
  )
ORDER BY r.start_ms ASC"#,
        )?;
        let rows = stmt
            .query_map(params![cutoff_ms, expiry_cutoff_ms], Self::row_to_run)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    pub fn delete_run(&self, run_id: &str) -> Result<(), StateError> {
        let tx = self.conn.unchecked_transaction()?;
        tx.execute("DELETE FROM deliveries WHERE run_id=?1", params![run_id])?;
        tx.execute("DELETE FROM runs WHERE run_id=?1", params![run_id])?;
        tx.commit()?;
        Ok(())
    }

    /// Remove cohort tombstones only after their retry horizon has elapsed and
    /// run retention has removed every membership row. Sending cohorts stay
    /// durable so orphan reconciliation can still recover their ownership.
    fn prune_empty_digest_cohorts(&self, now_ms: i64) -> Result<usize, StateError> {
        let expiry_cutoff_ms = now_ms.saturating_sub(DELIVERY_RETRY_MAX_AGE_MS);
        Ok(self.conn.execute(
            r#"DELETE FROM digest_cohorts
WHERE state != 'sending' AND digest_end_ms < ?1
  AND NOT EXISTS (
    SELECT 1 FROM deliveries AS d WHERE d.digest_cohort_id=digest_cohorts.id
  )"#,
            params![expiry_cutoff_ms],
        )?)
    }

    /// Oldest-first candidates for byte-cap output pruning: terminal runs that
    /// still have stored output.
    pub fn output_prune_candidates(&self) -> Result<Vec<RunRow>, StateError> {
        let mut stmt = self.conn.prepare(
            r#"SELECT * FROM runs WHERE status != 'active' AND output_pruned_ms IS NULL
AND (stdout_bytes_stored > 0 OR stderr_bytes_stored > 0)
ORDER BY start_ms ASC"#,
        )?;
        let rows = stmt
            .query_map([], Self::row_to_run)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    pub fn total_stored_output_bytes(&self) -> Result<u64, StateError> {
        let n: i64 = self.conn.query_row(
            r#"SELECT COALESCE(SUM(stdout_bytes_stored + stderr_bytes_stored), 0)
FROM runs WHERE output_pruned_ms IS NULL AND status != 'active'"#,
            [],
            |r| r.get(0),
        )?;
        Ok(n.max(0) as u64)
    }

    pub fn mark_output_pruned(&self, run_id: &str, now: i64) -> Result<(), StateError> {
        self.conn.execute(
            "UPDATE runs SET output_pruned_ms=?2 WHERE run_id=?1",
            params![run_id, now],
        )?;
        Ok(())
    }
}

/// Result of one prune pass.
#[derive(Debug, Default)]
pub struct PruneReport {
    pub aged_runs: Vec<String>,
    pub output_pruned_runs: Vec<String>,
    pub bytes_freed: u64,
}

impl PruneReport {
    pub fn is_empty(&self) -> bool {
        self.aged_runs.is_empty() && self.output_pruned_runs.is_empty()
    }
}

/// Apply retention (SPEC §7): `max_age` deletes run rows + output + delivery
/// rows; `max_bytes` deletes oldest runs' output files only, keeping metadata
/// and stamping `output_pruned_at`.
pub fn prune(
    db: &Db,
    output_root: &Path,
    max_age: std::time::Duration,
    max_bytes: u64,
    dry_run: bool,
) -> Result<PruneReport, StateError> {
    let now = now_ms();
    let mut report = PruneReport::default();

    let cutoff = now.saturating_sub(crate::util::duration_ms_i64(max_age));
    let mut aged_stored = 0u64;
    for run in db.runs_older_than(cutoff, now)? {
        let dir = run_dir(output_root, &run);
        if run.output_pruned_ms.is_none() {
            aged_stored += run.stdout.bytes_stored + run.stderr.bytes_stored;
        }
        if !dry_run {
            report.bytes_freed += remove_dir_size(&dir);
            db.delete_run(&run.run_id)?;
        } else {
            report.bytes_freed += dir_size(&dir);
        }
        report.aged_runs.push(run.run_id);
    }

    let mut total = db.total_stored_output_bytes()?;
    if dry_run {
        // Age-pruned runs would already be gone when the byte cap applies.
        total = total.saturating_sub(aged_stored);
    }
    if total > max_bytes {
        let aged: std::collections::HashSet<&String> = report.aged_runs.iter().collect();
        for run in db.output_prune_candidates()? {
            if total <= max_bytes {
                break;
            }
            if aged.contains(&run.run_id) {
                continue; // dry-run only: row still present but counted above
            }
            let stored = run.stdout.bytes_stored + run.stderr.bytes_stored;
            let dir = run_dir(output_root, &run);
            if !dry_run {
                report.bytes_freed += remove_dir_size(&dir);
                db.mark_output_pruned(&run.run_id, now)?;
            } else {
                report.bytes_freed += dir_size(&dir);
            }
            total = total.saturating_sub(stored);
            report.output_pruned_runs.push(run.run_id);
        }
    }

    if !dry_run {
        db.prune_empty_digest_cohorts(now)?;
    }

    // Clear out empty per-job directories left behind.
    if !dry_run {
        if let Ok(entries) = std::fs::read_dir(output_root) {
            for e in entries.flatten() {
                let _ = std::fs::remove_dir(e.path()); // fails unless empty — fine
            }
        }
    }
    Ok(report)
}

fn run_dir(output_root: &Path, run: &RunRow) -> PathBuf {
    output_root.join(&run.job_id).join(&run.run_id)
}

fn dir_size(dir: &Path) -> u64 {
    let mut total = 0;
    if let Ok(entries) = std::fs::read_dir(dir) {
        for e in entries.flatten() {
            if let Ok(md) = e.metadata() {
                if md.is_file() {
                    total += md.len();
                }
            }
        }
    }
    total
}

fn remove_dir_size(dir: &Path) -> u64 {
    let size = dir_size(dir);
    let _ = std::fs::remove_dir_all(dir);
    size
}

fn nonnegative_u64(value: i64) -> u64 {
    value.max(0) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_db() -> (tempfile::TempDir, Db) {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open(&dir.path().join("uatu.db")).unwrap();
        (dir, db)
    }

    fn mk_run(run_id: &str, job: &str, status: &str, start_ms: i64) -> RunRow {
        RunRow {
            run_id: run_id.into(),
            job_id: job.into(),
            job_id_inferred: false,
            inferred_basename: None,
            mode: "direct".into(),
            argv_json: Some("[\"true\"]".into()),
            shell_cmd: None,
            cwd: None,
            env_names_json: None,
            host: "h".into(),
            schedule_label: None,
            status: status.into(),
            start_ms,
            end_ms: None,
            end_is_detection: false,
            exit_code: None,
            signal_no: None,
            timeout_fired: false,
            interrupted_by: None,
            start_error: None,
            wrapper_pid: 1,
            wrapper_start_ticks: 1,
            boot_id: "b".into(),
            child_pid: None,
            expected_duration_ms: None,
            long_run_fired: false,
            detached_children: false,
            stdout: CaptureMeta::default(),
            stderr: CaptureMeta::default(),
            output_pruned_ms: None,
        }
    }

    #[test]
    fn schema_version_and_wal() {
        let (_d, db) = test_db();
        let v: i64 = db
            .conn
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v, SCHEMA_VERSION);
        let mode: String = db
            .conn
            .query_row("PRAGMA journal_mode", [], |r| r.get(0))
            .unwrap();
        assert_eq!(mode.to_lowercase(), "wal");
        let timeout: i64 = db
            .conn
            .query_row("PRAGMA busy_timeout", [], |r| r.get(0))
            .unwrap();
        assert_eq!(timeout, 5000);
    }

    #[test]
    fn bounded_open_times_out_without_partially_migrating() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("uatu.db");
        {
            let db = Db::open(&path).unwrap();
            db.conn
                .execute_batch(
                    r#"
DROP INDEX idx_deliv_cohort_state;
DROP INDEX idx_digest_cohort_due;
DROP TABLE digest_cohorts;
ALTER TABLE deliveries DROP COLUMN digest_cohort_id;
CREATE INDEX idx_deliv_digest ON deliveries(state, event, reporter, digest_period, digest_start_ms, digest_end_ms, next_attempt_ms);
PRAGMA user_version=3;
"#,
                )
                .unwrap();
        }

        let blocker = Connection::open(&path).unwrap();
        blocker.execute_batch("BEGIN IMMEDIATE;").unwrap();
        let started = std::time::Instant::now();
        let error = match Db::open_bounded(&path, std::time::Duration::from_millis(50)) {
            Ok(_) => panic!("bounded open unexpectedly acquired the migration lock"),
            Err(error) => error,
        };
        assert!(
            error.to_string().contains("deadline exceeded"),
            "unexpected bounded-open error: {error}"
        );
        assert!(started.elapsed() < std::time::Duration::from_secs(1));
        blocker.execute_batch("ROLLBACK;").unwrap();

        let inspect = Connection::open(&path).unwrap();
        let version: i64 = inspect
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(version, 3);
        let cohort_column: i64 = inspect
            .query_row(
                r#"SELECT COUNT(*) FROM pragma_table_info('deliveries')
WHERE name='digest_cohort_id'"#,
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(cohort_column, 0, "timed-out migration must roll back");
        drop(inspect);

        let db = Db::open_bounded(&path, std::time::Duration::from_secs(1)).unwrap();
        let version: i64 = db
            .conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);
        let timeout: i64 = db
            .conn
            .query_row("PRAGMA busy_timeout", [], |row| row.get(0))
            .unwrap();
        assert_eq!(timeout, 5_000);
    }

    #[test]
    fn migrates_v1_delivery_rows_for_digest_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("uatu.db");
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(
                r#"
CREATE TABLE deliveries (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL,
  job_id TEXT NOT NULL,
  event TEXT NOT NULL,
  reporter TEXT NOT NULL,
  state TEXT NOT NULL,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  created_ms INTEGER NOT NULL,
  next_attempt_ms INTEGER,
  delivered_ms INTEGER,
  last_error TEXT,
  owner_pid INTEGER,
  owner_start_ticks INTEGER,
  owner_boot_id TEXT
);
INSERT INTO deliveries
  (run_id, job_id, event, reporter, state, created_ms, next_attempt_ms)
VALUES ('R1', 'job-a', 'failure', 'discord.main', 'queued', 10, 20);
PRAGMA user_version = 1;
"#,
            )
            .unwrap();
        }

        let db = Db::open(&path).unwrap();
        let v: i64 = db
            .conn
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v, SCHEMA_VERSION);

        let mut stmt = db.conn.prepare("PRAGMA table_info(deliveries)").unwrap();
        let columns: Vec<String> = stmt
            .query_map([], |r| r.get::<_, String>(1))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert!(columns.contains(&"digest_period".to_string()));
        assert!(columns.contains(&"digest_start_ms".to_string()));
        assert!(columns.contains(&"digest_end_ms".to_string()));
        assert!(columns.contains(&"digest_cohort_id".to_string()));
        let queued = db.get_delivery(1).unwrap().unwrap();
        assert_eq!(queued.run_id, "R1");
        assert_eq!(queued.state, "queued");
        assert_eq!(queued.next_attempt_ms, Some(20));
        assert_eq!(queued.digest_period, None);
        assert_eq!(queued.digest_cohort_id, None);
    }

    #[test]
    fn migrates_v2_digest_rows_and_replaces_per_job_index() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("uatu.db");
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(
                r#"
CREATE TABLE deliveries (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL,
  job_id TEXT NOT NULL,
  event TEXT NOT NULL,
  reporter TEXT NOT NULL,
  state TEXT NOT NULL,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  created_ms INTEGER NOT NULL,
  next_attempt_ms INTEGER,
  delivered_ms INTEGER,
  last_error TEXT,
  owner_pid INTEGER,
  owner_start_ticks INTEGER,
  owner_boot_id TEXT,
  digest_period TEXT,
  digest_start_ms INTEGER,
  digest_end_ms INTEGER
);
CREATE INDEX idx_deliv_digest ON deliveries
  (state, event, reporter, job_id, digest_period, digest_start_ms, digest_end_ms);
INSERT INTO deliveries
  (run_id, job_id, event, reporter, state, attempt_count, created_ms, next_attempt_ms,
   digest_period, digest_start_ms, digest_end_ms)
VALUES ('R2', 'job-b', 'digest', 'smtp.main', 'queued', 2, 100, 200,
        'monthly', 50, 300);
INSERT INTO deliveries
  (run_id, job_id, event, reporter, state, attempt_count, created_ms,
   next_attempt_ms, delivered_ms, digest_period, digest_start_ms, digest_end_ms)
VALUES ('R3', 'job-c', 'digest', 'smtp.main', 'delivered', 9, 90,
        900, 150, 'monthly', 50, 300);
PRAGMA user_version = 2;
"#,
            )
            .unwrap();
        }

        let db = Db::open(&path).unwrap();
        let version: i64 = db
            .conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(version, 4);
        let queued = db.get_delivery(1).unwrap().unwrap();
        assert_eq!(queued.run_id, "R2");
        assert_eq!(queued.digest_period.as_deref(), Some("monthly"));
        assert_eq!(queued.digest_start_ms, Some(50));
        assert_eq!(queued.digest_end_ms, Some(300));
        let cohort = db
            .get_digest_cohort(queued.digest_cohort_id.unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(cohort.host, "", "minimal legacy DB has no run host");
        assert_eq!(cohort.state, "queued");
        assert_eq!(cohort.member_count, 1);
        assert_eq!(cohort.attempt_count, 2);
        assert_eq!(cohort.next_attempt_ms, Some(200));
        assert_eq!(cohort.delivered_ms, None);
        assert_eq!(db.get_delivery(2).unwrap().unwrap().state, "delivered");
    }

    #[test]
    fn migrates_v3_cohorts_by_recorded_host_and_group_retry_max() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("uatu.db");
        {
            let db = Db::open(&path).unwrap();
            for (id, job, host) in [
                ("R1", "job-a", "host-a"),
                ("R2", "job-b", "host-a"),
                ("R3", "job-c", "host-b"),
            ] {
                let mut run = mk_run(id, job, "success", 10);
                run.host = host.into();
                db.insert_run(&run).unwrap();
            }
            for (run, job, next) in [
                ("R1", "job-a", 100),
                ("R2", "job-b", 300),
                ("R3", "job-c", 200),
            ] {
                db.conn
                    .execute(
                        r#"INSERT INTO deliveries (
run_id, job_id, event, reporter, state, created_ms, next_attempt_ms,
digest_period, digest_start_ms, digest_end_ms
) VALUES (?1,?2,'digest','discord.main','queued',10,?3,'daily',0,1000)"#,
                        params![run, job, next],
                    )
                    .unwrap();
            }
            db.conn.execute_batch(
                r#"
DROP INDEX idx_deliv_cohort_state;
DROP INDEX idx_digest_cohort_due;
DROP TABLE digest_cohorts;
ALTER TABLE deliveries DROP COLUMN digest_cohort_id;
CREATE INDEX idx_deliv_digest ON deliveries(state, event, reporter, digest_period, digest_start_ms, digest_end_ms, next_attempt_ms);
PRAGMA user_version=3;
"#,
            ).unwrap();
        }

        let db = Db::open(&path).unwrap();
        let cohorts = db.due_digest_cohorts(1_000).unwrap();
        assert_eq!(cohorts.len(), 2, "recorded hosts form distinct cohorts");
        let host_a = cohorts
            .iter()
            .find(|cohort| cohort.host == "host-a")
            .unwrap();
        assert_eq!(host_a.next_attempt_ms, Some(300));
        assert_eq!(host_a.member_count, 2);
        let host_a_members: i64 = db
            .conn
            .query_row(
                "SELECT COUNT(*) FROM deliveries WHERE digest_cohort_id=?1",
                params![host_a.id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(host_a_members, 2);
    }

    #[test]
    fn newer_schema_refused() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("uatu.db");
        {
            let db = Db::open(&path).unwrap();
            db.conn.pragma_update(None, "user_version", 99).unwrap();
        }
        match Db::open(&path) {
            Err(StateError::NewerSchema(99)) => {}
            Err(other) => panic!("expected NewerSchema, got {other:?}"),
            Ok(_) => panic!("expected NewerSchema, got Ok"),
        }
    }

    #[test]
    fn prefix_resolution() {
        let (_d, db) = test_db();
        db.insert_run(&mk_run("01ABCDEF11111111111111111X", "j", "success", 10))
            .unwrap();
        db.insert_run(&mk_run("01ABXYZF11111111111111111Y", "j", "success", 20))
            .unwrap();
        assert_eq!(
            db.resolve_run_prefix("01ABC").unwrap().unwrap(),
            "01ABCDEF11111111111111111X"
        );
        // lowercase prefix accepted
        assert_eq!(
            db.resolve_run_prefix("01abc").unwrap().unwrap(),
            "01ABCDEF11111111111111111X"
        );
        // ambiguous lists candidates
        let cands = db.resolve_run_prefix("01AB").unwrap().unwrap_err();
        assert_eq!(cands.len(), 2);
        // not found
        assert!(db
            .resolve_run_prefix("9999")
            .unwrap()
            .unwrap_err()
            .is_empty());
    }

    #[test]
    fn recovery_terminal_lookup_ordered_by_start() {
        let (_d, db) = test_db();
        db.insert_run(&mk_run("R1", "j", "failure", 100)).unwrap();
        db.insert_run(&mk_run("R2", "j", "success", 200)).unwrap();
        db.insert_run(&mk_run("R3", "j", "active", 300)).unwrap(); // ignored: not terminal
        db.insert_run(&mk_run("R4", "j", "timeout", 400)).unwrap();
        let s = db.last_terminal_status_before("j", 500, "R5").unwrap();
        assert_eq!(s.as_deref(), Some("timeout"));
        let s = db.last_terminal_status_before("j", 250, "RX").unwrap();
        assert_eq!(s.as_deref(), Some("success"));
        let s = db.last_terminal_status_before("other", 500, "RX").unwrap();
        assert_eq!(s, None);
    }

    #[test]
    fn retention_by_age_and_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open(&dir.path().join("uatu.db")).unwrap();
        let output = dir.path().join("output");
        let now = now_ms();
        let day = 86_400_000i64;

        // Old run (40 days): aged out entirely.
        let mut old = mk_run("ROLD", "j", "success", now - 40 * day);
        old.stdout.bytes_stored = 10;
        db.insert_run(&old).unwrap();
        db.conn
            .execute(
                "UPDATE runs SET stdout_bytes_stored=10 WHERE run_id='ROLD'",
                [],
            )
            .unwrap();
        db.insert_delivery(
            "ROLD",
            "j",
            "success",
            "discord.d",
            "delivered",
            now,
            None,
            None,
        )
        .unwrap();
        let d = output.join("j").join("ROLD");
        std::fs::create_dir_all(&d).unwrap();
        std::fs::write(d.join("stdout.log"), b"0123456789").unwrap();

        // A run just over 30 days old remains metadata-protected while its
        // monthly digest is queued. Byte-cap pruning may still remove output.
        let mut monthly = mk_run("RMONTH", "monthly-job", "success", now - 31 * day);
        monthly.stdout.bytes_stored = 10;
        db.insert_run(&monthly).unwrap();
        db.conn
            .execute(
                "UPDATE runs SET stdout_bytes_stored=10 WHERE run_id='RMONTH'",
                [],
            )
            .unwrap();
        db.insert_digest_delivery(
            "RMONTH",
            "monthly-job",
            "digest",
            "discord.d",
            "queued",
            now - 31 * day,
            Some(now + day),
            None,
            &DeliveryDigest {
                period: "monthly".into(),
                start_ms: now - 31 * day,
                end_ms: now + day,
            },
        )
        .unwrap();
        let d = output.join("monthly-job").join("RMONTH");
        std::fs::create_dir_all(&d).unwrap();
        std::fs::write(d.join("stdout.log"), b"0123456789").unwrap();

        // Protection is bounded: an otherwise queued digest whose window
        // ended more than seven days ago no longer keeps the run row alive.
        db.insert_run(&mk_run(
            "REXPIRED",
            "monthly-job",
            "failure",
            now - 39 * day,
        ))
        .unwrap();
        let expired_delivery = db
            .insert_digest_delivery(
                "REXPIRED",
                "monthly-job",
                "digest",
                "discord.d",
                "queued",
                now - 39 * day,
                Some(now - 8 * day),
                None,
                &DeliveryDigest {
                    period: "monthly".into(),
                    start_ms: now - 39 * day,
                    end_ms: now - 8 * day,
                },
            )
            .unwrap();
        let expired_cohort = db
            .get_delivery(expired_delivery)
            .unwrap()
            .unwrap()
            .digest_cohort_id
            .unwrap();

        // Two recent runs with output; byte cap forces oldest-first pruning.
        for (id, start, bytes) in [("RA", now - 2 * day, 600u64), ("RB", now - day, 600u64)] {
            db.insert_run(&mk_run(id, "j", "success", start)).unwrap();
            db.conn
                .execute(
                    "UPDATE runs SET stdout_bytes_stored=?2 WHERE run_id=?1",
                    params![id, bytes as i64],
                )
                .unwrap();
            let d = output.join("j").join(id);
            std::fs::create_dir_all(&d).unwrap();
            std::fs::write(d.join("stdout.log"), vec![b'x'; bytes as usize]).unwrap();
        }

        // Dry run first: nothing changes.
        let dry = prune(
            &db,
            &output,
            std::time::Duration::from_secs(30 * 86400),
            1000,
            true,
        )
        .unwrap();
        assert_eq!(dry.aged_runs, vec!["ROLD", "REXPIRED"]);
        assert_eq!(dry.output_pruned_runs, vec!["RMONTH", "RA"]);
        assert!(
            db.get_run("ROLD").unwrap().is_some(),
            "dry-run must not delete rows"
        );
        assert!(output.join("j").join("ROLD").exists());
        assert!(db.get_run("RMONTH").unwrap().is_some());
        assert!(output.join("monthly-job").join("RMONTH").exists());

        let report = prune(
            &db,
            &output,
            std::time::Duration::from_secs(30 * 86400),
            1000,
            false,
        )
        .unwrap();
        assert_eq!(report.aged_runs, vec!["ROLD", "REXPIRED"]);
        assert_eq!(report.output_pruned_runs, vec!["RMONTH", "RA"]);
        // ROLD fully gone (row + deliveries + files).
        assert!(db.get_run("ROLD").unwrap().is_none());
        assert!(db.deliveries_for_run("ROLD").unwrap().is_empty());
        assert!(!output.join("j").join("ROLD").exists());
        assert!(db.get_run("REXPIRED").unwrap().is_none());
        assert!(
            db.get_digest_cohort(expired_cohort).unwrap().is_none(),
            "empty non-sending cohort tombstones expire with their retry horizon"
        );
        // Monthly digest membership survives max-age retention, while its
        // output remains eligible for the independent byte cap.
        let monthly = db.get_run("RMONTH").unwrap().unwrap();
        assert!(monthly.output_pruned_ms.is_some());
        assert_eq!(db.deliveries_for_run("RMONTH").unwrap()[0].state, "queued");
        assert!(!output.join("monthly-job").join("RMONTH").exists());
        // RA output gone but metadata kept + stamped.
        let ra = db.get_run("RA").unwrap().unwrap();
        assert!(ra.output_pruned_ms.is_some());
        assert!(!output.join("j").join("RA").exists());
        // RB untouched.
        assert!(output.join("j").join("RB").join("stdout.log").exists());
    }

    #[test]
    fn digest_tombstone_pruning_keeps_recent_and_sending_cohorts() {
        let (_d, db) = test_db();
        let now = now_ms();
        let day = 86_400_000;
        let owner = crate::liveness::Liveness {
            pid: 10,
            start_ticks: 20,
            boot_id: "boot-a".into(),
        };

        let mut cohort_ids = Vec::new();
        for (run_id, end_ms) in [("ROLD", now - 8 * day), ("RRECENT", now - 6 * day)] {
            db.insert_run(&mk_run(run_id, "job", "success", end_ms - day))
                .unwrap();
            let delivery_id = db
                .insert_digest_delivery(
                    run_id,
                    "job",
                    "digest",
                    "discord.main",
                    "queued",
                    end_ms - day,
                    Some(end_ms),
                    None,
                    &DeliveryDigest {
                        period: "daily".into(),
                        start_ms: end_ms - day,
                        end_ms,
                    },
                )
                .unwrap();
            cohort_ids.push(
                db.get_delivery(delivery_id)
                    .unwrap()
                    .unwrap()
                    .digest_cohort_id
                    .unwrap(),
            );
            db.delete_run(run_id).unwrap();
        }

        db.insert_run(&mk_run("RSENDING", "job", "failure", now - 9 * day))
            .unwrap();
        let sending_delivery = db
            .insert_digest_delivery(
                "RSENDING",
                "job",
                "digest",
                "discord.sending",
                "queued",
                now - 9 * day,
                Some(now - 8 * day),
                None,
                &DeliveryDigest {
                    period: "daily".into(),
                    start_ms: now - 9 * day,
                    end_ms: now - 8 * day,
                },
            )
            .unwrap();
        let sending_cohort = db
            .get_delivery(sending_delivery)
            .unwrap()
            .unwrap()
            .digest_cohort_id
            .unwrap();
        db.claim_digest_cohort(sending_cohort, &owner, now)
            .unwrap()
            .unwrap();
        db.delete_run("RSENDING").unwrap();

        assert_eq!(db.prune_empty_digest_cohorts(now).unwrap(), 1);
        assert!(db.get_digest_cohort(cohort_ids[0]).unwrap().is_none());
        assert!(db.get_digest_cohort(cohort_ids[1]).unwrap().is_some());
        assert!(db.get_digest_cohort(sending_cohort).unwrap().is_some());
    }

    #[test]
    fn limited_due_digest_query_filters_oversized_before_limit() {
        let (_d, db) = test_db();
        db.insert_run(&mk_run("RBIG", "job-big", "success", 10))
            .unwrap();
        db.insert_run(&mk_run("RSMALL", "job-small", "success", 20))
            .unwrap();
        let digest = DeliveryDigest {
            period: "daily".into(),
            start_ms: 0,
            end_ms: 1_000,
        };
        let mut cohorts = Vec::new();
        for (run_id, job_id, reporter, due) in [
            ("RBIG", "job-big", "discord.big", 100),
            ("RSMALL", "job-small", "discord.small", 200),
        ] {
            let delivery_id = db
                .insert_digest_delivery(
                    run_id,
                    job_id,
                    "digest",
                    reporter,
                    "queued",
                    10,
                    Some(due),
                    None,
                    &digest,
                )
                .unwrap();
            cohorts.push(
                db.get_delivery(delivery_id)
                    .unwrap()
                    .unwrap()
                    .digest_cohort_id
                    .unwrap(),
            );
        }
        db.conn
            .execute(
                "UPDATE digest_cohorts SET member_count=2049 WHERE id=?1",
                params![cohorts[0]],
            )
            .unwrap();

        let exhaustive = db.due_digest_cohorts(1_000).unwrap();
        assert_eq!(
            exhaustive
                .iter()
                .map(|cohort| cohort.id)
                .collect::<Vec<_>>(),
            cohorts
        );
        let opportunistic = db.due_digest_cohorts_limited(1_000, 1, 2_048).unwrap();
        assert_eq!(opportunistic.len(), 1);
        assert_eq!(opportunistic[0].id, cohorts[1]);
    }

    #[test]
    fn delivery_state_machine() {
        let (_d, db) = test_db();
        db.insert_run(&mk_run("R1", "j", "failure", 100)).unwrap();
        let owner = crate::liveness::Liveness {
            pid: 1,
            start_ticks: 2,
            boot_id: "b".into(),
        };
        let id = db
            .insert_delivery(
                "R1",
                "j",
                "failure",
                "discord.d",
                "queued",
                100,
                Some(100),
                None,
            )
            .unwrap();
        assert!(db.claim_delivery(id, &owner).unwrap());
        assert!(!db.claim_delivery(id, &owner).unwrap(), "already sending");
        db.delivery_queued(id, 500, "boom").unwrap();
        let row = db.get_delivery(id).unwrap().unwrap();
        assert_eq!(row.state, "queued");
        assert_eq!(row.attempt_count, 1);
        assert_eq!(row.next_attempt_ms, Some(500));
        assert!(db.claim_delivery(id, &owner).unwrap());
        db.delivery_delivered(id, 600).unwrap();
        let row = db.get_delivery(id).unwrap().unwrap();
        assert_eq!(row.state, "delivered");
        assert_eq!(row.delivered_ms, Some(600));
    }

    #[test]
    fn concurrent_digest_memberships_share_one_cohort_without_loss() {
        const WRAPPERS: usize = 16;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("uatu.db");
        {
            let db = Db::open(&path).unwrap();
            for index in 0..WRAPPERS {
                let run_id = format!("R{index:02}");
                db.insert_run(&mk_run(&run_id, &format!("job-{index:02}"), "success", 10))
                    .unwrap();
            }
        }

        let barrier = std::sync::Arc::new(std::sync::Barrier::new(WRAPPERS));
        let mut handles = Vec::new();
        for index in 0..WRAPPERS {
            let path = path.clone();
            let barrier = std::sync::Arc::clone(&barrier);
            handles.push(std::thread::spawn(move || {
                let db = Db::open(&path).unwrap();
                barrier.wait();
                db.insert_digest_delivery(
                    &format!("R{index:02}"),
                    &format!("job-{index:02}"),
                    "digest",
                    "discord.main",
                    "queued",
                    10,
                    Some(1_000),
                    None,
                    &DeliveryDigest {
                        period: "daily".into(),
                        start_ms: 0,
                        end_ms: 1_000,
                    },
                )
            }));
        }
        for handle in handles {
            handle.join().unwrap().unwrap();
        }

        let db = Db::open(&path).unwrap();
        let cohorts = db.due_digest_cohorts(1_000).unwrap();
        assert_eq!(cohorts.len(), 1);
        assert_eq!(cohorts[0].member_count, WRAPPERS as i64);
        let membership_rows: i64 = db
            .conn
            .query_row(
                "SELECT COUNT(*) FROM deliveries WHERE digest_cohort_id=?1",
                params![cohorts[0].id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(membership_rows, WRAPPERS as i64);
    }

    #[test]
    fn batch_digest_insert_separates_reporters_and_preserves_tombstones() {
        let (_d, db) = test_db();
        db.insert_run(&mk_run("R1", "job-a", "success", 100))
            .unwrap();
        let reporters = vec!["discord.main".to_string(), "smtp.main".to_string()];
        let digest = DeliveryDigest {
            period: "daily".into(),
            start_ms: 0,
            end_ms: 1_000,
        };
        let ids = db
            .insert_digest_deliveries(
                "R1",
                "job-a",
                "digest",
                &reporters,
                100,
                Some(1_000),
                &digest,
            )
            .unwrap();
        assert_eq!(ids.len(), 2);
        let first = db.get_delivery(ids[0]).unwrap().unwrap();
        let second = db.get_delivery(ids[1]).unwrap().unwrap();
        assert_eq!(first.reporter, reporters[0]);
        assert_eq!(second.reporter, reporters[1]);
        assert_ne!(first.digest_cohort_id, second.digest_cohort_id);
        for cohort_id in [
            first.digest_cohort_id.unwrap(),
            second.digest_cohort_id.unwrap(),
        ] {
            let cohort = db.get_digest_cohort(cohort_id).unwrap().unwrap();
            assert_eq!(cohort.state, "queued");
            assert_eq!(cohort.host, "h");
            assert_eq!(cohort.member_count, 1);
        }

        let owner = crate::liveness::Liveness {
            pid: 10,
            start_ticks: 20,
            boot_id: "boot-a".into(),
        };
        let discord_cohort = first.digest_cohort_id.unwrap();
        let claim = db
            .claim_digest_cohort(discord_cohort, &owner, 1_000)
            .unwrap()
            .unwrap();
        assert_eq!(db.digest_group_delivered(&claim, &owner, 1_100).unwrap(), 1);

        db.insert_run(&mk_run("R2", "job-b", "failure", 200))
            .unwrap();
        let late_ids = db
            .insert_digest_deliveries(
                "R2",
                "job-b",
                "digest",
                &reporters,
                200,
                Some(1_000),
                &digest,
            )
            .unwrap();
        assert_eq!(late_ids.len(), 2);
        let suppressed = db.get_delivery(late_ids[0]).unwrap().unwrap();
        assert_eq!(suppressed.state, "expired");
        assert!(suppressed.last_error.unwrap().contains("already delivered"));
        assert_eq!(suppressed.digest_cohort_id, Some(discord_cohort));
        assert_eq!(
            db.get_digest_cohort(discord_cohort)
                .unwrap()
                .unwrap()
                .member_count,
            1,
            "terminal-suppressed membership is real but not accepted"
        );

        let accepted = db.get_delivery(late_ids[1]).unwrap().unwrap();
        assert_eq!(accepted.state, "queued");
        let smtp_cohort = accepted.digest_cohort_id.unwrap();
        assert_eq!(smtp_cohort, second.digest_cohort_id.unwrap());
        assert_eq!(
            db.get_digest_cohort(smtp_cohort)
                .unwrap()
                .unwrap()
                .member_count,
            2
        );
    }

    #[test]
    fn bounded_digest_batch_rolls_back_members_staged_before_deadline() {
        let (_d, db) = test_db();
        db.insert_run(&mk_run("R1", "job-a", "success", 100))
            .unwrap();
        let digest = DeliveryDigest {
            period: "daily".into(),
            start_ms: 0,
            end_ms: 1_000,
        };
        let second_reporter_requested =
            std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let yielded = std::sync::Arc::clone(&second_reporter_requested);
        let reporters = std::iter::once("discord.main").chain(std::iter::once_with(move || {
            yielded.store(true, std::sync::atomic::Ordering::SeqCst);
            std::thread::sleep(std::time::Duration::from_millis(550));
            "smtp.main"
        }));
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(500);
        let error = db
            .insert_digest_deliveries_inner(
                "R1",
                "job-a",
                "digest",
                reporters,
                100,
                Some(1_000),
                &digest,
                Some(deadline),
            )
            .unwrap_err();
        db.conn
            .busy_timeout(std::time::Duration::from_millis(5_000))
            .unwrap();
        assert!(second_reporter_requested.load(std::sync::atomic::Ordering::SeqCst));
        assert!(error.to_string().contains("digest queue deadline exceeded"));
        let memberships: i64 = db
            .conn
            .query_row("SELECT COUNT(*) FROM deliveries", [], |row| row.get(0))
            .unwrap();
        let cohorts: i64 = db
            .conn
            .query_row("SELECT COUNT(*) FROM digest_cohorts", [], |row| row.get(0))
            .unwrap();
        assert_eq!(memberships, 0);
        assert_eq!(cohorts, 0);
    }

    #[test]
    fn bounded_digest_batch_preserves_connection_timeout_and_result_order() {
        let (_d, db) = test_db();
        db.insert_run(&mk_run("R1", "job-a", "success", 100))
            .unwrap();
        db.conn
            .busy_timeout(std::time::Duration::from_millis(1_234))
            .unwrap();
        let reporters = vec!["discord.main".to_string(), "smtp.main".to_string()];
        let ids = db
            .insert_digest_deliveries_bounded(
                "R1",
                "job-a",
                "digest",
                &reporters,
                100,
                Some(1_000),
                &DeliveryDigest {
                    period: "daily".into(),
                    start_ms: 0,
                    end_ms: 1_000,
                },
                std::time::Instant::now() + std::time::Duration::from_secs(1),
            )
            .unwrap();
        assert_eq!(ids.len(), reporters.len());
        assert_eq!(
            db.get_delivery(ids[0]).unwrap().unwrap().reporter,
            reporters[0]
        );
        assert_eq!(
            db.get_delivery(ids[1]).unwrap().unwrap().reporter,
            reporters[1]
        );
        let timeout: i64 = db
            .conn
            .query_row("PRAGMA busy_timeout", [], |row| row.get(0))
            .unwrap();
        assert_eq!(timeout, 1_234);
    }

    #[test]
    fn digest_claim_and_outcomes_are_atomic_across_jobs() {
        let (_d, db) = test_db();
        db.insert_run(&mk_run("R1", "job-a", "success", 100))
            .unwrap();
        db.insert_run(&mk_run("R2", "job-b", "failure", 200))
            .unwrap();
        let mut other_host = mk_run("R3", "job-c", "timeout", 300);
        other_host.host = "other-host".into();
        db.insert_run(&other_host).unwrap();
        let digest = DeliveryDigest {
            period: "daily".into(),
            start_ms: 0,
            end_ms: 1_000,
        };
        let mut ids = Vec::new();
        for (run, job, next) in [
            ("R1", "job-a", 500),
            ("R2", "job-b", 800),
            ("R3", "job-c", 900),
        ] {
            ids.push(
                db.insert_digest_delivery(
                    run,
                    job,
                    "digest",
                    "discord.main",
                    "queued",
                    100,
                    Some(next),
                    None,
                    &digest,
                )
                .unwrap(),
            );
        }
        let owner = crate::liveness::Liveness {
            pid: 10,
            start_ticks: 20,
            boot_id: "boot-a".into(),
        };
        let other_owner = crate::liveness::Liveness {
            pid: 11,
            start_ticks: 21,
            boot_id: "boot-a".into(),
        };

        let cohort_id = db
            .get_delivery(ids[0])
            .unwrap()
            .unwrap()
            .digest_cohort_id
            .unwrap();
        assert_eq!(
            db.get_delivery(ids[1]).unwrap().unwrap().digest_cohort_id,
            Some(cohort_id)
        );
        assert_ne!(
            db.get_delivery(ids[2]).unwrap().unwrap().digest_cohort_id,
            Some(cohort_id),
            "recorded host is part of cohort identity"
        );
        // Force the cohort gate due while one legacy member still carries a
        // future row timestamp. Claiming must take the whole current cohort.
        db.conn
            .execute(
                "UPDATE digest_cohorts SET next_attempt_ms=500 WHERE id=?1",
                params![cohort_id],
            )
            .unwrap();
        let claim = db
            .claim_digest_cohort(cohort_id, &owner, 500)
            .unwrap()
            .unwrap();
        assert_eq!(claim.member_count, 2);
        assert_eq!(
            db.get_delivery(ids[1]).unwrap().unwrap().state,
            "sending",
            "member-level retry time cannot split a due cohort"
        );
        let aggregate = db.load_digest_aggregate(&claim, &owner).unwrap();
        assert_eq!(aggregate.total_jobs, 2);
        assert_eq!(aggregate.total_executions, 2);
        assert_eq!(aggregate.statuses.success, 1);
        assert_eq!(aggregate.statuses.failure, 1);
        assert_eq!(
            db.digest_group_delivered(&claim, &other_owner, 600)
                .unwrap(),
            0,
            "a different owner cannot finalize the group"
        );

        // A member arriving after the snapshot waits queued. A retry folds it
        // into the next attempt instead of creating another cohort.
        db.insert_run(&mk_run("R4", "job-d", "success", 400))
            .unwrap();
        let late_id = db
            .insert_digest_delivery(
                "R4",
                "job-d",
                "digest",
                "discord.main",
                "queued",
                400,
                Some(500),
                None,
                &digest,
            )
            .unwrap();
        assert_eq!(db.get_delivery(late_id).unwrap().unwrap().state, "queued");
        assert_eq!(
            db.get_digest_cohort(cohort_id)
                .unwrap()
                .unwrap()
                .member_count,
            3
        );
        assert_eq!(
            db.digest_group_queued(&claim, &owner, 900, "temporary")
                .unwrap(),
            3
        );
        let retry = db
            .claim_digest_cohort(cohort_id, &owner, 900)
            .unwrap()
            .unwrap();
        assert_eq!(retry.member_count, 3);
        let aggregate = db.load_digest_aggregate(&retry, &owner).unwrap();
        assert_eq!(aggregate.total_executions, 3);
        assert_eq!(db.digest_group_delivered(&retry, &owner, 1_000).unwrap(), 3);

        // Success closes the membership snapshot and expires a member that
        // arrived while the send was in flight.
        let other_cohort_id = db
            .get_delivery(ids[2])
            .unwrap()
            .unwrap()
            .digest_cohort_id
            .unwrap();
        let other_claim = db
            .claim_digest_cohort(other_cohort_id, &owner, 900)
            .unwrap()
            .unwrap();
        assert_eq!(other_claim.member_count, 1);
        let mut other_late = mk_run("R6", "job-f", "success", 450);
        other_late.host = "other-host".into();
        db.insert_run(&other_late).unwrap();
        let other_late_id = db
            .insert_digest_delivery(
                "R6",
                "job-f",
                "digest",
                "discord.main",
                "queued",
                450,
                Some(900),
                None,
                &digest,
            )
            .unwrap();
        assert_eq!(
            db.get_digest_cohort(other_cohort_id)
                .unwrap()
                .unwrap()
                .member_count,
            2
        );
        assert_eq!(
            db.digest_group_delivered(&other_claim, &owner, 1_000)
                .unwrap(),
            1
        );
        let other_late = db.get_delivery(other_late_id).unwrap().unwrap();
        assert_eq!(other_late.state, "expired");
        assert!(other_late
            .last_error
            .unwrap()
            .contains("before late membership"));

        db.insert_run(&mk_run("R5", "job-e", "success", 500))
            .unwrap();
        let suppressed = db
            .insert_digest_delivery(
                "R5",
                "job-e",
                "digest",
                "discord.main",
                "queued",
                500,
                Some(500),
                None,
                &digest,
            )
            .unwrap();
        let suppressed = db.get_delivery(suppressed).unwrap().unwrap();
        assert_eq!(suppressed.state, "expired");
        assert!(suppressed.last_error.unwrap().contains("already delivered"));
        assert_eq!(
            (
                db.get_digest_cohort(cohort_id).unwrap().unwrap().state,
                db.get_digest_cohort(cohort_id)
                    .unwrap()
                    .unwrap()
                    .member_count,
            ),
            ("delivered".to_string(), 3),
            "terminal-suppressed rows do not inflate the accepted count"
        );
    }

    #[test]
    fn digest_aggregate_reads_are_exact_and_bounded() {
        let (_d, db) = test_db();
        let digest = DeliveryDigest {
            period: "daily".into(),
            start_ms: 0,
            end_ms: 1_000,
        };
        let mut cohort_id = None;
        for job_index in 0..70 {
            let job = format!("job-{job_index:03}");
            for run_index in 0..5 {
                let run_id = format!("R{job_index:03}-{run_index:03}");
                let status = if run_index < 3 { "failure" } else { "success" };
                let start_ms = i64::from(job_index * 10 + run_index);
                let mut run = mk_run(&run_id, &job, status, start_ms);
                run.end_ms = Some(run.start_ms + 1_000);
                db.insert_run(&run).unwrap();
                let delivery_id = db
                    .insert_digest_delivery(
                        &run_id,
                        &job,
                        "digest",
                        "discord.main",
                        "queued",
                        10,
                        Some(1_000),
                        None,
                        &digest,
                    )
                    .unwrap();
                cohort_id = db
                    .get_delivery(delivery_id)
                    .unwrap()
                    .unwrap()
                    .digest_cohort_id;
            }
        }
        let owner = crate::liveness::Liveness {
            pid: 10,
            start_ticks: 20,
            boot_id: "boot-a".into(),
        };
        let claim = db
            .claim_digest_cohort(cohort_id.unwrap(), &owner, 1_000)
            .unwrap()
            .unwrap();
        assert_eq!(claim.member_count, 350);
        let aggregate = db.load_digest_aggregate(&claim, &owner).unwrap();
        assert_eq!(aggregate.total_jobs, 70);
        assert_eq!(aggregate.total_executions, 350);
        assert_eq!(aggregate.total_problem_executions, 210);
        assert_eq!(aggregate.total_success_executions, 140);
        assert_eq!(aggregate.statuses.failure, 210);
        assert_eq!(aggregate.statuses.success, 140);
        assert_eq!(aggregate.job_summaries.len(), DIGEST_JOB_SUMMARY_LIMIT);
        assert_eq!(aggregate.problem_details.len(), DIGEST_PROBLEM_DETAIL_LIMIT);
        assert_eq!(aggregate.success_details.len(), DIGEST_SUCCESS_DETAIL_LIMIT);
        let shown: std::collections::HashSet<&str> = aggregate
            .job_summaries
            .iter()
            .map(|job| job.job_id.as_str())
            .collect();
        assert!(aggregate
            .problem_details
            .iter()
            .all(|detail| shown.contains(detail.job_id.as_str())));
        assert_eq!(aggregate.success_details[0].job_id, "job-069");
        assert!(aggregate
            .success_details
            .iter()
            .any(|detail| !shown.contains(detail.job_id.as_str())));
    }

    #[test]
    fn digest_problem_details_use_capacity_beyond_job_summary_limit() {
        let (_d, db) = test_db();
        let digest = DeliveryDigest {
            period: "daily".into(),
            start_ms: 0,
            end_ms: 1_000,
        };
        let mut cohort_id = None;
        for index in 0..70 {
            let job_id = format!("job-{index:03}");
            let run_id = format!("R{index:03}");
            db.insert_run(&mk_run(&run_id, &job_id, "failure", index))
                .unwrap();
            let delivery_id = db
                .insert_digest_delivery(
                    &run_id,
                    &job_id,
                    "digest",
                    "discord.main",
                    "queued",
                    10,
                    Some(1_000),
                    None,
                    &digest,
                )
                .unwrap();
            cohort_id = db
                .get_delivery(delivery_id)
                .unwrap()
                .unwrap()
                .digest_cohort_id;
        }
        let owner = crate::liveness::Liveness {
            pid: 10,
            start_ticks: 20,
            boot_id: "boot-a".into(),
        };
        let claim = db
            .claim_digest_cohort(cohort_id.unwrap(), &owner, 1_000)
            .unwrap()
            .unwrap();
        let aggregate = db.load_digest_aggregate(&claim, &owner).unwrap();
        assert_eq!(aggregate.job_summaries.len(), DIGEST_JOB_SUMMARY_LIMIT);
        assert_eq!(aggregate.problem_details.len(), 70);
        assert_eq!(aggregate.problem_details[0].job_id, "job-000");
        assert_eq!(aggregate.problem_details[69].job_id, "job-069");
    }

    #[test]
    fn digest_orphan_requeue_is_owner_scoped_and_idempotent() {
        let (_d, db) = test_db();
        db.insert_run(&mk_run("R1", "job-a", "failure", 100))
            .unwrap();
        let delivery_id = db
            .insert_digest_delivery(
                "R1",
                "job-a",
                "digest",
                "discord.main",
                "queued",
                100,
                Some(500),
                None,
                &DeliveryDigest {
                    period: "daily".into(),
                    start_ms: 0,
                    end_ms: 500,
                },
            )
            .unwrap();
        let cohort_id = db
            .get_delivery(delivery_id)
            .unwrap()
            .unwrap()
            .digest_cohort_id
            .unwrap();
        let owner = crate::liveness::Liveness {
            pid: 10,
            start_ticks: 20,
            boot_id: "boot-a".into(),
        };
        db.claim_digest_cohort(cohort_id, &owner, 500)
            .unwrap()
            .unwrap();
        assert!(db.sending_deliveries().unwrap().is_empty());
        let sending_cohorts = db.sending_digest_cohorts().unwrap();
        assert_eq!(sending_cohorts.len(), 1);
        let sending = &sending_cohorts[0];
        assert_eq!(db.digest_cohort_requeue_orphan(sending, 600).unwrap(), 1);
        assert_eq!(
            db.digest_cohort_requeue_orphan(sending, 600).unwrap(),
            0,
            "reconcile may encounter several stale member snapshots"
        );
        let cohort = db.get_digest_cohort(cohort_id).unwrap().unwrap();
        assert_eq!(cohort.state, "queued");
        assert_eq!(cohort.next_attempt_ms, Some(600));

        let claim = db
            .claim_digest_cohort(cohort_id, &owner, 600)
            .unwrap()
            .unwrap();
        assert_eq!(
            db.digest_group_expired(&claim, &owner, "permanent")
                .unwrap(),
            1
        );
        assert_eq!(
            db.get_digest_cohort(cohort_id).unwrap().unwrap().state,
            "expired"
        );
    }
}
