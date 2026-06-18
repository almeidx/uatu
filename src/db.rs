//! SQLite state (SPEC §7): run metadata, liveness identity, capture state,
//! delivery rows with their state machine, retention pruning. WAL mode,
//! busy_timeout 5000ms, forward-only migrations via `user_version`.

use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

use rusqlite::{params, Connection, OptionalExtension};

use crate::util::now_ms;

pub const SCHEMA_VERSION: i64 = 2;

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
}

#[derive(Clone, Debug)]
pub struct DeliveryDigest {
    pub period: String,
    pub start_ms: i64,
    pub end_ms: i64,
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

    fn migrate(&self) -> Result<bool, StateError> {
        let v: i64 = self
            .conn
            .query_row("PRAGMA user_version", [], |r| r.get(0))?;
        if v > SCHEMA_VERSION {
            return Err(StateError::NewerSchema(v));
        }
        if v == SCHEMA_VERSION {
            return Ok(false);
        }
        if v == 1 {
            self.conn.execute_batch(
                r#"
BEGIN EXCLUSIVE;
ALTER TABLE deliveries ADD COLUMN digest_period TEXT;
ALTER TABLE deliveries ADD COLUMN digest_start_ms INTEGER;
ALTER TABLE deliveries ADD COLUMN digest_end_ms INTEGER;
CREATE INDEX IF NOT EXISTS idx_deliv_digest ON deliveries(state, event, reporter, job_id, digest_period, digest_start_ms, digest_end_ms);
PRAGMA user_version = 2;
COMMIT;
"#,
            )?;
            return Ok(true);
        }
        // Forward-only migration inside an exclusive transaction (SPEC §7).
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
  digest_end_ms INTEGER
);
CREATE INDEX IF NOT EXISTS idx_deliv_state ON deliveries(state, next_attempt_ms);
CREATE INDEX IF NOT EXISTS idx_deliv_run ON deliveries(run_id);
CREATE INDEX IF NOT EXISTS idx_deliv_digest ON deliveries(state, event, reporter, job_id, digest_period, digest_start_ms, digest_end_ms);
PRAGMA user_version = 2;
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
        state: &str,
        created_ms: i64,
        next_attempt_ms: Option<i64>,
        owner: Option<&crate::liveness::Liveness>,
        digest: &DeliveryDigest,
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
            Some(digest),
        )
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
            "SELECT * FROM deliveries WHERE state='queued' AND next_attempt_ms <= ?1 ORDER BY next_attempt_ms ASC",
        )?;
        let rows = stmt
            .query_map(params![now], Self::row_to_delivery)?
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

    pub fn claim_digest_group(
        &self,
        row: &DeliveryRow,
        owner: &crate::liveness::Liveness,
        now: i64,
    ) -> Result<Vec<DeliveryRow>, StateError> {
        let (Some(period), Some(start), Some(end)) = (
            row.digest_period.as_deref(),
            row.digest_start_ms,
            row.digest_end_ms,
        ) else {
            return Ok(Vec::new());
        };
        let n = self.conn.execute(
            r#"UPDATE deliveries
SET state='sending', owner_pid=?7, owner_start_ticks=?8, owner_boot_id=?9
WHERE state='queued'
  AND event=?1 AND reporter=?2 AND job_id=?3
  AND digest_period=?4 AND digest_start_ms=?5 AND digest_end_ms=?6
  AND next_attempt_ms <= ?10"#,
            params![
                row.event,
                row.reporter,
                row.job_id,
                period,
                start,
                end,
                owner.pid as i64,
                owner.start_ticks as i64,
                owner.boot_id,
                now,
            ],
        )?;
        if n == 0 {
            return Ok(Vec::new());
        }
        let mut stmt = self.conn.prepare(
            r#"SELECT * FROM deliveries
WHERE state='sending'
  AND event=?1 AND reporter=?2 AND job_id=?3
  AND digest_period=?4 AND digest_start_ms=?5 AND digest_end_ms=?6
  AND owner_pid=?7 AND owner_start_ticks=?8 AND owner_boot_id=?9
ORDER BY created_ms ASC, id ASC"#,
        )?;
        let rows = stmt
            .query_map(
                params![
                    row.event,
                    row.reporter,
                    row.job_id,
                    period,
                    start,
                    end,
                    owner.pid as i64,
                    owner.start_ticks as i64,
                    owner.boot_id,
                ],
                Self::row_to_delivery,
            )?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
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
        let mut stmt = self
            .conn
            .prepare("SELECT * FROM deliveries WHERE state='sending'")?;
        let rows = stmt
            .query_map([], Self::row_to_delivery)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    // ----- retention (SPEC §7) -----

    /// Runs whose row (and output, and deliveries) age out entirely.
    pub fn runs_older_than(&self, cutoff_ms: i64) -> Result<Vec<RunRow>, StateError> {
        let mut stmt = self.conn.prepare(
            "SELECT * FROM runs WHERE status != 'active' AND start_ms < ?1 ORDER BY start_ms ASC",
        )?;
        let rows = stmt
            .query_map(params![cutoff_ms], Self::row_to_run)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    pub fn delete_run(&self, run_id: &str) -> Result<(), StateError> {
        self.conn
            .execute("DELETE FROM deliveries WHERE run_id=?1", params![run_id])?;
        self.conn
            .execute("DELETE FROM runs WHERE run_id=?1", params![run_id])?;
        Ok(())
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
    for run in db.runs_older_than(cutoff)? {
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
            "queued",
            now,
            Some(now),
            None,
        )
        .unwrap();
        let d = output.join("j").join("ROLD");
        std::fs::create_dir_all(&d).unwrap();
        std::fs::write(d.join("stdout.log"), b"0123456789").unwrap();

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
        assert_eq!(dry.aged_runs, vec!["ROLD"]);
        assert_eq!(dry.output_pruned_runs, vec!["RA"]);
        assert!(
            db.get_run("ROLD").unwrap().is_some(),
            "dry-run must not delete rows"
        );
        assert!(output.join("j").join("ROLD").exists());

        let report = prune(
            &db,
            &output,
            std::time::Duration::from_secs(30 * 86400),
            1000,
            false,
        )
        .unwrap();
        assert_eq!(report.aged_runs, vec!["ROLD"]);
        assert_eq!(report.output_pruned_runs, vec!["RA"]);
        // ROLD fully gone (row + deliveries + files).
        assert!(db.get_run("ROLD").unwrap().is_none());
        assert!(db.deliveries_for_run("ROLD").unwrap().is_empty());
        assert!(!output.join("j").join("ROLD").exists());
        // RA output gone but metadata kept + stamped.
        let ra = db.get_run("RA").unwrap().unwrap();
        assert!(ra.output_pruned_ms.is_some());
        assert!(!output.join("j").join("RA").exists());
        // RB untouched.
        assert!(output.join("j").join("RB").join("stdout.log").exists());
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
}
