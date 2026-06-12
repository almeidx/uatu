//! `uatu history` / `show` / `status` (SPEC §3): reconcile (enqueue-only,
//! never send), then render human or JSON output (contract in SPEC §11).

use std::path::PathBuf;

use serde_json::json;

use crate::commands::open_for_inspection;
use crate::db::{DeliveryRow, RunRow};
use crate::reconcile;
use crate::util::{format_duration_ms, now_ms, rfc3339};

pub struct HistoryArgs {
    pub config: Option<PathBuf>,
    pub data_dir: Option<PathBuf>,
    pub limit: usize,
    pub job: Option<String>,
    pub status: Option<String>,
    pub json: bool,
}

pub struct ShowArgs {
    pub config: Option<PathBuf>,
    pub data_dir: Option<PathBuf>,
    pub run_id: String,
    pub stdout: bool,
    pub stderr: bool,
    pub json: bool,
}

pub struct StatusArgs {
    pub config: Option<PathBuf>,
    pub data_dir: Option<PathBuf>,
    pub json: bool,
}

/// JSON encoding (SPEC §11): RFC3339 UTC timestamps, integer-ms durations,
/// snake_case enums, integers for byte counts, null for absent — pinned.
fn run_json(r: &RunRow) -> serde_json::Value {
    let argv: serde_json::Value = r
        .argv_json
        .as_deref()
        .and_then(|j| serde_json::from_str(j).ok())
        .unwrap_or(serde_json::Value::Null);
    let env_names: serde_json::Value = r
        .env_names_json
        .as_deref()
        .and_then(|j| serde_json::from_str(j).ok())
        .unwrap_or_else(|| json!([]));
    let capture = |m: &crate::db::CaptureMeta| {
        json!({
            "path": m.path,
            "bytes_total": m.bytes_total,
            "bytes_stored": m.bytes_stored,
            "bytes_omitted": m.bytes_omitted,
            "reason": m.reason,
        })
    };
    json!({
        "run_id": r.run_id,
        "job_id": r.job_id,
        "job_id_inferred": r.job_id_inferred,
        "mode": r.mode,
        "argv": argv,
        "shell_cmd": r.shell_cmd,
        "cwd": r.cwd,
        "env_names": env_names,
        "host": r.host,
        "schedule_label": r.schedule_label,
        "status": r.status,
        "started_at": rfc3339(r.start_ms),
        "ended_at": r.end_ms.map(rfc3339),
        "end_is_detection_time": r.end_is_detection,
        "duration_ms": r.duration_ms(),
        "exit_code": r.exit_code,
        "signal": r.signal_no,
        "timeout_fired": r.timeout_fired,
        "interrupted_by": r.interrupted_by,
        "start_error": r.start_error,
        "wrapper_pid": r.wrapper_pid,
        "child_pid": r.child_pid,
        "expected_duration_ms": r.expected_duration_ms,
        "long_run_fired": r.long_run_fired,
        "detached_children": r.detached_children,
        "stdout": capture(&r.stdout),
        "stderr": capture(&r.stderr),
        "output_pruned_at": r.output_pruned_ms.map(rfc3339),
    })
}

fn delivery_json(d: &DeliveryRow) -> serde_json::Value {
    json!({
        "id": d.id,
        "event": d.event,
        "reporter": d.reporter,
        "state": d.state,
        "attempt_count": d.attempt_count,
        "created_at": rfc3339(d.created_ms),
        "next_attempt_at": d.next_attempt_ms.map(rfc3339),
        "delivered_at": d.delivered_ms.map(rfc3339),
        "last_error": d.last_error,
    })
}

fn exit_label(r: &RunRow) -> String {
    match (&r.exit_code, &r.signal_no) {
        (Some(c), _) => c.to_string(),
        (None, Some(s)) => format!("sig {s}"),
        _ => "-".to_string(),
    }
}

fn duration_label(r: &RunRow) -> String {
    match r.duration_ms() {
        Some(d) if !r.end_is_detection => format_duration_ms(d.max(0) as u64),
        _ => "-".to_string(),
    }
}

pub fn cmd_history(args: HistoryArgs) -> i32 {
    let opened = match open_for_inspection(args.config.as_deref(), args.data_dir.as_deref(), false)
    {
        Ok(o) => o,
        Err(e) => {
            eprintln!("uatu: error: {e}");
            return 1;
        }
    };
    // Reconcile stale active runs before rendering; enqueue only (SPEC §3).
    reconcile::reconcile(&opened.db, &opened.config, &opened.oplog);

    let runs = match opened
        .db
        .history(args.limit, args.job.as_deref(), args.status.as_deref())
    {
        Ok(r) => r,
        Err(e) => {
            eprintln!("uatu: error: cannot read history: {e}");
            return 1;
        }
    };

    if args.json {
        let arr: Vec<_> = runs.iter().map(run_json).collect();
        println!(
            "{}",
            serde_json::to_string_pretty(&arr).unwrap_or_else(|_| "[]".into())
        );
        return 0;
    }

    if runs.is_empty() {
        println!("no runs recorded");
        return 0;
    }
    println!(
        "{:<26}  {:<24}  {:<12}  {:<20}  {:>10}  {:>6}",
        "RUN ID", "JOB", "STATUS", "STARTED (UTC)", "DURATION", "EXIT"
    );
    for r in &runs {
        println!(
            "{:<26}  {:<24}  {:<12}  {:<20}  {:>10}  {:>6}",
            r.run_id,
            truncate(&r.job_id, 24),
            r.status,
            rfc3339(r.start_ms),
            duration_label(r),
            exit_label(r),
        );
    }

    // Identity-fragmentation hint (SPEC §5).
    if let Ok(hints) = opened.db.fragmentation_hints(now_ms(), 10) {
        for (basename, count) in hints {
            println!(
                "note: {count} distinct inferred job ids share the basename \"{basename}\" in the last 30 days; \
jobs with variable arguments should pass --name to keep one history"
            );
        }
    }
    0
}

pub fn cmd_show(args: ShowArgs) -> i32 {
    let opened = match open_for_inspection(args.config.as_deref(), args.data_dir.as_deref(), false)
    {
        Ok(o) => o,
        Err(e) => {
            eprintln!("uatu: error: {e}");
            return 1;
        }
    };
    reconcile::reconcile(&opened.db, &opened.config, &opened.oplog);

    let run_id = match opened.db.resolve_run_prefix(&args.run_id) {
        Ok(Ok(id)) => id,
        Ok(Err(candidates)) if candidates.is_empty() => {
            eprintln!("uatu: error: no run matches {:?}", args.run_id);
            return 1;
        }
        Ok(Err(candidates)) => {
            eprintln!(
                "uatu: error: ambiguous run id prefix {:?}; candidates:",
                args.run_id
            );
            for c in candidates {
                eprintln!("  {c}");
            }
            return 1;
        }
        Err(e) => {
            eprintln!("uatu: error: {e}");
            return 1;
        }
    };
    let Some(run) = opened.db.get_run(&run_id).ok().flatten() else {
        eprintln!("uatu: error: run {run_id} not found");
        return 1;
    };
    let deliveries = opened.db.deliveries_for_run(&run_id).unwrap_or_default();

    let read_stream = |path: Option<&str>| -> Option<String> {
        path.and_then(|p| std::fs::read(p).ok())
            .map(|b| String::from_utf8_lossy(&b).into_owned())
    };

    if args.json {
        let mut v = run_json(&run);
        v["deliveries"] = serde_json::Value::Array(deliveries.iter().map(delivery_json).collect());
        if args.stdout {
            v["stdout_content"] = match read_stream(run.stdout.path.as_deref()) {
                Some(s) => json!(s),
                None => serde_json::Value::Null,
            };
        }
        if args.stderr {
            v["stderr_content"] = match read_stream(run.stderr.path.as_deref()) {
                Some(s) => json!(s),
                None => serde_json::Value::Null,
            };
        }
        println!("{}", serde_json::to_string_pretty(&v).unwrap_or_default());
        return 0;
    }

    println!("run:        {}", run.run_id);
    println!(
        "job:        {}{}",
        run.job_id,
        if run.job_id_inferred {
            " (inferred)"
        } else {
            ""
        }
    );
    println!(
        "status:     {} ({})",
        run.status,
        crate::events::status_detail(&run)
    );
    println!("mode:       {}", run.mode);
    if let Some(argv) = &run.argv_json {
        println!("argv:       {argv}");
    }
    if let Some(sh) = &run.shell_cmd {
        println!("shell cmd:  {sh}");
    }
    if let Some(cwd) = &run.cwd {
        println!("cwd:        {cwd}");
    }
    if let Some(env) = &run.env_names_json {
        println!("env names:  {env}");
    }
    println!("host:       {}", run.host);
    if let Some(label) = &run.schedule_label {
        println!("schedule:   {label}");
    }
    println!("started:    {}", rfc3339(run.start_ms));
    if let Some(end) = run.end_ms {
        let label = if run.end_is_detection {
            " (stale detection time, not actual end)"
        } else {
            ""
        };
        println!("ended:      {}{label}", rfc3339(end));
    }
    println!("duration:   {}", duration_label(&run));
    println!("exit:       {}", exit_label(&run));
    if run.timeout_fired {
        println!("timeout:    fired (TERM-then-KILL applied)");
    }
    if let Some(by) = &run.interrupted_by {
        println!("interrupted by: {by}");
    }
    if let Some(err) = &run.start_error {
        println!("start error: {err}");
    }
    println!(
        "wrapper pid: {}   child pid: {}",
        run.wrapper_pid,
        run.child_pid
            .map(|p| p.to_string())
            .unwrap_or_else(|| "-".into())
    );
    if run.long_run_fired {
        println!("long_run:   alert fired mid-run");
    }
    if run.detached_children {
        println!("detached:   child processes still held the output pipe at exit");
    }
    for (name, m) in [("stdout", &run.stdout), ("stderr", &run.stderr)] {
        println!(
            "{name}:     {} produced, {} stored, {} omitted{}{}",
            m.bytes_total,
            m.bytes_stored,
            m.bytes_omitted,
            m.reason
                .as_deref()
                .map(|r| format!(" (degraded: {r})"))
                .unwrap_or_default(),
            m.path
                .as_deref()
                .map(|p| format!(" — {p}"))
                .unwrap_or_default(),
        );
    }
    if let Some(pruned) = run.output_pruned_ms {
        println!("output pruned at: {}", rfc3339(pruned));
    }
    if !deliveries.is_empty() {
        println!("deliveries:");
        for d in &deliveries {
            println!(
                "  - event={} reporter={} state={} attempts={}{}{}{}",
                d.event,
                d.reporter,
                d.state,
                d.attempt_count,
                d.delivered_ms
                    .map(|m| format!(" delivered_at={}", rfc3339(m)))
                    .unwrap_or_default(),
                d.next_attempt_ms
                    .filter(|_| d.state == "queued")
                    .map(|m| format!(" next_attempt_at={}", rfc3339(m)))
                    .unwrap_or_default(),
                d.last_error
                    .as_deref()
                    .map(|e| format!(" last_error={e:?}"))
                    .unwrap_or_default(),
            );
        }
    }
    if args.stdout {
        println!("--- stdout ---");
        print!(
            "{}",
            read_stream(run.stdout.path.as_deref()).unwrap_or_default()
        );
    }
    if args.stderr {
        println!("--- stderr ---");
        print!(
            "{}",
            read_stream(run.stderr.path.as_deref()).unwrap_or_default()
        );
    }
    0
}

pub fn cmd_status(args: StatusArgs) -> i32 {
    let opened = match open_for_inspection(args.config.as_deref(), args.data_dir.as_deref(), false)
    {
        Ok(o) => o,
        Err(e) => {
            eprintln!("uatu: error: {e}");
            return 1;
        }
    };
    reconcile::reconcile(&opened.db, &opened.config, &opened.oplog);

    let mut runs = opened.db.runs_with_status("active").unwrap_or_default();
    runs.extend(opened.db.runs_with_status("stale").unwrap_or_default());

    if args.json {
        let arr: Vec<_> = runs.iter().map(run_json).collect();
        println!(
            "{}",
            serde_json::to_string_pretty(&arr).unwrap_or_else(|_| "[]".into())
        );
        return 0;
    }
    if runs.is_empty() {
        println!("no active or stale runs");
        return 0;
    }
    println!(
        "{:<24}  {:<26}  {:>8}  {:>8}  {:<20}  {:>10}  STATE",
        "JOB", "RUN ID", "WPID", "CPID", "STARTED (UTC)", "AGE"
    );
    let now = now_ms();
    for r in &runs {
        let age = format_duration_ms((now - r.start_ms).max(0) as u64);
        println!(
            "{:<24}  {:<26}  {:>8}  {:>8}  {:<20}  {:>10}  {}",
            truncate(&r.job_id, 24),
            r.run_id,
            r.wrapper_pid,
            r.child_pid
                .map(|p| p.to_string())
                .unwrap_or_else(|| "-".into()),
            rfc3339(r.start_ms),
            age,
            if r.status == "stale" {
                "STALE"
            } else {
                "active"
            },
        );
    }
    0
}

fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        s.chars().take(max - 1).collect::<String>() + "…"
    }
}
