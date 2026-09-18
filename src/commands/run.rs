//! `uatu run` (SPEC §3, §6): the wrapper. The foundational invariant — output
//! reaches cron byte-for-byte, the exit status is preserved, and no
//! observability failure may prevent or alter the job's execution.

use std::ffi::OsString;
use std::os::fd::{AsRawFd, OwnedFd};
use std::os::unix::process::{CommandExt, ExitStatusExt};
use std::path::PathBuf;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use crate::capture::{CaptureSpec, CaptureTask, spawn_capture};
use crate::config::{self, CaptureMode, CliOverrides, Config, Effective};
use crate::db::{CaptureMeta, Db, RunRow};
use crate::events::{self, Event};
use crate::identity::{self, ExecMode};
use crate::liveness::{self, Liveness};
use crate::lock;
use crate::oplog::OpLog;
use crate::redact::Redactor;
use crate::report::{self, DeliverCtx, Sender};
use crate::state;
use crate::util::now_ms;

pub struct RunArgs {
    pub name: Option<String>,
    pub shell: bool,
    pub config: Option<PathBuf>,
    pub data_dir: Option<PathBuf>,
    pub cwd: Option<PathBuf>,
    pub env: Vec<(String, String)>,
    pub timeout: Option<Duration>,
    pub kill_grace: Option<Duration>,
    pub expected_duration: Option<Duration>,
    pub cmd: Vec<OsString>,
}

// Exit codes (SPEC §3): 124 timeout, 125 internal pre-start, 126 not
// executable, 127 not found, 128+N signal.
const EXIT_TIMEOUT: i32 = 124;
const EXIT_INTERNAL: i32 = 125;
const EXIT_NOT_EXECUTABLE: i32 = 126;
const EXIT_NOT_FOUND: i32 = 127;

// Opening includes forward-only migrations. A legacy queue may make a
// migration expensive, but state maintenance must never hold the child start
// indefinitely; the run degrades to passthrough after the normal SQLite
// contention budget.
const STATE_OPEN_BUDGET: Duration = Duration::from_secs(5);
const SQLITE_BUSY_BUDGET: Duration = Duration::from_secs(5);
const DIGEST_QUEUE_MAX_RESERVE: Duration = Duration::from_millis(250);

fn warn(msg: &str) {
    eprintln!("uatu: warning: {msg}");
}

pub fn cmd_run(args: RunArgs) -> i32 {
    // ----- config (lenient: SPEC §10) -----
    let loaded = config::load_runtime(args.config.as_deref());
    let cfg = loaded.config.clone();
    if let Some(err) = &loaded.invalid {
        warn(&format!("{err}; running local-only"));
    }
    for w in &loaded.warnings {
        warn(w);
    }

    // ----- redaction (SPEC §9): invalid → metadata-only, reporters disabled -----
    let mut redaction_invalid = loaded.redaction_invalid.clone();
    let redactor = match Redactor::new(
        &cfg.redaction.literals,
        &cfg.redaction.regex,
        &cfg.auto_secrets(),
    ) {
        Ok(r) => Arc::new(r),
        Err(e) => {
            redaction_invalid = Some(e);
            Arc::new(Redactor::empty())
        }
    };
    if let Some(e) = &redaction_invalid {
        warn(&format!(
            "invalid redaction config: {e}; running metadata-only (capture and reporters disabled)"
        ));
    }
    let metadata_only = redaction_invalid.is_some();
    let reporters_enabled = !metadata_only && loaded.invalid.is_none();

    // ----- identity (SPEC §5) -----
    let mode = if args.shell {
        ExecMode::Shell
    } else {
        ExecMode::Direct
    };
    if args.shell && args.cmd.len() != 1 {
        // CLI usage error before any child starts (SPEC §3).
        eprintln!("uatu: error: --shell requires exactly one command string after --");
        return 2;
    }
    let argv_lossy: Vec<String> = args
        .cmd
        .iter()
        .map(|a| a.to_string_lossy().into_owned())
        .collect();
    let identity_cwd = args
        .cwd
        .clone()
        .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from("/")));
    let identity_cwd = identity_cwd.canonicalize().unwrap_or(identity_cwd);
    let uid = unsafe { libc::getuid() };
    let (job_id, inferred_basename, inferred) = match &args.name {
        Some(name) => (name.clone(), None, false),
        None => {
            let (id, basename) = identity::infer_job_id(uid, &identity_cwd, mode, &argv_lossy);
            (id, Some(basename), true)
        }
    };
    let run_id = identity::new_run_id();

    // ----- effective settings (CLI > job > global > default) -----
    let cli_overrides = CliOverrides {
        cwd: args.cwd.clone(),
        env: args.env.clone(),
        timeout: args.timeout,
        kill_grace: args.kill_grace,
        expected_duration: args.expected_duration,
    };
    let eff = config::resolve_effective(&cfg, &job_id, &cli_overrides);

    // ----- state (SPEC §10: failure → passthrough) -----
    let state_dir = config::resolve_state_dir(args.data_dir.as_deref(), &cfg);
    let opened = open_state(&state_dir, &cfg, &redactor);
    let (paths, db, oplog) = match opened {
        Ok(t) => t,
        Err(e) => {
            warn(&format!(
                "{e}; running passthrough (no history, capture, queue, or pruning)"
            ));
            let oplog = OpLog::disabled();
            return run_passthrough(&args, &cfg, &eff, mode, &job_id, reporters_enabled, &oplog);
        }
    };
    for w in &loaded.warnings {
        oplog.warn("config_warning", w, &[]);
    }
    if let Some(e) = &loaded.invalid {
        oplog.warn("config_warning", e, &[]);
    }
    if let Some(e) = &redaction_invalid {
        oplog.error(
            "config_warning",
            &format!("invalid redaction config: {e}"),
            &[],
        );
    }

    // ----- storage preflight (SPEC §6) -----
    let mut capture_enabled = !metadata_only && eff.capture_mode != CaptureMode::Off;
    let mut preflight_note: Option<String> = None;
    if capture_enabled
        && let Some(free) = state::free_bytes(&paths.state_dir)
        && free < eff.min_free_bytes
    {
        capture_enabled = false;
        let msg = format!(
            "free space {} below min_free_bytes {}; capture disabled for this run (metadata-only)",
            crate::util::format_bytes(free),
            crate::util::format_bytes(eff.min_free_bytes)
        );
        warn(&msg);
        oplog.warn(
            "preflight_low_space",
            &msg,
            &[("run_id", serde_json::json!(run_id))],
        );
        preflight_note = Some("preflight: low free space".to_string());
    }

    // ----- signal handling -----
    // Installed before the run row and the child: from the moment a run is
    // observable, SIGTERM/SIGINT/SIGHUP get the orderly TERM-then-KILL path.
    let mut signals = install_signals();

    // ----- record run start (liveness identity: SPEC §6) -----
    let me = liveness::current();
    let start_ms = now_ms();
    let argv_json = if metadata_only {
        None
    } else if mode == ExecMode::Direct {
        serde_json::to_string(
            &argv_lossy
                .iter()
                .map(|a| redactor.redact_str(a))
                .collect::<Vec<_>>(),
        )
        .ok()
    } else {
        None
    };
    let shell_cmd = if metadata_only {
        None
    } else if mode == ExecMode::Shell {
        Some(redactor.redact_str(&argv_lossy[0]))
    } else {
        None
    };
    let mut env_names: Vec<String> = eff.env.keys().cloned().collect();
    env_names.sort();
    let row = RunRow {
        run_id: run_id.clone(),
        job_id: job_id.clone(),
        job_id_inferred: inferred,
        inferred_basename: inferred_basename.clone(),
        mode: mode.as_str().to_string(),
        argv_json,
        shell_cmd,
        cwd: eff.cwd.as_ref().map(|p| p.to_string_lossy().into_owned()),
        env_names_json: serde_json::to_string(&env_names).ok(),
        host: cfg.host_name(),
        schedule_label: eff.schedule_label.clone(),
        status: "active".to_string(),
        start_ms,
        end_ms: None,
        end_is_detection: false,
        exit_code: None,
        signal_no: None,
        timeout_fired: false,
        interrupted_by: None,
        start_error: None,
        wrapper_pid: me.pid as i64,
        wrapper_start_ticks: me.start_ticks as i64,
        boot_id: me.boot_id.clone(),
        child_pid: None,
        expected_duration_ms: eff.expected_duration.map(crate::util::duration_ms_i64),
        long_run_fired: false,
        detached_children: false,
        stdout: CaptureMeta::default(),
        stderr: CaptureMeta::default(),
        output_pruned_ms: None,
    };
    let mut db_ok = true;
    if let Err(e) = db.insert_run(&row) {
        db_ok = false;
        warn(&format!("cannot record run start: {e}"));
    }
    oplog.info(
        "run_started",
        &format!("job {job_id} started"),
        &[
            ("run_id", serde_json::json!(run_id)),
            ("job_id", serde_json::json!(job_id)),
        ],
    );

    // ----- pre-start checks → 125 (SPEC §3) -----
    if let Some(cwd) = &eff.cwd
        && !cwd.is_dir()
    {
        let msg = format!("working directory {} does not exist", cwd.display());
        return finish_start_failure(
            &db,
            &oplog,
            &cfg,
            &paths,
            &me,
            &run_id,
            &job_id,
            EXIT_INTERNAL,
            &msg,
            reporters_enabled,
            db_ok,
            &redactor,
        );
    }

    // ----- spawn child in a new process group (SPEC §6) -----
    // Both readers must exist before the child can write into either pipe.
    // If reservation fails, run once with inherited output and retain history.
    let stop = Arc::new(AtomicBool::new(false));
    let pumps = match reserve_pumps(&stop) {
        Ok(pumps) => Some(pumps),
        Err(e) => {
            let note =
                format!("cannot start output workers: {e}; output inherited, capture disabled");
            warn(&note);
            preflight_note = Some(note);
            capture_enabled = false;
            None
        }
    };
    let mut command = build_command(&args.cmd, mode, &eff);
    if pumps.is_none() {
        command.stdout(Stdio::inherit()).stderr(Stdio::inherit());
    }
    let mut child = match command.spawn() {
        Ok(c) => c,
        Err(e) => {
            let code = match e.kind() {
                std::io::ErrorKind::NotFound => EXIT_NOT_FOUND,
                std::io::ErrorKind::PermissionDenied => EXIT_NOT_EXECUTABLE,
                _ => EXIT_INTERNAL,
            };
            let msg = format!("cannot start command: {e}");
            return finish_start_failure(
                &db,
                &oplog,
                &cfg,
                &paths,
                &me,
                &run_id,
                &job_id,
                code,
                &msg,
                reporters_enabled,
                db_ok,
                &redactor,
            );
        }
    };
    let child_pid = child.id() as i32;
    if db_ok {
        let _ = db.set_child_pid(&run_id, child_pid as i64);
    }

    // ----- stream pumps + capture (SPEC §6) -----
    let run_dir = paths.run_output_dir(&job_id, &run_id);
    let mut capture_dir_err: Option<String> = None;
    if capture_enabled
        && (eff.capture_stdout || eff.capture_stderr)
        && let Err(e) = state::mkdir_0700_all(&run_dir)
    {
        capture_dir_err = Some(format!("cannot create output dir: {e}"));
        capture_enabled = false;
    }
    let (out_pump, err_pump, out_capture, err_capture) = if let Some((out, err)) = pumps {
        let (out_pump, out_capture) = start_stream(
            out,
            child.stdout.take().expect("stdout piped").into(),
            capture_enabled && eff.capture_stdout,
            &eff,
            run_dir.join("stdout.log"),
            &redactor,
        );
        let (err_pump, err_capture) = start_stream(
            err,
            child.stderr.take().expect("stderr piped").into(),
            capture_enabled && eff.capture_stderr,
            &eff,
            run_dir.join("stderr.log"),
            &redactor,
        );
        (Some(out_pump), Some(err_pump), out_capture, err_capture)
    } else {
        (None, None, None, None)
    };

    // ----- supervise (timeout, long-run, interruption) -----
    let started = Instant::now();
    let timeout_at = eff.timeout.map(|t| started + t);
    let mut long_run_at = eff.expected_duration.map(|d| started + d);
    let mut timeout_fired = false;
    let mut interrupted_by: Option<&'static str> = None;
    let mut long_run_thread: Option<std::thread::JoinHandle<()>> = None;
    let mut long_run_deferred = false;

    let exit_status: ExitStatus = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) => {}
            Err(e) => {
                warn(&format!("waitpid failed: {e}"));
            }
        }
        // Interruption of the wrapper itself (SPEC §3): TERM-then-KILL the
        // group, record the child's real result, enqueue-only, exit promptly.
        if let Some(sig) = wait_for_signal(&mut signals) {
            interrupted_by = Some(signal_name(sig));
            let status = term_then_kill(&mut child, child_pid, eff.kill_grace);
            break status;
        }
        if let Some(t) = timeout_at
            && Instant::now() >= t
            && !timeout_fired
        {
            timeout_fired = true;
            let status = term_then_kill(&mut child, child_pid, eff.kill_grace);
            break status;
        }
        if let Some(t) = long_run_at
            && Instant::now() >= t
        {
            long_run_at = None; // once per run (SPEC §6)
            match fire_long_run(
                &paths.db,
                &cfg,
                &run_id,
                &job_id,
                &redactor,
                reporters_enabled,
                eff.expected_from_cli,
                &oplog,
                db_ok,
            ) {
                Ok(task) => long_run_thread = task,
                Err(e) => {
                    warn(&format!(
                        "cannot start long-run reporter: {e}; notifications will be queued after the child exits"
                    ));
                    long_run_deferred = true;
                }
            }
            if db_ok {
                let _ = db.set_long_run_fired(&run_id);
            }
        }
    };

    drop(signals);

    // ----- drain pumps -----
    // EOF arrives immediately unless a detached child holds the pipe open;
    // in that case stop draining after a short grace (SPEC §6: detached
    // children are ignored, noted in metadata when detectable).
    let detached = if let (Some(out_pump), Some(err_pump)) = (out_pump, err_pump) {
        let drain_deadline = Instant::now() + Duration::from_secs(2);
        while !(out_pump.is_finished() && err_pump.is_finished()) && Instant::now() < drain_deadline
        {
            std::thread::sleep(Duration::from_millis(10));
        }
        stop.store(true, Ordering::SeqCst);
        let out_eof = out_pump.join().unwrap_or(false);
        let err_eof = err_pump.join().unwrap_or(false);
        !out_eof || !err_eof
    } else {
        false
    };
    let mut stdout_meta = join_capture(out_capture);
    let mut stderr_meta = join_capture(err_capture);
    if let Some(e) = &capture_dir_err {
        stdout_meta.reason.get_or_insert_with(|| e.clone());
        stderr_meta.reason.get_or_insert_with(|| e.clone());
        warn(e);
    }
    if let Some(n) = &preflight_note {
        stdout_meta.reason.get_or_insert_with(|| n.clone());
        stderr_meta.reason.get_or_insert_with(|| n.clone());
    }
    for (stream, meta) in [("stdout", &stdout_meta), ("stderr", &stderr_meta)] {
        if let Some(reason) = &meta.reason {
            if preflight_note.is_none() && capture_dir_err.is_none() {
                warn(&format!("capture degraded ({stream}): {reason}"));
            }
            oplog.warn(
                "capture_degraded",
                &format!("{stream}: {reason}"),
                &[("run_id", serde_json::json!(run_id))],
            );
        }
    }

    // ----- compute status + exit code (SPEC §3, §7) -----
    let (status_str, exit_code, signal_no, wrapper_exit) =
        classify_exit(&exit_status, timeout_fired);

    // ----- final record -----
    let end_ms = now_ms();
    if db_ok
        && let Err(e) = db.finish_run(
            &run_id,
            status_str,
            end_ms,
            exit_code,
            signal_no,
            timeout_fired,
            interrupted_by,
            None,
            detached,
            &stdout_meta,
            &stderr_meta,
        )
    {
        warn(&format!("cannot record run result: {e}"));
        db_ok = false;
    }
    oplog.info(
        "run_finished",
        &format!("job {job_id} finished: {status_str}"),
        &[
            ("run_id", serde_json::json!(run_id)),
            ("job_id", serde_json::json!(job_id)),
            ("status", serde_json::json!(status_str)),
            ("exit_code", serde_json::json!(exit_code)),
        ],
    );

    // ----- events + bounded delivery (SPEC §8) -----
    if reporters_enabled && db_ok {
        let mut events_to_send: Vec<Event> = Vec::new();
        match status_str {
            "success" => {
                events_to_send.push(Event::Success);
                if let Ok(Some(prev)) = db.last_terminal_status_before(&job_id, start_ms, &run_id)
                    && matches!(
                        prev.as_str(),
                        "failure" | "timeout" | "stale" | "start_failed"
                    )
                {
                    events_to_send.push(Event::Recovery);
                }
            }
            "failure" | "timeout" => events_to_send.push(Event::Failure),
            _ => {}
        }
        if long_run_deferred {
            events_to_send.push(Event::LongRun);
        }
        deliver_run_events(
            &db,
            &cfg,
            &oplog,
            &paths,
            &me,
            &run_id,
            &job_id,
            &events_to_send,
            eff.expected_from_cli,
            interrupted_by.is_some() || long_run_deferred,
            end_ms,
            &redactor,
        );
    }

    // Collect a completed long_run sender without waiting past the child path.
    // An unfinished sender remains owner-scoped and is requeued as an orphan
    // after this wrapper exits (at-least-once is fine).
    if let Some(t) = long_run_thread
        && t.is_finished()
    {
        let _ = t.join();
    }

    wrapper_exit
}

fn open_state(
    state_dir: &std::path::Path,
    cfg: &Config,
    redactor: &Arc<Redactor>,
) -> Result<(state::Paths, Db, OpLog), String> {
    let paths = state::prepare(state_dir)
        .map_err(|e| format!("cannot prepare state dir {}: {e}", state_dir.display()))?;
    let oplog = OpLog::new(
        config::resolve_log_path(cfg, &paths.state_dir),
        config::log_max_bytes(cfg),
        Arc::clone(redactor),
    );
    let db = Db::open_bounded(&paths.db, STATE_OPEN_BUDGET).map_err(|e| {
        oplog.error("state_unavailable", &e.to_string(), &[]);
        format!("cannot open state database: {e}")
    })?;
    Ok((paths, db, oplog))
}

/// State unavailable: still run the child faithfully (SPEC §10). Reporters
/// attempt synchronously when possible; failures are logged, never queued.
fn run_passthrough(
    args: &RunArgs,
    cfg: &Config,
    eff: &Effective,
    mode: ExecMode,
    job_id: &str,
    reporters_enabled: bool,
    _oplog: &OpLog,
) -> i32 {
    if let Some(cwd) = &eff.cwd
        && !cwd.is_dir()
    {
        eprintln!(
            "uatu: error: working directory {} does not exist",
            cwd.display()
        );
        return EXIT_INTERNAL;
    }
    let mut signals = install_signals();

    let mut command = build_command(&args.cmd, mode, eff);
    command.stdout(Stdio::inherit()).stderr(Stdio::inherit());
    let mut child = match command.spawn() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("uatu: error: cannot start command: {e}");
            return match e.kind() {
                std::io::ErrorKind::NotFound => EXIT_NOT_FOUND,
                std::io::ErrorKind::PermissionDenied => EXIT_NOT_EXECUTABLE,
                _ => EXIT_INTERNAL,
            };
        }
    };
    let child_pid = child.id() as i32;

    let started = Instant::now();
    let timeout_at = eff.timeout.map(|t| started + t);
    let mut timeout_fired = false;
    let status = loop {
        match child.try_wait() {
            Ok(Some(s)) => break s,
            Ok(None) => {}
            Err(_) => {}
        }
        if wait_for_signal(&mut signals).is_some() {
            break term_then_kill(&mut child, child_pid, eff.kill_grace);
        }
        if let Some(t) = timeout_at
            && Instant::now() >= t
            && !timeout_fired
        {
            timeout_fired = true;
            break term_then_kill(&mut child, child_pid, eff.kill_grace);
        }
    };
    drop(signals);
    let (status_str, _, _, wrapper_exit) = classify_exit(&status, timeout_fired);
    let _ = (cfg, job_id, reporters_enabled, status_str);
    wrapper_exit
}

fn build_command(cmd: &[OsString], mode: ExecMode, eff: &Effective) -> Command {
    let mut command = match mode {
        ExecMode::Shell => {
            // SPEC §3: `$SHELL -c`, NOT `-l` — login shells diverge from cron.
            let shell = std::env::var_os("SHELL")
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| OsString::from("/bin/sh"));
            let mut c = Command::new(shell);
            c.arg("-c").arg(&cmd[0]);
            c
        }
        ExecMode::Direct => {
            let mut c = Command::new(&cmd[0]);
            c.args(&cmd[1..]);
            c
        }
    };
    if let Some(cwd) = &eff.cwd {
        command.current_dir(cwd);
    }
    for (k, v) in &eff.env {
        command.env(k, v); // only adds/overrides; the rest is inherited untouched
    }
    command.stdout(Stdio::piped()).stderr(Stdio::piped());
    command.process_group(0); // new process group for signal fanout (SPEC §6)
    command
}

fn install_signals() -> Option<signal_hook::iterator::Signals> {
    use signal_hook::consts::{SIGHUP, SIGINT, SIGTERM};
    match signal_hook::iterator::Signals::new([SIGTERM, SIGINT, SIGHUP]) {
        Ok(signals) => Some(signals),
        Err(e) => {
            warn(&format!("cannot install signal handlers: {e}"));
            None
        }
    }
}

fn wait_for_signal(signals: &mut Option<signal_hook::iterator::Signals>) -> Option<i32> {
    // Poll on the supervisor's existing cadence, without a forwarding worker.
    // Sleeping also bounds CPU use when installing the handlers failed.
    std::thread::sleep(Duration::from_millis(50));
    signals
        .as_mut()
        .and_then(|signals| signals.pending().next())
}

fn signal_name(sig: i32) -> &'static str {
    match sig {
        libc::SIGTERM => "SIGTERM",
        libc::SIGINT => "SIGINT",
        libc::SIGHUP => "SIGHUP",
        _ => "signal",
    }
}

/// SPEC §3: TERM the process group, wait the kill-grace, then KILL the group.
fn term_then_kill(child: &mut Child, pgid: i32, grace: Duration) -> ExitStatus {
    unsafe {
        libc::killpg(pgid, libc::SIGTERM);
    }
    let deadline = Instant::now() + grace;
    loop {
        if let Ok(Some(status)) = child.try_wait() {
            return status;
        }
        if Instant::now() >= deadline {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    unsafe {
        libc::killpg(pgid, libc::SIGKILL);
    }
    child
        .wait()
        .unwrap_or_else(|_| ExitStatus::from_raw(libc::SIGKILL))
}

fn classify_exit(
    status: &ExitStatus,
    timeout_fired: bool,
) -> (&'static str, Option<i64>, Option<i64>, i32) {
    if timeout_fired {
        // Timeout always wins: status `timeout`, exit 124 (SPEC §3, §7).
        let signal_no = status.signal().map(|s| s as i64);
        return ("timeout", Some(124), signal_no, EXIT_TIMEOUT);
    }
    if let Some(sig) = status.signal() {
        return ("failure", None, Some(sig as i64), 128 + sig);
    }
    let code = status.code().unwrap_or(EXIT_INTERNAL);
    let status_str = if code == 0 { "success" } else { "failure" };
    (status_str, Some(code as i64), None, code)
}

type PumpHandle = std::thread::JoinHandle<bool>;

struct PumpInput {
    pipe: OwnedFd,
    capture_tx: Option<mpsc::Sender<Vec<u8>>>,
    raw_total: Arc<AtomicU64>,
}

struct ReservedPump {
    input: Option<mpsc::Sender<PumpInput>>,
    handle: Option<PumpHandle>,
}

impl ReservedPump {
    fn new(name: &'static str, dest_fd: i32, stop: &Arc<AtomicBool>) -> std::io::Result<Self> {
        let (input, rx) = mpsc::channel::<PumpInput>();
        let stop = Arc::clone(stop);
        let handle = crate::worker::spawn(name, move || match rx.recv() {
            Ok(input) => pump(input.pipe, dest_fd, input.capture_tx, input.raw_total, stop),
            Err(_) => true,
        })?;
        Ok(Self {
            input: Some(input),
            handle: Some(handle),
        })
    }

    fn start(mut self, input: PumpInput) -> PumpHandle {
        // A reserved worker only waits for this channel and cannot exit before
        // receiving its input. Sending transfers descriptor ownership to it.
        let _ = self.input.take().expect("reserved sender").send(input);
        self.handle.take().expect("reserved worker")
    }
}

impl Drop for ReservedPump {
    fn drop(&mut self) {
        self.input.take();
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn reserve_pumps(stop: &Arc<AtomicBool>) -> std::io::Result<(ReservedPump, ReservedPump)> {
    let stdout = ReservedPump::new("uatu-stdout", libc::STDOUT_FILENO, stop)?;
    let stderr = ReservedPump::new("uatu-stderr", libc::STDERR_FILENO, stop)?;
    Ok((stdout, stderr))
}

struct StreamCapture {
    task: std::io::Result<CaptureTask>,
    raw_total: Arc<AtomicU64>,
}

fn start_stream(
    reserved: ReservedPump,
    pipe: OwnedFd,
    capture: bool,
    eff: &Effective,
    capture_path: PathBuf,
    redactor: &Arc<Redactor>,
) -> (PumpHandle, Option<StreamCapture>) {
    set_nonblocking(pipe.as_raw_fd());
    let raw_total = Arc::new(AtomicU64::new(0));
    let (tx, capture_task) = if capture {
        let (tx, rx) = mpsc::channel::<Vec<u8>>();
        let task = spawn_capture(
            CaptureSpec {
                mode: eff.capture_mode,
                head_bytes: eff.capture_head_bytes,
                tail_bytes: eff.capture_tail_bytes,
                path: capture_path,
            },
            Arc::clone(redactor),
            rx,
            Arc::clone(&raw_total),
        );
        let tx = task.is_ok().then_some(tx);
        (
            tx,
            Some(StreamCapture {
                task,
                raw_total: Arc::clone(&raw_total),
            }),
        )
    } else {
        (None, None)
    };
    let handle = reserved.start(PumpInput {
        pipe,
        capture_tx: tx,
        raw_total,
    });
    (handle, capture_task)
}

fn join_capture(capture: Option<StreamCapture>) -> CaptureMeta {
    let Some(capture) = capture else {
        return CaptureMeta::default();
    };
    let bytes_total = capture.raw_total.load(Ordering::SeqCst);
    let mut meta = match capture.task {
        Ok(task) => task.handle.join().unwrap_or_default(),
        Err(e) => CaptureMeta {
            bytes_omitted: bytes_total,
            reason: Some(format!("cannot start capture worker: {e}")),
            ..CaptureMeta::default()
        },
    };
    meta.bytes_total = bytes_total;
    meta
}

fn set_nonblocking(fd: i32) {
    unsafe {
        let flags = libc::fcntl(fd, libc::F_GETFL);
        if flags >= 0 {
            libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK);
        }
    }
}

/// Raw passthrough pump (SPEC §6): bytes go to the parent fd the moment they
/// arrive; capture sees a copy via an unbounded channel. Returns true on EOF
/// (false = stopped while a detached child still held the pipe).
fn pump(
    pipe: OwnedFd,
    dest_fd: i32,
    capture_tx: Option<mpsc::Sender<Vec<u8>>>,
    raw_total: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
) -> bool {
    let src_fd = pipe.as_raw_fd();
    let mut buf = vec![0u8; 64 * 1024];
    let mut eof = false;
    'outer: loop {
        let mut pfd = libc::pollfd {
            fd: src_fd,
            events: libc::POLLIN,
            revents: 0,
        };
        let r = unsafe { libc::poll(&mut pfd, 1, 100) };
        if r < 0 {
            let err = std::io::Error::last_os_error();
            if err.kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            break;
        }
        if r > 0 {
            loop {
                let n =
                    unsafe { libc::read(src_fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len()) };
                if n == 0 {
                    eof = true;
                    break 'outer;
                }
                if n < 0 {
                    let err = std::io::Error::last_os_error();
                    match err.kind() {
                        std::io::ErrorKind::WouldBlock => break,
                        std::io::ErrorKind::Interrupted => continue,
                        _ => break 'outer,
                    }
                }
                let n = n as usize;
                raw_total.fetch_add(n as u64, Ordering::SeqCst);
                // Passthrough first, before any capture work.
                if !write_all_fd(dest_fd, &buf[..n]) {
                    // Parent pipe gone (cron died): closing our read end
                    // propagates EPIPE to the child like the bare line would.
                    break 'outer;
                }
                if let Some(tx) = &capture_tx {
                    let _ = tx.send(buf[..n].to_vec());
                }
            }
        }
        if stop.load(Ordering::SeqCst) {
            break;
        }
    }
    eof
}

fn write_all_fd(fd: i32, mut data: &[u8]) -> bool {
    while !data.is_empty() {
        let n = unsafe { libc::write(fd, data.as_ptr() as *const libc::c_void, data.len()) };
        if n < 0 {
            let err = std::io::Error::last_os_error();
            if err.kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            return false;
        }
        data = &data[n as usize..];
    }
    true
}

/// Long-run alert (SPEC §6): fires once, mid-run, off the supervisor thread
/// so kill semantics stay timing-independent. Uses its own DB connection.
#[allow(clippy::too_many_arguments)]
fn fire_long_run(
    db_path: &std::path::Path,
    cfg: &Config,
    run_id: &str,
    job_id: &str,
    redactor: &Arc<Redactor>,
    reporters_enabled: bool,
    expected_from_cli: bool,
    oplog: &OpLog,
    db_ok: bool,
) -> std::io::Result<Option<std::thread::JoinHandle<()>>> {
    oplog.warn(
        "long_run_detected",
        &format!("job {job_id} exceeded its expected duration"),
        &[("run_id", serde_json::json!(run_id))],
    );
    if !reporters_enabled || !db_ok {
        return Ok(None);
    }
    let reporters = events::reporters_for_event(cfg, job_id, Event::LongRun, expected_from_cli);
    if reporters.is_empty() {
        return Ok(None);
    }
    let db_path = db_path.to_path_buf();
    let cfg = cfg.clone();
    let run_id = run_id.to_string();
    let job_id = job_id.to_string();
    let oplog = oplog.clone();
    let redactor = Arc::clone(redactor);
    crate::worker::spawn("uatu-long-run", move || {
        let Ok(db) = Db::open(&db_path) else { return };
        let me = liveness::current();
        let now = now_ms();
        let mut ids = Vec::new();
        for reporter in &reporters {
            if let Ok(id) = db.insert_delivery(
                &run_id,
                &job_id,
                Event::LongRun.as_str(),
                reporter,
                "sending",
                now,
                None,
                Some(&me),
            ) {
                ids.push(id);
            }
        }
        let Ok(sender) = Sender::new() else {
            for id in ids {
                let _ = db.delivery_requeue(id, now);
            }
            return;
        };
        let ctx = DeliverCtx {
            db: &db,
            cfg: &cfg,
            oplog: &oplog,
            sender: &sender,
            host: cfg.host_name(),
            redactor: redactor.as_ref(),
        };
        for id in ids {
            if let Ok(Some(row)) = db.get_delivery(id) {
                report::deliver_row(&ctx, &row, report::per_reporter_budget());
            }
        }
    })
    .map(Some)
}

/// Queue and synchronously attempt this run's immediate events first, then
/// queue its digest membership and opportunistically flush already-queued
/// deliveries within the shared post-child budget (SPEC §3, §8). Reconcile
/// and retention work stays in inspection/maintenance commands, where it
/// cannot delay a completed cron job.
#[allow(clippy::too_many_arguments)]
fn deliver_run_events(
    db: &Db,
    cfg: &Config,
    oplog: &OpLog,
    paths: &state::Paths,
    me: &Liveness,
    run_id: &str,
    job_id: &str,
    events_to_send: &[Event],
    expected_from_cli: bool,
    enqueue_only: bool,
    digest_event_ms: i64,
    redactor: &Arc<Redactor>,
) {
    let overall_budget = report::overall_budget();
    let overall_deadline = Instant::now() + overall_budget;
    let digest_targeted = config::digest_period(cfg, job_id) != config::DigestPeriod::Off
        && !events::reporters_for_digest(cfg, job_id).is_empty();
    // Immediate network attempts stay first, but cannot consume the final
    // sliver needed to durably batch this execution into its digest cohort.
    // With the production budget this reserves 250ms; smaller test budgets
    // reserve 10% so immediate alerts retain 90% of the cap.
    let digest_reserve = if digest_targeted {
        Duration::from_millis(
            ((overall_budget.as_millis() / 10).min(DIGEST_QUEUE_MAX_RESERVE.as_millis())) as u64,
        )
    } else {
        Duration::ZERO
    };
    let immediate_deadline = overall_deadline
        .checked_sub(digest_reserve)
        .unwrap_or(overall_deadline);
    let now = now_ms();
    let mut own_rows: Vec<i64> = Vec::new();

    'immediate: for event in events_to_send {
        for reporter in events::reporters_for_event(cfg, job_id, *event, expected_from_cli) {
            let remaining = overall_deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                let msg = "post-child budget exhausted before all immediate alerts were queued";
                warn(msg);
                oplog.warn(
                    "delivery_queue_failed",
                    msg,
                    &[
                        ("run_id", serde_json::json!(run_id)),
                        ("job_id", serde_json::json!(job_id)),
                    ],
                );
                break 'immediate;
            }
            // A busy SQLite writer must not consume more than the remaining
            // post-child budget. Restore the normal timeout after insertion.
            if let Err(e) = db.conn.busy_timeout(remaining.min(SQLITE_BUSY_BUDGET)) {
                let msg = format!("cannot bound immediate-alert queue write: {e}");
                warn(&msg);
                oplog.warn("delivery_queue_failed", &msg, &[]);
                break 'immediate;
            }
            // Interrupted wrappers and unavailable reporter workers enqueue
            // without sending (SPEC §3).
            let (state, next, owner) = if enqueue_only {
                ("queued", Some(now), None)
            } else {
                ("sending", None, Some(me))
            };
            match db.insert_delivery(
                run_id,
                job_id,
                event.as_str(),
                &reporter,
                state,
                now,
                next,
                owner,
            ) {
                Ok(id) if !enqueue_only => own_rows.push(id),
                Ok(_) => {}
                Err(e) => {
                    let msg = format!("cannot queue immediate alert via {reporter}: {e}");
                    warn(&msg);
                    oplog.warn(
                        "delivery_queue_failed",
                        &msg,
                        &[
                            ("run_id", serde_json::json!(run_id)),
                            ("job_id", serde_json::json!(job_id)),
                        ],
                    );
                }
            }
        }
    }
    let _ = db.conn.busy_timeout(SQLITE_BUSY_BUDGET);

    if enqueue_only {
        queue_digest_with_budget(
            db,
            cfg,
            oplog,
            run_id,
            job_id,
            digest_event_ms,
            overall_deadline,
        );
        return;
    }

    let sender = match Sender::new() {
        Ok(s) => Some(s),
        Err(e) => {
            eprintln!("uatu: warning: {e}");
            None
        }
    };
    if let Some(sender) = &sender {
        let ctx = DeliverCtx {
            db,
            cfg,
            oplog,
            sender,
            host: cfg.host_name(),
            redactor: redactor.as_ref(),
        };
        for id in &own_rows {
            let remaining = immediate_deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                // Leave the remaining owner-scoped rows in `sending` rather
                // than wait past the cap. Reconciliation requeues them after
                // this wrapper exits.
                break;
            }
            let _ = db.conn.busy_timeout(remaining.min(SQLITE_BUSY_BUDGET));
            if let Ok(Some(row)) = db.get_delivery(*id) {
                report::deliver_row_with_deadline(
                    &ctx,
                    &row,
                    report::per_reporter_budget().min(remaining),
                    Some(immediate_deadline),
                );
            }
        }
    } else {
        for id in &own_rows {
            let remaining = immediate_deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break; // orphan reconciliation recovers the rest
            }
            let _ = db.conn.busy_timeout(remaining.min(SQLITE_BUSY_BUDGET));
            let _ = db.delivery_requeue(*id, now_ms());
        }
    }

    // Digest bookkeeping is one transaction across all reporters and happens
    // only after this run's configured immediate alerts have been attempted.
    queue_digest_with_budget(
        db,
        cfg,
        oplog,
        run_id,
        job_id,
        digest_event_ms,
        overall_deadline,
    );

    let Some(sender) = &sender else { return };
    let ctx = DeliverCtx {
        db,
        cfg,
        oplog,
        sender,
        host: cfg.host_name(),
        redactor: redactor.as_ref(),
    };
    // Opportunistic delivery only if the flush lock is free (SPEC §7). Keep
    // reconciliation and retention out of the child-exit path: both can scan
    // unbounded local state and are handled by inspection/flush/prune.
    if !overall_deadline
        .saturating_duration_since(Instant::now())
        .is_zero()
        && let Ok(Some(_guard)) = lock::try_acquire(&paths.lock)
    {
        report::deliver_due(&ctx, me, Some(overall_deadline));
    }
}

#[allow(clippy::too_many_arguments)]
fn queue_digest_with_budget(
    db: &Db,
    cfg: &Config,
    oplog: &OpLog,
    run_id: &str,
    job_id: &str,
    event_ms: i64,
    overall_deadline: Instant,
) {
    let remaining = overall_deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        let period = config::digest_period(cfg, job_id);
        if period != config::DigestPeriod::Off
            && !events::reporters_for_digest(cfg, job_id).is_empty()
        {
            let msg = "post-child budget exhausted before digest membership could be queued";
            warn(msg);
            oplog.warn(
                "digest_queue_failed",
                msg,
                &[
                    ("run_id", serde_json::json!(run_id)),
                    ("job_id", serde_json::json!(job_id)),
                ],
            );
        }
        return;
    }
    if let Err(e) = db.conn.busy_timeout(remaining.min(SQLITE_BUSY_BUDGET)) {
        let msg = format!("cannot bound digest queue write: {e}");
        warn(&msg);
        oplog.warn("digest_queue_failed", &msg, &[]);
        return;
    }
    let result =
        report::queue_digest_for_run_bounded(db, cfg, run_id, job_id, event_ms, overall_deadline);
    let _ = db.conn.busy_timeout(SQLITE_BUSY_BUDGET);
    if let Err(e) = result {
        let msg = format!("cannot queue run for digest: {e}");
        warn(&msg);
        oplog.warn(
            "digest_queue_failed",
            &msg,
            &[
                ("run_id", serde_json::json!(run_id)),
                ("job_id", serde_json::json!(job_id)),
            ],
        );
    }
}

#[allow(clippy::too_many_arguments)]
fn finish_start_failure(
    db: &Db,
    oplog: &OpLog,
    cfg: &Config,
    paths: &state::Paths,
    me: &Liveness,
    run_id: &str,
    job_id: &str,
    code: i32,
    msg: &str,
    reporters_enabled: bool,
    db_ok: bool,
    redactor: &Arc<Redactor>,
) -> i32 {
    eprintln!("uatu: error: {msg}");
    oplog.error(
        "child_start_failed",
        msg,
        &[
            ("run_id", serde_json::json!(run_id)),
            ("job_id", serde_json::json!(job_id)),
        ],
    );
    if db_ok {
        let end_ms = now_ms();
        let recorded = match db.finish_run(
            run_id,
            "start_failed",
            end_ms,
            Some(code as i64),
            None,
            false,
            None,
            Some(msg),
            false,
            &CaptureMeta::default(),
            &CaptureMeta::default(),
        ) {
            Ok(()) => true,
            Err(e) => {
                warn(&format!("cannot record start failure: {e}"));
                false
            }
        };
        if reporters_enabled && recorded {
            // start_failed reports as a failure event with start detail (SPEC §8).
            deliver_run_events(
                db,
                cfg,
                oplog,
                paths,
                me,
                run_id,
                job_id,
                &[Event::Failure],
                false,
                false,
                end_ms,
                redactor,
            );
        }
    }
    code
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;

    const OUT: &[u8] = b"out:\0\xff\n";
    const ERR: &[u8] = b"err:\0\xfe\n";

    // Fault selection exists only in the unit-test binary. Running cmd_run in
    // a subprocess isolates signal registrations and captures real raw fds.
    #[test]
    fn worker_failure_child() {
        let Ok(failure) = std::env::var("UATU_TEST_WORKER_FAILURE") else {
            return;
        };
        let worker = match failure.as_str() {
            "stdout" => "uatu-stdout",
            "stderr" => "uatu-stderr",
            "capture" => "uatu-capture",
            "long-run" => "uatu-long-run",
            _ => panic!("unknown worker"),
        };
        crate::worker::FAIL_NAME.with(|name| name.set(Some(worker)));
        let dir = PathBuf::from(std::env::var_os("UATU_TEST_DIR").unwrap());
        let mode = std::env::var("UATU_TEST_MODE").unwrap();
        let tail = match mode.as_str() {
            "timeout" | "interrupt" => "while :; do sleep 1; done",
            "long-run" => "sleep 0.3; exit 37",
            _ => "exit 37",
        };
        let script = format!(
            "trap 'exit 37' TERM; printf 'out:\\000\\377\\n'; printf 'err:\\000\\376\\n' >&2; printf x >> \"$UATU_TEST_MARKER\"; {tail}"
        );
        let code = cmd_run(RunArgs {
            name: Some("worker-failure".into()),
            shell: false,
            config: Some(dir.join("config.toml")),
            data_dir: Some(dir.join("state")),
            cwd: None,
            env: vec![(
                "UATU_TEST_MARKER".into(),
                dir.join("started").to_string_lossy().into_owned(),
            )],
            timeout: (mode == "timeout").then_some(Duration::from_millis(150)),
            kill_grace: Some(Duration::from_millis(300)),
            expected_duration: (mode == "long-run").then_some(Duration::from_millis(20)),
            cmd: vec!["/bin/sh".into(), "-c".into(), script.into()],
        });
        std::process::exit(code);
    }

    fn run_failure(failure: &str, mode: &str) -> (tempfile::TempDir, Db) {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("config.toml"),
            if mode == "long-run" {
                "[global]\nmin_free_bytes = \"0 B\"\n[notify]\nevents = [\"long_run\"]\nreporters = [\"discord.d\"]\n[reporters.discord.d]\nwebhook_url = \"http://127.0.0.1:1/unreachable\"\n"
            } else {
                "[global]\nmin_free_bytes = \"0 B\"\n"
            },
        ).unwrap();
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "commands::run::tests::worker_failure_child",
                "--nocapture",
            ])
            .env("UATU_TEST_WORKER_FAILURE", failure)
            .env("UATU_TEST_MODE", mode)
            .env("UATU_TEST_DIR", dir.path())
            .env("XDG_CONFIG_HOME", dir.path().join("xdg"))
            .env("UATU_OVERALL_BUDGET_MS", "300")
            .env("UATU_PER_REPORTER_BUDGET_MS", "100")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(8);
        let mut interrupted = false;
        loop {
            if child.try_wait().unwrap().is_some() {
                break;
            }
            if mode == "interrupt" && !interrupted && dir.path().join("started").exists() {
                assert_eq!(unsafe { libc::kill(child.id() as i32, libc::SIGTERM) }, 0);
                interrupted = true;
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!("worker-failure run did not finish: {failure}/{mode}");
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let output = child.wait_with_output().unwrap();
        assert_eq!(
            output.status.code(),
            Some(if mode == "timeout" { 124 } else { 37 }),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        // The unit-test harness writes its heading before the helper starts;
        // the helper exits directly, leaving the child's bytes as the suffix.
        assert!(output.stdout.ends_with(OUT), "stdout: {:?}", output.stdout);
        assert_eq!(
            output
                .stdout
                .windows(OUT.len())
                .filter(|w| *w == OUT)
                .count(),
            1
        );
        assert_eq!(
            output
                .stderr
                .windows(ERR.len())
                .filter(|w| *w == ERR)
                .count(),
            1
        );
        assert_eq!(
            std::fs::read(dir.path().join("started")).unwrap(),
            b"x",
            "child executed exactly once"
        );
        let db = Db::open(&dir.path().join("state/uatu.db")).unwrap();
        let (count, exit): (u32, i32) = db
            .conn
            .query_row("SELECT COUNT(*), exit_code FROM runs", [], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })
            .unwrap();
        assert_eq!(count, 1);
        assert_eq!(exit, if mode == "timeout" { 124 } else { 37 });
        (dir, db)
    }

    #[test]
    fn failed_pump_reservation_preserves_output_exit_and_history() {
        for failure in ["stdout", "stderr"] {
            let (_dir, db) = run_failure(failure, "normal");
            let (reason, stored, detached): (String, i64, bool) = db
                .conn
                .query_row(
                    "SELECT stdout_reason, stdout_bytes_stored, detached_children FROM runs",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .unwrap();
            assert!(reason.contains("cannot start output workers"));
            assert_eq!(stored, 0);
            assert!(!detached);
        }
    }

    #[test]
    fn failed_pump_reservation_retains_timeout_and_interruption() {
        for failure in ["stdout", "stderr"] {
            let (_dir, db) = run_failure(failure, "timeout");
            let timed_out: bool = db
                .conn
                .query_row("SELECT timeout_fired FROM runs", [], |row| row.get(0))
                .unwrap();
            assert!(timed_out);
            let (_dir, db) = run_failure(failure, "interrupt");
            let signal: String = db
                .conn
                .query_row("SELECT interrupted_by FROM runs", [], |row| row.get(0))
                .unwrap();
            assert_eq!(signal, "SIGTERM");
        }
    }

    #[test]
    fn failed_capture_workers_preserve_output_and_record_omitted_bytes() {
        let (_dir, db) = run_failure("capture", "normal");
        for stream in ["stdout", "stderr"] {
            let (total, stored, omitted, reason, path): (i64, i64, i64, String, Option<String>) = db.conn.query_row(
                &format!("SELECT {stream}_bytes_total, {stream}_bytes_stored, {stream}_bytes_omitted, {stream}_reason, {stream}_path FROM runs"), [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
            ).unwrap();
            assert_eq!(total, OUT.len() as i64);
            assert_eq!(stored, 0);
            assert_eq!(omitted, total);
            assert!(reason.contains("cannot start capture worker"));
            assert!(path.is_none());
        }
    }

    #[test]
    fn failed_long_run_worker_queues_delivery_after_child_exit() {
        let (_dir, db) = run_failure("long-run", "long-run");
        let fired: bool = db
            .conn
            .query_row("SELECT long_run_fired FROM runs", [], |row| row.get(0))
            .unwrap();
        assert!(fired);
        let (event, state, attempts): (String, String, u32) = db
            .conn
            .query_row(
                "SELECT event, state, attempt_count FROM deliveries",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(event, "long_run");
        assert_eq!(state, "queued");
        assert_eq!(
            attempts, 0,
            "failed worker must not trigger synchronous network work"
        );
    }
}
