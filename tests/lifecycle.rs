//! Integration tests: run lifecycle — timeout, interruption, capture modes,
//! degradation, stale recovery, schema mismatch (SPEC §14).

mod common;

use common::{FakeDiscord, TestEnv};
use std::time::{Duration, Instant};

fn discord_config(url: &str, events: &str) -> String {
    format!(
        r#"
[notify]
events = {events}
reporters = ["discord.d"]
[reporters.discord.d]
webhook_url = "{url}"
"#
    )
}

#[test]
fn timeout_term_then_kill_exit_124() {
    let env = TestEnv::new();
    // Child ignores TERM: KILL must finish it after the grace period.
    let started = Instant::now();
    let (code, _, _) = env.run_code(&[
        "run",
        "--timeout",
        "300ms",
        "--kill-grace",
        "400ms",
        "--",
        "sh",
        "-c",
        "trap '' TERM; sleep 10",
    ]);
    let elapsed = started.elapsed();
    assert_eq!(code, 124, "GNU timeout(1) parity");
    assert!(
        elapsed < Duration::from_secs(5),
        "TERM→grace→KILL, not 10s: {elapsed:?}"
    );
    assert!(
        elapsed >= Duration::from_millis(600),
        "grace was honored: {elapsed:?}"
    );
    let run = env.latest_run();
    assert_eq!(run["status"], "timeout");
    assert_eq!(run["exit_code"], 124);
    assert_eq!(run["timeout_fired"], true);
}

#[test]
fn timeout_with_cooperative_child_is_fast() {
    let env = TestEnv::new();
    let started = Instant::now();
    let (code, _, _) = env.run_code(&["run", "--timeout", "200ms", "--", "sleep", "10"]);
    assert_eq!(code, 124);
    assert!(started.elapsed() < Duration::from_secs(2));
}

#[test]
fn timeout_signals_whole_process_group() {
    let env = TestEnv::new();
    // The direct child spawns a grandchild; killpg must reach it. If only the
    // direct child were signaled, the grandchild would hold the pipe open and
    // we would sit in the drain window.
    let (code, _, _) = env.run_code(&[
        "run",
        "--timeout",
        "300ms",
        "--kill-grace",
        "200ms",
        "--",
        "sh",
        "-c",
        "sleep 10 & sleep 10",
    ]);
    assert_eq!(code, 124);
    let run = env.latest_run();
    assert_eq!(
        run["detached_children"], false,
        "group kill reaped the pipe holders"
    );
}

#[test]
fn interruption_forwards_term_records_and_enqueues_only() {
    let server = FakeDiscord::start(vec![]);
    let env = TestEnv::new();
    env.write_config(&discord_config(&server.url(), r#"["success", "failure"]"#));

    let mut child = common::spawn_sleeper(&env, "10", &["--name", "interruptee"]);
    // Give the wrapper time to start its child.
    assert!(common::wait_until(Duration::from_secs(3), || {
        let (_, out, _) = env.run_code(&["status", "--json"]);
        out.contains("interruptee")
    }));
    let started = Instant::now();
    unsafe {
        libc::kill(child.id() as i32, libc::SIGTERM);
    }
    let status = child.wait().unwrap();
    assert!(started.elapsed() < Duration::from_secs(5), "exits promptly");
    // Child died from TERM → 128+15.
    assert_eq!(status.code(), Some(143));

    let run = env.latest_run();
    assert_eq!(run["interrupted_by"], "SIGTERM");
    assert_eq!(run["status"], "failure");
    assert_eq!(run["signal"], 15);

    // Events were enqueued, not sent (SPEC §3 interruption).
    assert_eq!(server.hit_count(), 0, "no synchronous sends while dying");
    let db = env.db();
    let n: i64 = db
        .conn
        .query_row(
            "SELECT COUNT(*) FROM deliveries WHERE state='queued' AND event='failure'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(n, 1, "failure event queued for flush");

    // flush delivers it later.
    env.run_ok(&["flush"]);
    assert_eq!(server.hit_count(), 1);
}

#[test]
fn capped_capture_head_marker_tail() {
    let env = TestEnv::new();
    env.write_config(
        r#"
[global]
capture_head_bytes = "1KB"
capture_tail_bytes = "1KB"
"#,
    );
    // seq 1..1000 ≈ 3.9 KB of output.
    let (code, _, _) = env.run_code(&["run", "--name", "capped", "--", "seq", "1", "1000"]);
    assert_eq!(code, 0);
    let run = env.latest_run();
    let path = run["stdout"]["path"].as_str().unwrap();
    let content = std::fs::read_to_string(path).unwrap();
    assert!(content.starts_with("1\n2\n3\n"), "head preserved");
    assert!(
        content.contains("bytes omitted (capped capture)"),
        "marker present"
    );
    assert!(content.ends_with("999\n1000\n"), "tail preserved");

    let total = run["stdout"]["bytes_total"].as_u64().unwrap();
    let stored = run["stdout"]["bytes_stored"].as_u64().unwrap();
    let omitted = run["stdout"]["bytes_omitted"].as_u64().unwrap();
    assert_eq!(total, 3893, "seq 1 1000 byte count");
    assert_eq!(stored + omitted, total, "accounting adds up");
    assert!((1900..=2000).contains(&stored), "≈head+tail: {stored}");
}

#[test]
fn capture_off_keeps_no_files_but_counts() {
    let env = TestEnv::new();
    env.write_config("[global]\ncapture_mode = \"off\"\n");
    let out = env.cmd(&["run", "--", "echo", "hello"]).output().unwrap();
    assert_eq!(out.stdout, b"hello\n", "passthrough unaffected");
    let run = env.latest_run();
    assert!(run["stdout"]["path"].is_null());
    assert_eq!(run["stdout"]["bytes_stored"], 0);
}

#[test]
fn full_capture_mode_stores_everything() {
    let env = TestEnv::new();
    env.write_config("[global]\ncapture_mode = \"full\"\ncapture_head_bytes = \"1KB\"\ncapture_tail_bytes = \"1KB\"\n");
    env.run_ok(&["run", "--", "seq", "1", "1000"]);
    let run = env.latest_run();
    assert_eq!(run["stdout"]["bytes_stored"], 3893);
    assert_eq!(run["stdout"]["bytes_omitted"], 0);
}

#[test]
fn preflight_low_space_degrades_to_metadata_only() {
    let env = TestEnv::new();
    // An impossible threshold forces the preflight to trip.
    env.write_config("[global]\nmin_free_bytes = \"1000000GB\"\n");
    let (code, out, err) = env.run_code(&["run", "--", "echo", "still-streams"]);
    assert_eq!(code, 0, "job continuity over observability");
    assert_eq!(out, "still-streams\n", "passthrough unaffected");
    assert!(err.contains("min_free_bytes"), "warning expected: {err}");
    let run = env.latest_run();
    assert_eq!(run["status"], "success");
    assert_eq!(run["stdout"]["bytes_stored"], 0, "no capture");
    assert!(
        run["stdout"]["reason"]
            .as_str()
            .unwrap_or("")
            .contains("low free space"),
        "degradation recorded: {run}"
    );
}

#[test]
fn redaction_applies_to_capture_and_argv_but_not_passthrough() {
    let env = TestEnv::new();
    env.write_config(
        r#"
[redaction]
literals = ["hunter2"]
regex = ['''password=[^ ]+''']
"#,
    );
    let out = env
        .cmd(&[
            "run",
            "--name",
            "redact-me",
            "--",
            "sh",
            "-c",
            "echo token hunter2 password=abc",
        ])
        .output()
        .unwrap();
    // Raw passthrough is NOT redacted (SPEC §9).
    assert_eq!(out.stdout, b"token hunter2 password=abc\n");
    // Captured file is redacted.
    let run = env.latest_run();
    let content = std::fs::read_to_string(run["stdout"]["path"].as_str().unwrap()).unwrap();
    assert_eq!(content, "token [REDACTED] [REDACTED]\n");
    // Stored argv passes the redaction pipeline too (SPEC §9).
    let argv = serde_json::to_string(&run["argv"]).unwrap();
    assert!(!argv.contains("hunter2"), "argv redacted: {argv}");
    assert!(argv.contains("[REDACTED]"));
}

#[test]
fn invalid_redaction_runs_metadata_only_reporters_disabled() {
    let server = FakeDiscord::start(vec![]);
    let env = TestEnv::new();
    env.write_config(&format!(
        r#"
[redaction]
regex = ["[unclosed"]
{}"#,
        discord_config(&server.url(), r#"["success", "failure"]"#)
    ));
    let (code, out, err) = env.run_code(&["run", "--", "echo", "secret-data"]);
    assert_eq!(code, 0, "the job still runs: {err}");
    assert_eq!(out, "secret-data\n", "raw streaming unchanged");
    assert!(err.contains("metadata-only"), "{err}");

    let run = env.latest_run();
    assert_eq!(run["status"], "success");
    assert!(
        run["argv"].is_null(),
        "argv not stored without working redaction"
    );
    assert_eq!(run["stdout"]["bytes_stored"], 0, "no capture persisted");
    assert_eq!(server.hit_count(), 0, "reporters disabled");
    let db = env.db();
    let n: i64 = db
        .conn
        .query_row("SELECT COUNT(*) FROM deliveries", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 0, "nothing queued either");
}

#[test]
fn invalid_toml_still_runs_local_only() {
    let env = TestEnv::new();
    env.write_config("this is { not toml ===");
    let (code, out, err) = env.run_code(&["run", "--", "echo", "ok"]);
    assert_eq!(code, 0);
    assert_eq!(out, "ok\n");
    assert!(
        err.contains("invalid TOML") || err.contains("local-only"),
        "{err}"
    );
    assert_eq!(env.latest_run()["status"], "success");
}

#[test]
fn unknown_config_key_warns_but_reporters_still_work() {
    let server = FakeDiscord::start(vec![]);
    let env = TestEnv::new();
    env.write_config(&format!(
        "{}\n[global]\ntypo_key = true\n",
        discord_config(&server.url(), r#"["success"]"#)
    ));
    let (code, _, err) = env.run_code(&["run", "--", "true"]);
    assert_eq!(code, 0);
    assert!(
        err.contains("typo_key"),
        "runtime warning lands in cron MAILTO: {err}"
    );
    assert_eq!(
        server.hit_count(),
        1,
        "lenient runtime keeps reporting alive"
    );
}

#[test]
fn state_failure_still_runs_child_passthrough() {
    let env = TestEnv::new();
    // Point --data-dir at a FILE: state cannot be prepared.
    let blocker = env.dir.path().join("blocker");
    std::fs::write(&blocker, "x").unwrap();
    let out = std::process::Command::new(common::uatu_bin())
        .args([
            "run",
            "--data-dir",
            blocker.to_str().unwrap(),
            "--",
            "sh",
            "-c",
            "echo through; exit 9",
        ])
        .env("XDG_CONFIG_HOME", env.dir.path().join("none"))
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(9), "exit preserved");
    assert_eq!(out.stdout, b"through\n", "passthrough works");
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("passthrough"), "{err}");
}

#[test]
fn newer_db_schema_degrades_safely() {
    let env = TestEnv::new();
    env.run_ok(&["run", "--", "true"]);
    {
        let db = env.db();
        db.conn.pragma_update(None, "user_version", 99).unwrap();
    }
    // run: passthrough degradation, child still runs with its exit code.
    let (code, out, err) = env.run_code(&["run", "--", "sh", "-c", "echo alive; exit 5"]);
    assert_eq!(code, 5);
    assert!(out.contains("alive"));
    assert!(err.contains("older than its database"), "{err}");
    // Inspection commands: clear error (SPEC §7).
    let (code, _, err) = env.run_code(&["history"]);
    assert_eq!(code, 1);
    assert!(err.contains("older than its database"), "{err}");
}

#[test]
fn stale_detection_pid_reuse_and_reboot() {
    let server = FakeDiscord::start(vec![]);
    let env = TestEnv::new();
    env.write_config(&discord_config(&server.url(), r#"["failure", "stale"]"#));
    env.run_ok(&["run", "--", "true"]); // create schema
    let me = uatu::liveness::current();
    let now = uatu::util::now_ms();
    {
        let db = env.db();
        // R1: nonexistent pid.
        common::insert_run_row(
            &db,
            "STALE0PID11111111111111111",
            "dead-pid",
            "active",
            now - 60_000,
            0x3ffff0,
            1,
            &me.boot_id,
        );
        // R2: pid reuse — OUR live pid but wrong start ticks.
        common::insert_run_row(
            &db,
            "STALE0TICKS111111111111111",
            "pid-reuse",
            "active",
            now - 60_000,
            me.pid as i64,
            me.start_ticks as i64 + 7,
            &me.boot_id,
        );
        // R3: boot id mismatch (reboot) — our pid+ticks, stale boot.
        common::insert_run_row(
            &db,
            "STALE0BOOT1111111111111111",
            "rebooted",
            "active",
            now - 60_000,
            me.pid as i64,
            me.start_ticks as i64,
            "00000000-0000-0000-0000-000000000000",
        );
        // R4: genuinely alive (this test process's identity).
        common::insert_run_row(
            &db,
            "ALIVE000001111111111111111",
            "alive-job",
            "active",
            now - 60_000,
            me.pid as i64,
            me.start_ticks as i64,
            &me.boot_id,
        );
    }

    // `status` reconciles + enqueues, but never touches the network (SPEC §3).
    let (_, out, _) = env.run_code(&["status"]);
    assert!(out.contains("STALE"), "{out}");
    assert!(
        out.contains("alive-job"),
        "live wrapper stays active: {out}"
    );
    assert_eq!(server.hit_count(), 0, "inspection commands never deliver");

    let db = env.db();
    for (run, expect) in [
        ("STALE0PID11111111111111111", "stale"),
        ("STALE0TICKS111111111111111", "stale"),
        ("STALE0BOOT1111111111111111", "stale"),
        ("ALIVE000001111111111111111", "active"),
    ] {
        let status = db.get_run(run).unwrap().unwrap().status;
        assert_eq!(status, expect, "{run}");
    }
    // Stale events enqueued (3 dead wrappers), end timestamp = detection time.
    let queued: i64 = db
        .conn
        .query_row(
            "SELECT COUNT(*) FROM deliveries WHERE event='stale' AND state='queued'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(queued, 3);
    let stale = db.get_run("STALE0PID11111111111111111").unwrap().unwrap();
    assert!(stale.end_is_detection);

    // flush delivers the stale alerts.
    env.run_ok(&["flush"]);
    assert_eq!(server.hit_count(), 3);
    let embed = server.embed(0);
    assert!(
        embed["title"].as_str().unwrap().starts_with("STALE:"),
        "{embed}"
    );
    assert_eq!(embed["color"], 0x95A5A6, "stale is grey");
}

#[test]
fn long_run_alert_fires_once_mid_run() {
    let server = FakeDiscord::start(vec![]);
    let env = TestEnv::new();
    // Reporter configured but long_run NOT in config events: the CLI flag
    // --expected-duration implies the alert (SPEC §3 flag doc).
    env.write_config(&discord_config(&server.url(), r#"["failure"]"#));
    let started = Instant::now();
    let (code, _, _) = env.run_code(&[
        "run",
        "--name",
        "slowpoke",
        "--expected-duration",
        "300ms",
        "--",
        "sleep",
        "1",
    ]);
    assert_eq!(code, 0);
    assert!(started.elapsed() >= Duration::from_secs(1));
    let run = env.latest_run();
    assert_eq!(run["long_run_fired"], true);
    assert_eq!(run["status"], "success");
    // Exactly one long_run delivery; the run completed afterwards normally.
    assert!(
        common::wait_until(Duration::from_secs(2), || server.hit_count() >= 1),
        "long_run alert delivered"
    );
    assert_eq!(server.hit_count(), 1, "fires once, success not in events");
    let embed = server.embed(0);
    assert!(
        embed["title"].as_str().unwrap().starts_with("LONG_RUN:"),
        "{embed}"
    );
    // Delivered mid-run, not after: the run took 1s, the alert came at ~300ms.
    let db = env.db();
    let row = &db
        .deliveries_for_run(run["run_id"].as_str().unwrap())
        .unwrap()[0];
    assert_eq!(row.event, "long_run");
    assert_eq!(row.state, "delivered");
    let delivered = row.delivered_ms.unwrap();
    assert!(
        delivered - row.created_ms < 700,
        "sent at detection time, not run end"
    );
}

#[test]
fn detached_children_noted_and_do_not_block_exit() {
    let env = TestEnv::new();
    let started = Instant::now();
    // Direct child exits immediately; a background grandchild keeps the pipe.
    let (code, _, _) = env.run_code(&["run", "--", "sh", "-c", "sleep 5 & exit 0"]);
    let elapsed = started.elapsed();
    assert_eq!(code, 0);
    assert!(
        elapsed < Duration::from_secs(4),
        "did not wait for the detached child: {elapsed:?}"
    );
    let run = env.latest_run();
    assert_eq!(run["status"], "success");
    assert_eq!(run["detached_children"], true, "noted in metadata");
}

#[test]
fn capture_dir_failure_degrades_but_run_succeeds() {
    let env = TestEnv::new();
    // Run once to create the state tree, then make output/ unwritable.
    env.run_ok(&["run", "--", "true"]);
    let output_dir = env.state_dir().join("output");
    let mut perms = std::fs::metadata(&output_dir).unwrap().permissions();
    use std::os::unix::fs::PermissionsExt;
    perms.set_mode(0o500);
    std::fs::set_permissions(&output_dir, perms).unwrap();

    let (code, out, _) = env.run_code(&["run", "--name", "degraded", "--", "echo", "fine"]);
    let mut perms = std::fs::metadata(&output_dir).unwrap().permissions();
    perms.set_mode(0o700);
    std::fs::set_permissions(&output_dir, perms).unwrap();

    assert_eq!(code, 0, "child unaffected by capture failure");
    assert_eq!(out, "fine\n");
    let run = env.latest_run();
    assert_eq!(run["status"], "success");
    assert!(
        run["stdout"]["reason"]
            .as_str()
            .unwrap_or("")
            .contains("output dir"),
        "degradation reason recorded: {run}"
    );
}

#[test]
fn concurrent_wrappers_on_fresh_state_all_succeed() {
    // SPEC §7: top-of-the-hour bursts are the normal case — including the
    // worst variant, many wrappers racing to create + migrate a fresh db.
    let env = TestEnv::new();
    let children: Vec<_> = (0..10)
        .map(|i| {
            let name = format!("burst-{i}");
            env.cmd(&["run", "--name", &name, "--", "true"])
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .unwrap()
        })
        .collect();
    for child in children {
        let out = child.wait_with_output().unwrap();
        assert_eq!(
            out.status.code(),
            Some(0),
            "stderr: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let err = String::from_utf8_lossy(&out.stderr);
        assert!(
            !err.contains("passthrough") && !err.contains("cannot record"),
            "no wrapper silently degraded: {err}"
        );
    }
    let runs = env.history_json();
    assert_eq!(runs.as_array().unwrap().len(), 10, "all 10 runs recorded");
    // WAL + busy_timeout actually in effect (SPEC §7 storage tests).
    let db = env.db();
    let mode: String = db
        .conn
        .query_row("PRAGMA journal_mode", [], |r| r.get(0))
        .unwrap();
    assert_eq!(mode.to_lowercase(), "wal");
}

#[test]
fn state_files_have_restrictive_modes() {
    use std::os::unix::fs::PermissionsExt;
    let env = TestEnv::new();
    env.run_ok(&["run", "--name", "modes", "--", "echo", "hi"]);
    let mode = |p: &std::path::Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode(&env.state_dir()), 0o700);
    assert_eq!(mode(&env.state_dir().join("output")), 0o700);
    assert_eq!(mode(&env.state_dir().join("uatu.db")), 0o600);
    let run = env.latest_run();
    let stdout_path = std::path::PathBuf::from(run["stdout"]["path"].as_str().unwrap());
    assert_eq!(mode(&stdout_path), 0o600);
    assert_eq!(mode(stdout_path.parent().unwrap()), 0o700);
}
