//! Integration tests: CLI surface, exit codes, streaming, identity, JSON
//! (SPEC §14 integration list, CLI parts).

mod common;

use common::TestEnv;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;

#[test]
fn run_true_exits_0() {
    let env = TestEnv::new();
    let (code, _, _) = env.run_code(&["run", "--", "true"]);
    assert_eq!(code, 0);
}

#[test]
fn run_exit_7_preserved() {
    let env = TestEnv::new();
    let (code, _, _) = env.run_code(&["run", "--", "sh", "-c", "exit 7"]);
    assert_eq!(code, 7);
}

#[test]
fn stdout_and_stderr_stream_byte_for_byte_and_are_captured() {
    let env = TestEnv::new();
    let out = env
        .cmd(&[
            "run",
            "--name",
            "stream-test",
            "--",
            "sh",
            "-c",
            "printf 'out-a\\nout-b'; printf 'err-x\\nerr-y' >&2",
        ])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(0));
    // Byte-for-byte passthrough, no trailing-newline normalization.
    assert_eq!(out.stdout, b"out-a\nout-b");
    assert_eq!(out.stderr, b"err-x\nerr-y");
    // Captured to files too.
    let run = env.latest_run();
    let stdout_path = run["stdout"]["path"].as_str().expect("stdout path");
    let stderr_path = run["stderr"]["path"].as_str().expect("stderr path");
    assert_eq!(std::fs::read(stdout_path).unwrap(), b"out-a\nout-b");
    assert_eq!(std::fs::read(stderr_path).unwrap(), b"err-x\nerr-y");
    assert_eq!(run["stdout"]["bytes_total"], 11);
    assert_eq!(run["stderr"]["bytes_total"], 11);
}

#[test]
fn shell_mode_uses_sh_dash_c_and_returns_3() {
    let env = TestEnv::new();
    // SHELL is removed by the harness → /bin/sh; `&&` proves a real shell.
    let out = env
        .cmd(&["run", "--shell", "--", "echo hi && exit 3"])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(3));
    assert_eq!(out.stdout, b"hi\n");
    let run = env.latest_run();
    assert_eq!(run["mode"], "shell");
    assert_eq!(run["shell_cmd"], "echo hi && exit 3");
    assert!(run["job_id"].as_str().unwrap().starts_with("shell-"));
}

#[test]
fn shell_mode_respects_shell_env_var() {
    let env = TestEnv::new();
    // Point SHELL at a wrapper that proves it was used (and used with -c).
    let fake_shell = env.dir.path().join("fakeshell.sh");
    std::fs::write(
        &fake_shell,
        "#!/bin/sh\necho fakeshell:$1\nexec /bin/sh \"$@\"\n",
    )
    .unwrap();
    std::fs::set_permissions(&fake_shell, std::fs::Permissions::from_mode(0o755)).unwrap();
    let out = env
        .cmd(&["run", "--shell", "--", "exit 0"])
        .env("SHELL", &fake_shell)
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(0));
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("fakeshell:-c"), "got: {stdout}");
}

#[test]
fn shell_mode_requires_exactly_one_arg() {
    let env = TestEnv::new();
    let (code, _, err) = env.run_code(&["run", "--shell", "--", "echo", "two"]);
    assert_eq!(code, 2);
    assert!(err.contains("exactly one command string"), "{err}");
}

#[test]
fn usage_errors_exit_2() {
    let env = TestEnv::new();
    // invalid --name slug
    let (code, _, err) = env.run_code(&["run", "--name", "bad name!", "--", "true"]);
    assert_eq!(code, 2, "{err}");
    // invalid --env (bad variable name)
    let (code, _, _) = env.run_code(&["run", "--env", "1BAD=x", "--", "true"]);
    assert_eq!(code, 2);
    // invalid duration
    let (code, _, _) = env.run_code(&["run", "--timeout", "5parsecs", "--", "true"]);
    assert_eq!(code, 2);
    // missing command
    let (code, _, _) = env.run_code(&["run"]);
    assert_eq!(code, 2);
    // run-id prefix too short
    let (code, _, _) = env.run_code(&["show", "abc"]);
    assert_eq!(code, 2);
}

#[test]
fn exec_failures_map_to_127_126_125() {
    let env = TestEnv::new();
    // 127: not found
    let (code, _, _) = env.run_code(&["run", "--", "/definitely/not/a/binary"]);
    assert_eq!(code, 127);
    // 126: found but not executable
    let not_exec = env.dir.path().join("not-exec.sh");
    std::fs::write(&not_exec, "#!/bin/sh\ntrue\n").unwrap();
    std::fs::set_permissions(&not_exec, std::fs::Permissions::from_mode(0o644)).unwrap();
    let (code, _, _) = env.run_code(&["run", "--", not_exec.to_str().unwrap()]);
    assert_eq!(code, 126);
    // 125: internal pre-start failure (cwd missing)
    let (code, _, err) = env.run_code(&["run", "--cwd", "/no/such/dir", "--", "true"]);
    assert_eq!(code, 125, "{err}");
    // All recorded as start_failed with the exit code preserved.
    let runs = env.history_json();
    let statuses: Vec<&str> = runs
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["status"].as_str().unwrap())
        .collect();
    assert_eq!(statuses, vec!["start_failed"; 3]);
}

#[test]
fn cwd_and_env_apply_to_child() {
    let env = TestEnv::new();
    let subdir = env.dir.path().join("workdir");
    std::fs::create_dir(&subdir).unwrap();
    let out = env
        .cmd(&[
            "run",
            "--cwd",
            subdir.to_str().unwrap(),
            "--env",
            "UATU_TEST_VAR=hello",
            "--",
            "sh",
            "-c",
            "pwd; printf '%s' \"$UATU_TEST_VAR\"",
        ])
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("workdir"), "{stdout}");
    assert!(stdout.ends_with("hello"), "{stdout}");
    // env stored as names only (SPEC §9).
    let run = env.latest_run();
    assert_eq!(run["env_names"], serde_json::json!(["UATU_TEST_VAR"]));
    let dumped = serde_json::to_string(&run).unwrap();
    assert!(!dumped.contains("hello"), "env value must never be stored");
}

#[test]
fn configless_local_only_works() {
    let env = TestEnv::new();
    // No config file at all: run + history still work.
    env.run_ok(&["run", "--", "true"]);
    let runs = env.history_json();
    assert_eq!(runs.as_array().unwrap().len(), 1);
    assert_eq!(runs[0]["status"], "success");
    // And no deliveries were created (no reporters configured).
    let db = env.db();
    let n: i64 = db
        .conn
        .query_row("SELECT COUNT(*) FROM deliveries", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 0);
}

#[test]
fn history_filters_and_limit() {
    let env = TestEnv::new();
    env.run_ok(&["run", "--name", "job-a", "--", "true"]);
    let (c, _, _) = env.run_code(&["run", "--name", "job-b", "--", "false"]);
    assert_eq!(c, 1);
    env.run_ok(&["run", "--name", "job-a", "--", "true"]);

    let all = env.history_json();
    assert_eq!(all.as_array().unwrap().len(), 3);

    let out = env.run_ok(&["history", "--job", "job-a", "--json"]);
    let ja: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(ja.as_array().unwrap().len(), 2);

    let out = env.run_ok(&["history", "--status", "failure", "--json"]);
    let jf: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(jf.as_array().unwrap().len(), 1);
    assert_eq!(jf[0]["job_id"], "job-b");

    let out = env.run_ok(&["history", "--limit", "1", "--json"]);
    let j1: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(j1.as_array().unwrap().len(), 1);

    // Human output includes the essentials (SPEC §3).
    let out = env.run_ok(&["history"]);
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(text.contains("RUN ID") && text.contains("STATUS") && text.contains("DURATION"));
    assert!(text.contains("job-a") && text.contains("job-b"));
}

#[test]
fn json_contract_fields_pinned() {
    let env = TestEnv::new();
    env.run_ok(&["run", "--name", "contract", "--", "true"]);
    let run = env.latest_run();
    // Timestamps: RFC3339 UTC strings.
    let started = run["started_at"].as_str().unwrap();
    assert!(started.ends_with('Z') && started.contains('T'), "{started}");
    // Durations: integer ms. Byte counts: integers. Statuses: snake_case.
    assert!(run["duration_ms"].is_i64() || run["duration_ms"].is_u64());
    assert!(run["stdout"]["bytes_total"].is_u64() || run["stdout"]["bytes_total"].is_i64());
    assert_eq!(run["status"], "success");
    // Absent values are null, not sentinel strings.
    assert!(run["signal"].is_null());
    assert!(run["interrupted_by"].is_null());
}

#[test]
fn show_resolves_unique_prefix_and_errors_on_ambiguous() {
    let env = TestEnv::new();
    env.run_ok(&[
        "run",
        "--name",
        "showme",
        "--",
        "sh",
        "-c",
        "echo captured-line",
    ]);
    let full_id = env.latest_run()["run_id"].as_str().unwrap().to_string();

    // Unique full id and unique long prefix work; --stdout prints capture.
    let out = env.run_ok(&["show", &full_id, "--stdout"]);
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(text.contains("showme"), "{text}");
    assert!(text.contains("captured-line"), "{text}");
    let prefix = &full_id[..20];
    env.run_ok(&["show", prefix]);

    // JSON variant exposes the content fields only when asked.
    let v = env.show_json(&full_id);
    assert!(v.get("stdout_content").is_none());
    let out = env.run_ok(&["show", &full_id, "--json", "--stdout"]);
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(v["stdout_content"], "captured-line\n");

    // Ambiguous prefix: ULIDs created in the same ms share 4+ chars — force
    // ambiguity via fake rows.
    let db = env.db();
    common::insert_run_row(
        &db,
        "AMBIG00A11111111111111111A",
        "x",
        "success",
        1,
        1,
        1,
        "b",
    );
    common::insert_run_row(
        &db,
        "AMBIG00B11111111111111111B",
        "x",
        "success",
        2,
        1,
        1,
        "b",
    );
    let (code, _, err) = env.run_code(&["show", "AMBIG"]);
    assert_eq!(code, 1);
    assert!(err.contains("ambiguous"), "{err}");
    assert!(err.contains("AMBIG00A") && err.contains("AMBIG00B"));

    // Unknown prefix.
    let (code, _, err) = env.run_code(&["show", "ZZZZZZ"]);
    assert_eq!(code, 1);
    assert!(err.contains("no run matches"), "{err}");
}

#[test]
fn status_shows_active_run_with_pids() {
    let env = TestEnv::new();
    let mut child = common::spawn_sleeper(&env, "2", &["--name", "sleepy"]);
    if !common::wait_until(std::time::Duration::from_secs(3), || {
        let (_, out, _) = env.run_code(&["status"]);
        out.contains("sleepy") && out.contains("active")
    }) {
        let (_, status, serr) = env.run_code(&["status"]);
        let (_, hist, _) = env.run_code(&["history"]);
        panic!("active run never appeared.\nstatus:\n{status}\n{serr}\nhistory:\n{hist}");
    }
    let (_, out, _) = env.run_code(&["status", "--json"]);
    let v: serde_json::Value = serde_json::from_str(&out).unwrap();
    let run = &v[0];
    assert_eq!(run["status"], "active");
    assert!(run["wrapper_pid"].as_i64().unwrap() > 0);
    assert!(run["child_pid"].as_i64().unwrap() > 0);
    assert!(run["ended_at"].is_null());
    child.wait().unwrap();
    let (_, out, _) = env.run_code(&["status"]);
    assert!(out.contains("no active or stale runs"), "{out}");
}

#[test]
fn inferred_identity_is_stable_and_fragmentation_hint_fires() {
    let env = TestEnv::new();
    env.run_ok(&["run", "--", "true"]);
    env.run_ok(&["run", "--", "true"]);
    let runs = env.history_json();
    assert_eq!(runs[0]["job_id"], runs[1]["job_id"], "same line, same id");
    assert_eq!(runs[0]["job_id_inferred"], true);

    // >10 distinct inferred ids on one basename within 30 days → hint.
    for i in 0..11 {
        env.run_ok(&["run", "--", "true", &format!("arg-{i}")]);
    }
    let out = env.run_ok(&["history"]);
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(
        text.contains("distinct inferred job ids") && text.contains("--name"),
        "hint expected, got:\n{text}"
    );
}

#[test]
fn cron_example_emits_absolute_path_and_flush_line() {
    let env = TestEnv::new();
    let out = env.run_ok(&["cron", "example", "--name", "backup"]);
    let text = String::from_utf8_lossy(&out.stdout);
    let bin = common::uatu_bin();
    assert!(
        text.contains(bin),
        "must embed the absolute binary path:\n{text}"
    );
    assert!(!text.contains("\nuatu "), "never a bare `uatu`");
    assert!(text.contains("PATH"), "PATH warning comment");
    assert!(
        text.contains(&format!("*/10 * * * * {bin} flush")),
        "flush line:\n{text}"
    );
    assert!(text.contains("--name backup"));
    // Shell variant.
    let out = env.run_ok(&["cron", "example", "--shell"]);
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(text.contains("--shell -- '"), "{text}");
}

#[test]
fn init_writes_refuses_and_forces() {
    let env = TestEnv::new();
    let target = env.dir.path().join("cfg").join("uatu.toml");
    let t = target.to_str().unwrap();
    let (code, _, _) = env.run_code(&["init", "--path", t]);
    assert_eq!(code, 0);
    assert!(target.exists());
    // Refuses to overwrite without --force.
    let (code, _, err) = env.run_code(&["init", "--path", t]);
    assert_eq!(code, 1);
    assert!(err.contains("--force"), "{err}");
    let (code, _, _) = env.run_code(&["init", "--path", t, "--force"]);
    assert_eq!(code, 0);
    // --stdout prints without writing.
    let (code, out, _) = env.run_code(&["init", "--stdout"]);
    assert_eq!(code, 0);
    assert!(
        out.contains("uatu config validate"),
        "ends with validate hint"
    );
    assert!(out.contains("uatu notify test"));
    // The generated file validates cleanly.
    let (code, out, _) = env.run_code(&["config", "validate", "--config", t]);
    assert_eq!(code, 0, "{out}");
}

#[test]
fn validate_strictness() {
    let env = TestEnv::new();

    // Unknown key → hard error naming key and table.
    env.write_config("[global]\nkil_grace = \"30s\"\n");
    let (code, out, _) = env.run_code(&["config", "validate"]);
    assert_eq!(code, 1);
    assert!(
        out.contains("kil_grace") && out.contains("[global]"),
        "{out}"
    );

    // Bad duration → error.
    env.write_config("[retention]\nmax_age = \"3 fortnights\"\n");
    let (code, out, _) = env.run_code(&["config", "validate"]);
    assert_eq!(code, 1);
    assert!(out.contains("retention"), "{out}");

    // Invalid redaction regex → error (unsafe config).
    env.write_config("[redaction]\nregex = [\"[unclosed\"]\n");
    let (code, out, _) = env.run_code(&["config", "validate"]);
    assert_eq!(code, 1);
    assert!(out.contains("redaction"), "{out}");

    // Undefined reporter reference → error.
    env.write_config("[notify]\nreporters = [\"discord.nope\"]\n");
    let (code, out, _) = env.run_code(&["config", "validate"]);
    assert_eq!(code, 1);
    assert!(out.contains("discord.nope"), "{out}");

    // Bad event name → error.
    env.write_config("[notify]\nevents = [\"sucess\"]\n");
    let (code, out, _) = env.run_code(&["config", "validate"]);
    assert_eq!(code, 1);
    assert!(out.contains("sucess"), "{out}");

    // expected_duration > timeout → warning (exit 0), plus the
    // execution-affecting note (SPEC §3).
    env.write_config("[jobs.j]\ncwd = \"/tmp\"\ntimeout = \"10m\"\nexpected_duration = \"1h\"\n");
    let (code, out, _) = env.run_code(&["config", "validate"]);
    assert_eq!(code, 0, "{out}");
    assert!(
        out.contains("warning") && out.contains("expected_duration"),
        "{out}"
    );
    assert!(out.contains("note") && out.contains("cwd"), "{out}");

    // Valid config passes.
    env.write_config(
        r#"
[notify]
events = ["failure", "recovery"]
reporters = ["discord.d"]
[reporters.discord.d]
webhook_url = "https://discord.com/api/webhooks/1/x"
"#,
    );
    let (code, out, _) = env.run_code(&["config", "validate"]);
    assert_eq!(code, 0, "{out}");
    assert!(out.contains("config OK"), "{out}");
}

#[test]
fn run_id_is_ulid_and_sortable() {
    let env = TestEnv::new();
    env.run_ok(&["run", "--", "true"]);
    let id = env.latest_run()["run_id"].as_str().unwrap().to_string();
    assert_eq!(id.len(), 26, "ULID length");
    assert!(id.bytes().all(|b| b.is_ascii_alphanumeric()));
}

#[test]
fn passthrough_handles_binary_bytes() {
    let env = TestEnv::new();
    let out = env
        .cmd(&["run", "--", "sh", "-c", "printf '\\000\\001\\377\\376ok'"])
        .output()
        .unwrap();
    assert_eq!(out.stdout, b"\x00\x01\xff\xfeok");
    let mut sink = std::io::sink();
    sink.write_all(&out.stdout).unwrap();
}
