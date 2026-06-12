//! Reporter tests (SPEC §14): Discord embeds, SMTP, routing, budgets, retry
//! queue, flush lock, notify test.

mod common;

use common::{Behavior, FakeDiscord, FakeSmtp, TestEnv};
use std::time::{Duration, Instant};

fn discord_config(url: &str, events: &str) -> String {
    format!(
        r#"
[global]
host_name = "test-host-01"
[notify]
events = {events}
reporters = ["discord.d"]
[reporters.discord.d]
webhook_url = "{url}"
"#
    )
}

#[test]
fn discord_receives_expected_embed_payload() {
    let server = FakeDiscord::start(vec![]);
    let env = TestEnv::new();
    env.write_config(&discord_config(&server.url(), r#"["success", "failure"]"#));
    env.run_ok(&["run", "--name", "embed-job", "--", "true"]);

    assert_eq!(server.hit_count(), 1);
    let embed = server.embed(0);
    assert_eq!(embed["title"], "SUCCESS: embed-job");
    assert_eq!(embed["color"], 0x2ECC71, "success is green");
    let desc = embed["description"].as_str().unwrap();
    assert!(desc.contains("host: test-host-01"), "{desc}");
    assert!(desc.contains("run: "), "{desc}");
    assert!(desc.contains("exit code 0"), "{desc}");
    assert!(
        desc.contains("started: <t:"),
        "Discord timestamp markup: {desc}"
    );
    // Delivery row is `delivered`.
    let db = env.db();
    let row: String = db
        .conn
        .query_row("SELECT state FROM deliveries", [], |r| r.get(0))
        .unwrap();
    assert_eq!(row, "delivered");
}

#[test]
fn success_compact_failure_includes_redacted_tails() {
    let server = FakeDiscord::start(vec![]);
    let env = TestEnv::new();
    env.write_config(&format!(
        "{}\n[redaction]\nliterals = [\"sekrit\"]\n",
        discord_config(&server.url(), r#"["success", "failure"]"#)
    ));

    env.run_ok(&[
        "run",
        "--name",
        "quiet-ok",
        "--",
        "sh",
        "-c",
        "echo all good",
    ]);
    assert_eq!(server.hit_count(), 1);
    let ok_desc = server.embed(0)["description"].as_str().unwrap().to_string();
    assert!(
        !ok_desc.contains("stdout (tail)") && !ok_desc.contains("all good"),
        "success notifications are compact by default: {ok_desc}"
    );

    let (code, _, _) = env.run_code(&[
        "run",
        "--name",
        "loud-fail",
        "--",
        "sh",
        "-c",
        "echo out with sekrit; echo bad stuff >&2; exit 1",
    ]);
    assert_eq!(code, 1);
    assert_eq!(server.hit_count(), 2);
    let fail_embed = server.embed(1);
    assert_eq!(fail_embed["color"], 0xE74C3C, "failure is red");
    let desc = fail_embed["description"].as_str().unwrap();
    assert!(
        desc.contains("stdout (tail)") && desc.contains("stderr (tail)"),
        "{desc}"
    );
    assert!(desc.contains("bad stuff"), "{desc}");
    assert!(
        desc.contains("[REDACTED]") && !desc.contains("sekrit"),
        "snippets come from the redacted capture: {desc}"
    );
    assert!(
        desc.contains("output files:"),
        "local paths included: {desc}"
    );
    assert!(desc.contains("exit code 1"), "{desc}");
}

#[test]
fn failure_output_false_suppresses_snippets() {
    let server = FakeDiscord::start(vec![]);
    let env = TestEnv::new();
    env.write_config(&format!(
        r#"
[notify]
events = ["failure"]
reporters = ["discord.d"]
failure_output = false
[reporters.discord.d]
webhook_url = "{}"
"#,
        server.url()
    ));
    let _ = env.run_code(&[
        "run",
        "--name",
        "hush",
        "--",
        "sh",
        "-c",
        "echo noisy; exit 1",
    ]);
    assert_eq!(server.hit_count(), 1);
    let desc = server.embed(0)["description"].as_str().unwrap().to_string();
    assert!(!desc.contains("noisy"), "{desc}");
}

#[test]
fn timeout_reports_as_failure_event_with_detail_orange() {
    let server = FakeDiscord::start(vec![]);
    let env = TestEnv::new();
    env.write_config(&discord_config(&server.url(), r#"["failure"]"#));
    let (code, _, _) = env.run_code(&[
        "run",
        "--name",
        "slow",
        "--timeout",
        "200ms",
        "--",
        "sleep",
        "5",
    ]);
    assert_eq!(code, 124);
    assert_eq!(server.hit_count(), 1);
    let embed = server.embed(0);
    assert_eq!(
        embed["title"], "FAILURE: slow",
        "timeout maps to the failure event"
    );
    assert_eq!(embed["color"], 0xE67E22, "timeout detail shown via orange");
    assert!(
        embed["description"].as_str().unwrap().contains("timeout"),
        "{embed}"
    );
}

#[test]
fn start_failed_reports_failure_with_start_detail() {
    let server = FakeDiscord::start(vec![]);
    let env = TestEnv::new();
    env.write_config(&discord_config(&server.url(), r#"["failure"]"#));
    let (code, _, _) = env.run_code(&["run", "--name", "ghost", "--", "/no/such/bin"]);
    assert_eq!(code, 127);
    assert_eq!(server.hit_count(), 1);
    let desc = server.embed(0)["description"].as_str().unwrap().to_string();
    assert!(desc.contains("could not start"), "{desc}");
}

#[test]
fn recovery_event_after_failure() {
    let server = FakeDiscord::start(vec![]);
    let env = TestEnv::new();
    // The quiet profile: only failure + recovery (SPEC §8 README guidance).
    env.write_config(&discord_config(&server.url(), r#"["failure", "recovery"]"#));

    env.run_ok(&["run", "--name", "flaky", "--", "true"]);
    assert_eq!(server.hit_count(), 0, "success suppressed in quiet profile");

    let _ = env.run_code(&["run", "--name", "flaky", "--", "false"]);
    assert_eq!(server.hit_count(), 1, "failure alert");

    env.run_ok(&["run", "--name", "flaky", "--", "true"]);
    assert_eq!(
        server.hit_count(),
        2,
        "recovery alert (independent of success)"
    );
    let embed = server.embed(1);
    assert_eq!(embed["title"], "RECOVERY: flaky");
    assert_eq!(embed["color"], 0x2ECC71);

    env.run_ok(&["run", "--name", "flaky", "--", "true"]);
    assert_eq!(server.hit_count(), 2, "no recovery after success");
}

#[test]
fn smtp_email_subject_and_body() {
    let smtp = FakeSmtp::start();
    let env = TestEnv::new();
    env.write_config(&format!(
        r#"
[global]
host_name = "prod-worker-01"
[notify]
events = ["failure"]
reporters = ["smtp.ops"]
[reporters.smtp.ops]
host = "127.0.0.1"
port = {}
tls = "none"
from = "uatu@test.local"
recipients = ["ops@test.local", "oncall@test.local"]
"#,
        smtp.addr.port()
    ));
    let (code, _, err) = env.run_code(&[
        "run",
        "--name",
        "nightly-backup",
        "--",
        "sh",
        "-c",
        "echo disk full >&2; exit 2",
    ]);
    assert_eq!(code, 2, "{err}");
    assert!(
        common::wait_until(Duration::from_secs(3), || smtp.message_count() >= 1),
        "email delivered"
    );
    let msg = smtp.last_message_decoded();
    assert!(
        msg.contains("Subject: [uatu] FAILURE: nightly-backup on prod-worker-01"),
        "SPEC subject format, got:\n{msg}"
    );
    assert!(msg.contains("To: ops@test.local"), "{msg}");
    assert!(msg.contains("oncall@test.local"), "{msg}");
    assert!(
        msg.contains("(UTC)") && msg.contains("(host local)"),
        "both timezones: {msg}"
    );
    assert!(msg.contains("disk full"), "failure tail included: {msg}");
    assert!(msg.contains("exit code 2"), "{msg}");
}

#[test]
fn per_reporter_events_filter_intersection() {
    let server = FakeDiscord::start(vec![]);
    let smtp = FakeSmtp::start();
    let env = TestEnv::new();
    // Discord gets everything, email only failures (SPEC §8 routing example).
    env.write_config(&format!(
        r#"
[notify]
events = ["success", "failure"]
reporters = ["discord.d", "smtp.ops"]
[reporters.discord.d]
webhook_url = "{}"
[reporters.smtp.ops]
host = "127.0.0.1"
port = {}
tls = "none"
from = "uatu@test.local"
recipients = ["ops@test.local"]
events = ["failure"]
"#,
        server.url(),
        smtp.addr.port()
    ));
    env.run_ok(&["run", "--name", "routed", "--", "true"]);
    assert!(common::wait_until(Duration::from_secs(2), || server
        .hit_count()
        == 1));
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(smtp.message_count(), 0, "success filtered out for smtp");

    let _ = env.run_code(&["run", "--name", "routed", "--", "false"]);
    assert!(common::wait_until(Duration::from_secs(3), || smtp
        .message_count()
        == 1));
    assert_eq!(server.hit_count(), 2, "both got the failure");
}

#[test]
fn per_reporter_retry_only_failed_reporter_queued() {
    let good = FakeDiscord::start(vec![]);
    // A closed port: connection refused instantly.
    let dead_port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let env = TestEnv::new();
    env.write_config(&format!(
        r#"
[notify]
events = ["success"]
reporters = ["discord.good", "discord.dead"]
[reporters.discord.good]
webhook_url = "{}"
[reporters.discord.dead]
webhook_url = "http://127.0.0.1:{dead_port}/api/webhooks/1/x"
"#,
        good.url()
    ));
    env.run_ok(&["run", "--name", "split", "--", "true"]);
    assert_eq!(good.hit_count(), 1);
    let db = env.db();
    let (delivered, queued): (String, String) = db
        .conn
        .query_row(
            "SELECT
               (SELECT reporter FROM deliveries WHERE state='delivered'),
               (SELECT reporter FROM deliveries WHERE state='queued')",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(delivered, "discord.good");
    assert_eq!(queued, "discord.dead");
    // Backoff schedule: first retry ~1m out (±20%).
    let next: i64 = db
        .conn
        .query_row(
            "SELECT next_attempt_ms FROM deliveries WHERE state='queued'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let delta = next - uatu::util::now_ms();
    assert!(
        (40_000..=80_000).contains(&delta),
        "1m ± 20% backoff, got {delta}ms"
    );
}

#[test]
fn slow_endpoint_respects_budgets_and_queues() {
    let server = FakeDiscord::start(vec![Behavior::Hang(Duration::from_secs(8))]);
    let env = TestEnv::new();
    env.write_config(&discord_config(&server.url(), r#"["success"]"#));
    let started = Instant::now();
    let out = env
        .cmd(&["run", "--name", "budgeted", "--", "true"])
        .env("UATU_PER_REPORTER_BUDGET_MS", "500")
        .env("UATU_OVERALL_BUDGET_MS", "1500")
        .output()
        .unwrap();
    let elapsed = started.elapsed();
    assert_eq!(out.status.code(), Some(0));
    assert!(
        elapsed < Duration::from_secs(5),
        "wrapper exit time bounded by the send budget: {elapsed:?}"
    );
    let db = env.db();
    let state: String = db
        .conn
        .query_row("SELECT state FROM deliveries", [], |r| r.get(0))
        .unwrap();
    assert_eq!(state, "queued", "unfinished delivery enqueued");
}

#[test]
fn http_429_retry_after_overrides_backoff() {
    let server = FakeDiscord::start(vec![Behavior::RateLimited(3600)]);
    let env = TestEnv::new();
    env.write_config(&discord_config(&server.url(), r#"["success"]"#));
    env.run_ok(&["run", "--name", "limited", "--", "true"]);
    let db = env.db();
    let (state, next, err): (String, i64, String) = db
        .conn
        .query_row(
            "SELECT state, next_attempt_ms, last_error FROM deliveries",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert_eq!(state, "queued");
    assert!(err.contains("429"), "{err}");
    let delta = next - uatu::util::now_ms();
    assert!(
        delta > 3_500_000,
        "Retry-After (3600s) exceeds the 1m backoff and wins: {delta}ms"
    );
}

#[test]
fn http_429_pathological_retry_after_neither_panics_nor_wraps() {
    for raw in ["nan", "inf", "-5", "1e300", "soon"] {
        let server = FakeDiscord::start(vec![Behavior::RateLimitedRaw(raw)]);
        let env = TestEnv::new();
        env.write_config(&discord_config(&server.url(), r#"["success"]"#));
        // Invariant: the wrapper must still exit with the child's code (0),
        // never a panic's 101.
        let (code, _out, err) = env.run_code(&["run", "--name", "hostile", "--", "true"]);
        assert_eq!(code, 0, "Retry-After {raw:?} altered the exit code: {err}");
        let db = env.db();
        let (state, next): (String, i64) = db
            .conn
            .query_row("SELECT state, next_attempt_ms FROM deliveries", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .unwrap();
        assert_eq!(state, "queued");
        let now = uatu::util::now_ms();
        assert!(
            next > now - 60_000,
            "{raw:?}: next_attempt_ms wrapped: {next}"
        );
        assert!(
            next <= now + 25 * 3_600_000,
            "{raw:?}: next_attempt_ms beyond the 1d clamp (+backoff): {next}"
        );
    }
}

#[test]
fn delayed_retry_is_marked_with_both_timestamps() {
    let server = FakeDiscord::start(vec![Behavior::Status(500), Behavior::Ok]);
    let env = TestEnv::new();
    env.write_config(&discord_config(&server.url(), r#"["success"]"#));
    env.run_ok(&["run", "--name", "delayed", "--", "true"]);
    assert_eq!(server.hit_count(), 1, "first attempt hit the 500");
    let db = env.db();
    let state: String = db
        .conn
        .query_row("SELECT state FROM deliveries", [], |r| r.get(0))
        .unwrap();
    assert_eq!(state, "queued");
    // Make the retry due now (avoids waiting out the 1m backoff).
    db.conn
        .execute(
            "UPDATE deliveries SET next_attempt_ms = next_attempt_ms - 120000",
            [],
        )
        .unwrap();
    drop(db);
    env.run_ok(&["flush"]);
    assert_eq!(server.hit_count(), 2);
    let desc = server.embed(1)["description"].as_str().unwrap().to_string();
    assert!(desc.contains("DELAYED NOTIFICATION"), "{desc}");
    assert!(
        desc.contains("occurred at") && desc.contains("delivered at"),
        "{desc}"
    );
}

#[test]
fn flush_waits_for_lock_then_does_the_work() {
    let server = FakeDiscord::start(vec![]);
    let env = TestEnv::new();
    env.write_config(&discord_config(&server.url(), r#"["stale", "failure"]"#));
    env.run_ok(&["run", "--", "true"]);
    // Queue something deliverable.
    {
        let db = env.db();
        let now = uatu::util::now_ms();
        common::insert_run_row(
            &db,
            "QUEUED0000RUN0000000000001",
            "locked-job",
            "failure",
            now - 5000,
            1,
            1,
            "dead-boot",
        );
        db.conn
            .execute(
                &format!(
                    "INSERT INTO deliveries (run_id, job_id, event, reporter, state, attempt_count, created_ms, next_attempt_ms)
                     VALUES ('QUEUED0000RUN0000000000001', 'locked-job', 'failure', 'discord.d', 'queued', 0, {now}, {now})"
                ),
                [],
            )
            .unwrap();
    }
    // Hold the flush lock; release it after ~1s from another thread.
    let lock_path = env.state_dir().join("flush.lock");
    let guard = uatu::lock::try_acquire(&lock_path)
        .unwrap()
        .expect("hold lock");
    let releaser = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_secs(1));
        drop(guard);
    });
    let started = Instant::now();
    let (code, _, _) = env.run_code(&["flush"]);
    releaser.join().unwrap();
    assert_eq!(code, 0);
    assert!(
        started.elapsed() >= Duration::from_millis(900),
        "waited for the lock"
    );
    assert!(
        started.elapsed() < Duration::from_secs(9),
        "did not hit the 10s give-up"
    );
    assert_eq!(
        server.hit_count(),
        1,
        "flush delivered after acquiring the lock"
    );
}

#[test]
fn opportunistic_flush_skips_when_locked_no_double_send() {
    let server = FakeDiscord::start(vec![]);
    let env = TestEnv::new();
    env.write_config(&discord_config(&server.url(), r#"["success", "failure"]"#));
    env.run_ok(&["run", "--", "true"]); // bootstrap schema (also delivers #1)
    assert_eq!(server.hit_count(), 1);
    {
        let db = env.db();
        let now = uatu::util::now_ms();
        common::insert_run_row(
            &db,
            "QUEUED0000RUN0000000000002",
            "other-job",
            "failure",
            now - 5000,
            1,
            1,
            "dead-boot",
        );
        db.conn
            .execute(
                &format!(
                    "INSERT INTO deliveries (run_id, job_id, event, reporter, state, attempt_count, created_ms, next_attempt_ms)
                     VALUES ('QUEUED0000RUN0000000000002', 'other-job', 'failure', 'discord.d', 'queued', 0, {now}, {now})"
                ),
                [],
            )
            .unwrap();
    }
    // Hold the lock during the whole run: own event delivers, queued row stays.
    let lock_path = env.state_dir().join("flush.lock");
    let _guard = uatu::lock::try_acquire(&lock_path)
        .unwrap()
        .expect("hold lock");
    env.run_ok(&["run", "--name", "own-event", "--", "true"]);
    assert_eq!(server.hit_count(), 2, "own delivery does not need the lock");
    let db = env.db();
    let state: String = db
        .conn
        .query_row(
            "SELECT state FROM deliveries WHERE run_id='QUEUED0000RUN0000000000002'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(state, "queued", "opportunistic flush skipped while locked");
}

#[test]
fn orphaned_sending_rows_are_requeued_and_delivered() {
    let server = FakeDiscord::start(vec![]);
    let env = TestEnv::new();
    env.write_config(&discord_config(&server.url(), r#"["failure"]"#));
    env.run_ok(&["run", "--", "true"]);
    {
        let db = env.db();
        let now = uatu::util::now_ms();
        common::insert_run_row(
            &db,
            "ORPHAN0000RUN0000000000001",
            "orphan-job",
            "failure",
            now - 5000,
            1,
            1,
            "dead-boot",
        );
        // A `sending` row owned by a dead wrapper (bogus pid/ticks/boot).
        db.conn
            .execute(
                &format!(
                    "INSERT INTO deliveries (run_id, job_id, event, reporter, state, attempt_count, created_ms, owner_pid, owner_start_ticks, owner_boot_id)
                     VALUES ('ORPHAN0000RUN0000000000001', 'orphan-job', 'failure', 'discord.d', 'sending', 0, {now}, 4194000, 1, 'dead-boot')"
                ),
                [],
            )
            .unwrap();
    }
    env.run_ok(&["flush"]);
    assert_eq!(server.hit_count(), 1, "orphan requeued and delivered");
    let db = env.db();
    let state: String = db
        .conn
        .query_row(
            "SELECT state FROM deliveries WHERE run_id='ORPHAN0000RUN0000000000001'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(state, "delivered");
}

#[test]
fn reporter_failure_never_changes_child_exit_code() {
    // No server at all: reporter target refused.
    let dead_port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let env = TestEnv::new();
    env.write_config(&discord_config(
        &format!("http://127.0.0.1:{dead_port}/api/webhooks/1/x"),
        r#"["success", "failure"]"#,
    ));
    let (code, _, _) = env.run_code(&["run", "--", "sh", "-c", "exit 42"]);
    assert_eq!(
        code, 42,
        "notification failure must not alter the exit code"
    );
    let (code, _, _) = env.run_code(&["run", "--", "true"]);
    assert_eq!(code, 0);
}

#[test]
fn notify_test_command() {
    let server = FakeDiscord::start(vec![]);
    let smtp = FakeSmtp::start();
    let env = TestEnv::new();
    env.write_config(&format!(
        r#"
[reporters.discord.d]
webhook_url = "{}"
[reporters.smtp.ops]
host = "127.0.0.1"
port = {}
tls = "none"
from = "uatu@test.local"
recipients = ["ops@test.local"]
"#,
        server.url(),
        smtp.addr.port()
    ));
    // All reporters, marked as tests, includes host + config path.
    let (code, out, _) = env.run_code(&["notify", "test"]);
    assert_eq!(code, 0, "{out}");
    assert!(
        out.contains("discord.d: OK") && out.contains("smtp.ops: OK"),
        "{out}"
    );
    assert_eq!(server.hit_count(), 1);
    let embed = server.embed(0);
    assert!(embed["title"].as_str().unwrap().contains("TEST"), "{embed}");
    assert_eq!(embed["color"], 0x5865F2, "test is blurple");
    assert!(embed["description"].as_str().unwrap().contains("config:"));
    assert!(common::wait_until(Duration::from_secs(2), || smtp
        .message_count()
        == 1));
    assert!(smtp
        .last_message()
        .contains("Subject: [uatu] TEST: smtp.ops on"));
    // Not queued for retry: no delivery rows at all.
    env.run_ok(&["run", "--", "true"]); // ensure db exists
    let db = env.db();
    let n: i64 = db
        .conn
        .query_row(
            "SELECT COUNT(*) FROM deliveries WHERE event='test'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(n, 0);

    // A failing reporter → nonzero, per-reporter error shown.
    let (code, out, _) = env.run_code(&["notify", "test", "--reporter", "discord.nope"]);
    assert_eq!(code, 1);
    assert!(out.is_empty(), "error goes to stderr: {out}");
}

#[test]
fn notify_test_failing_reporter_exits_nonzero() {
    let dead_port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let good = FakeDiscord::start(vec![]);
    let env = TestEnv::new();
    env.write_config(&format!(
        r#"
[reporters.discord.good]
webhook_url = "{}"
[reporters.discord.bad]
webhook_url = "http://127.0.0.1:{dead_port}/api/webhooks/1/x"
"#,
        good.url()
    ));
    let (code, out, _) = env.run_code(&["notify", "test"]);
    assert_eq!(code, 1, "any failed reporter → nonzero");
    assert!(out.contains("discord.good: OK"), "{out}");
    assert!(out.contains("discord.bad: FAILED"), "{out}");
}

#[test]
fn oplog_is_jsonl_and_trims_preserving_records() {
    let env = TestEnv::new();
    env.write_config("[log]\nmax_bytes = \"2KB\"\n");
    for _ in 0..15 {
        env.run_ok(&["run", "--name", "logger", "--", "true"]);
    }
    let log_path = env.state_dir().join("uatu.jsonl");
    let text = std::fs::read_to_string(&log_path).unwrap();
    assert!(text.len() <= 2000, "head-trimmed under cap: {}", text.len());
    assert!(!text.is_empty());
    for line in text.lines() {
        let v: serde_json::Value =
            serde_json::from_str(line).expect("whole JSONL records survive trimming");
        for key in ["ts", "level", "event", "message"] {
            assert!(v.get(key).is_some(), "structured fields present: {line}");
        }
    }
    assert!(text.contains("run_started") && text.contains("run_finished"));
}

#[test]
fn prune_dry_run_and_real() {
    let env = TestEnv::new();
    env.write_config("[retention]\nmax_age = \"30d\"\nmax_bytes = \"1GB\"\n");
    env.run_ok(&[
        "run",
        "--name",
        "old-job",
        "--",
        "sh",
        "-c",
        "echo old data",
    ]);
    let run_id = env.latest_run()["run_id"].as_str().unwrap().to_string();
    let output_path = env.latest_run()["stdout"]["path"]
        .as_str()
        .unwrap()
        .to_string();
    {
        // Age the run artificially (40 days).
        let db = env.db();
        db.conn
            .execute(
                &format!(
                    "UPDATE runs SET start_ms = start_ms - 3456000000 WHERE run_id = '{run_id}'"
                ),
                [],
            )
            .unwrap();
    }
    let (code, out, _) = env.run_code(&["prune", "--dry-run"]);
    assert_eq!(code, 0);
    assert!(
        out.contains("would prune") && out.contains(&run_id),
        "{out}"
    );
    assert!(
        std::path::Path::new(&output_path).exists(),
        "dry-run deletes nothing"
    );
    assert!(env.db().get_run(&run_id).unwrap().is_some());

    let (code, out, _) = env.run_code(&["prune"]);
    assert_eq!(code, 0);
    assert!(out.contains("pruned: 1 aged runs"), "{out}");
    assert!(
        !std::path::Path::new(&output_path).exists(),
        "output deleted"
    );
    assert!(env.db().get_run(&run_id).unwrap().is_none(), "row deleted");
}

#[test]
fn delivery_errors_never_store_the_webhook_secret() {
    // A webhook URL whose port refuses connections: bind, take the port,
    // drop the listener. reqwest connect errors embed the URL unless
    // stripped/redacted — the token must never reach the database.
    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let url = format!("http://127.0.0.1:{port}/api/webhooks/1/sekrit-token");
    let env = TestEnv::new();
    env.write_config(&discord_config(&url, r#"["success"]"#));
    let (code, _out, _err) = env.run_code(&["run", "--name", "leaky", "--", "true"]);
    assert_eq!(code, 0);
    let db = env.db();
    let err: String = db
        .conn
        .query_row("SELECT last_error FROM deliveries", [], |r| r.get(0))
        .unwrap();
    assert!(
        !err.contains("sekrit-token"),
        "webhook token persisted in last_error: {err}"
    );
}
