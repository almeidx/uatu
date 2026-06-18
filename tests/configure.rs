//! Integration tests for the interactive configuration wizard
//! (`config wizard` / `init --interactive`): the real binary is driven through
//! a piped stdin, and the resulting config file is checked for correctness,
//! restrictive modes, and that `config validate` accepts it.

mod common;

use common::{Behavior, FakeDiscord, TestEnv};
use std::os::unix::fs::PermissionsExt;

fn mode(p: &std::path::Path) -> u32 {
    std::fs::metadata(p).unwrap().permissions().mode() & 0o777
}

#[test]
fn config_wizard_adds_discord_and_validates() {
    let env = TestEnv::new();
    // hub:reporters, add:discord, name=default, url, no event restriction,
    // done, save, decline test.
    let script = "1\n1\ndefault\nhttps://discord.com/api/webhooks/1/abc\nn\n3\n6\nn\n";
    let (code, out, err) = env.run_input(env.cmd_raw(&["config", "wizard"]), script);
    assert_eq!(code, 0, "stdout:\n{out}\nstderr:\n{err}");

    let target = env.xdg_config_target();
    assert!(target.exists(), "wizard must write the XDG config target");
    assert_eq!(mode(&target), 0o600, "config file must be 0600");

    let text = std::fs::read_to_string(&target).unwrap();
    assert!(text.contains("[reporters.discord.default]"), "{text}");
    assert!(
        text.contains("https://discord.com/api/webhooks/1/abc"),
        "{text}"
    );
    // New reporter is wired into global routing so it actually fires.
    assert!(text.contains("reporters = [\"discord.default\"]"), "{text}");

    let (vcode, vout, _) = env.run_input(
        env.cmd_raw(&["config", "validate", "--config", target.to_str().unwrap()]),
        "",
    );
    assert_eq!(vcode, 0, "generated config must validate:\n{vout}");
    assert!(vout.contains("config OK"), "{vout}");
}

#[test]
fn init_interactive_writes_minimal_valid_config() {
    let env = TestEnv::new();
    let target = env.dir.path().join("cfg").join("uatu.toml");
    let t = target.to_str().unwrap();
    // Blank stream: the hub defaults to "Review & save", producing a valid
    // (empty) config without any further questions.
    let (code, out, err) =
        env.run_input(env.cmd_raw(&["init", "--interactive", "--path", t]), "\n");
    assert_eq!(code, 0, "stdout:\n{out}\nstderr:\n{err}");
    assert!(target.exists());
    assert_eq!(mode(&target), 0o600);
    assert_eq!(
        mode(target.parent().unwrap()),
        0o700,
        "created dir must be 0700"
    );

    let (vcode, vout, _) = env.run_input(env.cmd_raw(&["config", "validate", "--config", t]), "");
    assert_eq!(vcode, 0, "{vout}");
}

#[test]
fn init_interactive_conflicts_with_template_flags() {
    let env = TestEnv::new();
    for flag in ["--stdout", "--force"] {
        let (code, _, err) = env.run_input(env.cmd_raw(&["init", "--interactive", flag]), "");
        assert_eq!(code, 2, "`init --interactive {flag}` must be a usage error");
        assert!(
            err.contains("cannot be used with"),
            "expected a clap conflict error, got: {err}"
        );
    }
}

#[test]
fn wizard_preserves_existing_config_and_writes_backup() {
    let env = TestEnv::new();
    let target = env.dir.path().join("existing.toml");
    let original = "[jobs.existing]\nschedule_label = \"hourly\"\n";
    std::fs::write(&target, original).unwrap();
    // A loose, world-readable original (as a hand-created config often is).
    std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o644)).unwrap();
    let t = target.to_str().unwrap();

    let script = "1\n1\ndefault\nhttps://discord.com/api/webhooks/1/abc\nn\n3\n6\nn\n";
    let (code, out, err) = env.run_input(env.cmd_raw(&["config", "wizard", "--config", t]), script);
    assert_eq!(code, 0, "stdout:\n{out}\nstderr:\n{err}");

    // The pre-existing file is backed up verbatim — and the backup is 0600,
    // never inheriting the original's loose mode (it can hold secrets).
    let bak = env.dir.path().join("existing.toml.bak");
    assert!(bak.exists(), "a .bak backup must be written");
    assert_eq!(std::fs::read_to_string(&bak).unwrap(), original);
    assert_eq!(mode(&bak), 0o600, "backup must be 0600, not inherit 0644");

    // The new file keeps the old job AND has the new reporter.
    let text = std::fs::read_to_string(&target).unwrap();
    assert!(text.contains("[jobs.existing]"), "{text}");
    assert!(text.contains("schedule_label = \"hourly\""), "{text}");
    assert!(text.contains("[reporters.discord.default]"), "{text}");

    let (vcode, vout, _) = env.run_input(env.cmd_raw(&["config", "validate", "--config", t]), "");
    assert_eq!(vcode, 0, "{vout}");
}

#[test]
fn wizard_smtp_reporter_round_trips_and_validates() {
    let env = TestEnv::new();
    // hub:reporters, add:smtp, name=ops, host, port=587, tls=starttls,
    // username blank, from, recipients, no restriction, done, save, decline.
    let script =
        "1\n2\nops\nsmtp.example.com\n587\n1\n\nops@example.com\nops@example.com\nn\n3\n6\nn\n";
    let (code, out, err) = env.run_input(env.cmd_raw(&["config", "wizard"]), script);
    assert_eq!(code, 0, "stdout:\n{out}\nstderr:\n{err}");

    let target = env.xdg_config_target();
    let text = std::fs::read_to_string(&target).unwrap();
    assert!(text.contains("[reporters.smtp.ops]"), "{text}");
    assert!(text.contains("host = \"smtp.example.com\""), "{text}");
    assert!(
        text.contains("recipients = [\"ops@example.com\"]"),
        "{text}"
    );

    let (vcode, vout, _) = env.run_input(
        env.cmd_raw(&["config", "validate", "--config", target.to_str().unwrap()]),
        "",
    );
    assert_eq!(vcode, 0, "{vout}");
}

#[test]
fn wizard_handles_dotted_reporter_name_and_still_validates() {
    let env = TestEnv::new();
    // A dotted name is permitted by --name slug rules; it must be quoted in the
    // emitted table header so the reporter is not lost as a nested table.
    let script = "1\n1\nteam.alpha\nhttps://discord.com/api/webhooks/1/abc\nn\n3\n6\nn\n";
    let (code, out, err) = env.run_input(env.cmd_raw(&["config", "wizard"]), script);
    assert_eq!(code, 0, "stdout:\n{out}\nstderr:\n{err}");

    let target = env.xdg_config_target();
    let text = std::fs::read_to_string(&target).unwrap();
    assert!(
        text.contains("[reporters.discord.\"team.alpha\"]"),
        "dotted name must be quoted:\n{text}"
    );
    assert!(
        text.contains("reporters = [\"discord.team.alpha\"]"),
        "{text}"
    );

    let (vcode, vout, _) = env.run_input(
        env.cmd_raw(&["config", "validate", "--config", target.to_str().unwrap()]),
        "",
    );
    assert_eq!(vcode, 0, "dotted-name config must validate:\n{vout}");
    assert!(vout.contains("1 reporter(s)"), "{vout}");
}

#[test]
fn wizard_optional_test_send_reaches_reporter() {
    let env = TestEnv::new();
    let discord = FakeDiscord::start(vec![Behavior::Ok]);
    // Same as the discord flow but accept the test send at the end.
    let script = format!("1\n1\ndefault\n{}\nn\n3\n6\ny\n", discord.url());
    let (code, out, err) = env.run_input(env.cmd_raw(&["config", "wizard"]), &script);
    assert_eq!(code, 0, "stdout:\n{out}\nstderr:\n{err}");
    assert_eq!(
        discord.hit_count(),
        1,
        "the test send must reach the webhook"
    );
    assert!(out.contains("discord.default: OK"), "{out}");
}

#[test]
fn wizard_quit_writes_nothing() {
    let env = TestEnv::new();
    let (code, out, _) = env.run_input(env.cmd_raw(&["config", "wizard"]), "7\n");
    assert_eq!(code, 0);
    assert!(out.contains("No changes written"), "{out}");
    assert!(
        !env.xdg_config_target().exists(),
        "quit must not write a file"
    );
}

#[test]
fn wizard_eof_midway_still_writes_valid_config() {
    let env = TestEnv::new();
    // Enter the reporters menu, then the input stream ends: the wizard must
    // unwind to a save rather than hang, producing a valid file.
    let (code, out, err) = env.run_input(env.cmd_raw(&["config", "wizard"]), "1\n");
    assert_eq!(code, 0, "stdout:\n{out}\nstderr:\n{err}");
    let target = env.xdg_config_target();
    assert!(target.exists());
    let (vcode, vout, _) = env.run_input(
        env.cmd_raw(&["config", "validate", "--config", target.to_str().unwrap()]),
        "",
    );
    assert_eq!(vcode, 0, "{vout}");
}
