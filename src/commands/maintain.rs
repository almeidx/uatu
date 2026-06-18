//! `uatu prune`, `uatu init`, `uatu config validate`, `uatu cron example`,
//! `uatu notify test` (SPEC §3).

use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;

use crate::commands::open_for_inspection;
use crate::config::{self, SmtpTls};
use crate::events::{self, Event};
use crate::lock;
use crate::redact::Redactor;
use crate::report::{per_reporter_budget, SendOutcome, Sender};
use crate::util::{format_bytes, format_duration_ms};

// ----- prune -----

pub struct PruneArgs {
    pub config: Option<PathBuf>,
    pub data_dir: Option<PathBuf>,
    pub dry_run: bool,
}

pub fn cmd_prune(args: PruneArgs) -> i32 {
    let opened = match open_for_inspection(args.config.as_deref(), args.data_dir.as_deref(), false)
    {
        Ok(o) => o,
        Err(e) => {
            eprintln!("uatu: error: {e}");
            return 1;
        }
    };
    // Prune mutates shared state → flush lock (SPEC §3). Dry-run reads only.
    let _guard = if !args.dry_run {
        match lock::acquire_wait(&opened.paths.lock, Duration::from_secs(10)) {
            Ok(Some(g)) => Some(g),
            Ok(None) => {
                eprintln!("uatu: error: flush lock is held by another process; try again");
                return 1;
            }
            Err(e) => {
                eprintln!("uatu: error: cannot acquire flush lock: {e}");
                return 1;
            }
        }
    } else {
        None
    };
    match crate::db::prune(
        &opened.db,
        &opened.paths.output,
        opened.config.retention_max_age(),
        opened.config.retention_max_bytes(),
        args.dry_run,
    ) {
        Ok(r) => {
            let verb = if args.dry_run {
                "would prune"
            } else {
                "pruned"
            };
            println!(
                "{verb}: {} aged runs (rows + output), {} runs' output (byte cap), {} freed",
                r.aged_runs.len(),
                r.output_pruned_runs.len(),
                format_bytes(r.bytes_freed)
            );
            if args.dry_run {
                for id in r.aged_runs.iter().chain(r.output_pruned_runs.iter()) {
                    println!("  {id}");
                }
            } else if !r.is_empty() {
                opened.oplog.info(
                    "prune_completed",
                    &format!(
                        "pruned {} aged runs, {} output dirs, {} freed",
                        r.aged_runs.len(),
                        r.output_pruned_runs.len(),
                        format_bytes(r.bytes_freed)
                    ),
                    &[],
                );
            }
            0
        }
        Err(e) => {
            eprintln!("uatu: error: prune failed: {e}");
            1
        }
    }
}

// ----- init -----

pub struct InitArgs {
    pub path: Option<PathBuf>,
    pub stdout: bool,
    pub force: bool,
}

const SAMPLE_CONFIG: &str = r#"# uatu configuration — https://github.com/almeidx/uatu
# Without this file uatu runs local-only: history and capture, no reporters.
# Uncomment and edit what you need.

[global]
# data_dir = "~/.local/state/uatu"
# host_name = "prod-worker-01"        # default: system hostname
# capture_stdout = true
# capture_stderr = true
# capture_mode = "capped"             # "capped" | "full" | "off"
# capture_head_bytes = "64KiB"        # capped mode: head kept on disk
# capture_tail_bytes = "1MiB"         # capped mode: tail kept via ring buffer
# min_free_bytes = "100MB"            # below this free space, capture disables
# kill_grace = "30s"                  # TERM-to-KILL grace for timeouts/interrupts

[retention]
# max_age = "30d"
# max_bytes = "1GB"

[log]
# path = "~/.local/state/uatu/uatu.jsonl"
# max_bytes = "50MB"
# trim = "head"

[redaction]
# Applied before uatu stores or sends anything (never to live cron output).
# literals = ["literal-token-to-hide"]
# regex = ['''password=[^[:space:]]+''']

[notify]
# events = ["success", "failure"]     # valid: success, failure, recovery, stale, long_run, digest
# reporters = ["discord.default", "smtp.ops"]
# failure_output = true               # include redacted output tails on failure
# digest = "off"                      # off | hourly | daily | weekly | monthly

# [reporters.discord.default]
# webhook_url = "https://discord.com/api/webhooks/..."
# max_message_chars = 3500
# events = ["success", "failure", "recovery", "stale", "long_run", "digest"]

# [reporters.smtp.ops]
# host = "smtp.example.com"
# port = 587
# tls = "starttls"                    # "starttls" | "smtps" | "none"
# username = "uatu@example.com"
# password = "smtp-password"
# from = "uatu@example.com"
# recipients = ["ops@example.com"]
# events = ["failure", "recovery", "stale"]   # email only wakes people for problems

# [jobs.nightly-backup]
# cwd = "/srv/app"
# env = { RUST_LOG = "info" }
# timeout = "2h"                      # affects execution, not just observability
# expected_duration = "45m"           # one long_run alert when exceeded
# schedule_label = "nightly at 02:00" # display only
# reporters = ["discord.default"]
# events = ["failure", "recovery"]    # the quiet profile for frequent jobs
# digest = "daily"                    # periodic total/status summary for this job

# Run `uatu config validate` after every edit.
# Run `uatu notify test` after configuring reporters.
"#;

pub fn cmd_init(args: InitArgs) -> i32 {
    if args.stdout {
        print!("{SAMPLE_CONFIG}");
        return 0;
    }
    let target = args.path.unwrap_or_else(config::default_config_target);
    if target.exists() && !args.force {
        eprintln!(
            "uatu: error: {} already exists; pass --force to overwrite",
            target.display()
        );
        return 1;
    }
    // Config will hold webhook URLs / SMTP passwords: 0700 dir, 0600 file (SPEC §7 spirit).
    if let Some(parent) = target.parent() {
        if let Err(e) = crate::state::mkdir_0700_all(parent) {
            eprintln!("uatu: error: cannot create {}: {e}", parent.display());
            return 1;
        }
    }
    match crate::state::write_0600(&target, SAMPLE_CONFIG.as_bytes()) {
        Ok(()) => {
            println!("wrote {}", target.display());
            println!("next: edit it, then run `uatu config validate` and `uatu notify test`");
            0
        }
        Err(e) => {
            eprintln!("uatu: error: cannot write {}: {e}", target.display());
            1
        }
    }
}

// ----- config validate -----

pub struct ValidateArgs {
    pub config: Option<PathBuf>,
}

pub fn cmd_validate(args: ValidateArgs) -> i32 {
    let Some(path) = config::resolve_config_path(args.config.as_deref()) else {
        eprintln!("uatu: error: no config file found (searched --config, $XDG_CONFIG_HOME/uatu/uatu.toml, ~/.config/uatu/uatu.toml, /etc/uatu/uatu.toml)");
        return 1;
    };
    println!("validating {}", path.display());
    let text = match std::fs::read_to_string(&path) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("error: cannot read {}: {e}", path.display());
            return 1;
        }
    };
    let doc: toml::Value = match toml::from_str(&text) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("error: invalid TOML: {e}");
            return 1;
        }
    };

    let mut errors: Vec<String> = Vec::new();
    let mut warnings: Vec<String> = Vec::new();
    let mut notes: Vec<String> = Vec::new();

    // Unknown keys are hard errors here (typo protection; SPEC §3, §10).
    errors.extend(config::unknown_keys(&doc));

    // Type errors (durations, byte sizes, enums) are errors in validate.
    let mut table_warnings = Vec::new();
    let mut redaction_invalid = None;
    let cfg = config::test_parse_tables(&doc, &mut table_warnings, &mut redaction_invalid);
    errors.extend(table_warnings);
    if let Some(e) = redaction_invalid {
        errors.push(e);
    }

    // Redaction rules must compile (unsafe config → nonzero; SPEC §3, §9).
    if let Err(e) = Redactor::new(&cfg.redaction.literals, &cfg.redaction.regex, &[]) {
        errors.push(e);
    }

    // Reporter definitions.
    for (name, d) in &cfg.discord {
        if d.webhook_url.is_empty() {
            errors.push(format!("reporters.discord.{name}: webhook_url is empty"));
        } else if !d.webhook_url.starts_with("http://") && !d.webhook_url.starts_with("https://") {
            errors.push(format!(
                "reporters.discord.{name}: webhook_url must be an http(s) URL"
            ));
        }
        if let Some(ev) = &d.events {
            validate_events(ev, &format!("reporters.discord.{name}"), &mut errors);
        }
    }
    for (name, s) in &cfg.smtp {
        if s.host.is_empty() {
            errors.push(format!("reporters.smtp.{name}: host is empty"));
        }
        if s.from.is_empty() {
            errors.push(format!("reporters.smtp.{name}: from is empty"));
        }
        if s.recipients.is_empty() {
            errors.push(format!("reporters.smtp.{name}: recipients is empty"));
        }
        if s.tls == Some(SmtpTls::None) {
            warnings.push(format!(
                "reporters.smtp.{name}: tls = \"none\" sends credentials and mail in cleartext; only use for localhost relays"
            ));
        }
        if s.username.is_some() != s.password.is_some() {
            warnings.push(format!(
                "reporters.smtp.{name}: username and password should be set together; auth is skipped unless both are present"
            ));
        }
        if let Some(ev) = &s.events {
            validate_events(ev, &format!("reporters.smtp.{name}"), &mut errors);
        }
    }

    // Reporter references and events lists.
    let reporter_exists = |name: &str| events::lookup_reporter(&cfg, name).is_some();
    if let Some(reporters) = &cfg.notify.reporters {
        for r in reporters {
            if !reporter_exists(r) {
                errors.push(format!(
                    "notify.reporters references undefined reporter {r:?}"
                ));
            }
        }
    }
    if let Some(ev) = &cfg.notify.events {
        validate_events(ev, "notify", &mut errors);
    }
    for (job, j) in &cfg.jobs {
        if !crate::identity::valid_slug(job) {
            errors.push(format!(
                "jobs.{job}: job names must match ^[A-Za-z0-9._-]+$ to be matchable by --name"
            ));
        }
        if let Some(reporters) = &j.reporters {
            for r in reporters {
                if !reporter_exists(r) {
                    errors.push(format!(
                        "jobs.{job}.reporters references undefined reporter {r:?}"
                    ));
                }
            }
        }
        if let Some(ev) = &j.events {
            validate_events(ev, &format!("jobs.{job}"), &mut errors);
        }
        // expected_duration > timeout is almost certainly a mistake (SPEC §3).
        if let (Some(exp), Some(timeout)) = (j.expected_duration, j.timeout) {
            if exp.0 > timeout.0 {
                warnings.push(format!(
                    "jobs.{job}: expected_duration ({}) exceeds timeout ({}); the long_run alert can never fire",
                    format_duration_ms(exp.0.as_millis() as u64),
                    format_duration_ms(timeout.0.as_millis() as u64),
                ));
            }
        }
        // Execution-affecting keys notice (SPEC §3).
        let mut affecting = Vec::new();
        if j.cwd.is_some() {
            affecting.push("cwd");
        }
        if !j.env.is_empty() {
            affecting.push("env");
        }
        if j.timeout.is_some() {
            affecting.push("timeout");
        }
        if !affecting.is_empty() {
            notes.push(format!(
                "jobs.{job} sets {} — these change how the job executes, not just how it is observed",
                affecting.join(", ")
            ));
        }
    }

    for e in &errors {
        println!("error: {e}");
    }
    for w in &warnings {
        println!("warning: {w}");
    }
    for n in &notes {
        println!("note: {n}");
    }
    if errors.is_empty() {
        println!(
            "config OK ({} reporter(s), {} job(s))",
            cfg.discord.len() + cfg.smtp.len(),
            cfg.jobs.len()
        );
        0
    } else {
        println!("config INVALID: {} error(s)", errors.len());
        1
    }
}

fn validate_events(list: &[String], where_: &str, errors: &mut Vec<String>) {
    for e in list {
        if Event::parse(e).is_none() {
            errors.push(format!(
                "{where_}: unknown event {e:?} (valid: success, failure, recovery, stale, long_run, digest)"
            ));
        }
    }
}

// ----- cron example -----

pub struct CronExampleArgs {
    pub name: Option<String>,
    pub shell: bool,
}

pub fn cmd_cron_example(args: CronExampleArgs) -> i32 {
    // Always the absolute resolved binary path: cron's PATH is typically
    // /usr/bin:/bin and a bare `uatu` silently breaks the job (SPEC §3).
    let exe = std::env::current_exe()
        .ok()
        .and_then(|p| p.canonicalize().ok())
        .map(|p| p.display().to_string())
        .unwrap_or_else(|| "/usr/local/bin/uatu".to_string());
    let name = args.name.as_deref().unwrap_or("nightly-backup");
    let stdout = std::io::stdout();
    let mut w = stdout.lock();
    let _ = writeln!(w, "# uatu cron examples — copy into `crontab -e`.");
    let _ = writeln!(
        w,
        "# cron runs with a minimal PATH (usually /usr/bin:/bin): always use absolute"
    );
    let _ = writeln!(
        w,
        "# paths for uatu AND for your command, as these examples do."
    );
    if args.shell {
        let _ = writeln!(
            w,
            "0 2 * * * {exe} run --name {name} --shell -- 'cd /srv/app && ./backup.sh >> backup.log 2>&1'"
        );
    } else {
        let _ = writeln!(
            w,
            "0 2 * * * {exe} run --name {name} -- /usr/local/bin/backup"
        );
    }
    let _ = writeln!(w);
    let _ = writeln!(
        w,
        "# Recommended: retry queued notifications and detect stale runs every 10 minutes."
    );
    let _ = writeln!(
        w,
        "# Without this line, stale alerts wait until the next uatu command runs."
    );
    let _ = writeln!(w, "*/10 * * * * {exe} flush");
    0
}

// ----- notify test -----

pub struct NotifyTestArgs {
    pub reporter: Option<String>,
    pub config: Option<PathBuf>,
    pub data_dir: Option<PathBuf>,
}

pub fn cmd_notify_test(args: NotifyTestArgs) -> i32 {
    let loaded = config::load_runtime(args.config.as_deref());
    if let Some(e) = &loaded.invalid {
        eprintln!("uatu: error: {e}");
        return 1;
    }
    for w in &loaded.warnings {
        eprintln!("uatu: warning: {w}");
    }
    let cfg = loaded.config;
    let config_path = loaded
        .path
        .as_ref()
        .map(|p| p.display().to_string())
        .unwrap_or_else(|| "(no config file)".to_string());
    let host = cfg.host_name();

    let mut targets: Vec<String> = Vec::new();
    match &args.reporter {
        Some(name) => {
            if events::lookup_reporter(&cfg, name).is_none() {
                eprintln!("uatu: error: reporter {name:?} is not configured");
                let available: Vec<String> = cfg
                    .discord
                    .keys()
                    .map(|n| format!("discord.{n}"))
                    .chain(cfg.smtp.keys().map(|n| format!("smtp.{n}")))
                    .collect();
                if available.is_empty() {
                    eprintln!("  (no reporters are configured at all)");
                } else {
                    eprintln!("  configured reporters: {}", available.join(", "));
                }
                return 1;
            }
            targets.push(name.clone());
        }
        None => {
            targets.extend(cfg.discord.keys().map(|n| format!("discord.{n}")));
            targets.extend(cfg.smtp.keys().map(|n| format!("smtp.{n}")));
        }
    }
    if targets.is_empty() {
        eprintln!("uatu: error: no reporters configured (config: {config_path}); nothing to test");
        return 1;
    }

    let sender = match Sender::new() {
        Ok(s) => s,
        Err(e) => {
            eprintln!("uatu: error: {e}");
            return 1;
        }
    };

    // Synchronous, never queued for retry (SPEC §3).
    let mut failed = false;
    for name in &targets {
        let outcome = match events::lookup_reporter(&cfg, name) {
            Some(events::ReporterRef::Discord(d)) => {
                let payload = events::test_discord_payload(name, &host, &config_path);
                sender.send_discord(&d.webhook_url, &payload, per_reporter_budget())
            }
            Some(events::ReporterRef::Smtp(s)) => {
                let (subject, body) = events::test_email_message(name, &host, &config_path);
                sender.send_smtp(s, &subject, &body, per_reporter_budget())
            }
            None => SendOutcome::Failed {
                error: "not configured".to_string(),
                retry_after: None,
            },
        };
        match outcome {
            SendOutcome::Delivered => println!("{name}: OK"),
            SendOutcome::Failed { error, .. } => {
                println!("{name}: FAILED — {error}");
                failed = true;
            }
        }
    }
    if failed {
        1
    } else {
        0
    }
}
