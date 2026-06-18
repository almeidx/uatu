//! Interactive configuration wizard (`config wizard` / `init --interactive`,
//! SPEC §3).
//!
//! A hub-and-spoke flow: the user repeatedly picks an area to configure
//! (reporters, notification routing, per-job overrides, global/capture,
//! retention/redaction), then saves. Every question offers a default that
//! Enter accepts, so a user can skip straight to a working config. The result
//! is rendered to the `[reporters.*]` / `[jobs.*]` / ... schema (SPEC §4) and
//! written 0600; an optional final step reuses `notify test` (SPEC §3) to
//! verify reporters end to end.
//!
//! This command writes config and (only when the user opts in at the end)
//! sends test notifications. It never opens run state or touches the database.

use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::commands::maintain::{cmd_notify_test, NotifyTestArgs};
use crate::config::{
    self, ByteSize, CaptureMode, Config, DiscordCfg, Dur, JobCfg, SmtpCfg, SmtpTls,
};
use crate::identity::valid_slug;
use crate::prompt::{self, LinePrompt, PromptError, PromptResult, TermUi, Ui};
use crate::state;
use crate::util::{parse_bytes, parse_duration};

const EVENT_NAMES: [&str; 5] = ["success", "failure", "recovery", "stale", "long_run"];

pub struct ConfigureArgs {
    /// Explicit target path (`--config` for `config wizard`, `--path` for
    /// `init --interactive`). When absent, the standard config location is
    /// used.
    pub target: Option<PathBuf>,
}

pub fn cmd_configure(args: ConfigureArgs) -> i32 {
    let target = args
        .target
        .or_else(|| config::resolve_config_path(None))
        .unwrap_or_else(config::default_config_target);

    let mut cfg = Config::default();
    let existed = target.exists();
    if existed {
        let loaded = config::load_runtime(Some(&target));
        if let Some(e) = &loaded.invalid {
            eprintln!("uatu: warning: existing config is unusable ({e})");
            eprintln!("uatu: warning: a backup is kept; the wizard starts from an empty config");
        } else {
            for w in &loaded.warnings {
                eprintln!("uatu: warning: {w}");
            }
            cfg = loaded.config;
        }
    }

    // Arrow-key TUI when attached to a terminal; line-based reader otherwise
    // (pipes, redirected input, tests) so the wizard stays fully scriptable.
    let outcome_res = if prompt::stdio_is_tty() {
        drive_ui(&mut cfg, &mut TermUi::new(), existed, &target)
    } else {
        let stdin = io::stdin();
        let stdout = io::stdout();
        let mut p = LinePrompt::new(stdin.lock(), stdout.lock());
        drive_ui(&mut cfg, &mut p, existed, &target)
    };

    let outcome = match outcome_res {
        Ok(o) => o,
        // Ctrl-C (Abort) exits the wizard; a stray Cancel that reached the top
        // is treated the same — write nothing.
        Err(PromptError::Abort) | Err(PromptError::Cancel) => {
            println!("\nCancelled — no changes written.");
            return 0;
        }
        Err(PromptError::Io(e)) => {
            eprintln!("uatu: error: input error: {e}");
            return 1;
        }
    };

    let send_test = match outcome {
        Outcome::Quit => {
            println!("No changes written.");
            return 0;
        }
        Outcome::Save { send_test } => send_test,
    };

    let rendered = render_config(&cfg);
    if let Some(parent) = target.parent() {
        if let Err(e) = state::mkdir_0700_all(parent) {
            eprintln!("uatu: error: cannot create {}: {e}", parent.display());
            return 1;
        }
    }
    if existed {
        let bak = backup_path(&target);
        // Write the backup through write_0600 (not fs::copy, which would
        // inherit the source file's mode) so a previously loose-mode config
        // never leaves its webhook URLs / SMTP passwords world-readable.
        let backed_up = std::fs::read(&target).and_then(|bytes| state::write_0600(&bak, &bytes));
        if let Err(e) = backed_up {
            eprintln!(
                "uatu: error: cannot back up {} to {}: {e}",
                target.display(),
                bak.display()
            );
            return 1;
        }
        println!("Backed up previous config to {}", bak.display());
    }
    if let Err(e) = state::write_0600(&target, rendered.as_bytes()) {
        eprintln!("uatu: error: cannot write {}: {e}", target.display());
        return 1;
    }
    println!("Wrote {}", target.display());
    println!("Run `uatu config validate` to double-check.");

    if send_test {
        println!("Sending test notification(s)...");
        return cmd_notify_test(NotifyTestArgs {
            reporter: None,
            config: Some(target),
            data_dir: None,
        });
    }
    0
}

enum Outcome {
    Save { send_test: bool },
    Quit,
}

fn drive_ui<U: Ui>(
    cfg: &mut Config,
    ui: &mut U,
    existed: bool,
    target: &Path,
) -> PromptResult<Outcome> {
    if existed {
        ui.say(&format!("Editing existing config: {}", target.display()))?;
        ui.say("Comments and formatting are normalized on save; a .bak backup is kept.")?;
    }
    run_wizard(cfg, ui)
}

fn run_wizard<U: Ui>(cfg: &mut Config, p: &mut U) -> PromptResult<Outcome> {
    p.say("uatu interactive configuration")?;
    p.say("Enter accepts the [default]; Esc goes back; Ctrl-C quits without saving.")?;
    loop {
        p.blank()?;
        p.say(&format!(
            "Current: {} Discord, {} email reporter(s), {} job(s).",
            cfg.discord.len(),
            cfg.smtp.len(),
            cfg.jobs.len()
        ))?;
        let choice = match p.select(
            "What would you like to configure?",
            &[
                "Reporters (Discord / email alerts)",
                "Notification routing (events, failure output)",
                "Job overrides (per-job settings)",
                "Global & capture settings",
                "Retention & redaction",
                "Review & save",
                "Quit without saving",
            ],
            5,
        ) {
            Ok(c) => c,
            // Esc at the top menu has nowhere to go back to: just redisplay it.
            Err(PromptError::Cancel) => continue,
            Err(e) => return Err(e),
        };
        let section = match choice {
            0 => configure_reporters(cfg, p),
            1 => configure_notify(cfg, p),
            2 => configure_job(cfg, p),
            3 => configure_global(cfg, p),
            4 => configure_retention_redaction(cfg, p),
            5 => break,
            _ => return Ok(Outcome::Quit),
        };
        match section {
            Ok(()) => {}
            // Esc inside a section discards its pending edits and returns here.
            Err(PromptError::Cancel) => p.say("  (cancelled — back to menu)")?,
            Err(e) => return Err(e),
        }
        if p.at_eof() {
            break;
        }
    }

    let send_test = if cfg.discord.is_empty() && cfg.smtp.is_empty() {
        false
    } else {
        match p.confirm(
            "Send a test notification through the configured reporters now?",
            true,
        ) {
            Ok(b) => b,
            Err(PromptError::Cancel) => false,
            Err(e) => return Err(e),
        }
    };
    Ok(Outcome::Save { send_test })
}

// ----- reporters -----

fn configure_reporters<U: Ui>(cfg: &mut Config, p: &mut U) -> PromptResult<()> {
    loop {
        let kind = match p.select(
            "Add or edit a reporter:",
            &["Discord webhook", "Email (SMTP)", "Done with reporters"],
            2,
        ) {
            Ok(k) => k,
            // Esc at the reporter menu leaves the section (back to the hub).
            Err(PromptError::Cancel) => return Ok(()),
            Err(e) => return Err(e),
        };
        let added = match kind {
            0 => add_discord(cfg, p),
            1 => add_smtp(cfg, p),
            _ => return Ok(()),
        };
        match added {
            Ok(()) => {}
            // Esc while adding one reporter discards just it; stay in the menu.
            Err(PromptError::Cancel) => p.say("  (cancelled this reporter)")?,
            Err(e) => return Err(e),
        }
        if p.at_eof() {
            break;
        }
    }
    Ok(())
}

fn add_discord<U: Ui>(cfg: &mut Config, p: &mut U) -> PromptResult<()> {
    let name = p.text_validated("Reporter name", Some("default"), slug_check)?;
    if !valid_slug(&name) {
        p.say("  (skipped — invalid reporter name)")?;
        return Ok(());
    }
    let (cur_url, cur_events, cur_max) = match cfg.discord.get(&name) {
        Some(d) => (
            Some(d.webhook_url.clone()),
            d.events.clone(),
            d.max_message_chars,
        ),
        None => (None, None, None),
    };
    let url = p.text_validated("Discord webhook URL", cur_url.as_deref(), |a| {
        if a.starts_with("https://") || a.starts_with("http://") {
            Ok(())
        } else {
            Err("must be an http(s):// URL".into())
        }
    })?;
    if !(url.starts_with("http://") || url.starts_with("https://")) {
        p.say("  (skipped — no valid webhook URL given)")?;
        return Ok(());
    }
    let events = ask_event_restriction(p, cur_events)?;
    cfg.discord.insert(
        name.clone(),
        DiscordCfg {
            webhook_url: url,
            max_message_chars: cur_max,
            events,
        },
    );
    enable_global_reporter(cfg, &format!("discord.{name}"));
    p.say(&format!(
        "  ✓ discord.{name} saved and enabled in [notify].reporters"
    ))?;
    Ok(())
}

fn add_smtp<U: Ui>(cfg: &mut Config, p: &mut U) -> PromptResult<()> {
    let name = p.text_validated("Reporter name", Some("ops"), slug_check)?;
    if !valid_slug(&name) {
        p.say("  (skipped — invalid reporter name)")?;
        return Ok(());
    }
    let existing = cfg.smtp.get(&name).cloned();
    let cur = |f: fn(&SmtpCfg) -> Option<String>| existing.as_ref().and_then(f);

    let host = ask_required(p, "SMTP host", cur(|s| Some(s.host.clone())).as_deref())?;
    let port_default = existing
        .as_ref()
        .and_then(|s| s.port)
        .map(|n| n.to_string())
        .unwrap_or_else(|| "587".to_string());
    let port_str = p.text_validated("SMTP port", Some(&port_default), |a| {
        a.parse::<u16>()
            .map(|_| ())
            .map_err(|_| "must be a port number (1-65535)".into())
    })?;
    let port = port_str.parse::<u16>().ok();
    let tls_opts = ["starttls", "smtps", "none"];
    let tls_default = existing
        .as_ref()
        .and_then(|s| s.tls)
        .map(smtp_tls_str)
        .and_then(|t| tls_opts.iter().position(|o| *o == t))
        .unwrap_or(0);
    let tls = parse_smtp_tls(tls_opts[p.select("Transport security:", &tls_opts, tls_default)?]);
    if tls == SmtpTls::None {
        p.say(
            "  ! note: tls = none sends mail and credentials in cleartext (localhost relays only)",
        )?;
    }
    let username = ask_opt_string(
        p,
        "SMTP username (auth)",
        cur(|s| s.username.clone()).as_deref(),
    )?;
    let password = if username.is_some() {
        ask_opt_secret(p, "SMTP password", cur(|s| s.password.clone()).as_deref())?
    } else {
        None
    };
    let from = ask_required(p, "From address", cur(|s| Some(s.from.clone())).as_deref())?;
    let rcpt_default = existing.as_ref().map(|s| s.recipients.join(", "));
    let rcpt_raw = p.text_validated(
        "Recipients (comma-separated)",
        rcpt_default.as_deref(),
        |a| {
            if a.split(',').any(|x| !x.trim().is_empty()) {
                Ok(())
            } else {
                Err("at least one recipient is required".into())
            }
        },
    )?;
    let recipients: Vec<String> = rcpt_raw
        .split(',')
        .map(|x| x.trim().to_string())
        .filter(|x| !x.is_empty())
        .collect();
    if host.trim().is_empty() || from.trim().is_empty() || recipients.is_empty() {
        p.say("  (skipped — host, from and at least one recipient are required)")?;
        return Ok(());
    }
    let events = ask_event_restriction(p, existing.as_ref().and_then(|s| s.events.clone()))?;
    cfg.smtp.insert(
        name.clone(),
        SmtpCfg {
            host,
            port,
            tls: Some(tls),
            username,
            password,
            from,
            recipients,
            max_message_chars: existing.as_ref().and_then(|s| s.max_message_chars),
            events,
        },
    );
    enable_global_reporter(cfg, &format!("smtp.{name}"));
    p.say(&format!(
        "  ✓ smtp.{name} saved and enabled in [notify].reporters"
    ))?;
    Ok(())
}

/// Ask whether a reporter should only receive a subset of events; `None` means
/// "all events" (the schema default).
fn ask_event_restriction<U: Ui>(
    p: &mut U,
    current: Option<Vec<String>>,
) -> PromptResult<Option<Vec<String>>> {
    let restrict = p.confirm(
        "Restrict this reporter to specific events? (No = all events)",
        current.is_some(),
    )?;
    if !restrict {
        return Ok(None);
    }
    let defaults = event_defaults(current.as_deref(), &EVENT_NAMES);
    let idx = p.multi_select("Events for this reporter:", &EVENT_NAMES, &defaults)?;
    if idx.is_empty() {
        p.say("  ! note: no events selected — this reporter will stay silent until you add some")?;
    }
    Ok(Some(events_from_indices(&idx)))
}

fn enable_global_reporter(cfg: &mut Config, full_name: &str) {
    let list = cfg.notify.reporters.get_or_insert_with(Vec::new);
    if !list.iter().any(|r| r == full_name) {
        list.push(full_name.to_string());
    }
}

// ----- notification routing -----

fn configure_notify<U: Ui>(cfg: &mut Config, p: &mut U) -> PromptResult<()> {
    // Collect every answer first; only commit once the section completes, so an
    // Esc partway through discards the whole edit.
    let defaults = event_defaults(cfg.notify.events.as_deref(), &["success", "failure"]);
    let idx = p.multi_select(
        "Alert on which events (global default)?",
        &EVENT_NAMES,
        &defaults,
    )?;
    let events = events_from_indices(&idx);

    let all = reporter_names(cfg);
    let reporters = if all.is_empty() {
        p.say("  (no reporters configured yet — add some from the main menu to route alerts)")?;
        None
    } else {
        let current = cfg.notify.reporters.clone().unwrap_or_default();
        let opts: Vec<&str> = all.iter().map(|s| s.as_str()).collect();
        let defaults: Vec<bool> = all
            .iter()
            .map(|r| current.is_empty() || current.iter().any(|c| c == r))
            .collect();
        let idx = p.multi_select("Which reporters receive alerts globally?", &opts, &defaults)?;
        Some(
            idx.iter()
                .filter_map(|&i| all.get(i).cloned())
                .collect::<Vec<_>>(),
        )
    };

    let fo = p.confirm(
        "Include redacted output tails on failure alerts?",
        cfg.notify.failure_output.unwrap_or(true),
    )?;

    cfg.notify.events = Some(events);
    if let Some(reporters) = reporters {
        cfg.notify.reporters = Some(reporters);
    }
    cfg.notify.failure_output = Some(fo);
    Ok(())
}

// ----- per-job overrides -----

fn configure_job<U: Ui>(cfg: &mut Config, p: &mut U) -> PromptResult<()> {
    // Offer the jobs already in the config to edit, plus a "new job" entry.
    // (The wizard never reads run history, so only configured jobs are known.)
    let existing: Vec<String> = cfg.jobs.keys().cloned().collect();
    let name = if existing.is_empty() {
        match prompt_new_job_name(p)? {
            Some(n) => n,
            None => return Ok(()),
        }
    } else {
        let mut opts: Vec<String> = existing.clone();
        opts.push("+ Add a new job".to_string());
        let labels: Vec<&str> = opts.iter().map(|s| s.as_str()).collect();
        let idx = p.select("Which job?", &labels, labels.len() - 1)?;
        if idx < existing.len() {
            existing[idx].clone()
        } else {
            match prompt_new_job_name(p)? {
                Some(n) => n,
                None => return Ok(()),
            }
        }
    };

    let existed_job = cfg.jobs.contains_key(&name);
    let mut job = cfg.jobs.get(&name).cloned().unwrap_or_default();

    job.schedule_label = ask_opt_string(
        p,
        "Schedule label (display only)",
        job.schedule_label.as_deref(),
    )?;
    job.cwd = ask_opt_string(
        p,
        "Working directory (changes how the job runs)",
        job.cwd.as_deref(),
    )?;
    job.timeout = ask_opt_duration(
        p,
        "Timeout (kills the job when exceeded)",
        job.timeout.map(|d| d.0),
    )?;
    job.expected_duration = ask_opt_duration(
        p,
        "Expected duration (one long_run alert past this)",
        job.expected_duration.map(|d| d.0),
    )?;

    let all = reporter_names(cfg);
    if !all.is_empty()
        && p.confirm(
            "Restrict this job to specific reporters? (No = use global)",
            job.reporters.is_some(),
        )?
    {
        let opts: Vec<&str> = all.iter().map(|s| s.as_str()).collect();
        let current = job.reporters.clone().unwrap_or_default();
        let defaults: Vec<bool> = all.iter().map(|r| current.iter().any(|c| c == r)).collect();
        let idx = p.multi_select("Reporters for this job:", &opts, &defaults)?;
        job.reporters = Some(idx.iter().filter_map(|&i| all.get(i).cloned()).collect());
    }

    if p.confirm(
        "Restrict this job to specific events? (No = use global)",
        job.events.is_some(),
    )? {
        let defaults = event_defaults(job.events.as_deref(), &EVENT_NAMES);
        let idx = p.multi_select("Events for this job:", &EVENT_NAMES, &defaults)?;
        job.events = Some(events_from_indices(&idx));
    }

    if p.confirm(
        "Set environment variables for this job?",
        !job.env.is_empty(),
    )? {
        loop {
            let kv = p.text("NAME=value (blank to stop)", None)?;
            if kv.is_empty() {
                break;
            }
            match kv.split_once('=') {
                Some((k, v)) if is_env_name(k) => {
                    job.env.insert(k.to_string(), v.to_string());
                }
                _ => p.say("  ! expected NAME=value with a valid variable name")?,
            }
            if p.at_eof() {
                break;
            }
        }
    }

    // A job with no fields renders to nothing (push_table drops an empty
    // table), so don't claim it was saved. Editing an existing job that the
    // user cleared removes its (now empty) entry instead.
    if job_has_fields(&job) {
        cfg.jobs.insert(name.clone(), job);
        p.say(&format!("  ✓ job {name} saved"))?;
    } else if existed_job {
        cfg.jobs.remove(&name);
        p.say(&format!("  (all overrides cleared — removed job {name})"))?;
    } else {
        p.say("  (nothing set — job not recorded)")?;
    }
    Ok(())
}

fn job_has_fields(j: &JobCfg) -> bool {
    j.cwd.is_some()
        || !j.env.is_empty()
        || j.timeout.is_some()
        || j.expected_duration.is_some()
        || j.schedule_label.is_some()
        || j.reporters.is_some()
        || j.events.is_some()
        || j.capture_stdout.is_some()
        || j.capture_stderr.is_some()
        || j.capture_mode.is_some()
        || j.capture_head_bytes.is_some()
        || j.capture_tail_bytes.is_some()
        || j.kill_grace.is_some()
        || j.failure_output.is_some()
}

/// Prompt for a brand-new job name. `None` means the user cancelled (blank or
/// an invalid name).
fn prompt_new_job_name<U: Ui>(p: &mut U) -> PromptResult<Option<String>> {
    let name = p.text(
        "New job name (matches `uatu run --name`, blank to cancel)",
        None,
    )?;
    if name.is_empty() {
        return Ok(None);
    }
    if !valid_slug(&name) {
        p.say("  ! invalid name; must match ^[A-Za-z0-9._-]+$")?;
        return Ok(None);
    }
    Ok(Some(name))
}

// ----- global & capture -----

fn configure_global<U: Ui>(cfg: &mut Config, p: &mut U) -> PromptResult<()> {
    // Collect first, commit at the end so an Esc discards the whole section.
    let host_default = cfg
        .global
        .host_name
        .clone()
        .unwrap_or_else(crate::util::hostname);
    let host = p.text("Host name shown in alerts", Some(&host_default))?;

    let modes = ["capped", "full", "off"];
    let cur_mode = cfg
        .global
        .capture_mode
        .map(capture_mode_str)
        .unwrap_or("capped");
    let di = modes.iter().position(|m| *m == cur_mode).unwrap_or(0);
    let mi = p.select("Capture mode for child output:", &modes, di)?;

    let stdout = p.confirm("Capture stdout?", cfg.global.capture_stdout.unwrap_or(true))?;
    let stderr = p.confirm("Capture stderr?", cfg.global.capture_stderr.unwrap_or(true))?;
    let kill_grace = ask_opt_duration(
        p,
        "Kill grace (TERM→KILL window)",
        cfg.global.kill_grace.map(|d| d.0),
    )?;

    cfg.global.host_name = if host.trim().is_empty() {
        None
    } else {
        Some(host)
    };
    cfg.global.capture_mode = Some(parse_capture_mode(modes[mi]));
    cfg.global.capture_stdout = Some(stdout);
    cfg.global.capture_stderr = Some(stderr);
    cfg.global.kill_grace = kill_grace;
    Ok(())
}

// ----- retention & redaction -----

fn configure_retention_redaction<U: Ui>(cfg: &mut Config, p: &mut U) -> PromptResult<()> {
    // Collect into locals, commit at the end so an Esc discards the section.
    let max_age = ask_opt_duration(
        p,
        "Retention: max age (e.g. 30d)",
        cfg.retention.max_age.map(|d| d.0),
    )?;
    let max_bytes = ask_opt_bytes(
        p,
        "Retention: max bytes (e.g. 1GB)",
        cfg.retention.max_bytes.map(|b| b.0),
    )?;

    let mut literals = Vec::new();
    if p.confirm("Add redaction literal strings?", false)? {
        loop {
            let s = p.text("Literal to redact (blank to stop)", None)?;
            if s.is_empty() {
                break;
            }
            literals.push(s);
            if p.at_eof() {
                break;
            }
        }
    }
    let mut regexes = Vec::new();
    if p.confirm("Add redaction regex patterns?", false)? {
        loop {
            let s = p.text_validated("Regex to redact (blank to stop)", None, |a| {
                if a.is_empty() {
                    Ok(())
                } else {
                    regex::Regex::new(a)
                        .map(|_| ())
                        .map_err(|e| format!("invalid regex: {e}"))
                }
            })?;
            if s.is_empty() {
                break;
            }
            regexes.push(s);
            if p.at_eof() {
                break;
            }
        }
    }

    cfg.retention.max_age = max_age;
    cfg.retention.max_bytes = max_bytes;
    for s in literals {
        if !cfg.redaction.literals.contains(&s) {
            cfg.redaction.literals.push(s);
        }
    }
    for s in regexes {
        if !cfg.redaction.regex.contains(&s) {
            cfg.redaction.regex.push(s);
        }
    }
    Ok(())
}

// ----- small prompt helpers -----

fn slug_check(a: &str) -> Result<(), String> {
    if valid_slug(a) {
        Ok(())
    } else {
        Err("must match ^[A-Za-z0-9._-]+$".into())
    }
}

fn ask_required<U: Ui>(p: &mut U, label: &str, default: Option<&str>) -> PromptResult<String> {
    p.text_validated(label, default, |a| {
        if a.trim().is_empty() {
            Err("this field is required".into())
        } else {
            Ok(())
        }
    })
}

/// Optional free-text field: blank keeps the current value, `-` clears it.
fn ask_opt_string<U: Ui>(
    p: &mut U,
    label: &str,
    current: Option<&str>,
) -> PromptResult<Option<String>> {
    let hint = if current.is_some() {
        format!("{label} (blank = keep, - = clear)")
    } else {
        format!("{label} (blank = none)")
    };
    let ans = p.text(&hint, current)?;
    if ans == "-" || ans.is_empty() {
        Ok(None)
    } else {
        Ok(Some(ans))
    }
}

/// Like [`ask_opt_string`] but for secret values: the current value is never
/// echoed (shown as `[unchanged]`), so re-editing a reporter does not print a
/// stored password to the terminal. Blank keeps it, `-` clears it.
fn ask_opt_secret<U: Ui>(
    p: &mut U,
    label: &str,
    current: Option<&str>,
) -> PromptResult<Option<String>> {
    let hint = if current.is_some() {
        format!("{label} (blank = keep, - = clear)")
    } else {
        format!("{label} (blank = none)")
    };
    let placeholder = current.map(|_| "unchanged");
    let ans = p.text(&hint, placeholder)?;
    if current.is_some() && (ans == "unchanged" || ans.is_empty()) {
        return Ok(current.map(str::to_string));
    }
    if ans == "-" || ans.is_empty() {
        Ok(None)
    } else {
        Ok(Some(ans))
    }
}

fn ask_opt_duration<U: Ui>(
    p: &mut U,
    label: &str,
    current: Option<Duration>,
) -> PromptResult<Option<Dur>> {
    let cur = current.map(fmt_duration_toml);
    let hint = if current.is_some() {
        format!("{label} (blank = keep, - = clear)")
    } else {
        format!("{label} (blank = none)")
    };
    let ans = p.text_validated(&hint, cur.as_deref(), |a| {
        if a.is_empty() || a == "-" {
            Ok(())
        } else {
            parse_duration(a).map(|_| ())
        }
    })?;
    if ans == "-" || ans.is_empty() {
        Ok(None)
    } else {
        Ok(parse_duration(&ans).ok().map(Dur))
    }
}

fn ask_opt_bytes<U: Ui>(
    p: &mut U,
    label: &str,
    current: Option<u64>,
) -> PromptResult<Option<ByteSize>> {
    let cur = current.map(fmt_bytes_toml);
    let hint = if current.is_some() {
        format!("{label} (blank = keep, - = clear)")
    } else {
        format!("{label} (blank = none)")
    };
    let ans = p.text_validated(&hint, cur.as_deref(), |a| {
        if a.is_empty() || a == "-" {
            Ok(())
        } else {
            parse_bytes(a).map(|_| ())
        }
    })?;
    if ans == "-" || ans.is_empty() {
        Ok(None)
    } else {
        Ok(parse_bytes(&ans).ok().map(ByteSize))
    }
}

fn reporter_names(cfg: &Config) -> Vec<String> {
    cfg.discord
        .keys()
        .map(|n| format!("discord.{n}"))
        .chain(cfg.smtp.keys().map(|n| format!("smtp.{n}")))
        .collect()
}

fn event_defaults(current: Option<&[String]>, fallback: &[&str]) -> [bool; 5] {
    let active: Vec<&str> = match current {
        Some(list) => list.iter().map(|s| s.as_str()).collect(),
        None => fallback.to_vec(),
    };
    let mut d = [false; 5];
    for (i, name) in EVENT_NAMES.iter().enumerate() {
        d[i] = active.contains(name);
    }
    d
}

fn events_from_indices(idx: &[usize]) -> Vec<String> {
    idx.iter()
        .filter_map(|&i| EVENT_NAMES.get(i))
        .map(|s| s.to_string())
        .collect()
}

fn is_env_name(k: &str) -> bool {
    !k.is_empty()
        && k.bytes()
            .next()
            .is_some_and(|b| b.is_ascii_alphabetic() || b == b'_')
        && k.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

fn capture_mode_str(m: CaptureMode) -> &'static str {
    match m {
        CaptureMode::Capped => "capped",
        CaptureMode::Full => "full",
        CaptureMode::Off => "off",
    }
}

fn parse_capture_mode(s: &str) -> CaptureMode {
    match s {
        "full" => CaptureMode::Full,
        "off" => CaptureMode::Off,
        _ => CaptureMode::Capped,
    }
}

fn smtp_tls_str(t: SmtpTls) -> &'static str {
    match t {
        SmtpTls::Starttls => "starttls",
        SmtpTls::Smtps => "smtps",
        SmtpTls::None => "none",
    }
}

fn parse_smtp_tls(s: &str) -> SmtpTls {
    match s {
        "smtps" => SmtpTls::Smtps,
        "none" => SmtpTls::None,
        _ => SmtpTls::Starttls,
    }
}

fn backup_path(target: &Path) -> PathBuf {
    let mut s = target.as_os_str().to_os_string();
    s.push(".bak");
    PathBuf::from(s)
}

// ----- rendering -----

/// Render a single-unit, `parse_duration`-parseable form (largest unit that
/// divides the value evenly).
fn fmt_duration_toml(d: Duration) -> String {
    let ms = d.as_millis();
    for (per, unit) in [
        (86_400_000u128, "d"),
        (3_600_000, "h"),
        (60_000, "m"),
        (1_000, "s"),
    ] {
        if ms != 0 && ms.is_multiple_of(per) {
            return format!("{}{unit}", ms / per);
        }
    }
    format!("{ms}ms")
}

/// Render a `parse_bytes`-parseable form, preferring round binary then decimal
/// units, else bare bytes.
fn fmt_bytes_toml(n: u64) -> String {
    for (per, unit) in [(1u64 << 30, "GiB"), (1 << 20, "MiB"), (1 << 10, "KiB")] {
        if n >= per && n.is_multiple_of(per) {
            return format!("{}{unit}", n / per);
        }
    }
    for (per, unit) in [(1_000_000_000u64, "GB"), (1_000_000, "MB"), (1_000, "KB")] {
        if n >= per && n.is_multiple_of(per) {
            return format!("{}{unit}", n / per);
        }
    }
    n.to_string()
}

fn toml_str(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            // TOML basic strings forbid raw control chars and raw U+007F (DEL).
            c if (c as u32) < 0x20 || (c as u32) == 0x7f => {
                out.push_str(&format!("\\u{:04X}", c as u32))
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn toml_str_array(items: &[String]) -> String {
    let inner: Vec<String> = items.iter().map(|s| toml_str(s)).collect();
    format!("[{}]", inner.join(", "))
}

/// Render a name as a TOML key segment: bare when it is a valid bare key,
/// otherwise quoted. `valid_slug` permits `.`, which is a key separator, so a
/// name like `team.alpha` MUST be quoted or it becomes a nested table.
fn toml_key(s: &str) -> String {
    if !s.is_empty()
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        s.to_string()
    } else {
        toml_str(s)
    }
}

fn push_table(out: &mut String, header: &str, lines: &[String]) {
    if lines.is_empty() {
        return;
    }
    out.push_str(&format!("\n[{header}]\n"));
    for l in lines {
        out.push_str(l);
        out.push('\n');
    }
}

/// Render the in-memory config back to TOML. Only set fields are emitted, so a
/// round trip is value-lossless for everything the schema models (comments and
/// any unknown keys are dropped — unknown keys are errors in `config validate`
/// anyway).
pub fn render_config(cfg: &Config) -> String {
    let mut o = String::new();
    o.push_str("# uatu configuration — generated by the interactive wizard.\n");
    o.push_str(
        "# Re-run `uatu config wizard` to edit, or hand-edit and run `uatu config validate`.\n",
    );

    let g = &cfg.global;
    let mut lines = Vec::new();
    if let Some(v) = &g.data_dir {
        lines.push(format!("data_dir = {}", toml_str(v)));
    }
    if let Some(v) = &g.host_name {
        lines.push(format!("host_name = {}", toml_str(v)));
    }
    if let Some(v) = g.capture_stdout {
        lines.push(format!("capture_stdout = {v}"));
    }
    if let Some(v) = g.capture_stderr {
        lines.push(format!("capture_stderr = {v}"));
    }
    if let Some(v) = g.capture_mode {
        lines.push(format!("capture_mode = {}", toml_str(capture_mode_str(v))));
    }
    if let Some(v) = g.capture_head_bytes {
        lines.push(format!(
            "capture_head_bytes = {}",
            toml_str(&fmt_bytes_toml(v.0))
        ));
    }
    if let Some(v) = g.capture_tail_bytes {
        lines.push(format!(
            "capture_tail_bytes = {}",
            toml_str(&fmt_bytes_toml(v.0))
        ));
    }
    if let Some(v) = g.min_free_bytes {
        lines.push(format!(
            "min_free_bytes = {}",
            toml_str(&fmt_bytes_toml(v.0))
        ));
    }
    if let Some(v) = g.kill_grace {
        lines.push(format!(
            "kill_grace = {}",
            toml_str(&fmt_duration_toml(v.0))
        ));
    }
    push_table(&mut o, "global", &lines);

    let r = &cfg.retention;
    let mut lines = Vec::new();
    if let Some(v) = r.max_age {
        lines.push(format!("max_age = {}", toml_str(&fmt_duration_toml(v.0))));
    }
    if let Some(v) = r.max_bytes {
        lines.push(format!("max_bytes = {}", toml_str(&fmt_bytes_toml(v.0))));
    }
    push_table(&mut o, "retention", &lines);

    let l = &cfg.log;
    let mut lines = Vec::new();
    if let Some(v) = &l.path {
        lines.push(format!("path = {}", toml_str(v)));
    }
    if let Some(v) = l.max_bytes {
        lines.push(format!("max_bytes = {}", toml_str(&fmt_bytes_toml(v.0))));
    }
    if let Some(v) = &l.trim {
        lines.push(format!("trim = {}", toml_str(v)));
    }
    push_table(&mut o, "log", &lines);

    let mut lines = Vec::new();
    if !cfg.redaction.literals.is_empty() {
        lines.push(format!(
            "literals = {}",
            toml_str_array(&cfg.redaction.literals)
        ));
    }
    if !cfg.redaction.regex.is_empty() {
        lines.push(format!("regex = {}", toml_str_array(&cfg.redaction.regex)));
    }
    push_table(&mut o, "redaction", &lines);

    let n = &cfg.notify;
    let mut lines = Vec::new();
    if let Some(v) = &n.events {
        lines.push(format!("events = {}", toml_str_array(v)));
    }
    if let Some(v) = &n.reporters {
        lines.push(format!("reporters = {}", toml_str_array(v)));
    }
    if let Some(v) = n.failure_output {
        lines.push(format!("failure_output = {v}"));
    }
    push_table(&mut o, "notify", &lines);

    for (name, d) in &cfg.discord {
        let mut lines = vec![format!("webhook_url = {}", toml_str(&d.webhook_url))];
        if let Some(v) = d.max_message_chars {
            lines.push(format!("max_message_chars = {v}"));
        }
        if let Some(v) = &d.events {
            lines.push(format!("events = {}", toml_str_array(v)));
        }
        push_table(
            &mut o,
            &format!("reporters.discord.{}", toml_key(name)),
            &lines,
        );
    }

    for (name, s) in &cfg.smtp {
        let mut lines = vec![format!("host = {}", toml_str(&s.host))];
        if let Some(v) = s.port {
            lines.push(format!("port = {v}"));
        }
        if let Some(v) = s.tls {
            lines.push(format!("tls = {}", toml_str(smtp_tls_str(v))));
        }
        if let Some(v) = &s.username {
            lines.push(format!("username = {}", toml_str(v)));
        }
        if let Some(v) = &s.password {
            lines.push(format!("password = {}", toml_str(v)));
        }
        lines.push(format!("from = {}", toml_str(&s.from)));
        if !s.recipients.is_empty() {
            lines.push(format!("recipients = {}", toml_str_array(&s.recipients)));
        }
        if let Some(v) = s.max_message_chars {
            lines.push(format!("max_message_chars = {v}"));
        }
        if let Some(v) = &s.events {
            lines.push(format!("events = {}", toml_str_array(v)));
        }
        push_table(
            &mut o,
            &format!("reporters.smtp.{}", toml_key(name)),
            &lines,
        );
    }

    for (name, j) in &cfg.jobs {
        let mut lines = Vec::new();
        if let Some(v) = &j.cwd {
            lines.push(format!("cwd = {}", toml_str(v)));
        }
        if !j.env.is_empty() {
            let inner: Vec<String> = j
                .env
                .iter()
                .map(|(k, val)| format!("{k} = {}", toml_str(val)))
                .collect();
            lines.push(format!("env = {{ {} }}", inner.join(", ")));
        }
        if let Some(v) = j.timeout {
            lines.push(format!("timeout = {}", toml_str(&fmt_duration_toml(v.0))));
        }
        if let Some(v) = j.expected_duration {
            lines.push(format!(
                "expected_duration = {}",
                toml_str(&fmt_duration_toml(v.0))
            ));
        }
        if let Some(v) = &j.schedule_label {
            lines.push(format!("schedule_label = {}", toml_str(v)));
        }
        if let Some(v) = &j.reporters {
            lines.push(format!("reporters = {}", toml_str_array(v)));
        }
        if let Some(v) = &j.events {
            lines.push(format!("events = {}", toml_str_array(v)));
        }
        if let Some(v) = j.capture_stdout {
            lines.push(format!("capture_stdout = {v}"));
        }
        if let Some(v) = j.capture_stderr {
            lines.push(format!("capture_stderr = {v}"));
        }
        if let Some(v) = j.capture_mode {
            lines.push(format!("capture_mode = {}", toml_str(capture_mode_str(v))));
        }
        if let Some(v) = j.capture_head_bytes {
            lines.push(format!(
                "capture_head_bytes = {}",
                toml_str(&fmt_bytes_toml(v.0))
            ));
        }
        if let Some(v) = j.capture_tail_bytes {
            lines.push(format!(
                "capture_tail_bytes = {}",
                toml_str(&fmt_bytes_toml(v.0))
            ));
        }
        if let Some(v) = j.kill_grace {
            lines.push(format!(
                "kill_grace = {}",
                toml_str(&fmt_duration_toml(v.0))
            ));
        }
        if let Some(v) = j.failure_output {
            lines.push(format!("failure_output = {v}"));
        }
        push_table(&mut o, &format!("jobs.{}", toml_key(name)), &lines);
    }

    o
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn reparse(text: &str) -> (Config, Vec<String>, Option<String>) {
        let doc: toml::Value = toml::from_str(text).expect("rendered TOML must parse");
        let mut warnings = config::unknown_keys(&doc);
        let mut redaction_invalid = None;
        let cfg = config::test_parse_tables(&doc, &mut warnings, &mut redaction_invalid);
        (cfg, warnings, redaction_invalid)
    }

    #[test]
    fn duration_and_bytes_render_round_trip() {
        for ms in [1u64, 999, 1_000, 90_000, 3_600_000, 86_400_000] {
            let s = fmt_duration_toml(Duration::from_millis(ms));
            assert_eq!(
                parse_duration(&s).unwrap(),
                Duration::from_millis(ms),
                "{s}"
            );
        }
        for n in [
            0u64,
            512,
            1_024,
            65_536,
            1 << 20,
            100_000_000,
            1_000_000_000,
            123_457,
        ] {
            let s = fmt_bytes_toml(n);
            assert_eq!(parse_bytes(&s).unwrap(), n, "{s}");
        }
    }

    #[test]
    fn toml_key_quotes_only_when_needed() {
        assert_eq!(toml_key("ops"), "ops");
        assert_eq!(toml_key("nightly-backup_1"), "nightly-backup_1");
        assert_eq!(toml_key("team.alpha"), "\"team.alpha\"");
    }

    #[test]
    fn dotted_reporter_and_job_names_round_trip() {
        let mut cfg = Config::default();
        cfg.discord.insert(
            "team.alpha".into(),
            DiscordCfg {
                webhook_url: "https://discord.com/api/webhooks/1/x".into(),
                max_message_chars: None,
                events: None,
            },
        );
        cfg.jobs.insert(
            "web.api".into(),
            JobCfg {
                schedule_label: Some("hourly".into()),
                ..Default::default()
            },
        );
        let (parsed, warnings, _) = reparse(&render_config(&cfg));
        assert_eq!(warnings, Vec::<String>::new());
        assert!(
            parsed.discord.contains_key("team.alpha"),
            "dotted reporter survives"
        );
        assert!(parsed.jobs.contains_key("web.api"), "dotted job survives");
    }

    #[test]
    fn control_chars_including_del_are_escaped() {
        let mut cfg = Config::default();
        cfg.redaction.literals = vec!["secret\u{7f}tail".into(), "a\u{1}b".into()];
        let rendered = render_config(&cfg);
        assert!(rendered.contains("\\u007F"), "DEL escaped:\n{rendered}");
        let (parsed, warnings, _) = reparse(&rendered);
        assert!(warnings.is_empty());
        assert_eq!(parsed.redaction.literals[0], "secret\u{7f}tail");
    }

    #[test]
    fn render_round_trips_a_full_config() {
        let mut cfg = Config::default();
        cfg.global.host_name = Some("prod-1".into());
        cfg.global.capture_mode = Some(CaptureMode::Capped);
        cfg.global.capture_head_bytes = Some(ByteSize(65_536));
        cfg.global.kill_grace = Some(Dur(Duration::from_secs(45)));
        cfg.retention.max_age = Some(Dur(Duration::from_secs(30 * 86_400)));
        cfg.retention.max_bytes = Some(ByteSize(1_000_000_000));
        cfg.redaction.literals = vec!["s3cret".into()];
        cfg.redaction.regex = vec![r"password=\S+".into()];
        cfg.notify.events = Some(vec!["failure".into(), "recovery".into()]);
        cfg.notify.reporters = Some(vec!["discord.default".into(), "smtp.ops".into()]);
        cfg.notify.failure_output = Some(true);
        cfg.discord.insert(
            "default".into(),
            DiscordCfg {
                webhook_url: "https://discord.com/api/webhooks/1/\"abc\"".into(),
                max_message_chars: Some(3500),
                events: Some(vec!["failure".into()]),
            },
        );
        cfg.smtp.insert(
            "ops".into(),
            SmtpCfg {
                host: "smtp.example.com".into(),
                port: Some(587),
                tls: Some(SmtpTls::Starttls),
                username: Some("uatu@example.com".into()),
                password: Some("pw\\with\\slashes".into()),
                from: "uatu@example.com".into(),
                recipients: vec!["ops@example.com".into(), "oncall@example.com".into()],
                max_message_chars: None,
                events: Some(vec!["failure".into(), "recovery".into()]),
            },
        );
        let mut env = BTreeMap::new();
        env.insert("RUST_LOG".to_string(), "info".to_string());
        cfg.jobs.insert(
            "nightly-backup".into(),
            JobCfg {
                cwd: Some("/srv/app".into()),
                env,
                timeout: Some(Dur(Duration::from_secs(7200))),
                expected_duration: Some(Dur(Duration::from_secs(2700))),
                schedule_label: Some("nightly at 02:00".into()),
                reporters: Some(vec!["discord.default".into()]),
                events: Some(vec!["failure".into(), "recovery".into()]),
                ..Default::default()
            },
        );

        let rendered = render_config(&cfg);
        let (parsed, warnings, redaction_invalid) = reparse(&rendered);
        assert_eq!(warnings, Vec::<String>::new(), "rendered:\n{rendered}");
        assert!(redaction_invalid.is_none());

        assert_eq!(parsed.global.host_name.as_deref(), Some("prod-1"));
        assert_eq!(parsed.global.capture_head_bytes.map(|b| b.0), Some(65_536));
        assert_eq!(
            parsed.global.kill_grace.map(|d| d.0),
            Some(Duration::from_secs(45))
        );
        assert_eq!(
            parsed.retention.max_age.map(|d| d.0),
            Some(Duration::from_secs(30 * 86_400))
        );
        assert_eq!(parsed.redaction.literals, vec!["s3cret".to_string()]);
        assert_eq!(parsed.redaction.regex, vec![r"password=\S+".to_string()]);
        assert_eq!(
            parsed.notify.events,
            Some(vec!["failure".into(), "recovery".into()])
        );
        let d = parsed.discord.get("default").unwrap();
        assert_eq!(d.webhook_url, "https://discord.com/api/webhooks/1/\"abc\"");
        assert_eq!(d.max_message_chars, Some(3500));
        let s = parsed.smtp.get("ops").unwrap();
        assert_eq!(s.password.as_deref(), Some("pw\\with\\slashes"));
        assert_eq!(s.recipients.len(), 2);
        let j = parsed.jobs.get("nightly-backup").unwrap();
        assert_eq!(j.cwd.as_deref(), Some("/srv/app"));
        assert_eq!(j.env.get("RUST_LOG").map(String::as_str), Some("info"));
        assert_eq!(j.timeout.map(|d| d.0), Some(Duration::from_secs(7200)));
    }

    #[test]
    fn empty_config_renders_to_valid_header_only() {
        let rendered = render_config(&Config::default());
        let (parsed, warnings, _) = reparse(&rendered);
        assert!(warnings.is_empty());
        assert!(parsed.discord.is_empty() && parsed.smtp.is_empty() && parsed.jobs.is_empty());
        assert!(
            !rendered.contains("\n["),
            "no tables for an empty config:\n{rendered}"
        );
    }

    /// Drive the whole wizard with a scripted answer stream and inspect the
    /// resulting config + outcome.
    fn drive(script: &str) -> (Config, bool) {
        let mut cfg = Config::default();
        let mut p = LinePrompt::new(script.as_bytes(), Vec::new());
        let outcome = run_wizard(&mut cfg, &mut p).expect("wizard io");
        let send_test = matches!(outcome, Outcome::Save { send_test: true });
        (cfg, send_test)
    }

    #[test]
    fn wizard_adds_discord_sets_routing_and_job() {
        // 1=reporters; 1=discord, name(default), url, no-restrict, 3=done.
        // 2=notify routing (events default, reporters default, failure_output
        // default). 3=job "nightly" with a schedule label (so it is recorded),
        // remaining fields blank, no restrictions, no env. 6=save. test? no.
        let script = "\
1
1
default
https://discord.com/api/webhooks/1/abc
n
3
2



3
nightly
nightly at 2



n
n
n
6
n
";
        let (cfg, send_test) = drive(script);
        assert!(
            cfg.discord.contains_key("default"),
            "discord reporter added"
        );
        assert_eq!(
            cfg.discord["default"].webhook_url,
            "https://discord.com/api/webhooks/1/abc"
        );
        assert!(
            cfg.discord["default"].events.is_none(),
            "no restriction => all events"
        );
        let reporters = cfg.notify.reporters.clone().unwrap_or_default();
        assert!(
            reporters.contains(&"discord.default".to_string()),
            "auto-enabled globally"
        );
        assert!(cfg.notify.events.is_some(), "routing events set");
        assert_eq!(cfg.notify.failure_output, Some(true));
        assert_eq!(
            cfg.jobs
                .get("nightly")
                .and_then(|j| j.schedule_label.as_deref()),
            Some("nightly at 2")
        );
        assert!(!send_test, "declined the test send");

        // And the produced config is renderable + parses clean.
        let (_, warnings, redaction_invalid) = reparse(&render_config(&cfg));
        assert!(warnings.is_empty());
        assert!(redaction_invalid.is_none());
    }

    #[test]
    fn wizard_does_not_record_an_empty_job() {
        // 3=job, name "emptyjob", every field blank/declined; no reporters
        // exist so the reporter-restriction question is skipped. 6=save.
        let script = "3\nemptyjob\n\n\n\n\nn\nn\n6\n";
        let (cfg, _) = drive(script);
        assert!(
            !cfg.jobs.contains_key("emptyjob"),
            "a job with no fields must not be recorded"
        );
    }

    #[test]
    fn job_section_lists_existing_jobs_for_editing() {
        // A config that already has a [jobs.web] entry: the job section offers
        // it for editing instead of forcing the name to be retyped.
        let mut cfg = Config::default();
        cfg.jobs.insert(
            "web".into(),
            JobCfg {
                schedule_label: Some("hourly".into()),
                ..Default::default()
            },
        );
        // 3=job; select 1 (the existing "web"); keep schedule; blank cwd/timeout/
        // expected; decline event restriction and env; 6=save.
        let script = "3\n1\n\n\n\n\nn\nn\n6\n";
        let mut p = LinePrompt::new(script.as_bytes(), Vec::new());
        run_wizard(&mut cfg, &mut p).expect("wizard");
        assert_eq!(
            cfg.jobs
                .get("web")
                .and_then(|j| j.schedule_label.as_deref()),
            Some("hourly"),
            "editing the existing job preserves its kept fields"
        );
    }

    #[test]
    fn job_section_offers_new_job_alongside_existing() {
        let mut cfg = Config::default();
        cfg.jobs.insert(
            "web".into(),
            JobCfg {
                schedule_label: Some("hourly".into()),
                ..Default::default()
            },
        );
        // 3=job; select 2 ("+ Add a new job"); name "api"; schedule "nightly";
        // blank cwd/timeout/expected; decline restrictions and env; 6=save.
        let script = "3\n2\napi\nnightly\n\n\n\nn\nn\n6\n";
        let mut p = LinePrompt::new(script.as_bytes(), Vec::new());
        run_wizard(&mut cfg, &mut p).expect("wizard");
        assert!(cfg.jobs.contains_key("web"), "existing job is preserved");
        assert_eq!(
            cfg.jobs
                .get("api")
                .and_then(|j| j.schedule_label.as_deref()),
            Some("nightly"),
            "a brand-new job can still be added"
        );
    }

    #[test]
    fn wizard_quit_discards_everything() {
        let (cfg, _) = {
            let mut cfg = Config::default();
            let mut p = LinePrompt::new(&b"7\n"[..], Vec::new());
            let outcome = run_wizard(&mut cfg, &mut p).unwrap();
            assert!(matches!(outcome, Outcome::Quit));
            (cfg, ())
        };
        assert!(cfg.discord.is_empty() && cfg.smtp.is_empty());
    }

    #[test]
    fn blank_stream_saves_empty_config() {
        // Immediate EOF: hub defaults to "Review & save", no reporters => no
        // test prompt consumed.
        let (cfg, send_test) = drive("");
        assert!(cfg.discord.is_empty());
        assert!(!send_test);
    }

    /// A `Ui` driven by a fixed list of responses, so cancel/abort can be
    /// injected at an exact prompt (the line backend never cancels).
    enum Resp {
        Sel(usize),
        Multi(Vec<usize>),
        Text(&'static str),
        Cancel,
        Abort,
    }

    struct ScriptUi {
        steps: std::collections::VecDeque<Resp>,
    }

    impl ScriptUi {
        fn new(steps: Vec<Resp>) -> ScriptUi {
            ScriptUi {
                steps: steps.into(),
            }
        }
        fn next(&mut self, what: &str) -> Resp {
            self.steps
                .pop_front()
                .unwrap_or_else(|| panic!("ran out of scripted steps at a {what} prompt"))
        }
    }

    impl Ui for ScriptUi {
        fn say(&mut self, _line: &str) -> io::Result<()> {
            Ok(())
        }
        fn at_eof(&self) -> bool {
            false
        }
        fn text(&mut self, _q: &str, _d: Option<&str>) -> PromptResult<String> {
            match self.next("text") {
                Resp::Text(s) => Ok(s.to_string()),
                Resp::Cancel => Err(PromptError::Cancel),
                Resp::Abort => Err(PromptError::Abort),
                _ => panic!("expected a text response"),
            }
        }
        fn confirm(&mut self, _q: &str, _d: bool) -> PromptResult<bool> {
            match self.next("confirm") {
                Resp::Cancel => Err(PromptError::Cancel),
                Resp::Abort => Err(PromptError::Abort),
                _ => panic!("expected a cancel/abort at this confirm"),
            }
        }
        fn select(&mut self, _q: &str, _o: &[&str], _d: usize) -> PromptResult<usize> {
            match self.next("select") {
                Resp::Sel(i) => Ok(i),
                Resp::Cancel => Err(PromptError::Cancel),
                Resp::Abort => Err(PromptError::Abort),
                _ => panic!("expected a select response"),
            }
        }
        fn multi_select(&mut self, _q: &str, _o: &[&str], _d: &[bool]) -> PromptResult<Vec<usize>> {
            match self.next("multi_select") {
                Resp::Multi(v) => Ok(v),
                Resp::Cancel => Err(PromptError::Cancel),
                Resp::Abort => Err(PromptError::Abort),
                _ => panic!("expected a multi_select response"),
            }
        }
    }

    #[test]
    fn esc_in_a_section_discards_edits_and_returns_to_menu() {
        // Enter Global settings, answer the host prompt, then Esc — the section
        // must commit nothing — then Save from the hub.
        let mut cfg = Config::default();
        let mut ui = ScriptUi::new(vec![
            Resp::Sel(3),         // hub: Global & capture settings
            Resp::Text("myhost"), // host name
            Resp::Cancel,         // Esc at capture-mode select
            Resp::Sel(5),         // hub: Review & save
        ]);
        let outcome = run_wizard(&mut cfg, &mut ui).expect("wizard");
        assert!(matches!(outcome, Outcome::Save { .. }));
        // The host we typed was discarded along with the cancelled section.
        assert!(
            cfg.global.host_name.is_none(),
            "partial edit must be discarded"
        );
        assert!(cfg.global.capture_mode.is_none());
    }

    #[test]
    fn esc_discards_a_partial_notify_edit() {
        let mut cfg = Config::default();
        let mut ui = ScriptUi::new(vec![
            Resp::Sel(1),               // hub: Notification routing
            Resp::Multi(vec![0, 1, 2]), // events chosen...
            Resp::Cancel,               // ...but Esc at failure_output confirm
            Resp::Sel(5),               // hub: Review & save
        ]);
        run_wizard(&mut cfg, &mut ui).expect("wizard");
        assert!(
            cfg.notify.events.is_none(),
            "events must not be committed when the section is cancelled"
        );
    }

    #[test]
    fn ctrl_c_aborts_the_whole_wizard() {
        let mut cfg = Config::default();
        let mut ui = ScriptUi::new(vec![Resp::Abort]); // Ctrl-C at the hub
        let res = run_wizard(&mut cfg, &mut ui);
        assert!(matches!(res, Err(PromptError::Abort)));
    }

    #[test]
    fn esc_at_the_hub_redisplays_then_save() {
        let mut cfg = Config::default();
        let mut ui = ScriptUi::new(vec![
            Resp::Cancel, // Esc at the hub is a no-op (redisplay)
            Resp::Sel(5), // then Review & save
        ]);
        let outcome = run_wizard(&mut cfg, &mut ui).expect("wizard");
        assert!(matches!(outcome, Outcome::Save { .. }));
    }
}
