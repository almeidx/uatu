//! Events and reporting content (SPEC §8): event routing (job events ∩
//! reporter events), Discord embed and SMTP plain-text message building.

use std::collections::BTreeSet;

use crate::config::{Config, DEFAULT_DISCORD_MAX_CHARS, DEFAULT_EVENTS, DEFAULT_SMTP_MAX_CHARS};
use crate::db::RunRow;
use crate::util::{format_duration_ms, local_time, rfc3339, tail_chars};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Event {
    Success,
    Failure,
    Recovery,
    Stale,
    LongRun,
    Digest,
}

pub const ALL_EVENTS: [Event; 6] = [
    Event::Success,
    Event::Failure,
    Event::Recovery,
    Event::Stale,
    Event::LongRun,
    Event::Digest,
];

impl Event {
    pub fn as_str(&self) -> &'static str {
        match self {
            Event::Success => "success",
            Event::Failure => "failure",
            Event::Recovery => "recovery",
            Event::Stale => "stale",
            Event::LongRun => "long_run",
            Event::Digest => "digest",
        }
    }

    pub fn parse(s: &str) -> Option<Event> {
        match s {
            "success" => Some(Event::Success),
            "failure" => Some(Event::Failure),
            "recovery" => Some(Event::Recovery),
            "stale" => Some(Event::Stale),
            "long_run" => Some(Event::LongRun),
            "digest" => Some(Event::Digest),
            _ => None,
        }
    }
}

/// Parse an events list leniently: unknown names are reported, known ones
/// kept (SPEC §10 runtime leniency; `validate` treats them as errors).
pub fn parse_events(list: &[String], warnings: &mut Vec<String>) -> BTreeSet<Event> {
    let mut set = BTreeSet::new();
    for s in list {
        match Event::parse(s) {
            Some(e) => {
                set.insert(e);
            }
            None => warnings.push(format!(
                "unknown event {s:?} (valid: success, failure, recovery, stale, long_run, digest)"
            )),
        }
    }
    set
}

/// Job-effective events: job config > global notify > default
/// ["success", "failure"]. `--expected-duration` on the CLI implies the user
/// wants the long_run alert (SPEC §3 flag doc) even without an events entry.
pub fn job_events(cfg: &Config, job_id: &str, expected_from_cli: bool) -> BTreeSet<Event> {
    let mut warnings = Vec::new();
    let list = cfg
        .jobs
        .get(job_id)
        .and_then(|j| j.events.clone())
        .or_else(|| cfg.notify.events.clone())
        .unwrap_or_else(|| DEFAULT_EVENTS.iter().map(|s| s.to_string()).collect());
    let mut set = parse_events(&list, &mut warnings);
    if expected_from_cli {
        set.insert(Event::LongRun);
    }
    set
}

/// Job-effective reporter list: job config > global notify > none.
pub fn job_reporters(cfg: &Config, job_id: &str) -> Vec<String> {
    cfg.jobs
        .get(job_id)
        .and_then(|j| j.reporters.clone())
        .or_else(|| cfg.notify.reporters.clone())
        .unwrap_or_default()
}

pub enum ReporterRef<'a> {
    Discord(&'a crate::config::DiscordCfg),
    Smtp(&'a crate::config::SmtpCfg),
}

/// Look up `discord.<name>` / `smtp.<name>` in the config.
pub fn lookup_reporter<'a>(cfg: &'a Config, full_name: &str) -> Option<ReporterRef<'a>> {
    let (kind, name) = full_name.split_once('.')?;
    match kind {
        "discord" => cfg.discord.get(name).map(ReporterRef::Discord),
        "smtp" => cfg.smtp.get(name).map(ReporterRef::Smtp),
        _ => None,
    }
}

/// Per-reporter events filter; default: all events (SPEC §4).
pub fn reporter_accepts(cfg: &Config, full_name: &str, event: Event) -> bool {
    let events = match lookup_reporter(cfg, full_name) {
        Some(ReporterRef::Discord(d)) => d.events.clone(),
        Some(ReporterRef::Smtp(s)) => s.events.clone(),
        None => return false,
    };
    match events {
        None => true,
        Some(list) => {
            let mut w = Vec::new();
            parse_events(&list, &mut w).contains(&event)
        }
    }
}

/// Effective delivery targets for a (job, event): the event must be in the
/// job-effective set AND in each reporter's set (SPEC §4, §8).
pub fn reporters_for_event(
    cfg: &Config,
    job_id: &str,
    event: Event,
    expected_from_cli: bool,
) -> Vec<String> {
    if !job_events(cfg, job_id, expected_from_cli).contains(&event) {
        return Vec::new();
    }
    job_reporters(cfg, job_id)
        .into_iter()
        .filter(|name| reporter_accepts(cfg, name, event))
        .collect()
}

/// Digest targets are controlled by `digest`, not by job-effective `events`.
/// Per-reporter event filters may still opt out by omitting `digest`.
pub fn reporters_for_digest(cfg: &Config, job_id: &str) -> Vec<String> {
    job_reporters(cfg, job_id)
        .into_iter()
        .filter(|name| reporter_accepts(cfg, name, Event::Digest))
        .collect()
}

/// Everything needed to render one notification.
pub struct MsgCtx<'a> {
    pub run: &'a RunRow,
    pub event: Event,
    pub host: &'a str,
    /// `Some((event_ms, now_ms))` when this delivery is delayed/retried.
    pub delayed: Option<(i64, i64)>,
    /// Redacted output tails for failure notifications (stdout, stderr).
    pub snippets: Option<(String, String)>,
    pub output_files: Vec<String>,
}

/// Everything needed to render one digest notification.
pub struct DigestMsgCtx<'a> {
    pub job_id: &'a str,
    pub period: &'a str,
    pub window_start_ms: i64,
    pub window_end_ms: i64,
    pub host: &'a str,
    pub runs: &'a [RunRow],
    /// `Some((due_ms, now_ms))` when a digest retry is delayed.
    pub delayed: Option<(i64, i64)>,
}

fn event_color(event: Event, run: &RunRow) -> u32 {
    match event {
        Event::Success | Event::Recovery => 0x2ECC71, // green
        Event::Failure if run.timeout_fired => 0xE67E22, // orange (timeout)
        Event::Failure => 0xE74C3C,                   // red
        Event::LongRun => 0xF1C40F,                   // yellow
        Event::Stale => 0x95A5A6,                     // grey
        Event::Digest => 0x3498DB,                    // blue
    }
}

pub fn status_detail(run: &RunRow) -> String {
    match run.status.as_str() {
        "success" => "exit code 0".to_string(),
        "failure" => match (run.exit_code, run.signal_no) {
            (Some(c), _) => format!("exit code {c}"),
            (None, Some(s)) => format!("killed by signal {s}"),
            _ => "failed".to_string(),
        },
        "timeout" => "configured timeout fired (exit code 124)".to_string(),
        "start_failed" => format!(
            "could not start: {}",
            run.start_error.as_deref().unwrap_or("unknown error")
        ),
        "stale" => {
            "uatu lost track of this run (wrapper died before recording a result)".to_string()
        }
        "active" => "still running".to_string(),
        other => other.to_string(),
    }
}

fn delayed_line(delayed: Option<(i64, i64)>) -> Option<String> {
    delayed.map(|(event_ms, now_ms)| {
        format!(
            "DELAYED NOTIFICATION: event occurred at {}, delivered at {}",
            rfc3339(event_ms),
            rfc3339(now_ms)
        )
    })
}

fn delayed_digest_line(delayed: Option<(i64, i64)>) -> Option<String> {
    delayed.map(|(due_ms, now_ms)| {
        format!(
            "DELAYED DIGEST RETRY: digest was due at {}, delivered at {}",
            rfc3339(due_ms),
            rfc3339(now_ms)
        )
    })
}

fn common_lines(ctx: &MsgCtx) -> Vec<String> {
    let run = ctx.run;
    let mut lines = vec![
        format!("host: {}", ctx.host),
        format!("run: {}", run.run_id),
        format!("status: {} ({})", run.status, status_detail(run)),
    ];
    if let Some(label) = &run.schedule_label {
        lines.push(format!("schedule: {label}"));
    }
    if let Some(d) = run.duration_ms() {
        if !run.end_is_detection {
            lines.push(format!("duration: {}", format_duration_ms(d.max(0) as u64)));
        }
    }
    if ctx.event == Event::Stale {
        lines.push(format!(
            "marked stale at: {} (detection time, not actual end)",
            rfc3339(run.end_ms.unwrap_or(run.start_ms))
        ));
    }
    if ctx.event == Event::LongRun {
        if let Some(exp) = run.expected_duration_ms {
            lines.push(format!(
                "still running past expected duration ({})",
                format_duration_ms(exp.max(0) as u64)
            ));
        }
    }
    if let Some(by) = &run.interrupted_by {
        lines.push(format!("wrapper interrupted by: {by}"));
    }
    lines
}

/// Discord embed payload (SPEC §8). Status-colored, Discord timestamp markup,
/// capped to `max_message_chars` (embed description hard limit 4096).
pub fn discord_payload(ctx: &MsgCtx, max_message_chars: Option<usize>) -> serde_json::Value {
    let run = ctx.run;
    let cap = max_message_chars
        .unwrap_or(DEFAULT_DISCORD_MAX_CHARS)
        .min(4096);
    let title = format!("{}: {}", ctx.event.as_str().to_uppercase(), run.job_id);

    let mut desc_lines = Vec::new();
    if let Some(d) = delayed_line(ctx.delayed) {
        desc_lines.push(format!("⏰ {d}"));
    }
    desc_lines.extend(common_lines(ctx));
    desc_lines.push(format!("started: <t:{}:F>", run.start_ms / 1000));
    let mut description = desc_lines.join("\n");

    if let Some((out, err)) = &ctx.snippets {
        let budget = cap.saturating_sub(description.chars().count() + 200);
        let each = (budget / 2).min(900);
        if !out.is_empty() && each > 40 {
            description.push_str(&format!(
                "\nstdout (tail):\n```\n{}\n```",
                tail_chars(out, each)
            ));
        }
        if !err.is_empty() && each > 40 {
            description.push_str(&format!(
                "\nstderr (tail):\n```\n{}\n```",
                tail_chars(err, each)
            ));
        }
    }
    if !ctx.output_files.is_empty() {
        description.push_str(&format!("\noutput files: {}", ctx.output_files.join(", ")));
    }
    if description.chars().count() > cap {
        description = description
            .chars()
            .take(cap.saturating_sub(1))
            .collect::<String>()
            + "…";
    }

    serde_json::json!({
        "embeds": [{
            "title": title,
            "description": description,
            "color": event_color(ctx.event, run),
        }]
    })
}

fn digest_lines(ctx: &DigestMsgCtx, discord: bool) -> Vec<String> {
    let mut lines = Vec::new();
    if let Some(d) = delayed_digest_line(ctx.delayed) {
        lines.push(d);
    }
    lines.push(format!("host: {}", ctx.host));
    lines.push(format!("period: {}", ctx.period));
    lines.push(format!(
        "window: {} to {} (UTC)",
        rfc3339(ctx.window_start_ms),
        rfc3339(ctx.window_end_ms)
    ));
    lines.push(format!("total runs: {}", ctx.runs.len()));
    let count = |status: &str| ctx.runs.iter().filter(|r| r.status == status).count();
    let success = count("success");
    let failure = count("failure");
    let timeout = count("timeout");
    let start_failed = count("start_failed");
    let stale = count("stale");
    lines.push(format!(
        "statuses: success={success}, failure={failure}, timeout={timeout}, start_failed={start_failed}, stale={stale}"
    ));
    let durations: Vec<i64> = ctx
        .runs
        .iter()
        .filter(|r| !r.end_is_detection)
        .filter_map(|r| r.duration_ms())
        .collect();
    if !durations.is_empty() {
        let total: i64 = durations.iter().sum();
        let avg = (total / durations.len() as i64).max(0) as u64;
        let max = durations.iter().copied().max().unwrap_or(0).max(0) as u64;
        lines.push(format!(
            "duration: avg {}, max {}",
            format_duration_ms(avg),
            format_duration_ms(max)
        ));
    }
    if let Some(first) = ctx.runs.first() {
        lines.push(format!("first run: {}", rfc3339(first.start_ms)));
    }
    if let Some(last) = ctx.runs.last() {
        let end = last.end_ms.unwrap_or(last.start_ms);
        lines.push(format!("last run: {}", rfc3339(end)));
    }

    let shown = ctx.runs.len().min(10);
    if shown > 0 {
        lines.push("recent runs:".to_string());
        let start = ctx.runs.len().saturating_sub(shown);
        for run in &ctx.runs[start..] {
            let short_id: String = run.run_id.chars().take(8).collect();
            let duration = run
                .duration_ms()
                .filter(|_| !run.end_is_detection)
                .map(|d| format_duration_ms(d.max(0) as u64))
                .unwrap_or_else(|| "-".to_string());
            let started = if discord {
                format!("<t:{}:f>", run.start_ms / 1000)
            } else {
                rfc3339(run.start_ms)
            };
            lines.push(format!(
                "- {short_id} started {started}, duration {duration}"
            ));
        }
        let omitted = ctx.runs.len().saturating_sub(shown);
        if omitted > 0 {
            lines.push(format!("... and {omitted} earlier run(s)"));
        }
    }
    lines
}

pub fn discord_digest_payload(
    ctx: &DigestMsgCtx,
    max_message_chars: Option<usize>,
) -> serde_json::Value {
    let cap = max_message_chars
        .unwrap_or(DEFAULT_DISCORD_MAX_CHARS)
        .min(4096);
    let title = format!("DIGEST: {}", ctx.job_id);
    let mut description = digest_lines(ctx, true).join("\n");
    if description.chars().count() > cap {
        description = description
            .chars()
            .take(cap.saturating_sub(1))
            .collect::<String>()
            + "…";
    }

    serde_json::json!({
        "embeds": [{
            "title": title,
            "description": description,
            "color": 0x3498DBu32,
        }]
    })
}

/// SMTP subject + plain-text body (SPEC §8). Subject:
/// `[uatu] <EVENT>: <job-id> on <host>`. Body shows UTC and host-local time.
pub fn email_message(ctx: &MsgCtx, max_message_chars: Option<usize>) -> (String, String) {
    let run = ctx.run;
    let cap = max_message_chars.unwrap_or(DEFAULT_SMTP_MAX_CHARS);
    let subject = format!(
        "[uatu] {}: {} on {}",
        ctx.event.as_str().to_uppercase(),
        run.job_id,
        ctx.host
    );
    let mut lines = Vec::new();
    if let Some(d) = delayed_line(ctx.delayed) {
        lines.push(d);
        lines.push(String::new());
    }
    lines.push(format!("job: {}", run.job_id));
    lines.extend(common_lines(ctx));
    lines.push(format!(
        "started: {} (UTC) / {} (host local)",
        rfc3339(run.start_ms),
        local_time(run.start_ms)
    ));
    if let Some(end) = run.end_ms {
        let label = if run.end_is_detection {
            "detected"
        } else {
            "ended"
        };
        lines.push(format!(
            "{label}: {} (UTC) / {} (host local)",
            rfc3339(end),
            local_time(end)
        ));
    }
    let mut body = lines.join("\n");
    if let Some((out, err)) = &ctx.snippets {
        let budget = cap.saturating_sub(body.chars().count() + 200);
        let each = (budget / 2).min(4000);
        if !out.is_empty() && each > 40 {
            body.push_str(&format!(
                "\n\n--- stdout (redacted tail) ---\n{}",
                tail_chars(out, each)
            ));
        }
        if !err.is_empty() && each > 40 {
            body.push_str(&format!(
                "\n\n--- stderr (redacted tail) ---\n{}",
                tail_chars(err, each)
            ));
        }
    }
    if !ctx.output_files.is_empty() {
        body.push_str("\n\noutput files:\n");
        for f in &ctx.output_files {
            body.push_str(&format!("  {f}\n"));
        }
    }
    if body.chars().count() > cap {
        body = body.chars().take(cap.saturating_sub(1)).collect::<String>() + "…";
    }
    (subject, body)
}

pub fn digest_email_message(
    ctx: &DigestMsgCtx,
    max_message_chars: Option<usize>,
) -> (String, String) {
    let cap = max_message_chars.unwrap_or(DEFAULT_SMTP_MAX_CHARS);
    let subject = format!("[uatu] DIGEST: {} on {}", ctx.job_id, ctx.host);
    let mut body = digest_lines(ctx, false).join("\n");
    if body.chars().count() > cap {
        body = body.chars().take(cap.saturating_sub(1)).collect::<String>() + "…";
    }
    (subject, body)
}

/// Test notification content (SPEC §3 `notify test`): visually distinct and
/// carrying host + config path so environments are distinguishable.
pub fn test_discord_payload(reporter: &str, host: &str, config_path: &str) -> serde_json::Value {
    serde_json::json!({
        "embeds": [{
            "title": format!("TEST: {reporter}"),
            "description": format!(
                "This is a uatu test notification.\nhost: {host}\nconfig: {config_path}\nIf you can read this, the reporter works."
            ),
            "color": 0x5865F2u32, // blurple
        }]
    })
}

pub fn test_email_message(reporter: &str, host: &str, config_path: &str) -> (String, String) {
    (
        format!("[uatu] TEST: {reporter} on {host}"),
        format!(
            "This is a uatu test notification.\n\nhost: {host}\nconfig: {config_path}\n\nIf you can read this, the reporter works.\n"
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    fn cfg(text: &str) -> Config {
        let doc: toml::Value = toml::from_str(text).unwrap();
        let mut warnings = Vec::new();
        let mut red = None;
        let out = crate::config::test_parse_tables(&doc, &mut warnings, &mut red);
        assert!(red.is_none());
        out
    }

    #[test]
    fn default_events_success_failure() {
        let c = cfg("");
        let ev = job_events(&c, "j", false);
        assert!(ev.contains(&Event::Success) && ev.contains(&Event::Failure));
        assert!(!ev.contains(&Event::Recovery) && !ev.contains(&Event::Stale));
    }

    #[test]
    fn per_reporter_intersection() {
        let c = cfg(r#"
[notify]
events = ["success", "failure", "recovery", "stale"]
reporters = ["discord.d", "smtp.s"]
[reporters.discord.d]
webhook_url = "https://x"
[reporters.smtp.s]
host = "h"
from = "f@x"
recipients = ["o@x"]
events = ["failure", "recovery", "stale"]
"#);
        // Discord (no filter = all) gets success; SMTP does not.
        assert_eq!(
            reporters_for_event(&c, "j", Event::Success, false),
            vec!["discord.d"]
        );
        // Both get failure.
        assert_eq!(
            reporters_for_event(&c, "j", Event::Failure, false),
            vec!["discord.d", "smtp.s"]
        );
        // long_run not in job events -> nobody.
        assert!(reporters_for_event(&c, "j", Event::LongRun, false).is_empty());
        // --expected-duration CLI flag implies long_run opt-in.
        assert_eq!(
            reporters_for_event(&c, "j", Event::LongRun, true),
            vec!["discord.d"]
        );
        // Unknown reporter reference silently filtered at runtime.
        let c2 = cfg("[notify]\nreporters = [\"discord.nope\"]\n");
        assert!(reporters_for_event(&c2, "j", Event::Failure, false).is_empty());
    }

    #[test]
    fn job_overrides_notify_events() {
        let c = cfg(r#"
[notify]
events = ["success", "failure"]
reporters = ["discord.d"]
[reporters.discord.d]
webhook_url = "https://x"
[jobs.quiet]
events = ["failure", "recovery"]
"#);
        assert!(reporters_for_event(&c, "quiet", Event::Success, false).is_empty());
        assert_eq!(
            reporters_for_event(&c, "quiet", Event::Recovery, false),
            vec!["discord.d"]
        );
        assert_eq!(
            reporters_for_event(&c, "other", Event::Success, false),
            vec!["discord.d"]
        );
    }

    fn sample_run() -> RunRow {
        RunRow {
            run_id: "01TESTRUN".into(),
            job_id: "nightly".into(),
            job_id_inferred: false,
            inferred_basename: None,
            mode: "direct".into(),
            argv_json: None,
            shell_cmd: None,
            cwd: None,
            env_names_json: None,
            host: "h".into(),
            schedule_label: Some("nightly at 2".into()),
            status: "failure".into(),
            start_ms: 1_700_000_000_000,
            end_ms: Some(1_700_000_042_000),
            end_is_detection: false,
            exit_code: Some(1),
            signal_no: None,
            timeout_fired: false,
            interrupted_by: None,
            start_error: None,
            wrapper_pid: 1,
            wrapper_start_ticks: 1,
            boot_id: "b".into(),
            child_pid: Some(2),
            expected_duration_ms: None,
            long_run_fired: false,
            detached_children: false,
            stdout: Default::default(),
            stderr: Default::default(),
            output_pruned_ms: None,
        }
    }

    #[test]
    fn discord_embed_shape_and_cap() {
        let run = sample_run();
        let ctx = MsgCtx {
            run: &run,
            event: Event::Failure,
            host: "prod-1",
            delayed: None,
            snippets: Some(("x".repeat(10_000), "err line".into())),
            output_files: vec!["/p/stdout.log".into()],
        };
        let v = discord_payload(&ctx, Some(3500));
        let embed = &v["embeds"][0];
        assert_eq!(embed["title"], "FAILURE: nightly");
        assert_eq!(embed["color"], 0xE74C3C);
        let desc = embed["description"].as_str().unwrap();
        assert!(desc.chars().count() <= 3500);
        assert!(desc.contains("<t:1700000000:F>"));
        assert!(desc.contains("exit code 1"));
        assert!(desc.contains("stdout (tail)"));
    }

    #[test]
    fn email_subject_format_and_delayed_marker() {
        let run = sample_run();
        let ctx = MsgCtx {
            run: &run,
            event: Event::Failure,
            host: "prod-worker-01",
            delayed: Some((1_700_000_042_000, 1_700_003_642_000)),
            snippets: None,
            output_files: vec![],
        };
        let (subject, body) = email_message(&ctx, None);
        assert_eq!(subject, "[uatu] FAILURE: nightly on prod-worker-01");
        assert!(body.contains("DELAYED NOTIFICATION"));
        assert!(body.contains("2023-11-14T22:14:02Z")); // event time, RFC3339 UTC
        assert!(body.contains("(host local)"));
    }
}
