//! Events and reporting content (SPEC §8): immediate-event routing uses job
//! events ∩ reporter events; digest routing instead uses the effective cadence
//! plus the reporter's digest filter. This module also builds Discord embeds
//! and SMTP plain-text messages.

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

/// Comma-separated list of every valid event name, for `valid: ...`
/// diagnostics. Derived from [`ALL_EVENTS`] so it never drifts.
pub fn valid_events() -> String {
    ALL_EVENTS
        .iter()
        .map(|e| e.as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

/// Parse an events list leniently: unknown names are reported, known ones
/// kept (SPEC §10 runtime leniency; `validate` treats them as errors).
pub fn parse_events(list: &[String], warnings: &mut Vec<String>) -> BTreeSet<Event> {
    let mut set = BTreeSet::new();
    let mut valid: Option<String> = None;
    for s in list {
        match Event::parse(s) {
            Some(e) => {
                set.insert(e);
            }
            None => {
                let valid = valid.get_or_insert_with(valid_events);
                warnings.push(format!("unknown event {s:?} (valid: {valid})"));
            }
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

/// Maximum pre-aggregated rows accepted by the digest renderer. Database
/// queries should use these as their `LIMIT`s; the renderer applies them again
/// defensively so its work stays bounded even for an invalid caller.
pub const DIGEST_MAX_JOB_SUMMARIES: usize = 64;
pub const DIGEST_MAX_PROBLEM_DETAILS: usize = 128;
pub const DIGEST_MAX_SUCCESS_DETAILS: usize = 64;

/// Exact counts for the finite run-status vocabulary.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DigestStatusCounts {
    pub success: u64,
    pub failure: u64,
    pub timeout: u64,
    pub start_failed: u64,
    pub stale: u64,
    pub active: u64,
}

impl DigestStatusCounts {
    fn has_problem(self) -> bool {
        self.failure
            .saturating_add(self.timeout)
            .saturating_add(self.start_failed)
            .saturating_add(self.stale)
            .saturating_add(self.active)
            > 0
    }
}

/// Duration aggregate for executions whose end timestamp is an actual child
/// end rather than a later stale-detection timestamp.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DigestDurationSummary {
    pub average_ms: u64,
    pub max_ms: u64,
}

/// Latest execution fields shown in a per-job summary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DigestLatestExecution {
    pub status: String,
    pub start_ms: i64,
    /// `None` for an active run or a stale run whose end is only a detection
    /// timestamp.
    pub duration_ms: Option<u64>,
    pub schedule_label: Option<String>,
}

/// Exact aggregate for one job. `job_summaries` in [`DigestSummary`] is a
/// capped priority prefix, while these counts still describe every execution
/// for the job.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DigestJobSummary {
    pub job_id: String,
    pub total_executions: u64,
    pub statuses: DigestStatusCounts,
    pub durations: Option<DigestDurationSummary>,
    pub latest: DigestLatestExecution,
}

/// One bounded execution-detail row. Problem and success totals remain exact
/// even when their detail vectors are capped.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DigestExecutionDetail {
    pub job_id: String,
    pub run_id: String,
    pub status: String,
    pub start_ms: i64,
    pub duration_ms: Option<u64>,
}

/// Pre-aggregated digest data. Callers must order/cap job summaries as problem
/// jobs first then job id, problem details by job id then newest first, and
/// success details newest first. The renderer sorts each bounded prefix again
/// for deterministic output.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DigestSummary {
    pub total_jobs: u64,
    pub total_executions: u64,
    pub statuses: DigestStatusCounts,
    pub total_problem_executions: u64,
    pub total_success_executions: u64,
    pub job_summaries: Vec<DigestJobSummary>,
    pub problem_details: Vec<DigestExecutionDetail>,
    pub success_details: Vec<DigestExecutionDetail>,
}

/// Everything needed to render one digest notification.
pub struct DigestMsgCtx<'a> {
    pub period: &'a str,
    pub window_start_ms: i64,
    pub window_end_ms: i64,
    /// Host recorded for this cohort, not the current config's host label.
    pub recorded_host: &'a str,
    pub summary: &'a DigestSummary,
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

#[derive(Clone, Copy)]
enum DigestFormat {
    Discord,
    Email,
}

struct DigestModel<'a> {
    jobs: Vec<&'a DigestJobSummary>,
    problems: Vec<&'a DigestExecutionDetail>,
    successes: Vec<&'a DigestExecutionDetail>,
}

struct DigestSelection {
    base_lines: Vec<bool>,
    /// `None` omits the job. `Some(false)` drops only its optional schedule.
    job_schedules: Vec<Option<bool>>,
    problem_count: usize,
    success_count: usize,
}

const DIGEST_MAX_FIELD_CHARS: usize = 512;

fn digest_field_fits(value: &str) -> bool {
    value.chars().take(DIGEST_MAX_FIELD_CHARS + 1).count() <= DIGEST_MAX_FIELD_CHARS
}

fn digest_field(value: &str) -> Option<String> {
    let mut out = String::new();
    for (index, c) in value.chars().enumerate() {
        if index == DIGEST_MAX_FIELD_CHARS {
            return None;
        }
        out.push(if matches!(c, '\n' | '\r') { ' ' } else { c });
    }
    Some(out)
}

fn digest_display_field(value: &str) -> String {
    digest_field(value).unwrap_or_else(|| "[oversized]".to_string())
}

fn digest_time(start_ms: i64, format: DigestFormat) -> String {
    match format {
        DigestFormat::Discord => format!("<t:{}:f>", start_ms / 1000),
        DigestFormat::Email => rfc3339(start_ms),
    }
}

fn digest_duration(duration_ms: Option<u64>) -> String {
    duration_ms
        .map(format_duration_ms)
        .unwrap_or_else(|| "-".to_string())
}

fn digest_status_counts(counts: DigestStatusCounts) -> String {
    let entries = [
        ("success", counts.success),
        ("failure", counts.failure),
        ("timeout", counts.timeout),
        ("start_failed", counts.start_failed),
        ("stale", counts.stale),
        ("active", counts.active),
    ];
    let shown = entries
        .into_iter()
        .filter(|(_, count)| *count > 0)
        .map(|(status, count)| format!("{status}={count}"))
        .collect::<Vec<_>>();
    if shown.is_empty() {
        return "none".to_string();
    }
    shown.join(", ")
}

fn digest_row_limit(total: u64, maximum: usize) -> usize {
    usize::try_from(total).unwrap_or(maximum).min(maximum)
}

fn digest_model(summary: &DigestSummary) -> DigestModel<'_> {
    let mut jobs = summary
        .job_summaries
        .iter()
        .take(digest_row_limit(
            summary.total_jobs,
            DIGEST_MAX_JOB_SUMMARIES,
        ))
        .filter(|job| digest_field_fits(&job.job_id))
        .filter(|job| digest_field_fits(&job.latest.status))
        .collect::<Vec<_>>();
    jobs.sort_by(|left, right| {
        right
            .statuses
            .has_problem()
            .cmp(&left.statuses.has_problem())
            .then_with(|| left.job_id.cmp(&right.job_id))
    });

    let mut problems = summary
        .problem_details
        .iter()
        .take(digest_row_limit(
            summary.total_problem_executions,
            DIGEST_MAX_PROBLEM_DETAILS,
        ))
        .filter(|detail| digest_field_fits(&detail.job_id))
        .filter(|detail| digest_field_fits(&detail.run_id))
        .filter(|detail| digest_field_fits(&detail.status))
        .collect::<Vec<_>>();
    problems.sort_by(|left, right| {
        left.job_id
            .cmp(&right.job_id)
            .then_with(|| right.start_ms.cmp(&left.start_ms))
            .then_with(|| left.run_id.cmp(&right.run_id))
    });

    let mut successes = summary
        .success_details
        .iter()
        .take(digest_row_limit(
            summary.total_success_executions,
            DIGEST_MAX_SUCCESS_DETAILS,
        ))
        .filter(|detail| digest_field_fits(&detail.job_id))
        .filter(|detail| digest_field_fits(&detail.run_id))
        .filter(|detail| digest_field_fits(&detail.status))
        .collect::<Vec<_>>();
    successes.sort_by(|left, right| {
        right
            .start_ms
            .cmp(&left.start_ms)
            .then_with(|| left.job_id.cmp(&right.job_id))
            .then_with(|| left.run_id.cmp(&right.run_id))
    });

    DigestModel {
        jobs,
        problems,
        successes,
    }
}

fn digest_base_lines(ctx: &DigestMsgCtx<'_>) -> Vec<String> {
    let mut lines = Vec::new();
    if let Some(delayed) = delayed_digest_line(ctx.delayed) {
        lines.push(delayed);
    }
    if let Some(host) = digest_field(ctx.recorded_host) {
        lines.push(format!("host: {host}"));
    }
    if let Some(period) = digest_field(ctx.period) {
        lines.push(format!("period: {period}"));
    }
    lines.push(format!(
        "window: {} to {} (UTC)",
        rfc3339(ctx.window_start_ms),
        rfc3339(ctx.window_end_ms)
    ));
    lines.push(format!("observed jobs: {}", ctx.summary.total_jobs));
    lines.push(format!("executions: {}", ctx.summary.total_executions));
    lines.push(format!(
        "statuses: {}",
        digest_status_counts(ctx.summary.statuses)
    ));
    lines
}

fn digest_job_lines(
    job: &DigestJobSummary,
    format: DigestFormat,
    include_schedule: bool,
) -> Vec<String> {
    let duration_line = match job.durations {
        Some(durations) => format!(
            "  duration: avg {}, max {}",
            format_duration_ms(durations.average_ms),
            format_duration_ms(durations.max_ms)
        ),
        None => "  duration: avg -, max -".to_string(),
    };

    let latest = &job.latest;
    let mut latest_line = format!(
        "  latest: status={}; started={}; duration={}",
        digest_display_field(&latest.status),
        digest_time(latest.start_ms, format),
        digest_duration(latest.duration_ms)
    );
    if include_schedule {
        if let Some(schedule) = &latest.schedule_label {
            if let Some(schedule) = digest_field(schedule) {
                latest_line.push_str(&format!("; schedule={schedule}"));
            }
        }
    }

    vec![
        format!("job: {}", digest_display_field(&job.job_id)),
        format!(
            "  executions: {}; statuses: {}",
            job.total_executions,
            digest_status_counts(job.statuses)
        ),
        duration_line,
        latest_line,
    ]
}

fn digest_execution_line(detail: &DigestExecutionDetail, format: DigestFormat) -> String {
    let short_id: String = detail.run_id.chars().take(8).collect();
    format!(
        "- {}/{}: status={}; started={}; duration={}",
        digest_display_field(&detail.job_id),
        digest_display_field(&short_id),
        digest_display_field(&detail.status),
        digest_time(detail.start_ms, format),
        digest_duration(detail.duration_ms)
    )
}

fn digest_omissions(ctx: &DigestMsgCtx<'_>, selection: &DigestSelection) -> (u64, u64) {
    let included_jobs = selection
        .job_schedules
        .iter()
        .filter(|schedule| schedule.is_some())
        .count() as u64;
    let included_executions = selection
        .problem_count
        .saturating_add(selection.success_count) as u64;
    let total_detail = ctx
        .summary
        .total_problem_executions
        .saturating_add(ctx.summary.total_success_executions);
    (
        ctx.summary.total_jobs.saturating_sub(included_jobs),
        total_detail.saturating_sub(included_executions),
    )
}

fn count_label(count: u64, singular: &str, plural: &str) -> String {
    if count == 1 {
        format!("1 {singular}")
    } else {
        format!("{count} {plural}")
    }
}

fn digest_footer_variants(omitted_jobs: u64, omitted_executions: u64) -> [String; 2] {
    [
        format!(
            "omitted: {}, {} (message cap)",
            count_label(omitted_jobs, "job", "jobs"),
            count_label(omitted_executions, "execution", "executions")
        ),
        format!("omitted: jobs={omitted_jobs}, executions={omitted_executions}"),
    ]
}

fn digest_selected_lines(
    model: &DigestModel<'_>,
    base_lines: &[String],
    selection: &DigestSelection,
    format: DigestFormat,
) -> Vec<String> {
    let mut lines = base_lines
        .iter()
        .zip(&selection.base_lines)
        .filter(|(_, selected)| **selected)
        .map(|(line, _)| line.clone())
        .collect::<Vec<_>>();

    for (job, schedule) in model.jobs.iter().zip(&selection.job_schedules) {
        if let Some(include_schedule) = schedule {
            lines.extend(digest_job_lines(job, format, *include_schedule));
        }
    }
    if selection.problem_count > 0 {
        lines.push("problem executions:".to_string());
        for detail in &model.problems[..selection.problem_count] {
            lines.push(digest_execution_line(detail, format));
        }
    }
    if selection.success_count > 0 {
        lines.push("recent successes:".to_string());
        for detail in &model.successes[..selection.success_count] {
            lines.push(digest_execution_line(detail, format));
        }
    }
    lines
}

fn digest_body_for_selection(
    ctx: &DigestMsgCtx<'_>,
    model: &DigestModel<'_>,
    base_lines: &[String],
    selection: &DigestSelection,
    format: DigestFormat,
    cap: usize,
) -> Option<String> {
    let mut lines = digest_selected_lines(model, base_lines, selection, format);
    let (omitted_jobs, omitted_executions) = digest_omissions(ctx, selection);
    if omitted_jobs == 0 && omitted_executions == 0 {
        let body = lines.join("\n");
        return (body.chars().count() <= cap).then_some(body);
    }

    for footer in digest_footer_variants(omitted_jobs, omitted_executions) {
        lines.push(footer);
        let body = lines.join("\n");
        if body.chars().count() <= cap {
            return Some(body);
        }
        lines.pop();
    }
    None
}

fn digest_body(ctx: &DigestMsgCtx<'_>, format: DigestFormat, cap: usize) -> String {
    let model = digest_model(ctx.summary);
    let base_lines = digest_base_lines(ctx);

    // This all-data candidate is attempted only when exact totals prove the
    // already bounded input contains the complete cohort. Large cohorts never
    // allocate a complete candidate.
    let input_is_complete = model.jobs.len() as u64 == ctx.summary.total_jobs
        && model.problems.len() as u64 == ctx.summary.total_problem_executions
        && model.successes.len() as u64 == ctx.summary.total_success_executions;
    if input_is_complete {
        let complete = DigestSelection {
            base_lines: vec![true; base_lines.len()],
            job_schedules: model.jobs.iter().map(|_| Some(true)).collect(),
            problem_count: model.problems.len(),
            success_count: model.successes.len(),
        };
        if let Some(body) =
            digest_body_for_selection(ctx, &model, &base_lines, &complete, format, cap)
        {
            return body;
        }
    }

    let mut selection = DigestSelection {
        base_lines: vec![false; base_lines.len()],
        job_schedules: model.jobs.iter().map(|_| None).collect(),
        problem_count: 0,
        success_count: 0,
    };
    // When even the compact exact-omission footer cannot fit, an empty body is
    // the only representation that both honors the cap and avoids cutting a
    // semantic line in half.
    if digest_body_for_selection(ctx, &model, &base_lines, &selection, format, cap).is_none() {
        return String::new();
    }

    // Global context and totals are more useful than individual execution
    // detail, so admit those lines first. Oversized user-provided fields are
    // skipped as whole lines rather than sliced.
    for index in 0..base_lines.len() {
        selection.base_lines[index] = true;
        if digest_body_for_selection(ctx, &model, &base_lines, &selection, format, cap).is_none() {
            selection.base_lines[index] = false;
        }
    }

    // A job summary is atomic. Problem jobs are already first in `model.jobs`,
    // followed by job id, so stopping at the first non-fitting summary keeps a
    // deterministic priority prefix.
    for index in 0..model.jobs.len() {
        selection.job_schedules[index] = Some(true);
        let fits_with_schedule =
            digest_body_for_selection(ctx, &model, &base_lines, &selection, format, cap).is_some();
        if fits_with_schedule {
            continue;
        }
        if model.jobs[index].latest.schedule_label.is_some() {
            selection.job_schedules[index] = Some(false);
            if digest_body_for_selection(ctx, &model, &base_lines, &selection, format, cap)
                .is_some()
            {
                continue;
            }
        }
        selection.job_schedules[index] = None;
        break;
    }

    // Execution detail is admitted in strict priority order: the bounded
    // problem prefix first, then the bounded globally recent success prefix.
    for index in 0..model.problems.len() {
        selection.problem_count = index + 1;
        if digest_body_for_selection(ctx, &model, &base_lines, &selection, format, cap).is_none() {
            selection.problem_count = index;
            break;
        }
    }

    let all_problem_details_available =
        model.problems.len() as u64 == ctx.summary.total_problem_executions;
    if all_problem_details_available && selection.problem_count == model.problems.len() {
        for index in 0..model.successes.len() {
            selection.success_count = index + 1;
            if digest_body_for_selection(ctx, &model, &base_lines, &selection, format, cap)
                .is_none()
            {
                selection.success_count = index;
                break;
            }
        }
    }

    digest_body_for_selection(ctx, &model, &base_lines, &selection, format, cap).unwrap_or_default()
}

pub fn discord_digest_payload(
    ctx: &DigestMsgCtx,
    max_message_chars: Option<usize>,
) -> serde_json::Value {
    let cap = max_message_chars
        .unwrap_or(DEFAULT_DISCORD_MAX_CHARS)
        .min(4096);
    let title = format!("DIGEST: {}", digest_display_field(ctx.period));
    let description = digest_body(ctx, DigestFormat::Discord, cap);

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
    let subject = format!(
        "[uatu] DIGEST ({}) on {}",
        digest_display_field(ctx.period),
        digest_display_field(ctx.recorded_host)
    );
    let body = digest_body(ctx, DigestFormat::Email, cap);
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

    fn counts(
        success: u64,
        failure: u64,
        timeout: u64,
        start_failed: u64,
        stale: u64,
    ) -> DigestStatusCounts {
        DigestStatusCounts {
            success,
            failure,
            timeout,
            start_failed,
            stale,
            active: 0,
        }
    }

    fn latest(
        status: &str,
        start_ms: i64,
        duration_ms: Option<u64>,
        schedule_label: Option<&str>,
    ) -> DigestLatestExecution {
        DigestLatestExecution {
            status: status.to_string(),
            start_ms,
            duration_ms,
            schedule_label: schedule_label.map(str::to_string),
        }
    }

    fn job(
        job_id: &str,
        total_executions: u64,
        statuses: DigestStatusCounts,
        durations: Option<(u64, u64)>,
        latest: DigestLatestExecution,
    ) -> DigestJobSummary {
        DigestJobSummary {
            job_id: job_id.to_string(),
            total_executions,
            statuses,
            durations: durations
                .map(|(average_ms, max_ms)| DigestDurationSummary { average_ms, max_ms }),
            latest,
        }
    }

    fn detail(
        job_id: &str,
        run_id: &str,
        status: &str,
        start_ms: i64,
        duration_ms: Option<u64>,
    ) -> DigestExecutionDetail {
        DigestExecutionDetail {
            job_id: job_id.to_string(),
            run_id: run_id.to_string(),
            status: status.to_string(),
            start_ms,
            duration_ms,
        }
    }

    fn digest_ctx(summary: &DigestSummary) -> DigestMsgCtx<'_> {
        DigestMsgCtx {
            period: "daily",
            window_start_ms: 1_700_000_000_000,
            window_end_ms: 1_700_086_400_000,
            recorded_host: "prod-worker-01",
            summary,
            delayed: None,
        }
    }

    fn discord_digest_description(ctx: &DigestMsgCtx<'_>, cap: usize) -> String {
        discord_digest_payload(ctx, Some(cap))["embeds"][0]["description"]
            .as_str()
            .unwrap()
            .to_string()
    }

    fn assert_exact_omission_footer(body: &str, jobs: u64, executions: u64) {
        let shown_jobs = body
            .lines()
            .filter(|line| line.starts_with("job: "))
            .count() as u64;
        let shown_executions = body.lines().filter(|line| line.starts_with("- ")).count() as u64;
        let omitted_jobs = jobs.saturating_sub(shown_jobs);
        let omitted_executions = executions.saturating_sub(shown_executions);
        let verbose = digest_footer_variants(omitted_jobs, omitted_executions)[0].clone();
        let compact = digest_footer_variants(omitted_jobs, omitted_executions)[1].clone();
        let footer = body.lines().last().unwrap_or_default();
        assert!(
            footer == verbose || footer == compact,
            "expected exact omission footer, got {footer:?} in:\n{body}"
        );
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

    #[test]
    fn aggregate_digest_groups_jobs_and_prioritizes_problems() {
        let base = 1_700_000_000_000;
        let summary = DigestSummary {
            total_jobs: 2,
            total_executions: 5,
            statuses: counts(3, 1, 0, 0, 1),
            total_problem_executions: 2,
            total_success_executions: 3,
            // Deliberately reverse the priority order; rendering must sort the
            // bounded database prefix deterministically.
            job_summaries: vec![
                job(
                    "alpha",
                    2,
                    counts(2, 0, 0, 0, 0),
                    Some((2_000, 3_000)),
                    latest("success", base + 4_000, Some(3_000), None),
                ),
                job(
                    "zeta",
                    3,
                    counts(1, 1, 0, 0, 1),
                    // The stale detection timestamp has already been excluded
                    // by the aggregate query.
                    Some((3_000, 4_000)),
                    latest("success", base + 3_000, Some(2_000), Some("daily import")),
                ),
            ],
            problem_details: vec![
                detail(
                    "zeta",
                    "FAIL0001-extra",
                    "failure",
                    base + 1_000,
                    Some(4_000),
                ),
                detail("zeta", "STALE001-extra", "stale", base + 2_000, None),
            ],
            success_details: vec![
                detail(
                    "alpha",
                    "ALPHA001-extra",
                    "success",
                    base + 500,
                    Some(1_000),
                ),
                detail(
                    "zeta",
                    "GOOD0001-extra",
                    "success",
                    base + 3_000,
                    Some(2_000),
                ),
                detail(
                    "alpha",
                    "ALPHA002-extra",
                    "success",
                    base + 4_000,
                    Some(3_000),
                ),
            ],
        };
        let ctx = digest_ctx(&summary);

        let discord = discord_digest_payload(&ctx, None);
        let embed = &discord["embeds"][0];
        assert_eq!(embed["title"], "DIGEST: daily");
        let description = embed["description"].as_str().unwrap();
        assert!(description.contains("observed jobs: 2"), "{description}");
        assert!(description.contains("executions: 5"), "{description}");
        assert!(
            description.contains("statuses: success=3, failure=1, stale=1"),
            "{description}"
        );

        let zeta = description.find("job: zeta").unwrap();
        let alpha = description.find("job: alpha").unwrap();
        assert!(zeta < alpha, "problem jobs must sort first:\n{description}");
        let zeta_block = &description[zeta..alpha];
        assert!(
            zeta_block.contains("executions: 3; statuses: success=1, failure=1, stale=1"),
            "{zeta_block}"
        );
        assert!(
            zeta_block.contains("duration: avg 3s, max 4s"),
            "detection timestamps must be excluded from duration stats:\n{zeta_block}"
        );
        assert!(
            zeta_block.contains(
                "latest: status=success; started=<t:1700000003:f>; duration=2s; schedule=daily import"
            ),
            "{zeta_block}"
        );

        let problems = description.find("problem executions:").unwrap();
        let successes = description.find("recent successes:").unwrap();
        assert!(problems > alpha && problems < successes, "{description}");
        assert!(
            description.find("zeta/STALE001").unwrap() < description.find("zeta/FAIL0001").unwrap(),
            "problem executions should be newest first:\n{description}"
        );
        assert!(description.find("alpha/ALPHA002").unwrap() > successes);

        let (subject, email) = digest_email_message(&ctx, None);
        assert_eq!(subject, "[uatu] DIGEST (daily) on prod-worker-01");
        assert!(!subject.contains("zeta") && !subject.contains("alpha"));
        assert!(email.contains("started=2023-11-14T22:13:23Z"), "{email}");
    }

    #[test]
    fn digest_caps_keep_semantic_lines_and_exact_omission_counts() {
        let base = 1_700_000_000_000;
        let mut job_summaries = Vec::new();
        let mut problem_details = Vec::new();
        let mut success_details = Vec::new();
        for job_id in ["a-problem", "b-problem", "c-success"] {
            for n in 0..5 {
                let status = if job_id.ends_with("problem") && n == 0 {
                    "timeout"
                } else {
                    "success"
                };
                let execution = detail(
                    job_id,
                    &format!("{job_id}-{n:08}"),
                    status,
                    base + n * 1_000,
                    Some((1_000 + n) as u64),
                );
                if status == "success" {
                    success_details.push(execution);
                } else {
                    problem_details.push(execution);
                }
            }
            let statuses = if job_id.ends_with("problem") {
                counts(4, 0, 1, 0, 0)
            } else {
                counts(5, 0, 0, 0, 0)
            };
            job_summaries.push(job(
                job_id,
                5,
                statuses,
                Some((1_002, 1_004)),
                latest("success", base + 4_000, Some(1_004), None),
            ));
        }
        let summary = DigestSummary {
            total_jobs: 3,
            total_executions: 15,
            statuses: counts(13, 0, 2, 0, 0),
            total_problem_executions: 2,
            total_success_executions: 13,
            job_summaries,
            problem_details,
            success_details,
        };
        let ctx = digest_ctx(&summary);
        let full = discord_digest_description(&ctx, 4096);
        assert!(!full.contains("omitted:"), "fixture must fit at 4096");

        let (discord_cap, discord) = (80..full.chars().count())
            .find_map(|cap| {
                let body = discord_digest_description(&ctx, cap);
                (body.contains("job: ") && body.contains("omitted:")).then_some((cap, body))
            })
            .expect("a partial semantic Discord rendering");
        assert!(discord.chars().count() <= discord_cap);
        assert!(
            !discord.contains('…'),
            "raw tail clipping returned:\n{discord}"
        );
        assert_exact_omission_footer(&discord, 3, 15);
        if discord.contains("recent successes:") {
            assert_eq!(
                discord
                    .lines()
                    .filter(|line| line.contains("status=timeout"))
                    .count(),
                2,
                "success detail must only appear after every problem:\n{discord}"
            );
        }

        let email_cap = 420;
        let (_, email) = digest_email_message(&ctx, Some(email_cap));
        assert!(email.chars().count() <= email_cap, "{email}");
        assert!(email.contains("omitted:"), "{email}");
        assert!(!email.contains('…'), "{email}");
        assert_exact_omission_footer(&email, 3, 15);

        // Exact metadata remains useful for cohorts far larger than the
        // bounded rows supplied to the renderer.
        let large = DigestSummary {
            total_jobs: 1_000_000,
            total_executions: 9_000_000_000,
            statuses: counts(8_999_999_900, 100, 0, 0, 0),
            total_problem_executions: 100,
            total_success_executions: 8_999_999_900,
            job_summaries: vec![job(
                "large",
                9_000_000_000,
                counts(8_999_999_900, 100, 0, 0, 0),
                Some((1_000, 2_000)),
                latest("success", base, Some(1_000), None),
            )],
            problem_details: vec![detail(
                "large",
                "problem-0001",
                "failure",
                base,
                Some(2_000),
            )],
            success_details: vec![detail(
                "large",
                "success-0001",
                "success",
                base,
                Some(1_000),
            )],
        };
        let large_ctx = digest_ctx(&large);
        let discord = discord_digest_description(&large_ctx, usize::MAX);
        assert!(discord.chars().count() <= 4096);
        assert!(discord.contains("observed jobs: 1000000"), "{discord}");
        assert!(discord.contains("executions: 9000000000"), "{discord}");
        assert!(
            discord.contains("statuses: success=8999999900, failure=100"),
            "{discord}"
        );
        assert_exact_omission_footer(&discord, 1_000_000, 9_000_000_000);
    }

    #[test]
    fn digest_defensively_bounds_every_preaggregated_prefix() {
        let base = 1_700_000_000_000;
        let job_count = DIGEST_MAX_JOB_SUMMARIES + 5;
        let problem_count = DIGEST_MAX_PROBLEM_DETAILS + 5;
        let success_count = DIGEST_MAX_SUCCESS_DETAILS + 5;
        let summary = DigestSummary {
            total_jobs: job_count as u64,
            total_executions: (problem_count + success_count) as u64,
            statuses: counts(success_count as u64, problem_count as u64, 0, 0, 0),
            total_problem_executions: problem_count as u64,
            total_success_executions: success_count as u64,
            job_summaries: (0..job_count)
                .map(|index| {
                    job(
                        &format!("job-{index:03}"),
                        1,
                        counts(1, 0, 0, 0, 0),
                        Some((1_000, 1_000)),
                        latest("success", base + index as i64, Some(1_000), None),
                    )
                })
                .collect(),
            problem_details: (0..problem_count)
                .map(|index| {
                    detail(
                        "job-000",
                        &format!("problem-{index:08}"),
                        "failure",
                        base + index as i64,
                        Some(1_000),
                    )
                })
                .collect(),
            success_details: (0..success_count)
                .map(|index| {
                    detail(
                        "job-001",
                        &format!("success-{index:08}"),
                        "success",
                        base + index as i64,
                        Some(1_000),
                    )
                })
                .collect(),
        };

        let model = digest_model(&summary);
        assert_eq!(model.jobs.len(), DIGEST_MAX_JOB_SUMMARIES);
        assert_eq!(model.problems.len(), DIGEST_MAX_PROBLEM_DETAILS);
        assert_eq!(model.successes.len(), DIGEST_MAX_SUCCESS_DETAILS);
        let body = discord_digest_description(&digest_ctx(&summary), usize::MAX);
        assert!(body.chars().count() <= 4096);
        assert!(
            !body.contains("recent successes:"),
            "success detail must wait until every exact problem detail is available:\n{body}"
        );
        assert_exact_omission_footer(
            &body,
            job_count as u64,
            (problem_count + success_count) as u64,
        );
    }

    #[test]
    fn digest_unicode_and_tiny_caps_never_split_text() {
        let job_id = "café-東京-🧪";
        let summary = DigestSummary {
            total_jobs: 1,
            total_executions: 1,
            statuses: counts(0, 1, 0, 0, 0),
            total_problem_executions: 1,
            total_success_executions: 0,
            job_summaries: vec![job(
                job_id,
                1,
                counts(0, 1, 0, 0, 0),
                Some((1_234, 1_234)),
                latest(
                    "failure",
                    1_700_000_000_000,
                    Some(1_234),
                    Some("nuit 🌙\nproduction"),
                ),
            )],
            problem_details: vec![detail(
                job_id,
                "🧪🧪🧪🧪🧪🧪🧪🧪-suffix",
                "failure",
                1_700_000_000_000,
                Some(1_234),
            )],
            success_details: vec![],
        };
        let ctx = DigestMsgCtx {
            recorded_host: "hébergeur-🛰️",
            ..digest_ctx(&summary)
        };

        let full = discord_digest_description(&ctx, 4096);
        assert!(full.contains("job: café-東京-🧪"), "{full}");
        assert!(full.contains("schedule=nuit 🌙 production"), "{full}");
        assert!(full.contains("🧪🧪🧪🧪🧪🧪🧪🧪"), "{full}");
        for cap in 0..80 {
            let body = discord_digest_description(&ctx, cap);
            assert!(body.chars().count() <= cap, "cap={cap}, body={body:?}");
            assert!(!body.contains('�'));
            assert!(!body.contains('…'));
        }
        assert!(discord_digest_description(&ctx, 0).is_empty());
        let (_, tiny_email) = digest_email_message(&ctx, Some(1));
        assert!(tiny_email.is_empty());
    }
}
