//! Delivery (SPEC §8): bounded synchronous sends over reqwest (Discord
//! webhooks) and lettre (SMTP), retry backoff with jitter, Retry-After
//! handling, 7-day expiry, and the shared immediate-row / digest-group
//! delivery driver used by `run` (own events + opportunistic flush) and
//! `flush`.

use std::path::Path;
use std::time::{Duration, Instant};

use lettre::transport::smtp::authentication::Credentials;
use lettre::{AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor};

use crate::config::{self, Config, DigestPeriod, SmtpCfg, SmtpTls};
use crate::db::{Db, DeliveryDigest, DeliveryRow, DigestAggregate, DigestClaim, StateError};
use crate::events::{self, Event, MsgCtx, ReporterRef};
use crate::liveness::Liveness;
use crate::oplog::OpLog;
use crate::redact::Redactor;
use crate::util::now_ms;

/// Per-reporter attempt budget (connect + send): 10 seconds.
pub fn per_reporter_budget() -> Duration {
    env_ms("UATU_PER_REPORTER_BUDGET_MS").unwrap_or(Duration::from_secs(10))
}

/// Overall post-child delivery budget: 30 seconds.
pub fn overall_budget() -> Duration {
    env_ms("UATU_OVERALL_BUDGET_MS").unwrap_or(Duration::from_secs(30))
}

fn env_ms(name: &str) -> Option<Duration> {
    // Undocumented test hook; defaults are the SPEC-mandated budgets.
    std::env::var(name)
        .ok()?
        .parse()
        .ok()
        .map(Duration::from_millis)
}

/// Queued deliveries older than this are marked `expired` (SPEC §8).
pub use crate::db::DELIVERY_RETRY_MAX_AGE_MS as RETRY_MAX_AGE_MS;

/// A delivery counts as "delayed" when retried or sent well after the event.
pub const DELAYED_THRESHOLD_MS: i64 = 90_000;

/// Keep aggregate SQL work bounded in the child-exit path. Explicit `flush`
/// has no cohort-size limit and is the recommended queue drain.
const OPPORTUNISTIC_DIGEST_MAX_MEMBERS: i64 = 2_048;

/// Queue this terminal run for a later aggregate digest. Rows remain per run
/// and reporter so inspection and retry state stay explainable; delivery
/// groups them by reporter, cadence, and UTC window across jobs. The digest is
/// independent of the job-effective alert events; reporter-level filters may
/// opt out by omitting `digest`.
pub fn queue_digest_for_run(
    db: &Db,
    cfg: &Config,
    run_id: &str,
    job_id: &str,
    event_ms: i64,
) -> Result<(), crate::db::StateError> {
    queue_digest_for_run_inner(db, cfg, run_id, job_id, event_ms, None)
}

/// Child-path variant of [`queue_digest_for_run`] that rolls the whole
/// all-reporter batch back if it cannot commit before `deadline`.
pub fn queue_digest_for_run_bounded(
    db: &Db,
    cfg: &Config,
    run_id: &str,
    job_id: &str,
    event_ms: i64,
    deadline: Instant,
) -> Result<(), crate::db::StateError> {
    queue_digest_for_run_inner(db, cfg, run_id, job_id, event_ms, Some(deadline))
}

fn queue_digest_for_run_inner(
    db: &Db,
    cfg: &Config,
    run_id: &str,
    job_id: &str,
    event_ms: i64,
    deadline: Option<Instant>,
) -> Result<(), crate::db::StateError> {
    let period = config::digest_period(cfg, job_id);
    if period == DigestPeriod::Off {
        return Ok(());
    }
    let Some((start_ms, end_ms)) = period.window_for(event_ms) else {
        return Ok(());
    };
    let digest = DeliveryDigest {
        period: period.as_str().to_string(),
        start_ms,
        end_ms,
    };
    let reporters = events::reporters_for_digest(cfg, job_id);
    match deadline {
        Some(deadline) => db.insert_digest_deliveries_bounded(
            run_id,
            job_id,
            Event::Digest.as_str(),
            &reporters,
            event_ms,
            Some(end_ms),
            &digest,
            deadline,
        )?,
        None => db.insert_digest_deliveries(
            run_id,
            job_id,
            Event::Digest.as_str(),
            &reporters,
            event_ms,
            Some(end_ms),
            &digest,
        )?,
    };
    Ok(())
}

/// Backoff schedule: 1m, 5m, 25m, 2h, then every 6h (SPEC §8).
/// `attempts_failed` is the total failures so far (≥1).
pub fn backoff_base(attempts_failed: i64) -> Duration {
    match attempts_failed {
        i64::MIN..=1 => Duration::from_secs(60),
        2 => Duration::from_secs(5 * 60),
        3 => Duration::from_secs(25 * 60),
        4 => Duration::from_secs(2 * 3600),
        _ => Duration::from_secs(6 * 3600),
    }
}

/// Largest Retry-After we will honor (1 day). Discord sends seconds or
/// fractional seconds; anything larger is a broken/hostile server and must
/// not be able to schedule a delivery past the 7-day expiry horizon — and
/// must never panic the wrapper (NaN/inf/negative all parse as f64).
const MAX_RETRY_AFTER_SECS: f64 = 86_400.0;

/// Total parse of a Retry-After header value: seconds (possibly fractional)
/// → bounded Duration. Anything non-finite, negative, or unparseable → None.
pub fn parse_retry_after(value: &str) -> Option<Duration> {
    let secs: f64 = value.trim().parse().ok()?;
    if !secs.is_finite() || secs < 0.0 {
        return None;
    }
    Duration::try_from_secs_f64(secs.min(MAX_RETRY_AFTER_SECS)).ok()
}

/// Apply ±20% jitter, then honor Retry-After when it exceeds the scheduled
/// backoff (SPEC §8, Discord rate limits).
pub fn next_attempt_delay(attempts_failed: i64, retry_after: Option<Duration>) -> Duration {
    use rand::RngExt;
    let base = backoff_base(attempts_failed);
    let jitter = rand::rng().random_range(0.8..=1.2);
    let scheduled = base.mul_f64(jitter);
    match retry_after {
        Some(ra) if ra > scheduled => ra,
        _ => scheduled,
    }
}

pub enum SendOutcome {
    Delivered,
    Failed {
        error: String,
        retry_after: Option<Duration>,
    },
}

pub struct Sender {
    rt: tokio::runtime::Runtime,
    client: reqwest::Client,
}

impl Sender {
    pub fn new() -> Result<Sender, String> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| format!("cannot build async runtime: {e}"))?;
        let client = reqwest::Client::builder()
            .user_agent(concat!("uatu/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|e| format!("cannot build HTTP client: {e}"))?;
        Ok(Sender { rt, client })
    }

    pub fn send_discord(
        &self,
        webhook_url: &str,
        payload: &serde_json::Value,
        budget: Duration,
    ) -> SendOutcome {
        let fut = async {
            match self
                .client
                .post(webhook_url)
                .json(payload)
                .timeout(budget)
                .send()
                .await
            {
                Ok(resp) => {
                    let status = resp.status();
                    if status.is_success() {
                        SendOutcome::Delivered
                    } else if status.as_u16() == 429 {
                        let retry_after = resp
                            .headers()
                            .get("retry-after")
                            .and_then(|v| v.to_str().ok())
                            .and_then(parse_retry_after);
                        SendOutcome::Failed {
                            error: "discord webhook rate limited (429)".to_string(),
                            retry_after,
                        }
                    } else {
                        SendOutcome::Failed {
                            error: format!("discord webhook returned HTTP {status}"),
                            retry_after: None,
                        }
                    }
                }
                Err(e) => SendOutcome::Failed {
                    error: format!("discord webhook request failed: {}", e.without_url()),
                    retry_after: None,
                },
            }
        };
        self.rt.block_on(async {
            match tokio::time::timeout(budget + Duration::from_secs(1), fut).await {
                Ok(outcome) => outcome,
                Err(_) => SendOutcome::Failed {
                    error: format!("discord send exceeded {}s budget", budget.as_secs_f64()),
                    retry_after: None,
                },
            }
        })
    }

    pub fn send_smtp(
        &self,
        cfg: &SmtpCfg,
        subject: &str,
        body: &str,
        budget: Duration,
    ) -> SendOutcome {
        let result = self.rt.block_on(async {
            tokio::time::timeout(budget, send_smtp_inner(cfg, subject, body)).await
        });
        match result {
            Ok(Ok(())) => SendOutcome::Delivered,
            Ok(Err(e)) => SendOutcome::Failed {
                error: e,
                retry_after: None,
            },
            Err(_) => SendOutcome::Failed {
                error: format!("smtp send exceeded {}s budget", budget.as_secs_f64()),
                retry_after: None,
            },
        }
    }
}

async fn send_smtp_inner(cfg: &SmtpCfg, subject: &str, body: &str) -> Result<(), String> {
    let tls = cfg.tls.unwrap_or(SmtpTls::Starttls);
    let mut builder = match tls {
        SmtpTls::Starttls => AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(&cfg.host)
            .map_err(|e| format!("smtp starttls setup: {e}"))?,
        SmtpTls::Smtps => AsyncSmtpTransport::<Tokio1Executor>::relay(&cfg.host)
            .map_err(|e| format!("smtp tls setup: {e}"))?,
        SmtpTls::None => AsyncSmtpTransport::<Tokio1Executor>::builder_dangerous(&cfg.host),
    };
    let default_port = match tls {
        SmtpTls::Starttls => 587,
        SmtpTls::Smtps => 465,
        SmtpTls::None => 25,
    };
    builder = builder.port(cfg.port.unwrap_or(default_port));
    if let (Some(user), Some(pass)) = (&cfg.username, &cfg.password) {
        builder = builder.credentials(Credentials::new(user.clone(), pass.clone()));
    }
    let transport = builder.build();

    let mut msg = Message::builder()
        .from(
            cfg.from
                .parse()
                .map_err(|e| format!("invalid from address {:?}: {e}", cfg.from))?,
        )
        .subject(subject);
    for r in &cfg.recipients {
        msg = msg.to(r
            .parse()
            .map_err(|e| format!("invalid recipient {r:?}: {e}"))?);
    }
    let email = msg
        .header(lettre::message::header::ContentType::TEXT_PLAIN)
        .body(body.to_string())
        .map_err(|e| format!("cannot build email: {e}"))?;
    transport
        .send(email)
        .await
        .map(|_| ())
        .map_err(|e| format!("smtp send failed: {e}"))
}

/// Read the redacted tail of a captured output file for snippets.
pub fn read_tail(path: Option<&str>, max_bytes: usize) -> String {
    let Some(path) = path else {
        return String::new();
    };
    let Ok(data) = std::fs::read(path) else {
        return String::new();
    };
    let start = data.len().saturating_sub(max_bytes);
    String::from_utf8_lossy(&data[start..]).into_owned()
}

/// Shared context for delivering rows.
pub struct DeliverCtx<'a> {
    pub db: &'a Db,
    pub cfg: &'a Config,
    pub oplog: &'a OpLog,
    pub sender: &'a Sender,
    pub host: String,
    pub redactor: &'a Redactor,
}

const STATE_BUSY_BUDGET: Duration = Duration::from_secs(5);

fn with_state_deadline<T>(
    db: &Db,
    deadline: Option<Instant>,
    operation: impl FnOnce() -> Result<T, StateError>,
) -> Result<T, StateError> {
    let Some(deadline) = deadline else {
        return operation();
    };
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(StateError::Busy(
            "post-child state deadline exceeded".to_string(),
        ));
    }
    db.conn.busy_timeout(remaining.min(STATE_BUSY_BUDGET))?;
    let result = operation();
    let _ = db.conn.busy_timeout(STATE_BUSY_BUDGET);
    result
}

/// Attempt one claimed delivery row (state `sending`, owned by us) and write
/// its final state. Never returns an error: failures queue or expire.
pub fn deliver_row(ctx: &DeliverCtx, row: &DeliveryRow, budget: Duration) {
    deliver_row_with_deadline(ctx, row, budget, None);
}

/// Run-path variant of [`deliver_row`] whose local state transitions cannot
/// wait for SQLite beyond the shared post-child deadline.
pub fn deliver_row_with_deadline(
    ctx: &DeliverCtx,
    row: &DeliveryRow,
    budget: Duration,
    deadline: Option<Instant>,
) {
    let now = now_ms();
    ctx.oplog.info(
        "delivery_attempted",
        &format!("attempting {} via {}", row.event, row.reporter),
        &[
            ("run_id", serde_json::json!(row.run_id)),
            ("job_id", serde_json::json!(row.job_id)),
            ("reporter", serde_json::json!(row.reporter)),
            ("attempt", serde_json::json!(row.attempt_count + 1)),
        ],
    );

    if now.saturating_sub(row.created_ms) > RETRY_MAX_AGE_MS {
        let _ = with_state_deadline(ctx.db, deadline, || {
            ctx.db
                .delivery_expired(row.id, "retry max age (7d) exceeded")
        });
        ctx.oplog.warn(
            "delivery_expired",
            &format!("{} via {} expired after 7d", row.event, row.reporter),
            &[("run_id", serde_json::json!(row.run_id))],
        );
        return;
    }

    let outcome = attempt_send(ctx, row, budget);
    match outcome {
        Ok(SendOutcome::Delivered) => {
            let _ = with_state_deadline(ctx.db, deadline, || {
                ctx.db.delivery_delivered(row.id, now_ms())
            });
            ctx.oplog.info(
                "delivery_succeeded",
                &format!("{} via {} delivered", row.event, row.reporter),
                &[("run_id", serde_json::json!(row.run_id))],
            );
        }
        Ok(SendOutcome::Failed { error, retry_after }) => {
            let attempts = row.attempt_count + 1;
            let delay = next_attempt_delay(attempts, retry_after);
            let next = now_ms().saturating_add(crate::util::duration_ms_i64(delay));
            let error = ctx.redactor.redact_str(&error);
            let _ = with_state_deadline(ctx.db, deadline, || {
                ctx.db.delivery_queued(row.id, next, &error)
            });
            ctx.oplog.warn(
                "delivery_failed",
                &format!("{} via {} failed: {error}", row.event, row.reporter),
                &[
                    ("run_id", serde_json::json!(row.run_id)),
                    (
                        "next_attempt_at",
                        serde_json::json!(crate::util::rfc3339(next)),
                    ),
                ],
            );
        }
        Err(permanent) => {
            let permanent = ctx.redactor.redact_str(&permanent);
            let _ = with_state_deadline(ctx.db, deadline, || {
                ctx.db.delivery_expired(row.id, &permanent)
            });
            ctx.oplog.warn(
                "delivery_expired",
                &format!("{} via {}: {permanent}", row.event, row.reporter),
                &[("run_id", serde_json::json!(row.run_id))],
            );
        }
    }
}

fn deliver_digest_claim(
    ctx: &DeliverCtx,
    claim: &DigestClaim,
    me: &Liveness,
    deadline: Option<std::time::Instant>,
) {
    let now = now_ms();
    ctx.oplog.info(
        "delivery_attempted",
        &format!("attempting {} digest via {}", claim.period, claim.reporter),
        &[
            ("reporter", serde_json::json!(claim.reporter)),
            ("attempt", serde_json::json!(claim.attempt_count + 1)),
        ],
    );

    if now.saturating_sub(claim.end_ms) > RETRY_MAX_AGE_MS {
        let _ = with_state_deadline(ctx.db, deadline, || {
            ctx.db
                .digest_group_expired(claim, me, "retry max age (7d) exceeded")
        });
        ctx.oplog.warn(
            "delivery_expired",
            &format!(
                "{} digest via {} expired after 7d",
                claim.event, claim.reporter
            ),
            &[("reporter", serde_json::json!(claim.reporter))],
        );
        return;
    }

    let outcome = attempt_send_digest(ctx, claim, me, deadline);
    match outcome {
        Ok(Some((SendOutcome::Delivered, jobs, executions))) => {
            let delivered = now_ms();
            let _ = with_state_deadline(ctx.db, deadline, || {
                ctx.db.digest_group_delivered(claim, me, delivered)
            });
            ctx.oplog.info(
                "delivery_succeeded",
                &format!("{} digest via {} delivered", claim.event, claim.reporter),
                &[
                    ("reporter", serde_json::json!(claim.reporter)),
                    ("digest_jobs", serde_json::json!(jobs)),
                    ("digest_count", serde_json::json!(executions)),
                ],
            );
        }
        Ok(Some((SendOutcome::Failed { error, retry_after }, jobs, executions))) => {
            let attempts = claim.attempt_count + 1;
            let delay = next_attempt_delay(attempts, retry_after);
            let next = now_ms().saturating_add(crate::util::duration_ms_i64(delay));
            let error = ctx.redactor.redact_str(&error);
            let _ = with_state_deadline(ctx.db, deadline, || {
                ctx.db.digest_group_queued(claim, me, next, &error)
            });
            ctx.oplog.warn(
                "delivery_failed",
                &format!(
                    "{} digest via {} failed: {error}",
                    claim.event, claim.reporter
                ),
                &[
                    ("reporter", serde_json::json!(claim.reporter)),
                    ("digest_jobs", serde_json::json!(jobs)),
                    ("digest_count", serde_json::json!(executions)),
                    (
                        "next_attempt_at",
                        serde_json::json!(crate::util::rfc3339(next)),
                    ),
                ],
            );
        }
        Ok(None) => {
            // Aggregate loading/rendering consumed the remaining post-child
            // budget. Reopen the cohort without counting a network attempt.
            let _ = with_state_deadline(ctx.db, deadline, || {
                ctx.db.digest_group_requeue(claim, me, now_ms())
            });
        }
        Err(permanent) => {
            let permanent = ctx.redactor.redact_str(&permanent);
            let _ = with_state_deadline(ctx.db, deadline, || {
                ctx.db.digest_group_expired(claim, me, &permanent)
            });
            ctx.oplog.warn(
                "delivery_expired",
                &format!("{} digest via {}: {permanent}", claim.event, claim.reporter),
                &[("reporter", serde_json::json!(claim.reporter))],
            );
        }
    }
}

/// Err(_) means permanently undeliverable (no such reporter/run/event).
fn attempt_send(
    ctx: &DeliverCtx,
    row: &DeliveryRow,
    budget: Duration,
) -> Result<SendOutcome, String> {
    let Some(event) = Event::parse(&row.event) else {
        return Err(format!("unknown event {:?}", row.event));
    };
    let Some(run) = ctx.db.get_run(&row.run_id).ok().flatten() else {
        return Err("run row no longer exists".to_string());
    };
    let Some(reporter) = events::lookup_reporter(ctx.cfg, &row.reporter) else {
        return Err(format!("reporter {:?} is not configured", row.reporter));
    };

    let now = now_ms();
    let delayed = if row.attempt_count > 0 || now - row.created_ms > DELAYED_THRESHOLD_MS {
        Some((row.created_ms, now))
    } else {
        None
    };

    // Failure notifications include capped redacted output tails (SPEC §8),
    // resolved via job-effective failure_output at delivery time.
    let failure_output = ctx
        .cfg
        .jobs
        .get(&row.job_id)
        .and_then(|j| j.failure_output)
        .or(ctx.cfg.notify.failure_output)
        .unwrap_or(true);
    let include_snippets = event == Event::Failure && failure_output;
    let snippets = if include_snippets && run.output_pruned_ms.is_none() {
        Some((
            read_tail(run.stdout.path.as_deref(), 8 * 1024),
            read_tail(run.stderr.path.as_deref(), 8 * 1024),
        ))
    } else {
        None
    };
    let mut output_files = Vec::new();
    if run.output_pruned_ms.is_none() {
        for p in [&run.stdout.path, &run.stderr.path].into_iter().flatten() {
            if Path::new(p).exists() {
                output_files.push(p.clone());
            }
        }
    }

    let mctx = MsgCtx {
        run: &run,
        event,
        host: &ctx.host,
        delayed,
        snippets,
        output_files,
    };

    Ok(match reporter {
        ReporterRef::Discord(d) => {
            let payload = events::discord_payload(&mctx, d.max_message_chars);
            ctx.sender.send_discord(&d.webhook_url, &payload, budget)
        }
        ReporterRef::Smtp(s) => {
            let (subject, body) = events::email_message(&mctx, s.max_message_chars);
            ctx.sender.send_smtp(s, &subject, &body, budget)
        }
    })
}

fn digest_status_counts(totals: crate::db::DigestStatusTotals) -> events::DigestStatusCounts {
    events::DigestStatusCounts {
        success: totals.success,
        failure: totals.failure,
        timeout: totals.timeout,
        start_failed: totals.start_failed,
        stale: totals.stale,
        active: totals.active,
    }
}

fn digest_summary(aggregate: DigestAggregate) -> events::DigestSummary {
    events::DigestSummary {
        total_jobs: aggregate.total_jobs,
        total_executions: aggregate.total_executions,
        statuses: digest_status_counts(aggregate.statuses),
        total_problem_executions: aggregate.total_problem_executions,
        total_success_executions: aggregate.total_success_executions,
        job_summaries: aggregate
            .job_summaries
            .into_iter()
            .map(|job| events::DigestJobSummary {
                job_id: job.job_id,
                total_executions: job.total_executions,
                statuses: digest_status_counts(job.statuses),
                durations: job.durations.map(|duration| events::DigestDurationSummary {
                    average_ms: duration.average_ms,
                    max_ms: duration.max_ms,
                }),
                latest: events::DigestLatestExecution {
                    status: job.latest.status,
                    start_ms: job.latest.start_ms,
                    duration_ms: job.latest.duration_ms,
                    schedule_label: job.latest.schedule_label,
                },
            })
            .collect(),
        problem_details: aggregate
            .problem_details
            .into_iter()
            .map(|detail| events::DigestExecutionDetail {
                job_id: detail.job_id,
                run_id: detail.run_id,
                status: detail.status,
                start_ms: detail.start_ms,
                duration_ms: detail.duration_ms,
            })
            .collect(),
        success_details: aggregate
            .success_details
            .into_iter()
            .map(|detail| events::DigestExecutionDetail {
                job_id: detail.job_id,
                run_id: detail.run_id,
                status: detail.status,
                start_ms: detail.start_ms,
                duration_ms: detail.duration_ms,
            })
            .collect(),
    }
}

fn digest_network_budget(deadline: Option<std::time::Instant>) -> Option<Duration> {
    let budget = deadline.map_or_else(per_reporter_budget, report_budget_remaining);
    (!budget.is_zero()).then_some(budget)
}

/// `Err` means permanently undeliverable. `Ok(None)` means aggregate
/// preparation exhausted the post-child deadline before network I/O began.
fn attempt_send_digest(
    ctx: &DeliverCtx,
    claim: &DigestClaim,
    me: &Liveness,
    deadline: Option<std::time::Instant>,
) -> Result<Option<(SendOutcome, u64, u64)>, String> {
    if claim.event != Event::Digest.as_str() {
        return Err(format!(
            "digest delivery does not support event {:?}",
            claim.event
        ));
    }
    let Some(reporter) = events::lookup_reporter(ctx.cfg, &claim.reporter) else {
        return Err(format!("reporter {:?} is not configured", claim.reporter));
    };

    let aggregate =
        match with_state_deadline(ctx.db, deadline, || ctx.db.load_digest_aggregate(claim, me)) {
            Ok(aggregate) => aggregate,
            Err(e) => {
                if digest_network_budget(deadline).is_none() {
                    return Ok(None);
                }
                return Ok(Some((
                    SendOutcome::Failed {
                        error: format!("cannot load digest aggregate: {e}"),
                        retry_after: None,
                    },
                    0,
                    0,
                )));
            }
        };
    if aggregate.total_executions == 0 {
        return Err("digest contains no existing terminal run rows".to_string());
    }
    let jobs = aggregate.total_jobs;
    let executions = aggregate.total_executions;
    let summary = digest_summary(aggregate);

    let now = now_ms();
    let delayed = if claim.attempt_count > 0 {
        Some((claim.end_ms, now))
    } else {
        None
    };
    let dctx = events::DigestMsgCtx {
        period: &claim.period,
        window_start_ms: claim.start_ms,
        window_end_ms: claim.end_ms,
        recorded_host: &claim.host,
        summary: &summary,
        delayed,
    };

    let outcome = match reporter {
        ReporterRef::Discord(d) => {
            let payload = events::discord_digest_payload(&dctx, d.max_message_chars);
            let Some(budget) = digest_network_budget(deadline) else {
                return Ok(None);
            };
            ctx.sender.send_discord(&d.webhook_url, &payload, budget)
        }
        ReporterRef::Smtp(s) => {
            let (subject, body) = events::digest_email_message(&dctx, s.max_message_chars);
            let Some(budget) = digest_network_budget(deadline) else {
                return Ok(None);
            };
            ctx.sender.send_smtp(s, &subject, &body, budget)
        }
    };
    Ok(Some((outcome, jobs, executions)))
}

/// Claim and deliver all due queued rows (used by `flush` and `run`'s
/// opportunistic flush). `deadline` bounds total time (None = unbounded).
pub fn deliver_due(ctx: &DeliverCtx, me: &Liveness, deadline: Option<std::time::Instant>) {
    const OPPORTUNISTIC_SCAN_LIMIT: usize = 256;

    if deadline.is_some_and(|d| std::time::Instant::now() >= d) {
        return;
    }
    // Immediate alerts retain priority over periodic summaries. A run-path
    // flush materializes only a bounded prefix before checking its deadline;
    // explicit `flush` remains exhaustive.
    let due = with_state_deadline(ctx.db, deadline, || match deadline {
        Some(_) => ctx
            .db
            .due_deliveries_limited(now_ms(), OPPORTUNISTIC_SCAN_LIMIT),
        None => ctx.db.due_deliveries(now_ms()),
    });
    let due = match due {
        Ok(d) => d,
        Err(_) => return,
    };
    for row in due {
        if let Some(d) = deadline {
            if std::time::Instant::now() >= d {
                return; // remaining rows stay queued and due
            }
        }
        match with_state_deadline(ctx.db, deadline, || ctx.db.claim_delivery(row.id, me)) {
            Ok(true) => {}
            _ => continue, // someone else took it
        }
        let budget = match deadline {
            Some(d) => report_budget_remaining(d),
            None => per_reporter_budget(),
        };
        if budget.is_zero() {
            let _ = with_state_deadline(ctx.db, deadline, || {
                ctx.db.delivery_requeue(row.id, now_ms())
            });
            return;
        }
        // Re-read the row to carry the claimed state forward.
        if let Ok(Some(claimed)) =
            with_state_deadline(ctx.db, deadline, || ctx.db.get_delivery(row.id))
        {
            deliver_row_with_deadline(ctx, &claimed, budget, deadline);
        }
    }

    if deadline.is_some_and(|d| std::time::Instant::now() >= d) {
        return;
    }
    let cohorts = with_state_deadline(ctx.db, deadline, || match deadline {
        Some(_) => ctx.db.due_digest_cohorts_limited(
            now_ms(),
            OPPORTUNISTIC_SCAN_LIMIT,
            OPPORTUNISTIC_DIGEST_MAX_MEMBERS,
        ),
        None => ctx.db.due_digest_cohorts(now_ms()),
    });
    let cohorts = match cohorts {
        Ok(cohorts) => cohorts,
        Err(_) => return,
    };
    for cohort in cohorts {
        if deadline.is_some_and(|d| std::time::Instant::now() >= d) {
            return;
        }
        if deadline.is_some() && cohort.member_count > OPPORTUNISTIC_DIGEST_MAX_MEMBERS {
            continue;
        }
        let claim = match with_state_deadline(ctx.db, deadline, || {
            ctx.db.claim_digest_cohort(cohort.id, me, now_ms())
        }) {
            Ok(Some(claim)) => claim,
            _ => continue,
        };
        // Close the insertion race between listing and claiming without
        // running aggregate queries for a cohort that crossed the bound.
        if deadline.is_some() && claim.member_count > OPPORTUNISTIC_DIGEST_MAX_MEMBERS {
            let _ = with_state_deadline(ctx.db, deadline, || {
                ctx.db.digest_group_requeue(&claim, me, now_ms())
            });
            continue;
        }
        deliver_digest_claim(ctx, &claim, me, deadline);
    }
}

fn report_budget_remaining(deadline: std::time::Instant) -> Duration {
    per_reporter_budget().min(deadline.saturating_duration_since(std::time::Instant::now()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_schedule_pinned() {
        assert_eq!(backoff_base(1), Duration::from_secs(60));
        assert_eq!(backoff_base(2), Duration::from_secs(300));
        assert_eq!(backoff_base(3), Duration::from_secs(1500));
        assert_eq!(backoff_base(4), Duration::from_secs(7200));
        assert_eq!(backoff_base(5), Duration::from_secs(21600));
        assert_eq!(backoff_base(50), Duration::from_secs(21600));
    }

    #[test]
    fn jitter_within_20_percent() {
        for attempts in 1..=6 {
            let base = backoff_base(attempts);
            for _ in 0..200 {
                let d = next_attempt_delay(attempts, None);
                assert!(
                    d >= base.mul_f64(0.8) && d <= base.mul_f64(1.2),
                    "{d:?} vs {base:?}"
                );
            }
        }
    }

    #[test]
    fn retry_after_honored_when_larger() {
        // Retry-After larger than scheduled backoff wins.
        let big = Duration::from_secs(3600);
        assert_eq!(next_attempt_delay(1, Some(big)), big);
        // Smaller Retry-After: the scheduled backoff stands.
        let small = Duration::from_secs(1);
        let d = next_attempt_delay(4, Some(small));
        assert!(d >= Duration::from_secs(7200).mul_f64(0.8));
    }

    #[test]
    fn parse_retry_after_cases() {
        // Normal integer
        assert_eq!(parse_retry_after("3600"), Some(Duration::from_secs(3600)));
        // Fractional seconds
        assert_eq!(parse_retry_after("1.5"), Some(Duration::from_millis(1500)));
        // Pathological: non-finite
        assert_eq!(parse_retry_after("nan"), None);
        assert_eq!(parse_retry_after("inf"), None);
        // Negative
        assert_eq!(parse_retry_after("-5"), None);
        // Unparseable
        assert_eq!(parse_retry_after(""), None);
        assert_eq!(parse_retry_after("soon"), None);
        // Huge value: clamped to MAX_RETRY_AFTER_SECS (86400s)
        assert_eq!(
            parse_retry_after("1e300"),
            Some(Duration::from_secs(86_400))
        );
    }
}
