//! Delivery (SPEC §8): bounded synchronous sends over reqwest (Discord
//! webhooks) and lettre (SMTP), retry backoff with jitter, Retry-After
//! handling, 7-day expiry, and the shared per-row delivery driver used by
//! `run` (own events + opportunistic flush) and `flush`.

use std::path::Path;
use std::time::Duration;

use lettre::transport::smtp::authentication::Credentials;
use lettre::{AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor};

use crate::config::{Config, SmtpCfg, SmtpTls};
use crate::db::{Db, DeliveryRow};
use crate::events::{self, Event, MsgCtx, ReporterRef};
use crate::liveness::Liveness;
use crate::oplog::OpLog;
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
pub const RETRY_MAX_AGE_MS: i64 = 7 * 86_400_000;

/// A delivery counts as "delayed" when retried or sent well after the event.
pub const DELAYED_THRESHOLD_MS: i64 = 90_000;

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

/// Apply ±20% jitter, then honor Retry-After when it exceeds the scheduled
/// backoff (SPEC §8, Discord rate limits).
pub fn next_attempt_delay(attempts_failed: i64, retry_after: Option<Duration>) -> Duration {
    use rand::Rng;
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
                            .and_then(|s| s.trim().parse::<f64>().ok())
                            .map(Duration::from_secs_f64);
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
                    error: format!("discord webhook request failed: {e}"),
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
}

/// Attempt one claimed delivery row (state `sending`, owned by us) and write
/// its final state. Never returns an error: failures queue or expire.
pub fn deliver_row(ctx: &DeliverCtx, row: &DeliveryRow, budget: Duration) {
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

    if now - row.created_ms > RETRY_MAX_AGE_MS {
        let _ = ctx
            .db
            .delivery_expired(row.id, "retry max age (7d) exceeded");
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
            let _ = ctx.db.delivery_delivered(row.id, now_ms());
            ctx.oplog.info(
                "delivery_succeeded",
                &format!("{} via {} delivered", row.event, row.reporter),
                &[("run_id", serde_json::json!(row.run_id))],
            );
        }
        Ok(SendOutcome::Failed { error, retry_after }) => {
            let attempts = row.attempt_count + 1;
            let delay = next_attempt_delay(attempts, retry_after);
            let next = now_ms() + delay.as_millis() as i64;
            let _ = ctx.db.delivery_queued(row.id, next, &error);
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
            let _ = ctx.db.delivery_expired(row.id, &permanent);
            ctx.oplog.warn(
                "delivery_expired",
                &format!("{} via {}: {permanent}", row.event, row.reporter),
                &[("run_id", serde_json::json!(row.run_id))],
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

/// Claim and deliver all due queued rows (used by `flush` and `run`'s
/// opportunistic flush). `deadline` bounds total time (None = unbounded).
pub fn deliver_due(ctx: &DeliverCtx, me: &Liveness, deadline: Option<std::time::Instant>) {
    let due = match ctx.db.due_deliveries(now_ms()) {
        Ok(d) => d,
        Err(_) => return,
    };
    for row in due {
        if let Some(d) = deadline {
            if std::time::Instant::now() >= d {
                return; // remaining rows stay queued and due
            }
        }
        match ctx.db.claim_delivery(row.id, me) {
            Ok(true) => {}
            _ => continue, // someone else took it
        }
        let budget = match deadline {
            Some(d) => {
                per_reporter_budget().min(d.saturating_duration_since(std::time::Instant::now()))
            }
            None => per_reporter_budget(),
        };
        if budget.is_zero() {
            let _ = ctx.db.delivery_requeue(row.id, now_ms());
            return;
        }
        // Re-read the row to carry the claimed state forward.
        if let Ok(Some(claimed)) = ctx.db.get_delivery(row.id) {
            deliver_row(ctx, &claimed, budget);
        }
    }
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
}
