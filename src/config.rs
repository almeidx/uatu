//! Configuration (SPEC §4): TOML parsing, path resolution, unknown-key
//! handling (lenient at runtime, strict in `config validate`), and effective
//! per-run setting resolution (CLI > job > global > built-in default).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;

use crate::util::{expand_tilde, parse_bytes, parse_duration};

// Built-in defaults (SPEC §4, §6, §7, §8).
pub const DEFAULT_CAPTURE_HEAD: u64 = 64 * 1024;
pub const DEFAULT_CAPTURE_TAIL: u64 = 1 << 20;
pub const DEFAULT_MIN_FREE: u64 = 100_000_000;
pub const DEFAULT_KILL_GRACE: Duration = Duration::from_secs(30);
pub const DEFAULT_RETENTION_AGE: Duration = Duration::from_secs(30 * 86_400);
pub const DEFAULT_RETENTION_BYTES: u64 = 1_000_000_000;
pub const DEFAULT_LOG_MAX: u64 = 50_000_000;
pub const DEFAULT_DISCORD_MAX_CHARS: usize = 3_500;
pub const DEFAULT_SMTP_MAX_CHARS: usize = 20_000;
pub const DEFAULT_EVENTS: [&str; 2] = ["success", "failure"];

/// Duration config value: string like "30s".
#[derive(Clone, Copy, Debug)]
pub struct Dur(pub Duration);

impl<'de> Deserialize<'de> for Dur {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        parse_duration(&s)
            .map(Dur)
            .map_err(serde::de::Error::custom)
    }
}

/// Byte-size config value: string like "64KiB" / "50MB".
#[derive(Clone, Copy, Debug)]
pub struct ByteSize(pub u64);

impl<'de> Deserialize<'de> for ByteSize {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        parse_bytes(&s)
            .map(ByteSize)
            .map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CaptureMode {
    Capped,
    Full,
    Off,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DigestPeriod {
    Off,
    Hourly,
    Daily,
    Weekly,
    Monthly,
}

impl DigestPeriod {
    pub fn as_str(self) -> &'static str {
        match self {
            DigestPeriod::Off => "off",
            DigestPeriod::Hourly => "hourly",
            DigestPeriod::Daily => "daily",
            DigestPeriod::Weekly => "weekly",
            DigestPeriod::Monthly => "monthly",
        }
    }

    pub fn window_for(self, ms: i64) -> Option<(i64, i64)> {
        const HOUR_MS: i64 = 3_600_000;
        const DAY_MS: i64 = 86_400_000;
        match self {
            DigestPeriod::Off => None,
            DigestPeriod::Hourly => Some(fixed_window(ms, HOUR_MS)),
            DigestPeriod::Daily => Some(fixed_window(ms, DAY_MS)),
            DigestPeriod::Weekly => {
                let days = ms.div_euclid(DAY_MS);
                // 1970-01-01 was Thursday; ISO-style UTC weeks start Monday.
                let start_days = (days + 3).div_euclid(7) * 7 - 3;
                Some((
                    start_days.saturating_mul(DAY_MS),
                    (start_days + 7).saturating_mul(DAY_MS),
                ))
            }
            DigestPeriod::Monthly => {
                let days = ms.div_euclid(DAY_MS);
                let (year, month, _) = civil_from_days(days);
                let start_days = days_from_civil(year, month, 1);
                let (next_year, next_month) = if month == 12 {
                    (year + 1, 1)
                } else {
                    (year, month + 1)
                };
                let end_days = days_from_civil(next_year, next_month, 1);
                Some((
                    start_days.saturating_mul(DAY_MS),
                    end_days.saturating_mul(DAY_MS),
                ))
            }
        }
    }
}

impl<'de> Deserialize<'de> for DigestPeriod {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        match s.as_str() {
            "off" | "none" | "disabled" => Ok(DigestPeriod::Off),
            "hourly" => Ok(DigestPeriod::Hourly),
            "daily" => Ok(DigestPeriod::Daily),
            "weekly" => Ok(DigestPeriod::Weekly),
            "monthly" => Ok(DigestPeriod::Monthly),
            _ => Err(serde::de::Error::custom(
                "invalid digest (valid: off, hourly, daily, weekly, monthly)",
            )),
        }
    }
}

fn fixed_window(ms: i64, width_ms: i64) -> (i64, i64) {
    let start = ms.div_euclid(width_ms).saturating_mul(width_ms);
    (start, start.saturating_add(width_ms))
}

fn civil_from_days(z: i64) -> (i32, u32, u32) {
    // Howard Hinnant's civil calendar algorithms, with z = days since Unix epoch.
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 }.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    let year = y + i64::from(month <= 2);
    (year as i32, month as u32, day as u32)
}

fn days_from_civil(year: i32, month: u32, day: u32) -> i64 {
    let mut y = i64::from(year);
    let m = i64::from(month);
    let d = i64::from(day);
    y -= i64::from(m <= 2);
    let era = if y >= 0 { y } else { y - 399 }.div_euclid(400);
    let yoe = y - era * 400;
    let mp = m + if m > 2 { -3 } else { 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SmtpTls {
    Starttls,
    Smtps,
    None,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct GlobalCfg {
    pub data_dir: Option<String>,
    pub host_name: Option<String>,
    pub capture_stdout: Option<bool>,
    pub capture_stderr: Option<bool>,
    pub capture_mode: Option<CaptureMode>,
    pub capture_head_bytes: Option<ByteSize>,
    pub capture_tail_bytes: Option<ByteSize>,
    pub min_free_bytes: Option<ByteSize>,
    pub kill_grace: Option<Dur>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct RetentionCfg {
    pub max_age: Option<Dur>,
    pub max_bytes: Option<ByteSize>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct LogCfg {
    pub path: Option<String>,
    pub max_bytes: Option<ByteSize>,
    pub trim: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct RedactionCfg {
    #[serde(default)]
    pub literals: Vec<String>,
    #[serde(default)]
    pub regex: Vec<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct NotifyCfg {
    pub events: Option<Vec<String>>,
    pub reporters: Option<Vec<String>>,
    pub failure_output: Option<bool>,
    pub digest: Option<DigestPeriod>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct DiscordCfg {
    pub webhook_url: String,
    pub max_message_chars: Option<usize>,
    pub events: Option<Vec<String>>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct SmtpCfg {
    pub host: String,
    pub port: Option<u16>,
    pub tls: Option<SmtpTls>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub from: String,
    #[serde(default)]
    pub recipients: Vec<String>,
    pub max_message_chars: Option<usize>,
    pub events: Option<Vec<String>>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct JobCfg {
    pub cwd: Option<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    pub timeout: Option<Dur>,
    pub expected_duration: Option<Dur>,
    pub schedule_label: Option<String>,
    pub reporters: Option<Vec<String>>,
    pub events: Option<Vec<String>>,
    pub capture_stdout: Option<bool>,
    pub capture_stderr: Option<bool>,
    pub capture_mode: Option<CaptureMode>,
    pub capture_head_bytes: Option<ByteSize>,
    pub capture_tail_bytes: Option<ByteSize>,
    pub kill_grace: Option<Dur>,
    pub failure_output: Option<bool>,
    pub digest: Option<DigestPeriod>,
}

#[derive(Clone, Debug, Default)]
pub struct Config {
    pub global: GlobalCfg,
    pub retention: RetentionCfg,
    pub log: LogCfg,
    pub redaction: RedactionCfg,
    pub notify: NotifyCfg,
    pub discord: BTreeMap<String, DiscordCfg>,
    pub smtp: BTreeMap<String, SmtpCfg>,
    pub jobs: BTreeMap<String, JobCfg>,
}

impl Config {
    /// Auto-derived literal redaction secrets (SPEC §4): Discord webhook URLs
    /// and SMTP passwords.
    pub fn auto_secrets(&self) -> Vec<String> {
        let mut v = Vec::new();
        for d in self.discord.values() {
            if !d.webhook_url.is_empty() {
                v.push(d.webhook_url.clone());
            }
        }
        for s in self.smtp.values() {
            if let Some(p) = &s.password {
                if !p.is_empty() {
                    v.push(p.clone());
                }
            }
        }
        v
    }

    pub fn host_name(&self) -> String {
        self.global
            .host_name
            .clone()
            .unwrap_or_else(crate::util::hostname)
    }

    pub fn retention_max_age(&self) -> Duration {
        self.retention
            .max_age
            .map(|d| d.0)
            .unwrap_or(DEFAULT_RETENTION_AGE)
    }

    pub fn retention_max_bytes(&self) -> u64 {
        self.retention
            .max_bytes
            .map(|b| b.0)
            .unwrap_or(DEFAULT_RETENTION_BYTES)
    }
}

/// Outcome of the lenient runtime load (SPEC §10): never an error.
pub struct LoadOutcome {
    pub path: Option<PathBuf>,
    pub config: Config,
    /// Non-fatal problems: unknown keys, per-table parse fallbacks, etc.
    pub warnings: Vec<String>,
    /// Whole-file TOML syntax failure → run local-only with defaults.
    pub invalid: Option<String>,
    /// The [redaction] table failed to parse → metadata-only mode (SPEC §9).
    pub redaction_invalid: Option<String>,
}

pub fn resolve_config_path(cli: Option<&Path>) -> Option<PathBuf> {
    if let Some(p) = cli {
        return Some(p.to_path_buf());
    }
    let xdg = std::env::var_os("XDG_CONFIG_HOME")
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| Path::new(&h).join(".config")));
    if let Some(base) = xdg {
        let p = base.join("uatu").join("uatu.toml");
        if p.is_file() {
            return Some(p);
        }
    }
    let etc = Path::new("/etc/uatu/uatu.toml");
    if etc.is_file() {
        return Some(etc.to_path_buf());
    }
    None
}

/// Default location `uatu init` writes to (first two steps of resolution).
pub fn default_config_target() -> PathBuf {
    let base = std::env::var_os("XDG_CONFIG_HOME")
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| Path::new(&h).join(".config")))
        .unwrap_or_else(|| PathBuf::from("."));
    base.join("uatu").join("uatu.toml")
}

/// Lenient runtime load: missing file → configless; invalid TOML → local-only;
/// per-table type errors → that table falls back to defaults with a warning;
/// unknown keys → warnings (SPEC §4, §10).
pub fn load_runtime(cli: Option<&Path>) -> LoadOutcome {
    let mut out = LoadOutcome {
        path: None,
        config: Config::default(),
        warnings: Vec::new(),
        invalid: None,
        redaction_invalid: None,
    };
    let Some(path) = resolve_config_path(cli) else {
        return out;
    };
    out.path = Some(path.clone());
    let text = match std::fs::read_to_string(&path) {
        Ok(t) => t,
        Err(e) => {
            out.invalid = Some(format!("cannot read config {}: {e}", path.display()));
            return out;
        }
    };
    let doc: toml::Value = match toml::from_str(&text) {
        Ok(v) => v,
        Err(e) => {
            out.invalid = Some(format!("invalid TOML in {}: {e}", path.display()));
            return out;
        }
    };
    out.warnings.extend(unknown_keys(&doc));
    out.config = parse_tables(&doc, &mut out.warnings, &mut out.redaction_invalid);
    out
}

fn get_table<'v>(doc: &'v toml::Value, key: &str) -> Option<&'v toml::Value> {
    doc.as_table().and_then(|t| t.get(key))
}

fn parse_table<T: serde::de::DeserializeOwned + Default>(
    doc: &toml::Value,
    key: &str,
    warnings: &mut Vec<String>,
) -> T {
    match get_table(doc, key) {
        None => T::default(),
        Some(v) => match v.clone().try_into() {
            Ok(t) => t,
            Err(e) => {
                warnings.push(format!(
                    "config table [{key}] is invalid and was ignored: {e}"
                ));
                T::default()
            }
        },
    }
}

fn parse_named_entries<T: serde::de::DeserializeOwned>(
    section: Option<&toml::Value>,
    label: &str,
    warnings: &mut Vec<String>,
) -> BTreeMap<String, T> {
    let mut out = BTreeMap::new();
    let Some(table) = section.and_then(|v| v.as_table()) else {
        if section.is_some() {
            warnings.push(format!(
                "config table [{label}] is not a table and was ignored"
            ));
        }
        return out;
    };
    for (name, val) in table {
        match val.clone().try_into() {
            Ok(t) => {
                out.insert(name.clone(), t);
            }
            Err(e) => warnings.push(format!(
                "config entry [{label}.{name}] is invalid and was ignored: {e}"
            )),
        }
    }
    out
}

fn parse_tables(
    doc: &toml::Value,
    warnings: &mut Vec<String>,
    redaction_invalid: &mut Option<String>,
) -> Config {
    let mut cfg = Config {
        global: parse_table(doc, "global", warnings),
        retention: parse_table(doc, "retention", warnings),
        log: parse_table(doc, "log", warnings),
        redaction: RedactionCfg::default(),
        notify: parse_table(doc, "notify", warnings),
        discord: BTreeMap::new(),
        smtp: BTreeMap::new(),
        jobs: BTreeMap::new(),
    };
    // [redaction] is security-relevant: a type error here must not silently
    // fall back to "no redaction" — it forces metadata-only mode instead.
    if let Some(v) = get_table(doc, "redaction") {
        match v.clone().try_into::<RedactionCfg>() {
            Ok(r) => cfg.redaction = r,
            Err(e) => {
                *redaction_invalid = Some(format!("config table [redaction] is invalid: {e}"))
            }
        }
    }
    let reporters = get_table(doc, "reporters");
    cfg.discord = parse_named_entries(
        reporters
            .and_then(|r| r.as_table())
            .and_then(|t| t.get("discord")),
        "reporters.discord",
        warnings,
    );
    cfg.smtp = parse_named_entries(
        reporters
            .and_then(|r| r.as_table())
            .and_then(|t| t.get("smtp")),
        "reporters.smtp",
        warnings,
    );
    cfg.jobs = parse_named_entries(get_table(doc, "jobs"), "jobs", warnings);
    if let Some(t) = cfg.log.trim.as_deref() {
        if t != "head" {
            warnings.push(format!(
                "config: log.trim={t:?} is not supported (only \"head\"); using \"head\""
            ));
        }
    }
    cfg
}

/// Parse a pre-parsed TOML document into a Config (exposed for tests and
/// `config validate`, which need table parsing without file I/O).
pub fn test_parse_tables(
    doc: &toml::Value,
    warnings: &mut Vec<String>,
    redaction_invalid: &mut Option<String>,
) -> Config {
    parse_tables(doc, warnings, redaction_invalid)
}

const ROOT_KEYS: &[&str] = &[
    "global",
    "retention",
    "log",
    "redaction",
    "notify",
    "reporters",
    "jobs",
];
const GLOBAL_KEYS: &[&str] = &[
    "data_dir",
    "host_name",
    "capture_stdout",
    "capture_stderr",
    "capture_mode",
    "capture_head_bytes",
    "capture_tail_bytes",
    "min_free_bytes",
    "kill_grace",
];
const RETENTION_KEYS: &[&str] = &["max_age", "max_bytes"];
const LOG_KEYS: &[&str] = &["path", "max_bytes", "trim"];
const REDACTION_KEYS: &[&str] = &["literals", "regex"];
const NOTIFY_KEYS: &[&str] = &["events", "reporters", "failure_output", "digest"];
const DISCORD_KEYS: &[&str] = &["webhook_url", "max_message_chars", "events"];
const SMTP_KEYS: &[&str] = &[
    "host",
    "port",
    "tls",
    "username",
    "password",
    "from",
    "recipients",
    "max_message_chars",
    "events",
];
const JOB_KEYS: &[&str] = &[
    "cwd",
    "env",
    "timeout",
    "expected_duration",
    "schedule_label",
    "reporters",
    "events",
    "capture_stdout",
    "capture_stderr",
    "capture_mode",
    "capture_head_bytes",
    "capture_tail_bytes",
    "kill_grace",
    "failure_output",
    "digest",
];

/// Walk the parsed document against the known-key schema, returning one
/// message per unknown key, naming the key and the table it appeared in.
pub fn unknown_keys(doc: &toml::Value) -> Vec<String> {
    let mut found = Vec::new();
    let Some(root) = doc.as_table() else {
        return found;
    };
    let check =
        |table: Option<&toml::Value>, allowed: &[&str], where_: &str, found: &mut Vec<String>| {
            if let Some(t) = table.and_then(|v| v.as_table()) {
                for k in t.keys() {
                    if !allowed.contains(&k.as_str()) {
                        found.push(format!("unknown key `{k}` in [{where_}]"));
                    }
                }
            }
        };
    for k in root.keys() {
        if !ROOT_KEYS.contains(&k.as_str()) {
            found.push(format!("unknown top-level table or key `{k}`"));
        }
    }
    check(root.get("global"), GLOBAL_KEYS, "global", &mut found);
    check(
        root.get("retention"),
        RETENTION_KEYS,
        "retention",
        &mut found,
    );
    check(root.get("log"), LOG_KEYS, "log", &mut found);
    check(
        root.get("redaction"),
        REDACTION_KEYS,
        "redaction",
        &mut found,
    );
    check(root.get("notify"), NOTIFY_KEYS, "notify", &mut found);
    if let Some(reporters) = root.get("reporters").and_then(|v| v.as_table()) {
        for (kind, entries) in reporters {
            match kind.as_str() {
                "discord" | "smtp" => {
                    if let Some(map) = entries.as_table() {
                        for (name, body) in map {
                            let allowed = if kind == "discord" {
                                DISCORD_KEYS
                            } else {
                                SMTP_KEYS
                            };
                            check(
                                Some(body),
                                allowed,
                                &format!("reporters.{kind}.{name}"),
                                &mut found,
                            );
                        }
                    }
                }
                other => found.push(format!(
                    "unknown reporter kind `{other}` in [reporters] (expected discord or smtp)"
                )),
            }
        }
    }
    if let Some(jobs) = root.get("jobs").and_then(|v| v.as_table()) {
        for (name, body) in jobs {
            check(Some(body), JOB_KEYS, &format!("jobs.{name}"), &mut found);
        }
    }
    found
}

/// State directory resolution (SPEC §4): --data-dir > global.data_dir >
/// $XDG_STATE_HOME/uatu > ~/.local/state/uatu.
pub fn resolve_state_dir(cli: Option<&Path>, cfg: &Config) -> PathBuf {
    if let Some(p) = cli {
        return p.to_path_buf();
    }
    if let Some(d) = &cfg.global.data_dir {
        return expand_tilde(d);
    }
    if let Some(x) = std::env::var_os("XDG_STATE_HOME").filter(|v| !v.is_empty()) {
        return Path::new(&x).join("uatu");
    }
    match std::env::var_os("HOME") {
        Some(h) => Path::new(&h).join(".local").join("state").join("uatu"),
        None => PathBuf::from("./.uatu-state"),
    }
}

/// CLI-provided overrides relevant to setting precedence (SPEC §3).
#[derive(Clone, Debug, Default)]
pub struct CliOverrides {
    pub cwd: Option<PathBuf>,
    pub env: Vec<(String, String)>,
    pub timeout: Option<Duration>,
    pub kill_grace: Option<Duration>,
    pub expected_duration: Option<Duration>,
}

/// Effective settings for one run after precedence resolution.
#[derive(Clone, Debug)]
pub struct Effective {
    pub cwd: Option<PathBuf>,
    pub env: BTreeMap<String, String>,
    pub timeout: Option<Duration>,
    pub kill_grace: Duration,
    pub expected_duration: Option<Duration>,
    /// True when --expected-duration came from the CLI flag, which implies
    /// the user wants the long_run alert even without an events entry.
    pub expected_from_cli: bool,
    pub capture_stdout: bool,
    pub capture_stderr: bool,
    pub capture_mode: CaptureMode,
    pub capture_head_bytes: u64,
    pub capture_tail_bytes: u64,
    pub min_free_bytes: u64,
    pub failure_output: bool,
    pub schedule_label: Option<String>,
}

pub fn resolve_effective(cfg: &Config, job_id: &str, cli: &CliOverrides) -> Effective {
    let job = cfg.jobs.get(job_id);
    let g = &cfg.global;

    let mut env = job.map(|j| j.env.clone()).unwrap_or_default();
    for (k, v) in &cli.env {
        env.insert(k.clone(), v.clone()); // CLI wins per key (SPEC §3)
    }

    Effective {
        cwd: cli
            .cwd
            .clone()
            .or_else(|| job.and_then(|j| j.cwd.as_deref().map(expand_tilde))),
        env,
        timeout: cli
            .timeout
            .or_else(|| job.and_then(|j| j.timeout.map(|d| d.0))),
        kill_grace: cli
            .kill_grace
            .or_else(|| job.and_then(|j| j.kill_grace.map(|d| d.0)))
            .or_else(|| g.kill_grace.map(|d| d.0))
            .unwrap_or(DEFAULT_KILL_GRACE),
        expected_duration: cli
            .expected_duration
            .or_else(|| job.and_then(|j| j.expected_duration.map(|d| d.0))),
        expected_from_cli: cli.expected_duration.is_some(),
        capture_stdout: job
            .and_then(|j| j.capture_stdout)
            .or(g.capture_stdout)
            .unwrap_or(true),
        capture_stderr: job
            .and_then(|j| j.capture_stderr)
            .or(g.capture_stderr)
            .unwrap_or(true),
        capture_mode: job
            .and_then(|j| j.capture_mode)
            .or(g.capture_mode)
            .unwrap_or(CaptureMode::Capped),
        capture_head_bytes: job
            .and_then(|j| j.capture_head_bytes)
            .or(g.capture_head_bytes)
            .map(|b| b.0)
            .unwrap_or(DEFAULT_CAPTURE_HEAD),
        capture_tail_bytes: job
            .and_then(|j| j.capture_tail_bytes)
            .or(g.capture_tail_bytes)
            .map(|b| b.0)
            .unwrap_or(DEFAULT_CAPTURE_TAIL),
        min_free_bytes: g.min_free_bytes.map(|b| b.0).unwrap_or(DEFAULT_MIN_FREE),
        failure_output: job
            .and_then(|j| j.failure_output)
            .or(cfg.notify.failure_output)
            .unwrap_or(true),
        schedule_label: job.and_then(|j| j.schedule_label.clone()),
    }
}

/// Operational log path: log.path or `<state-dir>/uatu.jsonl`.
pub fn resolve_log_path(cfg: &Config, state_dir: &Path) -> PathBuf {
    match &cfg.log.path {
        Some(p) => expand_tilde(p),
        None => state_dir.join("uatu.jsonl"),
    }
}

pub fn log_max_bytes(cfg: &Config) -> u64 {
    cfg.log.max_bytes.map(|b| b.0).unwrap_or(DEFAULT_LOG_MAX)
}

pub fn digest_period(cfg: &Config, job_id: &str) -> DigestPeriod {
    cfg.jobs
        .get(job_id)
        .and_then(|j| j.digest)
        .or(cfg.notify.digest)
        .unwrap_or(DigestPeriod::Off)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(text: &str) -> LoadOutcome {
        let doc: toml::Value = toml::from_str(text).unwrap();
        let mut out = LoadOutcome {
            path: None,
            config: Config::default(),
            warnings: Vec::new(),
            invalid: None,
            redaction_invalid: None,
        };
        out.warnings.extend(unknown_keys(&doc));
        out.config = parse_tables(&doc, &mut out.warnings, &mut out.redaction_invalid);
        out
    }

    #[test]
    fn unknown_keys_detected_with_table_names() {
        let out = parse(
            r#"
[global]
kil_grace = "30s"
[jobs.x]
timout = "1h"
[reporters.discord.d]
webhook_url = "https://x"
wrong = 1
"#,
        );
        let joined = out.warnings.join("\n");
        assert!(joined.contains("`kil_grace` in [global]"), "{joined}");
        assert!(joined.contains("`timout` in [jobs.x]"), "{joined}");
        assert!(
            joined.contains("`wrong` in [reporters.discord.d]"),
            "{joined}"
        );
    }

    #[test]
    fn bad_table_falls_back_and_warns() {
        let out = parse("[retention]\nmax_age = \"3x\"\n");
        assert!(out.invalid.is_none());
        assert!(out.warnings.iter().any(|w| w.contains("[retention]")));
        assert_eq!(out.config.retention_max_age(), DEFAULT_RETENTION_AGE);
    }

    #[test]
    fn invalid_redaction_flagged_not_defaulted() {
        let out = parse("[redaction]\nliterals = [1, 2]\n");
        assert!(out.redaction_invalid.is_some());
    }

    #[test]
    fn precedence_cli_over_job_over_global() {
        let out = parse(
            r#"
[global]
kill_grace = "10s"
capture_stdout = false
[jobs.j]
kill_grace = "20s"
timeout = "1h"
env = { A = "job", B = "job" }
cwd = "/from-job"
"#,
        );
        let cfg = out.config;
        // job overrides global:
        let eff = resolve_effective(&cfg, "j", &CliOverrides::default());
        assert_eq!(eff.kill_grace, Duration::from_secs(20));
        assert_eq!(eff.timeout, Some(Duration::from_secs(3600)));
        assert!(!eff.capture_stdout, "global capture_stdout=false applies");
        assert_eq!(eff.cwd, Some(PathBuf::from("/from-job")));
        // CLI overrides job, and --env merges per key:
        let cli = CliOverrides {
            cwd: Some(PathBuf::from("/from-cli")),
            env: vec![("A".into(), "cli".into()), ("C".into(), "cli".into())],
            timeout: Some(Duration::from_secs(5)),
            kill_grace: Some(Duration::from_secs(1)),
            expected_duration: None,
        };
        let eff = resolve_effective(&cfg, "j", &cli);
        assert_eq!(eff.kill_grace, Duration::from_secs(1));
        assert_eq!(eff.timeout, Some(Duration::from_secs(5)));
        assert_eq!(eff.cwd, Some(PathBuf::from("/from-cli")));
        assert_eq!(eff.env.get("A").unwrap(), "cli");
        assert_eq!(eff.env.get("B").unwrap(), "job");
        assert_eq!(eff.env.get("C").unwrap(), "cli");
        // unmatched job falls back to global/default:
        let eff = resolve_effective(&cfg, "other", &CliOverrides::default());
        assert_eq!(eff.kill_grace, Duration::from_secs(10));
        assert_eq!(eff.timeout, None);
        assert_eq!(eff.capture_head_bytes, DEFAULT_CAPTURE_HEAD);
    }

    #[test]
    fn auto_secrets_collected() {
        let out = parse(
            r#"
[reporters.discord.d]
webhook_url = "https://discord.com/api/webhooks/1/abc"
[reporters.smtp.s]
host = "h"
from = "f@x"
password = "smtp-pass"
recipients = ["o@x"]
"#,
        );
        let secrets = out.config.auto_secrets();
        assert!(secrets.contains(&"https://discord.com/api/webhooks/1/abc".to_string()));
        assert!(secrets.contains(&"smtp-pass".to_string()));
    }

    #[test]
    fn digest_precedence_and_utc_windows() {
        let out = parse(
            r#"
[notify]
digest = "hourly"
[jobs.fast]
digest = "daily"
[jobs.immediate]
digest = "off"
"#,
        );
        let cfg = out.config;
        assert_eq!(digest_period(&cfg, "other"), DigestPeriod::Hourly);
        assert_eq!(digest_period(&cfg, "fast"), DigestPeriod::Daily);
        assert_eq!(digest_period(&cfg, "immediate"), DigestPeriod::Off);

        let ms = 1_700_000_000_000; // 2023-11-14T22:13:20Z
        let (week_start, week_end) = DigestPeriod::Weekly.window_for(ms).unwrap();
        assert_eq!(crate::util::rfc3339(week_start), "2023-11-13T00:00:00Z");
        assert_eq!(crate::util::rfc3339(week_end), "2023-11-20T00:00:00Z");

        let (month_start, month_end) = DigestPeriod::Monthly.window_for(ms).unwrap();
        assert_eq!(crate::util::rfc3339(month_start), "2023-11-01T00:00:00Z");
        assert_eq!(crate::util::rfc3339(month_end), "2023-12-01T00:00:00Z");
    }

    #[test]
    fn sample_config_from_spec_parses_clean() {
        let out = parse(include_str!("sample_config_test.toml"));
        assert!(out.invalid.is_none());
        assert!(out.redaction_invalid.is_none());
        assert_eq!(out.warnings, Vec::<String>::new());
        assert_eq!(out.config.discord.len(), 1);
        assert_eq!(out.config.smtp.len(), 1);
        assert!(out.config.jobs.contains_key("nightly-backup"));
    }
}
