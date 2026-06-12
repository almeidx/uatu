//! Operational JSONL log (SPEC §7): structured entries, head-trim over a size
//! cap via rewrite-to-temp + atomic rename, content passed through redaction.
//! Failures here never affect anything else (SPEC §6, §10).

use std::fs::OpenOptions;
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use crate::redact::Redactor;
use crate::util::{now_ms, rfc3339};

#[derive(Clone)]
pub struct OpLog {
    inner: Arc<Mutex<Inner>>,
}

struct Inner {
    path: Option<PathBuf>,
    max_bytes: u64,
    redactor: Arc<Redactor>,
}

impl OpLog {
    pub fn new(path: PathBuf, max_bytes: u64, redactor: Arc<Redactor>) -> OpLog {
        OpLog {
            inner: Arc::new(Mutex::new(Inner {
                path: Some(path),
                max_bytes,
                redactor,
            })),
        }
    }

    /// A log that silently drops everything (state unavailable).
    pub fn disabled() -> OpLog {
        OpLog {
            inner: Arc::new(Mutex::new(Inner {
                path: None,
                max_bytes: 0,
                redactor: Arc::new(Redactor::empty()),
            })),
        }
    }

    pub fn log(
        &self,
        level: &str,
        event: &str,
        message: &str,
        fields: &[(&str, serde_json::Value)],
    ) {
        let Ok(inner) = self.inner.lock() else { return };
        let Some(path) = inner.path.clone() else {
            return;
        };
        let mut obj = serde_json::Map::new();
        obj.insert("ts".into(), serde_json::Value::String(rfc3339(now_ms())));
        obj.insert("level".into(), serde_json::Value::String(level.to_string()));
        obj.insert("event".into(), serde_json::Value::String(event.to_string()));
        obj.insert(
            "message".into(),
            serde_json::Value::String(inner.redactor.redact_str(message)),
        );
        for (k, v) in fields {
            let v = match v {
                serde_json::Value::String(s) => {
                    serde_json::Value::String(inner.redactor.redact_str(s))
                }
                other => other.clone(),
            };
            obj.insert((*k).to_string(), v);
        }
        let mut line = serde_json::Value::Object(obj).to_string();
        line.push('\n');
        // Best-effort: any failure below is ignored by design.
        let _ = (|| -> std::io::Result<()> {
            let mut f = OpenOptions::new()
                .create(true)
                .append(true)
                .mode(0o600)
                .open(&path)?;
            f.write_all(line.as_bytes())?;
            let len = f.metadata()?.len();
            drop(f);
            if len > inner.max_bytes {
                head_trim(&path, inner.max_bytes / 2)?;
            }
            Ok(())
        })();
    }

    pub fn info(&self, event: &str, message: &str, fields: &[(&str, serde_json::Value)]) {
        self.log("info", event, message, fields);
    }

    pub fn warn(&self, event: &str, message: &str, fields: &[(&str, serde_json::Value)]) {
        self.log("warn", event, message, fields);
    }

    pub fn error(&self, event: &str, message: &str, fields: &[(&str, serde_json::Value)]) {
        self.log("error", event, message, fields);
    }
}

/// Drop whole records from the head until the file is at most `target`
/// bytes, writing to a temp file and renaming over the original so a crash
/// never leaves a partial record.
fn head_trim(path: &std::path::Path, target: u64) -> std::io::Result<()> {
    let data = std::fs::read(path)?;
    let mut start = 0usize;
    while data.len() - start > target as usize {
        match data[start..].iter().position(|&b| b == b'\n') {
            Some(nl) => start += nl + 1,
            None => {
                start = data.len();
                break;
            }
        }
    }
    let tmp = path.with_extension("jsonl.tmp");
    {
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .mode(0o600)
            .open(&tmp)?;
        f.write_all(&data[start..])?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writes_redacted_json_lines_and_trims_whole_records() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("uatu.jsonl");
        let red = Arc::new(Redactor::new(&["s3cret".into()], &[], &[]).unwrap());
        let log = OpLog::new(path.clone(), 600, red);
        for i in 0..50 {
            log.info(
                "run_finished",
                &format!("run {i} done with s3cret inside"),
                &[("run_id", serde_json::json!(format!("R{i}")))],
            );
        }
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(text.len() <= 600, "trimmed under cap, got {}", text.len());
        for line in text.lines() {
            let v: serde_json::Value = serde_json::from_str(line).expect("whole JSON records");
            assert!(v["message"].as_str().unwrap().contains("[REDACTED]"));
            assert!(v["ts"].is_string() && v["level"].is_string() && v["event"].is_string());
        }
        // newest records survive head-trim
        assert!(text.lines().last().unwrap().contains("R49"));
    }
}
