//! Job identity (SPEC §5): explicit `--name` slugs or inferred
//! `<basename>-<hash>` ids, plus ULID run ids.

use std::path::Path;

use sha2::{Digest, Sha256};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExecMode {
    Direct,
    Shell,
}

impl ExecMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            ExecMode::Direct => "direct",
            ExecMode::Shell => "shell",
        }
    }
}

/// `--name` must match `^[A-Za-z0-9._-]+$`.
pub fn valid_slug(s: &str) -> bool {
    !s.is_empty()
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'_' || b == b'-')
}

/// Inferred identity: stable short hash over Unix uid, resolved cwd,
/// execution mode, and argv (or shell command string). Returns
/// `(job_id, basename)`; the basename feeds the fragmentation hint.
pub fn infer_job_id(uid: u32, cwd: &Path, mode: ExecMode, argv: &[String]) -> (String, String) {
    let basename = match mode {
        ExecMode::Shell => "shell".to_string(),
        ExecMode::Direct => {
            let raw = argv
                .first()
                .map(|a| {
                    Path::new(a)
                        .file_name()
                        .map(|f| f.to_string_lossy().into_owned())
                        .unwrap_or_else(|| a.clone())
                })
                .unwrap_or_default();
            sanitize_basename(&raw)
        }
    };

    let mut hasher = Sha256::new();
    hasher.update(uid.to_string().as_bytes());
    hasher.update([0]);
    hasher.update(cwd.to_string_lossy().as_bytes());
    hasher.update([0]);
    hasher.update(mode.as_str().as_bytes());
    for arg in argv {
        hasher.update([0]);
        hasher.update(arg.as_bytes());
    }
    let digest = hasher.finalize();
    let hash: String = digest[..6].iter().map(|b| format!("{b:02x}")).collect(); // 12 hex chars

    (format!("{basename}-{hash}"), basename)
}

fn sanitize_basename(raw: &str) -> String {
    let s: String = raw
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-' {
                c
            } else {
                '-'
            }
        })
        .collect();
    if s.is_empty() {
        "cmd".to_string()
    } else {
        s
    }
}

pub fn new_run_id() -> String {
    ulid::Ulid::new().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn slug_validation() {
        assert!(valid_slug("nightly-backup"));
        assert!(valid_slug("a.b_c-1"));
        assert!(!valid_slug(""));
        assert!(!valid_slug("has space"));
        assert!(!valid_slug("slash/y"));
        assert!(!valid_slug("ütf"));
    }

    #[test]
    fn inference_is_stable() {
        let cwd = PathBuf::from("/srv/app");
        let argv = vec!["/usr/local/bin/backup".to_string(), "--full".to_string()];
        let (a, base_a) = infer_job_id(1000, &cwd, ExecMode::Direct, &argv);
        let (b, _) = infer_job_id(1000, &cwd, ExecMode::Direct, &argv);
        assert_eq!(a, b);
        assert_eq!(base_a, "backup");
        assert!(a.starts_with("backup-"));
        let hash = a.rsplit('-').next().unwrap();
        assert_eq!(hash.len(), 12);
        assert!(hash
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()));
    }

    #[test]
    fn inference_varies_with_inputs() {
        let cwd = PathBuf::from("/srv/app");
        let argv1 = vec![
            "backup".to_string(),
            "--date".to_string(),
            "2026-06-10".to_string(),
        ];
        let argv2 = vec![
            "backup".to_string(),
            "--date".to_string(),
            "2026-06-11".to_string(),
        ];
        let (a, _) = infer_job_id(1000, &cwd, ExecMode::Direct, &argv1);
        let (b, _) = infer_job_id(1000, &cwd, ExecMode::Direct, &argv2);
        assert_ne!(a, b, "different argv must fragment (documented caveat)");
        let (c, _) = infer_job_id(1001, &cwd, ExecMode::Direct, &argv1);
        assert_ne!(a, c, "different uid differs");
        let (d, _) = infer_job_id(1000, Path::new("/other"), ExecMode::Direct, &argv1);
        assert_ne!(a, d, "different cwd differs");
    }

    #[test]
    fn shell_mode_basename() {
        let (id, base) = infer_job_id(
            1000,
            Path::new("/"),
            ExecMode::Shell,
            &["cd /x && run".to_string()],
        );
        assert_eq!(base, "shell");
        assert!(id.starts_with("shell-"));
    }
}
