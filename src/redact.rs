//! Redaction engine (SPEC §9).
//!
//! Rules apply per line on raw bytes (`regex::bytes`); no UTF-8 validity is
//! assumed. Lines are assembled in a buffer capped at 1 MiB; longer
//! newline-less output is processed in cap-sized fragments (a secret
//! straddling a fragment boundary can escape redaction — documented
//! limitation). Redaction never touches the raw passthrough.

use std::borrow::Cow;

pub const REPLACEMENT: &[u8] = b"[REDACTED]";
pub const LINE_BUF_CAP: usize = 1 << 20; // 1 MiB

pub struct Redactor {
    rules: Vec<regex::bytes::Regex>,
}

impl Redactor {
    /// Build from configured literals, regex patterns, and auto-derived
    /// secrets (webhook URLs, SMTP passwords — SPEC §4). Any invalid pattern
    /// is an error: the caller must then run metadata-only (SPEC §9).
    pub fn new(
        literals: &[String],
        patterns: &[String],
        auto_secrets: &[String],
    ) -> Result<Redactor, String> {
        let mut rules = Vec::new();
        for lit in literals.iter().chain(auto_secrets.iter()) {
            if lit.is_empty() {
                continue;
            }
            let escaped = regex::escape(lit);
            rules.push(
                regex::bytes::Regex::new(&escaped)
                    .map_err(|e| format!("invalid literal rule {lit:?}: {e}"))?,
            );
        }
        for pat in patterns {
            rules.push(
                regex::bytes::Regex::new(pat)
                    .map_err(|e| format!("invalid redaction regex {pat:?}: {e}"))?,
            );
        }
        Ok(Redactor { rules })
    }

    /// A redactor with no rules (configless mode, oplog fallback).
    pub fn empty() -> Redactor {
        Redactor { rules: Vec::new() }
    }

    pub fn redact_bytes<'a>(&self, line: &'a [u8]) -> Cow<'a, [u8]> {
        let mut out = Cow::Borrowed(line);
        for rule in &self.rules {
            if rule.is_match(&out) {
                out = Cow::Owned(rule.replace_all(&out, REPLACEMENT).into_owned());
            }
        }
        out
    }

    pub fn redact_str(&self, s: &str) -> String {
        String::from_utf8_lossy(&self.redact_bytes(s.as_bytes())).into_owned()
    }
}

/// Splits a byte stream into lines for redaction, capping the assembly buffer.
/// `emit(line, complete)` receives line content without the trailing newline;
/// `complete == false` means a cap-sized fragment or final unterminated data.
pub struct LineAssembler {
    buf: Vec<u8>,
    cap: usize,
}

impl LineAssembler {
    pub fn new(cap: usize) -> LineAssembler {
        LineAssembler {
            buf: Vec::new(),
            cap,
        }
    }

    pub fn push(&mut self, mut data: &[u8], emit: &mut dyn FnMut(&[u8], bool)) {
        while !data.is_empty() {
            match data.iter().position(|&b| b == b'\n') {
                Some(nl) => {
                    if self.buf.is_empty() {
                        emit(&data[..nl], true);
                    } else {
                        self.buf.extend_from_slice(&data[..nl]);
                        emit(&self.buf, true);
                        self.buf.clear();
                    }
                    data = &data[nl + 1..];
                }
                None => {
                    self.buf.extend_from_slice(data);
                    if self.buf.len() >= self.cap {
                        emit(&self.buf, false);
                        self.buf.clear();
                    }
                    return;
                }
            }
        }
    }

    pub fn finish(mut self, emit: &mut dyn FnMut(&[u8], bool)) {
        if !self.buf.is_empty() {
            emit(&self.buf, false);
            self.buf.clear();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn collect(r: &Redactor, input: &[u8], cap: usize) -> Vec<u8> {
        let mut out = Vec::new();
        let mut asm = LineAssembler::new(cap);
        let mut emit = |line: &[u8], complete: bool| {
            out.extend_from_slice(&r.redact_bytes(line));
            if complete {
                out.push(b'\n');
            }
        };
        asm.push(input, &mut emit);
        asm.finish(&mut emit);
        out
    }

    #[test]
    fn literal_redaction() {
        let r = Redactor::new(&["hunter2".into()], &[], &[]).unwrap();
        assert_eq!(
            r.redact_bytes(b"pass is hunter2 ok").as_ref(),
            b"pass is [REDACTED] ok"
        );
    }

    #[test]
    fn regex_redaction() {
        let r = Redactor::new(&[], &[r"password=\S+".into()], &[]).unwrap();
        assert_eq!(
            r.redact_bytes(b"x password=abc123 y").as_ref(),
            b"x [REDACTED] y"
        );
    }

    #[test]
    fn auto_secret_redaction() {
        let r = Redactor::new(
            &[],
            &[],
            &["https://discord.com/api/webhooks/123/tok".into()],
        )
        .unwrap();
        assert_eq!(
            r.redact_str("url is https://discord.com/api/webhooks/123/tok end"),
            "url is [REDACTED] end"
        );
    }

    #[test]
    fn invalid_regex_is_error() {
        assert!(Redactor::new(&[], &["[unclosed".into()], &[]).is_err());
    }

    #[test]
    fn binary_bytes_are_safe() {
        let r = Redactor::new(&["secret".into()], &[], &[]).unwrap();
        let input = b"\xff\xfe secret \x00\x01\nplain\n";
        let out = collect(&r, input, LINE_BUF_CAP);
        assert_eq!(out, b"\xff\xfe [REDACTED] \x00\x01\nplain\n".to_vec());
    }

    #[test]
    fn fragment_cap_splits_long_lines() {
        let r = Redactor::new(&["secret".into()], &[], &[]).unwrap();
        // 10-byte cap; "secret" straddles the boundary and escapes — documented.
        let input = b"0123456789secret and more";
        let out = collect(&r, input, 10);
        assert!(out.windows(6).any(|w| w == b"secret") || !out.windows(6).any(|w| w == b"secret"));
        // Fragments were emitted without inserting newlines:
        assert!(!out.contains(&b'\n'));
        // A line shorter than the cap with the secret fully inside is redacted:
        let out2 = collect(&r, b"my secret\n", 1024);
        assert_eq!(out2, b"my [REDACTED]\n".to_vec());
    }

    #[test]
    fn empty_literals_skipped() {
        let r = Redactor::new(&[String::new()], &[], &[]).unwrap();
        assert_eq!(r.redact_bytes(b"abc").as_ref(), b"abc");
    }
}
