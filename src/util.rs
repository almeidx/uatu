//! Duration / byte-size parsing and formatting, plus timestamp helpers.
//!
//! Formats per SPEC §4: durations accept `ms`, `s`, `m`, `h`, `d`; byte sizes
//! accept base-2 `KiB`/`MiB`/`GiB` and base-10 `KB`/`MB`/`GB` (bare integers
//! and a trailing `B` are accepted as plain bytes).

use std::time::Duration;

use jiff::Timestamp;

pub fn parse_duration(s: &str) -> Result<Duration, String> {
    let s = s.trim();
    let split = s
        .find(|c: char| !c.is_ascii_digit())
        .ok_or_else(|| format!("invalid duration \"{s}\": missing unit (use ms, s, m, h, d)"))?;
    let (num, unit) = s.split_at(split);
    if num.is_empty() {
        return Err(format!("invalid duration \"{s}\": missing numeric value"));
    }
    let n: u64 = num
        .parse()
        .map_err(|_| format!("invalid duration \"{s}\": bad number"))?;
    let per_unit: u64 = match unit {
        "ms" => 1,
        "s" => 1_000,
        "m" => 60_000,
        "h" => 3_600_000,
        "d" => 86_400_000,
        _ => {
            return Err(format!(
                "invalid duration \"{s}\": unknown unit \"{unit}\" (use ms, s, m, h, d)"
            ))
        }
    };
    let ms = n
        .checked_mul(per_unit)
        .ok_or_else(|| format!("invalid duration \"{s}\": overflow"))?;
    Ok(Duration::from_millis(ms))
}

pub fn parse_bytes(s: &str) -> Result<u64, String> {
    let s = s.trim();
    let split = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    let (num, unit) = s.split_at(split);
    if num.is_empty() {
        return Err(format!("invalid byte size \"{s}\": missing numeric value"));
    }
    let n: u64 = num
        .parse()
        .map_err(|_| format!("invalid byte size \"{s}\": bad number"))?;
    let mult: u64 = match unit.trim() {
        "" | "B" => 1,
        "KiB" => 1 << 10,
        "MiB" => 1 << 20,
        "GiB" => 1 << 30,
        "KB" => 1_000,
        "MB" => 1_000_000,
        "GB" => 1_000_000_000,
        u => {
            return Err(format!(
                "invalid byte size \"{s}\": unknown unit \"{u}\" (use KiB, MiB, GiB, KB, MB, GB)"
            ))
        }
    };
    n.checked_mul(mult)
        .ok_or_else(|| format!("invalid byte size \"{s}\": overflow"))
}

pub fn format_duration_ms(ms: u64) -> String {
    if ms < 1_000 {
        return format!("{ms}ms");
    }
    let secs = ms / 1_000;
    let (d, h, m, s) = (
        secs / 86_400,
        (secs % 86_400) / 3_600,
        (secs % 3_600) / 60,
        secs % 60,
    );
    let mut parts = Vec::new();
    if d > 0 {
        parts.push(format!("{d}d"));
    }
    if h > 0 {
        parts.push(format!("{h}h"));
    }
    if m > 0 {
        parts.push(format!("{m}m"));
    }
    if s > 0 || parts.is_empty() {
        parts.push(format!("{s}s"));
    }
    parts.join(" ")
}

pub fn format_bytes(n: u64) -> String {
    const UNITS: [(&str, u64); 3] = [("GiB", 1 << 30), ("MiB", 1 << 20), ("KiB", 1 << 10)];
    for (name, size) in UNITS {
        if n >= size {
            return format!("{:.1} {name}", n as f64 / size as f64);
        }
    }
    format!("{n} B")
}

/// Current time as Unix epoch milliseconds (how all timestamps are stored).
pub fn now_ms() -> i64 {
    Timestamp::now().as_millisecond()
}

/// RFC 3339 UTC rendering of stored epoch milliseconds (SPEC §11).
pub fn rfc3339(ms: i64) -> String {
    match Timestamp::from_millisecond(ms) {
        Ok(ts) => ts.to_string(),
        Err(_) => format!("<invalid timestamp {ms}>"),
    }
}

/// Host-local rendering for email bodies (SPEC §8).
pub fn local_time(ms: i64) -> String {
    match Timestamp::from_millisecond(ms) {
        Ok(ts) => ts
            .to_zoned(jiff::tz::TimeZone::system())
            .strftime("%Y-%m-%d %H:%M:%S %:z")
            .to_string(),
        Err(_) => format!("<invalid timestamp {ms}>"),
    }
}

pub fn hostname() -> String {
    let mut buf = [0u8; 256];
    let r = unsafe { libc::gethostname(buf.as_mut_ptr() as *mut libc::c_char, buf.len()) };
    if r == 0 {
        let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
        if let Ok(s) = std::str::from_utf8(&buf[..end]) {
            if !s.is_empty() {
                return s.to_string();
            }
        }
    }
    "unknown-host".to_string()
}

/// Expand a leading `~/` to the user's home directory.
pub fn expand_tilde(path: &str) -> std::path::PathBuf {
    if path == "~" {
        if let Some(home) = std::env::var_os("HOME") {
            return std::path::PathBuf::from(home);
        }
    } else if let Some(rest) = path.strip_prefix("~/") {
        if let Some(home) = std::env::var_os("HOME") {
            return std::path::Path::new(&home).join(rest);
        }
    }
    std::path::PathBuf::from(path)
}

/// Keep the last `max_chars` characters of `s`, prefixing a truncation note.
pub fn tail_chars(s: &str, max_chars: usize) -> String {
    let count = s.chars().count();
    if count <= max_chars {
        return s.to_string();
    }
    let skip = count - max_chars;
    let tail: String = s.chars().skip(skip).collect();
    format!("…[{skip} chars truncated]…{tail}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations_parse() {
        assert_eq!(parse_duration("500ms").unwrap(), Duration::from_millis(500));
        assert_eq!(parse_duration("30s").unwrap(), Duration::from_secs(30));
        assert_eq!(parse_duration("15m").unwrap(), Duration::from_secs(900));
        assert_eq!(parse_duration("2h").unwrap(), Duration::from_secs(7200));
        assert_eq!(
            parse_duration("30d").unwrap(),
            Duration::from_secs(2_592_000)
        );
        assert!(parse_duration("10").is_err());
        assert!(parse_duration("s").is_err());
        assert!(parse_duration("10w").is_err());
        assert!(parse_duration("-3s").is_err());
        assert!(parse_duration("1.5s").is_err());
    }

    #[test]
    fn bytes_parse() {
        assert_eq!(parse_bytes("64KiB").unwrap(), 65_536);
        assert_eq!(parse_bytes("1MiB").unwrap(), 1_048_576);
        assert_eq!(parse_bytes("1GiB").unwrap(), 1 << 30);
        assert_eq!(parse_bytes("50MB").unwrap(), 50_000_000);
        assert_eq!(parse_bytes("1GB").unwrap(), 1_000_000_000);
        assert_eq!(parse_bytes("100KB").unwrap(), 100_000);
        assert_eq!(parse_bytes("123").unwrap(), 123);
        assert_eq!(parse_bytes("123B").unwrap(), 123);
        assert!(parse_bytes("10TB").is_err());
        assert!(parse_bytes("KB").is_err());
    }

    #[test]
    fn duration_formatting() {
        assert_eq!(format_duration_ms(450), "450ms");
        assert_eq!(format_duration_ms(42_000), "42s");
        assert_eq!(format_duration_ms(3_920_000), "1h 5m 20s");
        assert_eq!(format_duration_ms(90_061_000), "1d 1h 1m 1s");
    }

    #[test]
    fn tail_chars_truncates_front() {
        assert_eq!(tail_chars("hello", 10), "hello");
        let t = tail_chars("abcdefghij", 4);
        assert!(t.ends_with("ghij"));
        assert!(t.contains("truncated"));
    }
}
