//! Wrapper liveness identity (SPEC §6): pid + /proc start time + boot id.
//!
//! A recorded wrapper is alive iff the stored boot id equals the current boot
//! id AND the pid exists AND that pid's /proc start time equals the stored
//! one. A bare `kill(pid, 0)` is insufficient (pid reuse) and is only used as
//! the non-Linux dev fallback.

use std::fs;

#[derive(Clone, Debug, Default)]
pub struct Liveness {
    pub pid: i32,
    pub start_ticks: u64,
    pub boot_id: String,
}

pub fn current() -> Liveness {
    let pid = std::process::id() as i32;
    Liveness {
        pid,
        start_ticks: proc_start_ticks(pid).unwrap_or(0),
        boot_id: boot_id(),
    }
}

/// Kernel boot id from /proc/sys/kernel/random/boot_id; empty when unreadable.
pub fn boot_id() -> String {
    fs::read_to_string("/proc/sys/kernel/random/boot_id")
        .map(|s| s.trim().to_string())
        .unwrap_or_default()
}

/// Field 22 of /proc/<pid>/stat (starttime, clock ticks since boot). The comm
/// field (2) may contain spaces and parens, so parse after the last ')'.
pub fn proc_start_ticks(pid: i32) -> Option<u64> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let after = &stat[stat.rfind(')')? + 1..];
    // `after` starts at field 3 (state); starttime is field 22 → index 19 here.
    after.split_whitespace().nth(19)?.parse().ok()
}

pub fn is_alive(pid: i32, start_ticks: u64, boot_id_stored: &str) -> bool {
    if cfg!(target_os = "linux") {
        if boot_id_stored.is_empty() || boot_id_stored != boot_id() {
            return false;
        }
        match proc_start_ticks(pid) {
            Some(ticks) => ticks == start_ticks,
            None => false,
        }
    } else {
        // macOS best-effort dev fallback only; not a supported platform.
        unsafe { libc::kill(pid, 0) == 0 }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg(target_os = "linux")]
    fn current_process_is_alive() {
        let me = current();
        assert!(me.start_ticks > 0);
        assert!(!me.boot_id.is_empty());
        assert!(is_alive(me.pid, me.start_ticks, &me.boot_id));
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn pid_reuse_and_reboot_detected_as_dead() {
        let me = current();
        // Same pid, wrong start time (pid reuse simulation):
        assert!(!is_alive(me.pid, me.start_ticks + 1, &me.boot_id));
        // Wrong boot id (reboot simulation):
        assert!(!is_alive(me.pid, me.start_ticks, "0000-dead-beef"));
        // Nonexistent pid:
        assert!(!is_alive(0x3ffffff, 1, &me.boot_id));
    }
}
