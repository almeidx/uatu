//! Advisory flush lock (SPEC §7): `flock` on `<state-dir>/flush.lock`
//! serializes flush and prune. `flush` waits up to 10s; `run`'s opportunistic
//! flush skips silently when held. Held for the lifetime of the guard.

use std::fs::{File, OpenOptions};
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::io::AsRawFd;
use std::path::Path;
use std::time::{Duration, Instant};

pub struct FlushLock {
    _file: File, // lock released on close (drop)
}

pub fn try_acquire(path: &Path) -> std::io::Result<Option<FlushLock>> {
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .mode(0o600)
        .open(path)?;
    let r = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if r == 0 {
        return Ok(Some(FlushLock { _file: file }));
    }
    let err = std::io::Error::last_os_error();
    if err.raw_os_error() == Some(libc::EWOULDBLOCK) {
        Ok(None)
    } else {
        Err(err)
    }
}

/// Poll for the lock until `timeout` elapses.
pub fn acquire_wait(path: &Path, timeout: Duration) -> std::io::Result<Option<FlushLock>> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(lock) = try_acquire(path)? {
            return Ok(Some(lock));
        }
        if Instant::now() >= deadline {
            return Ok(None);
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lock_excludes_and_releases() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("flush.lock");
        let held = try_acquire(&path).unwrap().expect("first acquire");
        assert!(
            try_acquire(&path).unwrap().is_none(),
            "second acquire blocked"
        );
        drop(held);
        assert!(try_acquire(&path).unwrap().is_some(), "released on drop");
    }
}
