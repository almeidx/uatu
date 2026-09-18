//! Advisory flush lock (SPEC §7): `flock` on `<state-dir>/flush.lock`
//! serializes flush and prune. `flush` waits up to 10s; `run`'s opportunistic
//! flush skips silently when held. Held for the lifetime of the guard.

use std::fs::{File, OpenOptions, TryLockError};
use std::os::unix::fs::OpenOptionsExt;
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
    match file.try_lock() {
        Ok(()) => Ok(Some(FlushLock { _file: file })),
        Err(TryLockError::WouldBlock) => Ok(None),
        Err(TryLockError::Error(err)) => Err(err),
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

    #[test]
    fn lock_interoperates_with_existing_flock_users() {
        use std::os::fd::AsRawFd;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("flush.lock");
        let held = try_acquire(&path).unwrap().unwrap();
        let legacy = OpenOptions::new().write(true).open(&path).unwrap();
        let try_legacy =
            || unsafe { libc::flock(legacy.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };

        assert_eq!(try_legacy(), -1);
        assert_eq!(
            std::io::Error::last_os_error().raw_os_error(),
            Some(libc::EWOULDBLOCK)
        );
        drop(held);
        assert_eq!(try_legacy(), 0);
        assert!(try_acquire(&path).unwrap().is_none());
        drop(legacy);
        assert!(try_acquire(&path).unwrap().is_some());
    }
}
