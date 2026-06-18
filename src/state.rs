//! State directory layout and preparation (SPEC §7): dirs 0700, files 0600.

use std::fs::{DirBuilder, OpenOptions};
use std::io::Write;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

#[derive(Clone, Debug)]
pub struct Paths {
    pub state_dir: PathBuf,
    pub db: PathBuf,
    pub lock: PathBuf,
    pub output: PathBuf,
}

impl Paths {
    pub fn run_output_dir(&self, job_id: &str, run_id: &str) -> PathBuf {
        self.output.join(job_id).join(run_id)
    }
}

/// Create the state directory tree with restrictive modes. Errors here put
/// `run` into passthrough mode (SPEC §10).
pub fn prepare(state_dir: &Path) -> std::io::Result<Paths> {
    mkdir_0700_all(state_dir)?;
    let output = state_dir.join("output");
    mkdir_0700_all(&output)?;
    Ok(Paths {
        state_dir: state_dir.to_path_buf(),
        db: state_dir.join("uatu.db"),
        lock: state_dir.join("flush.lock"),
        output,
    })
}

pub fn mkdir_0700_all(path: &Path) -> std::io::Result<()> {
    if path.is_dir() {
        return Ok(());
    }
    DirBuilder::new().recursive(true).mode(0o700).create(path)
}

/// Write `bytes` to `target` with 0600 permissions, truncating any existing
/// file. Config files can hold webhook URLs / SMTP passwords, so permissions
/// are tightened through the open file handle (avoiding a path-based TOCTOU)
/// *before* any bytes are written: the file is already truncated to empty at
/// this point, so a pre-existing loose-mode file (`create().mode()` only
/// applies on creation) never briefly holds world-readable secrets.
pub fn write_0600(target: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let mut f = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .mode(0o600)
        .open(target)?;
    f.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    f.write_all(bytes)
}

/// Free bytes available to unprivileged users on the filesystem holding
/// `path` (statvfs; SPEC §6 storage preflight). None when undeterminable.
pub fn free_bytes(path: &Path) -> Option<u64> {
    use std::os::unix::ffi::OsStrExt;
    let c = std::ffi::CString::new(path.as_os_str().as_bytes()).ok()?;
    let mut s: libc::statvfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::statvfs(c.as_ptr(), &mut s) } != 0 {
        return None;
    }
    // f_bavail/f_frsize widths vary by platform; the casts keep this portable.
    #[allow(clippy::unnecessary_cast)]
    Some(s.f_bavail as u64 * s.f_frsize as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn dirs_created_0700() {
        let dir = tempfile::tempdir().unwrap();
        let sd = dir.path().join("state");
        let paths = prepare(&sd).unwrap();
        for p in [&paths.state_dir, &paths.output] {
            let mode = std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o700, "{}", p.display());
        }
    }

    #[test]
    fn free_bytes_reports_something() {
        // `/` can be a read-only composefs with 0 avail (ostree systems);
        // use a writable location.
        let tmp = std::env::temp_dir();
        assert!(free_bytes(&tmp).unwrap() > 0);
    }
}
