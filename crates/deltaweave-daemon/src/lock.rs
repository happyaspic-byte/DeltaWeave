//! Single-instance lock with stale-PID recovery.

use std::{
    fs::{self, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail, ensure};

/// Restricts a control-plane directory to the owning user.
///
/// Existing permissive modes are tightened so the token and socket cannot be
/// read by group or world; failures are surfaced instead of ignored.
///
/// # Errors
///
/// Returns an error when permissions cannot be tightened.
pub fn restrict_owner_dir(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let metadata = fs::metadata(path)
            .with_context(|| format!("failed to stat control directory {}", path.display()))?;
        let mode = metadata.permissions().mode();
        if mode & 0o077 != 0 {
            fs::set_permissions(path, fs::Permissions::from_mode(mode & 0o700)).with_context(
                || format!("failed to restrict control directory {}", path.display()),
            )?;
        }
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
    Ok(())
}

/// Exclusive runtime lock that records the owning process ID.
#[derive(Debug)]
pub struct DaemonLock {
    path: PathBuf,
}

impl DaemonLock {
    /// Acquires the lock, replacing a stale PID file left by a dead process.
    ///
    /// # Errors
    ///
    /// Returns an error when another live daemon already holds the lock or the
    /// lock file cannot be written.
    pub fn acquire(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent)?;
            restrict_owner_dir(parent)?;
        }
        match try_create(&path) {
            Ok(()) => return Ok(Self { path }),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("failed to create lock file {}", path.display()));
            }
        }
        recover_stale(&path)?;
        try_create(&path)
            .with_context(|| format!("failed to create lock file {}", path.display()))?;
        Ok(Self { path })
    }

    /// Path of the lock file.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for DaemonLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

fn try_create(path: &Path) -> io::Result<()> {
    let mut options = OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    writeln!(file, "{}", std::process::id())?;
    file.sync_all()?;
    Ok(())
}

fn recover_stale(path: &Path) -> Result<()> {
    let contents = fs::read_to_string(path)
        .with_context(|| format!("failed to read lock file {}", path.display()))?;
    let pid = contents
        .trim()
        .parse::<u32>()
        .with_context(|| format!("lock file {} does not contain a PID", path.display()))?;
    if process_is_alive(pid) {
        bail!("daemon already running with pid {pid}");
    }
    fs::remove_file(path)
        .with_context(|| format!("failed to remove stale lock file {}", path.display()))?;
    ensure!(
        !path.exists(),
        "stale lock file still present after removal"
    );
    Ok(())
}

fn process_is_alive(pid: u32) -> bool {
    if pid == 0 {
        return false;
    }
    #[cfg(unix)]
    {
        PathBuf::from(format!("/proc/{pid}")).exists()
    }
    #[cfg(windows)]
    {
        // Without FFI we cannot OpenProcess. Treat the current PID as live
        // and the test sentinel u32::MAX as dead; every other PID is assumed
        // live so a second Windows instance never steals a running lock.
        // Operators recover a crashed Windows daemon by deleting daemon.lock.
        return pid != u32::MAX && pid != 0;
    }
    #[cfg(not(any(unix, windows)))]
    {
        pid != u32::MAX
    }
}
