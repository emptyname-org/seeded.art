//! Bounded online SQLite backups. Publication never replaces an existing path.

use std::fs::{self, File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use rusqlite::backup::{Backup, StepResult};
use rusqlite::{Connection, InterruptHandle, OpenFlags};

const POLL: Duration = Duration::from_millis(10);
const MAX_STEP_PAGES: u32 = 4096;
const STAGING_PREFIX: &str = ".seeded-backup-v2-";
const LEASE_NAME: &str = ".seeded-backup.lock";
const MAX_SWEEP_ENTRIES: usize = 4096;
static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug)]
pub struct BackupOptions {
    pub timeout: Duration,
    pub pages_per_step: u32,
    /// Delay after a busy/locked result; successful batches yield without sleeping.
    pub retry_delay: Duration,
    pub cancellation: Option<Arc<AtomicBool>>,
}

impl Default for BackupOptions {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(300),
            pages_per_step: 128,
            retry_delay: Duration::from_millis(25),
            cancellation: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackupReport {
    pub pages: u32,
    pub bytes: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum BackupError {
    #[error("invalid backup request: {0}")]
    InvalidInput(&'static str),
    #[error("backup destination already exists; nothing was replaced")]
    DestinationExists,
    #[error("backup cancelled before publication")]
    Cancelled,
    #[error("backup deadline exceeded before publication")]
    Deadline,
    #[error("backup failed SQLite validation: {0}")]
    InvalidBackup(String),
    #[error("SQLite backup failed: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("backup filesystem operation failed: {0}")]
    Io(#[from] io::Error),
    #[error("backup was published; keep the destination, but cleanup/directory sync failed: {0}")]
    PublishedButNotSynced(io::Error),
}

struct Control {
    deadline: Instant,
    cancelled: Arc<AtomicBool>,
}

impl Control {
    fn check(&self) -> Result<(), BackupError> {
        if self.cancelled.load(Ordering::Acquire) {
            Err(BackupError::Cancelled)
        } else if Instant::now() >= self.deadline {
            Err(BackupError::Deadline)
        } else {
            Ok(())
        }
    }

    fn sqlite(&self, error: rusqlite::Error) -> BackupError {
        self.check().err().unwrap_or(BackupError::Sqlite(error))
    }

    fn pause(&self, delay: Duration) -> Result<(), BackupError> {
        let until = Instant::now() + delay;
        loop {
            self.check()?;
            let now = Instant::now();
            if now >= until {
                return Ok(());
            }
            thread::sleep(
                POLL.min(until - now)
                    .min(self.deadline.saturating_duration_since(now)),
            );
        }
    }
}

/// Interrupts SQLite work on cancellation/deadline; always joined before return.
struct Watchdog {
    stop: Arc<(Mutex<bool>, Condvar)>,
    thread: Option<JoinHandle<()>>,
}

impl Watchdog {
    fn start(
        source: InterruptHandle,
        destination: InterruptHandle,
        control: Arc<Control>,
    ) -> io::Result<Self> {
        let stop = Arc::new((Mutex::new(false), Condvar::new()));
        let state = stop.clone();
        let thread = thread::Builder::new()
            .name("sqlite-backup-deadline".into())
            .spawn(move || {
                let (lock, changed) = &*state;
                let mut stopped = lock.lock().unwrap_or_else(|error| error.into_inner());
                while !*stopped {
                    if control.check().is_err() {
                        source.interrupt();
                        destination.interrupt();
                    }
                    let waited = changed
                        .wait_timeout(stopped, POLL)
                        .unwrap_or_else(|error| error.into_inner());
                    stopped = waited.0;
                }
            })?;
        Ok(Self {
            stop,
            thread: Some(thread),
        })
    }
}

impl Drop for Watchdog {
    fn drop(&mut self) {
        let (lock, changed) = &*self.stop;
        *lock.lock().unwrap_or_else(|error| error.into_inner()) = true;
        changed.notify_all();
        if let Some(thread) = self.thread.take() {
            // No code in the worker can intentionally panic; still join on every exit.
            let _ = thread.join();
        }
    }
}

/// A persistent lock inode serializes backups using this directory. SQLite releases
/// the exclusive transaction after process death; never unlink or replace this file.
struct DirectoryLease {
    _connection: Connection,
}

fn private_new_file(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path)
}

fn sqlite_busy(error: &rusqlite::Error) -> bool {
    matches!(error, rusqlite::Error::SqliteFailure(error, _) if matches!(
        error.code, rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked
    ))
}

impl DirectoryLease {
    fn acquire(parent: &Path, control: &Control) -> Result<Self, BackupError> {
        let path = parent.join(LEASE_NAME);
        match private_new_file(&path) {
            Ok(file) => drop(file),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
        if !fs::symlink_metadata(&path)?.is_file() {
            return Err(BackupError::InvalidInput(
                "backup directory lease must be a regular file, not a symlink",
            ));
        }
        let connection = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_NO_MUTEX
                | OpenFlags::SQLITE_OPEN_NOFOLLOW,
        )?;
        connection.busy_timeout(Duration::ZERO)?;
        loop {
            control.check()?;
            match connection.execute_batch("BEGIN EXCLUSIVE;") {
                Ok(()) => break,
                Err(error) if sqlite_busy(&error) => control.pause(POLL)?,
                Err(error) => return Err(control.sqlite(error)),
            }
        }
        Ok(Self {
            _connection: connection,
        })
    }

    /// Only this lease protocol's staging names are eligible. Complete the bounded
    /// scan before deletion; directories, links, legacy names and backups stay intact.
    fn sweep(&self, parent: &Path, control: &Control) -> Result<(), BackupError> {
        let mut orphans = Vec::new();
        for (visited, entry) in fs::read_dir(parent)?.enumerate() {
            control.check()?;
            if visited >= MAX_SWEEP_ENTRIES {
                return Err(BackupError::InvalidInput(
                    "backup directory exceeds the 4096-entry cleanup limit",
                ));
            }
            let entry = entry?;
            if entry.file_name().to_str().is_some_and(staging_name) && entry.file_type()?.is_file()
            {
                orphans.push(entry.path());
            }
        }
        for path in &orphans {
            control.check()?;
            fs::remove_file(path)?;
        }
        if !orphans.is_empty() {
            File::open(parent)?.sync_all()?;
        }
        Ok(())
    }
}

fn staging_name(name: &str) -> bool {
    let Some(rest) = name.strip_prefix(STAGING_PREFIX) else {
        return false;
    };
    let base = [".sqlite", ".sqlite-journal", ".sqlite-wal", ".sqlite-shm"]
        .into_iter()
        .find_map(|suffix| rest.strip_suffix(suffix));
    let Some((pid, sequence)) = base.and_then(|base| base.split_once('-')) else {
        return false;
    };
    !pid.is_empty()
        && !sequence.is_empty()
        && pid.bytes().all(|byte| byte.is_ascii_digit())
        && sequence.bytes().all(|byte| byte.is_ascii_digit())
        && pid.parse::<u32>().is_ok()
        && sequence.parse::<u64>().is_ok()
}

fn reserved_name(name: &str) -> bool {
    name.starts_with(STAGING_PREFIX) || name.starts_with(LEASE_NAME)
}

struct Staging {
    path: PathBuf,
}

impl Staging {
    fn create(parent: &Path) -> io::Result<Self> {
        for _ in 0..100 {
            let sequence = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
            let path = parent.join(format!(
                "{STAGING_PREFIX}{}-{sequence}.sqlite",
                std::process::id()
            ));
            match private_new_file(&path) {
                Ok(file) => {
                    drop(file);
                    return Ok(Self { path });
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error),
            }
        }
        Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "backup staging names exhausted",
        ))
    }

    fn cleanup(&self) -> io::Result<()> {
        for suffix in ["", "-journal", "-wal", "-shm"] {
            let mut path = self.path.as_os_str().to_os_string();
            path.push(suffix);
            match fs::remove_file(PathBuf::from(path)) {
                Err(error) if error.kind() != io::ErrorKind::NotFound => return Err(error),
                _ => {}
            }
        }
        Ok(())
    }
}

impl Drop for Staging {
    fn drop(&mut self) {
        let _ = self.cleanup();
    }
}

/// Create a self-contained, checked backup in an existing private directory.
///
/// The source is opened read-only. A same-directory temporary file is published
/// using an exclusive hard link, then the directory is synced. Existing files,
/// directories and symlinks are never replaced. Cancellation wins before the
/// publication point; after publication a completed backup is retained.
/// A directory lease serializes backups and protects bounded cleanup of orphaned
/// version-2 staging. Legacy staging requires manual inspection. The lease must
/// remain in place for the lifetime of this private backup directory.
///
/// Deadline checks and SQLite interrupts bound retries and SQL work. A blocked
/// operating-system filesystem call cannot be preempted by this synchronous API.
pub fn create_backup(
    source: &Path,
    destination: &Path,
    options: BackupOptions,
) -> Result<BackupReport, BackupError> {
    if options.timeout.is_zero()
        || !(1..=MAX_STEP_PAGES).contains(&options.pages_per_step)
        || options.retry_delay.is_zero()
        || options.retry_delay > Duration::from_secs(1)
    {
        return Err(BackupError::InvalidInput(
            "positive timeout, 1..4096 pages per step, and 1ns..1s retry delay required",
        ));
    }
    let control = Arc::new(Control {
        deadline: Instant::now()
            .checked_add(options.timeout)
            .ok_or(BackupError::InvalidInput("timeout is too large"))?,
        cancelled: options
            .cancellation
            .unwrap_or_else(|| Arc::new(AtomicBool::new(false))),
    });
    control.check()?;
    if source
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(reserved_name)
    {
        return Err(BackupError::InvalidInput(
            "source uses a reserved backup staging or lease name",
        ));
    }
    match fs::symlink_metadata(destination) {
        Ok(_) => return Err(BackupError::DestinationExists),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let metadata = fs::symlink_metadata(source)?;
    if !metadata.is_file() || metadata.len() == 0 {
        return Err(BackupError::InvalidInput(
            "source must be a nonempty regular database file, not a symlink",
        ));
    }
    let name = destination
        .file_name()
        .ok_or(BackupError::InvalidInput("destination needs a file name"))?;
    if name.to_str().is_some_and(reserved_name) {
        return Err(BackupError::InvalidInput(
            "destination uses a reserved backup staging or lease name",
        ));
    }
    let parent = destination
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
        .canonicalize()?;
    let destination = parent.join(name);
    let from = Connection::open_with_flags(
        source,
        OpenFlags::SQLITE_OPEN_READ_ONLY
            | OpenFlags::SQLITE_OPEN_NO_MUTEX
            | OpenFlags::SQLITE_OPEN_NOFOLLOW,
    )?;
    from.busy_timeout(Duration::ZERO)?;
    // Declared before staging: every exit drops staging before releasing the lease.
    let lease = DirectoryLease::acquire(&parent, &control)?;
    lease.sweep(&parent, &control)?;
    let staging = Staging::create(&parent)?;
    let mut to = Connection::open_with_flags(
        &staging.path,
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_NO_MUTEX
            | OpenFlags::SQLITE_OPEN_NOFOLLOW,
    )?;
    to.busy_timeout(Duration::ZERO)?;
    to.pragma_update(None, "synchronous", "FULL")?;
    let watchdog = Watchdog::start(
        from.get_interrupt_handle(),
        to.get_interrupt_handle(),
        control.clone(),
    )?;
    let pages = {
        control.check()?;
        // Retain one source snapshot across steps, so concurrent WAL commits cannot
        // restart the copy. Bounded steps preserve cancellation responsiveness.
        from.execute_batch("BEGIN DEFERRED;")
            .map_err(|error| control.sqlite(error))?;
        loop {
            control.check()?;
            match from.query_row("SELECT count(*) FROM sqlite_schema", [], |row| {
                row.get::<_, i64>(0)
            }) {
                Ok(_) => break,
                Err(rusqlite::Error::SqliteFailure(error, _))
                    if matches!(
                        error.code,
                        rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked
                    ) =>
                {
                    control.pause(options.retry_delay)?
                }
                Err(error) => return Err(control.sqlite(error)),
            }
        }
        let backup = Backup::new(&from, &mut to).map_err(|error| control.sqlite(error))?;
        loop {
            control.check()?;
            match backup
                .step(options.pages_per_step as i32)
                .map_err(|error| control.sqlite(error))?
            {
                StepResult::Done => {
                    break u32::try_from(backup.progress().pagecount)
                        .map_err(|_| BackupError::InvalidBackup("invalid page count".into()))?;
                }
                StepResult::Busy | StepResult::Locked => control.pause(options.retry_delay)?,
                StepResult::More => thread::yield_now(),
                _ => {
                    return Err(BackupError::InvalidBackup(
                        "unknown SQLite backup state".into(),
                    ));
                }
            }
        }
    };
    from.execute_batch("ROLLBACK;")
        .map_err(|error| control.sqlite(error))?;
    control.check()?;
    let mode: String = to
        .pragma_update_and_check(None, "journal_mode", "DELETE", |row| row.get(0))
        .map_err(|error| control.sqlite(error))?;
    if !mode.eq_ignore_ascii_case("delete") {
        return Err(BackupError::InvalidBackup(
            "backup is not self-contained".into(),
        ));
    }
    control.check()?;
    let check: String = to
        .query_row("PRAGMA quick_check", [], |row| row.get(0))
        .map_err(|error| control.sqlite(error))?;
    if check != "ok" {
        return Err(BackupError::InvalidBackup(check));
    }
    control.check()?;
    drop(watchdog);
    to.close()
        .map_err(|(_, error)| BackupError::Sqlite(error))?;
    drop(from);
    File::open(&staging.path)?.sync_all()?;
    let bytes = fs::metadata(&staging.path)?.len();
    control.check()?;
    fs::hard_link(&staging.path, &destination).map_err(|error| {
        if error.kind() == io::ErrorKind::AlreadyExists {
            BackupError::DestinationExists
        } else {
            BackupError::Io(error)
        }
    })?;
    staging
        .cleanup()
        .and_then(|()| File::open(&parent)?.sync_all())
        .map_err(BackupError::PublishedButNotSynced)?;
    Ok(BackupReport { pages, bytes })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn watchdog_interrupts_long_sql_validation_at_the_deadline() {
        let source = Connection::open_in_memory().unwrap();
        let destination = Connection::open_in_memory().unwrap();
        let started = Instant::now();
        let control = Arc::new(Control {
            deadline: started + Duration::from_millis(25),
            cancelled: Arc::new(AtomicBool::new(false)),
        });
        let watchdog = Watchdog::start(
            source.get_interrupt_handle(),
            destination.get_interrupt_handle(),
            control.clone(),
        )
        .unwrap();
        let result = destination.query_row::<i64, _, _>(
            "WITH RECURSIVE n(v) AS (VALUES(1) UNION ALL SELECT v+1 FROM n WHERE v<100000000) SELECT sum(v) FROM n",
            [],
            |row| row.get(0),
        );
        assert!(matches!(
            control.sqlite(result.unwrap_err()),
            BackupError::Deadline
        ));
        drop(watchdog);
        assert!(started.elapsed() < Duration::from_secs(1));
        let value: i64 = destination
            .query_row("SELECT 1", [], |row| row.get(0))
            .unwrap();
        assert_eq!(value, 1, "watchdog must stop interrupting when joined");
    }
}
