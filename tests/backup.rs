use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use rusqlite::{Connection, OpenFlags};
use seeded_archive::backup::{BackupError, BackupOptions, create_backup};

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

struct TestFiles {
    directory: PathBuf,
    source: PathBuf,
    destination: PathBuf,
}

impl TestFiles {
    fn new() -> Self {
        let parent = Path::new(env!("CARGO_MANIFEST_DIR")).join("tmp");
        fs::create_dir_all(&parent).unwrap();
        loop {
            let sequence = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
            let directory = parent.join(format!("backup-test-{}-{sequence}", std::process::id()));
            match fs::create_dir(&directory) {
                Ok(()) => {
                    return Self {
                        source: directory.join("source.sqlite"),
                        destination: directory.join("backup.sqlite"),
                        directory,
                    };
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => panic!("create test directory: {error}"),
            }
        }
    }

    fn source(&self, journal: &str) -> Connection {
        let connection = Connection::open(&self.source).unwrap();
        connection
            .pragma_update(None, "journal_mode", journal)
            .unwrap();
        connection
            .execute_batch(
                "CREATE TABLE records(id INTEGER PRIMARY KEY, version INTEGER NOT NULL, data BLOB);\
             WITH RECURSIVE n(v) AS (VALUES(1) UNION ALL SELECT v+1 FROM n WHERE v<1200)\
             INSERT INTO records SELECT v, 0, zeroblob(128) FROM n;",
            )
            .unwrap();
        connection
    }

    fn options(&self) -> BackupOptions {
        BackupOptions {
            timeout: Duration::from_secs(3),
            pages_per_step: 4,
            retry_delay: Duration::from_millis(5),
            ..BackupOptions::default()
        }
    }

    fn staging_count(&self) -> usize {
        fs::read_dir(&self.directory)
            .unwrap()
            .map(|entry| entry.unwrap())
            .filter(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".seeded-backup-")
            })
            .count()
    }
}

impl Drop for TestFiles {
    fn drop(&mut self) {
        if let Err(error) = fs::remove_dir_all(&self.directory) {
            if std::thread::panicking() {
                eprintln!("backup test cleanup failed: {error}");
            } else {
                panic!("backup test cleanup failed: {error}");
            }
        }
    }
}

fn snapshot(path: &Path) -> (i64, i64, i64) {
    let connection = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let check: String = connection
        .query_row("PRAGMA quick_check", [], |row| row.get(0))
        .unwrap();
    assert_eq!(check, "ok");
    connection
        .query_row(
            "SELECT count(*), min(version), max(version) FROM records",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap()
}

#[test]
fn backup_reopens_without_wal_sidecars_and_uses_private_permissions() {
    let files = TestFiles::new();
    let source = files.source("WAL");
    source.execute("UPDATE records SET version=7", []).unwrap();
    let report = create_backup(&files.source, &files.destination, files.options()).unwrap();
    assert!(report.pages > 0 && report.bytes > 0);
    assert_eq!(
        report.bytes,
        fs::metadata(&files.destination).unwrap().len()
    );
    assert_eq!(snapshot(&files.destination), (1200, 7, 7));
    assert_eq!(files.staging_count(), 0);
    assert!(
        !files
            .destination
            .with_file_name("backup.sqlite-wal")
            .exists()
    );
    assert!(
        !files
            .destination
            .with_file_name("backup.sqlite-shm")
            .exists()
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            fs::metadata(&files.destination)
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
    assert_eq!(snapshot(&files.source), (1200, 7, 7));
}

#[test]
fn backup_keeps_a_consistent_snapshot_while_a_wal_writer_is_active() {
    let files = TestFiles::new();
    drop(files.source("WAL"));
    thread::scope(|scope| {
        let (started, ready) = mpsc::channel();
        let source_path = files.source.clone();
        scope.spawn(move || {
            let writer = Connection::open(&source_path).unwrap();
            writer
                .execute_batch("BEGIN IMMEDIATE; UPDATE records SET version=9;")
                .unwrap();
            started.send(()).unwrap();
            thread::sleep(Duration::from_millis(100));
            writer.execute_batch("COMMIT;").unwrap();
        });
        ready.recv_timeout(Duration::from_secs(1)).unwrap();
        create_backup(&files.source, &files.destination, files.options()).unwrap();
        let (count, minimum, maximum) = snapshot(&files.destination);
        assert_eq!(count, 1200);
        assert_eq!(minimum, maximum);
        assert!([0, 9].contains(&minimum));
    });
    assert_eq!(snapshot(&files.source), (1200, 9, 9));
    assert_eq!(files.staging_count(), 0);
}

#[test]
fn a_busy_source_is_retried_and_can_complete_after_unlock() {
    let files = TestFiles::new();
    drop(files.source("DELETE"));
    thread::scope(|scope| {
        let (started, ready) = mpsc::channel();
        let source_path = files.source.clone();
        scope.spawn(move || {
            let writer = Connection::open(&source_path).unwrap();
            writer
                .execute_batch("BEGIN EXCLUSIVE; UPDATE records SET version=4;")
                .unwrap();
            started.send(()).unwrap();
            thread::sleep(Duration::from_millis(80));
            writer.execute_batch("COMMIT;").unwrap();
        });
        ready.recv_timeout(Duration::from_secs(1)).unwrap();
        create_backup(&files.source, &files.destination, files.options()).unwrap();
    });
    assert_eq!(snapshot(&files.destination), (1200, 4, 4));
    assert_eq!(files.staging_count(), 0);
}

#[test]
fn deadline_on_a_busy_source_cleans_staging_and_allows_a_later_retry() {
    let files = TestFiles::new();
    let blocker = files.source("DELETE");
    blocker.execute_batch("BEGIN EXCLUSIVE;").unwrap();
    let started = Instant::now();
    let result = create_backup(
        &files.source,
        &files.destination,
        BackupOptions {
            timeout: Duration::from_millis(30),
            ..files.options()
        },
    );
    blocker.execute_batch("ROLLBACK;").unwrap();
    assert!(matches!(result, Err(BackupError::Deadline)), "{result:?}");
    assert!(started.elapsed() < Duration::from_secs(1));
    assert!(!files.destination.exists());
    assert_eq!(files.staging_count(), 0);
    create_backup(&files.source, &files.destination, files.options()).unwrap();
    assert_eq!(snapshot(&files.destination), (1200, 0, 0));
}

#[test]
fn cancellation_during_a_busy_backup_cleans_staging_without_publishing() {
    let files = TestFiles::new();
    let blocker = files.source("DELETE");
    blocker.execute_batch("BEGIN EXCLUSIVE;").unwrap();
    let cancelled = Arc::new(AtomicBool::new(false));
    thread::scope(|scope| {
        scope.spawn(|| {
            thread::sleep(Duration::from_millis(30));
            cancelled.store(true, Ordering::Release);
        });
        let result = create_backup(
            &files.source,
            &files.destination,
            BackupOptions {
                cancellation: Some(cancelled.clone()),
                ..files.options()
            },
        );
        assert!(matches!(result, Err(BackupError::Cancelled)), "{result:?}");
    });
    blocker.execute_batch("ROLLBACK;").unwrap();
    assert!(!files.destination.exists());
    assert_eq!(files.staging_count(), 0);
}

#[test]
fn cancellation_before_work_never_creates_a_destination_or_staging_file() {
    let files = TestFiles::new();
    drop(files.source("WAL"));
    let result = create_backup(
        &files.source,
        &files.destination,
        BackupOptions {
            cancellation: Some(Arc::new(AtomicBool::new(true))),
            ..files.options()
        },
    );
    assert!(matches!(result, Err(BackupError::Cancelled)));
    assert!(!files.destination.exists());
    assert_eq!(files.staging_count(), 0);
}

#[test]
fn an_existing_destination_is_never_replaced() {
    let files = TestFiles::new();
    drop(files.source("WAL"));
    fs::write(&files.destination, b"keep original").unwrap();
    assert!(matches!(
        create_backup(&files.source, &files.destination, files.options()),
        Err(BackupError::DestinationExists)
    ));
    assert_eq!(fs::read(&files.destination).unwrap(), b"keep original");
    assert_eq!(files.staging_count(), 0);
}

#[test]
fn a_destination_created_during_backup_wins_the_publication_race() {
    let files = TestFiles::new();
    let blocker = files.source("DELETE");
    blocker.execute_batch("BEGIN EXCLUSIVE;").unwrap();
    thread::scope(|scope| {
        let backup =
            scope.spawn(|| create_backup(&files.source, &files.destination, files.options()));
        let deadline = Instant::now() + Duration::from_secs(1);
        while files.staging_count() == 0 && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(2));
        }
        fs::write(&files.destination, b"created concurrently").unwrap();
        blocker.execute_batch("ROLLBACK;").unwrap();
        let result = backup.join().unwrap();
        assert!(
            matches!(result, Err(BackupError::DestinationExists)),
            "{result:?}"
        );
    });
    assert_eq!(
        fs::read(&files.destination).unwrap(),
        b"created concurrently"
    );
    assert_eq!(files.staging_count(), 0);
}

#[cfg(unix)]
#[test]
fn destination_symlinks_are_never_followed_even_if_the_target_is_missing() {
    use std::os::unix::fs::symlink;
    let files = TestFiles::new();
    drop(files.source("WAL"));
    let missing = files.directory.join("missing.sqlite");
    symlink(&missing, &files.destination).unwrap();
    assert!(matches!(
        create_backup(&files.source, &files.destination, files.options()),
        Err(BackupError::DestinationExists)
    ));
    assert!(files.destination.is_symlink());
    assert!(!missing.exists());
}

#[test]
fn corrupt_or_missing_sources_do_not_publish_or_create_a_source() {
    let files = TestFiles::new();
    assert!(create_backup(&files.source, &files.destination, files.options()).is_err());
    assert!(!files.source.exists());
    fs::write(
        &files.source,
        b"This is a deliberately invalid SQLite input.",
    )
    .unwrap();
    assert!(create_backup(&files.source, &files.destination, files.options()).is_err());
    assert!(!files.destination.exists());
    assert_eq!(files.staging_count(), 0);
}

#[test]
fn invalid_resource_bounds_are_rejected_before_creating_files() {
    let files = TestFiles::new();
    for options in [
        BackupOptions {
            pages_per_step: 0,
            ..files.options()
        },
        BackupOptions {
            pages_per_step: 4097,
            ..files.options()
        },
        BackupOptions {
            timeout: Duration::ZERO,
            ..files.options()
        },
        BackupOptions {
            retry_delay: Duration::from_secs(2),
            ..files.options()
        },
    ] {
        assert!(matches!(
            create_backup(&files.source, &files.destination, options),
            Err(BackupError::InvalidInput(_))
        ));
    }
    assert!(!files.source.exists());
    assert_eq!(files.staging_count(), 0);
}

#[test]
fn backup_finishes_while_wal_commits_continue_without_restarting_the_copy() {
    let files = TestFiles::new();
    let source = files.source("WAL");
    source
        .execute("UPDATE records SET data=zeroblob(4096)", [])
        .unwrap();
    let done = Arc::new(AtomicBool::new(false));
    thread::scope(|scope| {
        let stop = done.clone();
        let path = files.source.clone();
        let (started, ready) = mpsc::channel();
        let writer = scope.spawn(move || {
            let c = Connection::open(path).unwrap();
            c.execute("UPDATE records SET version=1 WHERE id=1", [])
                .unwrap();
            started.send(()).unwrap();
            let mut commits = 1;
            while !stop.load(Ordering::Acquire) {
                c.execute("UPDATE records SET version=version+1 WHERE id=1", [])
                    .unwrap();
                commits += 1;
            }
            commits
        });
        ready.recv_timeout(Duration::from_secs(1)).unwrap();
        let result = create_backup(
            &files.source,
            &files.destination,
            BackupOptions {
                pages_per_step: 1,
                ..files.options()
            },
        );
        done.store(true, Ordering::Release);
        assert!(writer.join().unwrap() > 1);
        result.unwrap();
    });
    assert_eq!(snapshot(&files.destination).0, 1200);
    assert_eq!(files.staging_count(), 0);
}

struct BackupChild(Child);

impl BackupChild {
    fn start(files: &TestFiles) -> Self {
        Self(
            Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "backup_subprocess_for_crash_test", "--nocapture"])
                .env("SEEDED_BACKUP_TEST_DIRECTORY", &files.directory)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap(),
        )
    }

    fn kill_and_wait(&mut self) {
        self.0.kill().unwrap();
        assert!(!self.0.wait().unwrap().success());
    }
}

impl Drop for BackupChild {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn wait_for_staging(files: &TestFiles) -> PathBuf {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        if let Some(path) = fs::read_dir(&files.directory)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| {
                path.file_name().unwrap().to_str().is_some_and(|name| {
                    name.starts_with(".seeded-backup-v2-") && name.ends_with(".sqlite")
                })
            })
        {
            return path;
        }
        assert!(Instant::now() < deadline, "backup did not create staging");
        thread::sleep(Duration::from_millis(2));
    }
}

#[test]
fn backup_subprocess_for_crash_test() {
    let Some(directory) = std::env::var_os("SEEDED_BACKUP_TEST_DIRECTORY") else {
        return;
    };
    let directory = PathBuf::from(directory);
    create_backup(
        &directory.join("source.sqlite"),
        &directory.join("child-backup.sqlite"),
        BackupOptions {
            timeout: Duration::from_secs(30),
            ..BackupOptions::default()
        },
    )
    .unwrap();
}

#[test]
fn killed_backup_is_reaped_on_restart_after_its_directory_lease_is_released() {
    let files = TestFiles::new();
    let blocker = files.source("DELETE");
    blocker.execute_batch("BEGIN EXCLUSIVE;").unwrap();
    let mut child = BackupChild::start(&files);
    let orphan = wait_for_staging(&files);
    child.kill_and_wait();
    assert!(orphan.is_file(), "SIGKILL bypasses RAII cleanup");
    assert!(!files.directory.join("child-backup.sqlite").exists());
    blocker.execute_batch("ROLLBACK;").unwrap();
    create_backup(&files.source, &files.destination, files.options()).unwrap();
    assert!(!orphan.exists());
    assert_eq!(files.staging_count(), 0);
    assert_eq!(snapshot(&files.destination), (1200, 0, 0));
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            fs::metadata(files.directory.join(".seeded-backup.lock"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
}

#[test]
fn waiting_backup_deadline_and_cancellation_never_remove_another_live_staging_file() {
    let files = TestFiles::new();
    let blocker = files.source("DELETE");
    blocker.execute_batch("BEGIN EXCLUSIVE;").unwrap();
    let mut child = BackupChild::start(&files);
    let active = wait_for_staging(&files);
    let result = create_backup(
        &files.source,
        &files.destination,
        BackupOptions {
            timeout: Duration::from_millis(40),
            ..files.options()
        },
    );
    assert!(matches!(result, Err(BackupError::Deadline)), "{result:?}");
    assert!(active.exists());
    assert_eq!(files.staging_count(), 1);
    let cancelled = Arc::new(AtomicBool::new(false));
    thread::scope(|scope| {
        scope.spawn(|| {
            thread::sleep(Duration::from_millis(30));
            cancelled.store(true, Ordering::Release);
        });
        let result = create_backup(
            &files.source,
            &files.destination,
            BackupOptions {
                cancellation: Some(cancelled.clone()),
                ..files.options()
            },
        );
        assert!(matches!(result, Err(BackupError::Cancelled)), "{result:?}");
    });
    assert!(active.exists());
    assert_eq!(files.staging_count(), 1);
    blocker.execute_batch("ROLLBACK;").unwrap();
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        if let Some(status) = child.0.try_wait().unwrap() {
            assert!(status.success());
            break;
        }
        assert!(Instant::now() < deadline);
        thread::sleep(Duration::from_millis(2));
    }
    assert_eq!(
        snapshot(&files.directory.join("child-backup.sqlite")),
        (1200, 0, 0)
    );
    create_backup(&files.source, &files.destination, files.options()).unwrap();
    assert_eq!(files.staging_count(), 0);
}

#[test]
fn orphan_cleanup_preserves_published_hard_links_and_legacy_staging() {
    let files = TestFiles::new();
    let source = files.source("WAL");
    source.execute("UPDATE records SET version=7", []).unwrap();
    let published = files.directory.join("published.sqlite");
    create_backup(&files.source, &published, files.options()).unwrap();
    let orphan = files.directory.join(".seeded-backup-v2-123-456.sqlite");
    fs::hard_link(&published, &orphan).unwrap();
    for suffix in ["-journal", "-wal", "-shm"] {
        fs::write(
            files
                .directory
                .join(format!(".seeded-backup-v2-123-456.sqlite{suffix}")),
            b"orphan",
        )
        .unwrap();
    }
    let legacy = files.directory.join(".seeded-backup-123-456.sqlite");
    fs::write(&legacy, b"legacy requires manual inspection").unwrap();
    source.execute("UPDATE records SET version=8", []).unwrap();
    create_backup(&files.source, &files.destination, files.options()).unwrap();
    assert!(!orphan.exists());
    for suffix in ["-journal", "-wal", "-shm"] {
        assert!(
            !files
                .directory
                .join(format!(".seeded-backup-v2-123-456.sqlite{suffix}"))
                .exists()
        );
    }
    assert_eq!(
        fs::read(&legacy).unwrap(),
        b"legacy requires manual inspection"
    );
    assert_eq!(snapshot(&published), (1200, 7, 7));
    assert_eq!(snapshot(&files.destination), (1200, 8, 8));
}

#[cfg(unix)]
#[test]
fn cleanup_does_not_follow_symlinks_or_recurse_into_directories() {
    use std::os::unix::fs::symlink;
    let files = TestFiles::new();
    drop(files.source("WAL"));
    let target = files.directory.join("keep.sqlite");
    fs::write(&target, b"keep").unwrap();
    let link = files.directory.join(".seeded-backup-v2-123-1.sqlite");
    symlink(&target, &link).unwrap();
    let directory = files.directory.join(".seeded-backup-v2-123-2.sqlite");
    fs::create_dir(&directory).unwrap();
    fs::write(directory.join("keep"), b"inside").unwrap();
    let unrelated = files.directory.join(".seeded-backup-v2-custom.sqlite");
    fs::write(&unrelated, b"not a staging name").unwrap();
    create_backup(&files.source, &files.destination, files.options()).unwrap();
    assert!(link.is_symlink());
    assert_eq!(fs::read(&target).unwrap(), b"keep");
    assert_eq!(fs::read(directory.join("keep")).unwrap(), b"inside");
    assert_eq!(fs::read(&unrelated).unwrap(), b"not a staging name");
}

#[cfg(unix)]
#[test]
fn a_symlink_at_the_directory_lease_is_rejected_without_touching_its_target() {
    use std::os::unix::fs::symlink;
    let files = TestFiles::new();
    drop(files.source("WAL"));
    let target = files.directory.join("keep.sqlite");
    fs::write(&target, b"keep").unwrap();
    symlink(&target, files.directory.join(".seeded-backup.lock")).unwrap();
    assert!(matches!(
        create_backup(&files.source, &files.destination, files.options()),
        Err(BackupError::InvalidInput(_))
    ));
    assert_eq!(fs::read(&target).unwrap(), b"keep");
    assert!(!files.destination.exists());
    assert_eq!(files.staging_count(), 0);
}

#[test]
fn cleanup_scan_exhaustion_preserves_existing_files_and_creates_no_backup() {
    let files = TestFiles::new();
    drop(files.source("WAL"));
    let orphan = files.directory.join(".seeded-backup-v2-123-456.sqlite");
    fs::write(&orphan, b"untouched until scan completes").unwrap();
    for i in 0..4096 {
        fs::write(files.directory.join(format!("keep-{i}")), b"keep").unwrap();
    }
    let result = create_backup(&files.source, &files.destination, files.options());
    assert!(
        matches!(result, Err(BackupError::InvalidInput(_))),
        "{result:?}"
    );
    assert_eq!(
        fs::read(&orphan).unwrap(),
        b"untouched until scan completes"
    );
    assert!(!files.destination.exists());
    assert_eq!(
        fs::read(files.directory.join("keep-4095")).unwrap(),
        b"keep"
    );
}

#[test]
fn published_destinations_cannot_use_reserved_cleanup_or_lease_names() {
    let files = TestFiles::new();
    drop(files.source("WAL"));
    for name in [
        ".seeded-backup-v2-123-456.sqlite",
        ".seeded-backup.lock",
        ".seeded-backup.lock-journal",
    ] {
        let destination = files.directory.join(name);
        assert!(matches!(
            create_backup(&files.source, &destination, files.options()),
            Err(BackupError::InvalidInput(_))
        ));
        assert!(!destination.exists());
    }
    assert_eq!(files.staging_count(), 0);
}

#[test]
fn reserved_sqlite_sources_are_rejected_before_any_cleanup_or_file_changes() {
    let files = TestFiles::new();
    drop(files.source("DELETE"));
    let original = fs::read(&files.source).unwrap();
    let orphan = files.directory.join(".seeded-backup-v2-987-654.sqlite");
    fs::write(&orphan, b"must not be swept on refusal").unwrap();
    for name in [
        ".seeded-backup-v2-123-456.sqlite",
        ".seeded-backup.lock",
        ".seeded-backup.lock-journal",
    ] {
        let source = files.directory.join(name);
        fs::copy(&files.source, &source).unwrap();
        assert_eq!(snapshot(&source), (1200, 0, 0));
        let entries = fs::read_dir(&files.directory).unwrap().count();
        let result = create_backup(&source, &files.destination, files.options());
        assert!(
            matches!(result, Err(BackupError::InvalidInput(_))),
            "{result:?}"
        );
        assert_eq!(fs::read(&source).unwrap(), original);
        assert_eq!(fs::read(&orphan).unwrap(), b"must not be swept on refusal");
        assert_eq!(fs::read_dir(&files.directory).unwrap().count(), entries);
        assert!(!files.destination.exists());
    }
    assert_eq!(fs::read(&files.source).unwrap(), original);
}

#[test]
fn same_process_backup_connections_cannot_sweep_each_others_live_staging() {
    let files = TestFiles::new();
    let blocker = files.source("DELETE");
    blocker.execute_batch("BEGIN EXCLUSIVE;").unwrap();
    let first_destination = files.directory.join("first.sqlite");
    thread::scope(|scope| {
        let first =
            scope.spawn(|| create_backup(&files.source, &first_destination, files.options()));
        let active = wait_for_staging(&files);
        let result = create_backup(
            &files.source,
            &files.destination,
            BackupOptions {
                timeout: Duration::from_millis(40),
                ..files.options()
            },
        );
        assert!(matches!(result, Err(BackupError::Deadline)), "{result:?}");
        assert!(
            active.exists(),
            "second connection must not unlink live staging"
        );
        assert_eq!(files.staging_count(), 1);
        assert!(!files.destination.exists());
        blocker.execute_batch("ROLLBACK;").unwrap();
        first.join().unwrap().unwrap();
    });
    assert_eq!(snapshot(&first_destination), (1200, 0, 0));
    assert_eq!(files.staging_count(), 0);
    create_backup(&files.source, &files.destination, files.options()).unwrap();
    assert_eq!(snapshot(&files.destination), (1200, 0, 0));
}
