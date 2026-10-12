//! Known content roots held in memory as 16-byte BLAKE2b digests, so artwork requests for
//! them skip the database. Only positive answers come from memory: an unknown root still asks
//! the database, so a newly added root is served at once. A changed hidden list withdraws the
//! set before it is reloaded; a snapshot-only change (syncs only add roots) keeps serving it
//! during the reload. After any commit to the archive (SQLite `data_version`) the set waits for
//! a fingerprint check, so hiding a project applies on the next request. A failed, timed-out or
//! stale set is never used, nor one loaded from a file that has since gone or been replaced.
use std::{
    path::PathBuf,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use blake2::{Blake2b, Digest, digest::consts::U16};
use rusqlite::{Connection, OpenFlags};

use super::super::database::{FileId, file_id};
use crate::store::{Store, StoreError};

type Key = [u8; 16];
/// The snapshot row and the hidden project ids.
type Fingerprint = (String, String);
/// The archive file, which watch connection (a fresh connection restarts `data_version`), and
/// its `data_version`, which changes on every commit by another connection.
type Stamp = (FileId, u64, i64);

/// A used set is revalidated by the first request this long after the last check, and not
/// used once this old without a confirmation.
const CHECK_EVERY: Duration = Duration::from_secs(10);
const MAX_AGE: Duration = Duration::from_secs(30);
const LOAD_TIMEOUT: Duration = Duration::from_secs(120);

pub(super) struct RootIndex {
    database: Arc<PathBuf>,
    state: Mutex<State>,
    watch: Mutex<Option<(FileId, u64, Connection)>>,
    opened: std::sync::atomic::AtomicU64,
    check_every: Duration,
    max_age: Duration,
    load_timeout: Duration,
    #[cfg(test)]
    hooks: TestHooks,
}

#[cfg(test)]
#[derive(Default)]
struct TestHooks {
    loads: std::sync::atomic::AtomicUsize,
    finished: std::sync::atomic::AtomicUsize,
    rows: std::sync::atomic::AtomicUsize,
    /// Sleeps before the fingerprint read, and between it and the root scan.
    before: Mutex<Duration>,
    reload: Mutex<Duration>,
}

#[derive(Default)]
struct State {
    roots: Option<Arc<Vec<Key>>>,
    fingerprint: Option<Fingerprint>,
    /// The stamp the set was confirmed at, and the one seen when a refresh last started.
    stamp: Option<Stamp>,
    attempted_stamp: Option<Stamp>,
    /// When the set's data was last read and found current, and when a refresh last started.
    confirmed: Option<Instant>,
    attempted: Option<Instant>,
    refreshing: bool,
    generation: u64,
}

enum Loaded {
    Unchanged,
    Changed(Fingerprint, Vec<Key>),
}

fn key(scheme: &str, canonical: &str) -> Key {
    let mut hash = Blake2b::<U16>::new();
    hash.update(scheme.as_bytes());
    hash.update([0]);
    hash.update(canonical.as_bytes());
    hash.finalize().into()
}

impl RootIndex {
    pub fn new(database: Arc<PathBuf>) -> Arc<Self> {
        Self::with_timing(database, CHECK_EVERY, MAX_AGE, LOAD_TIMEOUT)
    }

    fn with_timing(
        database: Arc<PathBuf>,
        check_every: Duration,
        max_age: Duration,
        load_timeout: Duration,
    ) -> Arc<Self> {
        Arc::new(Self {
            database,
            state: Mutex::new(State::default()),
            watch: Mutex::new(None),
            opened: Default::default(),
            check_every,
            max_age,
            load_timeout,
            #[cfg(test)]
            hooks: TestHooks::default(),
        })
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// None when the file is missing or unreadable. About 3 µs; one page read after a commit.
    fn stamp(&self) -> Option<Stamp> {
        let file = file_id(&self.database).ok()?;
        let mut watch = self.watch.lock().unwrap_or_else(|e| e.into_inner());
        if watch
            .as_ref()
            .is_none_or(|(watched, _, _)| *watched != file)
        {
            *watch = None;
            let flags = OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX;
            let conn = Connection::open_with_flags(&*self.database, flags).ok()?;
            // Never wait on a lock: a busy check answers from the database instead.
            conn.busy_timeout(Duration::ZERO).ok()?;
            let epoch = self.opened.fetch_add(1, Ordering::Relaxed);
            *watch = Some((file.clone(), epoch, conn));
        }
        let version = watch.as_ref().map(|(_, epoch, conn)| {
            conn.prepare_cached("PRAGMA data_version")
                .and_then(|mut st| st.query_row([], |r| r.get::<_, i64>(0)))
                .map(|version| (*epoch, version))
        });
        match version {
            Some(Ok((epoch, version))) => Some((file, epoch, version)),
            _ => {
                *watch = None;
                None
            }
        }
    }

    /// True only when a current set holds the canonical root; false means ask the database.
    /// Starts a refresh when the last one is due; must run inside the Tokio runtime.
    pub fn known(self: &Arc<Self>, scheme: &str, canonical: &str) -> bool {
        let now = Instant::now();
        let stamp = self.stamp();
        let (roots, start) = {
            let mut state = self.lock();
            let due = state
                .attempted
                .is_none_or(|t| now.saturating_duration_since(t) >= self.check_every)
                || state.attempted_stamp != stamp;
            let start = (!state.refreshing && due).then(|| {
                state.refreshing = true;
                state.attempted = Some(now);
                state.attempted_stamp = stamp.clone();
                state.generation += 1;
                // Another file is reloaded in full, whatever its fingerprint.
                let same_file = state.stamp.as_ref().map(|s| &s.0) == stamp.as_ref().map(|s| &s.0);
                let current = state.fingerprint.clone().filter(|_| same_file);
                (state.generation, current, stamp.clone())
            });
            let fresh = stamp.is_some()
                && state.stamp == stamp
                && state
                    .confirmed
                    .is_some_and(|t| now.saturating_duration_since(t) < self.max_age);
            (state.roots.clone().filter(|_| fresh), start)
        };
        if let Some((generation, fingerprint, stamp)) = start {
            self.refresh(generation, fingerprint, stamp);
        }
        roots.is_some_and(|roots| roots.binary_search(&key(scheme, canonical)).is_ok())
    }

    /// `stamp` was read before the load's transaction, so a commit during the load leaves the
    /// result unconfirmed for the next check.
    fn refresh(
        self: &Arc<Self>,
        generation: u64,
        current: Option<Fingerprint>,
        stamp: Option<Stamp>,
    ) {
        let index = self.clone();
        tokio::spawn(async move {
            let loader = index.clone();
            let abandoned = Arc::new(AtomicBool::new(false));
            let stop = abandoned.clone();
            let at = stamp.clone();
            let work =
                tokio::task::spawn_blocking(move || loader.load(generation, current, at, &stop));
            let outcome = tokio::time::timeout(index.load_timeout, work).await;
            if outcome.is_err() {
                // The scan stops at its next row; its result is dropped with the handle.
                abandoned.store(true, Ordering::Relaxed);
            }
            let mut state = index.lock();
            if state.generation != generation {
                return;
            }
            state.refreshing = false;
            match outcome {
                Ok(Ok(Ok((Loaded::Unchanged, read_at)))) => {
                    state.stamp = stamp;
                    state.confirmed = Some(read_at);
                }
                Ok(Ok(Ok((Loaded::Changed(fingerprint, roots), read_at)))) => {
                    state.roots = Some(Arc::new(roots));
                    state.fingerprint = Some(fingerprint);
                    state.stamp = stamp;
                    state.confirmed = Some(read_at);
                }
                // Failure, panic or timeout: the database answers until a later load succeeds.
                _ => {
                    state.roots = None;
                    state.fingerprint = None;
                    state.stamp = None;
                    state.confirmed = None;
                }
            }
        });
    }

    fn load(
        &self,
        generation: u64,
        current: Option<Fingerprint>,
        stamp: Option<Stamp>,
        abandoned: &AtomicBool,
    ) -> Result<(Loaded, Instant), StoreError> {
        #[cfg(test)]
        let _finished = {
            struct Finished<'a>(&'a std::sync::atomic::AtomicUsize);
            impl Drop for Finished<'_> {
                fn drop(&mut self) {
                    self.0.fetch_add(1, Ordering::Relaxed);
                }
            }
            self.hooks.loads.fetch_add(1, Ordering::Relaxed);
            std::thread::sleep(*self.hooks.before.lock().unwrap());
            Finished(&self.hooks.finished)
        };
        let store = Store::open_read_only(&self.database)?;
        // Taken before the transaction's first read, so the age never understates.
        let read_at = Instant::now();
        store.read_snapshot(|store| {
            let fingerprint = store.content_root_fingerprint()?;
            if current.as_ref() == Some(&fingerprint) {
                return Ok((Loaded::Unchanged, read_at));
            }
            // A changed hidden list stops the old set now; the reload follows. With the same
            // hidden list the old set is still a subset of the truth, so it stays confirmed.
            {
                let mut state = self.lock();
                if state.generation == generation {
                    if current.as_ref().is_none_or(|c| c.1 != fingerprint.1) {
                        state.roots = None;
                        state.fingerprint = None;
                        state.stamp = None;
                        state.confirmed = None;
                    } else {
                        state.stamp = stamp;
                        state.confirmed = Some(read_at);
                    }
                }
            }
            #[cfg(test)]
            std::thread::sleep(*self.hooks.reload.lock().unwrap());
            let mut roots = Vec::new();
            store.each_visible_content_root(|scheme, cid| {
                if abandoned.load(Ordering::Relaxed) {
                    return Err(StoreError::Unavailable("root index load abandoned".into()));
                }
                #[cfg(test)]
                self.hooks.rows.fetch_add(1, Ordering::Relaxed);
                roots.push(key(scheme, cid));
                Ok(())
            })?;
            roots.sort_unstable();
            roots.dedup();
            Ok((Loaded::Changed(fingerprint, roots), read_at))
        })
    }

    /// As if the check interval and maximum age had passed.
    #[cfg(test)]
    pub fn expire(&self) {
        let mut state = self.lock();
        state.attempted = None;
        state.confirmed = None;
    }

    /// Waits until no refresh is running.
    #[cfg(test)]
    pub async fn settle(&self) {
        for _ in 0..2_000 {
            if !self.lock().refreshing {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("root index refresh did not finish");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Fixture(PathBuf);
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    const SHOWN: &str = "bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy";
    const HIDDEN: &str = "bafkreidivzimqfqtoqxkrpge6bjyhlvxqs3rhe73owtmdulaxr5do5in7u";

    fn fixture() -> (Fixture, Arc<PathBuf>) {
        use std::sync::atomic::AtomicU64;
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        let dir = Fixture(root.join(format!(
            "tmp/root-index-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        )));
        std::fs::create_dir_all(&dir.0).unwrap();
        let path = dir.0.join("archive.sqlite");
        let mut store = Store::open(&path).unwrap();
        crate::import::import_fixtures(&mut store, &root.join("assets/fixtures")).unwrap();
        drop(store);
        let c = writer(&path);
        let hidden = other_project(&c, 0);
        c.execute("UPDATE projects SET hidden = 1 WHERE id = ?1", [hidden])
            .unwrap();
        c.execute(
            "INSERT INTO content_roots (scheme, cid, project_id) VALUES ('ipfs', ?1, 29437), \
             ('ipfs', ?2, ?3)",
            rusqlite::params![SHOWN, HIDDEN, hidden],
        )
        .unwrap();
        (dir, Arc::new(path))
    }

    fn other_project(c: &rusqlite::Connection, n: i64) -> i64 {
        c.query_row(
            "SELECT id FROM projects WHERE id <> 29437 ORDER BY id LIMIT 1 OFFSET ?1",
            [n],
            |r| r.get(0),
        )
        .unwrap()
    }

    fn writer(path: &std::path::Path) -> rusqlite::Connection {
        rusqlite::Connection::open(path).unwrap()
    }

    fn checking(path: Arc<PathBuf>) -> Arc<RootIndex> {
        RootIndex::with_timing(path, Duration::ZERO, Duration::from_secs(30), LOAD_TIMEOUT)
    }

    async fn loaded(index: &Arc<RootIndex>) {
        index.known("ipfs", SHOWN);
        index.settle().await;
        assert!(index.lock().roots.is_some());
    }

    #[tokio::test]
    async fn known_roots_come_from_memory_and_unknown_or_hidden_ones_do_not() {
        let (_dir, path) = fixture();
        let index = RootIndex::new(path);
        assert!(!index.known("ipfs", SHOWN), "nothing is loaded yet");
        index.settle().await;
        assert!(index.known("ipfs", SHOWN));
        assert!(
            !index.known("onchfs", SHOWN),
            "the scheme is part of the key"
        );
        assert!(!index.known("ipfs", HIDDEN));
        assert!(!index.known("ipfs", "bafkreiunknown"));
        assert_eq!(index.hooks.loads.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn unchanged_data_is_revalidated_without_reloading_and_hiding_withdraws_a_root() {
        let (_dir, path) = fixture();
        let index = checking(path.clone());
        loaded(&index).await;
        let scanned = index.hooks.rows.load(Ordering::Relaxed);
        for _ in 0..3 {
            assert!(index.known("ipfs", SHOWN));
            index.settle().await;
        }
        assert_eq!(index.hooks.loads.load(Ordering::Relaxed), 4);
        assert_eq!(
            index.hooks.rows.load(Ordering::Relaxed),
            scanned,
            "one scan only"
        );
        writer(&path)
            .execute("UPDATE projects SET hidden = 1 WHERE id = 29437", [])
            .unwrap();
        index.known("ipfs", SHOWN);
        index.settle().await;
        assert!(!index.known("ipfs", SHOWN));
        index.settle().await;
        writer(&path)
            .execute("UPDATE projects SET hidden = 0 WHERE id = 29437", [])
            .unwrap();
        index.known("ipfs", SHOWN);
        index.settle().await;
        assert!(index.known("ipfs", SHOWN));
    }

    #[tokio::test]
    async fn a_hidden_list_change_withdraws_the_set_during_its_reload_and_a_sync_does_not() {
        let (_dir, path) = fixture();
        let index = checking(path.clone());
        loaded(&index).await;
        *index.hooks.reload.lock().unwrap() = Duration::from_millis(300);
        let c = writer(&path);
        c.execute("UPDATE snapshot SET as_of = '2099-01-01' WHERE id = 1", [])
            .unwrap();
        assert!(!index.known("ipfs", SHOWN), "a commit waits for its check");
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            index.known("ipfs", SHOWN),
            "a snapshot-only change keeps serving"
        );
        index.settle().await;
        c.execute(
            "UPDATE projects SET hidden = 1 WHERE id = ?1",
            [other_project(&c, 1)],
        )
        .unwrap();
        index.known("ipfs", SHOWN);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            !index.known("ipfs", SHOWN),
            "a hidden change withdraws at once"
        );
        index.settle().await;
        assert!(
            index.known("ipfs", SHOWN),
            "the reloaded set still holds the root"
        );
    }

    #[tokio::test]
    async fn a_snapshot_only_reload_reconfirms_the_old_set_while_it_runs() {
        let (_dir, path) = fixture();
        let index = RootIndex::with_timing(
            path.clone(),
            Duration::from_millis(100),
            Duration::from_millis(250),
            LOAD_TIMEOUT,
        );
        loaded(&index).await;
        *index.hooks.reload.lock().unwrap() = Duration::from_millis(500);
        writer(&path)
            .execute("UPDATE snapshot SET as_of = '2099-01-01' WHERE id = 1", [])
            .unwrap();
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(
            !index.known("ipfs", SHOWN),
            "the commit starts a check and reload"
        );
        // Past the old confirmation's age limit, while the reload still runs.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(index.lock().refreshing);
        assert!(index.known("ipfs", SHOWN));
        index.settle().await;
        // The reload outlasted the age limit, so the next check confirms it again.
        index.known("ipfs", SHOWN);
        index.settle().await;
        assert!(index.known("ipfs", SHOWN));
    }

    #[tokio::test]
    async fn a_hide_applies_on_the_next_request_and_other_commits_only_recheck() {
        let (_dir, path) = fixture();
        let index = RootIndex::new(path.clone());
        loaded(&index).await;
        let c = writer(&path);
        c.execute("UPDATE projects SET hidden = 1 WHERE id = 29437", [])
            .unwrap();
        assert!(!index.known("ipfs", SHOWN), "no check interval to wait for");
        index.settle().await;
        assert!(!index.known("ipfs", SHOWN));
        c.execute("UPDATE projects SET hidden = 0 WHERE id = 29437", [])
            .unwrap();
        index.known("ipfs", SHOWN);
        index.settle().await;
        assert!(index.known("ipfs", SHOWN));
        let (loads, rows) = (
            index.hooks.loads.load(Ordering::Relaxed),
            index.hooks.rows.load(Ordering::Relaxed),
        );
        c.execute(
            "INSERT INTO content_roots (scheme, cid, project_id) VALUES ('ipfs', 'bafkreinew', 29437)",
            [],
        )
        .unwrap();
        assert!(
            !index.known("ipfs", SHOWN),
            "any commit waits for its check"
        );
        index.settle().await;
        assert!(index.known("ipfs", SHOWN));
        assert!(
            !index.known("ipfs", "bafkreinew"),
            "new roots are answered by the database"
        );
        assert_eq!(index.hooks.loads.load(Ordering::Relaxed), loads + 1);
        assert_eq!(index.hooks.rows.load(Ordering::Relaxed), rows, "no reload");
    }

    #[tokio::test]
    async fn a_reopened_watch_never_matches_an_earlier_confirmation() {
        let (_dir, path) = fixture();
        let index = RootIndex::new(path);
        loaded(&index).await;
        assert!(index.known("ipfs", SHOWN));
        // As after a failed pragma: the next lookup opens a new watch connection.
        *index.watch.lock().unwrap() = None;
        assert!(
            !index.known("ipfs", SHOWN),
            "a new watch needs its own check"
        );
        index.settle().await;
        assert!(index.known("ipfs", SHOWN));
    }

    #[tokio::test]
    async fn a_failed_load_answers_from_the_database_and_retries_later() {
        let (dir, path) = fixture();
        let index = checking(path.clone());
        loaded(&index).await;
        let moved = dir.0.join("moved.sqlite");
        std::fs::rename(&*path, &moved).unwrap();
        index.known("ipfs", SHOWN);
        index.settle().await;
        assert!(
            !index.known("ipfs", SHOWN),
            "a failed check withdraws the set"
        );
        index.settle().await;
        std::fs::rename(&moved, &*path).unwrap();
        index.known("ipfs", SHOWN);
        index.settle().await;
        assert!(index.known("ipfs", SHOWN));
    }

    #[tokio::test]
    async fn a_missing_or_replaced_archive_file_stops_the_set_at_once_and_reloads() {
        let (dir, path) = fixture();
        let index = RootIndex::new(path.clone());
        loaded(&index).await;
        let moved = dir.0.join("moved.sqlite");
        std::fs::rename(&*path, &moved).unwrap();
        assert!(
            !index.known("ipfs", SHOWN),
            "a missing file is never trusted"
        );
        index.settle().await;
        let copy = dir.0.join("copy.sqlite");
        std::fs::copy(&moved, &copy).unwrap();
        std::fs::rename(&copy, &*path).unwrap();
        assert!(
            !index.known("ipfs", SHOWN),
            "a replaced file is never trusted"
        );
        index.settle().await;
        assert!(
            index.known("ipfs", SHOWN),
            "the replacement is loaded at once"
        );
        assert_eq!(index.hooks.loads.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn failed_loads_are_retried_no_more_often_than_the_check_interval() {
        let (dir, path) = fixture();
        std::fs::remove_file(&*path).unwrap();
        let index = RootIndex::with_timing(
            Arc::new(dir.0.join("missing.sqlite")),
            Duration::from_secs(3600),
            Duration::from_secs(30),
            LOAD_TIMEOUT,
        );
        for _ in 0..20 {
            assert!(!index.known("ipfs", SHOWN));
            index.settle().await;
        }
        assert_eq!(index.hooks.loads.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn a_set_older_than_its_maximum_age_is_not_used() {
        let (_dir, path) = fixture();
        let index = RootIndex::with_timing(
            path,
            Duration::from_secs(3600),
            Duration::from_millis(300),
            LOAD_TIMEOUT,
        );
        loaded(&index).await;
        assert!(index.known("ipfs", SHOWN));
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert!(!index.known("ipfs", SHOWN));
    }

    #[tokio::test]
    async fn concurrent_requests_start_one_load() {
        let (_dir, path) = fixture();
        let index = RootIndex::new(path);
        *index.hooks.before.lock().unwrap() = Duration::from_millis(50);
        for _ in 0..50 {
            assert!(!index.known("ipfs", SHOWN));
        }
        index.settle().await;
        assert_eq!(index.hooks.loads.load(Ordering::Relaxed), 1);
        assert!(index.known("ipfs", SHOWN));
    }

    #[tokio::test]
    async fn a_load_past_its_deadline_is_abandoned_and_cannot_undo_a_newer_one() {
        let (_dir, path) = fixture();
        let index = RootIndex::with_timing(
            path,
            Duration::ZERO,
            Duration::from_secs(30),
            Duration::from_millis(40),
        );
        *index.hooks.before.lock().unwrap() = Duration::from_millis(300);
        index.known("ipfs", SHOWN);
        index.settle().await;
        assert!(index.lock().roots.is_none());
        *index.hooks.before.lock().unwrap() = Duration::ZERO;
        loaded(&index).await;
        assert!(index.known("ipfs", SHOWN));
        index.settle().await;
        // The abandoned first load now reads a fingerprint unlike its own (none) and would
        // withdraw the set, but belongs to an old generation.
        let hooks = &index.hooks;
        for _ in 0..200 {
            if hooks.finished.load(Ordering::Relaxed) == hooks.loads.load(Ordering::Relaxed) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(
            hooks.finished.load(Ordering::Relaxed),
            hooks.loads.load(Ordering::Relaxed)
        );
        assert!(index.lock().roots.is_some());
        assert!(index.known("ipfs", SHOWN));
    }

    #[tokio::test]
    async fn an_abandoned_scan_stops_at_its_next_row() {
        let (_dir, path) = fixture();
        let index = RootIndex::with_timing(
            path,
            Duration::from_secs(3600),
            Duration::from_secs(30),
            Duration::from_millis(40),
        );
        *index.hooks.reload.lock().unwrap() = Duration::from_millis(150);
        index.known("ipfs", SHOWN);
        index.settle().await;
        for _ in 0..200 {
            if index.hooks.finished.load(Ordering::Relaxed) == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(index.hooks.finished.load(Ordering::Relaxed), 1);
        assert_eq!(index.hooks.rows.load(Ordering::Relaxed), 0);
        assert!(!index.known("ipfs", SHOWN));
    }
}
