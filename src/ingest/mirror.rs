//! Preservation mirror job (`seeded-index mirror-list`, `mirror`, `mirror-status`).
//!
//! `mirror-list` writes the visible projects, most traded first, to a JSON list where the
//! catalogue lives. `mirror` runs on any host: it copies each listed project's complete
//! generator package into a mirror directory ([`MirrorWriter`]) in list order and within a
//! byte cap, so the cap keeps the most traded works. Progress is `status.sqlite` in that
//! directory, one row per project:
//!
//! - `ok`: every file of the package verified and stored;
//! - `pending`: a transient failure (timeout, 429/5xx, open breaker) or a not-found answer
//!   not yet repeated; retried by a later run after a backoff of 10 minutes doubling to a day;
//! - `missing`: not found in [`MISSING_AFTER`] separate runs;
//! - `too_big`: a file beyond the object limit, or (untiered) beyond the bytes left under
//!   the cap;
//! - `failed`: data that cannot be mirrored (hash mismatches, unsupported layouts);
//! - `waiting`: larger than the passes so far allowed; `bytes` is its known size, an
//!   unverified estimate or a verified lower bound (bytes staged before the attempt stopped).
//!   Sizes only schedule passes. One the cap stopped keeps `blocked_used` and `blocked_cap`,
//!   the mirror bytes then in use and the cap, and is tried again once the bytes in use
//!   change, the cap grows, or `--recheck`.
//!
//! Tiered passes (the default) take small packages first: a pass at threshold T attempts, in
//! list order, every due package whose known size is at most T (unknown sizes are estimated
//! first), each limited to staging T bytes; one beyond becomes `waiting`. The next pass
//! raises T by the step, or straight to the first step that takes the smallest waiting size,
//! until nothing is left or the cap cannot take the smallest one. The threshold is kept in
//! `mirror_meta`, so an interrupted run resumes at it.
//!
//! A re-run skips `ok` packages unless `--recheck`, which re-verifies stored packages
//! locally and retries every other state. Packages in flight when a run is interrupted, cut
//! by a deadline or stopped at the cap record nothing; their staging is discarded.

use std::collections::HashMap;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use rusqlite::{Connection, OpenFlags, OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use tokio::task::JoinSet;

use super::{IngestError, iso_utc, now_iso, unix_now};
use crate::content::{ContentStore, MirrorWriter, PackageError, PackageStats};
use crate::store::{Store, StoreError, classify};

pub const LIST_VERSION: u32 = 1;
/// Separate runs with a not-found answer before a package is `missing`.
pub const MISSING_AFTER: u32 = 3;
pub const STATUS_FILE: &str = "status.sqlite";
const BACKOFF_BASE: i64 = 600;
const BACKOFF_MAX: i64 = 86_400;
const PACKAGE_DEADLINE: Duration = Duration::from_secs(45 * 60);
/// Below this many free bytes (at most 1/64 of the cap) a run starts no further package.
const CAP_FLOOR: u64 = 1 << 20;
const MAX_LIST_BYTES: u64 = 64 << 20;
const STATUS_APPLICATION_ID: i64 = 0x5345_4d52;
const STATUS_VERSION: i64 = 4;
/// Version 1 lacked `waiting`, 2 `blocked_used`, 3 `blocked_cap`; each upgrades in place.
const STATUS_V1: i64 = 1;
const STATUS_V2: i64 = 2;
const STATUS_V3: i64 = 3;
const MIB: u64 = 1 << 20;
/// Consecutive transient outcomes before new packages wait; the wait doubles to a cap.
const PAUSE_AFTER: u32 = 4;
const PAUSE_BASE: Duration = Duration::from_secs(30);
const PAUSE_MAX: Duration = Duration::from_secs(15 * 60);
const PROGRESS_EVERY: Duration = Duration::from_secs(60);
const POLL: Duration = Duration::from_millis(250);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListedProject {
    pub id: u64,
    pub root_uri: String,
    pub volume_mutez: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectList {
    pub version: u32,
    pub generated_at: String,
    pub projects: Vec<ListedProject>,
}

fn io_error(what: &Path, error: impl std::fmt::Display) -> IngestError {
    IngestError::Io(format!("{}: {error}", what.display()))
}

fn sql(error: rusqlite::Error) -> IngestError {
    IngestError::Store(classify(&error))
}

/// Writes the visible projects with an `ipfs://` or `onchfs://` root, most traded first, to
/// `out` (owner-only, replaced atomically). Returns the listed and skipped counts.
pub fn write_list(store: &Store, out: &Path) -> Result<(usize, usize), IngestError> {
    let roots = store.mirror_roots()?;
    let total = roots.len();
    let projects: Vec<ListedProject> = roots
        .into_iter()
        .filter(|root| {
            root.root_uri.starts_with("ipfs://") || root.root_uri.starts_with("onchfs://")
        })
        .map(|root| ListedProject {
            id: root.project_id,
            root_uri: root.root_uri,
            volume_mutez: root.volume_mutez,
        })
        .collect();
    let listed = projects.len();
    let list = ProjectList {
        version: LIST_VERSION,
        generated_at: now_iso(),
        projects,
    };
    let data = serde_json::to_vec(&list).map_err(|e| io_error(out, e))?;
    let part = out.with_file_name(format!(
        ".{}.part",
        out.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("mirror-list.json")
    ));
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let written = options.open(&part).and_then(|mut file| {
        file.write_all(&data)?;
        file.sync_all()
    });
    if let Err(error) = written.and_then(|()| std::fs::rename(&part, out)) {
        let _ = std::fs::remove_file(&part);
        return Err(io_error(out, error));
    }
    Ok((listed, total - listed))
}

/// Reads and checks a list written by [`write_list`].
pub fn read_list(path: &Path) -> Result<ProjectList, IngestError> {
    let meta = std::fs::metadata(path).map_err(|e| io_error(path, e))?;
    if meta.len() > MAX_LIST_BYTES {
        return Err(IngestError::Usage(format!(
            "{} exceeds {MAX_LIST_BYTES} bytes",
            path.display()
        )));
    }
    let data = std::fs::read(path).map_err(|e| io_error(path, e))?;
    let list: ProjectList = serde_json::from_slice(&data)
        .map_err(|e| IngestError::Usage(format!("{} is not a mirror list: {e}", path.display())))?;
    if list.version != LIST_VERSION {
        return Err(IngestError::Usage(format!(
            "{}: list version {}, expected {LIST_VERSION}",
            path.display(),
            list.version
        )));
    }
    let mut ids = std::collections::HashSet::new();
    for project in &list.projects {
        if !ids.insert(project.id) || i64::try_from(project.id).is_err() {
            return Err(IngestError::Usage(format!(
                "{}: project {} is listed twice or out of range",
                path.display(),
                project.id
            )));
        }
    }
    Ok(list)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MirrorState {
    Pending,
    Ok,
    Missing,
    TooBig,
    Failed,
    Waiting,
}

impl MirrorState {
    pub const ALL: [MirrorState; 6] = [
        MirrorState::Ok,
        MirrorState::Pending,
        MirrorState::Missing,
        MirrorState::TooBig,
        MirrorState::Failed,
        MirrorState::Waiting,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            MirrorState::Pending => "pending",
            MirrorState::Ok => "ok",
            MirrorState::Missing => "missing",
            MirrorState::TooBig => "too_big",
            MirrorState::Failed => "failed",
            MirrorState::Waiting => "waiting",
        }
    }

    fn parse(text: &str) -> Option<Self> {
        MirrorState::ALL
            .into_iter()
            .find(|state| state.as_str() == text)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatusRow {
    pub project_id: u64,
    pub root_uri: String,
    pub state: MirrorState,
    pub bytes: u64,
    pub files: u64,
    pub attempts: u32,
    pub not_found: u32,
    pub last_error: Option<String>,
    pub checked_at: String,
    pub checked_unix: i64,
    /// `waiting` stopped by the cap: mirror bytes in use then, and the cap.
    pub blocked_used: Option<u64>,
    pub blocked_cap: Option<u64>,
}

/// One package attempt, as recorded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Ok(PackageStats),
    NotFound(String),
    Transient(String),
    Failed(String),
    TooBig {
        bytes: u64,
        error: String,
    },
    /// Beyond the pass threshold or the room left; `bytes` is its known size.
    /// `blocked`: the cap stopped it with (bytes in use, cap).
    Waiting {
        bytes: u64,
        error: String,
        blocked: Option<(u64, u64)>,
    },
}

/// Seconds a `pending` row waits after `attempts` consecutive unsuccessful attempts.
pub fn backoff(attempts: u32) -> i64 {
    match attempts {
        0 => 0,
        n => (BACKOFF_BASE << (n - 1).min(16)).min(BACKOFF_MAX),
    }
}

/// Whether a run at `now` should attempt the project with this row.
pub fn due(row: Option<&StatusRow>, root_uri: &str, now: i64, recheck: bool) -> bool {
    match row {
        None => true,
        Some(row) if row.root_uri != root_uri => true,
        Some(row) => match row.state {
            MirrorState::Pending => {
                recheck || now >= row.checked_unix.saturating_add(backoff(row.attempts))
            }
            // Its pass decides.
            MirrorState::Waiting => true,
            MirrorState::Ok | MirrorState::Missing | MirrorState::TooBig | MirrorState::Failed => {
                recheck
            }
        },
    }
}

fn status_table(name: &str) -> String {
    format!(
        "CREATE TABLE {name} (
  project_id INTEGER PRIMARY KEY CHECK (project_id >= 0),
  root_uri TEXT NOT NULL,
  state TEXT NOT NULL
    CHECK (state IN ('pending', 'ok', 'missing', 'too_big', 'failed', 'waiting')),
  bytes INTEGER NOT NULL DEFAULT 0 CHECK (bytes >= 0),
  files INTEGER NOT NULL DEFAULT 0 CHECK (files >= 0),
  attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  not_found INTEGER NOT NULL DEFAULT 0 CHECK (not_found >= 0),
  last_error TEXT,
  checked_at TEXT NOT NULL,
  blocked_used INTEGER CHECK (blocked_used >= 0),
  blocked_cap INTEGER CHECK (blocked_cap >= 0)
) STRICT;"
    )
}

const UPGRADE_V3: &str =
    "ALTER TABLE mirror_status ADD COLUMN blocked_cap INTEGER CHECK (blocked_cap >= 0);";
const UPGRADE_V2: &str =
    "ALTER TABLE mirror_status ADD COLUMN blocked_used INTEGER CHECK (blocked_used >= 0);";

const STATUS_COLUMNS: &str =
    "project_id, root_uri, state, bytes, files, attempts, not_found, last_error, checked_at";
const STATUS_INDEX: &str = "CREATE INDEX mirror_status_state ON mirror_status (state);";

/// Version 1 to 4: the state check gains `waiting`; every row is copied unchanged.
fn upgrade_v1() -> String {
    format!(
        "{}
INSERT INTO mirror_status_v2 ({STATUS_COLUMNS}) SELECT {STATUS_COLUMNS} FROM mirror_status;
DROP TABLE mirror_status;
ALTER TABLE mirror_status_v2 RENAME TO mirror_status;
{STATUS_INDEX}",
        status_table("mirror_status_v2")
    )
}

const STATUS_SCHEMA: &str = "
CREATE TABLE mirror_list (
  position INTEGER PRIMARY KEY,
  project_id INTEGER NOT NULL UNIQUE,
  root_uri TEXT NOT NULL,
  volume_mutez INTEGER NOT NULL CHECK (volume_mutez >= 0)
) STRICT;
CREATE TABLE mirror_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL) STRICT, WITHOUT ROWID;
";

const ROW: &str = "SELECT project_id, root_uri, state, bytes, files, attempts, not_found, \
     last_error, checked_at, coalesce(CAST(strftime('%s', checked_at) AS INTEGER), 0), \
     blocked_used, blocked_cap FROM mirror_status";
/// [`ROW`] of databases not yet upgraded, opened read-only.
const ROW_V3: &str = "SELECT project_id, root_uri, state, bytes, files, attempts, \
     not_found, last_error, checked_at, \
     coalesce(CAST(strftime('%s', checked_at) AS INTEGER), 0), blocked_used, NULL \
     FROM mirror_status";
const ROW_BEFORE_V3: &str = "SELECT project_id, root_uri, state, bytes, files, attempts, \
     not_found, last_error, checked_at, \
     coalesce(CAST(strftime('%s', checked_at) AS INTEGER), 0), NULL, NULL FROM mirror_status";

fn status_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<StatusRow> {
    let state: String = r.get(2)?;
    let unsigned = |i: usize| -> rusqlite::Result<u64> {
        let v: i64 = r.get(i)?;
        u64::try_from(v).map_err(|_| rusqlite::Error::IntegralValueOutOfRange(i, v))
    };
    Ok(StatusRow {
        project_id: unsigned(0)?,
        root_uri: r.get(1)?,
        state: MirrorState::parse(&state).ok_or_else(|| {
            rusqlite::Error::FromSqlConversionFailure(
                2,
                rusqlite::types::Type::Text,
                "unknown mirror state".into(),
            )
        })?,
        bytes: unsigned(3)?,
        files: unsigned(4)?,
        attempts: u32::try_from(unsigned(5)?).unwrap_or(u32::MAX),
        not_found: u32::try_from(unsigned(6)?).unwrap_or(u32::MAX),
        last_error: r.get(7)?,
        checked_at: r.get(8)?,
        checked_unix: r.get(9)?,
        blocked_used: r
            .get::<_, Option<i64>>(10)?
            .map(|v| u64::try_from(v).unwrap_or(0)),
        blocked_cap: r
            .get::<_, Option<i64>>(11)?
            .map(|v| u64::try_from(v).unwrap_or(0)),
    })
}

/// Counts and failures for `mirror-status`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StatusReport {
    pub listed: u64,
    pub listed_volume_mutez: u64,
    /// Rows per state, with their bytes and files.
    pub states: Vec<(MirrorState, u64, u64, u64)>,
    /// Listed projects without a row.
    pub unvisited: u64,
    /// Volume of listed projects whose current root is `ok`.
    pub ok_volume_mutez: u64,
    /// (state, last error, rows), most frequent first.
    pub top_errors: Vec<(MirrorState, String, u64)>,
    pub meta: Vec<(String, String)>,
}

/// `status.sqlite` of a mirror directory.
pub struct MirrorStatus {
    conn: Connection,
    row_sql: &'static str,
}

impl MirrorStatus {
    /// Opens or creates the status database in `mirror` (owner-only, WAL).
    pub fn open(mirror: &Path) -> Result<Self, IngestError> {
        let path = mirror.join(STATUS_FILE);
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create(true).truncate(false);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        options.open(&path).map_err(|e| io_error(&path, e))?;
        let conn = Connection::open_with_flags(
            &path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_NO_MUTEX
                | OpenFlags::SQLITE_OPEN_NOFOLLOW,
        )
        .map_err(sql)?;
        conn.busy_timeout(Duration::from_secs(2)).map_err(sql)?;
        let mode: String = conn
            .pragma_update_and_check(None, "journal_mode", "WAL", |r| r.get(0))
            .map_err(sql)?;
        if !mode.eq_ignore_ascii_case("wal") {
            return Err(IngestError::Store(StoreError::Internal(format!(
                "journal_mode is {mode}, expected wal"
            ))));
        }
        conn.pragma_update(None, "synchronous", "NORMAL")
            .map_err(sql)?;
        let mut status = Self { conn, row_sql: ROW };
        let tx = status
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)?;
        match identity(&tx)? {
            (0, 0) => {
                tx.execute_batch(&status_table("mirror_status"))
                    .map_err(sql)?;
                tx.execute_batch(STATUS_INDEX).map_err(sql)?;
                tx.execute_batch(STATUS_SCHEMA).map_err(sql)?;
                tx.pragma_update(None, "application_id", STATUS_APPLICATION_ID)
                    .map_err(sql)?;
                tx.pragma_update(None, "user_version", STATUS_VERSION)
                    .map_err(sql)?;
            }
            (STATUS_APPLICATION_ID, STATUS_V1) => {
                tx.execute_batch(&upgrade_v1()).map_err(sql)?;
                tx.pragma_update(None, "user_version", STATUS_VERSION)
                    .map_err(sql)?;
            }
            (STATUS_APPLICATION_ID, STATUS_V2) => {
                tx.execute_batch(UPGRADE_V2).map_err(sql)?;
                tx.execute_batch(UPGRADE_V3).map_err(sql)?;
                tx.pragma_update(None, "user_version", STATUS_VERSION)
                    .map_err(sql)?;
            }
            (STATUS_APPLICATION_ID, STATUS_V3) => {
                tx.execute_batch(UPGRADE_V3).map_err(sql)?;
                tx.pragma_update(None, "user_version", STATUS_VERSION)
                    .map_err(sql)?;
            }
            (STATUS_APPLICATION_ID, STATUS_VERSION) => {}
            _ => return Err(not_status(&path)),
        }
        tx.commit().map_err(sql)?;
        Ok(status)
    }

    pub fn open_read_only(mirror: &Path) -> Result<Self, IngestError> {
        let path = mirror.join(STATUS_FILE);
        if !path.is_file() {
            return Err(IngestError::Usage(format!(
                "{} does not exist; run `seeded-index mirror` first",
                path.display()
            )));
        }
        let conn = Connection::open_with_flags(
            &path,
            OpenFlags::SQLITE_OPEN_READ_ONLY
                | OpenFlags::SQLITE_OPEN_NO_MUTEX
                | OpenFlags::SQLITE_OPEN_NOFOLLOW,
        )
        .map_err(sql)?;
        conn.busy_timeout(Duration::from_secs(2)).map_err(sql)?;
        // An older database reads the same; `mirror` upgrades it.
        let row_sql = match identity(&conn)? {
            (STATUS_APPLICATION_ID, STATUS_VERSION) => ROW,
            (STATUS_APPLICATION_ID, STATUS_V3) => ROW_V3,
            (STATUS_APPLICATION_ID, STATUS_V1 | STATUS_V2) => ROW_BEFORE_V3,
            _ => return Err(not_status(&path)),
        };
        Ok(Self { conn, row_sql })
    }

    /// Replaces the stored copy of the list (for coverage in the report).
    pub fn replace_list(&mut self, projects: &[ListedProject]) -> Result<(), IngestError> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)?;
        tx.execute("DELETE FROM mirror_list", []).map_err(sql)?;
        {
            let mut insert = tx
                .prepare(
                    "INSERT INTO mirror_list (position, project_id, root_uri, volume_mutez) \
                     VALUES (?1, ?2, ?3, ?4)",
                )
                .map_err(sql)?;
            for (position, project) in projects.iter().enumerate() {
                insert
                    .execute(params![
                        position as i64,
                        project.id as i64,
                        project.root_uri,
                        i64::try_from(project.volume_mutez).unwrap_or(i64::MAX)
                    ])
                    .map_err(sql)?;
            }
        }
        tx.commit().map_err(sql)
    }

    pub fn rows(&self) -> Result<HashMap<u64, StatusRow>, IngestError> {
        let mut statement = self.conn.prepare(self.row_sql).map_err(sql)?;
        let rows = statement.query_map([], status_row).map_err(sql)?;
        let mut out = HashMap::new();
        for row in rows {
            let row = row.map_err(sql)?;
            out.insert(row.project_id, row);
        }
        Ok(out)
    }

    pub fn row(&self, project_id: u64) -> Result<Option<StatusRow>, IngestError> {
        self.conn
            .query_row(
                &format!("{} WHERE project_id = ?1", self.row_sql),
                [project_id as i64],
                status_row,
            )
            .optional()
            .map_err(sql)
    }

    /// Records one attempt and returns the new state. A changed root starts a fresh row.
    pub fn record(
        &mut self,
        project: &ListedProject,
        outcome: &Outcome,
        now: i64,
    ) -> Result<MirrorState, IngestError> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)?;
        let previous: Option<(String, i64, i64)> = tx
            .query_row(
                "SELECT root_uri, attempts, not_found FROM mirror_status WHERE project_id = ?1",
                [project.id as i64],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()
            .map_err(sql)?;
        let (attempts, not_found) = match previous {
            Some((root, attempts, not_found)) if root == project.root_uri => (attempts, not_found),
            _ => (0, 0),
        };
        let blocked = match outcome {
            Outcome::Waiting { blocked, .. } => *blocked,
            _ => None,
        };
        let sql_u64 = |v: u64| i64::try_from(v).unwrap_or(i64::MAX);
        let (state, bytes, files, attempts, not_found, error) = match outcome {
            Outcome::Ok(stats) => (MirrorState::Ok, stats.bytes, stats.files, 0, 0, None),
            Outcome::NotFound(error) => {
                let not_found = not_found + 1;
                let state = if not_found >= i64::from(MISSING_AFTER) {
                    MirrorState::Missing
                } else {
                    MirrorState::Pending
                };
                (state, 0, 0, attempts + 1, not_found, Some(error))
            }
            Outcome::Transient(error) => (
                MirrorState::Pending,
                0,
                0,
                attempts + 1,
                not_found,
                Some(error),
            ),
            Outcome::Failed(error) => (MirrorState::Failed, 0, 0, attempts + 1, 0, Some(error)),
            Outcome::TooBig { bytes, error } => {
                (MirrorState::TooBig, *bytes, 0, attempts + 1, 0, Some(error))
            }
            // Not a failure: the counters wait with it.
            Outcome::Waiting { bytes, error, .. } => (
                MirrorState::Waiting,
                *bytes,
                0,
                attempts,
                not_found,
                Some(error),
            ),
        };
        tx.execute(
            "INSERT INTO mirror_status (project_id, root_uri, state, bytes, files, attempts, \
             not_found, last_error, checked_at, blocked_used, blocked_cap) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11) \
             ON CONFLICT (project_id) DO UPDATE SET root_uri = excluded.root_uri, \
             state = excluded.state, bytes = excluded.bytes, files = excluded.files, \
             attempts = excluded.attempts, not_found = excluded.not_found, \
             last_error = excluded.last_error, checked_at = excluded.checked_at, \
             blocked_used = excluded.blocked_used, blocked_cap = excluded.blocked_cap",
            params![
                project.id as i64,
                project.root_uri,
                state.as_str(),
                i64::try_from(bytes).unwrap_or(i64::MAX),
                i64::try_from(files).unwrap_or(i64::MAX),
                attempts,
                not_found,
                error.map(|e| e.chars().take(500).collect::<String>()),
                iso_utc(now),
                blocked.map(|(used, _)| sql_u64(used)),
                blocked.map(|(_, cap)| sql_u64(cap)),
            ],
        )
        .map_err(sql)?;
        tx.commit().map_err(sql)?;
        Ok(state)
    }

    pub fn meta(&self, key: &str) -> Result<Option<String>, IngestError> {
        self.conn
            .query_row("SELECT value FROM mirror_meta WHERE key = ?1", [key], |r| {
                r.get(0)
            })
            .optional()
            .map_err(sql)
    }

    pub fn set_meta(&self, pairs: &[(&str, String)]) -> Result<(), IngestError> {
        let mut statement = self
            .conn
            .prepare_cached(
                "INSERT INTO mirror_meta (key, value) VALUES (?1, ?2) \
                 ON CONFLICT (key) DO UPDATE SET value = excluded.value",
            )
            .map_err(sql)?;
        for (key, value) in pairs {
            statement.execute(params![key, value]).map_err(sql)?;
        }
        Ok(())
    }

    /// Counts per state, coverage of the list and the `top` most frequent failures.
    pub fn report(&self, top: usize) -> Result<StatusReport, IngestError> {
        let tx = self.conn.unchecked_transaction().map_err(sql)?;
        let (listed, listed_volume): (i64, i64) = tx
            .query_row(
                "SELECT count(*), coalesce(sum(volume_mutez), 0) FROM mirror_list",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .map_err(sql)?;
        let mut states = Vec::new();
        for state in MirrorState::ALL {
            let (rows, bytes, files): (i64, i64, i64) = tx
                .query_row(
                    "SELECT count(*), coalesce(sum(bytes), 0), coalesce(sum(files), 0) \
                     FROM mirror_status WHERE state = ?1",
                    [state.as_str()],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )
                .map_err(sql)?;
            states.push((state, rows as u64, bytes as u64, files as u64));
        }
        let (unvisited, ok_volume): (i64, i64) = tx
            .query_row(
                "SELECT count(*) FILTER (WHERE s.project_id IS NULL), \
                 coalesce(sum(l.volume_mutez) FILTER (WHERE s.state = 'ok' \
                   AND s.root_uri = l.root_uri), 0) \
                 FROM mirror_list l LEFT JOIN mirror_status s ON s.project_id = l.project_id",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .map_err(sql)?;
        let top_errors = {
            let mut statement = tx
                .prepare(
                    "SELECT state, last_error, count(*) FROM mirror_status \
                     WHERE state <> 'ok' AND last_error IS NOT NULL \
                     GROUP BY state, last_error ORDER BY count(*) DESC, state, last_error \
                     LIMIT ?1",
                )
                .map_err(sql)?;
            let rows = statement
                .query_map([top as i64], |r| {
                    let state: String = r.get(0)?;
                    Ok((state, r.get::<_, String>(1)?, r.get::<_, i64>(2)?))
                })
                .map_err(sql)?;
            let mut out = Vec::new();
            for row in rows {
                let (state, error, count) = row.map_err(sql)?;
                if let Some(state) = MirrorState::parse(&state) {
                    out.push((state, error, count as u64));
                }
            }
            out
        };
        let meta = {
            let mut statement = tx
                .prepare("SELECT key, value FROM mirror_meta ORDER BY key")
                .map_err(sql)?;
            let rows = statement
                .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
                .map_err(sql)?;
            rows.collect::<rusqlite::Result<Vec<_>>>().map_err(sql)?
        };
        tx.commit().map_err(sql)?;
        Ok(StatusReport {
            listed: listed as u64,
            listed_volume_mutez: listed_volume as u64,
            states,
            unvisited: unvisited as u64,
            ok_volume_mutez: ok_volume as u64,
            top_errors,
            meta,
        })
    }
}

fn identity(conn: &Connection) -> Result<(i64, i64), IngestError> {
    let app = conn
        .pragma_query_value(None, "application_id", |r| r.get(0))
        .map_err(sql)?;
    let version = conn
        .pragma_query_value(None, "user_version", |r| r.get(0))
        .map_err(sql)?;
    Ok((app, version))
}

fn not_status(path: &Path) -> IngestError {
    IngestError::Usage(format!(
        "{} is not a mirror status database at version {STATUS_VERSION}",
        path.display()
    ))
}

/// Tiered passes: packages up to `start` bytes first, then `step` more per pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Tiers {
    pub start: u64,
    pub step: u64,
}

impl Default for Tiers {
    fn default() -> Self {
        Self {
            start: MIB,
            step: MIB,
        }
    }
}

/// The first threshold of `threshold + n * step` that takes `smallest`; `threshold` itself
/// when it already does (room held in flight kept a package back).
pub fn next_threshold(threshold: u64, step: u64, smallest: u64) -> u64 {
    if smallest <= threshold {
        return threshold;
    }
    let step = step.max(1);
    threshold.saturating_add((smallest - threshold).div_ceil(step).saturating_mul(step))
}

#[derive(Debug, Clone)]
pub struct MirrorOptions {
    /// Packages to start at most.
    pub limit: Option<usize>,
    pub concurrency: usize,
    /// Only this listed project, whatever its state and size.
    pub project: Option<u64>,
    pub recheck: bool,
    /// `None`: one pass in list order without a package limit.
    pub tiers: Option<Tiers>,
    /// Unix seconds; injectable for tests.
    pub clock: fn() -> i64,
}

impl Default for MirrorOptions {
    fn default() -> Self {
        Self {
            limit: None,
            concurrency: 2,
            project: None,
            recheck: false,
            tiers: Some(Tiers::default()),
            clock: unix_now,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MirrorRun {
    pub listed: usize,
    pub due: usize,
    pub started: usize,
    pub ok: usize,
    /// `ok` from stored objects alone.
    pub local: usize,
    pub pending: usize,
    pub missing: usize,
    pub too_big: usize,
    pub failed: usize,
    /// Recorded `waiting` (a package may wait in several passes).
    pub waiting: usize,
    /// Not started for lack of room held by packages in flight; nothing recorded.
    pub deferred: usize,
    /// Due packages not finished when the run ended (cap, limit, interruption).
    pub left: usize,
    pub new_bytes: u64,
    pub package_bytes: u64,
    /// Threshold of the last pass; 0 untiered.
    pub threshold: u64,
    pub interrupted: bool,
    pub cap_reached: bool,
    pub limited: bool,
}

impl MirrorRun {
    /// Settled for this run; `waiting` ones are attempted again in a later pass.
    fn finished(&self) -> usize {
        self.ok + self.pending + self.missing + self.too_big + self.failed + self.deferred
    }
}

fn mib(bytes: u64) -> f64 {
    bytes as f64 / (1u64 << 20) as f64
}

fn threshold_text(bytes: u64) -> String {
    if bytes % MIB == 0 {
        format!("{} MiB", bytes / MIB)
    } else {
        format!("{:.2} MiB", mib(bytes))
    }
}

/// A due package and what is known of its size.
#[derive(Debug, Clone)]
struct Item {
    position: usize,
    project: ListedProject,
    /// Size hint from its `waiting` row or an attempt of this run; schedules passes only.
    known: Option<u64>,
    /// Attempted whatever the threshold, without a limit or an estimate.
    fixed: bool,
    /// The cap stopped it and the stored objects have not changed since: not attempted.
    parked: bool,
}

enum Attempt {
    Done(Result<PackageStats, PackageError>),
    /// Estimated beyond the limit or the room left; nothing downloaded.
    Estimated(u64),
}

const META_THRESHOLD: &str = "threshold";

/// The threshold to start at: the stored one of an unfinished tiered run, else `start`.
fn first_threshold(status: &MirrorStatus, tiers: Tiers) -> Result<u64, IngestError> {
    let finished = status.meta("run_state")?.as_deref() == Some("done");
    let stored = match status.meta(META_THRESHOLD)? {
        Some(value) if !finished => value.parse::<u64>().unwrap_or(0),
        _ => 0,
    };
    Ok(tiers.start.max(stored))
}

/// Mirrors the due projects of `list` in tiered passes (see the module), each in list order
/// with up to `concurrency` packages in flight. `stop` cancels packages in flight (nothing
/// recorded for them) and ends the run.
pub async fn run_mirror(
    list: &ProjectList,
    status: &mut MirrorStatus,
    content: &ContentStore,
    writer: &Arc<MirrorWriter>,
    opts: &MirrorOptions,
    stop: &AtomicBool,
    log: &dyn Fn(&str),
) -> Result<MirrorRun, IngestError> {
    if opts.concurrency == 0 || opts.concurrency > 16 {
        return Err(IngestError::Usage("--concurrency must be 1..=16".into()));
    }
    if opts
        .tiers
        .is_some_and(|tiers| tiers.start == 0 || tiers.step == 0)
    {
        return Err(IngestError::Usage(
            "--start-threshold and --threshold-step must be positive".into(),
        ));
    }
    status.replace_list(&list.projects)?;
    let rows = status.rows()?;
    let start = (opts.clock)();
    let tiered = opts.tiers.is_some() && opts.project.is_none();
    let mut items: Vec<Item> = match opts.project {
        Some(id) => vec![Item {
            position: 0,
            project: list
                .projects
                .iter()
                .find(|project| project.id == id)
                .cloned()
                .ok_or_else(|| IngestError::Usage(format!("project {id} is not in the list")))?,
            known: None,
            fixed: true,
            parked: false,
        }],
        None => list
            .projects
            .iter()
            .enumerate()
            .filter(|(_, p)| due(rows.get(&p.id), &p.root_uri, start, opts.recheck))
            .map(|(position, project)| {
                let row = rows
                    .get(&project.id)
                    .filter(|row| row.root_uri == project.root_uri);
                let state = row.map(|row| row.state);
                let waiting = row.filter(|row| row.state == MirrorState::Waiting);
                Item {
                    position,
                    project: project.clone(),
                    known: waiting.map(|row| row.bytes),
                    // Stored packages re-verify locally.
                    fixed: !tiered || state == Some(MirrorState::Ok),
                    // Tried again when blocks stored since may be its own, the cap grew, or
                    // on --recheck.
                    parked: tiered
                        && !opts.recheck
                        && waiting.is_some_and(|row| {
                            row.blocked_used == Some(writer.used_bytes())
                                && row.blocked_cap.is_some_and(|cap| writer.max_bytes() <= cap)
                        }),
                }
            })
            .collect(),
    };
    drop(rows);
    let mut run = MirrorRun {
        listed: list.projects.len(),
        due: items.len(),
        ..MirrorRun::default()
    };
    let step = opts.tiers.map_or(0, |tiers| tiers.step);
    let mut threshold = match opts.tiers {
        Some(tiers) if tiered => first_threshold(status, tiers)?,
        _ => u64::MAX,
    };
    status.set_meta(&[
        ("run_state", "running".into()),
        ("run_started_at", iso_utc(start)),
        ("max_bytes", writer.max_bytes().to_string()),
        ("used_bytes", writer.used_bytes().to_string()),
        ("objects", writer.objects().to_string()),
    ])?;
    log(&format!(
        "mirror: {} listed, {} due; mirror {:.1} of {:.1} GiB, {} objects",
        run.listed,
        run.due,
        mib(writer.used_bytes()) / 1024.0,
        mib(writer.max_bytes()) / 1024.0,
        writer.objects()
    ));
    let mut streak = 0u32;
    let mut pause_until: Option<Instant> = None;
    let began = Instant::now();
    let mut progress_at = Instant::now();
    let shown = |threshold: u64| tiered.then_some(threshold);
    loop {
        if tiered {
            run.threshold = threshold;
            status.set_meta(&[(META_THRESHOLD, threshold.to_string())])?;
        }
        let (now, mut later): (Vec<Item>, Vec<Item>) = items.into_iter().partition(|item| {
            !item.parked && (item.fixed || item.known.is_none_or(|known| known <= threshold))
        });
        if tiered {
            log(&format!(
                "mirror: pass at {} threshold, {} packages, {} waiting",
                threshold_text(threshold),
                now.len(),
                later.len()
            ));
        }
        let mut queue = now.into_iter().peekable();
        let mut set = JoinSet::new();
        loop {
            if stop.load(Ordering::SeqCst) {
                set.abort_all();
                while set.join_next().await.is_some() {}
                run.interrupted = true;
                break;
            }
            if pause_until.is_some_and(|until| Instant::now() >= until) {
                pause_until = None;
            }
            while set.len() < opts.concurrency && pause_until.is_none() && queue.peek().is_some() {
                if opts.limit.is_some_and(|limit| run.started >= limit) {
                    run.limited = true;
                    break;
                }
                if writer.remaining() < CAP_FLOOR.min(writer.max_bytes() / 64) {
                    run.cap_reached = true;
                    break;
                }
                let Some(item) = queue.next() else {
                    break;
                };
                run.started += 1;
                let content = content.clone();
                let writer = writer.clone();
                let limit = if item.fixed { u64::MAX } else { threshold };
                set.spawn(async move {
                    let started = Instant::now();
                    let attempt = async {
                        if !item.fixed && item.known.is_none() {
                            if let Some(estimate) = content
                                .mirror_estimate(&item.project.root_uri, &writer)
                                .await
                            {
                                // A hint: only staging measures what the cap must take.
                                if estimate > limit {
                                    return Attempt::Estimated(estimate);
                                }
                            }
                        }
                        Attempt::Done(if limit == u64::MAX {
                            content
                                .mirror_package(&item.project.root_uri, &writer)
                                .await
                        } else {
                            content
                                .mirror_package_within(&item.project.root_uri, &writer, limit)
                                .await
                        })
                    };
                    let result = tokio::time::timeout(PACKAGE_DEADLINE, attempt)
                        .await
                        .unwrap_or_else(|_| {
                            Attempt::Done(Err(PackageError::Transient {
                                message: "package deadline exceeded".into(),
                                retry_after: None,
                            }))
                        });
                    (item, result, started.elapsed())
                });
            }
            if set.is_empty()
                && (pause_until.is_none()
                    || run.limited
                    || run.cap_reached
                    || queue.peek().is_none())
            {
                break;
            }
            let joined = tokio::select! {
                joined = set.join_next(), if !set.is_empty() => joined,
                () = tokio::time::sleep(POLL) => None,
            };
            if progress_at.elapsed() >= PROGRESS_EVERY {
                progress_at = Instant::now();
                log(&progress(&run, writer, began.elapsed(), shown(threshold)));
            }
            let Some(joined) = joined else {
                continue;
            };
            let (item, result, elapsed) =
                joined.map_err(|e| IngestError::Io(format!("mirror worker: {e}")))?;
            let project = &item.project;
            let outcome = match result {
                Attempt::Estimated(bytes) => Outcome::Waiting {
                    bytes,
                    error: "estimated beyond the limit of this pass".into(),
                    blocked: None,
                },
                Attempt::Done(Ok(stats)) => Outcome::Ok(stats),
                Attempt::Done(Err(PackageError::NotFound(error))) => Outcome::NotFound(error),
                Attempt::Done(Err(PackageError::Transient {
                    message,
                    retry_after,
                })) => {
                    streak += 1;
                    if streak >= PAUSE_AFTER {
                        let pause = PAUSE_BASE
                            .saturating_mul(1 << (streak - PAUSE_AFTER).min(8))
                            .min(PAUSE_MAX)
                            .max(retry_after.unwrap_or_default().min(PAUSE_MAX));
                        pause_until = Some(Instant::now() + pause);
                        log(&format!(
                            "mirror: {streak} transient failures in a row; waiting {}s",
                            pause.as_secs()
                        ));
                    }
                    Outcome::Transient(message)
                }
                Attempt::Done(Err(PackageError::Invalid(error))) => Outcome::Failed(error),
                Attempt::Done(Err(PackageError::TooBig { message, bytes })) => Outcome::TooBig {
                    bytes,
                    error: message,
                },
                Attempt::Done(Err(PackageError::Waiting {
                    message,
                    bytes,
                    cap,
                })) => Outcome::Waiting {
                    bytes,
                    error: message,
                    blocked: cap.then(|| (writer.used_bytes(), writer.max_bytes())),
                },
                Attempt::Done(Err(PackageError::Deferred(error))) => {
                    run.deferred += 1;
                    log(&format!(
                        "mirror {} deferred: {error} {}",
                        project.id, project.root_uri
                    ));
                    continue;
                }
                Attempt::Done(Err(PackageError::Local(error))) => {
                    set.abort_all();
                    while set.join_next().await.is_some() {}
                    status.set_meta(&[("run_state", "failed".into())])?;
                    return Err(IngestError::Io(format!("mirror directory: {error}")));
                }
            };
            if !matches!(outcome, Outcome::Transient(_)) {
                streak = 0;
            }
            let state = status.record(project, &outcome, (opts.clock)())?;
            match state {
                MirrorState::Ok => run.ok += 1,
                MirrorState::Pending => run.pending += 1,
                MirrorState::Missing => run.missing += 1,
                MirrorState::TooBig => run.too_big += 1,
                MirrorState::Failed => run.failed += 1,
                MirrorState::Waiting => run.waiting += 1,
            }
            let detail = match &outcome {
                Outcome::Ok(stats) => {
                    run.local += usize::from(stats.local);
                    run.new_bytes += stats.new_bytes;
                    run.package_bytes += stats.bytes;
                    format!(
                        "files {} {:.2} MiB new {:.2} MiB{}",
                        stats.files,
                        mib(stats.bytes),
                        mib(stats.new_bytes),
                        if stats.local { " stored" } else { "" }
                    )
                }
                Outcome::NotFound(error) | Outcome::Transient(error) | Outcome::Failed(error) => {
                    error.clone()
                }
                Outcome::TooBig { error, .. } => error.clone(),
                Outcome::Waiting { bytes, error, .. } => format!("{error}: {:.2} MiB", mib(*bytes)),
            };
            log(&format!(
                "mirror {} {} {detail} {:.1}s {}",
                project.id,
                state.as_str(),
                elapsed.as_secs_f64(),
                project.root_uri
            ));
            if let Outcome::Waiting { bytes, blocked, .. } = outcome {
                later.push(Item {
                    known: Some(bytes),
                    // Once per run: blocks other packages add are tried by the next run.
                    parked: blocked.is_some(),
                    ..item
                });
            }
            status.set_meta(&[
                ("used_bytes", writer.used_bytes().to_string()),
                ("objects", writer.objects().to_string()),
                ("updated_at", iso_utc((opts.clock)())),
            ])?;
        }
        later.sort_by_key(|item| item.position);
        later.extend(queue);
        items = later;
        if run.interrupted || run.limited || run.cap_reached || items.is_empty() {
            break;
        }
        // Items left that are not parked have a hint beyond this threshold: unknown ones
        // were attempted in this pass. Only parked ones, stopped by the cap, end the run.
        let Some(smallest) = items
            .iter()
            .filter(|item| !item.parked)
            .filter_map(|item| item.known)
            .min()
        else {
            run.cap_reached = true;
            break;
        };
        threshold = next_threshold(threshold, step, smallest.max(threshold.saturating_add(1)));
    }
    run.left = items.len();
    let state = if run.interrupted {
        "interrupted"
    } else if run.cap_reached {
        "cap reached"
    } else if run.limited {
        "limited"
    } else {
        "done"
    };
    status.set_meta(&[
        ("run_state", state.into()),
        ("run_finished_at", iso_utc((opts.clock)())),
        ("used_bytes", writer.used_bytes().to_string()),
        ("objects", writer.objects().to_string()),
    ])?;
    log(&progress(&run, writer, began.elapsed(), shown(threshold)));
    Ok(run)
}

fn progress(
    run: &MirrorRun,
    writer: &MirrorWriter,
    elapsed: Duration,
    threshold: Option<u64>,
) -> String {
    let minutes = elapsed.as_secs_f64() / 60.0;
    let rate = |n: f64| if minutes > 0.0 { n / minutes } else { 0.0 };
    let tier = threshold.map_or_else(String::new, |threshold| {
        format!("; threshold {}", threshold_text(threshold))
    });
    format!(
        "progress {}/{} in {minutes:.1} min: ok {} (stored before {}), pending {}, missing {}, \
         too_big {}, failed {}, waiting {}, deferred {}; {:.1} MiB new; mirror {:.2} of {:.1} GiB; \
         {:.1} packages/min, {:.1} MiB/min{tier}",
        run.finished(),
        run.due,
        run.ok,
        run.local,
        run.pending,
        run.missing,
        run.too_big,
        run.failed,
        run.waiting,
        run.deferred,
        mib(run.new_bytes),
        mib(writer.used_bytes()) / 1024.0,
        mib(writer.max_bytes()) / 1024.0,
        rate(run.finished() as f64),
        rate(mib(run.new_bytes)),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_passes_are_skipped_to_the_step_that_takes_the_smallest_waiting_size() {
        assert_eq!(next_threshold(MIB, MIB, MIB + 1), 2 * MIB);
        assert_eq!(next_threshold(MIB, MIB, 2 * MIB), 2 * MIB);
        assert_eq!(next_threshold(MIB, MIB, 7 * MIB + 5), 8 * MIB);
        assert_eq!(
            next_threshold(3 * MIB, MIB, MIB),
            3 * MIB,
            "room held in flight"
        );
        assert_eq!(next_threshold(u64::MAX - 1, MIB, u64::MAX), u64::MAX);
    }

    #[test]
    fn waiting_is_due_for_its_pass_and_keeps_its_counters() {
        let row = StatusRow {
            project_id: 1,
            root_uri: "ipfs://a".into(),
            state: MirrorState::Waiting,
            bytes: 5 * MIB,
            files: 0,
            attempts: 2,
            not_found: 1,
            last_error: None,
            checked_at: String::new(),
            checked_unix: 100,
            blocked_used: None,
            blocked_cap: None,
        };
        assert!(due(Some(&row), "ipfs://a", 100, false));
        let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tmp/mirror-unit")
            .join(format!("waiting-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let mut status = MirrorStatus::open(&dir).unwrap();
        let project = ListedProject {
            id: 1,
            root_uri: "ipfs://a".into(),
            volume_mutez: 1,
        };
        status
            .record(&project, &Outcome::Transient("timeout".into()), 100)
            .unwrap();
        let waiting = Outcome::Waiting {
            bytes: 3 * MIB,
            error: "package exceeds the limit of this pass".into(),
            blocked: None,
        };
        assert_eq!(
            status.record(&project, &waiting, 200).unwrap(),
            MirrorState::Waiting
        );
        let kept = status.row(1).unwrap().unwrap();
        assert_eq!((kept.bytes, kept.attempts), (3 * MIB, 1));
        let report = status.report(5).unwrap();
        assert!(
            report
                .states
                .contains(&(MirrorState::Waiting, 1, 3 * MIB, 0))
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
