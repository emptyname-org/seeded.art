//! Preservation mirror: complete generator packages, verified and never evicted.
//!
//! Layout under the root (`SEEDED_MIRROR`):
//! - `ipfs/<xx>/<key>`: IPFS blocks; `key` is the hex of the CIDv1 bytes, `xx` its last two
//!   digits;
//! - `onchfs/<xx>/<cid>`: ONCHFS directory and file records, in the cache record encoding;
//! - `tmp/`: the staging of packages in flight, cleared when a writer opens;
//! - `lock`: SQLite lease of the one writer.
//!
//! Playback trusts write-time verification: even first reads do not hash block or record
//! bytes. The private mirror must remain immutable except through its writer. A writer
//! stages a package and moves its objects into place only after every file verified, the
//! root last. Shared blocks are stored once. Playback never writes or evicts anything here.

use std::collections::{HashMap, HashSet};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use bytes::Bytes;
use cid::Cid;
use tokio::sync::Semaphore;

use super::car::{self, CarStream, Outcome};
use super::http::{FetchError, Streaming};
use super::ipfs::{self, CAR_TYPE, IpfsResolver};
use super::onchfs::{self, Hash, OnchfsResolver, Record, Resolved};
use super::path::{self, Segment};
use super::{Content, ContentError, ErrorKind, Verification};

const ALLOCATION: u64 = 4096;
/// Deepest directory nesting a package may have (paths are limited to 64 segments).
const MAX_DEPTH: u32 = 64;
const MAX_FILES: u64 = 50_000;
/// One whole-package CAR transfer; a slower gateway is a transient failure.
const STREAM_LIMIT: Duration = Duration::from_secs(30 * 60);
/// Verified blocks written per staging batch: entries, and the memory they hold.
const BATCH_BYTES: usize = 4 << 20;
const BATCH_BLOCKS: usize = 1024;
/// What a batch entry allocates besides its block and key.
const ENTRY_OVERHEAD: usize = std::mem::size_of::<(String, Vec<u8>)>() + 32;
/// Blocks one CAR transfer tracks, received or linked; the walk fetches any beyond.
const TRACKED: usize = 131_072;
/// Sections and bytes a CAR may carry beyond the blocks the DAG needs (duplicates, unlinked
/// blocks, framing), in addition to as many as it needs.
const WASTE_SECTIONS: u64 = 16_384;
const WASTE_BYTES: u64 = 16 << 20;
const READ_WAIT: Duration = Duration::from_secs(2);
const WARN_EVERY: Duration = Duration::from_secs(60);
/// Metadata headroom of an ONCHFS file record over its content.
const RECORD_OVERHEAD: u64 = 1 << 20;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Ns {
    Ipfs,
    Onchfs,
}

impl Ns {
    fn dir(self) -> &'static str {
        match self {
            Ns::Ipfs => "ipfs",
            Ns::Onchfs => "onchfs",
        }
    }
}

type Key = (Ns, String);

fn ipfs_key(cid: &Cid) -> String {
    hex::encode(car::normalize(*cid).to_bytes())
}

fn object_path(root: &Path, ns: Ns, key: &str) -> PathBuf {
    root.join(ns.dir()).join(&key[key.len() - 2..]).join(key)
}

/// A stored object, or `None` if absent. Links and oversized files are refused.
fn read_object(path: &Path, cap: u64) -> io::Result<Option<Vec<u8>>> {
    let size = match std::fs::symlink_metadata(path) {
        Ok(meta) if meta.is_file() && meta.len() <= cap => meta.len(),
        Ok(_) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "mirror object is not a regular file within the object limit",
            ));
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let mut bytes = vec![0; size as usize];
    let mut file = match std::fs::File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    file.read_exact(&mut bytes)?;
    if file.read(&mut [0])? != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "mirror object grew beyond the object limit",
        ));
    }
    Ok(Some(bytes))
}

fn peek_object(path: &Path, cap: u64, prefix_cap: usize) -> io::Result<Option<(Vec<u8>, u64)>> {
    match std::fs::symlink_metadata(path) {
        Ok(meta) if meta.is_file() && meta.len() <= cap => {}
        Ok(_) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid mirror object",
            ));
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    }
    let file = match std::fs::File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let size = file.metadata()?.len();
    if size > cap {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "mirror object grew",
        ));
    }
    let limit = size.min(prefix_cap as u64);
    let mut bytes = Vec::with_capacity(limit as usize);
    file.take(limit).read_to_end(&mut bytes)?;
    Ok(Some((bytes, size)))
}

fn charge_size(logical: u64) -> u64 {
    logical
        .div_ceil(ALLOCATION)
        .saturating_mul(ALLOCATION)
        .max(ALLOCATION)
}

/// Bytes a file holds on disk: rounded logical size or allocated blocks, whichever is larger.
fn charge(meta: &std::fs::Metadata) -> u64 {
    #[cfg(unix)]
    let allocated = {
        use std::os::unix::fs::MetadataExt;
        meta.blocks().saturating_mul(512)
    };
    #[cfg(not(unix))]
    let allocated = 0;
    charge_size(meta.len()).max(allocated)
}

fn private_directory(path: &Path) -> io::Result<()> {
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(path)
}

fn private_file() -> std::fs::OpenOptions {
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options
}

fn locked<T>(mutex: &Mutex<T>) -> io::Result<MutexGuard<'_, T>> {
    mutex
        .lock()
        .map_err(|_| io::Error::other("mirror accounting unavailable"))
}

/// Read side, used by [`super::ContentStore`] before the cache and upstream.
pub(crate) struct Mirror {
    root: PathBuf,
    cap: u64,
    reads: Arc<Semaphore>,
    warned: Mutex<Option<Instant>>,
    busy: std::sync::atomic::AtomicU64,
}

impl Mirror {
    pub(crate) fn new(root: PathBuf, cap: u64, readers: usize) -> Self {
        if !root.is_dir() {
            eprintln!(
                "Mirror {} is absent; serving from the cache and upstream.",
                root.display()
            );
        }
        Self {
            root,
            cap,
            reads: Arc::new(Semaphore::new(readers.max(16))),
            warned: Mutex::new(None),
            busy: std::sync::atomic::AtomicU64::new(0),
        }
    }

    pub(super) async fn probe_ipfs(
        &self,
        cid: &str,
        path: &str,
    ) -> Option<Result<super::LocalHint, ContentError>> {
        let root = car::parse_cid(cid).ok()?;
        let segments = path::split(path).ok()?;
        let dir = self.root.clone();
        let cap = self.cap;
        self.settle(
            self.blocking(move || {
                let size = car::probe_mirrored_plan(root, &segments, cap, &mut |cid| {
                    if cid.hash().code() == 0 {
                        let bytes = cid.hash().digest().to_vec();
                        return Ok(Some((bytes.clone(), bytes.len() as u64)));
                    }
                    Ok(peek_object(
                        &object_path(&dir, Ns::Ipfs, &ipfs_key(cid)),
                        cap.saturating_add(1024),
                        if cid.codec() == 0x55 { 0 } else { 1 << 20 },
                    )?)
                })?;
                Ok(size.map(|(body_bytes, leaf)| super::LocalHint {
                    body_bytes,
                    scratch_bytes: body_bytes.saturating_mul(2).saturating_add(
                        leaf.as_ref().map_or(32 << 20, |leaf| {
                            if leaf.cid.codec() == 0x55 {
                                64 << 10
                            } else {
                                leaf.block_bytes + (256 << 10)
                            }
                        }),
                    ),
                    cache_keys: vec![],
                    mirror_leaf: leaf,
                }))
            })
            .await,
        )
    }

    pub(super) async fn ipfs_capped(
        &self,
        cid: &str,
        path: &str,
        body_cap: u64,
        leaf: Option<car::LeafPlan>,
    ) -> Option<Result<Content, ContentError>> {
        let root = car::parse_cid(cid).ok()?;
        let segments = path::split(path).ok()?;
        let dir = self.root.clone();
        let cap = self.cap.min(body_cap);
        let result = self
            .blocking(move || {
                if let Some(leaf) = leaf {
                    let bytes = if leaf.cid.hash().code() == 0 {
                        Some(leaf.cid.hash().digest().to_vec())
                    } else {
                        read_object(
                            &object_path(&dir, Ns::Ipfs, &ipfs_key(&leaf.cid)),
                            leaf.block_bytes,
                        )?
                    };
                    let Some(bytes) = bytes else { return Ok(None) };
                    let body = car::mirrored_leaf(&leaf, bytes, cap)?;
                    let filename = if leaf.directory {
                        "index.html"
                    } else {
                        segments
                            .last()
                            .map_or("", |segment| segment.decoded.as_str())
                    };
                    return Ok(Some(Content {
                        delivery_accounted: false,
                        content_type: ipfs::mime(filename, &body),
                        body: Bytes::from(body),
                        content_encoding: None,
                        verification: Verification::IpfsCar,
                        directory_index: leaf.directory,
                        from_cache: true,
                        etag: format!("\"ipfs-{}\"", leaf.cid),
                    }));
                }
                ipfs_lookup(&dir, root, &segments, cap)
            })
            .await;
        self.settle(result)
    }

    pub(super) async fn probe_onchfs(
        &self,
        cid: &str,
        path: &str,
    ) -> Option<Result<super::LocalHint, ContentError>> {
        let root = onchfs::parse_cid(cid).ok()?;
        let segments = path::split(path).ok()?;
        let dir = self.root.clone();
        let cap = self.cap;
        self.settle(
            self.blocking(move || {
                onchfs::probe_records(root, &segments, cap, &mut |cid| {
                    peek_object(
                        &object_path(&dir, Ns::Onchfs, &hex::encode(cid)),
                        cap.saturating_add(RECORD_OVERHEAD).saturating_add(5),
                        (1 << 20) + 5,
                    )
                })
            })
            .await,
        )
    }

    pub(super) async fn onchfs_capped(
        &self,
        cid: &str,
        path: &str,
        body_cap: u64,
    ) -> Option<Result<Resolved, ContentError>> {
        let root = onchfs::parse_cid(cid).ok()?;
        let segments = path::split(path).ok()?;
        let dir = self.root.clone();
        let cap = self.cap.min(body_cap);
        let result = self
            .blocking(move || {
                let mut load = |cid: &Hash| {
                    let path = object_path(&dir, Ns::Onchfs, &hex::encode(cid));
                    read_object(&path, cap.saturating_add(RECORD_OVERHEAD).saturating_add(5))
                        .map(|found| found.map(Bytes::from))
                };
                onchfs::resolve_mirrored_records(root, &segments, cap, &mut load)
            })
            .await;
        self.settle(result)
    }

    pub(super) fn busy_count(&self) -> u64 {
        self.busy.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn settle<T>(
        &self,
        result: Result<Option<T>, ContentError>,
    ) -> Option<Result<T, ContentError>> {
        match result {
            Ok(Some(found)) => Some(Ok(found)),
            Ok(None) => None,
            // A verified directory proves the absence, as a verified fetch would.
            Err(error) if error.kind == ErrorKind::NotFound => Some(Err(error)),
            Err(error) => {
                self.warn(&error);
                None
            }
        }
    }

    fn warn(&self, error: &ContentError) {
        if let Ok(mut last) = self.warned.lock() {
            if last.is_none_or(|at| at.elapsed() >= WARN_EVERY) {
                *last = Some(Instant::now());
                eprintln!("Mirror read failed; falling back: {error}");
            }
        }
    }

    async fn blocking<T: Send + 'static>(
        &self,
        work: impl FnOnce() -> Result<Option<T>, ContentError> + Send + 'static,
    ) -> Result<Option<T>, ContentError> {
        let permit = tokio::time::timeout(READ_WAIT, self.reads.clone().acquire_owned())
            .await
            .map_err(|_| {
                self.busy.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                ContentError::unavailable("mirror readers busy")
            })?
            .map_err(|_| ContentError::unavailable("mirror readers closed"))?;
        tokio::task::spawn_blocking(move || {
            // Cancellation keeps the permit until the read and its buffers end.
            let _permit = permit;
            work()
        })
        .await
        .map_err(|_| ContentError::unavailable("mirror reader failed"))?
    }
}

fn index_path(segments: &[Segment]) -> Vec<Segment> {
    let mut index = segments.to_vec();
    index.push(Segment {
        raw: "index.html".into(),
        decoded: "index.html".into(),
    });
    index
}

fn ipfs_lookup(
    dir: &Path,
    root: Cid,
    segments: &[Segment],
    cap: u64,
) -> Result<Option<Content>, ContentError> {
    let mut load = |cid: &Cid| -> Result<Option<Vec<u8>>, ContentError> {
        Ok(read_object(
            &object_path(dir, Ns::Ipfs, &ipfs_key(cid)),
            cap.saturating_add(4 << 20),
        )?)
    };
    let (body, directory, file_cid) =
        match car::resolve_mirrored_with(root, segments, cap, &mut load)? {
            None => return Ok(None),
            Some((Outcome::File(body), cid)) => (body, false, cid),
            Some((Outcome::Directory, _)) => {
                match car::resolve_mirrored_with(root, &index_path(segments), cap, &mut load)? {
                    None => return Ok(None),
                    Some((Outcome::File(body), cid)) => (body, true, cid),
                    Some((Outcome::Directory, _)) => {
                        return Err(ContentError::invalid("directory index is not a file"));
                    }
                }
            }
        };
    let filename = if directory {
        "index.html"
    } else {
        segments
            .last()
            .map(|s| s.decoded.as_str())
            .unwrap_or_default()
    };
    let content_type = ipfs::mime(filename, &body);
    Ok(Some(Content {
        delivery_accounted: false,
        etag: format!("\"ipfs-{file_cid}\""),
        body: Bytes::from(body),
        content_type,
        content_encoding: None,
        verification: Verification::IpfsCar,
        directory_index: directory,
        from_cache: true,
    }))
}

/// Mirroring one package.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PackageStats {
    /// Files of the package and their bytes, as served (shared files counted per path).
    pub files: u64,
    pub bytes: u64,
    /// Bytes this package added to the mirror.
    pub new_bytes: u64,
    /// Complete from stored objects, without upstream requests.
    pub local: bool,
}

/// Why a package is not mirrored; nothing of it was recorded as complete.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PackageError {
    /// Enough upstream answers in this attempt said the root or a block does not exist.
    #[error("not found: {0}")]
    NotFound(String),
    /// Timeout, 429/5xx, breaker open or an interrupted transfer: retry later.
    #[error("{message}")]
    Transient {
        message: String,
        retry_after: Option<Duration>,
    },
    /// Data that cannot be mirrored: hash mismatches, unsupported layouts, bad roots.
    #[error("invalid: {0}")]
    Invalid(String),
    /// Beyond the bytes left in the mirror, or a file beyond the object limit.
    #[error("too big: {message}")]
    TooBig { message: String, bytes: u64 },
    /// A limited attempt staged more than its limit, or more than the mirror has left:
    /// `bytes` were seen, a verified lower bound of what the package needs. Not a gateway
    /// failure; retry with a larger limit.
    /// `cap`: the bytes left in the mirror stopped it, not the limit.
    #[error("waiting: {message}")]
    Waiting {
        message: String,
        bytes: u64,
        cap: bool,
    },
    /// Room is held by other packages in flight.
    #[error("deferred: {0}")]
    Deferred(String),
    /// The mirror directory failed; stop writing.
    #[error("mirror i/o: {0}")]
    Local(String),
}

fn local(error: impl std::fmt::Display) -> PackageError {
    PackageError::Local(error.to_string())
}

fn transient(message: impl Into<String>) -> PackageError {
    PackageError::Transient {
        message: message.into(),
        retry_after: None,
    }
}

fn file_too_big(bytes: u64) -> PackageError {
    PackageError::TooBig {
        message: "a file exceeds the object limit".into(),
        bytes,
    }
}

#[derive(Default)]
struct Budget {
    used: u64,
    reserved: u64,
    objects: u64,
}

/// The one writer of a mirror directory, with byte accounting against its cap.
pub struct MirrorWriter {
    root: PathBuf,
    max_bytes: u64,
    budget: Mutex<Budget>,
    seq: AtomicU64,
    _lease: Mutex<rusqlite::Connection>,
}

impl MirrorWriter {
    /// Takes the directory lease (another writer is refused), clears leftover staging and
    /// measures the stored objects.
    pub fn open(root: &Path, max_bytes: u64) -> io::Result<Self> {
        private_directory(root)?;
        let lease = lease(root)?;
        let tmp = root.join("tmp");
        match std::fs::remove_dir_all(&tmp) {
            Err(error) if error.kind() != io::ErrorKind::NotFound => return Err(error),
            _ => {}
        }
        for dir in [tmp, root.join(Ns::Ipfs.dir()), root.join(Ns::Onchfs.dir())] {
            private_directory(&dir)?;
        }
        let (used, objects) = measure(root)?;
        Ok(Self {
            root: root.to_path_buf(),
            max_bytes,
            budget: Mutex::new(Budget {
                used,
                reserved: 0,
                objects,
            }),
            seq: AtomicU64::new(0),
            _lease: Mutex::new(lease),
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn max_bytes(&self) -> u64 {
        self.max_bytes
    }

    /// Bytes the stored objects hold on disk.
    pub fn used_bytes(&self) -> u64 {
        self.budget.lock().map_or(u64::MAX, |budget| budget.used)
    }

    pub fn objects(&self) -> u64 {
        self.budget.lock().map_or(0, |budget| budget.objects)
    }

    /// Bytes neither stored nor held by packages in flight.
    pub fn remaining(&self) -> u64 {
        self.budget.lock().map_or(0, |budget| {
            self.max_bytes
                .saturating_sub(budget.used.saturating_add(budget.reserved))
        })
    }
}

/// SQLite releases the exclusive lock when the process dies; never unlink the file.
fn lease(root: &Path) -> io::Result<rusqlite::Connection> {
    use rusqlite::{Connection, ErrorCode, OpenFlags};
    let path = root.join("lock");
    match private_file().open(&path) {
        Ok(file) => drop(file),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error),
    }
    if !std::fs::symlink_metadata(&path)?.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "mirror lock must be a regular file",
        ));
    }
    let connection = Connection::open_with_flags(
        &path,
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_NO_MUTEX
            | OpenFlags::SQLITE_OPEN_NOFOLLOW,
    )
    .map_err(io::Error::other)?;
    connection
        .busy_timeout(Duration::ZERO)
        .map_err(io::Error::other)?;
    match connection.execute_batch("BEGIN EXCLUSIVE;") {
        Ok(()) => Ok(connection),
        Err(rusqlite::Error::SqliteFailure(error, _))
            if matches!(
                error.code,
                ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked
            ) =>
        {
            Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "another mirror job is writing this directory",
            ))
        }
        Err(error) => Err(io::Error::other(error)),
    }
}

fn measure(root: &Path) -> io::Result<(u64, u64)> {
    let unexpected = || io::Error::new(io::ErrorKind::InvalidData, "unexpected mirror entry");
    let (mut used, mut objects) = (0u64, 0u64);
    for ns in [Ns::Ipfs, Ns::Onchfs] {
        for shard in std::fs::read_dir(root.join(ns.dir()))? {
            let shard = shard?.path();
            if !shard.symlink_metadata()?.is_dir() {
                return Err(unexpected());
            }
            for entry in std::fs::read_dir(&shard)? {
                let meta = entry?.path().symlink_metadata()?;
                if !meta.is_file() {
                    return Err(unexpected());
                }
                used = used.saturating_add(charge(&meta));
                objects += 1;
            }
        }
    }
    Ok((used, objects))
}

enum StageError {
    Full {
        alone: bool,
        bytes: u64,
        limited: bool,
    },
    /// Staged bytes passed the package limit.
    Over(u64),
    Io(io::Error),
}

impl StageError {
    fn package(self) -> PackageError {
        match self {
            StageError::Full {
                alone: true,
                bytes,
                limited: true,
            } => PackageError::Waiting {
                message: "package exceeds the bytes left in the mirror".into(),
                bytes,
                cap: true,
            },
            StageError::Full {
                alone: true, bytes, ..
            } => PackageError::TooBig {
                message: "package exceeds the bytes left in the mirror".into(),
                bytes,
            },
            StageError::Full { alone: false, .. } => PackageError::Deferred(
                "the rest of the mirror cap is held by packages in flight".into(),
            ),
            StageError::Over(bytes) => PackageError::Waiting {
                message: "package exceeds the limit of this pass".into(),
                bytes,
                cap: false,
            },
            StageError::Io(error) => local(error),
        }
    }
}

/// No per-package limit.
const UNLIMITED: u64 = u64::MAX;
/// Response bytes one size probe may receive, across its requests; also its block bound.
const PROBE_BYTES: u64 = 2 << 20;
/// Requests one size probe may send.
const PROBE_REQUESTS: usize = 2;
const PROBE_WAIT: Duration = Duration::from_secs(60);

#[derive(Default)]
struct Staged {
    reserved: u64,
    /// Bytes staged, as received.
    logical: u64,
    objects: HashMap<Key, u64>,
}

/// One package's verified objects before they move into the store. Dropping it removes
/// whatever was not committed and releases its reservation.
struct Staging {
    writer: Arc<MirrorWriter>,
    dir: PathBuf,
    /// Bytes the package may stage; [`UNLIMITED`] for none.
    limit: u64,
    state: Mutex<Staged>,
}

impl Drop for Staging {
    fn drop(&mut self) {
        if let (Ok(state), Ok(mut budget)) = (self.state.lock(), self.writer.budget.lock()) {
            budget.reserved = budget.reserved.saturating_sub(state.reserved);
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

impl Staging {
    fn new(writer: &Arc<MirrorWriter>, limit: u64) -> io::Result<Arc<Self>> {
        let n = writer.seq.fetch_add(1, Ordering::Relaxed);
        let dir = writer
            .root
            .join("tmp")
            .join(format!("{}-{n}", std::process::id()));
        private_directory(&dir)?;
        Ok(Arc::new(Self {
            writer: writer.clone(),
            dir,
            limit,
            state: Mutex::new(Staged::default()),
        }))
    }

    fn staged_path(&self, ns: Ns, key: &str) -> PathBuf {
        self.dir.join(format!("{}-{key}", ns.dir()))
    }

    fn is_staged(&self, ns: Ns, key: &str) -> io::Result<bool> {
        Ok(locked(&self.state)?
            .objects
            .contains_key(&(ns, key.to_owned())))
    }

    /// Stored copies are verified when the package is walked.
    fn is_stored(&self, ns: Ns, key: &str) -> bool {
        object_path(&self.writer.root, ns, key).is_file()
    }

    /// Whether `extra` more bytes would pass the package limit.
    fn exceeds(&self, extra: u64) -> io::Result<bool> {
        Ok(locked(&self.state)?.logical.saturating_add(extra) > self.limit)
    }

    /// Stages verified bytes once; refuses what would pass the package limit or the mirror
    /// cap.
    fn put(&self, ns: Ns, key: &str, parts: &[&[u8]]) -> Result<(), StageError> {
        let logical: u64 = parts.iter().map(|part| part.len() as u64).sum();
        let charge = charge_size(logical);
        {
            let mut state = locked(&self.state).map_err(StageError::Io)?;
            if state.objects.contains_key(&(ns, key.to_owned())) {
                return Ok(());
            }
            let seen = state.logical.saturating_add(logical);
            if seen > self.limit {
                return Err(StageError::Over(seen));
            }
            let mut budget = locked(&self.writer.budget).map_err(StageError::Io)?;
            if budget
                .used
                .saturating_add(budget.reserved)
                .saturating_add(charge)
                > self.writer.max_bytes
            {
                let bytes = state.reserved.saturating_add(charge);
                return Err(StageError::Full {
                    alone: budget.used.saturating_add(bytes) > self.writer.max_bytes,
                    bytes,
                    limited: self.limit != UNLIMITED,
                });
            }
            budget.reserved += charge;
            state.reserved += charge;
            state.logical = seen;
            state.objects.insert((ns, key.to_owned()), charge);
        }
        let path = self.staged_path(ns, key);
        let written = private_file()
            .open(&path)
            .and_then(|mut file| parts.iter().try_for_each(|part| file.write_all(part)));
        if let Err(error) = written {
            let _ = std::fs::remove_file(&path);
            if let (Ok(mut state), Ok(mut budget)) =
                (locked(&self.state), locked(&self.writer.budget))
            {
                if let Some(charge) = state.objects.remove(&(ns, key.to_owned())) {
                    state.reserved -= charge;
                    state.logical -= logical;
                    budget.reserved = budget.reserved.saturating_sub(charge);
                }
            }
            return Err(StageError::Io(error));
        }
        Ok(())
    }

    /// The staged copy, else the stored one.
    fn read(&self, ns: Ns, key: &str, cap: u64) -> io::Result<Option<Vec<u8>>> {
        if self.is_staged(ns, key)? {
            return read_object(&self.staged_path(ns, key), cap);
        }
        read_object(&object_path(&self.writer.root, ns, key), cap)
    }

    /// Moves the package's staged objects into the store, root last, and charges them.
    /// Returns the bytes added.
    fn commit(&self, objects: &[Key]) -> io::Result<u64> {
        let mut state = locked(&self.state)?;
        let mut budget = locked(&self.writer.budget)?;
        let mut added = 0u64;
        for key in objects.iter().rev() {
            let Some(reserved) = state.objects.remove(key) else {
                continue;
            };
            state.reserved -= reserved;
            budget.reserved = budget.reserved.saturating_sub(reserved);
            let staged = self.staged_path(key.0, &key.1);
            let target = object_path(&self.writer.root, key.0, &key.1);
            let previous = match target.symlink_metadata() {
                Ok(meta) if meta.is_file() => Some(charge(&meta)),
                Ok(_) => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "mirror object is not a regular file",
                    ));
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => None,
                Err(error) => return Err(error),
            };
            if let Some(parent) = target.parent() {
                private_directory(parent)?;
            }
            let size = charge(&std::fs::metadata(&staged)?);
            // Replacing a stored copy repairs one that failed verification.
            std::fs::rename(&staged, &target)?;
            budget.used = budget.used.saturating_sub(previous.unwrap_or(0)) + size;
            if previous.is_none() {
                budget.objects += 1;
                added += size;
            }
        }
        Ok(added)
    }
}

async fn blocking<T: Send + 'static>(
    work: impl FnOnce() -> T + Send + 'static,
) -> Result<T, PackageError> {
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|_| PackageError::Local("mirror worker failed".into()))
}

/// Mirrors the package under `uri` (`ipfs://<cid>` or `onchfs://<cid>`; a path or query
/// after the root is ignored).
pub(crate) async fn package(
    ipfs: &IpfsResolver,
    onchfs: &OnchfsResolver,
    writer: &Arc<MirrorWriter>,
    uri: &str,
) -> Result<PackageStats, PackageError> {
    package_within(ipfs, onchfs, writer, uri, UNLIMITED).await
}

fn package_root(uri: &str) -> Result<(&str, &str), PackageError> {
    let invalid = || PackageError::Invalid(format!("unsupported package root {uri:?}"));
    let (scheme, rest) = uri.split_once("://").ok_or_else(invalid)?;
    let root = rest
        .split(['/', '?', '#'])
        .next()
        .filter(|root| !root.is_empty())
        .ok_or_else(invalid)?;
    Ok((scheme, root))
}

impl super::ContentStore {
    /// As [`super::ContentStore::mirror_package`], but stops once the package stages more
    /// than `limit` new bytes, or alone needs more than the mirror has left: then
    /// [`PackageError::Waiting`] with the bytes seen, and nothing of it is kept.
    pub async fn mirror_package_within(
        &self,
        uri: &str,
        writer: &Arc<MirrorWriter>,
        limit: u64,
    ) -> Result<PackageStats, PackageError> {
        package_within(&self.ipfs, &self.onchfs, writer, uri, limit).await
    }

    /// Unverified size hint of the package under `uri`, for scheduling only: an IPFS root
    /// node's own bytes plus the sizes its links declare, from the stored root or one
    /// bounded trustless block request ([`probe_root`]). `None` when unknown (ONCHFS, no
    /// answer, budget spent, undecodable root). Nothing is staged or stored.
    pub async fn mirror_estimate(&self, uri: &str, writer: &Arc<MirrorWriter>) -> Option<u64> {
        let (scheme, root) = package_root(uri).ok()?;
        if scheme != "ipfs" {
            return None;
        }
        let cid = car::normalize(car::parse_cid(root).ok()?);
        let block = if cid.hash().code() == 0x00 {
            cid.hash().digest().to_vec()
        } else {
            let path = object_path(&writer.root, Ns::Ipfs, &ipfs_key(&cid));
            let cap = self.ipfs.cap.saturating_add(1024);
            let stored = blocking(move || {
                read_object(&path, cap)
                    .ok()
                    .flatten()
                    .filter(|block| car::verify_block(cid, block).is_ok())
            })
            .await
            .ok()?;
            match stored {
                Some(block) => block,
                None => probe_root(&self.ipfs, cid).await?,
            }
        };
        estimate_root(&cid, &block)
    }
}

/// The verified root block from a `dag-scope=block` CAR, reading at most [`PROBE_BYTES`]
/// across at most [`PROBE_REQUESTS`] requests and stopping at the root, whatever else a
/// gateway sends. Every outcome leaves the breakers alone: a probe is only a hint.
async fn probe_root(resolver: &IpfsResolver, root: Cid) -> Option<Vec<u8>> {
    let max_block = usize::try_from(resolver.cap.min(PROBE_BYTES)).unwrap_or(usize::MAX);
    let mut received = 0u64;
    let mut requests = 0;
    for (index, base) in resolver.gateways.iter().enumerate() {
        if requests >= PROBE_REQUESTS || received >= PROBE_BYTES {
            break;
        }
        let Ok(_lease) = resolver.lease(index) else {
            continue;
        };
        requests += 1;
        let url = format!("{base}{root}?format=car&dag-scope=block");
        let read = async {
            let mut response = resolver.http.stream(&url, Some(CAR_TYPE)).await.ok()?;
            let answer = classify(
                response.status,
                response.content_type.as_deref(),
                response.content_encoding.is_some(),
            );
            if !matches!(answer, Answer::Car) {
                return None;
            }
            let mut parser = CarStream::new(max_block);
            while let Some(chunk) = response.chunk().await.ok()? {
                // Only bytes within the budget are parsed: a root past it stays unknown.
                let left =
                    usize::try_from(PROBE_BYTES.saturating_sub(received)).unwrap_or(usize::MAX);
                let taken = &chunk[..chunk.len().min(left)];
                received += taken.len() as u64;
                parser.push(taken);
                while let Some((cid, block)) = parser.next().ok()? {
                    if cid == root {
                        return Some(block);
                    }
                }
                if received >= PROBE_BYTES {
                    return None;
                }
            }
            None
        };
        if let Ok(Some(block)) = tokio::time::timeout(PROBE_WAIT, read).await {
            return Some(block);
        }
    }
    None
}

/// Root block bytes plus the cumulative sizes (`Tsize`) its DAG-PB links declare, or the
/// file size it declares if larger; a raw root is its own size.
fn estimate_root(cid: &Cid, block: &[u8]) -> Option<u64> {
    let own = block.len() as u64;
    if cid.codec() == 0x55 {
        return Some(own);
    }
    let declared = car::inspect(cid, block).ok()?.file_size;
    let mut links = 0u64;
    pb_fields(block, &mut |field, value| {
        if let (2, PbValue::Bytes(link)) = (field, value) {
            let mut tsize = 0;
            pb_fields(link, &mut |field, value| {
                if let (3, PbValue::Varint(size)) = (field, value) {
                    tsize = size;
                }
            })?;
            links = links.saturating_add(tsize);
        }
        Some(())
    })?;
    Some(own.saturating_add(links).max(declared))
}

enum PbValue<'a> {
    Varint(u64),
    Bytes(&'a [u8]),
}

fn pb_varint(bytes: &[u8], at: &mut usize) -> Option<u64> {
    let mut value = 0u64;
    for shift in (0..64).step_by(7) {
        let byte = *bytes.get(*at)?;
        *at += 1;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Some(value);
        }
    }
    None
}

/// Visits each top-level protobuf field; `None` on malformed input.
fn pb_fields<'a, R>(bytes: &'a [u8], visit: &mut impl FnMut(u64, PbValue<'a>) -> R) -> Option<()>
where
    R: Into<Option<()>>,
{
    let mut at = 0;
    while at < bytes.len() {
        let key = pb_varint(bytes, &mut at)?;
        let value = match key & 7 {
            0 => PbValue::Varint(pb_varint(bytes, &mut at)?),
            2 => {
                let len = usize::try_from(pb_varint(bytes, &mut at)?).ok()?;
                let end = at.checked_add(len).filter(|end| *end <= bytes.len())?;
                let value = PbValue::Bytes(&bytes[at..end]);
                at = end;
                value
            }
            _ => return None,
        };
        visit(key >> 3, value).into()?;
    }
    Some(())
}

async fn package_within(
    ipfs: &IpfsResolver,
    onchfs: &OnchfsResolver,
    writer: &Arc<MirrorWriter>,
    uri: &str,
    limit: u64,
) -> Result<PackageStats, PackageError> {
    let invalid = || PackageError::Invalid(format!("unsupported package root {uri:?}"));
    let (scheme, root) = package_root(uri)?;
    let staging = Staging::new(writer, limit).map_err(local)?;
    match scheme {
        "ipfs" => {
            let cid = car::parse_cid(root).map_err(|_| invalid())?;
            ipfs_package(ipfs, &staging, cid).await
        }
        "onchfs" => {
            let cid = onchfs::parse_cid(root).map_err(|_| invalid())?;
            onchfs_package(onchfs, &staging, cid, ipfs.cap).await
        }
        _ => Err(invalid()),
    }
}

enum Walked {
    /// An object is absent and the walk may not fetch it.
    Missing,
    Failed(PackageError),
}

impl From<PackageError> for Walked {
    fn from(error: PackageError) -> Self {
        Walked::Failed(error)
    }
}

/// What a package walk found: totals and every object it read, in walk order.
struct Walk {
    files: u64,
    bytes: u64,
    objects: Vec<Key>,
    seen: HashSet<Key>,
}

impl Walk {
    fn new() -> Self {
        Self {
            files: 0,
            bytes: 0,
            objects: Vec::new(),
            seen: HashSet::new(),
        }
    }

    fn object(&mut self, key: Key) {
        if self.seen.insert(key.clone()) {
            self.objects.push(key);
        }
    }

    fn file(&mut self, bytes: u64) -> Result<(), Walked> {
        self.files += 1;
        self.bytes = self.bytes.saturating_add(bytes);
        if self.files > MAX_FILES {
            return Err(
                PackageError::Invalid(format!("package has more than {MAX_FILES} files")).into(),
            );
        }
        Ok(())
    }

    fn stats(&self, local: bool) -> PackageStats {
        PackageStats {
            files: self.files,
            bytes: self.bytes,
            new_bytes: 0,
            local,
        }
    }
}

async fn commit(staging: &Arc<Staging>, walk: Walk) -> Result<PackageStats, PackageError> {
    let stats = walk.stats(false);
    let staging = staging.clone();
    let new_bytes = blocking(move || staging.commit(&walk.objects))
        .await?
        .map_err(local)?;
    Ok(PackageStats { new_bytes, ..stats })
}

async fn ipfs_package(
    resolver: &IpfsResolver,
    staging: &Arc<Staging>,
    root: Cid,
) -> Result<PackageStats, PackageError> {
    let root = car::normalize(root);
    match walk_ipfs(None, staging, root, resolver.cap).await {
        Ok(walk) => return Ok(walk.stats(true)),
        Err(Walked::Missing) => {}
        Err(Walked::Failed(error)) => return Err(error),
    }
    let streamed = fetch_car(resolver, staging, root, "all").await?;
    if !streamed.complete && streamed.blocks == 0 && streamed.answers.refused == 0 {
        return Err(streamed.answers.error(resolver.gateways.len()));
    }
    // Fills whatever the stream lacked, or the whole package where dag-scope=all is refused.
    match walk_ipfs(Some(resolver), staging, root, resolver.cap).await {
        Ok(walk) => commit(staging, walk).await,
        Err(Walked::Failed(error)) => Err(error),
        Err(Walked::Missing) => Err(transient("IPFS package is incomplete")),
    }
}

/// Walks the DAG under `root` through staged and stored blocks, assembling and checking
/// every file. With a resolver, a missing node is fetched once as an entity CAR.
async fn walk_ipfs(
    resolver: Option<&IpfsResolver>,
    staging: &Arc<Staging>,
    root: Cid,
    cap: u64,
) -> Result<Walk, Walked> {
    let mut walk = Walk::new();
    let mut pending = vec![(root, 0u32)];
    let mut sizes: HashMap<Cid, u64> = HashMap::new();
    let mut directories = HashSet::new();
    let mut fetched = HashSet::new();
    while let Some((cid, depth)) = pending.pop() {
        if let Some(size) = sizes.get(&cid) {
            walk.file(*size)?;
            continue;
        }
        // An inline (identity) CID holds its node: nothing to load, fetch or store.
        let inline = cid.hash().code() == 0x00;
        if cid.codec() != 0x55 {
            let info = if inline {
                car::inspect(&cid, cid.hash().digest())
                    .map_err(|error| PackageError::Invalid(error.message))?
            } else {
                loop {
                    let shared = staging.clone();
                    let read = blocking(move || -> Result<Option<car::NodeInfo>, ContentError> {
                        let key = ipfs_key(&cid);
                        let Some(block) = shared.read(Ns::Ipfs, &key, cap.saturating_add(1024))?
                        else {
                            return Ok(None);
                        };
                        if car::verify_block(cid, &block).is_err() {
                            return Ok(None);
                        }
                        car::inspect(&cid, &block).map(Some)
                    })
                    .await?;
                    match read {
                        Ok(Some(info)) => break info,
                        Ok(None) => fetch_once(resolver, staging, cid, &mut fetched).await?,
                        Err(error) if error.kind == ErrorKind::Unavailable => {
                            return Err(local(error.message).into());
                        }
                        Err(error) => return Err(PackageError::Invalid(error.message).into()),
                    }
                }
            };
            if info.kind == 1 {
                if depth >= MAX_DEPTH {
                    return Err(PackageError::Invalid("package nests too deeply".into()).into());
                }
                if !inline {
                    walk.object((Ns::Ipfs, ipfs_key(&cid)));
                }
                if directories.insert(cid) {
                    pending.extend(info.links.iter().rev().map(|link| (link.cid, depth + 1)));
                }
                continue;
            }
            if info.file_size > cap {
                return Err(file_too_big(info.file_size).into());
            }
        }
        let (size, used) = loop {
            let shared = staging.clone();
            match blocking(move || assemble(&shared, cid, cap)).await? {
                Ok(Some(found)) => break found,
                Ok(None) => fetch_once(resolver, staging, cid, &mut fetched).await?,
                Err(error) if error.kind == ErrorKind::TooLarge => {
                    return Err(file_too_big(0).into());
                }
                Err(error) if error.kind == ErrorKind::Unavailable => {
                    return Err(local(error.message).into());
                }
                Err(error) => return Err(PackageError::Invalid(error.message).into()),
            }
        };
        for key in used {
            walk.object((Ns::Ipfs, key));
        }
        sizes.insert(cid, size);
        walk.file(size)?;
    }
    Ok(walk)
}

/// Assembles one file from staged and stored blocks; a block failing verification counts as
/// absent. Returns its size and the blocks it used.
fn assemble(
    staging: &Staging,
    cid: Cid,
    cap: u64,
) -> Result<Option<(u64, Vec<String>)>, ContentError> {
    let mut used = Vec::new();
    let outcome = {
        let mut load = |block_cid: &Cid| -> Result<Option<Vec<u8>>, ContentError> {
            let key = ipfs_key(block_cid);
            match staging.read(Ns::Ipfs, &key, cap.saturating_add(1024))? {
                Some(block) if car::verify_block(*block_cid, &block).is_ok() => {
                    used.push(key);
                    Ok(Some(block))
                }
                _ => Ok(None),
            }
        };
        car::resolve_with(cid, &[], cap, &mut load)?
    };
    match outcome {
        None => Ok(None),
        Some(Outcome::File(body)) => Ok(Some((body.len() as u64, used))),
        Some(Outcome::Directory) => Err(ContentError::invalid("a file link names a directory")),
    }
}

async fn fetch_once(
    resolver: Option<&IpfsResolver>,
    staging: &Arc<Staging>,
    cid: Cid,
    fetched: &mut HashSet<Cid>,
) -> Result<(), Walked> {
    let Some(resolver) = resolver else {
        return Err(Walked::Missing);
    };
    if !fetched.insert(cid) {
        return Err(transient("gateway CAR omitted blocks").into());
    }
    fetch_entity(resolver, staging, cid)
        .await
        .map_err(Walked::Failed)
}

enum Answer {
    Car,
    NotFound,
    Refused,
    Busy,
    NotCar,
    Other,
}

fn classify(status: u16, content_type: Option<&str>, encoded: bool) -> Answer {
    let car = content_type
        .is_some_and(|kind| kind.split(';').next().unwrap_or_default().trim() == CAR_TYPE);
    match status {
        200 if car && !encoded => Answer::Car,
        200 => Answer::NotCar,
        404 | 410 => Answer::NotFound,
        // An unfollowed redirect (too many hops, another scheme, a private address): this
        // gateway cannot serve it; the others decide.
        300..=399 | 400 | 406 | 415 | 501 => Answer::Refused,
        429 | 500..=599 => Answer::Busy,
        _ => Answer::Other,
    }
}

/// Gateway answers within one attempt.
#[derive(Default)]
struct Answers {
    not_found: usize,
    refused: usize,
    invalid: usize,
    transient: usize,
    retry_after: Option<Duration>,
    last: String,
}

impl Answers {
    fn transient(&mut self, retry_after: Option<Duration>, message: impl Into<String>) {
        self.transient += 1;
        self.retry_after = self.retry_after.max(retry_after);
        self.last = message.into();
    }

    fn invalid(&mut self, message: impl Into<String>) {
        self.invalid += 1;
        self.last = message.into();
    }

    /// Absent only when at least two gateways (or the only one) able to serve a trustless
    /// CAR said so and none sent bad data; invalid only when every answer was invalid.
    fn error(&self, gateways: usize) -> PackageError {
        let voters = gateways.saturating_sub(self.refused);
        if self.not_found >= voters.clamp(1, 2) && self.invalid == 0 {
            PackageError::NotFound(format!("{} IPFS gateways answered 404", self.not_found))
        } else if self.invalid > 0 && self.transient == 0 && self.not_found == 0 {
            PackageError::Invalid(self.last.clone())
        } else if self.last.is_empty() {
            transient(if self.refused > 0 {
                "IPFS gateways refuse trustless CARs"
            } else {
                "no IPFS gateway answered"
            })
        } else {
            PackageError::Transient {
                message: self.last.clone(),
                retry_after: self.retry_after,
            }
        }
    }

    /// Records a reply without a CAR.
    fn reply(
        &mut self,
        resolver: &IpfsResolver,
        index: usize,
        status: u16,
        answer: Answer,
        retry_after: Option<Duration>,
    ) {
        match answer {
            Answer::Car => {}
            Answer::NotFound => {
                resolver.success(index);
                self.not_found += 1;
            }
            Answer::Refused => self.refused += 1,
            Answer::Busy => {
                resolver.failure(index, retry_after);
                self.transient(retry_after, format!("IPFS gateway returned {status}"));
            }
            // Plain bytes cannot be checked against the CID; they are never stored.
            Answer::NotCar => self.invalid("IPFS gateway answered without a CAR"),
            Answer::Other => self.transient(None, format!("IPFS gateway returned {status}")),
        }
    }

    fn fetch_error(&mut self, resolver: &IpfsResolver, index: usize, error: &FetchError, cap: u64) {
        if matches!(error, FetchError::Connect(_)) {
            resolver.failure(index, None);
        }
        self.transient(None, error.to_content_error("IPFS gateway", cap).message);
    }
}

/// One node's entity CAR (a directory block, or every block of a file), from the first
/// gateway that has it.
async fn fetch_entity(
    resolver: &IpfsResolver,
    staging: &Arc<Staging>,
    cid: Cid,
) -> Result<(), PackageError> {
    let fetched = fetch_car(resolver, staging, cid, "entity").await?;
    if fetched.complete {
        Ok(())
    } else {
        Err(fetched.answers.error(resolver.gateways.len()))
    }
}

struct Streamed {
    complete: bool,
    blocks: u64,
    answers: Answers,
}

enum StreamFailure {
    /// The gateway's bytes: invalid, truncated, or a section beyond the object limit.
    Gateway(ContentError),
    /// More unneeded sections or bytes than [`Transfer::check`] allows; `blame` unless the
    /// DAG outgrew the tracked blocks.
    Wasteful { message: String, blame: bool },
    /// Settled for the package by verified blocks: a file beyond the object limit, the cap,
    /// local I/O.
    Package(PackageError),
}

/// A trustless CAR of `cid` (`dag-scope=all`: the whole DAG; `entity`: one node), verified
/// block by block and staged as it arrives, from the first gateway that completes it.
async fn fetch_car(
    resolver: &IpfsResolver,
    staging: &Arc<Staging>,
    root: Cid,
    scope: &str,
) -> Result<Streamed, PackageError> {
    let mut answers = Answers::default();
    let mut blocks = 0u64;
    for (index, base) in resolver.gateways.iter().enumerate() {
        let _lease = match resolver.lease(index) {
            Ok(lease) => lease,
            Err(error) => {
                answers.transient(error.retry_after, error.message);
                continue;
            }
        };
        let url = format!("{base}{root}?format=car&dag-scope={scope}");
        let mut response = match resolver.http.stream(&url, Some(CAR_TYPE)).await {
            Ok(response) => response,
            Err(error) => {
                answers.fetch_error(resolver, index, &error, resolver.cap);
                continue;
            }
        };
        let answer = classify(
            response.status,
            response.content_type.as_deref(),
            response.content_encoding.is_some(),
        );
        if !matches!(answer, Answer::Car) {
            answers.reply(
                resolver,
                index,
                response.status,
                answer,
                response.retry_after,
            );
            continue;
        }
        // An entity is fetched because a stored copy is absent or failed verification.
        let skip_stored = scope == "all";
        let read = read_stream(
            resolver,
            staging,
            root,
            &mut response,
            &mut blocks,
            skip_stored,
        );
        match tokio::time::timeout(STREAM_LIMIT, read).await {
            Ok(Ok(())) => {
                resolver.success(index);
                return Ok(Streamed {
                    complete: true,
                    blocks,
                    answers,
                });
            }
            Ok(Err(StreamFailure::Package(error))) => return Err(error),
            // A section length is the gateway's claim, not the file's: another may serve it.
            Ok(Err(StreamFailure::Gateway(error))) if error.kind == ErrorKind::TooLarge => {
                resolver.failure(index, None);
                answers.transient(
                    None,
                    "IPFS gateway sent a CAR block beyond the object limit",
                );
            }
            Ok(Err(StreamFailure::Gateway(error))) => match error.kind {
                ErrorKind::Invalid | ErrorKind::BadRequest => answers.invalid(error.message),
                _ => answers.transient(error.retry_after, error.message),
            },
            Ok(Err(StreamFailure::Wasteful { message, blame })) => {
                if blame {
                    resolver.failure(index, None);
                }
                answers.transient(None, message);
            }
            Err(_) => answers.transient(None, "IPFS CAR transfer exceeded 30 minutes"),
        }
    }
    Ok(Streamed {
        complete: false,
        blocks,
        answers,
    })
}

async fn read_stream(
    resolver: &IpfsResolver,
    staging: &Arc<Staging>,
    root: Cid,
    response: &mut Streaming,
    blocks: &mut u64,
    skip_stored: bool,
) -> Result<(), StreamFailure> {
    let cap = resolver.cap;
    let mut parser = CarStream::new(usize::try_from(cap).unwrap_or(usize::MAX));
    let mut transfer = Transfer::new(root);
    let mut batch = Batch::default();
    loop {
        let chunk = response
            .chunk()
            .await
            .map_err(|error| StreamFailure::Gateway(error.to_content_error("IPFS gateway", cap)))?;
        let Some(chunk) = chunk else {
            break;
        };
        parser.push(&chunk);
        loop {
            let before = parser.consumed();
            let Some((cid, block)) = parser.next().map_err(StreamFailure::Gateway)? else {
                break;
            };
            *blocks += 1;
            let length = parser.consumed() - before;
            if !transfer
                .offer(cid, &block, length, cap)
                .map_err(StreamFailure::Package)?
            {
                transfer.check(parser.consumed())?;
                continue;
            }
            let key = ipfs_key(&cid);
            if !present(staging, &key, skip_stored).await?
                && (batch.push(key, block)
                    || staging
                        .exceeds(batch.logical)
                        .map_err(|error| StreamFailure::Package(local(error)))?)
            {
                // Over the package limit: staging the batch stops the transfer.
                stage_batch(staging, batch.take()).await?;
            }
            if transfer.complete() {
                // Every linked block arrived; the rest of the body could only be waste.
                return stage_batch(staging, batch.take()).await;
            }
        }
        transfer.check(parser.consumed())?;
    }
    stage_batch(staging, batch.take()).await?;
    parser.finish().map_err(StreamFailure::Gateway)
}

/// One CAR transfer of a DAG: the blocks still expected and the work spent. Only blocks
/// linked from the root through received nodes are taken, each once; the rest is waste.
struct Transfer {
    /// Linked and not yet received.
    wanted: HashSet<Cid>,
    received: HashSet<Cid>,
    /// More than [`TRACKED`] blocks: links beyond were dropped.
    overflow: bool,
    sections: u64,
    taken: u64,
    /// Section bytes of the blocks taken.
    useful: u64,
}

impl Transfer {
    fn new(root: Cid) -> Self {
        let mut transfer = Self {
            wanted: HashSet::new(),
            received: HashSet::new(),
            overflow: false,
            sections: 0,
            taken: 0,
            useful: 0,
        };
        transfer.want(car::normalize(root));
        transfer
    }

    /// Gateways omit inline (identity) blocks; their links are wanted at once.
    fn want(&mut self, cid: Cid) {
        let mut pending = vec![cid];
        while let Some(cid) = pending.pop() {
            if cid.hash().code() == 0x00 {
                if let Ok(info) = car::inspect(&cid, cid.hash().digest()) {
                    pending.extend(info.links.iter().map(|link| link.cid));
                }
                continue;
            }
            if self.received.contains(&cid) || self.wanted.contains(&cid) {
                continue;
            }
            if self.wanted.len() + self.received.len() >= TRACKED {
                self.overflow = true;
                continue;
            }
            self.wanted.insert(cid);
        }
    }

    /// Whether to keep a verified block of `length` section bytes. A file node of the DAG
    /// declaring more than the object limit settles the package as too big.
    fn offer(
        &mut self,
        cid: Cid,
        block: &[u8],
        length: u64,
        cap: u64,
    ) -> Result<bool, PackageError> {
        self.sections += 1;
        if !self.wanted.remove(&cid) {
            return Ok(false);
        }
        self.received.insert(cid);
        self.taken += 1;
        self.useful = self.useful.saturating_add(length);
        // An undecodable node is left to the walk, which reports it.
        if let Ok(info) = car::inspect(&cid, block) {
            if info.kind == 2 && info.file_size > cap {
                return Err(file_too_big(info.file_size));
            }
            for link in info.links {
                self.want(link.cid);
            }
        }
        Ok(true)
    }

    fn complete(&self) -> bool {
        self.wanted.is_empty() && !self.overflow
    }

    /// Stops a gateway whose CAR carries more waste than the DAG it delivered allows.
    fn check(&self, consumed: u64) -> Result<(), StreamFailure> {
        let sections = self.sections - self.taken;
        let bytes = consumed.saturating_sub(self.useful);
        if sections > WASTE_SECTIONS.saturating_add(self.taken)
            || bytes > WASTE_BYTES.saturating_add(self.useful)
        {
            return Err(StreamFailure::Wasteful {
                message: format!(
                    "IPFS gateway sent {sections} unneeded CAR blocks ({bytes} bytes)"
                ),
                blame: !self.overflow,
            });
        }
        Ok(())
    }
}

/// Verified blocks waiting to be staged.
#[derive(Default)]
struct Batch {
    blocks: Vec<(String, Vec<u8>)>,
    held: usize,
    /// Block bytes held.
    logical: u64,
}

impl Batch {
    /// Keeps a block; true once the batch is due for staging.
    fn push(&mut self, key: String, block: Vec<u8>) -> bool {
        self.held += block.capacity() + key.capacity() + ENTRY_OVERHEAD;
        self.logical += block.len() as u64;
        self.blocks.push((key, block));
        self.held >= BATCH_BYTES || self.blocks.len() >= BATCH_BLOCKS
    }

    fn take(&mut self) -> Vec<(String, Vec<u8>)> {
        self.held = 0;
        self.logical = 0;
        std::mem::take(&mut self.blocks)
    }
}

/// Staged by this package already, or stored and `stored` counts (a whole-DAG transfer; an
/// entity is fetched because its stored copy is absent or failed verification).
async fn present(staging: &Arc<Staging>, key: &str, stored: bool) -> Result<bool, StreamFailure> {
    if staging
        .is_staged(Ns::Ipfs, key)
        .map_err(|error| StreamFailure::Package(local(error)))?
    {
        return Ok(true);
    }
    if !stored {
        return Ok(false);
    }
    let shared = staging.clone();
    let key = key.to_owned();
    blocking(move || shared.is_stored(Ns::Ipfs, &key))
        .await
        .map_err(StreamFailure::Package)
}

async fn stage_batch(
    staging: &Arc<Staging>,
    batch: Vec<(String, Vec<u8>)>,
) -> Result<(), StreamFailure> {
    if batch.is_empty() {
        return Ok(());
    }
    let shared = staging.clone();
    blocking(move || {
        batch
            .iter()
            .try_for_each(|(key, block)| shared.put(Ns::Ipfs, key, &[block]))
    })
    .await
    .map_err(StreamFailure::Package)?
    .map_err(|error| StreamFailure::Package(error.package()))
}

async fn onchfs_package(
    resolver: &OnchfsResolver,
    staging: &Arc<Staging>,
    root: Hash,
    cap: u64,
) -> Result<PackageStats, PackageError> {
    match walk_onchfs(None, staging, root, cap).await {
        Ok(walk) => return Ok(walk.stats(true)),
        Err(Walked::Missing) => {}
        Err(Walked::Failed(error)) => return Err(error),
    }
    match walk_onchfs(Some(resolver), staging, root, cap).await {
        Ok(walk) => commit(staging, walk).await,
        Err(Walked::Failed(error)) => Err(error),
        Err(Walked::Missing) => Err(transient("ONCHFS package is incomplete")),
    }
}

fn onchfs_error(error: ContentError) -> PackageError {
    match error.kind {
        ErrorKind::NotFound => PackageError::NotFound(error.message),
        ErrorKind::Unavailable => PackageError::Transient {
            message: error.message,
            retry_after: error.retry_after,
        },
        ErrorKind::TooLarge => file_too_big(0),
        ErrorKind::Invalid | ErrorKind::BadRequest => PackageError::Invalid(error.message),
    }
}

/// Walks the inode tree under `root` through staged and stored records; with a resolver,
/// missing inodes are read from the chain and staged.
async fn walk_onchfs(
    resolver: Option<&OnchfsResolver>,
    staging: &Arc<Staging>,
    root: Hash,
    cap: u64,
) -> Result<Walk, Walked> {
    let mut walk = Walk::new();
    let mut pending = vec![(root, 0u32)];
    let mut sizes: HashMap<Hash, u64> = HashMap::new();
    let mut directories = HashSet::new();
    while let Some((cid, depth)) = pending.pop() {
        if let Some(size) = sizes.get(&cid) {
            walk.file(*size)?;
            continue;
        }
        let key = hex::encode(cid);
        let shared = staging.clone();
        let stored_key = key.clone();
        let stored = blocking(move || {
            shared
                .read(
                    Ns::Onchfs,
                    &stored_key,
                    cap.saturating_add(RECORD_OVERHEAD).saturating_add(5),
                )
                .map(|raw| raw.and_then(|raw| Record::decode(Bytes::from(raw), &cid)))
        })
        .await?
        .map_err(local)?;
        let record = match (stored, resolver) {
            (Some(record), _) => record,
            (None, None) => return Err(Walked::Missing),
            (None, Some(resolver)) => {
                let record = resolver
                    .record(cid, cid == root)
                    .await
                    .map_err(onchfs_error)?;
                let parts = record.encode();
                let shared = staging.clone();
                let staged_key = key.clone();
                blocking(move || {
                    let parts: Vec<&[u8]> = parts.iter().map(|part| part.as_ref()).collect();
                    shared.put(Ns::Onchfs, &staged_key, &parts)
                })
                .await?
                .map_err(StageError::package)?;
                record
            }
        };
        walk.object((Ns::Onchfs, key));
        match record {
            Record::Directory(entries) => {
                if depth >= MAX_DEPTH {
                    return Err(PackageError::Invalid("package nests too deeply".into()).into());
                }
                if directories.insert(cid) {
                    pending.extend(entries.values().rev().map(|child| (*child, depth + 1)));
                }
            }
            Record::File { content, .. } => {
                let size = content.len() as u64;
                if size > cap {
                    return Err(file_too_big(size).into());
                }
                sizes.insert(cid, size);
                walk.file(size)?;
            }
        }
    }
    Ok(walk)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};

    const CAP: u64 = 1 << 20;

    fn field(tag: u8, bytes: &[u8]) -> Vec<u8> {
        assert!(bytes.len() < 128);
        let mut out = vec![tag, bytes.len() as u8];
        out.extend(bytes);
        out
    }

    fn cid(codec: u64, hash: u64, bytes: &[u8]) -> Cid {
        let digest = match hash {
            0x12 => Sha256::digest(bytes).to_vec(),
            _ => bytes.to_vec(),
        };
        Cid::new_v1(
            codec,
            cid::multihash::Multihash::wrap(hash, &digest).unwrap(),
        )
    }

    fn directory(name: &str, child: Cid) -> Vec<u8> {
        let mut link = field(10, &child.to_bytes());
        link.extend(field(18, name.as_bytes()));
        let mut node = field(18, &link);
        node.extend(field(10, &[8, 1]));
        node
    }

    #[test]
    fn transfer_takes_linked_blocks_once_and_stops_duplicates_and_unlinked_blocks() {
        let leaf = b"artwork".to_vec();
        let leaf_cid = cid(0x55, 0x12, &leaf);
        let empty = cid(0x55, 0x12, b"");
        // Inline nodes are never sent: the leaf behind one is wanted at once.
        let inline = cid(0x70, 0x00, &directory("a", leaf_cid));
        let root = directory("inline", inline);
        let root_cid = cid(0x70, 0x12, &root);
        let mut transfer = Transfer::new(root_cid);
        assert!(transfer.offer(root_cid, &root, 90, CAP).unwrap());
        assert!(transfer.wanted.contains(&leaf_cid));
        assert!(
            !transfer.offer(root_cid, &root, 90, CAP).unwrap(),
            "duplicate"
        );
        assert!(!transfer.offer(empty, b"", 37, CAP).unwrap(), "unlinked");
        assert!(transfer.offer(leaf_cid, &leaf, 44, CAP).unwrap());
        assert!(transfer.complete());
        let mut consumed = 90 * 2 + 37 + 44;
        let stopped = (0..1_000_000u64).find_map(|n| {
            let (cid, block) = if n % 2 == 0 {
                (root_cid, root.as_slice())
            } else {
                (empty, &[][..])
            };
            assert!(!transfer.offer(cid, block, 37, CAP).unwrap());
            consumed += 37;
            transfer.check(consumed).err().map(|error| (n, error))
        });
        let Some((n, StreamFailure::Wasteful { blame, .. })) = stopped else {
            panic!("a wasteful gateway is stopped");
        };
        assert!(blame);
        assert_eq!(n, WASTE_SECTIONS, "two wasted before, two taken allowed");
        assert_eq!(
            (transfer.received.len(), transfer.wanted.len()),
            (2, 0),
            "waste allocates nothing"
        );
        // Large duplicates are stopped by bytes first.
        let mut transfer = Transfer::new(leaf_cid);
        assert!(transfer.offer(leaf_cid, &leaf, 44, CAP).unwrap());
        let mut consumed = 44;
        let stopped = (1..100u64).find(|_| {
            assert!(!transfer.offer(leaf_cid, &leaf, 1 << 20, CAP).unwrap());
            consumed += 1 << 20;
            transfer.check(consumed).is_err()
        });
        assert_eq!(stopped, Some(17));
    }

    #[test]
    fn only_a_linked_file_node_beyond_the_object_limit_settles_the_package() {
        let file = field(10, &[8, 2, 24, 0x80, 0x80, 0x80, 0x01]);
        let file_cid = cid(0x70, 0x12, &file);
        let root = directory("video.mp4", file_cid);
        let root_cid = cid(0x70, 0x12, &root);
        let mut unrelated = Transfer::new(cid(0x55, 0x12, b"other"));
        assert!(!unrelated.offer(file_cid, &file, 64, CAP).unwrap());
        let mut transfer = Transfer::new(root_cid);
        assert!(transfer.offer(root_cid, &root, 64, CAP).unwrap());
        assert_eq!(
            transfer.offer(file_cid, &file, 64, CAP).unwrap_err(),
            file_too_big(2 << 20)
        );
    }

    #[test]
    fn estimate_is_the_root_block_plus_its_declared_link_sizes() {
        let leaf = cid(0x55, 0x12, b"artwork");
        let mut link = field(10, &leaf.to_bytes());
        link.extend(field(18, b"index.html"));
        link.extend([24, 0x90, 0x4e]);
        let mut node = field(18, &link);
        node.extend(field(10, &[8, 1]));
        let root = cid(0x70, 0x12, &node);
        assert_eq!(
            estimate_root(&root, &node),
            Some(node.len() as u64 + 10_000)
        );
        let bare = directory("index.html", leaf);
        assert_eq!(
            estimate_root(&cid(0x70, 0x12, &bare), &bare),
            Some(bare.len() as u64),
            "undeclared sizes count nothing"
        );
        assert_eq!(estimate_root(&leaf, b"artwork"), Some(7));
        assert_eq!(
            pb_fields(&[0x12, 0x05, 1], &mut |_, _| ()),
            None,
            "truncated"
        );
    }

    #[test]
    fn batch_is_due_by_entries_or_by_held_memory() {
        let mut batch = Batch::default();
        let key = "f".repeat(72);
        let due = (1..=BATCH_BLOCKS * 2).find(|_| batch.push(key.clone(), Vec::new()));
        assert_eq!(due, Some(BATCH_BLOCKS));
        assert_eq!(batch.take().len(), BATCH_BLOCKS);
        assert_eq!(batch.held, 0);
        let size = 64 << 10;
        let due = (1..=BATCH_BLOCKS).find(|_| batch.push(key.clone(), vec![0; size]));
        assert_eq!(due, Some(BATCH_BYTES.div_ceil(size + 72 + ENTRY_OVERHEAD)));
    }

    #[test]
    fn unfollowed_redirects_refuse_and_leave_the_verdict_to_the_other_gateways() {
        assert!(matches!(classify(301, None, false), Answer::Refused));
        assert!(matches!(classify(308, None, false), Answer::Refused));
        let answers = Answers {
            refused: 1,
            not_found: 1,
            ..Answers::default()
        };
        assert!(matches!(answers.error(2), PackageError::NotFound(_)));
        let busy = Answers {
            refused: 1,
            not_found: 1,
            transient: 1,
            last: "IPFS gateway returned 503".into(),
            ..Answers::default()
        };
        assert!(matches!(busy.error(3), PackageError::Transient { .. }));
    }
    #[tokio::test]
    async fn mirror_busy_counter_increments_once_and_recovers() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tmp")
            .join(format!("mirror-busy-metrics-{}", std::process::id()));
        let mirror_root = root.join("mirror");
        std::fs::create_dir_all(&mirror_root).unwrap();
        let mut config = super::super::ContentConfig::mainnet(&root);
        config.cache_dir = root.join("cache");
        config.mirror_dir = Some(mirror_root);
        let store = super::super::ContentStore::new(config).unwrap();
        let mirror = store.mirror.as_ref().unwrap();
        let held = mirror.reads.clone().acquire_many_owned(16).await.unwrap();
        let error = mirror
            .blocking(|| Ok::<_, ContentError>(Some(1)))
            .await
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::Unavailable);
        assert_eq!(store.metrics().resolution.mirror_busy, 1);
        drop(held);
        assert_eq!(
            mirror
                .blocking(|| Ok::<_, ContentError>(Some(1)))
                .await
                .unwrap(),
            Some(1)
        );
        assert_eq!(store.metrics().resolution.mirror_busy, 1);
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn fifteen_small_mirror_reservations_leave_room_for_a_large_graph() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join(format!("tmp/mirror-small-scratch-{}", std::process::id()));
        let body = vec![42; 4096];
        let leaf = cid(0x55, 0x12, &body);
        let dir = directory("file.js", leaf);
        let dir_cid = cid(0x70, 0x12, &dir);
        for (cid, bytes) in [(leaf, body), (dir_cid, dir)] {
            let path = object_path(&root, Ns::Ipfs, &ipfs_key(&cid));
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, bytes).unwrap();
        }
        let mirror = Mirror::new(root.clone(), 96 << 20, 4);
        assert_eq!(mirror.reads.available_permits(), 16);
        let hint = mirror
            .probe_ipfs(&dir_cid.to_string(), "file.js")
            .await
            .unwrap()
            .unwrap();
        assert!(hint.mirror_leaf.is_some());
        assert!(hint.scratch_bytes < (1 << 20));
        let budget = super::super::local::Delivery::default();
        let mut small = Vec::new();
        for _ in 0..15 {
            small.push(
                budget
                    .reserve(hint.scratch_bytes as usize, budget.deadline())
                    .await
                    .unwrap(),
            );
        }
        let large = budget
            .reserve(2 * 71_000_000 + (32 << 20), budget.deadline())
            .await
            .unwrap();
        assert_eq!(budget.snapshot().waits, 0);
        let served = mirror
            .ipfs_capped(
                &dir_cid.to_string(),
                "file.js",
                hint.body_bytes,
                hint.mirror_leaf,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(served.body.len(), 4096);
        assert!(!served.delivery_accounted);
        drop((small, large));
        assert_eq!(budget.snapshot().bytes_used, 0);
        std::fs::remove_dir_all(root).unwrap();
    }
}
