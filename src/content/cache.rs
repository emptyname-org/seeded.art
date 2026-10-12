//! Content-addressed disk cache.
//!
//! Layout under the root:
//! - `<ns>/<k0k1>/<key>`: verified objects (callers re-verify on read);
//! - `absent/<ns>/<k0k1>/<key>`: confirmed absences, `permanent` or with an expiry;
//! - `tmp/`: staging files, renamed into place only when complete.
//!
//! Transient failures are never written. Keys are lowercase hex, so they
//! cannot escape the root.
//! Quota charge per file: max(4 KiB, rounded logical bytes, allocated bytes)
//! plus 4 KiB metadata reserve. Entries are capped separately; absences use at
//! most 1/8 of the byte budget. Replacement staging also reserves its charge.

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;

const ALLOCATION: u64 = 4096;
const ENTRY_OVERHEAD: u64 = 4096;
const MAX_ENTRIES: usize = 65_536;
const MAX_ABSENT_ENTRIES: usize = 4096;
const MAX_CHUNK_ENTRIES: usize = 8192;
const MAX_SCAN_ENTRIES: usize = MAX_ENTRIES * 4;
const MAX_SCAN_TIME: Duration = Duration::from_secs(10);
const EXPIRY_BATCH: usize = 8;
const EVICTION_BATCH: usize = 128;
const EVICTION_TIME: Duration = Duration::from_secs(2);

#[derive(Clone, Copy)]
struct Charge {
    bytes: u64,
    absent: bool,
    chunk: bool,
}

struct Entry {
    charge: Charge,
    touched: u64,
}

enum Room {
    Ready,
    Progress,
    Full,
}

#[derive(Debug, Clone, Copy, serde::Serialize)]
pub struct CacheStats {
    pub bytes_used: u64,
    pub bytes_quota: u64,
    pub entries: usize,
    pub evictions: u64,
}

#[derive(Default)]
struct Budget {
    bytes: u64,
    absent_bytes: u64,
    chunk_bytes: u64,
    chunks: usize,
    entries: HashMap<PathBuf, Entry>,
    absences: VecDeque<PathBuf>,
    reservations: HashMap<PathBuf, Charge>,
    order: BTreeSet<(u64, PathBuf)>,
    absent_order: BTreeSet<(u64, PathBuf)>,
    chunk_order: BTreeSet<(u64, PathBuf)>,
    clock: u64,
    evictions: u64,
    evict_root: Option<PathBuf>,
}

impl Budget {
    fn record(&mut self, path: PathBuf, charge: Charge) {
        self.forget(&path);
        self.bytes = self.bytes.saturating_add(charge.bytes);
        if charge.absent {
            self.absent_bytes = self.absent_bytes.saturating_add(charge.bytes);
            self.absences.push_back(path.clone());
        }
        if charge.chunk {
            self.chunk_bytes = self.chunk_bytes.saturating_add(charge.bytes);
            self.chunks += 1;
        }
        self.clock = self.clock.saturating_add(1);
        self.order_insert(&path, charge, self.clock);
        self.entries.insert(
            path,
            Entry {
                charge,
                touched: self.clock,
            },
        );
    }

    fn forget(&mut self, path: &Path) {
        if let Some(entry) = self.entries.remove(path) {
            self.order_remove(path, entry.touched);
            let charge = entry.charge;
            self.bytes = self.bytes.saturating_sub(charge.bytes);
            if charge.absent {
                self.absent_bytes = self.absent_bytes.saturating_sub(charge.bytes);
                self.absences.retain(|entry| entry != path);
            }
            if charge.chunk {
                self.chunk_bytes = self.chunk_bytes.saturating_sub(charge.bytes);
                self.chunks -= 1;
            }
        }
    }

    fn touch(&mut self, path: &Path) {
        if let Some(entry) = self.entries.get_mut(path) {
            let old = entry.touched;
            self.clock = self.clock.saturating_add(1);
            entry.touched = self.clock;
            let charge = entry.charge;
            self.order_remove(path, old);
            self.order_insert(path, charge, self.clock);
        }
    }

    fn order_insert(&mut self, path: &Path, charge: Charge, touched: u64) {
        if self
            .evict_root
            .as_ref()
            .is_some_and(|root| !eviction_path(root, path))
        {
            return;
        }
        let key = (touched, path.to_path_buf());
        self.order.insert(key.clone());
        if charge.absent {
            self.absent_order.insert(key.clone());
        }
        if charge.chunk {
            self.chunk_order.insert(key);
        }
    }

    fn order_remove(&mut self, path: &Path, touched: u64) {
        let key = (touched, path.to_path_buf());
        self.order.remove(&key);
        self.absent_order.remove(&key);
        self.chunk_order.remove(&key);
    }

    fn absent_fits(&self, path: &Path, charge: Charge, maximum: u64) -> bool {
        let pending = self.reservations.values().filter(|c| c.absent);
        !charge.absent
            || (self
                .absent_bytes
                .saturating_add(pending.clone().map(|c| c.bytes).sum())
                .saturating_add(charge.bytes)
                <= maximum / 8
                && (self.entries.get(path).is_some_and(|old| old.charge.absent)
                    || self.absences.len() + pending.count() < MAX_ABSENT_ENTRIES))
    }

    fn chunk_fits(&self, path: &Path, charge: Charge, maximum: u64) -> bool {
        let pending = self.reservations.values().filter(|c| c.chunk);
        !charge.chunk
            || (self
                .chunk_bytes
                .saturating_add(pending.clone().map(|c| c.bytes).sum())
                .saturating_add(charge.bytes)
                <= maximum / 4
                && (self.entries.get(path).is_some_and(|old| old.charge.chunk)
                    || self.chunks + pending.count() < MAX_CHUNK_ENTRIES))
    }

    fn admits(&self, path: &Path, charge: Charge, maximum: u64) -> bool {
        let pending = self.reservations.values().map(|c| c.bytes).sum::<u64>();
        // Reserve the staging file as well as the old destination until rename.
        !self.reservations.contains_key(path)
            && self
                .bytes
                .saturating_add(pending)
                .saturating_add(charge.bytes)
                <= maximum
            && (self.entries.contains_key(path)
                || self.entries.len() + self.reservations.len() < MAX_ENTRIES)
            && self.absent_fits(path, charge, maximum)
            && self.chunk_fits(path, charge, maximum)
    }

    /// Only temporary objects can be evicted. Reserve space for staging as well
    /// as existing destinations; in-progress writes and unknown files stay put.
    fn make_room(&mut self, path: &Path, charge: Charge, maximum: u64) -> io::Result<Room> {
        if self.admits(path, charge, maximum) {
            return Ok(Room::Ready);
        }
        let Some(root) = self.evict_root.clone() else {
            return Ok(Room::Full);
        };
        // A negative/chunk cache write must never displace complete objects to
        // satisfy its smaller quota. An impossible write must not flush the cache.
        let pending: u64 = self.reservations.values().map(|c| c.bytes).sum();
        let old = self.entries.get(path).map(|e| e.charge);
        let destination = old.map_or(0, |c| c.bytes);
        let pending_absent: u64 = self
            .reservations
            .values()
            .filter(|c| c.absent)
            .map(|c| c.bytes)
            .sum();
        let pending_chunks: u64 = self
            .reservations
            .values()
            .filter(|c| c.chunk)
            .map(|c| c.bytes)
            .sum();
        if self.reservations.contains_key(path)
            || pending
                .saturating_add(destination)
                .saturating_add(charge.bytes)
                > maximum
            || (charge.absent
                && pending_absent
                    .saturating_add(old.filter(|c| c.absent).map_or(0, |c| c.bytes))
                    .saturating_add(charge.bytes)
                    > maximum / 8)
            || (charge.chunk
                && pending_chunks
                    .saturating_add(old.filter(|c| c.chunk).map_or(0, |c| c.bytes))
                    .saturating_add(charge.bytes)
                    > maximum / 4)
        {
            return Ok(Room::Full);
        }
        for _ in 0..EVICTION_BATCH {
            // Satisfy sub-caps from their own LRU; total pressure can evict any kind.
            let order = if !self.absent_fits(path, charge, maximum) {
                &self.absent_order
            } else if !self.chunk_fits(path, charge, maximum) {
                &self.chunk_order
            } else {
                &self.order
            };
            let Some((_, candidate)) = order.iter().find(|(_, candidate)| {
                candidate != path && !self.reservations.contains_key(candidate)
            }) else {
                return Ok(Room::Full);
            };
            let candidate = candidate.clone();
            remove_entry(&root, &candidate, self)?;
            self.evictions = self.evictions.saturating_add(1);
            if self.admits(path, charge, maximum) {
                return Ok(Room::Ready);
            }
        }
        Ok(Room::Progress)
    }
}

pub struct DiskCache {
    root: PathBuf,
    seq: AtomicU64,
    max_entry_bytes: u64,
    max_bytes: u64,
    used: Arc<Mutex<Budget>>,
    io_slots: Arc<tokio::sync::Semaphore>,
    enabled: bool,
    read_only: bool,
}

impl DiskCache {
    /// Creates the root and removes stale staging files.
    pub fn open(root: &Path) -> io::Result<Self> {
        Self::open_with_limits(root, 2 * 1024 * 1024 * 1024, 33 * 1024 * 1024)
    }

    /// A full cache bypasses writes; fetching remains available.
    pub fn open_with_limits(root: &Path, max_bytes: u64, max_entry_bytes: u64) -> io::Result<Self> {
        let scanned = private_directory(&root.join("tmp"))
            .and_then(|()| scan(root, max_bytes, MAX_SCAN_ENTRIES));
        let (used, enabled) = match scanned {
            Ok(used) => (used, true),
            Err(_) => {
                eprintln!("Content cache bypassed: initialization or bounded accounting failed.");
                (Budget::default(), false)
            }
        };
        Ok(Self {
            root: root.to_path_buf(),
            seq: AtomicU64::new(0),
            max_entry_bytes,
            max_bytes,
            used: Arc::new(Mutex::new(used)),
            io_slots: Arc::new(tokio::sync::Semaphore::new(4)),
            enabled,
            read_only: false,
        })
    }

    /// Temporary cache: successful reads refresh LRU order, and bounded eviction
    /// admits new objects when full. Never use the preservation mirror as this root.
    /// Recency resets to scan order on restart; byte accounting is reconstructed.
    pub fn open_evicting(root: &Path, max_bytes: u64, max_entry_bytes: u64) -> io::Result<Self> {
        let cache = Self::open_with_limits(root, max_bytes, max_entry_bytes)?;
        {
            let mut budget = cache.used.lock().map_err(accounting_error)?;
            budget.evict_root = Some(root.to_path_buf());
            // Scanning accounts unknown files but must never make them eviction targets.
            budget.order.retain(|(_, path)| eviction_path(root, path));
            budget
                .absent_order
                .retain(|(_, path)| eviction_path(root, path));
            budget
                .chunk_order
                .retain(|(_, path)| eviction_path(root, path));
        }
        // A reduced quota must make progress even when more than one batch is needed.
        // Bound startup work; subsequent admissions keep trimming if this budget expires.
        let started = Instant::now();
        'trim: loop {
            let mut budget = cache.used.lock().map_err(accounting_error)?;
            for _ in 0..EVICTION_BATCH {
                if budget.bytes <= max_bytes || started.elapsed() >= MAX_SCAN_TIME {
                    break 'trim;
                }
                let Some((_, path)) = budget.order.first().cloned() else {
                    break 'trim;
                };
                if remove_entry(root, &path, &mut budget).is_err() {
                    eprintln!("Cache trim paused: entry unavailable.");
                    break 'trim;
                }
                budget.evictions = budget.evictions.saturating_add(1);
            }
            drop(budget);
            std::thread::yield_now();
        }
        Ok(cache)
    }

    /// Read existing verified entries without admitting media into the preservation cache.
    pub fn read_only(&self) -> Self {
        Self {
            root: self.root.clone(),
            seq: AtomicU64::new(0),
            max_entry_bytes: self.max_entry_bytes,
            max_bytes: self.max_bytes,
            used: self.used.clone(),
            io_slots: self.io_slots.clone(),
            enabled: self.enabled,
            read_only: true,
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    fn relative(ns: &str, key: &str) -> io::Result<PathBuf> {
        let namespace_ok = !ns.is_empty()
            && !matches!(ns, "absent" | "tmp")
            && ns.len() <= 32
            && ns
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_' || b == b'-');
        let ok = namespace_ok
            && (2..=128).contains(&key.len())
            && key
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b));
        if !ok {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cache key must be hex",
            ));
        }
        Ok(Path::new(ns).join(&key[..2]).join(key))
    }

    fn object_path(&self, ns: &str, key: &str) -> io::Result<PathBuf> {
        Ok(self.root.join(Self::relative(ns, key)?))
    }

    fn absent_path(&self, ns: &str, key: &str) -> io::Result<PathBuf> {
        Ok(self.root.join("absent").join(Self::relative(ns, key)?))
    }

    /// Refresh a retained object's backing entry without reading or hashing its bytes.
    pub(super) fn touch(&self, ns: &str, key: &str) {
        if self.enabled {
            if let Ok(path) = self.object_path(ns, key) {
                if let Ok(mut used) = self.used.try_lock() {
                    used.touch(&path);
                }
            }
        }
    }

    pub(super) fn peek_local(
        &self,
        ns: &str,
        key: &str,
        prefix_cap: usize,
    ) -> io::Result<Option<(Vec<u8>, u64)>> {
        if !self.enabled {
            return Ok(None);
        }
        let path = self.object_path(ns, key)?;
        let file = match std::fs::File::open(path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        let meta = file.metadata()?;
        if !meta.is_file() || meta.len() > self.max_entry_bytes {
            return Ok(None);
        }
        let size = meta.len();
        let limit = size.min(prefix_cap as u64);
        let mut bytes = Vec::with_capacity(limit as usize);
        file.take(limit).read_to_end(&mut bytes)?;
        Ok(Some((bytes, size)))
    }

    pub(super) fn get_local_capped(
        &self,
        ns: &str,
        key: &str,
        cap: u64,
    ) -> io::Result<Option<Vec<u8>>> {
        let path = self.object_path(ns, key)?;
        if !self.enabled {
            return Ok(None);
        }
        match read_capped(&path, self.max_entry_bytes.min(cap)) {
            Ok(bytes) => {
                self.used.lock().map_err(accounting_error)?.touch(&path);
                Ok(Some(bytes))
            }
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::NotFound | io::ErrorKind::InvalidData
                ) =>
            {
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    /// Reads an object, or `None` if it is not cached.
    pub async fn get(&self, ns: &'static str, key: &str) -> io::Result<Option<Vec<u8>>> {
        let path = self.object_path(ns, key)?;
        if !self.enabled {
            return Ok(None);
        }
        let cap = self.max_entry_bytes;
        let read_only = self.read_only;
        let used = self.used.clone();
        let root = self.root.clone();
        self.blocking(move || match read_capped(&path, cap) {
            Ok(v) => {
                used.lock().map_err(accounting_error)?.touch(&path);
                Ok(Some(v))
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) if e.kind() == io::ErrorKind::InvalidData => {
                if !read_only {
                    remove_budgeted(&root, &path, &used)?;
                }
                Ok(None)
            }
            Err(e) => Err(e),
        })
        .await
    }

    /// Writes an object atomically and clears any absence marker for it.
    pub async fn put(&self, ns: &'static str, key: &str, data: Vec<u8>) -> io::Result<()> {
        self.put_parts(ns, key, vec![Bytes::from(data)]).await
    }

    /// Writes up to eight shared segments without copying the full object.
    pub async fn put_parts(
        &self,
        ns: &'static str,
        key: &str,
        parts: Vec<Bytes>,
    ) -> io::Result<()> {
        let path = self.object_path(ns, key)?;
        if !self.enabled || self.read_only {
            return Ok(());
        }
        if parts.len() > 8 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "too many cache segments",
            ));
        }
        let absent = self.absent_path(ns, key)?;
        let tmp = self.tmp_path();
        let used = self.used.clone();
        let root = self.root.clone();
        let max = self.max_bytes;
        if parts.iter().map(|part| part.len() as u64).sum::<u64>() > self.max_entry_bytes {
            return Ok(());
        }
        self.blocking(move || {
            write_budgeted(&tmp, &path, &parts, false, &used, max)?;
            remove_budgeted(&root, &absent, &used)
        })
        .await
    }

    /// Drops a corrupt or unwanted object.
    pub async fn remove(&self, ns: &'static str, key: &str) -> io::Result<()> {
        let path = self.object_path(ns, key)?;
        if !self.enabled || self.read_only {
            return Ok(());
        }
        let used = self.used.clone();
        let root = self.root.clone();
        self.blocking(move || remove_budgeted(&root, &path, &used))
            .await
    }

    /// Records a confirmed absence; `None` means it can never appear.
    pub async fn mark_absent(
        &self,
        ns: &'static str,
        key: &str,
        ttl: Option<Duration>,
    ) -> io::Result<()> {
        let path = self.absent_path(ns, key)?;
        if !self.enabled || self.read_only {
            return Ok(());
        }
        let tmp = self.tmp_path();
        let body = match ttl {
            None => "permanent\n".to_string(),
            Some(ttl) => {
                let expiry = SystemTime::now().checked_add(ttl).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "absence TTL is too large")
                })?;
                format!("until={}\n", unix_millis(expiry))
            }
        };
        let used = self.used.clone();
        let max = self.max_bytes;
        self.blocking(move || write_budgeted(&tmp, &path, &[Bytes::from(body)], true, &used, max))
            .await
    }

    /// True while a recorded absence is still in force; expired markers are removed.
    pub async fn is_absent(&self, ns: &'static str, key: &str) -> io::Result<bool> {
        let path = self.absent_path(ns, key)?;
        if !self.enabled {
            return Ok(false);
        }
        let used = self.used.clone();
        let root = self.root.clone();
        let read_only = self.read_only;
        self.blocking(move || {
            let live = absence_live(&path, SystemTime::now())?;
            if !live && !read_only {
                remove_budgeted(&root, &path, &used)?;
            }
            Ok(live)
        })
        .await
    }

    /// Charged quota bytes, including a minimum allocation and metadata reserve.
    /// This is not a claim about filesystem-wide physical allocation.
    pub fn disk_usage(&self) -> io::Result<u64> {
        if !self.enabled {
            return Err(io::Error::other(
                "cache accounting unavailable; cache bypassed",
            ));
        }
        Ok(self.used.lock().map_err(accounting_error)?.bytes)
    }

    pub fn stats(&self) -> io::Result<CacheStats> {
        if !self.enabled {
            return Err(io::Error::other(
                "cache accounting unavailable; cache bypassed",
            ));
        }
        let budget = self.used.lock().map_err(accounting_error)?;
        Ok(CacheStats {
            bytes_used: budget.bytes,
            bytes_quota: self.max_bytes,
            entries: budget.entries.len(),
            evictions: budget.evictions,
        })
    }

    /// Probe the private staging directory using the same bounded worker pool.
    /// The empty private probe is removed on success and every failure path.
    pub async fn check_writable(&self) -> io::Result<()> {
        if !self.enabled || self.read_only {
            return Err(io::Error::other("cache bypassed"));
        }
        let tmp = self.tmp_path();
        self.blocking(move || {
            let file = private_file().open(&tmp)?;
            let mut guard = Staging(&tmp, false);
            file.sync_all()?;
            drop(file);
            std::fs::remove_file(&tmp)?;
            guard.1 = true;
            Ok(())
        })
        .await
    }

    async fn blocking<T: Send + 'static>(
        &self,
        f: impl FnOnce() -> io::Result<T> + Send + 'static,
    ) -> io::Result<T> {
        let permit = tokio::time::timeout(
            Duration::from_secs(2),
            self.io_slots.clone().acquire_owned(),
        )
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::WouldBlock, "cache workers busy"))?
        .map_err(|_| io::Error::other("cache workers closed"))?;
        let used = self.used.clone();
        let root = self.root.clone();
        let read_only = self.read_only;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            if !read_only {
                let mut budget = used.lock().map_err(accounting_error)?;
                sweep_absences(&root, &mut budget, EXPIRY_BATCH)?;
            }
            f()
        })
        .await
        .map_err(io::Error::other)?
    }

    fn tmp_path(&self) -> PathBuf {
        let n = self.seq.fetch_add(1, Ordering::Relaxed);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        self.root
            .join("tmp")
            .join(format!("{}-{nanos}-{n}", std::process::id()))
    }
}

/// Removes the staging file unless it was renamed into place.
struct Staging<'a>(&'a Path, bool);

impl Drop for Staging<'_> {
    fn drop(&mut self) {
        if !self.1 {
            let _ = std::fs::remove_file(self.0);
        }
    }
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

fn remove_if_exists(path: &Path) -> io::Result<()> {
    match std::fs::remove_file(path) {
        Err(e) if e.kind() != io::ErrorKind::NotFound => Err(e),
        _ => Ok(()),
    }
}

fn unix_millis(t: SystemTime) -> u128 {
    t.duration_since(UNIX_EPOCH).unwrap_or_default().as_millis()
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

fn regular_size(path: &Path) -> io::Result<Option<u64>> {
    match std::fs::symlink_metadata(path) {
        Ok(meta) if meta.is_file() => Ok(Some(meta.len())),
        Ok(_) => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "cache entry is not a regular file",
        )),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

fn read_capped(path: &Path, cap: u64) -> io::Result<Vec<u8>> {
    let size = regular_size(path)?.unwrap_or(0);
    if size > cap {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "cache entry exceeds limit",
        ));
    }
    let mut bytes = vec![0; size as usize];
    let mut file = std::fs::File::open(path)?;
    file.read_exact(&mut bytes)?;
    if file.read(&mut [0])? != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "cache entry grew beyond limit",
        ));
    }
    Ok(bytes)
}

struct Reservation<'a> {
    path: &'a Path,
    used: &'a Mutex<Budget>,
    active: bool,
}

impl Drop for Reservation<'_> {
    fn drop(&mut self) {
        if self.active {
            if let Ok(mut budget) = self.used.lock() {
                budget.reservations.remove(self.path);
            }
        }
    }
}

fn write_budgeted(
    tmp: &Path,
    path: &Path,
    parts: &[Bytes],
    absent: bool,
    used: &Mutex<Budget>,
    max: u64,
) -> io::Result<()> {
    write_budgeted_with(tmp, path, parts, absent, used, max, |file| {
        for part in parts {
            file.write_all(part)?;
        }
        file.sync_all()
    })
}

fn write_budgeted_with(
    tmp: &Path,
    path: &Path,
    parts: &[Bytes],
    absent: bool,
    used: &Mutex<Budget>,
    max: u64,
    write: impl FnOnce(&mut std::fs::File) -> io::Result<()>,
) -> io::Result<()> {
    let predicted = Charge {
        bytes: charge_size(parts.iter().map(|part| part.len() as u64).sum(), 0),
        absent,
        chunk: !absent && is_chunk(path),
    };
    {
        let Some(mut budget) = room_for_write(used, path, predicted, max, false)? else {
            return Ok(());
        };
        budget.reservations.insert(path.to_path_buf(), predicted);
    }
    let mut reservation = Reservation {
        path,
        used,
        active: true,
    };
    let mut file = private_file().open(tmp)?;
    let mut staging = Staging(tmp, false);
    write(&mut file)?;
    let actual = Charge {
        bytes: charge_metadata(&file.metadata()?),
        absent,
        chunk: predicted.chunk,
    };
    drop(file);
    if let Some(parent) = path.parent() {
        private_directory(parent)?;
    }
    let Some(mut budget) = room_for_write(used, path, actual, max, true)? else {
        return Ok(());
    };
    reservation.active = false;
    if let Some(parent) = path.parent() {
        private_directory(parent)?;
    }
    if regular_size(path)?.is_some() && !budget.entries.contains_key(path) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "untracked cache entry",
        ));
    }
    std::fs::rename(tmp, path)?;
    staging.1 = true;
    budget.record(path.to_path_buf(), actual);
    Ok(())
}

/// Release accounting between deletion batches. Partial progress survives a timeout;
/// a later attempt can continue. The caller's destination stays reserved between batches.
fn room_for_write<'a>(
    used: &'a Mutex<Budget>,
    path: &Path,
    charge: Charge,
    max: u64,
    owns_reservation: bool,
) -> io::Result<Option<std::sync::MutexGuard<'a, Budget>>> {
    let started = Instant::now();
    loop {
        let mut budget = used.lock().map_err(accounting_error)?;
        if regular_size(path)?.is_some() && !budget.entries.contains_key(path) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "untracked cache entry",
            ));
        }
        let own = if owns_reservation {
            budget.reservations.remove(path)
        } else {
            None
        };
        let room = budget.make_room(path, charge, max);
        if matches!(room, Ok(Room::Ready)) {
            return Ok(Some(budget));
        }
        if let Some(own) = own {
            budget.reservations.insert(path.to_path_buf(), own);
        }
        match room? {
            Room::Full => return Ok(None),
            Room::Progress if started.elapsed() < EVICTION_TIME => {}
            _ => return Ok(None),
        }
        drop(budget);
        std::thread::yield_now();
    }
}

fn accounting_error<T>(_: std::sync::PoisonError<T>) -> io::Error {
    io::Error::other("cache accounting unavailable")
}

fn eviction_path(root: &Path, path: &Path) -> bool {
    let Ok(relative) = path.strip_prefix(root) else {
        return false;
    };
    let relative = relative.strip_prefix("absent").unwrap_or(relative);
    let Some(ns) = relative
        .components()
        .next()
        .and_then(|c| c.as_os_str().to_str())
    else {
        return false;
    };
    let Some(key) = relative.file_name().and_then(|name| name.to_str()) else {
        return false;
    };
    DiskCache::relative(ns, key).is_ok_and(|expected| expected == relative)
}

fn is_chunk(path: &Path) -> bool {
    path.parent()
        .and_then(Path::parent)
        .and_then(Path::file_name)
        .is_some_and(|name| name == "onchfs-chunk")
}

fn remove_budgeted(root: &Path, path: &Path, used: &Mutex<Budget>) -> io::Result<()> {
    let mut budget = used.lock().map_err(accounting_error)?;
    remove_entry(root, path, &mut budget)
}

fn remove_entry(root: &Path, path: &Path, budget: &mut Budget) -> io::Result<()> {
    remove_if_exists(path)?;
    budget.forget(path);
    // Empty shard/namespace directories must not accumulate after unique misses.
    let mut parent = path.parent();
    for _ in 0..3 {
        let Some(directory) =
            parent.filter(|dir| *dir != root && *dir != root.join("tmp") && dir.starts_with(root))
        else {
            break;
        };
        if std::fs::remove_dir(directory).is_err() {
            break;
        }
        parent = directory.parent();
    }
    Ok(())
}

fn charge_size(logical: u64, allocated: u64) -> u64 {
    logical
        .div_ceil(ALLOCATION)
        .saturating_mul(ALLOCATION)
        .max(allocated)
        .max(ALLOCATION)
        .saturating_add(ENTRY_OVERHEAD)
}

fn charge_metadata(metadata: &std::fs::Metadata) -> u64 {
    #[cfg(unix)]
    let allocated = {
        use std::os::unix::fs::MetadataExt;
        metadata.blocks().saturating_mul(512)
    };
    #[cfg(not(unix))]
    let allocated = 0;
    charge_size(metadata.len(), allocated)
}

fn absence_live(path: &Path, now: SystemTime) -> io::Result<bool> {
    let bytes = match read_capped(path, 128) {
        Ok(bytes) => bytes,
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::NotFound | io::ErrorKind::InvalidData
            ) =>
        {
            return Ok(false);
        }
        Err(error) => return Err(error),
    };
    let Ok(text) = std::str::from_utf8(&bytes) else {
        return Ok(false);
    };
    Ok(match text.trim().strip_prefix("until=") {
        None => text.trim() == "permanent",
        Some(ms) => ms.parse::<u128>().is_ok_and(|ms| ms > unix_millis(now)),
    })
}

fn sweep_absences(root: &Path, budget: &mut Budget, limit: usize) -> io::Result<()> {
    let now = SystemTime::now();
    for _ in 0..limit.min(budget.absences.len()) {
        let Some(path) = budget.absences.pop_front() else {
            break;
        };
        match absence_live(&path, now) {
            Ok(true) => budget.absences.push_back(path),
            Ok(false) => {
                // Keep the queue consistent if removal fails.
                budget.absences.push_back(path.clone());
                remove_entry(root, &path, budget)?;
            }
            Err(error) => {
                budget.absences.push_back(path);
                return Err(error);
            }
        }
    }
    Ok(())
}

fn scan(root: &Path, maximum: u64, limit: usize) -> io::Result<Budget> {
    scan_with_entry_limit(root, maximum, limit, MAX_ENTRIES)
}

fn staging_name(name: &str) -> bool {
    let mut parts = name.split('-');
    let numeric = |part: Option<&str>| {
        part.is_some_and(|part| !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit()))
    };
    numeric(parts.next())
        && numeric(parts.next())
        && numeric(parts.next())
        && parts.next().is_none()
}

fn scan_with_entry_limit(
    root: &Path,
    maximum: u64,
    limit: usize,
    entry_limit: usize,
) -> io::Result<Budget> {
    let mut pending = vec![(root.to_path_buf(), 0u8)];
    let mut budget = Budget::default();
    let mut visited = 0;
    let started = Instant::now();
    while let Some((directory, depth)) = pending.pop() {
        for entry in std::fs::read_dir(directory)? {
            visited += 1;
            if visited > limit || started.elapsed() >= MAX_SCAN_TIME {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "cache scan limit exceeded",
                ));
            }
            let entry = entry?;
            let metadata = entry.path().symlink_metadata()?;
            if metadata.is_dir() {
                if depth >= 8 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "cache directory too deep",
                    ));
                }
                pending.push((entry.path(), depth + 1));
            } else if metadata.is_file() {
                let path = entry.path();
                let absent = path.starts_with(root.join("absent"));
                let staging = path.parent() == Some(root.join("tmp").as_path())
                    && entry.file_name().to_str().is_some_and(staging_name);
                if staging {
                    // A live process may still own this reservation. Recover dead
                    // writers immediately; never count transient files as entries.
                    #[cfg(target_os = "linux")]
                    if let Some(pid) = entry
                        .file_name()
                        .to_str()
                        .and_then(|name| name.split('-').next())
                        .and_then(|pid| pid.parse::<u32>().ok())
                    {
                        if Path::new("/proc").is_dir()
                            && !Path::new("/proc").join(pid.to_string()).try_exists()?
                        {
                            std::fs::remove_file(&path)?;
                        }
                    }
                    continue;
                }
                let charge = Charge {
                    bytes: charge_metadata(&metadata),
                    absent,
                    chunk: !absent && is_chunk(&path),
                };
                if absent
                    && (!absence_live(&path, SystemTime::now())?
                        || budget.absences.len() >= MAX_ABSENT_ENTRIES
                        || budget.absent_bytes.saturating_add(charge.bytes) > maximum / 8)
                {
                    remove_entry(root, &path, &mut budget)?;
                    continue;
                }
                if budget.entries.len() >= entry_limit {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "cache entry limit exceeded",
                    ));
                }
                budget.record(path, charge);
            } else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "cache scan refuses nonregular entries",
                ));
            }
        }
    }
    // An existing verified cache may exceed newly lowered limits. Preserve it,
    // discard only negative entries, and bypass new writes until space is freed.
    while budget.bytes > maximum {
        let Some(path) = budget.absences.front().cloned() else {
            break;
        };
        remove_entry(root, &path, &mut budget)?;
    }
    Ok(budget)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_root(name: &str) -> PathBuf {
        let p = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tmp")
            .join("unit-cache")
            .join(format!("{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&p);
        p
    }

    #[tokio::test]
    async fn hot_touch_skips_contended_eviction_accounting() {
        let root = temp_root("touch-contention");
        let cache = Arc::new(DiskCache::open(&root).unwrap());
        cache.put("t", "ab01", vec![1]).await.unwrap();
        let lock = cache.used.lock().unwrap();
        let (sent, received) = std::sync::mpsc::channel();
        let touching = cache.clone();
        let worker = std::thread::spawn(move || {
            touching.touch("t", "ab01");
            sent.send(()).unwrap();
        });
        let result = received.recv_timeout(Duration::from_secs(1));
        drop(lock);
        worker.join().unwrap();
        assert!(result.is_ok(), "hot hit must not wait for eviction I/O");
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn local_peek_is_bounded_and_growth_beyond_read_hint_is_refused() {
        let root = temp_root("peek-bounds");
        let cache = DiskCache::open(&root).unwrap();
        assert!(cache.peek_local("t", "ab01", 5).unwrap().is_none());
        cache.put("t", "ab01", vec![7; 65537]).await.unwrap();
        let (prefix, size) = cache.peek_local("t", "ab01", 5).unwrap().unwrap();
        assert_eq!((prefix.len(), size), (5, 65537));
        assert!(
            cache
                .get_local_capped("t", "ab01", size - 1)
                .unwrap()
                .is_none()
        );
        let bytes = cache.get_local_capped("t", "ab01", size).unwrap().unwrap();
        assert_eq!(bytes.capacity(), bytes.len());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn put_get_remove_roundtrip() {
        let root = temp_root("roundtrip");
        let c = DiskCache::open(&root).unwrap();
        assert_eq!(c.get("t", "abcd").await.unwrap(), None);
        c.put("t", "abcd", b"hello".to_vec()).await.unwrap();
        assert_eq!(
            c.get("t", "abcd").await.unwrap().as_deref(),
            Some(&b"hello"[..])
        );
        c.remove("t", "abcd").await.unwrap();
        assert_eq!(c.get("t", "abcd").await.unwrap(), None);
        assert_eq!(std::fs::read_dir(root.join("tmp")).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn rejects_non_hex_keys() {
        let c = DiskCache::open(&temp_root("keys")).unwrap();
        for k in ["../etc", "AB", "a", "zz", "ab/cd"] {
            assert!(c.get("t", k).await.is_err(), "{k}");
        }
        for ns in ["../elsewhere", "/absolute", "bad/path", "", "absent", "tmp"] {
            assert!(c.get(ns, "abcd").await.is_err());
        }
    }

    #[tokio::test]
    async fn full_temporary_cache_evicts_the_least_recently_read_object() {
        let root = temp_root("lru");
        let unit = charge_size(1, 0);
        let cache = DiskCache::open_evicting(&root, unit * 2, 16).unwrap();
        cache.put("t", "aa01", vec![1]).await.unwrap();
        cache.put("t", "aa02", vec![2]).await.unwrap();
        assert_eq!(
            cache.get_local_capped("t", "aa01", 16).unwrap(),
            Some(vec![1])
        );
        cache.put("t", "bb03", vec![3]).await.unwrap();
        assert!(cache.get("t", "aa02").await.unwrap().is_none());
        assert_eq!(cache.get("t", "aa01").await.unwrap(), Some(vec![1]));
        assert_eq!(cache.get("t", "bb03").await.unwrap(), Some(vec![3]));
        let stats = cache.stats().unwrap();
        assert_eq!(
            (stats.entries, stats.evictions, stats.bytes_used),
            (2, 1, unit * 2)
        );
        assert_eq!(cache.used.lock().unwrap().order.len(), 2);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn temporary_cache_recovers_after_restart_with_a_smaller_budget() {
        let root = temp_root("lru-restart");
        let unit = charge_size(1, 0);
        let cache = DiskCache::open_evicting(&root, unit * 3, 16).unwrap();
        for n in 0..3 {
            cache
                .put("t", &format!("ab{n:02x}"), vec![1])
                .await
                .unwrap();
        }
        drop(cache);
        let cache = DiskCache::open_evicting(&root, unit, 16).unwrap();
        cache.put("t", "ff00", vec![2]).await.unwrap();
        assert_eq!(cache.get("t", "ff00").await.unwrap(), Some(vec![2]));
        assert_eq!(cache.stats().unwrap().evictions, 3);
        assert_eq!(cache.disk_usage().unwrap(), unit);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn impossible_write_preserves_objects_but_large_write_evicts_multiple_batches() {
        let root = temp_root("lru-impossible");
        let unit = charge_size(1, 0);
        let quota = unit * (EVICTION_BATCH as u64 + 2);
        let cache = DiskCache::open_evicting(&root, quota, quota * 2).unwrap();
        for n in 0..EVICTION_BATCH + 2 {
            cache
                .put("t", &format!("ab{n:04x}"), vec![1])
                .await
                .unwrap();
        }
        cache
            .put("t", "ff00", vec![0; (quota * 2) as usize])
            .await
            .unwrap();
        assert_eq!(cache.stats().unwrap().evictions, 0);
        cache
            .put("t", "ff01", vec![0; (quota - ENTRY_OVERHEAD) as usize])
            .await
            .unwrap();
        assert_eq!(cache.stats().unwrap().evictions, EVICTION_BATCH as u64 + 2);
        assert_eq!(cache.disk_usage().unwrap(), quota);
        assert!(cache.get("t", "ff01").await.unwrap().is_some());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn startup_trims_a_quota_reduction_larger_than_one_eviction_batch() {
        let root = temp_root("lru-large-restart");
        let unit = charge_size(1, 0);
        let count = EVICTION_BATCH * 2 + 3;
        let cache = DiskCache::open_evicting(&root, unit * count as u64, 16).unwrap();
        for n in 0..count {
            cache.put("t", &format!("{n:04x}"), vec![1]).await.unwrap();
        }
        drop(cache);
        let cache = DiskCache::open_evicting(&root, unit, 16).unwrap();
        assert_eq!(cache.disk_usage().unwrap(), unit);
        assert_eq!(cache.stats().unwrap().evictions, count as u64 - 1);
        cache.put("t", "ffff", vec![2]).await.unwrap();
        assert_eq!(cache.get("t", "ffff").await.unwrap(), Some(vec![2]));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn total_pressure_admits_absences_and_chunks_by_evicting_objects() {
        let root = temp_root("lru-total-pressure");
        let unit = charge_size(1, 0);
        let cache = DiskCache::open_evicting(&root, unit * 8, 16).unwrap();
        for n in 0..8 {
            cache
                .put("t", &format!("aa{n:02x}"), vec![1])
                .await
                .unwrap();
        }
        cache.mark_absent("t", "ffff", None).await.unwrap();
        assert!(cache.is_absent("t", "ffff").await.unwrap());
        cache.put("onchfs-chunk", "eeee", vec![2]).await.unwrap();
        assert_eq!(
            cache.get("onchfs-chunk", "eeee").await.unwrap(),
            Some(vec![2])
        );
        assert_eq!(cache.stats().unwrap().evictions, 2);
        assert_eq!(cache.disk_usage().unwrap(), unit * 8);
        assert!(cache.get("t", "aa00").await.unwrap().is_none());
        assert!(cache.get("t", "aa01").await.unwrap().is_none());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn eviction_preserves_open_readers_and_a_failed_writer_releases_its_reservation() {
        let root = temp_root("lru-open-reader");
        let unit = charge_size(1, 0);
        let cache = DiskCache::open_evicting(&root, unit, 16).unwrap();
        cache.put("t", "aa01", b"kept".to_vec()).await.unwrap();
        let old = cache.object_path("t", "aa01").unwrap();
        let mut reader = std::fs::File::open(&old).unwrap();
        // A reader that statted the old pathname will see a normal miss after eviction.
        assert!(regular_size(&old).unwrap().is_some());
        let new = cache.object_path("t", "bb02").unwrap();
        let tmp = cache.tmp_path();
        let failure = write_budgeted_with(
            &tmp,
            &new,
            &[Bytes::from_static(b"new")],
            false,
            &cache.used,
            unit,
            |file| {
                file.write_all(b"part")?;
                Err(io::Error::other("injected write failure"))
            },
        );
        assert!(failure.is_err());
        assert_eq!(
            read_capped(&old, 16).unwrap_err().kind(),
            io::ErrorKind::NotFound
        );
        let mut held = Vec::new();
        reader.read_to_end(&mut held).unwrap();
        assert_eq!(held, b"kept");
        assert!(!tmp.exists());
        assert!(cache.used.lock().unwrap().reservations.is_empty());
        assert_eq!(cache.disk_usage().unwrap(), 0);
        cache.put("t", "bb02", b"new".to_vec()).await.unwrap();
        assert_eq!(cache.get("t", "bb02").await.unwrap(), Some(b"new".to_vec()));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn eviction_keeps_unknown_files_and_reserved_destinations() {
        let root = temp_root("lru-reserved");
        let unit = charge_size(1, 0);
        private_directory(&root.join("tmp")).unwrap();
        std::fs::write(root.join("tmp/interrupted"), b"keep").unwrap();
        let cache = DiskCache::open_evicting(&root, unit * 4, 16).unwrap();
        cache.put("t", "aa01", vec![1]).await.unwrap();
        cache.put("t", "aa02", vec![2]).await.unwrap();
        let path = root.join("t/aa/aa01");
        cache.used.lock().unwrap().reservations.insert(
            path.clone(),
            Charge {
                bytes: unit,
                absent: false,
                chunk: false,
            },
        );
        cache.put("t", "bb03", vec![3]).await.unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), vec![1]);
        assert_eq!(
            std::fs::read(root.join("tmp/interrupted")).unwrap(),
            b"keep"
        );
        assert!(cache.get("t", "aa02").await.unwrap().is_none());
        assert_eq!(cache.get("t", "bb03").await.unwrap(), Some(vec![3]));
        assert_eq!(cache.stats().unwrap().evictions, 1);
        cache.used.lock().unwrap().reservations.remove(&path);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn eviction_failure_keeps_accounting_and_allows_retry() {
        let root = temp_root("lru-unlink-failure");
        let unit = charge_size(1, 0);
        let cache = DiskCache::open_evicting(&root, unit, 16).unwrap();
        cache.put("t", "aa01", vec![1]).await.unwrap();
        let old = root.join("t/aa/aa01");
        std::fs::remove_file(&old).unwrap();
        std::fs::create_dir(&old).unwrap();
        assert!(cache.put("t", "bb02", vec![2]).await.is_err());
        assert_eq!(cache.disk_usage().unwrap(), unit);
        assert_eq!(cache.stats().unwrap().evictions, 0);
        assert!(cache.used.lock().unwrap().reservations.is_empty());
        std::fs::remove_dir(&old).unwrap();
        std::fs::write(&old, [1]).unwrap();
        cache.put("t", "bb02", vec![2]).await.unwrap();
        assert_eq!(cache.get("t", "bb02").await.unwrap(), Some(vec![2]));
        assert_eq!(std::fs::read_dir(root.join("tmp")).unwrap().count(), 0);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn concurrent_evictions_keep_reservations_bounded_and_release_staging() {
        let root = temp_root("lru-concurrent");
        let quota = charge_size(1, 0) * 8;
        let cache = Arc::new(DiskCache::open_evicting(&root, quota, 16).unwrap());
        let mut tasks = tokio::task::JoinSet::new();
        for n in 0..16 {
            let cache = cache.clone();
            tasks.spawn(async move {
                for attempt in 0..10 {
                    match cache.put("t", &format!("{n:04x}"), vec![1]).await {
                        Ok(()) => break,
                        Err(error) if error.kind() == io::ErrorKind::WouldBlock && attempt < 9 => {
                            tokio::task::yield_now().await;
                        }
                        Err(error) => panic!("cache write failed: {error}"),
                    }
                }
                let budget = cache.used.lock().unwrap();
                let pending: u64 = budget.reservations.values().map(|c| c.bytes).sum();
                assert!(budget.bytes + pending <= quota);
            });
        }
        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
        assert!(cache.stats().unwrap().evictions > 0);
        assert!(cache.used.lock().unwrap().reservations.is_empty());
        assert_eq!(std::fs::read_dir(root.join("tmp")).unwrap().count(), 0);
        assert!(cache.disk_usage().unwrap() <= quota);
        let order_len = cache.used.lock().unwrap().order.len();
        assert_eq!(order_len, cache.stats().unwrap().entries);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn negative_and_chunk_pressure_never_evicts_complete_objects() {
        let root = temp_root("lru-subquotas");
        let unit = charge_size(1, 0);
        let cache = DiskCache::open_evicting(&root, unit * 8, 16).unwrap();
        cache.put("t", "aa01", vec![1]).await.unwrap();
        for n in 0..8 {
            cache
                .mark_absent("t", &format!("ab{n:02x}"), None)
                .await
                .unwrap();
            cache
                .put("onchfs-chunk", &format!("cc{n:02x}"), vec![2])
                .await
                .unwrap();
        }
        assert_eq!(cache.get("t", "aa01").await.unwrap(), Some(vec![1]));
        assert!(cache.is_absent("t", "ab07").await.unwrap());
        assert_eq!(
            cache.get("onchfs-chunk", "cc07").await.unwrap(),
            Some(vec![2])
        );
        let budget = cache.used.lock().unwrap();
        assert_eq!((budget.absences.len(), budget.chunks), (1, 2));
        assert!(budget.bytes <= unit * 8);
        drop(budget);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn cache_budget_survives_restart_and_bypasses_full_writes() {
        let root = temp_root("budget");
        let charge = charge_size(4, 0);
        let cache = DiskCache::open_with_limits(&root, charge, 8).unwrap();
        cache.put("t", "abcd", b"1234".to_vec()).await.unwrap();
        cache.put("t", "abce", b"5678".to_vec()).await.unwrap();
        assert!(cache.get("t", "abce").await.unwrap().is_none());
        assert_eq!(cache.disk_usage().unwrap(), charge);
        drop(cache);
        let cache = DiskCache::open_with_limits(&root, charge, 8).unwrap();
        cache.remove("t", "abcd").await.unwrap();
        cache.put("t", "abce", b"5678".to_vec()).await.unwrap();
        assert_eq!(
            cache.get("t", "abce").await.unwrap(),
            Some(b"5678".to_vec())
        );
        drop(cache);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn oversized_cache_entries_are_removed_before_retry() {
        let root = temp_root("oversize");
        let cache = DiskCache::open_with_limits(&root, charge_size(4, 0), 4).unwrap();
        cache.put("t", "abcd", vec![1; 4]).await.unwrap();
        std::fs::write(root.join("t/ab/abcd"), vec![2; 10]).unwrap();
        assert_eq!(cache.get("t", "abcd").await.unwrap(), None);
        cache.put("t", "abcd", vec![3; 4]).await.unwrap();
        assert_eq!(cache.get("t", "abcd").await.unwrap(), Some(vec![3; 4]));
        drop(cache);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn absence_expires_and_is_cleared_by_put() {
        let c = DiskCache::open(&temp_root("absent")).unwrap();
        c.mark_absent("t", "ab01", Some(Duration::from_millis(150)))
            .await
            .unwrap();
        assert!(c.is_absent("t", "ab01").await.unwrap());
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!c.is_absent("t", "ab01").await.unwrap());
        c.mark_absent("t", "ab02", None).await.unwrap();
        assert!(c.is_absent("t", "ab02").await.unwrap());
        c.put("t", "ab02", vec![1]).await.unwrap();
        assert!(!c.is_absent("t", "ab02").await.unwrap());
    }

    #[tokio::test]
    async fn one_byte_entries_are_charged_and_smaller_restart_budget_preserves_objects() {
        let root = temp_root("tiny-allocation");
        let unit = charge_size(1, 0);
        let cache = DiskCache::open_with_limits(&root, unit * 3, 4).unwrap();
        for number in 0..20 {
            cache
                .put("t", &format!("ab{number:04x}"), vec![1])
                .await
                .unwrap();
        }
        assert_eq!(cache.disk_usage().unwrap(), unit * 3);
        assert_eq!(std::fs::read_dir(root.join("t/ab")).unwrap().count(), 3);
        assert!(unit >= 8192);
        drop(cache);
        let cache = DiskCache::open_with_limits(&root, unit, 4).unwrap();
        for number in 0..3 {
            assert_eq!(
                cache.get("t", &format!("ab{number:04x}")).await.unwrap(),
                Some(vec![1])
            );
        }
        cache.put("t", "abcd", vec![2]).await.unwrap();
        assert!(cache.get("t", "abcd").await.unwrap().is_none());
        assert_eq!(cache.disk_usage().unwrap(), unit * 3);
        drop(cache);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn absence_budget_leaves_capacity_for_verified_objects_after_restart() {
        let root = temp_root("negative-budget");
        let unit = charge_size(1, 0);
        let cache = DiskCache::open_with_limits(&root, unit * 16, 16).unwrap();
        for number in 0..10 {
            cache
                .mark_absent("t", &format!("ab{number:04x}"), None)
                .await
                .unwrap();
        }
        assert_eq!(
            std::fs::read_dir(root.join("absent/t/ab")).unwrap().count(),
            2
        );
        assert_eq!(cache.disk_usage().unwrap(), unit * 2);
        drop(cache);
        let cache = DiskCache::open_with_limits(&root, unit * 16, 16).unwrap();
        assert!(cache.is_absent("t", "ab0000").await.unwrap());
        assert!(cache.is_absent("t", "ab0001").await.unwrap());
        cache.put("t", "abcd", vec![1]).await.unwrap();
        assert_eq!(cache.get("t", "abcd").await.unwrap(), Some(vec![1]));
        assert_eq!(cache.disk_usage().unwrap(), unit * 3);
        drop(cache);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn entry_count_caps_apply_even_with_a_large_byte_budget() {
        let mut budget = Budget::default();
        let absence = Charge {
            bytes: charge_size(1, 0),
            absent: true,
            chunk: false,
        };
        for number in 0..MAX_ABSENT_ENTRIES {
            budget.record(PathBuf::from(format!("absent/{number}")), absence);
        }
        assert!(!budget.admits(Path::new("absent/next"), absence, u64::MAX));
        let positive = Charge {
            absent: false,
            ..absence
        };
        assert!(budget.admits(Path::new("object/next"), positive, u64::MAX));
        for number in MAX_ABSENT_ENTRIES..MAX_ENTRIES {
            budget.record(PathBuf::from(format!("object/{number}")), positive);
        }
        assert!(!budget.admits(Path::new("object/next"), positive, u64::MAX));
    }

    #[tokio::test]
    async fn unrelated_reads_reap_expired_unique_markers_in_bounded_batches() {
        let root = temp_root("opportunistic-expiry");
        let cache = DiskCache::open(&root).unwrap();
        for number in 0..EXPIRY_BATCH * 3 {
            cache
                .mark_absent("t", &format!("ab{number:04x}"), None)
                .await
                .unwrap();
        }
        for number in 0..EXPIRY_BATCH * 3 {
            std::fs::write(
                root.join(format!("absent/t/ab/ab{number:04x}")),
                b"until=0\n",
            )
            .unwrap();
        }
        cache.get("t", "ffff").await.unwrap();
        assert_eq!(cache.used.lock().unwrap().absences.len(), EXPIRY_BATCH * 2);
        cache.get("t", "ffff").await.unwrap();
        cache.get("t", "ffff").await.unwrap();
        assert_eq!(cache.disk_usage().unwrap(), 0);
        assert!(
            !root.join("absent").exists(),
            "empty shards must not accumulate"
        );
        drop(cache);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn startup_and_reads_discard_corrupt_oversized_and_expired_absences() {
        let root = temp_root("corrupt-markers");
        private_directory(&root.join("absent/t/ab")).unwrap();
        let bodies: [&[u8]; 4] = [b"1", b"\xff", &[b'x'; 129], b"until=0\n"];
        for (number, body) in bodies.iter().enumerate() {
            std::fs::write(root.join(format!("absent/t/ab/ab{number:04x}")), body).unwrap();
        }
        std::fs::write(root.join("absent/t/ab/abcd"), b"permanent\n").unwrap();
        let cache = DiskCache::open(&root).unwrap();
        assert_eq!(cache.used.lock().unwrap().absences.len(), 1);
        assert!(cache.is_absent("t", "abcd").await.unwrap());
        for body in bodies {
            std::fs::write(root.join("absent/t/ab/abcd"), body).unwrap();
            assert!(!cache.is_absent("t", "abcd").await.unwrap());
            assert!(!root.join("absent/t/ab/abcd").exists());
            cache.mark_absent("t", "abcd", None).await.unwrap();
        }
        drop(cache);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn startup_scan_bounds_directories_and_rejects_links_without_following_them() {
        let root = temp_root("scan-bound");
        private_directory(&root).unwrap();
        for number in 0..20 {
            std::fs::create_dir(root.join(number.to_string())).unwrap();
        }
        assert_eq!(
            scan(&root, u64::MAX, 10).err().unwrap().kind(),
            io::ErrorKind::InvalidData
        );
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(root.join("missing"), root.join("link")).unwrap();
            assert_eq!(
                scan(&root, u64::MAX, 100).err().unwrap().kind(),
                io::ErrorKind::InvalidData
            );
        }
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn startup_scan_ignores_fresh_staging_at_the_entry_limit_without_removing_it() {
        let root = temp_root("fresh-staging");
        private_directory(&root.join("tmp")).unwrap();
        private_directory(&root.join("t/ab")).unwrap();
        std::fs::write(root.join("t/ab/abcd"), b"verified").unwrap();
        let staging = root.join(format!("tmp/{}-456-7", std::process::id()));
        std::fs::write(&staging, b"partial write").unwrap();

        let budget = scan_with_entry_limit(&root, u64::MAX, 100, 1).unwrap();

        assert_eq!(budget.entries.len(), 1);
        assert_eq!(std::fs::read(&staging).unwrap(), b"partial write");
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn startup_recovers_fresh_staging_from_a_dead_writer() {
        let root = temp_root("dead-writer");
        private_directory(&root.join("tmp")).unwrap();
        let file = root.join("tmp/4294967295-456-7");
        std::fs::write(&file, b"partial").unwrap();
        let budget = scan_with_entry_limit(&root, u64::MAX, 100, 0).unwrap();
        assert!(!file.exists());
        assert_eq!(budget.bytes, 0);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn read_only_cache_reuses_existing_objects_without_writes_or_cleanup() {
        let root = temp_root("read-only");
        let cache = DiskCache::open(&root).unwrap();
        cache.put("t", "abcd", b"verified".to_vec()).await.unwrap();
        cache.mark_absent("t", "abce", None).await.unwrap();
        let reader = cache.read_only();
        let before = cache.disk_usage().unwrap();
        assert_eq!(
            reader.get("t", "abcd").await.unwrap(),
            Some(b"verified".to_vec())
        );
        assert!(reader.is_absent("t", "abce").await.unwrap());
        reader.put("t", "ffff", b"media".to_vec()).await.unwrap();
        reader.mark_absent("t", "fffe", None).await.unwrap();
        reader.remove("t", "abcd").await.unwrap();
        assert!(cache.get("t", "ffff").await.unwrap().is_none());
        assert!(!cache.is_absent("t", "fffe").await.unwrap());
        assert!(cache.get("t", "abcd").await.unwrap().is_some());
        assert_eq!(cache.disk_usage().unwrap(), before);
        assert_eq!(std::fs::read_dir(root.join("tmp")).unwrap().count(), 0);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn startup_scan_preserves_and_accounts_for_non_staging_tmp_files() {
        let root = temp_root("non-staging-tmp");
        private_directory(&root.join("tmp")).unwrap();
        let file = root.join("tmp/interrupted");
        std::fs::write(&file, b"keep").unwrap();

        let budget = scan_with_entry_limit(&root, u64::MAX, 100, 1).unwrap();

        assert_eq!(budget.entries.len(), 1);
        assert_eq!(std::fs::read(&file).unwrap(), b"keep");
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn writable_probe_cleans_up_and_recovers_after_staging_failure() {
        let root = temp_root("writable-probe");
        let cache = DiskCache::open_with_limits(&root, 0, 0).unwrap();
        cache.check_writable().await.unwrap();
        assert_eq!(std::fs::read_dir(root.join("tmp")).unwrap().count(), 0);
        assert_eq!(cache.disk_usage().unwrap(), 0);
        std::fs::remove_dir(root.join("tmp")).unwrap();
        std::fs::write(root.join("tmp"), b"blocked").unwrap();
        assert!(cache.check_writable().await.is_err());
        std::fs::remove_file(root.join("tmp")).unwrap();
        private_directory(&root.join("tmp")).unwrap();
        cache.check_writable().await.unwrap();
        assert_eq!(std::fs::read_dir(root.join("tmp")).unwrap().count(), 0);
        drop(cache);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn startup_scan_failure_bypasses_cache_without_writing_or_removing_verified_files() {
        let root = temp_root("bypass-startup");
        private_directory(&root.join("t/ab")).unwrap();
        std::fs::write(root.join("t/ab/abcd"), b"verified").unwrap();
        private_directory(&root.join("a/b/c/d/e/f/g/h/i/j")).unwrap();
        let cache = DiskCache::open(&root).unwrap();
        assert!(!cache.is_enabled());
        assert!(cache.get("t", "abcd").await.unwrap().is_none());
        cache.put("t", "abce", vec![1]).await.unwrap();
        cache.mark_absent("t", "abcf", None).await.unwrap();
        cache.remove("t", "abcd").await.unwrap();
        assert_eq!(std::fs::read(root.join("t/ab/abcd")).unwrap(), b"verified");
        assert!(!root.join("t/ab/abce").exists());
        assert!(!root.join("absent").exists());
        assert!(cache.disk_usage().is_err());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn chunks_have_separate_capacity_and_leave_room_for_complete_files() {
        let root = temp_root("chunk-budget");
        let unit = charge_size(1, 0);
        let cache = DiskCache::open_with_limits(&root, unit * 8, 8).unwrap();
        for number in 0..10 {
            cache
                .put("onchfs-chunk", &format!("ab{number:02x}"), vec![1])
                .await
                .unwrap();
        }
        assert_eq!(cache.used.lock().unwrap().chunks, 2);
        cache
            .put_parts(
                "t",
                "abcd",
                vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")],
            )
            .await
            .unwrap();
        assert_eq!(cache.get("t", "abcd").await.unwrap(), Some(b"ab".to_vec()));
        drop(cache);
        let cache = DiskCache::open_with_limits(&root, unit * 8, 8).unwrap();
        assert_eq!(cache.used.lock().unwrap().chunks, 2);
        assert_eq!(cache.disk_usage().unwrap(), unit * 3);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn write_failure_rolls_back_reservation_and_staging_then_allows_retry() {
        let root = temp_root("reservation-rollback");
        private_directory(&root.join("tmp")).unwrap();
        let used = Mutex::new(Budget::default());
        let path = root.join("t/ab/abcd");
        let tmp = root.join("tmp/staging");
        let parts = [Bytes::from_static(b"x")];
        let result = write_budgeted_with(&tmp, &path, &parts, false, &used, 8192, |file| {
            file.write_all(b"partial")?;
            Err(io::Error::other("simulated ENOSPC"))
        });
        assert!(result.is_err());
        assert!(used.lock().unwrap().reservations.is_empty());
        assert!(!tmp.exists());
        assert!(!path.exists());
        write_budgeted(&tmp, &path, &parts, false, &used, 8192).unwrap();
        assert_eq!(std::fs::read(path).unwrap(), b"x");
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn cancelled_request_retains_worker_and_quota_until_unlocked_write_finishes() {
        let root = temp_root("cancel-reserved-write");
        let cache = Arc::new(DiskCache::open_evicting(&root, 8192, 8).unwrap());
        let (started, ready) = tokio::sync::oneshot::channel();
        let (release, released) = std::sync::mpsc::channel();
        let (finished, done) = tokio::sync::oneshot::channel();
        let worker_cache = cache.clone();
        let used = cache.used.clone();
        let path = root.join("t/ab/abcd");
        let tmp = cache.tmp_path();
        let task = tokio::spawn(async move {
            worker_cache
                .blocking(move || {
                    let result = write_budgeted_with(
                        &tmp,
                        &path,
                        &[Bytes::from_static(b"x")],
                        false,
                        &used,
                        8192,
                        |file| {
                            let _ = started.send(());
                            released
                                .recv_timeout(Duration::from_secs(2))
                                .map_err(io::Error::other)?;
                            file.write_all(b"x")?;
                            file.sync_all()
                        },
                    );
                    let _ = finished.send(());
                    result
                })
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), ready)
            .await
            .unwrap()
            .unwrap();
        {
            let budget = cache
                .used
                .try_lock()
                .expect("bulk IO must not hold accounting lock");
            assert_eq!(budget.reservations.len(), 1);
        }
        cache.put("t", "abce", vec![2]).await.unwrap();
        assert!(
            !root.join("t/ab/abce").exists(),
            "reservation must prevent overbooking"
        );
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(cache.io_slots.available_permits(), 3);
        assert_eq!(cache.used.lock().unwrap().reservations.len(), 1);
        release.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(2), done)
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while cache.io_slots.available_permits() != 4 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(cache.used.lock().unwrap().reservations.is_empty());
        assert_eq!(cache.get("t", "abcd").await.unwrap(), Some(vec![b'x']));
        assert_eq!(std::fs::read_dir(root.join("tmp")).unwrap().count(), 0);
        std::fs::remove_dir_all(root).unwrap();
    }
}
