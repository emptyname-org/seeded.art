//! Reuse only idle, read-only connections; every operation takes a new snapshot.
use std::{path::Path, sync::Mutex};

use crate::store::{Store, StoreError};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

#[derive(Default)]
pub(super) struct ReadCounters {
    pub waiting_full: AtomicU64,
    pub waiting_timeout: AtomicU64,
    pub query_timeout: AtomicU64,
    pub query_unavailable: AtomicU64,
}

impl ReadCounters {
    pub fn increment(counter: &AtomicU64) {
        let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
            Some(n.saturating_add(1))
        });
    }
}

#[derive(Clone)]
pub struct DatabaseMetricsHandle(pub(super) Arc<ReadCounters>);

#[derive(Debug, Clone, Copy, serde::Serialize)]
pub struct DatabaseMetricsSnapshot {
    pub waiting_full: u64,
    pub waiting_timeout: u64,
    pub query_timeout: u64,
    pub query_unavailable: u64,
}

impl DatabaseMetricsHandle {
    pub fn snapshot(&self) -> DatabaseMetricsSnapshot {
        DatabaseMetricsSnapshot {
            waiting_full: self.0.waiting_full.load(Ordering::Relaxed),
            waiting_timeout: self.0.waiting_timeout.load(Ordering::Relaxed),
            query_timeout: self.0.query_timeout.load(Ordering::Relaxed),
            query_unavailable: self.0.query_unavailable.load(Ordering::Relaxed),
        }
    }
}

/// The archive file, and the attached sales file when it existed at open.
#[derive(Clone, PartialEq, Eq)]
struct PoolKey(FileId, Option<FileId>);

#[derive(Default)]
pub(super) struct ReadPool {
    idle: Mutex<Vec<(PoolKey, Store)>>,
    #[cfg(test)]
    opened: std::sync::atomic::AtomicUsize,
}

#[derive(Clone, PartialEq, Eq)]
pub(super) struct FileId {
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
    #[cfg(not(unix))]
    modified: std::time::SystemTime,
    #[cfg(not(unix))]
    size: u64,
}

pub(super) fn file_id(path: &Path) -> Result<FileId, StoreError> {
    let meta = std::fs::metadata(path)
        .map_err(|_| StoreError::Unavailable("Archive unavailable".into()))?;
    if !meta.is_file() {
        return Err(StoreError::Unavailable("Archive unavailable".into()));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        Ok(FileId {
            device: meta.dev(),
            inode: meta.ino(),
        })
    }
    #[cfg(not(unix))]
    Ok(FileId {
        modified: meta
            .modified()
            .map_err(|_| StoreError::Unavailable("Archive unavailable".into()))?,
        size: meta.len(),
    })
}

impl ReadPool {
    /// Called only by workers holding one of the eight database permits. `sales` is attached
    /// when present; a connection is reused only while neither file was replaced.
    pub fn read<T>(
        &self,
        path: &Path,
        sales: Option<&Path>,
        operation: impl FnOnce(&Store) -> Result<T, StoreError>,
    ) -> Result<T, StoreError> {
        let key = |path: &Path| -> Result<PoolKey, StoreError> {
            Ok(PoolKey(file_id(path)?, sales.and_then(|s| file_id(s).ok())))
        };
        let id = key(path)?;
        let reused = {
            let mut idle = self
                .idle
                .lock()
                .map_err(|_| StoreError::Unavailable("Archive readers unavailable".into()))?;
            idle.retain(|(old, _)| *old == id);
            idle.pop().map(|(_, store)| store)
        };
        let store = match reused {
            Some(store) => store,
            None => {
                let store = Store::open_read_only(path)?;
                #[cfg(test)]
                self.opened
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                store
            }
        };
        // Idempotent: rechecks an attached file and retries one whose attach failed.
        if let (Some(sales), Some(_)) = (sales, &id.1) {
            let _ = crate::sales::store::attach(store.conn(), sales);
        }
        let result = operation(&store);
        // A replaced file or failed transaction never returns a stale connection.
        if result.is_ok() && store.conn().is_autocommit() && key(path).is_ok_and(|now| now == id) {
            if let Ok(mut idle) = self.idle.lock() {
                if idle.len() < super::MAX_DATABASE_READS {
                    idle.push((id, store));
                }
            }
        }
        result
    }

    #[cfg(test)]
    pub fn opened(&self) -> usize {
        self.opened.load(std::sync::atomic::Ordering::Relaxed)
    }
}
