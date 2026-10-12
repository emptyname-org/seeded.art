//! `seeded-articles sync`: refresh chain facts, then fetch missing IPFS objects.
//!
//! 1. Chain: one TzKT snapshot (about ten requests for the whole contract), written in one
//!    transaction. fx(text) is small (1,716 tokens), so chain facts are re-read in full each
//!    run instead of tracked with cursors; nothing can be missed or applied twice.
//! 2. Metadata JSON, then bodies: only objects never stored, last seen transient, or absent
//!    as not found for a week (with `recheck`, also absent ones and unverified copies).
//!    Content-addressed, so a stored object is never fetched again; a metadata revision
//!    names a new URI and is fetched as a new object.
//!
//! 3. With a catalogue (`archive`), author names are copied right after the chain step (see
//!    [`super::names`]) and, after fetching, references to fxhash projects and tokens are
//!    resolved (see [`super::refs`]). Both are local. Extraction itself happens as bodies are
//!    stored. Without a catalogue the stored names are kept.
//!
//! Workers fetch in parallel; the calling thread is the only writer and commits results in
//! small batches, so an interrupted run keeps everything it finished.

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use super::http::ApiError;
use super::ipfs::{Fetcher, Outcome};
use super::names::{NamesReport, open_catalogue};
use super::refs::open_archive;
use super::store::{
    ArticleStore, ArticleStoreError, FetchState, ObjectKind, PendingObject, ResolveReport,
    SnapshotReport,
};
use super::tzkt::Tzkt;

/// Results per commit.
const BATCH: usize = 32;
/// Longest wait before committing a partial batch.
const FLUSH_AFTER: Duration = Duration::from_secs(2);

#[derive(Debug, thiserror::Error)]
pub enum SyncError {
    #[error(transparent)]
    Store(#[from] ArticleStoreError),
    #[error(transparent)]
    Api(#[from] ApiError),
}

impl SyncError {
    /// A later run may succeed unchanged.
    pub fn is_transient(&self) -> bool {
        matches!(
            self,
            SyncError::Api(ApiError::Transient(_))
                | SyncError::Store(ArticleStoreError::Unavailable(_))
        )
    }

    pub fn is_interrupted(&self) -> bool {
        matches!(self, SyncError::Api(ApiError::Interrupted))
    }
}

#[derive(Debug, Clone)]
pub struct SyncOptions {
    /// Most IPFS objects fetched this run (metadata first, then bodies).
    pub limit: Option<usize>,
    pub concurrency: usize,
    /// Also retry absent objects and look for verified copies of unverified ones.
    pub recheck: bool,
    /// The catalogue (`seeded.sqlite`), opened read-only, to copy author names from and
    /// resolve references with.
    pub archive: Option<PathBuf>,
}

impl Default for SyncOptions {
    fn default() -> Self {
        SyncOptions {
            limit: None,
            concurrency: 4,
            recheck: false,
            archive: None,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FetchReport {
    pub attempted: usize,
    pub ok_verified: usize,
    pub ok_unverified: usize,
    pub transient: usize,
    pub absent: usize,
    /// Of `transient`, objects every gateway reported as not found this run.
    pub not_found: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SyncReport {
    pub chain: SnapshotReport,
    pub metadata: FetchReport,
    pub bodies: FetchReport,
    /// Objects still pending after this run because of `limit`.
    pub left: usize,
    pub interrupted: bool,
    /// Reference resolution, when a catalogue was given and the run was not interrupted.
    pub refs: Option<ResolveReport>,
    /// Author names, when a catalogue was given.
    pub names: Option<NamesReport>,
}

impl SyncReport {
    pub fn transient(&self) -> usize {
        self.metadata.transient + self.bodies.transient
    }
}

pub fn sync(
    store: &mut ArticleStore,
    tzkt: &Tzkt,
    fetcher: &Fetcher,
    opts: &SyncOptions,
    stop: &AtomicBool,
    log: &dyn Fn(&str),
) -> Result<SyncReport, SyncError> {
    let mut report = SyncReport::default();
    let started = Instant::now();
    // Fail before any network work when the catalogue is unusable.
    let archive = opts.archive.as_deref().map(open_archive).transpose()?;
    let catalogue = opts.archive.as_deref().map(open_catalogue).transpose()?;
    let snap = tzkt.snapshot()?;
    // Tokens are never removed from a contract: fewer than stored means a lagging or partial
    // API answer, which must not overwrite good state.
    let stored = store.article_count()?;
    if (snap.tokens.len() as u64) < stored {
        return Err(SyncError::Api(ApiError::Transient(format!(
            "TzKT listed {} fx(text) tokens, fewer than the {stored} stored; not applied",
            snap.tokens.len()
        ))));
    }
    report.chain = store.apply_snapshot(&snap)?;
    log(&format!(
        "chain at level {}: {} articles ({} skipped), {} holder rows, {} listings, \
         {} moderation entries ({:.1}s)",
        snap.level,
        report.chain.articles,
        report.chain.skipped,
        report.chain.holders,
        report.chain.listings,
        report.chain.moderation,
        started.elapsed().as_secs_f64()
    ));
    drop(snap);
    if let Some(catalogue) = &catalogue {
        let started = Instant::now();
        let r = store.resolve_names(catalogue)?;
        log(&format!(
            "names: {} authors, {} named, {} changed, {} index rows rewritten ({:.1}s)",
            r.authors,
            r.named,
            r.changed,
            r.reindexed,
            started.elapsed().as_secs_f64()
        ));
        report.names = Some(r);
    }
    let mut budget = opts.limit.unwrap_or(usize::MAX);
    for kind in [ObjectKind::Metadata, ObjectKind::Body] {
        if stop.load(Ordering::Relaxed) {
            report.interrupted = true;
            break;
        }
        let mut pending = store.pending_objects(kind, opts.recheck)?;
        let total = pending.len();
        if pending.len() > budget {
            report.left += pending.len() - budget;
            pending.truncate(budget);
        }
        budget -= pending.len();
        log(&format!(
            "{}: {} to fetch ({} pending)",
            kind.as_str(),
            pending.len(),
            total
        ));
        let r = fetch_all(store, fetcher, pending, opts.concurrency, stop, log)?;
        if stop.load(Ordering::Relaxed) {
            report.interrupted = true;
        }
        match kind {
            ObjectKind::Metadata => report.metadata = r,
            ObjectKind::Body => report.bodies = r,
        }
    }
    if report.interrupted {
        // Count what an interrupted run left behind so the operator sees it.
        report.left = store.pending_objects(ObjectKind::Metadata, false)?.len()
            + store.pending_objects(ObjectKind::Body, false)?.len();
    } else if let Some(archive) = &archive {
        let started = Instant::now();
        let r = store.resolve_refs(archive)?;
        log(&format!(
            "references: {} targets looked up, {} rows resolved, {} generator rows, \
             {} projects checked, {} unresolved ({:.1}s)",
            r.looked_up,
            r.resolved,
            r.generators,
            r.projects_checked,
            r.unresolved,
            started.elapsed().as_secs_f64()
        ));
        report.refs = Some(r);
    }
    Ok(report)
}

/// Fetches `items` with `concurrency` workers; this thread writes. Stops taking new work
/// when `stop` is set, and on a write error (after the workers drained).
fn fetch_all(
    store: &mut ArticleStore,
    fetcher: &Fetcher,
    items: Vec<PendingObject>,
    concurrency: usize,
    stop: &AtomicBool,
    log: &dyn Fn(&str),
) -> Result<FetchReport, SyncError> {
    let mut report = FetchReport::default();
    if items.is_empty() {
        return Ok(report);
    }
    let total = items.len();
    let queue = Mutex::new(VecDeque::from(items));
    let abort = AtomicBool::new(false);
    let workers = concurrency.clamp(1, 32).min(total);
    let (tx, rx) = mpsc::channel::<(PendingObject, Outcome)>();
    let mut error: Option<SyncError> = None;
    std::thread::scope(|scope| {
        for _ in 0..workers {
            let tx = tx.clone();
            let queue = &queue;
            let abort = &abort;
            scope.spawn(move || {
                loop {
                    if stop.load(Ordering::Relaxed) || abort.load(Ordering::Relaxed) {
                        return;
                    }
                    let next = queue
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .pop_front();
                    let Some(item) = next else {
                        return;
                    };
                    let outcome = fetcher.fetch(&item.key);
                    if tx.send((item, outcome)).is_err() {
                        return;
                    }
                }
            });
        }
        drop(tx);
        let mut batch: Vec<(PendingObject, Outcome)> = Vec::new();
        let mut last_flush = Instant::now();
        let mut done = 0usize;
        loop {
            let received = rx.recv_timeout(FLUSH_AFTER);
            let closed = matches!(received, Err(mpsc::RecvTimeoutError::Disconnected));
            if let Ok(pair) = received {
                // An interrupted fetch says nothing about the object: leave it pending.
                if pair.1 != Outcome::Interrupted {
                    batch.push(pair);
                }
            }
            let due = batch.len() >= BATCH || last_flush.elapsed() >= FLUSH_AFTER || closed;
            if due && !batch.is_empty() && error.is_none() {
                match store.record_objects(&batch) {
                    Ok(states) => {
                        for ((_, outcome), state) in batch.iter().zip(states) {
                            tally(&mut report, outcome, state);
                        }
                        done += batch.len();
                        log(&format!("  {done}/{total} stored"));
                    }
                    Err(e) => {
                        abort.store(true, Ordering::Relaxed);
                        error = Some(e.into());
                    }
                }
                batch.clear();
                last_flush = Instant::now();
            }
            if closed {
                break;
            }
        }
    });
    match error {
        Some(e) => Err(e),
        None => Ok(report),
    }
}

fn tally(report: &mut FetchReport, outcome: &Outcome, state: Option<FetchState>) {
    report.attempted += 1;
    match (state, outcome) {
        (Some(FetchState::Ok), Outcome::Ok { verification, .. }) if verification.is_verified() => {
            report.ok_verified += 1
        }
        (Some(FetchState::Ok), _) => report.ok_unverified += 1,
        (Some(FetchState::Absent), _) => report.absent += 1,
        (Some(FetchState::Transient), Outcome::NotFound(_)) => {
            report.transient += 1;
            report.not_found += 1;
        }
        (Some(FetchState::Transient), _) => report.transient += 1,
        (Some(FetchState::Pending) | None, _) => {}
    }
}

/// The content layer's gateway order.
pub fn default_gateways() -> Vec<String> {
    crate::content::DEFAULT_IPFS_GATEWAYS
        .iter()
        .map(|g| (*g).to_owned())
        .collect()
}

/// Parses `SEEDED_IPFS_GATEWAYS` (comma-separated, at most eight, each ending in `/ipfs/`).
pub fn parse_gateways(list: &str) -> Result<Vec<String>, String> {
    let gateways = list
        .split(',')
        .filter(|g| !g.trim().is_empty())
        .map(super::ipfs::gateway)
        .collect::<Result<Vec<_>, _>>()?;
    if gateways.is_empty() || gateways.len() > 8 {
        return Err("SEEDED_IPFS_GATEWAYS needs one to eight gateways".into());
    }
    Ok(gateways)
}
