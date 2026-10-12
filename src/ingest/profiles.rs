//! `seeded-index profiles`: names, bios, links and avatar URIs per address, with provenance
//! (`profiles_imported`). Read precedence is in [`crate::store::profile`].
//!
//! Sources, verified 2026-10-05:
//! - **fxhash**: the "FXHASH User Registery" [`USER_REGISTRY`] (via TzKT). Big maps
//!   `users_name` (address -> hex UTF-8 name) and `users_metadata` (address -> hex
//!   `ipfs://<cid>` of `{"description", "identicon"}`), 19,112 keys each, unchanged since
//!   2023-10-26. The registry is read whole once, then only keys changed after the last level;
//!   metadata documents come from IPFS gateways, only for addresses that matter.
//! - **objkt**: `holder` in https://data.objkt.com/v3/graphql (one row per address, at most
//!   500 per answer), [`OBJKT_BATCH`] addresses per query: alias, description, website,
//!   social URLs, logo (an https URL on objkt's CDN) and flag. Flagged holders (flag other
//!   than `none`, e.g. `banned`) are not imported.
//! - **Tezos Profiles**: not resolvable. Profile contracts (`contract_type: tzprofiles`) keep
//!   only `kepler://` claim URLs; kepler.tzprofiles.com and indexer.tzprofiles.com no longer
//!   resolve, and tzprofiles.com redirects to github.com/spruceid/tzprofiles. Skipped.
//!
//! Addresses that matter: artists, collaboration members, holders and live-listing sellers.
//! Avatar images are never downloaded; only their URIs are stored. Every batch updates the
//! search indexes' shown names in its own transaction (`store::search`).
//!
//! Outcomes: an answer is applied (an objkt holder that is missing or flagged loses its objkt
//! row); an IPFS document every gateway serves as unusable JSON is invalid for good (content
//! is immutable); any other per-document failure leaves it pending for the next pass. Each
//! pass walks addresses in order with a committed cursor, so neither short runs nor documents
//! that keep failing starve later addresses. A source that cannot be reached at all (a whole
//! batch failing transiently, objkt or TzKT failing) stops the run as transient (exit 75),
//! keeping every committed batch.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde_json::{Value, json};

use super::http::{ApiError, Client};
use super::sync::to_i64;
use super::tzkt::{BigMapKey, Tzkt};
use super::{FINALITY, IngestError, now_iso};
use crate::store::profile::{MAX_IMPORTED_NAME_CHARS, https_link, name_key};
use crate::store::{Db, Store, StoreError, canonical_ipfs, is_tezos_address, search};

/// FXHASH User Registery (TzKT alias), originated by the FXHASH Admin.
pub const USER_REGISTRY: &str = "KT1Ezht4PDKZri7aVppVGT4Jkw39sesaFnww";
/// Addresses per objkt query: half objkt's 500-row answer cap, so an answer is never cut.
pub const OBJKT_BATCH: usize = 250;
/// objkt answers at most this many rows per query.
const OBJKT_ROW_CAP: usize = 500;
const REGISTRY_PAGE: usize = 1000;
/// More registry keys than this in one read is a malformed answer.
const REGISTRY_CAP: usize = 500_000;
/// Pending metadata rows read per scan step.
const SCAN: usize = 1000;
const MAX_BIO_CHARS: usize = 5000;
const MAX_IMPORTED_LINKS: usize = 10;

/// objkt `holder` fields that hold profile URLs, in display order.
const OBJKT_LINKS: [&str; 9] = [
    "twitter",
    "instagram",
    "github",
    "gitlab",
    "farcaster",
    "medium",
    "reddit",
    "facebook",
    "telegram",
];

const OBJKT_QUERY: &str = "query Profiles($addresses: [String!]) { holder(where: \
     {address: {_in: $addresses}}) { address alias description website logo flag twitter \
     instagram github gitlab farcaster medium reddit facebook telegram } }";

pub const TZPROFILES_NOTE: &str = "Tezos Profiles skipped: claims live on kepler.tzprofiles.com \
     and indexer.tzprofiles.com, which no longer resolve (tzprofiles.com redirects to \
     github.com/spruceid/tzprofiles)";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProfileSource {
    Fxhash,
    Objkt,
    Tzprofiles,
}

impl ProfileSource {
    pub fn as_str(self) -> &'static str {
        match self {
            ProfileSource::Fxhash => "fxhash",
            ProfileSource::Objkt => "objkt",
            ProfileSource::Tzprofiles => "tzprofiles",
        }
    }

    /// `fxhash`, `objkt`, `tzprofiles` or `all`.
    pub fn parse(s: &str) -> Option<Vec<ProfileSource>> {
        Some(match s {
            "fxhash" => vec![ProfileSource::Fxhash],
            "objkt" => vec![ProfileSource::Objkt],
            "tzprofiles" => vec![ProfileSource::Tzprofiles],
            "all" => vec![
                ProfileSource::Fxhash,
                ProfileSource::Objkt,
                ProfileSource::Tzprofiles,
            ],
            _ => return None,
        })
    }
}

#[derive(Debug, Clone)]
pub struct ProfilesOptions {
    pub sources: Vec<ProfileSource>,
    /// Per source and run: metadata documents (fxhash) or addresses (objkt).
    pub limit: Option<usize>,
    /// Concurrent IPFS fetches, 1..=16.
    pub concurrency: usize,
    /// Metadata documents per committed batch.
    pub batch: usize,
}

impl Default for ProfilesOptions {
    fn default() -> Self {
        ProfilesOptions {
            sources: ProfileSource::parse("all").unwrap_or_default(),
            limit: None,
            concurrency: 4,
            batch: 64,
        }
    }
}

/// Where each source is read from.
pub struct ProfileClients<'a> {
    pub tzkt: &'a Tzkt,
    /// IPFS gateways in fallback order; base `https://host/ipfs`, path `/<cid>`.
    pub ipfs: &'a [Client],
    /// The objkt GraphQL endpoint as base, path empty.
    pub objkt: &'a Client,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SourceReport {
    /// Registry keys read (fxhash).
    pub registry_keys: usize,
    /// Documents fetched (fxhash) or addresses answered (objkt).
    pub fetched: usize,
    /// Rows written.
    pub written: usize,
    /// Rows removed: registry keys gone, objkt holders missing or flagged.
    pub removed: usize,
    /// Documents found permanently unusable.
    pub invalid: usize,
    /// Documents no gateway served this time; retried next pass.
    pub unavailable: usize,
    /// The pass reached its last address this run.
    pub pass_complete: bool,
    /// Metadata documents of addresses that matter still pending (fxhash).
    pub pending: usize,
    pub note: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProfilesReport {
    pub sources: Vec<(ProfileSource, SourceReport)>,
    pub limited: bool,
    pub interrupted: bool,
    /// A source could not be reached; run again to resume.
    pub transient: bool,
}

/// How a source step ended.
enum Step {
    Done,
    Limited,
}

/// Runs the selected sources in order: fxhash, objkt, then Tezos Profiles (a note only).
pub fn sync_profiles(
    store: &mut Store,
    clients: &ProfileClients<'_>,
    opts: &ProfilesOptions,
    log: &(dyn Fn(&str) + Sync),
) -> Result<ProfilesReport, IngestError> {
    if !(1..=16).contains(&opts.concurrency) || !(1..=10_000).contains(&opts.batch) {
        return Err(IngestError::Usage(
            "profiles concurrency must be 1..=16 and batch 1..=10000".into(),
        ));
    }
    let mut report = ProfilesReport::default();
    let matter = addresses_that_matter(store.conn())?;
    log(&format!("profiles: {} addresses matter", matter.len()));
    for &source in &opts.sources {
        let mut sr = SourceReport::default();
        let result = match source {
            ProfileSource::Fxhash => fxhash(store, clients, opts, &matter, &mut sr, log),
            ProfileSource::Objkt => objkt(store, clients.objkt, opts, &matter, &mut sr, log),
            ProfileSource::Tzprofiles => {
                sr.note = Some(TZPROFILES_NOTE.into());
                Ok(Step::Done)
            }
        };
        let stop = match result {
            Ok(Step::Done) => false,
            Ok(Step::Limited) => {
                report.limited = true;
                false
            }
            Err(IngestError::Api(ApiError::Interrupted)) => {
                report.interrupted = true;
                true
            }
            Err(e) if e.is_transient() => {
                log(&format!("profiles {}: paused: {e}", source.as_str()));
                report.transient = true;
                true
            }
            Err(e) => return Err(e),
        };
        if source == ProfileSource::Fxhash {
            sr.pending = pending_documents(store.conn(), &matter)?;
        }
        report.sources.push((source, sr));
        if stop {
            break;
        }
    }
    Ok(report)
}

/// Artists, collaboration members, holders and live-listing sellers, sorted and distinct.
pub fn addresses_that_matter(c: &Connection) -> Result<Vec<String>, StoreError> {
    let rows: Vec<String> = c
        .prepare(
            "SELECT address FROM artists WHERE address IS NOT NULL \
             UNION SELECT member FROM collaborations \
             UNION SELECT holder_address FROM holder_counts WHERE held > 0 \
             UNION SELECT seller FROM escrow_listings ORDER BY 1",
        )
        .db()?
        .query_map([], |r| r.get(0))
        .db()?
        .collect::<rusqlite::Result<_>>()
        .db()?;
    Ok(rows.into_iter().filter(|a| is_tezos_address(a)).collect())
}

struct SyncState {
    cursor: Option<String>,
    synced_level: Option<i64>,
}

fn load_state(c: &Connection, source: &str) -> Result<SyncState, StoreError> {
    Ok(
        c.prepare_cached("SELECT cursor, synced_level FROM profile_sync WHERE source = ?1")
            .db()?
            .query_row([source], |r| {
                Ok(SyncState {
                    cursor: r.get(0)?,
                    synced_level: r.get(1)?,
                })
            })
            .optional()
            .db()?
            .unwrap_or(SyncState {
                cursor: None,
                synced_level: None,
            }),
    )
}

/// Saves the pass cursor; `None` ends the pass.
fn save_cursor(c: &Connection, source: &str, cursor: Option<&str>) -> Result<(), StoreError> {
    let now = now_iso();
    c.prepare_cached(
        "INSERT INTO profile_sync (source, cursor, pass_started_at, pass_done_at, updated_at) \
         VALUES (?1, ?2, CASE WHEN ?2 IS NOT NULL THEN ?3 END, CASE WHEN ?2 IS NULL THEN ?3 END, \
         ?3) ON CONFLICT (source) DO UPDATE SET cursor = excluded.cursor, \
         pass_started_at = CASE WHEN profile_sync.cursor IS NULL AND excluded.cursor IS NOT NULL \
           THEN excluded.updated_at ELSE profile_sync.pass_started_at END, \
         pass_done_at = CASE WHEN excluded.cursor IS NULL THEN excluded.updated_at \
           ELSE profile_sync.pass_done_at END, updated_at = excluded.updated_at",
    )
    .db()?
    .execute(params![source, cursor, now])
    .db()?;
    Ok(())
}

/// Text from an untrusted source: trimmed, control characters dropped (newlines and tabs
/// kept when `multiline`), at most `max` characters; `None` when blank.
pub fn clean_text(text: &str, max: usize, multiline: bool) -> Option<String> {
    let kept: String = text
        .chars()
        .filter(|c| !c.is_control() || (multiline && matches!(c, '\n' | '\t')))
        .take(max)
        .collect();
    let kept = kept.trim();
    (!kept.is_empty()).then(|| kept.to_owned())
}

/// A name from an untrusted source: one line, at most 100 characters (longer is dropped,
/// never cut), no control characters.
pub fn clean_name(text: &str) -> Option<String> {
    let t = text.trim();
    if t.is_empty()
        || t.chars().count() > MAX_IMPORTED_NAME_CHARS
        || t.chars().any(char::is_control)
    {
        return None;
    }
    Some(t.to_owned())
}

/// A website: an absolute http(s) URL with a host and no credentials, normalised.
fn website(text: &str) -> Option<String> {
    let t = text.trim();
    if t.is_empty() || t.len() > 300 || t.chars().any(|c| c.is_whitespace() || c.is_control()) {
        return None;
    }
    let url = reqwest::Url::parse(t).ok()?;
    (matches!(url.scheme(), "http" | "https")
        && url.host_str().is_some_and(|h| !h.is_empty())
        && url.username().is_empty()
        && url.password().is_none())
    .then(|| url.to_string())
}

/// An avatar URI: `ipfs://<cid>[/path]` or an https URL.
fn avatar(text: &str) -> Option<String> {
    let t = text.trim();
    if let Some(rest) = t.strip_prefix("ipfs://") {
        let root = rest.split('/').next().unwrap_or_default();
        return (canonical_ipfs(root).is_some()
            && t.len() <= 300
            && !t.chars().any(|c| c.is_whitespace() || c.is_control()))
        .then(|| t.to_owned());
    }
    https_link(t)
}

fn decode_hex_text(value: &Value) -> Option<String> {
    let bytes = hex::decode(value.as_str()?).ok()?;
    String::from_utf8(bytes).ok()
}

// ---- fxhash -------------------------------------------------------------------------------

fn fxhash(
    store: &mut Store,
    clients: &ProfileClients<'_>,
    opts: &ProfilesOptions,
    matter: &[String],
    sr: &mut SourceReport,
    log: &(dyn Fn(&str) + Sync),
) -> Result<Step, IngestError> {
    registry(store, clients.tzkt, sr)?;
    log(&format!(
        "profiles fxhash: registry applied ({} keys)",
        sr.registry_keys
    ));
    if clients.ipfs.is_empty() {
        return Err(IngestError::Usage("no IPFS gateway configured".into()));
    }
    metadata(store, clients.ipfs, opts, matter, sr, log)
}

/// Reads `path` of the registry: every key, or keys changed after `since`. Removed keys are
/// included either way.
fn read_map(tzkt: &Tzkt, path: &str, since: Option<i64>) -> Result<Vec<BigMapKey>, ApiError> {
    let filter = since.map_or_else(String::new, |level| format!("lastLevel.gt={level}"));
    let mut out = Vec::new();
    let mut after = 0;
    loop {
        let page = tzkt.bigmap_keys(USER_REGISTRY, path, &filter, after, REGISTRY_PAGE, |k| {
            since.is_none_or(|level| i64::try_from(k.last_level).is_ok_and(|l| l > level))
        })?;
        let n = page.len();
        if let Some(k) = page.last() {
            after = k.id;
        }
        out.extend(page);
        if out.len() > REGISTRY_CAP {
            return Err(ApiError::Permanent(format!(
                "registry {path}: more than {REGISTRY_CAP} keys"
            )));
        }
        if n < REGISTRY_PAGE {
            return Ok(out);
        }
    }
}

/// Applies the registry's final names and metadata URIs (keys last changed at or below head −
/// [`FINALITY`]). The level read advances to that final level only when no key changed above
/// it: a rollback restores a key's previous value and level, which may be older than any
/// overlap, so until then the next run reads from the same level again.
fn registry(store: &mut Store, tzkt: &Tzkt, sr: &mut SourceReport) -> Result<(), IngestError> {
    let since = load_state(store.conn(), "fxhash")?.synced_level;
    let level = tzkt.head_level()?.saturating_sub(FINALITY);
    let final_level = to_i64(level)?;
    // Incremental: keys changed above the final level, read first so a key a rollback restores
    // later shows in the reads below. A first read lists every key, removed ones included, so
    // its own rows show a change above the final level; a separate check could not make it
    // atomic.
    let mut unsettled = since.is_some()
        && (!read_map(tzkt, "users_name", Some(final_level))?.is_empty()
            || !read_map(tzkt, "users_metadata", Some(final_level))?.is_empty());
    let mut names = read_map(tzkt, "users_name", since)?;
    let mut metas = read_map(tzkt, "users_metadata", since)?;
    sr.registry_keys = names.len() + metas.len();
    unsettled |= names.iter().chain(&metas).any(|k| k.last_level > level);
    names.retain(|k| k.last_level <= level);
    metas.retain(|k| k.last_level <= level);
    let recorded = if unsettled { since } else { Some(final_level) };
    let now = now_iso();
    let tx = store
        .conn_mut()
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .db()?;
    let mut touched: HashSet<String> = HashSet::new();
    {
        let mut set_name = tx
            .prepare_cached(
                "INSERT INTO profiles_imported (address, source, name, name_key, fetched_at) \
                 VALUES (?1, 'fxhash', ?2, ?3, ?4) ON CONFLICT (address, source) DO UPDATE SET \
                 name = excluded.name, name_key = excluded.name_key, \
                 fetched_at = excluded.fetched_at",
            )
            .db()?;
        let mut clear_name = tx
            .prepare_cached(
                "UPDATE profiles_imported SET name = NULL, name_key = NULL, fetched_at = ?2 \
                 WHERE address = ?1 AND source = 'fxhash' AND name IS NOT NULL",
            )
            .db()?;
        for k in &names {
            if !is_tezos_address(&k.key) {
                continue;
            }
            let name = k
                .active
                .then(|| decode_hex_text(&k.value))
                .flatten()
                .and_then(|n| clean_name(&n));
            match name {
                Some(n) => {
                    sr.written += set_name
                        .execute(params![k.key, n, name_key(&n), now])
                        .db()?;
                }
                None => sr.removed += clear_name.execute(params![k.key, now]).db()?,
            }
            touched.insert(k.key.clone());
        }
        let mut current = tx
            .prepare_cached(
                "SELECT uri FROM profile_fetch WHERE source = 'fxhash' AND address = ?1",
            )
            .db()?;
        let mut set_fetch = tx
            .prepare_cached(
                "INSERT INTO profile_fetch (source, address, uri, state, attempts, detail, \
                 updated_at) VALUES ('fxhash', ?1, ?2, 'pending', 0, NULL, ?3) \
                 ON CONFLICT (source, address) DO UPDATE SET uri = excluded.uri, \
                 state = 'pending', attempts = 0, detail = NULL, updated_at = excluded.updated_at",
            )
            .db()?;
        let mut drop_fetch = tx
            .prepare_cached("DELETE FROM profile_fetch WHERE source = 'fxhash' AND address = ?1")
            .db()?;
        let mut clear_doc = tx
            .prepare_cached(
                "UPDATE profiles_imported SET bio = NULL, avatar_uri = NULL, fetched_at = ?2 \
                 WHERE address = ?1 AND source = 'fxhash'",
            )
            .db()?;
        for k in &metas {
            if !is_tezos_address(&k.key) {
                continue;
            }
            let uri = k
                .active
                .then(|| decode_hex_text(&k.value))
                .flatten()
                .map(|u| u.trim().to_owned())
                .filter(|u| !u.is_empty() && u.len() <= 300);
            let old: Option<String> = current.query_row([&k.key], |r| r.get(0)).optional().db()?;
            if uri == old {
                continue;
            }
            // The previous document no longer describes the address.
            clear_doc.execute(params![k.key, now]).db()?;
            match uri {
                Some(u) => {
                    set_fetch.execute(params![k.key, u, now]).db()?;
                }
                None => {
                    sr.removed += drop_fetch.execute([&k.key]).db()?;
                }
            }
            touched.insert(k.key.clone());
        }
        let mut prune = tx
            .prepare_cached(
                "DELETE FROM profiles_imported WHERE address = ?1 AND source = 'fxhash' \
                 AND name IS NULL AND bio IS NULL AND avatar_uri IS NULL AND website IS NULL \
                 AND links = '[]'",
            )
            .db()?;
        for a in &touched {
            prune.execute([a]).db()?;
        }
    }
    tx.execute(
        "INSERT INTO profile_sync (source, synced_level, updated_at) VALUES ('fxhash', ?1, ?2) \
         ON CONFLICT (source) DO UPDATE SET synced_level = excluded.synced_level, \
         updated_at = excluded.updated_at",
        params![recorded, now],
    )
    .db()?;
    crate::store::assign_names(&tx)?;
    search::drain(&tx)?;
    tx.commit().db()?;
    Ok(())
}

/// One metadata document's outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Doc {
    Usable {
        bio: Option<String>,
        avatar: Option<String>,
    },
    /// Every gateway served it, and it is not a usable document.
    Invalid(String),
    /// Not served this time (missing, refused, or a mix of failures).
    Unavailable(String),
    /// Every gateway failed transiently.
    Transient(String),
    Interrupted,
}

fn parse_doc(v: &Value) -> Doc {
    let Some(o) = v.as_object() else {
        return Doc::Invalid("metadata is not a JSON object".into());
    };
    Doc::Usable {
        bio: o
            .get("description")
            .and_then(Value::as_str)
            .and_then(|d| clean_text(d, MAX_BIO_CHARS, true)),
        avatar: o
            .get("identicon")
            .or_else(|| o.get("avatar"))
            .and_then(Value::as_str)
            .and_then(avatar),
    }
}

/// `ipfs://<cid>` only; the CID in canonical form, or why not.
fn document_cid(uri: &str) -> Result<String, String> {
    let rest = uri
        .strip_prefix("ipfs://")
        .ok_or_else(|| "metadata URI is not ipfs://".to_owned())?;
    canonical_ipfs(rest).ok_or_else(|| "metadata URI is not ipfs://<cid>".to_owned())
}

fn fetch_doc(ipfs: &[Client], uri: &str) -> Doc {
    let cid = match document_cid(uri) {
        Ok(c) => c,
        Err(why) => return Doc::Invalid(why),
    };
    let (mut unusable, mut transient, mut other) = (0, 0, Vec::new());
    for gw in ipfs {
        match gw.get_json::<Value>(&format!("/{cid}")) {
            Ok(Some(v)) => return parse_doc(&v),
            Ok(None) => other.push(format!("{}: not found", gw.base())),
            Err(ApiError::Interrupted) => return Doc::Interrupted,
            Err(ApiError::Transient(e)) => {
                transient += 1;
                other.push(e);
            }
            Err(ApiError::Permanent(e)) if e.contains("unusable JSON") => {
                unusable += 1;
                other.push(e);
            }
            Err(ApiError::Permanent(e)) => other.push(e),
        }
    }
    let detail = other.join("; ");
    if unusable == ipfs.len() {
        Doc::Invalid(detail)
    } else if transient == ipfs.len() {
        Doc::Transient(detail)
    } else {
        Doc::Unavailable(detail)
    }
}

/// Fetches `jobs` with at most `concurrency` workers.
fn fetch_all(ipfs: &[Client], jobs: &[(String, String)], concurrency: usize) -> Vec<Doc> {
    let next = AtomicUsize::new(0);
    let out: Mutex<Vec<Option<Doc>>> = Mutex::new(vec![None; jobs.len()]);
    std::thread::scope(|s| {
        for _ in 0..concurrency.min(jobs.len()) {
            s.spawn(|| {
                loop {
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    let Some((_, uri)) = jobs.get(i) else { break };
                    let doc = fetch_doc(ipfs, uri);
                    out.lock().unwrap_or_else(|p| p.into_inner())[i] = Some(doc);
                }
            });
        }
    });
    out.into_inner()
        .unwrap_or_else(|p| p.into_inner())
        .into_iter()
        .map(|d| d.unwrap_or(Doc::Interrupted))
        .collect()
}

fn metadata(
    store: &mut Store,
    ipfs: &[Client],
    opts: &ProfilesOptions,
    matter: &[String],
    sr: &mut SourceReport,
    log: &(dyn Fn(&str) + Sync),
) -> Result<Step, IngestError> {
    let matter: HashSet<&str> = matter.iter().map(String::as_str).collect();
    let mut budget = opts.limit;
    loop {
        if ipfs.iter().any(Client::stopped) {
            return Err(ApiError::Interrupted.into());
        }
        if budget == Some(0) {
            return Ok(Step::Limited);
        }
        let cursor = load_state(store.conn(), "fxhash")?.cursor;
        let scanned: Vec<(String, String)> = store
            .conn()
            .prepare_cached(
                "SELECT address, uri FROM profile_fetch WHERE source = 'fxhash' \
                 AND state = 'pending' AND address > ?1 ORDER BY address LIMIT ?2",
            )
            .db()?
            .query_map(params![cursor.as_deref().unwrap_or(""), SCAN as i64], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .db()?
            .collect::<rusqlite::Result<_>>()
            .db()?;
        if scanned.is_empty() {
            save_cursor(store.conn(), "fxhash", None)?;
            sr.pass_complete = true;
            return Ok(Step::Done);
        }
        let want = budget.map_or(opts.batch, |b| b.min(opts.batch));
        let mut jobs = Vec::new();
        let mut last = scanned[0].0.clone();
        for (address, uri) in scanned {
            if jobs.len() == want {
                break;
            }
            last = address.clone();
            if matter.contains(address.as_str()) {
                jobs.push((address, uri));
            }
        }
        let docs = fetch_all(ipfs, &jobs, opts.concurrency);
        let interrupted = docs.iter().any(|d| *d == Doc::Interrupted);
        let all_transient = !docs.is_empty() && docs.iter().all(|d| matches!(d, Doc::Transient(_)));
        // Finished documents are kept either way. An interrupted batch keeps the cursor so
        // its unfinished documents come first next time; a failed one moves past them (they
        // stay pending for the next pass), so no document can stall the pass.
        apply_docs(
            store,
            &jobs,
            &docs,
            (!interrupted).then_some(last.as_str()),
            sr,
        )?;
        if interrupted {
            return Err(ApiError::Interrupted.into());
        }
        if let Some(b) = &mut budget {
            *b = b.saturating_sub(jobs.len());
        }
        if all_transient {
            let detail = match &docs[0] {
                Doc::Transient(d) => d.clone(),
                _ => String::new(),
            };
            return Err(ApiError::Transient(format!("every IPFS gateway failed: {detail}")).into());
        }
        log(&format!(
            "profiles fxhash: {} documents fetched, through {last}",
            sr.fetched
        ));
    }
}

fn apply_docs(
    store: &mut Store,
    jobs: &[(String, String)],
    docs: &[Doc],
    cursor: Option<&str>,
    sr: &mut SourceReport,
) -> Result<(), IngestError> {
    let now = now_iso();
    let tx = store
        .conn_mut()
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .db()?;
    {
        let mut done = tx
            .prepare_cached(
                "UPDATE profile_fetch SET state = ?3, detail = ?4, updated_at = ?5, \
                 attempts = attempts + 1 WHERE source = 'fxhash' AND address = ?1 AND uri = ?2",
            )
            .db()?;
        let mut write = tx
            .prepare_cached(
                "INSERT INTO profiles_imported (address, source, bio, avatar_uri, fetched_at) \
                 VALUES (?1, 'fxhash', ?2, ?3, ?4) ON CONFLICT (address, source) DO UPDATE SET \
                 bio = excluded.bio, avatar_uri = excluded.avatar_uri, \
                 fetched_at = excluded.fetched_at",
            )
            .db()?;
        let mut prune = tx
            .prepare_cached(
                "DELETE FROM profiles_imported WHERE address = ?1 AND source = 'fxhash' \
                 AND name IS NULL AND bio IS NULL AND avatar_uri IS NULL AND website IS NULL \
                 AND links = '[]'",
            )
            .db()?;
        for ((address, uri), doc) in jobs.iter().zip(docs) {
            match doc {
                Doc::Usable { bio, avatar } => {
                    // Only while the registry still points at this document.
                    if done
                        .execute(params![address, uri, "done", None::<String>, now])
                        .db()?
                        > 0
                    {
                        write.execute(params![address, bio, avatar, now]).db()?;
                        prune.execute([address]).db()?;
                        sr.written += 1;
                    }
                    sr.fetched += 1;
                }
                Doc::Invalid(why) => {
                    done.execute(params![address, uri, "invalid", why, now])
                        .db()?;
                    sr.invalid += 1;
                }
                Doc::Unavailable(why) | Doc::Transient(why) => {
                    done.execute(params![address, uri, "pending", why, now])
                        .db()?;
                    sr.unavailable += 1;
                }
                Doc::Interrupted => {}
            }
        }
    }
    if let Some(last) = cursor {
        save_cursor(&tx, "fxhash", Some(last))?;
    }
    crate::store::assign_names(&tx)?;
    search::drain(&tx)?;
    tx.commit().db()?;
    Ok(())
}

fn pending_documents(c: &Connection, matter: &[String]) -> Result<usize, StoreError> {
    let matter: HashSet<&str> = matter.iter().map(String::as_str).collect();
    let pending: Vec<String> = c
        .prepare_cached(
            "SELECT address FROM profile_fetch WHERE source = 'fxhash' AND state = 'pending'",
        )
        .db()?
        .query_map([], |r| r.get(0))
        .db()?
        .collect::<rusqlite::Result<_>>()
        .db()?;
    Ok(pending
        .iter()
        .filter(|a| matter.contains(a.as_str()))
        .count())
}

// ---- objkt --------------------------------------------------------------------------------

/// One objkt holder, cleaned; `None` when nothing is worth keeping.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjktProfile {
    pub name: Option<String>,
    pub bio: Option<String>,
    pub website: Option<String>,
    pub links: Vec<String>,
    pub avatar_uri: Option<String>,
}

/// Cleans an objkt `holder` row. Flagged holders yield `None`.
pub fn objkt_profile(row: &Value) -> Option<ObjktProfile> {
    let text = |k: &str| row.get(k).and_then(Value::as_str);
    if text("flag").is_some_and(|f| f != "none") {
        return None;
    }
    let mut links: Vec<String> = Vec::new();
    for k in OBJKT_LINKS {
        if let Some(l) = text(k).and_then(https_link) {
            if !links.contains(&l) && links.len() < MAX_IMPORTED_LINKS {
                links.push(l);
            }
        }
    }
    let p = ObjktProfile {
        name: text("alias").and_then(clean_name),
        bio: text("description").and_then(|d| clean_text(d, MAX_BIO_CHARS, true)),
        website: text("website").and_then(website),
        links,
        avatar_uri: text("logo").and_then(avatar),
    };
    let empty = p.name.is_none()
        && p.bio.is_none()
        && p.website.is_none()
        && p.links.is_empty()
        && p.avatar_uri.is_none();
    (!empty).then_some(p)
}

fn objkt(
    store: &mut Store,
    client: &Client,
    opts: &ProfilesOptions,
    matter: &[String],
    sr: &mut SourceReport,
    log: &(dyn Fn(&str) + Sync),
) -> Result<Step, IngestError> {
    let mut budget = opts.limit;
    let cursor = load_state(store.conn(), "objkt")?.cursor;
    let mut start = match &cursor {
        Some(c) => matter.partition_point(|a| a.as_str() <= c.as_str()),
        None => 0,
    };
    loop {
        if client.stopped() {
            return Err(ApiError::Interrupted.into());
        }
        if start >= matter.len() {
            save_cursor(store.conn(), "objkt", None)?;
            sr.pass_complete = true;
            return Ok(Step::Done);
        }
        if budget == Some(0) {
            return Ok(Step::Limited);
        }
        let n = budget.map_or(OBJKT_BATCH, |b| b.min(OBJKT_BATCH));
        let chunk = &matter[start..(start + n).min(matter.len())];
        let answer: Value = client
            .post_json(
                "",
                &json!({ "query": OBJKT_QUERY, "variables": { "addresses": chunk } }),
            )?
            .ok_or_else(|| ApiError::Permanent("objkt GraphQL endpoint is absent".into()))?;
        if let Some(errors) = answer.get("errors") {
            return Err(ApiError::Permanent(format!("objkt GraphQL errors: {errors}")).into());
        }
        let rows = answer
            .pointer("/data/holder")
            .and_then(Value::as_array)
            .ok_or_else(|| ApiError::Permanent("objkt answer has no data.holder list".into()))?;
        if rows.len() >= OBJKT_ROW_CAP.min(chunk.len() + 1) {
            return Err(ApiError::Permanent(format!(
                "objkt answered {} rows for {} addresses",
                rows.len(),
                chunk.len()
            ))
            .into());
        }
        let mut by_address: HashMap<&str, &Value> = HashMap::new();
        for r in rows {
            if let Some(a) = r.get("address").and_then(Value::as_str) {
                by_address.insert(a, r);
            }
        }
        apply_objkt(store, chunk, &by_address, sr)?;
        start += chunk.len();
        if let Some(b) = &mut budget {
            *b = b.saturating_sub(chunk.len());
        }
        log(&format!(
            "profiles objkt: {} addresses answered, through {}",
            sr.fetched,
            chunk.last().map_or("", String::as_str)
        ));
    }
}

fn apply_objkt(
    store: &mut Store,
    chunk: &[String],
    by_address: &HashMap<&str, &Value>,
    sr: &mut SourceReport,
) -> Result<(), IngestError> {
    let now = now_iso();
    let tx = store
        .conn_mut()
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .db()?;
    {
        let mut upsert = tx
            .prepare_cached(
                "INSERT INTO profiles_imported (address, source, name, name_key, bio, website, \
                 links, avatar_uri, fetched_at) VALUES (?1, 'objkt', ?2, ?3, ?4, ?5, ?6, ?7, ?8) \
                 ON CONFLICT (address, source) DO UPDATE SET name = excluded.name, \
                 name_key = excluded.name_key, bio = excluded.bio, website = excluded.website, \
                 links = excluded.links, avatar_uri = excluded.avatar_uri, \
                 fetched_at = excluded.fetched_at",
            )
            .db()?;
        let mut remove = tx
            .prepare_cached("DELETE FROM profiles_imported WHERE address = ?1 AND source = 'objkt'")
            .db()?;
        for address in chunk {
            sr.fetched += 1;
            match by_address
                .get(address.as_str())
                .and_then(|r| objkt_profile(r))
            {
                Some(p) => {
                    let links = serde_json::to_string(&p.links)
                        .map_err(|e| IngestError::Io(e.to_string()))?;
                    upsert
                        .execute(params![
                            address,
                            p.name,
                            p.name.as_deref().map(name_key),
                            p.bio,
                            p.website,
                            links,
                            p.avatar_uri,
                            now
                        ])
                        .db()?;
                    sr.written += 1;
                }
                None => sr.removed += remove.execute([address]).db()?,
            }
        }
    }
    let last = chunk.last().map(String::as_str);
    save_cursor(&tx, "objkt", last)?;
    crate::store::assign_names(&tx)?;
    search::drain(&tx)?;
    tx.commit().db()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn objkt_rows_are_cleaned_and_flagged_holders_dropped() {
        let row = json!({
            "address": "tz1gL9H53BzjDqmMi2JkKALeCzT7UAoopzEL",
            "alias": " EmptyName ",
            "description": "Artist\u{0007} and philosopher",
            "website": "http://emptyname.org",
            "twitter": "https://x.com/emptyname_org",
            "github": "javascript:alert(1)",
            "farcaster": "https://x.com/emptyname_org",
            "logo": "https://assets.objkt.media/file/assets-004/h/x/logo?cb=1",
            "flag": "none"
        });
        let p = objkt_profile(&row).unwrap();
        assert_eq!(p.name.as_deref(), Some("EmptyName"));
        assert_eq!(p.bio.as_deref(), Some("Artist and philosopher"));
        assert_eq!(p.website.as_deref(), Some("http://emptyname.org/"));
        assert_eq!(p.links, vec!["https://x.com/emptyname_org".to_owned()]);
        assert!(
            p.avatar_uri
                .unwrap()
                .starts_with("https://assets.objkt.media/")
        );
        let mut banned = row.clone();
        banned["flag"] = "banned".into();
        assert_eq!(objkt_profile(&banned), None);
        assert_eq!(
            objkt_profile(&json!({"address": "tz1", "flag": "none"})),
            None
        );
    }

    #[test]
    fn metadata_documents_parse_description_and_identicon() {
        let doc = parse_doc(&json!({
            "description": "hello\n",
            "identicon": "ipfs://QmURUAU4YPa6Wwco3JSVrcN7WfCrFBZH7hY51BLrc87WjM"
        }));
        assert_eq!(
            doc,
            Doc::Usable {
                bio: Some("hello".into()),
                avatar: Some("ipfs://QmURUAU4YPa6Wwco3JSVrcN7WfCrFBZH7hY51BLrc87WjM".into())
            }
        );
        assert_eq!(
            parse_doc(&json!({"description": "", "identicon": ""})),
            Doc::Usable {
                bio: None,
                avatar: None
            }
        );
        assert!(matches!(parse_doc(&json!([1])), Doc::Invalid(_)));
        assert!(document_cid("ipfs://QmURUAU4YPa6Wwco3JSVrcN7WfCrFBZH7hY51BLrc87WjM").is_ok());
        assert!(document_cid("https://x/y").is_err());
        assert!(document_cid("ipfs://QmURUAU4YPa6Wwco3JSVrcN7WfCrFBZH7hY51BLrc87WjM/a").is_err());
    }

    #[test]
    fn imported_text_is_bounded() {
        assert_eq!(clean_name(&"n".repeat(101)), None);
        assert_eq!(clean_name("a\u{0000}b"), None);
        assert_eq!(clean_name("  ok "), Some("ok".into()));
        assert_eq!(clean_text("a\nb\u{0007}", 10, true), Some("a\nb".into()));
        assert_eq!(clean_text("a\nb", 10, false), Some("ab".into()));
        assert_eq!(clean_text(&"x".repeat(20), 5, true), Some("xxxxx".into()));
        assert_eq!(avatar("ipfs://not-a-cid"), None);
        assert_eq!(avatar("javascript:x"), None);
    }
}
