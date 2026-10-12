//! fxhash marketplace v2 buy offers, read through TzKT from its `offers` (token offers) and
//! `collection_offers` big maps, and the block times of offer and listing creation levels.
//!
//! Each offer book keeps a resumable pass in `bigmap_state`, like escrow passes:
//! - full (first run): live keys only (`active=true`; collection offers also `amount > 0`),
//!   paged by big-map key id, each page committed with its cursor;
//! - incremental: keys changed after `synced_level`, removed ones included.
//!
//! A pass is recorded at a final level (head − [`FINALITY`]), while it reads current state.
//! Rows stored from a change above the previous pass's level were not final when read: the
//! next incremental pass first re-reads those keys at any level and applies what TzKT has
//! then, so a change dropped by a reorg is undone.
//!
//! A live key upserts its offer. A removed key (cancelled or accepted), an exhausted
//! collection offer (`amount` 0) or a key naming an unknown token version marks a stored offer
//! inactive; the row stays so a lookup can tell that the offer ended.
//!
//! Upstream rows are validated: ids, levels, prices and amounts must be decimal strings that
//! fit SQLite, addresses well-formed, prices positive and `price × amount` within mutez. A
//! violation rejects the whole page (permanent error, nothing written, cursor unchanged).
//!
//! Block times: levels of live offers and listings without a time are fetched newest first,
//! 100 per request, only at least [`FINALITY`] blocks below head, and stored once.
//!
//! Jobs (each book, block times) take turns by `sync_rotation`: the least recently served
//! goes first, so a run limit never starves one of them. A permanent error in one book does
//! not stop the others; the run still fails at the end.

use std::collections::BTreeSet;

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde::Deserialize;
use serde_json::Value;

pub use super::FINALITY;
use super::http::ApiError;
use super::tzkt::{HasId, MAX_IN, MAX_PAGE, Tzkt, check_page};
use super::{FX_PARAMS, GENTK_V1, GENTK_V2, IngestError, now_iso};
use crate::store::market::{FXHASH_V2, OfferKind};
use crate::store::{Db, Store, StoreError, is_tezos_address};

/// One offer big map of a marketplace.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OfferBook {
    pub marketplace: &'static str,
    pub path: &'static str,
    pub kind: OfferKind,
}

/// fxhash marketplace v1 has no buy offers (its "offers" are listings, see `escrow`).
pub const BOOKS: [OfferBook; 2] = [
    OfferBook {
        marketplace: FXHASH_V2.contract,
        path: "offers",
        kind: OfferKind::Token,
    },
    OfferBook {
        marketplace: FXHASH_V2.contract,
        path: "collection_offers",
        kind: OfferKind::Collection,
    },
];

/// fxhash marketplace v2 `gentk_contracts` (verified 2026-10-05): `version` -> token contract.
pub const GENTK_VERSIONS: [&str; 3] = [GENTK_V1, GENTK_V2, FX_PARAMS];

/// What an offer key targets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Target {
    Token {
        contract: &'static str,
        token_id: u64,
    },
    /// fxhash project (issuer) id: any token whose `get_token_data.issuer_id` equals it.
    Collection { project_id: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveOffer {
    pub buyer: String,
    /// Mutez per token, > 0.
    pub price: u64,
    /// Tokens still wanted: 1 for token offers; `price * remaining` fits i64.
    pub remaining: u64,
    pub target: Target,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OfferState {
    Live(LiveOffer),
    /// Removed from the big map (cancelled or accepted), or exhausted (collection `amount` 0).
    Ended {
        exhausted: bool,
    },
    /// Well-typed, but names a token version this archive does not know.
    Unsupported,
}

/// One big-map key, validated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OfferKey {
    /// TzKT's key id: the paging cursor.
    pub id: u64,
    pub offer_id: u64,
    pub active: bool,
    pub first_level: u64,
    pub last_level: u64,
    pub state: OfferState,
}

impl HasId for OfferKey {
    fn id(&self) -> u64 {
        self.id
    }
}

/// A decimal nat string that fits SQLite's i64.
fn nat(v: &Value, what: &str) -> Result<u64, String> {
    let s = v
        .as_str()
        .ok_or_else(|| format!("{what} is not a decimal string: {v}"))?;
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return Err(format!("{what} {s:?} is not a decimal nat"));
    }
    s.parse::<u64>()
        .ok()
        .filter(|n| i64::try_from(*n).is_ok())
        .ok_or_else(|| format!("{what} {s:?} is not below 2^63"))
}

fn field<'a>(v: &'a Value, path: &[&str]) -> &'a Value {
    path.iter().fold(v, |v, k| &v[*k])
}

/// Interprets one offer key from TzKT (`key`, `value`, `active`). Reusable for a click-time
/// re-check of `GET /v1/bigmaps/<ptr>/keys/<id>`. `Err` is upstream data that contradicts
/// the contract's types.
pub fn parse_offer(
    kind: OfferKind,
    key: &Value,
    value: &Value,
    active: bool,
) -> Result<(u64, OfferState), String> {
    let offer_id = nat(key, "offer id")?;
    if !active {
        return Ok((offer_id, OfferState::Ended { exhausted: false }));
    }
    let buyer = field(value, &["buyer"])
        .as_str()
        .filter(|a| is_tezos_address(a))
        .ok_or_else(|| format!("offer {offer_id}: buyer is not an address"))?
        .to_owned();
    let price = nat(field(value, &["price"]), "price")?;
    if price == 0 {
        return Err(format!("offer {offer_id}: price 0"));
    }
    let state = match kind {
        OfferKind::Token => {
            let token_id = nat(field(value, &["gentk", "id"]), "gentk id")?;
            let version = nat(field(value, &["gentk", "version"]), "gentk version")?;
            match usize::try_from(version)
                .ok()
                .and_then(|v| GENTK_VERSIONS.get(v))
            {
                Some(contract) => OfferState::Live(LiveOffer {
                    buyer,
                    price,
                    remaining: 1,
                    target: Target::Token { contract, token_id },
                }),
                None => OfferState::Unsupported,
            }
        }
        OfferKind::Collection => {
            let remaining = nat(field(value, &["amount"]), "amount")?;
            let project_id = nat(field(value, &["collection"]), "collection")?;
            price
                .checked_mul(remaining)
                .filter(|n| i64::try_from(*n).is_ok())
                .ok_or_else(|| format!("offer {offer_id}: price × amount exceeds mutez"))?;
            if remaining == 0 {
                OfferState::Ended { exhausted: true }
            } else {
                OfferState::Live(LiveOffer {
                    buyer,
                    price,
                    remaining,
                    target: Target::Collection { project_id },
                })
            }
        }
    };
    Ok((offer_id, state))
}

#[derive(Deserialize)]
struct RawKey {
    /// `null` when TzKT is asked with `select` (never done here): rejected.
    id: Option<u64>,
    key: Value,
    #[serde(default)]
    value: Value,
    active: bool,
    #[serde(rename = "firstLevel")]
    first_level: u64,
    #[serde(rename = "lastLevel")]
    last_level: u64,
}

fn bad(book: &OfferBook, detail: impl std::fmt::Display) -> ApiError {
    ApiError::Permanent(format!(
        "malformed TzKT key in {}/{}: {detail}",
        book.marketplace, book.path
    ))
}

impl OfferBook {
    fn name(&self) -> String {
        format!("{}/{}", self.marketplace, self.path)
    }

    fn parse_raw(&self, r: RawKey) -> Result<OfferKey, ApiError> {
        let id =
            r.id.filter(|i| i64::try_from(*i).is_ok())
                .ok_or_else(|| bad(self, "key id missing or not below 2^63"))?;
        if r.first_level > r.last_level || i64::try_from(r.last_level).is_err() {
            return Err(bad(
                self,
                format!("key {id}: levels {}..{}", r.first_level, r.last_level),
            ));
        }
        let (offer_id, state) =
            parse_offer(self.kind, &r.key, &r.value, r.active).map_err(|e| bad(self, e))?;
        Ok(OfferKey {
            id,
            offer_id,
            active: r.active,
            first_level: r.first_level,
            last_level: r.last_level,
            state,
        })
    }

    /// Live keys only, filtered server-side.
    fn full_filter(&self) -> &'static str {
        match self.kind {
            OfferKind::Token => "active=true",
            OfferKind::Collection => "active=true&value.amount.gt=0",
        }
    }

    /// One validated page of keys after key id `after`. Big-map keys page with `offset.cr`.
    fn keys_page(
        &self,
        api: &Tzkt,
        filter: &str,
        after: u64,
        page: usize,
        keep: impl Fn(&OfferKey) -> bool,
    ) -> Result<Vec<OfferKey>, ApiError> {
        let rows: Vec<RawKey> = api
            .client
            .get_json(&format!(
                "/contracts/{}/bigmaps/{}/keys?{filter}&sort.asc=id&offset.cr={after}\
                 &limit={page}",
                self.marketplace, self.path
            ))?
            .ok_or_else(|| ApiError::Permanent(format!("big map {} is absent", self.name())))?;
        let keys = rows
            .into_iter()
            .map(|r| self.parse_raw(r))
            .collect::<Result<Vec<_>, _>>()?;
        check_page(
            &format!("big map {}", self.name()),
            &keys,
            page,
            after,
            &keep,
        )?;
        Ok(keys)
    }
}

#[derive(Debug, Clone, Default)]
struct State {
    synced_level: Option<u64>,
    pass_level: Option<u64>,
    pass_full: bool,
    cursor: u64,
}

fn load(c: &Connection, book: &OfferBook) -> Result<State, StoreError> {
    Ok(c.query_row(
        "SELECT synced_level, pass_level, pass_full, cursor_id FROM bigmap_state \
             WHERE contract = ?1 AND path = ?2",
        [book.marketplace, book.path],
        |r| {
            Ok(State {
                synced_level: r.get::<_, Option<i64>>(0)?.map(|v| v as u64),
                pass_level: r.get::<_, Option<i64>>(1)?.map(|v| v as u64),
                pass_full: r.get::<_, Option<i64>>(2)?.unwrap_or(0) != 0,
                cursor: r.get::<_, i64>(3)? as u64,
            })
        },
    )
    .optional()
    .db()?
    .unwrap_or_default())
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OfferReport {
    /// Key pages and block-time chunks committed.
    pub batches: u64,
    /// Live keys written.
    pub live: u64,
    /// Removed or exhausted keys applied.
    pub ended: u64,
    /// Keys naming an unknown token version (not stored as live).
    pub unsupported: u64,
    /// Rows stored from a non-final change whose keys were re-read.
    pub rechecked: u64,
    /// Rechecked rows whose key TzKT no longer has (marked ended).
    pub recheck_missing: u64,
    pub times_written: u64,
    /// Final levels TzKT returned no block for; retried next run.
    pub times_missing: u64,
    /// `contract/path` of books whose pass completed this run.
    pub completed: Vec<String>,
    /// Books still without a completed pass, or with one in progress.
    pub pending: Vec<String>,
    /// The run's last block-times phase left no final level of a stored live offer or
    /// listing unrequested.
    pub times_complete: bool,
    pub limited: bool,
    pub interrupted: bool,
    pub transient_failures: u64,
}

/// Applies keys in one transaction, then `after` (cursor update) inside it. Guarded writes
/// ignore data older than what is stored (a lower `lastLevel`), so duplicated or reordered
/// pages cannot regress; a recheck writes unguarded, to undo reorged-out changes.
fn apply_keys(
    tx: &Connection,
    book: &OfferBook,
    keys: &[OfferKey],
    guarded: bool,
    report: &mut OfferReport,
) -> Result<(), StoreError> {
    let kind = book.kind.as_str();
    let guard = if guarded {
        "WHERE excluded.level >= market_offers.level"
    } else {
        ""
    };
    let end_guard = if guarded { "AND level <= ?4" } else { "" };
    let (mut live, mut ended, mut unsupported) = (0, 0, 0);
    for k in keys {
        match &k.state {
            OfferState::Live(o) => {
                let (contract, token_id, project_id) = match &o.target {
                    Target::Token { contract, token_id } => {
                        (Some(*contract), Some(*token_id as i64), None)
                    }
                    Target::Collection { project_id } => (None, None, Some(*project_id as i64)),
                };
                tx.prepare_cached(&format!(
                    "INSERT INTO market_offers (marketplace, kind, offer_id, buyer, price, \
                     remaining, token_contract, token_id, project_id, created_level, level, \
                     active) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, 1) \
                     ON CONFLICT (marketplace, kind, offer_id) DO UPDATE SET \
                     buyer = excluded.buyer, price = excluded.price, \
                     remaining = excluded.remaining, token_contract = excluded.token_contract, \
                     token_id = excluded.token_id, project_id = excluded.project_id, \
                     created_level = excluded.created_level, level = excluded.level, active = 1 \
                     {guard}"
                ))
                .db()?
                .execute(params![
                    book.marketplace,
                    kind,
                    k.offer_id as i64,
                    o.buyer,
                    o.price as i64,
                    o.remaining as i64,
                    contract,
                    token_id,
                    project_id,
                    k.first_level as i64,
                    k.last_level as i64,
                ])
                .db()?;
                live += 1;
            }
            OfferState::Ended { .. } | OfferState::Unsupported => {
                let exhausted = matches!(k.state, OfferState::Ended { exhausted: true });
                tx.prepare_cached(&format!(
                    "UPDATE market_offers SET active = 0, level = ?4, \
                     remaining = CASE WHEN ?5 THEN 0 ELSE remaining END \
                     WHERE marketplace = ?1 AND kind = ?2 AND offer_id = ?3 {end_guard}"
                ))
                .db()?
                .execute(params![
                    book.marketplace,
                    kind,
                    k.offer_id as i64,
                    k.last_level as i64,
                    exhausted
                ])
                .db()?;
                if k.state == OfferState::Unsupported {
                    unsupported += 1;
                } else {
                    ended += 1;
                }
            }
        }
    }
    report.live += live;
    report.ended += ended;
    report.unsupported += unsupported;
    Ok(())
}

/// Applies one page of keys and advances the cursor, in one transaction.
fn write_page(
    store: &mut Store,
    book: &OfferBook,
    keys: &[OfferKey],
    cursor: u64,
    report: &mut OfferReport,
) -> Result<(), StoreError> {
    let tx = store
        .conn_mut()
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .db()?;
    let mut counts = OfferReport::default();
    apply_keys(&tx, book, keys, true, &mut counts)?;
    tx.execute(
        "UPDATE bigmap_state SET cursor_id = ?3, updated_at = ?4 WHERE contract = ?1 AND path = ?2",
        params![book.marketplace, book.path, cursor as i64, now_iso()],
    )
    .db()?;
    tx.commit().db()?;
    report.batches += 1;
    report.live += counts.live;
    report.ended += counts.ended;
    report.unsupported += counts.unsupported;
    Ok(())
}

/// Most rows one recheck re-reads (100 per request); more wait for the next pass.
const MAX_RECHECK: usize = 10_000;

/// Re-reads the keys of rows stored from a change above `since` (not final when read) and
/// applies their current state unguarded; rows whose key is gone are marked ended.
/// `Ok(false)` when stopped or out of budget.
fn recheck(
    store: &mut Store,
    api: &Tzkt,
    book: &OfferBook,
    since: u64,
    budget: &mut Option<usize>,
    report: &mut OfferReport,
) -> Result<bool, IngestError> {
    let ids: Vec<u64> = store
        .conn_mut()
        .prepare(
            "SELECT offer_id FROM market_offers WHERE marketplace = ?1 AND kind = ?2 \
             AND level > ?3 ORDER BY offer_id LIMIT ?4",
        )
        .db()?
        .query_map(
            params![
                book.marketplace,
                book.kind.as_str(),
                since as i64,
                MAX_RECHECK as i64
            ],
            |r| r.get::<_, i64>(0),
        )
        .db()?
        .map(|r| r.map(|v| v as u64))
        .collect::<rusqlite::Result<_>>()
        .db()?;
    for chunk in ids.chunks(MAX_IN) {
        if api.client.stopped() {
            report.interrupted = true;
            return Ok(false);
        }
        if !spend(budget) {
            report.limited = true;
            return Ok(false);
        }
        let rows: Vec<RawKey> = api
            .client
            .get_json(&format!(
                "/contracts/{}/bigmaps/{}/keys?{}&limit={}",
                book.marketplace,
                book.path,
                super::tzkt::json_in("key", chunk),
                chunk.len()
            ))?
            .ok_or_else(|| ApiError::Permanent(format!("big map {} is absent", book.name())))?;
        let keys = rows
            .into_iter()
            .map(|r| book.parse_raw(r))
            .collect::<Result<Vec<_>, _>>()?;
        let wanted: BTreeSet<u64> = chunk.iter().copied().collect();
        let mut seen = BTreeSet::new();
        for k in &keys {
            if !wanted.contains(&k.offer_id) || !seen.insert(k.offer_id) {
                return Err(
                    bad(book, format!("recheck returned key {} unasked", k.offer_id)).into(),
                );
            }
        }
        let tx = store
            .conn_mut()
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .db()?;
        apply_keys(&tx, book, &keys, false, report)?;
        // A key TzKT no longer has was created in a dropped block: the offer never existed.
        for id in wanted.difference(&seen) {
            tx.prepare_cached(
                "UPDATE market_offers SET active = 0 WHERE marketplace = ?1 AND kind = ?2 \
                 AND offer_id = ?3",
            )
            .db()?
            .execute(params![book.marketplace, book.kind.as_str(), *id as i64])
            .db()?;
        }
        tx.commit().db()?;
        report.batches += 1;
        report.rechecked += keys.len() as u64;
        report.recheck_missing += (chunk.len() - keys.len()) as u64;
    }
    Ok(true)
}

fn finish(store: &mut Store, book: &OfferBook) -> Result<(), StoreError> {
    let now = now_iso();
    store
        .conn_mut()
        .execute(
            "UPDATE bigmap_state SET synced_level = pass_level, synced_at = ?3, pass_level = NULL, \
             pass_full = NULL, cursor_id = 0, updated_at = ?3 WHERE contract = ?1 AND path = ?2",
            params![book.marketplace, book.path, now],
        )
        .db()?;
    Ok(())
}

fn spend(budget: &mut Option<usize>) -> bool {
    match budget {
        Some(0) => false,
        Some(n) => {
            *n -= 1;
            true
        }
        None => true,
    }
}

/// One pass over `book`; `Ok(true)` when it completed.
fn pass(
    store: &mut Store,
    api: &Tzkt,
    book: &OfferBook,
    page: usize,
    budget: &mut Option<usize>,
    report: &mut OfferReport,
) -> Result<bool, IngestError> {
    let mut state = load(store.conn_mut(), book)?;
    if api.client.stopped() {
        report.interrupted = true;
        return Ok(false);
    }
    if *budget == Some(0) {
        report.limited = true;
        return Ok(false);
    }
    if state.pass_level.is_none() {
        let full = state.synced_level.is_none();
        if !full
            && !recheck(
                store,
                api,
                book,
                state.synced_level.unwrap_or(0),
                budget,
                report,
            )?
        {
            return Ok(false);
        }
        let level = api.head_level()?.saturating_sub(FINALITY);
        let tx = store
            .conn_mut()
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .db()?;
        if full {
            tx.execute(
                "DELETE FROM market_offers WHERE marketplace = ?1 AND kind = ?2",
                [book.marketplace, book.kind.as_str()],
            )
            .db()?;
        }
        tx.execute(
            "INSERT INTO bigmap_state (contract, path, pass_level, pass_full, cursor_id, \
             updated_at) VALUES (?1, ?2, ?3, ?4, 0, ?5) ON CONFLICT (contract, path) DO UPDATE \
             SET pass_level = excluded.pass_level, pass_full = excluded.pass_full, cursor_id = 0, \
             updated_at = excluded.updated_at",
            params![book.marketplace, book.path, level as i64, full, now_iso()],
        )
        .db()?;
        tx.commit().db()?;
        state.pass_level = Some(level);
        state.pass_full = full;
        state.cursor = 0;
    }
    let since = state.synced_level.unwrap_or(0);
    let full = state.pass_full;
    let filter = if full {
        book.full_filter().to_owned()
    } else {
        format!("lastLevel.gt={since}")
    };
    loop {
        if api.client.stopped() {
            report.interrupted = true;
            return Ok(false);
        }
        if !spend(budget) {
            report.limited = true;
            return Ok(false);
        }
        let keys = book.keys_page(api, &filter, state.cursor, page, |k| {
            if full {
                matches!(k.state, OfferState::Live(_) | OfferState::Unsupported)
            } else {
                k.last_level > since
            }
        })?;
        let n = keys.len();
        let last = keys.last().map_or(state.cursor, |k| k.id);
        write_page(store, book, &keys, last, report)?;
        state.cursor = last;
        if n < page {
            break;
        }
    }
    finish(store, book)?;
    Ok(true)
}

#[derive(Deserialize)]
struct RawBlock {
    level: u64,
    timestamp: String,
}

/// `YYYY-MM-DDTHH:MM:SSZ`.
fn is_utc_time(s: &str) -> bool {
    let b = s.as_bytes();
    b.len() == 20
        && b.iter().enumerate().all(|(i, c)| match i {
            4 | 7 => *c == b'-',
            10 => *c == b'T',
            13 | 16 => *c == b':',
            19 => *c == b'Z',
            _ => c.is_ascii_digit(),
        })
}

/// Final levels of live offers and listings that have no block time yet, newest first.
fn pending_levels(c: &Connection, final_level: u64) -> Result<Vec<u64>, StoreError> {
    c.prepare(
        "SELECT level FROM (SELECT created_level AS level FROM market_offers WHERE active = 1 \
         UNION SELECT level FROM escrow_listings) \
         WHERE level <= ?1 AND level NOT IN (SELECT level FROM block_times) ORDER BY level DESC",
    )
    .db()?
    .query_map([final_level as i64], |r| r.get::<_, i64>(0))
    .db()?
    .map(|r| r.map(|v| v as u64))
    .collect::<rusqlite::Result<Vec<_>>>()
    .db()
}

/// Fills `block_times`; `Ok(true)` when no final level is left without a request this run.
fn resolve_times(
    store: &mut Store,
    api: &Tzkt,
    budget: &mut Option<usize>,
    report: &mut OfferReport,
) -> Result<bool, IngestError> {
    if api.client.stopped() {
        report.interrupted = true;
        return Ok(false);
    }
    let final_level = api.head_level()?.saturating_sub(FINALITY);
    let pending = pending_levels(store.conn_mut(), final_level)?;
    for chunk in pending.chunks(MAX_IN) {
        if api.client.stopped() {
            report.interrupted = true;
            return Ok(false);
        }
        if !spend(budget) {
            report.limited = true;
            return Ok(false);
        }
        let list = chunk
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(",");
        let rows: Vec<RawBlock> = api
            .client
            .get_json(&format!(
                "/blocks?level.in={list}&select=level,timestamp&limit={}",
                chunk.len()
            ))?
            .ok_or_else(|| ApiError::Permanent("/blocks is absent".into()))?;
        let wanted: BTreeSet<u64> = chunk.iter().copied().collect();
        let mut seen = BTreeSet::new();
        for b in &rows {
            if !wanted.contains(&b.level) || !seen.insert(b.level) || !is_utc_time(&b.timestamp) {
                return Err(ApiError::Permanent(format!(
                    "malformed TzKT block {} {:?} for the requested levels",
                    b.level, b.timestamp
                ))
                .into());
            }
        }
        let tx = store
            .conn_mut()
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .db()?;
        for b in &rows {
            tx.prepare_cached("INSERT OR IGNORE INTO block_times (level, time) VALUES (?1, ?2)")
                .db()?
                .execute(params![b.level as i64, b.timestamp])
                .db()?;
        }
        tx.commit().db()?;
        report.batches += 1;
        report.times_written += rows.len() as u64;
        report.times_missing += (chunk.len() - rows.len()) as u64;
    }
    Ok(true)
}

#[derive(Debug, Clone)]
pub struct OfferOptions {
    /// Stop after this many requests for key pages and block chunks this run.
    pub limit: Option<usize>,
    /// Keys per page, at most 10,000.
    pub page: usize,
}

impl Default for OfferOptions {
    fn default() -> Self {
        OfferOptions {
            limit: None,
            page: MAX_PAGE,
        }
    }
}

/// A unit of work that takes turns in a run.
#[derive(Debug, Clone, Copy)]
enum Job {
    Book(&'static OfferBook),
    Times,
}

impl Job {
    fn name(self) -> String {
        match self {
            Job::Book(b) => b.name(),
            Job::Times => "block_times".into(),
        }
    }
}

fn served(c: &Connection, job: Job) -> Result<i64, StoreError> {
    Ok(c.query_row(
        "SELECT served FROM sync_rotation WHERE job = ?1",
        [job.name()],
        |r| r.get(0),
    )
    .optional()
    .db()?
    .unwrap_or(0))
}

fn mark_served(c: &Connection, job: Job) -> Result<(), StoreError> {
    c.execute(
        "INSERT INTO sync_rotation (job, served) VALUES (?1, \
         (SELECT coalesce(max(served), 0) + 1 FROM sync_rotation)) \
         ON CONFLICT (job) DO UPDATE SET served = excluded.served",
        [job.name()],
    )
    .db()?;
    Ok(())
}

/// How a job ended within a run.
enum Turn {
    Done,
    /// Limit, interruption or transient failure: the run stops here.
    Stop,
}

fn turn(
    store: &mut Store,
    api: &Tzkt,
    job: Job,
    page: usize,
    budget: &mut Option<usize>,
    report: &mut OfferReport,
    log: &(dyn Fn(&str) + Sync),
) -> Result<Turn, IngestError> {
    let outcome = match job {
        Job::Book(book) => pass(store, api, book, page, budget, report).inspect(|done| {
            if *done {
                log(&format!("{}: offer pass complete", book.name()));
                report.completed.push(book.name());
            }
        }),
        Job::Times => {
            report.times_complete = false;
            resolve_times(store, api, budget, report).inspect(|done| {
                report.times_complete = *done;
            })
        }
    };
    match outcome {
        Ok(true) => Ok(Turn::Done),
        Ok(false) => Ok(Turn::Stop),
        Err(IngestError::Api(ApiError::Interrupted)) => {
            report.interrupted = true;
            Ok(Turn::Stop)
        }
        Err(err) if err.is_transient() => {
            log(&format!("{}: paused: {err}", job.name()));
            report.transient_failures += 1;
            Ok(Turn::Stop)
        }
        Err(err) => Err(err),
    }
}

/// Indexes fxhash v2 buy offers and block times (`seeded-index offers`). Reads only the two
/// offer big maps, the head level and block timestamps.
pub fn sync_offers(
    store: &mut Store,
    api: &Tzkt,
    opts: &OfferOptions,
    log: &(dyn Fn(&str) + Sync),
) -> Result<OfferReport, IngestError> {
    if !(1..=MAX_PAGE).contains(&opts.page) {
        return Err(IngestError::Usage("offers page must be 1..=10000".into()));
    }
    let mut report = OfferReport::default();
    let mut budget = opts.limit;
    let mut jobs: Vec<(i64, usize, Job)> = Vec::new();
    for (i, job) in BOOKS.iter().map(Job::Book).chain([Job::Times]).enumerate() {
        jobs.push((served(store.conn_mut(), job)?, i, job));
    }
    jobs.sort_by_key(|(served, i, _)| (*served, *i));
    let mut failure: Option<IngestError> = None;
    let mut stopped = false;
    // Whether a book ran after the last block-times phase (its new levels still need times).
    let mut times_behind = false;
    for (_, _, job) in &jobs {
        if api.client.stopped() {
            report.interrupted = true;
            stopped = true;
            break;
        }
        if budget == Some(0) {
            report.limited = true;
            stopped = true;
            break;
        }
        mark_served(store.conn_mut(), *job)?;
        match turn(store, api, *job, opts.page, &mut budget, &mut report, log) {
            Ok(Turn::Done) => times_behind = matches!(job, Job::Book(_)),
            Ok(Turn::Stop) => {
                stopped = true;
                break;
            }
            Err(err) => {
                log(&format!("{}: failed: {err}", job.name()));
                failure.get_or_insert(err);
            }
        }
    }
    if !stopped && times_behind && budget != Some(0) {
        turn(
            store,
            api,
            Job::Times,
            opts.page,
            &mut budget,
            &mut report,
            log,
        )?;
    }
    for b in &BOOKS {
        let s = load(store.conn_mut(), b)?;
        if s.synced_level.is_none() || s.pass_level.is_some() {
            report.pending.push(b.name());
        }
    }
    match failure {
        Some(err) => Err(err),
        None => Ok(report),
    }
}

/// Market totals. "Visible" counts only offers and listings the store's reads return.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OfferStats {
    pub token_offers: u64,
    pub token_locked_mutez: u64,
    pub collection_offers: u64,
    pub collection_locked_mutez: u64,
    pub visible_offers: u64,
    pub visible_locked_mutez: u64,
    /// Distinct buyers of live offers.
    pub buyers: u64,
    pub listings: u64,
    pub listings_priced: u64,
    pub visible_listings: u64,
    /// Distinct sellers of live listings.
    pub sellers: u64,
    /// Distinct addresses with a live listing or offer.
    pub addresses: u64,
    /// Distinct addresses with a visible live listing or offer.
    pub visible_addresses: u64,
    /// Levels of live offers and listings still without a block time.
    pub times_pending: u64,
}

const VISIBLE_OFFERS: &str = "FROM market_offers o \
     LEFT JOIN tokens t ON o.kind = 'token' AND t.contract = o.token_contract \
       AND t.token_id = o.token_id \
     JOIN projects p ON p.id = coalesce(t.project_id, o.project_id) AND p.hidden = 0 \
     WHERE o.active = 1";
const VISIBLE_LISTINGS: &str = "FROM escrow_listings l \
     JOIN tokens t ON t.contract = l.contract AND t.token_id = l.token_id \
     JOIN projects p ON p.id = t.project_id AND p.hidden = 0";

pub fn offer_stats(store: &Store) -> Result<OfferStats, StoreError> {
    let c = store.conn();
    let one = |sql: &str| -> Result<u64, StoreError> {
        let v: i64 = c.query_row(sql, [], |r| r.get(0)).db()?;
        u64::try_from(v).map_err(|_| StoreError::Internal(format!("negative total: {sql}")))
    };
    let kind = |k: &str| {
        Ok::<_, StoreError>((
            one(&format!(
                "SELECT count(*) FROM market_offers WHERE active = 1 AND kind = '{k}'"
            ))?,
            one(&format!(
                "SELECT coalesce(sum(price * remaining), 0) FROM market_offers \
                 WHERE active = 1 AND kind = '{k}'"
            ))?,
        ))
    };
    let (token_offers, token_locked_mutez) = kind("token")?;
    let (collection_offers, collection_locked_mutez) = kind("collection")?;
    Ok(OfferStats {
        token_offers,
        token_locked_mutez,
        collection_offers,
        collection_locked_mutez,
        visible_offers: one(&format!("SELECT count(*) {VISIBLE_OFFERS}"))?,
        visible_locked_mutez: one(&format!(
            "SELECT coalesce(sum(o.price * o.remaining), 0) {VISIBLE_OFFERS}"
        ))?,
        buyers: one("SELECT count(DISTINCT buyer) FROM market_offers WHERE active = 1")?,
        listings: one("SELECT count(*) FROM escrow_listings")?,
        listings_priced: one("SELECT count(*) FROM escrow_listings WHERE price IS NOT NULL")?,
        visible_listings: one(&format!("SELECT count(*) {VISIBLE_LISTINGS}"))?,
        sellers: one("SELECT count(DISTINCT seller) FROM escrow_listings")?,
        addresses: one(
            "SELECT count(*) FROM (SELECT seller FROM escrow_listings UNION \
             SELECT buyer FROM market_offers WHERE active = 1)",
        )?,
        visible_addresses: one(&format!(
            "SELECT count(*) FROM (SELECT l.seller {VISIBLE_LISTINGS} UNION \
             SELECT o.buyer {VISIBLE_OFFERS})"
        ))?,
        times_pending: one(
            "SELECT count(*) FROM (SELECT created_level AS level FROM market_offers \
             WHERE active = 1 UNION SELECT level FROM escrow_listings) \
             WHERE level NOT IN (SELECT level FROM block_times)",
        )?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const B: &str = "tz1P719ymGCtNwavZKh2LmhMs53dGsEMzjnm";

    fn token_value(id: &str, version: &str, price: &str) -> Value {
        json!({"buyer": B, "gentk": {"id": id, "version": version}, "price": price})
    }

    #[test]
    fn token_offers_parse_by_gentk_version() {
        let (id, s) = parse_offer(
            OfferKind::Token,
            &json!("79120"),
            &token_value("188628", "2", "333300"),
            true,
        )
        .unwrap();
        assert_eq!(id, 79_120);
        assert_eq!(
            s,
            OfferState::Live(LiveOffer {
                buyer: B.into(),
                price: 333_300,
                remaining: 1,
                target: Target::Token {
                    contract: FX_PARAMS,
                    token_id: 188_628
                },
            })
        );
        for (v, c) in [("0", GENTK_V1), ("1", GENTK_V2)] {
            let (_, s) = parse_offer(
                OfferKind::Token,
                &json!("1"),
                &token_value("5", v, "1"),
                true,
            )
            .unwrap();
            assert!(
                matches!(s, OfferState::Live(LiveOffer { target: Target::Token { contract, .. }, .. }) if contract == c)
            );
        }
        let (_, s) = parse_offer(
            OfferKind::Token,
            &json!("1"),
            &token_value("5", "3", "1"),
            true,
        )
        .unwrap();
        assert_eq!(s, OfferState::Unsupported);
    }

    #[test]
    fn collection_offers_parse_amount_and_exhaustion() {
        let v = json!({"buyer": B, "price": "500000", "amount": "4", "collection": "6539"});
        let (_, s) = parse_offer(OfferKind::Collection, &json!("5422"), &v, true).unwrap();
        assert_eq!(
            s,
            OfferState::Live(LiveOffer {
                buyer: B.into(),
                price: 500_000,
                remaining: 4,
                target: Target::Collection { project_id: 6539 },
            })
        );
        let v = json!({"buyer": B, "price": "2060000", "amount": "0", "collection": "20930"});
        let (_, s) = parse_offer(OfferKind::Collection, &json!("115625"), &v, true).unwrap();
        assert_eq!(s, OfferState::Ended { exhausted: true });
        // A removed key needs only its id.
        let (id, s) = parse_offer(OfferKind::Collection, &json!("7"), &Value::Null, false).unwrap();
        assert_eq!((id, s), (7, OfferState::Ended { exhausted: false }));
    }

    #[test]
    fn malformed_offers_are_rejected() {
        let good = token_value("1", "1", "1");
        let cases: Vec<(Value, Value)> = vec![
            (json!(5), good.clone()),
            (json!("-5"), good.clone()),
            (json!("9223372036854775808"), good.clone()),
            (json!("1"), token_value("1", "1", "0")),
            (json!("1"), token_value("1", "1", "1.5")),
            (json!("1"), token_value("1", "1", " 1")),
            (json!("1"), token_value("18446744073709551616", "1", "1")),
            (json!("1"), token_value("1", "x", "1")),
            (
                json!("1"),
                json!({"buyer": "tz1Short", "gentk": {"id": "1", "version": "1"}, "price": "1"}),
            ),
            (
                json!("1"),
                json!({"gentk": {"id": "1", "version": "1"}, "price": "1"}),
            ),
        ];
        for (key, value) in cases {
            assert!(
                parse_offer(OfferKind::Token, &key, &value, true).is_err(),
                "{key} {value}"
            );
        }
        // price × amount beyond mutez cannot exist on chain.
        let v =
            json!({"buyer": B, "price": "4611686018427387904", "amount": "2", "collection": "1"});
        assert!(parse_offer(OfferKind::Collection, &json!("1"), &v, true).is_err());
    }

    #[test]
    fn keys_without_an_id_or_with_reversed_levels_are_rejected() {
        let book = &BOOKS[0];
        let raw = |id: Value, first: u64, last: u64| -> RawKey {
            serde_json::from_value(json!({"id": id, "key": "1", "active": true,
                "firstLevel": first, "lastLevel": last, "value": token_value("1", "1", "1")}))
            .unwrap()
        };
        assert!(book.parse_raw(raw(json!(5), 1, 2)).is_ok());
        assert!(
            book.parse_raw(raw(Value::Null, 1, 2)).is_err(),
            "TzKT select quirk"
        );
        assert!(
            book.parse_raw(raw(json!(9_223_372_036_854_775_808_u64), 1, 2))
                .is_err()
        );
        assert!(book.parse_raw(raw(json!(5), 3, 2)).is_err());
    }

    #[test]
    fn block_times_must_be_utc_seconds() {
        assert!(is_utc_time("2022-05-28T23:25:29Z"));
        for bad in [
            "2022-05-28 23:25:29Z",
            "2022-05-28T23:25:29",
            "2022-05-28T23:25:29.1Z",
            "",
        ] {
            assert!(!is_utc_time(bad), "{bad}");
        }
    }
}
