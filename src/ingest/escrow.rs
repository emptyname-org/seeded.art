//! Marketplace escrow: the seller behind a token whose ledger owner is a marketplace contract.
//!
//! Listed tokens sit in the marketplace contract; the seller is named only in its listing big
//! map. Each escrow keeps its own resumable pass (`escrow_state`), like token sync:
//! - full: live listings now (`active=true`, plus "amount > 0" where sold listings linger),
//!   paged by big-map key id, each page committed with its cursor; at the end every token the
//!   escrow holds is resolved;
//! - incremental: keys changed after `synced_level`, removed ones included.
//!
//! A token is resolved only when the escrow holds it and exactly one live listing names it;
//! otherwise it stays shown as escrow (`listed_by = NULL`, no collector). Never guessed.
//!
//! A listing is live only with a decimal id, a token, a valid seller address and a price that
//! fits SQLite; anything else from upstream counts as not live.
//!
//! A pass is recorded at a final level (head − [`FINALITY`]) while it reads current state.
//! Listings stored, and removals applied (kept in `escrow_removed`), from a change above the
//! previous pass's level were not final when read: the next incremental pass first re-reads
//! those keys and applies what TzKT has then, so a change dropped by a reorg is undone.

use std::collections::{BTreeSet, HashSet};

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde_json::Value;

use super::http::ApiError;
use super::sync::SyncReport;
use super::tzkt::{BigMapKey, Tzkt};
use super::tzkt::{MAX_IN, json_in};
use super::{FINALITY, FX_PARAMS, GENTK_V1, GENTK_V2, IngestError, TOKEN_CONTRACTS, now_iso};
use crate::store::{Db, Store, StoreError, is_tezos_address};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    /// `listings`: key = listing id; value {gentk {id, version}, seller, price}.
    FxhashV2,
    /// `offers`: value {objkt_id, issuer, objkt_amount, price, ...}; sold offers keep amount 0.
    FxhashV1,
    /// `asks`: value {fa2, objkt_id, issuer, amount, xtz_per_objkt, ...}, any FA2; sold asks
    /// keep amount 0.
    ObjktV1,
}

/// A marketplace contract that holds listed tokens.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Escrow {
    pub address: &'static str,
    pub name: &'static str,
    path: &'static str,
    kind: Kind,
}

/// Identified 2026-10-03 from TzKT aliases and contract metadata. Ledger holdings of fxhash
/// tokens: v2 205,973, v1 48,737, objkt (old) 1,205. The next largest contract holder
/// (1,009, "sudojamesvault") is a personal vault, not a marketplace.
pub const ESCROWS: [Escrow; 3] = [
    Escrow {
        address: "KT1GbyoDi7H1sfXmimXpptZJuCdHMh66WS9u",
        name: "fxhash marketplace v2",
        path: "listings",
        kind: Kind::FxhashV2,
    },
    Escrow {
        address: "KT1Xo5B7PNBAeynZPmca4bRh6LQow4og1Zb9",
        name: "fxhash marketplace v1",
        path: "offers",
        kind: Kind::FxhashV1,
    },
    Escrow {
        address: "KT1FvqJwEDWb1Gwc55Jd1jjTHRVWbYKUUpyq",
        name: "objkt.com marketplace (old)",
        path: "asks",
        kind: Kind::ObjktV1,
    },
];

/// fxhash marketplace v2 `gentk_contracts` big map (verified 2026-10-03): version -> FA2.
const V2_VERSIONS: [&str; 3] = [GENTK_V1, GENTK_V2, FX_PARAMS];

/// One listing key, interpreted.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Listing {
    token: Option<(String, u64)>,
    seller: Option<String>,
    /// Mutez; per token for objkt asks.
    price: Option<u64>,
    live: bool,
}

fn text(v: &Value, path: &[&str]) -> Option<String> {
    let mut v = v;
    for k in path {
        v = v.get(k)?;
    }
    v.as_str().map(str::to_owned)
}

/// A decimal nat: ASCII digits only (no sign or spaces).
fn decimal(s: &str) -> Option<u64> {
    (!s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()))
        .then(|| s.parse().ok())
        .flatten()
}

fn number(v: &Value, path: &[&str]) -> Option<u64> {
    text(v, path).as_deref().and_then(decimal)
}

impl Escrow {
    pub fn by_address(address: &str) -> Option<&'static Escrow> {
        ESCROWS.iter().find(|e| e.address == address)
    }

    fn fa2_filter() -> String {
        json_in("value.fa2", &TOKEN_CONTRACTS)
    }

    /// Live listings only, filtered server-side.
    fn full_filter(&self) -> String {
        match self.kind {
            Kind::FxhashV2 => "active=true".into(),
            Kind::FxhashV1 => "active=true&value.objkt_amount.gt=0".into(),
            Kind::ObjktV1 => format!("active=true&value.amount.gt=0&{}", Self::fa2_filter()),
        }
    }

    /// Every key changed after `level`, removed ones included.
    fn change_filter(&self, level: u64) -> String {
        match self.kind {
            Kind::ObjktV1 => format!("lastLevel.gt={level}&{}", Self::fa2_filter()),
            _ => format!("lastLevel.gt={level}"),
        }
    }

    fn parse(&self, k: &BigMapKey) -> Listing {
        let v = &k.value;
        let price_field = match self.kind {
            Kind::ObjktV1 => "xtz_per_objkt",
            _ => "price",
        };
        let price = number(v, &[price_field]).filter(|p| i64::try_from(*p).is_ok());
        let key_ok = decimal(&k.key).is_some_and(|id| i64::try_from(id).is_ok());
        let (token, seller, live) = match self.kind {
            Kind::FxhashV2 => {
                let contract = number(v, &["gentk", "version"])
                    .and_then(|i| V2_VERSIONS.get(usize::try_from(i).ok()?))
                    .map(|c| (*c).to_owned());
                let id = number(v, &["gentk", "id"]);
                (contract.zip(id), text(v, &["seller"]), k.active)
            }
            Kind::FxhashV1 => (
                number(v, &["objkt_id"]).map(|id| (GENTK_V1.to_owned(), id)),
                text(v, &["issuer"]),
                k.active && number(v, &["objkt_amount"]).is_some_and(|n| n > 0),
            ),
            Kind::ObjktV1 => (
                text(v, &["fa2"])
                    .filter(|f| TOKEN_CONTRACTS.contains(&f.as_str()))
                    .zip(number(v, &["objkt_id"])),
                text(v, &["issuer"]),
                k.active && number(v, &["amount"]).is_some_and(|n| n > 0),
            ),
        };
        let seller = seller.filter(|s| is_tezos_address(s));
        Listing {
            live: live && key_ok && token.is_some() && seller.is_some() && price.is_some(),
            token,
            seller,
            price,
        }
    }
}

/// The Tezos burn address (verified 2026-10-03: holds 14,582 fxhash tokens; GENTK v1's
/// `issuer` was set to it when minting moved to v2).
pub const BURN_ADDRESS: &str = "tz1burnburnburnburnburnburnburjAYjjX";

/// Registers escrow contracts and the burn address so owner writes can recognise them.
pub(crate) fn register(c: &Connection) -> Result<(), StoreError> {
    let mut st = c
        .prepare_cached(
            "INSERT INTO known_addresses (address, name, kind) VALUES (?1, ?2, ?3) \
             ON CONFLICT (address) DO UPDATE SET name = excluded.name, kind = excluded.kind",
        )
        .db()?;
    for e in &ESCROWS {
        st.execute(params![e.address, e.name, "escrow"]).db()?;
    }
    st.execute(params![BURN_ADDRESS, "burn address", "burn"])
        .db()?;
    Ok(())
}

/// Addresses that never count as collectors.
#[derive(Debug, Clone, Default)]
pub(crate) struct Known {
    escrows: HashSet<String>,
    burns: HashSet<String>,
}

pub(crate) fn known_addresses(c: &Connection) -> Result<Known, StoreError> {
    let mut st = c
        .prepare_cached("SELECT address, kind FROM known_addresses")
        .db()?;
    let rows: Vec<(String, String)> = st
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .db()?
        .collect::<rusqlite::Result<_>>()
        .db()?;
    let mut known = Known::default();
    for (address, kind) in rows {
        if kind == "burn" {
            known.burns.insert(address);
        } else {
            known.escrows.insert(address);
        }
    }
    Ok(known)
}

/// Re-derives `listed_by` and `holder_address` of every token held by a known address (one
/// scan each): escrowed tokens go to their single live seller or nobody, burned ones to
/// nobody.
pub(crate) fn resolve_known(c: &Connection) -> Result<(), StoreError> {
    c.execute(
        "UPDATE tokens SET listed_by = (SELECT CASE WHEN k.kind = 'escrow' AND count(l.seller) = 1 \
         THEN min(l.seller) END FROM known_addresses k LEFT JOIN escrow_listings l \
         INDEXED BY escrow_listings_token ON l.contract = tokens.contract \
         AND l.token_id = tokens.token_id AND l.escrow = k.address \
         WHERE k.address = tokens.owner_address) \
         WHERE owner_address IN (SELECT address FROM known_addresses)",
        [],
    )
    .db()?;
    c.execute(
        "UPDATE tokens SET holder_address = listed_by \
         WHERE owner_address IN (SELECT address FROM known_addresses) \
         AND holder_address IS NOT listed_by",
        [],
    )
    .db()?;
    Ok(())
}

/// The seller when exactly one live listing in `escrow` names the token.
pub(crate) fn listed_by(
    c: &Connection,
    escrow: &str,
    contract: &str,
    token_id: u64,
) -> Result<Option<String>, StoreError> {
    let sellers: Vec<String> = c
        .prepare_cached(
            "SELECT seller FROM escrow_listings INDEXED BY escrow_listings_token \
             WHERE contract = ?1 AND token_id = ?2 AND escrow = ?3 LIMIT 2",
        )
        .db()?
        .query_map(params![contract, token_id as i64, escrow], |r| r.get(0))
        .db()?
        .collect::<rusqlite::Result<_>>()
        .db()?;
    Ok(match sellers.as_slice() {
        [one] => Some(one.clone()),
        _ => None,
    })
}

/// `(listed_by, holder)` for a ledger owner.
pub(crate) fn holder_of(
    c: &Connection,
    known: &Known,
    owner: &str,
    contract: &str,
    token_id: u64,
) -> Result<(Option<String>, Option<String>), StoreError> {
    if known.burns.contains(owner) {
        Ok((None, None))
    } else if known.escrows.contains(owner) {
        let seller = listed_by(c, owner, contract, token_id)?;
        Ok((seller.clone(), seller))
    } else {
        Ok((None, Some(owner.to_owned())))
    }
}

/// Recomputes `listed_by` and `holder_address` of one token from its owner and listings.
fn resolve(
    c: &Connection,
    escrows: &Known,
    contract: &str,
    token_id: u64,
) -> Result<(), StoreError> {
    let owner: Option<Option<String>> = c
        .prepare_cached("SELECT owner_address FROM tokens WHERE contract = ?1 AND token_id = ?2")
        .db()?
        .query_row(params![contract, token_id as i64], |r| r.get(0))
        .optional()
        .db()?;
    let Some(Some(owner)) = owner else {
        return Ok(());
    };
    let (listed, holder) = holder_of(c, escrows, &owner, contract, token_id)?;
    c.prepare_cached(
        "UPDATE tokens SET listed_by = ?3, holder_address = ?4 WHERE contract = ?1 \
         AND token_id = ?2 AND (listed_by IS NOT ?3 OR holder_address IS NOT ?4)",
    )
    .db()?
    .execute(params![contract, token_id as i64, listed, holder])
    .db()?;
    Ok(())
}

#[derive(Debug, Clone, Default)]
struct State {
    synced_level: Option<u64>,
    pass_level: Option<u64>,
    pass_full: bool,
    cursor: u64,
}

fn load(c: &Connection, escrow: &str) -> Result<State, StoreError> {
    Ok(c.query_row(
        "SELECT synced_level, pass_level, pass_full, cursor_id FROM escrow_state \
             WHERE escrow = ?1",
        [escrow],
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

/// Applies one listing key: upserts a live listing, deletes anything else and keeps a
/// tombstone of the removal (pruned once final). Collects the tokens to re-resolve.
fn apply_key(
    tx: &Connection,
    escrow: &Escrow,
    k: &BigMapKey,
    affected: &mut BTreeSet<(String, u64)>,
) -> Result<bool, StoreError> {
    let listing = escrow.parse(k);
    let old: Option<(String, i64)> = tx
        .prepare_cached(
            "SELECT contract, token_id FROM escrow_listings WHERE escrow = ?1 AND listing = ?2",
        )
        .db()?
        .query_row(params![escrow.address, k.key], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })
        .optional()
        .db()?;
    if let Some((contract, id)) = old {
        affected.insert((contract, id as u64));
    }
    let live = match (&listing.token, &listing.seller) {
        (Some((contract, id)), Some(seller)) if listing.live => {
            tx.prepare_cached(
                "INSERT INTO escrow_listings (escrow, listing, contract, token_id, seller, \
                 level, price) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) ON CONFLICT (escrow, \
                 listing) DO UPDATE SET contract = excluded.contract, \
                 token_id = excluded.token_id, seller = excluded.seller, \
                 level = excluded.level, price = excluded.price",
            )
            .db()?
            .execute(params![
                escrow.address,
                k.key,
                contract,
                *id as i64,
                seller,
                k.last_level as i64,
                listing.price.map(|p| p as i64)
            ])
            .db()?;
            tx.prepare_cached("DELETE FROM escrow_removed WHERE escrow = ?1 AND listing = ?2")
                .db()?
                .execute(params![escrow.address, k.key])
                .db()?;
            true
        }
        _ => {
            tx.prepare_cached("DELETE FROM escrow_listings WHERE escrow = ?1 AND listing = ?2")
                .db()?
                .execute(params![escrow.address, k.key])
                .db()?;
            tx.prepare_cached(
                "INSERT INTO escrow_removed (escrow, listing, level) VALUES (?1, ?2, ?3) \
                 ON CONFLICT (escrow, listing) DO UPDATE SET level = excluded.level",
            )
            .db()?
            .execute(params![escrow.address, k.key, k.last_level as i64])
            .db()?;
            false
        }
    };
    if let Some(t) = listing.token {
        affected.insert(t);
    }
    Ok(live)
}

/// Applies one page of listing keys and advances the cursor, in one transaction.
fn write_page(
    store: &mut Store,
    escrow: &Escrow,
    keys: &[BigMapKey],
    cursor: u64,
    report: &mut SyncReport,
) -> Result<(), StoreError> {
    let tx = store
        .conn_mut()
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .db()?;
    let escrows = known_addresses(&tx)?;
    let mut affected: BTreeSet<(String, u64)> = BTreeSet::new();
    let (mut live, mut gone) = (0, 0);
    for k in keys {
        if apply_key(&tx, escrow, k, &mut affected)? {
            live += 1;
        } else {
            gone += 1;
        }
    }
    for (contract, id) in &affected {
        resolve(&tx, &escrows, contract, *id)?;
    }
    tx.execute(
        "UPDATE escrow_state SET cursor_id = ?2, updated_at = ?3 WHERE escrow = ?1",
        params![escrow.address, cursor as i64, now_iso()],
    )
    .db()?;
    tx.commit().db()?;
    report.batches += 1;
    report.listings_live += live;
    report.listings_gone += gone;
    Ok(())
}

/// Most listings one recheck re-reads (100 per request); more wait for the next pass.
const MAX_RECHECK: usize = 10_000;

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

/// Re-reads the keys of listings stored or removed from a change above `since` (not final
/// when read) and applies their current state; a listing whose key TzKT no longer has never
/// existed on the final chain and is deleted. `Ok(false)` when stopped or out of budget.
fn recheck(
    store: &mut Store,
    api: &Tzkt,
    escrow: &Escrow,
    since: u64,
    budget: &mut Option<usize>,
    report: &mut SyncReport,
) -> Result<bool, IngestError> {
    let keys: Vec<String> = store
        .conn_mut()
        .prepare(
            "SELECT listing FROM escrow_listings WHERE escrow = ?1 AND level > ?2 UNION \
             SELECT listing FROM escrow_removed WHERE escrow = ?1 AND level > ?2 \
             ORDER BY 1 LIMIT ?3",
        )
        .db()?
        .query_map(
            params![escrow.address, since as i64, MAX_RECHECK as i64],
            |r| r.get(0),
        )
        .db()?
        .collect::<rusqlite::Result<_>>()
        .db()?;
    for chunk in keys.chunks(MAX_IN) {
        if api.client.stopped() {
            report.interrupted = true;
            return Ok(false);
        }
        if !spend(budget) {
            report.limited = true;
            return Ok(false);
        }
        let found = api.bigmap_keys(
            escrow.address,
            escrow.path,
            &json_in("key", chunk),
            0,
            chunk.len(),
            |_| true,
        )?;
        let wanted: BTreeSet<&str> = chunk.iter().map(String::as_str).collect();
        let mut seen: BTreeSet<&str> = BTreeSet::new();
        for k in &found {
            if !wanted.contains(k.key.as_str()) || !seen.insert(k.key.as_str()) {
                return Err(ApiError::Permanent(format!(
                    "{} {}: recheck returned key {} unasked",
                    escrow.address, escrow.path, k.key
                ))
                .into());
            }
        }
        let tx = store
            .conn_mut()
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .db()?;
        let escrows = known_addresses(&tx)?;
        let mut affected: BTreeSet<(String, u64)> = BTreeSet::new();
        for k in &found {
            if apply_key(&tx, escrow, k, &mut affected)? {
                report.listings_live += 1;
            } else {
                report.listings_gone += 1;
            }
        }
        for gone in wanted.difference(&seen) {
            let old: Option<(String, i64)> = tx
                .prepare_cached(
                    "DELETE FROM escrow_listings WHERE escrow = ?1 AND listing = ?2 \
                     RETURNING contract, token_id",
                )
                .db()?
                .query_row(params![escrow.address, gone], |r| {
                    Ok((r.get(0)?, r.get(1)?))
                })
                .optional()
                .db()?;
            if let Some((contract, id)) = old {
                affected.insert((contract, id as u64));
                report.listings_gone += 1;
            }
            tx.prepare_cached("DELETE FROM escrow_removed WHERE escrow = ?1 AND listing = ?2")
                .db()?
                .execute(params![escrow.address, gone])
                .db()?;
        }
        for (contract, id) in &affected {
            resolve(&tx, &escrows, contract, *id)?;
        }
        tx.commit().db()?;
        report.batches += 1;
    }
    Ok(true)
}

/// Ends a pass; a full pass first resolves every token the escrow holds.
fn finish(store: &mut Store, escrow: &Escrow, state: &State) -> Result<(), StoreError> {
    let tx = store
        .conn_mut()
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .db()?;
    if state.pass_full {
        tx.execute(
            "UPDATE tokens SET listed_by = (SELECT CASE WHEN count(*) = 1 THEN min(l.seller) END \
             FROM escrow_listings l INDEXED BY escrow_listings_token \
             WHERE l.contract = tokens.contract \
             AND l.token_id = tokens.token_id AND l.escrow = ?1) WHERE owner_address = ?1",
            [escrow.address],
        )
        .db()?;
        tx.execute(
            "UPDATE tokens SET holder_address = listed_by WHERE owner_address = ?1 \
             AND holder_address IS NOT listed_by",
            [escrow.address],
        )
        .db()?;
    }
    // Removals at or below the pass level are final: no reorg can undo them.
    tx.execute(
        "DELETE FROM escrow_removed WHERE escrow = ?1 \
         AND level <= (SELECT pass_level FROM escrow_state WHERE escrow = ?1)",
        [escrow.address],
    )
    .db()?;
    let now = now_iso();
    tx.execute(
        "UPDATE escrow_state SET synced_level = pass_level, synced_at = ?2, pass_level = NULL, \
         pass_full = NULL, cursor_id = 0, updated_at = ?2 WHERE escrow = ?1",
        params![escrow.address, now],
    )
    .db()?;
    tx.commit().db()?;
    Ok(())
}

/// One pass over `escrow`; `Ok(true)` when it completed.
pub(crate) fn pass(
    store: &mut Store,
    api: &Tzkt,
    escrow: &Escrow,
    page: usize,
    budget: &mut Option<usize>,
    report: &mut SyncReport,
) -> Result<bool, IngestError> {
    let mut state = load(store.conn_mut(), escrow.address)?;
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
        if !full {
            let since = state.synced_level.unwrap_or(0);
            if !recheck(store, api, escrow, since, budget, report)? {
                return Ok(false);
            }
        }
        let head = api.head_level()?.saturating_sub(FINALITY);
        let tx = store
            .conn_mut()
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .db()?;
        if full {
            tx.execute(
                "DELETE FROM escrow_listings WHERE escrow = ?1",
                [escrow.address],
            )
            .db()?;
            tx.execute(
                "DELETE FROM escrow_removed WHERE escrow = ?1",
                [escrow.address],
            )
            .db()?;
        }
        tx.execute(
            "INSERT INTO escrow_state (escrow, pass_level, pass_full, cursor_id, updated_at) \
             VALUES (?1, ?2, ?3, 0, ?4) ON CONFLICT (escrow) DO UPDATE SET \
             pass_level = excluded.pass_level, pass_full = excluded.pass_full, cursor_id = 0, \
             updated_at = excluded.updated_at",
            params![escrow.address, head as i64, full, now_iso()],
        )
        .db()?;
        tx.commit().db()?;
        state.pass_level = Some(head);
        state.pass_full = full;
        state.cursor = 0;
    }
    let filter = if state.pass_full {
        escrow.full_filter()
    } else {
        escrow.change_filter(state.synced_level.unwrap_or(0))
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
        let since = state.synced_level.unwrap_or(0);
        let full = state.pass_full;
        let keys = api.bigmap_keys(
            escrow.address,
            escrow.path,
            &filter,
            state.cursor,
            page,
            |k| {
                if full { k.active } else { k.last_level > since }
            },
        )?;
        let n = keys.len();
        let last = keys.iter().map(|k| k.id).max().unwrap_or(state.cursor);
        if n == page && last <= state.cursor {
            return Err(ApiError::Permanent(format!(
                "{} {}: paging does not advance",
                escrow.address, escrow.path
            ))
            .into());
        }
        write_page(store, escrow, &keys, last, report)?;
        state.cursor = last;
        if n < page {
            break;
        }
    }
    finish(store, escrow, &state)?;
    Ok(true)
}

#[derive(Debug, Clone)]
pub struct EscrowOptions {
    /// Stop after this many listing pages this run.
    pub limit: Option<usize>,
    /// Listing keys per page, at most 10,000.
    pub page: usize,
}

impl Default for EscrowOptions {
    fn default() -> Self {
        EscrowOptions {
            limit: None,
            page: super::tzkt::MAX_PAGE,
        }
    }
}

/// Resolves escrow sellers from every marketplace's listing big map (`seeded-index escrow`).
/// Reads nothing but those big maps and the head level.
pub fn sync_escrows(
    store: &mut Store,
    api: &Tzkt,
    opts: &EscrowOptions,
    log: &(dyn Fn(&str) + Sync),
) -> Result<SyncReport, IngestError> {
    if !(1..=super::tzkt::MAX_PAGE).contains(&opts.page) {
        return Err(IngestError::Usage("escrow page must be 1..=10000".into()));
    }
    register(store.conn_mut())?;
    let mut report = SyncReport::default();
    let mut budget = opts.limit;
    // Passes in progress first, then the stalest escrow: a run limit never starves the rest.
    let mut order: Vec<(bool, u64, String, &Escrow)> = Vec::new();
    for e in &ESCROWS {
        let state = load(store.conn_mut(), e.address)?;
        let synced_at: Option<String> = store
            .conn_mut()
            .query_row(
                "SELECT synced_at FROM escrow_state WHERE escrow = ?1",
                [e.address],
                |r| r.get(0),
            )
            .optional()
            .db()?
            .flatten();
        order.push((
            state.pass_level.is_none(),
            state.synced_level.unwrap_or(0),
            synced_at.unwrap_or_default(),
            e,
        ));
    }
    order.sort_by(|a, b| (a.0, a.1, &a.2).cmp(&(b.0, b.1, &b.2)));
    for (_, _, _, e) in order {
        match pass(store, api, e, opts.page, &mut budget, &mut report) {
            Ok(true) => {
                log(&format!("{} ({}): escrow pass complete", e.address, e.name));
                report.completed.push(e.address.to_owned());
            }
            Ok(false) => break,
            Err(IngestError::Api(ApiError::Interrupted)) => {
                report.interrupted = true;
                break;
            }
            Err(err) if err.is_transient() => {
                log(&format!("{}: escrow pass paused: {err}", e.address));
                report.transient_failures += 1;
                break;
            }
            Err(err) => return Err(err),
        }
    }
    for e in &ESCROWS {
        let state = load(store.conn_mut(), e.address)?;
        if state.synced_level.is_none() || state.pass_level.is_some() {
            report.pending.push(e.address.to_owned());
        }
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn key(active: bool, value: Value) -> BigMapKey {
        BigMapKey {
            id: 1,
            key: "1".into(),
            value,
            active,
            last_level: 1,
        }
    }

    const S: &str = "tz1WMa1cAKk4JjFGZCpXNkTDi5o5AqCsFF6E";

    #[test]
    fn listings_parse_per_marketplace() {
        let v2 = &ESCROWS[0];
        let l = v2.parse(&key(
            true,
            json!({"gentk": {"id": "188628", "version": "2"}, "seller": S, "price": "18000000"}),
        ));
        assert_eq!(l.token, Some((FX_PARAMS.to_owned(), 188_628)));
        assert_eq!(l.seller.as_deref(), Some(S));
        assert_eq!(l.price, Some(18_000_000));
        assert!(l.live);
        assert!(
            !v2.parse(&key(
                false,
                json!({"gentk": {"id": "1", "version": "1"}, "seller": S, "price": "1"})
            ))
            .live
        );
        // An unknown gentk version maps to no token, so nothing is resolved.
        let unknown = v2.parse(&key(
            true,
            json!({"gentk": {"id": "1", "version": "9"}, "seller": S, "price": "1"}),
        ));
        assert_eq!((unknown.token, unknown.live), (None, false));

        let v1 = &ESCROWS[1];
        let sold = v1.parse(&key(
            true,
            json!({"objkt_id": "5810", "issuer": S, "objkt_amount": "0", "price": "1"}),
        ));
        assert_eq!(sold.token, Some((GENTK_V1.to_owned(), 5810)));
        assert!(!sold.live, "a sold v1 offer stays active with amount 0");
        let open = v1.parse(&key(
            true,
            json!({"objkt_id": "5810", "issuer": S, "objkt_amount": "1", "price": "15000000"}),
        ));
        assert!(open.live);
        assert_eq!(open.price, Some(15_000_000));

        let objkt = &ESCROWS[2];
        let foreign = objkt.parse(&key(
            true,
            json!({"fa2": "KT1JzwTDmePUBCfpoGnC8FZbA1CB2KqnKnDT", "objkt_id": "0", "issuer": S,
                   "amount": "1", "xtz_per_objkt": "1"}),
        ));
        assert_eq!((foreign.token, foreign.live), (None, false));
        let ours = objkt.parse(&key(
            true,
            json!({"fa2": GENTK_V1, "objkt_id": "41136", "issuer": S, "amount": "1",
                   "xtz_per_objkt": "2500000"}),
        ));
        assert_eq!(ours.token, Some((GENTK_V1.to_owned(), 41_136)));
        assert_eq!(ours.price, Some(2_500_000));
        assert!(ours.live);
    }

    #[test]
    fn malformed_listings_are_not_live() {
        let v2 = &ESCROWS[0];
        let good = json!({"gentk": {"id": "1", "version": "1"}, "seller": S, "price": "1"});
        assert!(v2.parse(&key(true, good.clone())).live);
        let mut bad_seller = good.clone();
        bad_seller["seller"] = json!("tz1S");
        assert!(!v2.parse(&key(true, bad_seller)).live);
        for price in [
            json!("-1"),
            json!("+5"),
            json!("9223372036854775808"),
            json!(5),
            json!(null),
        ] {
            let mut v = good.clone();
            v["price"] = price.clone();
            assert!(!v2.parse(&key(true, v)).live, "price {price}");
        }
        let mut k = key(true, good);
        for bad in ["x", "-1", "+5", " 5", "9223372036854775808", ""] {
            k.key = bad.into();
            assert!(!v2.parse(&k).live, "key {bad:?}");
        }
    }
}
