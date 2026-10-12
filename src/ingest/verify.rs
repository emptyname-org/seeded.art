//! Catalogue records against chain.
//!
//! - Local: every unverified catalogue token with a chain-sourced `tokens` row is compared
//!   with it (project, seed, artifact URI and the artifact's fxhash/fxiteration/fxminter).
//! - Absence: catalogue tokens still unseen after a completed full pass are looked up one
//!   by one; only a definite answer (HTTP 200 without the token, or 404) marks them absent.
//!   Transient errors change nothing.
//! - Live sample: N catalogue tokens re-read from TzKT now, compared with the catalogue and
//!   with the stored row (stale rows are counted, not rewritten: `sync` owns `tokens`).

use std::collections::{BTreeMap, HashMap};

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};

use super::http::ApiError;
use super::tzkt::{ChainToken, Mint, Tzkt};
use super::{IngestError, now_iso, query_param};
use crate::store::{Db, Store, StoreError};

/// Chain facts a catalogue record is checked against.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ChainFacts<'a> {
    pub project_id: u64,
    pub iteration: u32,
    pub minter: &'a str,
    pub seed: Option<&'a str>,
    pub artifact: Option<&'a str>,
}

/// `(verify_state, detail)` for a catalogue record.
pub(crate) fn compare(
    cat_project: u64,
    cat_seed: Option<&str>,
    cat_artifact: Option<&str>,
    chain: &ChainFacts<'_>,
) -> (&'static str, Option<String>) {
    let mut diffs: Vec<&str> = Vec::new();
    if cat_project != chain.project_id {
        diffs.push("project");
    }
    match (cat_seed, chain.seed) {
        (Some(a), Some(b)) if a != b => diffs.push("seed"),
        (Some(_), None) => diffs.push("seed"),
        (None, Some(_)) => diffs.push("catalogue_seed_missing"),
        _ => {}
    }
    match (cat_artifact, chain.artifact) {
        (Some(a), Some(b)) if a != b => diffs.push("artifact"),
        (Some(_), None) => diffs.push("artifact"),
        (None, Some(_)) => diffs.push("catalogue_artifact_missing"),
        _ => {}
    }
    if let Some(a) = cat_artifact {
        if query_param(a, "fxhash").is_some_and(|h| Some(h) != chain.seed) {
            diffs.push("artifact_seed");
        }
        if query_param(a, "fxiteration").is_some_and(|i| i != chain.iteration.to_string()) {
            diffs.push("artifact_iteration");
        }
        if query_param(a, "fxminter").is_some_and(|m| m != chain.minter) {
            diffs.push("artifact_minter");
        }
    }
    if !diffs.is_empty() {
        ("mismatch", Some(diffs.join(",")))
    } else if cat_seed.is_none() && chain.seed.is_none() {
        ("unsigned", None)
    } else {
        ("match", None)
    }
}

pub(crate) struct CatalogueRow {
    pub project_id: u64,
    pub seed: Option<String>,
    pub artifact: Option<String>,
}

pub(crate) fn catalogue_row(
    c: &Connection,
    contract: &str,
    token_id: u64,
) -> Result<Option<CatalogueRow>, StoreError> {
    c.prepare_cached(
        "SELECT project_id, seed, artifact_uri FROM catalogue_tokens \
         WHERE contract = ?1 AND token_id = ?2",
    )
    .db()?
    .query_row(params![contract, token_id as i64], |r| {
        Ok(CatalogueRow {
            project_id: r.get::<_, i64>(0)? as u64,
            seed: r.get(1)?,
            artifact: r.get(2)?,
        })
    })
    .optional()
    .db()
}

/// Compares and records; returns the new state when a catalogue record exists.
pub(crate) fn verify_one(
    c: &Connection,
    contract: &str,
    token_id: u64,
    chain: &ChainFacts<'_>,
    level: u64,
) -> Result<Option<&'static str>, StoreError> {
    let Some(cat) = catalogue_row(c, contract, token_id)? else {
        return Ok(None);
    };
    let (state, detail) = compare(
        cat.project_id,
        cat.seed.as_deref(),
        cat.artifact.as_deref(),
        chain,
    );
    c.prepare_cached(
        "UPDATE catalogue_tokens SET chain_state = 'present', verify_state = ?3, \
         verify_detail = ?4, verified_level = ?5 WHERE contract = ?1 AND token_id = ?2",
    )
    .db()?
    .execute(params![
        contract,
        token_id as i64,
        state,
        detail,
        level as i64
    ])
    .db()?;
    c.prepare_cached(
        "DELETE FROM ingest_issues WHERE contract = ?1 AND token_id = ?2 \
         AND kind = 'not_catalogued'",
    )
    .db()?
    .execute(params![contract, token_id as i64])
    .db()?;
    Ok(Some(state))
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VerifyReport {
    /// Local comparisons by resulting state.
    pub local: BTreeMap<String, u64>,
    pub absent_checked: u64,
    pub absent_confirmed: u64,
    pub sampled: u64,
    /// Live sample outcomes by state (`match`, `mismatch`, `unsigned`, `absent`).
    pub sample: BTreeMap<String, u64>,
    /// Sampled tokens whose stored row differs from chain now.
    pub stale_rows: u64,
    /// Lookups that failed transiently; nothing was recorded for them.
    pub transient: u64,
    /// Catalogue totals by verify state after the run.
    pub totals: BTreeMap<String, u64>,
    /// Up to 20 example mismatches: contract/token: detail.
    pub examples: Vec<String>,
}

pub struct VerifyOptions {
    pub sample: usize,
    pub absent_limit: usize,
    /// Seed for the deterministic sample.
    pub seed: u64,
}

const LOCAL_BATCH: usize = 20_000;

/// Runs the local pass, then (with `api`) absence checks and the live sample.
pub fn verify(
    store: &mut Store,
    api: Option<&Tzkt>,
    opts: &VerifyOptions,
) -> Result<VerifyReport, IngestError> {
    let mut report = VerifyReport::default();
    local_pass(store, &mut report)?;
    if let Some(api) = api {
        absent_pass(store, api, opts, &mut report)?;
        sample_pass(store, api, opts, &mut report)?;
    }
    let c = store.conn_mut();
    let mut st = c
        .prepare("SELECT verify_state, count(*) FROM catalogue_tokens GROUP BY verify_state")
        .db()?;
    report.totals = st
        .query_map([], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)? as u64))
        })
        .db()?
        .collect::<rusqlite::Result<_>>()
        .db()?;
    drop(st);
    let mut st = c
        .prepare(
            "SELECT contract, token_id, verify_detail FROM catalogue_tokens \
             WHERE verify_state = 'mismatch' ORDER BY contract, token_id LIMIT 20",
        )
        .db()?;
    report.examples = st
        .query_map([], |r| {
            Ok(format!(
                "{}/{}: {}",
                r.get::<_, String>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, Option<String>>(2)?.unwrap_or_default()
            ))
        })
        .db()?
        .collect::<rusqlite::Result<_>>()
        .db()?;
    Ok(report)
}

struct LocalRow {
    contract: String,
    token_id: i64,
    project_id: u64,
    iteration: u32,
    minter: String,
    seed: String,
    artifact: String,
    level: Option<i64>,
}

/// Compares up to [`LOCAL_BATCH`] unverified catalogue rows after `after` with their stored
/// chain rows; returns how many were read.
fn recompare_batch(
    c: &Connection,
    after: &mut (String, i64),
    counts: &mut BTreeMap<String, u64>,
) -> Result<usize, StoreError> {
    let rows: Vec<LocalRow> = {
        let mut st = c
            .prepare_cached(
                "SELECT ct.contract, ct.token_id, t.project_id, t.iteration, t.minter, \
                 t.seed, t.artifact_uri, t.chain_level FROM catalogue_tokens ct \
                 JOIN tokens t ON t.contract = ct.contract AND t.token_id = ct.token_id \
                 WHERE ct.verify_state <> 'match' AND ct.verify_state = 'unverified' \
                 AND t.source = 'tzkt' AND (ct.contract, ct.token_id) > (?1, ?2) \
                 ORDER BY ct.contract, ct.token_id LIMIT ?3",
            )
            .db()?;
        st.query_map(params![after.0, after.1, LOCAL_BATCH as i64], |r| {
            Ok(LocalRow {
                contract: r.get(0)?,
                token_id: r.get(1)?,
                project_id: r.get::<_, i64>(2)? as u64,
                iteration: r.get(3)?,
                minter: r.get(4)?,
                seed: r.get(5)?,
                artifact: r.get(6)?,
                level: r.get(7)?,
            })
        })
        .db()?
        .collect::<rusqlite::Result<_>>()
        .db()?
    };
    for row in &rows {
        let facts = ChainFacts {
            project_id: row.project_id,
            iteration: row.iteration,
            minter: &row.minter,
            seed: Some(row.seed.as_str()),
            artifact: Some(row.artifact.as_str()),
        };
        let level = row.level.unwrap_or(0) as u64;
        if let Some(state) = verify_one(c, &row.contract, row.token_id as u64, &facts, level)? {
            *counts.entry(state.to_owned()).or_insert(0) += 1;
        }
        *after = (row.contract.clone(), row.token_id);
    }
    Ok(rows.len())
}

/// Unverified rows seen on chain but never written (no seed): unsigned mints.
fn mark_unsigned(c: &Connection, counts: &mut BTreeMap<String, u64>) -> Result<(), StoreError> {
    // `<> 'match'` lets SQLite use the partial index of open rows.
    let n = c
        .execute(
            "UPDATE catalogue_tokens SET chain_state = 'present', verify_state = 'unsigned' \
             WHERE verify_state <> 'match' AND verify_state = 'unverified' AND seed IS NULL \
             AND EXISTS (SELECT 1 FROM ingest_issues i WHERE i.contract = catalogue_tokens.contract \
             AND i.token_id = catalogue_tokens.token_id AND i.kind = 'unsigned')",
            [],
        )
        .db()?;
    if n > 0 {
        *counts.entry("unsigned".to_owned()).or_insert(0) += n as u64;
    }
    Ok(())
}

/// Re-compares every unverified catalogue row with stored chain data, inside the caller's
/// transaction (bootstrap and upgrade). No network.
pub(crate) fn recompare_all(c: &Connection) -> Result<BTreeMap<String, u64>, StoreError> {
    let mut counts = BTreeMap::new();
    let mut after = (String::new(), -1);
    while recompare_batch(c, &mut after, &mut counts)? == LOCAL_BATCH {}
    mark_unsigned(c, &mut counts)?;
    Ok(counts)
}

/// Unverified catalogue rows against chain-sourced token rows, one transaction per batch.
fn local_pass(store: &mut Store, report: &mut VerifyReport) -> Result<(), IngestError> {
    let mut after: (String, i64) = (String::new(), -1);
    loop {
        let tx = store
            .conn_mut()
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .db()?;
        let n = recompare_batch(&tx, &mut after, &mut report.local)?;
        tx.commit().db()?;
        if n < LOCAL_BATCH {
            break;
        }
    }
    mark_unsigned(store.conn_mut(), &mut report.local)?;
    Ok(())
}

fn mark_absent(c: &Connection, contract: &str, token_id: u64) -> Result<(), StoreError> {
    c.prepare_cached(
        "UPDATE catalogue_tokens SET chain_state = 'absent', verify_state = 'absent', \
         verify_detail = NULL WHERE contract = ?1 AND token_id = ?2",
    )
    .db()?
    .execute(params![contract, token_id as i64])
    .db()?;
    Ok(())
}

/// Catalogue tokens unseen after a completed full pass, confirmed one lookup at a time.
fn absent_pass(
    store: &mut Store,
    api: &Tzkt,
    opts: &VerifyOptions,
    report: &mut VerifyReport,
) -> Result<(), IngestError> {
    let candidates: Vec<(String, u64)> = {
        let c = store.conn_mut();
        let mut st = c
            .prepare(
                "SELECT ct.contract, ct.token_id FROM catalogue_tokens ct \
                 JOIN sync_state s ON s.contract = ct.contract AND s.synced_level IS NOT NULL \
                 WHERE ct.chain_state = 'unknown' AND NOT EXISTS (SELECT 1 FROM tokens t \
                 WHERE t.contract = ct.contract AND t.token_id = ct.token_id) \
                 ORDER BY ct.contract, ct.token_id LIMIT ?1",
            )
            .db()?;
        st.query_map([opts.absent_limit as i64], |r| {
            Ok((r.get(0)?, r.get::<_, i64>(1)? as u64))
        })
        .db()?
        .collect::<rusqlite::Result<_>>()
        .db()?
    };
    // A 404 means absent only if the lookup works: probe each contract with a token we hold.
    let mut usable: HashMap<String, bool> = HashMap::new();
    for (contract, _) in &candidates {
        if usable.contains_key(contract) {
            continue;
        }
        let known: Option<i64> = store
            .conn_mut()
            .query_row(
                "SELECT token_id FROM tokens WHERE contract = ?1 AND source = 'tzkt' LIMIT 1",
                [contract],
                |r| r.get(0),
            )
            .optional()
            .db()?;
        let ok = match known {
            None => false,
            Some(id) => match api.token_exists(contract, id as u64) {
                Ok(true) => true,
                Ok(false) => {
                    return Err(ApiError::Permanent(format!(
                        "absence probe: {contract}/{id} is held but the lookup says absent"
                    ))
                    .into());
                }
                Err(ApiError::Transient(_)) => false,
                Err(e) => return Err(e.into()),
            },
        };
        usable.insert(contract.clone(), ok);
    }
    for (contract, token_id) in candidates {
        if !usable.get(&contract).copied().unwrap_or(false) {
            report.transient += 1;
            continue;
        }
        report.absent_checked += 1;
        match api.token_exists(&contract, token_id) {
            Ok(true) => {
                store
                    .conn_mut()
                    .execute(
                        "UPDATE catalogue_tokens SET chain_state = 'present' \
                         WHERE contract = ?1 AND token_id = ?2",
                        params![contract, token_id as i64],
                    )
                    .db()?;
            }
            Ok(false) => {
                mark_absent(store.conn_mut(), &contract, token_id)?;
                report.absent_confirmed += 1;
            }
            Err(ApiError::Transient(_)) => report.transient += 1,
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}

/// `n` catalogue tokens in a seeded pseudo-random order (multiplicative hash of the id).
fn sample_keys(c: &Connection, n: usize, seed: u64) -> Result<Vec<(String, u64)>, StoreError> {
    let mut st = c
        .prepare(
            "SELECT contract, token_id FROM catalogue_tokens \
             ORDER BY ((token_id + ?1) * 2654435761) % 4294967311, contract LIMIT ?2",
        )
        .db()?;
    let mut keys: Vec<(String, u64)> = st
        .query_map(params![(seed % 1_000_003) as i64, n as i64], |r| {
            Ok((r.get(0)?, r.get::<_, i64>(1)? as u64))
        })
        .db()?
        .collect::<rusqlite::Result<_>>()
        .db()?;
    keys.sort();
    Ok(keys)
}

/// Re-reads sampled catalogue tokens from TzKT and records the comparison.
fn sample_pass(
    store: &mut Store,
    api: &Tzkt,
    opts: &VerifyOptions,
    report: &mut VerifyReport,
) -> Result<(), IngestError> {
    let keys = sample_keys(store.conn_mut(), opts.sample, opts.seed)?;
    let mut by_contract: BTreeMap<String, Vec<u64>> = BTreeMap::new();
    for (c, id) in keys {
        by_contract.entry(c).or_default().push(id);
    }
    for (contract, ids) in by_contract {
        for chunk in ids.chunks(super::tzkt::MAX_IN) {
            let fetched = api
                .tokens_by_ids(&contract, chunk, None)
                .and_then(|t| Ok((t, api.mints_by_ids(&contract, chunk, None)?)));
            let (tokens, mints) = match fetched {
                Ok(v) => v,
                Err(ApiError::Transient(_)) => {
                    report.transient += chunk.len() as u64;
                    continue;
                }
                Err(e) => return Err(e.into()),
            };
            let tokens: HashMap<u64, ChainToken> =
                tokens.into_iter().map(|t| (t.token_id, t)).collect();
            let mints: HashMap<u64, Mint> = mints.into_iter().map(|m| (m.token_id, m)).collect();
            let tx = store
                .conn_mut()
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .db()?;
            for &id in chunk {
                report.sampled += 1;
                let state = match (tokens.get(&id), mints.get(&id)) {
                    (None, _) => {
                        mark_absent(&tx, &contract, id)?;
                        "absent"
                    }
                    (Some(_), None) => "mint_missing",
                    (Some(t), Some(m)) => {
                        let facts = ChainFacts {
                            project_id: m.project_id,
                            iteration: m.iteration,
                            minter: &m.minter,
                            seed: t.seed(),
                            artifact: t.artifact_uri.as_deref(),
                        };
                        let stored: Option<(String, String)> = tx
                            .prepare_cached(
                                "SELECT seed, artifact_uri FROM tokens WHERE contract = ?1 \
                                 AND token_id = ?2 AND source = 'tzkt'",
                            )
                            .db()?
                            .query_row(params![contract, id as i64], |r| Ok((r.get(0)?, r.get(1)?)))
                            .optional()
                            .db()?;
                        if let Some((seed, artifact)) = stored {
                            if Some(seed.as_str()) != facts.seed
                                || Some(artifact.as_str()) != facts.artifact
                            {
                                report.stale_rows += 1;
                            }
                        }
                        verify_one(&tx, &contract, id, &facts, t.last_level)?.unwrap_or("absent")
                    }
                };
                *report.sample.entry(state.to_owned()).or_insert(0) += 1;
            }
            tx.execute(
                "UPDATE sync_state SET updated_at = ?2 WHERE contract = ?1",
                params![contract, now_iso()],
            )
            .db()?;
            tx.commit().db()?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn facts<'a>(seed: Option<&'a str>, artifact: Option<&'a str>) -> ChainFacts<'a> {
        ChainFacts {
            project_id: 7,
            iteration: 3,
            minter: "tz1minter",
            seed,
            artifact,
        }
    }

    #[test]
    fn matching_records_match_and_unsigned_stays_unsigned() {
        let a = "ipfs://Qm?fxhash=ooS&fxiteration=3&fxminter=tz1minter";
        assert_eq!(
            compare(7, Some("ooS"), Some(a), &facts(Some("ooS"), Some(a))),
            ("match", None)
        );
        assert_eq!(
            compare(7, None, Some("ipfs://P"), &facts(None, Some("ipfs://P"))),
            ("unsigned", None)
        );
    }

    #[test]
    fn every_disagreeing_field_is_named() {
        let cat = "ipfs://Qm?fxhash=ooX&fxiteration=4&fxminter=tz1other";
        let chain = "ipfs://Qm?fxhash=ooS";
        let (state, detail) = compare(8, Some("ooX"), Some(cat), &facts(Some("ooS"), Some(chain)));
        assert_eq!(state, "mismatch");
        assert_eq!(
            detail.as_deref(),
            Some("project,seed,artifact,artifact_seed,artifact_iteration,artifact_minter")
        );
        assert_eq!(
            compare(7, None, None, &facts(Some("ooS"), Some(chain)))
                .1
                .as_deref(),
            Some("catalogue_seed_missing,catalogue_artifact_missing")
        );
    }
}
