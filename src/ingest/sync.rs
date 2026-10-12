//! TzKT sync for the fxhash token contracts.
//!
//! Per contract, two kinds of pass, each resumable:
//! - **Full**: the final level `L` (head − [`FINALITY`]) is recorded, then every token-id
//!   chunk is fetched (tokens, their mints, positive ledger balances) by bounded parallel
//!   workers and written by one writer, each chunk in one transaction together with its done
//!   marker. When every chunk is done, `synced_level = L`.
//! - **Incremental** (after a full pass): new mints, changed tokens and changed balances
//!   with `lastLevel > synced_level`, paged by TzKT id. Each page commits together with its
//!   cursor; the pass ends with `synced_level` = the final level recorded at its start.
//!
//! Every read is capped at the pass's final level (`level.le` / `lastLevel.le`), so nothing a
//! reorg can remove is ingested; what changed above it is read by the next pass. A rollback
//! stamps restored tokens and balances with the reverted level, above the cap, so they are
//! read again too.
//!
//! A cursor or done marker never commits without its rows, so an interrupted or failed
//! batch is simply fetched again. Transient failures leave work pending; nothing permanent
//! is recorded for them.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, mpsc};
use std::thread;
use std::time::Instant;

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};

use super::escrow;
use super::http::ApiError;
use super::tzkt::{Balance, ChainToken, Mint, Tzkt};
use super::verify::{ChainFacts, verify_one};
use super::{FINALITY, ISSUERS, IngestError, TOKEN_CONTRACTS, now_iso};
use crate::store::{Db, Store, StoreError, add_content_roots};

/// Owner rows written by sync carry this source.
pub const OWNER_SOURCE: &str = "tzkt";
/// Consecutive transient chunk failures before the run stops.
const MAX_CONSECUTIVE_FAILURES: usize = 5;
/// Issues a successful write resolves.
const RESOLVED_BY_WRITE: &str = "('mint_missing', 'token_missing', 'unsigned', \
     'metadata_missing', 'project_unknown', 'iteration_conflict')";
/// Issues re-fetched on every incremental pass. Unsigned mints are not: fxhash's signer is
/// gone, and a signature would move the token's lastLevel anyway.
const RETRIED: &str = "('mint_missing', 'token_missing', 'metadata_missing', \
     'project_unknown', 'iteration_conflict')";

#[derive(Debug, Clone)]
pub struct SyncOptions {
    /// Token contracts to sync; empty means all three.
    pub contracts: Vec<String>,
    /// Stop after this many batches (chunks or pages) this run.
    pub limit: Option<usize>,
    /// Parallel fetch workers for full passes.
    pub concurrency: usize,
    /// Token ids per full-pass chunk; rows per incremental page. At most 10,000.
    pub batch: u64,
}

impl Default for SyncOptions {
    fn default() -> Self {
        SyncOptions {
            contracts: Vec::new(),
            limit: None,
            concurrency: 4,
            batch: 500,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SyncReport {
    pub batches: usize,
    pub tokens_written: usize,
    pub owners_set: usize,
    pub owners_cleared: usize,
    pub mismatches: usize,
    /// New or refreshed issues by kind.
    pub issues: BTreeMap<String, usize>,
    pub transient_failures: usize,
    /// Escrow listing keys stored as live, and removed or not live.
    pub listings_live: usize,
    pub listings_gone: usize,
    /// Contracts (token contracts and escrows) whose passes completed in this run.
    pub completed: Vec<String>,
    /// Contracts with work left.
    pub pending: Vec<String>,
    pub interrupted: bool,
    pub limited: bool,
}

impl SyncReport {
    fn merge(&mut self, s: &BatchStats) {
        self.batches += 1;
        self.tokens_written += s.tokens;
        self.owners_set += s.owners_set;
        self.owners_cleared += s.owners_cleared;
        self.mismatches += s.mismatches;
        for (k, v) in &s.issues {
            *self.issues.entry((*k).to_owned()).or_insert(0) += v;
        }
    }
}

#[derive(Debug, Default)]
struct BatchStats {
    tokens: usize,
    owners_set: usize,
    owners_cleared: usize,
    mismatches: usize,
    issues: BTreeMap<&'static str, usize>,
}

/// Everything fetched for one write.
pub(crate) struct Batch {
    pub contract: String,
    pub mints: Vec<Mint>,
    pub tokens: Vec<ChainToken>,
    pub balances: Vec<Balance>,
    /// Token ids whose positive balances in `balances` are complete: a token here without
    /// one has no owner. Change feeds leave it empty; their zero rows clear owners instead.
    pub complete_for: BTreeSet<u64>,
    /// When the balances were read (UTC, ISO 8601).
    pub fetched_at: String,
}

#[derive(Debug, Clone, Default)]
struct State {
    synced_level: Option<u64>,
    full_level: Option<u64>,
    full_lo: u64,
    full_hi: u64,
    full_batch: u64,
    incr_level: Option<u64>,
    incr_mint_id: u64,
    incr_token_id: u64,
    incr_balance_id: u64,
    incr_phase: u8,
    incr_retry_after: u64,
}

fn opt_u64(v: Option<i64>) -> Option<u64> {
    v.and_then(|v| u64::try_from(v).ok())
}

fn load_state(c: &Connection, contract: &str) -> Result<Option<State>, StoreError> {
    c.query_row(
        "SELECT synced_level, full_level, full_lo, full_hi, full_batch, incr_level, \
         incr_mint_id, incr_token_id, incr_balance_id, incr_phase, incr_retry_after \
         FROM sync_state \
         WHERE contract = ?1",
        [contract],
        |r| {
            Ok(State {
                synced_level: opt_u64(r.get(0)?),
                full_level: opt_u64(r.get(1)?),
                full_lo: opt_u64(r.get(2)?).unwrap_or(0),
                full_hi: opt_u64(r.get(3)?).unwrap_or(0),
                full_batch: opt_u64(r.get(4)?).unwrap_or(0),
                incr_level: opt_u64(r.get(5)?),
                incr_mint_id: opt_u64(r.get(6)?).unwrap_or(0),
                incr_token_id: opt_u64(r.get(7)?).unwrap_or(0),
                incr_balance_id: opt_u64(r.get(8)?).unwrap_or(0),
                incr_phase: r.get::<_, i64>(9)?.clamp(0, i64::from(PHASE_DONE)) as u8,
                incr_retry_after: opt_u64(r.get(10)?).unwrap_or(0),
            })
        },
    )
    .optional()
    .db()
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Job {
    contract: String,
    lo: u64,
    hi: u64,
    /// The pass's final level: nothing changed above it is read.
    upto: u64,
}

/// Most chunks one full pass may plan; a smaller batch over a wider range is refused.
const MAX_PLANNED_CHUNKS: u64 = 1_000_000;

/// Number of `batch`-aligned chunks covering `[lo, hi)`, checked.
fn chunk_count(lo: u64, hi: u64, batch: u64) -> Result<u64, IngestError> {
    if batch == 0 || lo > hi {
        return Err(IngestError::Usage(format!(
            "bad chunk plan [{lo}, {hi}) / {batch}"
        )));
    }
    let start = lo / batch * batch;
    let n = (hi - start).div_ceil(batch);
    if n > MAX_PLANNED_CHUNKS {
        return Err(IngestError::Usage(format!(
            "{n} chunks for [{lo}, {hi}): raise SEEDED_SYNC_BATCH"
        )));
    }
    Ok(n)
}

/// Chunks of `[lo, hi)` on a `batch`-aligned grid, at most [`MAX_PLANNED_CHUNKS`].
fn plan(contract: &str, lo: u64, hi: u64, batch: u64, upto: u64) -> Result<Vec<Job>, IngestError> {
    let n = chunk_count(lo, hi, batch)?;
    let start = lo / batch * batch;
    Ok((0..n)
        .map(|i| {
            let a = start.saturating_add(i.saturating_mul(batch));
            Job {
                contract: contract.to_owned(),
                lo: a.max(lo),
                hi: a.saturating_add(batch).min(hi),
                upto,
            }
        })
        .collect())
}

fn record_issue(
    c: &Connection,
    stats: &mut BatchStats,
    contract: &str,
    token_id: u64,
    kind: &'static str,
    detail: Option<&str>,
    level: Option<u64>,
) -> Result<(), StoreError> {
    c.prepare_cached(
        "INSERT INTO ingest_issues (contract, token_id, kind, detail, level, seen_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6) ON CONFLICT (contract, token_id, kind) DO UPDATE SET \
         detail = excluded.detail, level = excluded.level, seen_at = excluded.seen_at",
    )
    .db()?
    .execute(params![
        contract,
        token_id as i64,
        kind,
        detail,
        level.map(|l| l as i64),
        now_iso()
    ])
    .db()?;
    *stats.issues.entry(kind).or_insert(0) += 1;
    Ok(())
}

fn is_unique_violation(e: &rusqlite::Error) -> bool {
    matches!(e, rusqlite::Error::SqliteFailure(f, _)
        if f.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_UNIQUE)
}

/// A project written in this batch: one of its tokens, and its earliest mint here.
struct Touch<'a> {
    token_id: u64,
    first_mint: Option<(u64, &'a str)>,
}

struct Known {
    project_id: u64,
    iteration: u32,
    minter: String,
    params: Option<String>,
}

/// Writes one batch into an open transaction.
fn apply(c: &Connection, b: &Batch) -> Result<BatchStats, StoreError> {
    let mut s = BatchStats::default();
    let contract = b.contract.as_str();
    let mints: HashMap<u64, &Mint> = b.mints.iter().map(|m| (m.token_id, m)).collect();
    let mut touched: BTreeMap<u64, Touch<'_>> = BTreeMap::new();
    for m in &b.mints {
        if !ISSUERS
            .iter()
            .any(|(i, t)| *i == m.issuer && *t == contract)
        {
            record_issue(
                c,
                &mut s,
                contract,
                m.token_id,
                "unknown_issuer",
                Some(&m.issuer),
                Some(m.level),
            )?;
        }
    }
    for t in &b.tokens {
        // The token came back: whatever happens next, it is no longer missing.
        c.prepare_cached(
            "DELETE FROM ingest_issues WHERE contract = ?1 AND token_id = ?2 \
             AND kind = 'token_missing'",
        )
        .db()?
        .execute(params![contract, to_i64(t.token_id)?])
        .db()?;
        c.prepare_cached(
            "UPDATE catalogue_tokens SET chain_state = 'present' \
             WHERE contract = ?1 AND token_id = ?2 AND chain_state <> 'present'",
        )
        .db()?
        .execute(params![contract, t.token_id as i64])
        .db()?;
        let known = match mints.get(&t.token_id) {
            Some(m) => Some(Known {
                project_id: m.project_id,
                iteration: m.iteration,
                minter: m.minter.clone(),
                params: m.params.clone(),
            }),
            None => c
                .prepare_cached(
                    "SELECT project_id, iteration, minter, params FROM tokens \
                     WHERE contract = ?1 AND token_id = ?2 AND source = 'tzkt'",
                )
                .db()?
                .query_row(params![contract, t.token_id as i64], |r| {
                    Ok(Known {
                        project_id: r.get::<_, i64>(0)? as u64,
                        iteration: r.get(1)?,
                        minter: r.get(2)?,
                        params: r.get(3)?,
                    })
                })
                .optional()
                .db()?,
        };
        let Some(known) = known else {
            record_issue(
                c,
                &mut s,
                contract,
                t.token_id,
                "mint_missing",
                None,
                Some(t.last_level),
            )?;
            continue;
        };
        let seed = t.seed();
        let facts = ChainFacts {
            project_id: known.project_id,
            iteration: known.iteration,
            minter: &known.minter,
            seed,
            artifact: t.artifact_uri.as_deref(),
        };
        let verified = verify_one(c, contract, t.token_id, &facts, t.last_level)?;
        s.mismatches += usize::from(verified == Some("mismatch"));
        let Some(seed) = seed else {
            record_issue(
                c,
                &mut s,
                contract,
                t.token_id,
                "unsigned",
                t.name.as_deref(),
                Some(t.last_level),
            )?;
            continue;
        };
        let Some(artifact) = t.artifact_uri.as_deref() else {
            record_issue(
                c,
                &mut s,
                contract,
                t.token_id,
                "metadata_missing",
                None,
                Some(t.last_level),
            )?;
            continue;
        };
        let project_known = c
            .prepare_cached("SELECT 1 FROM projects WHERE id = ?1")
            .db()?
            .exists([known.project_id as i64])
            .db()?;
        if !project_known {
            let detail = known.project_id.to_string();
            record_issue(
                c,
                &mut s,
                contract,
                t.token_id,
                "project_unknown",
                Some(&detail),
                Some(t.last_level),
            )?;
            continue;
        }
        if known.iteration == 0 {
            record_issue(
                c,
                &mut s,
                contract,
                t.token_id,
                "iteration_conflict",
                Some("iteration 0"),
                Some(t.last_level),
            )?;
            continue;
        }
        let attributes = if t.attributes.is_empty() {
            None
        } else {
            Some(
                serde_json::to_string(&t.attributes)
                    .map_err(|e| StoreError::Internal(e.to_string()))?,
            )
        };
        let minted = t.first_time.get(..10).unwrap_or(&t.first_time);
        let written = c
            .prepare_cached(
                "INSERT INTO tokens (contract, token_id, project_id, iteration, seed, minter, \
                 minted, artifact_uri, display_uri, thumbnail_uri, params, first_time, \
                 attributes, chain_level, source) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, 'tzkt') \
                 ON CONFLICT (contract, token_id) DO UPDATE SET \
                 project_id = excluded.project_id, iteration = excluded.iteration, \
                 seed = excluded.seed, minter = excluded.minter, minted = excluded.minted, \
                 artifact_uri = excluded.artifact_uri, display_uri = excluded.display_uri, \
                 thumbnail_uri = excluded.thumbnail_uri, params = excluded.params, \
                 first_time = excluded.first_time, attributes = excluded.attributes, \
                 chain_level = excluded.chain_level, source = 'tzkt'",
            )
            .db()?
            .execute(params![
                contract,
                t.token_id as i64,
                known.project_id as i64,
                known.iteration,
                seed,
                known.minter,
                minted,
                artifact,
                t.display_uri,
                t.thumbnail_uri,
                known.params,
                t.first_time,
                attributes,
                t.last_level as i64,
            ]);
        match written {
            Ok(_) => {}
            Err(e) if is_unique_violation(&e) => {
                let detail = format!("{}#{}", known.project_id, known.iteration);
                record_issue(
                    c,
                    &mut s,
                    contract,
                    t.token_id,
                    "iteration_conflict",
                    Some(&detail),
                    Some(t.last_level),
                )?;
                continue;
            }
            Err(e) => return Err(crate::store::classify(&e)),
        }
        add_content_roots(
            c,
            known.project_id,
            &[
                Some(artifact),
                t.display_uri.as_deref(),
                t.thumbnail_uri.as_deref(),
            ],
        )?;
        c.prepare_cached("DELETE FROM token_features WHERE contract = ?1 AND token_id = ?2")
            .db()?
            .execute(params![contract, t.token_id as i64])
            .db()?;
        c.prepare_cached(&format!(
            "DELETE FROM ingest_issues WHERE contract = ?1 AND token_id = ?2 \
             AND kind IN {RESOLVED_BY_WRITE}"
        ))
        .db()?
        .execute(params![contract, t.token_id as i64])
        .db()?;
        s.tokens += 1;
        if verified.is_none() {
            // Chain data with no catalogue record: shown, but not counted as verified.
            record_issue(
                c,
                &mut s,
                contract,
                t.token_id,
                "not_catalogued",
                None,
                Some(t.last_level),
            )?;
        }
        let touch = touched.entry(known.project_id).or_insert(Touch {
            token_id: t.token_id,
            first_mint: None,
        });
        if let Some(m) = mints.get(&t.token_id) {
            if touch.first_mint.is_none_or(|(id, _)| m.token_id < id) {
                touch.first_mint = Some((m.token_id, m.issuer.as_str()));
            }
        }
    }

    // A mint whose token a successful response did not return is retried later, not lost.
    let returned: BTreeSet<u64> = b.tokens.iter().map(|t| t.token_id).collect();
    for m in b.mints.iter().filter(|m| !returned.contains(&m.token_id)) {
        record_issue(
            c,
            &mut s,
            contract,
            m.token_id,
            "token_missing",
            Some("mint present, token absent from the response"),
            Some(m.level),
        )?;
    }

    // Owners, only ever from ledger balances.
    let escrows = escrow::known_addresses(c)?;
    let mut positive: BTreeMap<u64, &Balance> = BTreeMap::new();
    let mut zeros: Vec<&Balance> = Vec::new();
    for bal in &b.balances {
        if !bal.positive {
            zeros.push(bal);
            continue;
        }
        if let Some(prev) = positive.get(&bal.token_id) {
            if b.complete_for.contains(&bal.token_id) {
                record_issue(
                    c,
                    &mut s,
                    contract,
                    bal.token_id,
                    "multiple_holders",
                    Some(&bal.address),
                    Some(bal.last_level),
                )?;
            }
            if (prev.last_level, prev.id) > (bal.last_level, bal.id) {
                continue;
            }
        }
        positive.insert(bal.token_id, bal);
    }
    for bal in zeros {
        let n = c
            .prepare_cached(
                "UPDATE tokens SET owner_address = NULL, owner_alias = NULL, \
                 owner_alias_key = NULL, owner_as_of = NULL, owner_source = NULL, \
                 owner_level = NULL, listed_by = NULL, holder_address = NULL \
                 WHERE contract = ?1 AND token_id = ?2 AND owner_address = ?3",
            )
            .db()?
            .execute(params![contract, bal.token_id as i64, bal.address])
            .db()?;
        s.owners_cleared += n;
    }
    for &id in b
        .complete_for
        .iter()
        .filter(|id| !positive.contains_key(id))
    {
        let n = c
            .prepare_cached(
                "UPDATE tokens SET owner_address = NULL, owner_alias = NULL, \
                 owner_alias_key = NULL, owner_as_of = NULL, owner_source = NULL, \
                 owner_level = NULL, listed_by = NULL, holder_address = NULL \
                 WHERE contract = ?1 AND token_id = ?2 AND owner_address IS NOT NULL",
            )
            .db()?
            .execute(params![contract, id as i64])
            .db()?;
        s.owners_cleared += n;
    }
    for bal in positive.values() {
        let alias: Option<String> = c
            .prepare_cached(
                "SELECT coalesce((SELECT name FROM identities WHERE address = ?1), \
                 (SELECT name FROM users WHERE address = ?1))",
            )
            .db()?
            .query_row([&bal.address], |r| r.get(0))
            .optional()
            .db()?
            .flatten();
        let (listed, holder) =
            escrow::holder_of(c, &escrows, &bal.address, contract, bal.token_id)?;
        let n = c
            .prepare_cached(
                "UPDATE tokens SET owner_address = ?3, owner_alias = ?4, owner_alias_key = ?5, \
                 owner_as_of = ?6, owner_source = ?7, owner_level = ?8, listed_by = ?9, \
                 holder_address = ?10 WHERE contract = ?1 AND token_id = ?2",
            )
            .db()?
            .execute(params![
                contract,
                bal.token_id as i64,
                bal.address,
                alias,
                alias.as_deref().map(str::to_lowercase),
                b.fetched_at,
                OWNER_SOURCE,
                bal.last_level as i64,
                listed,
                holder,
            ])
            .db()?;
        s.owners_set += n;
    }

    for (project, touch) in touched {
        let current: Option<Option<String>> = c
            .prepare_cached("SELECT contract FROM projects WHERE id = ?1")
            .db()?
            .query_row([project as i64], |r| r.get(0))
            .optional()
            .db()?;
        if let Some(Some(catalogued)) = current.filter(|k| k.as_deref() != Some(contract)) {
            record_issue(
                c,
                &mut s,
                contract,
                touch.token_id,
                "contract_mismatch",
                Some(&catalogued),
                None,
            )?;
        }
        // The issuer is the first mint's sender, whatever order chunks arrive in.
        let (first_id, issuer) = touch.first_mint.unzip();
        c.prepare_cached(
            "UPDATE projects SET \
             editions = (SELECT nullif(count(*), 0) FROM tokens WHERE project_id = ?1), \
             first_mint = (SELECT min(minted) FROM tokens WHERE project_id = ?1), \
             last_mint = (SELECT max(minted) FROM tokens WHERE project_id = ?1), \
             index_state = CASE WHEN EXISTS (SELECT 1 FROM tokens WHERE project_id = ?1) \
               THEN 'indexed' ELSE 'not_indexed' END, \
             contract = coalesce(contract, ?2), \
             issuer = CASE WHEN ?4 = (SELECT min(token_id) FROM tokens WHERE project_id = ?1) \
               THEN ?3 ELSE coalesce(issuer, ?3) END \
             WHERE id = ?1",
        )
        .db()?
        .execute(params![
            project as i64,
            contract,
            issuer,
            first_id.map(|i| i as i64)
        ])
        .db()?;
    }
    Ok(s)
}

fn write_chunk(store: &mut Store, job: &Job, batch: &Batch) -> Result<BatchStats, StoreError> {
    let tx = store
        .conn_mut()
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .db()?;
    let stats = apply(&tx, batch)?;
    let now = now_iso();
    tx.execute(
        "INSERT INTO sync_chunks (contract, lo, hi, tokens, done_at) VALUES (?1, ?2, ?3, ?4, ?5) \
         ON CONFLICT (contract, lo) DO UPDATE SET hi = excluded.hi, tokens = excluded.tokens, \
         done_at = excluded.done_at",
        params![
            job.contract,
            job.lo as i64,
            job.hi as i64,
            batch.tokens.len() as i64,
            now
        ],
    )
    .db()?;
    tx.execute(
        "UPDATE sync_state SET updated_at = ?2 WHERE contract = ?1",
        params![job.contract, now],
    )
    .db()?;
    tx.commit().db()?;
    Ok(stats)
}

fn fetch_chunk(api: &Tzkt, job: &Job) -> Result<Batch, ApiError> {
    let tokens = api.tokens_range(&job.contract, job.lo, job.hi, job.upto)?;
    let mints = api.mints_range(&job.contract, job.lo, job.hi, job.upto)?;
    let fetched_at = now_iso();
    let balances = api.balances_range(&job.contract, job.lo, job.hi, job.upto)?;
    Ok(Batch {
        contract: job.contract.clone(),
        mints,
        complete_for: tokens.iter().map(|t| t.token_id).collect(),
        tokens,
        balances,
        fetched_at,
    })
}

/// Starts a full pass when none ran yet; returns its planned jobs minus done chunks.
fn full_pass_jobs(
    store: &mut Store,
    api: &Tzkt,
    contract: &str,
    batch: u64,
) -> Result<Option<Vec<Job>>, IngestError> {
    let state = load_state(store.conn_mut(), contract)?.unwrap_or_default();
    if state.synced_level.is_some() && state.full_level.is_none() {
        return Ok(None);
    }
    let state = if state.full_level.is_some() {
        state
    } else {
        let level = api.head_level()?.saturating_sub(FINALITY);
        // Token, mint and catalogue bounds together: a token row lagging at the edge of the
        // index must not shrink the range its mint and catalogue record fall in.
        let catalogue: Option<(u64, u64)> = store
            .conn_mut()
            .query_row(
                "SELECT min(token_id), max(token_id) FROM catalogue_tokens WHERE contract = ?1 \
                 HAVING count(*) > 0",
                [contract],
                |r| Ok((r.get::<_, i64>(0)? as u64, r.get::<_, i64>(1)? as u64)),
            )
            .optional()
            .db()?;
        let bounds = merge_bounds(&[
            api.token_bounds(contract)?,
            api.mint_bounds(contract)?,
            catalogue,
        ])?;
        let now = now_iso();
        let tx = store
            .conn_mut()
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .db()?;
        tx.execute("DELETE FROM sync_chunks WHERE contract = ?1", [contract])
            .db()?;
        let (lo, hi) = match bounds {
            None => (0, 0),
            Some((lo, hi)) => (
                lo,
                hi.checked_add(1)
                    .ok_or_else(|| ApiError::Permanent("token id bound overflows".into()))?,
            ),
        };
        chunk_count(lo, hi, batch)?;
        tx.execute(
            "INSERT INTO sync_state (contract, full_level, full_lo, full_hi, full_batch, \
             updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6) ON CONFLICT (contract) DO UPDATE SET \
             full_level = excluded.full_level, full_lo = excluded.full_lo, \
             full_hi = excluded.full_hi, full_batch = excluded.full_batch, \
             updated_at = excluded.updated_at",
            params![
                contract,
                level as i64,
                lo as i64,
                hi as i64,
                batch as i64,
                now
            ],
        )
        .db()?;
        tx.commit().db()?;
        load_state(store.conn_mut(), contract)?.unwrap_or_default()
    };
    let done: BTreeSet<u64> = {
        let c = store.conn_mut();
        let mut st = c
            .prepare("SELECT lo FROM sync_chunks WHERE contract = ?1")
            .db()?;
        st.query_map([contract], |r| r.get::<_, i64>(0))
            .db()?
            .map(|r| r.map(|v| v as u64))
            .collect::<rusqlite::Result<_>>()
            .db()?
    };
    let pass_batch = if state.full_batch > 0 {
        state.full_batch
    } else {
        batch
    };
    Ok(Some(
        plan(
            contract,
            state.full_lo,
            state.full_hi,
            pass_batch,
            state.full_level.unwrap_or_default(),
        )?
        .into_iter()
        .filter(|j| !done.contains(&j.lo))
        .collect(),
    ))
}

/// The union of present `(lo, hi)` bounds, within [`super::tzkt::MAX_TOKEN_SPAN`].
fn merge_bounds(parts: &[Option<(u64, u64)>]) -> Result<Option<(u64, u64)>, IngestError> {
    let present: Vec<(u64, u64)> = parts.iter().flatten().copied().collect();
    let (Some(lo), Some(hi)) = (
        present.iter().map(|b| b.0).min(),
        present.iter().map(|b| b.1).max(),
    ) else {
        return Ok(None);
    };
    if lo > hi || hi - lo >= super::tzkt::MAX_TOKEN_SPAN {
        return Err(ApiError::Permanent(format!("implausible token id bounds {lo}..={hi}")).into());
    }
    Ok(Some((lo, hi)))
}

/// Queues retry work for catalogue tokens that have neither a token row nor any issue (and
/// are not confirmed absent): the next incremental pass looks them up by id. `contract`
/// limits it to one contract (at full-pass completion); `None` covers every contract whose
/// full pass completed.
pub(crate) fn enqueue_catalogue_candidates(
    c: &Connection,
    contract: Option<&str>,
) -> Result<usize, StoreError> {
    let n = c
        .execute(
            "INSERT OR IGNORE INTO ingest_issues (contract, token_id, kind, detail, level, seen_at) \
             SELECT ct.contract, ct.token_id, 'token_missing', \
             'catalogue record without a token row', NULL, ?2 FROM catalogue_tokens ct \
             WHERE (ct.contract = ?1 OR (?1 IS NULL AND EXISTS (SELECT 1 FROM sync_state s \
               WHERE s.contract = ct.contract AND s.synced_level IS NOT NULL))) \
             AND ct.chain_state <> 'absent' \
             AND NOT EXISTS (SELECT 1 FROM tokens t WHERE t.contract = ct.contract \
               AND t.token_id = ct.token_id) \
             AND NOT EXISTS (SELECT 1 FROM ingest_issues i WHERE i.contract = ct.contract \
               AND i.token_id = ct.token_id)",
            params![contract, now_iso()],
        )
        .db()?;
    Ok(n)
}

/// Marks the full pass complete when every planned chunk is done.
fn finish_full_pass(store: &mut Store, contract: &str) -> Result<bool, IngestError> {
    let tx = store
        .conn_mut()
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .db()?;
    let Some(state) = load_state(&tx, contract)? else {
        return Ok(false);
    };
    let Some(level) = state.full_level else {
        return Ok(false);
    };
    let planned = chunk_count(state.full_lo, state.full_hi, state.full_batch.max(1))?;
    let done: i64 = tx
        .query_row(
            "SELECT count(*) FROM sync_chunks WHERE contract = ?1",
            [contract],
            |r| r.get(0),
        )
        .db()?;
    if (done as u64) < planned {
        return Ok(false);
    }
    enqueue_catalogue_candidates(&tx, Some(contract))?;
    let now = now_iso();
    tx.execute(
        "UPDATE sync_state SET synced_level = ?2, synced_at = ?3, full_level = NULL, \
         full_lo = NULL, full_hi = NULL, full_batch = NULL, incr_level = NULL, \
         incr_mint_id = 0, incr_token_id = 0, incr_balance_id = 0, incr_phase = 0, \
         incr_retry_after = 0, updated_at = ?3 \
         WHERE contract = ?1",
        params![contract, level as i64, now],
    )
    .db()?;
    tx.execute("DELETE FROM sync_chunks WHERE contract = ?1", [contract])
        .db()?;
    mark_snapshot(&tx, &now)?;
    tx.commit().db()?;
    Ok(true)
}

fn mark_snapshot(c: &Connection, now: &str) -> Result<(), StoreError> {
    c.execute(
        "UPDATE snapshot SET as_of = ?1, owner_source = ?2 WHERE id = 1",
        params![&now[..10], OWNER_SOURCE],
    )
    .db()?;
    Ok(())
}

/// Runs full-pass chunks with bounded parallel fetches and one writer.
fn run_jobs(
    store: &mut Store,
    api: &Tzkt,
    jobs: Vec<Job>,
    concurrency: usize,
    report: &mut SyncReport,
    log: &(dyn Fn(&str) + Sync),
) -> Result<(), IngestError> {
    let total = jobs.len();
    let queue = Mutex::new(VecDeque::from(jobs));
    let halt = AtomicBool::new(false);
    let workers = concurrency.clamp(1, 32).min(total.max(1));
    let started = Instant::now();
    let (tx, rx) = mpsc::sync_channel::<(Job, Result<Batch, ApiError>)>(workers);
    thread::scope(|scope| {
        for _ in 0..workers {
            let tx = tx.clone();
            let (queue, halt) = (&queue, &halt);
            scope.spawn(move || {
                loop {
                    if halt.load(Ordering::Relaxed) || api.client.stopped() {
                        return;
                    }
                    let job = queue.lock().unwrap_or_else(|p| p.into_inner()).pop_front();
                    let Some(job) = job else { return };
                    let result = fetch_chunk(api, &job);
                    if tx.send((job, result)).is_err() {
                        return;
                    }
                }
            });
        }
        drop(tx);
        let mut outcome: Result<(), IngestError> = Ok(());
        let mut consecutive = 0;
        let mut done = 0usize;
        for (job, result) in rx {
            if outcome.is_err() {
                continue;
            }
            match result {
                Ok(batch) => match write_chunk(store, &job, &batch) {
                    Ok(stats) => {
                        consecutive = 0;
                        done += 1;
                        report.merge(&stats);
                        if done % 50 == 0 || done == total {
                            let secs = started.elapsed().as_secs_f64().max(0.001);
                            log(&format!(
                                "chunks {done}/{total}, tokens {} ({:.0}/s), requests {}, \
                                 retries {}",
                                report.tokens_written,
                                report.tokens_written as f64 / secs,
                                api.client.counters.requests.load(Ordering::Relaxed),
                                api.client.counters.retries.load(Ordering::Relaxed),
                            ));
                        }
                    }
                    Err(e) => {
                        log(&format!(
                            "write failed for {} [{}, {}): {e}",
                            job.contract, job.lo, job.hi
                        ));
                        halt.store(true, Ordering::Relaxed);
                        outcome = Err(e.into());
                    }
                },
                Err(ApiError::Interrupted) => report.interrupted = true,
                Err(ApiError::Transient(e)) => {
                    report.transient_failures += 1;
                    consecutive += 1;
                    log(&format!(
                        "transient failure {} [{}, {}): {e}",
                        job.contract, job.lo, job.hi
                    ));
                    if consecutive >= MAX_CONSECUTIVE_FAILURES {
                        log("too many consecutive failures; stopping, progress is kept");
                        halt.store(true, Ordering::Relaxed);
                    }
                }
                Err(e @ ApiError::Permanent(_)) => {
                    log(&format!(
                        "permanent failure {} [{}, {}): {e}",
                        job.contract, job.lo, job.hi
                    ));
                    halt.store(true, Ordering::Relaxed);
                    outcome = Err(e.into());
                }
            }
        }
        outcome
    })
}

/// Incremental phases, persisted in `sync_state.incr_phase` so repeated short runs (`--limit`)
/// always advance: a phase is marked done in the same transaction as its last page.
const PHASE_MINTS: u8 = 0;
const PHASE_TOKENS: u8 = 1;
const PHASE_BALANCES: u8 = 2;
const PHASE_RETRY: u8 = 3;
const PHASE_DONE: u8 = 4;

/// One incremental pass over `contract`; `Ok(true)` when it completed.
fn incremental(
    store: &mut Store,
    api: &Tzkt,
    contract: &str,
    page: usize,
    budget: &mut Option<usize>,
    report: &mut SyncReport,
) -> Result<bool, IngestError> {
    let Some(mut state) = load_state(store.conn_mut(), contract)? else {
        return Ok(false);
    };
    let Some(since) = state.synced_level else {
        return Ok(false);
    };
    if api.client.stopped() {
        report.interrupted = true;
        return Ok(false);
    }
    if *budget == Some(0) {
        report.limited = true;
        return Ok(false);
    }
    if state.incr_level.is_none() {
        let level = api.head_level()?.saturating_sub(FINALITY);
        store
            .conn_mut()
            .execute(
                "UPDATE sync_state SET incr_level = ?2, incr_mint_id = 0, incr_token_id = 0, \
                 incr_balance_id = 0, incr_phase = 0, incr_retry_after = 0, updated_at = ?3 \
                 WHERE contract = ?1",
                params![contract, to_i64(level)?, now_iso()],
            )
            .db()?;
        state.incr_level = Some(level);
        state.incr_mint_id = 0;
        state.incr_token_id = 0;
        state.incr_balance_id = 0;
        state.incr_phase = PHASE_MINTS;
        state.incr_retry_after = 0;
    }
    // Nothing changed above the pass's final level is read; the next pass reads it.
    let upto = state.incr_level.unwrap_or(since);
    let spend = |budget: &mut Option<usize>, report: &mut SyncReport| -> bool {
        match budget {
            Some(0) => {
                report.limited = true;
                false
            }
            Some(n) => {
                *n -= 1;
                true
            }
            None => true,
        }
    };
    while state.incr_phase < PHASE_DONE {
        if api.client.stopped() {
            report.interrupted = true;
            return Ok(false);
        }
        match state.incr_phase {
            PHASE_MINTS => {
                if !spend(budget, report) {
                    return Ok(false);
                }
                let mints = api.mints_since(contract, (since, upto), state.incr_mint_id, page)?;
                let ids: Vec<u64> = mints.iter().map(|m| m.token_id).collect();
                let n = mints.len();
                let tokens = api.tokens_by_ids(contract, &ids, Some(upto))?;
                let fetched_at = now_iso();
                let balances = api.balances_by_ids(contract, &ids, Some(upto))?;
                if let Some(last) = mints.iter().map(|m| m.op_id).max() {
                    state.incr_mint_id = last;
                }
                let done = n < page;
                let batch = Batch {
                    contract: contract.to_owned(),
                    mints,
                    complete_for: ids.iter().copied().collect(),
                    tokens,
                    balances,
                    fetched_at,
                };
                let next = done.then_some(PHASE_TOKENS);
                write_page(
                    store,
                    &batch,
                    contract,
                    Some(("incr_mint_id", state.incr_mint_id)),
                    next,
                    report,
                )?;
                if done {
                    state.incr_phase = PHASE_TOKENS;
                }
            }
            PHASE_TOKENS => {
                if !spend(budget, report) {
                    return Ok(false);
                }
                let tokens =
                    api.tokens_changed(contract, (since, upto), state.incr_token_id, page)?;
                let n = tokens.len();
                let unknown: Vec<u64> = {
                    let c = store.conn_mut();
                    let mut st = c
                        .prepare_cached(
                            "SELECT 1 FROM tokens WHERE contract = ?1 AND token_id = ?2 \
                             AND source = 'tzkt'",
                        )
                        .db()?;
                    let mut v = Vec::new();
                    for t in &tokens {
                        if !st.exists(params![contract, to_i64(t.token_id)?]).db()? {
                            v.push(t.token_id);
                        }
                    }
                    v
                };
                // Tokens written for the first time need their mint and current owner too.
                let mints = api.mints_by_ids(contract, &unknown, Some(upto))?;
                let fetched_at = now_iso();
                let balances = api.balances_by_ids(contract, &unknown, Some(upto))?;
                if let Some(last) = tokens.iter().map(|t| t.id).max() {
                    state.incr_token_id = last;
                }
                let done = n < page;
                let batch = Batch {
                    contract: contract.to_owned(),
                    mints,
                    tokens,
                    balances,
                    complete_for: unknown.into_iter().collect(),
                    fetched_at,
                };
                let next = done.then_some(PHASE_BALANCES);
                write_page(
                    store,
                    &batch,
                    contract,
                    Some(("incr_token_id", state.incr_token_id)),
                    next,
                    report,
                )?;
                if done {
                    state.incr_phase = PHASE_BALANCES;
                }
            }
            PHASE_BALANCES => {
                if !spend(budget, report) {
                    return Ok(false);
                }
                let fetched_at = now_iso();
                let balances =
                    api.balances_changed(contract, (since, upto), state.incr_balance_id, page)?;
                let n = balances.len();
                if let Some(last) = balances.iter().map(|b| b.id).max() {
                    state.incr_balance_id = last;
                }
                let done = n < page;
                let batch = Batch {
                    contract: contract.to_owned(),
                    mints: Vec::new(),
                    tokens: Vec::new(),
                    balances,
                    complete_for: BTreeSet::new(),
                    fetched_at,
                };
                let next = done.then_some(PHASE_RETRY);
                write_page(
                    store,
                    &batch,
                    contract,
                    Some(("incr_balance_id", state.incr_balance_id)),
                    next,
                    report,
                )?;
                if done {
                    state.incr_phase = PHASE_RETRY;
                }
            }
            _ => {
                // Tokens skipped earlier for reasons a later pass may resolve (their lastLevel
                // may never move): one bounded page after the persisted token-id cursor, so
                // every open issue gets one attempt per pass, however short the runs. An
                // unresolved entry stays open for the next pass, which starts over at 0.
                let ids: Vec<u64> = {
                    let c = store.conn_mut();
                    let mut st = c
                        .prepare_cached(&format!(
                            "SELECT DISTINCT token_id FROM ingest_issues WHERE contract = ?1 \
                             AND kind IN {RETRIED} AND token_id >= ?2 ORDER BY token_id LIMIT ?3"
                        ))
                        .db()?;
                    st.query_map(
                        params![contract, to_i64(state.incr_retry_after)?, page as i64],
                        |r| r.get::<_, i64>(0),
                    )
                    .db()?
                    .map(|r| r.map(|v| v as u64))
                    .collect::<rusqlite::Result<_>>()
                    .db()?
                };
                let Some(&last) = ids.last() else {
                    store
                        .conn_mut()
                        .execute(
                            "UPDATE sync_state SET incr_phase = ?2 WHERE contract = ?1",
                            params![contract, PHASE_DONE],
                        )
                        .db()?;
                    state.incr_phase = PHASE_DONE;
                    continue;
                };
                if !spend(budget, report) {
                    return Ok(false);
                }
                let tokens = api.tokens_by_ids(contract, &ids, Some(upto))?;
                let mints = api.mints_by_ids(contract, &ids, Some(upto))?;
                let fetched_at = now_iso();
                let balances = api.balances_by_ids(contract, &ids, Some(upto))?;
                let done = ids.len() < page;
                let batch = Batch {
                    contract: contract.to_owned(),
                    mints,
                    complete_for: tokens.iter().map(|t| t.token_id).collect(),
                    tokens,
                    balances,
                    fetched_at,
                };
                // The cursor is the next token id to read (inclusive), so token 0 is reachable.
                let resume = last.checked_add(1);
                let next = (done || resume.is_none()).then_some(PHASE_DONE);
                let resume = resume.unwrap_or(last);
                write_page(
                    store,
                    &batch,
                    contract,
                    Some(("incr_retry_after", resume)),
                    next,
                    report,
                )?;
                state.incr_retry_after = resume;
                if done || next.is_some() {
                    state.incr_phase = PHASE_DONE;
                }
            }
        }
    }
    let now = now_iso();
    let tx = store
        .conn_mut()
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .db()?;
    tx.execute(
        "UPDATE sync_state SET synced_level = incr_level, synced_at = ?2, incr_level = NULL, \
         incr_mint_id = 0, incr_token_id = 0, incr_balance_id = 0, incr_phase = 0, \
         incr_retry_after = 0, updated_at = ?2 WHERE contract = ?1",
        params![contract, now],
    )
    .db()?;
    mark_snapshot(&tx, &now)?;
    tx.commit().db()?;
    Ok(true)
}

/// Writes an incremental page, advancing `cursor` (if any) and the phase (when `next` is
/// given) in the same transaction.
fn write_page(
    store: &mut Store,
    batch: &Batch,
    contract: &str,
    cursor: Option<(&'static str, u64)>,
    next: Option<u8>,
    report: &mut SyncReport,
) -> Result<(), IngestError> {
    let tx = store
        .conn_mut()
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .db()?;
    let stats = apply(&tx, batch)?;
    if let Some((cursor, value)) = cursor {
        tx.execute(
            &format!("UPDATE sync_state SET {cursor} = ?2, updated_at = ?3 WHERE contract = ?1"),
            params![contract, to_i64(value)?, now_iso()],
        )
        .db()?;
    }
    if let Some(phase) = next {
        tx.execute(
            "UPDATE sync_state SET incr_phase = ?2 WHERE contract = ?1",
            params![contract, phase],
        )
        .db()?;
    }
    tx.commit().db()?;
    report.merge(&stats);
    Ok(())
}

/// SQLite integers are i64; larger chain values are rejected, never wrapped.
pub(crate) fn to_i64(v: u64) -> Result<i64, StoreError> {
    i64::try_from(v).map_err(|_| StoreError::BadRequest(format!("value {v} exceeds 2^63")))
}

/// Syncs the selected contracts: pending full passes first, then incremental passes.
pub fn sync(
    store: &mut Store,
    api: &Tzkt,
    opts: &SyncOptions,
    log: &(dyn Fn(&str) + Sync),
) -> Result<SyncReport, IngestError> {
    let contracts: Vec<String> = if opts.contracts.is_empty() {
        TOKEN_CONTRACTS.iter().map(|c| (*c).to_owned()).collect()
    } else {
        opts.contracts.clone()
    };
    if let Some(bad) = contracts
        .iter()
        .find(|c| !TOKEN_CONTRACTS.contains(&c.as_str()))
    {
        return Err(IngestError::Usage(format!(
            "{bad} is not an fxhash token contract"
        )));
    }
    if !(1..=super::tzkt::MAX_PAGE as u64).contains(&opts.batch) {
        return Err(IngestError::Usage("batch must be 1..=10000".into()));
    }
    let projects: i64 = store
        .conn_mut()
        .query_row("SELECT count(*) FROM projects", [], |r| r.get(0))
        .db()?;
    if projects == 0 {
        return Err(IngestError::Usage(
            "no projects: run bootstrap first".into(),
        ));
    }
    escrow::register(store.conn_mut())?;
    let mut report = SyncReport::default();
    let mut budget = opts.limit;

    let mut per_contract: Vec<Vec<Job>> = Vec::new();
    for c in &contracts {
        if let Some(jobs) = full_pass_jobs(store, api, c, opts.batch)? {
            log(&format!("{c}: full pass, {} chunks to fetch", jobs.len()));
            per_contract.push(jobs);
        }
    }
    // Interleave contracts so every worker stays busy and each contract advances.
    let mut jobs = Vec::new();
    let longest = per_contract.iter().map(Vec::len).max().unwrap_or(0);
    for i in 0..longest {
        for list in &per_contract {
            if let Some(j) = list.get(i) {
                jobs.push(j.clone());
            }
        }
    }
    if let Some(n) = budget {
        if jobs.len() > n {
            jobs.truncate(n);
            report.limited = true;
        }
        budget = Some(n - jobs.len());
    }
    if !jobs.is_empty() {
        run_jobs(store, api, jobs, opts.concurrency, &mut report, log)?;
    }
    report.interrupted |= api.client.stopped();
    for c in &contracts {
        if finish_full_pass(store, c)? {
            log(&format!("{c}: full pass complete"));
            report.completed.push(c.clone());
        }
    }
    if !report.interrupted && report.transient_failures == 0 {
        // Passes in progress first, then the stalest contract, so a run limit never starves
        // the contracts after the first one.
        let mut order: Vec<(bool, u64, String, String)> = Vec::new();
        for c in &contracts {
            let (synced_at, state): (Option<String>, State) = (
                store
                    .conn_mut()
                    .query_row(
                        "SELECT synced_at FROM sync_state WHERE contract = ?1",
                        [c],
                        |r| r.get(0),
                    )
                    .optional()
                    .db()?
                    .flatten(),
                load_state(store.conn_mut(), c)?.unwrap_or_default(),
            );
            if state.synced_level.is_none() || state.full_level.is_some() {
                continue;
            }
            order.push((
                state.incr_level.is_none(),
                state.synced_level.unwrap_or(0),
                synced_at.unwrap_or_default(),
                c.clone(),
            ));
        }
        order.sort();
        for (_, _, _, c) in &order {
            match incremental(store, api, c, opts.batch as usize, &mut budget, &mut report) {
                Ok(true) => {
                    log(&format!("{c}: incremental pass complete"));
                    if !report.completed.contains(c) {
                        report.completed.push(c.clone());
                    }
                }
                Ok(false) => break,
                Err(IngestError::Api(ApiError::Interrupted)) => {
                    report.interrupted = true;
                    break;
                }
                Err(e) if e.is_transient() => {
                    log(&format!("{c}: incremental pass paused: {e}"));
                    report.transient_failures += 1;
                    break;
                }
                Err(e) => return Err(e),
            }
        }
    }
    for c in &contracts {
        let state = load_state(store.conn_mut(), c)?.unwrap_or_default();
        if state.synced_level.is_none() || state.full_level.is_some() || state.incr_level.is_some()
        {
            report.pending.push(c.clone());
        }
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_aligns_chunks_to_the_batch_grid() {
        let jobs = plan("KT1x", 589_146, 590_200, 500, 1).unwrap();
        let ranges: Vec<(u64, u64)> = jobs.iter().map(|j| (j.lo, j.hi)).collect();
        assert_eq!(
            ranges,
            vec![(589_146, 589_500), (589_500, 590_000), (590_000, 590_200)]
        );
        assert!(plan("KT1x", 0, 0, 500, 1).unwrap().is_empty());
        assert!(
            plan("KT1x", 0, 10_000_000, 1, 1).is_err(),
            "too many chunks"
        );
        assert!(plan("KT1x", 5, 1, 500, 1).is_err());
    }
}
