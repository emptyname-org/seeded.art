//! `seeded-index upgrade`: brings a database to the current schema and rebuilds what can be
//! derived from stored data alone. No network: escrow sellers come from `seeded-index escrow`.

use std::collections::BTreeMap;

use rusqlite::TransactionBehavior;

use rusqlite::{Connection, params};

use super::identity::IdentityReport;
use super::sync::enqueue_catalogue_candidates;
use super::tzkt::Tzkt;
use super::{IngestError, TOKEN_CONTRACTS, escrow, identity, now_iso, verify};
use crate::store::{Db, NamesNumbered, Store, backfill_content_roots, search};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReconcileReport {
    /// Catalogue tokens without a token row or issue, now queued for retry.
    pub catalogue_candidates: usize,
    /// Mints below or above a synced contract's known token ids, now queued for retry.
    pub boundary_candidates: usize,
}

/// Local part of the reconcile: catalogue records with no token row and no issue (and not
/// confirmed absent) become retry work. Idempotent.
pub fn reconcile(c: &Connection) -> Result<ReconcileReport, IngestError> {
    Ok(ReconcileReport {
        catalogue_candidates: enqueue_catalogue_candidates(c, None)?,
        boundary_candidates: 0,
    })
}

/// `seeded-index reconcile`: the local part, then, per fully synced contract, the mints
/// outside its known token ids (two bounded TzKT queries each, no rescan). Every candidate
/// without a token row or issue becomes retry work that the next `sync` fetches by id.
pub fn reconcile_store(store: &mut Store, api: &Tzkt) -> Result<ReconcileReport, IngestError> {
    let mut report = {
        let tx = store
            .conn_mut()
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .db()?;
        let r = reconcile(&tx)?;
        tx.commit().db()?;
        r
    };
    for contract in TOKEN_CONTRACTS {
        // Separate min/max subqueries so each is one primary-key seek.
        let known: (Option<i64>, Option<i64>, bool) = store
            .conn_mut()
            .query_row(
                "SELECT min((SELECT min(token_id) FROM tokens WHERE contract = ?1), \
                 coalesce((SELECT min(token_id) FROM ingest_issues WHERE contract = ?1), \
                 9223372036854775807)), \
                 max((SELECT max(token_id) FROM tokens WHERE contract = ?1), \
                 coalesce((SELECT max(token_id) FROM ingest_issues WHERE contract = ?1), -1)), \
                 EXISTS (SELECT 1 FROM sync_state WHERE contract = ?1 \
                 AND synced_level IS NOT NULL)",
                [contract],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .db()?;
        let (Some(lo), Some(hi), true) = known else {
            continue;
        };
        let mints = api.mints_outside(contract, lo as u64, hi as u64)?;
        let tx = store
            .conn_mut()
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .db()?;
        {
            let mut ins = tx
                .prepare_cached(
                    "INSERT OR IGNORE INTO ingest_issues (contract, token_id, kind, detail, \
                     level, seen_at) SELECT ?1, ?2, 'token_missing', \
                     'mint outside the indexed token ids', ?3, ?4 WHERE NOT EXISTS \
                     (SELECT 1 FROM tokens WHERE contract = ?1 AND token_id = ?2) \
                     AND NOT EXISTS (SELECT 1 FROM ingest_issues WHERE contract = ?1 \
                     AND token_id = ?2)",
                )
                .db()?;
            let now = now_iso();
            for m in &mints {
                report.boundary_candidates += ins
                    .execute(params![contract, m.token_id as i64, m.level as i64, now])
                    .db()?;
            }
        }
        tx.commit().db()?;
    }
    Ok(report)
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UpgradeReport {
    pub schema_version: i64,
    /// Rows in `content_roots` after the upgrade.
    pub content_roots: u64,
    /// Unverified catalogue rows re-compared with stored chain tokens, by resulting state.
    pub recompared: BTreeMap<String, u64>,
    pub identities: IdentityReport,
    pub names: NamesNumbered,
    pub reconcile: ReconcileReport,
}

/// The schema migration runs when the store is opened; this then rebuilds content roots,
/// re-compares verification, applies curated identities, reconciles missing tokens into
/// retry work and rebuilds the search indexes' shown names, in one transaction.
pub fn upgrade(store: &mut Store) -> Result<UpgradeReport, IngestError> {
    let tx = store
        .conn_mut()
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .db()?;
    escrow::register(&tx)?;
    escrow::resolve_known(&tx)?;
    backfill_content_roots(&tx)?;
    let recompared = verify::recompare_all(&tx)?;
    let identities = identity::apply(&tx)?;
    let reconcile = reconcile(&tx)?;
    let names = crate::store::assign_names(&tx)?;
    search::rebuild(&tx)?;
    let schema_version: i64 = tx
        .pragma_query_value(None, "user_version", |r| r.get(0))
        .db()?;
    let content_roots: i64 = tx
        .query_row("SELECT count(*) FROM content_roots", [], |r| r.get(0))
        .db()?;
    tx.commit().db()?;
    Ok(UpgradeReport {
        schema_version,
        content_roots: content_roots as u64,
        recompared,
        identities,
        names,
        reconcile,
    })
}
