//! Operational statistics: row counts, file and per-table sizes, sync state.

use std::fmt::Write as _;
use std::fs;
use std::path::Path;

use rusqlite::Connection;

use super::IngestError;
use crate::store::{Db, Store, StoreError};

fn rows(c: &Connection, sql: &str) -> Result<Vec<(String, i64)>, StoreError> {
    let mut st = c.prepare(sql).db()?;
    st.query_map([], |r| {
        Ok((
            r.get::<_, Option<String>>(0)?
                .unwrap_or_else(|| "(null)".into()),
            r.get(1)?,
        ))
    })
    .db()?
    .collect::<rusqlite::Result<_>>()
    .db()
}

fn one(c: &Connection, sql: &str) -> Result<i64, StoreError> {
    c.query_row(sql, [], |r| r.get(0)).db()
}

fn mib(bytes: u64) -> String {
    format!("{:.1} MiB", bytes as f64 / (1 << 20) as f64)
}

fn file_size(path: &Path, suffix: &str) -> u64 {
    let mut p = path.as_os_str().to_owned();
    p.push(suffix);
    fs::metadata(p).map_or(0, |m| m.len())
}

/// Imported names (case-insensitive) that more than one address carries, and those addresses:
/// the names profiles never show.
pub const SHARED_NAMES: &str = "WITH c AS (\
     SELECT name_key AS k, address AS a FROM users WHERE name_key IS NOT NULL \
     UNION SELECT name_key, address FROM artists WHERE address IS NOT NULL AND name <> address \
     UNION SELECT name_key, address FROM profiles_imported WHERE name_key IS NOT NULL), \
     s AS (SELECT k FROM c GROUP BY k HAVING count(DISTINCT a) > 1) \
     SELECT (SELECT count(*) FROM s), \
     (SELECT count(DISTINCT a) FROM c WHERE k IN (SELECT k FROM s))";

/// A plain-text report for `seeded-index stats`.
pub fn stats(store: &Store, db: &Path) -> Result<String, IngestError> {
    let status = store.index_status()?;
    let c = store.conn();
    let mut out = String::new();
    let w = &mut out;
    let main = file_size(db, "");
    let wal = file_size(db, "-wal");
    let page_size = one(c, "PRAGMA page_size")?;
    let pages = one(c, "PRAGMA page_count")?;
    let free = one(c, "PRAGMA freelist_count")?;
    let _ = writeln!(w, "database {}", db.display());
    let _ = writeln!(
        w,
        "  file {} ({main} bytes), wal {}, pages {pages} x {page_size} B, free pages {free}",
        mib(main),
        mib(wal)
    );
    let _ = writeln!(w, "rows");
    for (label, sql) in [
        ("projects", "SELECT count(*) FROM projects"),
        (
            "projects hidden (fxhash flag)",
            "SELECT count(*) FROM projects WHERE hidden = 1",
        ),
        (
            "projects indexed",
            "SELECT count(*) FROM projects WHERE index_state = 'indexed'",
        ),
        ("artists", "SELECT count(*) FROM artists"),
        ("project_artists", "SELECT count(*) FROM project_artists"),
        ("users", "SELECT count(*) FROM users"),
        (
            "collaboration contracts",
            "SELECT count(DISTINCT contract) FROM collaborations",
        ),
        ("tokens", "SELECT count(*) FROM tokens"),
        (
            "tokens with owner",
            "SELECT count(*) FROM tokens WHERE owner_address IS NOT NULL",
        ),
        (
            "token_features (fixture rows)",
            "SELECT count(*) FROM token_features",
        ),
        ("catalogue_tokens", "SELECT count(*) FROM catalogue_tokens"),
        ("ingest_issues", "SELECT count(*) FROM ingest_issues"),
    ] {
        let _ = writeln!(w, "  {label:<32} {}", one(c, sql)?);
    }
    let _ = writeln!(w, "tokens by contract and source");
    for (k, n) in rows(
        c,
        "SELECT contract || ' ' || source, count(*) FROM tokens GROUP BY contract, source",
    )? {
        let _ = writeln!(w, "  {k:<44} {n}");
    }
    let _ = writeln!(w, "catalogue tokens by contract");
    for (k, n) in rows(
        c,
        "SELECT contract, count(*) FROM catalogue_tokens GROUP BY contract",
    )? {
        let _ = writeln!(w, "  {k:<44} {n}");
    }
    let _ = writeln!(w, "catalogue verification");
    for (k, n) in rows(
        c,
        "SELECT verify_state || ' / chain ' || chain_state, count(*) FROM catalogue_tokens \
         GROUP BY verify_state, chain_state",
    )? {
        let _ = writeln!(w, "  {k:<44} {n}");
    }
    let _ = writeln!(w, "mismatch details");
    for (k, n) in rows(
        c,
        "SELECT verify_detail, count(*) FROM catalogue_tokens WHERE verify_state = 'mismatch' \
         GROUP BY verify_detail ORDER BY 2 DESC LIMIT 20",
    )? {
        let _ = writeln!(w, "  {k:<44} {n}");
    }
    let _ = writeln!(w, "ingest issues");
    for (k, n) in rows(
        c,
        "SELECT contract || ' ' || kind, count(*) FROM ingest_issues GROUP BY contract, kind",
    )? {
        let _ = writeln!(w, "  {k:<44} {n}");
    }
    let _ = writeln!(w, "known addresses (marketplace escrow, burn)");
    for (k, n) in rows(
        c,
        "SELECT e.name || ': listed by a resolved seller', count(*) FROM tokens t \
         JOIN known_addresses e ON e.address = t.owner_address AND e.kind = 'escrow' \
         WHERE t.listed_by IS NOT NULL GROUP BY e.name \
         UNION ALL SELECT e.name || CASE WHEN e.kind = 'burn' THEN ': burned, no collector' \
         ELSE ': unresolved, shown as escrow' END, count(*) FROM tokens t \
         JOIN known_addresses e ON e.address = t.owner_address \
         WHERE t.listed_by IS NULL GROUP BY e.name, e.kind \
         UNION ALL SELECT e.name || ': live listings stored', count(*) FROM escrow_listings l \
         JOIN known_addresses e ON e.address = l.escrow GROUP BY e.name",
    )? {
        let _ = writeln!(w, "  {k:<52} {n}");
    }
    let _ = writeln!(
        w,
        "  distinct collectors (holder_address)             {}",
        one(c, "SELECT count(DISTINCT holder_address) FROM tokens")?
    );
    let mut st = c
        .prepare(
            "SELECT escrow, synced_level, synced_at, pass_level, cursor_id FROM escrow_state \
             ORDER BY escrow",
        )
        .db()?;
    let states = st
        .query_map([], |r| {
            Ok(format!(
                "  {} synced_level {:?} at {:?}; pass level {:?}, cursor {}",
                r.get::<_, String>(0)?,
                r.get::<_, Option<i64>>(1)?,
                r.get::<_, Option<String>>(2)?,
                r.get::<_, Option<i64>>(3)?,
                r.get::<_, i64>(4)?,
            ))
        })
        .db()?
        .collect::<rusqlite::Result<Vec<_>>>()
        .db()?;
    for s in states {
        let _ = writeln!(w, "{s}");
    }
    let _ = writeln!(w, "sync state");
    let mut st = c
        .prepare(
            "SELECT s.contract, s.synced_level, s.synced_at, s.full_level, \
             (SELECT count(*) FROM sync_chunks k WHERE k.contract = s.contract), s.incr_level \
             FROM sync_state s ORDER BY s.contract",
        )
        .db()?;
    let states = st
        .query_map([], |r| {
            Ok(format!(
                "  {} synced_level {:?} at {:?}; full pass level {:?}, chunks done {}; \
                 incremental level {:?}",
                r.get::<_, String>(0)?,
                r.get::<_, Option<i64>>(1)?,
                r.get::<_, Option<String>>(2)?,
                r.get::<_, Option<i64>>(3)?,
                r.get::<_, i64>(4)?,
                r.get::<_, Option<i64>>(5)?,
            ))
        })
        .db()?
        .collect::<rusqlite::Result<Vec<_>>>()
        .db()?;
    for s in states {
        let _ = writeln!(w, "{s}");
    }
    let _ = writeln!(w, "profiles");
    for (k, n) in rows(
        c,
        "SELECT source || ' imported rows', count(*) FROM profiles_imported GROUP BY source \
         UNION ALL SELECT 'fxhash documents ' || state, count(*) FROM profile_fetch \
         GROUP BY state UNION ALL SELECT 'local edits', count(*) FROM profile_edits",
    )? {
        let _ = writeln!(w, "  {k:<44} {n}");
    }
    let (names, addresses): (i64, i64) = c
        .query_row(SHARED_NAMES, [], |r| Ok((r.get(0)?, r.get(1)?)))
        .db()?;
    let _ = writeln!(
        w,
        "  imported names shared by several addresses     {names} names, {addresses} addresses"
    );
    let _ = writeln!(
        w,
        "index status: {} visible projects ({} indexed, {} hidden), {} visible tokens \
         ({} chain match, {} mismatch)",
        status.projects_total,
        status.projects_indexed,
        status.projects_flagged_hidden,
        status.tokens_total,
        status.tokens_verified,
        status.tokens_mismatch
    );
    let _ = writeln!(w, "size by table and index (dbstat)");
    match rows(
        c,
        "SELECT name, sum(pgsize) FROM dbstat GROUP BY name ORDER BY 2 DESC LIMIT 30",
    ) {
        Ok(list) => {
            for (k, n) in list {
                let _ = writeln!(w, "  {k:<44} {}", mib(n as u64));
            }
        }
        Err(e) => {
            let _ = writeln!(w, "  unavailable: {e}");
        }
    }
    Ok(out)
}
