//! Per-project trading volume from the chain-derived sales database.
//!
//! Volume is every trade of a project's tokens priced in tez or wXTZ (1:1): primary mints
//! plus secondary sales, offer accepts, auctions and other paid market moves. Trades paid in
//! another token have no price; they are counted apart (`unpriced_count`). A project has a
//! row only when it has at least one trade; reads treat a missing row as zero volume and skip
//! flagged projects, as every public read does.
//!
//! A sale belongs to the project of its token: the chain token row (`tokens`), else the
//! catalogue row (`catalogue_tokens`) of a token minted but never signed, which has no chain
//! row yet was paid for and traded. Sales of tokens the archive has neither for are counted
//! as unmapped.
//!
//! [`Store::refresh_volumes`] (`seeded-index volumes`) attaches the sales file read-only and
//! aggregates in SQL, reading tokens in project order so nothing is sorted or held per token,
//! then adds the catalogue-only tokens' sales; memory is bounded by the number of projects. It
//! replaces the table in one short transaction, so a re-run is idempotent and readers never
//! see a partial table.
//!
//! [`ProjectSort::VolumeDesc`](super::ProjectSort::VolumeDesc) lists in two parts: ranked
//! projects (volume above zero) along `project_volume_rank`, then the rest by id, an ordered
//! primary-key walk that skips ranked projects. A cursor holds `(volume, id)`; a zero volume
//! resumes in the second part.

use std::collections::BTreeMap;
use std::path::Path;

use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use rusqlite::{Connection, OptionalExtension, ToSql, TransactionBehavior, params};
use serde::Serialize;
use serde_json::Value;

use super::{
    Db, PageRequest, SUMMARY_COLS, Store, StoreError, count, finish_summaries, get_opt_u64,
    get_u64, not_found, sql_id, summary_rows,
};
use crate::model::{ProjectSummary, ProjectVolume};

/// v8 -> v9, additive. `sales_count` counts priced trades (free mints included),
/// `unpriced_count` the others; a row has at least one of either. Re-running it (a hand
/// rollback of `user_version`) keeps the table.
pub(crate) const SCHEMA_V9: &str = "
CREATE TABLE IF NOT EXISTS project_volume (
  project_id INTEGER PRIMARY KEY,
  volume_mutez INTEGER NOT NULL CHECK (volume_mutez >= 0),
  primary_mutez INTEGER NOT NULL CHECK (primary_mutez >= 0),
  secondary_mutez INTEGER NOT NULL CHECK (secondary_mutez >= 0),
  sales_count INTEGER NOT NULL CHECK (sales_count >= 0),
  unpriced_count INTEGER NOT NULL CHECK (unpriced_count >= 0),
  computed_at TEXT NOT NULL,
  sales_level INTEGER CHECK (sales_level IS NULL OR sales_level > 0),
  CHECK (volume_mutez = primary_mutez + secondary_mutez),
  CHECK (sales_count + unpriced_count > 0)
) STRICT;
CREATE INDEX IF NOT EXISTS project_volume_rank ON project_volume (volume_mutez DESC, project_id);
";

/// Schema name the sales file is attached under while volumes are computed.
const SALES: &str = "volume_sales";

/// One row per project with trades, in project order: the `UNIQUE (project_id, iteration)`
/// index of `tokens` drives the loop, each token probes the sales `(contract, token_id, op_id)`
/// index, and the grouping needs no sort (`aggregation_reads_tokens_in_project_order`).
/// `sum` fails on integer overflow rather than wrapping.
pub(crate) const AGGREGATE: &str = "SELECT t.project_id, \
     (SELECT hidden FROM main.projects WHERE id = t.project_id), \
     coalesce(sum(CASE WHEN x.kind = 'mint' AND x.currency IN ('tez', 'wxtz') \
       THEN x.price_mutez END), 0), \
     coalesce(sum(CASE WHEN x.kind <> 'mint' AND x.currency IN ('tez', 'wxtz') \
       THEN x.price_mutez END), 0), \
     count(CASE WHEN x.currency IN ('tez', 'wxtz') THEN x.price_mutez END), \
     count(*) - count(CASE WHEN x.currency IN ('tez', 'wxtz') THEN x.price_mutez END) \
     FROM main.tokens t INDEXED BY sqlite_autoindex_tokens_2 \
     CROSS JOIN volume_sales.sales x ON x.contract = t.contract AND x.token_id = t.token_id \
     GROUP BY t.project_id";

/// Sales of catalogued tokens without a chain row (unsigned mints), one row per sale, with the
/// project's flag (NULL if the project is unknown). The open-catalogue partial index holds
/// every catalogue row not matched to a chain token; each probes `tokens` and the sales index.
pub(crate) const CATALOGUE_ONLY: &str = "SELECT ct.project_id, \
     (SELECT hidden FROM main.projects WHERE id = ct.project_id), \
     x.kind = 'mint', CASE WHEN x.currency IN ('tez', 'wxtz') THEN x.price_mutez END \
     FROM main.catalogue_tokens ct INDEXED BY catalogue_tokens_open \
     CROSS JOIN volume_sales.sales x ON x.contract = ct.contract AND x.token_id = ct.token_id \
     WHERE ct.verify_state <> 'match' AND NOT EXISTS (SELECT 1 FROM main.tokens t \
       WHERE t.contract = ct.contract AND t.token_id = ct.token_id)";

/// Every sales row, for what the archive could not map to a token (one pass over `sales`).
const SALES_TOTALS: &str = "SELECT count(*), \
     coalesce(sum(CASE WHEN currency IN ('tez', 'wxtz') THEN price_mutez END), 0) \
     FROM volume_sales.sales";

/// The level every stream completed, or NULL if one never completed a pass.
const SALES_LEVEL: &str = "SELECT CASE WHEN count(*) > 0 AND count(*) = count(synced_level) \
     THEN min(synced_level) END FROM volume_sales.cursors";

const INSERT: &str = "INSERT INTO project_volume (project_id, volume_mutez, primary_mutez, \
     secondary_mutez, sales_count, unpriced_count, computed_at, sales_level) \
     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)";

const READ: &str = "SELECT p.id, v.volume_mutez, v.primary_mutez, v.secondary_mutez, \
     v.sales_count, v.unpriced_count, v.computed_at, v.sales_level FROM projects p \
     LEFT JOIN project_volume v ON v.project_id = p.id WHERE p.id = ?1 AND p.hidden = 0";

/// `ORDER BY` of the ranked part.
pub(crate) const RANKED_ORDER: &str = "v.volume_mutez DESC, v.project_id";
/// Cursor key columns of both parts: `(volume, id)`.
pub(crate) const RANKED_KEY: &str = "v.volume_mutez, p.id";
const UNRANKED_KEY: &str = "0, p.id";
/// After `(:k0, :k1)` in the ranked order: a range of `project_volume_rank`.
const RANKED_AFTER: &str =
    " AND v.volume_mutez <= :k0 AND (v.volume_mutez < :k0 OR v.project_id > :k1)";
/// Unranked part from `:after` (exclusive; -1 from its start): a primary-key range.
const UNRANKED_AFTER: &str = " AND p.id > :after";

/// Ranked projects: visible, volume above zero. `filters` follows `p.hidden = 0`.
pub(crate) fn ranked_from(filters: &str) -> String {
    format!(
        "FROM project_volume v JOIN projects p ON p.id = v.project_id \
         WHERE v.volume_mutez > 0 AND p.hidden = 0{filters}"
    )
}

/// Visible projects without volume: no row, or a zero one.
pub(crate) fn unranked_from(filters: &str) -> String {
    format!(
        "FROM projects p WHERE p.hidden = 0{filters} AND NOT EXISTS (SELECT 1 FROM \
         project_volume v WHERE v.project_id = p.id AND v.volume_mutez > 0)"
    )
}

/// The filters of a listing `from` (`FROM projects p WHERE p.hidden = 0...`).
fn filters_of(from: &str) -> Result<&str, StoreError> {
    from.strip_prefix("FROM projects p WHERE p.hidden = 0")
        .ok_or_else(|| StoreError::Internal("unexpected project listing".into()))
}

pub(crate) fn ranked_sql(filters: &str, after: bool) -> String {
    format!(
        "SELECT {SUMMARY_COLS}, {RANKED_KEY} {}{} ORDER BY {RANKED_ORDER} LIMIT :limit",
        ranked_from(filters),
        if after { RANKED_AFTER } else { "" }
    )
}

pub(crate) fn unranked_sql(filters: &str) -> String {
    format!(
        "SELECT {SUMMARY_COLS}, {UNRANKED_KEY} {}{UNRANKED_AFTER} ORDER BY p.id LIMIT :limit",
        unranked_from(filters)
    )
}

/// Keyset page of [`ProjectSort::VolumeDesc`](super::ProjectSort::VolumeDesc): up to `limit`
/// summaries after `key` (`[volume, id]`), and the key of the last one when more follow.
pub(crate) fn volume_keyset(
    c: &Connection,
    from: &str,
    params: &[(&str, &dyn ToSql)],
    key: Option<&[Value]>,
    limit: u32,
) -> Result<(Vec<ProjectSummary>, Option<Vec<Value>>), StoreError> {
    let filters = filters_of(from)?;
    let key = match key {
        None => None,
        Some([v, id]) => match (v.as_i64(), id.as_i64()) {
            (Some(v), Some(id)) if v >= 0 => Some((v, id)),
            _ => return Err(super::bad_cursor("key shape")),
        },
        Some(_) => return Err(super::bad_cursor("key shape")),
    };
    let fetch = limit + 1;
    let mut rows = Vec::new();
    // A zero volume resumes in the unranked part.
    if key.is_none_or(|(v, _)| v > 0) {
        let mut all = params.to_vec();
        if let Some((v, id)) = &key {
            all.push((":k0", v));
            all.push((":k1", id));
        }
        all.push((":limit", &fetch));
        rows = summary_rows(c, &ranked_sql(filters, key.is_some()), &all, 2)?;
    }
    if rows.len() <= limit as usize {
        let after = match key {
            Some((0, id)) => id,
            _ => -1,
        };
        let rest = fetch - rows.len() as u32;
        let mut all = params.to_vec();
        all.push((":after", &after));
        all.push((":limit", &rest));
        rows.extend(summary_rows(c, &unranked_sql(filters), &all, 2)?);
    }
    let more = rows.len() > limit as usize;
    rows.truncate(limit as usize);
    let next = if more {
        rows.last().map(|(_, k)| k.clone())
    } else {
        None
    };
    Ok((finish_summaries(c, rows.into_iter().map(|(r, _)| r))?, next))
}

/// Offset page of [`ProjectSort::VolumeDesc`](super::ProjectSort::VolumeDesc), in the same
/// order as [`volume_keyset`].
pub(crate) fn volume_page(
    c: &Connection,
    from: &str,
    params: &[(&str, &dyn ToSql)],
    page: PageRequest,
) -> Result<super::Page<ProjectSummary>, StoreError> {
    let filters = filters_of(from)?;
    let total = count(c, from, params)?;
    let ranked = count(c, &ranked_from(filters), params)?;
    let offset = u64::from(page.offset);
    let mut rows = Vec::new();
    if offset < ranked {
        let sql = format!(
            "SELECT {SUMMARY_COLS} {} ORDER BY {RANKED_ORDER} LIMIT :limit OFFSET :offset",
            ranked_from(filters)
        );
        let mut all = params.to_vec();
        all.push((":limit", &page.limit));
        all.push((":offset", &page.offset));
        rows = summary_rows(c, &sql, &all, 0)?;
    }
    let rest = page.limit - rows.len() as u32;
    if rest > 0 {
        let skip = i64::try_from(offset.saturating_sub(ranked)).map_err(|_| overflow())?;
        let sql = format!(
            "SELECT {SUMMARY_COLS} {} ORDER BY p.id LIMIT :limit OFFSET :offset",
            unranked_from(filters)
        );
        let mut all = params.to_vec();
        all.push((":limit", &rest));
        all.push((":offset", &skip));
        rows.extend(summary_rows(c, &sql, &all, 0)?);
    }
    Ok(super::Page {
        items: finish_summaries(c, rows.into_iter().map(|(r, _)| r))?,
        total,
        offset: page.offset,
        limit: page.limit,
    })
}

/// What one [`Store::refresh_volumes`] wrote and could not map.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct VolumeRefresh {
    /// Rows written: projects with at least one trade, flagged ones included.
    pub projects: u64,
    /// Of those, visible projects.
    pub visible_projects: u64,
    /// Sum over every row, in mutez.
    pub volume_mutez: u64,
    pub primary_mutez: u64,
    pub secondary_mutez: u64,
    /// Sum over visible projects only.
    pub visible_volume_mutez: u64,
    pub sales_count: u64,
    pub unpriced_count: u64,
    /// Sales rows mapped through the catalogue (tokens without a chain row).
    pub catalogue_rows: u64,
    /// Sales rows whose token the archive does not have, and their priced sum.
    pub unmapped_rows: u64,
    pub unmapped_mutez: u64,
    pub sales_level: Option<u64>,
    pub computed_at: String,
}

/// One project's sums before writing.
#[derive(Default)]
struct Aggregate {
    hidden: bool,
    primary: i64,
    secondary: i64,
    priced: i64,
    unpriced: i64,
}

impl Aggregate {
    /// Adds one sale: priced in tez or wXTZ (`price`), or not.
    fn add(&mut self, mint: bool, price: Option<i64>) -> Result<(), StoreError> {
        let add = |a: i64, b: i64| a.checked_add(b).ok_or_else(overflow);
        match price {
            Some(p) if mint => self.primary = add(self.primary, p)?,
            Some(p) => self.secondary = add(self.secondary, p)?,
            None => {}
        }
        match price {
            Some(_) => self.priced = add(self.priced, 1)?,
            None => self.unpriced = add(self.unpriced, 1)?,
        }
        Ok(())
    }

    fn volume(&self) -> Result<i64, StoreError> {
        self.primary
            .checked_add(self.secondary)
            .ok_or_else(overflow)
    }
}

/// Everything one refresh reads: per-project sums, catalogue-mapped rows, all sales rows and
/// their priced sum, and the complete-through level.
struct Read {
    projects: BTreeMap<i64, Aggregate>,
    catalogue_rows: u64,
    sales_rows: u64,
    sales_mutez: u64,
    level: Option<u64>,
}

fn overflow() -> StoreError {
    StoreError::Internal("volume exceeds the i64 range of mutez".into())
}

fn checked(a: u64, b: i64) -> Result<u64, StoreError> {
    u64::try_from(b)
        .ok()
        .and_then(|b| a.checked_add(b))
        .ok_or_else(overflow)
}

/// The sales file at `path`, as a `file:` URI opened read-only.
fn read_only_uri(path: &Path) -> Result<String, StoreError> {
    const KEEP: &AsciiSet = &NON_ALPHANUMERIC
        .remove(b'/')
        .remove(b'.')
        .remove(b'-')
        .remove(b'_')
        .remove(b'~');
    let name = path.to_str().ok_or_else(|| {
        StoreError::BadRequest(format!(
            "sales database path {} is not UTF-8",
            path.display()
        ))
    })?;
    Ok(format!("file:{}?mode=ro", utf8_percent_encode(name, KEEP)))
}

fn sales_error(e: crate::sales::SalesError) -> StoreError {
    use crate::sales::SalesError;
    match e {
        SalesError::Unavailable(m) => StoreError::Unavailable(format!("sales database {m}")),
        SalesError::Db(e) => super::classify(&e),
        other => StoreError::BadRequest(format!("sales database: {other}")),
    }
}

/// Attaches the sales file at `path` read-only as [`SALES`], checking that it is a current
/// sales database; detaches again on any failure.
pub(crate) fn attach_sales(c: &Connection, path: &Path) -> Result<(), StoreError> {
    if !path.is_file() {
        return Err(StoreError::Unavailable(format!(
            "sales database {} does not exist",
            path.display()
        )));
    }
    c.execute("ATTACH DATABASE ?1 AS volume_sales", [read_only_uri(path)?])
        .db()?;
    let checked = match c.is_readonly(SALES) {
        Ok(true) => crate::sales::schema::check(c, SALES).map_err(sales_error),
        Ok(false) => Err(StoreError::Internal(
            "sales database attached writable".into(),
        )),
        Err(e) => Err(super::classify(&e)),
    };
    if checked.is_err() {
        let _ = c.execute("DETACH DATABASE volume_sales", []);
    }
    checked
}

fn aggregate(c: &Connection) -> Result<Read, StoreError> {
    let mut projects = BTreeMap::new();
    let mut st = c.prepare(AGGREGATE).db()?;
    let mut rows = st.query([]).db()?;
    while let Some(r) = rows.next().db()? {
        let hidden: Option<i64> = r.get(1).db()?;
        projects.insert(
            r.get::<_, i64>(0).db()?,
            Aggregate {
                hidden: hidden.unwrap_or(1) != 0,
                primary: r.get(2).db()?,
                secondary: r.get(3).db()?,
                priced: r.get(4).db()?,
                unpriced: r.get(5).db()?,
            },
        );
    }
    let mut catalogue_rows = 0u64;
    let mut st = c.prepare(CATALOGUE_ONLY).db()?;
    let mut rows = st.query([]).db()?;
    while let Some(r) = rows.next().db()? {
        // A catalogue row naming no known project maps nowhere.
        let Some(hidden) = r.get::<_, Option<i64>>(1).db()? else {
            continue;
        };
        let entry = projects
            .entry(r.get::<_, i64>(0).db()?)
            .or_insert_with(|| Aggregate {
                hidden: hidden != 0,
                ..Aggregate::default()
            });
        entry.add(r.get(2).db()?, r.get(3).db()?)?;
        catalogue_rows += 1;
    }
    let (sales_rows, sales_mutez) = c
        .query_row(SALES_TOTALS, [], |r| Ok((get_u64(r, 0)?, get_u64(r, 1)?)))
        .db()?;
    let level = c.query_row(SALES_LEVEL, [], |r| get_opt_u64(r, 0)).db()?;
    Ok(Read {
        projects,
        catalogue_rows,
        sales_rows,
        sales_mutez,
        level,
    })
}

/// A visible project's generator root, for the preservation mirror's project list.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MirrorRoot {
    pub project_id: u64,
    pub root_uri: String,
    pub volume_mutez: u64,
}

const MIRROR_ROOTS: &str = "SELECT p.id, \
     coalesce(nullif(trim(p.generative_uri), ''), nullif(trim(p.artifact_uri), '')), \
     coalesce(v.volume_mutez, 0) \
     FROM projects p LEFT JOIN project_volume v ON v.project_id = p.id \
     WHERE p.hidden = 0 ORDER BY 3 DESC, p.id";

impl Store {
    /// Visible projects with a generator root (`generative_uri`, else `artifact_uri`), most
    /// traded first, then the rest by id. Flagged projects are left out, as in every
    /// public read.
    pub fn mirror_roots(&self) -> Result<Vec<MirrorRoot>, StoreError> {
        self.read(|c| {
            let mut statement = c.prepare(MIRROR_ROOTS).db()?;
            let rows = statement
                .query_map([], |r| {
                    Ok(r.get::<_, Option<String>>(1)?.map(
                        |root_uri| -> rusqlite::Result<MirrorRoot> {
                            Ok(MirrorRoot {
                                project_id: get_u64(r, 0)?,
                                root_uri,
                                volume_mutez: get_u64(r, 2)?,
                            })
                        },
                    ))
                })
                .db()?;
            let mut roots = Vec::new();
            for row in rows {
                if let Some(root) = row.db()? {
                    roots.push(root.db()?);
                }
            }
            Ok(roots)
        })
    }

    /// Volume of a visible project; `None` when it has no trades. NotFound for unknown and
    /// flagged projects, like [`Store::project`].
    pub fn project_volume(&self, id: u64) -> Result<Option<ProjectVolume>, StoreError> {
        let key = sql_id(id).ok_or_else(|| not_found("project", id))?;
        let row = self.read(|c| {
            c.prepare_cached(READ)
                .db()?
                .query_row([key], |r| {
                    let id = get_u64(r, 0)?;
                    if r.get_ref(1)?.data_type() == rusqlite::types::Type::Null {
                        return Ok(None);
                    }
                    Ok(Some(ProjectVolume {
                        project_id: id,
                        volume_mutez: get_u64(r, 1)?,
                        primary_mutez: get_u64(r, 2)?,
                        secondary_mutez: get_u64(r, 3)?,
                        sales_count: get_u64(r, 4)?,
                        unpriced_count: get_u64(r, 5)?,
                        computed_at: r.get(6)?,
                        sales_level: get_opt_u64(r, 7)?,
                    }))
                })
                .optional()
                .db()
        })?;
        row.ok_or_else(|| not_found("project", id))
    }

    /// Recomputes every project's volume from the sales database at `sales`, opened
    /// read-only, and replaces `project_volume` in one immediate transaction. Unavailable if
    /// the file is missing or was never synced; BadRequest if it is not a sales database. On
    /// any error the previous table stays as it was.
    pub fn refresh_volumes(&mut self, sales: &Path) -> Result<VolumeRefresh, StoreError> {
        attach_sales(&self.conn, sales)?;
        let result = self.replace_volumes();
        let detached = self.conn.execute("DETACH DATABASE volume_sales", []).db();
        let report = result?;
        detached?;
        Ok(report)
    }

    fn replace_volumes(&mut self) -> Result<VolumeRefresh, StoreError> {
        // Read in one snapshot of both files, outside the write lock.
        let read = self.read(aggregate)?;
        let level = read.level;
        let computed_at = crate::ingest::now_iso();
        let mut r = VolumeRefresh {
            projects: 0,
            visible_projects: 0,
            volume_mutez: 0,
            primary_mutez: 0,
            secondary_mutez: 0,
            visible_volume_mutez: 0,
            sales_count: 0,
            unpriced_count: 0,
            catalogue_rows: read.catalogue_rows,
            unmapped_rows: 0,
            unmapped_mutez: 0,
            sales_level: level,
            computed_at: computed_at.clone(),
        };
        let level = level
            .map(i64::try_from)
            .transpose()
            .map_err(|_| overflow())?;
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .db()?;
        tx.execute("DELETE FROM project_volume", []).db()?;
        {
            let mut insert = tx.prepare(INSERT).db()?;
            for (project_id, a) in &read.projects {
                let volume = a.volume()?;
                insert
                    .execute(params![
                        project_id,
                        volume,
                        a.primary,
                        a.secondary,
                        a.priced,
                        a.unpriced,
                        computed_at,
                        level
                    ])
                    .db()?;
                r.projects += 1;
                r.volume_mutez = checked(r.volume_mutez, volume)?;
                r.primary_mutez = checked(r.primary_mutez, a.primary)?;
                r.secondary_mutez = checked(r.secondary_mutez, a.secondary)?;
                r.sales_count = checked(r.sales_count, a.priced)?;
                r.unpriced_count = checked(r.unpriced_count, a.unpriced)?;
                if !a.hidden {
                    r.visible_projects += 1;
                    r.visible_volume_mutez = checked(r.visible_volume_mutez, volume)?;
                }
            }
        }
        tx.commit().db()?;
        let mapped = r.sales_count + r.unpriced_count;
        r.unmapped_rows = read.sales_rows.saturating_sub(mapped);
        r.unmapped_mutez = read.sales_mutez.saturating_sub(r.volume_mutez);
        Ok(r)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{ARTIST_FILTER, PROJECT_TEXT_FILTER};

    fn plan(c: &Connection, sql: &str) -> Vec<String> {
        let mut st = c.prepare(&format!("EXPLAIN QUERY PLAN {sql}")).unwrap();
        // Parameters stay unbound (NULL): only the plan matters.
        let mut rows = st.raw_query();
        let mut plan = Vec::new();
        while let Some(r) = rows.next().unwrap() {
            plan.push(r.get::<_, String>(3).unwrap());
        }
        plan
    }

    fn test_dir(name: &str) -> std::path::PathBuf {
        let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tmp/store-unit")
            .join(name);
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn ranked_listing_walks_the_rank_index_without_scanning_or_sorting() {
        let store = Store::open_in_memory().unwrap();
        for after in [false, true] {
            let sql = ranked_sql("", after);
            let plan = plan(&store.conn, &sql);
            assert!(
                plan.iter()
                    .any(|s| s.starts_with("SEARCH v USING") && s.contains("project_volume_rank")),
                "{sql}\n{plan:?}"
            );
            assert!(
                plan.iter()
                    .any(|s| s.starts_with("SEARCH p USING INTEGER PRIMARY KEY")),
                "{sql}\n{plan:?}"
            );
            for step in &plan {
                assert!(!step.starts_with("SCAN"), "{sql}\n{plan:?}");
                assert!(!step.contains("TEMP B-TREE"), "{sql}\n{plan:?}");
            }
        }
    }

    #[test]
    fn unranked_listing_is_an_ordered_primary_key_range() {
        let store = Store::open_in_memory().unwrap();
        let sql = unranked_sql("");
        let plan = plan(&store.conn, &sql);
        assert_eq!(
            plan[0], "SEARCH p USING INTEGER PRIMARY KEY (rowid>?)",
            "{plan:?}"
        );
        assert!(
            plan.iter()
                .any(|s| s == "SEARCH v USING INTEGER PRIMARY KEY (rowid=?)"),
            "{plan:?}"
        );
        for step in &plan {
            assert!(!step.starts_with("SCAN"), "{sql}\n{plan:?}");
            assert!(!step.contains("TEMP B-TREE"), "{sql}\n{plan:?}");
        }
    }

    /// Artist filters probe the artist index and never scan; text filters start from the FTS
    /// match and sort only the matching projects (a temporary B-tree over matches, as for the
    /// other sorts with text).
    #[test]
    fn filtered_volume_listings_start_from_the_filter() {
        let store = Store::open_in_memory().unwrap();
        for sql in [
            ranked_sql(ARTIST_FILTER, false),
            ranked_sql(ARTIST_FILTER, true),
            unranked_sql(ARTIST_FILTER),
        ] {
            let plan = plan(&store.conn, &sql);
            assert!(
                plan.iter().any(|s| s.contains("project_artists_artist")),
                "{sql}\n{plan:?}"
            );
            assert!(
                !plan.iter().any(|s| s.starts_with("SCAN")),
                "{sql}\n{plan:?}"
            );
        }
        for sql in [
            ranked_sql(PROJECT_TEXT_FILTER, false),
            ranked_sql(PROJECT_TEXT_FILTER, true),
            unranked_sql(PROJECT_TEXT_FILTER),
        ] {
            let plan = plan(&store.conn, &sql);
            assert!(
                plan.iter()
                    .any(|s| s.contains("VIRTUAL TABLE INDEX") && s.contains(":M")),
                "{sql}\n{plan:?}"
            );
            for step in &plan {
                let scan = step.starts_with("SCAN") && !step.contains("VIRTUAL TABLE INDEX");
                assert!(!scan, "{sql}\n{plan:?}");
            }
        }
    }

    #[test]
    fn aggregation_reads_tokens_in_project_order() {
        let dir = test_dir("volume_plan");
        let sales = dir.join("sales.sqlite");
        drop(crate::sales::schema::open_writer(&sales).unwrap());
        let store = Store::open_in_memory().unwrap();
        attach_sales(&store.conn, &sales).unwrap();
        assert!(store.conn.is_readonly(SALES).unwrap());
        let plan = plan(&store.conn, AGGREGATE);
        assert!(
            plan[0].starts_with("SCAN t USING") && plan[0].contains("sqlite_autoindex_tokens_2"),
            "{plan:?}"
        );
        assert!(
            plan.iter()
                .any(|s| s.starts_with("SEARCH x USING INDEX sqlite_autoindex_sales_1")),
            "{plan:?}"
        );
        assert!(!plan.iter().any(|s| s.contains("TEMP B-TREE")), "{plan:?}");
        // The partial index holds only catalogue rows not matched to a chain token.
        let plan = super::tests::plan(&store.conn, CATALOGUE_ONLY);
        assert_eq!(
            plan[0], "SCAN ct USING INDEX catalogue_tokens_open",
            "{plan:?}"
        );
        for step in &plan[1..] {
            assert!(!step.starts_with("SCAN"), "{plan:?}");
            assert!(!step.contains("TEMP B-TREE"), "{plan:?}");
        }
        assert!(
            plan.iter()
                .any(|s| s.starts_with("SEARCH t USING PRIMARY KEY")),
            "{plan:?}"
        );
        assert!(
            plan.iter()
                .any(|s| s.starts_with("SEARCH x USING INDEX sqlite_autoindex_sales_1")),
            "{plan:?}"
        );
        store
            .conn
            .execute("DETACH DATABASE volume_sales", [])
            .unwrap();
    }

    #[test]
    fn v8_database_gains_an_empty_volume_table() {
        let mut store = Store::open_in_memory().unwrap();
        // A v8 database is v9 without the additive table.
        store
            .conn
            .execute_batch(
                "DROP TABLE project_volume;
                 PRAGMA user_version = 8;
                 INSERT INTO projects (id, title, title_key, artist_key, index_state)
                   VALUES (7, 'Kept', 'kept', 'a', 'indexed');",
            )
            .unwrap();
        store.migrate().unwrap();
        assert_eq!(super::super::user_version(&store.conn).unwrap(), 10);
        let objects: Vec<String> = store
            .conn
            .prepare(
                "SELECT name FROM sqlite_schema WHERE tbl_name = 'project_volume' \
                 AND name NOT LIKE 'sqlite_%' ORDER BY name",
            )
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(objects, ["project_volume", "project_volume_rank"]);
        assert_eq!(store.project_volume(7).unwrap(), None);
        assert_eq!(store.project(7).unwrap().title, "Kept");
        // Rows survive a hand rollback and re-migration.
        store
            .conn
            .execute(
                "INSERT INTO project_volume VALUES (7, 5, 2, 3, 2, 0, 'now', NULL)",
                [],
            )
            .unwrap();
        store.conn.execute_batch("PRAGMA user_version = 8").unwrap();
        store.migrate().unwrap();
        assert_eq!(store.project_volume(7).unwrap().unwrap().volume_mutez, 5);
    }

    #[test]
    fn project_sums_fail_instead_of_wrapping() {
        let mut a = Aggregate::default();
        a.add(true, Some(i64::MAX - 1)).unwrap();
        a.add(false, Some(2)).unwrap();
        a.add(false, None).unwrap();
        assert_eq!(
            (a.primary, a.secondary, a.priced, a.unpriced),
            (i64::MAX - 1, 2, 2, 1)
        );
        assert!(matches!(a.volume(), Err(StoreError::Internal(_))));
        assert!(matches!(a.add(true, Some(2)), Err(StoreError::Internal(_))));
        assert!(matches!(checked(u64::MAX, 1), Err(StoreError::Internal(_))));
        assert!(matches!(checked(0, -1), Err(StoreError::Internal(_))));
        assert_eq!(checked(1, 2).unwrap(), 3);
    }

    #[test]
    fn sales_paths_become_read_only_uris() {
        assert_eq!(
            read_only_uri(Path::new("var/sales.sqlite")).unwrap(),
            "file:var/sales.sqlite?mode=ro"
        );
        assert_eq!(
            read_only_uri(Path::new("/a b/x?y#z%.db")).unwrap(),
            "file:/a%20b/x%3Fy%23z%25.db?mode=ro"
        );
    }
}
