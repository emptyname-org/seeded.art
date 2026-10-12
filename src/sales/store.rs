//! Read API: the archive connection with the sales database attached read-only as `sales`.
//!
//! The web, per archive connection (each request opens one):
//! ```text
//! let store = Store::open_read_only(&config.database)?;
//! sales::store::attach(store.conn(), &sales_path)?;        // Unavailable if never synced
//! let rows = sales::store::sales_for_token(store.conn(), contract, token_id, 20)?;
//! let summary = sales::store::project_sales_summary(store.conn(), project_id)?;
//! ```
//! [`attach`] refuses a read-write connection: an attached file inherits the main
//! connection's flags, so a read-only archive connection keeps the sales file read-only too.
//! Rows only cover tokens the archive knows (`main.tokens`) of visible projects
//! (`main.projects.hidden = 0`); a flagged project's tokens have no price history here.

use std::path::Path;

use rusqlite::{Connection, OptionalExtension, Row, params};
use serde::Serialize;

use super::{SaleKind, SalesError, schema};

/// Schema name of the attached sales database.
pub const ATTACH_AS: &str = "sales";
/// Largest page of [`sales_for_token`].
pub const MAX_LIMIT: u32 = 100;

/// One trade of one token, newest first in lists.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Sale {
    pub contract: String,
    pub token_id: u64,
    pub kind: SaleKind,
    /// `None` when paid in a token that is not tez or wXTZ (see `currency`), or not attributable.
    pub price_mutez: Option<u64>,
    /// `tez`, `wxtz` (objkt.com wrapped tez, 1:1) or the paying token's contract.
    pub currency: String,
    /// `None` for mints, or when an escrowed token's listing was not seen.
    pub seller: Option<String>,
    pub buyer: String,
    pub marketplace: String,
    /// Market contract, or the issuer contract for mints.
    pub marketplace_contract: String,
    pub op_hash: String,
    pub level: u64,
    pub timestamp: String,
}

/// Secondary market of one project (mints excluded).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProjectSalesSummary {
    pub count: u64,
    /// Sum of tez and wXTZ prices.
    pub volume_mutez: u64,
    /// Lowest of each token's latest priced sale.
    pub floor_last_sold_mutez: Option<u64>,
    pub last_sale: Option<Sale>,
}

/// Primary mints of one project, as paid on chain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProjectMintSummary {
    pub minted: u64,
    /// Mints with a known price (ticket mints whose ticket is unknown are not).
    pub priced: u64,
    pub min_mutez: Option<u64>,
    pub max_mutez: Option<u64>,
    pub volume_mutez: u64,
    pub first: Option<String>,
    pub last: Option<String>,
}

/// How many sales rows the archive can show.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct Coverage {
    pub rows: u64,
    /// Token known to the archive, project visible.
    pub visible: u64,
    /// Token known, project flagged.
    pub hidden: u64,
    /// Token absent from the archive.
    pub unknown: u64,
}

fn is_attached(conn: &Connection) -> Result<bool, SalesError> {
    Ok(conn
        .query_row(
            "SELECT 1 FROM pragma_database_list WHERE name = ?1",
            [ATTACH_AS],
            |_| Ok(()),
        )
        .optional()?
        .is_some())
}

/// Attaches the sales database at `path` as `sales` to a read-only connection, checking its
/// application id and schema version. Attaching twice is a no-op.
pub fn attach(conn: &Connection, path: &Path) -> Result<(), SalesError> {
    if is_attached(conn)? {
        // A reused connection whose file was migrated in place stops reading it.
        let checked = schema::check(conn, ATTACH_AS);
        if checked.is_err() {
            let _ = conn.execute("DETACH DATABASE sales", []);
        }
        return checked;
    }
    if !path.is_file() {
        return Err(SalesError::Unavailable(format!(
            "{} does not exist",
            path.display()
        )));
    }
    let name = path
        .to_str()
        .ok_or_else(|| SalesError::Usage(format!("{} is not UTF-8", path.display())))?;
    conn.execute("ATTACH DATABASE ?1 AS sales", [name])?;
    let checked = match conn.is_readonly(ATTACH_AS) {
        Ok(true) => schema::check(conn, ATTACH_AS),
        Ok(false) => Err(SalesError::Usage(
            "attach the sales database to a read-only connection".into(),
        )),
        Err(e) => Err(e.into()),
    };
    if checked.is_err() {
        let _ = conn.execute("DETACH DATABASE sales", []);
    }
    checked
}

/// Runs `f` in one read transaction (or the caller's), so its statements share a snapshot.
fn read<T>(
    conn: &Connection,
    f: impl FnOnce(&Connection) -> Result<T, SalesError>,
) -> Result<T, SalesError> {
    if !conn.is_autocommit() {
        return f(conn);
    }
    let tx = conn.unchecked_transaction()?;
    let out = f(&tx)?;
    tx.commit()?;
    Ok(out)
}

const SALE_COLUMNS: &str = "s.contract, s.token_id, s.kind, s.price_mutez, s.currency, \
     s.seller, s.buyer, m.name, s.marketplace, s.op_hash, s.level, s.timestamp";

fn unsigned(row: &Row, i: usize) -> rusqlite::Result<u64> {
    let v: i64 = row.get(i)?;
    u64::try_from(v).map_err(|_| rusqlite::Error::IntegralValueOutOfRange(i, v))
}

fn sale(row: &Row) -> rusqlite::Result<Sale> {
    let kind: String = row.get(2)?;
    let price: Option<i64> = row.get(3)?;
    Ok(Sale {
        contract: row.get(0)?,
        token_id: unsigned(row, 1)?,
        kind: SaleKind::parse(&kind).ok_or_else(|| {
            rusqlite::Error::FromSqlConversionFailure(
                2,
                rusqlite::types::Type::Text,
                format!("unknown kind {kind:?}").into(),
            )
        })?,
        price_mutez: price
            .map(|p| u64::try_from(p).map_err(|_| rusqlite::Error::IntegralValueOutOfRange(3, p)))
            .transpose()?,
        currency: row.get(4)?,
        seller: row.get(5)?,
        buyer: row.get(6)?,
        marketplace: row.get(7)?,
        marketplace_contract: row.get(8)?,
        op_hash: row.get(9)?,
        level: unsigned(row, 10)?,
        timestamp: row.get(11)?,
    })
}

fn sql_id(v: u64) -> Result<i64, SalesError> {
    i64::try_from(v).map_err(|_| SalesError::Usage(format!("{v} is out of range")))
}

/// Mints and sales of one token, newest first, at most `limit` (clamped to 1..=100).
/// Empty for tokens the archive does not show.
pub fn sales_for_token(
    conn: &Connection,
    contract: &str,
    token_id: u64,
    limit: u32,
) -> Result<Vec<Sale>, SalesError> {
    let limit = limit.clamp(1, MAX_LIMIT);
    let token_id = sql_id(token_id)?;
    read(conn, |c| {
        let mut stmt = c.prepare_cached(&format!(
            "SELECT {SALE_COLUMNS} FROM main.tokens t
             JOIN main.projects p ON p.id = t.project_id AND p.hidden = 0
             JOIN sales.sales s ON s.contract = t.contract AND s.token_id = t.token_id
             JOIN sales.marketplaces m ON m.contract = s.marketplace
             WHERE t.contract = ?1 AND t.token_id = ?2
             ORDER BY s.op_id DESC LIMIT ?3"
        ))?;
        let rows = stmt.query_map(params![contract, token_id, limit], sale)?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    })
}

fn visible_project(c: &Connection, project_id: i64) -> Result<bool, SalesError> {
    Ok(
        c.prepare_cached("SELECT 1 FROM main.projects WHERE id = ?1 AND hidden = 0")?
            .query_row([project_id], |_| Ok(()))
            .optional()?
            .is_some(),
    )
}

/// Secondary sales of a visible project; `None` for a flagged or unknown project.
pub fn project_sales_summary(
    conn: &Connection,
    project_id: u64,
) -> Result<Option<ProjectSalesSummary>, SalesError> {
    let id = sql_id(project_id)?;
    read(conn, |c| {
        if !visible_project(c, id)? {
            return Ok(None);
        }
        let (count, volume): (i64, i64) = c
            .prepare_cached(
                "SELECT count(*), coalesce(sum(s.price_mutez), 0)
                 FROM main.tokens t
                 JOIN sales.sales s ON s.contract = t.contract AND s.token_id = t.token_id
                 WHERE t.project_id = ?1 AND s.kind != 'mint'",
            )?
            .query_row([id], |r| Ok((r.get(0)?, r.get(1)?)))?;
        // SQLite takes bare columns from the row holding max(): each token's latest sale.
        let floor: Option<i64> = c
            .prepare_cached(
                "SELECT min(price) FROM (
                   SELECT s.price_mutez AS price, max(s.op_id)
                   FROM main.tokens t
                   JOIN sales.sales s ON s.contract = t.contract AND s.token_id = t.token_id
                   WHERE t.project_id = ?1 AND s.kind != 'mint' AND s.price_mutez IS NOT NULL
                   GROUP BY s.contract, s.token_id)",
            )?
            .query_row([id], |r| r.get(0))?;
        let last_sale = c
            .prepare_cached(&format!(
                "SELECT {SALE_COLUMNS} FROM main.tokens t
                 JOIN sales.sales s ON s.contract = t.contract AND s.token_id = t.token_id
                 JOIN sales.marketplaces m ON m.contract = s.marketplace
                 WHERE t.project_id = ?1 AND s.kind != 'mint'
                 ORDER BY s.op_id DESC LIMIT 1"
            ))?
            .query_row([id], sale)
            .optional()?;
        Ok(Some(ProjectSalesSummary {
            count: u64::try_from(count).unwrap_or(0),
            volume_mutez: u64::try_from(volume).unwrap_or(0),
            floor_last_sold_mutez: floor.and_then(|f| u64::try_from(f).ok()),
            last_sale,
        }))
    })
}

/// Primary mints of a visible project; `None` for a flagged or unknown project.
pub fn project_mint_summary(
    conn: &Connection,
    project_id: u64,
) -> Result<Option<ProjectMintSummary>, SalesError> {
    let id = sql_id(project_id)?;
    read(conn, |c| {
        if !visible_project(c, id)? {
            return Ok(None);
        }
        let summary = c
            .prepare_cached(
                "SELECT count(*), count(s.price_mutez), min(s.price_mutez), max(s.price_mutez),
                   coalesce(sum(s.price_mutez), 0), min(s.timestamp), max(s.timestamp)
                 FROM main.tokens t
                 JOIN sales.sales s ON s.contract = t.contract AND s.token_id = t.token_id
                 WHERE t.project_id = ?1 AND s.kind = 'mint'",
            )?
            .query_row([id], |r| {
                let n = |i: usize| -> rusqlite::Result<u64> { unsigned(r, i) };
                let opt = |i: usize| -> rusqlite::Result<Option<u64>> {
                    Ok(r.get::<_, Option<i64>>(i)?
                        .and_then(|v| u64::try_from(v).ok()))
                };
                Ok(ProjectMintSummary {
                    minted: n(0)?,
                    priced: n(1)?,
                    min_mutez: opt(2)?,
                    max_mutez: opt(3)?,
                    volume_mutez: n(4)?,
                    first: r.get(5)?,
                    last: r.get(6)?,
                })
            })?;
        Ok(Some(summary))
    })
}

/// How many sales rows match archive tokens, visible or flagged (one pass over `sales`).
pub fn coverage(conn: &Connection) -> Result<Coverage, SalesError> {
    read(conn, |c| {
        let (rows, visible, hidden): (i64, i64, i64) = c.query_row(
            "SELECT count(*), coalesce(sum(p.hidden = 0), 0), coalesce(sum(p.hidden = 1), 0)
             FROM sales.sales s
             LEFT JOIN main.tokens t ON t.contract = s.contract AND t.token_id = s.token_id
             LEFT JOIN main.projects p ON p.id = t.project_id",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )?;
        let n = |v: i64| u64::try_from(v).unwrap_or(0);
        Ok(Coverage {
            rows: n(rows),
            visible: n(visible),
            hidden: n(hidden),
            unknown: n(rows - visible - hidden),
        })
    })
}

/// Operational counts of a sales database opened directly (schema `main`).
#[derive(Debug, Clone, Default, Serialize)]
pub struct Stats {
    pub rows: u64,
    /// (marketplace, contract, kind, rows, priced rows, volume in mutez of tez-like prices)
    pub by_market: Vec<(String, String, String, u64, u64, u64)>,
    /// (currency, rows)
    pub currencies: Vec<(String, u64)>,
    /// (stream, reason, count)
    pub issues: Vec<(String, String, u64)>,
    /// (stream, cursor id, synced level)
    pub cursors: Vec<(String, u64, Option<u64>)>,
    pub tickets: u64,
    pub escrow: u64,
}

pub fn stats(conn: &Connection) -> Result<Stats, SalesError> {
    read(conn, |c| {
        let n = |v: i64| u64::try_from(v).unwrap_or(0);
        let one = |sql: &str| -> Result<u64, SalesError> {
            Ok(n(c.query_row(sql, [], |r| r.get::<_, i64>(0))?))
        };
        let mut s = Stats {
            rows: one("SELECT count(*) FROM sales")?,
            tickets: one("SELECT count(*) FROM tickets")?,
            escrow: one("SELECT count(*) FROM escrow")?,
            ..Stats::default()
        };
        let mut stmt = c.prepare(
            "SELECT coalesce(m.name, s.marketplace), s.marketplace, s.kind, count(*),
               count(s.price_mutez), coalesce(sum(s.price_mutez), 0)
             FROM sales s LEFT JOIN marketplaces m ON m.contract = s.marketplace
             GROUP BY s.marketplace, s.kind ORDER BY count(*) DESC",
        )?;
        for r in stmt.query_map([], |r| {
            Ok((
                r.get(0)?,
                r.get(1)?,
                r.get(2)?,
                r.get::<_, i64>(3)?,
                r.get::<_, i64>(4)?,
                r.get::<_, i64>(5)?,
            ))
        })? {
            let (a, b, k, rows, priced, vol) = r?;
            s.by_market.push((a, b, k, n(rows), n(priced), n(vol)));
        }
        let mut stmt =
            c.prepare("SELECT currency, count(*) FROM sales GROUP BY currency ORDER BY 2 DESC")?;
        for r in stmt.query_map([], |r| Ok((r.get(0)?, r.get::<_, i64>(1)?)))? {
            let (cur, rows) = r?;
            s.currencies.push((cur, n(rows)));
        }
        let mut stmt = c.prepare(
            "SELECT stream, reason, count(*) FROM issues GROUP BY stream, reason ORDER BY 3 DESC",
        )?;
        for r in stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get::<_, i64>(2)?)))? {
            let (st, reason, count) = r?;
            s.issues.push((st, reason, n(count)));
        }
        let mut stmt =
            c.prepare("SELECT stream, cursor_id, synced_level FROM cursors ORDER BY stream")?;
        for r in stmt.query_map([], |r| {
            Ok((r.get(0)?, r.get::<_, i64>(1)?, r.get::<_, Option<i64>>(2)?))
        })? {
            let (st, id, level) = r?;
            s.cursors.push((st, n(id), level.map(n)));
        }
        Ok(s)
    })
}

impl std::fmt::Display for Stats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let tez = |m: u64| format!("{}.{:06}", m / 1_000_000, m % 1_000_000);
        writeln!(
            f,
            "rows {} (tickets {}, tokens in escrow {})",
            self.rows, self.tickets, self.escrow
        )?;
        for (name, contract, kind, rows, priced, vol) in &self.by_market {
            writeln!(
                f,
                "  {name} [{contract}] {kind}: {rows} rows, {priced} priced, {} tez",
                tez(*vol)
            )?;
        }
        for (cur, rows) in &self.currencies {
            writeln!(f, "  currency {cur}: {rows}")?;
        }
        for (stream, reason, count) in &self.issues {
            writeln!(f, "  issue {stream} {reason}: {count}")?;
        }
        for (stream, id, level) in &self.cursors {
            let level = level.map_or_else(|| "-".to_owned(), |l| l.to_string());
            writeln!(f, "  cursor {stream}: id {id}, synced level {level}")?;
        }
        Ok(())
    }
}
