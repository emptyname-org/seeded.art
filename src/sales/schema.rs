//! The sales database: its own file, its own schema version (`PRAGMA user_version`) and an
//! `application_id` so the archive and the sales file cannot be mixed up.
//!
//! - `sales`: one row per token per trade, unique per (token, moving operation);
//! - `marketplaces`: names of the market and issuer contracts rows point to;
//! - `escrow`: who listed each token a market holds (indexer state);
//! - `tickets`: fx(params) mint tickets and the price paid for them (indexer state);
//! - `cursors`: per stream, the last TzKT id fully written, the safe level its last complete
//!   pass reached, and when it was written (unix seconds);
//! - `issues`: what the rules could not settle, for review.

use std::path::Path;
use std::time::Duration;

use rusqlite::{Connection, OpenFlags, TransactionBehavior, params};

use super::SalesError;
use super::markets::{ISSUER_LIST, MARKETS};

pub const SCHEMA_VERSION: i64 = 1;
/// "5eed5a1e": a seeded.art sales database.
pub const APPLICATION_ID: i64 = 0x5eed_5a1e;
const BUSY_TIMEOUT: Duration = Duration::from_secs(30);

const SCHEMA_V1: &str = "
CREATE TABLE marketplaces (
  contract TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  role TEXT NOT NULL CHECK (role IN ('market', 'issuer'))
) STRICT, WITHOUT ROWID;
CREATE TABLE sales (
  id INTEGER PRIMARY KEY,
  contract TEXT NOT NULL,
  token_id INTEGER NOT NULL CHECK (token_id >= 0),
  kind TEXT NOT NULL CHECK (kind IN ('mint', 'sale', 'offer_accept', 'auction', 'other')),
  price_mutez INTEGER CHECK (price_mutez IS NULL OR price_mutez >= 0),
  currency TEXT NOT NULL,
  seller TEXT,
  buyer TEXT NOT NULL,
  marketplace TEXT NOT NULL REFERENCES marketplaces (contract),
  op_hash TEXT NOT NULL,
  op_id INTEGER NOT NULL CHECK (op_id > 0),
  level INTEGER NOT NULL CHECK (level > 0),
  timestamp TEXT NOT NULL,
  UNIQUE (contract, token_id, op_id),
  CHECK (kind != 'mint' OR seller IS NULL),
  CHECK (price_mutez IS NULL OR currency IN ('tez', 'wxtz'))
) STRICT;
CREATE TABLE escrow (
  market TEXT NOT NULL,
  contract TEXT NOT NULL,
  token_id INTEGER NOT NULL,
  seller TEXT NOT NULL,
  op_id INTEGER NOT NULL,
  PRIMARY KEY (market, contract, token_id)
) STRICT, WITHOUT ROWID;
CREATE TABLE tickets (
  ticket_id INTEGER PRIMARY KEY,
  op_id INTEGER NOT NULL UNIQUE,
  price_mutez INTEGER NOT NULL CHECK (price_mutez >= 0),
  owner TEXT NOT NULL,
  level INTEGER NOT NULL
) STRICT;
CREATE TABLE cursors (
  stream TEXT PRIMARY KEY,
  cursor_id INTEGER NOT NULL DEFAULT 0,
  synced_level INTEGER,
  updated_at INTEGER NOT NULL
) STRICT, WITHOUT ROWID;
CREATE TABLE issues (
  stream TEXT NOT NULL,
  op_id INTEGER NOT NULL,
  reason TEXT NOT NULL,
  op_hash TEXT NOT NULL,
  level INTEGER NOT NULL,
  detail TEXT NOT NULL,
  PRIMARY KEY (stream, op_id, reason)
) STRICT, WITHOUT ROWID;
";

fn user_version(conn: &Connection, schema: &str) -> Result<i64, SalesError> {
    Ok(conn.pragma_query_value(Some(schema), "user_version", |r| r.get(0))?)
}

fn application_id(conn: &Connection, schema: &str) -> Result<i64, SalesError> {
    Ok(conn.pragma_query_value(Some(schema), "application_id", |r| r.get(0))?)
}

/// Checks that schema `schema` of `conn` is a current sales database.
pub(crate) fn check(conn: &Connection, schema: &str) -> Result<(), SalesError> {
    match (application_id(conn, schema)?, user_version(conn, schema)?) {
        (APPLICATION_ID, SCHEMA_VERSION) => Ok(()),
        (APPLICATION_ID, v) => Err(SalesError::Schema(format!(
            "version {v}, expected {SCHEMA_VERSION}"
        ))),
        (0, 0) => {
            let tables: i64 = conn.query_row(
                &format!("SELECT count(*) FROM \"{schema}\".sqlite_schema"),
                [],
                |r| r.get(0),
            )?;
            Err(if tables == 0 {
                SalesError::Unavailable("not initialised".into())
            } else {
                SalesError::Schema("not a sales database".into())
            })
        }
        (id, _) => Err(SalesError::Schema(format!(
            "application_id {id:#x} is not a sales database"
        ))),
    }
}

/// Opens (creating owner-only if missing) and migrates the sales database for writing.
pub fn open_writer(path: &Path) -> Result<Connection, SalesError> {
    if !path.exists() {
        let mut file = std::fs::OpenOptions::new();
        file.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            file.mode(0o600);
        }
        file.open(path)
            .map_err(|e| SalesError::Io(format!("{}: {e}", path.display())))?;
    }
    let mut conn = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    conn.busy_timeout(BUSY_TIMEOUT)?;
    let mode: String = conn.pragma_update_and_check(None, "journal_mode", "WAL", |r| r.get(0))?;
    if !mode.eq_ignore_ascii_case("wal") {
        return Err(SalesError::Schema(format!(
            "journal_mode is {mode}, expected wal"
        )));
    }
    conn.pragma_update(None, "synchronous", "NORMAL")?;
    conn.pragma_update(None, "foreign_keys", true)?;
    migrate(&mut conn)?;
    Ok(conn)
}

/// Opens an existing sales database read-only (stats).
pub fn open_read_only(path: &Path) -> Result<Connection, SalesError> {
    if !path.is_file() {
        return Err(SalesError::Unavailable(format!(
            "{} does not exist",
            path.display()
        )));
    }
    let conn = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    conn.busy_timeout(BUSY_TIMEOUT)?;
    check(&conn, "main")?;
    Ok(conn)
}

fn migrate(conn: &mut Connection) -> Result<(), SalesError> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    match (application_id(&tx, "main")?, user_version(&tx, "main")?) {
        (0, 0) => {
            let tables: i64 =
                tx.query_row("SELECT count(*) FROM sqlite_schema", [], |r| r.get(0))?;
            if tables != 0 {
                return Err(SalesError::Schema(
                    "file has tables but is not a sales database".into(),
                ));
            }
            tx.execute_batch(SCHEMA_V1)?;
            tx.pragma_update(None, "application_id", APPLICATION_ID)?;
            tx.pragma_update(None, "user_version", SCHEMA_VERSION)?;
        }
        _ => check(&tx, "main")?,
    }
    // Names follow the code; rows keep the contract.
    let mut upsert = tx.prepare(
        "INSERT INTO marketplaces (contract, name, role) VALUES (?1, ?2, ?3)
         ON CONFLICT (contract) DO UPDATE SET name = excluded.name, role = excluded.role",
    )?;
    for m in &MARKETS {
        upsert.execute(params![m.address, m.name, "market"])?;
    }
    for i in &ISSUER_LIST {
        upsert.execute(params![i.address, i.name, "issuer"])?;
    }
    drop(upsert);
    tx.commit()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_foreign_database_is_refused() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE tokens (x INTEGER)")
            .unwrap();
        let mut conn = conn;
        assert!(matches!(migrate(&mut conn), Err(SalesError::Schema(_))));
        let mut fresh = Connection::open_in_memory().unwrap();
        migrate(&mut fresh).unwrap();
        check(&fresh, "main").unwrap();
        migrate(&mut fresh).unwrap();
        fresh
            .pragma_update(None, "user_version", SCHEMA_VERSION + 1)
            .unwrap();
        assert!(matches!(check(&fresh, "main"), Err(SalesError::Schema(_))));
    }
}
