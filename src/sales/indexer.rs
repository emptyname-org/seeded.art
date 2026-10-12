//! Resumable backfill and incremental sync of the sales database.
//!
//! Work is split into streams, each with its own cursor (the last TzKT operation id written):
//! - `tickets`: fx(params) mint tickets and their price, synced first;
//! - `mint:<issuer>`: the token contract `mint` calls of each issuer, priced by the issuer call;
//! - `market:<contract>`: every operation of a dedicated market (fxhash), or, for a shared
//!   market (objkt.com), the operations at the levels where it moved an fxhash token.
//!
//! Each page ends on a level boundary and is written in one transaction together with its
//! cursor and escrow changes, so an interrupted run (Ctrl-C, SIGTERM, transient failure) loses
//! at most the page in flight. Only levels at least `confirmations` below the head are read,
//! so a cursor never covers a block that can still change. Streams run in parallel on a
//! bounded number of workers; requests share the client's pace and retries.

use std::collections::{HashMap, VecDeque};
use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};

use super::chain::{Feed, Op};
use super::interpret::{self, Checks, Escrow, Outcome, TicketPrices};
use super::markets::{
    ISSUER_LIST, Issuer, MARKETS, Market, Scope, TICKET_ISSUER, is_token_contract,
};
use super::{SalesError, schema};
use crate::ingest::tzkt::Tzkt;
use crate::ingest::{FINALITY, TOKEN_CONTRACTS};

/// Levels per `level.in` query of a shared market.
const LEVELS_PER_QUERY: usize = 50;
/// Rows accepted for one `level.in` query, or for the issuer calls of one mint page.
const MAX_JOIN_ROWS: usize = 500_000;

#[derive(Debug, Clone)]
pub struct SyncOptions {
    /// Pages per stream in this run; `None` runs to the safe level.
    pub limit: Option<usize>,
    /// Streams synced at once.
    pub concurrency: usize,
    /// Rows per TzKT page (at most 10,000).
    pub page: usize,
    /// Levels below the head that are not read yet; at least [`FINALITY`].
    pub confirmations: u64,
}

impl Default for SyncOptions {
    fn default() -> Self {
        SyncOptions {
            limit: None,
            concurrency: 4,
            page: 10_000,
            confirmations: FINALITY,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stream {
    Tickets,
    Mint(&'static Issuer),
    Market(&'static Market),
}

impl Stream {
    pub fn name(&self) -> String {
        match self {
            Stream::Tickets => "tickets".into(),
            Stream::Mint(i) => format!("mint:{}", i.address),
            Stream::Market(m) => format!("market:{}", m.address),
        }
    }

    pub fn label(&self) -> &'static str {
        match self {
            Stream::Tickets => "fx(params) mint tickets",
            Stream::Mint(i) => i.name,
            Stream::Market(m) => m.name,
        }
    }

    /// Every stream; tickets first (ticket mints price on them).
    pub fn all() -> Vec<Stream> {
        let mut out = vec![Stream::Tickets];
        out.extend(ISSUER_LIST.iter().map(Stream::Mint));
        out.extend(MARKETS.iter().map(Stream::Market));
        out
    }
}

#[derive(Debug, Clone, Default)]
pub struct StreamReport {
    pub stream: String,
    pub label: &'static str,
    pub pages: u64,
    pub ops: u64,
    pub sales: u64,
    pub issues: u64,
    pub deposits: u64,
    pub returns: u64,
    /// Highest level written in this run.
    pub level: Option<u64>,
    pub checks: Checks,
    /// Reached the safe level.
    pub done: bool,
    pub limited: bool,
    pub interrupted: bool,
    pub transient: bool,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct SyncReport {
    pub head: u64,
    pub safe_level: u64,
    pub streams: Vec<StreamReport>,
}

impl SyncReport {
    pub fn interrupted(&self) -> bool {
        self.streams.iter().any(|s| s.interrupted)
    }

    pub fn limited(&self) -> bool {
        self.streams.iter().any(|s| s.limited)
    }

    pub fn transient_failures(&self) -> usize {
        self.streams.iter().filter(|s| s.transient).count()
    }

    pub fn permanent_failures(&self) -> usize {
        self.streams
            .iter()
            .filter(|s| s.error.is_some() && !s.transient)
            .count()
    }

    pub fn checks(&self) -> Checks {
        let mut c = Checks::default();
        for s in &self.streams {
            c.add(s.checks);
        }
        c
    }
}

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX))
}

/// u64 to SQLite integer; callers validated the bound, so failure is an internal error.
fn sql(v: u64) -> Result<i64, SalesError> {
    i64::try_from(v).map_err(|_| SalesError::Schema(format!("{v} does not fit SQLite")))
}

/// Escrow and ticket lookups inside the open write transaction.
struct DbBook<'a> {
    conn: &'a Connection,
    market: &'static str,
}

impl Escrow for DbBook<'_> {
    fn deposit(
        &mut self,
        contract: &str,
        token_id: u64,
        seller: &str,
        op_id: u64,
    ) -> Result<(), SalesError> {
        self.conn
            .prepare_cached(
                "INSERT INTO escrow (market, contract, token_id, seller, op_id)
                 VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT (market, contract, token_id)
                 DO UPDATE SET seller = excluded.seller, op_id = excluded.op_id",
            )?
            .execute(params![
                self.market,
                contract,
                sql(token_id)?,
                seller,
                sql(op_id)?
            ])?;
        Ok(())
    }

    fn take(&mut self, contract: &str, token_id: u64) -> Result<Option<String>, SalesError> {
        Ok(self
            .conn
            .prepare_cached(
                "DELETE FROM escrow WHERE market = ?1 AND contract = ?2 AND token_id = ?3
                 RETURNING seller",
            )?
            .query_row(params![self.market, contract, sql(token_id)?], |r| r.get(0))
            .optional()?)
    }
}

impl TicketPrices for DbBook<'_> {
    fn ticket_price(&mut self, ticket_id: u64) -> Result<Option<u64>, SalesError> {
        let price: Option<i64> = self
            .conn
            .prepare_cached("SELECT price_mutez FROM tickets WHERE ticket_id = ?1")?
            .query_row([sql(ticket_id)?], |r| r.get(0))
            .optional()?;
        Ok(price.and_then(|p| u64::try_from(p).ok()))
    }
}

fn cursor(conn: &Connection, stream: &str) -> Result<u64, SalesError> {
    conn.execute(
        "INSERT INTO cursors (stream, cursor_id, updated_at) VALUES (?1, 0, ?2)
         ON CONFLICT (stream) DO NOTHING",
        params![stream, unix_now()],
    )?;
    let id: i64 = conn.query_row(
        "SELECT cursor_id FROM cursors WHERE stream = ?1",
        [stream],
        |r| r.get(0),
    )?;
    u64::try_from(id).map_err(|_| SalesError::Schema(format!("cursor of {stream} is {id}")))
}

fn set_cursor(conn: &Connection, stream: &str, id: u64) -> Result<(), SalesError> {
    conn.execute(
        "UPDATE cursors SET cursor_id = ?2, updated_at = ?3 WHERE stream = ?1",
        params![stream, sql(id)?, unix_now()],
    )?;
    Ok(())
}

fn set_synced(conn: &Connection, stream: &str, level: u64) -> Result<(), SalesError> {
    conn.execute(
        "UPDATE cursors SET synced_level = ?2, updated_at = ?3 WHERE stream = ?1",
        params![stream, sql(level)?, unix_now()],
    )?;
    Ok(())
}

/// Writes one interpreted page; returns (sales inserted, issues inserted).
fn write(conn: &Connection, stream: &str, out: &Outcome) -> Result<(u64, u64), SalesError> {
    let mut sales = 0;
    let mut insert = conn.prepare_cached(
        "INSERT INTO sales (contract, token_id, kind, price_mutez, currency, seller, buyer,
           marketplace, op_hash, op_id, level, timestamp)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
         ON CONFLICT (contract, token_id, op_id) DO NOTHING",
    )?;
    for s in &out.sales {
        sales += insert.execute(params![
            s.contract,
            sql(s.token_id)?,
            s.kind.as_str(),
            s.price_mutez.map(sql).transpose()?,
            s.currency.as_str(),
            s.seller,
            s.buyer,
            s.marketplace_contract,
            s.op_hash,
            sql(s.op_id)?,
            sql(s.level)?,
            s.timestamp,
        ])? as u64;
    }
    let mut issues = 0;
    let mut note = conn.prepare_cached(
        "INSERT INTO issues (stream, op_id, reason, op_hash, level, detail)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6) ON CONFLICT DO NOTHING",
    )?;
    for i in &out.issues {
        issues += note.execute(params![
            stream,
            sql(i.op_id)?,
            i.reason,
            i.op_hash,
            sql(i.level)?,
            i.detail
        ])? as u64;
    }
    Ok((sales, issues))
}

struct Run<'a> {
    feed: Feed<'a>,
    safe: u64,
}

impl Run<'_> {
    /// Writes the next page of `stream`; `false` when it has reached the safe level.
    fn step(
        &self,
        stream: Stream,
        conn: &mut Connection,
        report: &mut StreamReport,
    ) -> Result<bool, SalesError> {
        let name = stream.name();
        let after = cursor(conn, &name)?;
        match stream {
            Stream::Tickets => self.tickets(conn, &name, after, report),
            Stream::Mint(issuer) => self.mints(issuer, conn, &name, after, report),
            Stream::Market(m) => self.market(m, conn, &name, after, report),
        }
    }

    fn tickets(
        &self,
        conn: &mut Connection,
        name: &str,
        after: u64,
        report: &mut StreamReport,
    ) -> Result<bool, SalesError> {
        let tickets = self.feed.ticket_mints(self.safe, after)?;
        let Some(last) = tickets.last() else {
            return Ok(false);
        };
        let prices: HashMap<u64, u64> = self.feed.ticket_prices(&tickets)?.into_iter().collect();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        {
            let mut insert = tx.prepare_cached(
                "INSERT INTO tickets (ticket_id, op_id, price_mutez, owner, level)
                 VALUES (?1, ?2, ?3, ?4, ?5) ON CONFLICT DO NOTHING",
            )?;
            for t in &tickets {
                let price = prices.get(&t.op_id).ok_or_else(|| {
                    super::bad(
                        "ticket",
                        format!("no mint operation for ticket {}", t.ticket_id),
                    )
                })?;
                insert.execute(params![
                    sql(t.ticket_id)?,
                    sql(t.op_id)?,
                    sql(*price)?,
                    t.owner,
                    sql(t.level)?
                ])?;
            }
        }
        set_cursor(&tx, name, last.id)?;
        tx.commit()?;
        report.pages += 1;
        report.ops += tickets.len() as u64;
        report.level = Some(last.level);
        Ok(true)
    }

    fn mints(
        &self,
        issuer: &'static Issuer,
        conn: &mut Connection,
        name: &str,
        after: u64,
        report: &mut StreamReport,
    ) -> Result<bool, SalesError> {
        let (safe, a, fa2) = (self.safe, issuer.address, issuer.token_contract);
        let children = self.feed.ops_levels(
            &format!(
                "/operations/transactions?sender={a}&target={fa2}&entrypoint=mint\
                 &status=applied&level.le={safe}"
            ),
            after,
            &|o| o.sender == a && o.target == fa2 && o.is_call("mint") && o.level <= safe,
        )?;
        let (Some(first), Some(last)) = (children.first(), children.last()) else {
            return Ok(false);
        };
        let (lo, hi) = (first.level, last.level);
        let parents = self.feed.ops_all(
            &format!(
                "/operations/transactions?target={a}&entrypoint.in=mint,mint_with_ticket\
                 &status=applied&level.ge={lo}&level.le={hi}"
            ),
            &|o| {
                o.target == a
                    && (o.is_call("mint") || o.is_call("mint_with_ticket"))
                    && (lo..=hi).contains(&o.level)
            },
            MAX_JOIN_ROWS,
        )?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let out = interpret::mint_ops(
            issuer,
            &children,
            &parents,
            &mut DbBook {
                conn: &tx,
                market: a,
            },
        )?;
        let (sales, issues) = write(&tx, name, &out)?;
        set_cursor(&tx, name, last.id)?;
        tx.commit()?;
        report.pages += 1;
        report.ops += (children.len() + parents.len()) as u64;
        report.sales += sales;
        report.issues += issues;
        report.level = Some(hi);
        Ok(true)
    }

    /// A dedicated market reads every operation it took part in, a page at a time. A shared
    /// market reads its fxhash token transfers, then everything it did at those levels, and
    /// commits every [`LEVELS_PER_QUERY`] levels so only one chunk is held in memory.
    fn market(
        &self,
        market: &'static Market,
        conn: &mut Connection,
        name: &str,
        after: u64,
        report: &mut StreamReport,
    ) -> Result<bool, SalesError> {
        let (safe, a) = (self.safe, market.address);
        let involves = |o: &Op| o.sender == a || o.target == a;
        if market.scope == Scope::Dedicated {
            let ops = self.feed.ops_levels(
                &format!(
                    "/operations/transactions?anyof.sender.target={a}&status=applied\
                     &level.le={safe}"
                ),
                after,
                &|o| involves(o) && o.level <= safe,
            )?;
            let Some(last) = ops.last() else {
                return Ok(false);
            };
            commit_market(market, conn, name, &ops, (last.id, last.level), report)?;
            report.pages += 1;
            report.ops += ops.len() as u64;
            return Ok(true);
        }
        let children = self.feed.ops_levels(
            &format!(
                "/operations/transactions?target.in={}&sender={a}&entrypoint=transfer\
                 &status=applied&level.le={safe}",
                TOKEN_CONTRACTS.join(",")
            ),
            after,
            &|o| {
                o.sender == a
                    && is_token_contract(&o.target)
                    && o.is_call("transfer")
                    && o.level <= safe
            },
        )?;
        if children.is_empty() {
            return Ok(false);
        }
        let mut levels: Vec<u64> = children.iter().map(|o| o.level).collect();
        levels.dedup();
        for chunk in levels.chunks(LEVELS_PER_QUERY) {
            let list = chunk
                .iter()
                .map(u64::to_string)
                .collect::<Vec<_>>()
                .join(",");
            let ops = self.feed.ops_all(
                &format!(
                    "/operations/transactions?anyof.sender.target={a}&status=applied\
                     &level.in={list}"
                ),
                &|o| involves(o) && chunk.binary_search(&o.level).is_ok(),
                MAX_JOIN_ROWS,
            )?;
            let top = chunk[chunk.len() - 1];
            let mut last = None;
            for c in children.iter().filter(|c| c.level <= top) {
                if chunk.binary_search(&c.level).is_err() {
                    continue;
                }
                if ops.binary_search_by_key(&c.id, |o| o.id).is_err() {
                    return Err(super::bad(
                        "market levels",
                        format!(
                            "transfer {} of {} is missing from its level",
                            c.id, market.name
                        ),
                    ));
                }
                last = Some(c.id);
            }
            let last = last.ok_or_else(|| super::bad("market levels", "empty chunk"))?;
            commit_market(market, conn, name, &ops, (last, top), report)?;
            report.ops += ops.len() as u64;
        }
        report.pages += 1;
        report.ops += children.len() as u64;
        Ok(true)
    }
}

/// Interprets `ops` and writes the result with the cursor `(id, level)` in one transaction.
fn commit_market(
    market: &'static Market,
    conn: &mut Connection,
    name: &str,
    ops: &[Op],
    (cursor_id, level): (u64, u64),
    report: &mut StreamReport,
) -> Result<(), SalesError> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let out = interpret::market_ops(
        market,
        ops,
        &mut DbBook {
            conn: &tx,
            market: market.address,
        },
    )?;
    let (sales, issues) = write(&tx, name, &out)?;
    set_cursor(&tx, name, cursor_id)?;
    tx.commit()?;
    report.sales += sales;
    report.issues += issues;
    report.deposits += out.deposits;
    report.returns += out.returns;
    report.checks.add(out.checks);
    report.level = Some(level);
    Ok(())
}

/// Runs one stream until the safe level, the page limit, a stop or a failure.
fn run_stream(
    stream: Stream,
    db: &Path,
    run: &Run,
    opts: &SyncOptions,
    abort: &AtomicBool,
    log: &(dyn Fn(&str) + Sync),
) -> StreamReport {
    let mut report = StreamReport {
        stream: stream.name(),
        label: stream.label(),
        ..StreamReport::default()
    };
    let mut conn = match schema::open_writer(db) {
        Ok(c) => c,
        Err(e) => {
            report.transient = e.is_transient();
            report.error = Some(e.to_string());
            return report;
        }
    };
    loop {
        if run.feed.api.client.stopped() {
            report.interrupted = true;
            break;
        }
        if abort.load(Ordering::SeqCst) {
            break;
        }
        if opts.limit.is_some_and(|l| report.pages >= l as u64) {
            report.limited = true;
            break;
        }
        let step = run.step(stream, &mut conn, &mut report);
        match step {
            Ok(true) => {
                if report.pages % 10 == 1 {
                    log(&format!(
                        "{}: page {}, level {} of {}, sales +{}",
                        report.label,
                        report.pages,
                        report.level.unwrap_or(0),
                        run.safe,
                        report.sales
                    ));
                }
            }
            Ok(false) => {
                match set_synced(&conn, &report.stream, run.safe) {
                    Ok(()) => report.done = true,
                    Err(e) => {
                        report.transient = e.is_transient();
                        report.error = Some(e.to_string());
                    }
                }
                break;
            }
            Err(e) if e.is_interrupted() => {
                report.interrupted = true;
                break;
            }
            Err(e) => {
                report.transient = e.is_transient();
                if !report.transient {
                    abort.store(true, Ordering::SeqCst);
                }
                report.error = Some(e.to_string());
                break;
            }
        }
    }
    log(&format!(
        "{}: {} pages, {} sales, {}",
        report.label,
        report.pages,
        report.sales,
        match (&report.error, report.done) {
            (Some(e), _) => format!("stopped: {e}"),
            (None, true) => "synced".to_owned(),
            (None, false) => "paused".to_owned(),
        }
    ));
    report
}

/// Syncs every stream to `head - confirmations`. Tickets go first; the rest run on
/// `concurrency` workers. A permanent failure stops the other streams after their page.
pub fn sync(
    db: &Path,
    api: &Tzkt,
    opts: &SyncOptions,
    log: &(dyn Fn(&str) + Sync),
) -> Result<SyncReport, SalesError> {
    if !(1..=10_000).contains(&opts.page) || opts.concurrency == 0 {
        return Err(SalesError::Usage(
            "page must be 1..=10000 and concurrency at least 1".into(),
        ));
    }
    if opts.confirmations < FINALITY {
        return Err(SalesError::Usage(format!(
            "confirmations must be at least {FINALITY}: younger blocks can still change"
        )));
    }
    drop(schema::open_writer(db)?);
    let head = api.head_level()?;
    let safe = head.saturating_sub(opts.confirmations);
    if safe == 0 {
        return Err(SalesError::Api(crate::ingest::http::ApiError::Permanent(
            format!("head level {head} is below the confirmation depth"),
        )));
    }
    let run = Run {
        feed: Feed {
            api,
            page: opts.page,
        },
        safe,
    };
    let abort = AtomicBool::new(false);
    let mut streams: VecDeque<Stream> = Stream::all().into();
    let mut reports = Vec::new();
    // Tickets first: mints redeemed with a ticket are priced from it, so the ticket issuer's
    // mints wait until every ticket up to the safe level is known.
    if let Some(first) = streams.pop_front() {
        let r = run_stream(first, db, &run, opts, &abort, log);
        let (done, limited) = (r.done, r.limited);
        reports.push(r);
        if !done && !limited {
            return Ok(SyncReport {
                head,
                safe_level: safe,
                streams: reports,
            });
        }
        if !done {
            streams.retain(|s| *s != Stream::Mint(TICKET_ISSUER));
            log("fx(params) mints wait for the ticket stream to finish");
        }
    }
    let queue = Mutex::new(streams);
    let done = Mutex::new(Vec::new());
    thread::scope(|s| {
        for _ in 0..opts.concurrency.min(16) {
            s.spawn(|| {
                loop {
                    let next = queue.lock().unwrap_or_else(|p| p.into_inner()).pop_front();
                    let Some(stream) = next else {
                        break;
                    };
                    let r = run_stream(stream, db, &run, opts, &abort, log);
                    done.lock().unwrap_or_else(|p| p.into_inner()).push(r);
                }
            });
        }
    });
    let mut rest = done.into_inner().unwrap_or_else(|p| p.into_inner());
    let order = Stream::all();
    rest.sort_by_key(|r| order.iter().position(|s| s.name() == r.stream));
    reports.extend(rest);
    Ok(SyncReport {
        head,
        safe_level: safe,
        streams: reports,
    })
}
