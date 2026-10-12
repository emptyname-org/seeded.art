//! seeded-sales: price history (mints and secondary sales) of fxhash tokens from TzKT.
//!
//! ```text
//! seeded-sales sync --sales-db <path> --archive-db <path> [--limit N]
//! seeded-sales stats --sales-db <path> [--archive-db <path>]
//! ```
//!
//! `sync` creates the sales database if needed and brings every stream to the safe level
//! (head minus SEEDED_SALES_CONFIRMATIONS); `--limit N` stops each stream after N pages. The
//! archive is only read: after a sync, how many rows match its tokens is reported. `stats`
//! prints counts per marketplace and kind, issues and cursors.
//!
//! Environment: SEEDED_TZKT_URL (https://api.tzkt.io, `/v1` optional),
//! SEEDED_TZKT_CONCURRENCY (4), SEEDED_TZKT_TIMEOUT_SECS (30), SEEDED_SALES_PAGE (10000),
//! SEEDED_SALES_CONFIRMATIONS (2).
//!
//! Exit codes: 0 done (or stopped at --limit), 1 failure, 64 usage, 75 transient failure
//! (re-run to resume), 130 interrupted (progress kept). A second Ctrl-C exits at once; the
//! open transaction rolls back and nothing is lost.

use std::env;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use rusqlite::{Connection, OpenFlags};
use seeded_archive::ingest::http::{ApiError, Client, ReqwestFetch, RetryPolicy};
use seeded_archive::ingest::tzkt::{Tzkt, api_base};
use seeded_archive::ingest::{FINALITY, peak_rss_kib};
use seeded_archive::sales::indexer::{SyncOptions, sync};
use seeded_archive::sales::{SalesError, schema, store};

const USAGE: &str = "usage:
  seeded-sales sync --sales-db <path> --archive-db <path> [--limit N]
  seeded-sales stats --sales-db <path> [--archive-db <path>]";
/// Minimum spacing between TzKT request starts, across workers.
const PACE: Duration = Duration::from_millis(100);

enum Outcome {
    Done,
    Transient,
    Interrupted,
}

struct Args {
    command: String,
    sales_db: Option<PathBuf>,
    archive_db: Option<PathBuf>,
    limit: Option<usize>,
}

fn usage(msg: impl Into<String>) -> SalesError {
    SalesError::Usage(msg.into())
}

fn parse_args(raw: &[String]) -> Result<Args, SalesError> {
    let mut it = raw.iter();
    let command = it.next().ok_or_else(|| usage(USAGE))?.clone();
    let mut a = Args {
        command,
        sales_db: None,
        archive_db: None,
        limit: None,
    };
    while let Some(flag) = it.next() {
        let mut value = || {
            it.next()
                .cloned()
                .ok_or_else(|| usage(format!("{flag} needs a value")))
        };
        match flag.as_str() {
            "--sales-db" => a.sales_db = Some(value()?.into()),
            "--archive-db" => a.archive_db = Some(value()?.into()),
            "--limit" => {
                let v = value()?;
                a.limit =
                    Some(v.parse::<usize>().ok().filter(|n| *n > 0).ok_or_else(|| {
                        usage(format!("--limit: {v:?} is not a positive number"))
                    })?);
            }
            other => return Err(usage(format!("unknown argument {other:?}\n{USAGE}"))),
        }
    }
    Ok(a)
}

fn env_number(
    name: &str,
    default: u64,
    range: std::ops::RangeInclusive<u64>,
) -> Result<u64, SalesError> {
    match env::var(name) {
        Err(_) => Ok(default),
        Ok(v) => v
            .trim()
            .parse::<u64>()
            .ok()
            .filter(|n| range.contains(n))
            .ok_or_else(|| usage(format!("{name}={v:?} must be in {range:?}"))),
    }
}

fn tzkt(stop: Arc<AtomicBool>, runtime: &tokio::runtime::Handle) -> Result<Tzkt, SalesError> {
    let raw = env::var("SEEDED_TZKT_URL").unwrap_or_else(|_| "https://api.tzkt.io".into());
    let base = api_base(&raw).ok_or_else(|| {
        usage(format!(
            "SEEDED_TZKT_URL={raw:?} must be an http(s) URL such as https://api.tzkt.io"
        ))
    })?;
    let timeout = env_number("SEEDED_TZKT_TIMEOUT_SECS", 30, 1..=600)?;
    let fetch = Arc::new(
        ReqwestFetch::new(runtime.clone(), Duration::from_secs(timeout))
            .map_err(|e| SalesError::Io(format!("HTTP client: {e}")))?,
    );
    Ok(Tzkt::new(Client::new(
        fetch,
        &base,
        RetryPolicy::default(),
        PACE,
        stop,
    )))
}

fn need<'a>(v: &'a Option<PathBuf>, flag: &str) -> Result<&'a Path, SalesError> {
    v.as_deref()
        .ok_or_else(|| usage(format!("{flag} is required\n{USAGE}")))
}

/// The archive, read-only, with the sales database attached.
fn archive_with_sales(archive: &Path, sales: &Path) -> Result<Connection, SalesError> {
    if !archive.is_file() {
        return Err(usage(format!("{} does not exist", archive.display())));
    }
    let conn = Connection::open_with_flags(
        archive,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    conn.busy_timeout(Duration::from_secs(5))?;
    store::attach(&conn, sales)?;
    Ok(conn)
}

fn print_coverage(archive: &Path, sales: &Path) -> Result<(), SalesError> {
    let conn = archive_with_sales(archive, sales)?;
    let c = store::coverage(&conn)?;
    println!(
        "archive coverage: {} rows, {} on visible tokens, {} on flagged projects, {} tokens unknown",
        c.rows, c.visible, c.hidden, c.unknown
    );
    Ok(())
}

fn file_size(path: &Path) -> String {
    let wal = PathBuf::from(format!("{}-wal", path.display()));
    let bytes = |p: &Path| std::fs::metadata(p).map_or(0, |m| m.len());
    format!(
        "{:.1} MiB (+ WAL {:.1} MiB)",
        bytes(path) as f64 / 1_048_576.0,
        bytes(&wal) as f64 / 1_048_576.0
    )
}

fn run(
    a: &Args,
    stop: Arc<AtomicBool>,
    runtime: &tokio::runtime::Handle,
) -> Result<Outcome, SalesError> {
    let sales_db = need(&a.sales_db, "--sales-db")?;
    match a.command.as_str() {
        "sync" => {
            let archive = need(&a.archive_db, "--archive-db")?;
            if !archive.is_file() {
                return Err(usage(format!("{} does not exist", archive.display())));
            }
            let api = tzkt(stop, runtime)?;
            let opts = SyncOptions {
                limit: a.limit,
                concurrency: env_number("SEEDED_TZKT_CONCURRENCY", 4, 1..=16)? as usize,
                page: env_number("SEEDED_SALES_PAGE", 10_000, 100..=10_000)? as usize,
                confirmations: env_number("SEEDED_SALES_CONFIRMATIONS", FINALITY, FINALITY..=1000)?,
            };
            let log = |m: &str| eprintln!("{m}");
            let r = sync(sales_db, &api, &opts, &log)?;
            println!("sync: head {}, safe level {}", r.head, r.safe_level);
            for s in &r.streams {
                let state = match (&s.error, s.done, s.limited, s.interrupted) {
                    (Some(e), ..) => format!("failed: {e}"),
                    (None, true, ..) => "synced".into(),
                    (None, _, true, _) => "limited".into(),
                    (None, _, _, true) => "interrupted".into(),
                    _ => "paused".into(),
                };
                println!(
                    "  {} [{}]: {} pages, {} ops, {} sales, {} issues, {} deposits, {} returns, \
                     level {}, {state}",
                    s.label,
                    s.stream,
                    s.pages,
                    s.ops,
                    s.sales,
                    s.issues,
                    s.deposits,
                    s.returns,
                    s.level.map_or_else(|| "-".into(), |l| l.to_string())
                );
            }
            let c = r.checks();
            println!(
                "  buy-now price checks: amount = payout {}, amount != payout {}",
                c.amount_equals_payout, c.amount_differs_from_payout
            );
            println!(
                "  requests {}, retries {}, sales db {}",
                api.client.counters.requests.load(Ordering::Relaxed),
                api.client.counters.retries.load(Ordering::Relaxed),
                file_size(sales_db)
            );
            if r.permanent_failures() > 0 {
                return Err(SalesError::Api(ApiError::Permanent(format!(
                    "{} streams failed",
                    r.permanent_failures()
                ))));
            }
            if r.interrupted() {
                return Ok(Outcome::Interrupted);
            }
            if r.transient_failures() > 0 {
                println!(
                    "  {} transient failures; run again to resume",
                    r.transient_failures()
                );
                return Ok(Outcome::Transient);
            }
            if r.limited() {
                println!("  stopped at --limit; run again to continue");
            }
            print_coverage(archive, sales_db)?;
            Ok(Outcome::Done)
        }
        "stats" => {
            let conn = schema::open_read_only(sales_db)?;
            print!("{}", store::stats(&conn)?);
            println!("  file {}", file_size(sales_db));
            if let Some(archive) = a.archive_db.as_deref() {
                print_coverage(archive, sales_db)?;
            }
            Ok(Outcome::Done)
        }
        other => Err(usage(format!("unknown command {other:?}\n{USAGE}"))),
    }
}

/// First SIGINT/SIGTERM: stop between pages. Second: exit now (the open transaction rolls
/// back; committed pages and their cursors stay).
async fn watch_signals(stop: Arc<AtomicBool>) {
    let mut term = match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
        Ok(s) => Some(s),
        Err(e) => {
            eprintln!("warning: no SIGTERM handler: {e}");
            None
        }
    };
    loop {
        let received = tokio::select! {
            r = tokio::signal::ctrl_c() => r.is_ok(),
            _ = async {
                match term.as_mut() {
                    Some(t) => {
                        t.recv().await;
                    }
                    None => std::future::pending::<()>().await,
                }
            } => true,
        };
        if !received {
            eprintln!("warning: no SIGINT handler");
            return;
        }
        if stop.swap(true, Ordering::SeqCst) {
            eprintln!("second signal: exiting now");
            std::process::exit(130);
        }
        eprintln!("stopping after the current page; signal again to exit now");
    }
}

fn main() -> ExitCode {
    let started = Instant::now();
    let stop = Arc::new(AtomicBool::new(false));
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name("seeded-sales-io")
        .enable_all()
        .build()
    {
        Ok(r) => r,
        Err(e) => {
            eprintln!("error: cannot start the I/O runtime: {e}");
            return ExitCode::from(1);
        }
    };
    runtime.spawn(watch_signals(stop.clone()));
    let raw: Vec<String> = env::args().skip(1).collect();
    let code = match parse_args(&raw).and_then(|a| run(&a, stop, runtime.handle())) {
        Ok(Outcome::Done) => 0,
        Ok(Outcome::Transient) => 75,
        Ok(Outcome::Interrupted) => {
            eprintln!("interrupted; progress up to the last committed page is kept");
            130
        }
        Err(e) => {
            eprintln!("error: {e}");
            match e {
                SalesError::Usage(_) => 64,
                e if e.is_transient() => 75,
                e if e.is_interrupted() => 130,
                _ => 1,
            }
        }
    };
    let rss =
        peak_rss_kib().map_or_else(|| "n/a".into(), |k| format!("{:.1} MiB", k as f64 / 1024.0));
    eprintln!(
        "wall {:.1}s, peak RSS {rss}",
        started.elapsed().as_secs_f64()
    );
    runtime.shutdown_background();
    ExitCode::from(code)
}
