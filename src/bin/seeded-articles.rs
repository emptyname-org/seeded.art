//! seeded-articles: fx(text) article archive for seeded.art.
//!
//! ```text
//! seeded-articles sync --db <path> [--limit N] [--recheck] [--archive-db <path>]
//! seeded-articles refs --db <path> [--archive-db <path>]
//! seeded-articles stats --db <path>
//! ```
//!
//! `sync` re-reads fx(text) chain facts from TzKT (tokens, metadata pointers, ledger holders,
//! marketplace listings, moderation), then fetches metadata JSON and article
//! bodies from IPFS that are not stored yet or failed transiently. `--limit N` caps the IPFS
//! objects fetched this run. `--recheck` also retries absent objects and looks for verified
//! copies of unverified ones. Production path: var/articles.sqlite.
//!
//! References from bodies to fxhash projects and tokens are extracted as bodies are stored
//! (and backfilled when an older database is migrated). `--archive-db` names the catalogue
//! (`seeded.sqlite`, opened read-only, current schema) to resolve them with: tokens and slugs
//! to projects, generator roots to projects, and project visibility. It also copies the name
//! the site displays for each author (`Store::display_names`), which search matches; the web
//! labels authors from the catalogue too, so search and display agree as of that run. Without
//! `--archive-db` the copied names are kept. `refs` does only the local part: migrate,
//! extract, resolve, copy names; no network.
//!
//! Environment: SEEDED_TZKT_URL (https://api.tzkt.io, `/v1` optional),
//! SEEDED_IPFS_GATEWAYS (comma-separated, each ending in /ipfs/; default: the content layer's
//! order), SEEDED_ARTICLES_CONCURRENCY (4), SEEDED_ARTICLES_TIMEOUT_SECS (30),
//! SEEDED_ARTICLES_MAX_BYTES (16777216, per object).
//!
//! Exit codes: 0 done (or stopped at --limit), 1 failure, 64 usage, 75 transient failures
//! (re-run to resume), 130 interrupted (finished fetches are kept).

use std::env;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use seeded_archive::articles::http::{JsonClient, ReqwestFetch, RetryPolicy, tzkt_api_base};
use seeded_archive::articles::ipfs::Fetcher;
use seeded_archive::articles::names::{NamesReport, open_catalogue};
use seeded_archive::articles::peak_rss_kib;
use seeded_archive::articles::refs::open_archive;
use seeded_archive::articles::store::{ArticleStats, ArticleStore, ResolveReport};
use seeded_archive::articles::sync::{
    SyncError, SyncOptions, default_gateways, parse_gateways, sync,
};
use seeded_archive::articles::tzkt::{MAX_PAGE, Tzkt};

const USAGE: &str = "usage:
  seeded-articles sync --db <path> [--limit N] [--recheck] [--archive-db <path>]
  seeded-articles refs --db <path> [--archive-db <path>]
  seeded-articles stats --db <path>";
/// Minimum spacing between TzKT request starts.
const PACE: Duration = Duration::from_millis(100);

enum Outcome {
    Done,
    Transient,
    Interrupted,
}

#[derive(Debug, thiserror::Error)]
enum CliError {
    #[error("{0}")]
    Usage(String),
    #[error(transparent)]
    Sync(#[from] SyncError),
    #[error("{0}")]
    Io(String),
}

impl From<seeded_archive::articles::store::ArticleStoreError> for CliError {
    fn from(e: seeded_archive::articles::store::ArticleStoreError) -> Self {
        CliError::Sync(e.into())
    }
}

struct Args {
    command: String,
    db: Option<PathBuf>,
    limit: Option<usize>,
    recheck: bool,
    archive_db: Option<PathBuf>,
}

fn usage(msg: impl Into<String>) -> CliError {
    CliError::Usage(msg.into())
}

fn parse_args(raw: &[String]) -> Result<Args, CliError> {
    let mut it = raw.iter();
    let command = it.next().ok_or_else(|| usage(USAGE))?.clone();
    let mut a = Args {
        command,
        db: None,
        limit: None,
        recheck: false,
        archive_db: None,
    };
    while let Some(flag) = it.next() {
        let mut value = || {
            it.next()
                .cloned()
                .ok_or_else(|| usage(format!("{flag} needs a value")))
        };
        match flag.as_str() {
            "--db" => a.db = Some(value()?.into()),
            "--limit" => {
                let v = value()?;
                a.limit = Some(
                    v.parse::<usize>()
                        .map_err(|_| usage(format!("--limit: {v:?} is not a number")))?,
                );
            }
            "--recheck" => a.recheck = true,
            "--archive-db" => a.archive_db = Some(value()?.into()),
            other => return Err(usage(format!("unknown argument {other:?}\n{USAGE}"))),
        }
    }
    Ok(a)
}

fn env_number(
    name: &str,
    default: u64,
    range: std::ops::RangeInclusive<u64>,
) -> Result<u64, CliError> {
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

fn need_db(a: &Args) -> Result<&Path, CliError> {
    a.db.as_deref()
        .ok_or_else(|| usage(format!("--db is required\n{USAGE}")))
}

/// Creates the database file owner-only before SQLite opens it.
fn open_db(db: &Path) -> Result<ArticleStore, CliError> {
    let mut file = std::fs::OpenOptions::new();
    file.write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        file.mode(0o600);
    }
    file.open(db)
        .map_err(|e| CliError::Io(format!("{}: {e}", db.display())))?;
    Ok(ArticleStore::open(db)?)
}

fn file_size(p: &Path) -> u64 {
    std::fs::metadata(p).map(|m| m.len()).unwrap_or(0)
}

fn mib(bytes: u64) -> String {
    format!("{:.1} MiB", bytes as f64 / (1024.0 * 1024.0))
}

fn print_resolve(r: &ResolveReport) {
    println!(
        "  references resolved: {} targets looked up, {} rows resolved, {} generator rows \
         added, {} projects checked, {} rows unresolved",
        r.looked_up, r.resolved, r.generators, r.projects_checked, r.unresolved
    );
}

fn print_names(r: &NamesReport) {
    println!(
        "  names: {} authors, {} named, {} changed, {} index rows rewritten",
        r.authors, r.named, r.changed, r.reindexed
    );
}

fn print_refs(s: &ArticleStats) {
    println!(
        "references {} rows in {} articles ({} visible articles link visible projects); \
         {} unresolved",
        s.refs, s.ref_articles, s.ref_articles_visible, s.refs_unresolved
    );
    println!(
        "  projects {} ({} visible, {} flagged, {} not in the catalogue); tokens {}",
        s.ref_projects,
        s.ref_projects_visible,
        s.ref_projects_hidden,
        s.ref_projects
            .saturating_sub(s.ref_projects_visible + s.ref_projects_hidden),
        s.ref_tokens
    );
    for (kind, form, rows, articles) in &s.refs_by_form {
        println!("  {kind} {form}: {rows} rows, {articles} articles");
    }
}

fn run(
    a: &Args,
    stop: Arc<AtomicBool>,
    runtime: &tokio::runtime::Handle,
) -> Result<Outcome, CliError> {
    let log = |m: &str| eprintln!("{m}");
    match a.command.as_str() {
        "sync" => {
            let db = need_db(a)?;
            let raw = env::var("SEEDED_TZKT_URL").unwrap_or_else(|_| "https://api.tzkt.io".into());
            let base = tzkt_api_base(&raw).ok_or_else(|| {
                usage(format!(
                    "SEEDED_TZKT_URL={raw:?} must be an http(s) URL such as https://api.tzkt.io"
                ))
            })?;
            let gateways = match env::var("SEEDED_IPFS_GATEWAYS") {
                Ok(list) => parse_gateways(&list).map_err(usage)?,
                Err(_) => default_gateways(),
            };
            let timeout = env_number("SEEDED_ARTICLES_TIMEOUT_SECS", 30, 1..=600)?;
            let cap = env_number("SEEDED_ARTICLES_MAX_BYTES", 16 << 20, 1024..=(256 << 20))?;
            let concurrency = env_number("SEEDED_ARTICLES_CONCURRENCY", 4, 1..=32)? as usize;
            let fetch = Arc::new(
                ReqwestFetch::new(runtime.clone(), Duration::from_secs(timeout))
                    .map_err(|e| CliError::Io(format!("HTTP client: {e}")))?,
            );
            let mut store = open_db(db)?;
            let client = JsonClient::new(
                fetch.clone(),
                &base,
                RetryPolicy::default(),
                PACE,
                stop.clone(),
            );
            let tzkt = Tzkt {
                client: &client,
                page: MAX_PAGE,
            };
            let fetcher = Fetcher::new(fetch, gateways, cap, stop.clone());
            let opts = SyncOptions {
                limit: a.limit,
                concurrency,
                recheck: a.recheck,
                archive: a.archive_db.clone(),
            };
            let r = sync(&mut store, &tzkt, &fetcher, &opts, &stop, &log)?;
            println!(
                "sync: {} articles ({} skipped), {} holder rows, {} listings, {} moderation",
                r.chain.articles,
                r.chain.skipped,
                r.chain.holders,
                r.chain.listings,
                r.chain.moderation
            );
            if let Some(names) = &r.names {
                print_names(names);
            }
            for (what, f) in [("metadata", &r.metadata), ("bodies", &r.bodies)] {
                println!(
                    "  {what}: {} fetched: {} verified, {} unverified, {} transient \
                     ({} not found everywhere), {} absent",
                    f.attempted, f.ok_verified, f.ok_unverified, f.transient, f.not_found, f.absent
                );
            }
            println!(
                "  requests: TzKT {} ({} retries), IPFS {}",
                client.requests.load(Ordering::Relaxed),
                client.retries.load(Ordering::Relaxed),
                fetcher.requests.load(Ordering::Relaxed)
            );
            if let Some(refs) = &r.refs {
                print_resolve(refs);
            }
            if r.left > 0 {
                println!("  {} objects left; run again to continue", r.left);
            }
            if r.interrupted {
                return Ok(Outcome::Interrupted);
            }
            if r.transient() > 0 {
                println!("  {} transient failures; run again to retry", r.transient());
                return Ok(Outcome::Transient);
            }
            Ok(Outcome::Done)
        }
        "refs" => {
            let db = need_db(a)?;
            if !db.is_file() {
                return Err(usage(format!("{} does not exist", db.display())));
            }
            let archive = a.archive_db.as_deref().map(open_archive).transpose()?;
            let catalogue = a.archive_db.as_deref().map(open_catalogue).transpose()?;
            let t = Instant::now();
            let mut store = ArticleStore::open(db)?;
            let opened = t.elapsed();
            let t = Instant::now();
            let extracted = store.refresh_refs()?;
            println!(
                "open and migrate {:.2}s; {extracted} articles extracted again ({:.2}s)",
                opened.as_secs_f64(),
                t.elapsed().as_secs_f64()
            );
            if let Some(archive) = &archive {
                let t = Instant::now();
                let r = store.resolve_refs(archive)?;
                print_resolve(&r);
                println!("  resolution {:.2}s", t.elapsed().as_secs_f64());
            }
            if let Some(catalogue) = &catalogue {
                let t = Instant::now();
                print_names(&store.resolve_names(catalogue)?);
                println!("  names {:.2}s", t.elapsed().as_secs_f64());
            }
            print_refs(&store.stats()?);
            Ok(Outcome::Done)
        }
        "stats" => {
            let db = need_db(a)?;
            if !db.is_file() {
                return Err(usage(format!("{} does not exist", db.display())));
            }
            let store = ArticleStore::open_read_only(db)?;
            let s = store.stats()?;
            println!(
                "articles {} (visible {}, hidden {}); metadata ipfs {}, tzkt {}, none {}",
                s.articles, s.visible, s.hidden, s.metadata_ipfs, s.metadata_tzkt, s.metadata_none
            );
            println!(
                "article bodies: ok {}, pending {}, transient {}, absent {}",
                s.articles_body_ok,
                s.articles_body_pending,
                s.articles_body_transient,
                s.articles_body_absent
            );
            for (kind, state, verification, n, bytes) in &s.objects {
                println!(
                    "objects {kind} {state} {}: {n} ({bytes} bytes)",
                    verification.as_deref().unwrap_or("-")
                );
            }
            println!(
                "body text {} bytes (largest {}); media {} ({} ipfs)",
                s.body_bytes, s.body_max_bytes, s.media, s.media_ipfs
            );
            println!(
                "holders {} rows, {} distinct; listings {} ({} editions); authors {} ({} named)",
                s.holder_rows,
                s.distinct_holders,
                s.listings,
                s.listed_editions,
                s.authors,
                s.named_authors
            );
            print_refs(&s);
            println!(
                "chain level {}, synced at {}",
                s.chain_level.map_or("-".into(), |l| l.to_string()),
                s.chain_synced_at.map_or("-".into(), |t| t.to_string())
            );
            let wal = PathBuf::from(format!("{}-wal", db.display()));
            println!(
                "database {} (+ WAL {})",
                mib(file_size(db)),
                mib(file_size(&wal))
            );
            Ok(Outcome::Done)
        }
        other => Err(usage(format!("unknown command {other:?}\n{USAGE}"))),
    }
}

/// First SIGINT/SIGTERM: stop after in-flight fetches. Second: exit now (committed batches
/// stay; the open transaction rolls back).
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
        eprintln!("stopping after in-flight fetches; signal again to exit now");
    }
}

fn main() -> ExitCode {
    let started = Instant::now();
    let stop = Arc::new(AtomicBool::new(false));
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name("seeded-articles-io")
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
            eprintln!("interrupted; finished fetches are kept");
            130
        }
        Err(e) => {
            eprintln!("error: {e}");
            match e {
                CliError::Usage(_) => 64,
                CliError::Sync(e) if e.is_interrupted() => 130,
                CliError::Sync(e) if e.is_transient() => 75,
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
