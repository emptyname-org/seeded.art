//! seeded-index: catalogue ingestion for seeded.art.
//!
//! ```text
//! seeded-index bootstrap --frank <dir> --db <path>
//! seeded-index sync --db <path> [--contract <KT1>]... [--limit N]
//! seeded-index verify --db <path> [--sample N]
//! seeded-index stats --db <path>
//! seeded-index upgrade --db <path>
//! seeded-index escrow --db <path> [--limit N]
//! seeded-index offers --db <path> [--limit N]
//! seeded-index identities --db <path>
//! seeded-index reconcile --db <path>
//! seeded-index profiles --db <path> [--source fxhash|objkt|tzprofiles|all] [--limit N]
//! seeded-index volumes --db <path> --sales-db <path>
//! seeded-index names --db <path>
//! seeded-index mirror-list --db <path> --out <file.json>
//! seeded-index mirror --list <file.json> [--mirror <dir>] [--max-bytes N] [--limit N]
//!     [--concurrency N] [--project ID] [--recheck] [--start-threshold SIZE]
//!     [--threshold-step SIZE]
//! seeded-index mirror-status [--mirror <dir>]
//! ```
//!
//! `upgrade` migrates to the current schema and rebuilds derived data (content roots,
//! verification, curated identities, catalogue reconcile) without network. `reconcile` also
//! asks TzKT for mints outside each contract's known token ids (two bounded queries each) and
//! queues every missing token as retry work; the next `sync` fetches them by id. `escrow` reads only marketplace listing
//! big maps from TzKT. `offers` reads the fxhash v2 offer big maps, then block times of listings
//! and offers. `sync` covers the three token contracts. `profiles` imports names, bios,
//! links and avatar URIs of the addresses that matter from the fxhash user registry (TzKT plus
//! IPFS documents) and objkt; Tezos Profiles is no longer resolvable and is skipped.
//! `volumes` recomputes per-project trading volume from the sales database (read-only, no
//! network) and replaces it in one transaction; re-running is idempotent. `names` numbers the
//! names several addresses carry (see `store::profile`); re-running is idempotent.
//! `mirror-list` writes the visible projects, most traded first, from a database opened
//! read-only. `mirror` copies their complete generator packages into a mirror directory on
//! any host, without a catalogue: verified IPFS blocks and ONCHFS records, within
//! `--max-bytes`, two packages at a time by default; `status.sqlite` there makes it
//! resumable (see `ingest::mirror`). Packages go in tiered passes, smallest first: up to
//! `--start-threshold` (1MiB), then `--threshold-step` (1MiB) more per pass, until the cap
//! is full or nothing is left; larger ones wait for their pass. SIZE is bytes or a number
//! with KiB, MiB or GiB. `mirror-status` reports it. The web app serves that
//! directory first when `SEEDED_MIRROR` names it.
//!
//! Environment: SEEDED_TZKT_URL (https://api.tzkt.io, `/v1` optional),
//! SEEDED_TZKT_CONCURRENCY (4),
//! SEEDED_TZKT_TIMEOUT_SECS (30), SEEDED_SYNC_BATCH (500); for `profiles`:
//! SEEDED_IPFS_GATEWAYS (comma-separated `https://host/ipfs/`, default the first two content
//! gateways), SEEDED_OBJKT_URL (https://data.objkt.com/v3/graphql),
//! SEEDED_PROFILE_CONCURRENCY (4); for `mirror`: SEEDED_MIRROR (var/mirror),
//! SEEDED_MIRROR_BYTES (50 GiB), SEEDED_IPFS_GATEWAYS (the content gateways),
//! SEEDED_MAX_OBJECT_BYTES (32 MiB per file).
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

use seeded_archive::config::Config;
use seeded_archive::content::{ContentStore, MirrorWriter};
use seeded_archive::ingest::escrow::{EscrowOptions, sync_escrows};
use seeded_archive::ingest::http::{Client, ReqwestFetch, RetryPolicy};
use seeded_archive::ingest::mirror::{
    MirrorOptions, MirrorStatus, Tiers, read_list, run_mirror, write_list,
};
use seeded_archive::ingest::offers::{OfferOptions, offer_stats, sync_offers};
use seeded_archive::ingest::profiles::{
    ProfileClients, ProfileSource, ProfilesOptions, sync_profiles,
};
use seeded_archive::ingest::sync::{SyncOptions, sync};
use seeded_archive::ingest::tzkt::{Tzkt, api_base};
use seeded_archive::ingest::verify::{VerifyOptions, verify};
use seeded_archive::ingest::{IngestError, frank, identity, peak_rss_kib, stats, upgrade};
use seeded_archive::store::Store;

const USAGE: &str = "usage:
  seeded-index bootstrap --frank <dir> --db <path>
  seeded-index sync --db <path> [--contract <KT1>]... [--limit N]
  seeded-index verify --db <path> [--sample N]
  seeded-index stats --db <path>
  seeded-index upgrade --db <path>
  seeded-index escrow --db <path> [--limit N]
  seeded-index offers --db <path> [--limit N]
  seeded-index identities --db <path>
  seeded-index reconcile --db <path>
  seeded-index profiles --db <path> [--source fxhash|objkt|tzprofiles|all] [--limit N]
  seeded-index volumes --db <path> --sales-db <path>
  seeded-index names --db <path>
  seeded-index mirror-list --db <path> --out <file.json>
  seeded-index mirror --list <file.json> [--mirror <dir>] [--max-bytes N] [--limit N]
      [--concurrency N] [--project ID] [--recheck] [--start-threshold SIZE]
      [--threshold-step SIZE]
  seeded-index mirror-status [--mirror <dir>]";
/// Minimum spacing between TzKT request starts, across workers.
const PACE: Duration = Duration::from_millis(100);
/// Minimum spacing between request starts per IPFS gateway, and for objkt.
const IPFS_PACE: Duration = Duration::from_millis(50);
const OBJKT_PACE: Duration = Duration::from_millis(250);
const OBJKT_URL: &str = "https://data.objkt.com/v3/graphql";

enum Outcome {
    Done,
    Transient,
    Interrupted,
}

struct Args {
    command: String,
    frank: Option<PathBuf>,
    db: Option<PathBuf>,
    sales_db: Option<PathBuf>,
    contracts: Vec<String>,
    limit: Option<usize>,
    sample: Option<usize>,
    source: Option<String>,
    list: Option<PathBuf>,
    out: Option<PathBuf>,
    mirror: Option<PathBuf>,
    max_bytes: Option<u64>,
    concurrency: Option<usize>,
    project: Option<u64>,
    recheck: bool,
    start_threshold: Option<u64>,
    threshold_step: Option<u64>,
}

fn usage(msg: impl Into<String>) -> IngestError {
    IngestError::Usage(msg.into())
}

fn parse_args(raw: &[String]) -> Result<Args, IngestError> {
    let mut it = raw.iter();
    let command = it.next().ok_or_else(|| usage(USAGE))?.clone();
    let mut a = Args {
        command,
        frank: None,
        db: None,
        sales_db: None,
        contracts: Vec::new(),
        limit: None,
        sample: None,
        source: None,
        list: None,
        out: None,
        mirror: None,
        max_bytes: None,
        concurrency: None,
        project: None,
        recheck: false,
        start_threshold: None,
        threshold_step: None,
    };
    while let Some(flag) = it.next() {
        if flag == "--recheck" {
            a.recheck = true;
            continue;
        }
        let mut value = || {
            it.next()
                .cloned()
                .ok_or_else(|| usage(format!("{flag} needs a value")))
        };
        let number = |v: String| {
            v.parse::<usize>()
                .map_err(|_| usage(format!("{flag}: {v:?} is not a number")))
        };
        let large = |v: String| {
            v.parse::<u64>()
                .map_err(|_| usage(format!("{flag}: {v:?} is not a number")))
        };
        match flag.as_str() {
            "--frank" => a.frank = Some(value()?.into()),
            "--db" => a.db = Some(value()?.into()),
            "--sales-db" => a.sales_db = Some(value()?.into()),
            "--contract" => a.contracts.push(value()?),
            "--limit" => a.limit = Some(number(value()?)?),
            "--sample" => a.sample = Some(number(value()?)?),
            "--source" => a.source = Some(value()?),
            "--list" => a.list = Some(value()?.into()),
            "--out" => a.out = Some(value()?.into()),
            "--mirror" => a.mirror = Some(value()?.into()),
            "--max-bytes" => a.max_bytes = Some(large(value()?)?),
            "--concurrency" => a.concurrency = Some(number(value()?)?),
            "--project" => a.project = Some(large(value()?)?),
            "--start-threshold" => a.start_threshold = Some(size(flag, &value()?)?),
            "--threshold-step" => a.threshold_step = Some(size(flag, &value()?)?),
            other => return Err(usage(format!("unknown argument {other:?}\n{USAGE}"))),
        }
    }
    Ok(a)
}

/// Bytes, or a positive number with a KiB, MiB or GiB suffix.
fn size(flag: &str, v: &str) -> Result<u64, IngestError> {
    let (digits, unit) = match v.find(|c: char| !c.is_ascii_digit()) {
        Some(at) => v.split_at(at),
        None => (v, ""),
    };
    let shift = match unit {
        "" => 0,
        "KiB" => 10,
        "MiB" => 20,
        "GiB" => 30,
        _ => return Err(usage(format!("{flag}: {v:?} is not a size"))),
    };
    digits
        .parse::<u64>()
        .ok()
        .and_then(|n| n.checked_mul(1 << shift))
        .filter(|n| *n > 0)
        .ok_or_else(|| usage(format!("{flag}: {v:?} is not a positive size")))
}

fn env_number(
    name: &str,
    default: u64,
    range: std::ops::RangeInclusive<u64>,
) -> Result<u64, IngestError> {
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

fn tzkt(stop: Arc<AtomicBool>, runtime: &tokio::runtime::Handle) -> Result<Tzkt, IngestError> {
    let raw = env::var("SEEDED_TZKT_URL").unwrap_or_else(|_| "https://api.tzkt.io".into());
    let base = api_base(&raw).ok_or_else(|| {
        usage(format!(
            "SEEDED_TZKT_URL={raw:?} must be an http(s) URL such as https://api.tzkt.io"
        ))
    })?;
    let timeout = env_number("SEEDED_TZKT_TIMEOUT_SECS", 30, 1..=600)?;
    let fetch = Arc::new(
        ReqwestFetch::new(runtime.clone(), Duration::from_secs(timeout))
            .map_err(|e| IngestError::Io(format!("HTTP client: {e}")))?,
    );
    Ok(Tzkt::new(Client::new(
        fetch,
        &base,
        RetryPolicy::default(),
        PACE,
        stop,
    )))
}

/// HTTP client for one non-TzKT endpoint; `max_body` bounds each answer.
fn endpoint(
    base: &str,
    max_body: u64,
    retry: RetryPolicy,
    pace: Duration,
    stop: &Arc<AtomicBool>,
    runtime: &tokio::runtime::Handle,
) -> Result<Client, IngestError> {
    let fetch = ReqwestFetch::new(runtime.clone(), Duration::from_secs(30))
        .map_err(|e| IngestError::Io(format!("HTTP client: {e}")))?
        .with_max_body(max_body);
    Ok(Client::new(
        Arc::new(fetch),
        base,
        retry,
        pace,
        stop.clone(),
    ))
}

/// IPFS gateways (`https://host/ipfs/`) for profile documents, in fallback order.
fn ipfs_gateways(
    stop: &Arc<AtomicBool>,
    runtime: &tokio::runtime::Handle,
) -> Result<Vec<Client>, IngestError> {
    let list: Vec<String> = match env::var("SEEDED_IPFS_GATEWAYS") {
        Ok(v) => v
            .split(',')
            .map(str::trim)
            .filter(|g| !g.is_empty())
            .map(str::to_owned)
            .collect(),
        Err(_) => seeded_archive::content::DEFAULT_IPFS_GATEWAYS
            .iter()
            .take(2)
            .map(|g| (*g).to_owned())
            .collect(),
    };
    if list.is_empty() || list.len() > 8 {
        return Err(usage("SEEDED_IPFS_GATEWAYS must name 1..=8 gateways"));
    }
    // Two quick attempts per gateway, then the next gateway.
    let retry = RetryPolicy {
        attempts: 2,
        base_delay: Duration::from_secs(1),
        max_delay: Duration::from_secs(4),
    };
    list.iter()
        .map(|g| {
            if !g.starts_with("https://") || !g.ends_with("/ipfs/") || g.contains(['?', '#', '@']) {
                return Err(usage(format!(
                    "SEEDED_IPFS_GATEWAYS entry {g:?} must look like https://host/ipfs/"
                )));
            }
            endpoint(g, 1 << 20, retry, IPFS_PACE, stop, runtime)
        })
        .collect()
}

fn need<'a>(v: &'a Option<PathBuf>, flag: &str) -> Result<&'a Path, IngestError> {
    v.as_deref()
        .ok_or_else(|| usage(format!("{flag} is required\n{USAGE}")))
}

fn existing_db(a: &Args) -> Result<Store, IngestError> {
    let db = need(&a.db, "--db")?;
    if !db.is_file() {
        return Err(usage(format!("{} does not exist", db.display())));
    }
    Ok(Store::open(db)?)
}

/// Creates the database file owner-only before SQLite opens it.
fn create_db(db: &Path) -> Result<Store, IngestError> {
    let mut file = std::fs::OpenOptions::new();
    file.write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        file.mode(0o600);
    }
    file.open(db)
        .map_err(|e| IngestError::Io(format!("{}: {e}", db.display())))?;
    Ok(Store::open(db)?)
}

fn run(
    a: &Args,
    stop: Arc<AtomicBool>,
    runtime: &tokio::runtime::Handle,
) -> Result<Outcome, IngestError> {
    let log = |m: &str| eprintln!("{m}");
    match a.command.as_str() {
        "bootstrap" => {
            let frank_dir = need(&a.frank, "--frank")?;
            let mut store = create_db(need(&a.db, "--db")?)?;
            let r = frank::bootstrap(&mut store, frank_dir)?;
            println!(
                "bootstrap {}: {}",
                if r.changed { "imported" } else { "unchanged" },
                r.source
            );
            println!("  content sha256 {}", r.content_sha256);
            if r.changed {
                println!(
                    "  projects {} (hidden {}), artists {}, users {}, collaboration contracts {}",
                    r.projects, r.hidden, r.artists, r.users, r.collaborations
                );
                println!(
                    "  catalogue tokens {} (without seed {})",
                    r.catalogue_tokens, r.without_seed
                );
                for (why, n) in &r.rejected {
                    println!("  rejected {why}: {n}");
                }
            }
            Ok(Outcome::Done)
        }
        "sync" => {
            let mut store = existing_db(a)?;
            let api = tzkt(stop, runtime)?;
            let opts = SyncOptions {
                contracts: a.contracts.clone(),
                limit: a.limit,
                concurrency: env_number("SEEDED_TZKT_CONCURRENCY", 4, 1..=16)? as usize,
                batch: env_number("SEEDED_SYNC_BATCH", 500, 1..=10_000)?,
            };
            let r = sync(&mut store, &api, &opts, &log)?;
            println!(
                "sync: {} batches, {} tokens written, owners set {} / cleared {}, \
                 catalogue mismatches {}",
                r.batches, r.tokens_written, r.owners_set, r.owners_cleared, r.mismatches
            );
            for (kind, n) in &r.issues {
                println!("  issue {kind}: {n}");
            }
            println!("  completed: {:?}", r.completed);
            println!("  pending: {:?}", r.pending);
            if r.limited {
                println!("  stopped at --limit; run again to continue");
            }
            if r.interrupted {
                return Ok(Outcome::Interrupted);
            }
            if r.transient_failures > 0 {
                println!(
                    "  {} transient failures; run again to resume",
                    r.transient_failures
                );
                return Ok(Outcome::Transient);
            }
            Ok(Outcome::Done)
        }
        "verify" => {
            let mut store = existing_db(a)?;
            let api = tzkt(stop, runtime)?;
            let opts = VerifyOptions {
                sample: a.sample.unwrap_or(100),
                absent_limit: 1000,
                seed: 0x5eed_eda7,
            };
            let r = verify(&mut store, Some(&api), &opts)?;
            println!("verify local: {:?}", r.local);
            println!(
                "  absence checked {}, confirmed absent {}",
                r.absent_checked, r.absent_confirmed
            );
            println!(
                "  live sample {}: {:?}; stale stored rows {}",
                r.sampled, r.sample, r.stale_rows
            );
            println!("  catalogue totals: {:?}", r.totals);
            for e in &r.examples {
                println!("  mismatch {e}");
            }
            if r.transient > 0 {
                println!(
                    "  {} lookups failed transiently; nothing recorded for them",
                    r.transient
                );
                return Ok(Outcome::Transient);
            }
            Ok(Outcome::Done)
        }
        "stats" => {
            let store = existing_db(a)?;
            print!("{}", stats::stats(&store, need(&a.db, "--db")?)?);
            Ok(Outcome::Done)
        }
        "upgrade" => {
            let mut store = existing_db(a)?;
            let r = upgrade::upgrade(&mut store)?;
            println!(
                "upgrade: schema v{}, content roots {}, re-compared {:?}",
                r.schema_version, r.content_roots, r.recompared
            );
            println!(
                "  identities {} (artists {}, projects {}, tokens {})",
                r.identities.identities,
                r.identities.artists,
                r.identities.projects,
                r.identities.tokens
            );
            println!("  {}", names_line(&r.names));
            println!(
                "  reconcile: {} catalogue candidates queued for retry",
                r.reconcile.catalogue_candidates
            );
            Ok(Outcome::Done)
        }
        "reconcile" => {
            let mut store = existing_db(a)?;
            let api = tzkt(stop, runtime)?;
            let r = upgrade::reconcile_store(&mut store, &api)?;
            println!(
                "reconcile: {} catalogue candidates, {} boundary mints queued for retry",
                r.catalogue_candidates, r.boundary_candidates
            );
            Ok(Outcome::Done)
        }
        "identities" => {
            let mut store = existing_db(a)?;
            let r = identity::apply_to(&mut store)?;
            println!(
                "identities {} (artists {}, projects {}, tokens {})",
                r.identities, r.artists, r.projects, r.tokens
            );
            Ok(Outcome::Done)
        }
        "profiles" => {
            let mut store = existing_db(a)?;
            let sources =
                ProfileSource::parse(a.source.as_deref().unwrap_or("all")).ok_or_else(|| {
                    usage(format!(
                        "--source must be fxhash, objkt, tzprofiles or all\n{USAGE}"
                    ))
                })?;
            let api = tzkt(stop.clone(), runtime)?;
            let ipfs = ipfs_gateways(&stop, runtime)?;
            let objkt_url = env::var("SEEDED_OBJKT_URL").unwrap_or_else(|_| OBJKT_URL.into());
            if !objkt_url.starts_with("https://") && !objkt_url.starts_with("http://") {
                return Err(usage("SEEDED_OBJKT_URL must be an http(s) URL"));
            }
            let objkt = endpoint(
                &objkt_url,
                16 << 20,
                RetryPolicy::default(),
                OBJKT_PACE,
                &stop,
                runtime,
            )?;
            let opts = ProfilesOptions {
                sources,
                limit: a.limit,
                concurrency: env_number("SEEDED_PROFILE_CONCURRENCY", 4, 1..=16)? as usize,
                ..ProfilesOptions::default()
            };
            let clients = ProfileClients {
                tzkt: &api,
                ipfs: &ipfs,
                objkt: &objkt,
            };
            let r = sync_profiles(&mut store, &clients, &opts, &log)?;
            for (source, s) in &r.sources {
                println!(
                    "profiles {}: registry keys {}, fetched {}, written {}, removed {}, \
                     invalid {}, unavailable {}, pending {}, pass {}",
                    source.as_str(),
                    s.registry_keys,
                    s.fetched,
                    s.written,
                    s.removed,
                    s.invalid,
                    s.unavailable,
                    s.pending,
                    if s.pass_complete {
                        "complete"
                    } else {
                        "in progress"
                    }
                );
                if let Some(note) = &s.note {
                    println!("  {note}");
                }
            }
            if r.limited {
                println!("  stopped at --limit; run again to continue");
            }
            if r.interrupted {
                return Ok(Outcome::Interrupted);
            }
            if r.transient {
                println!("  a source was unreachable; run again to resume");
                return Ok(Outcome::Transient);
            }
            Ok(Outcome::Done)
        }
        "escrow" => {
            let mut store = existing_db(a)?;
            let api = tzkt(stop, runtime)?;
            let opts = EscrowOptions {
                limit: a.limit,
                ..EscrowOptions::default()
            };
            let r = sync_escrows(&mut store, &api, &opts, &log)?;
            println!(
                "escrow: {} pages, listings {} live / {} removed or not live",
                r.batches, r.listings_live, r.listings_gone
            );
            println!("  completed: {:?}", r.completed);
            println!("  pending: {:?}", r.pending);
            if r.limited {
                println!("  stopped at --limit; run again to continue");
            }
            if r.interrupted {
                return Ok(Outcome::Interrupted);
            }
            if r.transient_failures > 0 {
                println!("  transient failure; run again to resume");
                return Ok(Outcome::Transient);
            }
            Ok(Outcome::Done)
        }
        "offers" => {
            let mut store = existing_db(a)?;
            let api = tzkt(stop, runtime)?;
            let opts = OfferOptions {
                limit: a.limit,
                ..OfferOptions::default()
            };
            let r = sync_offers(&mut store, &api, &opts, &log)?;
            println!(
                "offers: {} batches, keys {} live / {} ended / {} unsupported, \
                 block times {} written / {} missing",
                r.batches, r.live, r.ended, r.unsupported, r.times_written, r.times_missing
            );
            println!("  completed: {:?}", r.completed);
            println!("  pending: {:?}", r.pending);
            let s = offer_stats(&store)?;
            let tez = |m: u64| format!("{}.{:06} tez", m / 1_000_000, m % 1_000_000);
            println!(
                "  live token offers {} ({}), collection offers {} ({}), buyers {}",
                s.token_offers,
                tez(s.token_locked_mutez),
                s.collection_offers,
                tez(s.collection_locked_mutez),
                s.buyers
            );
            println!(
                "  visible offers {} ({}); listings {} (priced {}, visible {}), sellers {}",
                s.visible_offers,
                tez(s.visible_locked_mutez),
                s.listings,
                s.listings_priced,
                s.visible_listings,
                s.sellers
            );
            println!(
                "  addresses with a listing or offer {} (visible {}); levels without time {}",
                s.addresses, s.visible_addresses, s.times_pending
            );
            if r.limited {
                println!("  stopped at --limit; run again to continue");
            }
            if r.interrupted {
                return Ok(Outcome::Interrupted);
            }
            if r.transient_failures > 0 {
                println!("  transient failure; run again to resume");
                return Ok(Outcome::Transient);
            }
            Ok(Outcome::Done)
        }
        "volumes" => {
            let sales = need(&a.sales_db, "--sales-db")?;
            if !sales.is_file() {
                return Err(usage(format!("{} does not exist", sales.display())));
            }
            let mut store = existing_db(a)?;
            let r = store.refresh_volumes(sales)?;
            let tez = |m: u64| format!("{}.{:06} tez", m / 1_000_000, m % 1_000_000);
            println!(
                "volumes: {} projects ({} visible), {} ({} visible)",
                r.projects,
                r.visible_projects,
                tez(r.volume_mutez),
                tez(r.visible_volume_mutez)
            );
            println!(
                "  primary {}, secondary {}; priced trades {}, unpriced {}",
                tez(r.primary_mutez),
                tez(r.secondary_mutez),
                r.sales_count,
                r.unpriced_count
            );
            println!(
                "  sales rows mapped through the catalogue (unsigned tokens) {}",
                r.catalogue_rows
            );
            println!(
                "  sales rows without an archive token {} ({}); sales level {}, computed {}",
                r.unmapped_rows,
                tez(r.unmapped_mutez),
                r.sales_level
                    .map_or_else(|| "incomplete".to_owned(), |l| l.to_string()),
                r.computed_at
            );
            Ok(Outcome::Done)
        }
        "names" => {
            let mut store = existing_db(a)?;
            println!("{}", names_line(&store.assign_name_numbers()?));
            Ok(Outcome::Done)
        }
        "mirror-list" => {
            let db = need(&a.db, "--db")?;
            if !db.is_file() {
                return Err(usage(format!("{} does not exist", db.display())));
            }
            let out = need(&a.out, "--out")?;
            let (listed, skipped) = write_list(&Store::open_read_only(db)?, out)?;
            println!(
                "mirror-list: {listed} projects to {}; {skipped} without an ipfs:// or onchfs:// root",
                out.display()
            );
            Ok(Outcome::Done)
        }
        "mirror" => {
            let list = read_list(need(&a.list, "--list")?)?;
            let config = Config::from_env().map_err(|e| usage(e.to_string()))?;
            let dir = mirror_dir(a, &config)?;
            let max_bytes = a.max_bytes.unwrap_or(config.mirror_bytes);
            let writer = Arc::new(
                MirrorWriter::open(&dir, max_bytes)
                    .map_err(|e| IngestError::Io(format!("{}: {e}", dir.display())))?,
            );
            // Its own fetches only: no Kubo, no mirror reads, a cache that never writes.
            let mut content = config.content_config();
            content.cache_dir = dir.join("tmp").join("cache");
            content.max_cache_bytes = 0;
            content.mirror_dir = None;
            content.kubo = None;
            let content = ContentStore::new(content).map_err(|e| IngestError::Io(e.to_string()))?;
            let mut status = MirrorStatus::open(&dir)?;
            let opts = MirrorOptions {
                limit: a.limit,
                concurrency: a.concurrency.unwrap_or(2),
                project: a.project,
                recheck: a.recheck,
                tiers: Some(Tiers {
                    start: a.start_threshold.unwrap_or(Tiers::default().start),
                    step: a.threshold_step.unwrap_or(Tiers::default().step),
                }),
                ..MirrorOptions::default()
            };
            let r = runtime.block_on(run_mirror(
                &list,
                &mut status,
                &content,
                &writer,
                &opts,
                &stop,
                &log,
            ))?;
            println!(
                "mirror: listed {}, due {}, started {}; ok {} (stored before {}), pending {}, \
                 missing {}, too_big {}, failed {}, waiting {}, deferred {}",
                r.listed,
                r.due,
                r.started,
                r.ok,
                r.local,
                r.pending,
                r.missing,
                r.too_big,
                r.failed,
                r.waiting,
                r.deferred
            );
            println!(
                "  {} new, packages {}; mirror {} of {}, {} objects",
                bytes(r.new_bytes),
                bytes(r.package_bytes),
                bytes(writer.used_bytes()),
                bytes(writer.max_bytes()),
                writer.objects()
            );
            if r.threshold > 0 {
                println!(
                    "  threshold {}; {} due packages left",
                    bytes(r.threshold),
                    r.left
                );
            }
            if r.cap_reached {
                println!("  stopped at the byte cap");
            }
            if r.limited {
                println!("  stopped at --limit; run again to continue");
            }
            if r.interrupted {
                return Ok(Outcome::Interrupted);
            }
            if r.pending > 0 || r.deferred > 0 {
                println!("  pending packages are retried by a later run after their backoff");
                return Ok(Outcome::Transient);
            }
            Ok(Outcome::Done)
        }
        "mirror-status" => {
            let config = Config::from_env().map_err(|e| usage(e.to_string()))?;
            let dir = mirror_dir(a, &config)?;
            let r = MirrorStatus::open_read_only(&dir)?.report(10)?;
            let tez = |m: u64| format!("{}.{:06} tez", m / 1_000_000, m % 1_000_000);
            println!(
                "mirror-status {}: listed {} ({}), unvisited {}",
                dir.display(),
                r.listed,
                tez(r.listed_volume_mutez),
                r.unvisited
            );
            for (state, rows, size, files) in &r.states {
                println!(
                    "  {:<8} {rows:>6}  files {files:>8}  {}",
                    state.as_str(),
                    bytes(*size)
                );
            }
            let share = if r.listed_volume_mutez == 0 {
                0.0
            } else {
                r.ok_volume_mutez as f64 * 100.0 / r.listed_volume_mutez as f64
            };
            println!(
                "  volume mirrored: {} ({share:.1}%)",
                tez(r.ok_volume_mutez)
            );
            for (key, value) in &r.meta {
                let shown = match key.as_str() {
                    "used_bytes" | "max_bytes" | "threshold" => {
                        value.parse().map_or_else(|_| value.clone(), bytes)
                    }
                    _ => value.clone(),
                };
                println!("  {key}: {shown}");
            }
            if !r.top_errors.is_empty() {
                println!("  top failures:");
            }
            for (state, error, rows) in &r.top_errors {
                println!("  {rows:>6}  {:<8} {error}", state.as_str());
            }
            Ok(Outcome::Done)
        }
        other => Err(usage(format!("unknown command {other:?}\n{USAGE}"))),
    }
}

fn mirror_dir(a: &Args, config: &Config) -> Result<PathBuf, IngestError> {
    a.mirror
        .clone()
        .or_else(|| config.mirror.clone())
        .ok_or_else(|| usage(format!("--mirror is required\n{USAGE}")))
}

fn bytes(n: u64) -> String {
    if n < 1 << 30 {
        format!("{:.1} MiB", n as f64 / (1u64 << 20) as f64)
    } else {
        format!("{:.2} GiB", n as f64 / (1u64 << 30) as f64)
    }
}

fn names_line(r: &seeded_archive::store::NamesNumbered) -> String {
    format!(
        "names: shared {}, numbered {}, assigned {}, moved {}, tombstones {}; \
         plain held by claimed {}, curated {}, credited {}, other {}",
        r.shared_names,
        r.numbered,
        r.assigned,
        r.moved,
        r.tombstones,
        r.plain_claimed,
        r.plain_curated,
        r.plain_credited,
        r.plain_other
    )
}

/// First SIGINT/SIGTERM: stop between batches. Second: exit now (the open transaction rolls
/// back; committed batches and their cursors stay).
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
        eprintln!("stopping after the current batch; signal again to exit now");
    }
}

fn main() -> ExitCode {
    let started = Instant::now();
    let stop = Arc::new(AtomicBool::new(false));
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name("seeded-index-io")
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
            eprintln!("interrupted; progress up to the last committed batch is kept");
            130
        }
        Err(e) => {
            eprintln!("error: {e}");
            match e {
                IngestError::Usage(_) => 64,
                e if e.is_transient() => 75,
                IngestError::Api(seeded_archive::ingest::http::ApiError::Interrupted) => 130,
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
