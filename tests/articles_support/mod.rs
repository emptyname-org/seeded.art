//! Offline TzKT and IPFS gateways serving recorded fixtures (tests/fixtures/articles).
#![allow(dead_code)]

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use seeded_archive::articles::http::{
    Fetch, HttpResponse, JsonClient, RetryPolicy, TransportError,
};
use seeded_archive::articles::ipfs::Fetcher;
use seeded_archive::articles::store::ArticleStore;
use seeded_archive::articles::sync::{SyncError, SyncOptions, SyncReport, sync};
use seeded_archive::articles::tzkt::Tzkt;
use serde_json::Value;

pub const TZKT: &str = "http://tzkt.test/v1";
pub const GW1: &str = "http://gw1.test/ipfs/";
pub const GW2: &str = "http://gw2.test/ipfs/";
pub const CAR: &str = "application/vnd.ipld.car";

/// How a gateway answers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Serves recorded CARs; 404 for unknown CIDs.
    Normal,
    /// Recorded CARs with one byte of the last block flipped.
    Corrupt,
    /// Ignores `format=car` and serves plain bytes (`.plain` fixtures or `extra_plain`).
    Plain,
    /// HTTP 503.
    Down,
    /// Connection timeout.
    Timeout,
    /// HTTP 404 for everything.
    NotFound,
}

/// `tests/fixtures/articles`, from the real crate or the scratch crate two levels below it.
pub fn fixtures() -> PathBuf {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    ["tests/fixtures/articles", "../../tests/fixtures/articles"]
        .iter()
        .map(|p| root.join(p))
        .find(|p| p.is_dir())
        .expect("fixtures directory")
}

fn json_file(name: &str) -> Vec<Value> {
    let text = std::fs::read_to_string(fixtures().join("tzkt").join(name)).expect(name);
    match serde_json::from_str(&text).expect(name) {
        Value::Array(rows) => rows,
        other => vec![other],
    }
}

pub fn car_bytes(cid: &str) -> Option<Vec<u8>> {
    std::fs::read(fixtures().join("ipfs").join(format!("{cid}.car"))).ok()
}

pub fn plain_bytes(cid: &str) -> Option<Vec<u8>> {
    std::fs::read(fixtures().join("ipfs").join(format!("{cid}.plain"))).ok()
}

fn query(url: &str) -> HashMap<String, String> {
    url.split_once('?')
        .map(|(_, q)| q)
        .unwrap_or_default()
        .split('&')
        .filter_map(|kv| kv.split_once('='))
        .map(|(k, v)| (k.to_owned(), v.to_owned()))
        .collect()
}

pub struct FixtureNet {
    pub modes: Mutex<HashMap<String, Mode>>,
    /// Per-CID overrides of the gateway mode, any gateway.
    pub cid_modes: Mutex<HashMap<String, Mode>>,
    /// Extra plain bodies by CID (for synthetic raw CIDs).
    pub extra_plain: Mutex<HashMap<String, Vec<u8>>>,
    /// Extra CAR files by CID.
    pub extra_car: Mutex<HashMap<String, Vec<u8>>>,
    pub requests: Mutex<Vec<String>>,
    pub ipfs_requests: AtomicUsize,
    /// Set `stop` once this many IPFS requests were answered.
    pub stop_after: Mutex<Option<(usize, Arc<AtomicBool>)>>,
    /// Serve only the first N recorded tokens (a lagging API).
    pub token_limit: Mutex<Option<usize>>,
}

impl FixtureNet {
    pub fn new() -> Arc<FixtureNet> {
        let mut modes = HashMap::new();
        modes.insert(GW1.to_owned(), Mode::Normal);
        modes.insert(GW2.to_owned(), Mode::Normal);
        Arc::new(FixtureNet {
            modes: Mutex::new(modes),
            cid_modes: Mutex::new(HashMap::new()),
            extra_plain: Mutex::new(HashMap::new()),
            extra_car: Mutex::new(HashMap::new()),
            requests: Mutex::new(Vec::new()),
            ipfs_requests: AtomicUsize::new(0),
            stop_after: Mutex::new(None),
            token_limit: Mutex::new(None),
        })
    }

    pub fn set(&self, gateway: &str, mode: Mode) {
        self.modes.lock().unwrap().insert(gateway.to_owned(), mode);
    }

    pub fn set_cid(&self, cid: &str, mode: Mode) {
        self.cid_modes.lock().unwrap().insert(cid.to_owned(), mode);
    }

    pub fn clear_cids(&self) {
        self.cid_modes.lock().unwrap().clear();
    }

    /// Requests whose URL contains `needle`.
    pub fn count(&self, needle: &str) -> usize {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|u| u.contains(needle))
            .count()
    }

    fn ok(content_type: &str, body: Vec<u8>) -> Result<HttpResponse, TransportError> {
        Ok(HttpResponse {
            status: 200,
            content_type: Some(content_type.to_owned()),
            body,
        })
    }

    fn status(status: u16) -> Result<HttpResponse, TransportError> {
        Ok(HttpResponse {
            status,
            content_type: Some("text/plain".into()),
            body: Vec::new(),
        })
    }

    fn tzkt(&self, url: &str) -> Result<HttpResponse, TransportError> {
        let path = url
            .strip_prefix(TZKT)
            .unwrap()
            .split('?')
            .next()
            .unwrap()
            .to_owned();
        let q = query(url);
        let (rows, cursor) = match path.as_str() {
            "/head" => {
                let head = json_file("head.json").remove(0);
                return Self::ok("application/json", head.to_string().into_bytes());
            }
            "/tokens" => {
                let mut rows = json_file("tokens.json");
                if let Some(n) = *self.token_limit.lock().unwrap() {
                    rows.truncate(n);
                }
                (rows, "id.gt")
            }
            "/tokens/balances" => (json_file("balances.json"), "id.gt"),
            p if p.starts_with("/contracts/") => {
                let parts: Vec<&str> = p.split('/').collect();
                let (contract, map) = (parts[2], parts[4]);
                let file = match (contract, map) {
                    (_, "token_metadata") => "token_metadata.json",
                    (_, "token_data") => "token_data.json",
                    (_, "asset_contracts") => "asset_contracts.json",
                    (_, "listings") => "listings.json",
                    (_, "reasons") => "moderation_reasons.json",
                    (_, "tokens") => "moderation_tokens.json",
                    (_, "users_name") => "users_name.json",
                    _ => return Self::status(404),
                };
                let mut rows = json_file(file);
                if let Some(keys) = q.get("key.in") {
                    let keys: Vec<&str> = keys.split(',').collect();
                    rows.retain(|r| r["key"].as_str().is_some_and(|k| keys.contains(&k)));
                }
                (rows, "offset.cr")
            }
            _ => return Self::status(404),
        };
        let after: u64 = q.get(cursor).and_then(|v| v.parse().ok()).unwrap_or(0);
        let limit: usize = q.get("limit").and_then(|v| v.parse().ok()).unwrap_or(100);
        let mut rows: Vec<Value> = rows
            .into_iter()
            .filter(|r| r["id"].as_u64().unwrap_or(0) > after)
            .collect();
        rows.sort_by_key(|r| r["id"].as_u64().unwrap_or(0));
        rows.truncate(limit);
        Self::ok(
            "application/json",
            Value::Array(rows).to_string().into_bytes(),
        )
    }

    fn ipfs(&self, gateway: &str, rest: &str) -> Result<HttpResponse, TransportError> {
        let (cid_path, q) = rest.split_once('?').unwrap_or((rest, ""));
        let cid = cid_path.split('/').next().unwrap_or_default().to_owned();
        let wants_car = q.contains("format=car");
        let mode = self
            .cid_modes
            .lock()
            .unwrap()
            .get(&cid)
            .copied()
            .unwrap_or_else(|| self.modes.lock().unwrap()[gateway]);
        let car = || {
            car_bytes(&cid)
                .or_else(|| car_bytes(&format!("directory-{cid}")))
                .or_else(|| self.extra_car.lock().unwrap().get(&cid).cloned())
        };
        let plain =
            || plain_bytes(&cid).or_else(|| self.extra_plain.lock().unwrap().get(&cid).cloned());
        match mode {
            Mode::Down => Self::status(503),
            Mode::NotFound => Self::status(404),
            Mode::Timeout => Err(TransportError::Transient("operation timed out".into())),
            Mode::Plain => match plain() {
                Some(b) => Self::ok("text/plain", b),
                None => Self::status(404),
            },
            Mode::Normal | Mode::Corrupt if wants_car => match car() {
                Some(mut b) => {
                    if mode == Mode::Corrupt {
                        let last = b.len() - 1;
                        b[last] ^= 0x01;
                    }
                    Self::ok(CAR, b)
                }
                None => Self::status(404),
            },
            Mode::Normal | Mode::Corrupt => match plain() {
                Some(b) => Self::ok("text/plain", b),
                None => Self::status(404),
            },
        }
    }
}

impl Fetch for FixtureNet {
    fn get(&self, url: &str, _accept: &str, cap: u64) -> Result<HttpResponse, TransportError> {
        self.requests.lock().unwrap().push(url.to_owned());
        let reply = if url.starts_with(TZKT) {
            self.tzkt(url)
        } else {
            let gateway = [GW1, GW2]
                .into_iter()
                .find(|g| url.starts_with(g))
                .expect("unknown host");
            let r = self.ipfs(gateway, &url[gateway.len()..]);
            let n = self.ipfs_requests.fetch_add(1, Ordering::SeqCst) + 1;
            if let Some((after, stop)) = self.stop_after.lock().unwrap().as_ref() {
                if n >= *after {
                    stop.store(true, Ordering::SeqCst);
                }
            }
            r
        };
        match reply {
            Ok(r) if r.body.len() as u64 > cap => Err(TransportError::TooLarge(cap)),
            other => other,
        }
    }
}

pub struct Harness {
    pub net: Arc<FixtureNet>,
    pub store: ArticleStore,
    pub stop: Arc<AtomicBool>,
    pub cap: u64,
}

impl Harness {
    pub fn new() -> Harness {
        Harness {
            net: FixtureNet::new(),
            store: ArticleStore::open_in_memory().unwrap(),
            stop: Arc::new(AtomicBool::new(false)),
            cap: 1 << 20,
        }
    }

    pub fn fetcher(&self) -> Fetcher {
        Fetcher::new(
            self.net.clone(),
            vec![GW1.into(), GW2.into()],
            self.cap,
            self.stop.clone(),
        )
    }

    pub fn sync(&mut self, opts: &SyncOptions) -> Result<SyncReport, SyncError> {
        let client = JsonClient::new(
            self.net.clone(),
            TZKT,
            RetryPolicy::none(),
            Duration::ZERO,
            self.stop.clone(),
        );
        // Small pages exercise every paging loop.
        let tzkt = Tzkt {
            client: &client,
            page: 3,
        };
        let fetcher = self.fetcher();
        sync(&mut self.store, &tzkt, &fetcher, opts, &self.stop, &|_| {})
    }

    pub fn sync_default(&mut self) -> SyncReport {
        self.sync(&SyncOptions {
            concurrency: 2,
            ..SyncOptions::default()
        })
        .unwrap()
    }
}

// ---- synthetic articles and catalogues (reference tests) ----

use cid::Cid;
use cid::multihash::Multihash;
use seeded_archive::articles::ipfs::{Outcome, Verification, ipfs_ref};
use seeded_archive::articles::store::{ObjectKind, PendingObject};
use seeded_archive::articles::tzkt::{
    Balance, ChainSnapshot, Listing, Moderation, Token, TokenData,
};
use sha2::{Digest, Sha256};

/// A fresh path under the crate's `tmp/articles-tests/` (never the system /tmp), unique per
/// call so parallel tests never share a file.
pub fn scratch(name: &str) -> PathBuf {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let root = fixtures().join("../../../tmp/articles-tests");
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join(format!(
        "{name}-{}-{}.sqlite",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
    }
    path
}

pub fn remove_db(path: &std::path::Path) {
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
    }
}

/// Raw-codec CIDv1 of `bytes`.
pub fn raw_cid(bytes: &[u8]) -> String {
    let mh = Multihash::<64>::wrap(0x12, &Sha256::digest(bytes)).unwrap();
    Cid::new_v1(0x55, mh).to_string()
}

/// One synthetic article.
#[derive(Clone)]
pub struct Art {
    pub id: u64,
    pub author: &'static str,
    pub body: String,
    /// Moderated MALICIOUS.
    pub hidden: bool,
}

pub fn art(id: u64, author: &'static str, body: &str) -> Art {
    Art {
        id,
        author,
        body: body.to_owned(),
        hidden: false,
    }
}

fn body_uri(body: &str) -> String {
    format!("ipfs://{}", raw_cid(body.as_bytes()))
}

/// Chain facts for `arts`: TzKT's metadata copy names each body; `balances` are
/// `(token, address, amount)`, `listings` `(listing, token, seller, amount)`.
pub fn snapshot(
    arts: &[Art],
    balances: &[(u64, &str, i64)],
    listings: &[(u64, u64, &str, i64)],
) -> ChainSnapshot {
    ChainSnapshot {
        level: 5_000_000,
        tokens: arts
            .iter()
            .map(|a| Token {
                token_id: a.id,
                first_level: 1_000 + a.id,
                first_time: "2023-01-01T00:00:00Z".into(),
                supply: 10,
                minted: 10,
                burned: 0,
                metadata: Some(serde_json::json!({
                    "name": format!("Article {}", a.id),
                    "artifactUri": body_uri(&a.body),
                })),
            })
            .collect(),
        token_data: arts
            .iter()
            .map(|a| TokenData {
                token_id: a.id,
                minter: a.author.into(),
                royalties: Some(100),
                metadata_locked: false,
                splits: Vec::new(),
            })
            .collect(),
        balances: balances
            .iter()
            .enumerate()
            .map(|(i, (token_id, address, amount))| Balance {
                id: i as u64 + 1,
                token_id: *token_id,
                address: (*address).into(),
                amount: *amount,
                last_level: 4_000_000,
            })
            .collect(),
        listings: listings
            .iter()
            .map(|(listing_id, token_id, seller, amount)| Listing {
                listing_id: *listing_id,
                token_id: *token_id,
                seller: (*seller).into(),
                amount: *amount,
                price_mutez: Some(1_000_000),
                last_level: 4_000_000,
            })
            .collect(),
        moderation: arts
            .iter()
            .filter(|a| a.hidden)
            .map(|a| Moderation {
                token_id: a.id,
                state: 4,
                reason_id: None,
                last_level: 4_000_000,
            })
            .collect(),
        ..ChainSnapshot::default()
    }
}

/// Stores each article's body as a fetched object.
pub fn store_bodies(store: &mut ArticleStore, arts: &[Art]) {
    let results: Vec<(PendingObject, Outcome)> = arts
        .iter()
        .map(|a| {
            (
                PendingObject {
                    key: ipfs_ref(&body_uri(&a.body)).unwrap().key,
                    kind: ObjectKind::Body,
                },
                Outcome::Ok {
                    data: a.body.clone().into_bytes(),
                    verification: Verification::Sha256,
                    gateway: GW1.into(),
                },
            )
        })
        .collect();
    store.record_objects(&results).unwrap();
}

/// An in-memory articles store holding `arts`.
pub fn synthetic(
    arts: &[Art],
    balances: &[(u64, &str, i64)],
    listings: &[(u64, u64, &str, i64)],
) -> ArticleStore {
    let mut store = ArticleStore::open_in_memory().unwrap();
    store
        .apply_snapshot(&snapshot(arts, balances, listings))
        .unwrap();
    store_bodies(&mut store, arts);
    store
}

/// A catalogue project: `(id, slug, hidden, artifact_uri)`.
pub type CatProject<'a> = (u64, Option<&'a str>, bool, Option<&'a str>);
/// A catalogue token: `(contract, token_id, project_id, iteration)`.
pub type CatToken<'a> = (&'a str, u64, u64, u64);

/// A real catalogue file (the app's own schema) holding only `projects` and `tokens`.
pub fn catalogue(path: &std::path::Path, projects: &[CatProject<'_>], tokens: &[CatToken<'_>]) {
    remove_db(path);
    drop(seeded_archive::store::Store::open(path).unwrap());
    let c = rusqlite::Connection::open(path).unwrap();
    for (id, slug, hidden, artifact) in projects {
        c.execute(
            "INSERT INTO projects (id, title, title_key, artist_key, index_state, slug, hidden, \
             flag, artifact_uri, generative_uri) \
             VALUES (?1, 'P', 'p', 'a', 'indexed', ?2, ?3, ?4, ?5, ?5)",
            rusqlite::params![
                *id as i64,
                slug,
                *hidden,
                if *hidden { "MALICIOUS" } else { "NONE" },
                artifact
            ],
        )
        .unwrap();
    }
    for (contract, token_id, project, iteration) in tokens {
        c.execute(
            "INSERT INTO tokens (contract, token_id, project_id, iteration, seed, minter, \
             minted, artifact_uri) VALUES (?1, ?2, ?3, ?4, 'oo', 'tz1minter', \
             '2023-01-01T00:00:00Z', 'ipfs://QmToken')",
            rusqlite::params![
                contract,
                *token_id as i64,
                *project as i64,
                *iteration as i64
            ],
        )
        .unwrap();
    }
}

/// Where a catalogue name comes from (see `store::profile`'s name rule).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NameSource {
    /// A local claim (`profile_edits`).
    Claimed,
    /// A curated identity.
    Curated,
    /// The fxhash user registry, as imported (`profiles_imported`, source `fxhash`).
    Registry,
    Objkt,
}

/// Sets names in a catalogue file (raw rows, as the importers and edits write them).
pub fn set_names(path: &std::path::Path, names: &[(&str, NameSource, &str)]) {
    let c = rusqlite::Connection::open(path).unwrap();
    for (address, source, name) in names {
        let key = seeded_archive::store::name_key(name);
        let sql = match source {
            NameSource::Claimed => {
                "INSERT INTO profile_edits (address, name, name_key, links, updated_at, \
                 signed_message, signature, public_key) \
                 VALUES (?1, ?2, ?3, '[]', '2026-01-01T00:00:00Z', 'm', 's', 'p') \
                 ON CONFLICT (address) DO UPDATE SET name = ?2, name_key = ?3"
            }
            NameSource::Curated => {
                "INSERT INTO identities (address, name, name_key, aliases, provenance) \
                 VALUES (?1, ?2, ?3, '[]', 'test') \
                 ON CONFLICT (address) DO UPDATE SET name = ?2, name_key = ?3"
            }
            NameSource::Registry => {
                "INSERT INTO profiles_imported (address, source, name, name_key, fetched_at) \
                 VALUES (?1, 'fxhash', ?2, ?3, '2026-01-01T00:00:00Z') \
                 ON CONFLICT (address, source) DO UPDATE SET name = ?2, name_key = ?3"
            }
            NameSource::Objkt => {
                "INSERT INTO profiles_imported (address, source, name, name_key, fetched_at) \
                 VALUES (?1, 'objkt', ?2, ?3, '2026-01-01T00:00:00Z') \
                 ON CONFLICT (address, source) DO UPDATE SET name = ?2, name_key = ?3"
            }
        };
        c.execute(sql, rusqlite::params![address, name, key])
            .unwrap();
    }
}

/// Removes one source's name of `address` from a catalogue file.
pub fn remove_name(path: &std::path::Path, address: &str, source: NameSource) {
    let c = rusqlite::Connection::open(path).unwrap();
    let sql = match source {
        NameSource::Claimed => "DELETE FROM profile_edits WHERE address = ?1",
        NameSource::Curated => "DELETE FROM identities WHERE address = ?1",
        NameSource::Registry => {
            "DELETE FROM profiles_imported WHERE address = ?1 AND source = 'fxhash'"
        }
        NameSource::Objkt => {
            "DELETE FROM profiles_imported WHERE address = ?1 AND source = 'objkt'"
        }
    };
    c.execute(sql, [address]).unwrap();
}

/// A catalogue file (the app's own schema) holding only `names`.
pub fn name_catalogue(path: &std::path::Path, names: &[(&str, NameSource, &str)]) {
    catalogue(path, &[], &[]);
    set_names(path, names);
}

/// A sync with the catalogue at `archive`.
pub fn with_archive(archive: &std::path::Path) -> SyncOptions {
    SyncOptions {
        concurrency: 2,
        archive: Some(archive.to_owned()),
        ..SyncOptions::default()
    }
}
