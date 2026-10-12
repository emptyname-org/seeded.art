//! Marketplace test helpers: recorded offer keys and block times on top of the ingest fake.
#![allow(dead_code)] // each test file uses a different subset

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use seeded_archive::ingest::escrow::{EscrowOptions, sync_escrows};
use seeded_archive::ingest::http::{Client, Fetch, HttpResponse, RetryPolicy, TransportError};
use seeded_archive::ingest::offers::{OfferOptions, OfferReport, sync_offers};
use seeded_archive::ingest::sync::{SyncOptions, sync};
use seeded_archive::ingest::tzkt::Tzkt;
use seeded_archive::store::Store;
use serde_json::{Value, json};

use crate::ingest_support::{FakeTzkt, bootstrapped, read_json, root, test_dir};

pub const MARKET_V2: &str = "KT1GbyoDi7H1sfXmimXpptZJuCdHMh66WS9u";
pub const MARKET_V1: &str = "KT1Xo5B7PNBAeynZPmca4bRh6LQow4og1Zb9";
/// Buyer of the recorded token offer 79120 on Arboreal (fx(params) #188628).
pub const BUYER: &str = "tz1P719ymGCtNwavZKh2LmhMs53dGsEMzjnm";
/// Seller of the recorded Arboreal listing 1106836.
pub const ARBOREAL_SELLER: &str = "tz1Vkoqo2ZQsVXBniYJDfX6F2NMfH5RoEuGn";
pub const KETITAKI: &str = "tz1WMa1cAKk4JjFGZCpXNkTDi5o5AqCsFF6E";

/// The ingest fake plus TzKT's `/blocks` over recorded block times.
pub struct MarketFake {
    pub inner: Arc<FakeTzkt>,
    pub blocks: Mutex<BTreeMap<u64, String>>,
    /// Appended to every `/blocks` answer (to test unrequested rows).
    pub extra_block: Mutex<Option<Value>>,
    /// Fields merged into key `.0` when it is read by `key.in` (a different TzKT node).
    pub recheck_patch: Mutex<Option<(String, Value)>>,
}

impl MarketFake {
    /// Recorded tokens, mints, balances and listings, plus recorded offer keys and block times.
    pub fn recorded() -> Arc<MarketFake> {
        let inner = FakeTzkt::recorded();
        let dir = root().join("tests/fixtures/market");
        let Value::Array(offers) = read_json(&dir.join("offers.json")) else {
            panic!("offers.json is not an array");
        };
        inner.with(|s| s.listings.extend(offers));
        let Value::Array(blocks) = read_json(&dir.join("blocks.json")) else {
            panic!("blocks.json is not an array");
        };
        let blocks = blocks
            .iter()
            .map(|b| {
                (
                    b["level"].as_u64().unwrap(),
                    b["timestamp"].as_str().unwrap().to_owned(),
                )
            })
            .collect();
        Arc::new(MarketFake {
            inner,
            blocks: Mutex::new(blocks),
            extra_block: Mutex::new(None),
            recheck_patch: Mutex::new(None),
        })
    }

    /// Adds a block time for `level` (derived test data).
    pub fn block(&self, level: u64, time: &str) {
        self.blocks.lock().unwrap().insert(level, time.to_owned());
    }

    /// Edits the recorded offer key `key` of big map `path`.
    pub fn edit_offer(&self, path: &str, key: &str, edit: impl FnOnce(&mut Value)) {
        self.inner.with(|s| {
            let row = s
                .listings
                .iter_mut()
                .find(|r| r["_path"] == path && r["key"] == key)
                .unwrap_or_else(|| panic!("no {path} key {key}"));
            edit(row);
        });
    }

    pub fn push_key(&self, row: Value) {
        self.inner.with(|s| s.listings.push(row));
    }

    pub fn head(&self) -> u64 {
        self.inner.with(|s| s.head)
    }

    /// Moves head forward by `n` and returns the new head.
    pub fn advance(&self, n: u64) -> u64 {
        self.inner.with(|s| {
            s.head += n;
            s.head
        })
    }
}

impl Fetch for MarketFake {
    fn get(&self, url: &str) -> Result<HttpResponse, TransportError> {
        let rest = url.split_once("/v1").map_or(url, |(_, r)| r);
        let (path, query) = rest.split_once('?').unwrap_or((rest, ""));
        // Big-map keys by `key.in` (or `key=` for one), which the inner fake does not filter.
        // Like TzKT, `.in` on a JSON field needs at least two items.
        let filter = query.split('&').find_map(|p| {
            p.strip_prefix("key.in=")
                .map(|v| (true, v))
                .or_else(|| p.strip_prefix("key=").map(|v| (false, v)))
        });
        if let Some((list, keys)) = filter {
            if list && !keys.contains(',') {
                return Ok(HttpResponse {
                    status: 400,
                    body: br#"{"code":400,"errors":{"key.in":"JSON array must contain at least two items."}}"#
                        .to_vec(),
                });
            }
            let keys: Vec<&str> = keys.split(',').collect();
            let base = url.split_once('?').map_or(url, |(b, _)| b);
            let rest: Vec<&str> = query
                .split('&')
                .filter(|p| {
                    !p.starts_with("key.in=") && !p.starts_with("key=") && !p.starts_with("limit=")
                })
                .collect();
            let inner = self
                .inner
                .get(&format!("{base}?{}&limit=100000", rest.join("&")))?;
            if inner.status != 200 {
                return Ok(inner);
            }
            let rows: Vec<Value> = serde_json::from_slice(&inner.body).unwrap();
            let patch = self.recheck_patch.lock().unwrap().clone();
            let rows: Vec<Value> = rows
                .into_iter()
                .filter(|r| r["key"].as_str().is_some_and(|k| keys.contains(&k)))
                .map(|mut r| {
                    if let Some((key, fields)) = &patch {
                        if r["key"] == key.as_str() {
                            for (k, v) in fields.as_object().unwrap() {
                                r[k] = v.clone();
                            }
                        }
                    }
                    r
                })
                .collect();
            return Ok(HttpResponse {
                status: 200,
                body: serde_json::to_vec(&rows).unwrap(),
            });
        }
        // The inner fake records the request and applies faults; it has no `/blocks`.
        let inner = self.inner.get(url)?;
        if path != "/blocks" || inner.status != 404 {
            return Ok(inner);
        }
        let mut levels: Vec<u64> = Vec::new();
        let mut limit = 100;
        for pair in query.split('&') {
            match pair.split_once('=') {
                Some(("level.in", v)) => {
                    levels = v.split(',').map(|l| l.parse().unwrap()).collect()
                }
                Some(("limit", v)) => limit = v.parse().unwrap(),
                _ => {}
            }
        }
        levels.sort_unstable();
        let blocks = self.blocks.lock().unwrap();
        let mut rows: Vec<Value> = levels
            .iter()
            .filter_map(|l| blocks.get(l).map(|t| json!({"level": l, "timestamp": t})))
            .take(limit)
            .collect();
        rows.extend(self.extra_block.lock().unwrap().clone());
        Ok(HttpResponse {
            status: 200,
            body: serde_json::to_vec(&rows).unwrap(),
        })
    }
}

pub fn market_api(fake: &Arc<MarketFake>, stop: &Arc<AtomicBool>) -> Tzkt {
    let retry = RetryPolicy {
        attempts: 3,
        base_delay: Duration::ZERO,
        max_delay: Duration::ZERO,
    };
    Tzkt::new(Client::new(
        fake.clone(),
        "http://fake.test/v1",
        retry,
        Duration::ZERO,
        stop.clone(),
    ))
}

pub fn offers(store: &mut Store, api: &Tzkt) -> OfferReport {
    sync_offers(store, api, &OfferOptions::default(), &|_| {}).unwrap()
}

pub fn offers_paged(
    store: &mut Store,
    api: &Tzkt,
    page: usize,
    limit: Option<usize>,
) -> OfferReport {
    sync_offers(store, api, &OfferOptions { limit, page }, &|_| {}).unwrap()
}

/// A bootstrapped, synced and escrow-resolved store with offers indexed, in `tmp/ingest-tests/<name>`.
pub fn indexed(name: &str) -> (std::path::PathBuf, Store, Arc<MarketFake>, Tzkt) {
    let dir = test_dir(name);
    let mut store = bootstrapped(&dir);
    let fake = MarketFake::recorded();
    let stop = Arc::new(AtomicBool::new(false));
    let api = market_api(&fake, &stop);
    let o = SyncOptions {
        contracts: Vec::new(),
        limit: None,
        concurrency: 2,
        batch: 10_000,
    };
    sync(&mut store, &api, &o, &|_| {}).unwrap();
    sync_escrows(&mut store, &api, &EscrowOptions::default(), &|_| {}).unwrap();
    let r = offers(&mut store, &api);
    assert_eq!(r.transient_failures, 0);
    (dir, store, fake, api)
}

pub fn side(dir: &Path) -> rusqlite::Connection {
    rusqlite::Connection::open(dir.join("db.sqlite")).unwrap()
}

pub fn one(c: &rusqlite::Connection, sql: &str) -> i64 {
    c.query_row(sql, [], |r| r.get(0)).unwrap()
}

/// A derived offer key in TzKT's shape.
pub fn offer_key(
    path: &str,
    id: u64,
    key: &str,
    value: Value,
    active: bool,
    first: u64,
    last: u64,
) -> Value {
    json!({
        "_escrow": MARKET_V2, "_path": path, "id": id, "active": active, "key": key,
        "value": value, "firstLevel": first, "lastLevel": last, "updates": 1
    })
}
