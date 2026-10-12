//! Shared helpers for the ingest tests: fixture paths and an in-memory TzKT.
#![allow(dead_code)] // each test file uses a different subset

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use seeded_archive::ingest::frank::bootstrap;
use seeded_archive::ingest::http::{Client, Fetch, HttpResponse, RetryPolicy, TransportError};
use seeded_archive::ingest::tzkt::Tzkt;
use seeded_archive::store::Store;
use serde_json::{Value, json};

pub const V1: &str = "KT1KEa8z6vWXDJrVqtMrAeDVzsvxat3kHaCE";
pub const V2: &str = "KT1U6EHmNxJTkvaWJ4ThczG4FSDaHC21ssvi";
pub const PARAMS: &str = "KT1EfsNuqwLAWDd3o4pvfUx1CAh5GMdTrRvr";
pub const ARBOREAL: u64 = 29437;
pub const FLAGGED: u64 = 554;
pub const QUADRADO: &str = "tz1N64TWNCZR3yJZmmzBE4DRidN1tCaZCw8Q";
pub const HONEST: &str = "tz1PkLH6efZifS8NtKiZBWFWgDoHDmDwN9f1";

pub fn root() -> PathBuf {
    let mut dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    while !dir.join("tests/fixtures/ingest/frank").is_dir() {
        assert!(
            dir.pop(),
            "project root with tests/fixtures/ingest not found"
        );
    }
    dir
}

pub fn fixtures() -> PathBuf {
    root().join("tests/fixtures/ingest")
}

pub fn frank() -> PathBuf {
    fixtures().join("frank")
}

/// Fresh per-test directory under tmp/ingest-tests/.
pub fn test_dir(name: &str) -> PathBuf {
    let dir = root().join("tmp/ingest-tests").join(name);
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

pub fn copy_dir(src: &Path, dst: &Path) {
    fs::create_dir_all(dst).unwrap();
    for entry in fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let to = dst.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &to);
        } else {
            fs::copy(entry.path(), to).unwrap();
        }
    }
}

pub fn frank_copy(dir: &Path) -> PathBuf {
    let copy = dir.join("frank");
    copy_dir(&frank(), &copy);
    copy
}

pub fn edit_json(path: &Path, edit: impl FnOnce(&mut Value)) {
    let mut v: Value = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
    edit(&mut v);
    fs::write(path, serde_json::to_vec_pretty(&v).unwrap()).unwrap();
}

pub fn read_json(path: &Path) -> Value {
    serde_json::from_slice(&fs::read(path).unwrap()).unwrap()
}

/// A bootstrapped file database in `dir`.
pub fn bootstrapped(dir: &Path) -> Store {
    let mut store = Store::open(&dir.join("db.sqlite")).unwrap();
    assert!(bootstrap(&mut store, &frank()).unwrap().changed);
    store
}

/// A scripted response for requests whose URL contains `contains`.
pub struct Fault {
    pub contains: String,
    pub status: u16,
    /// `None`: every time.
    pub times: Option<usize>,
}

#[derive(Default)]
pub struct FakeState {
    pub head: u64,
    pub tokens: Vec<Value>,
    pub mints: Vec<Value>,
    pub balances: Vec<Value>,
    /// Big-map keys with `_escrow` (contract) and `_path` (big map name).
    pub listings: Vec<Value>,
    pub faults: Vec<Fault>,
    pub requests: Vec<String>,
    /// Sets the flag once this many requests were served.
    pub stop_after: Option<(usize, Arc<AtomicBool>)>,
}

/// An in-memory TzKT over recorded rows, with the filters the indexer uses.
pub struct FakeTzkt {
    pub state: Mutex<FakeState>,
}

impl FakeTzkt {
    pub fn recorded() -> Arc<FakeTzkt> {
        let dir = fixtures().join("tzkt");
        let rows = |f: &str| match read_json(&dir.join(f)) {
            Value::Array(v) => v,
            _ => panic!("{f} is not an array"),
        };
        Arc::new(FakeTzkt {
            state: Mutex::new(FakeState {
                head: read_json(&dir.join("head.json"))["level"].as_u64().unwrap(),
                tokens: rows("tokens.json"),
                mints: rows("mints.json"),
                balances: rows("balances.json"),
                listings: rows("listings.json"),
                ..FakeState::default()
            }),
        })
    }

    pub fn with<T>(&self, f: impl FnOnce(&mut FakeState) -> T) -> T {
        f(&mut self.state.lock().unwrap())
    }

    pub fn requests_containing(&self, needle: &str) -> usize {
        self.with(|s| s.requests.iter().filter(|r| r.contains(needle)).count())
    }
}

fn num(v: &Value) -> Option<u64> {
    match v {
        Value::Number(n) => n.as_u64(),
        Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

fn field<'a>(row: &'a Value, path: &str) -> &'a Value {
    if let Some(v) = row.get(path) {
        return v;
    }
    path.split('.').fold(row, |v, k| &v[k])
}

/// Applies `key[.mode]=value` against `row`, with `map` renaming API keys to row paths.
fn keep(row: &Value, key: &str, value: &str, map: &[(&str, &str)]) -> bool {
    let (name, mode) = match key.rsplit_once('.') {
        Some((n, m)) if ["gt", "ge", "lt", "le", "in", "eq"].contains(&m) => (n, m),
        _ => (key, "eq"),
    };
    let Some((_, path)) = map.iter().find(|(k, _)| *k == name) else {
        return true;
    };
    let actual = field(row, path);
    if let Value::Bool(b) = actual {
        return value == b.to_string();
    }
    if mode == "in" {
        return value.split(',').any(|v| {
            actual.as_str() == Some(v) || num(actual).is_some_and(|a| v.parse() == Ok(a))
        });
    }
    match (num(actual), value.parse::<u64>()) {
        (Some(a), Ok(v)) => match mode {
            "gt" => a > v,
            "ge" => a >= v,
            "lt" => a < v,
            "le" => a <= v,
            _ => a == v,
        },
        _ => actual.as_str() == Some(value),
    }
}

fn serve(rows: &[Value], query: &[(String, String)], map: &[(&str, &str)]) -> Value {
    let mut out: Vec<&Value> = rows
        .iter()
        .filter(|r| query.iter().all(|(k, v)| keep(r, k, v, map)))
        .collect();
    let get = |k: &str| query.iter().find(|(q, _)| q == k).map(|(_, v)| v.as_str());
    if let Some(key) = get("sort.asc") {
        out.sort_by_key(|r| {
            num(field(
                r,
                map.iter().find(|(k, _)| *k == key).map_or(key, |m| m.1),
            ))
        });
    }
    if let Some(key) = get("sort.desc") {
        out.sort_by_key(|r| {
            std::cmp::Reverse(num(field(
                r,
                map.iter().find(|(k, _)| *k == key).map_or(key, |m| m.1),
            )))
        });
    }
    let limit = get("limit").and_then(|l| l.parse().ok()).unwrap_or(100);
    out.truncate(limit);
    let select = get("select").unwrap_or("");
    if !select.contains(',') && !select.is_empty() {
        return Value::Array(out.iter().map(|r| field(r, select).clone()).collect());
    }
    Value::Array(
        out.into_iter()
            .map(|r| {
                let mut r = r.clone();
                r.as_object_mut().unwrap().remove("_contract");
                r
            })
            .collect(),
    )
}

impl Fetch for FakeTzkt {
    fn get(&self, url: &str) -> Result<HttpResponse, TransportError> {
        let mut s = self.state.lock().unwrap();
        s.requests.push(url.to_owned());
        if let Some((n, flag)) = &s.stop_after {
            if s.requests.len() >= *n {
                flag.store(true, Ordering::Relaxed);
            }
        }
        for f in s.faults.iter_mut() {
            if url.contains(&f.contains) && f.times != Some(0) {
                if let Some(t) = &mut f.times {
                    *t -= 1;
                }
                return Ok(HttpResponse {
                    status: f.status,
                    body: Vec::new(),
                });
            }
        }
        let rest = url.split_once("/v1").map_or(url, |(_, r)| r);
        let (path, query) = rest.split_once('?').unwrap_or((rest, ""));
        let is_bigmap = path.contains("/bigmaps/");
        let query: Vec<(String, String)> = query
            .split('&')
            .filter(|p| !p.is_empty())
            .map(|p| {
                let (k, v) = p.split_once('=').unwrap_or((p, ""));
                // Like TzKT: big-map keys page with `offset.cr` and ignore `id.gt`.
                let k = match (is_bigmap, k) {
                    (true, "offset.cr") => "id.gt",
                    (true, "id.gt") => "ignored.id.gt",
                    (false, "offset.cr") => "ignored.offset.cr",
                    _ => k,
                };
                (k.to_owned(), v.to_owned())
            })
            .collect();
        let body = match path {
            "/head" => json!({ "level": s.head }),
            "/tokens" => serve(
                &s.tokens,
                &query,
                &[
                    ("contract", "_contract"),
                    ("tokenId", "tokenId"),
                    ("lastLevel", "lastLevel"),
                    ("id", "id"),
                ],
            ),
            "/operations/transactions" => serve(
                &s.mints,
                &query,
                &[
                    ("target", "_contract"),
                    ("parameter.token_id", "parameter.value.token_id"),
                    ("level", "level"),
                    ("id", "id"),
                ],
            ),
            "/tokens/balances" => serve(
                &s.balances,
                &query,
                &[
                    ("token.contract", "_contract"),
                    ("token.tokenId", "token.tokenId"),
                    ("balance", "balance"),
                    ("lastLevel", "lastLevel"),
                    ("id", "id"),
                ],
            ),
            p if p.starts_with("/contracts/") && p.ends_with("/keys") => {
                let parts: Vec<&str> = p.split('/').collect();
                // ["", "contracts", address, "bigmaps", path, "keys"]
                let (escrow, map) = (parts[2], parts[4]);
                let rows: Vec<Value> = s
                    .listings
                    .iter()
                    .filter(|r| r["_escrow"] == escrow && r["_path"] == map)
                    .cloned()
                    .collect();
                let mut out = serve(
                    &rows,
                    &query,
                    &[
                        ("active", "active"),
                        ("lastLevel", "lastLevel"),
                        ("id", "id"),
                        ("value.fa2", "value.fa2"),
                        ("value.amount", "value.amount"),
                        ("value.objkt_amount", "value.objkt_amount"),
                    ],
                );
                for r in out.as_array_mut().unwrap() {
                    let o = r.as_object_mut().unwrap();
                    o.remove("_escrow");
                    o.remove("_path");
                }
                out
            }
            _ => {
                return Ok(HttpResponse {
                    status: 404,
                    body: Vec::new(),
                });
            }
        };
        Ok(HttpResponse {
            status: 200,
            body: serde_json::to_vec(&body).unwrap(),
        })
    }
}

pub fn tzkt(fake: &Arc<FakeTzkt>, stop: &Arc<AtomicBool>) -> Tzkt {
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

/// An in-memory IPFS gateway: `/<cid>` -> scripted status and body.
#[derive(Default)]
pub struct FakeGateway {
    pub docs: Mutex<std::collections::HashMap<String, (u16, Vec<u8>)>>,
    /// Status for every request while set (e.g. 503: gateway down).
    pub down: Mutex<Option<u16>>,
    pub requests: Mutex<Vec<String>>,
}

impl FakeGateway {
    pub fn serve(&self, cid: &str, status: u16, body: &str) {
        self.docs
            .lock()
            .unwrap()
            .insert(cid.to_owned(), (status, body.as_bytes().to_vec()));
    }

    pub fn requests_for(&self, cid: &str) -> usize {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|u| u.ends_with(&format!("/{cid}")))
            .count()
    }
}

impl Fetch for FakeGateway {
    fn get(&self, url: &str) -> Result<HttpResponse, TransportError> {
        self.requests.lock().unwrap().push(url.to_owned());
        if let Some(status) = *self.down.lock().unwrap() {
            return Ok(HttpResponse {
                status,
                body: Vec::new(),
            });
        }
        let cid = url.rsplit('/').next().unwrap_or_default();
        let (status, body) = self
            .docs
            .lock()
            .unwrap()
            .get(cid)
            .cloned()
            .unwrap_or((404, Vec::new()));
        Ok(HttpResponse { status, body })
    }
}

/// An in-memory objkt GraphQL endpoint answering `holder(where: {address: {_in: $addresses}})`.
#[derive(Default)]
pub struct FakeObjkt {
    pub holders: Mutex<std::collections::HashMap<String, Value>>,
    /// Status for every request while set.
    pub down: Mutex<Option<u16>>,
    /// Answer with GraphQL errors while set.
    pub errors: Mutex<bool>,
    /// The `addresses` variable of every request.
    pub requests: Mutex<Vec<Vec<String>>>,
}

impl FakeObjkt {
    pub fn holder(&self, row: Value) {
        let address = row["address"].as_str().unwrap().to_owned();
        self.holders.lock().unwrap().insert(address, row);
    }
}

impl Fetch for FakeObjkt {
    fn get(&self, _url: &str) -> Result<HttpResponse, TransportError> {
        Ok(HttpResponse {
            status: 405,
            body: Vec::new(),
        })
    }

    fn post_json(&self, _url: &str, body: &[u8]) -> Result<HttpResponse, TransportError> {
        let request: Value = serde_json::from_slice(body).unwrap();
        assert!(request["query"].as_str().unwrap().contains("holder("));
        let addresses: Vec<String> =
            serde_json::from_value(request["variables"]["addresses"].clone()).unwrap();
        self.requests.lock().unwrap().push(addresses.clone());
        if let Some(status) = *self.down.lock().unwrap() {
            return Ok(HttpResponse {
                status,
                body: Vec::new(),
            });
        }
        let answer = if *self.errors.lock().unwrap() {
            json!({ "errors": [{ "message": "field not found" }] })
        } else {
            let holders = self.holders.lock().unwrap();
            let rows: Vec<Value> = addresses
                .iter()
                .filter_map(|a| holders.get(a).cloned())
                .collect();
            json!({ "data": { "holder": rows } })
        };
        Ok(HttpResponse {
            status: 200,
            body: serde_json::to_vec(&answer).unwrap(),
        })
    }
}

/// A client over `fetch` with no pacing and quick retries.
pub fn quick_client(fetch: Arc<dyn Fetch>, base: &str, stop: &Arc<AtomicBool>) -> Client {
    let retry = RetryPolicy {
        attempts: 2,
        base_delay: Duration::ZERO,
        max_delay: Duration::ZERO,
    };
    Client::new(fetch, base, retry, Duration::ZERO, stop.clone())
}

/// Hex of UTF-8 text, as Michelson bytes come back from TzKT.
pub fn hex_text(text: &str) -> String {
    hex::encode(text.as_bytes())
}
