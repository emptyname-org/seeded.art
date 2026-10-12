//! Shared helpers for the sales tests: recorded TzKT rows (tests/fixtures/sales) and an
//! in-memory TzKT that answers the indexer's queries from them.
#![allow(dead_code)] // each test file uses a different subset

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use seeded_archive::ingest::http::{Client, Fetch, HttpResponse, RetryPolicy, TransportError};
use seeded_archive::ingest::tzkt::Tzkt;
use serde_json::Value;

pub const V1: &str = "KT1KEa8z6vWXDJrVqtMrAeDVzsvxat3kHaCE";
pub const V2: &str = "KT1U6EHmNxJTkvaWJ4ThczG4FSDaHC21ssvi";
pub const PARAMS: &str = "KT1EfsNuqwLAWDd3o4pvfUx1CAh5GMdTrRvr";
pub const FX_V2: &str = "KT1GbyoDi7H1sfXmimXpptZJuCdHMh66WS9u";
pub const FX_V1: &str = "KT1Xo5B7PNBAeynZPmca4bRh6LQow4og1Zb9";
pub const BASE: &str = "http://tzkt.test/v1";

pub fn root() -> PathBuf {
    let mut dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    while !dir.join("tests/fixtures/sales/ops.json").is_file() {
        assert!(
            dir.pop(),
            "project root with tests/fixtures/sales not found"
        );
    }
    dir
}

pub fn fixture(name: &str) -> Vec<u8> {
    fs::read(root().join("tests/fixtures/sales").join(name)).unwrap()
}

/// Recorded transactions, by increasing id.
pub fn ops_json() -> Vec<Value> {
    serde_json::from_slice(&fixture("ops.json")).unwrap()
}

pub fn tickets_json() -> Vec<Value> {
    serde_json::from_slice(&fixture("tickets.json")).unwrap()
}

/// Fresh per-test directory under tmp/sales-tests/.
pub fn test_dir(name: &str) -> PathBuf {
    let dir = root().join("tmp/sales-tests").join(name);
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

type Mutate = Box<dyn Fn(&str, &mut Vec<Value>) + Send + Sync>;

/// TzKT over recorded rows. Implements exactly the filters the indexer uses and panics on any
/// other, so a new query cannot pass unnoticed.
pub struct FakeTzkt {
    pub ops: Mutex<Vec<Value>>,
    pub tickets: Vec<Value>,
    pub head: AtomicU64,
    pub requests: Mutex<Vec<String>>,
    /// Sets `stop` once this many requests were served (0: never).
    pub stop_after: AtomicUsize,
    pub stop: Arc<AtomicBool>,
    /// Status for URLs containing the key (e.g. 503).
    pub fail: Mutex<Option<(String, u16)>>,
    /// Rewrites a response before it is served.
    pub mutate: Mutex<Option<Mutate>>,
}

impl FakeTzkt {
    pub fn new(head: u64) -> Arc<FakeTzkt> {
        Arc::new(FakeTzkt {
            ops: Mutex::new(ops_json()),
            tickets: tickets_json(),
            head: AtomicU64::new(head),
            requests: Mutex::new(Vec::new()),
            stop_after: AtomicUsize::new(0),
            stop: Arc::new(AtomicBool::new(false)),
            fail: Mutex::new(None),
            mutate: Mutex::new(None),
        })
    }

    pub fn request_count(&self) -> usize {
        self.requests.lock().unwrap().len()
    }

    pub fn urls(&self) -> Vec<String> {
        self.requests.lock().unwrap().clone()
    }
}

fn query(url: &str) -> (String, HashMap<String, String>) {
    let rest = url
        .strip_prefix(BASE)
        .expect("request outside the fake TzKT");
    let (path, q) = rest.split_once('?').unwrap_or((rest, ""));
    let params = q
        .split('&')
        .filter(|p| !p.is_empty())
        .map(|p| {
            let (k, v) = p.split_once('=').expect("parameter without value");
            (k.to_owned(), v.to_owned())
        })
        .collect();
    (path.to_owned(), params)
}

fn num(v: &Value) -> u64 {
    v.as_u64().unwrap()
}

fn list(v: &str) -> Vec<String> {
    v.split(',').map(str::to_owned).collect()
}

fn nums(v: &str) -> Vec<u64> {
    v.split(',').map(|x| x.parse().unwrap()).collect()
}

fn addr(row: &Value, key: &str) -> String {
    row[key]["address"].as_str().unwrap_or_default().to_owned()
}

fn transactions(rows: &[Value], params: &HashMap<String, String>) -> Vec<Value> {
    let mut out: Vec<Value> = rows
        .iter()
        .filter(|r| {
            let level = num(&r["level"]);
            let id = num(&r["id"]);
            let ep = r["parameter"]["entrypoint"].as_str().unwrap_or_default();
            params.iter().all(|(k, v)| match k.as_str() {
                "anyof.sender.target" => addr(r, "sender") == *v || addr(r, "target") == *v,
                "sender" => addr(r, "sender") == *v,
                "target" => addr(r, "target") == *v,
                "target.in" => list(v).contains(&addr(r, "target")),
                "entrypoint" => ep == v,
                "entrypoint.in" => list(v).iter().any(|e| e == ep),
                "status" => r["status"] == *v,
                "level" => level == v.parse::<u64>().unwrap(),
                "level.le" => level <= v.parse().unwrap(),
                "level.ge" => level >= v.parse().unwrap(),
                "level.in" => nums(v).contains(&level),
                "id.gt" => id > v.parse().unwrap(),
                "id.in" => nums(v).contains(&id),
                "sort.asc" => v == "id",
                "limit" | "select" => true,
                other => panic!("fake TzKT does not implement {other}"),
            })
        })
        .cloned()
        .collect();
    out.sort_by_key(|r| num(&r["id"]));
    out.truncate(params["limit"].parse().unwrap());
    out
}

fn ticket_mints(rows: &[Value], params: &HashMap<String, String>) -> Vec<Value> {
    let mut out: Vec<Value> = rows
        .iter()
        .filter(|r| {
            params.iter().all(|(k, v)| match k.as_str() {
                "token.contract" => v == "KT19etLCjCCzTLFFAxsxLFsVYMRPetr2bTD5",
                "from.null" => v == "true" && r["from"].is_null(),
                "level.le" => num(&r["level"]) <= v.parse().unwrap(),
                "id.gt" => num(&r["id"]) > v.parse().unwrap(),
                "sort.asc" => v == "id",
                "limit" | "select" => true,
                other => panic!("fake TzKT does not implement {other} on transfers"),
            })
        })
        .cloned()
        .collect();
    out.sort_by_key(|r| num(&r["id"]));
    out.truncate(params["limit"].parse().unwrap());
    out
}

impl Fetch for FakeTzkt {
    fn get(&self, url: &str) -> Result<HttpResponse, TransportError> {
        let n = {
            let mut reqs = self.requests.lock().unwrap();
            reqs.push(url.to_owned());
            reqs.len()
        };
        let after = self.stop_after.load(Ordering::SeqCst);
        if after > 0 && n >= after {
            self.stop.store(true, Ordering::SeqCst);
        }
        if let Some((key, status)) = self.fail.lock().unwrap().as_ref() {
            if url.contains(key.as_str()) {
                return Ok(HttpResponse {
                    status: *status,
                    body: Vec::new(),
                });
            }
        }
        let (path, params) = query(url);
        let mut rows = match path.as_str() {
            "/head" => {
                let head = serde_json::json!({"level": self.head.load(Ordering::SeqCst)});
                return Ok(HttpResponse {
                    status: 200,
                    body: head.to_string().into_bytes(),
                });
            }
            "/operations/transactions" => transactions(&self.ops.lock().unwrap(), &params),
            "/tokens/transfers" => ticket_mints(&self.tickets, &params),
            other => panic!("fake TzKT has no {other}"),
        };
        if let Some(m) = self.mutate.lock().unwrap().as_ref() {
            m(url, &mut rows);
        }
        Ok(HttpResponse {
            status: 200,
            body: Value::Array(rows).to_string().into_bytes(),
        })
    }
}

/// A client on `fake` without pacing; two attempts, no backoff.
pub fn api(fake: &Arc<FakeTzkt>) -> Tzkt {
    let retry = RetryPolicy {
        attempts: 2,
        base_delay: Duration::ZERO,
        max_delay: Duration::ZERO,
    };
    Tzkt::new(Client::new(
        fake.clone(),
        BASE,
        retry,
        Duration::ZERO,
        fake.stop.clone(),
    ))
}
