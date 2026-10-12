//! The sales indexer end to end over an in-memory TzKT serving recorded rows: complete runs,
//! page boundaries, interruption and resume, page limits, replays, confirmation depth, bad
//! upstream data and transient failures.

mod sales_support;

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::atomic::Ordering;

use rusqlite::Connection;
use sales_support::{FX_V2, FakeTzkt, api, test_dir};
use seeded_archive::ingest::FINALITY;
use seeded_archive::sales::SalesError;
use seeded_archive::sales::indexer::{SyncOptions, SyncReport, sync};
use serde_json::{Value, json};

/// Above every recorded level.
const HEAD: u64 = 16_000_000;

type Row = (
    i64,
    String,
    i64,
    String,
    Option<i64>,
    String,
    Option<String>,
    String,
    String,
);

fn rows(db: &Path) -> BTreeSet<Row> {
    let conn = Connection::open(db).unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT op_id, contract, token_id, kind, price_mutez, currency, seller, buyer,
               marketplace FROM sales",
        )
        .unwrap();
    stmt.query_map([], |r| {
        Ok((
            r.get(0)?,
            r.get(1)?,
            r.get(2)?,
            r.get(3)?,
            r.get(4)?,
            r.get(5)?,
            r.get(6)?,
            r.get(7)?,
            r.get(8)?,
        ))
    })
    .unwrap()
    .collect::<Result<_, _>>()
    .unwrap()
}

fn scalar(db: &Path, sql: &str) -> i64 {
    Connection::open(db)
        .unwrap()
        .query_row(sql, [], |r| r.get(0))
        .unwrap()
}

fn opts(page: usize, limit: Option<usize>) -> SyncOptions {
    SyncOptions {
        limit,
        concurrency: 3,
        page,
        confirmations: 2,
    }
}

fn run(fake: &std::sync::Arc<FakeTzkt>, db: &Path, o: &SyncOptions) -> SyncReport {
    sync(db, &api(fake), o, &|_| {}).unwrap()
}

fn all_done(r: &SyncReport) -> bool {
    r.streams.iter().all(|s| s.done) && r.streams.len() == 16
}

/// The reference: one complete run with full pages, in its own directory per caller.
fn reference(name: &str) -> BTreeSet<Row> {
    let dir = test_dir(&format!("{name}-reference"));
    let db = dir.join("sales.db");
    let fake = FakeTzkt::new(HEAD);
    let r = run(&fake, &db, &opts(10_000, None));
    assert!(all_done(&r), "{r:#?}");
    rows(&db)
}

#[test]
fn a_complete_sync_writes_every_recorded_sale_and_mint() {
    let dir = test_dir("sync-complete");
    let db = dir.join("sales.db");
    let fake = FakeTzkt::new(HEAD);
    let r = run(&fake, &db, &opts(10_000, None));
    assert!(all_done(&r), "{r:#?}");
    assert_eq!(r.safe_level, HEAD - 2);
    assert_eq!(r.permanent_failures() + r.transient_failures(), 0);
    let all = rows(&db);
    let kinds: BTreeSet<&str> = all.iter().map(|r| r.3.as_str()).collect();
    assert_eq!(
        kinds,
        ["auction", "mint", "offer_accept", "sale"]
            .into_iter()
            .collect()
    );
    assert_eq!(all.iter().filter(|r| r.3 == "mint").count(), 4);
    // 42 recorded trades (tests/sales_recognition.rs) and 4 mints.
    assert_eq!(all.len(), 46);
    // The ticket mint is priced from its ticket.
    assert!(all.contains(&(
        517_203_314_081_792,
        "KT1EfsNuqwLAWDd3o4pvfUx1CAh5GMdTrRvr".into(),
        16_408,
        "mint".into(),
        Some(16_000_000),
        "tez".into(),
        None,
        "tz1NkoBm4akiLs1ADTe6dLuae1EWWbJnkYps".into(),
        "KT1Xpmp15KfqoePNW9HczFmqaGNHwadV2a3b".into(),
    )));
    assert_eq!(scalar(&db, "SELECT count(*) FROM tickets"), 2);
    assert_eq!(
        scalar(
            &db,
            "SELECT count(*) FROM cursors WHERE synced_level = 15999998"
        ),
        16
    );
    // Running again finds nothing new.
    let again = run(&fake, &db, &opts(10_000, None));
    assert!(all_done(&again));
    assert!(again.streams.iter().all(|s| s.pages == 0));
    assert_eq!(rows(&db), all);
}

#[test]
fn tiny_pages_split_levels_without_changing_the_rows() {
    let dir = test_dir("sync-tiny-pages");
    let db = dir.join("sales.db");
    let fake = FakeTzkt::new(HEAD);
    let r = run(&fake, &db, &opts(2, None));
    assert!(all_done(&r), "{r:#?}");
    assert_eq!(rows(&db), reference("sync-tiny-pages"));
    // A full page of one level was completed with `level=`.
    assert!(fake.urls().iter().any(|u| u.contains("&level=")));
}

#[test]
fn an_interrupted_sync_resumes_to_the_same_rows() {
    let dir = test_dir("sync-interrupted");
    let db = dir.join("sales.db");
    let fake = FakeTzkt::new(HEAD);
    fake.stop_after.store(25, Ordering::SeqCst);
    let first = run(&fake, &db, &opts(3, None));
    assert!(first.interrupted(), "{first:#?}");
    let partial = rows(&db);
    assert!(
        !partial.is_empty(),
        "committed pages survive the interruption"
    );
    assert!(partial.len() < reference("sync-interrupted").len());
    fake.stop_after.store(0, Ordering::SeqCst);
    fake.stop.store(false, Ordering::SeqCst);
    let second = run(&fake, &db, &opts(3, None));
    assert!(all_done(&second), "{second:#?}");
    assert_eq!(rows(&db), reference("sync-interrupted"));
}

#[test]
fn a_page_limit_pauses_streams_and_reruns_finish() {
    let dir = test_dir("sync-limit");
    let db = dir.join("sales.db");
    let fake = FakeTzkt::new(HEAD);
    let mut runs = 0;
    loop {
        let r = run(&fake, &db, &opts(2, Some(1)));
        runs += 1;
        assert!(r.streams.iter().all(|s| s.pages <= 1));
        if all_done(&r) {
            break;
        }
        assert!(runs < 200, "no progress");
    }
    assert!(runs > 3);
    assert_eq!(rows(&db), reference("sync-limit"));
}

#[test]
fn ticket_mints_wait_until_every_ticket_is_known() {
    let dir = test_dir("sync-ticket-wait");
    let db = dir.join("sales.db");
    let fake = FakeTzkt::new(HEAD);
    let r = run(&fake, &db, &opts(1, Some(1)));
    let tickets = &r.streams[0];
    assert!(tickets.limited && !tickets.done);
    assert!(
        r.streams
            .iter()
            .all(|s| s.stream != "mint:KT1Xpmp15KfqoePNW9HczFmqaGNHwadV2a3b")
    );
    assert_eq!(
        scalar(
            &db,
            "SELECT count(*) FROM sales WHERE marketplace = 'KT1Xpmp15KfqoePNW9HczFmqaGNHwadV2a3b'"
        ),
        0
    );
    assert_eq!(
        scalar(
            &db,
            "SELECT count(*) FROM issues WHERE reason = 'ticket_unknown'"
        ),
        0
    );
}

#[test]
fn replaying_indexed_operations_adds_no_duplicates() {
    let dir = test_dir("sync-replay");
    let db = dir.join("sales.db");
    let fake = FakeTzkt::new(HEAD);
    assert!(all_done(&run(&fake, &db, &opts(10_000, None))));
    let before = rows(&db);
    let escrow = scalar(&db, "SELECT count(*) FROM escrow");
    Connection::open(&db)
        .unwrap()
        .execute("UPDATE cursors SET cursor_id = 0, synced_level = NULL", [])
        .unwrap();
    let r = run(&fake, &db, &opts(4, None));
    assert!(all_done(&r));
    assert!(r.streams.iter().map(|s| s.pages).sum::<u64>() > 0);
    assert!(r.streams.iter().all(|s| s.sales == 0), "{r:#?}");
    assert_eq!(rows(&db), before);
    assert_eq!(scalar(&db, "SELECT count(*) FROM escrow"), escrow);
}

#[test]
fn only_confirmed_levels_are_read_and_the_rest_follows_later() {
    let dir = test_dir("sync-confirmed");
    let db = dir.join("sales.db");
    // Head two levels above the last recorded collection offer: that block is not final yet.
    let late = 15_040_725;
    let fake = FakeTzkt::new(late + 1);
    let r = run(&fake, &db, &opts(10_000, None));
    assert!(all_done(&r));
    assert_eq!(r.safe_level, late - 1);
    for url in fake.urls() {
        for part in url.split(['?', '&']) {
            if let Some(level) = part.strip_prefix("level.le=") {
                assert!(level.parse::<u64>().unwrap() < late, "{url}");
            }
        }
    }
    assert_eq!(
        scalar(
            &db,
            &format!("SELECT count(*) FROM sales WHERE level > {}", late - 1)
        ),
        0
    );
    let early = rows(&db);
    assert!(early.len() < reference("sync-confirmed").len());
    fake.head.store(HEAD, Ordering::SeqCst);
    assert!(all_done(&run(&fake, &db, &opts(10_000, None))));
    assert_eq!(rows(&db), reference("sync-confirmed"));
}

/// A fresh sync whose `FX_V2` responses are rewritten by `bad`; the stream must fail
/// permanently and write nothing.
fn rejects(name: &str, bad: impl Fn(&mut Vec<Value>) + Send + Sync + 'static) {
    let dir = test_dir(name);
    let db = dir.join("sales.db");
    let fake = FakeTzkt::new(HEAD);
    *fake.mutate.lock().unwrap() = Some(Box::new(move |url: &str, rows: &mut Vec<Value>| {
        if url.contains(&format!("anyof.sender.target={FX_V2}")) && !rows.is_empty() {
            bad(rows);
        }
    }));
    let r = run(&fake, &db, &opts(10_000, None));
    let v2 = r
        .streams
        .iter()
        .find(|s| s.stream == format!("market:{FX_V2}"))
        .unwrap();
    assert!(v2.error.is_some() && !v2.transient, "{name}: {v2:#?}");
    assert!(r.permanent_failures() >= 1);
    assert_eq!(
        scalar(
            &db,
            &format!("SELECT count(*) FROM sales WHERE marketplace = '{FX_V2}'")
        ),
        0,
        "{name}"
    );
    assert_eq!(
        scalar(
            &db,
            &format!("SELECT cursor_id FROM cursors WHERE stream = 'market:{FX_V2}'")
        ),
        0,
        "{name}"
    );
}

#[test]
fn bad_upstream_data_is_rejected_and_no_cursor_moves() {
    rejects("bad-amount", |rows| {
        rows[0]["amount"] = json!(100_000_000_000_000_000u64)
    });
    rejects("bad-duplicate", |rows| {
        let first = rows[0].clone();
        rows.insert(0, first);
    });
    rejects("bad-order", |rows| rows.reverse());
    rejects("bad-address", |rows| {
        rows[0]["sender"]["address"] = json!("tz1notanaddress")
    });
    rejects("bad-unconfirmed", |rows| {
        let last = rows.len() - 1;
        rows[last]["level"] = json!(HEAD);
    });
    rejects("bad-outside-filter", |rows| {
        rows[0]["sender"]["address"] = json!("tz1bz2CabKhva5Y5Q3NWf3u7pBFQF72KJNm5");
        rows[0]["target"]["address"] = json!("KT1QJ71jypKGgyTNtXjkCAYJZNhCKWiHuT2r");
    });
    rejects("bad-token-id", |rows| {
        for r in rows.iter_mut() {
            if r["parameter"]["entrypoint"] == "transfer" {
                r["parameter"]["value"][0]["txs"][0]["token_id"] = json!("99999999999999999999");
            }
        }
    });
    rejects("bad-status", |rows| rows[0]["status"] = json!("failed"));
    rejects("bad-unit-amount", |rows| {
        for r in rows.iter_mut() {
            if r["parameter"]["entrypoint"] == "transfer" {
                r["parameter"]["value"][0]["txs"][0]["amount"] = json!("2");
            }
        }
    });
}

#[test]
fn transient_failures_keep_the_cursor_and_a_rerun_completes() {
    let dir = test_dir("sync-transient");
    let db = dir.join("sales.db");
    let fake = FakeTzkt::new(HEAD);
    *fake.fail.lock().unwrap() = Some((format!("anyof.sender.target={FX_V2}"), 503));
    let r = run(&fake, &db, &opts(10_000, None));
    assert_eq!(r.transient_failures(), 1, "{r:#?}");
    assert_eq!(r.permanent_failures(), 0);
    let others = r
        .streams
        .iter()
        .filter(|s| s.stream != format!("market:{FX_V2}"))
        .all(|s| s.done);
    assert!(others, "other streams carry on");
    assert_eq!(
        scalar(
            &db,
            &format!("SELECT cursor_id FROM cursors WHERE stream = 'market:{FX_V2}'")
        ),
        0
    );
    *fake.fail.lock().unwrap() = None;
    assert!(all_done(&run(&fake, &db, &opts(10_000, None))));
    assert_eq!(rows(&db), reference("sync-transient"));
}

#[test]
fn confirmations_below_finality_are_refused() {
    let dir = test_dir("sync-below-finality");
    let fake = FakeTzkt::new(HEAD);
    let o = SyncOptions {
        confirmations: FINALITY - 1,
        ..opts(10_000, None)
    };
    let r = sync(&dir.join("sales.db"), &api(&fake), &o, &|_| {});
    assert!(matches!(r, Err(SalesError::Usage(_))), "{r:?}");
    assert!(fake.urls().is_empty(), "nothing is read");
}

#[test]
fn the_head_must_be_above_the_confirmation_depth() {
    let dir = test_dir("sync-low-head");
    let fake = FakeTzkt::new(1);
    assert!(sync(&dir.join("sales.db"), &api(&fake), &opts(10, None), &|_| {}).is_err());
}
