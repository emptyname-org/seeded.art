//! fxhash buy offer indexing against an in-memory TzKT serving recorded big-map keys.

mod ingest_support;
mod market_support;

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use ingest_support::*;
use market_support::*;
use seeded_archive::ingest::IngestError;
use seeded_archive::ingest::escrow::{EscrowOptions, sync_escrows};
use seeded_archive::ingest::http::ApiError;
use seeded_archive::ingest::offers::{OfferOptions, offer_stats, sync_offers};
use seeded_archive::ingest::sync::{SyncOptions, sync};
use seeded_archive::store::market::{FXHASH_V2, MarketCounts, OfferKind};
use seeded_archive::store::{Store, StoreError};
use serde_json::json;

const ARBOREAL_TOKEN: u64 = 188_628;

fn synced_without_offers(
    name: &str,
) -> (std::path::PathBuf, Store, Arc<MarketFake>, Arc<AtomicBool>) {
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
    (dir, store, fake, stop)
}

fn not_found<T: std::fmt::Debug>(r: Result<T, StoreError>) {
    assert!(matches!(r, Err(StoreError::NotFound { .. })), "{r:?}");
}

#[test]
fn full_pass_indexes_live_offers_and_their_block_times() {
    let (dir, store, _fake, _api) = indexed("market_full");
    let c = side(&dir);
    // Recorded: 4 live token offers, 3 live collection offers; removed and exhausted keys
    // (64810, 67948, 78597, 78689, 115625) are not read by a full pass.
    assert_eq!(
        one(
            &c,
            "SELECT count(*) FROM market_offers WHERE active = 1 AND kind = 'token'"
        ),
        4
    );
    assert_eq!(
        one(
            &c,
            "SELECT count(*) FROM market_offers WHERE active = 1 AND kind = 'collection'"
        ),
        3
    );
    assert_eq!(
        one(&c, "SELECT count(*) FROM market_offers WHERE active = 0"),
        0
    );

    let o = store
        .offer(FXHASH_V2.contract, OfferKind::Token, 79_120)
        .unwrap();
    assert!(o.active);
    assert_eq!(
        (o.buyer.as_str(), o.price_mutez, o.remaining),
        (BUYER, 333_300, 1)
    );
    assert_eq!(o.locked_mutez, 333_300);
    let token = o.token.as_ref().unwrap();
    assert_eq!(
        (token.contract.as_str(), token.token_id),
        (PARAMS, ARBOREAL_TOKEN)
    );
    assert_eq!(
        token.iteration,
        store.token(PARAMS, ARBOREAL_TOKEN).unwrap().iteration
    );
    assert_eq!(
        (o.project.id, o.project.title.as_str()),
        (ARBOREAL, "Arboreal")
    );
    assert_eq!(o.created_level, 6_874_174);
    assert_eq!(o.created_at.as_deref(), Some("2024-10-17T12:42:10Z"));
    assert_eq!(o.marketplace, FXHASH_V2);

    // Offers on tokens or projects outside the index are stored but not visible.
    for (kind, id) in [
        (OfferKind::Token, 99),
        (OfferKind::Token, 298),
        (OfferKind::Token, 58_292),
        (OfferKind::Collection, 5_422),
        (OfferKind::Collection, 210),
    ] {
        not_found(store.offer(FXHASH_V2.contract, kind, id));
    }
    let s = offer_stats(&store).unwrap();
    assert_eq!(
        (s.token_offers, s.collection_offers, s.visible_offers),
        (4, 3, 1)
    );
    assert_eq!(
        s.token_locked_mutez,
        5_000_000 + 8_000_000 + 10_000_000 + 333_300
    );
    // 1.901 tez x 1 + 0.5 x 4 + 0.5 x 3.
    assert_eq!(s.collection_locked_mutez, 1_901_000 + 2_000_000 + 1_500_000);
    assert_eq!(
        s.times_pending, 0,
        "every recorded level is final and has a time"
    );
}

#[test]
fn incremental_pass_applies_cancels_accepts_and_new_offers() {
    let (dir, store, fake, api) = indexed("market_incremental");
    let mut store = store;
    let head = fake.advance(100);
    // Offer 79120 is cancelled; collection offer 5422 sells one token (4 -> 3); a new
    // collection offer on Arboreal appears.
    fake.edit_offer("offers", "79120", |r| {
        r["active"] = json!(false);
        r["lastLevel"] = json!(head - 60);
    });
    fake.edit_offer("collection_offers", "5422", |r| {
        r["value"]["amount"] = json!("3");
        r["lastLevel"] = json!(head - 50);
    });
    fake.push_key(offer_key(
        "collection_offers",
        90_000_001,
        "120000",
        json!({"buyer": BUYER, "price": "4000000", "amount": "2", "collection": "29437"}),
        true,
        head - 40,
        head - 40,
    ));
    fake.block(head - 40, "2026-10-05T00:00:00Z");
    let r = offers(&mut store, &api);
    assert_eq!((r.live, r.ended), (2, 1));
    assert_eq!(r.completed.len(), 2);

    let cancelled = store
        .offer(FXHASH_V2.contract, OfferKind::Token, 79_120)
        .unwrap();
    assert!(
        !cancelled.active,
        "a cancelled offer stays readable as ended"
    );
    assert_eq!(cancelled.updated_level, head - 60);
    assert_eq!((cancelled.remaining, cancelled.locked_mutez), (1, 0));
    let c = side(&dir);
    assert_eq!(
        one(
            &c,
            "SELECT remaining FROM market_offers WHERE kind = 'collection' AND offer_id = 5422"
        ),
        3
    );
    let for_token = store.offers_for_token(PARAMS, ARBOREAL_TOKEN).unwrap();
    assert_eq!(for_token.len(), 1);
    let new = &for_token[0];
    assert_eq!(
        (new.kind, new.offer_id, new.remaining),
        (OfferKind::Collection, 120_000, 2)
    );
    assert_eq!(new.locked_mutez, 8_000_000);
    assert!(new.token.is_none());
    assert_eq!(new.created_at.as_deref(), Some("2026-10-05T00:00:00Z"));
    let by_buyer = store.offers_by_buyer(BUYER, None, 10).unwrap();
    assert_eq!(by_buyer.total, 1);
    assert_eq!(by_buyer.items[0].offer_id, 120_000);

    // Both remaining tokens sell: the offer is exhausted and leaves every list.
    let head = fake.advance(10);
    fake.edit_offer("collection_offers", "120000", |r| {
        r["value"]["amount"] = json!("0");
        r["lastLevel"] = json!(head - 5);
    });
    let r = offers(&mut store, &api);
    assert_eq!((r.live, r.ended), (0, 1));
    let done = store
        .offer(FXHASH_V2.contract, OfferKind::Collection, 120_000)
        .unwrap();
    assert_eq!(
        (done.active, done.remaining, done.locked_mutez),
        (false, 0, 0)
    );
    assert!(
        store
            .offers_for_token(PARAMS, ARBOREAL_TOKEN)
            .unwrap()
            .is_empty()
    );
    assert_eq!(store.offers_by_buyer(BUYER, None, 10).unwrap().total, 0);
}

#[test]
fn collection_offers_match_every_token_of_their_project() {
    let (_dir, mut store, fake, api) = indexed("market_collection");
    let head = fake.advance(100);
    fake.push_key(offer_key(
        "collection_offers",
        90_000_001,
        "120000",
        json!({"buyer": HONEST, "price": "1000000", "amount": "1", "collection": "29437"}),
        true,
        head - 40,
        head - 40,
    ));
    offers(&mut store, &api);
    // The token offer (0.3333 tez) and the collection offer (1 tez), highest price first.
    let arboreal: Vec<(OfferKind, u64)> = store
        .offers_for_token(PARAMS, ARBOREAL_TOKEN)
        .unwrap()
        .iter()
        .map(|o| (o.kind, o.offer_id))
        .collect();
    assert_eq!(
        arboreal,
        vec![(OfferKind::Collection, 120_000), (OfferKind::Token, 79_120)]
    );
    for token in [133_459, 133_603] {
        let o = store.offers_for_token(PARAMS, token).unwrap();
        assert_eq!(o.len(), 1, "token {token}");
        assert_eq!((o[0].project.id, o[0].offer_id), (ARBOREAL, 120_000));
    }
    // A token of another project does not match.
    assert!(store.offers_for_token(V1, 34_558).unwrap().is_empty());
    // Holders of Arboreal tokens can accept it; the buyer's own offer is left out.
    let received = store.offers_received(QUADRADO, None, 10).unwrap();
    assert_eq!(received.total, 1);
    assert_eq!(received.items[0].offer_id, 120_000);
    assert_eq!(store.offers_received(HONEST, None, 10).unwrap().total, 0);
    // A holder's own collection offer on its project is not something it can accept.
    let head = fake.advance(10);
    fake.push_key(offer_key(
        "collection_offers",
        90_000_002,
        "120001",
        json!({"buyer": QUADRADO, "price": "500000", "amount": "1", "collection": "29437"}),
        true,
        head - 5,
        head - 5,
    ));
    offers(&mut store, &api);
    let received = store.offers_received(QUADRADO, None, 10).unwrap();
    assert_eq!(
        received
            .items
            .iter()
            .map(|o| o.offer_id)
            .collect::<Vec<_>>(),
        vec![120_000]
    );
    // The escrowed Arboreal token's seller can accept both (after cancelling the listing).
    let seller = store.offers_received(ARBOREAL_SELLER, None, 10).unwrap();
    let mut got: Vec<u64> = seller.items.iter().map(|o| o.offer_id).collect();
    got.sort_unstable();
    assert_eq!(got, vec![79_120, 120_000, 120_001]);
    // Held NFTs offered for a collection offer carry their own thumbnails.
    let held = store.held_project_tokens(QUADRADO, ARBOREAL).unwrap();
    assert!(!held.is_empty());
    assert!(
        held.iter()
            .all(|t| t.image.preview.is_some() || t.image.uri.is_some())
    );
    // The Listings and offers tab count adds the three market views.
    for address in [QUADRADO, HONEST, ARBOREAL_SELLER] {
        let views = store.listings_by_seller(address, None, 10).unwrap().total
            + store.offers_by_buyer(address, None, 10).unwrap().total
            + store.offers_received(address, None, 10).unwrap().total;
        assert_eq!(
            store.market_counts(address).unwrap().total(),
            views,
            "{address}"
        );
    }
    assert_eq!(
        store.market_counts(ARBOREAL_SELLER).unwrap(),
        MarketCounts {
            listings: 1,
            offers_made: 0,
            offers_received: 3
        }
    );
}

#[test]
fn flagged_projects_hide_their_offers_and_listings() {
    let (dir, mut store, fake, api) = indexed("market_flagged");
    let head = fake.advance(100);
    // Offers on the flagged project 554 (GENTK v1 #43308 belongs to it).
    fake.push_key(offer_key(
        "collection_offers",
        90_000_001,
        "120000",
        json!({"buyer": BUYER, "price": "1000000", "amount": "1", "collection": "554"}),
        true,
        head - 40,
        head - 40,
    ));
    fake.push_key(offer_key(
        "offers",
        90_000_002,
        "90000",
        json!({"buyer": BUYER, "gentk": {"id": "43308", "version": "0"}, "price": "1000000"}),
        true,
        head - 40,
        head - 40,
    ));
    offers(&mut store, &api);
    not_found(store.offer(FXHASH_V2.contract, OfferKind::Collection, 120_000));
    not_found(store.offer(FXHASH_V2.contract, OfferKind::Token, 90_000));
    not_found(store.offers_for_token(V1, 43_308));
    let by_buyer = store.offers_by_buyer(BUYER, None, 10).unwrap();
    assert_eq!(by_buyer.total, 1, "only the Arboreal offer");
    assert_eq!(by_buyer.items[0].offer_id, 79_120);
    assert_eq!(offer_stats(&store).unwrap().visible_offers, 1);

    // Flagging Arboreal hides its offer and listing too.
    side(&dir)
        .execute(
            "UPDATE projects SET hidden = 1 WHERE id = ?1",
            [ARBOREAL as i64],
        )
        .unwrap();
    not_found(store.offer(FXHASH_V2.contract, OfferKind::Token, 79_120));
    assert_eq!(store.offers_by_buyer(BUYER, None, 10).unwrap().total, 0);
    assert_eq!(
        store
            .listings_by_seller(ARBOREAL_SELLER, None, 10)
            .unwrap()
            .total,
        0
    );
    not_found(store.offers_for_token(PARAMS, ARBOREAL_TOKEN));
}

#[test]
fn bad_upstream_data_rejects_the_page_and_writes_nothing() {
    let (dir, mut store, fake, stop) = synced_without_offers("market_bad_data");
    let api = market_api(&fake, &stop);
    // A token offer whose price is not a nat, between recorded keys 298 and 58292.
    fake.push_key(offer_key(
        "offers",
        30_000_000,
        "80000",
        json!({"buyer": BUYER, "gentk": {"id": "1", "version": "1"}, "price": "12.5"}),
        true,
        7_000_000,
        7_000_000,
    ));
    let opts = OfferOptions {
        limit: None,
        page: 2,
    };
    let err = sync_offers(&mut store, &api, &opts, &|_| {}).unwrap_err();
    assert!(
        matches!(&err, IngestError::Api(ApiError::Permanent(m)) if m.contains("price")),
        "{err}"
    );
    // The first page (keys 99, 298) committed; the bad page wrote nothing and kept the cursor.
    // The collection book was not held back by the failure.
    let c = side(&dir);
    assert_eq!(
        one(
            &c,
            "SELECT count(*) FROM market_offers WHERE kind = 'collection' AND active = 1"
        ),
        3
    );
    assert_eq!(
        one(
            &c,
            "SELECT count(*) FROM market_offers WHERE kind = 'token'"
        ),
        2
    );
    let cursor = one(
        &c,
        "SELECT cursor_id FROM bigmap_state WHERE path = 'offers' AND pass_level IS NOT NULL",
    );
    // Fixed upstream, the pass resumes from that cursor and completes.
    fake.edit_offer("offers", "80000", |r| {
        r["value"]["price"] = json!("12500000")
    });
    let r = sync_offers(&mut store, &api, &opts, &|_| {}).unwrap();
    assert!(r.pending.is_empty(), "{r:?}");
    assert_eq!(
        one(
            &c,
            "SELECT count(*) FROM market_offers WHERE kind = 'token'"
        ),
        5
    );
    assert!(cursor > 0);

    // Other contract violations: an invalid buyer, levels out of order, price x amount
    // beyond mutez. Each lands above every book's synced level.
    type Case = fn(u64) -> serde_json::Value;
    let cases: [(&str, Case); 3] = [
        ("buyer", |h| {
            offer_key(
                "offers",
                95_000_000,
                "80002",
                json!({"buyer": "tz1notanaddress", "gentk": {"id": "1", "version": "1"}, "price": "1"}),
                true,
                h - 1,
                h - 1,
            )
        }),
        ("levels", |h| {
            offer_key(
                "offers",
                95_000_001,
                "80003",
                json!({"buyer": BUYER, "gentk": {"id": "1", "version": "1"}, "price": "1"}),
                true,
                h,
                h - 1,
            )
        }),
        ("overflow", |h| {
            offer_key(
                "collection_offers",
                95_000_002,
                "130000",
                json!({"buyer": BUYER, "price": "4611686018427387904", "amount": "2", "collection": "1"}),
                true,
                h - 1,
                h - 1,
            )
        }),
    ];
    for (what, case) in cases {
        let head = fake.advance(100);
        fake.push_key(case(head));
        let before = one(&c, "SELECT count(*) FROM market_offers");
        let err = sync_offers(&mut store, &api, &OfferOptions::default(), &|_| {}).unwrap_err();
        assert!(
            matches!(err, IngestError::Api(ApiError::Permanent(_))),
            "{what}: {err}"
        );
        assert_eq!(one(&c, "SELECT count(*) FROM market_offers"), before);
        fake.inner.with(|s| s.listings.pop());
    }
    let r = offers(&mut store, &api);
    assert!(r.pending.is_empty());
}

#[test]
fn bad_block_rows_are_rejected() {
    let (dir, mut store, fake, stop) = synced_without_offers("market_bad_blocks");
    let api = market_api(&fake, &stop);
    // A timestamp that is not UTC seconds.
    let level = *fake.blocks.lock().unwrap().keys().next().unwrap();
    let good = fake.blocks.lock().unwrap()[&level].clone();
    fake.block(level, &format!("{good} "));
    let stored = |level: u64| {
        one(
            &side(&dir),
            &format!("SELECT count(*) FROM block_times WHERE level = {level}"),
        )
    };
    let err = sync_offers(&mut store, &api, &OfferOptions::default(), &|_| {}).unwrap_err();
    assert!(
        matches!(&err, IngestError::Api(ApiError::Permanent(m)) if m.contains("block")),
        "{err}"
    );
    assert_eq!(stored(level), 0, "the chunk with the bad row wrote nothing");
    // A level that was not asked for.
    fake.block(level, &good);
    *fake.extra_block.lock().unwrap() =
        Some(json!({"level": 1, "timestamp": "2021-01-01T00:00:00Z"}));
    let err = sync_offers(&mut store, &api, &OfferOptions::default(), &|_| {}).unwrap_err();
    assert!(
        matches!(&err, IngestError::Api(ApiError::Permanent(m)) if m.contains("block")),
        "{err}"
    );
    assert_eq!((stored(level), stored(1)), (0, 0));
    *fake.extra_block.lock().unwrap() = None;
    let r = offers(&mut store, &api);
    assert!(r.times_complete);
    assert_eq!(stored(level), 1);
}

#[test]
fn transient_failures_pause_and_the_next_run_resumes() {
    let (_dir, mut store, fake, stop) = synced_without_offers("market_transient");
    let api = market_api(&fake, &stop);
    fake.inner.with(|s| {
        s.faults.push(Fault {
            contains: "/bigmaps/collection_offers/keys".into(),
            status: 503,
            times: Some(3),
        });
    });
    let r = offers(&mut store, &api);
    assert_eq!(r.transient_failures, 1);
    assert!(
        r.pending.iter().any(|p| p.ends_with("/collection_offers")),
        "{r:?}"
    );
    let r = offers(&mut store, &api);
    assert_eq!(r.transient_failures, 0);
    assert!(r.pending.is_empty() && r.times_complete);
    assert_eq!(offer_stats(&store).unwrap().collection_offers, 3);

    // Block time requests fail transiently too, then recover.
    let head = fake.advance(100);
    fake.push_key(offer_key(
        "offers",
        95_000_000,
        "90000",
        json!({"buyer": BUYER, "gentk": {"id": "133459", "version": "2"}, "price": "1"}),
        true,
        head - 50,
        head - 50,
    ));
    fake.block(head - 50, "2026-10-05T01:00:00Z");
    fake.inner.with(|s| {
        s.faults.push(Fault {
            contains: "/blocks?".into(),
            status: 502,
            times: Some(3),
        });
    });
    let r = offers(&mut store, &api);
    assert_eq!((r.transient_failures, r.times_complete), (1, false));
    let o = store
        .offer(FXHASH_V2.contract, OfferKind::Token, 90_000)
        .unwrap();
    assert_eq!(o.created_at, None);
    let r = offers(&mut store, &api);
    assert!(r.times_complete);
    let o = store
        .offer(FXHASH_V2.contract, OfferKind::Token, 90_000)
        .unwrap();
    assert_eq!(o.created_at.as_deref(), Some("2026-10-05T01:00:00Z"));
}

#[test]
fn limited_and_interrupted_runs_resume_to_the_same_result() {
    let (ref_dir, reference, _fake, _api) = indexed("market_reference");
    let want = offer_stats(&reference).unwrap();
    let want_rows = dump(&ref_dir);

    let (dir, mut store, fake, stop) = synced_without_offers("market_limited");
    let api = market_api(&fake, &stop);
    let mut runs = 0;
    loop {
        runs += 1;
        assert!(runs <= 20, "limited runs did not converge");
        let r = offers_paged(&mut store, &api, 2, Some(1));
        if r.pending.is_empty() && r.times_complete {
            break;
        }
        assert!(r.limited, "{r:?}");
    }
    assert!(
        runs > 4,
        "page 2 with one request per run takes several runs"
    );
    assert_eq!(offer_stats(&store).unwrap(), want);
    assert_eq!(dump(&dir), want_rows);

    // A stop signal mid-pass keeps committed pages; the next run finishes.
    let (dir2, mut store2, fake2, stop2) = synced_without_offers("market_interrupted");
    let api2 = market_api(&fake2, &stop2);
    let served = fake2.inner.with(|s| s.requests.len());
    fake2
        .inner
        .with(|s| s.stop_after = Some((served + 3, stop2.clone())));
    let r = offers_paged(&mut store2, &api2, 2, None);
    assert!(r.interrupted, "{r:?}");
    stop2.store(false, std::sync::atomic::Ordering::Relaxed);
    fake2.inner.with(|s| s.stop_after = None);
    let r = offers_paged(&mut store2, &api2, 2, None);
    assert!(r.pending.is_empty() && r.times_complete, "{r:?}");
    assert_eq!(offer_stats(&store2).unwrap(), want);
    assert_eq!(dump(&dir2), want_rows);
}

/// Every stored offer, listing and block time, in key order.
fn dump(dir: &std::path::Path) -> Vec<String> {
    let c = side(dir);
    let mut out = Vec::new();
    for sql in [
        "SELECT marketplace, kind, offer_id, buyer, price, remaining, token_contract, token_id, \
         project_id, created_level, level, active FROM market_offers \
         ORDER BY marketplace, kind, offer_id",
        "SELECT escrow, listing, contract, token_id, seller, level, price FROM escrow_listings \
         ORDER BY escrow, listing",
        "SELECT level, time FROM block_times ORDER BY level",
    ] {
        let mut st = c.prepare(sql).unwrap();
        let n = st.column_count();
        let rows = st
            .query_map([], |r| {
                (0..n)
                    .map(|i| {
                        r.get::<_, rusqlite::types::Value>(i)
                            .map(|v| format!("{v:?}"))
                    })
                    .collect::<rusqlite::Result<Vec<_>>>()
                    .map(|v| v.join("|"))
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        out.extend(rows);
    }
    assert!(out.len() > 10);
    out
}

#[test]
fn a_run_limit_rotates_between_books_and_block_times() {
    let (_dir, mut store, fake, stop) = synced_without_offers("market_rotation");
    let api = market_api(&fake, &stop);
    // 250 listings at distinct levels need three block-time requests.
    let head = fake.advance(1000);
    fake.inner.with(|s| {
        for i in 0..250_u64 {
            s.listings.push(json!({
                "_escrow": MARKET_V2, "_path": "listings", "id": 70_000_000 + i, "active": true,
                "key": (7_000_000 + i).to_string(), "firstLevel": head - 900 + i,
                "lastLevel": head - 900 + i,
                "value": {"gentk": {"id": (1_600_000 + i).to_string(), "version": "1"},
                          "seller": HONEST, "price": "1"}
            }));
        }
    });
    for i in 0..250_u64 {
        fake.block(head - 900 + i, "2026-10-05T03:00:00Z");
    }
    sync_escrows(&mut store, &api, &EscrowOptions::default(), &|_| {}).unwrap();
    // One request per run: the books are not held back behind the block times, nor the
    // reverse.
    let first = offers_paged(&mut store, &api, 10_000, Some(1));
    let second = offers_paged(&mut store, &api, 10_000, Some(1));
    assert_eq!(
        first.completed.len() + second.completed.len(),
        2,
        "{first:?} {second:?}"
    );
    assert!(second.pending.is_empty());
    let third = offers_paged(&mut store, &api, 10_000, Some(1));
    assert_eq!(third.times_written, 100, "{third:?}");
    let mut runs = 3;
    while offer_stats(&store).unwrap().times_pending > 0 {
        runs += 1;
        assert!(runs < 30, "block times did not finish");
        offers_paged(&mut store, &api, 10_000, Some(1));
    }
}

#[test]
fn block_times_wait_for_finality_and_retry_missing_levels() {
    let (_dir, mut store, fake, api) = indexed("market_finality");
    let head = fake.advance(100);
    // One offer created at head - 1 (not final), one at a final level TzKT has no block for.
    fake.push_key(offer_key(
        "offers",
        95_000_000,
        "90000",
        json!({"buyer": BUYER, "gentk": {"id": "133459", "version": "2"}, "price": "1"}),
        true,
        head - 1,
        head - 1,
    ));
    fake.push_key(offer_key(
        "offers",
        95_000_001,
        "90001",
        json!({"buyer": BUYER, "gentk": {"id": "133460", "version": "2"}, "price": "1"}),
        true,
        head - 30,
        head - 30,
    ));
    fake.block(head - 1, "2026-10-05T02:00:06Z");
    let r = offers(&mut store, &api);
    assert_eq!((r.times_written, r.times_missing), (0, 1));
    let fresh = |store: &Store, id| {
        store
            .offer(FXHASH_V2.contract, OfferKind::Token, id)
            .unwrap()
            .created_at
    };
    assert_eq!((fresh(&store, 90_000), fresh(&store, 90_001)), (None, None));
    assert_eq!(offer_stats(&store).unwrap().times_pending, 2);

    fake.advance(5);
    fake.block(head - 30, "2026-10-05T01:57:00Z");
    let r = offers(&mut store, &api);
    assert_eq!((r.times_written, r.times_missing), (2, 0));
    assert_eq!(
        fresh(&store, 90_000).as_deref(),
        Some("2026-10-05T02:00:06Z")
    );
    assert_eq!(
        fresh(&store, 90_001).as_deref(),
        Some("2026-10-05T01:57:00Z")
    );
    assert_eq!(offer_stats(&store).unwrap().times_pending, 0);
}

#[test]
fn non_final_changes_are_rechecked_so_reorgs_are_undone() {
    let (dir, mut store, fake, api) = indexed("market_reorg");
    let head = fake.advance(100);
    // At head - 1 (above the pass's final level): 5422 sells one, 79120 is cancelled, and a
    // new offer appears.
    fake.edit_offer("collection_offers", "5422", |r| {
        r["value"]["amount"] = json!("3");
        r["lastLevel"] = json!(head - 1);
    });
    fake.edit_offer("offers", "79120", |r| {
        r["active"] = json!(false);
        r["lastLevel"] = json!(head - 1);
    });
    fake.push_key(offer_key(
        "offers",
        95_000_000,
        "90000",
        json!({"buyer": BUYER, "gentk": {"id": "133459", "version": "2"}, "price": "1"}),
        true,
        head - 1,
        head - 1,
    ));
    offers(&mut store, &api);
    let c = side(&dir);
    let remaining = || {
        one(
            &c,
            "SELECT remaining FROM market_offers WHERE kind = 'collection' AND offer_id = 5422",
        )
    };
    assert_eq!(remaining(), 3);
    assert!(
        !store
            .offer(FXHASH_V2.contract, OfferKind::Token, 79_120)
            .unwrap()
            .active
    );
    // A reorg drops that block: TzKT restores both keys to their earlier, final state (lower
    // lastLevel than stored) and forgets the new key.
    fake.edit_offer("collection_offers", "5422", |r| {
        r["value"]["amount"] = json!("4");
        r["lastLevel"] = json!(13_507_648);
    });
    fake.edit_offer("offers", "79120", |r| {
        r["active"] = json!(true);
        r["lastLevel"] = json!(6_874_174);
    });
    fake.inner
        .with(|s| s.listings.retain(|r| r["key"] != "90000"));
    fake.advance(10);
    let r = offers(&mut store, &api);
    assert_eq!((r.rechecked, r.recheck_missing), (2, 1), "{r:?}");
    assert_eq!(remaining(), 4, "the reorged-out sale is undone");
    let o = store
        .offer(FXHASH_V2.contract, OfferKind::Token, 79_120)
        .unwrap();
    assert_eq!((o.active, o.updated_level), (true, 6_874_174));
    // The key TzKT no longer has never existed on the final chain.
    let gone = store
        .offer(FXHASH_V2.contract, OfferKind::Token, 90_000)
        .unwrap();
    assert!(!gone.active);
    // Rows at final levels are not re-read again.
    let r = offers(&mut store, &api);
    assert_eq!((r.rechecked, r.recheck_missing), (0, 0));
}

#[test]
fn pages_never_regress_what_a_recheck_applied() {
    let (dir, mut store, fake, api) = indexed("market_stale");
    let head = fake.advance(100);
    fake.edit_offer("collection_offers", "5422", |r| {
        r["value"]["amount"] = json!("3");
        r["lastLevel"] = json!(head - 1);
    });
    offers(&mut store, &api);
    // Next run: the recheck reaches an up-to-date TzKT node (amount 2 at head + 5) while
    // the page comes from a lagging one that still has amount 3 at head - 1.
    let head2 = fake.advance(10);
    *fake.recheck_patch.lock().unwrap() = Some((
        "5422".into(),
        json!({"value": {"buyer": "tz1bmPatnVaBa8brRzQ15i4vWzcBtdwJAkbn", "price": "500000",
                         "amount": "2", "collection": "6539"}, "lastLevel": head2 - 5}),
    ));
    let r = offers(&mut store, &api);
    assert_eq!((r.rechecked, r.live), (1, 2), "{r:?}");
    let c = side(&dir);
    assert_eq!(
        one(
            &c,
            "SELECT remaining FROM market_offers WHERE kind = 'collection' AND offer_id = 5422"
        ),
        2
    );
    // Running again with nothing new changes nothing.
    *fake.recheck_patch.lock().unwrap() = None;
    fake.edit_offer("collection_offers", "5422", |r| {
        r["value"]["amount"] = json!("2");
        r["lastLevel"] = json!(head2 - 5);
    });
    let before = offer_stats(&store).unwrap();
    let r = offers(&mut store, &api);
    assert_eq!((r.live, r.ended), (0, 0), "{r:?}");
    assert_eq!(offer_stats(&store).unwrap(), before);
}

#[test]
fn unknown_token_versions_are_counted_and_not_listed() {
    let (dir, mut store, fake, api) = indexed("market_unsupported");
    let head = fake.advance(100);
    fake.push_key(offer_key(
        "offers",
        95_000_000,
        "90000",
        json!({"buyer": BUYER, "gentk": {"id": "1", "version": "3"}, "price": "1"}),
        true,
        head - 10,
        head - 10,
    ));
    let r = offers(&mut store, &api);
    assert_eq!(r.unsupported, 1);
    assert_eq!(
        one(
            &side(&dir),
            "SELECT count(*) FROM market_offers WHERE offer_id = 90000"
        ),
        0
    );
}
