//! Store reads for listings and offers: keyset pages, visibility, snapshots, the v7 upgrade.

mod ingest_support;
mod market_support;

use ingest_support::*;
use market_support::*;
use seeded_archive::ingest::escrow::{EscrowOptions, sync_escrows};
use seeded_archive::store::market::{FXHASH_V1, FXHASH_V2, Listing, Offer, OfferKind};
use seeded_archive::store::{Cursor, KeysetPage, Store, StoreError};
use serde_json::json;

fn bad_request<T: std::fmt::Debug>(r: Result<T, StoreError>) {
    assert!(matches!(r, Err(StoreError::BadRequest(_))), "{r:?}");
}

/// Every page of a keyset listing, checking that `total` stays put.
fn all_pages<T: Clone>(
    limit: u32,
    mut page: impl FnMut(Option<&Cursor>, u32) -> Result<KeysetPage<T>, StoreError>,
) -> (Vec<T>, usize) {
    let mut items = Vec::new();
    let mut after: Option<Cursor> = None;
    let mut pages = 0;
    let mut total = None;
    loop {
        let p = page(after.as_ref(), limit).unwrap();
        pages += 1;
        assert!(p.items.len() <= limit as usize);
        assert_eq!(*total.get_or_insert(p.total), p.total);
        items.extend(p.items.iter().cloned());
        match p.next {
            Some(next) => after = Some(next),
            None => break,
        }
        assert!(pages < 100);
    }
    assert_eq!(items.len() as u64, total.unwrap());
    (items, pages)
}

/// Listings by `HONEST` on visible fixture tokens, one on an unknown token and one on the
/// flagged project 554, plus a v1 listing. Levels and times are derived test data.
fn add_honest_listings(fake: &MarketFake) -> Vec<(u64, &'static str, &'static str, u64)> {
    let head = fake.head();
    // (level, contract, listing key, token id)
    let rows: Vec<(u64, &'static str, &'static str, u64)> = vec![
        (head - 900, V1, "800001", 34_558),
        (head - 800, V1, "800002", 36_174),
        (head - 700, PARAMS, "800003", 133_459),
        (head - 700, PARAMS, "800010", 133_460),
        (head - 600, PARAMS, "800005", 133_562),
        (head - 500, V2, "800006", 599_001),
        (head - 400, V2, "800007", 9_999_999),
        (head - 300, V1, "800008", 43_308),
    ];
    let version = |c: &str| match c {
        c if c == V1 => "0",
        c if c == V2 => "1",
        _ => "2",
    };
    fake.inner.with(|s| {
        for (i, (level, contract, key, token)) in rows.iter().enumerate() {
            s.listings.push(json!({
                "_escrow": MARKET_V2, "_path": "listings", "id": 80_000_000 + i as u64,
                "active": true, "key": key, "firstLevel": level, "lastLevel": level,
                "value": {"gentk": {"id": token.to_string(), "version": version(contract)},
                          "seller": HONEST, "price": format!("{}", 1_000_000 * (i as u64 + 1))}
            }));
        }
        // fxhash v1 listing (its big map is called offers), still unsold.
        s.listings.push(json!({
            "_escrow": MARKET_V1, "_path": "offers", "id": 9_000_000, "active": true,
            "key": "460000", "firstLevel": head - 700, "lastLevel": head - 700,
            "value": {"objkt_id": "36463", "issuer": HONEST, "creator": HONEST,
                      "objkt_amount": "1", "price": "2500000", "royalties": "150"}
        }));
    });
    for (level, ..) in &rows {
        fake.block(*level, &format!("2026-10-0{}T00:00:00Z", 1 + level % 4));
    }
    rows
}

#[test]
fn listings_by_seller_page_newest_first_with_prices_and_times() {
    let (_dir, mut store, fake, api) = indexed("market_listings");
    // New listings land after the escrow's synced level.
    let head = fake.advance(1000);
    add_honest_listings(&fake);
    sync_escrows(&mut store, &api, &EscrowOptions::default(), &|_| {}).unwrap();
    offers(&mut store, &api);

    let (items, pages): (Vec<Listing>, usize) = all_pages(2, |after, limit| {
        store.listings_by_seller(HONEST, after, limit)
    });
    let got: Vec<(u64, &str, u64)> = items
        .iter()
        .map(|l| (l.created_level, l.marketplace.contract, l.listing_id))
        .collect();
    // Newest first; level ties by marketplace contract, then listing id text. Unknown and
    // flagged tokens are left out.
    assert_eq!(
        got,
        vec![
            (head - 500, MARKET_V2, 800_006),
            (head - 600, MARKET_V2, 800_005),
            (head - 700, MARKET_V2, 800_003),
            (head - 700, MARKET_V2, 800_010),
            (head - 700, MARKET_V1, 460_000),
            (head - 800, MARKET_V2, 800_002),
            (head - 900, MARKET_V2, 800_001),
        ]
    );
    assert_eq!(pages, 4);
    let v1 = items.iter().find(|l| l.listing_id == 460_000).unwrap();
    assert_eq!(
        (v1.marketplace, v1.price_mutez),
        (FXHASH_V1, Some(2_500_000))
    );
    assert_eq!(
        (v1.token.contract.as_str(), v1.token.token_id),
        (V1, 36_463)
    );
    let first = &items[0];
    assert_eq!(first.price_mutez, Some(6_000_000));
    assert_eq!(first.token.project.id, 11_003);
    assert!(first.created_at.is_some());

    // The recorded KetiTaki listing: 7.1 tez on GENTK v2 #1072705, listed 2022-08-29.
    let keti = store.listings_by_seller(KETITAKI, None, 10).unwrap();
    assert_eq!(keti.total, 1);
    let l = &keti.items[0];
    assert_eq!(
        (
            l.marketplace,
            l.listing_id,
            l.price_mutez,
            l.seller.as_str()
        ),
        (FXHASH_V2, 459_588, Some(7_100_000), KETITAKI)
    );
    assert_eq!(
        (l.token.contract.as_str(), l.token.token_id),
        (V2, 1_072_705)
    );
    assert_eq!(
        (l.created_level, l.created_at.as_deref()),
        (2_664_789, Some("2022-08-29T18:24:29Z"))
    );

    // A cancelled listing leaves the list on the next escrow pass.
    let h = fake.advance(10);
    fake.inner.with(|s| {
        let row = s
            .listings
            .iter_mut()
            .find(|r| r["key"] == "800006")
            .unwrap();
        row["active"] = json!(false);
        row["lastLevel"] = json!(h - 5);
    });
    sync_escrows(&mut store, &api, &EscrowOptions::default(), &|_| {}).unwrap();
    assert_eq!(store.listings_by_seller(HONEST, None, 10).unwrap().total, 6);
}

#[test]
fn offers_by_buyer_page_newest_first() {
    let (_dir, mut store, fake, api) = indexed("market_buyer_pages");
    let head = fake.advance(1000);
    let tokens = [133_459_u64, 133_460, 133_562, 133_603, 133_604];
    for (i, token) in tokens.iter().enumerate() {
        // Two offers share a level to exercise the tiebreak.
        let level = head - 500 + 200 * (i as u64 / 2);
        fake.push_key(offer_key(
            "offers",
            95_000_000 + i as u64,
            &(90_000 + i as u64).to_string(),
            json!({"buyer": BUYER, "gentk": {"id": token.to_string(), "version": "2"},
                   "price": "1000000"}),
            true,
            level,
            level,
        ));
    }
    fake.push_key(offer_key(
        "collection_offers",
        95_100_000,
        "120000",
        json!({"buyer": BUYER, "price": "2000000", "amount": "3", "collection": "11003"}),
        true,
        head - 500,
        head - 500,
    ));
    offers(&mut store, &api);
    let (items, _): (Vec<Offer>, usize) =
        all_pages(2, |after, limit| store.offers_by_buyer(BUYER, after, limit));
    let got: Vec<(u64, OfferKind, u64)> = items
        .iter()
        .map(|o| (o.created_level, o.kind, o.offer_id))
        .collect();
    let mut want = got.clone();
    want.sort_by(|a, b| {
        b.0.cmp(&a.0)
            .then(a.1.as_str().cmp(b.1.as_str()))
            .then(a.2.cmp(&b.2))
    });
    assert_eq!(got, want, "newest first, then kind and id");
    assert_eq!(got[0].1, OfferKind::Token, "head - 100 is a token offer");
    let tie: Vec<OfferKind> = got
        .iter()
        .filter(|g| g.0 == head - 500)
        .map(|g| g.1)
        .collect();
    assert_eq!(
        tie,
        vec![OfferKind::Collection, OfferKind::Token, OfferKind::Token],
        "a level shared by both kinds"
    );
    // 5 new token offers, the recorded Arboreal one, and the collection offer.
    assert_eq!(items.len(), 7);
    let coll = items
        .iter()
        .find(|o| o.kind == OfferKind::Collection)
        .unwrap();
    assert_eq!(
        (coll.remaining, coll.locked_mutez, coll.project.id),
        (3, 6_000_000, 11_003)
    );

    // Errors: bad address, limit, foreign or tampered cursors.
    bad_request(store.offers_by_buyer("tz1nope", None, 10));
    bad_request(store.offers_by_buyer(BUYER, None, 0));
    bad_request(store.offers_by_buyer(BUYER, None, 101));
    let next = store.offers_by_buyer(BUYER, None, 2).unwrap().next.unwrap();
    bad_request(store.offers_by_buyer(HONEST, Some(&next), 2));
    bad_request(store.listings_by_seller(BUYER, Some(&next), 2));
    bad_request(store.offers_received(BUYER, Some(&next), 2));
    let mut tampered = next.as_str().to_owned();
    tampered.replace_range(tampered.len() - 2.., "00");
    bad_request(Cursor::parse(&tampered));
    bad_request(store.listings_by_seller("KT1short", None, 10));
}

#[test]
fn offer_and_token_lookups_validate_their_keys() {
    let (_dir, store, _fake, _api) = indexed("market_lookups");
    for r in [
        store.offer("tz1notamarket", OfferKind::Token, 1),
        store.offer(FXHASH_V2.contract, OfferKind::Token, u64::MAX),
        store.offer(FXHASH_V2.contract, OfferKind::Collection, 79_120),
    ] {
        assert!(matches!(r, Err(StoreError::NotFound { .. })), "{r:?}");
    }
    assert!(matches!(
        store.offers_for_token("KT1x", 1),
        Err(StoreError::NotFound { .. })
    ));
    assert!(matches!(
        store.offers_for_token(PARAMS, 999_999_999),
        Err(StoreError::NotFound { .. })
    ));
}

#[test]
fn reads_inside_one_snapshot_ignore_concurrent_writes() {
    let (dir, store, _fake, _api) = indexed("market_snapshot");
    let writer = side(&dir);
    let (before, inside) = store
        .read_snapshot(|s| {
            let before = s.offers_by_buyer(BUYER, None, 10)?.total;
            writer
                .execute(
                    "INSERT INTO market_offers (marketplace, kind, offer_id, buyer, price, \
                     remaining, token_contract, token_id, created_level, level, active) \
                     VALUES (?1, 'token', 999, ?2, 1, 1, ?3, 133459, 1, 1, 1)",
                    rusqlite::params![MARKET_V2, BUYER, PARAMS],
                )
                .unwrap();
            let inside = (
                s.offers_by_buyer(BUYER, None, 10)?.total,
                s.offers_for_token(PARAMS, 133_459)?.len(),
            );
            Ok((before, inside))
        })
        .unwrap();
    assert_eq!(inside, (before, 0), "the snapshot predates the write");
    assert_eq!(
        store.offers_by_buyer(BUYER, None, 10).unwrap().total,
        before + 1
    );
    assert_eq!(store.offers_for_token(PARAMS, 133_459).unwrap().len(), 1);
}

#[test]
fn v6_databases_upgrade_to_v7_and_reread_listing_prices() {
    let (dir, store, fake, api) = indexed("market_upgrade");
    drop(store);
    // Back to the v6 shape: no v7 tables, no listing price.
    side(&dir)
        .execute_batch(
            "DROP TABLE market_offers; DROP TABLE bigmap_state; DROP TABLE block_times; \
             DROP TABLE sync_rotation; DROP TABLE escrow_removed; \
             DROP INDEX escrow_listings_seller; ALTER TABLE escrow_listings DROP COLUMN price; \
             PRAGMA user_version = 6;",
        )
        .unwrap();
    assert!(
        Store::open_read_only(&dir.join("db.sqlite")).is_err(),
        "v6 is refused read-only"
    );
    let mut store = Store::open(&dir.join("db.sqlite")).unwrap();
    let c = side(&dir);
    assert_eq!(one(&c, "PRAGMA user_version"), 10);
    assert_eq!(
        one(
            &c,
            "SELECT count(*) FROM escrow_state WHERE synced_level IS NOT NULL"
        ),
        0,
        "every escrow pass restarts as a full re-read"
    );
    // Until then, listings are shown without price or time.
    let keti = store.listings_by_seller(KETITAKI, None, 10).unwrap();
    assert_eq!(
        (
            keti.items[0].price_mutez,
            keti.items[0].created_at.as_deref()
        ),
        (None, None)
    );
    sync_escrows(&mut store, &api, &EscrowOptions::default(), &|_| {}).unwrap();
    offers(&mut store, &api);
    let keti = store.listings_by_seller(KETITAKI, None, 10).unwrap();
    assert_eq!(keti.items[0].price_mutez, Some(7_100_000));
    assert_eq!(
        keti.items[0].created_at.as_deref(),
        Some("2022-08-29T18:24:29Z")
    );
    assert_eq!(
        one(
            &c,
            "SELECT count(*) FROM escrow_listings WHERE price IS NULL"
        ),
        0
    );
    assert!(
        store
            .offer(FXHASH_V2.contract, OfferKind::Token, 79_120)
            .unwrap()
            .active
    );
    drop(fake);
}

#[test]
fn escrow_changes_above_the_final_level_are_rechecked() {
    let (dir, mut store, fake, api) = indexed("market_escrow_reorg");
    let escrow =
        |store: &mut Store| sync_escrows(store, &api, &EscrowOptions::default(), &|_| {}).unwrap();
    let head = fake.advance(100);
    // At head - 1 (not final): KetiTaki cancels listing 459588 and HONEST lists a token.
    fake.inner.with(|s| {
        let row = s
            .listings
            .iter_mut()
            .find(|r| r["_path"] == "listings" && r["key"] == "459588")
            .unwrap();
        row["active"] = json!(false);
        row["lastLevel"] = json!(head - 1);
        s.listings.push(json!({
            "_escrow": MARKET_V2, "_path": "listings", "id": 80_100_000, "active": true,
            "key": "800100", "firstLevel": head - 1, "lastLevel": head - 1,
            "value": {"gentk": {"id": "133459", "version": "2"}, "seller": HONEST,
                      "price": "5000000"}
        }));
    });
    escrow(&mut store);
    assert_eq!(
        store.listings_by_seller(KETITAKI, None, 10).unwrap().total,
        0
    );
    assert_eq!(store.listings_by_seller(HONEST, None, 10).unwrap().total, 1);
    let c = side(&dir);
    assert_eq!(one(&c, "SELECT count(*) FROM escrow_removed"), 1);
    let seller = |store: &Store| {
        store
            .token(V2, 1_072_705)
            .unwrap()
            .owner
            .unwrap()
            .listed_by
            .map(|o| o.address)
    };
    assert_eq!(
        seller(&store),
        None,
        "cancelled: the escrowed token has no seller"
    );

    // A reorg drops that block: TzKT restores 459588 at its old level and forgets 800100.
    fake.inner.with(|s| {
        let row = s
            .listings
            .iter_mut()
            .find(|r| r["_path"] == "listings" && r["key"] == "459588")
            .unwrap();
        row["active"] = json!(true);
        row["lastLevel"] = json!(2_664_789);
        s.listings.retain(|r| r["key"] != "800100");
    });
    fake.advance(10);
    escrow(&mut store);
    assert_eq!(
        store.listings_by_seller(KETITAKI, None, 10).unwrap().total,
        1
    );
    assert_eq!(seller(&store).as_deref(), Some(KETITAKI));
    assert_eq!(store.listings_by_seller(HONEST, None, 10).unwrap().total, 0);
    assert_eq!(one(&c, "SELECT count(*) FROM escrow_removed"), 0);

    // A removal at a final level is applied once and its tombstone pruned.
    let head = fake.advance(10);
    fake.inner.with(|s| {
        let row = s
            .listings
            .iter_mut()
            .find(|r| r["_path"] == "listings" && r["key"] == "459588")
            .unwrap();
        row["active"] = json!(false);
        row["lastLevel"] = json!(head - 5);
    });
    escrow(&mut store);
    assert_eq!(
        store.listings_by_seller(KETITAKI, None, 10).unwrap().total,
        0
    );
    assert_eq!(one(&c, "SELECT count(*) FROM escrow_removed"), 0);
}
