//! TzKT sync and verification against an in-memory TzKT serving recorded rows.

mod ingest_support;

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use ingest_support::*;
use seeded_archive::ingest::escrow::{EscrowOptions, sync_escrows};
use seeded_archive::ingest::frank::bootstrap;
use seeded_archive::ingest::sync::{SyncOptions, SyncReport, sync};
use seeded_archive::ingest::verify::{VerifyOptions, verify};
use seeded_archive::ingest::{FINALITY, IngestError};
use seeded_archive::model::{IndexState, Source, Verification};
use seeded_archive::store::{PageRequest, Store, StoreError};
use serde_json::json;

fn page(offset: u32, limit: u32) -> PageRequest {
    PageRequest { offset, limit }
}

fn opts(batch: u64, concurrency: usize, contracts: &[&str]) -> SyncOptions {
    SyncOptions {
        contracts: contracts.iter().map(|c| (*c).to_owned()).collect(),
        limit: None,
        concurrency,
        batch,
    }
}

fn run(store: &mut Store, api: &seeded_archive::ingest::tzkt::Tzkt, o: &SyncOptions) -> SyncReport {
    sync(store, api, o, &|_| {}).unwrap()
}

fn escrows(store: &mut Store, api: &seeded_archive::ingest::tzkt::Tzkt, page: usize) -> SyncReport {
    let o = EscrowOptions { limit: None, page };
    sync_escrows(store, api, &o, &|_| {}).unwrap()
}

fn side(dir: &Path) -> rusqlite::Connection {
    rusqlite::Connection::open(dir.join("db.sqlite")).unwrap()
}

fn one(c: &rusqlite::Connection, sql: &str) -> i64 {
    c.query_row(sql, [], |r| r.get(0)).unwrap()
}

fn catalogue(
    c: &rusqlite::Connection,
    token: i64,
) -> (String, String, Option<String>, Option<String>) {
    c.query_row(
        "SELECT verify_state, chain_state, verify_detail, seed FROM catalogue_tokens \
         WHERE token_id = ?1",
        [token],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
    )
    .unwrap()
}

#[test]
fn full_sync_materialises_tokens_owners_and_verifies_the_catalogue() {
    let dir = test_dir("sync_full");
    let mut store = bootstrapped(&dir);
    let fake = FakeTzkt::recorded();
    let head = fake.with(|s| s.head);
    let stop = Arc::new(AtomicBool::new(false));
    let api = tzkt(&fake, &stop);
    let r = run(&mut store, &api, &opts(10_000, 3, &[]));
    assert_eq!(r.tokens_written, 21, "22 chain tokens, one never signed");
    assert_eq!(r.issues.get("unsigned"), Some(&1));
    assert_eq!(r.mismatches, 0);
    assert_eq!(r.completed.len(), 3);
    assert!(r.pending.is_empty() && !r.interrupted && !r.limited);
    assert_eq!(r.transient_failures, 0);

    let arboreal = store.project(ARBOREAL).unwrap();
    assert_eq!(arboreal.index_state, IndexState::Indexed);
    assert_eq!(arboreal.editions, Some(10));
    assert_eq!(arboreal.supply, Some(320));
    assert_eq!(arboreal.first_mint.as_deref(), Some("2023-11-10"));
    assert_eq!(arboreal.last_mint.as_deref(), Some("2024-03-25"));
    assert_eq!(
        arboreal.issuer.as_deref(),
        Some("KT1Xpmp15KfqoePNW9HczFmqaGNHwadV2a3b")
    );

    let t = store.token(PARAMS, 133_459).unwrap();
    assert_eq!(t.project_id, ARBOREAL);
    assert_eq!(t.iteration, 1);
    assert_eq!(
        t.seed,
        "ooHBH4Q9m5BTSExqbvNCtpWDbdEqZqTXK4Ewh1HMU66V5PhMUE7"
    );
    assert_eq!(t.minter, "tz1YJoT4wT65hm9yrrCQU7Vr2CtzTMhBmfQR");
    assert_eq!(t.minted, "2023-11-10");
    assert!(t.artifact_uri.starts_with("onchfs://a71e98fced01"));
    assert_eq!(t.features.len(), 9);
    assert_eq!(t.features[0], ("Palette".into(), "Gold".into()));
    assert_eq!(t.source, Source::Tzkt);
    assert_eq!(t.verification, Verification::ChainMatch);
    assert_eq!(t.params, None, "empty input bytes are no params");
    assert!(t.display_uri.as_deref().unwrap().starts_with("ipfs://"));
    assert!(t.thumbnail_uri.as_deref().unwrap().starts_with("ipfs://"));
    let owner = t.owner.unwrap();
    assert_eq!(owner.owner.address, "tz2RXosjAZdS85oCrguy7Eom757144dcU1FT");
    assert_eq!(owner.owner.alias.as_deref(), Some("looksdope.art"));
    assert_eq!(owner.source, "tzkt");
    assert!(owner.as_of.ends_with('Z'), "sync time: {}", owner.as_of);
    let collatz = store.token(PARAMS, 150_042).unwrap();
    assert!(
        collatz
            .params
            .as_deref()
            .unwrap()
            .starts_with("4048800000000000")
    );
    assert_eq!(collatz.iteration, 6);
    // Non-array chain attributes are no features.
    assert!(store.token(V2, 1_072_702).unwrap().features.is_empty());
    let shares = store.feature_shares(ARBOREAL).unwrap();
    assert!(shares.iter().all(|s| s.total == 10));
    assert_eq!(
        shares
            .iter()
            .filter(|s| s.name == "Palette")
            .map(|s| s.count)
            .sum::<u32>(),
        10
    );

    let quadrado = store.collector(QUADRADO).unwrap();
    assert_eq!(quadrado.alias.as_deref(), Some("quadrado"));
    assert_eq!(quadrado.token_count, 4);
    let held = store.collector_holdings(QUADRADO, page(0, 100)).unwrap();
    let iterations: Vec<u32> = held.items.iter().map(|t| t.iteration).collect();
    assert_eq!(iterations, vec![4, 5, 6, 9]);
    let second = store.collector_holdings(QUADRADO, page(1, 2)).unwrap();
    assert_eq!(second.total, 4);
    assert_eq!(
        second.items.iter().map(|t| t.iteration).collect::<Vec<_>>(),
        vec![5, 6]
    );
    // Flagged work is synced but never served.
    let honest = store.collector(HONEST).unwrap();
    assert_eq!(honest.token_count, 2);
    let honest_held: Vec<u64> = store
        .collector_holdings(HONEST, page(0, 100))
        .unwrap()
        .items
        .iter()
        .map(|t| t.token_id)
        .collect();
    assert_eq!(honest_held, vec![36_174, 36_463]);
    assert!(matches!(
        store.token(V1, 43_308),
        Err(StoreError::NotFound { .. })
    ));
    assert!(matches!(
        store.collector("tz1burnburnburnburnburnburnburjAYjjX"),
        Err(StoreError::NotFound { .. })
    ));
    assert_eq!(
        store
            .collector_holdings("tz1burnburnburnburnburnburnburjAYjjX", page(0, 10))
            .unwrap()
            .total,
        0
    );
    assert!(matches!(
        store.collector("nope"),
        Err(StoreError::BadRequest(_))
    ));
    // The unsigned mint is known but not materialised.
    assert!(matches!(
        store.token(V2, 1_619_286),
        Err(StoreError::NotFound { .. })
    ));

    let c = side(&dir);
    assert_eq!(catalogue(&c, 1_619_286).0, "unsigned");
    assert_eq!(catalogue(&c, 133_459).0, "match");
    assert_eq!(
        catalogue(&c, 36_500).1,
        "unknown",
        "absence needs confirmation"
    );
    assert_eq!(
        one(
            &c,
            "SELECT count(*) FROM catalogue_tokens WHERE verify_state = 'match'"
        ),
        21
    );
    let status = store.index_status().unwrap();
    assert_eq!(status.tokens_total, 19);
    assert_eq!(status.tokens_verified, 19);
    assert_eq!(status.tokens_mismatch, 0);
    assert_eq!(status.projects_indexed, 5);
    assert!(
        status
            .contracts
            .iter()
            .all(|s| s.last_level == Some(head - FINALITY))
    );
    assert_eq!(store.snapshot().unwrap().owner_source, "tzkt");

    // A second run is an incremental pass that finds nothing new.
    let again = run(&mut store, &api, &opts(10_000, 3, &[]));
    assert_eq!(again.tokens_written, 0);
    assert_eq!(again.completed.len(), 3);
    assert_eq!(one(&c, "SELECT count(*) FROM tokens"), 21);
}

#[test]
fn seed_mismatch_is_flagged_and_the_catalogue_is_not_overwritten() {
    let dir = test_dir("sync_mismatch");
    let copy = frank_copy(&dir);
    edit_json(&copy.join("public/data/seeds/2/0000.json"), |v| {
        assert_eq!(v["from"], 133_459);
        v["seeds"][0] = "ooFrankSeedDiffers".into();
    });
    let mut store = Store::open(&dir.join("db.sqlite")).unwrap();
    bootstrap(&mut store, &copy).unwrap();
    let fake = FakeTzkt::recorded();
    let stop = Arc::new(AtomicBool::new(false));
    let r = run(&mut store, &tzkt(&fake, &stop), &opts(10_000, 2, &[]));
    assert_eq!(r.mismatches, 1);

    let t = store.token(PARAMS, 133_459).unwrap();
    assert_eq!(
        t.seed,
        "ooHBH4Q9m5BTSExqbvNCtpWDbdEqZqTXK4Ewh1HMU66V5PhMUE7"
    );
    assert_eq!(t.verification, Verification::Mismatch);
    let (state, chain, detail, seed) = catalogue(&side(&dir), 133_459);
    assert_eq!((state.as_str(), chain.as_str()), ("mismatch", "present"));
    assert_eq!(detail.as_deref(), Some("seed"));
    assert_eq!(seed.as_deref(), Some("ooFrankSeedDiffers"));
    let status = store.index_status().unwrap();
    assert_eq!((status.tokens_mismatch, status.tokens_verified), (1, 18));
}

#[test]
fn a_failure_mid_batch_rolls_back_and_does_not_advance_the_cursor() {
    let dir = test_dir("sync_rollback");
    let mut store = bootstrapped(&dir);
    let c = side(&dir);
    c.execute_batch(
        "CREATE TRIGGER fail_once BEFORE INSERT ON tokens WHEN NEW.token_id = 185776 \
         BEGIN SELECT RAISE(ABORT, 'injected write failure'); END;",
    )
    .unwrap();
    let fake = FakeTzkt::recorded();
    let stop = Arc::new(AtomicBool::new(false));
    let api = tzkt(&fake, &stop);
    let o = opts(1_000, 1, &[PARAMS]);
    let err = sync(&mut store, &api, &o, &|_| {}).unwrap_err();
    assert!(
        matches!(err, IngestError::Store(StoreError::Internal(_))),
        "{err:?}"
    );
    // Earlier chunks committed; the failing chunk left no rows and no done marker.
    assert_eq!(
        one(&c, "SELECT count(*) FROM tokens WHERE token_id < 185000"),
        9
    );
    assert_eq!(
        one(&c, "SELECT count(*) FROM tokens WHERE token_id >= 185000"),
        0
    );
    assert_eq!(
        one(
            &c,
            "SELECT count(*) FROM sync_chunks WHERE lo <= 185776 AND hi > 185776"
        ),
        0
    );
    assert_eq!(
        one(
            &c,
            "SELECT count(*) FROM sync_state WHERE synced_level IS NOT NULL"
        ),
        0
    );
    let before = fake.requests_containing("&tokenId.ge=133459");
    assert_eq!(before, 1);

    c.execute_batch("DROP TRIGGER fail_once").unwrap();
    let r = run(&mut store, &api, &o);
    assert_eq!(r.completed, vec![PARAMS.to_owned()]);
    assert_eq!(one(&c, "SELECT count(*) FROM tokens"), 11);
    assert_eq!(
        fake.requests_containing("&tokenId.ge=133459"),
        before,
        "done chunks are not fetched again"
    );
    assert_eq!(store.project(ARBOREAL).unwrap().editions, Some(10));
}

#[test]
fn an_interrupted_sync_keeps_committed_batches_and_resumes() {
    let dir = test_dir("sync_interrupt");
    let mut store = bootstrapped(&dir);
    let fake = FakeTzkt::recorded();
    let stop = Arc::new(AtomicBool::new(false));
    fake.with(|s| s.stop_after = Some((12, stop.clone())));
    let api = tzkt(&fake, &stop);
    let o = opts(1_000, 1, &[PARAMS]);
    let r = run(&mut store, &api, &o);
    assert!(r.interrupted);
    assert!(r.pending.contains(&PARAMS.to_owned()));
    let c = side(&dir);
    let chunks = one(&c, "SELECT count(*) FROM sync_chunks");
    assert!(chunks >= 1, "batches before the stop are kept");

    fake.with(|s| s.stop_after = None);
    stop.store(false, Ordering::Relaxed);
    let r = run(&mut store, &api, &o);
    assert!(!r.interrupted);
    assert_eq!(r.completed, vec![PARAMS.to_owned()]);
    assert_eq!(one(&c, "SELECT count(*) FROM tokens"), 11);
}

#[test]
fn limit_stops_after_n_batches_and_the_next_run_continues() {
    let dir = test_dir("sync_limit");
    let mut store = bootstrapped(&dir);
    let fake = FakeTzkt::recorded();
    let stop = Arc::new(AtomicBool::new(false));
    let api = tzkt(&fake, &stop);
    let mut o = opts(1_000, 2, &[PARAMS]);
    o.limit = Some(3);
    let r = run(&mut store, &api, &o);
    assert!(r.limited);
    assert_eq!(r.batches, 3);
    assert_eq!(one(&side(&dir), "SELECT count(*) FROM sync_chunks"), 3);
    o.limit = None;
    let r = run(&mut store, &api, &o);
    assert_eq!(r.completed, vec![PARAMS.to_owned()]);
}

#[test]
fn transient_errors_are_retried_and_only_a_definite_absence_is_recorded() {
    let dir = test_dir("sync_transient");
    let mut store = bootstrapped(&dir);
    let fake = FakeTzkt::recorded();
    fake.with(|s| {
        s.faults.push(Fault {
            contains: "/tokens?contract=".into(),
            status: 503,
            times: Some(2),
        })
    });
    let stop = Arc::new(AtomicBool::new(false));
    let api = tzkt(&fake, &stop);
    let r = run(&mut store, &api, &opts(10_000, 1, &[]));
    assert_eq!(r.tokens_written, 21);
    assert_eq!(r.transient_failures, 0);
    assert_eq!(api.client.counters.retries.load(Ordering::Relaxed), 2);

    // 36500 answers 404 (absent); 36501 keeps failing (transient, nothing recorded).
    fake.with(|s| {
        s.faults.push(Fault {
            contains: "tokenId=36500&".into(),
            status: 404,
            times: None,
        });
        s.faults.push(Fault {
            contains: "tokenId=36501&".into(),
            status: 503,
            times: None,
        });
    });
    let o = VerifyOptions {
        sample: 0,
        absent_limit: 10,
        seed: 7,
    };
    let v = verify(&mut store, Some(&api), &o).unwrap();
    assert_eq!(v.absent_checked, 2);
    assert_eq!(v.absent_confirmed, 1);
    assert_eq!(v.transient, 1);
    let c = side(&dir);
    let (state, chain, _, _) = catalogue(&c, 36_500);
    assert_eq!((state.as_str(), chain.as_str()), ("absent", "absent"));
    let (state, chain, _, _) = catalogue(&c, 36_501);
    assert_eq!((state.as_str(), chain.as_str()), ("unverified", "unknown"));

    // A persistent server error during sync leaves the work pending, not failed.
    fake.with(|s| {
        s.faults.push(Fault {
            contains: "/head".into(),
            status: 502,
            times: None,
        })
    });
    let r = run(&mut store, &api, &opts(10_000, 1, &[]));
    assert_eq!(r.transient_failures, 1);
    assert_eq!(one(&c, "SELECT count(*) FROM tokens"), 21);
}

#[test]
fn incremental_pass_applies_transfers_and_advances_the_level() {
    let dir = test_dir("sync_incremental");
    let mut store = bootstrapped(&dir);
    let fake = FakeTzkt::recorded();
    let stop = Arc::new(AtomicBool::new(false));
    let api = tzkt(&fake, &stop);
    run(&mut store, &api, &opts(10_000, 2, &[]));
    let head = fake.with(|s| {
        s.head += 100;
        let level = s.head - 50;
        for t in s.tokens.iter_mut() {
            if t["tokenId"] == "133603" {
                t["lastLevel"] = json!(level);
            }
            // An unsigned mint gets signed; its ledger row does not change.
            if t["tokenId"] == "1619286" {
                t["lastLevel"] = json!(level);
                t["metadata.name"] = json!("Complementary Dot Logic #297");
                t["metadata.iterationHash"] = json!("ooSignedLater");
                t["metadata.artifactUri"] = json!("ipfs://QmSigned?fxhash=ooSignedLater");
            }
        }
        for b in s.balances.iter_mut() {
            if b["token.tokenId"] == "133603" {
                b["balance"] = json!("0");
                b["lastLevel"] = json!(level);
            }
        }
        s.balances.push(json!({
            "_contract": PARAMS, "id": 9_999_999_999_999_999_u64, "token.tokenId": "133603",
            "account.address": HONEST, "balance": "1", "lastLevel": level
        }));
        s.head
    });
    let r = run(&mut store, &api, &opts(10_000, 2, &[]));
    assert_eq!(r.owners_cleared, 1);
    assert_eq!(r.owners_set, 2);
    let signed = store.token(V2, 1_619_286).unwrap();
    assert_eq!(signed.seed, "ooSignedLater");
    assert_eq!(signed.iteration, 297);
    assert_eq!(
        signed.owner.unwrap().owner.address,
        "tz1LYrMpZxUmmFArhgt49UHcLDEDjcvs6gQZ",
        "a token first written by the change feed gets its current owner"
    );
    assert_eq!(signed.verification, Verification::Mismatch);
    assert_eq!(
        store.project(11229).unwrap().index_state,
        IndexState::Indexed
    );
    let t = store.token(PARAMS, 133_603).unwrap();
    assert_eq!(t.owner.unwrap().owner.address, HONEST);
    assert_eq!(store.collector(QUADRADO).unwrap().token_count, 3);
    assert_eq!(store.collector(HONEST).unwrap().token_count, 3);
    let status = store.index_status().unwrap();
    assert!(
        status
            .contracts
            .iter()
            .all(|s| s.last_level == Some(head - FINALITY))
    );
}

fn owner_of_133603(store: &Store) -> String {
    store
        .token(PARAMS, 133_603)
        .unwrap()
        .owner
        .unwrap()
        .owner
        .address
}

/// Moves 133603 from its holder to `to` in the head block (TzKT row id `id`); returns that level.
fn transfer_133603_in_the_head_block(fake: &FakeTzkt, to: &str, id: u64) -> u64 {
    fake.with(|s| {
        s.head += 100;
        for b in s.balances.iter_mut() {
            if b["token.tokenId"] == "133603" {
                b["balance"] = json!("0");
                b["lastLevel"] = json!(s.head);
            }
        }
        for t in s.tokens.iter_mut() {
            if t["tokenId"] == "133603" {
                t["lastLevel"] = json!(s.head);
            }
        }
        s.balances.push(json!({
            "_contract": PARAMS, "id": id, "token.tokenId": "133603",
            "account.address": to, "balance": "1", "lastLevel": s.head
        }));
        s.head
    })
}

#[test]
fn a_transfer_is_read_once_final_and_a_replaced_block_counts() {
    const THIRD: &str = "tz1XnUuqs7A8vbS2YWR7UeqVkvJxgE7mzekH";
    let dir = test_dir("sync_reorg_tail");
    let mut store = bootstrapped(&dir);
    let fake = FakeTzkt::recorded();
    let stop = Arc::new(AtomicBool::new(false));
    let api = tzkt(&fake, &stop);
    run(&mut store, &api, &opts(10_000, 2, &[]));
    assert_eq!(owner_of_133603(&store), QUADRADO);
    // Not final yet: not read.
    let level = transfer_133603_in_the_head_block(&fake, HONEST, 9_999_999_999_999_996);
    run(&mut store, &api, &opts(10_000, 2, &[]));
    assert_eq!(owner_of_133603(&store), QUADRADO);
    // The block is replaced at the same level: the token went to THIRD instead.
    fake.with(|s| {
        s.balances
            .retain(|b| b["account.address"] != HONEST || b["token.tokenId"] != "133603");
        s.balances.push(json!({
            "_contract": PARAMS, "id": 9_999_999_999_999_995_u64, "token.tokenId": "133603",
            "account.address": THIRD, "balance": "1", "lastLevel": level
        }));
        s.head = level + FINALITY;
    });
    run(&mut store, &api, &opts(10_000, 2, &[]));
    assert_eq!(owner_of_133603(&store), THIRD);
}

#[test]
fn a_rolled_back_transfer_never_moves_the_owner_across_a_restart() {
    let dir = test_dir("sync_rollback_transfer");
    let mut store = bootstrapped(&dir);
    let fake = FakeTzkt::recorded();
    let stop = Arc::new(AtomicBool::new(false));
    let api = tzkt(&fake, &stop);
    run(&mut store, &api, &opts(10_000, 2, &[]));
    let before = fake.with(|s| (s.tokens.clone(), s.balances.clone()));
    let level = transfer_133603_in_the_head_block(&fake, HONEST, 9_999_999_999_999_996);
    run(&mut store, &api, &opts(10_000, 2, &[]));
    assert_eq!(owner_of_133603(&store), QUADRADO);
    drop(store);
    let mut store = Store::open(&dir.join("db.sqlite")).unwrap();
    // Rolled back without replacement: TzKT restores the rows, stamped with the reverted level.
    fake.with(|s| {
        (s.tokens, s.balances) = before;
        for b in s.balances.iter_mut() {
            if b["token.tokenId"] == "133603" {
                b["lastLevel"] = json!(level);
            }
        }
        for t in s.tokens.iter_mut() {
            if t["tokenId"] == "133603" {
                t["lastLevel"] = json!(level);
            }
        }
    });
    for _ in 0..=FINALITY {
        fake.with(|s| s.head += 1);
        run(&mut store, &api, &opts(10_000, 2, &[]));
        assert_eq!(owner_of_133603(&store), QUADRADO);
    }
}

#[test]
fn a_rolled_back_mint_is_never_ingested_and_a_final_one_is() {
    let dir = test_dir("sync_rollback_mint");
    let mut store = bootstrapped(&dir);
    let fake = FakeTzkt::recorded();
    let stop = Arc::new(AtomicBool::new(false));
    let api = tzkt(&fake, &stop);
    run(&mut store, &api, &opts(10_000, 2, &[]));
    let editions = |store: &Store| store.project(ARBOREAL).unwrap().editions;
    let absent = |store: &Store| {
        matches!(
            store.token(PARAMS, 188_700),
            Err(StoreError::NotFound { .. })
        )
    };
    assert!(absent(&store));
    assert_eq!(editions(&store), Some(10));
    let before = fake.with(|s| (s.tokens.clone(), s.mints.clone(), s.balances.clone()));
    // Edition 11 minted in the head block.
    let mint_at = |fake: &FakeTzkt, level: u64| {
        fake.with(|s| {
            let mut t = s
                .tokens
                .iter()
                .find(|t| t["tokenId"] == "188628")
                .unwrap()
                .clone();
            t["id"] = json!(9_000_000_000_000_001_u64);
            t["tokenId"] = json!("188700");
            t["firstLevel"] = json!(level);
            t["lastLevel"] = json!(level);
            t["metadata.iterationHash"] = json!("ooLateToken");
            t["metadata.artifactUri"] = json!(
                "onchfs://a71e98fced01c865f33be138100b5ea5486f32fc333ad1503f814b201234130c/\
                 ?fxhash=ooLateToken&fxiteration=11"
            );
            let mut m = s
                .mints
                .iter()
                .find(|m| m["parameter"]["value"]["token_id"] == "188628")
                .unwrap()
                .clone();
            m["id"] = json!(9_000_000_000_000_002_u64);
            m["level"] = json!(level);
            m["parameter"]["value"]["token_id"] = json!("188700");
            m["parameter"]["value"]["iteration"] = json!("11");
            s.mints.push(m);
            s.tokens.push(t);
            s.balances.push(json!({
                "_contract": PARAMS, "id": 9_000_000_000_000_003_u64, "token.tokenId": "188700",
                "account.address": QUADRADO, "balance": "1", "lastLevel": level
            }));
        })
    };
    let head = fake.with(|s| {
        s.head += 100;
        s.head
    });
    mint_at(&fake, head);
    run(&mut store, &api, &opts(10_000, 2, &[]));
    assert!(
        absent(&store),
        "a mint in a block that is not final is not read"
    );
    assert_eq!(editions(&store), Some(10));
    drop(store);
    let mut store = Store::open(&dir.join("db.sqlite")).unwrap();
    // Rolled back without replacement: TzKT deletes the mint, the token and its balance.
    fake.with(|s| (s.tokens, s.mints, s.balances) = before);
    for _ in 0..=FINALITY {
        fake.with(|s| s.head += 1);
        run(&mut store, &api, &opts(10_000, 2, &[]));
        assert!(absent(&store));
        assert_eq!(editions(&store), Some(10));
    }
    // Minted again later; read once that block is final.
    let level = fake.with(|s| s.head);
    mint_at(&fake, level);
    run(&mut store, &api, &opts(10_000, 2, &[]));
    assert!(absent(&store));
    fake.with(|s| s.head = level + FINALITY);
    run(&mut store, &api, &opts(10_000, 2, &[]));
    assert_eq!(store.token(PARAMS, 188_700).unwrap().iteration, 11);
    assert_eq!(editions(&store), Some(11));
}

#[test]
fn live_sample_rechecks_catalogue_tokens_and_counts_stale_rows() {
    let dir = test_dir("verify_sample");
    let mut store = bootstrapped(&dir);
    let fake = FakeTzkt::recorded();
    let stop = Arc::new(AtomicBool::new(false));
    let api = tzkt(&fake, &stop);
    run(&mut store, &api, &opts(10_000, 2, &[]));
    fake.with(|s| {
        for t in s.tokens.iter_mut() {
            if t["tokenId"] == "133460" {
                t["metadata.iterationHash"] = json!("ooChainChangedSince");
            }
        }
    });
    let o = VerifyOptions {
        sample: 100,
        absent_limit: 0,
        seed: 3,
    };
    let v = verify(&mut store, Some(&api), &o).unwrap();
    assert_eq!(v.sampled, 24);
    assert_eq!(v.sample.get("absent"), Some(&2));
    assert_eq!(v.sample.get("unsigned"), Some(&1));
    assert_eq!(v.sample.get("mismatch"), Some(&1));
    assert_eq!(v.sample.get("match"), Some(&20));
    assert_eq!(v.stale_rows, 1);
    assert_eq!(v.transient, 0);
    assert_eq!(v.totals.get("absent"), Some(&2));
    assert_eq!(v.examples.len(), 1);
    assert!(v.examples[0].contains("133460"));
}

#[test]
fn skipped_tokens_are_retried_on_the_next_pass() {
    let dir = test_dir("sync_retry");
    let copy = frank_copy(&dir);
    edit_json(&copy.join("public/data/tokens/index-000.json"), |v| {
        v.as_array_mut().unwrap().retain(|p| p["id"] != 29632);
    });
    let mut store = Store::open(&dir.join("db.sqlite")).unwrap();
    bootstrap(&mut store, &copy).unwrap();
    let fake = FakeTzkt::recorded();
    let stop = Arc::new(AtomicBool::new(false));
    let api = tzkt(&fake, &stop);
    let r = run(&mut store, &api, &opts(10_000, 2, &[PARAMS]));
    // Recorded by the full pass and refreshed by the incremental retry right after it.
    assert!(r.issues.contains_key("project_unknown"));
    assert_eq!(
        one(
            &side(&dir),
            "SELECT count(*) FROM ingest_issues WHERE kind = 'project_unknown'"
        ),
        1
    );
    assert!(matches!(
        store.token(PARAMS, 150_042),
        Err(StoreError::NotFound { .. })
    ));

    // The project arrives with a later catalogue; nothing on chain changed.
    bootstrap(&mut store, &frank()).unwrap();
    run(&mut store, &api, &opts(10_000, 2, &[PARAMS]));
    let t = store.token(PARAMS, 150_042).unwrap();
    assert_eq!(
        t.owner.unwrap().owner.address,
        "tz1YnuVaS6mFDwBMC58krtU6BNQ1s5cGnVZR"
    );
    assert_eq!(
        one(
            &side(&dir),
            "SELECT count(*) FROM ingest_issues WHERE kind = 'project_unknown'"
        ),
        0
    );
}

#[test]
fn a_missing_bounds_endpoint_is_permanent_and_completes_nothing() {
    let dir = test_dir("sync_bounds");
    let mut store = bootstrapped(&dir);
    let fake = FakeTzkt::recorded();
    fake.with(|s| {
        s.faults.push(Fault {
            contains: "sort.asc=tokenId".into(),
            status: 404,
            times: None,
        })
    });
    let stop = Arc::new(AtomicBool::new(false));
    let err = sync(
        &mut store,
        &tzkt(&fake, &stop),
        &opts(10_000, 1, &[PARAMS]),
        &|_| {},
    )
    .unwrap_err();
    assert!(!err.is_transient(), "{err:?}");
    assert_eq!(
        one(
            &side(&dir),
            "SELECT count(*) FROM sync_state WHERE synced_level IS NOT NULL"
        ),
        0
    );
}

const ESCROW_V2: &str = "KT1GbyoDi7H1sfXmimXpptZJuCdHMh66WS9u";
const KETITAKI: &str = "tz1WMa1cAKk4JjFGZCpXNkTDi5o5AqCsFF6E";

#[test]
fn escrowed_tokens_are_credited_to_their_seller() {
    let dir = test_dir("escrow_sellers");
    let mut store = bootstrapped(&dir);
    let fake = FakeTzkt::recorded();
    let stop = Arc::new(AtomicBool::new(false));
    let api = tzkt(&fake, &stop);
    run(&mut store, &api, &opts(10_000, 2, &[]));
    let r = escrows(&mut store, &api, 10_000);
    assert_eq!(r.listings_live, 2);
    assert!(r.completed.contains(&ESCROW_V2.to_owned()));

    let t = store.token(V2, 1_072_705).unwrap();
    let owner = t.owner.unwrap();
    assert_eq!(
        owner.owner.address, ESCROW_V2,
        "the ledger owner stays the escrow"
    );
    assert_eq!(owner.escrow.as_deref(), Some("fxhash marketplace v2"));
    let seller = owner.listed_by.unwrap();
    assert_eq!(seller.address, KETITAKI);
    assert_eq!(seller.alias.as_deref(), Some("KetiTaki"));

    let keti = store.collector(KETITAKI).unwrap();
    assert_eq!((keti.token_count, keti.listed), (1, 1));
    let held = store.collector_holdings(KETITAKI, page(0, 10)).unwrap();
    assert_eq!(held.items[0].token_id, 1_072_705);
    // The escrow is not a collector.
    assert!(matches!(
        store.collector(ESCROW_V2),
        Err(StoreError::NotFound { .. })
    ));
    assert_eq!(
        store
            .collector_holdings(ESCROW_V2, page(0, 10))
            .unwrap()
            .total,
        0
    );
    // A token outside escrow has no seller or escrow name.
    let plain = store.token(PARAMS, 133_459).unwrap().owner.unwrap();
    assert_eq!((plain.listed_by, plain.escrow), (None, None));
}

#[test]
fn ambiguous_or_withdrawn_listings_stay_unresolved_and_sales_move_the_holder() {
    let dir = test_dir("escrow_changes");
    let mut store = bootstrapped(&dir);
    let fake = FakeTzkt::recorded();
    let stop = Arc::new(AtomicBool::new(false));
    let api = tzkt(&fake, &stop);
    run(&mut store, &api, &opts(10_000, 2, &[]));
    escrows(&mut store, &api, 10_000);

    // A second live listing for the same token: two sellers, so no seller.
    fake.with(|s| {
        s.head += 100;
        let level = s.head - 10;
        s.listings.push(json!({
            "_escrow": ESCROW_V2, "_path": "listings", "id": 99_000_001_u64, "active": true,
            "key": "9999999", "lastLevel": level,
            "value": {"gentk": {"id": "1072705", "version": "1"}, "seller": HONEST, "price": "1"}
        }));
    });
    escrows(&mut store, &api, 10_000);
    let owner = store.token(V2, 1_072_705).unwrap().owner.unwrap();
    assert_eq!(owner.owner.address, ESCROW_V2);
    assert_eq!(owner.listed_by, None, "never guess between two sellers");
    assert!(matches!(
        store.collector(KETITAKI),
        Err(StoreError::NotFound { .. })
    ));

    // Both listings withdrawn, then the Arboreal listing sells to quadrado.
    fake.with(|s| {
        s.head += 100;
        let level = s.head - 10;
        for l in s.listings.iter_mut() {
            if l["value"]["gentk"]["id"] == "1072705" {
                l["active"] = json!(false);
                l["lastLevel"] = json!(level);
            }
            if l["value"]["gentk"]["id"] == "188628" {
                l["active"] = json!(false);
                l["lastLevel"] = json!(level);
            }
        }
        for b in s.balances.iter_mut() {
            if b["token.tokenId"] == "188628" {
                b["balance"] = json!("0");
                b["lastLevel"] = json!(level);
            }
        }
        s.balances.push(json!({
            "_contract": PARAMS, "id": 9_999_999_999_999_998_u64, "token.tokenId": "188628",
            "account.address": QUADRADO, "balance": "1", "lastLevel": level
        }));
    });
    run(&mut store, &api, &opts(10_000, 2, &[]));
    let r = escrows(&mut store, &api, 10_000);
    assert_eq!(r.listings_gone, 3);
    assert_eq!(one(&side(&dir), "SELECT count(*) FROM escrow_listings"), 0);
    let sold = store.token(PARAMS, 188_628).unwrap().owner.unwrap();
    assert_eq!(sold.owner.address, QUADRADO);
    assert_eq!((sold.listed_by, sold.escrow), (None, None));
    assert_eq!(store.collector(QUADRADO).unwrap().token_count, 5);
    let withdrawn = store.token(V2, 1_072_705).unwrap().owner.unwrap();
    assert_eq!(
        withdrawn.listed_by, None,
        "still in escrow, no live listing"
    );
}

#[test]
fn escrow_failures_stay_transient_and_leave_tokens_shown_as_escrow() {
    let dir = test_dir("escrow_transient");
    let mut store = bootstrapped(&dir);
    let fake = FakeTzkt::recorded();
    fake.with(|s| {
        s.faults.push(Fault {
            contains: "/bigmaps/listings/keys".into(),
            status: 503,
            times: None,
        })
    });
    let stop = Arc::new(AtomicBool::new(false));
    let api = tzkt(&fake, &stop);
    run(&mut store, &api, &opts(10_000, 2, &[]));
    let o = EscrowOptions {
        limit: None,
        page: 10_000,
    };
    let r = sync_escrows(&mut store, &api, &o, &|_| {}).unwrap();
    assert_eq!(r.transient_failures, 1);
    assert!(r.pending.contains(&ESCROW_V2.to_owned()));
    let owner = store.token(V2, 1_072_705).unwrap().owner.unwrap();
    assert_eq!(
        (owner.listed_by, owner.escrow.as_deref()),
        (None, Some("fxhash marketplace v2"))
    );
    assert!(matches!(
        store.collector(KETITAKI),
        Err(StoreError::NotFound { .. })
    ));

    fake.with(|s| s.faults.clear());
    let r = escrows(&mut store, &api, 10_000);
    assert!(r.pending.is_empty(), "{:?}", r.pending);
    assert_eq!(store.collector(KETITAKI).unwrap().listed, 1);
}

#[test]
fn escrow_listing_pages_follow_the_big_map_cursor() {
    let dir = test_dir("escrow_paging");
    let mut store = bootstrapped(&dir);
    let fake = FakeTzkt::recorded();
    fake.with(|s| {
        // 45 more live listings for tokens outside the index: three pages of 20.
        for i in 0..45_u64 {
            s.listings.push(json!({
                "_escrow": ESCROW_V2, "_path": "listings", "id": 1_000 + i, "active": true,
                "key": format!("{}", 5_000_000 + i), "lastLevel": 100,
                "value": {"gentk": {"id": format!("{}", 1_500_000 + i), "version": "1"},
                          "seller": HONEST, "price": "1"}
            }));
        }
    });
    let stop = Arc::new(AtomicBool::new(false));
    let api = tzkt(&fake, &stop);
    run(&mut store, &api, &opts(10_000, 2, &[]));
    // Listing pages of 20.
    let r = escrows(&mut store, &api, 20);
    assert!(
        r.completed.contains(&ESCROW_V2.to_owned()),
        "{:?}",
        r.pending
    );
    assert_eq!(r.listings_live, 47);
    assert_eq!(one(&side(&dir), "SELECT count(*) FROM escrow_listings"), 47);
    assert!(fake.requests_containing("offset.cr=") >= 3);
    assert_eq!(store.collector(KETITAKI).unwrap().listed, 1);
}

fn issue_on(dir: &Path, kind: &str, token_id: i64) -> i64 {
    side(dir)
        .query_row(
            "SELECT count(*) FROM ingest_issues WHERE kind = ?1 AND token_id = ?2",
            rusqlite::params![kind, token_id],
            |r| r.get(0),
        )
        .unwrap()
}

/// Drops the recorded token row for `token_id`, keeping its mint and balance.
fn hide_token(fake: &FakeTzkt, token_id: &str) -> serde_json::Value {
    fake.with(|s| {
        let i = s
            .tokens
            .iter()
            .position(|t| t["tokenId"] == token_id)
            .unwrap();
        s.tokens.remove(i)
    })
}

fn transfer_133603_to_honest(fake: &FakeTzkt) {
    fake.with(|s| {
        s.head += 100;
        let level = s.head - 50;
        for t in s.tokens.iter_mut() {
            if t["tokenId"] == "133603" {
                t["lastLevel"] = json!(level);
            }
        }
        for b in s.balances.iter_mut() {
            if b["token.tokenId"] == "133603" {
                b["balance"] = json!("0");
                b["lastLevel"] = json!(level);
            }
        }
        s.balances.push(json!({
            "_contract": PARAMS, "id": 9_999_999_999_999_997_u64, "token.tokenId": "133603",
            "account.address": HONEST, "balance": "1", "lastLevel": level
        }));
    });
}

#[test]
fn repeated_small_limits_complete_every_incremental_phase() {
    for limit in [1, 2] {
        let dir = test_dir(&format!("sync_phases_{limit}"));
        let mut store = bootstrapped(&dir);
        let fake = FakeTzkt::recorded();
        let missing = hide_token(&fake, "133605");
        let stop = Arc::new(AtomicBool::new(false));
        let api = tzkt(&fake, &stop);
        run(&mut store, &api, &opts(10_000, 2, &[]));
        assert_eq!(issue_on(&dir, "token_missing", 133605), 1);
        // The missing token reappears (same lastLevel), and another one changes hands.
        fake.with(|s| s.tokens.push(missing.clone()));
        transfer_133603_to_honest(&fake);
        let head = fake.with(|s| s.head);
        let mut o = opts(10_000, 2, &[]);
        o.limit = Some(limit);
        let caught_up = |dir: &Path| {
            one(
                &side(dir),
                &format!(
                    "SELECT count(*) FROM sync_state WHERE synced_level >= {} \
                     AND incr_level IS NULL",
                    head - FINALITY
                ),
            ) == 3
        };
        let mut runs = 0;
        while !caught_up(&dir) {
            runs += 1;
            assert!(runs <= 40, "limit {limit}: phases did not converge");
            let r = run(&mut store, &api, &o);
            assert!(r.limited || r.pending.is_empty(), "{r:?}");
        }
        assert!(runs > 3, "limit {limit} took {runs} runs");
        let moved = store.token(PARAMS, 133_603).unwrap().owner.unwrap();
        assert_eq!(moved.owner.address, HONEST, "limit {limit}");
        let back = store.token(PARAMS, 133_605).unwrap().owner.unwrap();
        assert_eq!(back.owner.address, QUADRADO, "limit {limit}");
        assert_eq!(issue_on(&dir, "token_missing", 133605), 0);
        let status = store.index_status().unwrap();
        assert!(
            status
                .contracts
                .iter()
                .all(|s| s.last_level == Some(head - FINALITY))
        );
    }
}

#[test]
fn a_token_missing_from_a_successful_response_is_retried_until_it_appears() {
    // Full pass.
    let dir = test_dir("sync_missing_full");
    let mut store = bootstrapped(&dir);
    let fake = FakeTzkt::recorded();
    let missing = hide_token(&fake, "133460");
    let stop = Arc::new(AtomicBool::new(false));
    let api = tzkt(&fake, &stop);
    let r = run(&mut store, &api, &opts(10_000, 2, &[]));
    assert_eq!(
        r.issues.get("token_missing"),
        Some(&2),
        "full pass, then the retry phase"
    );
    assert_eq!(issue_on(&dir, "token_missing", 133460), 1);
    assert!(matches!(
        store.token(PARAMS, 133_460),
        Err(StoreError::NotFound { .. })
    ));
    fake.with(|s| {
        s.tokens.push(missing);
        s.head += 10;
    });
    run(&mut store, &api, &opts(10_000, 2, &[]));
    let t = store.token(PARAMS, 133_460).unwrap();
    assert_eq!(
        t.owner.unwrap().owner.address,
        "tz1gL9H53BzjDqmMi2JkKALeCzT7UAoopzEL"
    );
    assert_eq!(issue_on(&dir, "token_missing", 133460), 0);

    // Incremental pass: a new mint whose token row lags behind.
    let new_token = fake.with(|s| {
        s.head += 100;
        let level = s.head - 20;
        let mut t = s.tokens.iter().find(|t| t["tokenId"] == "188628").unwrap().clone();
        t["id"] = json!(9_000_000_000_000_001_u64);
        t["tokenId"] = json!("188700");
        t["firstLevel"] = json!(level);
        t["lastLevel"] = json!(level);
        t["metadata.iterationHash"] = json!("ooLateToken");
        t["metadata.artifactUri"] = json!("onchfs://a71e98fced01c865f33be138100b5ea5486f32fc333ad1503f814b201234130c/?fxhash=ooLateToken&fxiteration=11");
        let mut m = s.mints.iter().find(|m| m["parameter"]["value"]["token_id"] == "188628").unwrap().clone();
        m["id"] = json!(9_000_000_000_000_002_u64);
        m["level"] = json!(level);
        m["parameter"]["value"]["token_id"] = json!("188700");
        m["parameter"]["value"]["iteration"] = json!("11");
        s.mints.push(m);
        s.balances.push(json!({
            "_contract": PARAMS, "id": 9_000_000_000_000_003_u64, "token.tokenId": "188700",
            "account.address": QUADRADO, "balance": "1", "lastLevel": level
        }));
        t
    });
    run(&mut store, &api, &opts(10_000, 2, &[]));
    assert_eq!(
        issue_on(&dir, "token_missing", 188700),
        1,
        "the new mint's token is not indexed yet"
    );
    fake.with(|s| {
        s.tokens.push(new_token);
        s.head += 10;
    });
    run(&mut store, &api, &opts(10_000, 2, &[]));
    let late = store.token(PARAMS, 188_700).unwrap();
    assert_eq!(late.iteration, 11);
    assert_eq!(late.owner.unwrap().owner.address, QUADRADO);
    assert_eq!(issue_on(&dir, "token_missing", 188700), 0);
    assert_eq!(store.project(ARBOREAL).unwrap().editions, Some(11));
}

#[test]
fn verification_follows_catalogue_changes_in_public_reads() {
    let dir = test_dir("verify_public_state");
    let mut store = bootstrapped(&dir);
    let fake = FakeTzkt::recorded();
    let stop = Arc::new(AtomicBool::new(false));
    let api = tzkt(&fake, &stop);
    run(&mut store, &api, &opts(10_000, 2, &[]));
    let verified = store.index_status().unwrap().tokens_verified;
    assert_eq!(verified, 19);

    // A changed catalogue seed is re-compared inside the bootstrap transaction.
    let copy = frank_copy(&dir);
    edit_json(&copy.join("public/data/seeds/2/0000.json"), |v| {
        v["seeds"][0] = "ooChangedSeed".into();
    });
    let r = bootstrap(&mut store, &copy).unwrap();
    assert_eq!(r.recompared.get("mismatch"), Some(&1));
    assert_eq!(
        store.token(PARAMS, 133_459).unwrap().verification,
        Verification::Mismatch
    );
    let status = store.index_status().unwrap();
    assert_eq!((status.tokens_verified, status.tokens_mismatch), (18, 1));

    // A record awaiting comparison is reported unverified, never as a chain match.
    side(&dir)
        .execute(
            "UPDATE catalogue_tokens SET verify_state = 'unverified' WHERE token_id = 133460",
            [],
        )
        .unwrap();
    assert_eq!(
        store.token(PARAMS, 133_460).unwrap().verification,
        Verification::Unverified
    );
    assert_eq!(store.index_status().unwrap().tokens_verified, 17);
    let o = VerifyOptions {
        sample: 0,
        absent_limit: 0,
        seed: 1,
    };
    verify(&mut store, None, &o).unwrap();
    assert_eq!(
        store.token(PARAMS, 133_460).unwrap().verification,
        Verification::ChainMatch
    );
    // Restoring the catalogue restores the match.
    bootstrap(&mut store, &frank()).unwrap();
    assert_eq!(
        store.token(PARAMS, 133_459).unwrap().verification,
        Verification::ChainMatch
    );
    assert_eq!(store.index_status().unwrap().tokens_verified, 19);
}

#[test]
fn content_roots_are_known_for_visible_projects_only() {
    let dir = test_dir("content_roots");
    let mut store = bootstrapped(&dir);
    // Project roots are known from the catalogue alone.
    let thumb = store.project(ARBOREAL).unwrap().thumbnail_uri.unwrap();
    let thumb_cid = thumb.trim_start_matches("ipfs://");
    assert!(store.content_root_known("ipfs", thumb_cid).unwrap());

    let fake = FakeTzkt::recorded();
    let stop = Arc::new(AtomicBool::new(false));
    run(&mut store, &tzkt(&fake, &stop), &opts(10_000, 2, &[]));
    let t = store.token(PARAMS, 133_459).unwrap();
    let display = t.display_uri.unwrap();
    let v0 = display.trim_start_matches("ipfs://");
    assert!(v0.starts_with("Qm"));
    let parsed = cid::Cid::try_from(v0).unwrap();
    let v1 = cid::Cid::new_v1(parsed.codec(), *parsed.hash()).to_string();
    assert!(v1.starts_with("bafy"));
    assert!(store.content_root_known("ipfs", v0).unwrap(), "CIDv0 form");
    assert!(store.content_root_known("ipfs", &v1).unwrap(), "CIDv1 form");
    let onchfs = "a71e98fced01c865f33be138100b5ea5486f32fc333ad1503f814b201234130c";
    assert!(store.content_root_known("onchfs", onchfs).unwrap());
    assert!(
        store
            .content_root_known("onchfs", &onchfs.to_uppercase())
            .unwrap()
    );
    // Roots of a flagged project are not served.
    let flagged = side(&dir)
        .query_row(
            "SELECT artifact_uri FROM tokens WHERE token_id = 43308",
            [],
            |r| r.get::<_, String>(0),
        )
        .unwrap();
    let flagged_cid = flagged
        .trim_start_matches("ipfs://")
        .split(['/', '?'])
        .next()
        .unwrap();
    assert!(!store.content_root_known("ipfs", flagged_cid).unwrap());
    assert!(!store.content_root_known("onchfs", &"0".repeat(64)).unwrap());
    for (scheme, cid) in [("http", v0), ("ipfs", "not-a-cid"), ("onchfs", "abcd")] {
        assert!(
            matches!(
                store.content_root_known(scheme, cid),
                Err(StoreError::BadRequest(_))
            ),
            "{scheme} {cid}"
        );
    }
}

#[test]
fn upgrade_rebuilds_roots_verification_and_identities_without_network() {
    let dir = test_dir("upgrade_local");
    let mut store = bootstrapped(&dir);
    let fake = FakeTzkt::recorded();
    let stop = Arc::new(AtomicBool::new(false));
    run(&mut store, &tzkt(&fake, &stop), &opts(10_000, 2, &[]));
    let roots = one(&side(&dir), "SELECT count(*) FROM content_roots");
    side(&dir)
        .execute_batch(
            "DELETE FROM content_roots; DELETE FROM identities;
             UPDATE catalogue_tokens SET verify_state = 'unverified' WHERE token_id < 140000;",
        )
        .unwrap();
    let requests = fake.with(|s| s.requests.len());
    let r = seeded_archive::ingest::upgrade::upgrade(&mut store).unwrap();
    assert_eq!(fake.with(|s| s.requests.len()), requests, "no network");
    assert_eq!(r.schema_version, 10);
    assert_eq!(r.content_roots as i64, roots);
    assert!(r.recompared.get("match").copied().unwrap_or(0) >= 8);
    assert_eq!(r.identities.identities, 1);
    assert_eq!(store.index_status().unwrap().tokens_verified, 19);
    assert_eq!(store.artist("emptyname").unwrap().name, "Emptyname");
}

#[test]
fn repeated_escrow_limits_reach_every_marketplace() {
    let dir = test_dir("escrow_limits");
    let mut store = bootstrapped(&dir);
    let fake = FakeTzkt::recorded();
    let stop = Arc::new(AtomicBool::new(false));
    let api = tzkt(&fake, &stop);
    run(&mut store, &api, &opts(10_000, 2, &[]));
    let o = EscrowOptions {
        limit: Some(1),
        page: 10_000,
    };
    let mut runs = 0;
    while one(
        &side(&dir),
        "SELECT count(*) FROM escrow_state WHERE synced_level IS NOT NULL AND pass_level IS NULL",
    ) < 3
    {
        runs += 1;
        assert!(runs <= 10, "escrow passes did not converge");
        sync_escrows(&mut store, &api, &o, &|_| {}).unwrap();
    }
    assert_eq!(store.collector(KETITAKI).unwrap().listed, 1);
}

/// `holder_counts` must equal a full recount of visible credited tokens.
fn assert_counts_consistent(dir: &Path) {
    let c = side(dir);
    let drift: i64 = c
        .query_row(
            "WITH truth AS (SELECT t.holder_address AS h, count(*) AS held, \
             sum(t.listed_by IS NOT NULL) AS listed FROM tokens t JOIN projects p \
             ON p.id = t.project_id AND p.hidden = 0 WHERE t.holder_address IS NOT NULL \
             GROUP BY t.holder_address) \
             SELECT (SELECT count(*) FROM truth t LEFT JOIN holder_counts c \
               ON c.holder_address = t.h WHERE c.held IS NOT t.held OR c.listed IS NOT t.listed) \
             + (SELECT count(*) FROM holder_counts c WHERE c.held <> 0 \
               AND NOT EXISTS (SELECT 1 FROM truth t WHERE t.h = c.holder_address))",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(drift, 0, "holder_counts drifted from the tokens");
}

#[test]
fn collector_totals_are_maintained_through_sync_escrow_transfers_and_moderation() {
    let dir = test_dir("holder_counts");
    let mut store = bootstrapped(&dir);
    let fake = FakeTzkt::recorded();
    let stop = Arc::new(AtomicBool::new(false));
    let api = tzkt(&fake, &stop);
    run(&mut store, &api, &opts(10_000, 2, &[]));
    assert_counts_consistent(&dir);
    escrows(&mut store, &api, 10_000);
    assert_counts_consistent(&dir);
    transfer_133603_to_honest(&fake);
    run(&mut store, &api, &opts(10_000, 2, &[]));
    assert_counts_consistent(&dir);
    assert_eq!(store.collector(HONEST).unwrap().token_count, 3);
    assert_eq!(
        store.collector_holdings(HONEST, page(0, 1)).unwrap().total,
        3
    );
    // Flagging a project removes its tokens from every collector total, unflagging restores.
    side(&dir)
        .execute(
            "UPDATE projects SET hidden = 1 WHERE id = ?1",
            [ARBOREAL as i64],
        )
        .unwrap();
    assert_counts_consistent(&dir);
    assert_eq!(store.collector(HONEST).unwrap().token_count, 2);
    assert!(matches!(
        store.collector(QUADRADO),
        Err(StoreError::NotFound { .. })
    ));
    side(&dir)
        .execute(
            "UPDATE projects SET hidden = 0 WHERE id = ?1",
            [ARBOREAL as i64],
        )
        .unwrap();
    assert_counts_consistent(&dir);
    assert_eq!(store.collector(QUADRADO).unwrap().token_count, 3);
    let keyset = store.collector_holdings_page(QUADRADO, None, 2).unwrap();
    assert_eq!(keyset.total, 3);
}

/// Feature shares aggregated from every token, as before schema v5.
const RECOUNT_SHARES: &str = "WITH f AS (SELECT tf.name, tf.value, tf.position \
     FROM token_features tf JOIN tokens t ON t.contract = tf.contract \
     AND t.token_id = tf.token_id WHERE t.project_id = ?1 \
     UNION ALL SELECT j.value ->> 0, j.value ->> 1, CAST(j.key AS INTEGER) \
     FROM tokens t, json_each(t.attributes) j WHERE t.project_id = ?1), \
     n AS (SELECT name, min(position) AS pos FROM f GROUP BY name) \
     SELECT f.name, f.value, count(*) AS c FROM f JOIN n ON n.name = f.name \
     GROUP BY n.pos, f.name, f.value ORDER BY n.pos, f.name, c DESC, f.value";

/// `feature_counts` equals a full recount, and every visible project's shares equal the
/// aggregation over its tokens.
fn assert_features_consistent(store: &Store, dir: &Path) {
    let c = side(dir);
    let drift = one(
        &c,
        "WITH truth AS (SELECT project_id, name, value, position, count(*) AS n FROM ( \
           SELECT t.project_id, j.value ->> 0 AS name, j.value ->> 1 AS value, \
             CAST(j.key AS INTEGER) AS position FROM tokens t, json_each(t.attributes) j \
           UNION ALL SELECT t.project_id, tf.name, tf.value, tf.position \
           FROM token_features tf JOIN tokens t \
             ON t.contract = tf.contract AND t.token_id = tf.token_id) \
         GROUP BY project_id, name, value, position) \
         SELECT (SELECT count(*) FROM truth t LEFT JOIN feature_counts f \
           USING (project_id, name, value, position) WHERE f.count IS NOT t.n) \
         + (SELECT count(*) FROM feature_counts f WHERE f.count <> 0 AND NOT EXISTS \
           (SELECT 1 FROM truth t WHERE (t.project_id, t.name, t.value, t.position) \
             = (f.project_id, f.name, f.value, f.position)))",
    );
    assert_eq!(drift, 0, "feature_counts drifted from the tokens");
    let projects: Vec<i64> = c
        .prepare(
            "SELECT DISTINCT p.id FROM projects p JOIN tokens t ON t.project_id = p.id \
             WHERE p.hidden = 0",
        )
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert!(!projects.is_empty());
    for p in projects {
        let expected: Vec<(String, String, u32)> = c
            .prepare(RECOUNT_SHARES)
            .unwrap()
            .query_map([p], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        let tokens: u32 = c
            .query_row(
                "SELECT count(*) FROM tokens WHERE project_id = ?1",
                [p],
                |r| r.get(0),
            )
            .unwrap();
        let got = store.feature_shares(p as u64).unwrap();
        let rows: Vec<(String, String, u32)> = got
            .iter()
            .map(|s| (s.name.clone(), s.value.clone(), s.count))
            .collect();
        assert_eq!(rows, expected, "project {p}");
        assert!(got.iter().all(|s| s.total == tokens), "project {p}");
    }
}

#[test]
fn feature_counts_match_a_recount_through_sync_transfers_retries_and_resync() {
    let dir = test_dir("feature_counts");
    let mut store = bootstrapped(&dir);
    let fake = FakeTzkt::recorded();
    let missing = hide_token(&fake, "133460");
    let stop = Arc::new(AtomicBool::new(false));
    let api = tzkt(&fake, &stop);
    run(&mut store, &api, &opts(10_000, 2, &[]));
    assert_eq!(issue_on(&dir, "token_missing", 133460), 1);
    assert!(
        one(
            &side(&dir),
            "SELECT count(*) FROM feature_counts WHERE count > 0"
        ) > 0
    );
    assert_features_consistent(&store, &dir);

    // A retried token arrives.
    fake.with(|s| {
        s.tokens.push(missing);
        s.head += 10;
    });
    run(&mut store, &api, &opts(10_000, 2, &[]));
    assert_eq!(issue_on(&dir, "token_missing", 133460), 0);
    assert_features_consistent(&store, &dir);

    transfer_133603_to_honest(&fake);
    run(&mut store, &api, &opts(10_000, 2, &[]));
    assert_features_consistent(&store, &dir);

    // Changed metadata replaces a token's features.
    fake.with(|s| {
        s.head += 100;
        let level = s.head - 50;
        for t in s.tokens.iter_mut() {
            if t["tokenId"] == "133604" {
                t["lastLevel"] = json!(level);
                t["metadata.attributes"] = json!([
                    {"name": "Palette", "value": "Gold"},
                    {"name": "Rare", "value": "Yes"}
                ]);
            }
        }
    });
    run(&mut store, &api, &opts(10_000, 2, &[]));
    assert_features_consistent(&store, &dir);
    let shares = store.feature_shares(ARBOREAL).unwrap();
    assert!(
        shares
            .iter()
            .any(|s| (s.name.as_str(), s.value.as_str(), s.count) == ("Rare", "Yes", 1))
    );

    // Moderation hides the shares and restores them unchanged.
    let set_hidden = |hidden: i64| {
        side(&dir)
            .execute(
                "UPDATE projects SET hidden = ?2 WHERE id = ?1",
                [ARBOREAL as i64, hidden],
            )
            .unwrap()
    };
    set_hidden(1);
    assert!(matches!(
        store.feature_shares(ARBOREAL),
        Err(StoreError::NotFound { .. })
    ));
    set_hidden(0);
    assert_eq!(store.feature_shares(ARBOREAL).unwrap(), shares);

    // Re-import: the catalogue again, then a full pass from empty cursors.
    bootstrap(&mut store, &frank()).unwrap();
    side(&dir)
        .execute_batch("DELETE FROM sync_state; DELETE FROM sync_chunks;")
        .unwrap();
    run(&mut store, &api, &opts(10_000, 2, &[]));
    assert_features_consistent(&store, &dir);
    assert_eq!(store.feature_shares(ARBOREAL).unwrap(), shares);
}

#[test]
fn the_burn_address_holds_tokens_but_is_no_collector() {
    let dir = test_dir("burn_address");
    let mut store = bootstrapped(&dir);
    let fake = FakeTzkt::recorded();
    let burn = "tz1burnburnburnburnburnburnburjAYjjX";
    fake.with(|s| {
        for b in s.balances.iter_mut() {
            if b["token.tokenId"] == "133604" {
                b["account.address"] = json!(burn);
            }
        }
    });
    let stop = Arc::new(AtomicBool::new(false));
    run(&mut store, &tzkt(&fake, &stop), &opts(10_000, 2, &[]));
    let owner = store.token(PARAMS, 133_604).unwrap().owner.unwrap();
    assert_eq!(owner.owner.address, burn);
    assert!(owner.burned);
    assert_eq!((owner.listed_by, owner.escrow), (None, None));
    assert!(matches!(
        store.collector(burn),
        Err(StoreError::NotFound { .. })
    ));
    assert_eq!(
        store.collector_holdings(burn, page(0, 10)).unwrap().total,
        0
    );
    assert_eq!(store.collector(QUADRADO).unwrap().token_count, 3);
    let plain = store.token(PARAMS, 133_603).unwrap().owner.unwrap();
    assert!(!plain.burned);
    assert_counts_consistent(&dir);
}

#[test]
fn alias_fallback_ignores_aliases_shared_by_two_artists() {
    let dir = test_dir("alias_fallback");
    let mut store = bootstrapped(&dir);
    let fake = FakeTzkt::recorded();
    let stop = Arc::new(AtomicBool::new(false));
    run(&mut store, &tzkt(&fake, &stop), &opts(10_000, 2, &[]));
    let keeper = "tz1gL9H53BzjDqmMi2JkKALeCzT7UAoopzEL";
    // EmptyName owns Arboreal #2 and is the only artist with that alias.
    let held = store.artist_known_holdings(keeper, page(0, 10)).unwrap();
    assert_eq!(held.items.len(), 1);
    assert_eq!(held.items[0].token_id, 133_460);
    // Once another artist claims the same alias, the fallback matches nothing.
    side(&dir)
        .execute(
            "INSERT INTO artist_aliases (artist_id, position, alias, alias_key) \
             VALUES ('tz1exYBh5zHhf7LKyJSKAQqm89ZYZmTwsBz2', 0, 'EmptyName', 'emptyname')",
            [],
        )
        .unwrap();
    assert_eq!(
        store
            .artist_known_holdings(keeper, page(0, 10))
            .unwrap()
            .total,
        0
    );
}

#[test]
fn retry_pages_advance_past_entries_that_stay_unresolved() {
    let dir = test_dir("retry_cursor");
    let mut store = bootstrapped(&dir);
    let fake = FakeTzkt::recorded();
    let _never = hide_token(&fake, "133460");
    let later = hide_token(&fake, "133603");
    let stop = Arc::new(AtomicBool::new(false));
    let api = tzkt(&fake, &stop);
    run(&mut store, &api, &opts(10_000, 1, &[PARAMS]));
    // 133603 comes back without its lastLevel moving; 133460 never does.
    fake.with(|s| {
        s.tokens.push(later);
        s.head += 100;
    });
    let mut short = opts(1, 1, &[PARAMS]);
    short.limit = Some(1);
    for _ in 0..20 {
        run(&mut store, &api, &short);
    }
    let t = store.token(PARAMS, 133_603).unwrap();
    assert_eq!(t.owner.unwrap().owner.address, QUADRADO);
    assert_eq!(
        issue_on(&dir, "token_missing", 133_460),
        1,
        "still open, still retried"
    );
}

/// Clones fixture token 188628 as token 0 (with its mint) and returns the token row, unpublished.
fn token_zero(fake: &FakeTzkt) -> serde_json::Value {
    fake.with(|s| {
        let mut t = s
            .tokens
            .iter()
            .find(|t| t["tokenId"] == "188628")
            .unwrap()
            .clone();
        t["id"] = serde_json::json!(9_000_000_000_000_001_u64);
        t["tokenId"] = serde_json::json!("0");
        let mut m = s
            .mints
            .iter()
            .find(|m| m["parameter"]["value"]["token_id"] == "188628")
            .unwrap()
            .clone();
        m["id"] = serde_json::json!(9_000_000_000_000_002_u64);
        m["parameter"]["value"]["token_id"] = serde_json::json!("0");
        m["parameter"]["value"]["iteration"] = serde_json::json!("11");
        s.mints.push(m);
        t
    })
}

#[test]
fn retry_recovers_token_zero() {
    let dir = test_dir("retry_token_zero");
    let mut store = bootstrapped(&dir);
    let fake = FakeTzkt::recorded();
    let zero = token_zero(&fake);
    let stop = Arc::new(AtomicBool::new(false));
    let api = tzkt(&fake, &stop);
    run(&mut store, &api, &opts(10_000, 1, &[PARAMS]));
    assert_eq!(issue_on(&dir, "token_missing", 0), 1, "minted, row absent");
    fake.with(|s| {
        s.tokens.push(zero);
        s.head += 100;
    });
    run(&mut store, &api, &opts(10_000, 1, &[PARAMS]));
    assert!(store.token(PARAMS, 0).is_ok(), "token 0 recovered by retry");
    assert_eq!(issue_on(&dir, "token_missing", 0), 0);
}

#[test]
fn unresolved_token_zero_does_not_starve_later_retries() {
    let dir = test_dir("retry_token_zero_progress");
    let mut store = bootstrapped(&dir);
    let fake = FakeTzkt::recorded();
    let _never = token_zero(&fake);
    let later = hide_token(&fake, "133603");
    let stop = Arc::new(AtomicBool::new(false));
    let api = tzkt(&fake, &stop);
    run(&mut store, &api, &opts(10_000, 1, &[PARAMS]));
    fake.with(|s| {
        s.tokens.push(later);
        s.head += 100;
    });
    let mut short = opts(1, 1, &[PARAMS]);
    short.limit = Some(1);
    for _ in 0..20 {
        run(&mut store, &api, &short);
    }
    assert!(
        store.token(PARAMS, 133_603).is_ok(),
        "progress past token 0"
    );
    assert_eq!(issue_on(&dir, "token_missing", 0), 1, "token 0 still open");
}

#[test]
fn a_lagging_highest_token_is_retried_and_reconcile_repairs_completed_indexes() {
    let dir = test_dir("boundary_token");
    let mut store = bootstrapped(&dir);
    let fake = FakeTzkt::recorded();
    let late = hide_token(&fake, "188628");
    let stop = Arc::new(AtomicBool::new(false));
    let api = tzkt(&fake, &stop);
    run(&mut store, &api, &opts(10_000, 1, &[PARAMS]));
    assert_eq!(
        issue_on(&dir, "token_missing", 188_628),
        1,
        "outside the token bounds"
    );
    fake.with(|s| {
        s.tokens.push(late.clone());
        s.head += 100;
    });
    run(&mut store, &api, &opts(10_000, 1, &[PARAMS]));
    assert!(
        store.token(PARAMS, 188_628).is_ok(),
        "recovered at its original lastLevel"
    );
    assert_eq!(issue_on(&dir, "token_missing", 188_628), 0);

    // A v3 index that already lost the token (no row, no issue) is repaired by reconcile.
    let dir = test_dir("boundary_reconcile");
    let mut store = bootstrapped(&dir);
    let fake = FakeTzkt::recorded();
    let late = hide_token(&fake, "188628");
    let api = tzkt(&fake, &stop);
    run(&mut store, &api, &opts(10_000, 1, &[PARAMS]));
    side(&dir)
        .execute_batch(
            "DELETE FROM ingest_issues WHERE token_id IN (188628, 36500, 36501);
             DELETE FROM catalogue_tokens WHERE token_id = 188628;",
        )
        .unwrap();
    fake.with(|s| {
        s.tokens.push(late);
        s.head += 100;
    });
    run(&mut store, &api, &opts(10_000, 1, &[PARAMS]));
    assert!(
        store.token(PARAMS, 188_628).is_err(),
        "nothing remembers it"
    );
    let r = seeded_archive::ingest::upgrade::reconcile_store(&mut store, &api).unwrap();
    assert_eq!(r.boundary_candidates, 1);
    let again = seeded_archive::ingest::upgrade::reconcile_store(&mut store, &api).unwrap();
    assert_eq!(
        (again.catalogue_candidates, again.boundary_candidates),
        (0, 0),
        "idempotent"
    );
    run(&mut store, &api, &opts(10_000, 1, &[PARAMS]));
    assert!(store.token(PARAMS, 188_628).is_ok());
    assert_counts_consistent(&dir);
}

#[test]
fn catalogue_records_without_rows_become_retry_work() {
    let dir = test_dir("catalogue_reconcile");
    let mut store = bootstrapped(&dir);
    let fake = FakeTzkt::recorded();
    let stop = Arc::new(AtomicBool::new(false));
    let api = tzkt(&fake, &stop);
    run(&mut store, &api, &opts(10_000, 1, &[PARAMS]));
    // Simulate a token row lost without an issue.
    side(&dir)
        .execute("DELETE FROM tokens WHERE token_id = 133459", [])
        .unwrap();
    let r = seeded_archive::ingest::upgrade::upgrade(&mut store).unwrap();
    assert_eq!(r.reconcile.catalogue_candidates, 1);
    fake.with(|s| s.head += 10);
    run(&mut store, &api, &opts(10_000, 1, &[PARAMS]));
    assert!(store.token(PARAMS, 133_459).is_ok());
    assert_eq!(issue_on(&dir, "token_missing", 133_459), 0);
    assert_counts_consistent(&dir);
}
