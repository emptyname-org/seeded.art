//! fx(text) sync against recorded TzKT and IPFS fixtures: verification, gateway fallback,
//! transient vs absent, resume and interruption. No network.

#[path = "articles_support/mod.rs"]
mod support;

use std::sync::atomic::Ordering;

use cid::Cid;
use cid::multihash::Multihash;
use seeded_archive::articles::ipfs::{Outcome, Verification, ipfs_ref};
use seeded_archive::articles::store::{
    ABSENT_AFTER_RUNS, ABSENT_RECHECK_SECS, ArticleStore, ArticleStoreError, FetchState,
    ObjectKind, PendingObject,
};
use seeded_archive::articles::sync::SyncOptions;
use sha2::{Digest, Sha256};
use support::{GW1, GW2, Harness, Mode, art, plain_bytes, scratch};

const AUTHOR: &str = "tz1fepn7jZsCYBqCDhpM63hzh9g2Ytqk4Tpv";

/// fx(text) #0's body (7,918 bytes, one dag-pb block).
const BODY0: &str = "QmaWa8P7wj9DixvBgfjDoevsJvcZ5v3wX6stQahVvHxysP";
/// #835's body.
const BODY835: &str = "Qmd99qDnUmdMk1SQBhDN9daqVVF47Hcn71BGBzb2MEQtPC";
/// A GENTK v1 project directory.
const DIRECTORY: &str = "QmV879nbAyyayAomaEqig7U3TJKspk2nyMYzJTJP4ZvtDQ";

fn opts() -> SyncOptions {
    SyncOptions {
        concurrency: 2,
        ..SyncOptions::default()
    }
}

#[test]
fn full_sync_archives_chain_facts_metadata_and_verified_bodies() {
    let mut h = Harness::new();
    let r = h.sync_default();
    assert_eq!(r.chain.articles, 8);
    assert_eq!(r.chain.skipped, 0);
    assert_eq!(r.chain.listings, 3);
    assert_eq!(r.chain.moderation, 3);
    // 7 metadata pointers (#21 points at "ipfs://undefined"), 6 distinct bodies.
    assert_eq!(r.metadata.attempted, 7);
    assert_eq!(r.metadata.ok_verified, 7);
    assert_eq!(r.bodies.attempted, 6);
    assert_eq!(r.bodies.ok_verified, 6);
    assert_eq!(r.transient(), 0);
    assert_eq!(r.left, 0);
    // Nobody asked a gateway for the broken pointer.
    assert_eq!(h.net.count("undefined"), 0);

    let s = h.store.stats().unwrap();
    assert_eq!((s.articles, s.visible, s.hidden), (8, 6, 2));
    assert_eq!(
        (s.metadata_ipfs, s.metadata_tzkt, s.metadata_none),
        (7, 0, 1)
    );
    assert_eq!(s.articles_body_ok, 7);
    assert_eq!(s.articles_body_absent, 1);

    let a = h.store.article(0).unwrap();
    let plain = plain_bytes(BODY0).unwrap();
    assert_eq!(a.body.state, FetchState::Ok);
    assert_eq!(a.body.verification.as_deref(), Some("ipfs-car"));
    assert_eq!(
        a.body.text.as_deref().map(str::as_bytes),
        Some(plain.as_slice())
    );
    assert_eq!(
        a.body.sha256.as_deref(),
        Some(hex::encode(Sha256::digest(&plain)).as_str())
    );
    assert_eq!(a.body.bytes, Some(7918));
    assert_eq!(a.body.format, "markdown");
    assert_eq!(a.metadata_source.as_deref(), Some("ipfs"));
    assert_eq!(a.summary.title.as_deref(), Some("fx(text)"));
    // Names come only from a catalogue (tests/articles_names.rs).
    assert_eq!(a.summary.author_name, None);
    assert_eq!(a.summary.editions, 2022);
    assert!(a.summary.tags.contains(&"fx-markdown".to_owned()));
}

#[test]
fn corrupt_car_falls_back_to_the_next_gateway() {
    let mut h = Harness::new();
    h.net.set(GW1, Mode::Corrupt);
    let r = h.sync_default();
    assert_eq!(r.bodies.ok_verified, 6);
    assert_eq!(r.transient(), 0);
    let f = h.fetcher();
    match f.fetch(&format!("ipfs://{BODY0}")) {
        Outcome::Ok {
            verification,
            gateway,
            ..
        } => {
            assert_eq!(verification, Verification::Car);
            assert_eq!(gateway, GW2);
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn corrupt_everywhere_is_transient_and_recovers_on_the_next_run() {
    let mut h = Harness::new();
    h.net.set_cid(BODY835, Mode::Corrupt);
    let r = h.sync_default();
    assert_eq!(r.bodies.transient, 1);
    assert_eq!(r.bodies.ok_verified, 5);
    let a = h.store.article(835).unwrap();
    assert_eq!(a.body.state, FetchState::Transient);
    assert!(a.body.text.is_none());
    assert!(a.body.error.as_deref().unwrap().contains("CAR rejected"));

    h.net.clear_cids();
    let before = h.net.count("/ipfs/");
    let r = h.sync_default();
    // Only the failed object is fetched again: one CAR request.
    assert_eq!(h.net.count("/ipfs/") - before, 1);
    assert_eq!(r.bodies.attempted, 1);
    assert_eq!(r.bodies.ok_verified, 1);
    assert_eq!(h.store.article(835).unwrap().body.state, FetchState::Ok);
    assert_eq!(
        h.store.article(835).unwrap().body.text.as_deref(),
        Some("In this work, we see layers of colored walls\n")
    );
}

#[test]
fn down_or_timing_out_first_gateway_falls_back() {
    for mode in [Mode::Down, Mode::Timeout] {
        let mut h = Harness::new();
        h.net.set(GW1, mode);
        let r = h.sync_default();
        assert_eq!(r.metadata.ok_verified, 7, "{mode:?}");
        assert_eq!(r.bodies.ok_verified, 6, "{mode:?}");
    }
}

#[test]
fn every_gateway_down_is_transient_not_absent() {
    let mut h = Harness::new();
    h.net.set(GW1, Mode::Down);
    h.net.set(GW2, Mode::Timeout);
    let r = h.sync_default();
    assert_eq!(r.metadata.transient, 7);
    assert_eq!(r.metadata.absent, 0);
    // TzKT's metadata copy fills in, so bodies are still known (and fail transiently too).
    assert_eq!(r.bodies.transient, 6);
    let a = h.store.article(1).unwrap();
    assert_eq!(a.metadata_source.as_deref(), Some("tzkt"));
    assert_eq!(a.summary.title.as_deref(), Some("Beauty in Code"));
    assert_eq!(a.body.state, FetchState::Transient);
}

#[test]
fn not_found_everywhere_becomes_absent_after_three_runs_and_can_be_retried() {
    let mut h = Harness::new();
    h.net.set_cid(BODY835, Mode::NotFound);
    for run in 1..=3 {
        let r = h.sync_default();
        assert_eq!(r.bodies.attempted, if run == 1 { 6 } else { 1 });
        let state = h.store.article(835).unwrap().body.state;
        if run < 3 {
            assert_eq!(state, FetchState::Transient, "run {run}");
            assert_eq!(r.bodies.not_found, 1);
        } else {
            assert_eq!(state, FetchState::Absent, "run {run}");
            assert_eq!(r.bodies.absent, 1);
        }
    }
    // Absent is not fetched again by a plain sync...
    let before = h.net.count(BODY835);
    let r = h.sync_default();
    assert_eq!(r.bodies.attempted, 0);
    assert_eq!(h.net.count(BODY835), before);
    // ...but is with recheck, and recovers when a gateway has it.
    h.net.clear_cids();
    let r = h
        .sync(&SyncOptions {
            recheck: true,
            ..opts()
        })
        .unwrap();
    assert_eq!(r.bodies.ok_verified, 1);
    assert_eq!(h.store.article(835).unwrap().body.state, FetchState::Ok);
}

fn body_object(store: &ArticleStore) -> PendingObject {
    let mut pending = store.pending_objects(ObjectKind::Body, false).unwrap();
    assert_eq!(pending.len(), 1);
    pending.remove(0)
}

#[test]
fn unverified_bytes_that_are_not_text_become_absent_after_three_runs() {
    let mut store = ArticleStore::open_in_memory().unwrap();
    store
        .apply_snapshot(&support::snapshot(&[art(1, AUTHOR, "x")], &[], &[]))
        .unwrap();
    let body = body_object(&store);
    let binary = Outcome::Ok {
        data: vec![0xff, 0xfe, 0x00, 0x89],
        verification: Verification::Unverified,
        gateway: GW1.into(),
    };
    for run in 1..=ABSENT_AFTER_RUNS {
        let state = store
            .record_objects(&[(body.clone(), binary.clone())])
            .unwrap();
        let want = if run < ABSENT_AFTER_RUNS {
            FetchState::Transient
        } else {
            FetchState::Absent
        };
        assert_eq!(state, [Some(want)], "run {run}");
    }
    assert!(
        store
            .pending_objects(ObjectKind::Body, false)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn objects_absent_as_not_found_are_fetched_again_after_a_week() {
    let path = scratch("absent-recheck");
    let mut store = ArticleStore::open(&path).unwrap();
    let arts = [art(1, AUTHOR, "missing"), art(2, AUTHOR, "a directory")];
    store
        .apply_snapshot(&support::snapshot(&arts, &[], &[]))
        .unwrap();
    let mut pending = store.pending_objects(ObjectKind::Body, false).unwrap();
    let (missing, directory) = (pending.remove(1), pending.remove(0));
    for _ in 0..ABSENT_AFTER_RUNS {
        store
            .record_objects(&[(missing.clone(), Outcome::NotFound("404".into()))])
            .unwrap();
    }
    store
        .record_objects(&[(directory, Outcome::Absent("a directory, not a file".into()))])
        .unwrap();
    assert!(
        store
            .pending_objects(ObjectKind::Body, false)
            .unwrap()
            .is_empty()
    );
    let age = || {
        rusqlite::Connection::open(&path)
            .unwrap()
            .execute(
                "UPDATE objects SET updated_at = updated_at - ?1",
                [ABSENT_RECHECK_SECS],
            )
            .unwrap();
    };
    // A week later only the not-found object is due; still missing, it waits another week.
    age();
    assert_eq!(body_object(&store).key, missing.key);
    let state = store
        .record_objects(&[(missing.clone(), Outcome::NotFound("404".into()))])
        .unwrap();
    assert_eq!(state, [Some(FetchState::Absent)]);
    assert!(
        store
            .pending_objects(ObjectKind::Body, false)
            .unwrap()
            .is_empty()
    );
    // Pinned again: the next due run stores it.
    age();
    let ok = Outcome::Ok {
        data: b"missing".to_vec(),
        verification: Verification::Sha256,
        gateway: GW1.into(),
    };
    store.record_objects(&[(missing, ok)]).unwrap();
    assert_eq!(store.article(1).unwrap().body.state, FetchState::Ok);
    drop(store);
    support::remove_db(&path);
}

#[test]
fn a_broken_metadata_pointer_is_absent_without_any_request() {
    let mut h = Harness::new();
    h.sync_default();
    let a = h.store.article(21).unwrap();
    assert_eq!(a.metadata_uri.as_deref(), Some("ipfs://undefined"));
    assert_eq!(a.metadata_source, None);
    assert_eq!(a.summary.title, None);
    assert_eq!(a.body.state, FetchState::Absent);
    assert_eq!(a.summary.body_state, FetchState::Absent);
    assert_eq!(a.body.error.as_deref(), Some("no readable metadata"));
    assert_eq!(h.net.count("undefined"), 0);
    assert!(matches!(
        h.fetcher().fetch("ipfs://undefined"),
        Outcome::Absent(e) if e.starts_with("invalid CID")
    ));
    for bad in ["https://example.com/a.md", "ar://abc", "ipfs://Qm/../x", ""] {
        assert!(
            matches!(h.fetcher().fetch(bad), Outcome::Absent(_)),
            "{bad:?}"
        );
    }
}

#[test]
fn a_directory_is_absent_not_retried() {
    let h = Harness::new();
    assert_eq!(
        h.fetcher().fetch(&format!("ipfs://{DIRECTORY}")),
        Outcome::Absent("a directory, not a file".into())
    );
    // The first gateway's verified answer is final.
    assert_eq!(h.net.count(GW2), 0);
}

#[test]
fn plain_dag_pb_bytes_are_unverified_and_only_used_without_a_verified_copy() {
    let h = Harness::new();
    h.net.set(GW1, Mode::Plain);
    h.net.set(GW2, Mode::Plain);
    match h.fetcher().fetch(&format!("ipfs://{BODY0}")) {
        Outcome::Ok {
            verification,
            gateway,
            data,
        } => {
            assert_eq!(verification, Verification::Unverified);
            assert_eq!(gateway, GW1);
            assert_eq!(data, plain_bytes(BODY0).unwrap());
        }
        other => panic!("{other:?}"),
    }
    // Both gateways were asked for a verifiable copy first.
    assert_eq!(h.net.count(GW2), 1);

    let h = Harness::new();
    h.net.set(GW1, Mode::Plain);
    match h.fetcher().fetch(&format!("ipfs://{BODY0}")) {
        Outcome::Ok {
            verification,
            gateway,
            ..
        } => {
            assert_eq!(verification, Verification::Car);
            assert_eq!(gateway, GW2);
        }
        other => panic!("{other:?}"),
    }
}

fn raw_cid(bytes: &[u8]) -> String {
    let mh = Multihash::<64>::wrap(0x12, &Sha256::digest(bytes)).unwrap();
    Cid::new_v1(0x55, mh).to_string()
}

#[test]
fn raw_cid_plain_bytes_are_checked_with_sha256() {
    let body = b"# A raw block\n".to_vec();
    let cid = raw_cid(&body);
    let h = Harness::new();
    h.net.set(GW1, Mode::Plain);
    h.net.set(GW2, Mode::Plain);
    h.net
        .extra_plain
        .lock()
        .unwrap()
        .insert(cid.clone(), body.clone());
    assert_eq!(
        h.fetcher().fetch(&format!("ipfs://{cid}")),
        Outcome::Ok {
            data: body,
            verification: Verification::Sha256,
            gateway: GW1.into()
        }
    );
    // Bytes that do not hash to the CID are rejected on every gateway: transient.
    h.net
        .extra_plain
        .lock()
        .unwrap()
        .insert(cid.clone(), b"tampered".to_vec());
    match h.fetcher().fetch(&format!("ipfs://{cid}")) {
        Outcome::Transient(e) => assert!(e.contains("do not match the CID"), "{e}"),
        other => panic!("{other:?}"),
    }
}

#[test]
fn identity_cids_need_no_gateway() {
    let mh = Multihash::<64>::wrap(0x00, b"hello").unwrap();
    let cid = Cid::new_v1(0x55, mh).to_string();
    let h = Harness::new();
    assert_eq!(
        h.fetcher().fetch(&format!("ipfs://{cid}")),
        Outcome::Ok {
            data: b"hello".to_vec(),
            verification: Verification::Identity,
            gateway: String::new()
        }
    );
    assert_eq!(h.net.ipfs_requests.load(Ordering::SeqCst), 0);
}

#[test]
fn objects_over_the_cap_are_absent() {
    let mut h = Harness::new();
    h.cap = 6000;
    let r = h.sync_default();
    // #0's body is 7,918 bytes; every other object fits.
    assert_eq!(r.bodies.absent, 1);
    assert_eq!(r.bodies.ok_verified, 5);
    let a = h.store.article(0).unwrap();
    assert_eq!(a.body.state, FetchState::Absent);
    assert!(a.body.error.unwrap().contains("larger than 6000 bytes"));
}

#[test]
fn a_limited_run_resumes_without_refetching() {
    let mut h = Harness::new();
    let r = h
        .sync(&SyncOptions {
            limit: Some(3),
            ..opts()
        })
        .unwrap();
    assert_eq!(r.metadata.attempted, 3);
    assert_eq!(r.bodies.attempted, 0);
    // 4 metadata objects and 6 bodies (known from TzKT's metadata copy) remain.
    assert_eq!(r.left, 4 + 6);
    let r = h
        .sync(&SyncOptions {
            limit: Some(6),
            ..opts()
        })
        .unwrap();
    assert_eq!((r.metadata.attempted, r.bodies.attempted), (4, 2));
    let r = h.sync_default();
    assert_eq!((r.metadata.attempted, r.bodies.attempted), (0, 4));
    assert_eq!(r.left, 0);
    // Every object was requested exactly once over the three runs.
    let requests = h.net.requests.lock().unwrap().clone();
    let car: Vec<&String> = requests
        .iter()
        .filter(|u| u.contains("format=car"))
        .collect();
    assert_eq!(car.len(), 13);
    let mut unique = car.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(unique.len(), 13);
    let s = h.store.stats().unwrap();
    assert_eq!(s.articles_body_ok, 7);
}

#[test]
fn an_interrupted_run_keeps_finished_fetches_and_the_next_completes() {
    let mut h = Harness::new();
    *h.net.stop_after.lock().unwrap() = Some((4, h.stop.clone()));
    let r = h
        .sync(&SyncOptions {
            concurrency: 1,
            ..SyncOptions::default()
        })
        .unwrap();
    assert!(r.interrupted);
    let stored = r.metadata.attempted + r.bodies.attempted;
    assert!((3..=4).contains(&stored), "{stored}");
    assert_eq!(r.left, 13 - stored);
    *h.net.stop_after.lock().unwrap() = None;
    h.stop.store(false, Ordering::SeqCst);
    let r = h.sync_default();
    assert!(!r.interrupted);
    assert_eq!(r.metadata.attempted + r.bodies.attempted, 13 - stored);
    assert_eq!(h.store.stats().unwrap().articles_body_ok, 7);
}

#[test]
fn a_stopped_chain_read_reports_interrupted() {
    let mut h = Harness::new();
    h.stop.store(true, Ordering::SeqCst);
    let e = h.sync(&opts()).unwrap_err();
    assert!(e.is_interrupted(), "{e}");
    assert!(matches!(
        h.store.article(0),
        Err(ArticleStoreError::NotFound { .. })
    ));
}

#[test]
fn ipfs_refs_normalize_and_reject_unusable_uris() {
    let r = ipfs_ref(&format!("ipfs://{BODY0}")).unwrap();
    assert_eq!(r.key, format!("ipfs://{BODY0}"));
    let r = ipfs_ref(&format!("ipfs://ipfs/{BODY0}/a b/c.md")).unwrap();
    assert_eq!(r.key, format!("ipfs://{BODY0}/a%20b/c.md"));
    for bad in [
        "ipfs://undefined",
        "ipfs://",
        "https://ipfs.io/ipfs/Qm",
        "ipfs://QmaWa8P7wj9DixvBgfjDoevsJvcZ5v3wX6stQahVvHxysP?x=1",
        "ipfs://QmaWa8P7wj9DixvBgfjDoevsJvcZ5v3wX6stQahVvHxysP/../x",
    ] {
        assert!(ipfs_ref(bad).is_err(), "{bad:?}");
    }
}

#[test]
fn recheck_upgrades_unverified_copies_to_verified_ones() {
    let mut h = Harness::new();
    h.net.set_cid(BODY0, Mode::Plain);
    let r = h.sync_default();
    assert_eq!((r.bodies.ok_verified, r.bodies.ok_unverified), (5, 1));
    let a = h.store.article(0).unwrap();
    assert_eq!(a.body.verification.as_deref(), Some("unverified"));
    h.net.clear_cids();
    // A plain sync keeps the unverified copy.
    let r = h.sync_default();
    assert_eq!(r.bodies.attempted, 0);
    let r = h
        .sync(&SyncOptions {
            recheck: true,
            ..opts()
        })
        .unwrap();
    assert_eq!((r.bodies.attempted, r.bodies.ok_verified), (1, 1));
    let a = h.store.article(0).unwrap();
    assert_eq!(a.body.verification.as_deref(), Some("ipfs-car"));
    assert_eq!(a.body.bytes, Some(7918));
}

#[test]
fn a_shrunken_token_list_is_not_applied() {
    let mut h = Harness::new();
    h.sync_default();
    let holders = h.store.stats().unwrap().holder_rows;
    *h.net.token_limit.lock().unwrap() = Some(5);
    let e = h.sync(&opts()).unwrap_err();
    assert!(e.is_transient(), "{e}");
    assert!(e.to_string().contains("fewer than the 8 stored"), "{e}");
    let s = h.store.stats().unwrap();
    assert_eq!((s.articles, s.holder_rows), (8, holders));
}
