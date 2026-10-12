//! `seeded-index profiles`: fxhash registry plus IPFS documents, objkt holders, Tezos
//! Profiles skipped; resumable passes, transient versus permanent outcomes, provenance.

mod ingest_support;

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use ingest_support::*;
use rusqlite::Connection;
use seeded_archive::ingest::http::{ApiError, Client, Fetch, HttpResponse, TransportError};
use seeded_archive::ingest::profiles::{
    OBJKT_BATCH, ProfileClients, ProfileSource, ProfilesOptions, ProfilesReport, SourceReport,
    USER_REGISTRY, addresses_that_matter, sync_profiles,
};
use seeded_archive::ingest::tzkt::Tzkt;
use seeded_archive::ingest::{FINALITY, IngestError};
use seeded_archive::model::{EditProof, ProfileEdit};
use seeded_archive::store::Store;
use serde_json::{Value, json};

/// Frank fixture artists (so they matter), and one registry user who does not.
const KETI: &str = "tz1WMa1cAKk4JjFGZCpXNkTDi5o5AqCsFF6E";
const RYAN: &str = "tz1WKNUBMpEDT9yXt3BMAFJXdx1YMCakUKh7";
const LIET: &str = "tz1igEhzCfn34qnwJmKBQb24pnDAAYTYux3m";
const OUTSIDER: &str = "tz1Ke2h7sDdakHJQh8WX4Z372du1KChsksyU";
/// Not in the registry fixture.
const NEWCOMER: &str = "tz1itwvWyP4qd6PY7qrYkn3LBvDC13vjQS9i";
/// CIDv0 in the registry, canonical CIDv1 on the gateway.
const DOC_KETI: (&str, &str) = (
    "QmURUAU4YPa6Wwco3JSVrcN7WfCrFBZH7hY51BLrc87WjM",
    "bafybeic2mina2lzepy3enwqclctu2fsjn2uarxc4gqcluvjysh24ez27vq",
);
const DOC_RYAN: (&str, &str) = (
    "QmSDby6PefU13Q8TXc5fmhe7pd2wXCrhYGpS4LA9L5tZry",
    "bafybeibzuckqjn6pxf5jx2qb2ub5av72vjak5fczt2jaejexky7rvjxsni",
);
const DOC_LIET: (&str, &str) = (
    "QmXNMVeGcYDWjvdsubieoSm8dcer4Pdi6CFWemmDprwpaS",
    "bafybeiegfdqqpphmodywvoyq4nxr5huac2xjy33pmnhlor3mmhis6rljb4",
);
const DOC_OUTSIDER: (&str, &str) = (
    "QmR3uJhQqAHa2hjL975i9RsfArgGcq3eJNSwY3cufCck3y",
    "bafybeibijcyphyl3tlskcgyenozapiuztk67cwnocs7c52ozyjg334e3ea",
);
const DOC_NEW: (&str, &str) = (
    "QmeGpDEfwbjpdrL85GA6SwkYSE6w2BhALe5ixA8pj3UggU",
    "bafybeihmx5uwxv2ldyyuy3z4tncdozk3luvpqcws4xm3tunth7yrvfduiu",
);
const AVATAR: &str = "ipfs://QmWtXz2doVUXEwX7REgmhTYYPwWRUCNwAGX3wwJRKnGnGQ";

struct Rig {
    tzkt_fake: Arc<FakeTzkt>,
    gateways: Vec<Arc<FakeGateway>>,
    objkt_fake: Arc<FakeObjkt>,
    stop: Arc<AtomicBool>,
}

impl Rig {
    fn new() -> Rig {
        let tzkt_fake = FakeTzkt::recorded();
        let rig = Rig {
            tzkt_fake,
            gateways: vec![
                Arc::new(FakeGateway::default()),
                Arc::new(FakeGateway::default()),
            ],
            objkt_fake: Arc::new(FakeObjkt::default()),
            stop: Arc::new(AtomicBool::new(false)),
        };
        let level = rig.tzkt_fake.with(|s| s.head);
        for (i, (address, name, doc)) in [
            (KETI, "KetiTaki", DOC_KETI.0),
            (RYAN, "RJArt", DOC_RYAN.0),
            (LIET, "liet", DOC_LIET.0),
            (OUTSIDER, "outsider", DOC_OUTSIDER.0),
        ]
        .into_iter()
        .enumerate()
        {
            rig.registry_key("users_name", 10 + i as u64, address, Some(name), level - 10);
            rig.registry_key(
                "users_metadata",
                20 + i as u64,
                address,
                Some(&format!("ipfs://{doc}")),
                level - 10,
            );
        }
        rig.both(DOC_KETI.1, 200, &doc("registry bio of keti", AVATAR));
        rig.both(DOC_RYAN.1, 200, &doc("registry bio of ryan", AVATAR));
        rig.both(DOC_LIET.1, 200, &doc("", ""));
        rig.both(DOC_OUTSIDER.1, 200, &doc("never fetched", ""));
        rig
    }

    /// Adds or replaces a registry big-map key (`text` None: removed).
    fn registry_key(&self, path: &str, id: u64, address: &str, text: Option<&str>, level: u64) {
        self.tzkt_fake.with(|s| {
            s.listings
                .retain(|r| !(r["_path"] == path && r["key"] == address));
            s.listings.push(json!({
                "_escrow": USER_REGISTRY,
                "_path": path,
                "id": id,
                "key": address,
                "value": text.map(hex_text),
                "active": text.is_some(),
                "lastLevel": level,
            }));
            s.listings.sort_by_key(|r| r["id"].as_u64());
        });
    }

    fn both(&self, cid: &str, status: u16, body: &str) {
        for g in &self.gateways {
            g.serve(cid, status, body);
        }
    }

    fn run(&self, store: &mut Store, sources: &str, limit: Option<usize>) -> ProfilesReport {
        self.try_run(store, sources, limit).unwrap()
    }

    fn try_run(
        &self,
        store: &mut Store,
        sources: &str,
        limit: Option<usize>,
    ) -> Result<ProfilesReport, IngestError> {
        let tzkt = tzkt(&self.tzkt_fake, &self.stop);
        self.try_run_with(store, &tzkt, sources, limit)
    }

    /// Runs with `tzkt` in place of the plain fake.
    fn try_run_with(
        &self,
        store: &mut Store,
        tzkt: &Tzkt,
        sources: &str,
        limit: Option<usize>,
    ) -> Result<ProfilesReport, IngestError> {
        let ipfs: Vec<Client> = self
            .gateways
            .iter()
            .map(|g| quick_client(g.clone(), "http://gw.test/ipfs/", &self.stop))
            .collect();
        let objkt = quick_client(
            self.objkt_fake.clone(),
            "http://objkt.test/v3/graphql",
            &self.stop,
        );
        let clients = ProfileClients {
            tzkt,
            ipfs: &ipfs,
            objkt: &objkt,
        };
        let opts = ProfilesOptions {
            sources: ProfileSource::parse(sources).unwrap(),
            limit,
            concurrency: 2,
            batch: 2,
        };
        sync_profiles(store, &clients, &opts, &|_| {})
    }

    fn doc_requests(&self, cid: &str) -> usize {
        self.gateways.iter().map(|g| g.requests_for(cid)).sum()
    }
}

fn doc(description: &str, identicon: &str) -> String {
    json!({ "description": description, "identicon": identicon }).to_string()
}

fn report(r: &ProfilesReport, source: ProfileSource) -> &SourceReport {
    &r.sources.iter().find(|(s, _)| *s == source).unwrap().1
}

fn imported(db: &Path, address: &str, source: &str) -> Option<(Option<String>, Option<String>)> {
    Connection::open(db)
        .unwrap()
        .query_row(
            "SELECT name, bio FROM profiles_imported WHERE address = ?1 AND source = ?2",
            [address, source],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .ok()
}

fn fetch_state(db: &Path, address: &str) -> (String, i64) {
    Connection::open(db)
        .unwrap()
        .query_row(
            "SELECT state, attempts FROM profile_fetch WHERE source = 'fxhash' AND address = ?1",
            [address],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap()
}

fn setup(name: &str) -> (Store, std::path::PathBuf) {
    let dir = test_dir(name);
    let store = bootstrapped(&dir);
    let matter = addresses_that_matter_of(&store, &dir);
    for a in [KETI, RYAN, LIET] {
        assert!(matter.contains(&a.to_owned()), "{a} must matter");
    }
    assert!(!matter.contains(&OUTSIDER.to_owned()));
    (store, dir.join("db.sqlite"))
}

fn addresses_that_matter_of(_store: &Store, dir: &Path) -> Vec<String> {
    addresses_that_matter(&Connection::open(dir.join("db.sqlite")).unwrap()).unwrap()
}

#[test]
fn fxhash_registry_names_and_documents_import_with_provenance() {
    let (mut store, db) = setup("profiles_fxhash");
    let rig = Rig::new();
    let r = rig.run(&mut store, "fxhash", None);
    let fx = report(&r, ProfileSource::Fxhash);
    assert_eq!(fx.registry_keys, 8);
    assert_eq!((fx.fetched, fx.invalid, fx.unavailable), (3, 0, 0));
    assert!(fx.pass_complete && !r.limited && !r.transient);
    assert_eq!(fx.pending, 0);
    // Names are read for everyone; documents only for addresses that matter.
    assert_eq!(
        imported(&db, OUTSIDER, "fxhash"),
        Some((Some("outsider".into()), None))
    );
    assert_eq!(rig.doc_requests(DOC_OUTSIDER.1), 0);
    assert_eq!(
        imported(&db, RYAN, "fxhash"),
        Some((Some("RJArt".into()), Some("registry bio of ryan".into())))
    );
    // Field by field: Frank's API snapshot (artists) first, the registry document next.
    let keti = store.profile(KETI).unwrap();
    assert_eq!(keti.name.as_deref(), Some("KetiTaki"));
    assert_eq!(keti.name_source.as_deref(), Some("fxhash"));
    assert_eq!(keti.bio.as_deref(), Some("Experimental Generative Coding."));
    // RYAN's artist record carries the name the artist asked for and an avatar, no bio.
    let ryan = store.profile(RYAN).unwrap();
    assert_eq!(ryan.name.as_deref(), Some("Plasma Renamed"));
    assert_eq!(
        ryan.bio.as_deref(),
        Some("registry bio of ryan"),
        "artist bio is blank"
    );
    assert!(
        ryan.avatar_uri.unwrap().starts_with("ipfs://QmTLeG"),
        "artist avatar wins"
    );
    let liet = store.profile(LIET).unwrap();
    assert_eq!((liet.bio, liet.avatar_uri), (None, None));
    // A complete pass leaves nothing to fetch: the next run asks the gateways nothing.
    let before = rig.doc_requests(DOC_KETI.1);
    let again = rig.run(&mut store, "fxhash", None);
    assert_eq!(report(&again, ProfileSource::Fxhash).fetched, 0);
    assert_eq!(rig.doc_requests(DOC_KETI.1), before);
}

#[test]
fn registry_changes_are_read_incrementally() {
    let (mut store, db) = setup("profiles_registry_incremental");
    let rig = Rig::new();
    rig.run(&mut store, "fxhash", None);
    let level = rig.tzkt_fake.with(|s| s.head);
    rig.tzkt_fake.with(|s| s.head = level + 5);
    // RYAN points at a new document; LIET removes the name; KETI unchanged.
    rig.registry_key(
        "users_metadata",
        21,
        RYAN,
        Some(&format!("ipfs://{}", DOC_NEW.0)),
        level + 2,
    );
    rig.both(DOC_NEW.1, 200, &doc("new bio", ""));
    rig.registry_key("users_name", 12, LIET, None, level + 3);
    let r = rig.run(&mut store, "fxhash", None);
    let fx = report(&r, ProfileSource::Fxhash);
    assert_eq!(fx.registry_keys, 2, "only changed keys are read");
    assert!(
        rig.tzkt_fake
            .requests_containing(&format!("lastLevel.gt={}", level - FINALITY))
            >= 2
    );
    assert_eq!(fx.fetched, 1);
    assert_eq!(
        imported(&db, RYAN, "fxhash"),
        Some((Some("RJArt".into()), Some("new bio".into())))
    );
    assert_eq!(imported(&db, LIET, "fxhash"), None, "nothing left for liet");
    assert_eq!(
        rig.doc_requests(DOC_KETI.1),
        1,
        "unchanged documents are not refetched"
    );
}

fn imported_name(db: &Path, address: &str) -> Option<String> {
    imported(db, address, "fxhash").and_then(|(name, _)| name)
}

#[test]
fn a_registry_change_is_read_once_final_and_a_replaced_block_counts() {
    let (mut store, db) = setup("profiles_registry_reorg_tail");
    let rig = Rig::new();
    rig.run(&mut store, "fxhash", None);
    let head = rig.tzkt_fake.with(|s| {
        s.head += 100;
        s.head
    });
    rig.registry_key("users_name", 13, OUTSIDER, Some("not final"), head);
    rig.run(&mut store, "fxhash", None);
    assert_eq!(imported_name(&db, OUTSIDER).as_deref(), Some("outsider"));
    // The head block is replaced by one setting another name at the same level.
    rig.registry_key("users_name", 13, OUTSIDER, Some("outsider two"), head);
    rig.tzkt_fake.with(|s| s.head = head + FINALITY);
    rig.run(&mut store, "fxhash", None);
    assert_eq!(
        imported_name(&db, OUTSIDER).as_deref(),
        Some("outsider two")
    );
}

#[test]
fn a_rolled_back_registry_change_never_replaces_the_final_name_across_a_restart() {
    let (mut store, db) = setup("profiles_registry_rollback");
    let rig = Rig::new();
    rig.run(&mut store, "fxhash", None);
    assert_eq!(imported_name(&db, OUTSIDER).as_deref(), Some("outsider"));
    // A final rename, then another rename and a new key in the head block.
    let head = rig.tzkt_fake.with(|s| {
        s.head += 100;
        s.head
    });
    rig.registry_key("users_name", 13, OUTSIDER, Some("final rename"), head - 10);
    let before = rig.tzkt_fake.with(|s| s.listings.clone());
    rig.registry_key("users_name", 13, OUTSIDER, Some("orphaned name"), head);
    rig.registry_key("users_name", 30, NEWCOMER, Some("orphaned key"), head);
    rig.run(&mut store, "fxhash", None);
    assert_eq!(imported_name(&db, OUTSIDER).as_deref(), Some("outsider"));
    assert_eq!(imported_name(&db, NEWCOMER), None);
    drop(store);
    let mut store = Store::open(&db).unwrap();
    // Rolled back without replacement: the key returns with its old level (the final rename,
    // below the final level of the last run), the new key is gone.
    rig.tzkt_fake.with(|s| s.listings = before);
    for _ in 0..=FINALITY {
        rig.tzkt_fake.with(|s| s.head += 1);
        rig.run(&mut store, "fxhash", None);
        assert_eq!(
            imported_name(&db, OUTSIDER).as_deref(),
            Some("final rename")
        );
        assert_eq!(imported_name(&db, NEWCOMER), None);
    }
}

#[test]
fn a_first_read_waits_while_a_key_changed_above_the_final_level() {
    let (mut store, db) = setup("profiles_registry_first_read_unsettled");
    let rig = Rig::new();
    let head = rig.tzkt_fake.with(|s| s.head);
    // Removed in a block that is not final: the live-key read does not list it.
    rig.registry_key("users_name", 13, OUTSIDER, None, head);
    rig.run(&mut store, "fxhash", None);
    assert_eq!(imported_name(&db, OUTSIDER), None);
    // Rolled back: the key returns with its old level, below the final level.
    rig.registry_key("users_name", 13, OUTSIDER, Some("outsider"), head - 10);
    rig.tzkt_fake.with(|s| s.head += 1);
    rig.run(&mut store, "fxhash", None);
    assert_eq!(imported_name(&db, OUTSIDER).as_deref(), Some("outsider"));
}

/// The fake TzKT, except that a block that is not final removes OUTSIDER's name just before
/// the first `users_name` read that is not a `lastLevel.gt` read.
struct RemoveBeforeFirstRead {
    inner: Arc<FakeTzkt>,
    removed: AtomicBool,
}

impl Fetch for RemoveBeforeFirstRead {
    fn get(&self, url: &str) -> Result<HttpResponse, TransportError> {
        if url.contains("/users_name/keys?")
            && !url.contains("lastLevel.gt=")
            && !self.removed.swap(true, Ordering::SeqCst)
        {
            self.inner.with(|s| {
                let head = s.head;
                for k in &mut s.listings {
                    if k["_path"] == "users_name" && k["key"] == OUTSIDER {
                        k["active"] = json!(false);
                        k["value"] = Value::Null;
                        k["lastLevel"] = json!(head);
                    }
                }
            });
        }
        self.inner.get(url)
    }
}

fn synced_level(db: &Path) -> Option<i64> {
    Connection::open(db)
        .unwrap()
        .query_row(
            "SELECT synced_level FROM profile_sync WHERE source = 'fxhash'",
            [],
            |r| r.get(0),
        )
        .unwrap()
}

#[test]
fn a_first_read_recovers_a_name_removed_by_a_nonfinal_block_then_rolled_back() {
    let (mut store, db) = setup("profiles_registry_first_read_race");
    let rig = Rig::new();
    let before = rig.tzkt_fake.with(|s| s.listings.clone());
    let racing = Arc::new(RemoveBeforeFirstRead {
        inner: rig.tzkt_fake.clone(),
        removed: AtomicBool::new(false),
    });
    let tzkt = Tzkt::new(quick_client(
        racing.clone(),
        "http://fake.test/v1",
        &rig.stop,
    ));
    rig.try_run_with(&mut store, &tzkt, "fxhash", None).unwrap();
    assert!(racing.removed.load(Ordering::SeqCst));
    assert_eq!(imported_name(&db, OUTSIDER), None);
    assert_eq!(synced_level(&db), None, "the first-read cursor stays put");
    drop(store);
    let mut store = Store::open(&db).unwrap();
    // Rolled back: the key returns with its old value and level, below any final level.
    let head = rig.tzkt_fake.with(|s| {
        s.listings = before;
        s.head += FINALITY + 1;
        s.head
    });
    rig.run(&mut store, "fxhash", None);
    assert_eq!(imported_name(&db, OUTSIDER).as_deref(), Some("outsider"));
    assert_eq!(synced_level(&db), Some((head - FINALITY) as i64));
}

#[test]
fn unusable_documents_are_permanent_and_missing_ones_retry_next_pass() {
    let (mut store, db) = setup("profiles_doc_outcomes");
    let rig = Rig::new();
    rig.both(DOC_KETI.1, 200, "<html>not json</html>");
    rig.both(DOC_RYAN.1, 404, "");
    let r = rig.run(&mut store, "fxhash", None);
    let fx = report(&r, ProfileSource::Fxhash);
    assert_eq!((fx.fetched, fx.invalid, fx.unavailable), (1, 1, 1));
    assert!(
        fx.pass_complete && !r.transient,
        "per-document failures do not stop a pass"
    );
    assert_eq!(fetch_state(&db, KETI), ("invalid".into(), 1));
    assert_eq!(fetch_state(&db, RYAN), ("pending".into(), 1));
    assert_eq!(fx.pending, 1);
    // The next pass retries the missing document only.
    rig.both(DOC_RYAN.1, 200, &doc("found later", ""));
    let r = rig.run(&mut store, "fxhash", None);
    assert_eq!(report(&r, ProfileSource::Fxhash).fetched, 1);
    assert_eq!(fetch_state(&db, RYAN), ("done".into(), 2));
    assert_eq!(fetch_state(&db, KETI).0, "invalid");
    assert_eq!(
        rig.doc_requests(DOC_KETI.1),
        2,
        "one request per gateway, once"
    );
    // One gateway serving garbage while another is down is not proof of anything.
    rig.both(DOC_LIET.1, 200, "garbage");
    rig.gateways[1].serve(DOC_LIET.1, 503, "");
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "UPDATE profile_fetch SET state = 'pending' WHERE address = ?1",
        [LIET],
    )
    .unwrap();
    rig.run(&mut store, "fxhash", None);
    assert_eq!(fetch_state(&db, LIET).0, "pending");
}

#[test]
fn unreachable_gateways_stop_transient_and_resume_where_they_stopped() {
    let (mut store, db) = setup("profiles_gateways_down");
    let rig = Rig::new();
    for g in &rig.gateways {
        *g.down.lock().unwrap() = Some(503);
    }
    // Batches of 2: RYAN and KETI fail together, so the run stops after that batch.
    let r = rig.run(&mut store, "fxhash", None);
    assert!(r.transient);
    let fx = report(&r, ProfileSource::Fxhash);
    assert_eq!((fx.fetched, fx.unavailable, fx.invalid), (0, 2, 0));
    assert!(!fx.pass_complete);
    // Nothing became permanent; the batch stays pending and LIET was not reached.
    assert_eq!(fetch_state(&db, RYAN), ("pending".into(), 1));
    assert_eq!(fetch_state(&db, KETI), ("pending".into(), 1));
    assert_eq!(fetch_state(&db, LIET), ("pending".into(), 0));
    for g in &rig.gateways {
        *g.down.lock().unwrap() = None;
    }
    // The pass resumes after the failed batch, then the next pass retries it.
    let r = rig.run(&mut store, "fxhash", None);
    assert!(!r.transient && report(&r, ProfileSource::Fxhash).pass_complete);
    assert_eq!(fetch_state(&db, LIET).0, "done");
    rig.run(&mut store, "fxhash", None);
    for a in [KETI, RYAN, LIET] {
        assert_eq!(fetch_state(&db, a).0, "done", "{a}");
    }
}

#[test]
fn limited_runs_walk_the_pass_without_starving_later_addresses() {
    let (mut store, db) = setup("profiles_limit");
    let rig = Rig::new();
    // The first address in order keeps failing; later ones must still be reached.
    let first = [KETI, RYAN, LIET].into_iter().min().unwrap();
    let cid = [(KETI, DOC_KETI.1), (RYAN, DOC_RYAN.1), (LIET, DOC_LIET.1)]
        .into_iter()
        .find(|(a, _)| *a == first)
        .unwrap()
        .1;
    rig.both(cid, 404, "");
    let mut fetched = 0;
    for _ in 0..3 {
        let r = rig.run(&mut store, "fxhash", Some(1));
        let fx = report(&r, ProfileSource::Fxhash);
        fetched += fx.fetched;
    }
    assert_eq!(
        fetched, 2,
        "both later documents arrived despite the failing first one"
    );
    assert_eq!(fetch_state(&db, first), ("pending".into(), 1));
    // The fourth run ends the pass; the fifth starts a new one and retries the failure.
    let r = rig.run(&mut store, "fxhash", Some(1));
    assert!(report(&r, ProfileSource::Fxhash).pass_complete);
    rig.run(&mut store, "fxhash", Some(1));
    assert_eq!(fetch_state(&db, first), ("pending".into(), 2));
}

#[test]
fn objkt_holders_import_and_flagged_or_missing_ones_are_removed() {
    let (mut store, db) = setup("profiles_objkt");
    let rig = Rig::new();
    rig.objkt_fake.holder(json!({
        "address": KETI, "alias": "Keti on objkt", "description": "objkt bio",
        "website": "https://keti.example", "twitter": "https://x.com/keti",
        "github": "not a url", "logo": "https://assets.objkt.media/keti/logo", "flag": "none"
    }));
    rig.objkt_fake.holder(json!({
        "address": RYAN, "alias": "Banned Ryan", "flag": "banned"
    }));
    rig.objkt_fake.holder(json!({
        "address": LIET, "alias": null, "description": null, "flag": "none"
    }));
    let r = rig.run(&mut store, "objkt", None);
    let ob = report(&r, ProfileSource::Objkt);
    let matter = addresses_that_matter_of(&store, db.parent().unwrap());
    assert_eq!(ob.fetched, matter.len());
    assert_eq!(ob.written, 1);
    assert!(ob.pass_complete);
    // Every address that matters was asked, in order, at most OBJKT_BATCH per query.
    let asked: Vec<String> = rig
        .objkt_fake
        .requests
        .lock()
        .unwrap()
        .iter()
        .inspect(|batch| assert!(batch.len() <= OBJKT_BATCH))
        .flatten()
        .cloned()
        .collect();
    assert_eq!(asked, matter);
    assert_eq!(
        imported(&db, KETI, "objkt"),
        Some((Some("Keti on objkt".into()), Some("objkt bio".into())))
    );
    assert_eq!(
        imported(&db, RYAN, "objkt"),
        None,
        "flagged holders are not imported"
    );
    assert_eq!(
        imported(&db, LIET, "objkt"),
        None,
        "empty holders are not imported"
    );
    let keti = store.profile(KETI).unwrap();
    assert_eq!(
        keti.name.as_deref(),
        Some("KetiTaki"),
        "fxhash name precedes objkt"
    );
    assert_eq!(keti.links, vec!["https://x.com/keti".to_owned()]);
    assert_eq!(keti.website.as_deref(), Some("https://keti.example/"));
    // A holder that disappears or gets flagged loses its objkt row on the next pass.
    rig.objkt_fake.holders.lock().unwrap().remove(KETI);
    let r = rig.run(&mut store, "objkt", None);
    assert_eq!(report(&r, ProfileSource::Objkt).removed, 1);
    assert_eq!(imported(&db, KETI, "objkt"), None);
}

#[test]
fn objkt_failures_keep_the_cursor_and_errors_are_permanent() {
    let (mut store, db) = setup("profiles_objkt_failures");
    let rig = Rig::new();
    rig.objkt_fake
        .holder(json!({"address": KETI, "alias": "Keti on objkt", "flag": "none"}));
    *rig.objkt_fake.down.lock().unwrap() = Some(503);
    let r = rig.run(&mut store, "objkt", None);
    assert!(r.transient);
    assert_eq!(report(&r, ProfileSource::Objkt).fetched, 0);
    *rig.objkt_fake.down.lock().unwrap() = None;
    *rig.objkt_fake.errors.lock().unwrap() = true;
    match rig.try_run(&mut store, "objkt", None) {
        Err(IngestError::Api(ApiError::Permanent(m))) => assert!(m.contains("GraphQL errors")),
        other => panic!("{other:?}"),
    }
    *rig.objkt_fake.errors.lock().unwrap() = false;
    let r = rig.run(&mut store, "objkt", None);
    assert!(report(&r, ProfileSource::Objkt).pass_complete);
    assert!(imported(&db, KETI, "objkt").is_some());
}

#[test]
fn tezos_profiles_is_skipped_with_a_note_and_no_request() {
    let (mut store, _db) = setup("profiles_tzprofiles");
    let rig = Rig::new();
    let r = rig.run(&mut store, "tzprofiles", None);
    let tz = report(&r, ProfileSource::Tzprofiles);
    assert!(
        tz.note
            .as_deref()
            .unwrap()
            .contains("kepler.tzprofiles.com")
    );
    assert_eq!(rig.tzkt_fake.with(|s| s.requests.len()), 0);
    assert!(rig.objkt_fake.requests.lock().unwrap().is_empty());
}

#[test]
fn an_interrupted_run_keeps_committed_batches() {
    let (mut store, db) = setup("profiles_interrupt");
    let rig = Rig::new();
    rig.run(&mut store, "objkt", None);
    rig.stop.store(true, Ordering::Relaxed);
    let r = rig.run(&mut store, "all", None);
    assert!(r.interrupted);
    rig.stop.store(false, Ordering::Relaxed);
    let r = rig.run(&mut store, "all", None);
    assert!(!r.interrupted && !r.transient);
    assert_eq!(fetch_state(&db, KETI).0, "done");
}

#[test]
fn profile_ingest_never_touches_local_edits() {
    let (mut store, db) = setup("profiles_keep_edits");
    // ciphrd's revealed key, read from TzKT; a live offer gives the address history.
    let owner = "tz1PoDdN2oyRyF6DA73zTWAWYhNL4UGr3Egj";
    Connection::open(&db)
        .unwrap()
        .execute(
            "INSERT INTO market_offers (marketplace, kind, offer_id, buyer, price, remaining, \
             project_id, created_level, level, active) VALUES \
             ('KT1GbyoDi7H1sfXmimXpptZJuCdHMh66WS9u', 'collection', 1, ?1, 1000000, 1, 29437, \
             1, 1, 1)",
            [owner],
        )
        .unwrap();
    store
        .save_profile_edit(
            owner,
            ProfileEdit {
                name: Some("Local Name".into()),
                bio: Some("local bio".into()),
                links: vec![],
            },
            EditProof {
                message: "seeded.art profile edit".into(),
                signature: "edsigtest".into(),
                public_key: "edpkukC966pBdnKAYgkRsc7KgxYJwzvj5wwLVFV5t8ozjhyjiyFWEj".into(),
            },
        )
        .unwrap();
    let rig = Rig::new();
    rig.registry_key("users_name", 30, owner, Some("ciphrd"), 1);
    rig.objkt_fake
        .holder(json!({"address": owner, "alias": "ciphrd", "flag": "none"}));
    rig.run(&mut store, "all", None);
    let edits: Vec<(String, Option<String>)> = Connection::open(&db)
        .unwrap()
        .prepare("SELECT address, name FROM profile_edits")
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert_eq!(edits, vec![(owner.to_owned(), Some("Local Name".into()))]);
    assert_eq!(
        store.profile(owner).unwrap().name.as_deref(),
        Some("Local Name")
    );
}

/// The name `artist_fts` holds for the artist at `address`.
fn indexed_name(db: &Path, address: &str) -> String {
    Connection::open(db)
        .unwrap()
        .query_row(
            "SELECT f.name FROM artists a JOIN artist_fts f ON f.rowid = a.seq \
             WHERE a.address = ?1",
            [address],
            |r| r.get(0),
        )
        .unwrap()
}

fn catalogue(store: &Store, text: &str) -> Vec<u64> {
    let filter = seeded_archive::store::ProjectFilter {
        text: Some(text.into()),
        artist: None,
        sort: seeded_archive::store::ProjectSort::IdAsc,
    };
    let page = store.projects_page(&filter, None, 100).unwrap();
    page.items.iter().map(|p| p.id).collect()
}

#[test]
fn profile_batches_keep_search_names_in_step() {
    const PIXEL: &str = "tz1YgYy8P9orNKvhbvoT2suV3upRCTAxXvnd";
    let (mut store, db) = setup("profiles_search_names");
    assert_eq!(catalogue(&store, "pixel"), vec![15755]);
    assert_eq!(catalogue(&store, "ketitaki"), vec![15755]);
    // A registry name shared with an artist's name is numbered in the registry batch: the
    // credited artist keeps the plain name, the outsider is "PIXEL-2".
    let rig = Rig::new();
    let level = rig.tzkt_fake.with(|s| s.head);
    rig.registry_key("users_name", 13, OUTSIDER, Some("PIXEL"), level - 10);
    rig.run(&mut store, "fxhash", None);
    assert_eq!(store.profile(PIXEL).unwrap().name.as_deref(), Some("Pixel"));
    assert_eq!(
        store.profile(OUTSIDER).unwrap().name.as_deref(),
        Some("PIXEL-2")
    );
    assert_eq!(indexed_name(&db, PIXEL), "Pixel");
    assert_eq!(catalogue(&store, "pixel"), vec![15755]);
    // An objkt alias an address never shows does not number anyone: LIET shows its artist
    // name, so KETI keeps the plain name.
    rig.objkt_fake
        .holder(json!({"address": LIET, "alias": "KetiTaki", "flag": "none"}));
    rig.run(&mut store, "objkt", None);
    assert_eq!(store.profile(LIET).unwrap().name.as_deref(), Some("liet"));
    assert_eq!(
        store.profile(KETI).unwrap().name.as_deref(),
        Some("KetiTaki")
    );
    assert_eq!(indexed_name(&db, LIET), "liet");
    assert_eq!(indexed_name(&db, KETI), "KetiTaki");
    assert_eq!(catalogue(&store, "ketitaki"), vec![15755]);
    rig.objkt_fake.holders.lock().unwrap().remove(LIET);
    rig.run(&mut store, "objkt", None);
    assert_eq!(indexed_name(&db, LIET), "liet");
    assert_eq!(indexed_name(&db, KETI), "KetiTaki");
    assert_eq!(catalogue(&store, "ketitaki"), vec![15755]);
    let queued: i64 = Connection::open(&db)
        .unwrap()
        .query_row("SELECT count(*) FROM search_dirty", [], |r| r.get(0))
        .unwrap();
    assert_eq!(queued, 0);
}
