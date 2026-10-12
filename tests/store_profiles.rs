//! Profiles: the duplicate-name rule, case-insensitive resolution, claims and their
//! collisions, edit validation, field precedence, and edits surviving every import.

mod ingest_support;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use ingest_support::*;
use rusqlite::{Connection, params};
use seeded_archive::import::import_fixtures;
use seeded_archive::ingest::frank::bootstrap;
use seeded_archive::ingest::sync::{SyncOptions, sync};
use seeded_archive::model::{AddressKind, EditProof, Profile, ProfileEdit};
use seeded_archive::store::{Cursor, ProjectFilter, ProjectSort, Store, StoreError};

/// Revealed keys read from TzKT on 2026-10-05: (address, public key).
const ED_A: (&str, &str) = (
    "tz1PoDdN2oyRyF6DA73zTWAWYhNL4UGr3Egj",
    "edpkukC966pBdnKAYgkRsc7KgxYJwzvj5wwLVFV5t8ozjhyjiyFWEj",
);
const ED_B: (&str, &str) = (
    "tz1itwvWyP4qd6PY7qrYkn3LBvDC13vjQS9i",
    "edpktrU1cZyBK4vBU1WSpcmrCfqgUsgC4NYGRcyim6ijn3XBipXYmh",
);
const SECP: (&str, &str) = (
    "tz2VtWQNPH2TsDixaShXp5615sxDNZ4uB436",
    "sppk7aEz5GzduJrXvNWQaTbWKRiWHqkUhrnSt3h4dD49PgLsyyKszVr",
);
const P256: (&str, &str) = (
    "tz3fPdUoSmoLYNggLXPHedyGiLCPQZM6jwCN",
    "p2pk67RZCKQ7LwRsGriNZL4YJKankdd24WrUfzYifxcqqQrLb3K2rsN",
);
const KEEPER: &str = "tz1gL9H53BzjDqmMi2JkKALeCzT7UAoopzEL";
const MINT: &str = "tz1Kq8NtEjSXkpswLmgod1tPFPVH2TZc8dwM";
/// Shares "Mint Eastwood" with MINT, in other letter case.
const MINT_TWIN: &str = "tz1TEL6xFJx1hJKukx21NCufAfGEEr9yYoKX";
const KETI: &str = "tz1WMa1cAKk4JjFGZCpXNkTDi5o5AqCsFF6E";
const STRANGER: &str = "tz1VGZPvqytFVAoSAsM8VrUveJdL3PpVy6K9";
const ESCROW_V2: &str = "KT1GbyoDi7H1sfXmimXpptZJuCdHMh66WS9u";
const BURN: &str = "tz1burnburnburnburnburnburnburjAYjjX";

/// The Frank fixture with "Mint Eastwood" also claimed by MINT_TWIN as "mint EASTWOOD". The
/// signing addresses have history (a live offer each), so they may claim names.
fn names_store(name: &str) -> (Store, PathBuf) {
    let dir = test_dir(name);
    let frank = frank_copy(&dir);
    edit_json(&frank.join("public/data/users.json"), |v| {
        v["names"][MINT_TWIN] = "mint EASTWOOD ".into();
    });
    let db = dir.join("db.sqlite");
    let mut store = Store::open(&db).unwrap();
    assert!(bootstrap(&mut store, &frank).unwrap().changed);
    for key in [ED_A, ED_B, SECP, P256] {
        offer(&db, key.0, true);
    }
    (store, db)
}

/// A collection offer by `buyer` (live or ended): history in the archive when live.
fn offer(db: &Path, buyer: &str, active: bool) {
    Connection::open(db)
        .unwrap()
        .execute(
            "INSERT INTO market_offers (marketplace, kind, offer_id, buyer, price, remaining, \
             project_id, created_level, level, active) VALUES (?1, 'collection', \
             (SELECT coalesce(max(offer_id), 0) + 1 FROM market_offers), ?2, 1000000, ?3, \
             29437, 1, 1, ?3)",
            params![ESCROW_V2, buyer, i64::from(active)],
        )
        .unwrap();
}

fn sql(db: &Path, statement: &str) {
    Connection::open(db)
        .unwrap()
        .execute_batch(statement)
        .unwrap();
}

fn objkt(db: &Path, address: &str, name: Option<&str>, bio: Option<&str>, links: &str) {
    Connection::open(db)
        .unwrap()
        .execute(
            "INSERT INTO profiles_imported (address, source, name, name_key, bio, links, \
             fetched_at) VALUES (?1, 'objkt', ?2, lower(?2), ?3, ?4, 'now') \
             ON CONFLICT (address, source) DO UPDATE SET name = excluded.name, \
             name_key = excluded.name_key, bio = excluded.bio, links = excluded.links",
            params![address, name, bio, links],
        )
        .unwrap();
}

fn proof(key: (&str, &str)) -> EditProof {
    EditProof {
        message: format!("seeded.art profile edit for {}", key.0),
        signature: "edsigtJ5aJVPNULkSdpVVuJ1FWdUNEH9JLQtCQRd4KfHd8TAs5S6Ybx".into(),
        public_key: key.1.into(),
    }
}

fn claim(store: &mut Store, key: (&str, &str), name: &str) -> Result<(), StoreError> {
    store.save_profile_edit(
        key.0,
        ProfileEdit {
            name: Some(name.into()),
            ..ProfileEdit::default()
        },
        proof(key),
    )
}

fn bad_request<T: std::fmt::Debug>(r: Result<T, StoreError>) -> String {
    match r {
        Err(StoreError::BadRequest(m)) => m,
        other => panic!("expected BadRequest, got {other:?}"),
    }
}

fn shown(store: &Store, address: &str) -> (Option<String>, Option<String>) {
    let p = store.profile(address).unwrap();
    (p.name, p.name_source)
}

fn named(name: &str, source: &str) -> (Option<String>, Option<String>) {
    (Some(name.into()), Some(source.into()))
}

fn by_name(store: &Store, name: &str) -> Option<String> {
    match store.profile_by_name(name) {
        Ok(p) => Some(p.address),
        Err(StoreError::NotFound { .. }) => None,
        Err(e) => panic!("{e:?}"),
    }
}

#[test]
fn imported_names_shared_case_insensitively_are_numbered() {
    let (store, _db) = names_store("profiles_shared_names");
    // MINT is credited on a project, MINT_TWIN is a registry name only.
    assert_eq!(shown(&store, MINT), named("Mint Eastwood", "fxhash"));
    assert_eq!(shown(&store, MINT_TWIN), named("mint EASTWOOD-2", "fxhash"));
    assert_eq!(by_name(&store, "Mint Eastwood").as_deref(), Some(MINT));
    assert_eq!(by_name(&store, "MINT EASTWOOD").as_deref(), Some(MINT));
    assert_eq!(
        by_name(&store, "Mint Eastwood-2").as_deref(),
        Some(MINT_TWIN)
    );
    assert_eq!(by_name(&store, "mint eastwood-3"), None);
    let labels = store.display_names(&[MINT, MINT_TWIN, QUADRADO]).unwrap();
    assert_eq!(labels.len(), 3);
    assert_eq!(labels[MINT], "Mint Eastwood");
    assert_eq!(labels[MINT_TWIN], "mint EASTWOOD-2");
    assert_eq!(labels[QUADRADO], "quadrado");
    // A unique name resolves in any letter case and with outer spaces.
    assert_eq!(shown(&store, QUADRADO), named("quadrado", "fxhash"));
    for spelling in ["quadrado", "QUADRADO", " QuAdRaDo "] {
        assert_eq!(by_name(&store, spelling).as_deref(), Some(QUADRADO));
    }
    assert!(matches!(
        store.profile_by_name("   "),
        Err(StoreError::BadRequest(_))
    ));
}

#[test]
fn a_shared_name_without_a_number_is_hidden_until_assigned() {
    let (mut store, db) = names_store("profiles_objkt_shared");
    objkt(&db, STRANGER, Some("HONEST"), None, "[]");
    assert_eq!(
        shown(&store, HONEST),
        (None, None),
        "fxhash name now shared, nobody numbered yet"
    );
    assert_eq!(shown(&store, STRANGER), (None, None));
    assert_eq!(by_name(&store, "honest"), None);
    let r = store.assign_name_numbers().unwrap();
    assert_eq!((r.assigned, r.moved), (2, 0));
    assert_eq!(shown(&store, HONEST), named("Honest", "fxhash"));
    assert_eq!(shown(&store, STRANGER), named("HONEST-2", "objkt"));
    assert_eq!(by_name(&store, "honest-2").as_deref(), Some(STRANGER));
    // A name that is an address is never a name.
    objkt(&db, STRANGER, Some(STRANGER), None, "[]");
    assert_eq!(shown(&store, STRANGER), (None, None));
    assert_eq!(shown(&store, HONEST), named("Honest", "fxhash"));
}

#[test]
fn only_the_highest_source_name_is_shown_or_numbered() {
    let (store, db) = names_store("profiles_fallback_name");
    // A third carrier of "Mint Eastwood" has no number yet: hidden, with no fall-through to
    // its lower objkt name.
    sql(
        &db,
        &format!(
            "INSERT INTO users (address, name, name_key) VALUES ('{STRANGER}', 'MINT eastwood', \
             'mint eastwood')"
        ),
    );
    objkt(
        &db,
        STRANGER,
        Some("Eastwood Prints"),
        Some("objkt bio"),
        "[]",
    );
    assert_eq!(shown(&store, STRANGER), (None, None));
    assert_eq!(by_name(&store, "eastwood prints"), None);
    // A lower name another address shows alone does not make it a carrier.
    objkt(&db, MINT, Some("Eastwood Prints"), None, "[]");
    assert_eq!(shown(&store, MINT), named("Mint Eastwood", "fxhash"));
    // Only the shown name resolves: a valid lower-precedence name hidden behind a valid
    // higher one does not.
    objkt(&db, QUADRADO, Some("Square Works"), None, "[]");
    assert_eq!(shown(&store, QUADRADO), named("quadrado", "fxhash"));
    assert_eq!(by_name(&store, "square works"), None);
}

#[test]
fn curated_identities_win_over_imported_names() {
    let (store, db) = names_store("profiles_curated");
    sql(
        &db,
        &format!(
            "INSERT INTO users (address, name, name_key) VALUES ('{STRANGER}', 'emptyNAME', \
             'emptyname')"
        ),
    );
    assert_eq!(shown(&store, KEEPER), named("Emptyname", "curated"));
    assert_eq!(shown(&store, STRANGER), (None, None));
    assert_eq!(by_name(&store, "EMPTYNAME").as_deref(), Some(KEEPER));
    let keeper = store.profile(KEEPER).unwrap();
    assert_eq!(keeper.website, None, "no hardcoded link");
    assert!(keeper.project_count >= 1, "Arboreal is credited");
}

#[test]
fn claimed_names_win_and_every_collision_is_a_bad_request() {
    let (mut store, db) = names_store("profiles_claims");
    claim(&mut store, ED_A, "Seeded One").unwrap();
    assert_eq!(shown(&store, ED_A.0), named("Seeded One", "claimed"));
    assert_eq!(by_name(&store, "SEEDED one").as_deref(), Some(ED_A.0));
    // Another claim, in any case.
    let m = bad_request(claim(&mut store, ED_B, "seeded ONE"));
    assert!(m.contains("taken"), "{m}");
    // A valid imported name of another address.
    bad_request(claim(&mut store, ED_B, "Quadrado"));
    // A curated identity.
    bad_request(claim(&mut store, ED_B, "emptyname"));
    // A shared (invalid) imported name cannot be taken over by a stranger either.
    bad_request(claim(&mut store, ED_B, "mint eastwood"));
    // The address's own imported name is fine to claim, and re-claiming replaces the edit.
    objkt(&db, ED_B.0, Some("Bee"), None, "[]");
    claim(&mut store, ED_B, "BEE").unwrap();
    assert_eq!(shown(&store, ED_B.0), named("BEE", "claimed"));
    claim(&mut store, ED_B, "Bee Two").unwrap();
    assert_eq!(by_name(&store, "bee two").as_deref(), Some(ED_B.0));
    assert_eq!(
        by_name(&store, "bee").as_deref(),
        None,
        "Bee is no longer shown"
    );
    // An imported name arriving later that collides with a claim is the invalid one.
    objkt(&db, STRANGER, Some("Seeded One"), None, "[]");
    assert_eq!(shown(&store, STRANGER), (None, None));
    assert_eq!(by_name(&store, "seeded one").as_deref(), Some(ED_A.0));
    // check_profile_edit applies the same rules without writing.
    let edit = ProfileEdit {
        name: Some("Seeded One".into()),
        ..ProfileEdit::default()
    };
    bad_request(store.check_profile_edit(ED_B.0, &edit));
    store.check_profile_edit(ED_A.0, &edit).unwrap();
}

#[test]
fn edits_are_validated_before_anything_is_stored() {
    let (mut store, db) = names_store("profiles_edit_validation");
    let save = |store: &mut Store, key: (&str, &str), edit: ProfileEdit| {
        store.save_profile_edit(key.0, edit, proof(key))
    };
    let name = |n: &str| ProfileEdit {
        name: Some(n.into()),
        ..ProfileEdit::default()
    };
    for bad in [
        "",
        "  ",
        "a  b",
        "-dash",
        "naïve",
        "<b>x</b>",
        &"n".repeat(41),
        STRANGER,
    ] {
        bad_request(save(&mut store, ED_A, name(bad)));
    }
    let bio = |b: String| ProfileEdit {
        bio: Some(b),
        ..ProfileEdit::default()
    };
    save(&mut store, ED_A, bio("é".repeat(1000))).unwrap();
    bad_request(save(&mut store, ED_A, bio("é".repeat(1001))));
    bad_request(save(&mut store, ED_A, bio("bell\u{0007}".into())));
    let links = |l: &[&str]| ProfileEdit {
        links: l.iter().map(|s| (*s).to_owned()).collect(),
        ..ProfileEdit::default()
    };
    let five: Vec<String> = (0..5).map(|i| format!("https://e{i}.org/")).collect();
    let five: Vec<&str> = five.iter().map(String::as_str).collect();
    save(&mut store, ED_A, links(&five)).unwrap();
    let six: Vec<String> = (0..6).map(|i| format!("https://e{i}.org/")).collect();
    let six: Vec<&str> = six.iter().map(String::as_str).collect();
    bad_request(save(&mut store, ED_A, links(&six)));
    for bad in [
        "http://e.org/",
        "https://user:pw@e.org/",
        "https://user@e.org/",
        "javascript:alert(1)",
        "/relative",
        "e.org",
    ] {
        bad_request(save(&mut store, ED_A, links(&[bad])));
    }
    bad_request(save(
        &mut store,
        ED_A,
        links(&["https://e.org/", "https://e.org/"]),
    ));
    // The key must be the address's: another key, a contract, or an unsupported kind fail.
    let mismatched = store.save_profile_edit(ED_A.0, name("Fine"), proof(ED_B));
    assert!(bad_request(mismatched).contains("public key"));
    bad_request(store.save_profile_edit(ESCROW_V2, name("Fine"), proof(ED_A)));
    bad_request(store.save_profile_edit("not-an-address", name("Fine"), proof(ED_A)));
    let mut empty = proof(ED_A);
    empty.message = " ".into();
    bad_request(store.save_profile_edit(ED_A.0, name("Fine"), empty));
    // tz2 and tz3 keys work too.
    save(&mut store, SECP, name("Secp Artist")).unwrap();
    save(&mut store, P256, name("P256 Artist")).unwrap();
    // Every rejected edit left nothing behind; accepted ones keep their proof for audit.
    let rows: Vec<(String, String, String)> = Connection::open(&db)
        .unwrap()
        .prepare("SELECT address, signed_message, public_key FROM profile_edits ORDER BY address")
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    let addresses: Vec<&str> = rows.iter().map(|r| r.0.as_str()).collect();
    assert_eq!(addresses, vec![ED_A.0, SECP.0, P256.0]);
    assert_eq!(rows[0].2, ED_A.1);
    assert!(rows[0].1.contains(ED_A.0));
    assert_eq!(store.profile(ED_A.0).unwrap().links, five);
}

#[test]
fn every_field_follows_edit_identity_fxhash_objkt_precedence() {
    let (mut store, db) = names_store("profiles_precedence");
    // KETI: fxhash artist bio and avatar; objkt adds links and a website.
    objkt(
        &db,
        KETI,
        Some("Keti objkt"),
        Some("objkt bio"),
        r#"["https://x.com/keti"]"#,
    );
    sql(
        &db,
        &format!(
            "UPDATE profiles_imported SET website = 'https://keti.example/', \
             avatar_uri = 'https://objkt.example/logo' WHERE address = '{KETI}'"
        ),
    );
    let p = store.profile(KETI).unwrap();
    assert_eq!(p.name.as_deref(), Some("KetiTaki"));
    assert_eq!(p.bio.as_deref(), Some("Experimental Generative Coding."));
    assert!(p.avatar_uri.as_deref().unwrap().starts_with("ipfs://"));
    assert_eq!(p.website.as_deref(), Some("https://keti.example/"));
    assert_eq!(p.links, vec!["https://x.com/keti".to_owned()]);
    // ED_A: objkt only, then the owner's edit overrides field by field.
    objkt(
        &db,
        ED_A.0,
        Some("Imported A"),
        Some("imported bio"),
        r#"["https://x.com/a"]"#,
    );
    let p = store.profile(ED_A.0).unwrap();
    assert_eq!(
        (p.name.as_deref(), p.bio.as_deref()),
        (Some("Imported A"), Some("imported bio"))
    );
    store
        .save_profile_edit(
            ED_A.0,
            ProfileEdit {
                name: None,
                bio: Some("my own words".into()),
                links: vec!["https://a.example/".into()],
            },
            proof(ED_A),
        )
        .unwrap();
    let p = store.profile(ED_A.0).unwrap();
    assert_eq!(
        p.name.as_deref(),
        Some("Imported A"),
        "no name edit keeps the import"
    );
    assert_eq!(p.name_source.as_deref(), Some("objkt"));
    assert_eq!(p.bio.as_deref(), Some("my own words"));
    assert_eq!(p.links, vec!["https://a.example/".to_owned()]);
    // An empty bio hides the imported one; empty links fall back to imported links.
    store
        .save_profile_edit(
            ED_A.0,
            ProfileEdit {
                name: None,
                bio: Some(String::new()),
                links: vec![],
            },
            proof(ED_A),
        )
        .unwrap();
    let p = store.profile(ED_A.0).unwrap();
    assert_eq!(p.bio, None);
    assert_eq!(p.links, vec!["https://x.com/a".to_owned()]);
    assert_eq!(
        store.profile_edit(ED_A.0).unwrap(),
        Some(ProfileEdit {
            name: None,
            bio: Some(String::new()),
            links: vec![],
        })
    );
    assert_eq!(store.profile_edit(ED_B.0).unwrap(), None);
}

#[test]
fn every_address_is_a_profile_and_kinds_are_labelled() {
    let (mut store, _db) = names_store("profiles_kinds");
    // Registers the escrow and burn addresses, as sync and upgrade do.
    seeded_archive::ingest::upgrade::upgrade(&mut store).unwrap();
    let empty = store.profile(STRANGER).unwrap();
    assert_eq!(
        empty,
        Profile {
            address: STRANGER.into(),
            name: None,
            name_source: None,
            bio: None,
            website: None,
            links: vec![],
            avatar_uri: None,
            kind: AddressKind::Account,
            project_count: 0,
            held: 0,
            listed: 0,
        }
    );
    for bad in [
        "",
        "tz5N64TWNCZR3yJZmmzBE4DRidN1tCaZCw8Q",
        "zancan",
        "tz1short",
    ] {
        assert!(
            matches!(store.profile(bad), Err(StoreError::BadRequest(_))),
            "{bad:?}"
        );
    }
    let escrow = store.profile(ESCROW_V2).unwrap();
    assert_eq!(escrow.kind, AddressKind::Escrow);
    assert_eq!(escrow.name.as_deref(), Some("fxhash marketplace v2"));
    assert_eq!(escrow.name_source.as_deref(), Some("known"));
    assert_eq!(store.profile(BURN).unwrap().kind, AddressKind::Burn);
    assert_eq!(
        store
            .profile("KT1EfsNuqwLAWDd3o4pvfUx1CAh5GMdTrRvr")
            .unwrap()
            .kind,
        AddressKind::Contract
    );
    assert!(matches!(
        store.display_names(&vec![QUADRADO; 501]),
        Err(StoreError::BadRequest(_))
    ));
}

#[test]
fn profile_counts_match_the_collector_and_artist_reads() {
    let dir = test_dir("profiles_counts");
    let mut store = bootstrapped(&dir);
    let fake = FakeTzkt::recorded();
    let stop = Arc::new(AtomicBool::new(false));
    let api = tzkt(&fake, &stop);
    let opts = SyncOptions {
        contracts: vec![V1.into(), V2.into(), PARAMS.into()],
        limit: None,
        concurrency: 1,
        batch: 500,
    };
    sync(&mut store, &api, &opts, &|_| {}).unwrap();
    let collector = store.collector(QUADRADO).unwrap();
    let p = store.profile(QUADRADO).unwrap();
    assert_eq!(
        (p.held, p.listed),
        (collector.token_count, collector.listed)
    );
    assert!(p.held > 0);
    let artist = store.artist_by_address(KEEPER).unwrap();
    let projects = store
        .artist_projects_page(&artist.id, None, 100)
        .unwrap()
        .total;
    assert_eq!(
        u64::from(store.profile(KEEPER).unwrap().project_count),
        projects
    );
}

#[test]
fn profile_edits_survive_fixture_imports_and_bootstraps() {
    // Fixture database: a changed fixture set replaces every catalogue table.
    let dir = test_dir("profiles_survive_fixtures");
    let fixtures = dir.join("fixtures");
    copy_dir(&root().join("assets/fixtures"), &fixtures);
    let mut store = Store::open(&dir.join("db.sqlite")).unwrap();
    assert!(import_fixtures(&mut store, &fixtures).unwrap().changed);
    offer(&dir.join("db.sqlite"), ED_A.0, true);
    claim(&mut store, ED_A, "Survivor").unwrap();
    edit_json(&fixtures.join("snapshot.json"), |v| {
        v["as_of"] = "2026-10-04".into();
    });
    assert!(import_fixtures(&mut store, &fixtures).unwrap().changed);
    assert_eq!(shown(&store, ED_A.0), named("Survivor", "claimed"));
    drop(store);
    // Bootstrapped database: a changed archive is re-imported over it.
    let dir = test_dir("profiles_survive_bootstrap");
    let frank = frank_copy(&dir);
    let mut store = Store::open(&dir.join("db.sqlite")).unwrap();
    assert!(bootstrap(&mut store, &frank).unwrap().changed);
    offer(&dir.join("db.sqlite"), ED_A.0, true);
    claim(&mut store, ED_A, "Survivor").unwrap();
    edit_json(&frank.join("public/data/users.json"), |v| {
        v["names"][STRANGER] = "newcomer".into();
    });
    assert!(bootstrap(&mut store, &frank).unwrap().changed);
    assert_eq!(shown(&store, ED_A.0), named("Survivor", "claimed"));
    assert_eq!(shown(&store, STRANGER), named("newcomer", "fxhash"));
}

#[test]
fn bootstrap_writes_name_keys_for_every_user() {
    let (_store, db) = names_store("profiles_name_keys");
    let missing: i64 = Connection::open(&db)
        .unwrap()
        .query_row(
            "SELECT count(*) FROM users WHERE name_key IS NULL OR name_key <> lower(trim(name))",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(missing, 0);
}

/// Runs `setup` (history for ED_A), checks a name claim is allowed, then `undo` and checks it is
/// refused again; `check_profile_edit` applies the same rule as a save.
fn claim_allowed_with(db: &Path, store: &Store, setup: &str, undo: &str) -> bool {
    let edit = ProfileEdit {
        name: Some("Fresh Face".into()),
        ..ProfileEdit::default()
    };
    sql(db, setup);
    let allowed = store.check_profile_edit(ED_A.0, &edit).is_ok();
    sql(db, undo);
    let m = bad_request(store.check_profile_edit(ED_A.0, &edit));
    assert!(m.contains("history in the archive"), "{m}");
    allowed
}

#[test]
fn name_claims_need_history_in_the_archive_but_bio_and_links_do_not() {
    let dir = test_dir("profiles_claim_history");
    let frank = frank_copy(&dir);
    let db = dir.join("db.sqlite");
    let mut store = Store::open(&db).unwrap();
    assert!(bootstrap(&mut store, &frank).unwrap().changed);
    // No history: a name is refused, by the check and the save alike.
    let m = bad_request(claim(&mut store, ED_A, "Fresh Face"));
    assert_eq!(m, "name claims need an address with history in the archive");
    // Bio and links stay open to every tz1/tz2/tz3 address.
    store
        .save_profile_edit(
            ED_A.0,
            ProfileEdit {
                name: None,
                bio: Some("just looking".into()),
                links: vec!["https://e.org/".into()],
            },
            proof(ED_A),
        )
        .unwrap();
    let p = store.profile(ED_A.0).unwrap();
    assert_eq!((p.name, p.bio.as_deref()), (None, Some("just looking")));
    // Each kind of history allows a claim.
    let a = ED_A.0;
    let artist = format!(
        "INSERT INTO artists (id, name, name_key, address) VALUES ('{a}', '{a}', lower('{a}'), \
         '{a}');"
    );
    let token = |project: u64, minter: &str, holder: &str| {
        format!(
            "INSERT INTO tokens (contract, token_id, project_id, iteration, seed, minter, minted, \
             artifact_uri, source, owner_address, owner_as_of, owner_source, holder_address) \
             VALUES ('{V2}', 9000001, {project}, 999, 'oo', '{minter}', '2023-01-01', \
             'ipfs://x', 'tzkt', '{holder}', 'now', 'tzkt', '{holder}');"
        )
    };
    let drop_token = format!("DELETE FROM tokens WHERE contract = '{V2}' AND token_id = 9000001;");
    let unlink = format!(
        "DELETE FROM project_artists WHERE artist_id = '{a}'; \
         DELETE FROM artists WHERE id = '{a}';"
    );
    let cases: Vec<(&str, String, String, bool)> = vec![
        (
            "credited artist of a visible project",
            format!(
                "{artist} INSERT INTO project_artists (project_id, position, artist_id) \
                 VALUES (3245, 5, '{a}');"
            ),
            unlink.clone(),
            true,
        ),
        (
            "credited artist of a flagged project only",
            format!(
                "{artist} INSERT INTO project_artists (project_id, position, artist_id) \
                 VALUES ({FLAGGED}, 5, '{a}');"
            ),
            unlink.clone(),
            false,
        ),
        (
            "minter of a visible token",
            token(3245, a, STRANGER),
            drop_token.clone(),
            true,
        ),
        (
            "minter of a flagged token only",
            token(FLAGGED, a, STRANGER),
            drop_token.clone(),
            false,
        ),
        ("holder", token(3245, STRANGER, a), drop_token.clone(), true),
        (
            "seller of a live listing",
            format!(
                "INSERT INTO escrow_listings (escrow, listing, contract, token_id, seller, level) \
                 VALUES ('{ESCROW_V2}', '77', '{V2}', 1, '{a}', 1);"
            ),
            "DELETE FROM escrow_listings WHERE listing = '77';".into(),
            true,
        ),
        (
            "buyer of a live offer",
            format!(
                "INSERT INTO market_offers (marketplace, kind, offer_id, buyer, price, remaining, \
                 project_id, created_level, level, active) VALUES ('{ESCROW_V2}', 'collection', \
                 77, '{a}', 1000000, 1, 3245, 1, 1, 1);"
            ),
            "DELETE FROM market_offers WHERE offer_id = 77;".into(),
            true,
        ),
        (
            "buyer of an ended offer only",
            format!(
                "INSERT INTO market_offers (marketplace, kind, offer_id, buyer, price, remaining, \
                 project_id, created_level, level, active) VALUES ('{ESCROW_V2}', 'collection', \
                 77, '{a}', 1000000, 0, 3245, 1, 1, 0);"
            ),
            "DELETE FROM market_offers WHERE offer_id = 77;".into(),
            false,
        ),
    ];
    for (what, setup, undo, allowed) in &cases {
        assert_eq!(
            claim_allowed_with(&db, &store, setup, undo),
            *allowed,
            "{what}"
        );
    }
    // A claim made with history is kept when the history ends; a new name is not.
    offer(&db, a, true);
    claim(&mut store, ED_A, "Fresh Face").unwrap();
    sql(
        &db,
        &format!("UPDATE market_offers SET active = 0, remaining = 0 WHERE buyer = '{a}'"),
    );
    store
        .save_profile_edit(
            a,
            ProfileEdit {
                name: Some("FRESH face".into()),
                bio: Some("still here".into()),
                links: vec![],
            },
            proof(ED_A),
        )
        .unwrap();
    assert_eq!(shown(&store, a), named("FRESH face", "claimed"));
    let m = bad_request(claim(&mut store, ED_A, "Other Face"));
    assert!(m.contains("history in the archive"), "{m}");
}

/// Every hit of a profile search, two per page: (address, name).
fn found(store: &Store, text: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let mut cursor: Option<Cursor> = None;
    for _ in 0..100 {
        let page = store.profiles_search(text, cursor.as_ref(), 2).unwrap();
        out.extend(page.items.into_iter().map(|h| (h.address, h.name)));
        match page.next {
            Some(c) => cursor = Some(c),
            None => {
                return out;
            }
        }
    }
    panic!("cursor never ends");
}

fn hit(address: &str, name: &str) -> (String, String) {
    (address.to_owned(), name.to_owned())
}

#[test]
fn profile_search_finds_shown_names_of_artists_and_collectors() {
    let (mut store, db) = names_store("profiles_search_names");
    sql(
        &db,
        &format!("INSERT INTO holder_counts VALUES ('{QUADRADO}', 3, 1)"),
    );
    // A named address that is no artist (a collector), found by a name prefix in any case.
    let page = store.profiles_search("QUADR", None, 10).unwrap();
    assert_eq!(page.total, 1);
    let q = &page.items[0];
    assert_eq!(
        (q.address.as_str(), q.name.as_str()),
        (QUADRADO, "quadrado")
    );
    assert_eq!(q.kind, AddressKind::Account);
    let profile = store.profile(QUADRADO).unwrap();
    assert_eq!((q.project_count, q.held, profile.held), (0, 3, 3));
    // An artist.
    let keti = &store.profiles_search("keti", None, 10).unwrap().items[0];
    assert_eq!(keti.address, KETI);
    assert_eq!(
        keti.project_count,
        store.profile(KETI).unwrap().project_count
    );
    assert!(keti.project_count > 0);
    // A name several addresses carry is found plain first, then numbered.
    let both = vec![
        hit(MINT, "Mint Eastwood"),
        hit(MINT_TWIN, "mint EASTWOOD-2"),
    ];
    assert_eq!(found(&store, "eastwood"), both);
    assert_eq!(found(&store, "mint"), both);
    // A claimed rename is found by the new name only.
    objkt(&db, ED_A.0, Some("Old Alias"), None, "[]");
    store.rebuild_search_names().unwrap();
    assert_eq!(found(&store, "alias"), vec![hit(ED_A.0, "Old Alias")]);
    claim(&mut store, ED_A, "Fresh Name").unwrap();
    assert!(found(&store, "alias").is_empty());
    assert_eq!(found(&store, "fresh name"), vec![hit(ED_A.0, "Fresh Name")]);
    // A name that becomes shared leaves the index at the next writer's drain, until numbered.
    objkt(&db, STRANGER, Some("QUADRADO"), None, "[]");
    store
        .save_profile_edit(
            ED_B.0,
            ProfileEdit {
                bio: Some("hello".into()),
                ..ProfileEdit::default()
            },
            proof(ED_B),
        )
        .unwrap();
    assert!(found(&store, "quadrado").is_empty());
    store.assign_name_numbers().unwrap();
    assert_eq!(found(&store, "quadrado").len(), 2);
    // The drained index equals a full rebuild.
    let before: Vec<_> = ["fresh", "keti", "bill", "honest", "pixel"]
        .iter()
        .map(|t| found(&store, t))
        .collect();
    store.rebuild_search_names().unwrap();
    let after: Vec<_> = ["fresh", "keti", "bill", "honest", "pixel"]
        .iter()
        .map(|t| found(&store, t))
        .collect();
    assert_eq!(before, after);
}

#[test]
fn profile_search_by_address_and_cursor_binding() {
    let (mut store, _db) = names_store("profiles_search_address");
    // Registers the escrow and burn addresses, as sync and upgrade do.
    seeded_archive::ingest::upgrade::upgrade(&mut store).unwrap();
    let page = store.profiles_search(QUADRADO, None, 5).unwrap();
    assert_eq!((page.total, page.next.is_none()), (1, true));
    assert_eq!(page.items[0].name, "quadrado");
    // An address typed exactly is found even without a valid name, labelled by itself.
    let page = store
        .profiles_search(&format!(" {MINT} "), None, 5)
        .unwrap();
    assert_eq!(page.items[0].name, "Mint Eastwood");
    let contract = &store.profiles_search(PARAMS, None, 5).unwrap().items[0];
    assert_eq!(contract.kind, AddressKind::Contract);
    // Escrow and burn addresses are never found, by address or by their names.
    for text in [ESCROW_V2, BURN, "fxhash marketplace", "burn"] {
        let page = store.profiles_search(text, None, 5).unwrap();
        assert!(page.items.is_empty() && page.total == 0, "{text}");
    }
    // Ordered by name, case-insensitively; the cursor resumes only its own text.
    let first = store.profiles_search("bill", None, 1).unwrap();
    assert_eq!(first.total, 2);
    assert_eq!(first.items[0].name, "Billy McSmithers");
    let cursor = first.next.unwrap();
    let second = store.profiles_search("bill", Some(&cursor), 1).unwrap();
    assert_eq!(second.items[0].name, "Dr. Bill Kolomyjec");
    assert!(second.next.is_none());
    let bad = |r: Result<(), StoreError>| matches!(r, Err(StoreError::BadRequest(_)));
    assert!(bad(store
        .profiles_search("billy", Some(&cursor), 1)
        .map(|_| ())));
    assert!(bad(store
        .profiles_search(QUADRADO, Some(&cursor), 1)
        .map(|_| ())));
    let projects = ProjectFilter {
        text: None,
        artist: None,
        sort: ProjectSort::IdAsc,
    };
    assert!(bad(store
        .projects_page(&projects, Some(&cursor), 1)
        .map(|_| ())));
    let project_cursor = store
        .projects_page(&projects, None, 1)
        .unwrap()
        .next
        .unwrap();
    assert!(bad(store
        .profiles_search("bill", Some(&project_cursor), 1)
        .map(|_| ())));
    // Limits, the term cap and length follow the project search.
    for limit in [0, 101] {
        assert!(bad(store.profiles_search("bill", None, limit).map(|_| ())));
    }
    assert!(bad(store
        .profiles_search("a b c d e f g h i", None, 5)
        .map(|_| ())));
    assert!(bad(store
        .profiles_search(&"x".repeat(201), None, 5)
        .map(|_| ())));
    for empty in ["", "   ", "!!"] {
        let page = store.profiles_search(empty, None, 5).unwrap();
        assert!(page.items.is_empty() && page.total == 0, "{empty:?}");
    }
}

#[test]
fn avatars_follow_profile_precedence_in_one_batch() {
    let (mut store, db) = names_store("profiles_avatars");
    seeded_archive::ingest::upgrade::upgrade(&mut store).unwrap();
    let imported = |address: &str, source: &str, avatar: &str| {
        Connection::open(&db)
            .unwrap()
            .execute(
                "INSERT INTO profiles_imported (address, source, avatar_uri, fetched_at) \
                 VALUES (?1, ?2, ?3, 'now') ON CONFLICT (address, source) \
                 DO UPDATE SET avatar_uri = excluded.avatar_uri",
                params![address, source, avatar],
            )
            .unwrap();
    };
    // KETI: the fxhash artist avatar wins over an objkt logo.
    imported(KETI, "objkt", "https://objkt.example/keti");
    // ED_A: objkt only, trimmed. ED_B: a blank fxhash avatar falls through to objkt.
    imported(ED_A.0, "objkt", " https://objkt.example/a ");
    imported(ED_B.0, "fxhash", "  ");
    imported(ED_B.0, "objkt", "https://objkt.example/b");
    // Escrow and burn addresses get none, whatever is stored.
    imported(ESCROW_V2, "objkt", "https://objkt.example/escrow");
    imported(BURN, "objkt", "https://objkt.example/burn");
    let asked = [
        KETI,
        ED_A.0,
        ED_B.0,
        ESCROW_V2,
        BURN,
        STRANGER,
        "not an address",
        KETI,
    ];
    let got = store.avatars(&asked).unwrap();
    assert_eq!(got.len(), 3, "{got:?}");
    for a in [KETI, ED_A.0, ED_B.0, STRANGER] {
        assert_eq!(
            got.get(a),
            store.profile(a).unwrap().avatar_uri.as_ref(),
            "{a}"
        );
    }
    assert!(got[KETI].starts_with("ipfs://"));
    assert_eq!(got[ED_A.0], "https://objkt.example/a");
    assert_eq!(got[ED_B.0], "https://objkt.example/b");
    assert!(store.avatars(&[]).unwrap().is_empty());
    assert_eq!(store.avatars(&vec![KETI; 500]).unwrap().len(), 1);
    assert!(matches!(
        store.avatars(&vec![KETI; 501]),
        Err(StoreError::BadRequest(_))
    ));
}

#[test]
fn collectors_search_leaves_out_visible_artists() {
    let (store, db) = names_store("collectors_search");
    sql(
        &db,
        &format!("INSERT INTO holder_counts VALUES ('{QUADRADO}', 3, 1)"),
    );
    assert_eq!(
        store.profiles_search("keti", None, 10).unwrap().items[0].address,
        KETI
    );
    let collectors = store.collectors_search("keti", None, 10).unwrap();
    assert!(collectors.items.iter().all(|hit| hit.address != KETI));
    assert!(
        store
            .collectors_search(KETI, None, 10)
            .unwrap()
            .items
            .is_empty()
    );
    let found = store.collectors_search("QUADR", None, 10).unwrap();
    assert_eq!(found.items[0].address, QUADRADO);
    // A profiles cursor never resumes the collectors listing.
    let first = store.profiles_search("bill", None, 1).unwrap();
    assert!(
        store
            .collectors_search("bill", first.next.as_ref(), 1)
            .is_err()
    );
}

#[test]
fn existing_write_open_never_creates_or_migrates_and_names_the_reason() {
    let (store, db) = names_store("open_existing_write");
    drop(store);
    let reason = |path: &std::path::Path| match Store::open_existing_write(path) {
        Ok(_) => String::new(),
        Err(e) => e.to_string(),
    };
    // A current database opens, and stays in WAL mode.
    drop(Store::open_existing_write(&db).unwrap());
    let mode: String = rusqlite::Connection::open(&db)
        .unwrap()
        .query_row("PRAGMA journal_mode", [], |r| r.get(0))
        .unwrap();
    assert_eq!(mode, "wal");
    // A missing file is not created.
    let missing = db.with_file_name("open_existing_write_missing.sqlite");
    assert!(Store::open_existing_write(&missing).is_err());
    assert!(!missing.exists());
    // An uninitialised file and another schema are refused with their reason, unchanged.
    let blank = db.with_file_name("open_existing_write_blank.sqlite");
    rusqlite::Connection::open(&blank).unwrap();
    assert!(reason(&blank).contains("not initialised"));
    sql(&db, "PRAGMA user_version = 8");
    assert!(reason(&db).contains("schema version 8"));
    let version: i64 = rusqlite::Connection::open(&db)
        .unwrap()
        .query_row("PRAGMA user_version", [], |r| r.get(0))
        .unwrap();
    assert_eq!(version, 8);
    let _ = std::fs::remove_file(blank);
}
