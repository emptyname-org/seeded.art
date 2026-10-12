//! Bootstrap from a trimmed copy of Frank Force's archive, and the v1 -> v2 migration.

mod ingest_support;

use std::fs;

use ingest_support::*;
use seeded_archive::import::import_fixtures;
use seeded_archive::ingest::frank::bootstrap;
use seeded_archive::model::{IndexState, Source, Verification};
use seeded_archive::store::{
    ArtistQuery, PageRequest, ProjectQuery, ProjectSort, Store, StoreError,
};

fn all() -> PageRequest {
    PageRequest {
        offset: 0,
        limit: 100,
    }
}

fn project_ids(store: &Store, text: Option<&str>) -> Vec<u64> {
    store
        .projects(&ProjectQuery {
            text: text.map(str::to_owned),
            artist: None,
            sort: ProjectSort::IdAsc,
            page: all(),
        })
        .unwrap()
        .items
        .iter()
        .map(|p| p.id)
        .collect()
}

#[test]
fn bootstrap_imports_projects_artists_users_and_catalogue_tokens() {
    let dir = test_dir("bootstrap_parse");
    let mut store = Store::open(&dir.join("db.sqlite")).unwrap();
    let r = bootstrap(&mut store, &frank()).unwrap();
    assert!(r.changed);
    assert_eq!(r.projects, 8);
    assert_eq!(r.hidden, 1);
    assert_eq!(r.users, 22);
    assert_eq!(r.collaborations, 2);
    assert_eq!(r.catalogue_tokens, 24);
    assert_eq!(
        r.without_seed, 3,
        "two absent tokens without seed records, one unsigned"
    );
    assert!(r.rejected.is_empty(), "{:?}", r.rejected);
    assert!(r.source.starts_with("KilledByAPixel/fxhashArchive"));

    let arboreal = store.project(ARBOREAL).unwrap();
    assert_eq!(arboreal.title, "Arboreal");
    assert_eq!(arboreal.source, Source::Frank);
    assert_eq!(arboreal.slug.as_deref(), Some("arboreal"));
    assert_eq!(arboreal.supply, Some(320));
    assert_eq!(arboreal.contract.as_deref(), Some(PARAMS));
    assert_eq!(arboreal.index_state, IndexState::NotIndexed);
    assert_eq!(arboreal.editions, None);
    assert!(
        arboreal
            .thumbnail_uri
            .as_deref()
            .unwrap()
            .starts_with("ipfs://")
    );
    assert!(
        arboreal
            .display_uri
            .as_deref()
            .unwrap()
            .starts_with("ipfs://")
    );
    assert!(
        arboreal
            .description
            .as_deref()
            .unwrap()
            .contains("Merleau-Ponty")
    );
    // A collaboration contract is credited to its members, named from users.json.
    let members: Vec<(&str, &str, Option<&str>)> = arboreal
        .artists
        .iter()
        .map(|a| (a.id.as_str(), a.name.as_str(), a.address.as_deref()))
        .collect();
    assert_eq!(
        members,
        vec![
            (
                "tz1exYBh5zHhf7LKyJSKAQqm89ZYZmTwsBz2",
                "Doppelganger",
                Some("tz1exYBh5zHhf7LKyJSKAQqm89ZYZmTwsBz2")
            ),
            (
                "tz1gL9H53BzjDqmMi2JkKALeCzT7UAoopzEL",
                "Emptyname",
                Some("tz1gL9H53BzjDqmMi2JkKALeCzT7UAoopzEL")
            ),
        ],
        "the curated identity names the keeper; the co-author stays credited"
    );
    // A rename the artist asked for wins over the registry name.
    let plasma = store.project(382).unwrap();
    assert_eq!(plasma.artists[0].name, "Plasma Renamed");
    let artist = store
        .artist_by_address("tz1WKNUBMpEDT9yXt3BMAFJXdx1YMCakUKh7")
        .unwrap();
    assert_eq!(artist.id, "tz1WKNUBMpEDT9yXt3BMAFJXdx1YMCakUKh7");
    assert_eq!(artist.address.as_deref(), Some(artist.id.as_str()));
    assert_eq!(project_ids(&store, Some("plasma renamed")), vec![382]);
    assert!(matches!(
        store.artist_by_address("not-an-address"),
        Err(StoreError::BadRequest(_))
    ));
    assert!(matches!(
        store.artist_by_address(QUADRADO),
        Err(StoreError::NotFound { .. })
    ));

    // A project without iterations stays catalogued and not indexed.
    let avoid = store.project(3245).unwrap();
    assert_eq!(avoid.contract, None);
    assert_eq!(avoid.index_state, IndexState::NotIndexed);
    // Bootstrap never materialises tokens.
    assert_eq!(store.tokens_for_project(ARBOREAL, all()).unwrap().total, 0);
    let snapshot = store.snapshot().unwrap();
    assert_eq!(snapshot.owner_source, "none");
    assert_eq!(snapshot.catalogue_source, r.source);
}

#[test]
fn flagged_projects_are_hidden_from_every_read() {
    let dir = test_dir("bootstrap_flagged");
    let store = bootstrapped(&dir);
    assert_eq!(
        project_ids(&store, None),
        vec![382, 3245, 11003, 11229, 15755, ARBOREAL, 29632]
    );
    assert!(project_ids(&store, Some("pipes")).is_empty());
    assert!(matches!(
        store.project(FLAGGED),
        Err(StoreError::NotFound { .. })
    ));
    assert!(matches!(
        store.tokens_for_project(FLAGGED, all()),
        Err(StoreError::NotFound { .. })
    ));
    assert!(matches!(
        store.feature_shares(FLAGGED),
        Err(StoreError::NotFound { .. })
    ));
    // The author of only flagged work is not listed, and has no visible projects.
    let flagged_author = "tz1VE1gFxMZ5TVCwWaYpXGANcfhyjgiUGkpg";
    let listed = store
        .artists(&ArtistQuery {
            text: None,
            page: all(),
        })
        .unwrap();
    assert!(listed.items.iter().all(|a| a.id != flagged_author));
    assert_eq!(listed.total as usize, listed.items.len());
    assert!(matches!(
        store.artist(flagged_author),
        Err(StoreError::NotFound { .. })
    ));
    assert!(matches!(
        store.artist_by_address(flagged_author),
        Err(StoreError::NotFound { .. })
    ));
    assert!(matches!(
        store.artist_projects(flagged_author, all()),
        Err(StoreError::NotFound { .. })
    ));
    let status = store.index_status().unwrap();
    assert_eq!(status.projects_total, 7);
    assert_eq!(status.projects_flagged_hidden, 1);
    assert_eq!(status.projects_indexed, 0);
}

#[test]
fn rerun_is_a_noop_and_changed_input_reimports_without_losing_verification() {
    let dir = test_dir("bootstrap_rerun");
    let db = dir.join("db.sqlite");
    let mut store = bootstrapped(&dir);
    let first = store.project(ARBOREAL).unwrap();
    let again = bootstrap(&mut store, &frank()).unwrap();
    assert!(!again.changed);
    assert_eq!(store.project(ARBOREAL).unwrap(), first);

    // Mark two catalogue rows verified, then change one of them in the source.
    let side = rusqlite::Connection::open(&db).unwrap();
    side.execute_batch(
        "UPDATE catalogue_tokens SET verify_state = 'match', chain_state = 'present' \
         WHERE token_id IN (133459, 133460)",
    )
    .unwrap();
    let copy = frank_copy(&dir);
    edit_json(&copy.join("public/data/seeds/2/0000.json"), |v| {
        assert_eq!(v["from"], 133459);
        v["seeds"][0] = "ooChangedSeed".into();
    });
    edit_json(&copy.join("public/data/tokens/index-000.json"), |v| {
        for p in v.as_array_mut().unwrap() {
            if p["id"] == ARBOREAL {
                p["name"] = "Arboreal Revised".into();
            }
        }
    });
    let r = bootstrap(&mut store, &copy).unwrap();
    assert!(r.changed);
    assert_ne!(r.content_sha256, again.content_sha256);
    assert_eq!(store.project(ARBOREAL).unwrap().title, "Arboreal Revised");
    assert_eq!(project_ids(&store, Some("revised")), vec![ARBOREAL]);
    let state = |id: i64| -> (String, Option<String>) {
        side.query_row(
            "SELECT verify_state, seed FROM catalogue_tokens WHERE token_id = ?1",
            [id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap()
    };
    assert_eq!(
        state(133459),
        ("unverified".into(), Some("ooChangedSeed".into()))
    );
    assert_eq!(
        state(133460).0,
        "match",
        "unchanged rows keep their verification"
    );
    let rows: i64 = side
        .query_row("SELECT count(*) FROM catalogue_tokens", [], |r| r.get(0))
        .unwrap();
    assert_eq!(rows, 24, "re-import upserts, never duplicates");
}

#[test]
fn bootstrap_failure_leaves_previous_state_intact() {
    let dir = test_dir("bootstrap_failure");
    let mut store = bootstrapped(&dir);
    let before = store.project(ARBOREAL).unwrap();
    let copy = frank_copy(&dir);
    edit_json(&copy.join("public/data/tokens/index-000.json"), |v| {
        for p in v.as_array_mut().unwrap() {
            if p["id"] == ARBOREAL {
                p["name"] = "Arboreal Broken".into();
            }
        }
    });
    fs::write(copy.join("public/data/seeds/2/0003.json"), b"{ not json").unwrap();
    assert!(bootstrap(&mut store, &copy).is_err());
    assert_eq!(store.project(ARBOREAL).unwrap(), before);
    assert!(!bootstrap(&mut store, &frank()).unwrap().changed);
}

#[test]
fn schema_v1_database_migrates_to_v2_in_place() {
    let dir = test_dir("migrate_v1");
    let db = dir.join("db.sqlite");
    {
        let c = rusqlite::Connection::open(&db).unwrap();
        c.execute_batch(&fs::read_to_string(fixtures().join("schema_v1.sql")).unwrap())
            .unwrap();
    }
    assert!(matches!(
        Store::open_read_only(&db),
        Err(StoreError::Internal(_))
    ));
    let store = Store::open(&db).unwrap();
    let project = store.project(2969).unwrap();
    assert_eq!(project.title, "Garden, Monoliths");
    assert_eq!(project.source, Source::Fixture);
    assert_eq!(project.supply, None);
    assert_eq!(
        project.artists[0].address, None,
        "fixture ids have no address"
    );
    let token = store.token(V1, 100_000).unwrap();
    assert_eq!(token.seed, "ooTestSeed");
    assert_eq!(token.features, vec![("Palette".into(), "Stone".into())]);
    assert_eq!(token.source, Source::Fixture);
    assert_eq!(token.verification, Verification::Unverified);
    assert_eq!(token.params, None);
    assert_eq!(
        token.owner.as_ref().unwrap().owner.address,
        "tz2RXosjAZdS85oCrguy7Eom757144dcU1FT"
    );
    assert_eq!(store.artist("zancan").unwrap().aliases, vec!["zancan"]);
    assert_eq!(
        store
            .collector("tz2RXosjAZdS85oCrguy7Eom757144dcU1FT")
            .unwrap()
            .token_count,
        1
    );
    drop(store);
    // Reopening does not migrate again, and read-only opens now work.
    let reopened = Store::open_read_only(&db).unwrap();
    assert_eq!(reopened.project(2969).unwrap(), project);
    let version: i64 = rusqlite::Connection::open(&db)
        .unwrap()
        .query_row("PRAGMA user_version", [], |r| r.get(0))
        .unwrap();
    assert_eq!(version, 10);
}

#[test]
fn fixture_import_refuses_to_replace_an_indexed_archive() {
    let dir = test_dir("bootstrap_then_fixtures");
    let mut store = bootstrapped(&dir);
    let before = store.project(382).unwrap();
    let err = import_fixtures(&mut store, &root().join("assets/fixtures")).unwrap_err();
    assert!(
        matches!(&err, seeded_archive::import::ImportError::Invalid { reason, .. }
            if reason.contains("refusing to replace an indexed archive")),
        "{err:?}"
    );
    assert_eq!(store.project(382).unwrap(), before, "nothing was wiped");
    assert_eq!(store.project(ARBOREAL).unwrap().source, Source::Frank);
    assert_eq!(store.index_status().unwrap().projects_flagged_hidden, 1);
}

#[test]
fn curated_identity_names_emptyname_and_keeps_every_coauthor() {
    let dir = test_dir("identity_emptyname");
    let store = bootstrapped(&dir);
    let keeper = "tz1gL9H53BzjDqmMi2JkKALeCzT7UAoopzEL";
    let artist = store.artist_by_address(keeper).unwrap();
    assert_eq!(artist.name, "Emptyname");
    assert_eq!(artist.website, None);
    assert_eq!(artist.aliases, vec!["EmptyName"]);
    // The fixture slug resolves to the same identity.
    assert_eq!(store.artist("emptyname").unwrap(), artist);
    assert_eq!(
        store.artist_projects("emptyname", all()).unwrap().items[0].id,
        ARBOREAL
    );
    // Search finds the artist and its project.
    let found = store
        .artists(&ArtistQuery {
            text: Some("emptyname".into()),
            page: all(),
        })
        .unwrap();
    assert_eq!(found.items.len(), 1);
    assert_eq!(found.items[0].id, keeper);
    assert_eq!(project_ids(&store, Some("EmptyName")), vec![ARBOREAL]);
    // Doppelganger is still credited first, unchanged.
    let credits: Vec<(String, String)> = store
        .project(ARBOREAL)
        .unwrap()
        .artists
        .into_iter()
        .map(|a| (a.id, a.name))
        .collect();
    assert_eq!(
        credits,
        vec![
            (
                "tz1exYBh5zHhf7LKyJSKAQqm89ZYZmTwsBz2".into(),
                "Doppelganger".into()
            ),
            (keeper.into(), "Emptyname".into()),
        ]
    );
    let c = rusqlite::Connection::open(dir.join("db.sqlite")).unwrap();
    let provenance: String = c
        .query_row(
            "SELECT provenance FROM identities WHERE address = ?1",
            [keeper],
            |r| r.get(0),
        )
        .unwrap();
    assert!(provenance.contains("objkt alias EmptyName"));
    // A website an earlier identity wrote is cleared when the identity no longer has one.
    c.execute(
        "UPDATE artists SET website = 'https://emptyname.org' WHERE id = ?1",
        [keeper],
    )
    .unwrap();
    drop(c);
    drop(store);
    let mut store = Store::open(&dir.join("db.sqlite")).unwrap();
    seeded_archive::ingest::identity::apply_to(&mut store).unwrap();
    assert_eq!(store.artist_by_address(keeper).unwrap().website, None);
    assert_eq!(store.profile(keeper).unwrap().website, None);
}
