//! Artist names in search: catalogue, artist directory and holdings searches find artists by
//! the name pages show (claimed > curated > fxhash > objkt), never by an imported name that is
//! hidden or invalid, and every writer keeps the indexes in step in its own transaction.

mod ingest_support;

use std::path::{Path, PathBuf};

use ingest_support::*;
use rusqlite::{Connection, params};
use seeded_archive::ingest::frank::bootstrap;
use seeded_archive::ingest::identity;
use seeded_archive::model::{EditProof, ProfileEdit};
use seeded_archive::store::{
    ArtistQuery, Cursor, ListingFilter, PageRequest, ProjectFilter, ProjectQuery, ProjectSort,
    Store, StoreError,
};

/// ciphrd's revealed key (TzKT, 2026-10-05): the artist who claims names here.
const ED_A: (&str, &str) = (
    "tz1PoDdN2oyRyF6DA73zTWAWYhNL4UGr3Egj",
    "edpkukC966pBdnKAYgkRsc7KgxYJwzvj5wwLVFV5t8ozjhyjiyFWEj",
);
const KEEPER: &str = "tz1gL9H53BzjDqmMi2JkKALeCzT7UAoopzEL";
const MIRROR: &str = "tz1Kq8NtEjSXkpswLmgod1tPFPVH2TZc8dwM";
/// Carries MIRROR's imported name in other letter case, so neither shows it.
const MIRROR_TWIN: &str = "tz1TEL6xFJx1hJKukx21NCufAfGEEr9yYoKX";
const HOBBS: &str = "tz1WMa1cAKk4JjFGZCpXNkTDi5o5AqCsFF6E";
const STRANGER: &str = "tz1VGZPvqytFVAoSAsM8VrUveJdL3PpVy6K9";
const ZANCAN: &str = "tz1VE1gFxMZ5TVCwWaYpXGANcfhyjgiUGkpg";
/// Holds one token of every project.
const HOLDER: &str = QUADRADO;
const LIET: &str = "tz1igEhzCfn34qnwJmKBQb24pnDAAYTYux3m";

/// Imported artist names (fxhash snapshot and registry).
const ARTISTS: [(&str, &str); 4] = [
    (ED_A.0, "Quiet Maker"),
    (KEEPER, KEEPER),
    (MIRROR, "Mirror Hand"),
    (HOBBS, "Tyler Hobbs"),
];
/// (id, title, credited addresses). Tokens alternate Palette Gold / Red by project id.
const PROJECTS: [(u64, &str, &[&str]); 5] = [
    (1, "Lantern Fields", &[ED_A.0]),
    (2, "Arbor Study", &[KEEPER]),
    (3, "Glass Orchard", &[MIRROR]),
    (4, "Fidenza Study", &[HOBBS]),
    (5, "Duet Lines", &[HOBBS, ED_A.0]),
];

/// A store whose rows are written around it (raw SQL) and then indexed once, as bootstrap does.
fn build(
    name: &str,
    artists: &[(&str, &str)],
    projects: &[(u64, &str, &[&str])],
    users: &[(&str, &str)],
) -> (Store, PathBuf) {
    let dir = test_dir(name);
    let db = dir.join("db.sqlite");
    drop(Store::open(&db).unwrap());
    let c = Connection::open(&db).unwrap();
    for (address, name) in artists {
        c.execute(
            "INSERT INTO artists (id, name, name_key, address) VALUES (?1, ?2, lower(?2), ?1)",
            params![address, name],
        )
        .unwrap();
        if address != name {
            c.execute(
                "INSERT INTO users (address, name, name_key) VALUES (?1, ?2, lower(?2))",
                params![address, name],
            )
            .unwrap();
        }
    }
    for (address, name) in users {
        c.execute(
            "INSERT INTO users (address, name, name_key) VALUES (?1, ?2, lower(?2))",
            params![address, name],
        )
        .unwrap();
    }
    for (id, title, credits) in projects {
        c.execute(
            "INSERT INTO projects (id, title, title_key, artist_key, index_state, editions, \
             contract) VALUES (?1, ?2, lower(?2), 'x', 'indexed', 1, ?3)",
            params![*id as i64, title, V2],
        )
        .unwrap();
        for (i, a) in credits.iter().enumerate() {
            c.execute(
                "INSERT INTO project_artists (project_id, position, artist_id) VALUES (?1, ?2, ?3)",
                params![*id as i64, i as i64, a],
            )
            .unwrap();
        }
        let palette = if id % 2 == 1 { "Gold" } else { "Red" };
        c.execute(
            "INSERT INTO tokens (contract, token_id, project_id, iteration, seed, minter, minted, \
             artifact_uri, source, attributes, owner_address, owner_as_of, owner_source, \
             holder_address) VALUES (?1, ?2, ?3, 1, 'oo', ?4, '2023-01-01', 'ipfs://x', 'tzkt', \
             ?5, ?4, 'now', 'tzkt', ?4)",
            params![
                V2,
                (*id * 10 + 1) as i64,
                *id as i64,
                HOLDER,
                format!(r#"[["Palette","{palette}"]]"#)
            ],
        )
        .unwrap();
    }
    drop(c);
    let mut store = Store::open(&db).unwrap();
    store.rebuild_search_names().unwrap();
    (store, db)
}

fn standard(name: &str) -> (Store, PathBuf) {
    build(name, &ARTISTS, &PROJECTS, &[(MIRROR_TWIN, "MIRROR hand")])
}

fn sql(db: &Path, statement: &str) {
    Connection::open(db)
        .unwrap()
        .execute_batch(statement)
        .unwrap();
}

fn edit(store: &mut Store, name: Option<&str>, bio: Option<&str>) -> Result<(), StoreError> {
    store.save_profile_edit(
        ED_A.0,
        ProfileEdit {
            name: name.map(str::to_owned),
            bio: bio.map(str::to_owned),
            links: vec![],
        },
        EditProof {
            message: format!("seeded.art profile edit for {}", ED_A.0),
            signature: "edsigtJ5aJVPNULkSdpVVuJ1FWdUNEH9JLQtCQRd4KfHd8TAs5S6Ybx".into(),
            public_key: ED_A.1.into(),
        },
    )
}

/// Artist ids matching `text`, walked two at a time; the offset listing agrees.
fn artists(store: &Store, text: &str) -> Vec<String> {
    let mut ids = Vec::new();
    let mut after: Option<Cursor> = None;
    loop {
        let page = store.artists_page(Some(text), after.as_ref(), 2).unwrap();
        ids.extend(page.items.iter().map(|a| a.id.clone()));
        match page.next {
            Some(c) => after = Some(c),
            None => break,
        }
    }
    let offset = store
        .artists(&ArtistQuery {
            text: Some(text.into()),
            page: PageRequest {
                offset: 0,
                limit: 100,
            },
        })
        .unwrap();
    assert_eq!(offset.total as usize, ids.len(), "{text}");
    assert_eq!(
        offset
            .items
            .iter()
            .map(|a| a.id.clone())
            .collect::<Vec<_>>(),
        ids
    );
    ids
}

/// Catalogue project ids matching `text`, in id order; the offset listing agrees.
fn catalogue(store: &Store, text: &str) -> Vec<u64> {
    let filter = ProjectFilter {
        text: Some(text.into()),
        artist: None,
        sort: ProjectSort::IdAsc,
    };
    let page = store.projects_page(&filter, None, 100).unwrap();
    let ids: Vec<u64> = page.items.iter().map(|p| p.id).collect();
    assert_eq!(page.total as usize, ids.len());
    let offset = store
        .projects(&ProjectQuery {
            text: Some(text.into()),
            artist: None,
            sort: ProjectSort::IdAsc,
            page: PageRequest {
                offset: 0,
                limit: 100,
            },
        })
        .unwrap();
    assert_eq!(offset.items.iter().map(|p| p.id).collect::<Vec<_>>(), ids);
    ids
}

/// Projects of HOLDER's tokens matching `text`.
fn holdings(store: &Store, text: &str) -> Vec<u64> {
    let filter = ListingFilter {
        text: Some(text),
        trait_filter: None,
    };
    let page = store
        .collector_holdings_search(HOLDER, &filter, None, 100)
        .unwrap();
    assert_eq!(page.total as usize, page.items.len());
    page.items.iter().map(|t| t.project_id).collect()
}

/// Both indexes hold exactly the shown names (`display_names`, or the address when none), and
/// nothing is left queued.
fn assert_indexes_follow_shown_names(store: &Store, db: &Path) {
    let c = Connection::open(db).unwrap();
    let artists: Vec<(i64, String, String)> = c
        .prepare(
            "SELECT a.seq, a.address, f.name FROM artists a JOIN artist_fts f ON f.rowid = a.seq",
        )
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    let total: i64 = c
        .query_row("SELECT count(*) FROM artists", [], |r| r.get(0))
        .unwrap();
    assert_eq!(artists.len() as i64, total, "every artist is indexed");
    let addresses: Vec<&str> = artists.iter().map(|a| a.1.as_str()).collect();
    let names = store.display_names(&addresses).unwrap();
    let label = |a: &str| names.get(a).cloned().unwrap_or_else(|| a.to_owned());
    for (_, address, indexed) in &artists {
        assert_eq!(indexed, &label(address), "artist {address}");
    }
    let projects: Vec<(i64, String)> = c
        .prepare("SELECT rowid, artists FROM project_fts ORDER BY rowid")
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    let total: i64 = c
        .query_row("SELECT count(*) FROM projects", [], |r| r.get(0))
        .unwrap();
    assert_eq!(projects.len() as i64, total, "every project is indexed");
    for (id, indexed) in projects {
        let credited: Vec<String> = c
            .prepare(
                "SELECT a.address FROM project_artists pa JOIN artists a ON a.id = pa.artist_id \
                 WHERE pa.project_id = ?1 ORDER BY pa.position",
            )
            .unwrap()
            .query_map([id], |r| r.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        let want: Vec<String> = credited.iter().map(|a| label(a)).collect();
        assert_eq!(indexed, want.join(" "), "project {id}");
    }
    let queued: i64 = c
        .query_row("SELECT count(*) FROM search_dirty", [], |r| r.get(0))
        .unwrap();
    assert_eq!(queued, 0);
}

const NONE: Vec<u64> = Vec::new();

#[test]
fn a_claimed_name_is_found_by_the_new_name_and_not_the_imported_one() {
    let (mut store, db) = standard("name_search_claim");
    assert_eq!(artists(&store, "quiet maker"), vec![ED_A.0]);
    assert_eq!(catalogue(&store, "quiet"), vec![1, 5]);
    assert_eq!(holdings(&store, "QUIET maker"), vec![1, 5]);
    assert_indexes_follow_shown_names(&store, &db);

    edit(&mut store, Some("Seeded One"), None).unwrap();
    assert_eq!(artists(&store, "seeded one"), vec![ED_A.0]);
    assert_eq!(catalogue(&store, "seeded"), vec![1, 5]);
    assert_eq!(
        catalogue(&store, "seeded hobbs"),
        vec![5],
        "co-credits combine"
    );
    assert_eq!(holdings(&store, "Seeded One"), vec![1, 5]);
    for old in ["quiet", "Quiet Maker", "maker"] {
        assert!(artists(&store, old).is_empty(), "{old}");
        assert_eq!(catalogue(&store, old), NONE, "{old}");
        assert_eq!(holdings(&store, old), NONE, "{old}");
    }
    // Titles and the address still find it.
    assert_eq!(catalogue(&store, "lantern"), vec![1]);
    assert_eq!(holdings(&store, "lantern fields"), vec![1]);
    assert_eq!(artists(&store, ED_A.0), vec![ED_A.0]);
    assert_indexes_follow_shown_names(&store, &db);

    // A second claim replaces the first; dropping the name shows the imported one again.
    edit(&mut store, Some("Second Name"), None).unwrap();
    assert_eq!(catalogue(&store, "seeded"), NONE);
    assert_eq!(catalogue(&store, "second name"), vec![1, 5]);
    edit(&mut store, None, Some("bio only")).unwrap();
    assert_eq!(catalogue(&store, "second"), NONE);
    assert_eq!(catalogue(&store, "quiet maker"), vec![1, 5]);
    assert_eq!(artists(&store, "quiet"), vec![ED_A.0]);
    assert_indexes_follow_shown_names(&store, &db);
}

#[test]
fn a_curated_identity_is_found_by_its_shown_name() {
    let (mut store, db) = standard("name_search_curated");
    // Unnamed: the address is what pages show and what is indexed.
    assert!(artists(&store, "emptyname").is_empty());
    assert_eq!(catalogue(&store, KEEPER), vec![2]);
    let report = identity::apply_to(&mut store).unwrap();
    assert_eq!(report.artists, 1);
    for text in ["EmptyName", "emptyname"] {
        assert_eq!(artists(&store, text), vec![KEEPER], "{text}");
        assert_eq!(catalogue(&store, text), vec![2], "{text}");
        assert_eq!(holdings(&store, text), vec![2], "{text}");
    }
    assert_eq!(catalogue(&store, "arbor"), vec![2]);
    assert_indexes_follow_shown_names(&store, &db);
    // Applying again changes nothing.
    identity::apply_to(&mut store).unwrap();
    assert_indexes_follow_shown_names(&store, &db);
}

#[test]
fn an_imported_name_shared_with_another_address_is_found_nowhere() {
    let (mut store, db) = standard("name_search_shared");
    for text in ["mirror", "Mirror Hand", "HAND"] {
        assert!(artists(&store, text).is_empty(), "{text}");
        assert_eq!(catalogue(&store, text), NONE, "{text}");
        assert_eq!(holdings(&store, text), NONE, "{text}");
    }
    assert_eq!(catalogue(&store, "glass orchard"), vec![3]);
    assert_eq!(holdings(&store, "glass"), vec![3]);
    assert_eq!(artists(&store, MIRROR), vec![MIRROR]);
    assert_eq!(catalogue(&store, MIRROR), vec![3]);
    assert_indexes_follow_shown_names(&store, &db);

    // A write around the store is queued by the triggers and taken by the next writer.
    sql(
        &db,
        &format!("DELETE FROM users WHERE address = '{MIRROR_TWIN}'"),
    );
    assert_eq!(catalogue(&store, "mirror"), NONE, "not drained yet");
    edit(&mut store, None, Some("bio only")).unwrap();
    assert_eq!(catalogue(&store, "mirror hand"), vec![3]);
    assert_eq!(artists(&store, "mirror"), vec![MIRROR]);
    assert_indexes_follow_shown_names(&store, &db);
    // An objkt alias of another address invalidates the name again (objkt-duplicate rule).
    sql(
        &db,
        &format!(
            "INSERT INTO profiles_imported (address, source, name, name_key, fetched_at) \
             VALUES ('{STRANGER}', 'objkt', 'mirror HAND', 'mirror hand', 'now')"
        ),
    );
    store.rebuild_search_names().unwrap();
    assert_eq!(catalogue(&store, "mirror"), NONE);
    assert_eq!(holdings(&store, "mirror"), NONE);
    assert_indexes_follow_shown_names(&store, &db);
}

#[test]
fn title_and_trait_searches_are_unchanged() {
    let (mut store, db) = standard("name_search_titles");
    edit(&mut store, Some("Seeded One"), None).unwrap();
    assert_eq!(catalogue(&store, "fidenza"), vec![4]);
    assert_eq!(catalogue(&store, "study"), vec![2, 4]);
    assert_eq!(catalogue(&store, "STUDY fidenza"), vec![4]);
    assert_eq!(catalogue(&store, "tyler"), vec![4, 5]);
    assert_eq!(holdings(&store, "duet"), vec![5]);
    let gold = |text: Option<&str>| -> Vec<u64> {
        let filter = ListingFilter {
            text,
            trait_filter: Some(("Palette", "Gold")),
        };
        store
            .collector_holdings_search(HOLDER, &filter, None, 100)
            .unwrap()
            .items
            .iter()
            .map(|t| t.project_id)
            .collect()
    };
    assert_eq!(gold(None), vec![1, 3, 5]);
    assert_eq!(gold(Some("seeded")), vec![1, 5]);
    assert_eq!(gold(Some("tyler")), vec![5]);
    // Escaping and the term cap are unchanged.
    for literal in ["lantern OR glass", "NEAR(lantern glass)"] {
        assert_eq!(catalogue(&store, literal), NONE, "{literal}");
    }
    for quoted in ["\"lantern", "lantern*)", "(lantern)"] {
        assert_eq!(catalogue(&store, quoted), vec![1], "{quoted}");
    }
    let nine = "a b c d e f g h i";
    assert!(matches!(
        store.artists_page(Some(nine), None, 10),
        Err(StoreError::BadRequest(_))
    ));
    let filter = ProjectFilter {
        text: Some(nine.into()),
        artist: None,
        sort: ProjectSort::IdAsc,
    };
    assert!(matches!(
        store.projects_page(&filter, None, 10),
        Err(StoreError::BadRequest(_))
    ));
    let text = ListingFilter {
        text: Some(nine),
        trait_filter: None,
    };
    assert!(matches!(
        store.collector_holdings_search(HOLDER, &text, None, 10),
        Err(StoreError::BadRequest(_))
    ));
    assert_indexes_follow_shown_names(&store, &db);
}

#[test]
fn cursors_across_a_rename_stay_valid_and_bound_to_their_filter() {
    let studio = [
        (STRANGER, "Studio Alpha"),
        (ZANCAN, "Studio Beta"),
        (MIRROR, "Studio Delta"),
        (ED_A.0, "Studio Quiet"),
    ];
    let projects: [(u64, &str, &[&str]); 4] = [
        (11, "One", &[STRANGER]),
        (12, "Two", &[ZANCAN]),
        (13, "Three", &[MIRROR]),
        (14, "Four", &[ED_A.0]),
    ];
    let (mut store, db) = build("name_search_cursor", &studio, &projects, &[]);
    let first_artists = store.artists_page(Some("studio"), None, 2).unwrap();
    assert_eq!(
        first_artists
            .items
            .iter()
            .map(|a| a.id.as_str())
            .collect::<Vec<_>>(),
        vec![STRANGER, ZANCAN]
    );
    let filter = ProjectFilter {
        text: Some("studio".into()),
        artist: None,
        sort: ProjectSort::IdAsc,
    };
    let first_projects = store.projects_page(&filter, None, 2).unwrap();
    assert_eq!(first_projects.total, 4);
    let text = ListingFilter {
        text: Some("studio"),
        trait_filter: None,
    };
    let first_holdings = store
        .collector_holdings_search(HOLDER, &text, None, 2)
        .unwrap();
    // The last artist renames itself out of the listings between pages.
    edit(&mut store, Some("Seeded One"), None).unwrap();
    let rest = store
        .artists_page(Some("studio"), first_artists.next.as_ref(), 2)
        .unwrap();
    assert_eq!(
        rest.items.iter().map(|a| a.id.as_str()).collect::<Vec<_>>(),
        vec![MIRROR]
    );
    assert_eq!((rest.next, rest.total), (None, 3));
    let rest = store
        .projects_page(&filter, first_projects.next.as_ref(), 2)
        .unwrap();
    assert_eq!(
        rest.items.iter().map(|p| p.id).collect::<Vec<_>>(),
        vec![13]
    );
    assert_eq!((rest.next, rest.total), (None, 3));
    let rest = store
        .collector_holdings_search(HOLDER, &text, first_holdings.next.as_ref(), 2)
        .unwrap();
    assert_eq!(
        rest.items.iter().map(|t| t.project_id).collect::<Vec<_>>(),
        vec![13]
    );
    assert_eq!((rest.next, rest.total), (None, 3));
    // A cursor never resumes another filter, renamed or not.
    let seeded = store.artists_page(Some("seeded"), first_artists.next.as_ref(), 2);
    assert!(matches!(seeded, Err(StoreError::BadRequest(_))));
    let other = ProjectFilter {
        text: Some("seeded".into()),
        ..filter.clone()
    };
    assert!(matches!(
        store.projects_page(&other, first_projects.next.as_ref(), 2),
        Err(StoreError::BadRequest(_))
    ));
    let other = ListingFilter {
        text: Some("seeded"),
        trait_filter: None,
    };
    assert!(matches!(
        store.collector_holdings_search(HOLDER, &other, first_holdings.next.as_ref(), 2),
        Err(StoreError::BadRequest(_))
    ));
    assert_eq!(catalogue(&store, "seeded"), vec![14]);
    assert_indexes_follow_shown_names(&store, &db);
}

#[test]
fn bootstrap_indexes_shown_names_only() {
    let dir = test_dir("name_search_bootstrap");
    let frank = frank_copy(&dir);
    edit_json(&frank.join("public/data/users.json"), |v| {
        v["names"][STRANGER] = "LIET".into();
    });
    let db = dir.join("db.sqlite");
    let mut store = Store::open(&db).unwrap();
    assert!(bootstrap(&mut store, &frank).unwrap().changed);
    // "liet" is shared with a stranger: the credited artist keeps the plain name, the
    // stranger shows "LIET-2".
    assert_eq!(
        store.profile(STRANGER).unwrap().name.as_deref(),
        Some("LIET-2")
    );
    assert_eq!(catalogue(&store, "liet"), vec![3245]);
    assert_eq!(artists(&store, "liet"), vec![LIET]);
    assert_eq!(catalogue(&store, "avoid"), vec![3245]);
    assert_eq!(artists(&store, LIET), vec![LIET]);
    // The renamed artist is found by the name it asked for, not the registry name.
    assert_eq!(catalogue(&store, "plasma renamed"), vec![382]);
    assert_eq!(catalogue(&store, "rjart"), NONE);
    // The curated identity is found by its name.
    assert_eq!(catalogue(&store, "emptyname"), vec![ARBOREAL]);
    assert_eq!(artists(&store, "EmptyName"), vec![KEEPER]);
    assert_indexes_follow_shown_names(&store, &db);
    // A changed archive without the clash is re-imported: the name stays found, plain.
    edit_json(&frank.join("public/data/users.json"), |v| {
        v["names"]
            .as_object_mut()
            .unwrap()
            .remove(STRANGER)
            .unwrap();
    });
    assert!(bootstrap(&mut store, &frank).unwrap().changed);
    assert_eq!(store.profile(STRANGER).unwrap().name, None);
    assert_eq!(catalogue(&store, "liet"), vec![3245]);
    assert_eq!(artists(&store, "liet"), vec![LIET]);
    assert_indexes_follow_shown_names(&store, &db);
}

/// Median and worst of `runs` timings of `f`, in milliseconds.
fn timed(runs: usize, mut f: impl FnMut()) -> (f64, f64) {
    let mut ms: Vec<f64> = (0..runs)
        .map(|_| {
            let t = std::time::Instant::now();
            f();
            t.elapsed().as_secs_f64() * 1000.0
        })
        .collect();
    ms.sort_by(f64::total_cmp);
    (ms[runs / 2], ms[runs - 1])
}

/// Migration, rebuild, backlog drain and search timings on a copy of a production database.
/// Never point it at `var/`: it migrates and writes the copy.
#[test]
#[ignore = "probe: set SEEDED_SEARCH_PROBE_DB to a copy of a production database"]
fn probe_migration_and_search_timings_on_a_copy() {
    let path = PathBuf::from(std::env::var("SEEDED_SEARCH_PROBE_DB").unwrap());
    assert!(!path.starts_with(root().join("var")), "never var/");
    let pages = |p: &Path| -> i64 {
        let c = Connection::open(p).unwrap();
        let count: i64 = c.query_row("PRAGMA page_count", [], |r| r.get(0)).unwrap();
        let size: i64 = c.query_row("PRAGMA page_size", [], |r| r.get(0)).unwrap();
        count * size
    };
    let before = pages(&path);
    let t = std::time::Instant::now();
    let mut store = Store::open(&path).unwrap();
    println!("open + migrate: {:.2} s", t.elapsed().as_secs_f64());
    println!(
        "growth: {:.1} MiB",
        (pages(&path) - before) as f64 / 1048576.0
    );
    let c = Connection::open(&path).unwrap();
    let one = |sql: &str| -> i64 { c.query_row(sql, [], |r| r.get(0)).unwrap() };
    println!(
        "artists {}, labelled by address {}, projects {}",
        one("SELECT count(*) FROM artists"),
        one(
            "SELECT count(*) FROM artists a JOIN artist_fts f ON f.rowid = a.seq \
             WHERE f.name = a.address"
        ),
        one("SELECT count(*) FROM projects"),
    );
    let holder: String = c
        .query_row(
            "SELECT holder_address FROM holder_counts ORDER BY held DESC LIMIT 1",
            [],
            |r| r.get(0),
        )
        .unwrap();
    for text in [
        "zancan",
        "emptyname",
        "garden",
        "a",
        "tz1",
        "studio of the",
        "a b c d e f g h",
    ] {
        let (am, aw) = timed(21, || {
            store.artists_page(Some(text), None, 48).unwrap();
        });
        let filter = |sort| ProjectFilter {
            text: Some(text.into()),
            artist: None,
            sort,
        };
        let (pm, pw) = timed(21, || {
            store
                .projects_page(&filter(ProjectSort::IdDesc), None, 48)
                .unwrap();
        });
        let (tm, tw) = timed(21, || {
            store
                .projects_page(&filter(ProjectSort::TitleAsc), None, 48)
                .unwrap();
        });
        let listing = ListingFilter {
            text: Some(text),
            trait_filter: None,
        };
        let (hm, hw) = timed(21, || {
            store
                .collector_holdings_search(&holder, &listing, None, 48)
                .unwrap();
        });
        let hits = store
            .projects_page(&filter(ProjectSort::IdDesc), None, 1)
            .unwrap()
            .total;
        println!(
            "{text:?} ({hits} projects): artists {am:.1}/{aw:.1} ms, catalogue newest \
             {pm:.1}/{pw:.1} ms, by title {tm:.1}/{tw:.1} ms, holdings {hm:.1}/{hw:.1} ms \
             (median/worst)"
        );
    }
    let t = std::time::Instant::now();
    let n = store.rebuild_search_names().unwrap();
    println!("rebuild: {n} artists in {:.2} s", t.elapsed().as_secs_f64());
    // A full profiles pass queues every imported address and name; one drain takes them.
    let queued = c
        .execute(
            "INSERT OR IGNORE INTO search_dirty (value) \
             SELECT address FROM users UNION SELECT name_key FROM users WHERE name_key IS NOT NULL \
             UNION SELECT address FROM profiles_imported \
             UNION SELECT name_key FROM profiles_imported WHERE name_key IS NOT NULL",
            [],
        )
        .unwrap();
    let t = std::time::Instant::now();
    edit(&mut store, None, Some("probe")).unwrap();
    println!(
        "drain of {queued} queued values (bio-only edit): {:.2} s",
        t.elapsed().as_secs_f64()
    );
    let t = std::time::Instant::now();
    edit(&mut store, None, Some("probe again")).unwrap();
    println!(
        "edit with an empty queue: {:.1} ms",
        t.elapsed().as_secs_f64() * 1000.0
    );
}
