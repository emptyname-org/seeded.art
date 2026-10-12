//! Author names the site displays, copied from the catalogue for search: claimed and curated
//! names, hidden shared or invalid names, runs without a catalogue, idempotent runs that write
//! only changes, batching, the schema 2 -> 3 migration, and query escaping. No network.

#[path = "articles_support/mod.rs"]
mod support;

use rusqlite::Connection;
use seeded_archive::articles::names::{NamesReport, open_catalogue};
use seeded_archive::articles::store::{
    ArticleStore, ArticleStoreError, MAX_QUERY_TERMS, SCHEMA_VERSION, fts_query,
};
use support::{
    Art, Harness, NameSource, art, name_catalogue, remove_db, remove_name, scratch, set_names,
    snapshot, synthetic, with_archive,
};

const ALICE: &str = "tz1fepn7jZsCYBqCDhpM63hzh9g2Ytqk4Tpv";
const BOB: &str = "tz1cNakLoctaqaCtBZSQGZs5RgAciYqB7cNf";
const CAROL: &str = "tz1TSdoeS5udkYmVr1cEpErTqxGVrC3Kcd2J";
const DAVE: &str = "tz1YqZRkYSzHaHL22CS7x5tyjC3PR6XHQPAA";
/// Not an author; shares a name with one.
const STRANGER: &str = "tz1Twz3EaEcAwkBkXV5YQwLNbLruErA5TV8k";
/// Fixture-sync author of article #1.
const RICHER: &str = "tz1cNakLoctaqaCtBZSQGZs5RgAciYqB7cNf";

/// Alice wrote #10 and #14.
fn arts() -> Vec<Art> {
    vec![
        art(10, ALICE, "first"),
        art(11, BOB, "second"),
        art(12, CAROL, "third"),
        art(13, DAVE, "fourth"),
        art(14, ALICE, "fifth"),
    ]
}

fn find(store: &ArticleStore, q: &str) -> Vec<u64> {
    store
        .articles_page(Some(q), None, 50)
        .unwrap()
        .items
        .iter()
        .map(|a| a.token_id)
        .collect()
}

fn author_name(store: &ArticleStore, id: u64) -> Option<String> {
    store.article(id).unwrap().summary.author_name
}

fn resolve(store: &mut ArticleStore, catalogue: &std::path::Path) -> NamesReport {
    store
        .resolve_names(&open_catalogue(catalogue).unwrap())
        .unwrap()
}

fn report(authors: u64, named: u64, changed: u64, reindexed: u64) -> NamesReport {
    NamesReport {
        authors,
        named,
        changed,
        reindexed,
    }
}

fn rows(c: &Connection, sql: &str) -> Vec<(i64, String)> {
    let mut stmt = c.prepare(sql).unwrap();
    stmt.query_map([], |r| {
        Ok((
            r.get::<_, Option<i64>>(0)?.unwrap_or(0),
            r.get::<_, String>(1)?,
        ))
    })
    .unwrap()
    .collect::<rusqlite::Result<_>>()
    .unwrap()
}

fn index_rows(c: &Connection) -> Vec<(i64, String)> {
    rows(c, "SELECT rowid, authors FROM articles_fts ORDER BY rowid")
}

fn name_rows(c: &Connection) -> Vec<(i64, String)> {
    rows(
        c,
        "SELECT NULL, address || '=' || name FROM names ORDER BY address",
    )
}

#[test]
fn claimed_and_curated_names_are_searchable_and_shown() {
    let mut store = synthetic(&arts(), &[], &[]);
    let cat = scratch("names-claimed");
    name_catalogue(
        &cat,
        &[
            (ALICE, NameSource::Registry, "alicereg"),
            (ALICE, NameSource::Claimed, "Alice Claims"),
            (BOB, NameSource::Objkt, "bobobjkt"),
            (BOB, NameSource::Curated, "Bob Curated"),
        ],
    );
    assert_eq!(resolve(&mut store, &cat), report(4, 2, 2, 3));
    assert_eq!(find(&store, "Claims"), vec![14, 10]);
    assert_eq!(
        find(&store, "alice clai"),
        vec![14, 10],
        "prefix terms, ANDed"
    );
    assert_eq!(find(&store, "Curated"), vec![11]);
    // Names the site does not display are not searchable.
    assert_eq!(find(&store, "alicereg"), Vec::<u64>::new());
    assert_eq!(find(&store, "bobobjkt"), Vec::<u64>::new());
    assert_eq!(find(&store, &ALICE[..12]), vec![14, 10], "address prefix");
    assert_eq!(author_name(&store, 10).as_deref(), Some("Alice Claims"));
    assert_eq!(author_name(&store, 11).as_deref(), Some("Bob Curated"));
    assert_eq!(author_name(&store, 12), None);
    assert_eq!(store.stats().unwrap().named_authors, 2);
    remove_db(&cat);
}

#[test]
fn shared_or_invalid_imported_names_are_not_searchable_but_the_address_is() {
    let mut store = synthetic(&arts(), &[], &[]);
    let cat = scratch("names-shared");
    name_catalogue(
        &cat,
        &[
            (CAROL, NameSource::Registry, "Twin"),
            // An address is never a name.
            (
                DAVE,
                NameSource::Registry,
                "KT1GtbuswcNMGhHF2TSuH1Yfaqn16do8Qtva",
            ),
        ],
    );
    assert_eq!(resolve(&mut store, &cat), report(4, 1, 1, 1));
    assert_eq!(find(&store, "twin"), vec![12]);
    assert_eq!(find(&store, "KT1Gtbusw"), Vec::<u64>::new());
    assert_eq!(find(&store, &DAVE[..12]), vec![13]);
    assert_eq!(author_name(&store, 13), None);

    // Another address carries the name (case and spacing differ): it is hidden on the site,
    // so the copy and the index lose it; the address stays searchable.
    set_names(&cat, &[(STRANGER, NameSource::Registry, " twin")]);
    assert_eq!(resolve(&mut store, &cat), report(4, 0, 1, 1));
    assert_eq!(find(&store, "twin"), Vec::<u64>::new());
    assert_eq!(find(&store, &CAROL[..12]), vec![12]);
    assert_eq!(author_name(&store, 12), None);
    assert_eq!(store.stats().unwrap().named_authors, 0);
    remove_db(&cat);
}

#[test]
fn runs_without_a_catalogue_keep_the_names() {
    let cat = scratch("names-keep");
    name_catalogue(&cat, &[(RICHER, NameSource::Claimed, "Richer Studio")]);
    let mut h = Harness::new();
    let r = h.sync(&with_archive(&cat)).unwrap();
    let names = r.names.unwrap();
    assert_eq!((names.named, names.changed, names.reindexed), (1, 1, 1));
    assert_eq!(find(&h.store, "studio"), vec![1]);

    // A sync without a catalogue re-derives the index from the copied names.
    let r = h.sync_default();
    assert_eq!(r.names, None);
    assert_eq!(find(&h.store, "studio"), vec![1]);
    assert_eq!(author_name(&h.store, 1).as_deref(), Some("Richer Studio"));
    assert_eq!(h.store.refresh_refs().unwrap(), 0);
    assert_eq!(find(&h.store, "studio"), vec![1]);

    // A catalogue at another schema fails before any network work and changes nothing.
    let old = scratch("names-keep-old");
    name_catalogue(&old, &[(RICHER, NameSource::Claimed, "Other Name")]);
    Connection::open(&old)
        .unwrap()
        .execute_batch("PRAGMA user_version = 8;")
        .unwrap();
    let requests = h.net.requests.lock().unwrap().len();
    let err = h.sync(&with_archive(&old)).unwrap_err();
    assert!(err.to_string().contains("seeded-index upgrade"), "{err}");
    assert_eq!(h.net.requests.lock().unwrap().len(), requests);
    assert_eq!(find(&h.store, "studio"), vec![1]);

    // The next run with the catalogue follows it: the claim is gone, so is the name.
    remove_name(&cat, RICHER, NameSource::Claimed);
    let names = h.sync(&with_archive(&cat)).unwrap().names.unwrap();
    assert_eq!((names.named, names.changed), (0, 1));
    assert_eq!(find(&h.store, "studio"), Vec::<u64>::new());
    assert_eq!(find(&h.store, &RICHER[..12]), vec![1]);
    for p in [cat, old] {
        remove_db(&p);
    }
}

#[test]
fn reruns_are_idempotent_and_write_only_changed_authors() {
    let path = scratch("names-idempotent");
    let mut store = ArticleStore::open(&path).unwrap();
    store.apply_snapshot(&snapshot(&arts(), &[], &[])).unwrap();
    let cat = scratch("names-idempotent-cat");
    name_catalogue(
        &cat,
        &[
            (ALICE, NameSource::Registry, "alicereg"),
            (ALICE, NameSource::Claimed, "Alice Claims"),
            (BOB, NameSource::Curated, "Bob Curated"),
        ],
    );
    let catalogue = open_catalogue(&cat).unwrap();
    assert_eq!(store.resolve_names(&catalogue).unwrap(), report(4, 2, 2, 3));
    let watcher = Connection::open(&path).unwrap();
    let version = |c: &Connection| -> i64 {
        c.query_row("PRAGMA data_version", [], |r| r.get(0))
            .unwrap()
    };
    let before = (version(&watcher), name_rows(&watcher), index_rows(&watcher));
    for _ in 0..2 {
        assert_eq!(store.resolve_names(&catalogue).unwrap(), report(4, 2, 0, 0));
    }
    assert_eq!(
        (version(&watcher), name_rows(&watcher), index_rows(&watcher)),
        before,
        "an unchanged run writes nothing"
    );

    // One author renamed: one stored name and that author's index row change.
    set_names(&cat, &[(BOB, NameSource::Curated, "Bob Renamed")]);
    assert_eq!(store.resolve_names(&catalogue).unwrap(), report(4, 2, 1, 1));
    assert_ne!(version(&watcher), before.0);
    let index = index_rows(&watcher);
    for (old, new) in before.2.iter().zip(&index) {
        assert_eq!(old == new, old.0 != 11, "only #11 is rewritten: {new:?}");
    }
    assert_eq!(find(&store, "renamed"), vec![11]);
    assert_eq!(find(&store, "curated"), Vec::<u64>::new());

    // Alice's claim is withdrawn: her registry name is displayed instead, on both articles.
    remove_name(&cat, ALICE, NameSource::Claimed);
    assert_eq!(store.resolve_names(&catalogue).unwrap(), report(4, 2, 1, 2));
    assert_eq!(find(&store, "alicereg"), vec![14, 10]);
    assert_eq!(find(&store, "claims"), Vec::<u64>::new());
    assert_eq!(
        name_rows(&watcher),
        vec![
            (0, format!("{BOB}=Bob Renamed")),
            (0, format!("{ALICE}=alicereg")),
        ]
    );
    drop((store, catalogue, watcher));
    for p in [path, cat] {
        remove_db(&p);
    }
}

#[test]
fn more_than_500_authors_are_resolved_in_batches() {
    const BASE58: &[u8] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
    let address = |i: usize| -> &'static str {
        let digits: String = [i / 58 / 58 % 58, i / 58 % 58, i % 58]
            .iter()
            .map(|d| BASE58[*d] as char)
            .collect();
        Box::leak(format!("tz1{}{digits}", "1".repeat(30)).into_boxed_str())
    };
    let arts: Vec<Art> = (0..1_201)
        .map(|i| art(100 + i as u64, address(i), "body"))
        .collect();
    let mut store = ArticleStore::open_in_memory().unwrap();
    store.apply_snapshot(&snapshot(&arts, &[], &[])).unwrap();
    let cat = scratch("names-batches");
    name_catalogue(
        &cat,
        &[
            (address(0), NameSource::Curated, "firstauthor"),
            (address(1_200), NameSource::Registry, "lastauthor"),
        ],
    );
    assert_eq!(resolve(&mut store, &cat), report(1_201, 2, 2, 2));
    assert_eq!(find(&store, "firstauthor"), vec![100]);
    assert_eq!(find(&store, "lastauthor"), vec![1_300]);
    remove_db(&cat);
}

/// Back to exactly a schema 2 file, with fxhash registry names in `names` and the index.
fn downgrade_to_v2(path: &std::path::Path) {
    let c = Connection::open(path).unwrap();
    c.execute_batch(&format!(
        "DROP TABLE names;
         CREATE TABLE names (address TEXT PRIMARY KEY, name TEXT NOT NULL,
           last_level INTEGER NOT NULL) WITHOUT ROWID;
         INSERT INTO names VALUES ('{CAROL}', 'Twin', 7), ('{ALICE}', 'alicereg', 7);
         DELETE FROM articles_fts;
         INSERT INTO articles_fts (rowid, title, authors, tags)
           SELECT a.token_id, COALESCE(a.title, ''), a.author || ' ' || COALESCE(n.name, ''), ''
           FROM articles a LEFT JOIN names n ON n.address = a.author;
         PRAGMA user_version = 2;"
    ))
    .unwrap();
    let hits: i64 = c
        .query_row(
            "SELECT count(*) FROM articles_fts WHERE articles_fts MATCH 'twin*'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(hits, 1, "schema 2 indexed registry names");
}

#[test]
fn migration_from_schema_2_drops_registry_names_until_a_catalogue_run() {
    let path = scratch("names-migration");
    {
        let mut store = ArticleStore::open(&path).unwrap();
        store.apply_snapshot(&snapshot(&arts(), &[], &[])).unwrap();
    }
    downgrade_to_v2(&path);
    // The web cannot read it until migrated, and says why.
    match ArticleStore::open_read_only(&path) {
        Err(ArticleStoreError::Unavailable(m)) => assert!(m.contains("schema 2"), "{m}"),
        other => panic!("expected Unavailable, got {:?}", other.map(|_| ())),
    }
    let mut store = ArticleStore::open(&path).unwrap();
    {
        let c = Connection::open(&path).unwrap();
        let version: i64 = c
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);
        assert_eq!(SCHEMA_VERSION, 3);
        let columns: Vec<String> = c
            .prepare("SELECT name FROM pragma_table_info('names') ORDER BY cid")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(columns, ["address", "name"]);
        assert!(
            name_rows(&c).is_empty(),
            "registry names are not displayed names"
        );
    }
    assert_eq!(find(&store, "twin"), Vec::<u64>::new());
    assert_eq!(find(&store, "alicereg"), Vec::<u64>::new());
    assert_eq!(find(&store, &CAROL[..12]), vec![12]);
    assert_eq!(store.stats().unwrap().named_authors, 0);
    drop(ArticleStore::open_read_only(&path).unwrap());

    let cat = scratch("names-migration-cat");
    name_catalogue(
        &cat,
        &[
            (CAROL, NameSource::Registry, "Twin"),
            (STRANGER, NameSource::Registry, "twin"),
            (ALICE, NameSource::Claimed, "Alice Claims"),
        ],
    );
    assert_eq!(resolve(&mut store, &cat), report(4, 1, 1, 2));
    assert_eq!(find(&store, "claims"), vec![14, 10]);
    assert_eq!(find(&store, "twin"), Vec::<u64>::new());
    // Reopening a current file migrates nothing and keeps the names.
    drop(store);
    let store = ArticleStore::open(&path).unwrap();
    assert_eq!(find(&store, "claims"), vec![14, 10]);
    drop(store);
    for p in [path, cat] {
        remove_db(&p);
    }
}

#[test]
fn name_search_keeps_the_query_escaping_and_term_cap() {
    let mut store = synthetic(&arts(), &[], &[]);
    let cat = scratch("names-escaping");
    name_catalogue(
        &cat,
        &[(BOB, NameSource::Curated, "O\"Hara title:mark NEAR(x) *")],
    );
    assert_eq!(resolve(&mut store, &cat), report(4, 1, 1, 1));
    assert_eq!(find(&store, "hara"), vec![11]);
    // Syntax in names and queries is inert: "title" and "near" are words here.
    assert_eq!(find(&store, "title:mark"), vec![11]);
    assert_eq!(find(&store, "NEAR(x)"), vec![11]);
    assert_eq!(find(&store, "\"O\"Hara"), vec![11]);
    for q in ["a OR b", "* ^ - :", "' ; DROP TABLE names; --"] {
        store.articles_page(Some(q), None, 10).unwrap();
    }
    let eight = "a b c d e f g h";
    assert_eq!(eight.split(' ').count(), MAX_QUERY_TERMS);
    store.articles_page(Some(eight), None, 10).unwrap();
    match store.articles_page(Some("o hara title mark near x b c d"), None, 10) {
        Err(ArticleStoreError::BadRequest(m)) => assert_eq!(m, "too many search terms"),
        other => panic!("expected BadRequest, got {:?}", other.map(|p| p.total)),
    }
    assert_eq!(
        fts_query("O\"Hara").unwrap().as_deref(),
        Some("\"O\" \"Hara\"*")
    );
    remove_db(&cat);
}
