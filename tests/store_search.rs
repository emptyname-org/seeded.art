//! Search inside listings: a holder's tokens by title or artist words and by trait, a
//! project's editions by number, token id, owner and trait; cursors bound to their filter.

mod ingest_support;

use std::path::PathBuf;

use ingest_support::test_dir;
use rusqlite::{Connection, params};
use seeded_archive::model::Token;
use seeded_archive::store::{Cursor, KeysetPage, ListingFilter, Store, StoreError};

const V2: &str = "KT1U6EHmNxJTkvaWJ4ThczG4FSDaHC21ssvi";
const HOLDER: &str = "tz1N64TWNCZR3yJZmmzBE4DRidN1tCaZCw8Q";
const OTHER: &str = "tz1PkLH6efZifS8NtKiZBWFWgDoHDmDwN9f1";
/// Shares the user name "twin" with TWIN_B, so neither name is valid.
const TWIN_A: &str = "tz1Kq8NtEjSXkpswLmgod1tPFPVH2TZc8dwM";
const TWIN_B: &str = "tz1TEL6xFJx1hJKukx21NCufAfGEEr9yYoKX";
const ZANCAN: &str = "tz1VE1gFxMZ5TVCwWaYpXGANcfhyjgiUGkpg";
const HOBBS: &str = "tz1WMa1cAKk4JjFGZCpXNkTDi5o5AqCsFF6E";
const ESCROW_V2: &str = "KT1GbyoDi7H1sfXmimXpptZJuCdHMh66WS9u";

/// Projects 1 "Garden Monoliths" (Zancan), 2 "Fidenza Study" (Tyler Hobbs), 3 "Garden of
/// Forking Paths" (Tyler Hobbs), 4 "Garden Hidden" (Zancan, flagged). Project 1 has editions
/// 1..=8 (token ids 101..), the others 4 each. HOLDER holds 1#1-5, 2#1-3, 3#1-4, 4#1-2 and
/// lists 1#6 through the v2 marketplace; OTHER, TWIN_A and TWIN_B hold the rest.
fn store(name: &str) -> (Store, PathBuf) {
    let dir = test_dir(name);
    let db = dir.join("db.sqlite");
    drop(Store::open(&db).unwrap());
    let c = Connection::open(&db).unwrap();
    c.execute_batch(&format!(
        "INSERT INTO artists (id, name, name_key, address) VALUES
           ('{ZANCAN}', 'Zancan', 'zancan', '{ZANCAN}'),
           ('{HOBBS}', 'Tyler Hobbs', 'tyler hobbs', '{HOBBS}');
         INSERT INTO users (address, name, name_key) VALUES
           ('{HOLDER}', 'quadrado', 'quadrado'),
           ('{TWIN_A}', 'twin', 'twin'), ('{TWIN_B}', 'Twin', 'twin');
         INSERT INTO known_addresses (address, name, kind) VALUES
           ('{ESCROW_V2}', 'fxhash marketplace v2', 'escrow');"
    ))
    .unwrap();
    let projects = [
        (1, "Garden Monoliths", ZANCAN, "Zancan", 8, 0),
        (2, "Fidenza Study", HOBBS, "Tyler Hobbs", 4, 0),
        (3, "Garden of Forking Paths", HOBBS, "Tyler Hobbs", 4, 0),
        (4, "Garden Hidden", ZANCAN, "Zancan", 4, 1),
    ];
    for (id, title, artist, artist_name, editions, hidden) in projects {
        c.execute(
            "INSERT INTO projects (id, title, title_key, artist_key, index_state, editions, \
             hidden, contract) VALUES (?1, ?2, lower(?2), lower(?3), 'indexed', ?4, ?5, ?6)",
            params![id, title, artist_name, editions, hidden, V2],
        )
        .unwrap();
        c.execute(
            "INSERT INTO project_artists (project_id, position, artist_id) VALUES (?1, 0, ?2)",
            params![id, artist],
        )
        .unwrap();
        c.execute(
            "INSERT INTO project_fts (rowid, title, artists) VALUES (?1, ?2, ?3)",
            params![id, title, artist_name],
        )
        .unwrap();
    }
    let mut token = c
        .prepare(
            "INSERT INTO tokens (contract, token_id, project_id, iteration, seed, minter, minted, \
             artifact_uri, source, attributes, owner_address, owner_as_of, owner_source, \
             holder_address, listed_by) VALUES (?1, ?2, ?3, ?4, 'oo', ?5, '2023-01-01', \
             'ipfs://x', 'tzkt', ?6, ?7, 'now', 'tzkt', ?8, ?9)",
        )
        .unwrap();
    for (project, editions) in [(1u64, 8u32), (2, 4), (3, 4), (4, 4)] {
        for iteration in 1..=editions {
            let token_id = project * 100 + u64::from(iteration);
            let held = match project {
                1 => iteration <= 5,
                2 => iteration <= 3,
                _ => iteration <= 4 && (project != 4 || iteration <= 2),
            };
            let (owner, holder, listed) = if project == 1 && iteration == 6 {
                (ESCROW_V2, Some(HOLDER), Some(HOLDER))
            } else if held {
                (HOLDER, Some(HOLDER), None)
            } else if project == 1 && iteration == 7 {
                (TWIN_A, Some(TWIN_A), None)
            } else {
                (OTHER, Some(OTHER), None)
            };
            let palette = if iteration % 2 == 1 { "Gold" } else { "Red" };
            let attributes = format!(r#"[["Palette","{palette}"],["Layers","{iteration}"]]"#);
            token
                .execute(params![
                    V2,
                    token_id as i64,
                    project as i64,
                    iteration,
                    OTHER,
                    attributes,
                    owner,
                    holder,
                    listed
                ])
                .unwrap();
        }
    }
    drop(token);
    drop(c);
    (Store::open(&db).unwrap(), db)
}

fn ids(page: &KeysetPage<Token>) -> Vec<u64> {
    page.items.iter().map(|t| t.token_id).collect()
}

fn text(t: &str) -> ListingFilter<'_> {
    ListingFilter {
        text: Some(t),
        trait_filter: None,
    }
}

fn holdings(store: &Store, f: &ListingFilter<'_>) -> (Vec<u64>, u64) {
    let page = store
        .collector_holdings_search(HOLDER, f, None, 100)
        .unwrap();
    (ids(&page), page.total)
}

fn editions(store: &Store, project: u64, f: &ListingFilter<'_>) -> (Vec<u64>, u64) {
    let page = store
        .tokens_for_project_search(project, f, None, 100)
        .unwrap();
    (ids(&page), page.total)
}

/// Every page of a filtered listing, `limit` at a time.
fn walk(mut next: impl FnMut(Option<&Cursor>) -> KeysetPage<Token>) -> (Vec<u64>, Vec<u64>) {
    let mut all = Vec::new();
    let mut totals = Vec::new();
    let mut cursor: Option<Cursor> = None;
    loop {
        let page = next(cursor.as_ref());
        all.extend(ids(&page));
        totals.push(page.total);
        match page.next {
            Some(c) => cursor = Some(c),
            None => return (all, totals),
        }
    }
}

#[test]
fn holdings_text_ending_in_a_number_matches_that_edition_or_token_id() {
    let (store, _db) = store("search_holdings_number");
    assert_eq!(
        holdings(&store, &text("3")),
        (vec![103, 203, 303], 3),
        "edition #3 of every held project"
    );
    assert_eq!(holdings(&store, &text("#2")), (vec![102, 202, 302], 3));
    assert_eq!(holdings(&store, &text("302")), (vec![302], 1), "token id");
    assert_eq!(
        holdings(&store, &text("garden 2")),
        (vec![102, 302], 2),
        "edition #2 of projects matching the words"
    );
    assert_eq!(holdings(&store, &text("Fidenza Study #1")), (vec![201], 1));
    assert_eq!(
        holdings(&store, &text("1")),
        (vec![101, 201, 301], 3),
        "hidden project 4 stays hidden"
    );
    assert_eq!(holdings(&store, &text("7")), (vec![], 0), "not held");
    assert_eq!(
        holdings(&store, &text("99999999999999999999")),
        (vec![], 0),
        "too large for an edition"
    );
    // Pages of a number search resume in order.
    let (all, totals) = walk(|after| {
        store
            .collector_holdings_search(HOLDER, &text("#2"), after, 1)
            .unwrap()
    });
    assert_eq!(all, vec![102, 202, 302]);
    assert!(totals.iter().all(|&t| t == 3));
}

#[test]
fn holdings_text_matches_title_and_artist_words_and_skips_hidden_projects() {
    let (store, _db) = store("search_holdings_text");
    let garden = (vec![101, 102, 103, 104, 105, 106, 301, 302, 303, 304], 10);
    assert_eq!(holdings(&store, &text("garden")), garden);
    assert_eq!(holdings(&store, &text("GARD")), garden, "prefix, any case");
    assert_eq!(
        holdings(&store, &text("zancan")),
        (vec![101, 102, 103, 104, 105, 106], 6)
    );
    assert_eq!(
        holdings(&store, &text("tyler garden")),
        (vec![301, 302, 303, 304], 4),
        "terms are ANDed across title and artist"
    );
    assert_eq!(holdings(&store, &text("nothing here")), (vec![], 0));
    assert_eq!(holdings(&store, &text("*")), (vec![], 0));
    // Blank text is no filter: the plain listing with its maintained total.
    let all = store.collector_holdings_page(HOLDER, None, 100).unwrap();
    assert_eq!(holdings(&store, &text("   ")), (ids(&all), all.total));
    assert_eq!(all.total, 13);
    match store.collector_holdings_search(HOLDER, &text("a b c d e f g h i"), None, 10) {
        Err(StoreError::BadRequest(m)) => assert_eq!(m, "too many search terms"),
        other => panic!("{other:?}"),
    }
    assert!(matches!(
        store.collector_holdings_search(HOLDER, &text(&"x".repeat(201)), None, 10),
        Err(StoreError::BadRequest(_))
    ));
    // The escrowed listing (1#6) is the seller's.
    let listed = store
        .collector_holdings_search(HOLDER, &text("monoliths"), None, 100)
        .unwrap();
    let t = listed.items.iter().find(|t| t.token_id == 106).unwrap();
    assert_eq!(
        t.owner
            .as_ref()
            .unwrap()
            .listed_by
            .as_ref()
            .unwrap()
            .address,
        HOLDER
    );
}

#[test]
fn holdings_trait_filter_is_exact_and_combines_with_text() {
    let (store, _db) = store("search_holdings_trait");
    let gold = ListingFilter {
        text: None,
        trait_filter: Some(("Palette", "Gold")),
    };
    assert_eq!(
        holdings(&store, &gold),
        (vec![101, 103, 105, 201, 203, 301, 303], 7)
    );
    let gold_zancan = ListingFilter {
        text: Some("zancan"),
        trait_filter: Some(("Palette", "Gold")),
    };
    assert_eq!(holdings(&store, &gold_zancan), (vec![101, 103, 105], 3));
    for no_match in [("Palette", "gold"), ("palette", "Gold"), ("Palette", "Go")] {
        let f = ListingFilter {
            text: None,
            trait_filter: Some(no_match),
        };
        assert_eq!(holdings(&store, &f), (vec![], 0), "{no_match:?}");
    }
    let layers = ListingFilter {
        text: None,
        trait_filter: Some(("Layers", "3")),
    };
    assert_eq!(holdings(&store, &layers), (vec![103, 203, 303], 3));
    let bad = ListingFilter {
        text: None,
        trait_filter: Some(("", "x")),
    };
    assert!(matches!(
        store.collector_holdings_search(HOLDER, &bad, None, 10),
        Err(StoreError::BadRequest(_))
    ));
}

#[test]
fn filtered_holdings_page_completely_in_order() {
    let (store, _db) = store("search_holdings_walk");
    for f in [
        text("garden"),
        ListingFilter {
            text: Some("garden"),
            trait_filter: Some(("Palette", "Red")),
        },
    ] {
        let (one, _) = holdings(&store, &f);
        let (walked, totals) = walk(|after| {
            store
                .collector_holdings_search(HOLDER, &f, after, 2)
                .unwrap()
        });
        assert_eq!(walked, one);
        assert!(totals.iter().all(|t| *t == one.len() as u64), "{totals:?}");
    }
}

#[test]
fn cursors_only_resume_the_filter_that_issued_them() {
    let (store, _db) = store("search_cursor_binding");
    let first = store
        .collector_holdings_search(HOLDER, &text("garden"), None, 2)
        .unwrap();
    let cursor = first.next.unwrap();
    let rejected = |r: Result<KeysetPage<Token>, StoreError>| {
        assert!(
            matches!(&r, Err(StoreError::BadRequest(m)) if m.contains("another listing")),
            "{r:?}"
        );
    };
    rejected(store.collector_holdings_search(HOLDER, &text("zancan"), Some(&cursor), 2));
    rejected(store.collector_holdings_page(HOLDER, Some(&cursor), 2));
    rejected(store.collector_holdings_search(
        HOLDER,
        &ListingFilter {
            text: Some("garden"),
            trait_filter: Some(("Palette", "Gold")),
        },
        Some(&cursor),
        2,
    ));
    rejected(store.collector_holdings_search(OTHER, &text("garden"), Some(&cursor), 2));
    // Same filter, with outer spaces, resumes.
    let second = store
        .collector_holdings_search(HOLDER, &text(" garden "), Some(&cursor), 2)
        .unwrap();
    assert_eq!(ids(&second), vec![103, 104]);
    // An unfiltered cursor does not resume a filtered listing either.
    let plain = store.collector_holdings_page(HOLDER, None, 2).unwrap();
    rejected(store.collector_holdings_search(HOLDER, &text("garden"), plain.next.as_ref(), 2));
    // Edition listings bind the same way.
    let gold = ListingFilter {
        text: None,
        trait_filter: Some(("Palette", "Gold")),
    };
    let ed = store.tokens_for_project_search(1, &gold, None, 1).unwrap();
    let c = ed.next.unwrap();
    rejected(store.tokens_for_project_page(1, Some(&c), 1));
    rejected(store.tokens_for_project_search(1, &text("3"), Some(&c), 1));
    rejected(store.tokens_for_project_search(3, &gold, Some(&c), 1));
    for other in [("Colour", "Gold"), ("Palette", "Red")] {
        let f = ListingFilter {
            text: None,
            trait_filter: Some(other),
        };
        rejected(store.tokens_for_project_search(1, &f, Some(&c), 1));
    }
    assert_eq!(
        ids(&store
            .tokens_for_project_search(1, &gold, Some(&c), 1)
            .unwrap()),
        vec![103]
    );
}

#[test]
fn editions_match_number_token_id_owner_and_trait() {
    let (store, _db) = store("search_editions");
    assert_eq!(editions(&store, 1, &text("3")), (vec![103], 1));
    assert_eq!(editions(&store, 1, &text("#3")), (vec![103], 1));
    assert_eq!(
        editions(&store, 1, &text("105")),
        (vec![105], 1),
        "token id"
    );
    assert_eq!(editions(&store, 1, &text("9")), (vec![], 0));
    assert_eq!(
        editions(&store, 1, &text("99999999999999999999999")),
        (vec![], 0)
    );
    assert_eq!(
        editions(&store, 1, &text(HOLDER)),
        (vec![101, 102, 103, 104, 105, 106], 6),
        "holder address, escrowed listing included"
    );
    assert_eq!(
        editions(&store, 1, &text(ESCROW_V2)),
        (vec![106], 1),
        "ledger owner"
    );
    assert_eq!(
        editions(&store, 1, &text("QUADRADO")),
        (vec![101, 102, 103, 104, 105, 106], 6),
        "a valid shown name, any case"
    );
    assert_eq!(
        editions(&store, 1, &text("twin")),
        (vec![], 0),
        "shared names never match"
    );
    assert_eq!(editions(&store, 1, &text(TWIN_A)), (vec![107], 1));
    assert_eq!(editions(&store, 1, &text("nobody")), (vec![], 0));
    let red = ListingFilter {
        text: None,
        trait_filter: Some(("Palette", "Red")),
    };
    assert_eq!(editions(&store, 1, &red), (vec![102, 104, 106, 108], 4));
    let red_holder = ListingFilter {
        text: Some(HOLDER),
        trait_filter: Some(("Palette", "Red")),
    };
    assert_eq!(editions(&store, 1, &red_holder), (vec![102, 104, 106], 3));
    let absent = ListingFilter {
        text: None,
        trait_filter: Some(("Palette", "Blue")),
    };
    assert_eq!(editions(&store, 1, &absent), (vec![], 0));
    let (walked, _) = walk(|after| store.tokens_for_project_search(1, &red, after, 3).unwrap());
    assert_eq!(walked, vec![102, 104, 106, 108]);
    // Hidden projects stay NotFound, filtered or not.
    assert!(matches!(
        store.tokens_for_project_search(4, &red, None, 10),
        Err(StoreError::NotFound { .. })
    ));
    // Unfiltered totals still come from editions.
    assert_eq!(editions(&store, 1, &ListingFilter::default()).1, 8);
}

/// Timings on a scratch copy of production (it is modified: one synthetic holder of 200,000
/// tokens is added). `SEEDED_MEASURE_DB=tmp/... cargo test --release --test store_search --
/// --ignored --nocapture`.
#[test]
#[ignore = "measures a scratch production copy named by SEEDED_MEASURE_DB"]
fn measure_search_on_a_production_copy() {
    use std::time::Instant;
    let Ok(path) = std::env::var("SEEDED_MEASURE_DB") else {
        panic!("set SEEDED_MEASURE_DB to a scratch copy");
    };
    let big = "tz1burnSyntheticHo1derForSearchTimingXx";
    let big = &big[..36];
    {
        let c = Connection::open(&path).unwrap();
        let have: i64 = c
            .query_row(
                "SELECT coalesce((SELECT held FROM holder_counts WHERE holder_address = ?1), 0)",
                [big],
                |r| r.get(0),
            )
            .unwrap();
        if have < 200_000 {
            let t = Instant::now();
            c.execute(
                "UPDATE tokens SET holder_address = ?1 WHERE (contract, token_id) IN \
                 (SELECT contract, token_id FROM tokens WHERE holder_address IS NOT NULL \
                  AND listed_by IS NULL ORDER BY contract, token_id LIMIT 200000)",
                [big],
            )
            .unwrap();
            println!("synthetic holder written in {:?}", t.elapsed());
        }
    }
    let store = Store::open_read_only(std::path::Path::new(&path)).unwrap();
    let top: String = Connection::open(&path)
        .unwrap()
        .query_row(
            "SELECT holder_address FROM holder_counts WHERE holder_address <> ?1 \
             ORDER BY held DESC LIMIT 1",
            [big],
            |r| r.get(0),
        )
        .unwrap();
    // The most common trait among the top holder's projects, so it matches something.
    let trait_pair: (String, String) = Connection::open(&path)
        .unwrap()
        .query_row(
            "SELECT name, value FROM feature_counts WHERE count > 0 AND project_id IN \
             (SELECT DISTINCT project_id FROM tokens WHERE holder_address = ?1) \
             GROUP BY name, value ORDER BY sum(count) DESC LIMIT 1",
            [&top],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    println!("trait {trait_pair:?}");
    for holder in [top.as_str(), big] {
        let held = store.profile(holder).unwrap().held;
        for (label, f) in [
            ("text garden", text("garden")),
            ("text the", text("the")),
            ("text a", text("a")),
            ("text a b c d e f g h", text("a b c d e f g h")),
            (
                "trait",
                ListingFilter {
                    text: None,
                    trait_filter: Some((&trait_pair.0, &trait_pair.1)),
                },
            ),
            (
                "text + trait",
                ListingFilter {
                    text: Some("the"),
                    trait_filter: Some((&trait_pair.0, &trait_pair.1)),
                },
            ),
        ] {
            let t = Instant::now();
            let first = store
                .collector_holdings_search(holder, &f, None, 48)
                .unwrap();
            let t1 = t.elapsed();
            let t = Instant::now();
            let second = match &first.next {
                Some(c) => store
                    .collector_holdings_search(holder, &f, Some(c), 48)
                    .unwrap()
                    .items
                    .len(),
                None => 0,
            };
            let t2 = t.elapsed();
            println!(
                "holder {held:>7} {label:<22} total {:>7} first page {t1:>10.2?} next page \
                 {t2:>10.2?} ({second} items)",
                first.total
            );
        }
    }
    let unicorn = 14048;
    let unicorn_trait: (String, String) = Connection::open(&path)
        .unwrap()
        .query_row(
            "SELECT name, value FROM feature_counts WHERE project_id = ?1 \
             ORDER BY count DESC LIMIT 1",
            [unicorn as i64],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    for (label, f) in [
        (
            "own trait",
            ListingFilter {
                text: None,
                trait_filter: Some((&unicorn_trait.0, &unicorn_trait.1)),
            },
        ),
        ("edition #5000", text("#5000")),
        ("owner address", text(top.as_str())),
        (
            "trait",
            ListingFilter {
                text: None,
                trait_filter: Some((&trait_pair.0, &trait_pair.1)),
            },
        ),
    ] {
        let t = Instant::now();
        let page = store
            .tokens_for_project_search(unicorn, &f, None, 48)
            .unwrap();
        println!(
            "Unicorn {label:<16} total {:>6} in {:?}",
            page.total,
            t.elapsed()
        );
    }
    let t = Instant::now();
    let p = store.profile(&top).unwrap();
    println!("profile {:?} in {:?}", p.name, t.elapsed());
    let t = Instant::now();
    let r = store.profile_by_name("zancan");
    println!(
        "profile_by_name zancan: {:?} in {:?}",
        r.map(|p| p.address),
        t.elapsed()
    );
}
