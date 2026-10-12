//! Keyset (cursor) pagination: complete, ordered traversal; tamper-proof cursors; stable
//! under concurrent inserts.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use seeded_archive::import::import_fixtures;
use seeded_archive::store::{
    ArtistQuery, Cursor, KeysetPage, PageRequest, ProjectFilter, ProjectQuery, ProjectSort, Store,
    StoreError,
};
use serde_json::Value;

const ARBOREAL: u64 = 29437;
const QUADRADO: &str = "tz1N64TWNCZR3yJZmmzBE4DRidN1tCaZCw8Q";
/// Fixtures have no sales, so `VolumeDesc` lists every project at volume 0, by id.
const SORTS: [ProjectSort; 5] = [
    ProjectSort::TitleAsc,
    ProjectSort::ArtistAsc,
    ProjectSort::IdAsc,
    ProjectSort::IdDesc,
    ProjectSort::VolumeDesc,
];

fn root() -> PathBuf {
    let mut dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    while !dir.join("assets/fixtures/projects.json").is_file() {
        assert!(dir.pop(), "project root not found");
    }
    dir
}

fn test_dir(name: &str) -> PathBuf {
    let dir = root().join("tmp/ingest-tests").join(name);
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

fn copy_dir(src: &Path, dst: &Path) {
    fs::create_dir_all(dst).unwrap();
    for entry in fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let to = dst.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &to);
        } else {
            fs::copy(entry.path(), to).unwrap();
        }
    }
}

/// Fixtures where several projects share a title and an artist, to exercise ties.
fn store_with_ties(dir: &Path) -> Store {
    let fixtures = dir.join("fixtures");
    copy_dir(&root().join("assets/fixtures"), &fixtures);
    let path = fixtures.join("projects.json");
    let mut v: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    for (i, p) in v.as_array_mut().unwrap().iter_mut().enumerate() {
        if i % 3 == 0 {
            p["title"] = "Same Title".into();
        }
    }
    fs::write(&path, serde_json::to_vec_pretty(&v).unwrap()).unwrap();
    let mut store = Store::open(&dir.join("db.sqlite")).unwrap();
    assert!(import_fixtures(&mut store, &fixtures).unwrap().changed);
    store
}

fn all() -> PageRequest {
    PageRequest {
        offset: 0,
        limit: 100,
    }
}

fn filter(sort: ProjectSort, text: Option<&str>) -> ProjectFilter {
    ProjectFilter {
        text: text.map(str::to_owned),
        artist: None,
        sort,
    }
}

/// Follows `next` to the end, checking that `total` never changes.
fn traverse<T>(mut page: impl FnMut(Option<&Cursor>) -> KeysetPage<T>) -> Vec<T> {
    let mut out = Vec::new();
    let mut cursor: Option<Cursor> = None;
    let mut total = None;
    loop {
        let p = page(cursor.as_ref());
        assert_eq!(*total.get_or_insert(p.total), p.total);
        let more = p.next.is_some();
        out.extend(p.items);
        if !more {
            assert_eq!(out.len() as u64, p.total, "every row once");
            return out;
        }
        cursor = p.next;
    }
}

fn offset_ids(store: &Store, sort: ProjectSort, text: Option<&str>) -> Vec<u64> {
    store
        .projects(&ProjectQuery {
            text: text.map(str::to_owned),
            artist: None,
            sort,
            page: all(),
        })
        .unwrap()
        .items
        .iter()
        .map(|p| p.id)
        .collect()
}

#[test]
fn keyset_traversal_returns_every_row_once_in_order_including_ties() {
    let dir = test_dir("keyset_traversal");
    let store = store_with_ties(&dir);
    for sort in SORTS {
        let expected = offset_ids(&store, sort, None);
        assert_eq!(expected.len(), 25);
        for limit in [1, 2, 3, 7, 100] {
            let got: Vec<u64> =
                traverse(|c| store.projects_page(&filter(sort, None), c, limit).unwrap())
                    .iter()
                    .map(|p| p.id)
                    .collect();
            assert_eq!(got, expected, "{sort:?} limit {limit}");
        }
    }
    let tied: Vec<u64> = traverse(|c| {
        store
            .projects_page(&filter(ProjectSort::TitleAsc, Some("same")), c, 1)
            .unwrap()
    })
    .iter()
    .map(|p| p.id)
    .collect();
    assert_eq!(
        tied,
        offset_ids(&store, ProjectSort::TitleAsc, Some("same"))
    );
    assert!(tied.len() >= 2);

    let artists: Vec<String> = traverse(|c| store.artists_page(None, c, 4).unwrap())
        .into_iter()
        .map(|a| a.id)
        .collect();
    let expected: Vec<String> = store
        .artists(&ArtistQuery {
            text: None,
            page: all(),
        })
        .unwrap()
        .items
        .into_iter()
        .map(|a| a.id)
        .collect();
    assert_eq!(artists, expected);

    let tokens: Vec<u32> = traverse(|c| store.tokens_for_project_page(ARBOREAL, c, 3).unwrap())
        .iter()
        .map(|t| t.iteration)
        .collect();
    assert_eq!(tokens, (1..=10).collect::<Vec<u32>>());

    let held: Vec<u64> = traverse(|c| store.collector_holdings_page(QUADRADO, c, 1).unwrap())
        .iter()
        .map(|t| t.token_id)
        .collect();
    let expected: Vec<u64> = store
        .collector_holdings(QUADRADO, all())
        .unwrap()
        .items
        .iter()
        .map(|t| t.token_id)
        .collect();
    assert_eq!(held, expected);
    assert_eq!(held.len(), 4);

    let zancan: Vec<u64> = traverse(|c| store.artist_projects_page("zancan", c, 1).unwrap())
        .iter()
        .map(|p| p.id)
        .collect();
    let expected: Vec<u64> = store
        .artist_projects("zancan", all())
        .unwrap()
        .items
        .iter()
        .map(|p| p.id)
        .collect();
    assert_eq!(zancan, expected);
    assert!(zancan.len() >= 2);
}

#[test]
fn tampered_or_foreign_cursors_are_bad_request() {
    let dir = test_dir("keyset_cursors");
    let store = store_with_ties(&dir);
    let first = store
        .projects_page(&filter(ProjectSort::TitleAsc, None), None, 2)
        .unwrap();
    let cursor = first.next.unwrap();
    assert!(cursor.as_str().bytes().all(|b| b.is_ascii_hexdigit()));
    assert_eq!(Cursor::parse(cursor.as_str()).unwrap(), cursor);

    let bad = |c: &Cursor| store.projects_page(&filter(ProjectSort::TitleAsc, None), Some(c), 2);
    let mut flipped = cursor.as_str().to_owned().into_bytes();
    let last = flipped.len() - 1;
    flipped[last] = if flipped[last] == b'0' { b'1' } else { b'0' };
    let flipped = String::from_utf8(flipped).unwrap();
    for s in [
        "",
        "zz",
        "ABC",
        &flipped,
        &cursor.as_str()[..10],
        &"0".repeat(2000),
    ] {
        assert!(
            matches!(Cursor::parse(s), Err(StoreError::BadRequest(_))),
            "{s:?}"
        );
    }
    assert!(bad(&cursor).is_ok());
    // A cursor only resumes the listing that issued it.
    let wrong_sort = store.projects_page(&filter(ProjectSort::IdAsc, None), Some(&cursor), 2);
    assert!(matches!(wrong_sort, Err(StoreError::BadRequest(_))));
    let wrong_text = store.projects_page(
        &filter(ProjectSort::TitleAsc, Some("garden")),
        Some(&cursor),
        2,
    );
    assert!(matches!(wrong_text, Err(StoreError::BadRequest(_))));
    assert!(matches!(
        store.artists_page(None, Some(&cursor), 2),
        Err(StoreError::BadRequest(_))
    ));
    assert!(matches!(
        store.tokens_for_project_page(ARBOREAL, Some(&cursor), 2),
        Err(StoreError::BadRequest(_))
    ));
    let tokens = store.tokens_for_project_page(ARBOREAL, None, 2).unwrap();
    let other_project = store.tokens_for_project_page(2969, tokens.next.as_ref(), 2);
    assert!(matches!(other_project, Err(StoreError::BadRequest(_))));
    for limit in [0, 101] {
        assert!(matches!(
            store.projects_page(&filter(ProjectSort::IdAsc, None), None, limit),
            Err(StoreError::BadRequest(_))
        ));
    }
    assert!(matches!(
        store.collector_holdings_page("nope", None, 2),
        Err(StoreError::BadRequest(_))
    ));
    assert!(matches!(
        store.tokens_for_project_page(424_242, None, 2),
        Err(StoreError::NotFound { .. })
    ));
}

fn insert_project(dir: &Path, id: i64, title: &str) {
    let c = rusqlite::Connection::open(dir.join("db.sqlite")).unwrap();
    c.execute(
        "INSERT INTO projects (id, title, title_key, artist_key, index_state) \
         VALUES (?1, ?2, lower(?2), 'zzz', 'not_indexed')",
        rusqlite::params![id, title],
    )
    .unwrap();
}

#[test]
fn keyset_pages_stay_consistent_while_rows_are_inserted() {
    let dir = test_dir("keyset_concurrent");
    let store = store_with_ties(&dir);
    let f = filter(ProjectSort::TitleAsc, None);
    let before = offset_ids(&store, ProjectSort::TitleAsc, None);

    // Within one snapshot, inserts from another connection are invisible.
    let seen = store
        .read_snapshot(|s| {
            let first = s.projects_page(&f, None, 5)?;
            insert_project(&dir, 900_001, "AAA First");
            insert_project(&dir, 900_002, "zzz Last");
            let mut ids: Vec<u64> = first.items.iter().map(|p| p.id).collect();
            let mut cursor = first.next;
            while let Some(c) = cursor {
                let p = s.projects_page(&f, Some(&c), 5)?;
                ids.extend(p.items.iter().map(|p| p.id));
                assert_eq!(p.total, 25);
                cursor = p.next;
            }
            Ok(ids)
        })
        .unwrap();
    assert_eq!(seen, before);

    // Across snapshots: rows inserted before the position are skipped, never repeated; rows
    // after it appear.
    let first = store.projects_page(&f, None, 5).unwrap();
    insert_project(&dir, 900_003, "AAB Before");
    insert_project(&dir, 900_004, "zzz Later");
    let mut ids: Vec<u64> = first.items.iter().map(|p| p.id).collect();
    let mut cursor = first.next;
    while let Some(c) = cursor {
        let p = store.projects_page(&f, Some(&c), 5).unwrap();
        ids.extend(p.items.iter().map(|p| p.id));
        cursor = p.next;
    }
    let unique: BTreeSet<u64> = ids.iter().copied().collect();
    assert_eq!(unique.len(), ids.len(), "no duplicates");
    assert!(ids.contains(&900_004));
    assert!(!ids.contains(&900_003));
    let mut sorted = offset_ids(&store, ProjectSort::TitleAsc, None);
    sorted.retain(|id| *id != 900_003);
    assert_eq!(ids, sorted, "order is preserved");
}
