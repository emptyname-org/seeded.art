//! Project metadata: publication time, tags and credited addresses, under the visibility rule
//! of `Store::project` and inside one read snapshot.

mod ingest_support;

use ingest_support::*;
use rusqlite::Connection;
use seeded_archive::import::import_fixtures;
use seeded_archive::model::ProjectMeta;
use seeded_archive::store::{Store, StoreError};

const ARBOREAL_KEY: i64 = ARBOREAL as i64;
const ARBOREAL_AUTHOR: &str = "KT1LZwGQgbi341zPAu1YmAd8EWvUeRLxrQLr";
const DOPPELGANGER: &str = "tz1exYBh5zHhf7LKyJSKAQqm89ZYZmTwsBz2";
const KEEPER: &str = "tz1gL9H53BzjDqmMi2JkKALeCzT7UAoopzEL";

fn not_found(r: Result<ProjectMeta, StoreError>) -> bool {
    matches!(
        r,
        Err(StoreError::NotFound {
            what: "project",
            ..
        })
    )
}

#[test]
fn bootstrapped_arboreal_has_publication_tags_and_collaborators() {
    let dir = test_dir("meta_bootstrap");
    let store = bootstrapped(&dir);
    let meta = store.project_meta(ARBOREAL).unwrap();
    assert_eq!(
        meta,
        ProjectMeta {
            id: ARBOREAL,
            published: Some("2023-11-10T22:40:40.000Z".into()),
            mint_opens_at: Some("2023-11-10T23:40:40.000Z".into()),
            tags: [
                "trees",
                "black and white",
                "wind",
                "human condition",
                "mindscape"
            ]
            .map(String::from)
            .to_vec(),
            generative_uri: Some(
                "onchfs://a71e98fced01c865f33be138100b5ea5486f32fc333ad1503f814b201234130c".into()
            ),
            author_address: Some(ARBOREAL_AUTHOR.into()),
            artist_addresses: vec![DOPPELGANGER.into(), KEEPER.into()],
        }
    );
    // Same credit order as the project page.
    let credited: Vec<String> = store
        .project(ARBOREAL)
        .unwrap()
        .artists
        .into_iter()
        .filter_map(|a| a.address)
        .collect();
    assert_eq!(meta.artist_addresses, credited);
}

#[test]
fn fixture_arboreal_has_no_catalogue_metadata() {
    let dir = test_dir("meta_fixture");
    let mut store = Store::open(&dir.join("db.sqlite")).unwrap();
    assert!(
        import_fixtures(&mut store, &root().join("assets/fixtures"))
            .unwrap()
            .changed
    );
    assert_eq!(
        store.project_meta(ARBOREAL).unwrap(),
        ProjectMeta {
            id: ARBOREAL,
            published: None,
            mint_opens_at: None,
            tags: Vec::new(),
            generative_uri: None,
            author_address: None,
            // The fixture artist has no address.
            artist_addresses: Vec::new(),
        }
    );
}

#[test]
fn flagged_and_unknown_projects_are_not_found() {
    let dir = test_dir("meta_flagged");
    let store = bootstrapped(&dir);
    assert!(matches!(
        store.project(FLAGGED),
        Err(StoreError::NotFound { .. })
    ));
    assert!(not_found(store.project_meta(FLAGGED)));
    assert!(not_found(store.project_meta(1)));
    assert!(not_found(store.project_meta(u64::MAX)));

    let sql = Connection::open(dir.join("db.sqlite")).unwrap();
    sql.execute(
        "UPDATE projects SET hidden = 1 WHERE id = ?1",
        [ARBOREAL_KEY],
    )
    .unwrap();
    assert!(not_found(store.project_meta(ARBOREAL)));
    sql.execute(
        "UPDATE projects SET hidden = 0 WHERE id = ?1",
        [ARBOREAL_KEY],
    )
    .unwrap();
    assert!(store.project_meta(ARBOREAL).is_ok());
}

#[test]
fn stored_tags_that_are_null_or_garbage_read_as_no_tags() {
    let dir = test_dir("meta_tags");
    let store = bootstrapped(&dir);
    let sql = Connection::open(dir.join("db.sqlite")).unwrap();
    for raw in [
        None,
        Some("not json"),
        Some("{\"tags\":[\"x\"]}"),
        Some("[1,2]"),
    ] {
        sql.execute(
            "UPDATE projects SET tags = ?1 WHERE id = ?2",
            rusqlite::params![raw, ARBOREAL_KEY],
        )
        .unwrap();
        let meta = store.project_meta(ARBOREAL).unwrap();
        assert!(meta.tags.is_empty(), "{raw:?}");
        assert_eq!(
            meta.published.as_deref(),
            Some("2023-11-10T22:40:40.000Z"),
            "{raw:?}"
        );
    }
}

#[test]
fn artist_addresses_follow_credit_position_and_skip_addressless_artists() {
    let dir = test_dir("meta_artist_order");
    let store = bootstrapped(&dir);
    let sql = Connection::open(dir.join("db.sqlite")).unwrap();
    // Positions 0, 1 become 0, -1: the second credited artist moves first.
    sql.execute(
        "UPDATE project_artists SET position = -position WHERE project_id = ?1",
        [ARBOREAL_KEY],
    )
    .unwrap();
    sql.execute(
        "INSERT INTO artists (id, name, name_key) VALUES ('no-address', 'Nameless', 'nameless')",
        [],
    )
    .unwrap();
    sql.execute(
        "INSERT INTO project_artists (project_id, position, artist_id) \
         VALUES (?1, -5, 'no-address')",
        [ARBOREAL_KEY],
    )
    .unwrap();
    assert_eq!(
        store.project_meta(ARBOREAL).unwrap().artist_addresses,
        [KEEPER, DOPPELGANGER]
    );

    sql.execute(
        "DELETE FROM project_artists WHERE project_id = ?1",
        [ARBOREAL_KEY],
    )
    .unwrap();
    let meta = store.project_meta(ARBOREAL).unwrap();
    assert!(meta.artist_addresses.is_empty());
    assert_eq!(meta.author_address.as_deref(), Some(ARBOREAL_AUTHOR));
}

#[test]
fn project_meta_reads_join_the_outer_snapshot() {
    let dir = test_dir("meta_snapshot");
    let store = bootstrapped(&dir);
    let writer = Connection::open(dir.join("db.sqlite")).unwrap();
    let (before, inside) = store
        .read_snapshot(|s| {
            let before = s.project_meta(ARBOREAL)?;
            writer
                .execute(
                    "UPDATE projects SET created_at = '2030-01-01T00:00:00.000Z' WHERE id = ?1",
                    [ARBOREAL_KEY],
                )
                .unwrap();
            Ok((before, s.project_meta(ARBOREAL)?))
        })
        .unwrap();
    assert_eq!(before, inside, "a write after the first read is not seen");
    assert_eq!(
        store.project_meta(ARBOREAL).unwrap().published.as_deref(),
        Some("2030-01-01T00:00:00.000Z")
    );
    // NotFound inside a snapshot is an ordinary error and rolls it back.
    assert!(not_found(store.read_snapshot(|s| s.project_meta(FLAGGED))));
}
