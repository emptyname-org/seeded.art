//! Name numbers: which address of a shared name shows it plain, how the others are numbered,
//! and that numbers stay put (stability, tombstones, class re-rank, claims, search, upgrade).

use std::fs;
use std::path::PathBuf;

use rusqlite::{Connection, params};
use seeded_archive::model::{EditProof, ProfileEdit};
use seeded_archive::store::{Store, StoreError};

const TEZ: i64 = 1_000_000;
/// Revealed key of a tz1 address, so it can claim a name (see `store_profiles`).
const ED_B: (&str, &str) = (
    "tz1itwvWyP4qd6PY7qrYkn3LBvDC13vjQS9i",
    "edpktrU1cZyBK4vBU1WSpcmrCfqgUsgC4NYGRcyim6ijn3XBipXYmh",
);

/// A well-formed tz1 address named by one letter; they sort in letter order.
fn addr(tag: &str) -> String {
    format!("tz1{tag:1>33}")
}

fn test_dir(name: &str) -> PathBuf {
    let mut dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    while !dir.join("assets/fixtures/projects.json").is_file() {
        assert!(dir.pop(), "project root not found");
    }
    let dir = dir.join("tmp/ingest-tests").join(name);
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

type Index = (Vec<(String, String)>, Vec<(i64, String)>);

struct Fx {
    store: Store,
    db: PathBuf,
}

impl Fx {
    fn new(name: &str) -> Fx {
        let db = test_dir(name).join("db.sqlite");
        let store = Store::open(&db).unwrap();
        Fx { store, db }
    }

    fn c(&self) -> Connection {
        Connection::open(&self.db).unwrap()
    }

    fn user(&self, tag: &str, name: &str) {
        self.c()
            .execute(
                "INSERT INTO users (address, name, name_key) VALUES (?1, ?2, ?3)",
                params![addr(tag), name, name.trim().to_lowercase()],
            )
            .unwrap();
    }

    fn artist(&self, tag: &str, name: &str) {
        let a = addr(tag);
        self.c()
            .execute(
                "INSERT INTO artists (id, name, name_key, address) VALUES (?1, ?2, ?3, ?1)",
                params![a, name, name.trim().to_lowercase()],
            )
            .unwrap();
    }

    /// Project `id`, credited to the artist `tag`, created at `created`, with `tez` of volume.
    fn project(&self, id: i64, tag: &str, hidden: bool, created: &str, tez: i64) {
        let c = self.c();
        c.execute(
            "INSERT INTO projects (id, title, title_key, artist_key, index_state, hidden, \
             created_at) VALUES (?1, ?2, ?2, 'a', 'indexed', ?3, ?4)",
            params![id, format!("P{id}"), i64::from(hidden), created],
        )
        .unwrap();
        c.execute(
            "INSERT INTO project_artists (project_id, position, artist_id) VALUES (?1, 0, ?2)",
            params![id, addr(tag)],
        )
        .unwrap();
        if tez > 0 {
            c.execute(
                "INSERT INTO project_volume (project_id, volume_mutez, primary_mutez, \
                 secondary_mutez, sales_count, unpriced_count, computed_at) \
                 VALUES (?1, ?2, ?2, 0, 1, 0, 'now')",
                params![id, tez * TEZ],
            )
            .unwrap();
        }
    }

    /// A token of project `project` minted by `tag` on `minted`.
    fn mint(&self, token_id: i64, project: i64, tag: &str, minted: &str) {
        self.c()
            .execute(
                "INSERT INTO tokens (contract, token_id, project_id, iteration, seed, minter, \
                 minted, artifact_uri) VALUES ('KT1x', ?1, ?2, ?1, 'oo', ?3, ?4, 'ipfs://x')",
                params![token_id, project, addr(tag), minted],
            )
            .unwrap();
    }

    fn sql(&self, statement: &str) {
        self.c().execute_batch(statement).unwrap();
    }

    fn assign(&mut self) -> seeded_archive::store::NamesNumbered {
        self.store.assign_name_numbers().unwrap()
    }

    fn shown(&self, tag: &str) -> Option<String> {
        self.store.profile(&addr(tag)).unwrap().name
    }

    fn numbers(&self, key: &str) -> Vec<(String, i64)> {
        let c = self.c();
        let mut st = c
            .prepare("SELECT address, number FROM name_numbers WHERE name_key = ?1 ORDER BY number")
            .unwrap();
        st.query_map([key], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap()
    }

    /// (address, name) of every `profile_names` row and the artist labels, in a stable order.
    fn index(&self) -> Index {
        let c = self.c();
        let names = c
            .prepare("SELECT address, name FROM profile_names ORDER BY address")
            .unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        let labels = c
            .prepare("SELECT rowid, name FROM artist_fts ORDER BY rowid")
            .unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        (names, labels)
    }

    /// The drained index equals a full rebuild.
    fn assert_index_is_rebuild(&mut self) {
        let drained = self.index();
        self.store.rebuild_search_names().unwrap();
        assert_eq!(drained, self.index());
    }
}

fn nums(rows: &[(String, i64)]) -> Vec<(&str, i64)> {
    rows.iter().map(|(a, n)| (a.as_str(), *n)).collect()
}

fn by_name(store: &Store, name: &str) -> Option<String> {
    match store.profile_by_name(name) {
        Ok(p) => Some(p.address),
        Err(StoreError::NotFound { .. }) => None,
        Err(e) => panic!("{e:?}"),
    }
}

#[test]
fn the_plain_name_goes_to_the_credited_artist_over_impostors() {
    let mut fx = Fx::new("names_credited_over_impostors");
    fx.user("U", "Zancan");
    fx.user("V", "ZANCAN ");
    fx.artist("A", "zancan");
    fx.project(1, "A", false, "2022-05-01T00:00:00.000Z", 0);
    // The impostors minted earlier than the artist's project; V is first seen, then U.
    fx.project(2, "A", false, "2023-01-01T00:00:00.000Z", 0);
    fx.mint(1, 2, "U", "2022-02-01");
    fx.mint(2, 2, "V", "2022-01-01");
    let r = fx.assign();
    assert_eq!((r.shared_names, r.numbered, r.assigned), (1, 3, 3));
    assert_eq!((r.plain_credited, r.plain_other), (1, 0));
    assert_eq!(fx.shown("A").as_deref(), Some("zancan"));
    assert_eq!(fx.shown("V").as_deref(), Some("ZANCAN-2"));
    assert_eq!(fx.shown("U").as_deref(), Some("Zancan-3"));
    assert_eq!(by_name(&fx.store, "ZANCAN").unwrap(), addr("A"));
    fx.assert_index_is_rebuild();
}

#[test]
fn among_credited_artists_the_highest_visible_volume_wins() {
    let mut fx = Fx::new("names_highest_volume");
    for t in ["A", "B", "C"] {
        fx.artist(t, "Foo");
    }
    fx.project(1, "A", false, "2021-01-01T00:00:00.000Z", 5);
    fx.project(2, "B", false, "2021-03-01T00:00:00.000Z", 9);
    // Hidden projects count for nothing: B's 100 tez do not, C's only visible one does.
    fx.project(3, "B", true, "2021-02-01T00:00:00.000Z", 100);
    fx.project(4, "C", false, "2021-04-01T00:00:00.000Z", 1);
    fx.assign();
    assert_eq!(fx.numbers("foo").len(), 3);
    assert_eq!(
        nums(&fx.numbers("foo")),
        [
            (addr("B").as_str(), 1),
            (addr("A").as_str(), 2),
            (addr("C").as_str(), 3)
        ]
    );
    assert_eq!(fx.shown("B").as_deref(), Some("Foo"));
    assert_eq!(fx.shown("A").as_deref(), Some("Foo-2"));
}

#[test]
fn without_credits_the_first_seen_address_wins() {
    let mut fx = Fx::new("names_first_seen");
    fx.artist("A", "Carrier"); // credited on a hidden project only: class d, seen 2020
    fx.project(1, "A", true, "2020-06-01T00:00:00.000Z", 0);
    fx.mint_project();
    for t in ["X", "Y", "Z"] {
        fx.user(t, "Carrier");
    }
    fx.mint(1, 9, "Z", "2021-05-05");
    fx.mint(2, 9, "X", "2022-05-05");
    // Y was never seen: last, whatever its address.
    let r = fx.assign();
    assert_eq!((r.plain_credited, r.plain_other), (0, 1));
    assert_eq!(
        nums(&fx.numbers("carrier")),
        [
            (addr("A").as_str(), 1),
            (addr("Z").as_str(), 2),
            (addr("X").as_str(), 3),
            (addr("Y").as_str(), 4)
        ]
    );
    assert_eq!(fx.shown("A").as_deref(), Some("Carrier"));
    assert_eq!(fx.shown("Y").as_deref(), Some("Carrier-4"));
}

#[test]
fn numbers_are_stable_never_reused_and_skip_real_names() {
    let mut fx = Fx::new("names_stability");
    fx.artist("A", "foo");
    fx.project(1, "A", false, "2022-01-01T00:00:00.000Z", 0);
    fx.user("R", "foo-2"); // a real name: 2 is not generated
    fx.user("U", "Foo");
    fx.user("V", "FOO");
    fx.mint(1, 1, "U", "2022-03-01");
    fx.mint(2, 1, "V", "2022-04-01");
    fx.assign();
    assert_eq!(
        nums(&fx.numbers("foo")),
        [
            (addr("A").as_str(), 1),
            (addr("U").as_str(), 3),
            (addr("V").as_str(), 4)
        ]
    );
    assert_eq!(fx.shown("R").as_deref(), Some("foo-2"));
    assert_eq!(fx.shown("U").as_deref(), Some("Foo-3"));
    // A new carrier seen first still takes the next number: existing rows keep theirs.
    fx.user("W", "foo");
    fx.mint(3, 1, "W", "2020-01-01");
    let r = fx.assign();
    assert_eq!((r.assigned, r.moved), (1, 0));
    assert_eq!(fx.shown("W").as_deref(), Some("foo-5"));
    assert_eq!(fx.shown("U").as_deref(), Some("Foo-3"));
    assert_eq!(fx.shown("V").as_deref(), Some("FOO-4"));
    // A carrier that disappears keeps its row, shows nothing, and its number is never reused.
    fx.sql(&format!(
        "DELETE FROM users WHERE address = '{}'",
        addr("U")
    ));
    let r = fx.assign();
    assert_eq!((r.assigned, r.moved, r.tombstones), (0, 0, 1));
    assert_eq!(fx.shown("U"), None);
    fx.user("X", "Foo");
    fx.assign();
    assert_eq!(fx.shown("X").as_deref(), Some("Foo-6"));
    assert_eq!(nums(&fx.numbers("foo"))[1], (addr("U").as_str(), 3));
    // The tombstone returns to its number.
    fx.user("U", "foo");
    let r = fx.assign();
    assert_eq!((r.assigned, r.moved, r.tombstones), (0, 0, 0));
    assert_eq!(fx.shown("U").as_deref(), Some("foo-3"));
    fx.assert_index_is_rebuild();
}

#[test]
fn an_alias_an_address_never_shows_numbers_no_one() {
    let mut fx = Fx::new("names_alias_not_a_carrier");
    fx.artist("A", "KetiTaki");
    fx.project(1, "A", false, "2022-01-01T00:00:00.000Z", 0);
    fx.user("U", "pixel");
    // A's objkt alias is "pixel", but its artist name shows first.
    fx.sql(&format!(
        "INSERT INTO profiles_imported (address, source, name, name_key, fetched_at) \
         VALUES ('{}', 'objkt', 'Pixel', 'pixel', 'now')",
        addr("A")
    ));
    let r = fx.assign();
    assert_eq!((r.shared_names, r.numbered), (0, 0));
    assert_eq!(fx.shown("U").as_deref(), Some("pixel"));
    assert_eq!(fx.shown("A").as_deref(), Some("KetiTaki"));
    assert_eq!(by_name(&fx.store, "pixel").unwrap(), addr("U"));
    // Dropping the higher name makes the alias A's shown name: now it competes.
    fx.sql(&format!(
        "UPDATE artists SET name = '{0}', name_key = lower('{0}') WHERE address = '{0}'",
        addr("A")
    ));
    let r = fx.assign();
    assert_eq!((r.shared_names, r.plain_credited), (1, 1));
    assert_eq!(fx.shown("A").as_deref(), Some("Pixel"));
    assert_eq!(fx.shown("U").as_deref(), Some("pixel-2"));
    fx.assert_index_is_rebuild();
}

#[test]
fn a_single_remaining_carrier_shows_the_plain_name() {
    let mut fx = Fx::new("names_single_carrier");
    fx.artist("A", "Solo");
    fx.project(1, "A", false, "2022-01-01T00:00:00.000Z", 0);
    fx.user("U", "solo");
    fx.assign();
    assert_eq!(fx.shown("U").as_deref(), Some("solo-2"));
    fx.sql(&format!(
        "UPDATE artists SET name = 'Other', name_key = 'other' WHERE address = '{}'",
        addr("A")
    ));
    fx.assign();
    assert_eq!(fx.shown("U").as_deref(), Some("solo"));
    assert_eq!(by_name(&fx.store, "solo").unwrap(), addr("U"));
    assert_eq!(by_name(&fx.store, "solo-2"), None, "no longer shown");
    // The rows stay: the plain holder is a tombstone, the survivor keeps number 2.
    assert_eq!(fx.numbers("solo").len(), 2);
    fx.assert_index_is_rebuild();
}

#[test]
fn a_later_credited_artist_takes_number_one_from_an_uncredited_holder() {
    let mut fx = Fx::new("names_class_rerank");
    fx.user("U", "Bar");
    fx.user("V", "bar");
    fx.mint_project();
    fx.mint(1, 9, "U", "2021-01-01");
    fx.mint(2, 9, "V", "2021-02-01");
    fx.assign();
    assert_eq!(fx.shown("U").as_deref(), Some("Bar"));
    assert_eq!(fx.shown("V").as_deref(), Some("bar-2"));
    // A credited artist appears: class c outranks class d; the displaced holder takes 3.
    fx.artist("A", "BAR");
    fx.project(1, "A", false, "2023-01-01T00:00:00.000Z", 1);
    let r = fx.assign();
    assert_eq!((r.assigned, r.moved), (1, 1));
    assert_eq!(
        nums(&fx.numbers("bar")),
        [
            (addr("A").as_str(), 1),
            (addr("V").as_str(), 2),
            (addr("U").as_str(), 3)
        ]
    );
    assert_eq!(fx.shown("A").as_deref(), Some("BAR"));
    assert_eq!(fx.shown("U").as_deref(), Some("Bar-3"));
    // Another credited artist with far more volume does not take it back: same class.
    fx.artist("B", "bar");
    fx.project(2, "B", false, "2023-02-01T00:00:00.000Z", 1000);
    fx.assign();
    assert_eq!(fx.shown("A").as_deref(), Some("BAR"));
    assert_eq!(fx.shown("B").as_deref(), Some("bar-4"));
    fx.assert_index_is_rebuild();
}

#[test]
fn an_existing_carrier_that_becomes_credited_swaps_with_the_holder() {
    let mut fx = Fx::new("names_class_swap");
    fx.user("U", "Baz");
    fx.user("V", "baz");
    fx.mint_project();
    fx.mint(1, 9, "U", "2021-01-01");
    fx.mint(2, 9, "V", "2021-02-01");
    fx.assign();
    fx.artist("V", "Baz");
    fx.project(1, "V", false, "2023-01-01T00:00:00.000Z", 0);
    let r = fx.assign();
    assert_eq!((r.assigned, r.moved), (0, 2));
    assert_eq!(fx.shown("V").as_deref(), Some("Baz"));
    assert_eq!(fx.shown("U").as_deref(), Some("Baz-2"));
}

#[test]
fn a_claimed_name_wins_and_a_curated_one_comes_second() {
    let mut fx = Fx::new("names_claimed_curated");
    let me = ED_B.0;
    fx.artist("A", "Aaa");
    fx.project(1, "A", false, "2020-01-01T00:00:00.000Z", 0);
    fx.sql(&format!(
        "INSERT INTO artists (id, name, name_key, address) VALUES ('{me}', 'Ed', 'ed', '{me}');\
         INSERT INTO project_artists (project_id, position, artist_id) VALUES (1, 1, '{me}')"
    ));
    // The claim is taken before anyone carries the name.
    fx.store
        .save_profile_edit(
            me,
            ProfileEdit {
                name: Some("Mine".into()),
                ..ProfileEdit::default()
            },
            EditProof {
                message: format!("seeded.art profile edit for {me}"),
                signature: "edsigtJ5aJVPNULkSdpVVuJ1FWdUNEH9JLQtCQRd4KfHd8TAs5S6Ybx".into(),
                public_key: ED_B.1.into(),
            },
        )
        .unwrap();
    fx.sql(&format!(
        "UPDATE artists SET name = 'mine', name_key = 'mine' WHERE address = '{}'",
        addr("A")
    ));
    fx.sql(&format!(
        "INSERT INTO identities (address, slug, name, aliases, provenance, name_key) \
         VALUES ('{}', 'mine-curated', 'MINE', '[]', 'test', 'mine')",
        addr("C")
    ));
    let r = fx.assign();
    assert_eq!((r.shared_names, r.plain_claimed), (1, 1));
    // The claim is plain; the others follow in first-seen order (A has a project, C none).
    assert_eq!(
        nums(&fx.numbers("mine")),
        [(me, 1), (addr("A").as_str(), 2), (addr("C").as_str(), 3)]
    );
    assert_eq!(fx.store.profile(me).unwrap().name.as_deref(), Some("Mine"));
    assert_eq!(fx.shown("A").as_deref(), Some("mine-2"));
    assert_eq!(fx.shown("C").as_deref(), Some("MINE-3"));
}

#[test]
fn a_claim_of_a_generated_name_is_refused() {
    let mut fx = Fx::new("names_claim_generated");
    fx.artist("A", "Qux");
    fx.project(1, "A", false, "2022-01-01T00:00:00.000Z", 0);
    fx.user("U", "qux");
    fx.assign();
    assert_eq!(fx.shown("U").as_deref(), Some("qux-2"));
    // The claimant has history (a visible credit), so only the name decides.
    let me = ED_B.0;
    fx.sql(&format!(
        "INSERT INTO artists (id, name, name_key, address) VALUES ('{me}', 'Ed', 'ed', '{me}');\
         INSERT INTO project_artists (project_id, position, artist_id) VALUES (1, 1, '{me}')"
    ));
    let edit = |name: &str| ProfileEdit {
        name: Some(name.into()),
        ..ProfileEdit::default()
    };
    for taken in ["qux-2", "QUX-2", "qux"] {
        match fx.store.check_profile_edit(me, &edit(taken)) {
            Err(StoreError::BadRequest(m)) => assert!(m.contains("taken"), "{m}"),
            other => panic!("{taken}: {other:?}"),
        }
    }
    fx.store.check_profile_edit(me, &edit("qux-3")).unwrap();
}

#[test]
fn a_real_name_equal_to_a_generated_one_is_numbered_from_two() {
    let mut fx = Fx::new("names_generated_collision");
    let objkt = |fx: &Fx, tag: &str, name: &str| {
        fx.sql(&format!(
            "INSERT INTO profiles_imported (address, source, name, name_key, fetched_at) \
             VALUES ('{}', 'objkt', '{name}', '{name}', 'now')",
            addr(tag)
        ))
    };
    fx.user("A", "alex");
    fx.user("B", "alex");
    fx.assign();
    assert_eq!(fx.shown("B").as_deref(), Some("alex-2"));
    // A later single carrier of B's generated name does not show it too.
    objkt(&fx, "C", "alex-2");
    fx.assign();
    assert_eq!(fx.shown("B").as_deref(), Some("alex-2"));
    assert_eq!(fx.shown("C").as_deref(), Some("alex-2-2"));
    assert_eq!(by_name(&fx.store, "alex-2").unwrap(), addr("B"));
    assert_eq!(by_name(&fx.store, "alex-2-2").unwrap(), addr("C"));
    // A second carrier: no number 1 for the generated key, so no UNIQUE failure.
    objkt(&fx, "D", "alex-2");
    fx.assign();
    assert_eq!(fx.shown("D").as_deref(), Some("alex-2-3"));
    assert_eq!(
        nums(&fx.numbers("alex-2")),
        [(addr("C").as_str(), 2), (addr("D").as_str(), 3)]
    );
    // Two carriers that arrived together, unnumbered (the state that failed every run).
    fx.user("E", "sam");
    fx.user("F", "sam");
    fx.assign();
    objkt(&fx, "G", "sam-2");
    objkt(&fx, "H", "sam-2");
    fx.assign();
    assert_eq!(fx.shown("F").as_deref(), Some("sam-2"));
    assert_eq!(fx.shown("G").as_deref(), Some("sam-2-2"));
    assert_eq!(fx.shown("H").as_deref(), Some("sam-2-3"));
    fx.assert_index_is_rebuild();
}

#[test]
fn numbered_names_resolve_display_and_search() {
    let mut fx = Fx::new("names_resolve_display_search");
    fx.artist("A", "Xan");
    fx.project(1, "A", false, "2022-01-01T00:00:00.000Z", 0);
    fx.user("U", "xan");
    fx.assign();
    assert_eq!(by_name(&fx.store, "Xan").unwrap(), addr("A"));
    assert_eq!(by_name(&fx.store, " XAN-2 ").unwrap(), addr("U"));
    assert_eq!(by_name(&fx.store, "xan-3"), None);
    let labels = fx
        .store
        .display_names(&[addr("A").as_str(), addr("U").as_str()])
        .unwrap();
    assert_eq!(labels[&addr("A")], "Xan");
    assert_eq!(labels[&addr("U")], "xan-2");
    let page = fx.store.profiles_search("xan", None, 10).unwrap();
    let hits: Vec<(&str, &str)> = page
        .items
        .iter()
        .map(|h| (h.address.as_str(), h.name.as_str()))
        .collect();
    assert_eq!(
        hits,
        [(addr("A").as_str(), "Xan"), (addr("U").as_str(), "xan-2")]
    );
    assert_eq!(page.total, 2);
    // The artist search finds the credited artist by its shown name.
    fx.assert_index_is_rebuild();
}

#[test]
fn assignment_is_idempotent() {
    let mut fx = Fx::new("names_idempotent");
    fx.artist("A", "Rep");
    fx.project(1, "A", false, "2022-01-01T00:00:00.000Z", 3);
    fx.user("U", "REP");
    fx.user("V", "rep");
    fx.user("W", "Lone");
    let first = fx.assign();
    assert_eq!((first.shared_names, first.assigned), (1, 3));
    let rows = fx.numbers("rep");
    let index = fx.index();
    let second = fx.assign();
    assert_eq!((second.assigned, second.moved), (0, 0));
    assert_eq!(
        (second.shared_names, second.numbered, second.tombstones),
        (first.shared_names, first.numbered, first.tombstones)
    );
    assert_eq!(fx.numbers("rep"), rows);
    assert_eq!(fx.index(), index);
    let queued: i64 = fx
        .c()
        .query_row("SELECT count(*) FROM search_dirty", [], |r| r.get(0))
        .unwrap();
    assert_eq!(queued, 0);
}

#[test]
fn a_v9_database_upgrades_to_v10_and_numbers_its_names() {
    let mut fx = Fx::new("names_upgrade_v9");
    fx.artist("A", "Old");
    fx.project(1, "A", false, "2022-01-01T00:00:00.000Z", 0);
    fx.user("U", "old");
    fx.user("V", "OLD");
    fx.assign();
    // Back to v9: no table, names shared (hidden in v9).
    fx.sql("DROP TABLE name_numbers; PRAGMA user_version = 9;");
    drop(fx.store);
    let mut store = Store::open(&fx.db).unwrap();
    let c = Connection::open(&fx.db).unwrap();
    let version: i64 = c
        .query_row("PRAGMA user_version", [], |r| r.get(0))
        .unwrap();
    assert_eq!(version, 10);
    let rows: Vec<(String, i64, String)> = c
        .prepare("SELECT address, number, shown_key FROM name_numbers ORDER BY number")
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert_eq!(
        rows,
        [
            (addr("A"), 1, "old".to_owned()),
            (addr("U"), 2, "old-2".to_owned()),
            (addr("V"), 3, "old-3".to_owned())
        ]
    );
    assert_eq!(
        store.profile(&addr("U")).unwrap().name.as_deref(),
        Some("old-2")
    );
    // The table is exactly the specified one.
    let sql: String = c
        .query_row(
            "SELECT sql FROM sqlite_schema WHERE name = 'name_numbers'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert!(
        sql.contains("STRICT") && sql.contains("WITHOUT ROWID"),
        "{sql}"
    );
    let again = store.assign_name_numbers().unwrap();
    assert_eq!((again.assigned, again.moved), (0, 0));
}

impl Fx {
    /// Project 9 with no credits, so tokens can be minted without crediting anyone.
    fn mint_project(&self) {
        self.c()
            .execute(
                "INSERT INTO projects (id, title, title_key, artist_key, index_state) \
                 VALUES (9, 'Minted', 'minted', 'a', 'indexed')",
                [],
            )
            .unwrap();
    }
}
