//! Per-project trading volume: what counts, idempotent refresh, failures that keep the old
//! table, and the "Highest volume" listing (keyset and offset) across ties and zero volume.

use std::fs;
use std::path::{Path, PathBuf};

use rusqlite::{Connection, params};
use seeded_archive::import::import_fixtures;
use seeded_archive::model::ProjectVolume;
use seeded_archive::sales::schema::open_writer;
use seeded_archive::store::volume::VolumeRefresh;
use seeded_archive::store::{
    Cursor, KeysetPage, PageRequest, ProjectFilter, ProjectQuery, ProjectSort, Store, StoreError,
};

const TEZ: i64 = 1_000_000;
const V1: &str = "KT1KEa8z6vWXDJrVqtMrAeDVzsvxat3kHaCE";
const ISSUER: &str = "KT1BJC12dG17CVvPKJ1VYaNnaT5mzfnUTwXv";
const FXHASH_V2: &str = "KT1GbyoDi7H1sfXmimXpptZJuCdHMh66WS9u";
const OBJKT_V2: &str = "KT1WvzYHCNBvDSdwafTHv7nJ1dWmZ8GCYuuC";
/// A token that is neither tez nor wXTZ.
const PEPE: &str = "KT1HMpujRLwyiMKfEaB3QeTKHrFGPBofznUT";
const BUYER: &str = "tz1N64TWNCZR3yJZmmzBE4DRidN1tCaZCw8Q";
const SELLER: &str = "tz1PkLH6efZifS8NtKiZBWFWgDoHDmDwN9f1";

const GARDEN: u64 = 2969;
const BUGGED: u64 = 15063;
const ETHEREAL: u64 = 8514;
const CHARCOAL: u64 = 26659;
const DRAGONS: u64 = 2613;
const RGB: u64 = 2;
const SMOLSKULL: u64 = 14;
const ARBOREAL: u64 = 29437;

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

struct Fixture {
    store: Store,
    db: PathBuf,
    sales: PathBuf,
}

fn token(c: &Connection, token_id: i64, project: u64, iteration: i64) {
    c.execute(
        "INSERT INTO tokens (contract, token_id, project_id, iteration, seed, minter, minted, \
         artifact_uri) VALUES (?1, ?2, ?3, ?4, 'oo', ?5, '2022-01-01', 'ipfs://x')",
        params![V1, token_id, project as i64, iteration, BUYER],
    )
    .unwrap();
}

/// A catalogued token without a chain row (minted, never signed).
fn unsigned(c: &Connection, token_id: i64, project: i64) {
    c.execute(
        "INSERT INTO catalogue_tokens (contract, token_id, project_id, chain_state, \
         verify_state) VALUES (?1, ?2, ?3, 'present', 'unsigned')",
        params![V1, token_id, project],
    )
    .unwrap();
}

fn sale(c: &Connection, op: i64, token_id: i64, kind: &str, price: Option<i64>, currency: &str) {
    let (market, seller) = match (kind, currency) {
        ("mint", _) => (ISSUER, None),
        (_, "wxtz") => (OBJKT_V2, Some(SELLER)),
        _ => (FXHASH_V2, Some(SELLER)),
    };
    c.execute(
        "INSERT INTO sales (contract, token_id, kind, price_mutez, currency, seller, buyer, \
         marketplace, op_hash, op_id, level, timestamp) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, '2022-01-01T00:00:00Z')",
        params![
            V1,
            token_id,
            kind,
            price,
            currency,
            seller,
            BUYER,
            market,
            format!("op{op}"),
            op,
            100 + op
        ],
    )
    .unwrap();
}

/// Fixture projects with these trades:
/// - Garden: mint 10, sale 25, offer accept 5 in wXTZ (40 tez);
/// - Bugged Forest: two mints of 20 (40); Ethereal Microcosm: one sale of 40 (40);
/// - Charcoal Seeds: auction 7 in wXTZ, a sale paid in another token, and a 3 tez mint of an
///   unsigned (catalogue-only) token (10);
/// - Dragons: a free mint (0); RGB: only a sale paid in another token (0);
/// - SMOLSKULL, flagged: mint 1000;
/// - not mapped: a sale of a token the archive lacks, one of a catalogue row for no project.
fn fixture(name: &str) -> Fixture {
    let dir = test_dir(name);
    let db = dir.join("db.sqlite");
    let mut store = Store::open(&db).unwrap();
    assert!(
        import_fixtures(&mut store, &root().join("assets/fixtures"))
            .unwrap()
            .changed
    );
    let c = Connection::open(&db).unwrap();
    for (id, project, iteration) in [
        (5001, GARDEN, 1),
        (5002, BUGGED, 1),
        (5003, BUGGED, 2),
        (5004, ETHEREAL, 1),
        (5005, CHARCOAL, 1),
        (5007, DRAGONS, 1),
        (5008, RGB, 1),
        (5009, SMOLSKULL, 1),
    ] {
        token(&c, id, project, iteration);
    }
    unsigned(&c, 5006, CHARCOAL as i64);
    unsigned(&c, 5010, 777_777);
    c.execute(
        "UPDATE projects SET hidden = 1 WHERE id = ?1",
        [SMOLSKULL as i64],
    )
    .unwrap();
    let sales = dir.join("sales.sqlite");
    let s = open_writer(&sales).unwrap();
    sale(&s, 1, 5001, "mint", Some(10 * TEZ), "tez");
    sale(&s, 2, 5001, "sale", Some(25 * TEZ), "tez");
    sale(&s, 3, 5001, "offer_accept", Some(5 * TEZ), "wxtz");
    sale(&s, 4, 5002, "mint", Some(20 * TEZ), "tez");
    sale(&s, 5, 5003, "mint", Some(20 * TEZ), "tez");
    sale(&s, 6, 5004, "sale", Some(40 * TEZ), "tez");
    sale(&s, 7, 5005, "auction", Some(7 * TEZ), "wxtz");
    sale(&s, 8, 5005, "sale", None, PEPE);
    sale(&s, 9, 5006, "mint", Some(3 * TEZ), "tez");
    sale(&s, 10, 5007, "mint", Some(0), "tez");
    sale(&s, 11, 5008, "sale", None, PEPE);
    sale(&s, 12, 5009, "mint", Some(1000 * TEZ), "tez");
    sale(&s, 13, 999_999, "sale", Some(3 * TEZ), "tez");
    sale(&s, 14, 5010, "sale", Some(2 * TEZ), "tez");
    s.execute_batch(
        "INSERT INTO cursors (stream, cursor_id, synced_level, updated_at) VALUES
           ('mint:a', 1, 120, 0), ('market:b', 1, 110, 0)",
    )
    .unwrap();
    Fixture { store, db, sales }
}

fn tez(n: i64) -> u64 {
    (n * TEZ) as u64
}

fn sales_conn(f: &Fixture) -> Connection {
    Connection::open(&f.sales).unwrap()
}

/// A stored row without its timestamp: id, volume, primary, secondary, priced, unpriced and
/// sales level.
type StoredRow = (i64, i64, i64, i64, i64, i64, Option<i64>);

/// Every stored row, in id order.
fn rows(db: &Path) -> Vec<StoredRow> {
    Connection::open(db)
        .unwrap()
        .prepare(
            "SELECT project_id, volume_mutez, primary_mutez, secondary_mutez, sales_count, \
             unpriced_count, sales_level FROM project_volume ORDER BY project_id",
        )
        .unwrap()
        .query_map([], |r| {
            Ok((
                r.get(0)?,
                r.get(1)?,
                r.get(2)?,
                r.get(3)?,
                r.get(4)?,
                r.get(5)?,
                r.get(6)?,
            ))
        })
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap()
}

fn volume(store: &Store, id: u64) -> ProjectVolume {
    store.project_volume(id).unwrap().unwrap()
}

fn sums(v: &ProjectVolume) -> (u64, u64, u64, u64, u64) {
    (
        v.volume_mutez,
        v.primary_mutez,
        v.secondary_mutez,
        v.sales_count,
        v.unpriced_count,
    )
}

#[test]
fn volume_counts_mints_and_secondary_trades_priced_in_tez_or_wxtz() {
    let mut f = fixture("volume_counts");
    let r = f.store.refresh_volumes(&f.sales).unwrap();
    assert_eq!(
        r,
        VolumeRefresh {
            projects: 7,
            visible_projects: 6,
            volume_mutez: tez(1130),
            primary_mutez: tez(1053),
            secondary_mutez: tez(77),
            visible_volume_mutez: tez(130),
            sales_count: 10,
            unpriced_count: 2,
            catalogue_rows: 1,
            unmapped_rows: 2,
            unmapped_mutez: tez(5),
            sales_level: Some(110),
            computed_at: r.computed_at.clone(),
        }
    );
    assert_eq!(r.computed_at.len(), 20, "{}", r.computed_at);
    let garden = volume(&f.store, GARDEN);
    assert_eq!(sums(&garden), (tez(40), tez(10), tez(30), 3, 0));
    assert_eq!(garden.sales_level, Some(110));
    assert_eq!(garden.computed_at, r.computed_at);
    assert_eq!(sums(&volume(&f.store, BUGGED)), (tez(40), tez(40), 0, 2, 0));
    assert_eq!(
        sums(&volume(&f.store, ETHEREAL)),
        (tez(40), 0, tez(40), 1, 0)
    );
    // The unsigned token's mint counts through the catalogue; the token-paid sale does not.
    assert_eq!(
        sums(&volume(&f.store, CHARCOAL)),
        (tez(10), tez(3), tez(7), 2, 1)
    );
    // Trades without volume still have a row; projects without trades have none.
    assert_eq!(sums(&volume(&f.store, DRAGONS)), (0, 0, 0, 1, 0));
    assert_eq!(sums(&volume(&f.store, RGB)), (0, 0, 0, 0, 1));
    assert_eq!(f.store.project_volume(ARBOREAL).unwrap(), None);
    for missing in [SMOLSKULL, 123_456, u64::MAX] {
        assert!(
            matches!(
                f.store.project_volume(missing),
                Err(StoreError::NotFound { .. })
            ),
            "{missing}"
        );
    }
    // A stream that never completed a pass leaves the level unknown.
    sales_conn(&f)
        .execute(
            "UPDATE cursors SET synced_level = NULL WHERE stream = 'mint:a'",
            [],
        )
        .unwrap();
    assert_eq!(f.store.refresh_volumes(&f.sales).unwrap().sales_level, None);
    assert_eq!(volume(&f.store, GARDEN).sales_level, None);
}

#[test]
fn volume_refresh_is_idempotent_and_replaces_the_whole_table() {
    let mut f = fixture("volume_idempotent");
    let first = f.store.refresh_volumes(&f.sales).unwrap();
    let stored = rows(&f.db);
    assert_eq!(stored.len(), 7);
    let second = f.store.refresh_volumes(&f.sales).unwrap();
    assert_eq!(rows(&f.db), stored);
    assert_eq!(
        VolumeRefresh {
            computed_at: String::new(),
            ..first
        },
        VolumeRefresh {
            computed_at: String::new(),
            ..second
        }
    );
    // A project whose trades are gone loses its row; the others are rewritten.
    sales_conn(&f)
        .execute("DELETE FROM sales WHERE op_id = 6", [])
        .unwrap();
    let third = f.store.refresh_volumes(&f.sales).unwrap();
    assert_eq!(third.projects, 6);
    assert_eq!(f.store.project_volume(ETHEREAL).unwrap(), None);
    assert_eq!(rows(&f.db).len(), 6);
}

#[test]
fn volume_refresh_reads_the_sales_file_without_writing_it() {
    let mut f = fixture("volume_read_only");
    // Settle the writer's WAL so the main file holds everything.
    drop(sales_conn(&f));
    let before = fs::read(&f.sales).unwrap();
    f.store.refresh_volumes(&f.sales).unwrap();
    assert_eq!(fs::read(&f.sales).unwrap(), before);
}

#[test]
fn failed_refreshes_keep_the_previous_table() {
    let mut f = fixture("volume_failures");
    f.store.refresh_volumes(&f.sales).unwrap();
    let stored = rows(&f.db);
    let dir = f.db.parent().unwrap().to_owned();

    let missing = dir.join("missing.sqlite");
    assert!(matches!(
        f.store.refresh_volumes(&missing),
        Err(StoreError::Unavailable(_))
    ));
    let empty = dir.join("empty.sqlite");
    fs::write(&empty, b"").unwrap();
    assert!(matches!(
        f.store.refresh_volumes(&empty),
        Err(StoreError::Unavailable(_))
    ));
    // The archive itself is not a sales database.
    assert!(matches!(
        f.store.refresh_volumes(&f.db),
        Err(StoreError::BadRequest(_))
    ));
    assert_eq!(rows(&f.db), stored);

    // Sums past i64 fail instead of wrapping: within one project's trades, and once the
    // catalogue-only mint is added to a project at the limit.
    sales_conn(&f)
        .execute(
            "UPDATE sales SET price_mutez = ?1 WHERE op_id = 4",
            [i64::MAX],
        )
        .unwrap();
    assert!(matches!(
        f.store.refresh_volumes(&f.sales),
        Err(StoreError::Internal(_))
    ));
    assert_eq!(rows(&f.db), stored);
    sales_conn(&f)
        .execute_batch(&format!(
            "UPDATE sales SET price_mutez = {} WHERE op_id = 4;
             UPDATE sales SET price_mutez = {} WHERE op_id = 7;",
            20 * TEZ,
            i64::MAX - 1
        ))
        .unwrap();
    assert!(matches!(
        f.store.refresh_volumes(&f.sales),
        Err(StoreError::Internal(_))
    ));
    assert_eq!(rows(&f.db), stored);

    // Every failure detached the sales file again: a good refresh still works.
    sales_conn(&f)
        .execute(
            "UPDATE sales SET price_mutez = ?1 WHERE op_id = 7",
            [7 * TEZ],
        )
        .unwrap();
    f.store.refresh_volumes(&f.sales).unwrap();
    assert_eq!(rows(&f.db), stored);
}

fn all() -> PageRequest {
    PageRequest {
        offset: 0,
        limit: 100,
    }
}

fn filter(text: Option<&str>, artist: Option<&str>) -> ProjectFilter {
    ProjectFilter {
        text: text.map(str::to_owned),
        artist: artist.map(str::to_owned),
        sort: ProjectSort::VolumeDesc,
    }
}

/// Follows `next` to the end, checking that `total` never changes.
fn traverse(mut page: impl FnMut(Option<&Cursor>) -> KeysetPage<u64>) -> Vec<u64> {
    let mut out = Vec::new();
    let mut cursor: Option<Cursor> = None;
    let mut total = None;
    for _ in 0..1000 {
        let p = page(cursor.as_ref());
        assert_eq!(*total.get_or_insert(p.total), p.total);
        out.extend(p.items);
        match p.next {
            Some(c) => cursor = Some(c),
            None => {
                assert_eq!(out.len() as u64, p.total, "every row once");
                return out;
            }
        }
    }
    panic!("cursor never ends: {out:?}");
}

fn keyset_ids(store: &Store, f: &ProjectFilter, limit: u32) -> Vec<u64> {
    traverse(|c| {
        let p = store.projects_page(f, c, limit).unwrap();
        KeysetPage {
            items: p.items.iter().map(|s| s.id).collect(),
            next: p.next,
            total: p.total,
        }
    })
}

fn offset_ids(store: &Store, f: &ProjectFilter, limit: u32) -> Vec<u64> {
    let mut out = Vec::new();
    for offset in (0..).step_by(limit as usize) {
        let p = store
            .projects(&ProjectQuery {
                text: f.text.clone(),
                artist: f.artist.clone(),
                sort: f.sort,
                page: PageRequest { offset, limit },
            })
            .unwrap();
        if p.items.is_empty() {
            assert_eq!(out.len() as u64, p.total);
            return out;
        }
        out.extend(p.items.iter().map(|s| s.id));
    }
    unreachable!()
}

#[test]
fn highest_volume_lists_every_visible_project_once_across_ties_and_zero_volume() {
    let mut f = fixture("volume_listing");
    f.store.refresh_volumes(&f.sales).unwrap();
    let visible: Vec<u64> = f
        .store
        .projects(&ProjectQuery {
            text: None,
            artist: None,
            sort: ProjectSort::IdAsc,
            page: all(),
        })
        .unwrap()
        .items
        .iter()
        .map(|p| p.id)
        .collect();
    assert_eq!(visible.len(), 24, "SMOLSKULL is flagged");
    // Three-way tie at 40 tez by id, then Charcoal, then every other project by id: zero
    // rows (Dragons, RGB) and projects without trades mixed.
    let ranked = [GARDEN, ETHEREAL, BUGGED, CHARCOAL];
    let mut expected = ranked.to_vec();
    expected.extend(visible.iter().filter(|id| !ranked.contains(id)));
    for limit in [1, 2, 3, 4, 5, 7, 100] {
        assert_eq!(
            keyset_ids(&f.store, &filter(None, None), limit),
            expected,
            "keyset limit {limit}"
        );
        assert_eq!(
            offset_ids(&f.store, &filter(None, None), limit),
            expected,
            "offset limit {limit}"
        );
    }
    for (text, artist, want) in [
        (Some("garden"), None, vec![GARDEN, 11104]),
        (None, Some("ciphrd"), vec![ETHEREAL, RGB]),
        (None, Some("zancan"), vec![GARDEN, BUGGED, CHARCOAL]),
        (Some("forest"), Some("zancan"), vec![BUGGED]),
    ] {
        for limit in [1, 2, 100] {
            assert_eq!(keyset_ids(&f.store, &filter(text, artist), limit), want);
            assert_eq!(offset_ids(&f.store, &filter(text, artist), limit), want);
        }
    }
    // A refresh between pages (same data) changes nothing; a flagged project left unranked
    // shows again, with its stored volume, once unflagged.
    let first = f.store.projects_page(&filter(None, None), None, 5).unwrap();
    f.store.refresh_volumes(&f.sales).unwrap();
    let mut ids: Vec<u64> = first.items.iter().map(|p| p.id).collect();
    let mut cursor = first.next;
    while let Some(c) = cursor {
        let p = f
            .store
            .projects_page(&filter(None, None), Some(&c), 5)
            .unwrap();
        ids.extend(p.items.iter().map(|p| p.id));
        cursor = p.next;
    }
    assert_eq!(ids, expected);
    Connection::open(&f.db)
        .unwrap()
        .execute(
            "UPDATE projects SET hidden = 0 WHERE id = ?1",
            [SMOLSKULL as i64],
        )
        .unwrap();
    assert_eq!(keyset_ids(&f.store, &filter(None, None), 3)[0], SMOLSKULL);
    assert_eq!(volume(&f.store, SMOLSKULL).volume_mutez, tez(1000));
}

#[test]
fn highest_volume_cursors_resume_only_their_own_listing() {
    let mut f = fixture("volume_cursors");
    f.store.refresh_volumes(&f.sales).unwrap();
    let ranked = f
        .store
        .projects_page(&filter(None, None), None, 2)
        .unwrap()
        .next
        .unwrap();
    // Past the ranked projects: the cursor sits in the zero-volume part.
    let unranked = f
        .store
        .projects_page(&filter(None, None), None, 6)
        .unwrap()
        .next
        .unwrap();
    for c in [&ranked, &unranked] {
        assert!(
            f.store
                .projects_page(&filter(None, None), Some(c), 2)
                .is_ok()
        );
        for other in [
            ProjectFilter {
                sort: ProjectSort::IdAsc,
                ..filter(None, None)
            },
            ProjectFilter {
                sort: ProjectSort::TitleAsc,
                ..filter(None, None)
            },
            ProjectFilter {
                sort: ProjectSort::IdDesc,
                ..filter(None, None)
            },
            filter(Some("garden"), None),
            filter(None, Some("zancan")),
        ] {
            assert!(
                matches!(
                    f.store.projects_page(&other, Some(c), 2),
                    Err(StoreError::BadRequest(_))
                ),
                "{other:?}"
            );
        }
    }
    let by_id = f
        .store
        .projects_page(
            &ProjectFilter {
                sort: ProjectSort::IdAsc,
                ..filter(None, None)
            },
            None,
            2,
        )
        .unwrap()
        .next
        .unwrap();
    assert!(matches!(
        f.store.projects_page(&filter(None, None), Some(&by_id), 2),
        Err(StoreError::BadRequest(_))
    ));
}
