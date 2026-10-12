use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use seeded_archive::import::{ImportError, import_fixtures};
use seeded_archive::model::{
    FeatureShare, IndexState, Owner, Project, ProjectSummary, Snapshot, Token,
};
use seeded_archive::store::{
    ArtistQuery, Page, PageRequest, ProjectQuery, ProjectSort, Store, StoreError,
};
use serde_json::Value;

const ARBOREAL: u64 = 29437;
const FX_PARAMS: &str = "KT1EfsNuqwLAWDd3o4pvfUx1CAh5GMdTrRvr";
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
        assert!(dir.pop(), "project root with assets/fixtures not found");
    }
    dir
}

fn fixtures() -> PathBuf {
    root().join("assets/fixtures")
}

/// Fresh per-test directory under tmp/claude-crate/testdata/.
fn test_dir(name: &str) -> PathBuf {
    let dir = root().join("tmp/claude-crate/testdata").join(name);
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

fn fixture_copy(dir: &Path) -> PathBuf {
    let copy = dir.join("fixtures");
    copy_dir(&fixtures(), &copy);
    copy
}

fn edit_json(path: &Path, edit: impl FnOnce(&mut Value)) {
    let mut v: Value = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
    edit(&mut v);
    fs::write(path, serde_json::to_vec_pretty(&v).unwrap()).unwrap();
}

fn page(offset: u32, limit: u32) -> PageRequest {
    PageRequest { offset, limit }
}

fn all() -> PageRequest {
    page(0, 100)
}

fn query(text: Option<&str>, sort: ProjectSort, page: PageRequest) -> ProjectQuery {
    ProjectQuery {
        text: text.map(str::to_owned),
        artist: None,
        sort,
        page,
    }
}

fn search(store: &Store, text: &str) -> Result<Page<ProjectSummary>, StoreError> {
    store.projects(&query(Some(text), ProjectSort::IdAsc, all()))
}

fn id_set(p: &Page<ProjectSummary>) -> BTreeSet<u64> {
    assert_eq!(p.total as usize, p.items.len());
    p.items.iter().map(|s| s.id).collect()
}

fn set(ids: &[u64]) -> BTreeSet<u64> {
    ids.iter().copied().collect()
}

fn imported(dir: &Path) -> Store {
    let mut store = Store::open(&dir.join("db.sqlite")).unwrap();
    let report = import_fixtures(&mut store, &fixtures()).unwrap();
    assert!(report.changed);
    store
}

/// Everything observable, for before/after comparisons.
#[derive(Debug, PartialEq)]
struct Dump {
    projects: Page<ProjectSummary>,
    arboreal: Project,
    tokens: Page<Token>,
    shares: Vec<FeatureShare>,
    artists: Vec<seeded_archive::model::ArtistSummary>,
    emptyname: seeded_archive::model::Artist,
    snapshot: Snapshot,
}

fn dump(store: &Store) -> Dump {
    Dump {
        projects: store
            .projects(&query(None, ProjectSort::IdAsc, all()))
            .unwrap(),
        arboreal: store.project(ARBOREAL).unwrap(),
        tokens: store.tokens_for_project(ARBOREAL, all()).unwrap(),
        shares: store.feature_shares(ARBOREAL).unwrap(),
        artists: store
            .artists(&ArtistQuery {
                text: None,
                page: all(),
            })
            .unwrap()
            .items,
        emptyname: store.artist("emptyname").unwrap(),
        snapshot: store.snapshot().unwrap(),
    }
}

#[test]
fn real_fixtures_import_with_expected_projects_tokens_and_owners() {
    let dir = test_dir("real_fixtures");
    let store = imported(&dir);

    let projects = store
        .projects(&query(None, ProjectSort::IdAsc, all()))
        .unwrap();
    assert_eq!(projects.total, 25);
    for p in &projects.items {
        if p.id == ARBOREAL {
            assert_eq!(p.editions, Some(10));
            assert_eq!(p.index_state, IndexState::Indexed);
        } else {
            assert_eq!(p.editions, None, "{}", p.id);
            assert_eq!(p.index_state, IndexState::NotIndexed, "{}", p.id);
        }
        assert!(p.preview.as_deref().unwrap().starts_with("previews/"));
    }

    let arboreal = store.project(ARBOREAL).unwrap();
    assert_eq!(arboreal.title, "Arboreal");
    assert_eq!(arboreal.artists.len(), 1);
    assert_eq!(arboreal.artists[0].id, "emptyname");
    assert_eq!(arboreal.artists[0].name, "EmptyName");
    assert_eq!(arboreal.contract.as_deref(), Some(FX_PARAMS));
    assert_eq!(
        arboreal.issuer.as_deref(),
        Some("KT1Xpmp15KfqoePNW9HczFmqaGNHwadV2a3b")
    );
    assert_eq!(
        arboreal.artifact_uri.as_deref(),
        Some("onchfs://a71e98fced01c865f33be138100b5ea5486f32fc333ad1503f814b201234130c")
    );
    assert_eq!(arboreal.first_mint.as_deref(), Some("2023-11-10"));
    assert_eq!(arboreal.last_mint.as_deref(), Some("2024-03-25"));
    assert!(
        arboreal
            .description
            .unwrap()
            .starts_with("The tree, algorithmically born")
    );

    let tokens = store.tokens_for_project(ARBOREAL, all()).unwrap();
    assert_eq!(tokens.total, 10);
    let iterations: Vec<u32> = tokens.items.iter().map(|t| t.iteration).collect();
    assert_eq!(iterations, (1..=10).collect::<Vec<_>>());

    let t = store.token(FX_PARAMS, 185776).unwrap();
    assert_eq!(t.iteration, 9);
    assert_eq!(t.project_id, ARBOREAL);
    assert_eq!(
        t.seed,
        "ooj6NosZ31zpAekeUueJah7J8sLRTYfEeLhcAhMTg3ErjdSXCje"
    );
    assert_eq!(t.minter, "tz1N64TWNCZR3yJZmmzBE4DRidN1tCaZCw8Q");
    assert_eq!(t.minted, "2024-03-12");
    assert_eq!(t.preview.as_deref(), Some("previews/arboreal/9.webp"));
    assert_eq!(t.features[0], ("Palette".to_owned(), "Light".to_owned()));
    assert_eq!(t.features.len(), 9);
    let owner = t.owner.as_ref().unwrap();
    assert_eq!(
        owner.owner,
        Owner {
            address: "tz1N64TWNCZR3yJZmmzBE4DRidN1tCaZCw8Q".into(),
            alias: Some("quadrado".into()),
        }
    );
    assert_eq!(owner.as_of, "2026-10-02");
    assert_eq!(owner.source, "objkt data.objkt.com/v3/graphql");
    assert_eq!(store.token_by_iteration(ARBOREAL, 9).unwrap(), t);

    // Token 1 has an unaliased owner; the full address must survive.
    let first = store.token_by_iteration(ARBOREAL, 1).unwrap();
    let first_owner = first.owner.unwrap().owner;
    assert_eq!(first_owner.address, "tz2RXosjAZdS85oCrguy7Eom757144dcU1FT");
    assert_eq!(first_owner.alias, None);

    let shares = store.feature_shares(ARBOREAL).unwrap();
    let light = shares
        .iter()
        .find(|s| s.name == "Palette" && s.value == "Light")
        .unwrap();
    assert_eq!((light.count, light.total), (7, 10));
    assert_eq!(shares[0].name, "Palette");
    let palette_total: u32 = shares
        .iter()
        .filter(|s| s.name == "Palette")
        .map(|s| s.count)
        .sum();
    assert_eq!(palette_total, 10);
    assert!(store.feature_shares(2969).unwrap().is_empty());

    let holdings = store.artist_known_holdings("emptyname", all()).unwrap();
    assert_eq!(holdings.total, 1);
    let held: Vec<(&str, u64)> = holdings
        .items
        .iter()
        .map(|t| (t.contract.as_str(), t.token_id))
        .collect();
    assert_eq!(held, vec![(FX_PARAMS, 133460)]);
    assert_eq!(
        store.artist_known_holdings("zancan", all()).unwrap().total,
        0
    );

    let emptyname = store.artist("emptyname").unwrap();
    assert_eq!(emptyname.website, None);
    assert_eq!(emptyname.aliases, vec!["EmptyName".to_owned()]);
    let zancan = store.artist_projects("zancan", all()).unwrap();
    assert_eq!(
        zancan.items.iter().map(|p| p.id).collect::<Vec<_>>(),
        vec![2969, 15063, 26659]
    );
    let collab = store.artist("zancan-and-yazid").unwrap();
    assert_eq!(collab.name, "zancan and Yazid");

    let artists = store
        .artists(&ArtistQuery {
            text: None,
            page: all(),
        })
        .unwrap();
    assert_eq!(artists.total, 22);
    let z = artists.items.iter().find(|a| a.id == "zancan").unwrap();
    assert_eq!(z.project_count, 3);

    let snap = store.snapshot().unwrap();
    assert_eq!(snap.as_of, "2026-10-02");
    assert_eq!(
        snap.catalogue_source,
        "KilledByAPixel/fxhashArchive summary.json (MIT)"
    );
    assert_eq!(snap.fixture_sha256.len(), 64);
    assert!(snap.fixture_sha256.bytes().all(|b| b.is_ascii_hexdigit()));
    assert_eq!(snap.imported_at.len(), "2026-10-03T00:00:00Z".len());
}

#[test]
fn in_memory_store_imports_and_empty_store_reports_no_snapshot() {
    let mut store = Store::open_in_memory().unwrap();
    assert!(matches!(store.snapshot(), Err(StoreError::NotFound { .. })));
    let empty = store
        .projects(&query(None, ProjectSort::IdAsc, all()))
        .unwrap();
    assert_eq!((empty.total, empty.items.len()), (0, 0));
    let report = import_fixtures(&mut store, &fixtures()).unwrap();
    assert_eq!(
        (report.projects, report.tokens, report.artists),
        (25, 10, 22)
    );
    assert!(report.changed);
    assert_eq!(
        store.snapshot().unwrap().fixture_sha256,
        report.fixture_sha256
    );
}

#[test]
fn reimport_is_a_noop_and_data_survives_restart() {
    let dir = test_dir("reimport_restart");
    let db = dir.join("db.sqlite");
    let mut store = imported(&dir);
    let before = dump(&store);

    let again = import_fixtures(&mut store, &fixtures()).unwrap();
    assert!(!again.changed);
    assert_eq!(again.fixture_sha256, before.snapshot.fixture_sha256);
    assert_eq!(dump(&store), before, "no-op import must not write");

    drop(store);
    let reopened = Store::open(&db).unwrap();
    assert_eq!(dump(&reopened), before);
    drop(reopened);
    let read_only = Store::open_read_only(&db).unwrap();
    assert_eq!(dump(&read_only), before);
}

/// `feature_counts` rows with a count, against a recount of the fixture features.
fn feature_drift(dir: &Path) -> (i64, i64) {
    let c = rusqlite::Connection::open(dir.join("db.sqlite")).unwrap();
    c.query_row(
        "WITH truth AS (SELECT t.project_id, tf.name, tf.value, tf.position, count(*) AS n \
         FROM token_features tf JOIN tokens t ON t.contract = tf.contract \
         AND t.token_id = tf.token_id GROUP BY 1, 2, 3, 4) \
         SELECT (SELECT count(*) FROM truth), \
         (SELECT count(*) FROM truth t LEFT JOIN feature_counts f \
           USING (project_id, name, value, position) WHERE f.count IS NOT t.n) \
         + (SELECT count(*) FROM feature_counts f WHERE f.count <> 0 AND NOT EXISTS \
           (SELECT 1 FROM truth t WHERE (t.project_id, t.name, t.value, t.position) \
             = (f.project_id, f.name, f.value, f.position)))",
        [],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )
    .unwrap()
}

#[test]
fn feature_counts_follow_reimports_that_replace_tokens() {
    let dir = test_dir("feature_counts_reimport");
    let mut store = imported(&dir);
    let (rows, drift) = feature_drift(&dir);
    assert!(rows > 0);
    assert_eq!(drift, 0);
    let before = store.feature_shares(ARBOREAL).unwrap();

    let copy = fixture_copy(&dir);
    edit_json(&copy.join("tokens.json"), |v| {
        for t in v.as_array_mut().unwrap() {
            if t["project_id"] == ARBOREAL {
                t["features"] = serde_json::json!([{"name": "Palette", "value": "Light"}]);
            }
        }
    });
    assert!(import_fixtures(&mut store, &copy).unwrap().changed);
    assert_eq!(feature_drift(&dir).1, 0);
    let light = store.feature_shares(ARBOREAL).unwrap();
    assert_eq!(light.len(), 1);
    assert_eq!((light[0].count, light[0].total), (10, 10));

    assert!(import_fixtures(&mut store, &fixtures()).unwrap().changed);
    assert_eq!(feature_drift(&dir), (rows, 0));
    assert_eq!(store.feature_shares(ARBOREAL).unwrap(), before);
}

#[test]
fn changed_fixtures_replace_data_and_report_changed() {
    let dir = test_dir("changed_fixtures");
    let mut store = imported(&dir);
    let old = store.snapshot().unwrap();
    let copy = fixture_copy(&dir);
    edit_json(&copy.join("tokens.json"), |v| {
        v[1]["owner"] = serde_json::json!({"address": "tz1N64TWNCZR3yJZmmzBE4DRidN1tCaZCw8Q", "alias": "quadrado"});
    });
    let report = import_fixtures(&mut store, &copy).unwrap();
    assert!(report.changed);
    assert_ne!(report.fixture_sha256, old.fixture_sha256);
    assert_eq!(
        store
            .artist_known_holdings("emptyname", all())
            .unwrap()
            .total,
        0
    );
    assert_eq!(
        store.snapshot().unwrap().fixture_sha256,
        report.fixture_sha256
    );

    // A changed preview image alone changes the hash.
    fs::write(copy.join("previews/arboreal/1.webp"), b"not the same bytes").unwrap();
    let report2 = import_fixtures(&mut store, &copy).unwrap();
    assert!(report2.changed);
    assert_ne!(report2.fixture_sha256, report.fixture_sha256);
}

/// Imports a mutated copy over the real data and checks that it is rejected and nothing changed.
fn assert_rejected(name: &str, file: &str, needle: &str, mutate: impl FnOnce(&Path)) {
    let dir = test_dir(name);
    let mut store = imported(&dir);
    let before = dump(&store);
    let copy = fixture_copy(&dir);
    mutate(&copy);
    match import_fixtures(&mut store, &copy) {
        Err(ImportError::Invalid { file: f, reason }) => {
            assert_eq!(f, file, "{reason}");
            assert!(reason.contains(needle), "{reason:?} lacks {needle:?}");
        }
        other => panic!("expected Invalid for {name}, got {other:?}"),
    }
    assert_eq!(dump(&store), before, "prior data must stay intact");
}

#[test]
fn rejects_duplicate_token_id() {
    assert_rejected(
        "dup_token",
        "tokens.json",
        "duplicate (contract, token_id)",
        |d| {
            edit_json(&d.join("tokens.json"), |v| {
                v[1]["token_id"] = v[0]["token_id"].clone()
            });
        },
    );
}

#[test]
fn rejects_iteration_above_editions() {
    assert_rejected("iteration_range", "tokens.json", "outside 1..=10", |d| {
        edit_json(&d.join("tokens.json"), |v| v[9]["iteration"] = 11.into());
    });
}

#[test]
fn rejects_preview_with_parent_component() {
    assert_rejected(
        "preview_parent",
        "tokens.json",
        "without '.' or '..'",
        |d| {
            edit_json(&d.join("tokens.json"), |v| {
                v[0]["preview"] = "previews/../../fixtures/previews/arboreal/1.webp".into();
            });
        },
    );
}

#[test]
fn rejects_missing_preview_file() {
    assert_rejected("preview_missing", "tokens.json", "does not exist", |d| {
        fs::remove_file(d.join("previews/arboreal/3.webp")).unwrap();
    });
}

#[test]
fn rejects_artifact_seed_mismatch() {
    assert_rejected(
        "seed_mismatch",
        "tokens.json",
        "fxhash does not match seed",
        |d| {
            edit_json(&d.join("tokens.json"), |v| {
                v[0]["seed"] = v[1]["seed"].clone()
            });
        },
    );
}

#[test]
fn rejects_bad_contract() {
    assert_rejected("bad_contract", "tokens.json", "not a KT1 address", |d| {
        edit_json(&d.join("tokens.json"), |v| {
            v[0]["contract"] = "KT1EfsNuqwLAWDd3o4pvfUx1CAh5GMdTrRv0".into();
        });
    });
}

#[test]
fn rejects_token_count_below_editions() {
    assert_rejected(
        "count_below_editions",
        "projects.json",
        "9 tokens for 10 editions",
        |d| {
            edit_json(&d.join("tokens.json"), |v| {
                v.as_array_mut().unwrap().pop();
            });
        },
    );
}

#[test]
fn rejects_unknown_artist_and_zero_editions() {
    assert_rejected("unknown_artist", "projects.json", "unknown artist", |d| {
        edit_json(&d.join("projects.json"), |v| {
            v[0]["artists"] = serde_json::json!(["nobody"])
        });
    });
    assert_rejected(
        "zero_editions",
        "projects.json",
        "editions must be positive",
        |d| {
            edit_json(&d.join("projects.json"), |v| v[0]["editions"] = 0.into());
        },
    );
}

#[test]
fn rejects_preview_symlink_escaping_fixture_dir() {
    assert_rejected(
        "preview_symlink",
        "tokens.json",
        "outside the fixture directory",
        |d| {
            let outside = d.parent().unwrap().join("outside.webp");
            fs::write(&outside, b"x").unwrap();
            let link = d.join("previews/arboreal/2.webp");
            fs::remove_file(&link).unwrap();
            std::os::unix::fs::symlink(&outside, &link).unwrap();
        },
    );
}

#[test]
fn failure_during_write_rolls_back_to_previous_data() {
    let dir = test_dir("write_rollback");
    let db = dir.join("db.sqlite");
    let mut store = imported(&dir);
    let before = dump(&store);
    let copy = fixture_copy(&dir);
    edit_json(&copy.join("snapshot.json"), |v| {
        v["as_of"] = "2026-10-03".into()
    });

    let injector = rusqlite::Connection::open(&db).unwrap();
    injector
        .execute_batch(
            "CREATE TRIGGER fail_last_token BEFORE INSERT ON tokens WHEN NEW.iteration = 10 \
             BEGIN SELECT RAISE(ABORT, 'injected'); END;",
        )
        .unwrap();
    let err = import_fixtures(&mut store, &copy).unwrap_err();
    assert!(
        matches!(err, ImportError::Store(StoreError::Internal(_))),
        "{err:?}"
    );
    assert_eq!(dump(&store), before);

    injector
        .execute_batch("DROP TRIGGER fail_last_token")
        .unwrap();
    assert!(import_fixtures(&mut store, &copy).unwrap().changed);
    assert_eq!(store.snapshot().unwrap().as_of, "2026-10-03");
}

#[test]
fn search_finds_projects_and_treats_operators_literally() {
    let dir = test_dir("search");
    let store = imported(&dir);

    let zancan = set(&[2969, 15063, 11104, 26659]);
    assert_eq!(id_set(&search(&store, "zancan").unwrap()), zancan);
    assert_eq!(id_set(&search(&store, "ZANCAN").unwrap()), zancan);
    assert_eq!(
        id_set(&search(&store, "zãncan").unwrap()),
        zancan,
        "diacritics removed"
    );
    assert_eq!(
        id_set(&search(&store, "zan").unwrap()),
        zancan,
        "prefix match"
    );
    assert_eq!(
        id_set(&search(&store, "-zancan").unwrap()),
        zancan,
        "'-' is not NOT"
    );

    let garden = set(&[2969, 11104]);
    assert_eq!(id_set(&search(&store, "garden").unwrap()), garden);
    assert_eq!(id_set(&search(&store, "garden\"").unwrap()), garden);
    assert_eq!(id_set(&search(&store, "^garden").unwrap()), garden);
    assert_eq!(id_set(&search(&store, "(kinder)").unwrap()), set(&[11104]));
    assert_eq!(
        id_set(&search(&store, "garden zancan").unwrap()),
        garden,
        "AND across columns"
    );
    assert_eq!(
        id_set(&search(&store, "garden zancan yazid").unwrap()),
        set(&[11104])
    );

    // Operators are literal prefix terms, so these AND an extra word that does not occur.
    for text in [
        "NEAR",
        "garden OR zancan",
        "NEAR(garden zancan)",
        "title:garden",
        "garden NOT zancan",
    ] {
        assert_eq!(search(&store, text).unwrap().total, 0, "{text}");
    }
    let a_or_b = search(&store, "a OR b").unwrap();
    assert_eq!(a_or_b, search(&store, "a or b").unwrap());
    // One character matches a whole word only ("zan" above is a prefix).
    assert_eq!(search(&store, "z").unwrap().total, 0);
    match search(&store, "garden zancan yazid a b c d e f") {
        Err(StoreError::BadRequest(m)) => assert_eq!(m, "too many search terms"),
        other => panic!("{other:?}"),
    }
    assert!(search(&store, "garden zancan yazid a b c d e").is_ok());

    for text in ["*", "-", "§§", "\"\"", "   ", "^:*"] {
        let p = search(&store, text).unwrap();
        assert_eq!((p.total, p.items.len()), (0, 0), "{text:?}");
    }
    for text in [
        "ा",
        "\u{301}",
        "©",
        "١٢٣",
        "🌳",
        "ǅ",
        "\u{200b}",
        "a\u{0}b",
        "'; DROP TABLE projects; --",
    ] {
        assert!(search(&store, text).is_ok(), "{text:?}");
    }

    assert!(matches!(
        search(&store, &"a".repeat(201)),
        Err(StoreError::BadRequest(_))
    ));
    assert!(search(&store, &format!("  {}  ", "a".repeat(200))).is_ok());

    let mut q = query(Some("garden"), ProjectSort::IdAsc, all());
    q.artist = Some("zancan".into());
    assert_eq!(id_set(&store.projects(&q).unwrap()), set(&[2969]));
    q.text = None;
    assert_eq!(
        id_set(&store.projects(&q).unwrap()),
        set(&[2969, 15063, 26659])
    );
    q.artist = Some("zancan-and-yazid".into());
    assert_eq!(id_set(&store.projects(&q).unwrap()), set(&[11104]));
    q.artist = Some("nope".into());
    assert_eq!(store.projects(&q).unwrap().total, 0);

    let artists = |text: &str| {
        store
            .artists(&ArtistQuery {
                text: Some(text.into()),
                page: all(),
            })
            .unwrap()
            .items
            .into_iter()
            .map(|a| a.id)
            .collect::<Vec<_>>()
    };
    assert_eq!(
        artists("iskra"),
        vec!["iskravelitchkova", "zachlieberman-and-iskravelitchkova"]
    );
    assert_eq!(artists("emptyname"), vec!["emptyname"]);
    assert!(artists("§§").is_empty());
}

#[test]
fn pagination_is_bounded_complete_and_stable() {
    let dir = test_dir("pagination");
    let store = imported(&dir);

    for bad in [page(0, 0), page(0, 101), page(10_001, 10)] {
        let q = query(None, ProjectSort::IdAsc, bad);
        assert!(
            matches!(store.projects(&q), Err(StoreError::BadRequest(_))),
            "{bad:?}"
        );
        assert!(matches!(
            store.tokens_for_project(ARBOREAL, bad),
            Err(StoreError::BadRequest(_))
        ));
        assert!(matches!(
            store.artists(&ArtistQuery {
                text: None,
                page: bad
            }),
            Err(StoreError::BadRequest(_))
        ));
    }
    let far = store
        .projects(&query(None, ProjectSort::IdAsc, page(10_000, 100)))
        .unwrap();
    assert_eq!((far.total, far.items.len()), (25, 0));

    let walk = |sort: ProjectSort| {
        let mut out = Vec::new();
        let mut offset = 0;
        loop {
            let p = store.projects(&query(None, sort, page(offset, 7))).unwrap();
            assert_eq!(p.total, 25);
            assert!(p.items.len() <= 7);
            if p.items.is_empty() {
                break;
            }
            out.extend(p.items);
            offset += 7;
        }
        out
    };
    for sort in SORTS {
        let items = walk(sort);
        let ids: Vec<u64> = items.iter().map(|p| p.id).collect();
        assert_eq!(ids.len(), 25, "{sort:?}");
        assert_eq!(
            ids.iter().copied().collect::<BTreeSet<_>>().len(),
            25,
            "{sort:?}"
        );
        assert_eq!(
            walk(sort).iter().map(|p| p.id).collect::<Vec<_>>(),
            ids,
            "stable {sort:?}"
        );
        let key = |p: &ProjectSummary| match sort {
            ProjectSort::TitleAsc => (p.title.to_lowercase(), p.id),
            ProjectSort::ArtistAsc => (p.artists[0].name.to_lowercase(), p.id),
            ProjectSort::IdDesc => (String::new(), u64::MAX - p.id),
            ProjectSort::IdAsc | ProjectSort::VolumeDesc => (String::new(), p.id),
        };
        assert!(
            items.windows(2).all(|w| key(&w[0]) < key(&w[1])),
            "{sort:?} order"
        );
    }

    let mut artist_ids = Vec::new();
    for offset in (0..).step_by(7) {
        let p = store
            .artists(&ArtistQuery {
                text: None,
                page: page(offset, 7),
            })
            .unwrap();
        assert_eq!(p.total, 22);
        if p.items.is_empty() {
            break;
        }
        artist_ids.extend(p.items.into_iter().map(|a| (a.name.to_lowercase(), a.id)));
    }
    assert_eq!(artist_ids.len(), 22);
    assert!(artist_ids.windows(2).all(|w| w[0] < w[1]));

    let tokens = store.tokens_for_project(ARBOREAL, page(7, 7)).unwrap();
    assert_eq!(tokens.total, 10);
    assert_eq!(
        tokens.items.iter().map(|t| t.iteration).collect::<Vec<_>>(),
        vec![8, 9, 10]
    );
}

#[test]
fn missing_keys_are_not_found_and_bad_contract_is_bad_request() {
    let dir = test_dir("not_found");
    let store = imported(&dir);
    let nf = |r: Result<(), StoreError>| matches!(r, Err(StoreError::NotFound { .. }));

    assert!(nf(store.project(1).map(drop)));
    assert!(nf(store.project(u64::MAX).map(drop)));
    assert!(nf(store.token(FX_PARAMS, 1).map(drop)));
    assert!(nf(store.token(FX_PARAMS, u64::MAX).map(drop)));
    assert!(nf(store.token_by_iteration(ARBOREAL, 11).map(drop)));
    assert!(nf(store.token_by_iteration(1, 1).map(drop)));
    assert!(nf(store.tokens_for_project(1, all()).map(drop)));
    assert!(nf(store.feature_shares(1).map(drop)));
    assert!(nf(store.artist("nope").map(drop)));
    assert!(nf(store.artist_projects("nope", all()).map(drop)));
    assert!(nf(store.artist_known_holdings("nope", all()).map(drop)));
    match store.project(1) {
        Err(StoreError::NotFound { what, key }) => {
            assert_eq!((what, key.as_str()), ("project", "1"))
        }
        other => panic!("{other:?}"),
    }

    for contract in [
        "abc",
        "",
        "KT1EfsNuqwLAWDd3o4pvfUx1CAh5GMdTrRv0",
        "tz1N64TWNCZR3yJZmmzBE4DRidN1tCaZCw8Q",
    ] {
        assert!(
            matches!(store.token(contract, 1), Err(StoreError::BadRequest(_))),
            "{contract:?}"
        );
    }
}

#[test]
fn held_write_lock_maps_to_unavailable_and_keeps_data() {
    let dir = test_dir("unavailable");
    let db = dir.join("db.sqlite");
    let mut store = imported(&dir);
    let before = dump(&store);
    let copy = fixture_copy(&dir);
    edit_json(&copy.join("snapshot.json"), |v| {
        v["as_of"] = "2026-10-03".into()
    });

    let blocker = rusqlite::Connection::open(&db).unwrap();
    blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
    let err = import_fixtures(&mut store, &copy).unwrap_err();
    assert!(
        matches!(err, ImportError::Store(StoreError::Unavailable(_))),
        "{err:?}"
    );
    assert_eq!(
        dump(&store),
        before,
        "WAL readers are not blocked by the writer"
    );
    // An unchanged import needs no lock.
    assert!(!import_fixtures(&mut store, &fixtures()).unwrap().changed);
    blocker.execute_batch("ROLLBACK").unwrap();

    assert!(import_fixtures(&mut store, &copy).unwrap().changed);
    assert_eq!(store.snapshot().unwrap().as_of, "2026-10-03");

    let missing = Store::open_read_only(&dir.join("missing.sqlite"));
    assert!(matches!(missing, Err(StoreError::Unavailable(_))));
}

#[test]
fn read_snapshot_keeps_one_view_while_another_connection_replaces_data() {
    let dir = test_dir("read_snapshot");
    let db = dir.join("db.sqlite");
    drop(imported(&dir));
    let copy = fixture_copy(&dir);
    edit_json(&copy.join("projects.json"), |v| {
        let arboreal = v
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|p| p["id"] == ARBOREAL)
            .unwrap();
        arboreal["title"] = "Arboreal Renamed".into();
    });
    let reader = Store::open(&db).unwrap();
    let mut writer = Store::open(&db).unwrap();
    let original = reader.project(ARBOREAL).unwrap().title;
    assert_ne!(original, "Arboreal Renamed");
    let renamed = |s: &Store| search(s, "renamed").map(|p| id_set(&p));

    let seen = reader
        .read_snapshot(|s| {
            let first = s.project(ARBOREAL)?.title;
            let report = import_fixtures(&mut writer, &copy).unwrap();
            assert!(report.changed);
            assert_eq!(
                writer.project(ARBOREAL).unwrap().title,
                "Arboreal Renamed",
                "the writer committed"
            );
            let second = s.project(ARBOREAL)?.title;
            let nested = s.read_snapshot(|s| s.project(ARBOREAL))?.title;
            Ok((first, second, nested, renamed(s)?))
        })
        .unwrap();
    assert_eq!(
        seen,
        (
            original.clone(),
            original.clone(),
            original.clone(),
            BTreeSet::new()
        )
    );

    let after = reader
        .read_snapshot(|s| Ok((s.project(ARBOREAL)?.title, renamed(s)?)))
        .unwrap();
    assert_eq!(after, ("Arboreal Renamed".to_owned(), set(&[ARBOREAL])));
    assert_eq!(reader.project(ARBOREAL).unwrap().title, "Arboreal Renamed");
}

#[test]
fn read_snapshot_error_rolls_back_and_store_stays_usable() {
    let dir = test_dir("read_snapshot_error");
    let mut store = imported(&dir);
    let before = dump(&store);

    let err = store
        .read_snapshot(|s| {
            s.project(ARBOREAL)?;
            s.project(999_999_999).map(|_| ())
        })
        .unwrap_err();
    assert!(matches!(err, StoreError::NotFound { .. }), "{err:?}");
    let err = store
        .read_snapshot(|s| -> Result<(), StoreError> {
            s.tokens_for_project(ARBOREAL, all())?;
            Err(StoreError::BadRequest("stop".into()))
        })
        .unwrap_err();
    assert_eq!(err, StoreError::BadRequest("stop".into()));

    assert_eq!(dump(&store), before);
    assert_eq!(store.read_snapshot(|s| Ok(dump(s))).unwrap(), before);
    // The same connection can start a write: no read transaction was left open.
    let copy = fixture_copy(&dir);
    edit_json(&copy.join("snapshot.json"), |v| {
        v["as_of"] = "2026-10-03".into()
    });
    assert!(import_fixtures(&mut store, &copy).unwrap().changed);
    assert_eq!(
        store.read_snapshot(|s| s.snapshot()).unwrap().as_of,
        "2026-10-03"
    );
}
