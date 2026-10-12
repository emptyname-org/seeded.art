//! Articles read API over a fixture sync: listing, search escaping, moderation, holders with
//! escrow credited to sellers, cursors, and opening rules.

#[path = "articles_support/mod.rs"]
mod support;

use seeded_archive::articles::store::{
    ArticleCursor, ArticleStore, ArticleStoreError, FetchState, MAX_QUERY_TERMS, fts_query,
};
use seeded_archive::articles::{ARTICLES_CONTRACT, MARKETPLACE_V3};
use support::{Harness, NameSource};

const FXHASH_ADMIN: &str = "tz1fepn7jZsCYBqCDhpM63hzh9g2Ytqk4Tpv";
const RICHER: &str = "tz1cNakLoctaqaCtBZSQGZs5RgAciYqB7cNf";
const TEZZARTS: &str = "tz1TSdoeS5udkYmVr1cEpErTqxGVrC3Kcd2J";

/// A fixture sync whose catalogue displays three registry names.
fn synced() -> Harness {
    let archive = support::scratch("store-names");
    support::name_catalogue(
        &archive,
        &[
            (FXHASH_ADMIN, NameSource::Registry, "fxhash"),
            (RICHER, NameSource::Registry, "richer"),
            (TEZZARTS, NameSource::Registry, "tezzarts"),
        ],
    );
    let mut h = Harness::new();
    h.sync(&support::with_archive(&archive)).unwrap();
    support::remove_db(&archive);
    h
}

fn ids(page: &seeded_archive::articles::store::ArticlePage) -> Vec<u64> {
    page.items.iter().map(|a| a.token_id).collect()
}

fn bad_request<T: std::fmt::Debug>(r: Result<T, ArticleStoreError>) -> String {
    match r {
        Err(ArticleStoreError::BadRequest(m)) => m,
        other => panic!("expected BadRequest, got {other:?}"),
    }
}

#[test]
fn listing_is_newest_first_without_hidden_articles_and_pages_by_cursor() {
    let h = synced();
    let first = h.store.articles_page(None, None, 4).unwrap();
    // #100 and #279 are moderated (MALICIOUS); #143 was reviewed back to NONE.
    assert_eq!(ids(&first), vec![835, 143, 21, 4]);
    assert_eq!(first.total, 6);
    let next = first.next.clone().unwrap();
    let second = h.store.articles_page(None, Some(&next), 4).unwrap();
    assert_eq!(ids(&second), vec![1, 0]);
    assert_eq!(second.next, None);
    let reparsed = ArticleCursor::parse(next.as_str()).unwrap();
    assert_eq!(
        ids(&h.store.articles_page(None, Some(&reparsed), 4).unwrap()),
        vec![1, 0]
    );
    let a = &first.items[3];
    assert_eq!(a.title.as_deref(), Some("gralis"));
    assert_eq!(a.author, TEZZARTS);
    assert_eq!(a.author_name.as_deref(), Some("tezzarts"));
    assert_eq!(a.body_state, FetchState::Ok);
    assert!(a.created_at.starts_with("2022-"));
}

#[test]
fn search_matches_title_author_name_address_and_tags() {
    let h = synced();
    let find = |q: &str| ids(&h.store.articles_page(Some(q), None, 10).unwrap());
    // #1's title, #835's tag.
    assert_eq!(find("beauty"), vec![835, 1]);
    assert_eq!(find("Beau cod"), vec![1], "prefix terms, ANDed");
    assert_eq!(find("richer"), vec![1], "displayed name");
    assert_eq!(find(&RICHER[..12]), vec![1], "address prefix");
    assert_eq!(find("markdown"), vec![0], "tag");
    assert_eq!(find("fxhash"), vec![0]);
    // Hidden articles never match.
    assert_eq!(find("Right Click Save"), Vec::<u64>::new());
    // Empty or separator-only text is no filter.
    assert_eq!(
        h.store.articles_page(Some(" -* "), None, 10).unwrap().total,
        6
    );
    let page = h.store.articles_page(Some("beauty"), None, 1).unwrap();
    assert_eq!((page.total, ids(&page)), (2, vec![835]));
    let rest = h
        .store
        .articles_page(Some("beauty"), page.next.as_ref(), 1)
        .unwrap();
    assert_eq!((ids(&rest), rest.next), (vec![1], None));
}

#[test]
fn search_text_is_escaped_and_capped_like_the_catalogue() {
    let h = synced();
    // FTS5 syntax is inert: operators, quotes, column filters, NEAR and parentheses.
    for q in [
        "a OR b",
        "\"beauty",
        "title:beauty",
        "NEAR(beauty code)",
        "beauty)",
        "* ^ - :",
        "AND",
        "' ; DROP TABLE articles; --",
    ] {
        h.store.articles_page(Some(q), None, 10).unwrap();
    }
    assert_eq!(
        ids(&h
            .store
            .articles_page(Some("title:beauty"), None, 10)
            .unwrap()),
        Vec::<u64>::new(),
        "\"title\" is a term here, not a column filter"
    );
    let eight = "a b c d e f g h";
    assert_eq!(eight.split(' ').count(), MAX_QUERY_TERMS);
    h.store.articles_page(Some(eight), None, 10).unwrap();
    assert_eq!(
        bad_request(h.store.articles_page(Some("a b c d e f g h i"), None, 10)),
        "too many search terms"
    );
    assert!(bad_request(h.store.articles_page(Some(&"x".repeat(201)), None, 10)).contains("200"));
    assert_eq!(
        fts_query("a OR b").unwrap().as_deref(),
        Some("\"a\" \"OR\"* \"b\"")
    );
    assert_eq!(fts_query("é").unwrap().as_deref(), Some("\"é\""));
    assert_eq!(
        fts_query("garden\"").unwrap().as_deref(),
        Some("\"garden\"*")
    );
    for empty in ["", "   ", "*", "-", "\"\""] {
        assert_eq!(fts_query(empty).unwrap(), None, "{empty:?}");
    }
}

#[test]
fn hidden_and_unknown_articles_are_not_found() {
    let h = synced();
    for id in [100, 279, 999_999, u64::MAX] {
        assert!(
            matches!(h.store.article(id), Err(ArticleStoreError::NotFound { .. })),
            "{id}"
        );
        assert!(
            matches!(
                h.store.article_holders(id),
                Err(ArticleStoreError::NotFound { .. })
            ),
            "{id}"
        );
    }
    assert_eq!(h.store.article(143).unwrap().flag, "NONE");
}

#[test]
fn article_returns_raw_markdown_splits_and_provenance() {
    let h = synced();
    let a = h.store.article(1).unwrap();
    assert_eq!(a.summary.title.as_deref(), Some("Beauty in Code"));
    assert_eq!(a.splits.len(), 2);
    assert_eq!(a.splits[0].address, RICHER);
    assert_eq!(a.splits[0].share, 500);
    assert_eq!(a.royalties, Some(160));
    assert!(a.revisions >= 10, "{}", a.revisions);
    assert!(a.metadata_uri.as_deref().unwrap().starts_with("ipfs://Qm"));
    assert_eq!(a.metadata_source.as_deref(), Some("ipfs"));
    let text = a.body.text.unwrap();
    assert!(text.contains("Yazid") || !text.is_empty());
    assert_eq!(a.body.verification.as_deref(), Some("ipfs-car"));
    assert_eq!(a.listed, 109);
    // Media lists are derived from the stored body.
    for m in &a.media {
        assert!(!m.src.is_empty());
    }
}

#[test]
fn holders_credit_escrowed_editions_to_their_sellers() {
    let h = synced();
    let holders = h.store.article_holders(4).unwrap();
    assert!(holders.iter().all(|x| x.address != MARKETPLACE_V3));
    let seller = holders.iter().find(|x| x.address == TEZZARTS).unwrap();
    assert_eq!(seller.listed, 89);
    assert!(seller.amount >= 89);
    assert_eq!(seller.name.as_deref(), Some("tezzarts"));
    let total: u64 = holders.iter().map(|x| x.amount).sum();
    assert_eq!(total, h.store.article(4).unwrap().summary.editions);
    // Largest first.
    assert!(holders.windows(2).all(|w| w[0].amount >= w[1].amount));

    let holders = h.store.article_holders(0).unwrap();
    let admin = holders.iter().find(|x| x.address == FXHASH_ADMIN).unwrap();
    assert_eq!(admin.listed, 1940);
    assert_eq!(holders.iter().map(|x| x.amount).sum::<u64>(), 2022);
    assert_eq!(h.store.article(0).unwrap().holders, holders.len() as u64);
}

#[test]
fn articles_by_author_matches_the_minter_only() {
    let h = synced();
    let page = h.store.articles_by_author(RICHER, None, None, 10).unwrap();
    assert_eq!(ids(&page), vec![1]);
    assert_eq!(page.total, 1);
    // A royalty-split recipient is not the author.
    let split_only = "tz1QgjmhrUD3X7kgS9mMHbUz4cS6uDiFGhAU";
    assert_eq!(
        h.store
            .articles_by_author(split_only, None, None, 10)
            .unwrap()
            .total,
        0
    );
    // The minter of only hidden articles has no visible ones.
    let hidden_minter = "tz1RcQM4Vgi1fkNTFTJtgBAxf7zqaJCStXt4";
    assert_eq!(
        h.store
            .articles_by_author(hidden_minter, None, None, 10)
            .unwrap()
            .total,
        0
    );
    for bad in [
        "",
        "tz1short",
        "zz1fepn7jZsCYBqCDhpM63hzh9g2Ytqk4Tpv",
        "tz1 '; --",
    ] {
        bad_request(h.store.articles_by_author(bad, None, None, 10));
    }
    bad_request(h.store.articles_by_author(ARTICLES_CONTRACT, None, None, 0));
    bad_request(
        h.store
            .articles_by_author(ARTICLES_CONTRACT, None, None, 101),
    );
}

#[test]
fn cursors_only_resume_their_own_listing() {
    let h = synced();
    let all = h.store.articles_page(None, None, 1).unwrap().next.unwrap();
    bad_request(h.store.articles_page(Some("beauty"), Some(&all), 1));
    bad_request(h.store.articles_by_author(RICHER, None, Some(&all), 1));
    let mut tampered = all.as_str().to_owned();
    let last = tampered.pop().unwrap();
    tampered.push(if last == '0' { '1' } else { '0' });
    bad_request(ArticleCursor::parse(&tampered));
    bad_request(ArticleCursor::parse("XYZ"));
    bad_request(ArticleCursor::parse(""));
}

#[test]
fn read_only_open_requires_an_initialised_articles_database() {
    let dir = support::fixtures().join("../../../tmp");
    let base = if dir.is_dir() {
        dir
    } else {
        std::env::current_dir().unwrap().join("tmp")
    };
    std::fs::create_dir_all(&base).unwrap();
    let path = base.join(format!("articles-open-test-{}.sqlite", std::process::id()));
    let _ = std::fs::remove_file(&path);
    // Missing file: SQLite cannot open read-only.
    assert!(ArticleStore::open_read_only(&path).is_err());
    // A foreign database (e.g. the catalogue) is refused, read-only and read-write.
    {
        let c = rusqlite::Connection::open(&path).unwrap();
        c.execute_batch("CREATE TABLE projects (id INTEGER PRIMARY KEY);")
            .unwrap();
    }
    assert!(matches!(
        ArticleStore::open_read_only(&path),
        Err(ArticleStoreError::Unavailable(_)) | Err(ArticleStoreError::Internal(_))
    ));
    assert!(matches!(
        ArticleStore::open(&path),
        Err(ArticleStoreError::Internal(_))
    ));
    std::fs::remove_file(&path).unwrap();
    // A fresh file opened read-write becomes a valid, empty articles database.
    {
        let s = ArticleStore::open(&path).unwrap();
        assert_eq!(s.stats().unwrap().articles, 0);
    }
    let ro = ArticleStore::open_read_only(&path).unwrap();
    assert_eq!(ro.articles_page(None, None, 10).unwrap().total, 0);
    drop(ro);
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
    }
}

#[test]
fn counts_by_author_counts_visible_articles_only() {
    let h = synced();
    let hidden_minter = "tz1RcQM4Vgi1fkNTFTJtgBAxf7zqaJCStXt4";
    let counts = h
        .store
        .counts_by_author(&[RICHER, hidden_minter, TEZZARTS])
        .unwrap();
    assert_eq!(counts.get(RICHER), Some(&1));
    assert!(!counts.contains_key(hidden_minter));
    let tezzarts = h
        .store
        .articles_by_author(TEZZARTS, None, None, 100)
        .unwrap()
        .total;
    assert_eq!(counts.get(TEZZARTS).copied().unwrap_or(0), tezzarts);
    assert!(h.store.counts_by_author(&[]).unwrap().is_empty());
    let too_many = vec![RICHER; seeded_archive::articles::store::MAX_AUTHOR_COUNTS + 1];
    assert!(h.store.counts_by_author(&too_many).is_err());
}
