//! Read API: the sales database attached read-only to an archive connection.
//!
//! The archive here is a minimal file with the columns the API reads (`tokens.contract`,
//! `tokens.token_id`, `tokens.project_id`, `projects.id`, `projects.hidden`).

mod sales_support;

use std::path::{Path, PathBuf};

use rusqlite::{Connection, OpenFlags};
use sales_support::{FakeTzkt, PARAMS, V1, V2, api, test_dir};
use seeded_archive::sales::indexer::{SyncOptions, sync};
use seeded_archive::sales::store::{
    self, attach, coverage, project_mint_summary, project_sales_summary, sales_for_token,
};
use seeded_archive::sales::{SaleKind, SalesError, schema};

const VISIBLE: u64 = 1;
const FLAGGED: u64 = 2;

/// A synced sales database and a minimal archive: project 1 (visible) holds three tokens,
/// project 2 (flagged) one.
fn setup(name: &str) -> (PathBuf, PathBuf) {
    let dir = test_dir(name);
    let sales = dir.join("sales.db");
    let fake = FakeTzkt::new(16_000_000);
    let opts = SyncOptions {
        limit: None,
        concurrency: 2,
        page: 10_000,
        confirmations: 2,
    };
    let r = sync(&sales, &api(&fake), &opts, &|_| {}).unwrap();
    assert!(r.streams.iter().all(|s| s.done));
    let archive = dir.join("archive.db");
    let conn = Connection::open(&archive).unwrap();
    conn.execute_batch(&format!(
        "CREATE TABLE projects (id INTEGER PRIMARY KEY, hidden INTEGER NOT NULL);
         CREATE TABLE tokens (contract TEXT NOT NULL, token_id INTEGER NOT NULL,
           project_id INTEGER NOT NULL, PRIMARY KEY (contract, token_id)) WITHOUT ROWID;
         CREATE INDEX tokens_project ON tokens (project_id);
         INSERT INTO projects VALUES ({VISIBLE}, 0), ({FLAGGED}, 1);
         INSERT INTO tokens VALUES
           ('{V1}', 243771, {VISIBLE}),
           ('{V2}', 590146, {VISIBLE}),
           ('{PARAMS}', 16408, {VISIBLE}),
           ('{V2}', 862456, {FLAGGED});"
    ))
    .unwrap();
    (archive, sales)
}

fn read_only(archive: &Path) -> Connection {
    Connection::open_with_flags(
        archive,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .unwrap()
}

#[test]
fn a_token_history_is_newest_first_and_includes_its_mint() {
    let (archive, sales) = setup("store-token");
    let conn = read_only(&archive);
    attach(&conn, &sales).unwrap();
    // Bought at 50 tez, then sold into a 100 tez offer in the same group.
    let h = sales_for_token(&conn, V1, 243_771, 20).unwrap();
    assert_eq!(h.len(), 2);
    assert_eq!(h[0].kind, SaleKind::OfferAccept);
    assert_eq!(h[0].price_mutez, Some(100_000_000));
    assert_eq!(h[0].marketplace, "objkt.com marketplace v2");
    assert_eq!(
        h[0].marketplace_contract,
        "KT1WvzYHCNBvDSdwafTHv7nJ1dWmZ8GCYuuC"
    );
    assert_eq!(h[1].kind, SaleKind::Sale);
    assert_eq!(h[1].price_mutez, Some(50_000_000));
    assert_eq!(sales_for_token(&conn, V1, 243_771, 1).unwrap().len(), 1);
    assert_eq!(sales_for_token(&conn, V1, 243_771, 0).unwrap().len(), 1);

    let mint = sales_for_token(&conn, PARAMS, 16_408, 20).unwrap();
    assert_eq!(mint.len(), 1);
    assert_eq!(mint[0].kind, SaleKind::Mint);
    assert_eq!(mint[0].price_mutez, Some(16_000_000));
    assert_eq!(mint[0].marketplace, "fxhash issuer fx(params)");
    assert_eq!(mint[0].seller, None);
    assert_eq!(mint[0].currency, "tez");
}

#[test]
fn flagged_and_unknown_tokens_have_no_history() {
    let (archive, sales) = setup("store-flagged");
    let conn = read_only(&archive);
    attach(&conn, &sales).unwrap();
    // Sold on fxhash, but its project is flagged.
    assert!(sales_for_token(&conn, V2, 862_456, 20).unwrap().is_empty());
    // Sold, but not in the archive.
    assert!(sales_for_token(&conn, V2, 747_114, 20).unwrap().is_empty());
    assert_eq!(project_sales_summary(&conn, FLAGGED).unwrap(), None);
    assert_eq!(project_sales_summary(&conn, 999).unwrap(), None);
    assert_eq!(project_mint_summary(&conn, FLAGGED).unwrap(), None);
}

#[test]
fn a_project_summary_counts_secondary_sales_and_mints_apart() {
    let (archive, sales) = setup("store-summary");
    let conn = read_only(&archive);
    attach(&conn, &sales).unwrap();
    let s = project_sales_summary(&conn, VISIBLE).unwrap().unwrap();
    assert_eq!(s.count, 2);
    assert_eq!(s.volume_mutez, 150_000_000);
    // The token's latest sale (100 tez) is the only token with secondary sales.
    assert_eq!(s.floor_last_sold_mutez, Some(100_000_000));
    let last = s.last_sale.unwrap();
    assert_eq!((last.token_id, last.kind), (243_771, SaleKind::OfferAccept));

    let m = project_mint_summary(&conn, VISIBLE).unwrap().unwrap();
    assert_eq!((m.minted, m.priced), (2, 2));
    assert_eq!(m.min_mutez, Some(1_000_000));
    assert_eq!(m.max_mutez, Some(16_000_000));
    assert_eq!(m.volume_mutez, 17_000_000);
    assert!(m.first.unwrap() < m.last.unwrap());

    let c = coverage(&conn).unwrap();
    assert_eq!(c.visible, 4);
    assert_eq!(c.hidden, 1);
    assert_eq!(c.rows, c.visible + c.hidden + c.unknown);
}

#[test]
fn attach_requires_a_read_only_connection_and_a_sales_file() {
    let (archive, sales) = setup("store-attach");
    let rw = Connection::open(&archive).unwrap();
    assert!(matches!(attach(&rw, &sales), Err(SalesError::Usage(_))));
    // Detached again after the refusal.
    assert!(rw.execute_batch("SELECT * FROM sales.sales").is_err());

    let conn = read_only(&archive);
    assert!(matches!(
        attach(&conn, &archive.with_file_name("missing.db")),
        Err(SalesError::Unavailable(_))
    ));
    assert!(matches!(
        attach(&conn, &archive),
        Err(SalesError::Schema(_))
    ));
    attach(&conn, &sales).unwrap();
    attach(&conn, &sales).unwrap();
    // The attached file cannot be written through the archive connection.
    assert!(conn.execute("DELETE FROM sales.sales", []).is_err());
    assert_eq!(store::ATTACH_AS, "sales");
}

#[test]
fn stats_report_rows_per_marketplace_and_cursors() {
    let (_, sales) = setup("store-stats");
    let conn = schema::open_read_only(&sales).unwrap();
    let s = store::stats(&conn).unwrap();
    assert_eq!(s.rows, s.by_market.iter().map(|m| m.3).sum::<u64>(), "{s}");
    assert_eq!(s.cursors.len(), 16);
    assert!(s.cursors.iter().all(|c| c.2 == Some(15_999_998)));
    assert_eq!(s.tickets, 2);
    let text = s.to_string();
    assert!(text.contains("fxhash marketplace v2"), "{text}");
    assert!(text.contains("issue market:KT1Xjap1TwmDR1d8yEd8ErkraAj2mbdMrPZY bulk_unsplittable"));
}
