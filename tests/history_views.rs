use rusqlite::{Connection, OpenFlags, params};
use seeded_archive::{
    import::import_fixtures,
    sales::{schema, store::attach},
    store::Store,
    web::history,
};
use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
const TOKEN: &str = "KT1EfsNuqwLAWDd3o4pvfUx1CAh5GMdTrRvr";
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture {
    dir: PathBuf,
    main: Connection,
    read: Connection,
    sales: Connection,
    project: seeded_archive::model::Project,
}
impl Fixture {
    fn new() -> Self {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let dir = root.join(format!(
            "tmp/history-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("main.sqlite");
        let mut store = Store::open(&path).unwrap();
        import_fixtures(&mut store, &root.join("assets/fixtures")).unwrap();
        let project = store.project(29437).unwrap();
        drop(store);
        let main = Connection::open(&path).unwrap();
        let sales_path = dir.join("sales.sqlite");
        let sales = schema::open_writer(&sales_path).unwrap();
        let read = Connection::open_with_flags(&path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
        attach(&read, &sales_path).unwrap();
        Self {
            dir,
            main,
            read,
            sales,
            project,
        }
    }
    fn row(&self, id: i64, kind: &str, price: Option<i64>, currency: &str) {
        self.sales.execute("INSERT INTO sales(contract,token_id,kind,price_mutez,currency,buyer,marketplace,op_hash,op_id,level,timestamp) VALUES (?,133459,?,?,?,'tz1gL9H53BzjDqmMi2JkKALeCzT7UAoopzEL','KT1GbyoDi7H1sfXmimXpptZJuCdHMh66WS9u','recorded-operation',?,1,'2026-10-05T00:00:00Z')",params![TOKEN,kind,price,currency,id]).unwrap();
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}
#[test]
fn history_keeps_exact_units_free_mints_and_unknown_prices_distinct() {
    let f = Fixture::new();
    f.row(1, "mint", Some(0), "tez");
    f.row(2, "sale", Some(1_005_123), "tez");
    f.row(3, "offer_accept", Some(2_250_000), "wxtz");
    f.row(4, "sale", None, "another-token");
    let html = history::token(&f.read, TOKEN, 133459);
    assert!(html.contains("0 tez"));
    assert!(html.contains("1.005123 tez"));
    assert!(html.contains("2.25 wXTZ"));
    assert!(html.contains("Unpriced (another-token)"));
    assert!(html.find("Unpriced").unwrap() < html.find("1.005123").unwrap());
    let project = history::project(&f.read, &f.project);
    assert!(project.contains("Prices paid at mint"));
    assert!(project.contains("Lowest latest priced sale"));
    assert!(project.contains("0 tez · 1 priced mints"));
}
#[test]
fn summary_cache_does_not_reveal_a_project_hidden_after_first_read() {
    let f = Fixture::new();
    f.row(1, "sale", Some(123_456_789), "tez");
    assert!(history::project(&f.read, &f.project).contains("123.456789"));
    f.main
        .execute("UPDATE projects SET hidden=1 WHERE id=29437", [])
        .unwrap();
    assert!(!history::project(&f.read, &f.project).contains("123.456789"));
    assert!(!history::token(&f.read, TOKEN, 133459).contains("123.456789"));
}

#[test]
fn completed_sales_sync_levels_are_reported_without_treating_cursor_updates_as_completion_times() {
    let f = Fixture::new();
    f.sales.execute("INSERT INTO cursors(stream,cursor_id,synced_level,updated_at) VALUES('tickets',1,123,1999999999)",[]).unwrap();
    let html = history::project(&f.read, &f.project);
    assert!(html.contains("Last completed sales index sync"));
    assert!(html.contains("<dt>fx(params) mint tickets</dt><dd>Block 123</dd>"));
    assert!(html.contains("No completed sync recorded"));
    assert!(!html.contains("1999999999"));
    f.sales
        .execute(
            "UPDATE cursors SET synced_level=124,updated_at=2000000000",
            [],
        )
        .unwrap();
    assert!(history::project(&f.read, &f.project).contains("Block 123"));
    assert!(!history::project(&f.read, &f.project).contains("Block 124"));
}

#[test]
fn sales_sync_coverage_shows_on_project_pages_only() {
    let f = Fixture::new();
    f.sales.execute("INSERT INTO cursors(stream,cursor_id,synced_level,updated_at) VALUES('tickets',1,123,1)",[]).unwrap();
    f.row(1, "sale", Some(1_000_000), "tez");
    let token = history::token(&f.read, TOKEN, 133459);
    assert!(token.contains("1 tez"));
    assert!(!token.contains("Last completed sales index sync"));
    assert!(!token.contains("Block 123"));
    let project = history::project(&f.read, &f.project);
    assert!(project.contains("Last completed sales index sync"));
    assert!(project.contains("Block 123"));
}
