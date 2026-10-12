mod articles_support;

use axum::{
    Router,
    body::Body,
    http::{HeaderMap, Request, StatusCode, header},
};
use http_body_util::BodyExt;
use seeded_archive::{
    articles::store::ArticleStore,
    config::Config,
    import::import_fixtures,
    store::Store,
    web::{AppState, router},
};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
use tower::ServiceExt;

static NEXT: AtomicU64 = AtomicU64::new(0);
const AUTHOR: &str = "tz1gL9H53BzjDqmMi2JkKALeCzT7UAoopzEL";

struct Fixture {
    dir: PathBuf,
    config: Config,
    _articles: articles_support::Harness,
}

impl Fixture {
    fn new() -> Self {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let dir = root.join(format!(
            "tmp/article-agent-routes-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let mut values = BTreeMap::new();
        values.insert(
            "SEEDED_DATABASE".into(),
            dir.join("main.sqlite").display().to_string(),
        );
        values.insert(
            "SEEDED_ARTICLES_DATABASE".into(),
            dir.join("articles.sqlite").display().to_string(),
        );
        values.insert(
            "SEEDED_SALES_DATABASE".into(),
            dir.join("missing.sqlite").display().to_string(),
        );
        let config = Config::from_values(&values).unwrap();
        let mut store = Store::open(&config.database).unwrap();
        import_fixtures(&mut store, &root.join("assets/fixtures")).unwrap();
        let mut articles = articles_support::Harness::new();
        articles.store = ArticleStore::open(&config.articles_database).unwrap();
        articles.sync_default();
        Self {
            dir,
            config,
            _articles: articles,
        }
    }

    fn app(&self) -> Router {
        router(AppState::new(&self.config.database, "assets").configured(&self.config))
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

async fn get(app: Router, path: &str) -> (StatusCode, String) {
    let (status, _, body) = get_accept(app, path, "application/json").await;
    (status, body)
}

async fn get_accept(app: Router, path: &str, accept: &str) -> (StatusCode, HeaderMap, String) {
    let response = app
        .oneshot(
            Request::get(path)
                .header(header::ACCEPT, accept)
                .header(header::HOST, "untrusted.invalid")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let body = String::from_utf8(
        response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    (status, headers, body)
}

fn payload(body: &str) -> Value {
    let value: Value = serde_json::from_str(body).expect("JSON response");
    value.get("data").cloned().unwrap_or(value)
}

#[tokio::test]
async fn article_agent_json_returns_raw_article_and_untrusted_body_without_html() {
    let fixture = Fixture::new();
    let (status, body) = get(fixture.app(), "/article/1").await;
    assert_eq!(status, StatusCode::OK);
    let value = payload(&body);
    assert_eq!(value["article"]["token_id"], 1);
    assert_eq!(value["body_untrusted"], true);
    assert!(value["author_name"].is_string());
    assert!(value["references"].is_array());
    assert!(value["media_urls"].is_array());
    assert!(!body.contains("<article"));
    assert!(!body.contains("<main"));
}

#[tokio::test]
async fn article_agent_json_keeps_listing_rows_raw_and_pagination_relative() {
    let fixture = Fixture::new();
    let (status, body) = get(fixture.app(), "/articles").await;
    assert_eq!(status, StatusCode::OK);
    let value = payload(&body);
    let first = value["items"].as_array().unwrap().first().unwrap();
    assert!(first["summary"]["token_id"].is_number());
    assert!(first["author_name"].is_string());
    assert_eq!(value["pagination"]["first"], "/articles");
    assert!(
        value["pagination"]["first"]
            .as_str()
            .unwrap()
            .starts_with('/')
    );
    assert!(!body.contains("<article"));
}

#[tokio::test]
async fn unfiltered_article_listing_uses_a_valid_relative_cursor_url() {
    let fixture = Fixture::new();
    let conn = rusqlite::Connection::open(&fixture.config.articles_database).unwrap();
    for id in 92000_i64..=92024 {
        conn.execute(
            "INSERT INTO articles(token_id,author,created_level,created_at,editions,minted,burned,chain_level,title,flag,hidden) VALUES(?1,?2,1,'2026-01-02T03:04:05Z',1,1,0,1,?3,'NONE',0)",
            rusqlite::params![id, AUTHOR, format!("Cursor article {id}")],
        )
        .unwrap();
    }
    let (status, body) = get(fixture.app(), "/articles").await;
    assert_eq!(status, StatusCode::OK);
    let first = payload(&body);
    let next = first["pagination"]["next"].as_str().unwrap();
    assert!(next.starts_with("/articles?after="));

    let (status, body) = get(fixture.app(), next).await;
    assert_eq!(status, StatusCode::OK);
    assert!(payload(&body)["items"].is_array());
}

#[tokio::test]
async fn article_agent_json_preserves_visibility_and_rejects_bad_cursors() {
    let fixture = Fixture::new();
    assert_eq!(
        get(fixture.app(), "/article/999999").await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        get(fixture.app(), "/articles?after=broken").await.0,
        StatusCode::BAD_REQUEST
    );
}

#[tokio::test]
async fn article_json_ld_and_canonical_escape_untrusted_metadata_and_host() {
    let fixture = Fixture::new();
    let conn = rusqlite::Connection::open(&fixture.config.articles_database).unwrap();
    conn.execute(
        "UPDATE articles SET title=?1,description=?2 WHERE token_id=1",
        [
            "</script><script>alert('title')</script>",
            "</script><script>alert('description')</script>",
        ],
    )
    .unwrap();

    let (status, headers, html) = get_accept(fixture.app(), "/article/1", "text/html").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        headers[header::LINK]
            .to_str()
            .unwrap()
            .contains("<http://127.0.0.1:3918/article/1>; rel=\"canonical\"")
    );
    assert!(html.contains("rel=\"canonical\" href=\"http://127.0.0.1:3918/article/1\""));
    assert!(!html.contains("untrusted.invalid"));
    assert!(!html.contains("<script>alert('title')</script>"));
    assert!(!html.contains("<script>alert('description')</script>"));
    let raw = html
        .split("<script type=\"application/ld+json\">")
        .nth(1)
        .unwrap()
        .split("</script>")
        .next()
        .unwrap();
    let schema: Value = serde_json::from_str(raw).unwrap();
    let article = schema["@graph"]
        .as_array()
        .and_then(|graph| graph.iter().find(|node| node["@type"] == "Article"))
        .unwrap();
    assert_eq!(article["@type"], "Article");
    assert!(raw.contains("\\u003c/script\\u003e"));
    assert!(!raw.contains('<'));
}

#[tokio::test]
async fn article_json_does_not_expose_stored_body_fetch_diagnostics() {
    let fixture = Fixture::new();
    let conn = rusqlite::Connection::open(&fixture.config.articles_database).unwrap();
    conn.execute(
        "UPDATE objects SET error=?1 WHERE uri=(SELECT body_key FROM articles WHERE token_id=1)",
        ["private fetch diagnostic sentinel"],
    )
    .unwrap();

    let (status, body) = get(fixture.app(), "/article/1").await;
    assert_eq!(status, StatusCode::OK);
    assert!(!body.contains("private fetch diagnostic sentinel"));
    assert!(payload(&body)["article"]["body"]["error"].is_null());
}

#[tokio::test]
async fn article_json_hides_stale_references_to_projects_hidden_after_article_sync() {
    let fixture = Fixture::new();
    let articles = rusqlite::Connection::open(&fixture.config.articles_database).unwrap();
    articles
        .execute(
            "INSERT OR REPLACE INTO ref_projects(project_id,hidden,checked_at) VALUES(29437,0,1)",
            [],
        )
        .unwrap();
    articles.execute(
        "INSERT OR REPLACE INTO article_refs(article_id,target,position,kind,form,project_id,source_text) VALUES(1,'project:29437',0,'project','fxhash-url',29437,'stale visible reference')",
        [],
    ).unwrap();
    let main = rusqlite::Connection::open(&fixture.config.database).unwrap();
    main.execute("UPDATE projects SET hidden=1 WHERE id=29437", [])
        .unwrap();

    let (status, body) = get(fixture.app(), "/article/1").await;
    assert_eq!(status, StatusCode::OK);
    let references = payload(&body)["references"].as_array().unwrap().clone();
    assert!(
        !references
            .iter()
            .any(|reference| reference["project_id"] == 29437)
    );
}

#[tokio::test]
async fn article_sitemap_omits_articles_hidden_after_indexing() {
    let fixture = Fixture::new();
    let (status, _, xml) =
        get_accept(fixture.app(), "/sitemap/articles/0", "application/xml").await;
    assert_eq!(status, StatusCode::OK);
    assert!(xml.contains("/article/1</loc>"));

    let conn = rusqlite::Connection::open(&fixture.config.articles_database).unwrap();
    conn.execute("UPDATE articles SET hidden=1 WHERE token_id=1", [])
        .unwrap();
    let (status, _, xml) =
        get_accept(fixture.app(), "/sitemap/articles/0", "application/xml").await;
    assert_eq!(status, StatusCode::OK);
    assert!(!xml.contains("/article/1</loc>"));

    // Visible again but with no fetched body: noindex, so still left out.
    conn.execute("UPDATE articles SET hidden=0 WHERE token_id=1", [])
        .unwrap();
    conn.execute(
        "DELETE FROM objects WHERE uri=(SELECT body_key FROM articles WHERE token_id=1)",
        [],
    )
    .unwrap();
    let (_, _, xml) = get_accept(fixture.app(), "/sitemap/articles/0", "application/xml").await;
    assert!(!xml.contains("/article/1</loc>"));
}

#[tokio::test]
async fn embedded_article_panel_uses_an_independent_cursor_and_returns_matching_raw_page() {
    let fixture = Fixture::new();
    let conn = rusqlite::Connection::open(&fixture.config.articles_database).unwrap();
    conn.execute("DELETE FROM article_refs", []).unwrap();
    conn.execute("DELETE FROM ref_projects", []).unwrap();
    conn.execute(
        "INSERT INTO ref_projects(project_id,hidden,checked_at) VALUES(29437,0,1)",
        [],
    )
    .unwrap();
    for id in 91001_i64..=91004 {
        conn.execute(
            "INSERT INTO articles(token_id,author,created_level,created_at,editions,minted,burned,chain_level,title,description,flag,hidden) VALUES(?1,?2,1,'2026-01-02T03:04:05Z',1,1,0,1,?3,'fixture','NONE',0)",
            rusqlite::params![id, AUTHOR, format!("Panel article {id}")],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO article_refs(article_id,target,position,kind,form,project_id,source_text) VALUES(?1,?2,0,'project','fxhash-url',29437,'fixture project reference')",
            rusqlite::params![id, format!("project:29437:{id}")],
        )
        .unwrap();
    }

    let (status, first_body) = get(fixture.app(), "/project/29437").await;
    assert_eq!(status, StatusCode::OK);
    let first = payload(&first_body);
    let first_items = first["articles"]["items"].as_array().unwrap();
    assert_eq!(first_items.len(), 3);
    let first_ids: Vec<_> = first_items
        .iter()
        .map(|item| item["summary"]["token_id"].as_u64().unwrap())
        .collect();
    assert_eq!(first_ids, [91004, 91003, 91002]);
    let next = first["articles"]["pagination"]["next"].as_str().unwrap();
    assert!(next.contains("articles_after="));
    assert!(next.contains("#project-articles"));

    let next_request = next.split('#').next().unwrap();
    let (status, next_body) = get(fixture.app(), next_request).await;
    assert_eq!(status, StatusCode::OK);
    let next_page = payload(&next_body);
    let items = next_page["articles"]["items"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["summary"]["token_id"], 91001);
    assert!(next_page["articles"]["pagination"]["next"].is_null());
}

#[tokio::test]
async fn embedded_article_json_distinguishes_available_empty_from_unavailable() {
    let mut fixture = Fixture::new();
    let conn = rusqlite::Connection::open(&fixture.config.articles_database).unwrap();
    conn.execute("UPDATE articles SET hidden=1", []).unwrap();

    let paths = [
        "/project/29437".to_owned(),
        format!("/profile/{AUTHOR}?view=projects"),
        format!("/profile/{AUTHOR}?view=collected"),
    ];
    for path in &paths {
        let (status, body) = get(fixture.app(), path).await;
        assert_eq!(status, StatusCode::OK, "{path}");
        let articles = payload(&body)["articles"].clone();
        assert_eq!(articles["available"], true, "{path}");
        assert_eq!(articles["total"], 0, "{path}");
        assert!(articles["items"].as_array().unwrap().is_empty(), "{path}");
    }

    fixture.config.articles_database = fixture.dir.join("missing-articles.sqlite");
    for path in &paths {
        let (status, body) = get(fixture.app(), path).await;
        assert_eq!(status, StatusCode::OK, "{path}");
        let articles = payload(&body)["articles"].clone();
        assert_eq!(articles["available"], false, "{path}");
        assert!(articles["total"].is_null(), "{path}");
        assert!(articles["items"].as_array().unwrap().is_empty(), "{path}");
    }
}

#[tokio::test]
async fn scoped_search_json_names_its_scope_and_searches_only_its_sections() {
    let fixture = Fixture::new();
    let (status, body) = get(fixture.app(), "/search?q=beauty&scope=articles").await;
    assert_eq!(status, StatusCode::OK);
    let value = payload(&body);
    assert_eq!(value["scope"], "articles");
    assert_eq!(value["projects"]["total"], 0);
    assert_eq!(value["artists"]["total"], 0);
    assert_eq!(value["collectors"]["total"], 0);
    assert!(value["articles"]["total"].as_u64().unwrap() > 0);
    let (status, body) = get(fixture.app(), "/search?q=beauty").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload(&body)["scope"], "");
    let (status, headers, _) =
        get_accept(fixture.app(), "/articles?q=beauty", "application/json").await;
    assert_eq!(status, StatusCode::SEE_OTHER);
    assert_eq!(headers["location"], "/search?q=beauty&scope=articles");
    let (status, _) = get(fixture.app(), "/search?q=beauty&scope=tags").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}
