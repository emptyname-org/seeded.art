mod articles_support;
use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
};
use http_body_util::BodyExt;
use seeded_archive::{
    articles::store::ArticleStore,
    config::Config,
    import::import_fixtures,
    store::Store,
    web::{AppState, router},
};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
use tower::ServiceExt;
static NEXT: AtomicU64 = AtomicU64::new(0);
const AUTHOR: &str = "tz1gL9H53BzjDqmMi2JkKALeCzT7UAoopzEL";
const HOLDER: &str = "tz2RXosjAZdS85oCrguy7Eom757144dcU1FT";
const CONTRACT: &str = "KT1EfsNuqwLAWDd3o4pvfUx1CAh5GMdTrRvr";
struct Fixture {
    dir: PathBuf,
    config: Config,
    _articles: articles_support::Harness,
}
impl Fixture {
    fn new() -> Self {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let dir = root.join(format!(
            "tmp/articles-web-{}-{}",
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

    fn placements() -> Self {
        let fixture = Self::new();
        let mut main = Store::open(&fixture.config.database).unwrap();
        seeded_archive::ingest::identity::apply_to(&mut main).unwrap();
        let mut conn = rusqlite::Connection::open(&fixture.config.articles_database).unwrap();
        let thumbnail: String = conn
            .query_row(
                "SELECT thumbnail_uri FROM articles WHERE thumbnail_uri IS NOT NULL LIMIT 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let tx = conn.transaction().unwrap();
        tx.execute_batch("DELETE FROM article_refs; DELETE FROM ref_projects; DELETE FROM holders; DELETE FROM listings; DELETE FROM articles;").unwrap();
        let insert_name = if seeded_archive::articles::store::SCHEMA_VERSION == 2 {
            "INSERT OR REPLACE INTO names(address,name,last_level) VALUES(?1,'WrongArticleRegistryName',1)"
        } else {
            "INSERT OR REPLACE INTO names(address,name) VALUES(?1,'WrongArticleRegistryName')"
        };
        tx.execute(insert_name, [AUTHOR]).unwrap();
        tx.execute("INSERT INTO ref_projects(project_id,hidden,checked_at) VALUES(29437,0,1),(999999999,0,1)", []).unwrap();
        for offset in 0..=30_i64 {
            let id = 10000 + offset;
            tx.execute(
                "INSERT INTO articles(token_id,author,created_level,created_at,editions,minted,burned,chain_level,title,description,thumbnail_uri,hidden) VALUES(?1,?2,1,'2026-01-02T03:04:05Z',10,10,0,1,?3,?4,?5,?6)",
                rusqlite::params![id, AUTHOR, format!("Article {id} <script>unsafe()</script>"), "<img src=x onerror=unsafe()> & notes", thumbnail, i64::from(offset == 30)],
            ).unwrap();
            if offset != 28 {
                tx.execute(
                    "INSERT INTO holders(token_id,address,amount,last_level) VALUES(?1,?2,2,1)",
                    rusqlite::params![id, HOLDER],
                )
                .unwrap();
            }
            tx.execute("INSERT INTO listings(listing_id,token_id,seller,amount,last_level) VALUES(?1,?1,?2,?3,1)", rusqlite::params![id,HOLDER,if offset == 28 {2} else {1}]).unwrap();
            if offset % 2 == 0 {
                tx.execute("INSERT INTO article_refs(article_id,target,position,kind,form,project_id,source_text) VALUES(?1,'project:29437',0,'project','url',29437,'fixture direct project reference')", [id]).unwrap();
            } else {
                tx.execute("INSERT INTO article_refs(article_id,target,position,kind,form,project_id,contract,token_id,source_text) VALUES(?1,'token:133459',0,'token','url',29437,?2,133459,'fixture token reference')", rusqlite::params![id,CONTRACT]).unwrap();
            }
        }
        // A second reference to the same project must not duplicate the article.
        tx.execute("INSERT INTO article_refs(article_id,target,position,kind,form,project_id,source_text) VALUES(10029,'project:29437',1,'project','url',29437,'duplicate project reference'),(10029,'project:999999999',2,'project','url',999999999,'stale unknown project reference')", []).unwrap();
        tx.commit().unwrap();
        fixture
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}
async fn get(app: Router, path: &str) -> (StatusCode, String) {
    let reply = app
        .oneshot(Request::get(path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = reply.status();
    let body = String::from_utf8(
        reply
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    (status, body)
}

/// Status and `Location` of a request that is not followed.
async fn redirect(app: Router, path: &str, accept: &str) -> (StatusCode, Option<String>) {
    let reply = app
        .oneshot(
            Request::get(path)
                .header(axum::http::header::ACCEPT, accept)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let location = reply
        .headers()
        .get(axum::http::header::LOCATION)
        .map(|value| value.to_str().unwrap().to_owned());
    (reply.status(), location)
}

/// The header search form, from its opening tag to its end.
fn header_search(html: &str) -> &str {
    html.split_once("<form id=\"global-search-form\"")
        .expect("header search form")
        .1
        .split_once("</form>")
        .unwrap()
        .0
}

fn section<'a>(html: &'a str, id: &str) -> &'a str {
    html.split_once(&format!("<section id=\"{id}\""))
        .unwrap_or_else(|| panic!("missing article section {id}"))
        .1
        .split_once("</section>")
        .unwrap()
        .0
}

fn article_ids(html: &str) -> Vec<u64> {
    let mut ids = Vec::new();
    for link in html.split("href=\"/article/").skip(1) {
        let id = link.split('"').next().unwrap().parse().unwrap();
        if !ids.contains(&id) {
            ids.push(id);
        }
    }
    ids
}

fn next_articles(html: &str) -> Option<String> {
    html.split("href=\"")
        .skip(1)
        .map(|link| link.split('"').next().unwrap())
        .find(|link| link.starts_with("/articles?") && link.contains("after="))
        .map(|link| link.replace("&amp;", "&").replace("&#38;", "&"))
}
#[tokio::test]
async fn article_search_routes_and_hidden_articles_follow_archive_policy() {
    let fixture = Fixture::new();
    let (status, body) = get(fixture.app(), "/search?q=beauty&scope=articles").await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("/article/835"));
    assert!(body.contains("/article/1"));
    for route in ["/article/100", "/article/279", "/article/999999"] {
        assert_eq!(get(fixture.app(), route).await.0, StatusCode::NOT_FOUND);
    }
    assert_eq!(
        get(fixture.app(), "/articles?after=broken").await.0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        get(fixture.app(), "/article/no-number").await.0,
        StatusCode::BAD_REQUEST
    );
    let (status, body) = get(fixture.app(), "/article/1").await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("Article record"));
    assert!(
        !body.contains(">richer<"),
        "unvalidated article registry names must not display"
    );
}
#[tokio::test]
async fn unavailable_secondary_database_does_not_break_primary_catalogue() {
    let mut fixture = Fixture::new();
    fixture.config.articles_database = fixture.dir.join("absent.sqlite");
    assert_eq!(
        get(fixture.app(), "/articles").await.0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(get(fixture.app(), "/projects").await.0, StatusCode::OK);
    let (status, body) = get(fixture.app(), "/project/29437").await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("temporarily unavailable"));
}
#[tokio::test]
async fn project_page_lists_related_articles_before_market_history() {
    let fixture = Fixture::placements();
    let (status, body) = get(fixture.app(), "/project/29437").await;
    assert_eq!(status, StatusCode::OK);
    let articles = body
        .find("id=\"project-articles\"")
        .expect("related articles");
    let history = body
        .find("<h2>Market history</h2>")
        .expect("market history");
    assert!(articles < history);
}
#[tokio::test]
async fn article_html_scripts_and_external_images_are_not_executed() {
    let fixture = Fixture::new();
    let conn = rusqlite::Connection::open(&fixture.config.articles_database).unwrap();
    conn.execute("UPDATE objects SET data=? WHERE uri=(SELECT body_key FROM articles WHERE token_id=1)", ["<script>leaked()</script>\n\n[bad](javascript:alert%281%29)\n\n![track](https://tracker.example/a)\n\n**Readable**"]).unwrap();
    let (status, body) = get(fixture.app(), "/article/1").await;
    assert_eq!(status, StatusCode::OK);
    assert!(!body.contains("leaked()"));
    assert!(!body.contains("href=\"javascript:"));
    assert!(!body.contains("src=\"https://tracker.example"));
    assert!(body.contains("<strong>Readable</strong>"));
}

#[tokio::test]
async fn article_hero_shows_the_thumbnail_linked_to_the_full_image() {
    let fixture = Fixture::new();
    let conn = rusqlite::Connection::open(&fixture.config.articles_database).unwrap();
    let hero = |html: &str| {
        html.split("<figure class=\"article-hero\">")
            .nth(1)
            .and_then(|tail| tail.split("</figure>").next())
            .expect("hero")
            .to_owned()
    };
    conn.execute(
        "UPDATE articles SET title='Hero title', thumbnail_uri='ipfs://thumb', display_uri='ipfs://full', thumbnail_caption=NULL WHERE token_id=1",
        [],
    )
    .unwrap();
    let (status, html) = get(fixture.app(), "/article/1").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        hero(&html),
        "<a href=\"http://127.0.0.1:3919/article-media/1/1\"><img src=\"http://127.0.0.1:3919/article-media/1/0\" alt=\"Hero title\" referrerpolicy=\"no-referrer\"></a>"
    );
    conn.execute(
        "UPDATE articles SET thumbnail_uri=NULL, thumbnail_caption='Hero caption' WHERE token_id=1",
        [],
    )
    .unwrap();
    let (_, html) = get(fixture.app(), "/article/1").await;
    assert_eq!(
        hero(&html),
        "<img src=\"http://127.0.0.1:3919/article-media/1/1\" alt=\"Hero caption\" referrerpolicy=\"no-referrer\"><figcaption>Hero caption</figcaption>"
    );
}

#[tokio::test]
async fn articles_show_completed_sync_metadata_and_recover_from_missing_coverage() {
    let fixture = Fixture::new();
    let conn = rusqlite::Connection::open(&fixture.config.articles_database).unwrap();
    conn.execute(
        "UPDATE sync_state SET chain_level=98765,chain_synced_at=0 WHERE id=1",
        [],
    )
    .unwrap();
    for path in ["/articles", "/article/1"] {
        let (status, html) = get(fixture.app(), path).await;
        assert_eq!(status, StatusCode::OK);
        assert!(html.contains("Last completed index sync: block 98765"));
        assert!(html.contains("Thu, 01 Jan 1970"));
    }
    conn.execute(
        "UPDATE sync_state SET chain_level=NULL,chain_synced_at=NULL WHERE id=1",
        [],
    )
    .unwrap();
    let (status, html) = get(fixture.app(), "/articles").await;
    assert_eq!(status, StatusCode::OK);
    assert!(html.contains("Index sync coverage is not recorded"));
}

#[tokio::test]
async fn profile_tabs_and_project_show_three_visible_articles_with_canonical_safe_cards() {
    let fixture = Fixture::placements();
    for (path, id, heading, listing) in [
        (
            format!("/profile/{AUTHOR}?view=projects"),
            "profile-articles",
            "Articles created",
            format!("/articles?author={AUTHOR}"),
        ),
        (
            format!("/profile/{HOLDER}?view=collected"),
            "profile-articles",
            "Articles collected",
            format!("/articles?holder={HOLDER}"),
        ),
        (
            "/project/29437".into(),
            "project-articles",
            "Related articles",
            "/articles?project=29437".into(),
        ),
    ] {
        let (status, html) = get(fixture.app(), &path).await;
        assert_eq!(status, StatusCode::OK, "{path}");
        let preview = section(&html, id);
        assert!(preview.contains(heading));
        assert!(html.contains(&format!("href=\"#{id}\"")), "{path}");
        assert_eq!(article_ids(preview), [10029, 10028, 10027], "{path}");
        assert!(!preview.contains("View all"));
        let _ = &listing;
        assert!(preview.contains(&format!("href=\"/profile/{AUTHOR}\">Emptyname</a>")));
        assert!(!html.contains("WrongArticleRegistryName"));
        assert!(preview.contains("&#60;script&#62;unsafe()&#60;/script&#62;"));
        assert!(preview.contains("&#60;img src=x onerror=unsafe()&#62;"));
        assert!(!preview.contains("<script>unsafe()"));
        assert!(!preview.contains("<img src=x"));
        assert!(preview.contains("src=\"http://127.0.0.1:3919/article-media/10029/0\""));
        assert!(!preview.contains("src=\"ipfs:"));
        assert!(!preview.contains("src=\"https:"));
        assert!(!article_ids(&html).contains(&10030));
        if heading == "Articles collected" {
            assert!(preview.contains("3 held · 1 listed"));
            assert!(preview.contains("2 held · 2 listed"));
        } else {
            assert!(!preview.contains("class=\"article-holding\""));
        }
    }
    // The other tab has its own article scope, not the opposite tab's records.
    let (_, html) = get(fixture.app(), &format!("/profile/{HOLDER}?view=projects")).await;
    assert!(!html.contains("<section id=\"profile-articles\""));
    assert!(!html.contains("href=\"#profile-articles\""));
}

#[tokio::test]
async fn profile_search_text_narrows_both_profile_article_sections() {
    let fixture = Fixture::placements();
    let conn = rusqlite::Connection::open(&fixture.config.articles_database).unwrap();
    conn.execute(
        "INSERT INTO articles_fts(rowid,title,authors,tags) SELECT token_id,title,author,'' FROM articles",
        [],
    )
    .unwrap();
    for path in [
        format!("/profile/{AUTHOR}?view=projects&q=10027"),
        format!("/profile/{HOLDER}?view=collected&q=10027"),
    ] {
        let (status, html) = get(fixture.app(), &path).await;
        assert_eq!(status, StatusCode::OK, "{path}");
        assert_eq!(
            article_ids(section(&html, "profile-articles")),
            [10027],
            "{path}"
        );
    }
    // The next page of matching articles keeps the search text.
    let (_, html) = get(
        fixture.app(),
        &format!("/profile/{HOLDER}?view=collected&q=Article"),
    )
    .await;
    let preview = section(&html, "profile-articles");
    assert_eq!(article_ids(preview), [10029, 10028, 10027]);
    assert!(preview.contains("q=Article&#38;articles_after="));
    let (status, html) = get(
        fixture.app(),
        &format!("/profile/{HOLDER}?view=collected&q=zzzz"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(!html.contains("<section id=\"profile-articles\""));
}

#[tokio::test]
async fn hidden_only_article_scopes_hide_project_and_both_profile_panel_links() {
    let fixture = Fixture::placements();
    let conn = rusqlite::Connection::open(&fixture.config.articles_database).unwrap();
    conn.execute("UPDATE articles SET hidden=1", []).unwrap();

    for (path, id) in [
        ("/project/29437".to_owned(), "project-articles"),
        (
            format!("/profile/{AUTHOR}?view=projects"),
            "profile-articles",
        ),
        (
            format!("/profile/{HOLDER}?view=collected"),
            "profile-articles",
        ),
    ] {
        let (status, html) = get(fixture.app(), &path).await;
        assert_eq!(status, StatusCode::OK, "{path}");
        assert!(!html.contains(&format!("<section id=\"{id}\"")), "{path}");
        assert!(!html.contains(&format!("href=\"#{id}\"")), "{path}");
        assert!(!html.contains("View all 30 articles"), "{path}");
    }
}

#[tokio::test]
async fn scoped_article_lists_traverse_all_keyset_pages_and_reject_swapped_cursors() {
    let fixture = Fixture::placements();
    let main = rusqlite::Connection::open(&fixture.config.database).unwrap();
    let other_project: i64 = main
        .query_row(
            "SELECT id FROM projects WHERE id<>29437 AND hidden=0 ORDER BY id LIMIT 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let expected: Vec<u64> = (10000..10030).rev().collect();
    for (path, mismatched) in [
        (
            format!("/articles?author={AUTHOR}"),
            format!("/articles?author={HOLDER}"),
        ),
        (
            format!("/articles?holder={HOLDER}"),
            format!("/articles?holder={AUTHOR}"),
        ),
        (
            "/articles?project=29437".into(),
            format!("/articles?project={other_project}"),
        ),
    ] {
        let (status, html) = get(fixture.app(), &path).await;
        assert_eq!(status, StatusCode::OK, "{path}");
        let first = article_ids(&html);
        assert_eq!(first, expected[..24]);
        assert!(html.contains("30 articles"));
        assert!(html.contains("Emptyname"));
        assert!(!html.contains("WrongArticleRegistryName"));
        let next = next_articles(&html).expect("a second keyset page");
        assert!(next.starts_with(&format!("{path}&after=")));
        assert!(!next.contains("page="));
        let (status, html) = get(fixture.app(), &next).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(article_ids(&html), expected[24..]);
        assert!(next_articles(&html).is_none());
        assert!(!article_ids(&html).contains(&10030));
        let cursor = next.split_once("&after=").unwrap().1;
        assert_eq!(
            get(fixture.app(), &format!("{mismatched}&after={cursor}"))
                .await
                .0,
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            get(fixture.app(), &format!("/articles?after={cursor}"))
                .await
                .0,
            StatusCode::BAD_REQUEST
        );
    }
}

#[tokio::test]
async fn article_scopes_reject_conflicting_filters_and_invalid_addresses() {
    let fixture = Fixture::placements();
    for path in [
        format!("/articles?author={AUTHOR}&holder={HOLDER}"),
        format!("/articles?author={AUTHOR}&project=29437"),
        format!("/articles?holder={HOLDER}&project=29437"),
        "/articles?author=not-an-address".into(),
        "/articles?holder=not-an-address".into(),
    ] {
        assert_eq!(
            get(fixture.app(), &path).await.0,
            StatusCode::BAD_REQUEST,
            "{path}"
        );
    }
}

#[tokio::test]
async fn current_main_project_visibility_overrides_stale_article_reference_visibility() {
    let fixture = Fixture::placements();
    let articles = ArticleStore::open_read_only(&fixture.config.articles_database).unwrap();
    assert_eq!(
        articles
            .articles_for_project(999999999, None, 24)
            .unwrap()
            .total,
        1
    );
    let app = fixture.app();
    assert_eq!(
        get(app.clone(), "/articles?project=999999999").await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        get(app.clone(), "/articles?project=29437").await.0,
        StatusCode::OK
    );
    let main = rusqlite::Connection::open(&fixture.config.database).unwrap();
    main.execute("UPDATE projects SET hidden=1 WHERE id=29437", [])
        .unwrap();
    assert_eq!(
        articles
            .articles_for_project(29437, None, 24)
            .unwrap()
            .total,
        30
    );
    for path in ["/project/29437", "/articles?project=29437"] {
        assert_eq!(
            get(app.clone(), path).await.0,
            StatusCode::NOT_FOUND,
            "{path}"
        );
    }
    main.execute("UPDATE projects SET hidden=0 WHERE id=29437", [])
        .unwrap();
    assert_eq!(get(app, "/articles?project=29437").await.0, StatusCode::OK);
}

#[tokio::test]
async fn unavailable_article_sections_preserve_primary_pages_and_recover_on_same_router() {
    let mut fixture = Fixture::placements();
    let source = fixture.config.articles_database.clone();
    let secondary = fixture.dir.join("recoverable-articles.sqlite");
    fixture.config.articles_database = secondary.clone();
    let app = fixture.app();
    let routes = [
        (
            format!("/profile/{AUTHOR}?view=projects"),
            "profile-articles",
        ),
        (
            format!("/profile/{HOLDER}?view=collected"),
            "profile-articles",
        ),
        ("/project/29437".into(), "project-articles"),
    ];
    for corrupt in [false, true] {
        if corrupt {
            std::fs::write(&secondary, b"not a sqlite database").unwrap();
        }
        for (path, id) in &routes {
            let (status, html) = get(app.clone(), path).await;
            assert_eq!(status, StatusCode::OK, "{path}, corrupt={corrupt}");
            assert!(!html.contains(&format!("<section id=\"{id}\"")), "{path}");
            assert!(!html.contains(&format!("href=\"#{id}\"")), "{path}");
            assert!(!html.contains("View all 30 articles"), "{path}");
            assert!(!html.contains("/article/10029"), "{path}");
        }
        assert!(get(app.clone(), "/articles").await.0.is_server_error());
    }
    std::fs::remove_file(&secondary).unwrap();
    let conn = rusqlite::Connection::open(source).unwrap();
    conn.execute("VACUUM INTO ?1", [secondary.to_str().unwrap()])
        .unwrap();
    for (path, id) in &routes {
        let (status, html) = get(app.clone(), path).await;
        assert_eq!(status, StatusCode::OK);
        let preview = section(&html, id);
        assert!(html.contains(&format!("href=\"#{id}\"")), "{path}");
        assert_eq!(article_ids(preview), [10029, 10028, 10027]);
        assert!(!preview.contains("temporarily unavailable"));
    }
    assert_eq!(get(app, "/articles").await.0, StatusCode::OK);
}

#[tokio::test]
async fn search_lists_articles_and_the_artist_directory_counts_them() {
    let fixture = Fixture::new();
    let (status, html) = get(fixture.app(), "/search?q=beauty").await;
    assert_eq!(status, StatusCode::OK);
    assert!(html.contains("href=\"#search-articles\""));
    let articles = section(&html, "search-articles");
    assert!(article_ids(articles).contains(&835));
    assert!(articles.contains("<dt>Published</dt>"));
    assert!(articles.contains("<dt>Minted NFTs</dt>"));
    assert!(!articles.contains("View all"));
    let fixture = Fixture::placements();
    // The article author is a credited artist of the project.
    let main = rusqlite::Connection::open(fixture.dir.join("main.sqlite")).unwrap();
    main.execute(
        "UPDATE artists SET address = ?1 WHERE id IN \
         (SELECT artist_id FROM project_artists WHERE project_id = 29437)",
        [AUTHOR],
    )
    .unwrap();
    let (status, html) = get(fixture.app(), "/artists").await;
    assert_eq!(status, StatusCode::OK);
    assert!(html.contains(" · 30 articles"), "{html}");
}

#[tokio::test]
async fn header_search_is_one_form_on_every_page_and_only_the_tab_scope_differs() {
    let fixture = Fixture::new();
    let pages = [
        ("/projects".to_owned(), "", ""),
        ("/artists".into(), "people", ""),
        ("/collectors".into(), "people", ""),
        ("/articles".into(), "articles", ""),
        ("/article/1".into(), "articles", ""),
        ("/search?q=beauty".into(), "", "beauty"),
        (
            "/search?q=beauty&scope=articles".into(),
            "articles",
            "beauty",
        ),
        ("/search?q=beauty&scope=people".into(), "people", "beauty"),
        (format!("/profile/{AUTHOR}"), "", ""),
        ("/project/29437".into(), "", ""),
        (format!("/token/{CONTRACT}/133459"), "", ""),
        ("/about".into(), "", ""),
    ];
    let (_, reference) = get(fixture.app(), "/projects").await;
    let reference = header_search(&reference).to_owned();
    for attribute in [
        "action=\"/search\"",
        "method=\"get\"",
        "<label class=\"sr-only\" for=\"global-search\">Search the archive</label>",
        "name=\"q\" type=\"search\" value=\"\" placeholder=\"Search\" maxlength=\"200\" required",
        "<button class=\"search-submit\" type=\"submit\">Search</button>",
    ] {
        assert!(reference.contains(attribute), "{attribute}: {reference}");
    }
    assert!(!reference.contains("type=\"hidden\""));
    for (path, scope, query) in pages {
        let (status, html) = get(fixture.app(), &path).await;
        assert_eq!(status, StatusCode::OK, "{path}");
        let form = header_search(&html);
        assert!(form.contains(&format!("value=\"{query}\"")), "{path}");
        let hidden = format!("<input type=\"hidden\" name=\"scope\" value=\"{scope}\">");
        assert_eq!(form.contains(&hidden), !scope.is_empty(), "{path}");
        assert_eq!(
            form.replace(&format!("value=\"{query}\""), "value=\"\"")
                .replace(&hidden, ""),
            reference,
            "{path}"
        );
        assert!(!html.contains("Clear search"), "{path}");
    }
}

#[tokio::test]
async fn listing_text_queries_move_to_search_in_their_tab_scope() {
    let fixture = Fixture::new();
    for (path, location) in [
        ("/projects?q=x".to_owned(), "/search?q=x"),
        ("/artists?q=x".into(), "/search?q=x&scope=people"),
        ("/articles?q=x".into(), "/search?q=x&scope=articles"),
        (
            "/projects?artist=zancan&q=a%20b%26c&sort=oldest&page=1".into(),
            "/search?q=a%20b%26c",
        ),
        ("/artists?after=any&q=x".into(), "/search?q=x&scope=people"),
        (
            format!("/articles?author={AUTHOR}&q=tree"),
            "/search?q=tree&scope=articles",
        ),
        (
            "/articles?project=29437&q=tree".into(),
            "/search?q=tree&scope=articles",
        ),
    ] {
        for accept in ["text/html", "application/json"] {
            assert_eq!(
                redirect(fixture.app(), &path, accept).await,
                (StatusCode::SEE_OTHER, Some(location.to_owned())),
                "{path} {accept}"
            );
        }
    }
    for path in ["/projects?q=", "/artists?q=%20", "/articles?q="] {
        assert_eq!(get(fixture.app(), path).await.0, StatusCode::OK, "{path}");
    }
    let long = "a".repeat(201);
    for listing in ["/projects", "/artists", "/articles"] {
        assert_eq!(
            get(fixture.app(), &format!("{listing}?q={long}")).await.0,
            StatusCode::BAD_REQUEST,
            "{listing}"
        );
    }
}

#[tokio::test]
async fn article_scope_searches_only_articles_in_the_search_template() {
    let fixture = Fixture::new();
    let (status, all) = get(fixture.app(), "/search?q=beauty").await;
    assert_eq!(status, StatusCode::OK);
    let (status, html) = get(fixture.app(), "/search?q=beauty&scope=articles").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        article_ids(section(&html, "search-articles")),
        article_ids(section(&all, "search-articles"))
    );
    for other in ["search-projects", "search-artists", "search-collectors"] {
        assert!(!html.contains(&format!("id=\"{other}\"")), "{other}");
        assert!(!html.contains(&format!("href=\"#{other}\"")), "{other}");
    }
    assert!(html.contains("<nav class=\"search-result-nav\""));
    assert!(html.contains("href=\"#search-articles\""));
    assert!(!html.contains("id=\"search-sort\""));
    assert!(
        html.contains("href=\"/articles\" aria-current=\"page\" class=\"active\">Articles</a>")
    );
    let (status, empty) = get(fixture.app(), "/search?q=zzzzqqq&scope=articles").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(empty.matches("class=\"empty-state\"").count(), 1);
    assert!(empty.contains("<p class=\"empty-state-title\">No results</p>"));
    assert_eq!(
        redirect(fixture.app(), "/search?q=&scope=articles", "text/html").await,
        (StatusCode::SEE_OTHER, Some("/articles".to_owned()))
    );
}

#[tokio::test]
async fn embedded_fxhash_items_render_as_linked_archive_cards() {
    let fixture = Fixture::new();
    let conn = rusqlite::Connection::open(&fixture.config.articles_database).unwrap();
    let body = "Intro text\n\n\
        ::tezos-storage-pointer[]{contract=\"KT1BJC12dG17CVvPKJ1VYaNnaT5mzfnUTwXv\" path=\"ledger::29437\" data_spec=\"FX-GEN-DATA-002\"}\n\n\
        ::tezos-storage-pointer[]{contract=\"KT1BJC12dG17CVvPKJ1VYaNnaT5mzfnUTwXv\" path=\"ledger::999999999\"}\n\n\
        See [the edition](https://www.fxhash.xyz/gentk/FX1-133459) and [elsewhere](https://example.org/).";
    let updated = conn
        .execute(
            "UPDATE objects SET data=?1 WHERE uri=(SELECT body_key FROM articles WHERE token_id=1)",
            [body],
        )
        .unwrap();
    assert_eq!(updated, 1);
    conn.execute("DELETE FROM article_refs WHERE article_id=1", [])
        .unwrap();
    conn.execute(
        "INSERT OR REPLACE INTO ref_projects(project_id,hidden,checked_at) VALUES(29437,0,1)",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO article_refs(article_id,target,position,kind,form,project_id,source_text) \
         VALUES(1,'project:29437',0,'project','storage-pointer',29437,'pointer')",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO article_refs(article_id,target,position,kind,form,project_id,contract,token_id,source_text) \
         VALUES(1,'token:133459',1,'token','fxhash-url',29437,?1,133459,'https://www.fxhash.xyz/gentk/FX1-133459')",
        [CONTRACT],
    )
    .unwrap();
    let (status, html) = get(fixture.app(), "/article/1").await;
    assert_eq!(status, StatusCode::OK);
    let prose = html.split_once("class=\"article-prose\">").unwrap().1;
    let embed = prose.find("<div class=\"article-embed\">").expect("card");
    assert!(prose.find("Intro text").unwrap() < embed);
    assert!(prose[embed..].contains("href=\"/project/29437\""));
    // Embedded cards stay lazy so they never compete with the hero image.
    let image = prose[embed..]
        .split("<img ")
        .nth(1)
        .and_then(|tail| tail.split('>').next())
        .expect("embedded card image");
    assert!(image.contains("loading=\"lazy\""));
    assert!(!image.contains("fetchpriority"));
    // The unknown project's pointer shows nothing; neither directive is printed.
    assert!(!prose.contains("tezos-storage-pointer"));
    assert!(prose.contains(&format!(
        "<a href=\"/token/{CONTRACT}/133459\">the edition</a>"
    )));
    assert!(prose.contains(
        "<a href=\"https://example.org/\" rel=\"nofollow ugc noopener noreferrer\">elsewhere</a>"
    ));
}

#[test]
fn no_template_offers_a_separate_clear_search() {
    let templates = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("templates");
    for entry in std::fs::read_dir(templates).unwrap() {
        let path = entry.unwrap().path();
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(!text.contains("Clear search"), "{}", path.display());
        assert!(!text.contains("matching “"), "{}", path.display());
        assert!(
            !text.contains("{% block search_"),
            "per-page header search override in {}",
            path.display()
        );
    }
}
