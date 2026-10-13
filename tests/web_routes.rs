use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{HeaderMap, Request, StatusCode, header};
use http_body_util::{BodyExt, Limited};
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use seeded_archive::import::import_fixtures;
use seeded_archive::store::{ProjectFilter, ProjectSort, Store};
use seeded_archive::web::{AppState, router};
use tower::ServiceExt;

mod ingest_support;

const CONTRACT: &str = "KT1EfsNuqwLAWDd3o4pvfUx1CAh5GMdTrRvr";
const SEED_ONE: &str = "ooHBH4Q9m5BTSExqbvNCtpWDbdEqZqTXK4Ewh1HMU66V5PhMUE7";
static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

struct TestArchive {
    directory: PathBuf,
    database: PathBuf,
}

impl TestArchive {
    fn empty() -> Self {
        let parent = project_root().join("tmp");
        fs::create_dir_all(&parent).expect("create project test directory");
        loop {
            let number = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
            let directory = parent.join(format!("web_routes-{}-{number}", std::process::id()));
            match fs::create_dir(&directory) {
                Ok(()) => {
                    return Self {
                        database: directory.join("archive.sqlite"),
                        directory,
                    };
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => panic!("create isolated HTTP test directory: {error}"),
            }
        }
    }

    fn imported() -> Self {
        let archive = Self::empty();
        archive.import();
        archive
    }

    fn chain_imported() -> Self {
        let archive = Self::empty();
        let mut store = Store::open(&archive.database).unwrap();
        seeded_archive::ingest::frank::bootstrap(&mut store, &ingest_support::frank()).unwrap();
        let fake = ingest_support::FakeTzkt::recorded();
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let api = ingest_support::tzkt(&fake, &stop);
        seeded_archive::ingest::sync::sync(&mut store, &api, &Default::default(), &|_| {}).unwrap();
        archive
    }

    fn configured_app(&self) -> Router {
        let config = seeded_archive::config::Config::from_values(&Default::default()).unwrap();
        router(AppState::new(&self.database, project_root().join("assets")).configured(&config))
    }

    fn import(&self) {
        let mut store = Store::open(&self.database).expect("open test store");
        import_fixtures(&mut store, &project_root().join("assets/fixtures"))
            .expect("import verified fixtures");
        seeded_archive::ingest::identity::apply_to(&mut store).unwrap();
    }

    fn app(&self) -> Router {
        router(AppState::new(
            self.database.clone(),
            project_root().join("assets"),
        ))
    }
}

impl Drop for TestArchive {
    fn drop(&mut self) {
        if let Err(error) = fs::remove_dir_all(&self.directory) {
            if std::thread::panicking() {
                eprintln!("HTTP test cleanup failed: {error}");
            } else {
                panic!("HTTP test cleanup failed: {error}");
            }
        }
    }
}

fn project_root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
}

#[tokio::test]
async fn favicon_is_linked_and_serves_the_supplied_image() {
    let archive = TestArchive::imported();
    let app = archive.app();
    let page = get(&app, "/").await;
    assert_eq!(page.status, StatusCode::OK);
    assert!(
        page.text()
            .contains("rel=\"icon\" type=\"image/png\" href=\"/static/favicon.png\"")
    );
    let redirect = get(&app, "/favicon.ico").await;
    assert_eq!(redirect.status, StatusCode::PERMANENT_REDIRECT);
    assert_eq!(redirect.headers[header::LOCATION], "/static/favicon.png");
    let icon = get(&app, "/static/favicon.png").await;
    assert_eq!(icon.status, StatusCode::OK);
    assert_eq!(icon.headers[header::CONTENT_TYPE], "image/png");
    assert_eq!(icon.body, include_bytes!("../assets/static/favicon.png"));
    assert!(icon.body.starts_with(b"\x89PNG\r\n\x1a\n"));
}

struct Response {
    status: StatusCode,
    headers: HeaderMap,
    body: Vec<u8>,
}

impl Response {
    fn text(&self) -> &str {
        std::str::from_utf8(&self.body).expect("UTF-8 HTML response")
    }
}

async fn get(app: &Router, uri: &str) -> Response {
    tokio::time::timeout(Duration::from_secs(10), async {
        let response = app
            .clone()
            .oneshot(
                Request::get(uri)
                    .body(Body::empty())
                    .expect("valid request"),
            )
            .await
            .expect("infallible router");
        let (parts, body) = response.into_parts();
        let bytes = Limited::new(body, 4 * 1024 * 1024)
            .collect()
            .await
            .expect("bounded complete response")
            .to_bytes();
        Response {
            status: parts.status,
            headers: parts.headers,
            body: bytes.to_vec(),
        }
    })
    .await
    .expect("HTTP request completes within ten seconds")
}

fn attribute_values<'a>(html: &'a str, name: &str) -> Vec<&'a str> {
    let marker = format!("{name}=\"");
    html.split(&marker)
        .skip(1)
        .map(|tail| tail.split('"').next().expect("attribute value"))
        .collect()
}

fn project_ids(response: &Response) -> Vec<u64> {
    attribute_values(response.text(), "data-project-id")
        .into_iter()
        .map(|value| value.parse().expect("numeric project identity"))
        .collect()
}

fn pagination_link(response: &Response, relation: &str) -> Option<String> {
    response.text().split("<a ").skip(1).find_map(|link| {
        let tag = link.split('>').next()?;
        tag.contains(&format!("rel=\"{relation}\"")).then(|| {
            attribute_values(tag, "href")[0]
                .replace("&#38;", "&")
                .replace("&amp;", "&")
        })
    })
}

fn token_ids(response: &Response) -> Vec<u64> {
    attribute_values(response.text(), "data-token-id")
        .iter()
        .map(|value| value.parse().unwrap())
        .collect()
}

fn fixture_project_ids() -> BTreeSet<u64> {
    let value: serde_json::Value =
        serde_json::from_str(include_str!("../assets/fixtures/projects.json")).unwrap();
    value
        .as_array()
        .unwrap()
        .iter()
        .map(|project| project["id"].as_u64().unwrap())
        .collect()
}

#[tokio::test]
async fn header_ends_with_wallet_control_and_about_stays_in_footer() {
    let archive = TestArchive::imported();
    let app = archive.app();
    for route in [
        "/projects".to_owned(),
        format!("/token/{CONTRACT}/133459"),
        "/about".to_owned(),
    ] {
        let response = get(&app, &route).await;
        assert_eq!(response.status, StatusCode::OK);
        let html = response.text();
        let header = html
            .split("<header")
            .nth(1)
            .unwrap()
            .split("</header>")
            .next()
            .unwrap();
        assert!(header.contains("aria-label=\"Primary\""));
        let account_access = header.split("class=\"account-access\"").nth(1).unwrap();
        assert!(
            account_access.contains("class=\"connect-wallet\" data-auth-link>Connect wallet</a>")
        );
        assert!(account_access.contains("href=\"/auth\""));
        let search = header.split("id=\"global-search-form\"").nth(1).unwrap();
        assert!(search.contains("action=\"/search\""));
        assert!(
            search.contains(
                "<label class=\"sr-only\" for=\"global-search\">Search the archive</label>"
            )
        );
        assert!(!header.contains("href=\"/about\""));
        let footer = html
            .split("<footer")
            .nth(1)
            .unwrap()
            .split("</footer>")
            .next()
            .unwrap();
        assert!(footer.contains("<a class=\"wordmark\" href=\"/\">seeded.art</a>"));
        assert!(!footer.contains("An archive by"));
        assert!(footer.contains(
            "<a class=\"footer-forum\" href=\"https://forum.seeded.art/\">Forum</a>\n    <a href=\"/about\">About</a>"
        ));
        assert_eq!(footer.matches("<a ").count(), 3);
        assert!(!footer.contains("Snapshot"));
        assert!(!footer.contains("footer-snapshot"));
        assert!(!html.contains("Connect address"));
    }
}

#[tokio::test]
async fn project_information_uses_publication_and_unfiltered_supply_with_safe_metadata() {
    let archive = TestArchive::chain_imported();
    let connection = rusqlite::Connection::open(&archive.database).unwrap();
    connection.execute(
        "UPDATE projects SET created_at='2001-02-03T04:05:06Z', editions=14, supply=48, tags=?1 WHERE id=29437",
        [r#"["trees","<script>unsafe()</script>"]"#],
    ).unwrap();
    let app = archive.app();
    let response = get(&app, "/project/29437?q=no-matching-edition").await;
    assert_eq!(response.status, StatusCode::OK);
    let html = response.text();
    assert!(html.contains("<dt>Minted</dt><dd>14 / 48</dd>"));
    assert!(html.contains("<dt>Published</dt><dd>2001-02-03</dd>"));
    assert!(!html.contains("<dt>First mint</dt>"));
    assert!(!html.contains("<dt>Last mint</dt>"));
    assert!(html.contains("<summary>Project information</summary>"));
    assert!(html.contains("<dt>fxhash project ID</dt><dd>29437</dd>"));
    assert!(html.contains("<dt>Token contract</dt>"));
    assert!(html.contains("<dt>Issuer (mint) contract</dt>"));
    assert!(html.contains("KT1Xpmp15KfqoePNW9HczFmqaGNHwadV2a3b"));
    assert!(html.contains("fxhash snippet · params: yes"));
    assert!(html.contains("/profile/tz1gL9H53BzjDqmMi2JkKALeCzT7UAoopzEL"));
    assert!(html.contains("/profile/tz1exYBh5zHhf7LKyJSKAQqm89ZYZmTwsBz2"));
    assert!(
        html.contains("onchfs://a71e98fced01c865f33be138100b5ea5486f32fc333ad1503f814b201234130c")
    );
    assert!(html.contains("&#60;script&#62;unsafe()&#60;/script&#62;"));
    assert!(!html.contains("<script>unsafe()</script>"));
}

#[tokio::test]
async fn minted_stat_omits_unknown_supply_and_never_invents_an_unindexed_count() {
    let archive = TestArchive::imported();
    let connection = rusqlite::Connection::open(&archive.database).unwrap();
    let app = archive.app();
    for (state, editions, supply, expected) in [
        ("indexed", Some(1), Some(100), "1 / 100"),
        ("indexed", Some(1), None, "1"),
        ("indexed", None, Some(100), "Not yet indexed"),
        ("not_indexed", None, None, "Not yet indexed"),
        ("not_indexed", None, Some(100), "Not yet indexed"),
        ("not_indexed", Some(1), Some(100), "Not yet indexed"),
    ] {
        connection
            .execute(
                "UPDATE projects SET index_state=?1, editions=?2, supply=?3 WHERE id=29437",
                rusqlite::params![state, editions, supply],
            )
            .unwrap();
        let response = get(&app, "/project/29437").await;
        assert_eq!(response.status, StatusCode::OK);
        let html = response.text();
        assert!(
            html.contains(&format!("<dt>Minted</dt><dd>{expected}</dd>")),
            "{state}, {editions:?}, {supply:?}"
        );
        assert!(html.contains("<dt>Published</dt>"));
        assert!(html.contains("<dt>Network</dt><dd>Tezos</dd>"));
    }
}

#[tokio::test]
async fn missing_project_metadata_is_explicit_and_does_not_reuse_mint_dates() {
    let archive = TestArchive::imported();
    let app = archive.app();
    let response = get(&app, "/project/29437").await;
    assert_eq!(response.status, StatusCode::OK);
    assert!(
        response
            .text()
            .contains("<dt>Published</dt><dd>Not yet indexed</dd>")
    );
    assert!(
        response
            .text()
            .contains("<dt>Tags</dt><dd>Not yet indexed</dd>")
    );
    assert!(!response.text().contains("<dt>First mint</dt>"));
}

#[tokio::test]
async fn home_lists_projects_and_pages_render_without_javascript() {
    let archive = TestArchive::imported();
    let app = archive.app();
    let home = get(&app, "/").await;
    assert_eq!(home.status, StatusCode::OK);
    assert!(home.text().contains("<h1>Projects</h1>"));
    assert!(
        home.text()
            .contains("<link rel=\"canonical\" href=\"http://127.0.0.1:3918/\">")
    );
    for path in [
        "/projects",
        "/project/29437",
        "/artists",
        "/profile/emptyname",
        "/about",
    ] {
        let response = get(&app, path).await;
        assert_eq!(response.status, StatusCode::OK, "{path}");
        assert!(
            response.headers["content-type"]
                .to_str()
                .unwrap()
                .starts_with("text/html")
        );
        assert!(response.text().contains("seeded.art"));
        assert!(
            response.text().contains("id=\"main-content\""),
            "SSR content {path}"
        );
    }
}

#[tokio::test]
async fn catalogue_search_treats_operators_and_markup_as_literal_input() {
    let archive = TestArchive::imported();
    let app = archive.app();
    let moved = get(&app, "/projects?q=Arboreal").await;
    assert_eq!(moved.status, StatusCode::SEE_OTHER);
    assert_eq!(moved.headers["location"], "/search?q=Arboreal");
    let found = get(&app, "/search?q=Arboreal").await;
    assert_eq!(found.status, StatusCode::OK);
    assert_eq!(project_ids(&found), vec![29437]);
    let artist = get(&app, "/search?q=zancan").await;
    assert_eq!(project_ids(&artist).len(), 4);

    for query in [
        "no-such-project-734",
        "' OR 1=1--",
        "Arboreal OR Garden",
        "<script>alert(1)</script>",
    ] {
        let encoded = utf8_percent_encode(query, NON_ALPHANUMERIC);
        let moved = get(&app, &format!("/projects?q={encoded}")).await;
        assert_eq!(moved.status, StatusCode::SEE_OTHER, "literal query {query}");
        assert_eq!(moved.headers["location"], format!("/search?q={encoded}"));
        let response = get(&app, &format!("/search?q={encoded}")).await;
        assert_eq!(response.status, StatusCode::OK, "literal query {query}");
        assert!(project_ids(&response).is_empty(), "literal query {query}");
        assert!(!response.text().contains("<script>alert(1)</script>"));
    }
    assert_eq!(
        project_ids(&get(&app, "/search?q=Arboreal").await),
        vec![29437]
    );
}

#[tokio::test]
async fn catalogue_pagination_returns_every_project_once_in_requested_order() {
    let archive = TestArchive::imported();
    let app = archive.app();
    let expected = fixture_project_ids();
    let mut found = Vec::new();
    let mut next = Some("/projects?sort=oldest".to_owned());
    while let Some(url) = next {
        let response = get(&app, &url).await;
        assert_eq!(response.status, StatusCode::OK);
        let ids = project_ids(&response);
        assert!(
            !ids.is_empty(),
            "pagination stopped before all projects were returned"
        );
        if found.is_empty() {
            assert!(ids.len() < expected.len(), "fixture spans multiple pages");
            assert!(
                pagination_link(&response, "next").is_some_and(|url| url.contains("after=")),
                "next page is discoverable"
            );
        }
        found.extend(ids);
        assert!(found.len() <= expected.len());
        next = pagination_link(&response, "next");
    }
    assert_eq!(found, expected.into_iter().collect::<Vec<_>>());
    for (sort, first) in [
        ("newest", 29437),
        ("oldest", 2),
        // Retired sorts and the default use highest volume; with no recorded volume the
        // order falls back to project id.
        ("volume", 2),
        ("title", 2),
        ("artist", 2),
        ("id", 2),
    ] {
        let response = get(&app, &format!("/projects?sort={sort}&page=1")).await;
        assert_eq!(response.status, StatusCode::OK);
        assert_eq!(project_ids(&response).first(), Some(&first), "sort {sort}");
    }
}

#[tokio::test]
async fn project_sort_control_defaults_to_highest_volume_and_keeps_form_filters() {
    let archive = TestArchive::imported();
    let app = archive.app();
    for path in ["/projects", "/projects?sort=title", "/projects?sort=artist"] {
        let response = get(&app, path).await;
        assert_eq!(response.status, StatusCode::OK);
        assert_eq!(project_ids(&response).first(), Some(&2), "{path}");
        let html = response.text();
        let sort_control = html
            .split("<select name=\"sort\"")
            .nth(1)
            .unwrap()
            .split("</select>")
            .next()
            .unwrap();
        assert_eq!(sort_control.matches("<option value=").count(), 3);
        assert!(html.contains("<option value=\"volume\" selected>Highest volume</option>"));
        assert!(html.contains("value=\"oldest\""));
        assert!(html.contains(">Oldest</option>"));
        assert!(html.contains("value=\"volume\"") && html.contains(">Highest volume</option>"));
        assert!(!html.contains("Title A–Z") && !html.contains("Artist A–Z"));
        assert!(!html.contains("Lowest volume"));
        assert!(!html.contains(">Filter</button>") && !html.contains("Filter projects"));
        assert!(!html.contains("class=\"reset-link\" href=\"/projects\""));
        let global_form = html.split("id=\"global-search-form\"").nth(1).unwrap();
        assert!(
            global_form
                .split('>')
                .next()
                .unwrap()
                .contains("action=\"/search\"")
        );
        assert!(
            html.contains(
                "<label class=\"sr-only\" for=\"global-search\">Search the archive</label>"
            )
        );
        assert!(html.contains("id=\"project-sort\""));
        assert!(html.contains("action=\"/projects\""));
        assert!(html.contains("/static/project_sort.js"));
        assert!(
            pagination_link(&response, "next")
                .unwrap()
                .contains("sort=volume")
        );
    }
    let filtered = get(&app, "/projects?artist=zancan&sort=oldest").await;
    assert_eq!(filtered.status, StatusCode::OK);
    assert!(!filtered.text().contains("name=\"q\" value=\"Arboreal\""));
    assert!(filtered.text().contains("name=\"artist\" value=\"zancan\""));
    assert!(
        filtered
            .text()
            .contains("<a class=\"reset-link\" href=\"/projects\">Clear filters</a>")
    );
    let global_form = filtered
        .text()
        .split("id=\"global-search-form\"")
        .nth(1)
        .unwrap();
    assert!(
        global_form
            .split('>')
            .next()
            .unwrap()
            .contains("action=\"/search\"")
    );
    assert!(filtered.text().contains("id=\"project-sort\""));
    // The header search is the same form on every page; it carries no page sort.
    assert!(
        !global_form
            .split("</form>")
            .next()
            .unwrap()
            .contains("type=\"hidden\"")
    );
    assert!(
        filtered
            .text()
            .contains("<option value=\"oldest\" selected>Oldest</option>")
    );
}

#[tokio::test]
async fn retired_project_sort_cursors_restart_at_the_default_sort() {
    let archive = TestArchive::imported();
    let store = Store::open(&archive.database).unwrap();
    for (alias, old_sort) in [
        ("title", ProjectSort::TitleAsc),
        ("artist", ProjectSort::ArtistAsc),
    ] {
        let page = store
            .projects_page(
                &ProjectFilter {
                    text: None,
                    artist: None,
                    sort: old_sort,
                },
                None,
                1,
            )
            .unwrap();
        let cursor = page.next.unwrap();
        let app = archive.app();
        let response = get(
            &app,
            &format!("/projects?sort={alias}&after={}&page=1", cursor.as_str()),
        )
        .await;
        assert_eq!(response.status, StatusCode::OK, "alias {alias}");
        assert_eq!(project_ids(&response).first(), Some(&2), "alias {alias}");
        assert!(
            pagination_link(&response, "next")
                .unwrap()
                .contains("sort=volume")
        );
    }
}

#[tokio::test]
async fn comparison_and_token_page_keep_real_edition_identity_and_snapshot_data() {
    let archive = TestArchive::imported();
    let app = archive.app();
    let compared = get(&app, "/project/29437?view=compare&left=1&right=9").await;
    assert_eq!(compared.status, StatusCode::PERMANENT_REDIRECT);
    assert_eq!(compared.headers["location"], "/project/29437");
    let first = get(&app, &format!("/token/{CONTRACT}/133459")).await;
    assert_eq!(first.status, StatusCode::OK);
    assert!(!first.text().contains("Compare with another edition"));
    for fact in [
        SEED_ONE,
        "133459",
        "2023-11-10",
        "tz2RXosjAZdS85oCrguy7Eom757144dcU1FT",
        "Gold",
    ] {
        assert!(first.text().contains(fact), "real token fact {fact}");
    }
    let main = first
        .text()
        .split("<main")
        .nth(1)
        .unwrap()
        .split("</main>")
        .next()
        .unwrap();
    assert!(main.to_lowercase().contains("snapshot"));
    let traits = main.find("<h2>Traits</h2>").expect("traits section");
    let history = main.find("Price history").expect("price history");
    assert!(traits < history, "traits come before price history");
    assert!(
        first
            .text()
            .contains(&format!("https://objkt.com/tokens/{CONTRACT}/133459"))
    );
    assert!(
        first.text().contains(&format!("/token/{CONTRACT}/133460")),
        "next edition"
    );
    let ninth = get(&app, &format!("/token/{CONTRACT}/185776")).await;
    assert_eq!(ninth.status, StatusCode::OK);
    assert!(ninth.text().contains("Light") && ninth.text().contains("7 of 10"));
}

#[tokio::test]
async fn artist_routes_separate_authored_projects_from_known_holdings() {
    let archive = TestArchive::imported();
    let app = archive.app();
    let directory = get(&app, "/search?q=EmptyName&scope=people").await;
    assert_eq!(directory.status, StatusCode::OK);
    assert!(
        directory
            .text()
            .contains("href=\"/profile/tz1gL9H53BzjDqmMi2JkKALeCzT7UAoopzEL\"")
    );
    let projects = get(&app, "/profile/emptyname?view=projects").await;
    assert_eq!(projects.status, StatusCode::OK);
    assert_eq!(project_ids(&projects), vec![29437]);
    assert!(
        !projects
            .text()
            .contains("class=\"profile-website\" href=\"https://emptyname.org\"")
    );
    let collected = get(&app, "/profile/emptyname?view=collected").await;
    assert_eq!(collected.status, StatusCode::OK);
    assert!(
        collected
            .text()
            .contains(&format!("/token/{CONTRACT}/133460"))
    );
    assert!(collected.text().contains("2026-10-02"));
    assert!(
        !collected
            .text()
            .contains(&format!("/token/{CONTRACT}/133459"))
    );
}

#[tokio::test]
async fn missing_records_return_not_found() {
    let archive = TestArchive::imported();
    let app = archive.app();
    for path in [
        "/project/999999".to_owned(),
        format!("/token/{CONTRACT}/999999"),
        "/profile/not-a-fixture-artist".to_owned(),
        "/route-that-does-not-exist".to_owned(),
    ] {
        let response = get(&app, &path).await;
        assert_eq!(response.status, StatusCode::NOT_FOUND, "{path}");
        assert!(
            !response.text().contains(SEED_ONE),
            "unknown identity must not fall back"
        );
    }
}

#[tokio::test]
async fn malformed_and_out_of_bounds_queries_return_bad_request() {
    let archive = TestArchive::imported();
    let app = archive.app();
    let long_query = format!("/projects?q={}", "a".repeat(201));
    for path in [
        "/projects?page=0",
        "/projects?page=-1",
        "/projects?page=nope",
        "/projects?page=4294967295",
        "/projects?sort=not-a-sort",
        "/artists?page=0",
        "/project/nope",
        "/project/18446744073709551615",
        "/project/29437?view=invalid",
        "/profile/emptyname?view=invalid",
        long_query.as_str(),
    ] {
        assert_eq!(
            get(&app, path).await.status,
            StatusCode::BAD_REQUEST,
            "{path}"
        );
    }
}

#[tokio::test]
async fn cursor_after_deleted_rows_keeps_count_and_first_page_recovery() {
    let archive = TestArchive::imported();
    let app = archive.app();
    let first = get(&app, "/projects?sort=oldest").await;
    let next = pagination_link(&first, "next").unwrap();
    rusqlite::Connection::open(&archive.database)
        .unwrap()
        .execute("UPDATE projects SET hidden=1 WHERE id=29437", [])
        .unwrap();
    let response = get(&app, &next).await;
    assert_eq!(response.status, StatusCode::OK);
    assert!(response.text().contains("24 projects"));
    assert!(response.text().contains("No projects on this page"));
    assert!(!response.text().contains("No matching projects"));
    assert_eq!(
        pagination_link(&response, "first").as_deref(),
        Some("/projects?artist=&sort=oldest")
    );
    assert!(pagination_link(&response, "next").is_none());
}

#[tokio::test]
async fn missing_database_is_retryable_without_creation_or_negative_caching() {
    let archive = TestArchive::empty();
    let app = archive.app();
    let response = get(&app, "/projects").await;
    assert_eq!(response.status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response.headers["retry-after"], "2");
    assert!(
        !archive.database.exists(),
        "HTTP reads must not create a database"
    );
    assert!(
        !response
            .text()
            .contains(&archive.database.display().to_string()),
        "no local path in errors"
    );
    archive.import();
    assert_eq!(
        get(&app, "/projects").await.status,
        StatusCode::OK,
        "retry after import"
    );
}

#[tokio::test]
async fn schema_without_a_snapshot_is_unavailable_until_import_completes() {
    let archive = TestArchive::empty();
    drop(Store::open(&archive.database).expect("create unimported schema"));
    let app = archive.app();
    for path in [
        "/projects",
        "/project/29437",
        "/artists",
        "/profile/emptyname",
        "/about",
    ] {
        let response = get(&app, path).await;
        assert_eq!(response.status, StatusCode::SERVICE_UNAVAILABLE, "{path}");
        assert_eq!(response.headers["retry-after"], "2");
    }
    archive.import();
    assert_eq!(get(&app, "/project/29437").await.status, StatusCode::OK);
}

#[tokio::test]
async fn corrupt_database_is_an_internal_error_without_path_leak_or_replacement() {
    let archive = TestArchive::empty();
    let original = b"not a SQLite database; preserve this file";
    fs::write(&archive.database, original).expect("write corrupt database fixture");
    let response = get(&archive.app(), "/projects").await;
    assert_eq!(response.status, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(
        !response
            .text()
            .contains(&archive.database.display().to_string())
    );
    assert_eq!(fs::read(&archive.database).unwrap(), original);
}

#[tokio::test]
async fn static_routes_serve_previews_without_exposing_fixtures_sources_or_database() {
    let archive = TestArchive::imported();
    let app = archive.app();
    let token = get(&app, &format!("/token/{CONTRACT}/133459")).await;
    let image = attribute_values(token.text(), "src")
        .into_iter()
        .find(|source| source.contains("arboreal"))
        .expect("local artwork preview");
    assert!(image.starts_with('/'));
    let preview = get(&app, image).await;
    assert_eq!(preview.status, StatusCode::OK);
    assert!(
        preview.headers["content-type"]
            .to_str()
            .unwrap()
            .starts_with("image/")
    );
    assert!(preview.body.starts_with(b"RIFF"), "actual WebP image");
    let database_route = format!(
        "/{}",
        archive
            .database
            .strip_prefix(project_root())
            .unwrap()
            .display()
    );
    for path in [
        "/Cargo.toml",
        "/src/main.rs",
        "/README.md",
        "/assets/fixtures/tokens.json",
        "/fixtures/tokens.json",
        "/fixtures/projects.json",
        "/fixtures/snapshot.json",
        "/fixtures/previews/../tokens.json",
        "/fixtures/previews/%2e%2e/tokens.json",
        "/static/../fixtures/tokens.json",
        "/static/%2e%2e/%2e%2e/Cargo.toml",
        database_route.as_str(),
    ] {
        let response = get(&app, path).await;
        assert!(
            matches!(
                response.status,
                StatusCode::NOT_FOUND | StatusCode::BAD_REQUEST
            ),
            "private route {path}: {}",
            response.status
        );
        assert!(!response.body.starts_with(b"SQLite format 3"));
        assert!(!response.text().contains("[package]"));
    }
}

#[tokio::test]
async fn liveness_is_independent_but_readiness_requires_the_local_snapshot() {
    let archive = TestArchive::empty();
    let app = archive.app();
    assert_eq!(
        get(&app, "/health/live").await.status,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        get(&app, "/health/ready").await.status,
        StatusCode::SERVICE_UNAVAILABLE
    );
    archive.import();
    assert_eq!(
        get(&app, "/health/ready").await.status,
        StatusCode::NO_CONTENT
    );
}

#[tokio::test]
async fn readiness_fails_and_recovers_when_cache_staging_is_unavailable() {
    let archive = TestArchive::imported();
    let cache = archive.directory.join("cache");
    let content = seeded_archive::content::ContentStore::new(
        seeded_archive::content::ContentConfig::mainnet(&cache),
    )
    .unwrap();
    let app = router(
        AppState::new(&archive.database, project_root().join("assets")).with_content(content),
    );
    assert_eq!(
        get(&app, "/health/ready").await.status,
        StatusCode::NO_CONTENT
    );
    fs::rename(cache.join("tmp"), cache.join("tmp-held")).unwrap();
    fs::write(cache.join("tmp"), b"unavailable directory").unwrap();
    assert_eq!(
        get(&app, "/health/live").await.status,
        StatusCode::NO_CONTENT
    );
    let unavailable = get(&app, "/health/ready").await;
    assert_eq!(unavailable.status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(unavailable.headers["cache-control"], "no-store");
    fs::remove_file(cache.join("tmp")).unwrap();
    fs::rename(cache.join("tmp-held"), cache.join("tmp")).unwrap();
    assert_eq!(
        get(&app, "/health/ready").await.status,
        StatusCode::NO_CONTENT
    );
}

#[tokio::test]
async fn edition_comparison_is_removed_and_old_links_land_on_the_project() {
    let archive = TestArchive::imported();
    let app = archive.app();
    let project = get(&app, "/project/29437").await;
    assert_eq!(project.status, StatusCode::OK);
    for removed in [
        "Compare editions",
        "view=compare",
        "Swap editions",
        "Traits, side by side",
    ] {
        assert!(!project.text().contains(removed), "{removed}");
    }
    let projects = get(&app, "/projects").await;
    assert!(!projects.text().contains("view=compare"));
    for old in [
        "/project/29437?view=compare",
        "/project/29437?view=compare&left=1&right=1",
    ] {
        let response = get(&app, old).await;
        assert_eq!(response.status, StatusCode::PERMANENT_REDIRECT, "{old}");
        assert_eq!(response.headers["location"], "/project/29437");
    }
}

#[tokio::test]
async fn project_cards_show_the_published_date_instead_of_the_project_id() {
    let archive = TestArchive::imported();
    let html = get(&archive.app(), "/projects").await;
    assert_eq!(html.status, StatusCode::OK);
    let cards = html.text().split("class=\"project-grid\"").nth(1).unwrap();
    assert!(cards.contains("<dt>Published</dt>"));
    assert!(!cards.contains("<dt>Project ID</dt>"));
    let project = get(&archive.app(), "/project/29437").await;
    assert!(
        project
            .text()
            .contains("<dt>fxhash project ID</dt><dd>29437</dd>")
    );
}

#[tokio::test]
async fn first_project_cards_and_edition_preview_load_eagerly_at_high_priority() {
    let archive = TestArchive::imported();
    let html = get(&archive.app(), "/projects").await;
    assert_eq!(html.status, StatusCode::OK);
    let images = html
        .text()
        .split("<article class=\"project-card\"")
        .skip(1)
        .map(|card| {
            card.split("</article>")
                .next()
                .unwrap()
                .split("<img ")
                .nth(1)
                .and_then(|tail| tail.split('>').next())
                .expect("card preview image")
        })
        .collect::<Vec<_>>();
    assert!(images.len() > 3, "{}", images.len());
    for (index, image) in images.iter().enumerate() {
        assert_eq!(
            image.contains("loading=\"lazy\""),
            index >= 3,
            "card {index}"
        );
        assert_eq!(
            image.contains("fetchpriority=\"high\""),
            index == 0,
            "card {index}"
        );
    }
    // Without playback the static preview is the edition's main image.
    let token = get(&archive.app(), &format!("/token/{CONTRACT}/133459")).await;
    let preview = token
        .text()
        .split("class=\"nft-artwork-frame\"")
        .nth(1)
        .and_then(|tail| tail.split("<img ").nth(1))
        .and_then(|tail| tail.split('>').next())
        .expect("static edition preview");
    assert!(preview.contains("fetchpriority=\"high\""));
}

#[tokio::test]
async fn owner_labels_preserve_complete_addresses_without_invalid_aliases() {
    let archive = TestArchive::imported();
    let token = get(&archive.app(), &format!("/token/{CONTRACT}/133459")).await;
    assert_eq!(token.status, StatusCode::OK);
    assert!(
        token
            .text()
            .contains("tz2RXosjAZdS85oCrguy7Eom757144dcU1FT")
    );
}

#[tokio::test]
async fn chain_catalogue_previews_use_only_the_isolated_content_origin() {
    let archive = TestArchive::chain_imported();
    let app = archive.configured_app();
    for route in [
        "/projects",
        "/project/29437",
        "/token/KT1EfsNuqwLAWDd3o4pvfUx1CAh5GMdTrRvr/133459",
    ] {
        let response = get(&app, route).await;
        assert_eq!(response.status, StatusCode::OK, "{route}");
        assert!(
            response
                .text()
                .contains("http://127.0.0.1:3919/content/ipfs/"),
            "{route}"
        );
        assert!(!response.text().contains("local sample"));
        for source in attribute_values(response.text(), "src") {
            assert!(
                source.starts_with('/') || source.starts_with("http://127.0.0.1:3919/"),
                "{source}"
            );
        }
        assert!(
            response.headers["content-security-policy"]
                .to_str()
                .unwrap()
                .contains("img-src 'self' data: http://127.0.0.1:3919")
        );
    }
    let token = get(&app, "/token/KT1EfsNuqwLAWDd3o4pvfUx1CAh5GMdTrRvr/133459").await;
    assert!(token.text().contains("Tezos via TzKT"));
}

#[tokio::test]
async fn collector_address_pages_show_real_holdings_and_project_titles() {
    let archive = TestArchive::chain_imported();
    let app = archive.configured_app();
    let address = ingest_support::QUADRADO;
    let response = get(&app, &format!("/profile/{address}?view=collected")).await;
    assert_eq!(response.status, StatusCode::OK);
    assert!(response.text().contains(address));
    assert!(response.text().contains("Arboreal #"));
    let expected = Store::open_read_only(&archive.database)
        .unwrap()
        .collector_holdings(
            address,
            seeded_archive::store::PageRequest {
                offset: 0,
                limit: 24,
            },
        )
        .unwrap();
    let actual: BTreeSet<_> = attribute_values(response.text(), "data-token-id")
        .into_iter()
        .map(|s| s.parse::<u64>().unwrap())
        .collect();
    assert_eq!(actual, expected.items.iter().map(|t| t.token_id).collect());
    let lookup = get(&app, &format!("/collectors?address={address}")).await;
    assert_eq!(lookup.status, StatusCode::SEE_OTHER);
    assert_eq!(
        lookup.headers["location"],
        format!("/profile/{address}?view=collected")
    );
    assert_eq!(get(&app, "/collectors").await.status, StatusCode::OK);
    assert_eq!(
        get(&app, "/profile/not-an-address").await.status,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        get(&app, &format!("/profile/{address}?page=0"))
            .await
            .status,
        StatusCode::BAD_REQUEST
    );
    let token = get(
        &app,
        &format!(
            "/token/{}/{}",
            expected.items[0].contract, expected.items[0].token_id
        ),
    )
    .await;
    assert!(
        token
            .text()
            .contains(&format!("href=\"/profile/{address}\""))
    );
}

#[tokio::test]
async fn about_page_lists_available_features_plans_and_credits() {
    let archive = TestArchive::imported();
    let about = get(&archive.app(), "/about").await;
    assert_eq!(about.status, StatusCode::OK);
    let text = about.text();
    for expected in [
        "<h1>About seeded.art</h1>",
        // Every page loads the usage counter from the same origin.
        "<script data-goatcounter=\"/stats/count\" async src=\"/stats/count.js\"></script>",
        "Please use the <a href=\"https://forum.seeded.art\">forum</a> for all communication.</p>",
        "<dt>Projects</dt><dd>",
        "<dt>NFTs</dt><dd>",
        "Seeded.art is an archive of NFTs rescued from fxhash after its shutdown.",
        "<h2>Available now</h2>",
        "Accepting or cancelling offers made on fxhash before it shut down.",
        "<h2>What’s next</h2>",
        "Only an NFT’s current owner would be able to initiate its migration to Seeded.art.",
        "the details will change during development.",
        "<h2>Credits</h2>",
    ] {
        assert!(text.contains(expected), "{expected}");
    }
    // Credits lines, in order.
    let mut from = text.find("<h2>Credits</h2>").unwrap();
    for line in [
        "Developed by <a href=\"https://emptyname.org\"",
        "send a private message on the <a href=\"https://forum.seeded.art/u/master\">forum</a>.</li>",
        "Frank Force’s fxhashArchive</a> (MIT license).</li>",
        "<a href=\"https://github.com/emptyname-org\" target=\"_blank\" rel=\"noopener noreferrer\">Seeded.art on GitHub</a></li>",
        "<li>Snapshot date: ",
        "<li>Not affiliated with fxhash or objkt. Artwork rights remain with the creators.</li>",
    ] {
        let at = text[from..].find(line).unwrap_or_else(|| panic!("{line}"));
        from += at + line.len();
    }
    assert!(!text.contains("Sources &amp; snapshot"));
    assert!(!text.contains("Import provenance"));
}

#[tokio::test]
async fn public_coverage_counts_and_flagged_routes_match_the_imported_index() {
    let archive = TestArchive::chain_imported();
    let app = archive.app();
    let status = Store::open_read_only(&archive.database)
        .unwrap()
        .index_status()
        .unwrap();
    let about = app
        .clone()
        .oneshot(
            Request::get("/about")
                .header(header::ACCEPT, "application/json")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(about.status(), StatusCode::OK);
    let body = about.into_body().collect().await.unwrap().to_bytes();
    let about: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let counts = &about["data"]["status"];
    assert_eq!(counts["projects_total"], status.projects_total);
    assert_eq!(counts["tokens_total"], status.tokens_total);
    assert_eq!(
        counts["projects_flagged_hidden"],
        status.projects_flagged_hidden
    );
    assert_eq!(
        get(&app, &format!("/project/{}", ingest_support::FLAGGED))
            .await
            .status,
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn artist_collections_prefer_recorded_wallet_over_conflicting_aliases() {
    let archive = TestArchive::imported();
    let connection = rusqlite::Connection::open(&archive.database).unwrap();
    let address: String = connection
        .query_row(
            "SELECT owner_address FROM tokens WHERE token_id=133460",
            [],
            |row| row.get(0),
        )
        .unwrap();
    connection
        .execute(
            "UPDATE artists SET address=?1 WHERE id='emptyname'",
            [&address],
        )
        .unwrap();
    connection.execute("UPDATE tokens SET owner_alias='EmptyName', owner_alias_key='emptyname' WHERE token_id=133459", []).unwrap();
    drop(connection);
    let page = get(&archive.app(), "/profile/emptyname?view=collected").await;
    assert_eq!(page.status, StatusCode::OK);
    let ids = attribute_values(page.text(), "data-token-id");
    assert!(ids.contains(&"133460"));
    assert!(!ids.contains(&"133459"));
    assert!(page.text().contains("Ownership snapshot"));
}

#[tokio::test]
async fn unusable_optional_cache_is_disclosed_without_hiding_a_ready_archive() {
    let archive = TestArchive::imported();
    let cache = archive.directory.join("cache-file");
    fs::write(&cache, b"not a directory").unwrap();
    let content = seeded_archive::content::ContentStore::new(
        seeded_archive::content::ContentConfig::mainnet(&cache),
    )
    .unwrap();
    assert!(!content.cache_enabled());
    let app = router(
        AppState::new(&archive.database, project_root().join("assets")).with_content(content),
    );
    let ready = get(&app, "/health/ready").await;
    assert_eq!(ready.status, StatusCode::NO_CONTENT);
    assert_eq!(ready.headers["x-seeded-cache"], "bypassed");
    assert_eq!(fs::read(cache).unwrap(), b"not a directory");
}

fn add_cursor_catalogue(archive: &TestArchive) {
    let connection = rusqlite::Connection::open(&archive.database).unwrap();
    connection.execute_batch(
        "BEGIN;
         WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<30)
         INSERT INTO artists(id,name,name_key)
         SELECT printf('cursor-artist-%02d',i), printf('Cursor Artist %02d',i), printf('cursor artist %02d',i) FROM n;
         WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<60)
         INSERT INTO projects(id,title,title_key,artist_key,index_state)
         SELECT 900000+i, printf('Cursor Project %02d',i),printf('cursor project %02d',i),'cursor artist','not_indexed' FROM n;
         INSERT INTO project_artists(project_id,position,artist_id)
         SELECT id,0,printf('cursor-artist-%02d',(id-900001)/2+1) FROM projects WHERE id>900000;
         INSERT INTO project_artists(project_id,position,artist_id)
         SELECT id,1,'emptyname' FROM projects WHERE id>900000;
         INSERT INTO project_fts(rowid,title,artists)
         SELECT id,title,'Cursor Artist EmptyName' FROM projects WHERE id>900000;
         INSERT INTO artist_fts(rowid,name,aliases)
         SELECT seq,name,name FROM artists WHERE id LIKE 'cursor-artist-%';
         COMMIT;"
    ).unwrap();
}

fn add_cursor_volumes(archive: &TestArchive) {
    let connection = rusqlite::Connection::open(&archive.database).unwrap();
    for index in 1_u32..=60 {
        let volume = match index {
            1 | 2 => Some(30_000_001_u64),
            3..=30 => Some(u64::from(index - 2) * 1_000_000),
            31 => Some(0),
            _ => None,
        };
        if let Some(volume) = volume {
            let primary = if index == 1 { 1_500_000 } else { volume };
            let secondary = volume - primary;
            connection
                .execute(
                    "INSERT INTO project_volume
                     (project_id,volume_mutez,primary_mutez,secondary_mutez,
                      sales_count,unpriced_count,computed_at,sales_level)
                     VALUES (?1,?2,?3,?4,1,0,'fixture',1)",
                    rusqlite::params![
                        i64::from(900_000 + index),
                        i64::try_from(volume).unwrap(),
                        i64::try_from(primary).unwrap(),
                        i64::try_from(secondary).unwrap()
                    ],
                )
                .unwrap();
        }
    }
}

#[tokio::test]
async fn volume_sort_orders_ties_and_unrecorded_tail_across_filtered_cursor_pages() {
    let archive = TestArchive::imported();
    add_cursor_catalogue(&archive);
    add_cursor_volumes(&archive);
    let app = archive.app();
    let first = "/projects?artist=emptyname&sort=volume".to_owned();
    let mut next = Some(first.clone());
    let mut ids = Vec::new();
    let mut tail_html = String::new();
    let mut page_count = 0;
    while let Some(url) = next {
        let response = get(&app, &url).await;
        assert_eq!(response.status, StatusCode::OK);
        page_count += 1;
        if page_count == 1 {
            let html = response.text();
            assert!(html.contains("<option value=\"volume\" selected>Highest volume</option>"));
            assert!(
                html.contains("<dt>Volume</dt><dd title=\"Includes tez and wXTZ\">30 tez</dd>")
            );
            assert!(!html.contains("Lowest volume"));
        }
        if page_count == 2 {
            tail_html = response.text().to_owned();
        }
        ids.extend(project_ids(&response));
        next = pagination_link(&response, "next");
        if let Some(next_url) = &next {
            assert!(next_url.contains("artist=emptyname&sort=volume"));
            for changed in [
                next_url.replace("artist=emptyname", "artist=zancan"),
                next_url.replace("sort=volume", "sort=newest"),
            ] {
                assert_eq!(
                    get(&app, &changed).await.status,
                    StatusCode::BAD_REQUEST,
                    "{changed}"
                );
            }
        }
    }
    assert_eq!(page_count, 3);
    assert!(tail_html.contains("Includes tez and wXTZ\">0 tez</dd>"));
    assert!(tail_html.contains(">Not recorded</dd>"));
    let mut expected = vec![900001, 900002];
    expected.extend((900003..=900030).rev());
    // Unranked projects (zero or no volume) follow by id; 29437 is also emptyname's.
    expected.push(29437);
    expected.extend(900031..=900060);
    assert_eq!(ids, expected);
    assert_eq!(
        ids[0..2],
        [900001, 900002],
        "equal volumes use id ascending"
    );
}

#[tokio::test]
async fn cursor_links_preserve_sort_and_artist_and_reject_swapped_filters() {
    let archive = TestArchive::imported();
    add_cursor_catalogue(&archive);
    let app = archive.app();
    for (sort, reverse) in [("oldest", false), ("newest", true)] {
        let first = format!("/projects?artist=emptyname&sort={sort}");
        let mut next = Some(first.clone());
        let mut ids = vec![];
        while let Some(url) = next {
            let response = get(&app, &url).await;
            assert_eq!(response.status, StatusCode::OK);
            if !ids.is_empty() {
                assert!(response.text().contains("First page"));
                assert!(!response.text().contains("Previous page"));
                assert_eq!(pagination_link(&response, "first"), Some(first.clone()));
            }
            ids.extend(project_ids(&response));
            assert!(ids.len() <= 61);
            next = pagination_link(&response, "next");
            if let Some(next) = &next {
                assert!(next.contains("artist=emptyname&sort="));
                assert!(!next.contains("page="));
                for changed in [
                    next.replace("artist=emptyname", "artist=zancan"),
                    next.replace(
                        &format!("sort={sort}"),
                        &format!(
                            "sort={}",
                            if sort == "oldest" { "newest" } else { "oldest" }
                        ),
                    ),
                    format!("{next}&page=1"),
                ] {
                    assert_eq!(
                        get(&app, &changed).await.status,
                        StatusCode::BAD_REQUEST,
                        "{changed}"
                    );
                }
            }
        }
        let mut expected: Vec<_> = std::iter::once(29437).chain(900001..=900060).collect();
        if reverse {
            expected.reverse();
        }
        assert_eq!(ids, expected, "{sort}");
    }
    for path in [
        "/projects?after=",
        "/artists?after=bad",
        "/project/29437?after=00",
        "/profile/emptyname?after=bad",
        "/projects?page=2",
    ] {
        assert_eq!(
            get(&app, path).await.status,
            StatusCode::BAD_REQUEST,
            "{path}"
        );
    }
}

#[tokio::test]
async fn artist_directory_and_authored_projects_follow_forward_cursors() {
    let archive = TestArchive::imported();
    add_cursor_catalogue(&archive);
    let connection = rusqlite::Connection::open(&archive.database).unwrap();
    let mut expected_links = BTreeSet::new();
    for id in 1_u8..=30 {
        let address = bs58::encode([&[6, 161, 159][..], &[id; 20]].concat())
            .with_check()
            .into_string();
        connection
            .execute(
                "UPDATE artists SET address=?1 WHERE id=?2",
                [&address, &format!("cursor-artist-{id:02}")],
            )
            .unwrap();
        expected_links.insert(format!("/profile/{address}"));
    }
    drop(connection);
    let app = archive.app();
    let first = get(&app, "/artists").await;
    assert_eq!(first.status, StatusCode::OK);
    let next = pagination_link(&first, "next").unwrap();
    assert!(next.starts_with("/artists?after="));
    let mut pages = vec![first];
    while let Some(url) = pagination_link(pages.last().unwrap(), "next") {
        let page = get(&app, &url).await;
        assert_eq!(page.status, StatusCode::OK);
        assert_eq!(pagination_link(&page, "first").as_deref(), Some("/artists"));
        pages.push(page);
    }
    let links: Vec<_> = pages
        .iter()
        .flat_map(|response| attribute_values(response.text(), "href"))
        .filter(|url| expected_links.contains(*url))
        .collect();
    assert_eq!(links.len(), 30);
    assert_eq!(links.iter().collect::<BTreeSet<_>>().len(), 30);
    assert_eq!(
        get(&app, &next.replace("/artists?", "/projects?"))
            .await
            .status,
        StatusCode::BAD_REQUEST
    );

    let mut next = Some("/profile/emptyname?view=projects".to_owned());
    let mut ids = vec![];
    while let Some(url) = next {
        let response = get(&app, &url).await;
        assert_eq!(response.status, StatusCode::OK);
        ids.extend(project_ids(&response));
        assert!(ids.len() <= 61);
        next = pagination_link(&response, "next");
        if let Some(next) = &next {
            assert_eq!(
                get(
                    &app,
                    &next.replace(
                        "tz1gL9H53BzjDqmMi2JkKALeCzT7UAoopzEL",
                        ingest_support::QUADRADO
                    )
                )
                .await
                .status,
                StatusCode::BAD_REQUEST
            );
        }
    }
    let expected: Vec<_> = (900001..=900060)
        .rev()
        .chain(std::iter::once(29437))
        .collect();
    assert_eq!(ids, expected);
}

#[tokio::test]
async fn collector_and_project_cursors_reach_editions_beyond_ten_thousand() {
    let archive = TestArchive::imported();
    let connection = rusqlite::Connection::open(&archive.database).unwrap();
    connection.execute(
        "WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<10049)
         INSERT INTO tokens(contract,token_id,project_id,iteration,seed,minter,minted,artifact_uri,owner_address,owner_as_of,owner_source,holder_address,owner_alias,owner_alias_key)
         SELECT contract,500000+i,project_id,100+i,seed,minter,minted,artifact_uri,?1,owner_as_of,owner_source,?1,'EmptyName','emptyname'
         FROM tokens,n WHERE token_id=133459",
        [ingest_support::QUADRADO],
    ).unwrap();
    connection
        .execute("UPDATE projects SET editions=10059 WHERE id=29437", [])
        .unwrap();
    drop(connection);
    let app = archive.app();
    let base = format!("/profile/{}?view=collected", ingest_support::QUADRADO);
    let mut next = Some(base.clone());
    let mut found = vec![];
    let mut last = None;
    let mut last_url = String::new();
    while let Some(url) = next {
        let response = get(&app, &url).await;
        assert_eq!(response.status, StatusCode::OK);
        let ids = token_ids(&response);
        assert!(!ids.is_empty());
        assert!(ids.len() <= 24);
        if !found.is_empty() {
            assert_eq!(pagination_link(&response, "first"), Some(base.clone()));
        }
        found.extend(ids);
        assert!(found.len() <= 10053);
        next = pagination_link(&response, "next");
        if next.is_none() {
            last_url = url;
            last = Some(response);
        }
    }
    assert_eq!(found.len(), 10053);
    assert_eq!(found.iter().copied().collect::<BTreeSet<_>>().len(), 10053);
    assert_eq!(found.last(), Some(&510049));
    assert!(last.unwrap().text().contains("First page"));

    // Names resolve by address; alias-only token labels never create holdings.
    let keeper = get(&app, "/profile/emptyname?view=collected").await;
    assert_eq!(keeper.status, StatusCode::OK);
    assert_eq!(token_ids(&keeper), vec![133460]);
    assert!(pagination_link(&keeper, "next").is_none());
    let wrong_holder = last_url.replace(
        ingest_support::QUADRADO,
        "tz2RXosjAZdS85oCrguy7Eom757144dcU1FT",
    );
    assert_eq!(
        get(&app, &wrong_holder).await.status,
        StatusCode::BAD_REQUEST
    );

    let mut next = Some("/project/29437".to_owned());
    let mut count = 0;
    let mut seen = BTreeSet::new();
    while let Some(url) = next {
        let response = get(&app, &url).await;
        assert_eq!(response.status, StatusCode::OK);
        for id in token_ids(&response) {
            assert!(seen.insert(id));
            count += 1;
        }
        assert!(count <= 10059);
        next = pagination_link(&response, "next");
    }
    assert_eq!(count, 10059);
    assert!(seen.contains(&510049));
}

#[tokio::test]
async fn curated_artist_slug_uses_resolved_id_for_both_collections() {
    let archive = TestArchive::chain_imported();
    let app = archive.app();
    let store = Store::open_read_only(&archive.database).unwrap();
    let artist = store.artist("emptyname").unwrap();
    assert_ne!(artist.id, "emptyname");
    let expected = store.artist_projects_page(&artist.id, None, 24).unwrap();
    let response = get(&app, "/profile/emptyname?view=projects").await;
    assert_eq!(response.status, StatusCode::OK);
    assert_eq!(
        project_ids(&response),
        expected.items.iter().map(|p| p.id).collect::<Vec<_>>()
    );
    let holdings = store
        .collector_holdings_page(artist.address.as_deref().unwrap(), None, 24)
        .unwrap();
    let response = get(&app, "/profile/emptyname?view=collected").await;
    assert_eq!(response.status, StatusCode::OK);
    assert_eq!(
        token_ids(&response),
        holdings
            .items
            .iter()
            .map(|t| t.token_id)
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn escrow_and_burn_profiles_are_labelled_without_collector_credit() {
    let archive = TestArchive::chain_imported();
    let app = archive.app();
    for address in seeded_archive::ingest::escrow::ESCROWS
        .iter()
        .map(|escrow| escrow.address)
        .chain(std::iter::once(
            seeded_archive::ingest::escrow::BURN_ADDRESS,
        ))
    {
        let response = get(&app, &format!("/profile/{address}?view=collected")).await;
        assert_eq!(response.status, StatusCode::OK, "{address}");
        assert!(
            response.text().contains("Marketplace contract")
                || response.text().contains("Burn address")
        );
        assert!(token_ids(&response).is_empty());
    }
}

#[tokio::test]
async fn volume_cards_show_grouped_whole_tez() {
    let archive = TestArchive::imported();
    add_cursor_catalogue(&archive);
    add_cursor_volumes(&archive);
    let db = rusqlite::Connection::open(&archive.database).unwrap();
    db.execute(
        "UPDATE project_volume SET volume_mutez=1607642019887, primary_mutez=1607642019887, secondary_mutez=0 WHERE project_id=900001",
        [],
    )
    .unwrap();
    let response = get(&archive.app(), "/projects?sort=volume").await;
    assert_eq!(response.status, StatusCode::OK);
    assert!(response.text().contains(">1,607,642 tez</dd>"));
    assert!(!response.text().contains("tez/wXTZ"));
}

#[tokio::test]
async fn menu_lists_projects_articles_artists_and_long_texts_get_read_more() {
    let archive = TestArchive::imported();
    let projects = get(&archive.app(), "/projects").await;
    let nav = projects
        .text()
        .split("aria-label=\"Primary\"")
        .nth(1)
        .unwrap();
    let position = |label: &str| nav.find(&format!(">{label}</a>")).unwrap();
    assert!(position("Projects") < position("Articles"));
    assert!(position("Articles") < position("Artists"));
    let project = get(&archive.app(), "/project/29437").await;
    assert!(project.text().contains("data-read-more"));
    assert!(project.text().contains("/static/read_more.js"));
}

#[tokio::test]
async fn projects_page_has_no_keeper_banner() {
    let archive = TestArchive::imported();
    for path in ["/projects", "/projects?sort=newest"] {
        let html = get(&archive.app(), path).await;
        assert!(!html.text().contains("keepers-shelf"), "{path}");
        assert!(!html.text().contains("Keeper’s shelf"), "{path}");
    }
}
