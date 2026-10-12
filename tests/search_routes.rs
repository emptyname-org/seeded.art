use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{HeaderMap, Request, StatusCode};
use http_body_util::{BodyExt, Limited};
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use rusqlite::{Connection, params};
use seeded_archive::import::import_fixtures;
use seeded_archive::store::Store;
use seeded_archive::web::{AppState, router};
use tower::ServiceExt;

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);
const CONTRACT: &str = "KT1EfsNuqwLAWDd3o4pvfUx1CAh5GMdTrRvr";

struct TestArchive {
    directory: PathBuf,
    database: PathBuf,
}

impl TestArchive {
    fn imported() -> Self {
        let parent = project_root().join("tmp");
        fs::create_dir_all(&parent).expect("create project test directory");
        loop {
            let number = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
            let directory = parent.join(format!("search_routes-{}-{number}", std::process::id()));
            match fs::create_dir(&directory) {
                Ok(()) => {
                    let database = directory.join("archive.sqlite");
                    let mut store = Store::open(&database).unwrap();
                    import_fixtures(&mut store, &project_root().join("assets/fixtures")).unwrap();
                    seeded_archive::ingest::identity::apply_to(&mut store).unwrap();
                    return Self {
                        directory,
                        database,
                    };
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => panic!("create isolated search test directory: {error}"),
            }
        }
    }

    fn app(&self) -> Router {
        router(AppState::new(&self.database, project_root().join("assets")))
    }
}

impl Drop for TestArchive {
    fn drop(&mut self) {
        if let Err(error) = fs::remove_dir_all(&self.directory) {
            if std::thread::panicking() {
                eprintln!("search HTTP test cleanup failed: {error}");
            } else {
                panic!("search HTTP test cleanup failed: {error}");
            }
        }
    }
}

fn project_root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
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
    let uri = uri.split('#').next().unwrap_or(uri);
    tokio::time::timeout(Duration::from_secs(10), async {
        let response = app
            .clone()
            .oneshot(Request::get(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        let (parts, body) = response.into_parts();
        let bytes = Limited::new(body, 4 * 1024 * 1024)
            .collect()
            .await
            .expect("bounded response")
            .to_bytes();
        Response {
            status: parts.status,
            headers: parts.headers,
            body: bytes.to_vec(),
        }
    })
    .await
    .expect("HTTP request completes")
}

fn attr_values<'a>(html: &'a str, name: &str) -> Vec<&'a str> {
    let marker = format!("{name}=\"");
    html.split(&marker)
        .skip(1)
        .map(|tail| tail.split('"').next().unwrap())
        .collect()
}

fn links(response: &Response, relation: &str) -> Vec<String> {
    response
        .text()
        .split("<a ")
        .skip(1)
        .filter_map(|tag| {
            let tag = tag.split('>').next()?;
            tag.contains(&format!("rel=\"{relation}\"")).then(|| {
                attr_values(tag, "href")[0]
                    .replace("&#38;", "&")
                    .replace("&amp;", "&")
            })
        })
        .collect()
}

fn section_content<'a>(response: &'a Response, section: &str) -> Option<&'a str> {
    let content = response.text().split(&format!("id=\"{section}\"")).nth(1)?;
    Some(&content[..content.find("</section>").unwrap_or(content.len())])
}

fn section_next(response: &Response, section: &str) -> Option<String> {
    let content = section_content(response, section)?;
    let partial = Response {
        status: response.status,
        headers: response.headers.clone(),
        body: content.as_bytes().to_vec(),
    };
    links(&partial, "next").into_iter().next()
}

fn section_link(response: &Response, section: &str, relation: &str) -> Option<String> {
    let content = section_content(response, section)?;
    let partial = Response {
        status: response.status,
        headers: response.headers.clone(),
        body: content.as_bytes().to_vec(),
    };
    links(&partial, relation).into_iter().next()
}

fn query_value<'a>(url: &'a str, name: &str) -> Option<&'a str> {
    url.split_once('?')?.1.split(['&', '#']).find_map(|part| {
        let (key, value) = part.split_once('=')?;
        (key == name).then_some(value)
    })
}

fn address(byte: u8) -> String {
    bs58::encode([&[6, 161, 159][..], &[byte; 20]].concat())
        .with_check()
        .into_string()
}

fn contract_address(byte: u8) -> String {
    bs58::encode([&[2, 90, 121][..], &[byte; 20]].concat())
        .with_check()
        .into_string()
}

fn add_search_rows(archive: &TestArchive) -> (Vec<u64>, Vec<String>, Vec<String>) {
    let connection = Connection::open(&archive.database).unwrap();
    let mut project_ids = Vec::new();
    let mut artist_addresses = Vec::new();
    let mut collector_addresses = Vec::new();
    for i in 1_u8..=60 {
        let artist_address = address(i);
        let collector_address = address(i + 80);
        let artist_id = format!("global-artist-{i:02}");
        let artist_name = format!("Globex Artist {i:02}");
        connection
            .execute(
                "INSERT INTO artists(id,name,name_key,address) VALUES (?1,?2,lower(?2),?3)",
                params![artist_id, artist_name, artist_address],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO users(address,name,name_key) VALUES (?1,?2,lower(?2))",
                params![artist_address, artist_name],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO holder_counts(holder_address,held,listed) VALUES (?1,?2,0)",
                params![artist_address, i32::from(i) + 100],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO users(address,name,name_key) VALUES (?1,?2,lower(?2))",
                params![collector_address, format!("Globex Collector {i:02}")],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO holder_counts(holder_address,held,listed) VALUES (?1,?2,0)",
                params![collector_address, i32::from(i)],
            )
            .unwrap();
        let project_id = 900_000_u64 + u64::from(i);
        let title = format!("Globex Project {i:02}");
        connection
            .execute(
                "INSERT INTO projects(id,title,title_key,artist_key,index_state,contract) \
                 VALUES (?1,?2,lower(?2),'globex','not_indexed',?3)",
                params![project_id as i64, title, CONTRACT],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO project_artists(project_id,position,artist_id) VALUES (?1,0,?2)",
                params![project_id as i64, artist_id],
            )
            .unwrap();
        project_ids.push(project_id);
        artist_addresses.push(artist_address);
        collector_addresses.push(collector_address);
    }
    drop(connection);
    Store::open(&archive.database)
        .unwrap()
        .rebuild_search_names()
        .unwrap();
    (project_ids, artist_addresses, collector_addresses)
}

#[tokio::test]
async fn blank_search_redirects_to_projects_before_searching() {
    let archive = TestArchive::imported();
    let response = get(&archive.app(), "/search?q=%20%20").await;
    assert_eq!(response.status, StatusCode::SEE_OTHER);
    assert_eq!(response.headers["location"], "/projects");
    assert_eq!(
        get(&archive.app(), "/search?q=&projects_after=bad")
            .await
            .status,
        StatusCode::BAD_REQUEST
    );
}

#[tokio::test]
async fn global_search_omits_sections_without_matches() {
    let archive = TestArchive::imported();
    add_search_rows(&archive);
    let response = get(&archive.app(), "/search?q=Globex%20Project%2001").await;
    assert_eq!(response.status, StatusCode::OK);
    let html = response.text();
    assert!(html.contains("id=\"search-projects\""));
    assert!(!html.contains("id=\"search-artists\""));
    assert!(!html.contains("id=\"search-collectors\""));
    assert!(!html.contains("id=\"search-articles\""));
    assert!(!html.contains("href=\"#search-artists\""));
    assert!(!html.contains("href=\"#search-collectors\""));
    assert!(!html.contains("href=\"#search-articles\""));
}

#[tokio::test]
async fn exact_valid_address_redirects_before_search_even_when_unknown() {
    let archive = TestArchive::imported();
    let app = archive.app();
    let unknown = address(230);
    let response = get(&app, &format!("/search?q=%20{unknown}%20")).await;
    assert_eq!(response.status, StatusCode::SEE_OTHER);
    assert_eq!(response.headers["location"], format!("/profile/{unknown}"));
    let contract = contract_address(231);
    let response = get(&app, &format!("/search?q={contract}")).await;
    assert_eq!(response.status, StatusCode::SEE_OTHER);
    assert_eq!(response.headers["location"], format!("/profile/{contract}"));
}

#[tokio::test]
async fn global_search_sections_paginate_independently_and_preserve_other_cursors() {
    let archive = TestArchive::imported();
    let (project_ids, artist_addresses, collector_addresses) = add_search_rows(&archive);
    let app = archive.app();
    let first = get(&app, "/search?q=Globex").await;
    assert_eq!(first.status, StatusCode::OK);
    for section in ["search-projects", "search-artists", "search-collectors"] {
        assert!(first.text().contains(&format!("id=\"{section}\"")));
        assert!(
            section_next(&first, section).is_some(),
            "{section} has its own next link"
        );
    }

    let project_next = section_next(&first, "search-projects").unwrap();
    let artist_next = section_next(&first, "search-artists").unwrap();
    let collector_next = section_next(&first, "search-collectors").unwrap();
    let project_cursor = project_next
        .split("projects_after=")
        .nth(1)
        .unwrap()
        .split(['&', '#'])
        .next()
        .unwrap();
    let artist_cursor = artist_next
        .split("artists_after=")
        .nth(1)
        .unwrap()
        .split(['&', '#'])
        .next()
        .unwrap();
    let collector_cursor = collector_next
        .split("collectors_after=")
        .nth(1)
        .unwrap()
        .split(['&', '#'])
        .next()
        .unwrap();
    let combined = format!(
        "/search?q=Globex&projects_after={project_cursor}&artists_after={artist_cursor}&collectors_after={collector_cursor}"
    );
    let project_page = get(&app, &combined).await;
    assert_eq!(project_page.status, StatusCode::OK);
    for (section, own, preserved) in [
        (
            "search-projects",
            "projects_after",
            [
                ("artists_after", artist_cursor),
                ("collectors_after", collector_cursor),
            ],
        ),
        (
            "search-artists",
            "artists_after",
            [
                ("projects_after", project_cursor),
                ("collectors_after", collector_cursor),
            ],
        ),
        (
            "search-collectors",
            "collectors_after",
            [
                ("projects_after", project_cursor),
                ("artists_after", artist_cursor),
            ],
        ),
    ] {
        let first = section_link(&project_page, section, "first").expect("first page link");
        assert!(
            query_value(&first, own).is_none(),
            "{section} resets its own cursor"
        );
        for (name, value) in preserved {
            assert_eq!(
                query_value(&first, name),
                Some(value),
                "{section} preserves {name}"
            );
        }
    }
    for (section, own, preserved) in [
        (
            "search-projects",
            "projects_after",
            [
                ("artists_after", artist_cursor),
                ("collectors_after", collector_cursor),
            ],
        ),
        (
            "search-artists",
            "artists_after",
            [
                ("projects_after", project_cursor),
                ("collectors_after", collector_cursor),
            ],
        ),
        (
            "search-collectors",
            "collectors_after",
            [
                ("projects_after", project_cursor),
                ("artists_after", artist_cursor),
            ],
        ),
    ] {
        let next = section_link(&project_page, section, "next").expect("next page link");
        assert_ne!(
            query_value(&next, own),
            query_value(&combined, own),
            "{section} advances its own cursor"
        );
        for (name, value) in preserved {
            assert_eq!(
                query_value(&next, name),
                Some(value),
                "{section} preserves {name}"
            );
        }
    }
    let swapped = format!("/search?q=Globex&projects_after={artist_cursor}");
    assert_eq!(get(&app, &swapped).await.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        get(&app, "/search?q=Globex&projects_after=bad")
            .await
            .status,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        get(&app, &combined.replace("q=Globex", "q=Changed"))
            .await
            .status,
        StatusCode::BAD_REQUEST
    );

    let mut project_results = Vec::new();
    let mut artist_results = Vec::new();
    let mut collector_results = Vec::new();
    for (section, initial, destination) in [
        ("search-projects", None, 0),
        ("search-artists", None, 1),
        ("search-collectors", None, 2),
    ] {
        let mut url = initial.unwrap_or_else(|| "/search?q=Globex".to_owned());
        loop {
            let response = get(&app, &url).await;
            assert_eq!(response.status, StatusCode::OK, "{url}");
            let section_html = section_content(&response, section).unwrap();
            match destination {
                0 => project_results.extend(
                    attr_values(section_html, "data-project-id")
                        .into_iter()
                        .filter_map(|value| value.parse::<u64>().ok())
                        .filter(|id| project_ids.contains(id)),
                ),
                1 | 2 => {
                    for href in attr_values(section_html, "href") {
                        if let Some(address) = href
                            .strip_prefix("/profile/")
                            .map(|rest| rest.trim_end_matches("?view=collected"))
                        {
                            if destination == 1
                                && artist_addresses.iter().any(|known| known == address)
                            {
                                artist_results.push(address.to_owned());
                            }
                            if destination == 2
                                && (artist_addresses.iter().any(|known| known == address)
                                    || collector_addresses.iter().any(|known| known == address))
                            {
                                collector_results.push(address.to_owned());
                            }
                        }
                    }
                    if destination == 2 {
                        for (i, address) in artist_addresses.iter().enumerate() {
                            if section_html.contains(address) {
                                assert!(section_html.contains(&format!("{} held", i + 101)));
                            }
                        }
                        for (i, address) in collector_addresses.iter().enumerate() {
                            if section_html.contains(address) {
                                assert!(section_html.contains(&format!("{} held", i + 1)));
                            }
                        }
                    }
                }
                _ => unreachable!(),
            }
            match section_next(&response, section) {
                Some(next) => url = next,
                None => break,
            }
        }
    }
    project_results.sort_unstable();
    artist_results.sort();
    collector_results.sort();
    // Artists are listed once, under Artists; Collectors holds everyone else.
    let mut expected_artists = artist_addresses;
    let mut expected_collectors = collector_addresses;
    expected_artists.sort();
    expected_collectors.sort();
    assert_eq!(project_results, project_ids);
    assert_eq!(artist_results, expected_artists);
    assert_eq!(collector_results, expected_collectors);
}

#[tokio::test]
async fn hostile_and_invalid_global_queries_are_safe_and_bounded() {
    let archive = TestArchive::imported();
    let app = archive.app();
    let hostile = "<script>alert(1)</script> OR title";
    let encoded = utf8_percent_encode(hostile, NON_ALPHANUMERIC);
    let response = get(&app, &format!("/search?q={encoded}")).await;
    assert_eq!(response.status, StatusCode::OK);
    assert!(!response.text().contains("<script>alert(1)</script>"));
    for query in ["x".repeat(201), "a b c d e f g h i".to_owned()] {
        let encoded = utf8_percent_encode(&query, NON_ALPHANUMERIC);
        assert_eq!(
            get(&app, &format!("/search?q={encoded}")).await.status,
            StatusCode::BAD_REQUEST
        );
    }
}

#[tokio::test]
async fn global_search_hides_a_name_shared_by_multiple_addresses() {
    let archive = TestArchive::imported();
    add_search_rows(&archive);
    let first = address(240);
    let second = address(241);
    let connection = Connection::open(&archive.database).unwrap();
    for address in [&first, &second] {
        connection
            .execute(
                "INSERT INTO users(address,name,name_key) VALUES (?1,'Shared Collector','shared collector')",
                [address],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO holder_counts(holder_address,held,listed) VALUES (?1,1,0)",
                [address],
            )
            .unwrap();
    }
    drop(connection);
    Store::open(&archive.database)
        .unwrap()
        .rebuild_search_names()
        .unwrap();
    let encoded = utf8_percent_encode("Shared Collector", NON_ALPHANUMERIC);
    let response = get(&archive.app(), &format!("/search?q={encoded}")).await;
    assert_eq!(response.status, StatusCode::OK);
    assert!(!response.text().contains(&first));
    assert!(!response.text().contains(&second));
}

#[tokio::test]
async fn search_projects_sort_is_selectable_and_preserved() {
    let archive = TestArchive::imported();
    add_search_rows(&archive);
    let app = archive.app();
    let first_project = |response: &Response| -> u64 {
        attr_values(
            section_content(response, "search-projects").unwrap(),
            "data-project-id",
        )[0]
        .parse()
        .unwrap()
    };
    let default = get(&app, "/search?q=Globex").await;
    assert_eq!(default.status, StatusCode::OK);
    assert!(
        default
            .text()
            .contains("<option value=\"volume\" selected>Highest volume</option>")
    );
    assert!(
        !default
            .text()
            .contains("<input type=\"hidden\" name=\"sort\"")
    );
    let newest = get(&app, "/search?q=Globex&sort=newest").await;
    assert_eq!(newest.status, StatusCode::OK);

    let oldest = get(&app, "/search?q=Globex&sort=oldest").await;
    assert_eq!(oldest.status, StatusCode::OK);
    assert!(
        oldest
            .text()
            .contains("<option value=\"oldest\" selected>Oldest</option>")
    );
    assert!(first_project(&oldest) < first_project(&newest));
    // The header search is the same form on every page; it carries no page sort.
    let header = oldest
        .text()
        .split("id=\"global-search-form\"")
        .nth(1)
        .unwrap();
    assert!(
        !header
            .split("</form>")
            .next()
            .unwrap()
            .contains("type=\"hidden\"")
    );
    for section in ["search-projects", "search-artists", "search-collectors"] {
        let next = section_next(&oldest, section).unwrap();
        assert_eq!(query_value(&next, "sort"), Some("oldest"), "{section}");
    }
    let page_two = get(&app, &section_next(&oldest, "search-projects").unwrap()).await;
    assert_eq!(page_two.status, StatusCode::OK);
    assert!(
        page_two
            .text()
            .contains("<option value=\"oldest\" selected>Oldest</option>")
    );

    let volume = get(&app, "/search?q=Globex&sort=volume").await;
    assert_eq!(volume.status, StatusCode::OK);
    assert!(volume.text().contains("<dt>Volume</dt>"));
    assert!(!volume.text().contains("Total volume (primary + secondary)"));
    assert_eq!(
        get(&app, "/search?q=Globex&sort=title").await.status,
        StatusCode::BAD_REQUEST
    );
}

#[tokio::test]
async fn collector_results_open_the_collected_view() {
    let archive = TestArchive::imported();
    add_search_rows(&archive);
    let response = get(&archive.app(), "/search?q=Globex").await;
    let collectors = section_content(&response, "search-collectors").unwrap();
    let links = attr_values(collectors, "href");
    assert!(!links.is_empty());
    for link in links.iter().filter(|link| link.starts_with("/profile/")) {
        assert!(link.ends_with("?view=collected"), "{link}");
    }
}

#[tokio::test]
async fn found_types_sit_under_the_header_search_in_section_order() {
    let archive = TestArchive::imported();
    add_search_rows(&archive);
    let app = archive.app();
    let response = get(&app, "/search?q=Globex").await;
    assert_eq!(response.status, StatusCode::OK);
    let main = response
        .text()
        .split_once("<main id=\"main-content\" tabindex=\"-1\">")
        .unwrap()
        .1;
    let (before, rest) = main
        .split_once("<nav class=\"search-result-nav\"")
        .expect("type summary");
    let leftover = before.replace("<h1 class=\"sr-only\">Search</h1>", "");
    assert!(leftover.trim().is_empty(), "{leftover}");
    let nav = rest.split_once("</nav>").unwrap().0;
    let sections = ["search-projects", "search-artists", "search-collectors"];
    assert_eq!(
        attr_values(nav, "href"),
        sections.map(|section| format!("#{section}"))
    );
    let positions: Vec<_> = sections
        .iter()
        .map(|section| rest.find(&format!("<section id=\"{section}\"")).unwrap())
        .collect();
    assert!(positions.windows(2).all(|pair| pair[0] < pair[1]));

    let empty = get(&app, "/search?q=zzzzqqq").await;
    assert_eq!(empty.status, StatusCode::OK);
    assert_eq!(empty.text().matches("class=\"empty-state\"").count(), 1);
    assert!(!empty.text().contains("search-result-nav"));
}

#[tokio::test]
async fn people_scope_searches_artists_and_collectors_in_the_same_template() {
    let archive = TestArchive::imported();
    add_search_rows(&archive);
    let app = archive.app();
    let all = get(&app, "/search?q=Globex").await;
    let people = get(&app, "/search?q=Globex&scope=people").await;
    assert_eq!(people.status, StatusCode::OK);
    let html = people.text();
    for section in ["search-artists", "search-collectors"] {
        let profiles = |response: &Response| -> Vec<String> {
            attr_values(section_content(response, section).unwrap(), "href")
                .into_iter()
                .filter(|link| link.starts_with("/profile/"))
                .map(str::to_owned)
                .collect()
        };
        assert_eq!(profiles(&people), profiles(&all), "{section}");
        assert!(html.contains(&format!("href=\"#{section}\"")));
        let next = section_next(&people, section).unwrap();
        assert_eq!(query_value(&next, "scope"), Some("people"), "{section}");
        let page_two = get(&app, &next).await;
        assert_eq!(page_two.status, StatusCode::OK, "{next}");
        assert!(section_content(&page_two, section).is_some(), "{next}");
        assert!(page_two.text().contains("name=\"scope\" value=\"people\""));
    }
    for other in ["search-projects", "search-articles"] {
        assert!(!html.contains(&format!("id=\"{other}\"")), "{other}");
        assert!(!html.contains(&format!("href=\"#{other}\"")), "{other}");
    }
    assert!(!html.contains("id=\"search-sort\""));
    assert!(!html.contains("Article search is temporarily unavailable"));
    assert!(html.contains("<input type=\"hidden\" name=\"scope\" value=\"people\">"));
    assert!(html.contains("href=\"/artists\" aria-current=\"page\" class=\"active\">Artists</a>"));

    let project_cursor = query_value(
        &section_next(&all, "search-projects").unwrap(),
        "projects_after",
    )
    .unwrap()
    .to_owned();
    let artist_cursor = query_value(
        &section_next(&all, "search-artists").unwrap(),
        "artists_after",
    )
    .unwrap()
    .to_owned();
    for path in [
        format!("/search?q=Globex&scope=people&projects_after={project_cursor}"),
        "/search?q=Globex&scope=people&articles_after=x".into(),
        format!("/search?q=Globex&scope=articles&artists_after={artist_cursor}"),
        "/search?q=Globex&scope=bogus".into(),
        "/search?q=Globex&scope=PEOPLE".into(),
    ] {
        assert_eq!(
            get(&app, &path).await.status,
            StatusCode::BAD_REQUEST,
            "{path}"
        );
    }
    for (path, location) in [
        (
            "/search?q=%20&scope=people".to_owned(),
            "/artists".to_owned(),
        ),
        ("/search?q=&scope=articles".into(), "/articles".into()),
        (
            format!("/search?q={}&scope=people", address(230)),
            format!("/profile/{}", address(230)),
        ),
    ] {
        let response = get(&app, &path).await;
        assert_eq!(response.status, StatusCode::SEE_OTHER, "{path}");
        assert_eq!(response.headers["location"], location.as_str(), "{path}");
    }
}
