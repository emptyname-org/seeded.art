use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use seeded_archive::import::import_fixtures;
use seeded_archive::store::Store;
use seeded_archive::web::{AppState, router};
use tower::ServiceExt;

const CONTRACT: &str = "KT1EfsNuqwLAWDd3o4pvfUx1CAh5GMdTrRvr";
const SEED: &str = "ooHBH4Q9m5BTSExqbvNCtpWDbdEqZqTXK4Ewh1HMU66V5PhMUE7";
static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

struct Archive(PathBuf);

impl Archive {
    fn imported() -> Self {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let parent = root.join("tmp");
        fs::create_dir_all(&parent).expect("create project test directory");
        let number = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let directory = parent.join(format!("taste_seed_views-{}-{number}", std::process::id()));
        fs::create_dir(&directory).expect("create isolated HTTP test directory");
        let database = directory.join("archive.sqlite");
        let mut store = Store::open(&database).expect("open test store");
        import_fixtures(&mut store, &root.join("assets/fixtures"))
            .expect("import verified fixtures");
        seeded_archive::ingest::identity::apply_to(&mut store).expect("apply identities");
        Self(directory)
    }

    fn app(&self) -> Router {
        let database = self.0.join("archive.sqlite");
        let assets = Path::new(env!("CARGO_MANIFEST_DIR")).join("assets");
        let config = seeded_archive::config::Config::from_values(&Default::default()).unwrap();
        router(AppState::new(database, assets).configured(&config))
    }
}

impl Drop for Archive {
    fn drop(&mut self) {
        if let Err(error) = fs::remove_dir_all(&self.0) {
            if std::thread::panicking() {
                eprintln!("HTTP test cleanup failed: {error}");
            } else {
                panic!("remove isolated HTTP test directory: {error}");
            }
        }
    }
}

async fn get(app: &Router, path: &str) -> (StatusCode, String) {
    let response = app
        .clone()
        .oneshot(Request::get(path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let body = http_body_util::BodyExt::collect(response.into_body())
        .await
        .unwrap()
        .to_bytes();
    (status, String::from_utf8(body.to_vec()).unwrap())
}

#[tokio::test]
async fn indexed_seed_is_shown_on_edition_card_and_token_page() {
    let archive = Archive::imported();
    let app = archive.app();

    let (status, project) = get(&app, "/project/29437").await;
    assert_eq!(status, StatusCode::OK);
    let card = project
        .split("<article class=\"artwork-card\" data-token-id=\"133459\"")
        .nth(1)
        .expect("fixture token has an edition card")
        .split("</article>")
        .next()
        .expect("edition card closes");
    assert!(card.contains("class=\"artwork-seed\""));
    assert!(card.contains(SEED), "edition card shows the indexed seed");

    let (status, token) = get(&app, &format!("/token/{CONTRACT}/133459")).await;
    assert_eq!(status, StatusCode::OK);
    assert!(token.contains("class=\"nft-seed\""));
    assert!(token.contains(SEED), "token page shows the indexed seed");
}

#[tokio::test]
async fn full_window_route_requires_the_exact_seed_and_keeps_the_player_boundary() {
    let archive = Archive::imported();
    let app = archive.app();
    let url = format!("/token/{CONTRACT}/133459/full?seed={SEED}");
    let (status, html) = get(&app, &url).await;
    assert_eq!(status, StatusCode::OK);
    assert!(html.contains("class=\"full-artwork\""));
    assert!(html.contains("data-autoplay=\"true\""));
    assert!(html.contains(&format!(
        "http://127.0.0.1:3919/render/{CONTRACT}/133459?fxhash={SEED}"
    )));
    assert!(html.contains("src=\"/static/player.js\""));
    assert!(!html.contains("<header"));
    assert!(!html.contains("<footer"));
    assert!(
        !html.contains("__seeded_instance="),
        "only the parent player creates the instance"
    );
    let (_, token) = get(&app, &format!("/token/{CONTRACT}/133459")).await;
    assert!(token.contains(&format!(
        "href=\"{url}\" target=\"_blank\" rel=\"nofollow noopener noreferrer\""
    )));
    let poster = token
        .split("class=\"player-poster\"><img ")
        .nth(1)
        .and_then(|tail| tail.split('>').next())
        .expect("edition poster image");
    assert!(poster.contains("fetchpriority=\"high\""));
    assert!(!poster.contains("loading=\"lazy\""));
    for suffix in ["", "?seed=wrong", "?seed=", "?seed=one&seed=two"] {
        let (status, _) = get(&app, &format!("/token/{CONTRACT}/133459/full{suffix}")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }
    let (status, _) = get(&app, &format!("/token/{CONTRACT}/999999/full?seed={SEED}")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let response = app
        .clone()
        .oneshot(
            Request::get(&url)
                .header("Accept", "application/json")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        response.headers()["content-security-policy"]
            .to_str()
            .unwrap()
            .contains("frame-src http://127.0.0.1:3919")
    );
    let body = http_body_util::BodyExt::collect(response.into_body())
        .await
        .unwrap()
        .to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["data"]["seed"], SEED);
    assert_eq!(json["data"]["token_id"], 133459);
}

#[tokio::test]
async fn missing_seed_is_labelled_and_cannot_launch_full_window() {
    let archive = Archive::imported();
    let conn = rusqlite::Connection::open(archive.0.join("archive.sqlite")).unwrap();
    conn.execute(
        "UPDATE tokens SET seed='', artifact_uri='onchfs://unresolved' WHERE token_id=133459",
        [],
    )
    .unwrap();
    let app = archive.app();
    let (_, html) = get(&app, &format!("/token/{CONTRACT}/133459")).await;
    assert!(html.contains("<code>Not yet indexed</code>"));
    assert!(!html.contains("Full screen"));
    assert_eq!(
        get(&app, &format!("/token/{CONTRACT}/133459/full?seed="))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
}

async fn response(app: &Router, path: &str) -> axum::response::Response {
    app.clone()
        .oneshot(Request::get(path).body(Body::empty()).unwrap())
        .await
        .unwrap()
}

#[tokio::test]
async fn theme_script_loads_before_the_stylesheet_and_header_has_icon_toggle() {
    let archive = Archive::imported();
    let app = archive.app();
    let script = "<script src=\"/static/theme.js\"></script>";
    let stylesheet = "<link rel=\"stylesheet\" href=\"/static/site.css\">";
    let toggle = "<button class=\"theme-toggle\" type=\"button\" aria-pressed=\"false\" aria-label=\"Switch to dark theme\" title=\"Switch to dark theme\"><svg class=\"icon-moon\"";
    for (path, expected) in [
        ("/projects".to_string(), StatusCode::OK),
        ("/about".to_string(), StatusCode::OK),
        ("/project/999999999".to_string(), StatusCode::NOT_FOUND),
        (
            format!("/token/{CONTRACT}/133459/full?seed={SEED}"),
            StatusCode::OK,
        ),
    ] {
        let (status, html) = get(&app, &path).await;
        assert_eq!(status, expected, "{path}");
        let head = html.split("</head>").next().unwrap();
        let script_at = head
            .find(script)
            .unwrap_or_else(|| panic!("{path}: theme.js in head"));
        let stylesheet_at = head.find(stylesheet).expect("stylesheet in head");
        assert!(
            script_at < stylesheet_at,
            "{path}: theme.js precedes the stylesheet"
        );
        if path.ends_with(SEED) {
            assert!(
                !html.contains("theme-toggle"),
                "full window has no header toggle"
            );
            continue;
        }
        let header = html
            .split("<header class=\"site-header\">")
            .nth(1)
            .and_then(|rest| rest.split("</header>").next())
            .expect("site header");
        let account = header
            .split("<div class=\"account-access\">")
            .nth(1)
            .expect("account controls");
        let toggle_at = account
            .find(toggle)
            .unwrap_or_else(|| panic!("{path}: toggle sits with account controls"));
        let connect_at = account.find("data-auth-link").expect("connect link");
        let menu_at = account.find("data-auth-menu").expect("account menu");
        assert!(
            toggle_at < connect_at && toggle_at < menu_at,
            "{path}: toggle precedes the account control"
        );
        assert!(!account.contains(">Invert<"), "{path}: no visible text");
    }
}

#[tokio::test]
async fn full_window_page_is_noindex_and_disallowed_for_crawlers() {
    let archive = Archive::imported();
    let app = archive.app();
    let url = format!("/token/{CONTRACT}/133459/full?seed={SEED}");
    let full = response(&app, &url).await;
    assert_eq!(full.status(), StatusCode::OK);
    assert_eq!(full.headers()["x-robots-tag"], "noindex");
    let body = http_body_util::BodyExt::collect(full.into_body())
        .await
        .unwrap()
        .to_bytes();
    assert!(String::from_utf8_lossy(&body).contains("<h1 class=\"sr-only\">"));
    let token = response(&app, &format!("/token/{CONTRACT}/133459")).await;
    assert_eq!(token.status(), StatusCode::OK);
    assert_eq!(token.headers()["x-robots-tag"], "max-image-preview:large");
    let (status, robots) = get(&app, "/robots.txt").await;
    assert_eq!(status, StatusCode::OK);
    assert!(robots.lines().any(|line| line == "Disallow: /token/*/full"));
}

#[tokio::test]
async fn full_window_ignores_tracking_params_but_rejects_a_repeated_seed() {
    let archive = Archive::imported();
    let app = archive.app();
    let base = format!("/token/{CONTRACT}/133459/full");
    for query in [
        format!("?seed={SEED}&utm_source=newsletter&utm_medium=email"),
        format!("?utm_campaign=launch&seed={SEED}"),
    ] {
        let (status, html) = get(&app, &format!("{base}{query}")).await;
        assert_eq!(status, StatusCode::OK, "{query}");
        assert!(html.contains("class=\"full-artwork\""));
    }
    for query in [
        format!("?seed={SEED}&seed={SEED}"),
        format!("?seed={SEED}&utm_source=x&seed=wrong"),
    ] {
        let (status, _) = get(&app, &format!("{base}{query}")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{query}");
    }
}
