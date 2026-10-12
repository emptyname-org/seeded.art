use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

use axum::{
    Router,
    body::Body,
    http::{HeaderMap, Request, StatusCode, header},
};
use http_body_util::BodyExt;
use seeded_archive::{
    import::import_fixtures,
    store::Store,
    web::{AppState, router},
};
use serde_json::Value;
use tower::ServiceExt;

const PLAIN: &str = "tz1gBXG9fg8RMDH69KfKqwoTH5sFDmzt5yzm";
const NUMBERED: &str = "tz2RXosjAZdS85oCrguy7Eom757144dcU1FT";
static NEXT: AtomicU64 = AtomicU64::new(0);

struct Archive(PathBuf);

impl Archive {
    fn new() -> Self {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let directory = root.join(format!(
            "tmp/numbered-name-routes-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&directory).unwrap();
        let mut store = Store::open(&directory.join("archive.sqlite")).unwrap();
        import_fixtures(&mut store, &root.join("assets/fixtures")).unwrap();
        let sql = rusqlite::Connection::open(directory.join("archive.sqlite")).unwrap();
        assert_eq!(
            sql.execute("UPDATE artists SET address=?1 WHERE id='zancan'", [PLAIN])
                .unwrap(),
            1
        );
        for address in [PLAIN, NUMBERED] {
            sql.execute(
                "INSERT INTO users(address,name,name_key) VALUES (?1,'zancan','zancan') \
                 ON CONFLICT(address) DO UPDATE SET name=excluded.name,name_key=excluded.name_key",
                [address],
            )
            .unwrap();
        }
        drop(sql);
        store.assign_name_numbers().unwrap();
        Self(directory)
    }

    fn app(&self) -> Router {
        router(AppState::new(self.0.join("archive.sqlite"), "assets"))
    }
}

impl Drop for Archive {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

async fn get(app: &Router, path: &str, accept: &str) -> (StatusCode, HeaderMap, String) {
    let response = app
        .clone()
        .oneshot(
            Request::get(path)
                .header(header::ACCEPT, accept)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let (parts, body) = response.into_parts();
    (
        parts.status,
        parts.headers,
        String::from_utf8(body.collect().await.unwrap().to_bytes().to_vec()).unwrap(),
    )
}

#[tokio::test]
async fn plain_and_numbered_profile_names_resolve_in_html_and_json_without_alias_collisions() {
    let archive = Archive::new();
    let app = archive.app();
    for (alias, name, address) in [
        ("zancan", "zancan", PLAIN),
        ("ZANCAN", "zancan", PLAIN),
        ("zancan-2", "zancan-2", NUMBERED),
        ("ZaNcAn-2", "zancan-2", NUMBERED),
    ] {
        let path = format!("/profile/{alias}");
        let canonical = format!("http://127.0.0.1:3918/profile/{address}");
        let (status, _, html) = get(&app, &path, "text/html").await;
        assert_eq!(status, StatusCode::OK, "{path}");
        assert!(html.contains(&format!("<h1>{name}</h1>")), "{path}");
        assert!(html.contains(&format!("rel=\"canonical\" href=\"{canonical}\"")));
        let (status, headers, body) = get(&app, &path, "application/json").await;
        assert_eq!(status, StatusCode::OK);
        assert!(
            headers[header::CONTENT_TYPE]
                .to_str()
                .unwrap()
                .starts_with("application/json")
        );
        let json: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(json["canonical"], canonical);
        assert_eq!(json["data"]["profile"]["address"], address);
        assert_eq!(json["data"]["profile"]["name"], name);
    }
    for accept in ["text/html", "application/json"] {
        assert_eq!(
            get(&app, "/profile/zancan-3", accept).await.0,
            StatusCode::NOT_FOUND
        );
    }
}

#[tokio::test]
async fn shared_name_search_lists_the_plain_artist_before_the_numbered_collector() {
    let archive = Archive::new();
    let app = archive.app();
    for query in ["zancan", "ZANCAN"] {
        let path = format!("/search?q={query}");
        let (status, _, html) = get(&app, &path, "text/html").await;
        assert_eq!(status, StatusCode::OK);
        let plain = html.find(">zancan</span>").unwrap();
        let numbered = html.find(">zancan-2</span>").unwrap();
        assert!(plain < numbered);
        assert!(html.contains(&format!("href=\"/profile/{NUMBERED}?view=collected\"")));

        let (status, _, body) = get(&app, &path, "application/json").await;
        assert_eq!(status, StatusCode::OK);
        let json: Value = serde_json::from_str(&body).unwrap();
        let data = &json["data"];
        assert_eq!(data["artists"]["items"][0]["name"], "zancan");
        assert_eq!(data["collectors"]["total"], 1);
        assert_eq!(data["collectors"]["items"][0]["name"], "zancan-2");
        assert_eq!(data["collectors"]["items"][0]["address"], NUMBERED);
    }
}
