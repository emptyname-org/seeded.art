use axum::{
    Router,
    body::Body,
    http::{HeaderMap, Request, StatusCode, header},
};
use http_body_util::{BodyExt, Limited};
use seeded_archive::{
    import::import_fixtures,
    store::Store,
    web::{AppState, router},
};
use serde_json::Value;
use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
use tower::ServiceExt;

static NEXT: AtomicU64 = AtomicU64::new(0);
const CONTRACT: &str = "KT1EfsNuqwLAWDd3o4pvfUx1CAh5GMdTrRvr";
struct Fixture {
    dir: PathBuf,
}
impl Fixture {
    fn new() -> Self {
        let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(format!(
            "tmp/agent-routes-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let mut store = Store::open(&dir.join("db.sqlite")).unwrap();
        import_fixtures(
            &mut store,
            &PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("assets/fixtures"),
        )
        .unwrap();
        seeded_archive::ingest::identity::apply_to(&mut store).unwrap();
        Self { dir }
    }
    fn app(&self) -> Router {
        router(AppState::new(self.dir.join("db.sqlite"), "assets"))
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.dir).unwrap();
    }
}
async fn get(app: &Router, path: &str, accept: &str) -> (StatusCode, HeaderMap, String) {
    let response = app
        .clone()
        .oneshot(
            Request::get(path)
                .header(header::ACCEPT, accept)
                .header(header::HOST, "untrusted.invalid")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let (parts, body) = response.into_parts();
    let bytes = Limited::new(body, 8 * 1024 * 1024)
        .collect()
        .await
        .unwrap()
        .to_bytes();
    (
        parts.status,
        parts.headers,
        String::from_utf8(bytes.to_vec()).unwrap(),
    )
}
#[tokio::test]
async fn negotiated_pages_keep_ids_cursors_and_filters_without_html_fields() {
    let fixture = Fixture::new();
    let app = fixture.app();
    for (path, kind) in [
        ("/projects", "projects"),
        ("/project/29437", "project"),
        ("/artists", "artists"),
        ("/profile/emptyname", "profile"),
        ("/collectors", "collectors"),
        ("/search?q=Arboreal", "search"),
        ("/about", "about"),
    ] {
        let (status, headers, body) = get(&app, path, "application/json").await;
        assert_eq!(status, StatusCode::OK, "{path} {body}");
        assert!(
            headers[header::CONTENT_TYPE]
                .to_str()
                .unwrap()
                .starts_with("application/json")
        );
        assert!(headers.get_all(header::VARY).iter().any(|v| v == "Accept"));
        let value: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(value["version"], "1");
        assert_eq!(value["kind"], kind);
        assert!(
            value["canonical"]
                .as_str()
                .unwrap()
                .starts_with("http://127.0.0.1:3918/")
        );
        assert!(!body.contains("<!doctype"));
        if kind == "project" {
            assert_eq!(value["data"]["project"]["id"], 29437);
            assert_eq!(value["data"]["history"]["available"], false);
        }
    }
    let (_, _, first) = get(&app, "/projects?sort=oldest", "application/json").await;
    let first: Value = serde_json::from_str(&first).unwrap();
    let next = first["data"]["pagination"]["next"].as_str().unwrap();
    assert!(next.contains("sort=oldest"));
    let (_, _, html) = get(&app, "/projects?sort=oldest", "text/html").await;
    assert!(
        html.contains(&next.replace('&', "&#38;")) || html.contains(&next.replace('&', "&amp;"))
    );
    let (status, _, last) = get(&app, next, "application/json").await;
    assert_eq!(status, StatusCode::OK);
    let last: Value = serde_json::from_str(&last).unwrap();
    assert!(last["data"]["pagination"]["next"].is_null());
    let mut ids = first["data"]["projects"]["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["id"].as_u64().unwrap())
        .collect::<Vec<_>>();
    ids.extend(
        last["data"]["projects"]["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| p["id"].as_u64().unwrap()),
    );
    assert_eq!(ids.len(), 25);
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(ids.len(), 25);
}
#[tokio::test]
async fn error_negotiation_and_exact_address_redirect_keep_statuses() {
    let fixture = Fixture::new();
    let app = fixture.app();
    for (path, status) in [
        ("/missing", 404),
        ("/project/999999", 404),
        ("/projects?after=invalid", 400),
        ("/project/29437?articles_after=invalid", 400),
    ] {
        let (code, headers, body) = get(&app, path, "application/json").await;
        assert_eq!(code.as_u16(), status);
        assert!(
            headers[header::CONTENT_TYPE]
                .to_str()
                .unwrap()
                .starts_with("application/json")
        );
        let value: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(value["error"]["status"], status);
    }
    let (status, headers, _) = get(
        &app,
        "/search?q=tz1gL9H53BzjDqmMi2JkKALeCzT7UAoopzEL",
        "application/json",
    )
    .await;
    assert_eq!(status, StatusCode::SEE_OTHER);
    assert_eq!(
        headers[header::LOCATION],
        "/profile/tz1gL9H53BzjDqmMi2JkKALeCzT7UAoopzEL"
    );
    let (_, headers, _) = get(&app, "/projects", "application/json;q=0,text/html").await;
    assert!(
        headers[header::CONTENT_TYPE]
            .to_str()
            .unwrap()
            .starts_with("text/html")
    );
    std::fs::rename(
        fixture.dir.join("db.sqlite"),
        fixture.dir.join("missing.sqlite"),
    )
    .unwrap();
    let (status, headers, body) = get(&app, "/projects", "application/json").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(headers[header::RETRY_AFTER], "2");
    assert_eq!(
        serde_json::from_str::<Value>(&body).unwrap()["error"]["code"],
        "unavailable"
    );
}
/// The JSON-LD node of `kind`, whether alone or in an `@graph`.
fn ld_node(value: &Value, kind: &str) -> Value {
    value["@graph"]
        .as_array()
        .and_then(|graph| graph.iter().find(|node| node["@type"] == kind))
        .cloned()
        .unwrap_or_else(|| value.clone())
}

#[tokio::test]
async fn canonical_and_json_ld_are_safe_and_do_not_trust_host() {
    let fixture = Fixture::new();
    let db = rusqlite::Connection::open(fixture.dir.join("db.sqlite")).unwrap();
    db.execute(
        "UPDATE projects SET description=? WHERE id=29437",
        ["</script><script>alert(1)</script>"],
    )
    .unwrap();
    drop(db);
    let app = fixture.app();
    for (path, kind) in [
        ("/project/29437".to_owned(), "VisualArtwork"),
        (format!("/token/{CONTRACT}/133459"), "VisualArtwork"),
        ("/profile/emptyname".into(), "Person"),
    ] {
        let (status, _, body) = get(&app, &path, "text/html").await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("rel=\"canonical\" href=\"http://127.0.0.1:3918/"));
        assert!(!body.contains("untrusted.invalid"));
        let raw = body
            .split("<script type=\"application/ld+json\">")
            .nth(1)
            .unwrap()
            .split("</script>")
            .next()
            .unwrap();
        let value: Value = serde_json::from_str(raw).unwrap();
        assert_eq!(ld_node(&value, kind)["@type"], kind);
        assert!(!raw.contains('<'));
        assert!(!body.contains("<script>alert(1)</script>"));
    }
}
#[tokio::test]
async fn sitemap_shards_exclude_hidden_records_and_have_bounded_routes() {
    let fixture = Fixture::new();
    let app = fixture.app();
    let (status, _, index) = get(&app, "/sitemap.xml", "*/*").await;
    assert_eq!(status, StatusCode::OK);
    assert!(index.contains("<sitemapindex"));
    assert!(index.contains("/sitemap/projects/25000"));
    let (_, headers, xml) = get(&app, "/sitemap/projects/25000", "*/*").await;
    assert!(
        headers[header::CONTENT_TYPE]
            .to_str()
            .unwrap()
            .starts_with("application/xml")
    );
    assert!(xml.contains("/project/29437"));
    let db = rusqlite::Connection::open(fixture.dir.join("db.sqlite")).unwrap();
    db.execute("UPDATE projects SET hidden=1 WHERE id=29437", [])
        .unwrap();
    drop(db);
    let (_, _, xml) = get(&app, "/sitemap/projects/25000", "*/*").await;
    assert!(!xml.contains("/project/29437"));
    let (_, _, xml) = get(&app, &format!("/sitemap/tokens/{CONTRACT}/130000"), "*/*").await;
    assert!(!xml.contains("/133459"));
    let (_, _, xml) = get(&app, "/sitemap/profiles/tz1g", "*/*").await;
    assert!(!xml.contains("tz1gL9H53BzjDqmMi2JkKALeCzT7UAoopzEL"));
    for path in [
        "/sitemap/projects/1",
        "/sitemap/projects/9223372036854775807",
        "/sitemap/profiles/00",
        "/sitemap/profiles/!",
        "/sitemap/tokens/invalid/0",
    ] {
        assert_eq!(get(&app, path, "*/*").await.0, StatusCode::BAD_REQUEST);
    }
    let (status, headers, _) = get(&app, "/sitemap/profiles/g", "*/*").await;
    assert_eq!(status, StatusCode::MOVED_PERMANENTLY);
    assert_eq!(headers[header::LOCATION], "/sitemap.xml");
}

#[tokio::test]
async fn sitemap_index_lists_populated_visible_profile_shards_only() {
    let fixture = Fixture::new();
    let holder = "tz1gzRX6sWGdKSghWXgGg3Wgeah4j2LHUBbT";
    assert!(holder.as_bytes()[4] >= b'h');
    let db = rusqlite::Connection::open(fixture.dir.join("db.sqlite")).unwrap();
    db.execute("DELETE FROM holder_counts", []).unwrap();
    db.execute(
        "INSERT INTO holder_counts(holder_address,held,listed) VALUES (?1,5,0)",
        [holder],
    )
    .unwrap();
    // Thin collectors (under 5 editions, no name) stay out; a name keeps one in.
    let thin = "tz1gzRX6sWGdKSghWXgGg3Wgeah4j2LHUBbU";
    let named = "tz1gzRX6sWGdKSghWXgGg3Wgeah4j2LHUBbV";
    for address in [thin, named] {
        db.execute(
            "INSERT INTO holder_counts(holder_address,held,listed) VALUES (?1,1,0)",
            [address],
        )
        .unwrap();
    }
    db.execute(
        "INSERT INTO users(address,name,name_key) VALUES (?1,'Named','named')",
        [named],
    )
    .unwrap();
    // A name shared with another address is never shown, so it does not count.
    let shared = "tz1gzRX6sWGdKSghWXgGg3Wgeah4j2LHUBbW";
    db.execute(
        "INSERT INTO holder_counts(holder_address,held,listed) VALUES (?1,1,0)",
        [shared],
    )
    .unwrap();
    for address in [shared, "tz1VE1gFxMZ5TVCwWaYpXGANcfhyjgiUGkpg"] {
        db.execute(
            "INSERT INTO users(address,name,name_key) VALUES (?1,'Twin','twin') ON CONFLICT(address) DO UPDATE SET name='Twin', name_key='twin'",
            [address],
        )
        .unwrap();
    }
    // Invalid names can make a prefix nonempty but must never appear as profile URLs.
    db.execute(
        "INSERT INTO holder_counts(holder_address,held,listed) VALUES ('tz1xprivate-name',1,0)",
        [],
    )
    .unwrap();
    drop(db);
    let (status, _, index) = get(&fixture.app(), "/sitemap.xml", "*/*").await;
    assert_eq!(status, StatusCode::OK);
    assert!(index.contains("/sitemap/profiles/tz1g"));
    assert!(!index.contains("/sitemap/profiles/tz4z"));
    assert!(!index.contains("tz1xprivate-name"));
    let (_, headers, shard) = get(&fixture.app(), "/sitemap/profiles/tz1g", "*/*").await;
    assert!(shard.contains(&format!("/profile/{holder}</loc><lastmod>")));
    assert!(shard.contains(&format!("/profile/{named}")));
    assert!(!shard.contains(thin));
    assert!(!shard.contains(shared));
    assert_eq!(headers["x-robots-tag"], "noindex");
    let (_, _, shard) = get(&fixture.app(), "/sitemap/profiles/tz1x", "*/*").await;
    assert!(!shard.contains("tz1xprivate-name"));
}
#[tokio::test]
async fn discovery_exposes_parseable_openapi_and_agent_guidance() {
    let fixture = Fixture::new();
    let app = fixture.app();
    let (status, _, body) = get(&app, "/openapi.json", "*/*").await;
    assert_eq!(status, StatusCode::OK);
    let spec: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(spec["openapi"], "3.1.0");
    for route in [
        "/projects",
        "/profile/{id}",
        "/api/market/prepare",
        "/api/market/check",
    ] {
        assert!(spec["paths"].get(route).is_some(), "{route}");
    }
    let (status, _, body) = get(&app, "/llms.txt", "*/*").await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("Accept: application/json"));
    assert!(body.contains("`/agents.md`"));
    assert!(body.contains("https://forum.seeded.art"));
    assert!(body.starts_with("# seeded.art\n\n> seeded.art archives fxhash,"));
    let pages = body.split("\n## Pages\n").nth(1).expect("pages list");
    for page in ["/", "/artists", "/articles", "/about"] {
        assert!(
            pages.contains(&format!("](https://seeded.art{page})")),
            "{page}"
        );
    }
    let ready = &spec["components"]["schemas"]["Ready"]["properties"]["operations"]["items"];
    assert_eq!(ready["$ref"], "#/components/schemas/Operation");
    assert_eq!(
        spec["components"]["schemas"]["Operation"]["properties"]["kind"]["const"],
        "transaction"
    );
}

#[tokio::test]
async fn agents_find_the_guide_policy_sign_in_recipe_and_forum_flow() {
    let fixture = Fixture::new();
    let app = fixture.app();
    let (status, headers, guide) = get(&app, "/agents.md", "*/*").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers[header::CONTENT_TYPE],
        "text/markdown; charset=utf-8"
    );
    for needle in [
        "## Policy",
        "Agents are welcome, as agents.",
        "BLAKE2b-256",
        "tz4 is not supported",
        "https://forum.seeded.art/auth/seeded",
        "POST https://forum.seeded.art/register",
        "inject through any mainnet RPC node",
    ] {
        assert!(guide.contains(needle), "{needle}");
    }
    // Crawler rules say they do not cover an agent's own sign-in.
    let (_, _, robots) = get(&app, "/robots.txt", "*/*").await;
    let first = robots.lines().next().unwrap();
    assert!(
        first.starts_with("# Disallow lines are for crawlers."),
        "{first}"
    );
    assert!(first.ends_with("/agents.md"), "{first}");
    assert!(robots.lines().any(|line| line == "Disallow: /api/auth/"));
    // Every JSON and HTML page points to the guide.
    let (_, headers, body) = get(&app, "/projects", "application/json").await;
    let page: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(page["links"]["agents"], "/agents.md");
    assert!(
        headers[header::LINK]
            .to_str()
            .unwrap()
            .contains("</agents.md>; rel=\"help\"")
    );
    let (_, _, html) = get(&app, "/projects", "text/html").await;
    assert!(html.contains("<link rel=\"help\" type=\"text/markdown\" href=\"/agents.md\">"));
}

#[tokio::test]
async fn sitemap_index_reserves_room_for_fixed_shards() {
    let fixture = Fixture::new();
    let db = rusqlite::Connection::open(fixture.dir.join("db.sqlite")).unwrap();
    // Numeric shards alone fit; fixed profile/page shards must also count.
    db.execute(
        "INSERT INTO projects (id,title,title_key,artist_key,index_state) VALUES (?,'Bound test','bound test','','not_indexed')",
        [48_999_i64 * 5_000 - 1],
    )
    .unwrap();
    drop(db);
    let (status, headers, xml) = get(&fixture.app(), "/sitemap.xml", "*/*").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers[header::CACHE_CONTROL], "public, max-age=300");
    assert!(xml.matches("<sitemap>").count() <= 49_000);
}

#[tokio::test]
async fn token_neighbors_expose_only_ids_and_never_raw_aliases() {
    let fixture = Fixture::new();
    let db = rusqlite::Connection::open(fixture.dir.join("db.sqlite")).unwrap();
    db.execute(
        "UPDATE tokens SET owner_alias='hidden-shared-probe' WHERE project_id=29437",
        [],
    )
    .unwrap();
    let (_, _, body) = get(
        &fixture.app(),
        &format!("/token/{CONTRACT}/133459"),
        "application/json",
    )
    .await;
    assert!(!body.contains("hidden-shared-probe"));
    let data: Value = serde_json::from_str(&body).unwrap();
    let next = &data["data"]["next"];
    assert_eq!(next.as_object().unwrap().len(), 3);
    assert_eq!(next["contract"], CONTRACT);
    assert_eq!(next["iteration"], 2);
    assert!(next["token_id"].is_u64());
}

#[tokio::test]
async fn list_canonicals_drop_tracking_and_normalize_legacy_sorts() {
    let fixture = Fixture::new();
    for (path, expected) in [
        (
            "/projects?utm_source=abc&zzz=%22&sort=id",
            "/projects?sort=oldest",
        ),
        ("/projects?sort=title&after=ignored", "/"),
        ("/projects", "/"),
        ("/?sort=newest", "/projects?sort=newest"),
        ("/artists?utm_source=abc", "/artists"),
        ("/search?q=Arboreal&utm_source=abc", "/search?q=Arboreal"),
        (
            "/search?utm_source=abc&scope=people&q=Arboreal",
            "/search?q=Arboreal&scope=people",
        ),
    ] {
        let (status, h, body) = get(&fixture.app(), path, "application/json").await;
        assert_eq!(status, StatusCode::OK);
        let data: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(
            data["canonical"],
            format!("http://127.0.0.1:3918{expected}")
        );
        if path.starts_with("/search") {
            assert_eq!(h["x-robots-tag"], "noindex, follow");
        }
    }
}

#[tokio::test]
async fn unavailable_article_index_does_not_remove_other_sitemaps() {
    let fixture = Fixture::new();
    let config = seeded_archive::config::Config::from_values(&std::collections::BTreeMap::from([
        (
            "SEEDED_ARTICLES_DATABASE".into(),
            fixture.dir.join("missing.sqlite").display().to_string(),
        ),
        (
            "SEEDED_SALES_DATABASE".into(),
            fixture
                .dir
                .join("missing-sales.sqlite")
                .display()
                .to_string(),
        ),
    ]))
    .unwrap();
    let app = router(AppState::new(fixture.dir.join("db.sqlite"), "assets").configured(&config));
    let (status, headers, xml) = get(&app, "/sitemap.xml", "*/*").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers[header::CACHE_CONTROL], "public, max-age=300");
    assert!(xml.contains("/sitemap/projects/"));
    assert!(!xml.contains("/sitemap/articles/"));
}

#[tokio::test]
async fn profile_json_filters_unsafe_websites_and_escapes_bio_in_structured_html() {
    let fixture = Fixture::new();
    let db = rusqlite::Connection::open(fixture.dir.join("db.sqlite")).unwrap();
    let address = "tz1gL9H53BzjDqmMi2JkKALeCzT7UAoopzEL";
    db.execute(
        "UPDATE artists SET address=? WHERE id='emptyname'",
        [address],
    )
    .unwrap();
    for unsafe_url in ["javascript:alert(1)", "https://secret@example.org"] {
        db.execute(
            "UPDATE identities SET website=?1 WHERE address=?2",
            rusqlite::params![unsafe_url, address],
        )
        .unwrap();
        db.execute(
            "UPDATE artists SET website=?1,description=?2 WHERE address=?3",
            rusqlite::params![unsafe_url, "</script><script>bio&probe</script>", address],
        )
        .unwrap();
        let (_, _, body) = get(
            &fixture.app(),
            &format!("/profile/{address}"),
            "application/json",
        )
        .await;
        let data: Value = serde_json::from_str(&body).unwrap();
        assert!(data["data"]["profile"]["website"].is_null());
        let (_, _, html) = get(&fixture.app(), &format!("/profile/{address}"), "text/html").await;
        let raw = html
            .split("<script type=\"application/ld+json\">")
            .nth(1)
            .unwrap()
            .split("</script>")
            .next()
            .unwrap();
        assert!(!raw.contains('<'));
        assert!(!raw.contains('&'));
        let ld: Value = serde_json::from_str(raw).unwrap();
        assert_eq!(
            ld_node(&ld, "Person")["description"],
            "</script><script>bio&probe</script>"
        );
    }
}

#[tokio::test]
async fn profile_sitemap_truncates_at_fifty_thousand_and_uses_index_ranges() {
    let fixture = Fixture::new();
    let db = rusqlite::Connection::open(fixture.dir.join("db.sqlite")).unwrap();
    db.execute("WITH RECURSIVE n(x) AS(VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<50001) INSERT INTO holder_counts(holder_address,held,listed) SELECT 'tz1x'||printf('%032d',x),1,0 FROM n",[]).unwrap();
    let (status, _, xml) = get(&fixture.app(), "/sitemap/profiles/tz1x", "*/*").await;
    assert_eq!(status, StatusCode::OK);
    assert!(xml.matches("<url>").count() <= 50000);
    let plan:String=db.prepare("EXPLAIN QUERY PLAN SELECT holder_address FROM holder_counts WHERE held>0 AND holder_address>=?1 AND holder_address<?2 ORDER BY holder_address LIMIT 50001").unwrap().query_map(["tz1x","tz1y"],|r|r.get::<_,String>(3)).unwrap().collect::<Result<Vec<_>,_>>().unwrap().join(" ");
    assert!(
        plan.contains("SEARCH holder_counts USING PRIMARY KEY"),
        "{plan}"
    );
}

fn head_of(html: &str) -> &str {
    html.split("</head>").next().unwrap()
}

fn meta(head: &str, attr: &str, name: &str) -> Option<String> {
    let marker = format!("<meta {attr}=\"{name}\" content=\"");
    let rest = head.split(&marker).nth(1)?;
    Some(rest.split('"').next()?.to_owned())
}

#[tokio::test]
async fn pages_carry_titles_descriptions_previews_and_structured_data() {
    let fixture = Fixture::new();
    let app = fixture.app();
    let (status, headers, html) = get(&app, "/project/29437", "text/html").await;
    assert_eq!(status, StatusCode::OK);
    let head = head_of(&html);
    let title = head
        .split("<title>")
        .nth(1)
        .unwrap()
        .split("</title>")
        .next()
        .unwrap();
    assert!(title.contains("| fxhash project"), "{title}");
    assert!(title.chars().count() <= 60, "{title}");
    let description = meta(head, "name", "description").unwrap();
    assert!(
        description.starts_with("fxhash generative art on Tezos"),
        "{description}"
    );
    assert!(description.chars().count() <= 160);
    assert_eq!(
        meta(head, "property", "og:site_name").unwrap(),
        "seeded.art"
    );
    assert_eq!(
        meta(head, "property", "og:url").unwrap(),
        "http://127.0.0.1:3918/project/29437"
    );
    assert_eq!(
        meta(head, "property", "og:description").unwrap(),
        description
    );
    assert_eq!(headers["x-robots-tag"], "max-image-preview:large");
    let raw = head
        .split("<script type=\"application/ld+json\">")
        .nth(1)
        .unwrap()
        .split("</script>")
        .next()
        .unwrap();
    let ld: Value = serde_json::from_str(raw).unwrap();
    assert_eq!(ld_node(&ld, "VisualArtwork")["@type"], "VisualArtwork");
    let crumbs = ld_node(&ld, "BreadcrumbList");
    assert_eq!(
        crumbs["itemListElement"][0]["item"],
        "http://127.0.0.1:3918/"
    );

    let (status, headers, html) = get(&app, "/", "text/html").await;
    assert_eq!(status, StatusCode::OK);
    let head = head_of(&html);
    assert!(head.contains("<title>seeded.art: generative art on Tezos (fxhash archive)</title>"));
    assert!(
        meta(head, "name", "description")
            .unwrap()
            .starts_with("Archive of fxhash generative art on Tezos")
    );
    // Pages without their own artwork share the 1200×630 site card.
    assert_eq!(
        meta(head, "property", "og:image").unwrap(),
        "http://127.0.0.1:3918/static/og.png"
    );
    assert_eq!(meta(head, "property", "og:image:width").unwrap(), "1200");
    assert_eq!(meta(head, "property", "og:image:height").unwrap(), "630");
    assert_eq!(
        meta(head, "name", "twitter:card").unwrap(),
        "summary_large_image"
    );
    assert_eq!(headers["x-robots-tag"], "max-image-preview:large");
    let raw = head
        .split("<script type=\"application/ld+json\">")
        .nth(1)
        .unwrap()
        .split("</script>")
        .next()
        .unwrap();
    let ld: Value = serde_json::from_str(raw).unwrap();
    let site = ld_node(&ld, "WebSite");
    assert_eq!(site["alternateName"], "fxhash archive");
    assert_eq!(
        site["potentialAction"]["target"]["urlTemplate"],
        "http://127.0.0.1:3918/search?q={search_term_string}"
    );
    assert_eq!(ld_node(&ld, "Organization")["name"], "seeded.art");

    let (_, _, html) = get(&app, &format!("/token/{CONTRACT}/133459"), "text/html").await;
    let head = head_of(&html);
    assert!(head.contains("| fxhash NFT"));
    assert!(
        meta(head, "name", "description")
            .unwrap()
            .starts_with("Minted ")
    );
}

#[tokio::test]
async fn thin_filtered_and_paginated_pages_are_not_indexed() {
    let fixture = Fixture::new();
    let app = fixture.app();
    for (path, expected) in [
        ("/projects?sort=newest", "noindex, follow"),
        ("/collectors", "noindex, follow"),
        ("/search?q=Arboreal", "noindex, follow"),
        ("/artists", "max-image-preview:large"),
        ("/about", "max-image-preview:large"),
    ] {
        let (status, headers, _) = get(&app, path, "text/html").await;
        assert_eq!(status, StatusCode::OK, "{path}");
        assert_eq!(headers["x-robots-tag"], expected, "{path}");
    }
}

#[tokio::test]
async fn sitemaps_list_indexable_pages_with_last_modified_dates() {
    let fixture = Fixture::new();
    let app = fixture.app();
    let (status, headers, pages) = get(&app, "/sitemap/pages.xml", "*/*").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers["x-robots-tag"], "noindex");
    assert!(pages.contains("<loc>http://127.0.0.1:3918/</loc><lastmod>"));
    assert!(
        !pages.contains("/projects<")
            && !pages.contains("/search")
            && !pages.contains("/collectors")
    );
    let (_, _, projects) = get(&app, "/sitemap/projects/25000", "*/*").await;
    assert!(
        projects.contains("/project/29437</loc><lastmod>2026-"),
        "{projects}"
    );
    let (_, _, tokens) = get(&app, &format!("/sitemap/tokens/{CONTRACT}/130000"), "*/*").await;
    assert!(tokens.contains("/133459</loc><lastmod>2026-"), "{tokens}");
}
