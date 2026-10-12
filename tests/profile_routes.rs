use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

use axum::{
    Router,
    body::Body,
    http::{HeaderMap, Request, StatusCode},
};
use http_body_util::BodyExt;
use seeded_archive::{
    auth::{AuthState, SignInRequest, VerifyRequest},
    import::import_fixtures,
    store::Store,
    web::{AppState, router},
};
use tower::ServiceExt;

const KEEPER: &str = "tz1gL9H53BzjDqmMi2JkKALeCzT7UAoopzEL";
const HOLDER: &str = "tz2RXosjAZdS85oCrguy7Eom757144dcU1FT";
const CONTRACT: &str = "KT1EfsNuqwLAWDd3o4pvfUx1CAh5GMdTrRvr";
static NEXT: AtomicU64 = AtomicU64::new(0);

struct Archive {
    directory: PathBuf,
    database: PathBuf,
}
impl Archive {
    fn new() -> Self {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let directory = root.join(format!(
            "tmp/profile-routes-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&directory).unwrap();
        let database = directory.join("archive.sqlite");
        let mut store = Store::open(&database).unwrap();
        import_fixtures(&mut store, &root.join("assets/fixtures")).unwrap();
        seeded_archive::ingest::identity::apply_to(&mut store).unwrap();
        Self {
            directory,
            database,
        }
    }
    fn state(&self) -> AppState {
        AppState::new(
            &self.database,
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("assets"),
        )
    }
    fn sql(&self) -> rusqlite::Connection {
        rusqlite::Connection::open(&self.database).unwrap()
    }
}
impl Drop for Archive {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.directory).unwrap();
    }
}

async fn get(app: &Router, path: &str, cookie: Option<&str>) -> (StatusCode, HeaderMap, String) {
    let mut request = Request::builder().uri(path);
    if let Some(cookie) = cookie {
        request = request.header("cookie", cookie);
    }
    let response = app
        .clone()
        .oneshot(request.body(Body::empty()).unwrap())
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
async fn names_and_addresses_share_profiles_with_case_insensitive_lookup() {
    let archive = Archive::new();
    let app = router(archive.state());
    let (_, _, canonical) = get(&app, &format!("/profile/{KEEPER}"), None).await;
    for name in ["emptyname", "EmptyName", "EMPTYNAME"] {
        let (status, _, html) = get(&app, &format!("/profile/{name}"), None).await;
        assert_eq!(status, StatusCode::OK);
        // Alternate JSON links retain the requested alias.
        assert_eq!(
            html.split("<body>").nth(1),
            canonical.split("<body>").nth(1)
        );
        let expected = format!("rel=\"canonical\" href=\"http://127.0.0.1:3918/profile/{KEEPER}\"");
        assert!(html.contains(&expected));
    }
    assert!(canonical.contains("Name curated by the archive"));
    assert!(canonical.contains("data-project-id=\"29437\""));
    assert!(canonical.contains("Projects <span>1</span>"));
    assert!(!canonical.contains("href=\"#profile-articles\">Articles</a>"));
    let (_, _, collected) = get(&app, &format!("/profile/{KEEPER}?view=collected"), None).await;
    assert!(collected.contains("Collected <span>1</span>"));
    assert!(!canonical.contains("Listings and offers"));
    assert!(!canonical.contains("Edit profile"));
}

#[tokio::test]
async fn profiles_open_on_projects_for_artists_and_on_the_collection_for_collectors() {
    let archive = Archive::new();
    let app = router(archive.state());
    let (status, _, collector) = get(&app, &format!("/profile/{HOLDER}"), None).await;
    assert_eq!(status, StatusCode::OK);
    assert!(collector.contains("Projects <span>0</span>"));
    assert!(collector.contains("?view=collected\" aria-current=\"page\""));
    // The tab names the view; no second heading below the tabs.
    assert!(collector.contains("<section id=\"generative-work\" aria-label=\"Collected NFTs\">"));
    assert!(!collector.contains("<h2>Collected NFTs</h2>"));
    assert!(collector.contains("data-token-id=\"133459\""));
    assert!(collector.contains(&format!(
        "rel=\"canonical\" href=\"http://127.0.0.1:3918/profile/{HOLDER}\""
    )));
    let (_, _, projects) = get(&app, &format!("/profile/{HOLDER}?view=projects"), None).await;
    assert!(projects.contains("?view=projects\" aria-current=\"page\""));
    let (status, _, artist) = get(&app, &format!("/profile/{KEEPER}"), None).await;
    assert_eq!(status, StatusCode::OK);
    assert!(artist.contains("?view=projects\" aria-current=\"page\""));
    assert!(artist.contains("data-project-id=\"29437\""));
    assert!(artist.contains(&format!(
        "rel=\"canonical\" href=\"http://127.0.0.1:3918/profile/{KEEPER}\""
    )));
    // Trait filters apply to a collector's default view, not an artist's.
    let traits = "?trait_name=Palette&trait_value=Gold";
    assert_eq!(
        get(&app, &format!("/profile/{HOLDER}{traits}"), None)
            .await
            .0,
        StatusCode::OK
    );
    assert_eq!(
        get(&app, &format!("/profile/{KEEPER}{traits}"), None)
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
}

#[tokio::test]
async fn legacy_routes_permanently_redirect_with_valid_tab_and_search() {
    let archive = Archive::new();
    let app = router(archive.state());
    for old in [
        format!("/collector/{KEEPER}?q=Arboreal&trait_name=Palette&trait_value=Gold"),
        "/artist/emptyname?view=collected&q=Arboreal&trait_name=Palette&trait_value=Gold".into(),
    ] {
        let (status, headers, _) = get(&app, &old, None).await;
        assert_eq!(status, StatusCode::PERMANENT_REDIRECT);
        assert_eq!(
            headers["location"],
            format!(
                "/profile/{KEEPER}?view=collected&q=Arboreal&trait_name=Palette&trait_value=Gold"
            )
        );
    }
    for path in [
        "/artist/emptyname?view=secret",
        "/profile/emptyname?view=market",
        "/profile/emptyname?trait_name=Palette",
        "/project/29437?trait_value=Gold",
    ] {
        assert_eq!(
            get(&app, path, None).await.0,
            StatusCode::BAD_REQUEST,
            "{path}"
        );
    }
}

#[tokio::test]
async fn duplicate_imported_names_never_resolve_or_appear_as_artist_or_owner_labels() {
    let archive = Archive::new();
    let sql = archive.sql();
    let second = "tz1N64TWNCZR3yJZmmzBE4DRidN1tCaZCw8Q";
    for (address, name) in [(HOLDER, "SharedName"), (second, "sharedname")] {
        sql.execute("INSERT INTO users(address,name,name_key) VALUES(?1,?2,'sharedname') ON CONFLICT(address) DO UPDATE SET name=excluded.name,name_key=excluded.name_key", [address, name]).unwrap();
    }
    sql.execute("UPDATE artists SET address=?1,name='SharedName',name_key='sharedname' WHERE id='emptyname'", [HOLDER]).unwrap();
    sql.execute(
        "UPDATE tokens SET owner_alias='SharedName' WHERE owner_address=?1",
        [HOLDER],
    )
    .unwrap();
    drop(sql);
    let app = router(archive.state());
    for name in ["SharedName", "sharedname", "SHAREDNAME"] {
        assert_eq!(
            get(&app, &format!("/profile/{name}"), None).await.0,
            StatusCode::NOT_FOUND
        );
    }
    for path in [
        // Newest first puts Arboreal (project 29437) on the first page.
        "/projects?sort=newest".to_owned(),
        "/artists".into(),
        "/project/29437".into(),
        format!("/token/{CONTRACT}/133459"),
        format!("/profile/{HOLDER}?view=collected"),
    ] {
        let (status, _, html) = get(&app, &path, None).await;
        assert_eq!(status, StatusCode::OK);
        assert!(!html.to_lowercase().contains("sharedname"), "{path}");
        assert!(html.contains(HOLDER), "address fallback {path}");
    }
}

#[tokio::test]
async fn edition_cards_show_the_artist_left_then_the_owner_right() {
    let archive = Archive::new();
    let app = router(archive.state());
    for path in [
        format!("/profile/{HOLDER}?view=collected"),
        "/project/29437".to_owned(),
    ] {
        let (status, _, html) = get(&app, &path, None).await;
        assert_eq!(status, StatusCode::OK);
        let card = html
            .split("<article class=\"artwork-card\"")
            .nth(1)
            .expect("an edition card");
        let people = card.split("class=\"artwork-people\"").nth(1).unwrap();
        let artist = people.find("class=\"artwork-artist\"").unwrap();
        let owner = people.find("class=\"artwork-owner\"").unwrap();
        assert!(artist < owner, "{path}");
        assert!(people[artist..owner].contains("Emptyname"), "{path}");
    }
}

#[tokio::test]
async fn holdings_and_editions_search_recover_and_keep_cursors_bound_to_the_filter() {
    let archive = Archive::new();
    let sql = archive.sql();
    sql.execute("WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<30) INSERT INTO tokens(contract,token_id,project_id,iteration,seed,minter,minted,artifact_uri,owner_address,owner_as_of,owner_source,holder_address) SELECT contract,900000+i,project_id,100+i,seed,minter,minted,artifact_uri,?1,owner_as_of,owner_source,?1 FROM tokens,n WHERE token_id=133459", [HOLDER]).unwrap();
    drop(sql);
    let app = router(archive.state());
    for path in [
        format!("/profile/{HOLDER}?view=collected&q=Arboreal"),
        format!("/project/29437?q={HOLDER}"),
    ] {
        let (status, _, html) = get(&app, &path, None).await;
        assert_eq!(status, StatusCode::OK);
        let next = html
            .split("href=\"")
            .filter_map(|part| part.split_once('"'))
            .find(|(_, rest)| rest.starts_with(" rel=\"next\""))
            .unwrap()
            .0
            .replace("&amp;", "&")
            .replace("&#38;", "&");
        assert!(next.contains("q="));
        assert_eq!(get(&app, &next, None).await.0, StatusCode::OK);
        let changed = next.replace("q=Arboreal", "q=Gold").replace(HOLDER, KEEPER);
        assert_eq!(get(&app, &changed, None).await.0, StatusCode::BAD_REQUEST);
        assert!(
            !html
                .split("<form class=\"collection-search\"")
                .nth(1)
                .unwrap()
                .split("</form>")
                .next()
                .unwrap()
                .contains("name=\"after\"")
        );
    }
    let (_, _, none) = get(
        &app,
        &format!("/profile/{HOLDER}?view=collected&q=nevermatches"),
        None,
    )
    .await;
    assert!(none.contains("No matching NFTs"));
    assert!(none.contains("Clear filters"));
    let (_, _, one) = get(&app, "/project/29437?q=%231", None).await;
    assert!(one.contains("data-token-id=\"133459\""));
    assert!(!one.contains("data-token-id=\"133460\""));
    let (_, _, traits) = get(
        &app,
        "/project/29437?trait_name=Palette&trait_value=Gold",
        None,
    )
    .await;
    assert!(traits.contains("data-token-id=\"133459\""));
    assert!(!traits.contains("data-token-id=\"185776\""));
}

#[tokio::test]
async fn token_autoplays_with_shared_sanitized_project_instructions_and_direct_owner_address() {
    let archive = Archive::new();
    let sql = archive.sql();
    sql.execute("UPDATE projects SET description='**A real project** <script>attack()</script>', iteration_description='Press **S** to save. [unsafe](javascript:attack())' WHERE id=29437", []).unwrap();
    drop(sql);
    let config = seeded_archive::config::Config::from_values(&Default::default()).unwrap();
    let app = router(archive.state().configured(&config));
    let (status, _, html) = get(&app, &format!("/token/{CONTRACT}/133459"), None).await;
    assert_eq!(status, StatusCode::OK);
    assert!(html.contains("data-autoplay=\"true\""));
    assert!(html.contains(">Replay</button>"));
    assert!(html.contains("<strong>A real project</strong>"));
    assert!(html.contains("Press <strong>S</strong>"));
    assert!(!html.contains("<script>attack()"));
    assert!(!html.contains("href=\"javascript:"));
    assert!(!html.contains("Ledger holder"));
    assert!(html.contains(HOLDER));
    let (_, _, project) = get(&app, "/project/29437", None).await;
    assert!(project.contains("<strong>A real project</strong>"));
}

#[tokio::test]
async fn only_a_verified_matching_session_exposes_profile_edit_and_market_tabs() {
    use blake2::{Blake2b, Digest, digest::consts::U32};
    use ed25519_dalek::{Signer, SigningKey};
    let archive = Archive::new();
    let auth = AuthState::new("http://127.0.0.1:3918").unwrap();
    let key = SigningKey::from_bytes(&[91; 32]);
    let public = bs58::encode([&[13, 15, 37, 217][..], key.verifying_key().as_bytes()].concat())
        .with_check()
        .into_string();
    let address = seeded_archive::auth::crypto::public_key_address(&public).unwrap();
    let mut headers = HeaderMap::new();
    headers.insert("origin", "http://127.0.0.1:3918".parse().unwrap());
    let challenge = auth
        .challenge(
            &headers,
            SignInRequest {
                address: address.clone(),
                public_key: public,
            },
        )
        .unwrap();
    headers.insert(
        "cookie",
        challenge.cookie.split(';').next().unwrap().parse().unwrap(),
    );
    headers.insert("x-csrf-token", challenge.view.csrf.parse().unwrap());
    let digest = Blake2b::<U32>::digest(hex::decode(challenge.view.payload).unwrap());
    let signature =
        bs58::encode([&[9, 245, 205, 134, 18][..], &key.sign(&digest).to_bytes()].concat())
            .with_check()
            .into_string();
    let session = auth
        .verify(&headers, VerifyRequest { signature })
        .await
        .unwrap();
    let cookie = session.cookie.split(';').next().unwrap();
    let app = router(archive.state().with_auth(auth));
    let (_, _, own) = get(&app, &format!("/profile/{address}"), Some(cookie)).await;
    // Edit profile is the fourth tab, and the own profile has no artists back link.
    assert!(own.contains(
        ">Listings and offers <span>0</span></a><a href=\"/auth/profile\" >Edit profile</a></nav>"
    ));
    assert!(!own.contains("button-link secondary-button\" href=\"/auth/profile\""));
    assert!(!own.contains("← All artists"));
    assert!(own.contains(&format!("/profile/{address}/market")));
    for (path, cookie) in [
        (format!("/profile/{address}"), None),
        (format!("/profile/{address}"), Some("seeded_session=forged")),
        ("/profile/emptyname".into(), Some(cookie)),
    ] {
        let (status, _, html) = get(&app, &path, cookie).await;
        assert_eq!(status, StatusCode::OK);
        assert!(!html.contains("Edit profile"));
        assert!(!html.contains("Listings and offers"));
        assert!(html.contains("← All artists"));
    }
    // The edit page shows the same header with Edit profile as the current tab.
    let (_, _, edit) = get(&app, "/auth/profile", Some(cookie)).await;
    assert!(edit.contains("<header class=\"profile-header\">"));
    assert!(
        edit.contains("<a href=\"/auth/profile\" aria-current=\"page\">Edit profile</a></nav>")
    );
    assert!(edit.contains("<form data-auth-profile-form"));
    assert!(!edit.contains("Blank fields keep imported values."));
}

#[tokio::test]
async fn profile_media_and_markup_keep_active_capabilities_separate_from_public_browsing() {
    let archive = Archive::new();
    let sql = archive.sql();
    sql.execute("INSERT INTO profiles_imported(address,source,bio,links,avatar_uri,fetched_at) VALUES(?1,'objkt',?2,?3,?4,'2026-10-05T00:00:00Z')", rusqlite::params![HOLDER,"**Artist** <script>alert(1)</script>",r#"["https://example.org/work","javascript:alert(1)"]"#,"ipfs://bafyfixture/avatar.png"]).unwrap();
    let config = seeded_archive::config::Config::from_values(&Default::default()).unwrap();
    let app = router(archive.state().configured(&config));
    for (uri, expected) in [
        (
            "ipfs://bafyfixture/avatar.png",
            format!("http://127.0.0.1:3919/profile-media/{HOLDER}"),
        ),
        (
            "https://gateway.example/ipfs/bafyfixture/avatar.png",
            format!("http://127.0.0.1:3919/profile-media/{HOLDER}"),
        ),
        (
            "https://assets.objkt.media/file/assets-004/h/tz2RXosjAZdS85oCrguy7Eom757144dcU1FT/logo?cb=0123abcd",
            format!("http://127.0.0.1:3919/profile-media/{HOLDER}"),
        ),
    ] {
        sql.execute(
            "UPDATE profiles_imported SET avatar_uri=?1 WHERE address=?2",
            [uri, HOLDER],
        )
        .unwrap();
        let (status, headers, html) = get(&app, &format!("/profile/{HOLDER}"), None).await;
        assert_eq!(status, StatusCode::OK);
        assert!(html.contains(&format!("src=\"{expected}\"")));
        assert!(html.contains("referrerpolicy=\"no-referrer\""));
        assert!(html.contains("<strong>Artist</strong>"));
        assert!(!html.contains("<script>alert(1)</script>"));
        assert!(!html.contains("href=\"javascript:"));
        let csp = headers["content-security-policy"].to_str().unwrap();
        assert!(csp.contains("media-src 'self' http://127.0.0.1:3919;"));
        assert!(!csp.contains("img-src 'self' data: https:"));
        assert!(html.contains("rel=\"nofollow ugc noopener noreferrer\""));
        assert!(!csp.contains("unsafe-inline"));
        assert!(!csp.contains("walletconnect"));
        assert!(!csp.contains("octez.io"));
    }
    sql.execute("UPDATE profiles_imported SET avatar_uri='https://images.example/avatar.png' WHERE address=?1",[HOLDER]).unwrap();
    let (_, _, unsupported) = get(&app, &format!("/profile/{HOLDER}"), None).await;
    assert!(!unsupported.contains("src=\"https://images.example"));
    assert!(!unsupported.contains("<img class=\"profile-avatar\""));
    let (_, headers, _) = get(&app, "/auth", None).await;
    let csp = headers["content-security-policy"].to_str().unwrap();
    assert!(csp.contains("style-src 'self' 'unsafe-inline'"));
    assert!(csp.contains("script-src 'self';"));
    assert!(csp.contains("img-src 'self' data: https:"));
    assert!(csp.contains("https://beacon-node-1.octez.io"));
    assert!(csp.contains("wss://relay.walletconnect.com"));
    assert!(!csp.contains("unsafe-eval"));
}

#[tokio::test]
async fn artist_directory_uses_profile_avatar_precedence_and_safe_content_urls() {
    let archive = Archive::new();
    let sql = archive.sql();
    sql.execute(
        "UPDATE artists SET address=?1 WHERE id='emptyname'",
        [KEEPER],
    )
    .unwrap();
    sql.execute(
        "UPDATE artists SET avatar_uri='ipfs://bafyfixture/artist.png' WHERE address=?1",
        [KEEPER],
    )
    .unwrap();
    sql.execute("INSERT INTO profiles_imported(address,source,avatar_uri,fetched_at) VALUES(?1,'objkt','https://images.example/lower-priority.png','2026-10-05T00:00:00Z')", [KEEPER]).unwrap();
    drop(sql);

    let config = seeded_archive::config::Config::from_values(&Default::default()).unwrap();
    let app = router(archive.state().configured(&config));
    let expected = format!("http://127.0.0.1:3919/profile-media/{KEEPER}");
    let (status, _, profile) = get(&app, &format!("/profile/{KEEPER}"), None).await;
    assert_eq!(status, StatusCode::OK);
    assert!(profile.contains(&format!("src=\"{expected}\"")));

    for uri in [
        "ipfs://bafyfixture/artist.png",
        "onchfs://bafyfixture/artist.png",
        "https://assets.objkt.media/file/assets-004/h/tz1gL9H53BzjDqmMi2JkKALeCzT7UAoopzEL/logo?cb=0123abcd",
    ] {
        archive
            .sql()
            .execute(
                "UPDATE artists SET avatar_uri=?1 WHERE address=?2",
                [uri, KEEPER],
            )
            .unwrap();
        let (status, headers, directory) = get(&app, "/artists", None).await;
        assert_eq!(status, StatusCode::OK);
        assert!(directory.contains(&format!("href=\"/profile/{KEEPER}\"")));
        assert!(directory.contains(&format!("src=\"{expected}\"")), "{uri}");
        assert!(!directory.contains(uri), "directory must not hotlink {uri}");
        let csp = headers["content-security-policy"].to_str().unwrap();
        assert!(csp.contains("img-src 'self' data: http://127.0.0.1:3919;"));
        assert!(!csp.contains("img-src 'self' data: https:"));
    }

    archive
        .sql()
        .execute(
            "UPDATE artists SET avatar_uri='https://images.example/arbitrary.png' WHERE address=?1",
            [KEEPER],
        )
        .unwrap();
    let (status, _, directory) = get(&app, "/artists", None).await;
    assert_eq!(status, StatusCode::OK);
    let (status, _, search) = get(&app, "/search?q=Empty&scope=people", None).await;
    assert_eq!(status, StatusCode::OK);
    assert!(!search.contains("https://images.example/arbitrary.png"));
    let input = search
        .split("id=\"global-search\"")
        .nth(1)
        .unwrap()
        .split('>')
        .next()
        .unwrap();
    assert!(input.contains("name=\"q\""));
    assert!(input.contains("value=\"Empty\""));
    assert!(!directory.contains(&format!("src=\"{expected}\"")));
    assert!(!directory.contains("https://images.example/arbitrary.png"));
    assert!(directory.contains(&format!("href=\"/profile/{KEEPER}\"")));
    assert!(directory.contains("<span class=\"profile-avatar\" aria-hidden=\"true\">E</span>"));
    let (_, _, profile) = get(&app, &format!("/profile/{KEEPER}"), None).await;
    assert!(profile.contains("<span class=\"profile-avatar\" aria-hidden=\"true\">E</span>"));

    archive
        .sql()
        .execute(
            "UPDATE artists SET avatar_uri='javascript:alert(1)' WHERE address=?1",
            [KEEPER],
        )
        .unwrap();
    let (_, _, unsafe_uri) = get(&app, "/artists", None).await;
    assert!(!unsafe_uri.contains(&format!("src=\"{expected}\"")));
    assert!(!unsafe_uri.contains("javascript:alert(1)"));
    assert!(unsafe_uri.contains("<span class=\"profile-avatar\" aria-hidden=\"true\">E</span>"));

    archive
        .sql()
        .execute(
            "UPDATE artists SET avatar_uri=NULL WHERE address=?1",
            [KEEPER],
        )
        .unwrap();
    archive
        .sql()
        .execute("DELETE FROM profiles_imported WHERE address=?1", [KEEPER])
        .unwrap();
    let (_, _, absent) = get(&app, "/artists", None).await;
    assert!(!absent.contains(&format!("src=\"{expected}\"")));
    assert!(absent.contains("<span class=\"profile-avatar\" aria-hidden=\"true\">E</span>"));
    let (_, _, absent_profile) = get(&app, &format!("/profile/{KEEPER}"), None).await;
    assert!(
        absent_profile.contains("<span class=\"profile-avatar\" aria-hidden=\"true\">E</span>")
    );

    archive
        .sql()
        .execute(
            "UPDATE artists SET avatar_uri='ipfs://bafyfixture/artist.png' WHERE address=?1",
            [KEEPER],
        )
        .unwrap();

    let app_without_content_origin = router(archive.state());
    let (status, _, no_origin) = get(&app_without_content_origin, "/artists", None).await;
    assert_eq!(status, StatusCode::OK);
    assert!(!no_origin.contains("/profile-media/"));
    assert!(no_origin.contains("<span class=\"profile-avatar\" aria-hidden=\"true\">E</span>"));
}

#[tokio::test]
async fn claimed_names_and_legacy_slugs_cannot_capture_addressless_artist_attribution() {
    use blake2::{Blake2b, Digest, digest::consts::U32};
    use ed25519_dalek::{Signer, SigningKey};
    use seeded_archive::model::{EditProof, ProfileEdit};
    let archive = Archive::new();
    let sql = archive.sql();
    sql.execute(
        "INSERT INTO artists(id,name,name_key) VALUES('legacy-orphan','OrphanName','orphanname')",
        [],
    )
    .unwrap();
    sql.execute("INSERT INTO project_artists(project_id,position,artist_id) VALUES(29437,9,'legacy-orphan')",[]).unwrap();
    drop(sql);
    let key = SigningKey::from_bytes(&[49; 32]);
    let public = bs58::encode([&[13, 15, 37, 217][..], key.verifying_key().as_bytes()].concat())
        .with_check()
        .into_string();
    let address = seeded_archive::auth::crypto::public_key_address(&public).unwrap();
    archive
        .sql()
        .execute(
            "UPDATE tokens SET minter=?1 WHERE project_id=29437 AND iteration=2",
            [&address],
        )
        .unwrap();
    let app = router(archive.state());
    for name in ["OrphanName", "legacy-orphan"] {
        let message = format!("Claim profile name {name}");
        let payload = seeded_archive::auth::crypto::packed_message(&message);
        let digest = Blake2b::<U32>::digest(hex::decode(&payload).unwrap());
        let signature =
            bs58::encode([&[9, 245, 205, 134, 18][..], &key.sign(&digest).to_bytes()].concat())
                .with_check()
                .into_string();
        seeded_archive::auth::crypto::verify_signature(&address, &public, &payload, &signature)
            .unwrap();
        let mut store = Store::open(&archive.database).unwrap();
        store
            .save_profile_edit(
                &address,
                ProfileEdit {
                    name: Some(name.into()),
                    ..Default::default()
                },
                EditProof {
                    message,
                    signature,
                    public_key: public.clone(),
                },
            )
            .unwrap();
        drop(store);
        let (status, _, profile) = get(&app, &format!("/profile/{name}"), None).await;
        assert_eq!(status, StatusCode::OK);
        assert!(profile.contains("Name set locally by this address"));
        assert_eq!(
            get(&app, "/artist/legacy-orphan", None).await.0,
            StatusCode::NOT_FOUND
        );
        for path in [
            "/projects?sort=newest".into(),
            "/artists".into(),
            "/project/29437".into(),
            format!("/token/{CONTRACT}/133459"),
        ] {
            let (status, _, html) = get(&app, &path, None).await;
            assert_eq!(status, StatusCode::OK);
            assert!(
                !html.contains(&format!("href=\"/profile/{address}\"")),
                "{path}"
            );
            assert!(
                !html.contains("href=\"/profile/legacy%2Dorphan\""),
                "{path}"
            );
            assert!(
                html.contains(&format!("href=\"/profile/{KEEPER}\"")),
                "curated binding lost on {path}"
            );
        }
        let (status, headers, _) = get(&app, "/artist/emptyname", None).await;
        assert_eq!(status, StatusCode::PERMANENT_REDIRECT);
        assert_eq!(
            headers["location"],
            format!("/profile/{KEEPER}?view=projects")
        );
    }
}

#[tokio::test]
async fn filtered_edition_counts_do_not_claim_the_whole_project_has_one_edition() {
    let archive = Archive::new();
    let app = router(archive.state());
    let (_, _, html) = get(&app, "/project/29437?q=%231", None).await;
    assert!(html.contains("NFTs <span class=\"section-count\">1</span>"));
    assert!(html.contains("<dt>Minted</dt><dd>10"));
    assert!(html.contains("Clear filters"));
    let (_, _, all) = get(&app, "/project/29437", None).await;
    assert!(all.contains("NFTs <span class=\"section-count\">10</span>"));
}
