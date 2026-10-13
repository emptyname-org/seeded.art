use axum::{
    body::Body,
    http::{HeaderMap, Request, StatusCode, header},
};
use ed25519_dalek::Signer;
use http_body_util::BodyExt;
use seeded_archive::{
    auth::{AuthState, SignInRequest, VerifyRequest, crypto},
    import::import_fixtures,
    store::Store,
    web::{AppState, router},
};
use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};
use tower::ServiceExt;
const ORIGIN: &str = "http://127.0.0.1:3918";
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Archive(PathBuf);
impl Archive {
    fn new() -> Self {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tmp/trading")
            .join(format!(
                "routes-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::SeqCst)
            ));
        std::fs::create_dir_all(&path).unwrap();
        let mut store = Store::open(&path.join("archive.sqlite")).unwrap();
        import_fixtures(
            &mut store,
            &Path::new(env!("CARGO_MANIFEST_DIR")).join("assets/fixtures"),
        )
        .unwrap();
        Self(path)
    }
}
impl Drop for Archive {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}
fn encoded(prefix: &[u8], payload: &[u8]) -> String {
    bs58::encode([prefix, payload].concat())
        .with_check()
        .into_string()
}
async fn session(auth: &AuthState) -> (String, HeaderMap) {
    let key = ed25519_dalek::SigningKey::from_bytes(&[53; 32]);
    let public_key = encoded(&[13, 15, 37, 217], key.verifying_key().as_bytes());
    let address = crypto::public_key_address(&public_key).unwrap();
    let mut headers = HeaderMap::new();
    headers.insert(header::ORIGIN, ORIGIN.parse().unwrap());
    let challenge = auth
        .challenge(
            &headers,
            SignInRequest {
                address: address.clone(),
                public_key,
            },
        )
        .unwrap();
    headers.insert(
        header::COOKIE,
        challenge.cookie.split(';').next().unwrap().parse().unwrap(),
    );
    headers.insert("x-csrf-token", challenge.view.csrf.parse().unwrap());
    let digest = <blake2::Blake2b<blake2::digest::consts::U32> as blake2::Digest>::digest(
        hex::decode(challenge.view.payload).unwrap(),
    );
    let signature = encoded(&[9, 245, 205, 134, 18], &key.sign(&digest).to_bytes());
    let grant = auth
        .verify(&headers, VerifyRequest { signature })
        .await
        .unwrap();
    headers.insert(
        header::COOKIE,
        grant.cookie.split(';').next().unwrap().parse().unwrap(),
    );
    headers.insert("x-csrf-token", grant.identity.csrf_token().parse().unwrap());
    (address, headers)
}
async fn request(
    app: axum::Router,
    path: &str,
    post: bool,
    headers: HeaderMap,
    body: Value,
) -> (StatusCode, HeaderMap, String) {
    let mut r = Request::builder()
        .method(if post { "POST" } else { "GET" })
        .uri(path)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    r.headers_mut().extend(headers);
    let response = app.oneshot(r).await.unwrap();
    let (parts, body) = response.into_parts();
    (
        parts.status,
        parts.headers,
        String::from_utf8(body.collect().await.unwrap().to_bytes().to_vec()).unwrap(),
    )
}
#[tokio::test]
async fn owner_page_and_prepare_require_current_session_origin_and_csrf() {
    let archive = Archive::new();
    let auth = AuthState::new(ORIGIN).unwrap();
    let app = router(
        AppState::new(
            archive.0.join("archive.sqlite"),
            Path::new(env!("CARGO_MANIFEST_DIR")).join("assets"),
        )
        .with_auth(auth.clone()),
    );
    let missing_client = app.clone();
    let app = app.layer(axum::Extension(seeded_archive::auth::TrustedClientIp(
        std::net::Ipv4Addr::LOCALHOST.into(),
    )));
    let (address, headers) = session(&auth).await;
    assert_eq!(
        request(
            missing_client,
            "/api/market/prepare",
            true,
            headers.clone(),
            json!({"actions":[]})
        )
        .await
        .0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    let own = format!("/profile/{address}/market");
    let (status, h, body) = request(app.clone(), &own, false, headers.clone(), Value::Null).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(h[header::CACHE_CONTROL], "no-store");
    assert!(body.contains("Listings and offers"));
    assert!(!body.contains("Index coverage"));
    assert!(!body.contains("no completed indexing pass"));
    let (status, headers, _) =
        request(app.clone(), &own, false, HeaderMap::new(), Value::Null).await;
    assert_eq!(status, StatusCode::SEE_OTHER);
    assert_eq!(headers[header::LOCATION], "/auth");
    let (_, headers) = session(&auth).await;
    assert_eq!(
        request(
            app.clone(),
            "/profile/tz1P719ymGCtNwavZKh2LmhMs53dGsEMzjnm/market",
            false,
            headers.clone(),
            Value::Null
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    let mut no_csrf = headers.clone();
    no_csrf.remove("x-csrf-token");
    assert_eq!(
        request(
            app.clone(),
            "/api/market/prepare",
            true,
            no_csrf,
            json!({"actions":[]})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    let mut bad_origin = headers.clone();
    bad_origin.insert(header::ORIGIN, "https://elsewhere.example".parse().unwrap());
    assert_eq!(
        request(
            app.clone(),
            "/api/market/prepare",
            true,
            bad_origin,
            json!({"actions":[]})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        request(
            app.clone(),
            "/api/market/prepare",
            true,
            headers.clone(),
            json!({"actions":[],"recipient":address})
        )
        .await
        .0,
        StatusCode::UNPROCESSABLE_ENTITY
    );
    auth.logout(&headers, false).unwrap();
    assert_eq!(
        request(app, &own, false, headers, Value::Null).await.0,
        StatusCode::SEE_OTHER
    );
}

#[tokio::test]
async fn json_auth_pages_expose_only_the_current_address() {
    let archive = Archive::new();
    let auth = AuthState::new(ORIGIN).unwrap();
    let app = router(
        AppState::new(
            archive.0.join("archive.sqlite"),
            Path::new(env!("CARGO_MANIFEST_DIR")).join("assets"),
        )
        .with_auth(auth.clone()),
    );
    let (address, mut headers) = session(&auth).await;
    headers.insert(header::ACCEPT, "application/json".parse().unwrap());

    let (status, _, body) =
        request(app.clone(), "/auth", false, headers.clone(), Value::Null).await;
    assert_eq!(status, StatusCode::OK);
    let auth_page: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(auth_page["kind"], "auth");
    assert_eq!(auth_page["data"]["address"], address);
    assert_eq!(auth_page["data"]["session"]["address"], address);

    let (status, _, body) = request(
        app.clone(),
        "/auth/profile",
        false,
        headers.clone(),
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let edit_page: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(edit_page["kind"], "profile_edit");
    assert_eq!(edit_page["data"]["address"], address);
    for key in ["proof", "signature", "public_key", "message"] {
        assert!(edit_page["data"].get(key).is_none());
    }

    let mut unauthenticated = HeaderMap::new();
    unauthenticated.insert(header::ACCEPT, "application/json".parse().unwrap());
    assert_eq!(
        request(app, "/auth/profile", false, unauthenticated, Value::Null)
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn json_market_page_paginates_and_preserves_token_selection() {
    const ARBOREAL: &str = "KT1EfsNuqwLAWDd3o4pvfUx1CAh5GMdTrRvr";
    const MARKET: &str = "KT1GbyoDi7H1sfXmimXpptZJuCdHMh66WS9u";

    let archive = Archive::new();
    let auth = AuthState::new(ORIGIN).unwrap();
    let app = router(
        AppState::new(
            archive.0.join("archive.sqlite"),
            Path::new(env!("CARGO_MANIFEST_DIR")).join("assets"),
        )
        .with_auth(auth.clone()),
    );
    let (address, mut headers) = session(&auth).await;
    headers.insert(header::ACCEPT, "application/json".parse().unwrap());

    let database = rusqlite::Connection::open(archive.0.join("archive.sqlite")).unwrap();
    for index in 0..51_u64 {
        database
            .execute(
                "INSERT INTO market_offers (marketplace, kind, offer_id, buyer, price, remaining, \
                 token_contract, token_id, created_level, level, active) \
                 VALUES (?1, 'token', ?2, ?3, 1000000, 1, ?4, 133459, ?5, ?5, 1)",
                rusqlite::params![
                    MARKET,
                    900_000_i64 + index as i64,
                    address,
                    ARBOREAL,
                    index as i64 + 1
                ],
            )
            .unwrap();
    }

    let path =
        format!("/profile/{address}/market?view=offers&token_contract={ARBOREAL}&token_id=133459");
    let mut unauthenticated = HeaderMap::new();
    unauthenticated.insert(header::ACCEPT, "application/json".parse().unwrap());
    assert_eq!(
        request(app.clone(), &path, false, unauthenticated, Value::Null)
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    let (other_address, mut other_headers) = session(&auth).await;
    assert_eq!(other_address, address);
    other_headers.insert(header::ACCEPT, "application/json".parse().unwrap());
    let other_path = format!(
        "/profile/tz1Vkoqo2ZQsVXBniYJDfX6F2NMfH5RoEuGn/market?view=offers&token_contract={ARBOREAL}&token_id=133459"
    );
    assert_eq!(
        request(app.clone(), &other_path, false, other_headers, Value::Null)
            .await
            .0,
        StatusCode::FORBIDDEN
    );

    let (status, response_headers, body) =
        request(app.clone(), &path, false, headers.clone(), Value::Null).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(response_headers[header::CACHE_CONTROL], "no-store");
    let page: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(page["kind"], "market");
    assert_eq!(page["data"]["items"].as_array().unwrap().len(), 50);
    assert_eq!(page["data"]["total"], 51);
    assert_eq!(page["data"]["items"][0]["contract"], ARBOREAL);
    assert_eq!(page["data"]["items"][0]["token_id"], 133459);
    // The own profile's tab counts listings, offers made and offers received.
    let mut html = headers.clone();
    html.remove(header::ACCEPT);
    for tab_page in [
        format!("/profile/{address}"),
        format!("/profile/{address}/market"),
    ] {
        let (status, _, body) =
            request(app.clone(), &tab_page, false, html.clone(), Value::Null).await;
        assert_eq!(status, StatusCode::OK, "{tab_page}");
        assert!(
            body.contains("Listings and offers <span>51</span>"),
            "{tab_page}"
        );
    }
    // Sub-tabs show their own counts, except zero; no second heading between the tab rows.
    // A collection offer shows its project's thumbnail, a token offer the NFT's.
    database
        .execute(
            "INSERT INTO market_offers (marketplace, kind, offer_id, buyer, price, remaining, \
             project_id, created_level, level, active) \
             VALUES (?1, 'collection', 990000, ?2, 1000000, 2, 29437, 100, 100, 1)",
            rusqlite::params![MARKET, address],
        )
        .unwrap();
    let (_, _, market) = request(
        app.clone(),
        &format!("/profile/{address}/market?view=offers"),
        false,
        html.clone(),
        Value::Null,
    )
    .await;
    assert!(market.contains(">Offers made <span>52</span></a>"));
    assert!(market.contains(">Listings</a>"));
    assert!(market.contains(">Offers received</a>"));
    assert!(!market.contains("<h2>Listings and offers</h2>"));
    assert!(market.contains(
        "<a class=\"market-thumb\" href=\"/project/29437\" tabindex=\"-1\" aria-hidden=\"true\"><img src=\"/fixtures/previews/arboreal/"
    ));
    assert!(market.contains(&format!(
        "<a class=\"market-thumb\" href=\"/token/{ARBOREAL}/133459\" tabindex=\"-1\" aria-hidden=\"true\"><img src=\"/fixtures/previews/arboreal/"
    )));
    database
        .execute("DELETE FROM market_offers WHERE offer_id = 990000", [])
        .unwrap();

    let first = page["data"]["pagination"]["first"].as_str().unwrap();
    let next = page["data"]["pagination"]["next"].as_str().unwrap();
    for url in [first, next] {
        assert!(url.starts_with(&format!("/profile/{address}/market?view=offers")));
        assert!(url.contains(&format!("token_contract={ARBOREAL}")));
        assert!(url.contains("token_id=133459"));
    }
    assert!(next.contains("&after="));

    let (status, _, body) = request(app, next, false, headers, Value::Null).await;
    assert_eq!(status, StatusCode::OK);
    let last_page: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(last_page["data"]["items"].as_array().unwrap().len(), 1);
    assert_eq!(last_page["data"]["total"], 51);
    assert!(last_page["data"]["pagination"]["next"].is_null());
}

#[tokio::test]
async fn received_offers_state_fee_and_disable_accepts_without_recipient() {
    use seeded_archive::trading::{
        TradingState,
        http::{Endpoint, Reply, Transport},
        types::{PlatformFee, TOKENS, V2},
    };
    struct NoNetwork;
    impl Transport for NoNetwork {
        fn request(&self, _: Endpoint, _: String, _: Option<Value>) -> Reply {
            panic!("Rendering the fee configuration must not query the chain")
        }
    }
    for enabled in [false, true] {
        let archive = Archive::new();
        let auth = AuthState::new(ORIGIN).unwrap();
        let (address, headers) = session(&auth).await;
        let db = rusqlite::Connection::open(archive.0.join("archive.sqlite")).unwrap();
        db.execute(
            "UPDATE tokens SET owner_address=?1, holder_address=?1 WHERE token_id=133459",
            [&address],
        )
        .unwrap();
        db.execute("INSERT INTO market_offers(marketplace,kind,offer_id,buyer,price,remaining,token_contract,token_id,created_level,level,active) VALUES(?1,'token',900000,?2,1000039,1,?3,133459,1,1,1)",rusqlite::params![V2,"tz1P719ymGCtNwavZKh2LmhMs53dGsEMzjnm",TOKENS[2]]).unwrap();
        db.execute("INSERT INTO market_offers(marketplace,kind,offer_id,buyer,price,remaining,project_id,created_level,level,active) VALUES(?1,'collection',900001,?2,1000000,1,29437,1,1,1)",rusqlite::params![V2,"tz1P719ymGCtNwavZKh2LmhMs53dGsEMzjnm"]).unwrap();
        let trading = TradingState::with_transport_and_fee(
            std::sync::Arc::new(NoNetwork),
            PlatformFee {
                bps: 250,
                address: enabled.then(|| "tz1gL9H53BzjDqmMi2JkKALeCzT7UAoopzEL".into()),
            },
        )
        .unwrap();
        let app = router(
            AppState::new(
                archive.0.join("archive.sqlite"),
                Path::new(env!("CARGO_MANIFEST_DIR")).join("assets"),
            )
            .with_auth(auth)
            .with_trading(std::sync::Arc::new(trading)),
        );
        let path = format!("/profile/{address}/market?view=received");
        let (status, _, body) =
            request(app.clone(), &path, false, headers.clone(), Value::Null).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("Platform fee: 2.5%"));
        assert_eq!(body.contains("disabled data-accept-disabled"), !enabled);
        if enabled {
            // The collection offer comes ready to accept with the token this address holds.
            let record = body
                .split("<article class=\"market-record\"")
                .find(|r| r.contains("Collection offer · ID 900001"))
                .unwrap();
            // Its NFT is a choice among the tokens of the project this address holds.
            assert!(record.contains(&format!(
                "<label>NFT<select name=\"token\"><option value=\"{0}/133459\" data-url=\"/token/{0}/133459\" data-thumb=\"/fixtures/previews/arboreal/",
                TOKENS[2]
            )));
            assert!(record.contains("\" selected>#"));
            // The record shows the selected NFT, linked to its page.
            assert!(record.contains(&format!(
                "<a class=\"market-thumb\" href=\"/token/{}/133459\" tabindex=\"-1\" aria-hidden=\"true\"><img src=\"/fixtures/previews/arboreal/",
                TOKENS[2]
            )));
            assert!(record.contains(" · ID 133459</option></select></label>"));
            assert!(!record.contains("name=\"token_id\""));
        }
        let mut json_headers = headers;
        json_headers.insert(header::ACCEPT, "application/json".parse().unwrap());
        let (_, _, body) = request(app, &path, false, json_headers, Value::Null).await;
        let page: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(page["data"]["accepts_enabled"], enabled);
        assert_eq!(page["data"]["platform_fee_bps"], 250);
    }
}

#[tokio::test]
async fn unavailable_fee_audit_disables_accepts_without_erasing_fee_configuration() {
    use seeded_archive::trading::{
        TradingState,
        http::{Endpoint, Reply, Transport},
        types::{PlatformFee, TOKENS, V2},
    };
    struct NoNetwork;
    impl Transport for NoNetwork {
        fn request(&self, _: Endpoint, _: String, _: Option<Value>) -> Reply {
            panic!("An unavailable fee audit must reject accepts before chain access")
        }
    }

    let archive = Archive::new();
    let auth = AuthState::new(ORIGIN).unwrap();
    let (address, headers) = session(&auth).await;
    let db = rusqlite::Connection::open(archive.0.join("archive.sqlite")).unwrap();
    db.execute(
        "UPDATE tokens SET owner_address=?1, holder_address=?1 WHERE token_id=133459",
        [&address],
    )
    .unwrap();
    db.execute(
        "INSERT INTO market_offers(marketplace,kind,offer_id,buyer,price,remaining,token_contract,token_id,created_level,level,active) VALUES(?1,'token',900003,?2,1000039,1,?3,133459,1,1,1)",
        rusqlite::params![V2, "tz1P719ymGCtNwavZKh2LmhMs53dGsEMzjnm", TOKENS[2]],
    )
    .unwrap();
    drop(db);

    let fee_address = "tz1gL9H53BzjDqmMi2JkKALeCzT7UAoopzEL";
    let trading = TradingState::with_transport_and_fee(
        std::sync::Arc::new(NoNetwork),
        PlatformFee {
            bps: 250,
            address: Some(fee_address.into()),
        },
    )
    .unwrap()
    .with_optional_fee_audit(&archive.0.join("missing-parent/fee.sqlite"));
    assert!(trading.fee_audit_unavailable());
    assert_eq!(trading.platform_fee().bps, 250);
    assert_eq!(trading.platform_fee().recipient().unwrap(), fee_address);

    let app = router(
        AppState::new(
            archive.0.join("archive.sqlite"),
            Path::new(env!("CARGO_MANIFEST_DIR")).join("assets"),
        )
        .with_auth(auth)
        .with_trading(std::sync::Arc::new(trading)),
    )
    .layer(axum::Extension(seeded_archive::auth::TrustedClientIp(
        std::net::Ipv4Addr::LOCALHOST.into(),
    )));
    let market_path = format!("/profile/{address}/market?view=received");
    let (status, _, html) = request(
        app.clone(),
        &market_path,
        false,
        headers.clone(),
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(html.contains("Platform fee: 2.5%"));
    assert!(html.contains("Accepting offers is unavailable right now (error 221)."));
    assert!(!html.contains("until a platform fee recipient is configured"));
    assert!(html.contains("disabled data-accept-disabled"));

    let mut json_headers = headers.clone();
    json_headers.insert(header::ACCEPT, "application/json".parse().unwrap());
    let (status, _, body) =
        request(app.clone(), &market_path, false, json_headers, Value::Null).await;
    assert_eq!(status, StatusCode::OK);
    let page: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(page["data"]["accepts_enabled"], false);
    assert_eq!(
        page["data"]["accepts_unavailable_reason"],
        "fee_audit_unavailable"
    );
    assert_eq!(page["data"]["platform_fee_bps"], 250);

    let (status, _, body) = request(
        app,
        "/api/market/prepare",
        true,
        headers,
        json!({"actions":[{"action":"accept","kind":"token","id":900003,"token":{"contract":TOKENS[2],"token_id":133459}}]}),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    let error: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(error["code"], "FeeAuditUnavailable");
    assert_eq!(error["number"], 221);
    assert_eq!(
        error["error"],
        "Accepting offers is unavailable right now (error 221)."
    );
}

#[tokio::test]
async fn market_browsing_hides_fee_and_disables_actions_when_trading_is_off() {
    use seeded_archive::trading::types::{TOKENS, V2};

    let archive = Archive::new();
    let auth = AuthState::new(ORIGIN).unwrap();
    let (address, headers) = session(&auth).await;
    let db = rusqlite::Connection::open(archive.0.join("archive.sqlite")).unwrap();
    db.execute(
        "UPDATE tokens SET owner_address=?1, holder_address=?1 WHERE token_id=133459",
        [&address],
    )
    .unwrap();
    db.execute(
        "INSERT INTO market_offers(marketplace,kind,offer_id,buyer,price,remaining,token_contract,token_id,created_level,level,active) VALUES(?1,'token',900001,?2,1000000,1,?3,133459,1,1,1)",
        rusqlite::params![V2, address, TOKENS[2]],
    )
    .unwrap();
    db.execute(
        "INSERT INTO escrow_listings(escrow,listing,contract,token_id,seller,level) VALUES(?1,'900002',?2,133459,?3,1)",
        rusqlite::params![V2, TOKENS[2], address],
    )
    .unwrap();
    drop(db);

    let app = router(
        AppState::new(
            archive.0.join("archive.sqlite"),
            Path::new(env!("CARGO_MANIFEST_DIR")).join("assets"),
        )
        .with_auth(auth),
    );
    let received_path = format!("/profile/{address}/market?view=received");
    let (status, _, body) = request(
        app.clone(),
        &received_path,
        false,
        headers.clone(),
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("Trading is unavailable right now."));
    assert!(!body.contains("Platform fee:"));
    assert!(!body.contains("fee recipient is configured"));
    assert!(body.contains("id=\"sign-market\" type=\"button\" disabled"));

    for view in ["offers", "listings"] {
        let path = format!("/profile/{address}/market?view={view}");
        let (status, _, body) =
            request(app.clone(), &path, false, headers.clone(), Value::Null).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("id=\"cancel-page\" type=\"button\" disabled"));
        assert!(body.contains("class=\"cancel-one\" type=\"button\" disabled"));
    }
}

#[tokio::test]
async fn submitted_operations_show_as_done_to_their_sender_until_indexed() {
    use seeded_archive::trading::{
        Overlay, RecordKey, Token, TradingState,
        http::{Endpoint, Reply, Transport},
        types::{Kind, PlatformFee, TOKENS, V2},
    };
    struct NoNetwork;
    impl Transport for NoNetwork {
        fn request(&self, _: Endpoint, _: String, _: Option<Value>) -> Reply {
            panic!("Showing submitted operations must not query the chain")
        }
    }
    let archive = Archive::new();
    let auth = AuthState::new(ORIGIN).unwrap();
    let (address, headers) = session(&auth).await;
    let db = rusqlite::Connection::open(archive.0.join("archive.sqlite")).unwrap();
    db.execute(
        "UPDATE tokens SET owner_address=?1, holder_address=?1 WHERE token_id=133459",
        [&address],
    )
    .unwrap();
    db.execute(
        "INSERT INTO escrow_listings (escrow, listing, contract, token_id, seller, level) \
         VALUES (?1, '555', ?2, 133459, ?3, 1)",
        rusqlite::params![V2, TOKENS[2], address],
    )
    .unwrap();
    let trading = std::sync::Arc::new(
        TradingState::with_transport_and_fee(
            std::sync::Arc::new(NoNetwork),
            PlatformFee {
                bps: 250,
                address: Some("tz1gL9H53BzjDqmMi2JkKALeCzT7UAoopzEL".into()),
            },
        )
        .unwrap(),
    );
    let app = router(
        AppState::new(
            archive.0.join("archive.sqlite"),
            Path::new(env!("CARGO_MANIFEST_DIR")).join("assets"),
        )
        .with_auth(auth)
        .with_trading(trading.clone()),
    );
    let listings = format!("/profile/{address}/market?view=listings");
    let collected = format!("/profile/{address}?view=collected");
    let token_link = format!("/token/{}/133459\"", TOKENS[2]);
    let (_, _, before) = request(app.clone(), &listings, false, headers.clone(), Value::Null).await;
    assert!(before.contains("ID 555"));
    // The listing's NFT is sold through an acceptance that also cancels the listing.
    let mut effects = Overlay::default();
    effects.records.insert(RecordKey {
        marketplace: V2.into(),
        kind: Kind::Listing,
        id: 555,
    });
    effects.tokens.insert(Token {
        contract: TOKENS[2].into(),
        token_id: 133459,
    });
    assert!(
        trading
            .record_submitted(
                &address,
                &"a".repeat(48),
                &format!("o{}", "a".repeat(50)),
                effects
            )
            .unwrap()
    );
    let (_, _, after) = request(app.clone(), &listings, false, headers.clone(), Value::Null).await;
    assert!(
        !after.contains("ID 555"),
        "the submitted cancellation shows at once"
    );
    assert!(
        after.contains(">Listings</a>"),
        "and the count drops to none"
    );
    assert!(after.contains("Listings and offers <span>0</span>"));
    let (_, _, own) = request(app.clone(), &collected, false, headers.clone(), Value::Null).await;
    assert!(
        !own.contains(&token_link),
        "the sold NFT leaves the owner's Collected tab"
    );
    assert!(own.contains("Collected <span>0</span>"));
    // Other visitors see the index until it has the operation.
    let (_, _, public) = request(
        app.clone(),
        &collected,
        false,
        HeaderMap::new(),
        Value::Null,
    )
    .await;
    assert!(public.contains(&token_link));
    // Once the index applies it, the overlay no longer changes anything.
    db.execute("DELETE FROM escrow_listings WHERE listing='555'", [])
        .unwrap();
    let (_, _, indexed) = request(app, &listings, false, headers, Value::Null).await;
    assert!(indexed.contains(">Listings</a>"));
}
