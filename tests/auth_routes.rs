use axum::{
    Router,
    body::Body,
    http::{HeaderMap, Request, StatusCode, header},
};
use blake2::{
    Blake2bVar,
    digest::{Update, VariableOutput},
};
use ed25519_dalek::Signer;
use http_body_util::BodyExt;
use seeded_archive::{
    auth::{AuthState, TrustedClientIp, crypto},
    import::import_fixtures,
    store::Store,
    web::{AppState, claims::ClaimRegistry, router},
};
use serde_json::{Value, json};
use std::{
    fs,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU8, AtomicU64, Ordering},
    },
};
use tower::ServiceExt;

const ORIGIN: &str = "https://seeded.art";
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Archive {
    directory: PathBuf,
    database: PathBuf,
    auth: AuthState,
    registry: ClaimRegistry,
    registry_mode: Arc<AtomicU8>,
    registry_calls: Arc<AtomicU64>,
    registry_release: Arc<tokio::sync::Notify>,
    registry_task: tokio::task::JoinHandle<()>,
}
impl Archive {
    fn new(import: bool) -> Self {
        let parent = Path::new(env!("CARGO_MANIFEST_DIR")).join("tmp/auth");
        fs::create_dir_all(&parent).unwrap();
        let directory = parent.join(format!(
            "routes-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&directory).unwrap();
        let database = directory.join("archive.sqlite");
        if import {
            let mut store = Store::open(&database).unwrap();
            import_fixtures(
                &mut store,
                Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("assets/fixtures")
                    .as_path(),
            )
            .unwrap();
            let connection = rusqlite::Connection::open(&database).unwrap();
            for (seed, order) in [(7, "MIN"), (8, "MAX")] {
                let address = crypto::public_key_address(&public_key(seed)).unwrap();
                connection.execute(&format!("UPDATE tokens SET minter=?1 WHERE token_id=(SELECT {order}(token_id) FROM tokens)"), [address]).unwrap();
            }
            connection.execute("INSERT INTO profile_sync (source,synced_level,updated_at) VALUES ('fxhash',100,'fixture')", []).unwrap();
        }
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let registry_mode = Arc::new(AtomicU8::new(0));
        let state = registry_mode.clone();
        let registry_calls = Arc::new(AtomicU64::new(0));
        let calls = registry_calls.clone();
        let registry_release = Arc::new(tokio::sync::Notify::new());
        let release = registry_release.clone();
        let registry_task = tokio::spawn(async move {
            let app = Router::new().fallback(axum::routing::get(move |uri: axum::http::Uri| {
                let state = state.clone();
                let calls = calls.clone();
                let release = release.clone();
                async move {
                    let call = calls.fetch_add(1, Ordering::SeqCst) + 1;
                    let mode = state.load(Ordering::SeqCst);
                    if mode == 11 {
                        state.store(12, Ordering::SeqCst);
                        release.notified().await;
                    }
                    if mode == 4 {
                        state.store(5, Ordering::SeqCst);
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    }
                    if mode == 3 { return (StatusCode::TOO_MANY_REQUESTS, axum::Json(json!({}))); }
                    if mode == 6 { return (StatusCode::OK, axum::Json(json!("x".repeat(70*1024)))); }
                    if mode == 7 { return (StatusCode::OK, axum::Json(json!(null))); }
                    let value = if uri.path() == "/v1/head" {
                        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap();
                        let stamp = httpdate::fmt_http_date(std::time::UNIX_EPOCH + now);
                        // Convert the standard HTTP date with an independent fixture formatter.
                        let parts: Vec<_> = stamp.split_whitespace().collect();
                        let month = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"].iter().position(|m| *m == parts[2]).unwrap()+1;
                        let timestamp = format!("{}-{month:02}-{}T{}Z", parts[3],parts[1],parts[4]);
                        json!({"chainId":if mode == 8 {"other"} else {"NetXdQprcVkpaWU"},"synced":mode != 2,"level":if mode == 9 {99} else if (mode == 17 && call >= 4) || (matches!(mode, 18 | 22 | 23) && call >= 3) {201} else {200},"hash":"fixture-head","timestamp":if mode == 10 {"2000-01-01T00:00:00Z"} else {&timestamp}})
                    } else if uri.path() == "/v1/blocks/200" {
                        json!({"hash": if mode == 23 {"reorganized"} else {"fixture-head"}})
                    } else {
                        assert_eq!(uri.path(), "/v1/contracts/KT1Ezht4PDKZri7aVppVGT4Jkw39sesaFnww/bigmaps/users_name/keys");
                        let query = uri.query().unwrap();
                        let fields: std::collections::HashMap<_, _> = query.split('&').map(|pair| pair.split_once('=').unwrap()).collect();
                        assert_eq!(fields["sort.asc"], "id");
                        assert_eq!(fields["select.fields"], "id,lastLevel,active,key,value");
                        let since: u64 = fields["lastLevel.gt"].parse().unwrap();
                        let after: u64 = fields["offset.cr"].parse().unwrap();
                        let limit: u64 = fields["limit"].parse().unwrap();
                        if mode == 24 { tokio::time::sleep(std::time::Duration::from_millis(1500)).await; }
                        if (mode == 20 && limit > 25) || mode == 21 {
                            assert_eq!(after, 0, "overflow must retry the same cursor");
                            return (StatusCode::OK, axum::Json(json!("x".repeat(70*1024))));
                        }
                        match mode {
                            1 => json!([{"lastLevel":101,"active":false}]),
                            13 => json!([{"id":1,"lastLevel":101,"active":true,"key":"tz1burnburnburnburnburnburnburjAYjjX","value":"46726573684e616d65"}]),
                            14 => json!([{"id":1,"lastLevel":101,"active":true,"key":"tz1burnburnburnburnburnburnburjAYjjX","value":"4f746865724e616d65"}]),
                            15 => json!([{"id":1,"lastLevel":101,"active":false,"key":"tz1burnburnburnburnburnburnburjAYjjX","value":"46726573684e616d65"}]),
                            16 | 17 if after == 0 && since == 100 => json!((1..=100).map(|id| json!({"id":id,"lastLevel":101,"active":true,"key":"tz1burnburnburnburnburnburnburjAYjjX","value":"4f746865724e616d65"})).collect::<Vec<_>>()),
                            16 => json!([{"id":101,"lastLevel":101,"active":true,"key":"tz1burnburnburnburnburnburnburjAYjjX","value":"46726573684e616d65"}]),
                            17 if since == 200 => json!([{"id":1,"lastLevel":201,"active":true,"key":"tz1burnburnburnburnburnburnburjAYjjX","value":"46726573684e616d65"}]),
                            17 => json!([]),
                            20 if after == 0 => json!((1..=25).map(|id| json!({"id":id,"lastLevel":101,"active":true,"key":"tz1burnburnburnburnburnburnburjAYjjX","value":"4f746865724e616d65"})).collect::<Vec<_>>()),
                            20 => json!([{"id":26,"lastLevel":101,"active":true,"key":"tz1burnburnburnburnburnburnburjAYjjX","value":"46726573684e616d65"}]),
                            22 if since == 200 => json!([{"id":1,"lastLevel":201,"active":false,"key":"tz1burnburnburnburnburnburnburjAYjjX","value":null}]),
                            23 if call >= 5 => json!([]),
                            22 | 23 => json!([{"id":1,"lastLevel":101,"active":true,"key":"tz1burnburnburnburnburnburnburjAYjjX","value":"46726573684e616d65"}]),
                            24 => json!(((after+1)..=(after+100).min(310)).map(|id| json!({"id":id,"lastLevel":101,"active":true,"key":"tz1burnburnburnburnburnburnburjAYjjX","value":"4f746865724e616d65"})).collect::<Vec<_>>()),
                            19 => json!([{"id":1,"lastLevel":101,"active":true,"key":"tz1burnburnburnburnburnburnburjAYjjX","value":"ff"}]),
                            _ => json!([]),
                        }
                    };
                    (StatusCode::OK, axum::Json(value))
                }
            }));
            axum::serve(tokio::net::TcpListener::from_std(listener).unwrap(), app)
                .await
                .unwrap();
        });
        Self {
            directory,
            database,
            auth: AuthState::new(ORIGIN).unwrap(),
            registry: ClaimRegistry::new(&base).unwrap(),
            registry_mode,
            registry_calls,
            registry_release,
            registry_task,
        }
    }
    fn app(&self) -> Router {
        router(
            AppState::new(
                &self.database,
                Path::new(env!("CARGO_MANIFEST_DIR")).join("assets"),
            )
            .with_auth(self.auth.clone())
            .with_claim_registry(self.registry.clone()),
        )
    }
}
impl Drop for Archive {
    fn drop(&mut self) {
        self.registry_task.abort();
        fs::remove_dir_all(&self.directory).unwrap();
    }
}

#[tokio::test]
async fn live_claim_check_rejects_oversized_malformed_wrong_chain_behind_and_stale_heads() {
    let archive = Archive::new(true);
    for mode in [6, 7, 8, 9, 10] {
        archive.registry_mode.store(mode, Ordering::SeqCst);
        assert!(
            archive
                .registry
                .check(100, "tz1caller", "freshname")
                .await
                .is_err(),
            "mode {mode}"
        );
    }
    archive.registry_mode.store(0, Ordering::SeqCst);
    assert!(
        archive
            .registry
            .check(100, "tz1caller", "freshname")
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn new_name_claims_fail_closed_on_registry_updates_lag_and_outage_then_retry() {
    let archive = Archive::new(true);
    let (cookie, csrf, address, _) = login(&archive, 7).await;
    for mode in [1, 2, 3] {
        archive.registry_mode.store(mode, Ordering::SeqCst);
        let response = request(
            archive.app(),
            "/api/auth/profile",
            "POST",
            Some(&cookie),
            Some(&csrf),
            Some(ORIGIN),
            json!({"name":"FreshName"}),
        )
        .await;
        assert_eq!(response.status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers[header::RETRY_AFTER], "60");
        assert!(
            Store::open_read_only(&archive.database)
                .unwrap()
                .profile_edit(&address)
                .unwrap()
                .is_none()
        );
    }
    archive.registry_mode.store(0, Ordering::SeqCst);
    assert_eq!(
        request(
            archive.app(),
            "/api/auth/profile",
            "POST",
            Some(&cookie),
            Some(&csrf),
            Some(ORIGIN),
            json!({"name":"FreshName"})
        )
        .await
        .status,
        StatusCode::OK
    );
    archive.registry_mode.store(3, Ordering::SeqCst);
    assert_eq!(
        request(
            archive.app(),
            "/api/auth/profile",
            "POST",
            Some(&cookie),
            Some(&csrf),
            Some(ORIGIN),
            json!({"name":"freshname","bio":"Updated during outage"})
        )
        .await
        .status,
        StatusCode::OK
    );
}

#[tokio::test]
async fn removing_the_database_during_a_registry_wait_does_not_recreate_it() {
    let archive = Archive::new(true);
    let (cookie, csrf, _, _) = login(&archive, 7).await;
    archive.registry_mode.store(11, Ordering::SeqCst);
    let app = archive.app();
    let edit = tokio::spawn(async move {
        request(
            app,
            "/api/auth/profile",
            "POST",
            Some(&cookie),
            Some(&csrf),
            Some(ORIGIN),
            json!({"name":"FreshName"}),
        )
        .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while archive.registry_mode.load(Ordering::SeqCst) != 12 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    fs::remove_file(&archive.database).unwrap();
    archive.registry_release.notify_one();
    assert_eq!(edit.await.unwrap().status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(!archive.database.exists());
}

#[tokio::test]
async fn profile_edit_does_not_migrate_a_stale_database_during_a_registry_wait() {
    let archive = Archive::new(true);
    let (cookie, csrf, _, _) = login(&archive, 7).await;
    archive.registry_mode.store(11, Ordering::SeqCst);
    let app = archive.app();
    let edit = tokio::spawn(async move {
        request(
            app,
            "/api/auth/profile",
            "POST",
            Some(&cookie),
            Some(&csrf),
            Some(ORIGIN),
            json!({"name":"FreshName"}),
        )
        .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while archive.registry_mode.load(Ordering::SeqCst) != 12 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    rusqlite::Connection::open(&archive.database)
        .unwrap()
        .execute_batch("PRAGMA user_version=8")
        .unwrap();
    archive.registry_release.notify_one();
    assert_eq!(edit.await.unwrap().status, StatusCode::SERVICE_UNAVAILABLE);
    let version: i64 = rusqlite::Connection::open(&archive.database)
        .unwrap()
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .unwrap();
    assert_eq!(version, 8);
}

#[tokio::test]
async fn changed_registry_keys_only_block_a_matching_active_name() {
    for (mode, expected) in [
        (13, StatusCode::BAD_REQUEST),
        (14, StatusCode::OK),
        (15, StatusCode::OK),
        (16, StatusCode::BAD_REQUEST),
        (17, StatusCode::BAD_REQUEST),
        (18, StatusCode::OK),
        (19, StatusCode::OK),
        (20, StatusCode::BAD_REQUEST),
        (22, StatusCode::OK),
        (23, StatusCode::OK),
    ] {
        let archive = Archive::new(true);
        archive.registry_mode.store(mode, Ordering::SeqCst);
        let (cookie, csrf, _, _) = login(&archive, 7).await;
        let response = request(
            archive.app(),
            "/api/auth/profile",
            "POST",
            Some(&cookie),
            Some(&csrf),
            Some(ORIGIN),
            json!({"name":"FreshName"}),
        )
        .await;
        assert_eq!(response.status, expected, "registry mode {mode}");
    }
}

#[tokio::test]
async fn missing_registry_baseline_blocks_only_new_name_claims() {
    let archive = Archive::new(true);
    rusqlite::Connection::open(&archive.database)
        .unwrap()
        .execute("DELETE FROM profile_sync", [])
        .unwrap();
    let (cookie, csrf, address, _) = login(&archive, 7).await;
    assert_eq!(
        request(
            archive.app(),
            "/api/auth/profile",
            "POST",
            Some(&cookie),
            Some(&csrf),
            Some(ORIGIN),
            json!({"name":"FreshName"})
        )
        .await
        .status,
        StatusCode::SERVICE_UNAVAILABLE
    );
    archive.registry_mode.store(3, Ordering::SeqCst);
    assert_eq!(
        request(
            archive.app(),
            "/api/auth/profile",
            "POST",
            Some(&cookie),
            Some(&csrf),
            Some(ORIGIN),
            json!({"bio":"Bio only"})
        )
        .await
        .status,
        StatusCode::OK
    );
    assert_eq!(
        Store::open_read_only(&archive.database)
            .unwrap()
            .profile_edit(&address)
            .unwrap()
            .unwrap()
            .bio
            .as_deref(),
        Some("Bio only")
    );
}

#[tokio::test]
async fn logout_during_live_registry_check_prevents_the_late_profile_write() {
    let archive = Archive::new(true);
    let (cookie, csrf, address, _) = login(&archive, 7).await;
    archive.registry_mode.store(4, Ordering::SeqCst);
    let app = archive.app();
    let edit_cookie = cookie.clone();
    let edit_csrf = csrf.clone();
    let edit = tokio::spawn(async move {
        request(
            app,
            "/api/auth/profile",
            "POST",
            Some(&edit_cookie),
            Some(&edit_csrf),
            Some(ORIGIN),
            json!({"name":"LateName"}),
        )
        .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while archive.registry_mode.load(Ordering::SeqCst) != 5 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        request(
            archive.app(),
            "/api/auth/logout",
            "POST",
            Some(&cookie),
            Some(&csrf),
            Some(ORIGIN),
            json!({})
        )
        .await
        .status,
        StatusCode::NO_CONTENT
    );
    assert_eq!(edit.await.unwrap().status, StatusCode::BAD_REQUEST);
    assert!(
        Store::open_read_only(&archive.database)
            .unwrap()
            .profile_edit(&address)
            .unwrap()
            .is_none()
    );
}
struct Reply {
    status: StatusCode,
    headers: HeaderMap,
    body: Vec<u8>,
}
impl Reply {
    fn json(&self) -> Value {
        serde_json::from_slice(&self.body).unwrap()
    }
}
async fn request(
    app: Router,
    path: &str,
    method: &str,
    cookies: Option<&str>,
    csrf: Option<&str>,
    origin: Option<&str>,
    body: Value,
) -> Reply {
    let mut request = Request::builder()
        .uri(path)
        .method(method)
        .header(header::CONTENT_TYPE, "application/json")
        .extension(TrustedClientIp(std::net::Ipv4Addr::LOCALHOST.into()));
    if let Some(cookies) = cookies {
        request = request.header(header::COOKIE, cookies);
    }
    if let Some(csrf) = csrf {
        request = request.header("x-csrf-token", csrf);
    }
    if let Some(origin) = origin {
        request = request.header(header::ORIGIN, origin);
    }
    let response = app
        .oneshot(request.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let (parts, body) = response.into_parts();
    Reply {
        status: parts.status,
        headers: parts.headers,
        body: body.collect().await.unwrap().to_bytes().to_vec(),
    }
}
fn encoded(prefix: &[u8], bytes: &[u8]) -> String {
    bs58::encode([prefix, bytes].concat())
        .with_check()
        .into_string()
}
fn public_key(seed: u8) -> String {
    encoded(
        &[13, 15, 37, 217],
        ed25519_dalek::SigningKey::from_bytes(&[seed; 32])
            .verifying_key()
            .as_bytes(),
    )
}
fn signature(seed: u8, payload: &str) -> String {
    let mut hasher = Blake2bVar::new(32).unwrap();
    hasher.update(&hex::decode(payload).unwrap());
    let mut digest = [0; 32];
    hasher.finalize_variable(&mut digest).unwrap();
    encoded(
        &[9, 245, 205, 134, 18],
        &ed25519_dalek::SigningKey::from_bytes(&[seed; 32])
            .sign(&digest)
            .to_bytes(),
    )
}
fn cookie(reply: &Reply) -> String {
    reply
        .headers
        .get_all(header::SET_COOKIE)
        .iter()
        .find(|value| value.to_str().unwrap().contains("session="))
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .into()
}

#[tokio::test]
async fn generated_name_claims_return_a_clear_conflict_without_changing_the_profile() {
    let archive = Archive::new(true);
    let plain = "tz1gBXG9fg8RMDH69KfKqwoTH5sFDmzt5yzm";
    let numbered = "tz2RXosjAZdS85oCrguy7Eom757144dcU1FT";
    let mut store = Store::open_existing_write(&archive.database).unwrap();
    let sql = rusqlite::Connection::open(&archive.database).unwrap();
    sql.execute("UPDATE artists SET address=?1 WHERE id='zancan'", [plain])
        .unwrap();
    for address in [plain, numbered] {
        sql.execute(
            "INSERT INTO users(address,name,name_key) VALUES (?1,'zancan','zancan') \
             ON CONFLICT(address) DO UPDATE SET name=excluded.name,name_key=excluded.name_key",
            [address],
        )
        .unwrap();
    }
    drop(sql);
    store.assign_name_numbers().unwrap();
    drop(store);
    let (cookie, csrf, address, _) = login(&archive, 7).await;
    for name in ["zancan-2", "ZANCAN-2"] {
        let reply = request(
            archive.app(),
            "/api/auth/profile",
            "POST",
            Some(&cookie),
            Some(&csrf),
            Some(ORIGIN),
            json!({"name":name}),
        )
        .await;
        assert_eq!(reply.status, StatusCode::BAD_REQUEST);
        assert_eq!(reply.json()["error"], "That name is taken.");
    }
    let store = Store::open_read_only(&archive.database).unwrap();
    assert!(store.profile_edit(&address).unwrap().is_none());
    assert_eq!(store.profile_by_name("zancan-2").unwrap().address, numbered);
    assert_eq!(archive.registry_calls.load(Ordering::SeqCst), 0);
}

async fn login(archive: &Archive, seed: u8) -> (String, String, String, String) {
    let public_key = public_key(seed);
    let address = crypto::public_key_address(&public_key).unwrap();
    let challenge = request(
        archive.app(),
        "/api/auth/challenge",
        "POST",
        None,
        None,
        Some(ORIGIN),
        json!({"address":address,"public_key":public_key}),
    )
    .await;
    assert_eq!(challenge.status, StatusCode::OK);
    let challenge_cookie = challenge.headers[header::SET_COOKIE]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap();
    let value = challenge.json();
    let verify = request(
        archive.app(),
        "/api/auth/verify",
        "POST",
        Some(challenge_cookie),
        value["csrf"].as_str(),
        Some(ORIGIN),
        json!({"signature":signature(seed,value["payload"].as_str().unwrap())}),
    )
    .await;
    assert_eq!(verify.status, StatusCode::OK);
    (
        cookie(&verify),
        verify.json()["session"]["csrf"].as_str().unwrap().into(),
        address,
        value["message"].as_str().unwrap().into(),
    )
}

#[tokio::test]
async fn sign_in_cookie_session_logout_and_body_limits_are_enforced() {
    let archive = Archive::new(true);
    let empty = request(
        archive.app(),
        "/api/auth/session",
        "GET",
        None,
        None,
        None,
        json!(null),
    )
    .await;
    assert_eq!(empty.json()["session"], Value::Null);
    assert_eq!(empty.headers[header::CACHE_CONTROL], "no-store");
    let (cookie, csrf, address, _) = login(&archive, 7).await;
    let session = request(
        archive.app(),
        "/api/auth/session",
        "GET",
        Some(&cookie),
        None,
        None,
        json!(null),
    )
    .await;
    assert_eq!(session.json()["session"]["address"], address);
    let bad = request(
        archive.app(),
        "/api/auth/verify",
        "POST",
        None,
        None,
        Some(ORIGIN),
        json!({"signature":"a".repeat(17000)}),
    )
    .await;
    assert_eq!(bad.status, StatusCode::PAYLOAD_TOO_LARGE);
    let denied = request(
        archive.app(),
        "/api/auth/logout",
        "GET",
        Some(&cookie),
        Some(&csrf),
        Some(ORIGIN),
        json!({}),
    )
    .await;
    assert_eq!(denied.status, StatusCode::METHOD_NOT_ALLOWED);
    let out = request(
        archive.app(),
        "/api/auth/logout",
        "POST",
        Some(&cookie),
        Some(&csrf),
        Some(ORIGIN),
        json!({"all":true}),
    )
    .await;
    assert_eq!(out.status, StatusCode::NO_CONTENT);
    assert!(
        out.headers[header::SET_COOKIE]
            .to_str()
            .unwrap()
            .contains("Max-Age=0")
    );
    assert_eq!(
        request(
            archive.app(),
            "/api/auth/session",
            "GET",
            Some(&cookie),
            None,
            None,
            json!(null)
        )
        .await
        .json()["session"],
        Value::Null
    );
}

#[tokio::test]
async fn streamed_logout_waits_for_complete_body_before_mutating_session() {
    struct StreamedBody(std::collections::VecDeque<Result<bytes::Bytes, std::io::Error>>);
    impl http_body::Body for StreamedBody {
        type Data = bytes::Bytes;
        type Error = std::io::Error;

        fn poll_frame(
            mut self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
            std::task::Poll::Ready(
                self.0
                    .pop_front()
                    .map(|part| part.map(http_body::Frame::data)),
            )
        }
    }

    let archive = Archive::new(true);
    let (cookie, csrf, address, _) = login(&archive, 7).await;
    for (tail, expected) in [
        (
            Ok(bytes::Bytes::from(vec![b' '; 17_000])),
            StatusCode::PAYLOAD_TOO_LARGE,
        ),
        (
            Err(std::io::Error::other("truncated request")),
            StatusCode::BAD_REQUEST,
        ),
    ] {
        let body = StreamedBody([Ok(bytes::Bytes::from_static(br#"{"all":true}"#)), tail].into());
        let response = archive
            .app()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/auth/logout")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::ORIGIN, ORIGIN)
                    .header(header::COOKIE, &cookie)
                    .header("x-csrf-token", &csrf)
                    .extension(TrustedClientIp(std::net::Ipv4Addr::LOCALHOST.into()))
                    .body(Body::new(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), expected);
        let session = request(
            archive.app(),
            "/api/auth/session",
            "GET",
            Some(&cookie),
            None,
            None,
            json!(null),
        )
        .await;
        assert_eq!(session.json()["session"]["address"], address);
    }
}

#[tokio::test]
async fn profile_edits_require_current_session_csrf_and_exact_origin() {
    let archive = Archive::new(true);
    let (cookie, csrf, address, _) = login(&archive, 7).await;
    let edit = json!({"name":"SeededOwner","bio":"Local bio","links":["https://example.org"]});
    for (cookie, csrf, origin, status) in [
        (None, None, Some(ORIGIN), StatusCode::UNAUTHORIZED),
        (
            Some(cookie.as_str()),
            None,
            Some(ORIGIN),
            StatusCode::FORBIDDEN,
        ),
        (
            Some(cookie.as_str()),
            Some(csrf.as_str()),
            Some("https://content.seeded.art"),
            StatusCode::FORBIDDEN,
        ),
        (
            Some(cookie.as_str()),
            Some(csrf.as_str()),
            None,
            StatusCode::FORBIDDEN,
        ),
    ] {
        assert_eq!(
            request(
                archive.app(),
                "/api/auth/profile",
                "POST",
                cookie,
                csrf,
                origin,
                edit.clone()
            )
            .await
            .status,
            status
        );
    }
    assert!(
        Store::open_read_only(&archive.database)
            .unwrap()
            .profile_edit(&address)
            .unwrap()
            .is_none()
    );
    let response = request(
        archive.app(),
        "/api/auth/profile",
        "POST",
        Some(&cookie),
        Some(&csrf),
        Some(ORIGIN),
        edit.clone(),
    )
    .await;
    assert_eq!(response.status, StatusCode::OK);
    let store = Store::open_read_only(&archive.database).unwrap();
    let profile = store.profile(&address).unwrap();
    assert_eq!(profile.name.as_deref(), Some("SeededOwner"));
    assert_eq!(profile.bio.as_deref(), Some("Local bio"));
    drop(store);
    let mut injected = edit.clone();
    injected["address"] = json!("tz1YJoT4wT65hm9yrrCQU7Vr2CtzTMhBmfQR");
    assert_eq!(
        request(
            archive.app(),
            "/api/auth/profile",
            "POST",
            Some(&cookie),
            Some(&csrf),
            Some(ORIGIN),
            injected
        )
        .await
        .status,
        StatusCode::UNPROCESSABLE_ENTITY
    );
    assert_eq!(
        request(
            archive.app(),
            "/api/auth/logout",
            "POST",
            Some(&cookie),
            Some(&csrf),
            Some(ORIGIN),
            json!({})
        )
        .await
        .status,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        request(
            archive.app(),
            "/api/auth/profile",
            "POST",
            Some(&cookie),
            Some(&csrf),
            Some(ORIGIN),
            edit
        )
        .await
        .status,
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn editing_keeps_sign_in_audit_proof_and_persists_across_auth_restart() {
    let archive = Archive::new(true);
    let (cookie, csrf, address, message) = login(&archive, 7).await;
    let edit = json!({"name":"LocalIdentity","bio":"<script>alert(1)</script>","links":["https://example.org/?q=%3Cscript%3E"]});
    assert_eq!(
        request(
            archive.app(),
            "/api/auth/profile",
            "POST",
            Some(&cookie),
            Some(&csrf),
            Some(ORIGIN),
            edit
        )
        .await
        .status,
        StatusCode::OK
    );
    let connection = rusqlite::Connection::open(&archive.database).unwrap();
    let signed: String = connection
        .query_row(
            "SELECT signed_message FROM profile_edits WHERE address=?1",
            [&address],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(signed, message);
    assert!(!signed.contains("LocalIdentity"));
    let form = request(
        archive.app(),
        "/auth/profile",
        "GET",
        Some(&cookie),
        None,
        None,
        json!(null),
    )
    .await;
    assert_eq!(form.status, StatusCode::OK);
    assert!(
        !String::from_utf8(form.body)
            .unwrap()
            .contains("<script>alert(1)</script>")
    );
    let cleared = json!({"name":"LocalIdentity","bio":"","links":[]});
    assert_eq!(
        request(
            archive.app(),
            "/api/auth/profile",
            "POST",
            Some(&cookie),
            Some(&csrf),
            Some(ORIGIN),
            cleared
        )
        .await
        .status,
        StatusCode::OK
    );
    assert_eq!(
        Store::open_read_only(&archive.database)
            .unwrap()
            .profile_edit(&address)
            .unwrap()
            .unwrap()
            .bio
            .as_deref(),
        Some("")
    );
    let restarted = router(
        AppState::new(
            &archive.database,
            Path::new(env!("CARGO_MANIFEST_DIR")).join("assets"),
        )
        .with_auth(AuthState::new(ORIGIN).unwrap()),
    );
    assert_eq!(
        request(
            restarted,
            "/auth/profile",
            "GET",
            Some(&cookie),
            None,
            None,
            json!(null)
        )
        .await
        .status,
        StatusCode::SEE_OTHER
    );
    assert_eq!(
        Store::open_read_only(&archive.database)
            .unwrap()
            .profile(&address)
            .unwrap()
            .name
            .as_deref(),
        Some("LocalIdentity")
    );
}

#[tokio::test]
async fn case_insensitive_name_conflicts_and_unsafe_links_fail_without_overwrite() {
    let archive = Archive::new(true);
    let (a_cookie, a_csrf, _, _) = login(&archive, 7).await;
    let (b_cookie, b_csrf, b_address, _) = login(&archive, 8).await;
    assert_eq!(
        request(
            archive.app(),
            "/api/auth/profile",
            "POST",
            Some(&a_cookie),
            Some(&a_csrf),
            Some(ORIGIN),
            json!({"name":"UniqueName","links":[]})
        )
        .await
        .status,
        StatusCode::OK
    );
    for edit in [
        json!({"name":"uniquename","links":[]}),
        json!({"bio":"safe","links":["javascript:alert(1)"]}),
    ] {
        assert_eq!(
            request(
                archive.app(),
                "/api/auth/profile",
                "POST",
                Some(&b_cookie),
                Some(&b_csrf),
                Some(ORIGIN),
                edit
            )
            .await
            .status,
            StatusCode::BAD_REQUEST
        );
    }
    assert!(
        Store::open_read_only(&archive.database)
            .unwrap()
            .profile_edit(&b_address)
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn missing_database_is_not_created_by_an_authenticated_edit() {
    let archive = Archive::new(false);
    let (cookie, csrf, _, _) = login(&archive, 7).await;
    assert_eq!(
        request(
            archive.app(),
            "/api/auth/profile",
            "POST",
            Some(&cookie),
            Some(&csrf),
            Some(ORIGIN),
            json!({"name":"NewName","links":[]})
        )
        .await
        .status,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert!(!archive.database.exists());
}

#[tokio::test]
async fn a_busy_database_returns_retryable_failure_and_recovers_without_lost_profile() {
    let archive = Archive::new(true);
    let (cookie, csrf, address, _) = login(&archive, 7).await;
    let connection = rusqlite::Connection::open(&archive.database).unwrap();
    connection.execute_batch("BEGIN IMMEDIATE").unwrap();
    let edit = json!({"name":"AfterRetry","links":[]});
    let response = request(
        archive.app(),
        "/api/auth/profile",
        "POST",
        Some(&cookie),
        Some(&csrf),
        Some(ORIGIN),
        edit.clone(),
    )
    .await;
    assert_eq!(response.status, StatusCode::SERVICE_UNAVAILABLE);
    connection.execute_batch("ROLLBACK").unwrap();
    assert!(
        Store::open_read_only(&archive.database)
            .unwrap()
            .profile_edit(&address)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        request(
            archive.app(),
            "/api/auth/profile",
            "POST",
            Some(&cookie),
            Some(&csrf),
            Some(ORIGIN),
            edit
        )
        .await
        .status,
        StatusCode::OK
    );
}

#[tokio::test]
async fn busy_profile_writer_keeps_its_wait_queue_bounded_and_recovers() {
    let archive = Archive::new(true);
    let identities = (7..17)
        .map(|seed| {
            let archive = &archive;
            async move { login(archive, seed).await }
        })
        .collect::<Vec<_>>();
    let mut sessions = Vec::new();
    for identity in identities {
        sessions.push(identity.await);
    }
    let connection = rusqlite::Connection::open(&archive.database).unwrap();
    connection.execute_batch("BEGIN IMMEDIATE").unwrap();
    let mut requests = tokio::task::JoinSet::new();
    for (cookie, csrf, _, _) in &sessions {
        let app = archive.app();
        let cookie = cookie.clone();
        let csrf = csrf.clone();
        requests.spawn(async move {
            request(
                app,
                "/api/auth/profile",
                "POST",
                Some(&cookie),
                Some(&csrf),
                Some(ORIGIN),
                json!({"bio":"busy writer fixture"}),
            )
            .await
            .status
        });
    }
    let mut unavailable = 0;
    let mut rate_limited = 0;
    while let Some(result) = requests.join_next().await {
        match result.unwrap() {
            StatusCode::SERVICE_UNAVAILABLE => unavailable += 1,
            StatusCode::TOO_MANY_REQUESTS => rate_limited += 1,
            status => panic!("unexpected profile response {status}"),
        }
    }
    assert_eq!(unavailable, 1);
    assert_eq!(rate_limited, 9);
    connection.execute_batch("ROLLBACK").unwrap();
    let (cookie, csrf, _, _) = &sessions[0];
    assert_eq!(
        request(
            archive.app(),
            "/api/auth/profile",
            "POST",
            Some(cookie),
            Some(csrf),
            Some(ORIGIN),
            json!({"bio":"recovered"}),
        )
        .await
        .status,
        StatusCode::OK
    );
}

#[tokio::test]
async fn public_forwarding_headers_cannot_supply_missing_trusted_client_identity() {
    let archive = Archive::new(true);
    let body = json!({"address":crypto::public_key_address(&public_key(7)).unwrap(),"public_key":public_key(7)});
    let response = archive
        .app()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/auth/challenge")
                .header(header::ORIGIN, ORIGIN)
                .header(header::CONTENT_TYPE, "application/json")
                .header("x-forwarded-for", "192.0.2.1")
                .header("forwarded", "for=192.0.2.1")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(!response.headers().contains_key(header::SET_COOKIE));
    login(&archive, 7).await;
}

#[tokio::test]
async fn edit_throttle_rejects_before_writing_and_other_addresses_can_still_edit() {
    let archive = Archive::new(true);
    let (cookie, csrf, address, _) = login(&archive, 7).await;
    for n in 0..6 {
        assert_eq!(
            request(
                archive.app(),
                "/api/auth/profile",
                "POST",
                Some(&cookie),
                Some(&csrf),
                Some(ORIGIN),
                json!({"bio":format!("Saved {n}")})
            )
            .await
            .status,
            StatusCode::OK
        );
    }
    let rejected = request(
        archive.app(),
        "/api/auth/profile",
        "POST",
        Some(&cookie),
        Some(&csrf),
        Some(ORIGIN),
        json!({"bio":"Must not persist"}),
    )
    .await;
    assert_eq!(rejected.status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(rejected.headers[header::RETRY_AFTER], "60");
    assert_eq!(
        Store::open_read_only(&archive.database)
            .unwrap()
            .profile_edit(&address)
            .unwrap()
            .unwrap()
            .bio
            .as_deref(),
        Some("Saved 5")
    );
    let (other_cookie, other_csrf, _, _) = login(&archive, 8).await;
    assert_eq!(
        request(
            archive.app(),
            "/api/auth/profile",
            "POST",
            Some(&other_cookie),
            Some(&other_csrf),
            Some(ORIGIN),
            json!({"bio":"Other address"})
        )
        .await
        .status,
        StatusCode::OK
    );
}

#[tokio::test]
async fn imported_values_prefill_editable_fields_and_unchanged_saves_follow_later_imports() {
    let archive = Archive::new(true);
    let (cookie, csrf, address, _) = login(&archive, 7).await;
    let connection = rusqlite::Connection::open(&archive.database).unwrap();
    connection.execute(r#"INSERT INTO profiles_imported (address,source,name,name_key,bio,links,fetched_at) VALUES (?1,'fxhash','ImportedName','importedname','Imported <bio>','["https://example.org"]','2026-10-05')"#, [&address]).unwrap();
    let form = request(
        archive.app(),
        "/auth/profile",
        "GET",
        Some(&cookie),
        None,
        None,
        json!(null),
    )
    .await;
    assert_eq!(form.status, StatusCode::OK);
    let html = String::from_utf8(form.body).unwrap();
    // Imported values are real, selectable field values, not placeholders.
    let form_html = &html[html.find("<form data-auth-profile-form").unwrap()..];
    assert!(!form_html[..form_html.find("</form>").unwrap()].contains("placeholder="));
    assert!(html.contains("value=\"ImportedName\" data-imported=\"ImportedName\""));
    assert!(html.contains("data-imported=\"Imported &#60;bio&#62;\" rows=\"7\" maxlength=\"1000\">Imported &#60;bio&#62;</textarea>"));
    assert!(html.contains("maxlength=\"1504\">https://example.org</textarea>"));
    assert!(!html.contains("name=\"hide_bio\" checked"));
    assert_eq!(
        request(
            archive.app(),
            "/api/auth/profile",
            "POST",
            Some(&cookie),
            Some(&csrf),
            Some(ORIGIN),
            json!({"name":null,"bio":null,"links":[]})
        )
        .await
        .status,
        StatusCode::OK
    );
    let edit = Store::open_read_only(&archive.database)
        .unwrap()
        .profile_edit(&address)
        .unwrap()
        .unwrap();
    assert_eq!(edit, Default::default());
    connection
        .execute(
            "UPDATE profiles_imported SET bio='New imported bio' WHERE address=?1",
            [&address],
        )
        .unwrap();
    assert_eq!(
        Store::open_read_only(&archive.database)
            .unwrap()
            .profile(&address)
            .unwrap()
            .bio
            .as_deref(),
        Some("New imported bio")
    );
    assert_eq!(
        request(
            archive.app(),
            "/api/auth/profile",
            "POST",
            Some(&cookie),
            Some(&csrf),
            Some(ORIGIN),
            json!({"bio":""})
        )
        .await
        .status,
        StatusCode::OK
    );
    let form = request(
        archive.app(),
        "/auth/profile",
        "GET",
        Some(&cookie),
        None,
        None,
        json!(null),
    )
    .await;
    assert!(
        String::from_utf8(form.body)
            .unwrap()
            .contains("name=\"hide_bio\" checked")
    );
}

#[tokio::test]
async fn passing_claim_checks_coalesce_and_recheck_when_the_import_level_changes() {
    let archive = Archive::new(true);
    let mut work = tokio::task::JoinSet::new();
    for _ in 0..16 {
        let registry = archive.registry.clone();
        work.spawn(async move { registry.check(100, "tz1caller", "freshname").await });
    }
    while let Some(result) = work.join_next().await {
        result.unwrap().unwrap();
    }
    for _ in 0..128 {
        archive
            .registry
            .check(100, "tz1caller", "freshname")
            .await
            .unwrap();
    }
    assert_eq!(archive.registry_calls.load(Ordering::SeqCst), 3);
    archive
        .registry
        .check(101, "tz1caller", "freshname")
        .await
        .unwrap();
    assert_eq!(archive.registry_calls.load(Ordering::SeqCst), 5);
}

#[tokio::test]
async fn delayed_claim_checks_leave_profile_writes_free_and_reject_a_changed_baseline() {
    let archive = Archive::new(true);
    let (cookie, csrf, address, _) = login(&archive, 7).await;
    let (bio_cookie, bio_csrf, _, _) = login(&archive, 8).await;
    archive.registry_mode.store(11, Ordering::SeqCst);
    let app = archive.app();
    let edit = tokio::spawn(async move {
        request(
            app,
            "/api/auth/profile",
            "POST",
            Some(&cookie),
            Some(&csrf),
            Some(ORIGIN),
            json!({"name":"AfterImport"}),
        )
        .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while archive.registry_mode.load(Ordering::SeqCst) != 12 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let bio = tokio::time::timeout(
        std::time::Duration::from_millis(250),
        request(
            archive.app(),
            "/api/auth/profile",
            "POST",
            Some(&bio_cookie),
            Some(&bio_csrf),
            Some(ORIGIN),
            json!({"bio":"Can save while another claim waits"}),
        ),
    )
    .await
    .unwrap();
    assert_eq!(bio.status, StatusCode::OK);
    rusqlite::Connection::open(&archive.database)
        .unwrap()
        .execute(
            "UPDATE profile_sync SET synced_level=101 WHERE source='fxhash'",
            [],
        )
        .unwrap();
    archive.registry_release.notify_one();
    let response = edit.await.unwrap();
    assert_eq!(response.status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(
        Store::open_read_only(&archive.database)
            .unwrap()
            .profile_edit(&address)
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn cancelling_the_leading_claim_check_releases_coalescing_for_retry() {
    let archive = Archive::new(true);
    archive.registry_mode.store(11, Ordering::SeqCst);
    let registry = archive.registry.clone();
    let pending = tokio::spawn(async move { registry.check(100, "tz1caller", "freshname").await });
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while archive.registry_mode.load(Ordering::SeqCst) != 12 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    pending.abort();
    assert!(pending.await.unwrap_err().is_cancelled());
    archive.registry_mode.store(0, Ordering::SeqCst);
    archive.registry_release.notify_one();
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        archive.registry.check(100, "tz1caller", "freshname"),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(archive.registry_calls.load(Ordering::SeqCst), 4);
}

#[tokio::test]
async fn shared_head_expires_and_failed_fetches_are_not_cached() {
    let archive = Archive::new(true);
    archive
        .registry
        .check(100, "tz1caller", "freshname")
        .await
        .unwrap();
    assert_eq!(archive.registry_calls.load(Ordering::SeqCst), 3);
    tokio::time::sleep(std::time::Duration::from_millis(2100)).await;
    archive.registry_mode.store(3, Ordering::SeqCst);
    assert!(
        archive
            .registry
            .check(100, "tz1caller", "freshname")
            .await
            .is_err()
    );
    assert_eq!(archive.registry_calls.load(Ordering::SeqCst), 4);
    archive.registry_mode.store(0, Ordering::SeqCst);
    archive
        .registry
        .check(100, "tz1caller", "freshname")
        .await
        .unwrap();
    assert_eq!(archive.registry_calls.load(Ordering::SeqCst), 5);
}

#[tokio::test]
async fn oversized_single_registry_row_fails_transiently_then_retries() {
    let archive = Archive::new(true);
    archive.registry_mode.store(21, Ordering::SeqCst);
    assert!(
        archive
            .registry
            .check(100, "tz1caller", "freshname")
            .await
            .is_err()
    );
    assert_eq!(archive.registry_calls.load(Ordering::SeqCst), 8);
    archive.registry_mode.store(0, Ordering::SeqCst);
    archive
        .registry
        .check(100, "tz1caller", "freshname")
        .await
        .unwrap();
}

#[tokio::test]
async fn waiters_share_a_successful_refresh_that_takes_over_five_seconds() {
    let archive = Archive::new(true);
    archive.registry_mode.store(24, Ordering::SeqCst);
    let registry = archive.registry.clone();
    let first = tokio::spawn(async move { registry.check(100, "tz1caller", "freshname").await });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    archive
        .registry
        .check(100, "tz1other", "freshname")
        .await
        .unwrap();
    first.await.unwrap().unwrap();
    assert_eq!(archive.registry_calls.load(Ordering::SeqCst), 6);
}

#[tokio::test]
async fn auth_page_redirects_signed_in_address_to_its_profile() {
    let archive = Archive::new(true);
    let out = request(archive.app(), "/auth", "GET", None, None, None, json!(null)).await;
    assert_eq!(out.status, StatusCode::OK);
    assert!(String::from_utf8_lossy(&out.body).contains("Connect wallet"));
    let (cookie, _, address, _) = login(&archive, 9).await;
    let signed_in = request(
        archive.app(),
        "/auth",
        "GET",
        Some(&cookie),
        None,
        None,
        json!(null),
    )
    .await;
    assert_eq!(signed_in.status, StatusCode::SEE_OTHER);
    assert_eq!(
        signed_in.headers[header::LOCATION],
        format!("/profile/{address}").as_str()
    );
}
