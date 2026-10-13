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
use percent_encoding::{NON_ALPHANUMERIC, percent_decode_str, utf8_percent_encode};
use seeded_archive::{
    auth::{
        AuthState, TrustedClientIp, crypto,
        oauth::{OAuthClient, OAuthClock, OAuthLimits, OAuthState},
    },
    import::import_fixtures,
    store::Store,
    web::{AppState, router},
};
use serde_json::{Value, json};
use std::{
    fs,
    net::{IpAddr, Ipv4Addr},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tower::ServiceExt;

const ORIGIN: &str = "https://seeded.art";
const CLIENT: &str = "forum";
const SECRET: &str = "forum-secret-0123456789";
const REDIRECT: &str = "https://forum.seeded.art/auth/seeded";
static NEXT: AtomicU64 = AtomicU64::new(0);

struct Archive {
    directory: PathBuf,
    database: PathBuf,
    auth: AuthState,
    oauth: Option<Arc<OAuthState>>,
    clock: OAuthClock,
}

impl Archive {
    fn new() -> Self {
        Self::with_limits(OAuthLimits::default())
    }
    fn with_limits(limits: OAuthLimits) -> Self {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let directory = root.join(format!(
            "tmp/oauth/routes-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&directory).unwrap();
        let database = directory.join("archive.sqlite");
        let mut store = Store::open(&database).unwrap();
        import_fixtures(&mut store, &root.join("assets/fixtures")).unwrap();
        let clock = OAuthClock::manual();
        let oauth = OAuthState::new(OAuthClient::new(CLIENT, SECRET, REDIRECT).unwrap())
            .with_clock(clock.clone())
            .with_limits(limits);
        Self {
            directory,
            database,
            auth: AuthState::new(ORIGIN).unwrap(),
            oauth: Some(Arc::new(oauth)),
            clock,
        }
    }
    fn app(&self) -> Router {
        let state = AppState::new(
            &self.database,
            Path::new(env!("CARGO_MANIFEST_DIR")).join("assets"),
        )
        .with_auth(self.auth.clone());
        router(match &self.oauth {
            Some(oauth) => state.with_oauth(oauth.clone()),
            None => state,
        })
    }
}
impl Drop for Archive {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.directory);
    }
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
    fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }
    fn location(&self) -> &str {
        self.headers[header::LOCATION].to_str().unwrap()
    }
}

async fn send(app: Router, request: Request<Body>) -> Reply {
    let response = app.oneshot(request).await.unwrap();
    let (parts, body) = response.into_parts();
    Reply {
        status: parts.status,
        headers: parts.headers,
        body: body.collect().await.unwrap().to_bytes().to_vec(),
    }
}

async fn get(archive: &Archive, path: &str, cookie: Option<&str>) -> Reply {
    get_from(archive, Ipv4Addr::LOCALHOST.into(), path, cookie).await
}

async fn get_from(archive: &Archive, ip: IpAddr, path: &str, cookie: Option<&str>) -> Reply {
    let mut request = Request::builder().uri(path).extension(TrustedClientIp(ip));
    if let Some(cookie) = cookie {
        request = request.header(header::COOKIE, cookie);
    }
    send(archive.app(), request.body(Body::empty()).unwrap()).await
}

fn encode(value: &str) -> String {
    utf8_percent_encode(value, NON_ALPHANUMERIC).to_string()
}

fn authorize_path(state: &str) -> String {
    format!(
        "/oauth/authorize?response_type=code&client_id={CLIENT}&redirect_uri={}&state={}&scope=user",
        encode(REDIRECT),
        encode(state)
    )
}

/// Query pairs of a redirect to the registered client.
fn returned(reply: &Reply) -> Vec<(String, String)> {
    assert_eq!(reply.status, StatusCode::SEE_OTHER);
    let url = reqwest::Url::parse(reply.location()).unwrap();
    assert_eq!(
        format!(
            "{}://{}{}",
            url.scheme(),
            url.host_str().unwrap(),
            url.path()
        ),
        REDIRECT
    );
    url.query_pairs().into_owned().collect()
}
fn field<'a>(pairs: &'a [(String, String)], key: &str) -> Option<&'a str> {
    pairs
        .iter()
        .find(|(name, _)| name == key)
        .map(|(_, value)| value.as_str())
}

async fn code(archive: &Archive, cookie: &str) -> String {
    code_from(archive, Ipv4Addr::LOCALHOST.into(), cookie).await
}

async fn code_from(archive: &Archive, ip: IpAddr, cookie: &str) -> String {
    let reply = get_from(archive, ip, &authorize_path("st-1"), Some(cookie)).await;
    let pairs = returned(&reply);
    assert_eq!(field(&pairs, "state"), Some("st-1"));
    field(&pairs, "code").unwrap().to_owned()
}

async fn token_request(archive: &Archive, body: &str, basic: Option<&str>) -> Reply {
    token_request_from(archive, Ipv4Addr::LOCALHOST.into(), body, basic).await
}

async fn token_request_from(
    archive: &Archive,
    ip: IpAddr,
    body: &str,
    basic: Option<&str>,
) -> Reply {
    let mut request = Request::builder()
        .uri("/oauth/token")
        .method("POST")
        .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
        .extension(TrustedClientIp(ip));
    if let Some(basic) = basic {
        request = request.header(header::AUTHORIZATION, basic);
    }
    send(
        archive.app(),
        request.body(Body::from(body.to_owned())).unwrap(),
    )
    .await
}

fn token_body(code: &str, secret: &str, redirect: &str) -> String {
    format!(
        "grant_type=authorization_code&code={}&redirect_uri={}&client_id={CLIENT}&client_secret={}",
        encode(code),
        encode(redirect),
        encode(secret)
    )
}

async fn exchange(archive: &Archive, code: &str) -> Reply {
    token_request(archive, &token_body(code, SECRET, REDIRECT), None).await
}

async fn user(archive: &Archive, token: &str) -> Reply {
    send(
        archive.app(),
        Request::builder()
            .uri("/oauth/user")
            .header(header::AUTHORIZATION, format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await
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
fn address(seed: u8) -> String {
    crypto::public_key_address(&public_key(seed)).unwrap()
}

async fn post_json(
    archive: &Archive,
    path: &str,
    cookie: Option<&str>,
    csrf: Option<&str>,
    body: Value,
) -> Reply {
    let mut request = Request::builder()
        .uri(path)
        .method("POST")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::ORIGIN, ORIGIN)
        .extension(TrustedClientIp(std::net::Ipv4Addr::LOCALHOST.into()));
    if let Some(cookie) = cookie {
        request = request.header(header::COOKIE, cookie);
    }
    if let Some(csrf) = csrf {
        request = request.header("x-csrf-token", csrf);
    }
    send(
        archive.app(),
        request.body(Body::from(body.to_string())).unwrap(),
    )
    .await
}

/// Signs in with the key derived from `seed`; returns the session cookie and CSRF token.
async fn login(archive: &Archive, seed: u8) -> (String, String) {
    let challenge = post_json(
        archive,
        "/api/auth/challenge",
        None,
        None,
        json!({"address":address(seed),"public_key":public_key(seed)}),
    )
    .await;
    assert_eq!(challenge.status, StatusCode::OK);
    let challenge_cookie = challenge.headers[header::SET_COOKIE]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned();
    let value = challenge.json();
    let verify = post_json(
        archive,
        "/api/auth/verify",
        Some(&challenge_cookie),
        value["csrf"].as_str(),
        json!({"signature":signature(seed,value["payload"].as_str().unwrap())}),
    )
    .await;
    assert_eq!(verify.status, StatusCode::OK);
    let cookie = verify
        .headers
        .get_all(header::SET_COOKIE)
        .iter()
        .map(|value| value.to_str().unwrap())
        .find(|value| value.contains("session="))
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned();
    (
        cookie,
        verify.json()["session"]["csrf"].as_str().unwrap().into(),
    )
}

fn assert_private(reply: &Reply) {
    assert_eq!(reply.headers[header::CACHE_CONTROL], "no-store");
    assert_eq!(reply.headers[header::REFERRER_POLICY], "no-referrer");
    assert_eq!(reply.headers["x-robots-tag"], "noindex");
}

#[tokio::test]
async fn signed_in_visitor_goes_from_authorize_to_code_token_and_user() {
    let archive = Archive::new();
    let (cookie, _) = login(&archive, 7).await;
    let authorize = get(&archive, &authorize_path("a b&c=d/é"), Some(&cookie)).await;
    assert_private(&authorize);
    let pairs = returned(&authorize);
    assert_eq!(field(&pairs, "state"), Some("a b&c=d/é"));
    let code = field(&pairs, "code").unwrap();
    assert_eq!(code.len(), 43);
    assert!(
        code.bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
    );
    let token = exchange(&archive, code).await;
    assert_eq!(token.status, StatusCode::OK);
    assert_private(&token);
    assert_eq!(token.headers[header::PRAGMA], "no-cache");
    let token = token.json();
    assert_eq!(token["token_type"], "Bearer");
    assert_eq!(token["expires_in"], 300);
    let access = token["access_token"].as_str().unwrap();
    let address = address(7);
    for _ in 0..2 {
        let reply = user(&archive, access).await;
        assert_eq!(reply.status, StatusCode::OK);
        assert_private(&reply);
        let body = reply.json();
        assert_eq!(body["id"], address);
        assert_eq!(body["address"], address);
        assert_eq!(body["name"], address);
        assert_eq!(
            body["username"],
            format!("{}-{}", &address[..8], &address[address.len() - 5..])
        );
        assert!(body.get("email").is_none());
    }
}

#[tokio::test]
async fn signed_out_authorize_goes_to_sign_in_and_returns_after_it() {
    let archive = Archive::new();
    let reply = get(&archive, &authorize_path("xyz"), None).await;
    assert_eq!(reply.status, StatusCode::SEE_OTHER);
    assert_private(&reply);
    let next = reply.location().strip_prefix("/auth?next=").unwrap();
    let decoded = percent_decode_str(next).decode_utf8().unwrap();
    assert!(decoded.starts_with("/oauth/authorize?"), "{decoded}");
    assert!(!decoded.contains("//"), "{decoded}");
    let page = get(&archive, reply.location(), None).await;
    assert_eq!(page.status, StatusCode::OK);
    assert!(page.text().contains("data-auth-next=\""), "{}", page.text());
    assert!(page.text().contains("<h1>Sign in to the forum</h1>"));
    assert!(page.text().contains("Signing in makes no transaction."));
    let plain = get(&archive, "/auth", None).await;
    assert!(plain.text().contains("<h1>Connect wallet</h1>"));
    assert!(!plain.text().contains("Signing in makes no transaction."));
    assert!(!plain.text().contains("Sign in to the forum"));
    let (cookie, _) = login(&archive, 7).await;
    let signed_in = get(&archive, reply.location(), Some(&cookie)).await;
    assert_eq!(signed_in.status, StatusCode::SEE_OTHER);
    assert_eq!(signed_in.location(), decoded);
    let back = get(&archive, &decoded, Some(&cookie)).await;
    let pairs = returned(&back);
    assert_eq!(field(&pairs, "state"), Some("xyz"));
    assert!(field(&pairs, "code").is_some());
}

#[tokio::test]
async fn sign_in_ignores_next_that_is_not_this_sites_authorize_endpoint() {
    let archive = Archive::new();
    let (cookie, _) = login(&archive, 7).await;
    let profile = format!("/profile/{}", address(7));
    let long = format!("/oauth/authorize?state={}", "a".repeat(2048));
    for next in [
        "//evil.example/oauth/authorize?x=1",
        "/\\evil.example/oauth/authorize?x=1",
        "https://evil.example/oauth/authorize?x=1",
        "/oauth/authorizeX?x=1",
        "/oauth/authorize",
        "/oauth/authorize?x=1//evil",
        "/oauth/authorize?x=\u{1}",
        long.as_str(),
    ] {
        let path = format!("/auth?next={}", encode(next));
        let signed_in = get(&archive, &path, Some(&cookie)).await;
        assert_eq!(signed_in.status, StatusCode::SEE_OTHER, "{next}");
        assert_eq!(signed_in.location(), profile, "{next}");
        let page = get(&archive, &path, None).await;
        assert_eq!(page.status, StatusCode::OK, "{next}");
        assert!(!page.text().contains("data-auth-next"), "{next}");
    }
}

#[tokio::test]
async fn unregistered_client_or_redirect_at_authorize_is_a_page_not_a_redirect() {
    let archive = Archive::new();
    let (cookie, _) = login(&archive, 7).await;
    for path in [
        format!(
            "/oauth/authorize?response_type=code&client_id=other&redirect_uri={}&state=s",
            encode(REDIRECT)
        ),
        format!(
            "/oauth/authorize?response_type=code&client_id={CLIENT}&redirect_uri={}&state=s",
            encode("https://evil.example/auth/seeded")
        ),
        format!(
            "/oauth/authorize?response_type=code&client_id={CLIENT}&redirect_uri={}/&state=s",
            encode(REDIRECT)
        ),
        format!("/oauth/authorize?response_type=code&client_id={CLIENT}&state=s"),
        format!(
            "/oauth/authorize?response_type=code&client_id={CLIENT}&client_id={CLIENT}&redirect_uri={}&state=s",
            encode(REDIRECT)
        ),
    ] {
        let reply = get(&archive, &path, Some(&cookie)).await;
        assert_eq!(reply.status, StatusCode::BAD_REQUEST, "{path}");
        assert!(reply.headers.get(header::LOCATION).is_none(), "{path}");
        assert_private(&reply);
    }
}

#[tokio::test]
async fn authorize_request_errors_return_to_the_client_with_state() {
    let archive = Archive::new();
    let (cookie, _) = login(&archive, 7).await;
    let base = format!("client_id={CLIENT}&redirect_uri={}", encode(REDIRECT));
    for (query, error, state) in [
        (
            format!("{base}&response_type=code"),
            "invalid_request",
            None,
        ),
        (
            format!("{base}&response_type=code&state="),
            "invalid_request",
            None,
        ),
        (
            format!("{base}&response_type=code&state={}", "s".repeat(513)),
            "invalid_request",
            None,
        ),
        (format!("{base}&state=s1"), "invalid_request", Some("s1")),
        (
            format!("{base}&response_type=token&state=s2"),
            "unsupported_response_type",
            Some("s2"),
        ),
    ] {
        let reply = get(
            &archive,
            &format!("/oauth/authorize?{query}"),
            Some(&cookie),
        )
        .await;
        let pairs = returned(&reply);
        assert_eq!(field(&pairs, "error"), Some(error), "{query}");
        assert_eq!(field(&pairs, "state"), state, "{query}");
        assert!(field(&pairs, "code").is_none(), "{query}");
    }
    let longest = "s".repeat(512);
    let reply = get(&archive, &authorize_path(&longest), Some(&cookie)).await;
    assert_eq!(field(&returned(&reply), "state"), Some(longest.as_str()));
}

#[tokio::test]
async fn wrong_client_credentials_are_invalid_client_and_spend_the_code() {
    let archive = Archive::new();
    let (cookie, _) = login(&archive, 7).await;
    let code = code(&archive, &cookie).await;
    let wrong = token_request(
        &archive,
        &token_body(&code, "forum-secret-0123456780", REDIRECT),
        None,
    )
    .await;
    assert_eq!(wrong.status, StatusCode::UNAUTHORIZED);
    assert_eq!(wrong.json(), json!({"error":"invalid_client"}));
    let missing = token_request(
        &archive,
        &format!(
            "grant_type=authorization_code&code={}&redirect_uri={}",
            encode(&code),
            encode(REDIRECT)
        ),
        None,
    )
    .await;
    assert_eq!(missing.status, StatusCode::UNAUTHORIZED);
    assert_eq!(missing.json(), json!({"error":"invalid_client"}));
    assert_eq!(
        exchange(&archive, &code).await.json()["error"],
        "invalid_grant"
    );
}

#[tokio::test]
async fn basic_client_authentication_is_accepted() {
    let archive = Archive::new();
    let (cookie, _) = login(&archive, 7).await;
    let basic = |id: &str, secret: &str| {
        let pair = format!("{id}:{secret}");
        let mut out = String::new();
        let alphabet = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        for chunk in pair.as_bytes().chunks(3) {
            let n = chunk
                .iter()
                .enumerate()
                .fold(0_u32, |n, (i, b)| n | (u32::from(*b) << (16 - 8 * i)));
            for i in 0..4 {
                out.push(if i <= chunk.len() {
                    alphabet[((n >> (18 - 6 * i)) & 63) as usize] as char
                } else {
                    '='
                });
            }
        }
        format!("Basic {out}")
    };
    let body = |code: &str| {
        format!(
            "grant_type=authorization_code&code={}&redirect_uri={}",
            encode(code),
            encode(REDIRECT)
        )
    };
    let code_one = code(&archive, &cookie).await;
    let reply = token_request(&archive, &body(&code_one), Some(&basic(CLIENT, SECRET))).await;
    assert_eq!(reply.status, StatusCode::OK);
    assert!(reply.json()["access_token"].is_string());
    let code_two = code(&archive, &cookie).await;
    let reply = token_request(
        &archive,
        &body(&code_two),
        Some(&basic(CLIENT, "wrong-secret-0123456789")),
    )
    .await;
    assert_eq!(reply.status, StatusCode::UNAUTHORIZED);
    assert_eq!(reply.json()["error"], "invalid_client");
    assert!(
        reply.headers[header::WWW_AUTHENTICATE]
            .to_str()
            .unwrap()
            .starts_with("Basic")
    );
}

#[tokio::test]
async fn wrong_redirect_uri_at_token_is_invalid_grant_and_spends_the_code() {
    let archive = Archive::new();
    let (cookie, _) = login(&archive, 7).await;
    let code = code(&archive, &cookie).await;
    let reply = token_request(
        &archive,
        &token_body(&code, SECRET, "https://forum.seeded.art/other"),
        None,
    )
    .await;
    assert_eq!(reply.status, StatusCode::BAD_REQUEST);
    assert_eq!(reply.json(), json!({"error":"invalid_grant"}));
    assert_eq!(
        exchange(&archive, &code).await.json()["error"],
        "invalid_grant"
    );
}

#[tokio::test]
async fn reused_code_is_invalid_grant() {
    let archive = Archive::new();
    let (cookie, _) = login(&archive, 7).await;
    let code = code(&archive, &cookie).await;
    assert_eq!(exchange(&archive, &code).await.status, StatusCode::OK);
    let again = exchange(&archive, &code).await;
    assert_eq!(again.status, StatusCode::BAD_REQUEST);
    assert_eq!(again.json(), json!({"error":"invalid_grant"}));
    let unknown = exchange(&archive, "not-a-code").await;
    assert_eq!(unknown.json(), json!({"error":"invalid_grant"}));
}

#[tokio::test]
async fn code_expires_after_sixty_seconds() {
    let archive = Archive::new();
    let (cookie, _) = login(&archive, 7).await;
    let fresh = code(&archive, &cookie).await;
    archive.clock.advance(Duration::from_secs(59));
    assert_eq!(exchange(&archive, &fresh).await.status, StatusCode::OK);
    let stale = code(&archive, &cookie).await;
    archive.clock.advance(Duration::from_secs(60));
    let reply = exchange(&archive, &stale).await;
    assert_eq!(reply.status, StatusCode::BAD_REQUEST);
    assert_eq!(reply.json(), json!({"error":"invalid_grant"}));
}

#[tokio::test]
async fn sign_out_between_authorize_and_token_is_invalid_grant_and_ends_tokens() {
    let archive = Archive::new();
    let (cookie, csrf) = login(&archive, 7).await;
    let issued = code(&archive, &cookie).await;
    let token = exchange(&archive, &issued).await.json()["access_token"]
        .as_str()
        .unwrap()
        .to_owned();
    let pending = code(&archive, &cookie).await;
    let logout = post_json(
        &archive,
        "/api/auth/logout",
        Some(&cookie),
        Some(&csrf),
        json!({"all":false}),
    )
    .await;
    assert_eq!(logout.status, StatusCode::NO_CONTENT);
    let reply = exchange(&archive, &pending).await;
    assert_eq!(reply.status, StatusCode::BAD_REQUEST);
    assert_eq!(reply.json(), json!({"error":"invalid_grant"}));
    assert_eq!(
        user(&archive, &token).await.status,
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn bad_or_expired_bearer_token_is_invalid_token() {
    let archive = Archive::new();
    let (cookie, _) = login(&archive, 7).await;
    let issued = code(&archive, &cookie).await;
    let token = exchange(&archive, &issued).await.json()["access_token"]
        .as_str()
        .unwrap()
        .to_owned();
    let bad = user(&archive, "not-a-token").await;
    assert_eq!(bad.status, StatusCode::UNAUTHORIZED);
    assert_eq!(
        bad.headers[header::WWW_AUTHENTICATE],
        "Bearer error=\"invalid_token\""
    );
    let none = get(&archive, "/oauth/user", None).await;
    assert_eq!(none.status, StatusCode::UNAUTHORIZED);
    assert!(none.headers.contains_key(header::WWW_AUTHENTICATE));
    archive.clock.advance(Duration::from_secs(299));
    assert_eq!(user(&archive, &token).await.status, StatusCode::OK);
    archive.clock.advance(Duration::from_secs(1));
    let expired = user(&archive, &token).await;
    assert_eq!(expired.status, StatusCode::UNAUTHORIZED);
    assert_eq!(
        expired.headers[header::WWW_AUTHENTICATE],
        "Bearer error=\"invalid_token\""
    );
}

/// Exchanges a fresh code of this session; returns the access token.
async fn token_from(archive: &Archive, ip: IpAddr, cookie: &str) -> String {
    let issued = code_from(archive, ip, cookie).await;
    let reply = exchange(archive, &issued).await;
    assert_eq!(reply.status, StatusCode::OK);
    reply.json()["access_token"].as_str().unwrap().to_owned()
}

#[tokio::test]
async fn repeated_sign_ins_of_one_session_keep_only_its_newest_tokens() {
    let archive = Archive::with_limits(OAuthLimits {
        tokens: 4,
        ..OAuthLimits::default()
    });
    let (cookie, _) = login(&archive, 7).await;
    let mut tokens = Vec::new();
    for _ in 0..6 {
        let token = token_from(&archive, Ipv4Addr::LOCALHOST.into(), &cookie).await;
        assert_eq!(user(&archive, &token).await.status, StatusCode::OK);
        tokens.push(token);
    }
    for (index, token) in tokens.iter().enumerate() {
        let expected = if index < 4 {
            StatusCode::UNAUTHORIZED
        } else {
            StatusCode::OK
        };
        assert_eq!(user(&archive, token).await.status, expected, "{index}");
    }
    let (other, _) = login(&archive, 8).await;
    let token = token_from(&archive, "192.0.2.8".parse().unwrap(), &other).await;
    assert_eq!(user(&archive, &token).await.status, StatusCode::OK);
}

#[tokio::test]
async fn one_address_or_network_keeps_only_its_newest_tokens() {
    let archive = Archive::with_limits(OAuthLimits {
        tokens_per_network: 2,
        ..OAuthLimits::default()
    });
    // One address, five sessions, five networks: its oldest token goes.
    let mut tokens = Vec::new();
    for host in 1..=5u8 {
        let (cookie, _) = login(&archive, 7).await;
        tokens.push(token_from(&archive, IpAddr::from([192, 0, 2, host]), &cookie).await);
    }
    // Three addresses on one visitor network: that network's oldest token goes.
    for seed in [1, 2, 3] {
        let (cookie, _) = login(&archive, seed).await;
        tokens.push(token_from(&archive, "198.51.100.1".parse().unwrap(), &cookie).await);
    }
    for (index, token) in tokens.iter().enumerate() {
        let expected = if index == 0 || index == 5 {
            StatusCode::UNAUTHORIZED
        } else {
            StatusCode::OK
        };
        assert_eq!(user(&archive, token).await.status, expected, "{index}");
    }
}

#[tokio::test]
async fn full_token_store_reclaims_the_busiest_network_until_tokens_expire() {
    let archive = Archive::with_limits(OAuthLimits {
        tokens: 4,
        ..OAuthLimits::default()
    });
    let attacker: IpAddr = "203.0.113.66".parse().unwrap();
    let mut attackers = Vec::new();
    for seed in 1..=4u8 {
        attackers.push(login(&archive, seed).await.0);
    }
    let mut held = Vec::new();
    for cookie in &attackers {
        held.push(token_from(&archive, attacker, cookie).await);
    }
    let (visitor, _) = login(&archive, 9).await;
    let visitor_ip: IpAddr = "198.51.100.9".parse().unwrap();
    let token = token_from(&archive, visitor_ip, &visitor).await;
    // Refilling reclaims the attacker's network again, never the visitor's token.
    for cookie in &attackers {
        token_from(&archive, attacker, cookie).await;
    }
    assert_eq!(user(&archive, &token).await.status, StatusCode::OK);
    assert_eq!(
        user(&archive, &held[0]).await.status,
        StatusCode::UNAUTHORIZED
    );
    archive.clock.advance(Duration::from_secs(299));
    assert_eq!(user(&archive, &token).await.status, StatusCode::OK);
    archive.clock.advance(Duration::from_secs(1));
    assert_eq!(
        user(&archive, &token).await.status,
        StatusCode::UNAUTHORIZED
    );
    let later = token_from(&archive, visitor_ip, &visitor).await;
    assert_eq!(user(&archive, &later).await.status, StatusCode::OK);
}

#[tokio::test]
async fn one_session_keeps_only_its_newest_codes_and_cannot_block_others() {
    let archive = Archive::with_limits(OAuthLimits {
        codes: 4,
        ..OAuthLimits::default()
    });
    let (busy, _) = login(&archive, 7).await;
    let mut codes = Vec::new();
    for _ in 0..6 {
        codes.push(code(&archive, &busy).await);
        archive.clock.advance(Duration::from_secs(1));
    }
    let (other, _) = login(&archive, 8).await;
    let other_code = code(&archive, &other).await;
    assert_eq!(exchange(&archive, &other_code).await.status, StatusCode::OK);
    assert_eq!(
        exchange(&archive, &codes[3]).await.json(),
        json!({"error":"invalid_grant"})
    );
    assert_eq!(exchange(&archive, &codes[4]).await.status, StatusCode::OK);
    assert_eq!(exchange(&archive, &codes[5]).await.status, StatusCode::OK);
}

#[tokio::test]
async fn one_address_or_network_keeps_only_its_newest_codes() {
    let archive = Archive::with_limits(OAuthLimits {
        codes_per_address: 2,
        codes_per_network: 2,
        ..OAuthLimits::default()
    });
    let networks: [IpAddr; 3] =
        ["192.0.2.1", "192.0.2.2", "192.0.2.3"].map(|ip| ip.parse().unwrap());
    // One address, three sessions, three networks: its oldest code goes.
    let mut codes = Vec::new();
    for ip in networks {
        let (cookie, _) = login(&archive, 7).await;
        codes.push(code_from(&archive, ip, &cookie).await);
    }
    // Three addresses on one network: that network's oldest code goes.
    for seed in [1, 2, 3] {
        let (cookie, _) = login(&archive, seed).await;
        codes.push(code_from(&archive, "198.51.100.1".parse().unwrap(), &cookie).await);
    }
    for (index, code) in codes.iter().enumerate() {
        let reply = exchange(&archive, code).await;
        if index == 0 || index == 3 {
            assert_eq!(reply.json(), json!({"error":"invalid_grant"}), "{index}");
        } else {
            assert_eq!(reply.status, StatusCode::OK, "{index}");
        }
    }
}

#[tokio::test]
async fn full_code_store_evicts_the_busiest_network_not_a_new_visitor() {
    let archive = Archive::with_limits(OAuthLimits {
        codes: 8,
        ..OAuthLimits::default()
    });
    let attacker: IpAddr = "203.0.113.66".parse().unwrap();
    let mut attackers = Vec::new();
    for seed in 1..=4u8 {
        attackers.push(login(&archive, seed).await.0);
    }
    let mut held = Vec::new();
    for cookie in &attackers {
        held.push(code_from(&archive, attacker, cookie).await);
        held.push(code_from(&archive, attacker, cookie).await);
    }
    let (visitor, _) = login(&archive, 9).await;
    let visitor_ip: IpAddr = "198.51.100.9".parse().unwrap();
    let issued = code_from(&archive, visitor_ip, &visitor).await;
    // Refilling the store evicts the attacker's network again, never the visitor's code.
    archive.clock.advance(Duration::from_secs(30));
    for cookie in &attackers {
        code_from(&archive, attacker, cookie).await;
        code_from(&archive, attacker, cookie).await;
    }
    assert_eq!(exchange(&archive, &issued).await.status, StatusCode::OK);
    assert_eq!(
        exchange(&archive, &held[0]).await.json(),
        json!({"error":"invalid_grant"})
    );
    archive.clock.advance(Duration::from_secs(31));
    let later = code_from(&archive, visitor_ip, &visitor).await;
    assert_eq!(exchange(&archive, &later).await.status, StatusCode::OK);
}

#[tokio::test]
async fn failed_client_authentication_is_rate_limited_per_client_network() {
    let archive = Archive::with_limits(OAuthLimits {
        client_failures_per_minute: 2,
        ..OAuthLimits::default()
    });
    let (cookie, _) = login(&archive, 7).await;
    let issued = code(&archive, &cookie).await;
    let wrong = token_body("unknown", "forum-secret-0123456780", REDIRECT);
    for _ in 0..2 {
        let reply = token_request(&archive, &wrong, None).await;
        assert_eq!(reply.status, StatusCode::UNAUTHORIZED);
    }
    // A network out of failures may not authenticate at all, even correctly.
    let limited = exchange(&archive, &issued).await;
    assert_eq!(limited.status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(limited.headers[header::RETRY_AFTER], "60");
    let other: IpAddr = "192.0.2.7".parse().unwrap();
    assert_eq!(
        token_request_from(
            &archive,
            other,
            &token_body(&issued, SECRET, REDIRECT),
            None
        )
        .await
        .status,
        StatusCode::OK
    );
    archive.clock.advance(Duration::from_secs(60));
    assert_eq!(
        token_request(&archive, &wrong, None).await.status,
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn invalid_codes_from_the_authenticated_client_never_block_its_sign_ins() {
    let archive = Archive::with_limits(OAuthLimits {
        client_failures_per_minute: 2,
        clients: 1,
        ..OAuthLimits::default()
    });
    let forum: IpAddr = "203.0.113.10".parse().unwrap();
    // Another network's failed authentication takes the only client window.
    let stranger: IpAddr = "192.0.2.8".parse().unwrap();
    let wrong = token_body("x", "forum-secret-0123456780", REDIRECT);
    assert_eq!(
        token_request_from(&archive, stranger, &wrong, None)
            .await
            .status,
        StatusCode::UNAUTHORIZED
    );
    // Forged forum callbacks: the forum presents junk codes with its own credentials.
    for _ in 0..10 {
        let reply =
            token_request_from(&archive, forum, &token_body("junk", SECRET, REDIRECT), None).await;
        assert_eq!(reply.json(), json!({"error":"invalid_grant"}));
    }
    let (cookie, _) = login(&archive, 7).await;
    let issued = code(&archive, &cookie).await;
    let reply = token_request_from(
        &archive,
        forum,
        &token_body(&issued, SECRET, REDIRECT),
        None,
    )
    .await;
    assert_eq!(reply.status, StatusCode::OK);
}

#[tokio::test]
async fn malformed_token_requests_name_the_oauth_error() {
    let archive = Archive::new();
    let (cookie, _) = login(&archive, 7).await;
    let code = code(&archive, &cookie).await;
    let client = format!("client_id={CLIENT}&client_secret={}", encode(SECRET));
    let redirect = encode(REDIRECT);
    for (body, error) in [
        (
            format!("grant_type=password&code=x&redirect_uri={redirect}&{client}"),
            "unsupported_grant_type",
        ),
        (
            format!("code=x&redirect_uri={redirect}&{client}"),
            "invalid_request",
        ),
        (
            format!("grant_type=authorization_code&redirect_uri={redirect}&{client}"),
            "invalid_request",
        ),
        (
            format!("grant_type=authorization_code&code=x&{client}"),
            "invalid_request",
        ),
        (
            format!("grant_type=authorization_code&code=x&code=y&redirect_uri={redirect}&{client}"),
            "invalid_request",
        ),
    ] {
        let reply = token_request(&archive, &body, None).await;
        assert_eq!(reply.status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(reply.json()["error"], error, "{body}");
    }
    let json_body = send(
        archive.app(),
        Request::builder()
            .uri("/oauth/token")
            .method("POST")
            .header(header::CONTENT_TYPE, "application/json")
            .extension(TrustedClientIp(std::net::Ipv4Addr::LOCALHOST.into()))
            .body(Body::from(token_body(&code, SECRET, REDIRECT)))
            .unwrap(),
    )
    .await;
    assert_eq!(json_body.status, StatusCode::BAD_REQUEST);
    assert_eq!(json_body.json()["error"], "invalid_request");
    let oversized = token_request(&archive, &"a".repeat(5 * 1024), None).await;
    assert_eq!(oversized.status, StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn oauth_routes_are_not_found_when_unconfigured() {
    let mut archive = Archive::new();
    archive.oauth = None;
    let (cookie, _) = login(&archive, 7).await;
    let path = format!("/auth?next={}", encode(&authorize_path("s")));
    let signed_in = get(&archive, &path, Some(&cookie)).await;
    assert_eq!(signed_in.location(), format!("/profile/{}", address(7)));
    assert!(
        !get(&archive, &path, None)
            .await
            .text()
            .contains("data-auth-next")
    );
    for path in [
        authorize_path("s"),
        "/oauth/user".to_owned(),
        "/oauth/token".to_owned(),
    ] {
        assert_eq!(
            get(&archive, &path, None).await.status,
            StatusCode::NOT_FOUND,
            "{path}"
        );
    }
    assert_eq!(
        token_request(&archive, "grant_type=authorization_code", None)
            .await
            .status,
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn robots_keep_oauth_out_of_search() {
    let archive = Archive::new();
    let robots = get(&archive, "/robots.txt", None).await.text();
    assert!(robots.lines().any(|line| line == "Disallow: /oauth/"));
}

#[tokio::test]
async fn user_name_and_username_follow_the_shown_numbered_name() {
    let archive = Archive::new();
    let addresses = [address(7), address(8)];
    let sql = rusqlite::Connection::open(&archive.database).unwrap();
    for address in &addresses {
        sql.execute(
            "INSERT INTO users(address,name,name_key) VALUES (?1,'zancan','zancan') \
             ON CONFLICT(address) DO UPDATE SET name=excluded.name,name_key=excluded.name_key",
            [address],
        )
        .unwrap();
    }
    drop(sql);
    Store::open_existing_write(&archive.database)
        .unwrap()
        .assign_name_numbers()
        .unwrap();
    let shown = Store::open_read_only(&archive.database)
        .unwrap()
        .display_names(&[addresses[0].as_str(), addresses[1].as_str()])
        .unwrap();
    let mut usernames = Vec::new();
    for (seed, address) in [7, 8].into_iter().zip(&addresses) {
        let (cookie, _) = login(&archive, seed).await;
        let issued = code(&archive, &cookie).await;
        let token = exchange(&archive, &issued).await.json()["access_token"]
            .as_str()
            .unwrap()
            .to_owned();
        let body = user(&archive, &token).await.json();
        assert_eq!(body["name"], shown[address]);
        assert_eq!(body["username"], shown[address]);
        usernames.push(body["username"].as_str().unwrap().to_owned());
    }
    usernames.sort();
    assert_eq!(usernames, ["zancan", "zancan-2"]);
}
