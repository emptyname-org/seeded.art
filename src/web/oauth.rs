//! OAuth2 authorization-code endpoints for the first-party forum. Registered only when configured.
use std::sync::Arc;

use axum::{
    Extension, Json, Router,
    body::Bytes,
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, HeaderValue, StatusCode, Uri, header},
    middleware,
    response::{IntoResponse, Redirect, Response},
    routing::{get, post},
};
use percent_encoding::{NON_ALPHANUMERIC, percent_decode_str, utf8_percent_encode};
use serde_json::json;

use super::{AppState, WebError};
use crate::auth::{
    AuthError, TrustedClientIp,
    oauth::{
        self, CodeGrant, MAX_STATE_BYTES, OAuthClient, OAuthError, OAuthState, TOKEN_LIFETIME,
    },
};

pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route("/oauth/authorize", get(authorize))
        .route("/oauth/token", post(token))
        .route("/oauth/user", get(user))
        .layer(DefaultBodyLimit::max(4 * 1024))
        .layer(middleware::map_response(private_headers))
}

async fn private_headers(mut response: Response) -> Response {
    let headers = response.headers_mut();
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert(header::PRAGMA, HeaderValue::from_static("no-cache"));
    headers.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
    headers.insert("x-robots-tag", HeaderValue::from_static("noindex"));
    response
}

/// Form or query pairs, decoded.
fn pairs(text: &str) -> Vec<(String, String)> {
    let mut url = reqwest::Url::parse("http://local/").expect("static URL");
    url.set_query(Some(text));
    url.query_pairs().into_owned().collect()
}

/// `Err` when repeated: parameters must not appear more than once.
fn single<'a>(pairs: &'a [(String, String)], key: &str) -> Result<Option<&'a str>, ()> {
    let mut values = pairs.iter().filter(|(name, _)| name == key);
    let first = values.next().map(|(_, value)| value.as_str());
    match values.next() {
        Some(_) => Err(()),
        None => Ok(first),
    }
}

fn oauth_state(state: &AppState) -> Result<Arc<OAuthState>, Response> {
    state.oauth.clone().ok_or_else(|| {
        WebError {
            status: StatusCode::NOT_FOUND,
            message: "This item is not in the local archive.",
        }
        .into_response()
    })
}

fn bad_client() -> Response {
    crate::error_number::log(111, "UnknownClient");
    WebError {
        status: StatusCode::BAD_REQUEST,
        message: "Unknown sign-in client or return address (error 111).",
    }
    .into_response()
}

/// Back to the registered client with `params` added to its query.
fn back(client: &OAuthClient, params: &[(&str, &str)]) -> Response {
    let Ok(mut url) = reqwest::Url::parse(client.redirect_uri()) else {
        return bad_client();
    };
    {
        let mut query = url.query_pairs_mut();
        for (key, value) in params {
            query.append_pair(key, value);
        }
    }
    Redirect::to(url.as_str()).into_response()
}

fn back_error(client: &OAuthClient, error: &str, state: Option<&str>) -> Response {
    match state {
        Some(state) => back(client, &[("error", error), ("state", state)]),
        None => back(client, &[("error", error)]),
    }
}

async fn authorize(
    State(app): State<AppState>,
    client_ip: Option<Extension<TrustedClientIp>>,
    headers: HeaderMap,
    uri: Uri,
) -> Response {
    let oauth = match oauth_state(&app) {
        Ok(oauth) => oauth,
        Err(response) => return response,
    };
    let client = oauth.client();
    let query = pairs(uri.query().unwrap_or_default());
    // Never redirect anywhere but the registered address.
    match (single(&query, "client_id"), single(&query, "redirect_uri")) {
        (Ok(Some(id)), Ok(Some(redirect)))
            if id == client.client_id() && redirect == client.redirect_uri() => {}
        _ => return bad_client(),
    }
    let state = single(&query, "state");
    let echo = state
        .ok()
        .flatten()
        .filter(|state| !state.is_empty() && state.len() <= MAX_STATE_BYTES);
    match single(&query, "response_type") {
        Ok(Some("code")) => {}
        Ok(Some(_)) => return back_error(client, "unsupported_response_type", echo),
        _ => return back_error(client, "invalid_request", echo),
    }
    let Some(echo) = echo else {
        return back_error(client, "invalid_request", None);
    };
    let Some(auth) = app.auth.as_ref() else {
        return back_error(client, "temporarily_unavailable", Some(echo));
    };
    let identity = match auth.identity(&headers) {
        Ok(identity) => identity,
        Err(AuthError::Unauthenticated) => None,
        Err(_) => return back_error(client, "temporarily_unavailable", Some(echo)),
    };
    let Some(identity) = identity else {
        // Rebuilt from the checked values so `next` stays a plain, encoded path and query.
        let mut next = reqwest::Url::parse("http://local/oauth/authorize").expect("static URL");
        next.query_pairs_mut()
            .append_pair("response_type", "code")
            .append_pair("client_id", client.client_id())
            .append_pair("redirect_uri", client.redirect_uri())
            .append_pair("state", echo);
        let next = format!("/oauth/authorize?{}", next.query().unwrap_or_default());
        return Redirect::to(&format!(
            "/auth?next={}",
            utf8_percent_encode(&next, NON_ALPHANUMERIC)
        ))
        .into_response();
    };
    let Some(Extension(client_ip)) = client_ip else {
        return back_error(client, "temporarily_unavailable", Some(echo));
    };
    match oauth.issue_code(&identity, client_ip) {
        Ok(code) => back(client, &[("code", &code), ("state", echo)]),
        Err(_) => back_error(client, "temporarily_unavailable", Some(echo)),
    }
}

fn token_error(status: StatusCode, error: &str) -> Response {
    let mut response = (status, Json(json!({"error":error}))).into_response();
    if status == StatusCode::TOO_MANY_REQUESTS {
        response
            .headers_mut()
            .insert(header::RETRY_AFTER, HeaderValue::from_static("60"));
    }
    response
}

fn invalid_client(basic: bool) -> Response {
    let mut response = token_error(StatusCode::UNAUTHORIZED, "invalid_client");
    if basic {
        response.headers_mut().insert(
            header::WWW_AUTHENTICATE,
            HeaderValue::from_static("Basic realm=\"seeded.art\""),
        );
    }
    response
}

fn unavailable(error: OAuthError) -> Response {
    match error {
        OAuthError::RateLimited => {
            token_error(StatusCode::TOO_MANY_REQUESTS, "temporarily_unavailable")
        }
        OAuthError::Full | OAuthError::Unavailable => {
            token_error(StatusCode::SERVICE_UNAVAILABLE, "temporarily_unavailable")
        }
    }
}

/// HTTP Basic client credentials, form-encoded before base64 (RFC 6749 2.3.1).
fn basic_credentials(headers: &HeaderMap) -> Result<Option<(String, String)>, ()> {
    let mut values = headers.get_all(header::AUTHORIZATION).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(());
    }
    let value = value.to_str().map_err(|_| ())?;
    let (scheme, encoded) = value.split_once(' ').ok_or(())?;
    if !scheme.eq_ignore_ascii_case("basic") {
        return Err(());
    }
    let decoded = oauth::decode_base64(encoded.trim()).ok_or(())?;
    let decoded = String::from_utf8(decoded).map_err(|_| ())?;
    let (id, secret) = decoded.split_once(':').ok_or(())?;
    let form = |text: &str| {
        percent_decode_str(&text.replace('+', " "))
            .decode_utf8()
            .map(|text| text.into_owned())
            .map_err(|_| ())
    };
    Ok(Some((form(id)?, form(secret)?)))
}

/// Form fields, every presented code (now spent) and the authenticated client's id.
type Authenticated = (Vec<(String, String)>, Vec<Option<CodeGrant>>, String);

fn authenticated(
    oauth: &OAuthState,
    headers: &HeaderMap,
    body: &[u8],
) -> Result<Authenticated, Response> {
    let form = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|value| {
            value
                .trim()
                .eq_ignore_ascii_case("application/x-www-form-urlencoded")
        });
    let Some(text) = std::str::from_utf8(body).ok().filter(|_| form) else {
        return Err(token_error(StatusCode::BAD_REQUEST, "invalid_request"));
    };
    let fields = pairs(text);
    // Every presented code is spent here, whatever the outcome below.
    let mut grants = Vec::new();
    for (_, code) in fields.iter().filter(|(name, _)| name == "code") {
        match oauth.take_code(code) {
            Ok(grant) => grants.push(grant),
            Err(error) => return Err(unavailable(error)),
        }
    }
    let basic = basic_credentials(headers).map_err(|()| invalid_client(true))?;
    let (Ok(body_id), Ok(body_secret)) = (
        single(&fields, "client_id"),
        single(&fields, "client_secret"),
    ) else {
        return Err(token_error(StatusCode::BAD_REQUEST, "invalid_request"));
    };
    let used_basic = basic.is_some();
    let (client_id, secret) = match (basic, body_id, body_secret) {
        (Some(_), _, Some(_)) => {
            return Err(token_error(StatusCode::BAD_REQUEST, "invalid_request"));
        }
        (Some((id, _)), Some(body_id), None) if body_id != id => return Err(invalid_client(true)),
        (Some((id, secret)), _, None) => (id, secret),
        (None, Some(id), Some(secret)) => (id.to_owned(), secret.to_owned()),
        (None, _, _) => return Err(invalid_client(false)),
    };
    if !oauth.client().authenticate(&client_id, &secret) {
        return Err(invalid_client(used_basic));
    }
    Ok((fields, grants, client_id))
}

async fn token(
    State(app): State<AppState>,
    client_ip: Option<Extension<TrustedClientIp>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let oauth = match oauth_state(&app) {
        Ok(oauth) => oauth,
        Err(response) => return response,
    };
    let Some(Extension(client_ip)) = client_ip else {
        return unavailable(OAuthError::Unavailable);
    };
    if let Err(error) = oauth.admit_client_attempt(client_ip) {
        return unavailable(error);
    }
    // Only failed client authentication spends the network's budget.
    let (fields, mut grants, client_id) = match authenticated(&oauth, &headers, &body) {
        Ok(request) => request,
        Err(response) => {
            return match oauth.charge_client_failure(client_ip) {
                Ok(()) => response,
                Err(error) => unavailable(error),
            };
        }
    };
    match single(&fields, "grant_type") {
        Ok(Some("authorization_code")) => {}
        Ok(Some(_)) => {
            return token_error(StatusCode::BAD_REQUEST, "unsupported_grant_type");
        }
        _ => return token_error(StatusCode::BAD_REQUEST, "invalid_request"),
    }
    let (Ok(Some(redirect_uri)), 1) = (single(&fields, "redirect_uri"), grants.len()) else {
        return token_error(StatusCode::BAD_REQUEST, "invalid_request");
    };
    let Some(grant) = grants.pop().flatten() else {
        return token_error(StatusCode::BAD_REQUEST, "invalid_grant");
    };
    if grant.client_id != client_id || grant.redirect_uri != redirect_uri {
        return token_error(StatusCode::BAD_REQUEST, "invalid_grant");
    }
    match app
        .auth
        .as_ref()
        .map(|auth| auth.is_current(&grant.identity))
    {
        Some(Ok(true)) => {}
        Some(Ok(false)) => return token_error(StatusCode::BAD_REQUEST, "invalid_grant"),
        _ => return unavailable(OAuthError::Unavailable),
    }
    match oauth.issue_token(grant) {
        Ok(token) => Json(json!({
            "access_token": token,
            "token_type": "Bearer",
            "expires_in": TOKEN_LIFETIME.as_secs(),
        }))
        .into_response(),
        Err(error) => unavailable(error),
    }
}

fn invalid_token(presented: bool) -> Response {
    let mut response = token_error(StatusCode::UNAUTHORIZED, "invalid_token");
    response.headers_mut().insert(
        header::WWW_AUTHENTICATE,
        HeaderValue::from_static(if presented {
            "Bearer error=\"invalid_token\""
        } else {
            "Bearer"
        }),
    );
    response
}

async fn user(State(app): State<AppState>, headers: HeaderMap) -> Response {
    let oauth = match oauth_state(&app) {
        Ok(oauth) => oauth,
        Err(response) => return response,
    };
    let mut values = headers.get_all(header::AUTHORIZATION).iter();
    let (Some(value), None) = (values.next(), values.next()) else {
        return invalid_token(false);
    };
    let Some(token) = value
        .to_str()
        .ok()
        .and_then(|value| value.split_once(' '))
        .filter(|(scheme, _)| scheme.eq_ignore_ascii_case("bearer"))
        .map(|(_, token)| token.trim())
    else {
        return invalid_token(false);
    };
    let identity = match oauth.token_identity(token) {
        Ok(Some(identity)) => identity,
        Ok(None) => return invalid_token(true),
        Err(error) => return unavailable(error),
    };
    // Signing out of seeded.art ends the token too.
    match app.auth.as_ref().map(|auth| auth.is_current(&identity)) {
        Some(Ok(true)) => {}
        Some(Ok(false)) => return invalid_token(true),
        _ => return unavailable(OAuthError::Unavailable),
    }
    let address = identity.address().to_owned();
    let lookup = address.clone();
    let name = match app
        .query(move |store| store.display_names(&[lookup.as_str()]))
        .await
    {
        Ok(mut names) => names.remove(&address),
        Err(_) => return unavailable(OAuthError::Unavailable),
    };
    let username = oauth::forum_username(name.as_deref(), &address);
    Json(json!({
        "id": address,
        "address": address,
        "name": name.unwrap_or_else(|| address.clone()),
        "username": username,
    }))
    .into_response()
}
