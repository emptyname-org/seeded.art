use std::time::Duration;

use askama::Template;
use axum::{
    Extension, Json, Router,
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Redirect, Response},
    routing::{get, post},
};
use serde::Deserialize;
use tokio::sync::OwnedSemaphorePermit;

use super::{AppState, views::Meta};
use crate::{
    auth::{AuthError, AuthState, SignInRequest, TrustedClientIp, VerifyRequest},
    error_number::numbered,
    model::ProfileEdit,
    store::{Store, StoreError, profile::NAME_TAKEN},
};

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/auth", get(connect))
        .route("/auth/profile", get(profile))
        .route("/api/auth/session", get(session))
        .route("/api/auth/challenge", post(challenge))
        .route("/api/auth/verify", post(verify))
        .route("/api/auth/logout", post(logout))
        .route("/api/auth/profile", post(save_profile))
        .layer(DefaultBodyLimit::max(16 * 1024))
}

fn spawn_blocking_with_waiter<T, E>(
    waiter: OwnedSemaphorePermit,
    work: impl FnOnce() -> Result<T, E> + Send + 'static,
) -> tokio::task::JoinHandle<(Result<T, E>, OwnedSemaphorePermit)>
where
    T: Send + 'static,
    E: Send + 'static,
{
    tokio::task::spawn_blocking(move || (work(), waiter))
}

fn auth(state: &AppState) -> Result<AuthState, AuthError> {
    state.auth.clone().ok_or(AuthError::Unavailable)
}
fn meta(title: &str) -> Meta {
    Meta {
        title: title.into(),
        active: "",
        query: String::new(),
        scope: "",
        snapshot: String::new(),
    }
}
fn cookie(response: &mut Response, value: String) -> Result<(), AuthError> {
    response.headers_mut().append(
        header::SET_COOKIE,
        HeaderValue::from_str(&value).map_err(|_| AuthError::Unavailable)?,
    );
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    Ok(())
}

#[derive(Template)]
#[template(path = "auth.html")]
struct ConnectPage {
    meta: Meta,
    address: String,
    /// Where to go after sign-in: a checked `/oauth/authorize?` request, or empty.
    next: String,
}

/// The `next` query value when it is a valid return to the OAuth authorize endpoint.
fn oauth_next(uri: &axum::http::Uri) -> Option<String> {
    let mut url = reqwest::Url::parse("http://local/").ok()?;
    url.set_query(uri.query());
    let mut values = url
        .query_pairs()
        .filter(|(key, _)| key == "next")
        .map(|(_, value)| value.into_owned());
    let next = values.next()?;
    (values.next().is_none() && crate::auth::oauth::valid_next(&next)).then_some(next)
}

async fn connect(
    State(state): State<AppState>,
    headers: HeaderMap,
    request: super::agent::Request,
) -> Result<Response, AuthError> {
    let identity = auth(&state)?.identity(&headers)?;
    let next = state.oauth.as_ref().and_then(|_| oauth_next(&request.uri));
    if let Some(identity) = identity.as_ref().filter(|_| !request.json) {
        let target = next
            .clone()
            .unwrap_or_else(|| format!("/profile/{}", identity.address()));
        let mut response = Redirect::to(&target).into_response();
        response
            .headers_mut()
            .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
        response
            .headers_mut()
            .insert(header::VARY, HeaderValue::from_static("Cookie"));
        return Ok(response);
    }
    let address = identity
        .as_ref()
        .map(|identity| identity.address().to_owned())
        .unwrap_or_default();
    let data = serde_json::json!({"address":address,"session":identity.map(|identity|identity.view()),"challenge":"/api/auth/challenge","verify":"/api/auth/verify"});
    request
        .respond("auth", data, Some("/auth"), None, || {
            ConnectPage {
                meta: meta("Connect wallet"),
                address,
                next: next.unwrap_or_default(),
            }
            .render()
        })
        .map_err(|_| AuthError::Unavailable)
}

async fn session(State(state): State<AppState>, headers: HeaderMap) -> Result<Response, AuthError> {
    let identity = auth(&state)?
        .identity(&headers)?
        .map(|identity| identity.view());
    let mut response = Json(serde_json::json!({"session":identity})).into_response();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
        .headers_mut()
        .insert(header::VARY, HeaderValue::from_static("Cookie"));
    Ok(response)
}

async fn challenge(
    State(state): State<AppState>,
    client: Option<Extension<TrustedClientIp>>,
    headers: HeaderMap,
    Json(request): Json<SignInRequest>,
) -> Result<Response, AuthError> {
    let grant =
        auth(&state)?.challenge_for(client.ok_or(AuthError::Unavailable)?.0, &headers, request)?;
    let mut response = Json(grant.view).into_response();
    cookie(&mut response, grant.cookie)?;
    Ok(response)
}

async fn verify(
    State(state): State<AppState>,
    client: Option<Extension<TrustedClientIp>>,
    headers: HeaderMap,
    Json(request): Json<VerifyRequest>,
) -> Result<Response, AuthError> {
    let grant = auth(&state)?
        .verify_for(client.ok_or(AuthError::Unavailable)?.0, &headers, request)
        .await?;
    let mut response = Json(serde_json::json!({"session":grant.identity.view()})).into_response();
    cookie(&mut response, grant.cookie)?;
    cookie(&mut response, grant.clear_challenge)?;
    Ok(response)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LogoutRequest {
    #[serde(default)]
    all: bool,
}
async fn logout(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<LogoutRequest>,
) -> Result<Response, AuthError> {
    let auth = auth(&state)?;
    auth.logout(&headers, request.all)?;
    let mut response = StatusCode::NO_CONTENT.into_response();
    cookie(&mut response, auth.clear_session_cookie())?;
    cookie(&mut response, auth.clear_challenge_cookie())?;
    Ok(response)
}

#[derive(Template)]
#[template(path = "auth_profile.html")]
#[derive(serde::Serialize)]
struct ProfilePage {
    #[serde(skip)]
    meta: Meta,
    #[serde(skip)]
    header: super::views::ProfileHeader,
    address: String,
    name: String,
    bio: String,
    links: String,
    imported_name: String,
    imported_bio: String,
    imported_links: String,
    hide_bio: bool,
}

async fn profile(
    State(state): State<AppState>,
    headers: HeaderMap,
    request: super::agent::Request,
) -> Result<Response, Response> {
    let auth = auth(&state).map_err(IntoResponse::into_response)?;
    let Some(identity) = auth
        .identity(&headers)
        .map_err(IntoResponse::into_response)?
    else {
        return Ok(if request.json {
            super::agent::error(StatusCode::UNAUTHORIZED)
        } else {
            Redirect::to("/auth").into_response()
        });
    };
    let address = identity.address().to_owned();
    let content_origin = state.content_origin.clone();
    let overlay = state.trading.as_ref().map(|t| t.overlay(&address));
    let form = state
        .query(move |store| {
            let (mut shown, _) = super::displayed_profile(store, &address)?;
            let pending = super::market::pending(store, &address, overlay.as_ref())?;
            shown.held = shown.held.saturating_sub(pending.tokens.len() as u64);
            let header = super::views::profile_header(
                &shown,
                content_origin.as_deref(),
                Some(pending.counts(store.market_counts(&address)?).total()),
                "edit",
            );
            let profile = store.profile(&address)?;
            let edit = store.profile_edit(&address)?;
            let edit = edit.unwrap_or(ProfileEdit {
                name: None,
                bio: None,
                links: Vec::new(),
            });
            let hide_bio = edit.bio.as_deref() == Some("");
            Ok(ProfilePage {
                meta: meta("Edit profile"),
                header,
                address,
                imported_name: if edit.name.is_none() {
                    profile.name.unwrap_or_default()
                } else {
                    String::new()
                },
                imported_bio: if edit.bio.is_none() {
                    profile.bio.unwrap_or_default()
                } else {
                    String::new()
                },
                imported_links: if edit.links.is_empty() {
                    profile.links.join("\n")
                } else {
                    String::new()
                },
                name: edit.name.unwrap_or_default(),
                bio: edit.bio.unwrap_or_default(),
                links: edit.links.join("\n"),
                hide_bio,
            })
        })
        .await
        .map_err(IntoResponse::into_response)?;
    request
        .respond(
            "profile_edit",
            serde_json::json!(&form),
            Some("/auth/profile"),
            None,
            || form.render(),
        )
        .map_err(|_| AuthError::Unavailable.into_response())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProfileRequest {
    name: Option<String>,
    bio: Option<String>,
    #[serde(default)]
    links: Vec<String>,
}

async fn save_profile(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<ProfileRequest>,
) -> Result<Response, Response> {
    let auth = auth(&state).map_err(IntoResponse::into_response)?;
    let identity = auth
        .require(&headers)
        .map_err(IntoResponse::into_response)?;
    auth.admit_profile_edit(&identity)
        .map_err(IntoResponse::into_response)?;
    let edit = ProfileEdit {
        name: request.name,
        bio: request.bio,
        links: request.links,
    };
    let preflight_address = identity.address().to_owned();
    let preflight_edit = edit.clone();
    let baseline = state
        .query(move |store| {
            Ok(super::claims::new_claim_baseline(
                store,
                &preflight_address,
                &preflight_edit,
            ))
        })
        .await
        .map_err(IntoResponse::into_response)?
        .map_err(profile_error)?;
    if let Some(level) = baseline {
        let candidate_key = edit
            .name
            .as_deref()
            .map(crate::store::profile::name_key)
            .ok_or_else(claim_unavailable)?;
        state
            .claims
            .as_ref()
            .ok_or_else(claim_unavailable)?
            .check(level, identity.address(), &candidate_key)
            .await
            .map_err(|error| match error {
                super::claims::ClaimCheckError::Conflict => {
                    profile_error(StoreError::BadRequest(NAME_TAKEN.into()))
                }
                super::claims::ClaimCheckError::Unavailable
                | super::claims::ClaimCheckError::RateLimited => claim_unavailable(),
            })?;
    }
    let waiting = auth
        .write_waiting
        .clone()
        .try_acquire_owned()
        .map_err(|_| AuthError::RateLimited.into_response())?;
    let database = state.database.clone();
    let opening =
        spawn_blocking_with_waiter(waiting, move || Store::open_existing_write(&database));
    let (store, waiting) = tokio::time::timeout(Duration::from_secs(3), opening)
        .await
        .map_err(|_| AuthError::Unavailable.into_response())?
        .map_err(|_| AuthError::Unavailable.into_response())?;
    let mut store = store.map_err(profile_error)?;
    let permit = tokio::time::timeout(
        Duration::from_millis(250),
        auth.writes.clone().acquire_owned(),
    )
    .await
    .map_err(|_| AuthError::RateLimited.into_response())?
    .map_err(|_| AuthError::Unavailable.into_response())?;
    drop(waiting);
    let task = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        if !auth
            .is_current(&identity)
            .map_err(|_| StoreError::Unavailable("Sign-in unavailable".into()))?
        {
            return Err(StoreError::BadRequest(SIGNED_OUT.into()));
        }
        // save_profile_edit repeats validation under its immediate transaction.
        super::claims::guard_write(&store, baseline, &edit)?;
        // This is the SIGN-IN proof. The session and CSRF check authorize this edit.
        store.save_profile_edit(identity.address(), edit, identity.proof.clone())
    });
    let result = tokio::time::timeout(Duration::from_secs(6), task)
        .await
        .map_err(|_| AuthError::Unavailable.into_response())?
        .map_err(|_| AuthError::Unavailable.into_response())?;
    match result {
        Ok(()) => Ok(Json(serde_json::json!({"saved":true})).into_response()),
        Err(error) => Err(profile_error(error)),
    }
}

/// Marks a session that ended before the save; never shown as is.
const SIGNED_OUT: &str = "signed out before saving";

/// A numbered profile error body; the journal gets the number.
fn numbered_profile_error(status: StatusCode, number: u16, sentence: &str, name: &str) -> Response {
    crate::error_number::log(number, name);
    let body = Json(serde_json::json!({"error":numbered(sentence, number),"number":number}));
    if status == StatusCode::SERVICE_UNAVAILABLE {
        (status, [(header::RETRY_AFTER, "60")], body).into_response()
    } else {
        (status, body).into_response()
    }
}

fn profile_error(error: StoreError) -> Response {
    match error {
        StoreError::BadRequest(message) if message == NAME_TAKEN => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error":message})),
        )
            .into_response(),
        StoreError::BadRequest(message) if message == SIGNED_OUT => {
            numbered_profile_error(StatusCode::BAD_REQUEST, 403, "Sign in again", "SignedOut")
        }
        // The store names the rejected field.
        StoreError::BadRequest(message) => {
            let mut chars = message.chars();
            let sentence = chars
                .next()
                .map(|first| first.to_uppercase().chain(chars).collect::<String>())
                .unwrap_or_default();
            numbered_profile_error(StatusCode::BAD_REQUEST, 404, &sentence, "ProfileInvalid")
        }
        _ => numbered_profile_error(
            StatusCode::SERVICE_UNAVAILABLE,
            401,
            "The profile wasn't saved; try again",
            "ProfileUnsaved",
        ),
    }
}

fn claim_unavailable() -> Response {
    numbered_profile_error(
        StatusCode::SERVICE_UNAVAILABLE,
        402,
        "The name couldn't be checked; try again later",
        "NameUnchecked",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn timed_out_blocking_open_keeps_its_waiter_until_work_finishes() {
        let slots = std::sync::Arc::new(tokio::sync::Semaphore::new(1));
        let waiter = slots.clone().acquire_owned().await.unwrap();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let opening = spawn_blocking_with_waiter(waiter, move || {
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            Ok::<_, ()>(())
        });
        started_rx.await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(20), opening)
                .await
                .is_err()
        );
        assert_eq!(slots.available_permits(), 0);
        release_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while slots.available_permits() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(slots.available_permits(), 1);
    }
}
