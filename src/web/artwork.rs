//! Artwork runs on its own origin with separate assembly and delivery budgets.
#[path = "avatar.rs"]
mod avatar;
#[path = "delivery.rs"]
mod delivery;
#[path = "roots.rs"]
mod roots;

pub(super) fn avatar_supported(uri: &str) -> bool {
    avatar::allowed(uri).is_some()
}
use std::{
    collections::BTreeMap,
    io::Read,
    sync::atomic::{AtomicU64, Ordering},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

#[cfg(test)]
use axum::body::Body;
use axum::{
    Extension, Json, Router,
    extract::{OriginalUri, Path, Query, Request, State, rejection::QueryRejection},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    middleware::{self, Next},
    response::{Html, IntoResponse, Response},
    routing::get,
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use super::AppState;
use crate::{
    config::Config,
    content::{Content, ContentError, ContentStore, ErrorKind},
    player,
};

#[derive(Clone)]
pub struct ArtworkState {
    app: AppState,
    avatars: avatar::AvatarClient,
    content: ContentStore,
    content_origin: String,
    max_bytes: u64,
    responses: Arc<Semaphore>,
    waiting: Arc<Semaphore>,
    transforms: Arc<Semaphore>,
    metadata_reads: Arc<Semaphore>,
    metadata_waiting: Arc<Semaphore>,
    metadata_timeout: Duration,
    handler_timeout: Duration,
    counters: Arc<ArtworkCounters>,
    delivery: delivery::Budget,
    playback: Arc<Mutex<PlaybackStatus>>,
    roots: Arc<roots::RootIndex>,
}

#[derive(Default)]
pub(super) struct ArtworkCounters {
    delivery_reclaim: crate::delivery_control::Registry,
    metadata_waiting_full: AtomicU64,
    metadata_waiting_timeout: AtomicU64,
    metadata_query_timeout: AtomicU64,
    metadata_query_unavailable: AtomicU64,
    handler_timeout: AtomicU64,
    delivery_client_table_full: AtomicU64,
    upstream_waiting_full: AtomicU64,
    upstream_waiting_timeout: AtomicU64,
    transform_timeout: AtomicU64,
    delivery_global_bytes_full: AtomicU64,
    delivery_client_bytes_full: AtomicU64,
}

impl ArtworkCounters {
    fn increment(counter: &AtomicU64) {
        let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
            Some(value.saturating_add(1))
        });
    }

    pub(super) fn metadata_waiting_full(&self) {
        Self::increment(&self.metadata_waiting_full);
    }
    pub(super) fn metadata_waiting_timeout(&self) {
        Self::increment(&self.metadata_waiting_timeout);
    }
    pub(super) fn delivery_client_table_full(&self) {
        Self::increment(&self.delivery_client_table_full);
    }
    pub(super) fn upstream_waiting_full(&self) {
        Self::increment(&self.upstream_waiting_full);
    }
    pub(super) fn upstream_waiting_timeout(&self) {
        Self::increment(&self.upstream_waiting_timeout);
    }
    pub(super) fn transform_timeout(&self) {
        Self::increment(&self.transform_timeout);
    }
    pub(super) fn delivery_global_bytes_full(&self) {
        Self::increment(&self.delivery_global_bytes_full);
    }
    pub(super) fn delivery_client_bytes_full(&self) {
        Self::increment(&self.delivery_client_bytes_full);
    }
}

/// Cloneable access to bounded artwork admission counters.
#[derive(Clone)]
pub struct ArtworkMetricsHandle {
    counters: Arc<ArtworkCounters>,
}

/// Monotonic counts of artwork requests rejected or timed out by each limiter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ArtworkMetricsSnapshot {
    pub metadata_query_timeout: u64,
    pub metadata_query_unavailable: u64,
    pub handler_timeout: u64,
    pub delivery_client_table_full: u64,
    /// Metadata requests rejected because the waiting queue had no slot.
    pub metadata_waiting_full: u64,
    /// Metadata requests that timed out waiting for a read slot.
    pub metadata_waiting_timeout: u64,
    /// Upstream requests rejected because the waiting queue had no slot.
    pub upstream_waiting_full: u64,
    /// Upstream requests that timed out waiting for an assembly slot.
    pub upstream_waiting_timeout: u64,
    /// Local transforms that timed out waiting for a worker slot.
    pub transform_timeout: u64,
    /// Responses rejected because the global byte budget was full.
    pub delivery_global_bytes_full: u64,
    /// Responses rejected because that client's byte budget was full.
    pub delivery_client_bytes_full: u64,
    pub delivery_reclaim_scans: u64,
    pub delivery_reclaimed: u64,
    pub delivery_reclaim_no_victim: u64,
}

impl ArtworkMetricsHandle {
    pub fn snapshot(&self) -> ArtworkMetricsSnapshot {
        let reclaim = self.counters.delivery_reclaim.snapshot();
        ArtworkMetricsSnapshot {
            delivery_reclaim_scans: reclaim.scans,
            delivery_reclaimed: reclaim.reclaimed,
            delivery_reclaim_no_victim: reclaim.no_victim,
            metadata_query_timeout: self.counters.metadata_query_timeout.load(Ordering::Relaxed),
            metadata_query_unavailable: self
                .counters
                .metadata_query_unavailable
                .load(Ordering::Relaxed),
            handler_timeout: self.counters.handler_timeout.load(Ordering::Relaxed),
            delivery_client_table_full: self
                .counters
                .delivery_client_table_full
                .load(Ordering::Relaxed),
            metadata_waiting_full: self.counters.metadata_waiting_full.load(Ordering::Relaxed),
            metadata_waiting_timeout: self
                .counters
                .metadata_waiting_timeout
                .load(Ordering::Relaxed),
            upstream_waiting_full: self.counters.upstream_waiting_full.load(Ordering::Relaxed),
            upstream_waiting_timeout: self
                .counters
                .upstream_waiting_timeout
                .load(Ordering::Relaxed),
            transform_timeout: self.counters.transform_timeout.load(Ordering::Relaxed),
            delivery_global_bytes_full: self
                .counters
                .delivery_global_bytes_full
                .load(Ordering::Relaxed),
            delivery_client_bytes_full: self
                .counters
                .delivery_client_bytes_full
                .load(Ordering::Relaxed),
        }
    }
}

const PLAYBACK_PER_PREFIX: usize = 32;
const MAX_PLAYBACK_RECORDS: usize = PLAYBACK_PER_PREFIX * 1024;
const PLAYBACK_TTL: Duration = Duration::from_secs(300);

#[derive(Default)]
struct PlaybackStatus {
    records: BTreeMap<(String, u64, String), (Instant, bool, std::net::IpAddr)>,
}
impl PlaybackStatus {
    fn prune(&mut self, now: Instant) {
        self.records
            .retain(|_, (created, _, _)| now.saturating_duration_since(*created) < PLAYBACK_TTL);
    }
    fn record(
        &mut self,
        client: std::net::IpAddr,
        contract: &str,
        token: u64,
        instance: &str,
        verified: bool,
        now: Instant,
    ) {
        self.prune(now);
        let client = crate::client_ip::rate_ip(client);
        let key = (contract.into(), token, instance.into());
        // A different prefix cannot replace a live frame's status or evict it.
        if self
            .records
            .get(&key)
            .is_some_and(|(_, _, owner)| *owner != client)
        {
            return;
        }
        if !self.records.contains_key(&key) {
            let own = self
                .records
                .iter()
                .filter(|(_, (_, _, owner))| *owner == client);
            if own.clone().count() >= PLAYBACK_PER_PREFIX {
                if let Some(oldest) = own
                    .min_by_key(|(_, (created, _, _))| *created)
                    .map(|(key, _)| key.clone())
                {
                    self.records.remove(&oldest);
                }
            }
            // At the global memory bound, preserve existing frames until expiry.
            if self.records.len() >= MAX_PLAYBACK_RECORDS {
                return;
            }
        }
        self.records.insert(key, (now, verified, client));
    }
    fn get(&mut self, contract: &str, token: u64, instance: &str, now: Instant) -> Option<bool> {
        self.prune(now);
        self.records
            .get(&(contract.into(), token, instance.into()))
            .map(|(_, verified, _)| *verified)
    }
}

impl ArtworkState {
    pub fn new(app: AppState, content: ContentStore, config: &Config) -> Self {
        let counters = Arc::new(ArtworkCounters::default());
        let app = app.isolated_database_pool();
        let roots = roots::RootIndex::new(app.database.clone());
        Self {
            metadata_reads: app.reads.clone(),
            metadata_waiting: app.waiting.clone(),
            metadata_timeout: Duration::from_secs(10),
            handler_timeout: Duration::from_secs(40),
            app,
            avatars: avatar::AvatarClient::default(),
            content,
            content_origin: config.content_origin.clone(),
            max_bytes: config.object_bytes,
            delivery: delivery::Budget::new(config.object_bytes, counters.clone()),
            responses: Arc::new(Semaphore::new(config.assemblies * 2)),
            waiting: Arc::new(Semaphore::new(config.assemblies * 8)),
            transforms: Arc::new(Semaphore::new(16)),
            counters,
            playback: Arc::new(Mutex::new(PlaybackStatus::default())),
            roots,
        }
    }

    pub fn metrics_handle(&self) -> ArtworkMetricsHandle {
        ArtworkMetricsHandle {
            counters: self.counters.clone(),
        }
    }

    async fn query<T, F>(&self, operation: F) -> Result<T, super::WebError>
    where
        T: Send + 'static,
        F: FnOnce(&crate::store::Store) -> Result<T, crate::store::StoreError> + Send + 'static,
    {
        self.query_with_timeout(operation, self.metadata_timeout)
            .await
    }

    async fn query_with_timeout<T, F>(
        &self,
        operation: F,
        timeout: Duration,
    ) -> Result<T, super::WebError>
    where
        T: Send + 'static,
        F: FnOnce(&crate::store::Store) -> Result<T, crate::store::StoreError> + Send + 'static,
    {
        let waiting = self
            .metadata_waiting
            .clone()
            .try_acquire_owned()
            .map_err(|error| {
                if matches!(error, tokio::sync::TryAcquireError::NoPermits) {
                    self.counters.metadata_waiting_full();
                }
                super::WebError::unavailable()
            })?;
        let read = tokio::time::timeout(timeout, self.metadata_reads.clone().acquire_owned())
            .await
            .map_err(|_| {
                self.counters.metadata_waiting_timeout();
                super::WebError::unavailable()
            })?
            .map_err(|_| super::WebError::unavailable())?;
        drop(waiting);
        self.app
            .query_reserved(operation, read)
            .await
            .map_err(|error| {
                if matches!(error, super::QueryError::Timeout) {
                    ArtworkCounters::increment(&self.counters.metadata_query_timeout);
                } else if matches!(
                    error,
                    super::QueryError::Store(crate::store::StoreError::Unavailable(_))
                ) {
                    ArtworkCounters::increment(&self.counters.metadata_query_unavailable);
                }
                error.into_web()
            })
    }
}

#[derive(Clone)]
struct Work {
    responses: Arc<Semaphore>,
    waiting: Arc<Semaphore>,
    transforms: Arc<Semaphore>,
    counters: Arc<ArtworkCounters>,
    upstream: Arc<tokio::sync::OnceCell<Arc<OwnedSemaphorePermit>>>,
}

impl Work {
    async fn upstream(&self) -> Result<Arc<OwnedSemaphorePermit>, ContentError> {
        self.upstream_with_timeout(Duration::from_secs(5)).await
    }

    async fn upstream_with_timeout(
        &self,
        timeout: Duration,
    ) -> Result<Arc<OwnedSemaphorePermit>, ContentError> {
        self.upstream
            .get_or_try_init(|| async {
                let _waiting = self.waiting.clone().try_acquire_owned().map_err(|error| {
                    if matches!(error, tokio::sync::TryAcquireError::NoPermits) {
                        self.counters.upstream_waiting_full();
                    }
                    ContentError::unavailable("upstream queue full")
                })?;
                let permit = tokio::time::timeout(timeout, self.responses.clone().acquire_owned())
                    .await
                    .map_err(|_| {
                        self.counters.upstream_waiting_timeout();
                        ContentError::unavailable("upstream assembly busy")
                    })?
                    .map_err(|_| ContentError::unavailable("upstream assembly closed"))?;
                Ok(Arc::new(permit))
            })
            .await
            .cloned()
    }

    async fn transform(&self) -> Result<Arc<OwnedSemaphorePermit>, ContentError> {
        self.transform_with_timeout(Duration::from_secs(10)).await
    }

    async fn transform_with_timeout(
        &self,
        timeout: Duration,
    ) -> Result<Arc<OwnedSemaphorePermit>, ContentError> {
        if let Some(permit) = self.upstream.get() {
            return Ok(permit.clone());
        }
        Ok(Arc::new(
            tokio::time::timeout(timeout, self.transforms.clone().acquire_owned())
                .await
                .map_err(|_| {
                    self.counters.transform_timeout();
                    ContentError::unavailable("local preparation busy")
                })?
                .map_err(|_| ContentError::unavailable("local preparation closed"))?,
        ))
    }
}

pub fn router(state: ArtworkState) -> Router {
    Router::new()
        .route("/content/{scheme}/{cid}", get(resource))
        .route("/content/{scheme}/{cid}/", get(resource))
        .route("/content/{scheme}/{cid}/{*path}", get(resource))
        .route("/article-media/{id}/{slot}", get(article_media))
        .route("/profile-media/{address}", get(profile_media))
        .route("/render/{contract}/{id}", get(render))
        .route("/playback-status/{contract}/{id}", get(playback_status))
        .fallback(|| async { failure(StatusCode::NOT_FOUND, None) })
        .layer(middleware::from_fn_with_state(
            state.clone(),
            bounded_response,
        ))
        .layer(middleware::map_response(noindex_unless_image))
        .with_state(state)
}

/// Images stay indexable as images of app pages; nothing else on this origin is.
async fn noindex_unless_image(mut response: Response) -> Response {
    let image = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value
                .trim_start()
                .get(..6)
                .is_some_and(|kind| kind.eq_ignore_ascii_case("image/"))
        });
    if !image {
        response.headers_mut().insert(
            header::HeaderName::from_static("x-robots-tag"),
            HeaderValue::from_static("noindex"),
        );
    }
    response
}

#[derive(Deserialize)]
struct StatusQuery {
    instance: String,
}
#[derive(Serialize)]
struct StatusReply {
    verification: &'static str,
}
async fn playback_status(
    State(state): State<ArtworkState>,
    Path((contract, id)): Path<(String, String)>,
    query: Result<Query<StatusQuery>, QueryRejection>,
) -> Response {
    let Ok(Query(query)) = query else {
        return failure(StatusCode::BAD_REQUEST, None);
    };
    let Ok(id) = super::numeric_id(&id) else {
        return failure(StatusCode::BAD_REQUEST, None);
    };
    if contract.len() > 64
        || query.instance.len() != 32
        || !query.instance.bytes().all(|b| b.is_ascii_hexdigit())
    {
        return failure(StatusCode::BAD_REQUEST, None);
    }
    let Ok(mut records) = state.playback.lock() else {
        return failure(StatusCode::SERVICE_UNAVAILABLE, None);
    };
    let (status, verification) = match records.get(&contract, id, &query.instance, Instant::now()) {
        Some(true) => (StatusCode::OK, "verified"),
        Some(false) => (StatusCode::OK, "unverified"),
        None => (StatusCode::ACCEPTED, "pending"),
    };
    (status, Json(StatusReply { verification })).into_response()
}

async fn bounded_response(
    State(state): State<ArtworkState>,
    mut request: Request,
    next: Next,
) -> Response {
    let client = request
        .extensions()
        .get::<crate::auth::TrustedClientIp>()
        .map(|ip| ip.0)
        .unwrap_or(std::net::Ipv4Addr::UNSPECIFIED.into());
    let connection = request
        .extensions()
        .get::<axum::extract::ConnectInfo<crate::transport::ConnectionInfo>>()
        .map(|info| info.0.control.clone());
    if request.version() == axum::http::Version::HTTP_2 {
        if let Some(connection) = &connection {
            connection.protect_multiplexed();
        }
    }
    let delivery_client = connection
        .clone()
        .map(|connection| crate::delivery_control::Client::new(client, connection));
    let work = Work {
        responses: state.responses.clone(),
        waiting: state.waiting.clone(),
        transforms: state.transforms.clone(),
        counters: state.counters.clone(),
        upstream: Arc::new(tokio::sync::OnceCell::new()),
    };
    request.extensions_mut().insert(work.clone());
    request.extensions_mut().insert(PlaybackClient(client));
    // RFC 9110 ranges apply to GET, not HEAD.
    if request.method() == axum::http::Method::HEAD {
        request.headers_mut().remove(header::RANGE);
    }
    let response = match tokio::time::timeout(
        state.handler_timeout,
        crate::delivery_control::CLIENT.scope(delivery_client.clone(), next.run(request)),
    )
    .await
    {
        Ok(response) => response,
        Err(_) => {
            ArtworkCounters::increment(&state.counters.handler_timeout);
            failure(StatusCode::SERVICE_UNAVAILABLE, None)
        }
    };
    // Small fixed errors retain no artwork; preserve their original status at overload.
    if response.extensions().get::<StaticError>().is_some() {
        return secure(response, &state);
    }
    let (mut parts, body) = secure(response, &state).into_parts();
    use axum::body::HttpBody;
    let Some(bytes) = body.size_hint().upper() else {
        return secure(failure(StatusCode::BAD_GATEWAY, None), &state);
    };
    if bytes == 0 {
        return Response::from_parts(parts, body);
    }
    let reservation = state
        .delivery
        .reserve_delivery_after(
            client,
            bytes,
            parts.extensions.get::<AccountedBody>().is_some(),
            connection.clone(),
            move || drop(work),
        )
        .await;
    let delivery = match reservation {
        Ok(reservation) => reservation,
        Err(status) => return secure(failure(status, None), &state),
    };
    if let Some(client) = delivery_client {
        client.ready();
    }
    if let Some(connection) = &connection {
        parts.extensions.insert(connection.clone());
    }
    Response::from_parts(
        parts,
        crate::request_limits::hold_delivery(body, delivery, connection),
    )
}

#[derive(Clone)]
struct PassiveMedia;
#[derive(Clone)]
struct AccountedBody;
#[derive(Clone)]
struct StaticError;
#[derive(Clone)]
struct PlaybackClient(std::net::IpAddr);

fn secure(mut response: Response, state: &ArtworkState) -> Response {
    let passive = response.extensions().get::<PassiveMedia>().is_some();
    let headers = response.headers_mut();
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_ORIGIN,
        HeaderValue::from_static("*"),
    );
    headers.insert(
        "cross-origin-resource-policy",
        HeaderValue::from_static("cross-origin"),
    );
    // Even frame-ancestors * rejects opaque sandboxed ancestors in Chromium.
    headers.insert(header::CONTENT_SECURITY_POLICY, HeaderValue::from_str(&format!(
        "default-src 'none'; script-src 'unsafe-inline' 'unsafe-eval' {content} blob:; style-src 'unsafe-inline' {content}; img-src {content} data: blob:; font-src {content} data:; media-src {content} data: blob:; connect-src {content} data: blob:; worker-src {content} blob:; frame-src {content}; object-src 'none'; base-uri {content}; form-action 'none'; sandbox allow-scripts",
        content = state.content_origin
    )).unwrap_or_else(|_| HeaderValue::from_static("default-src 'none'; sandbox")));
    if passive {
        headers.insert(header::CONTENT_SECURITY_POLICY, HeaderValue::from_static("default-src 'none'; script-src 'none'; object-src 'none'; frame-ancestors 'none'; sandbox"));
    }
    headers
        .entry(header::CACHE_CONTROL)
        .or_insert(HeaderValue::from_static("no-store"));
    response
}

fn failure(status: StatusCode, instance: Option<&str>) -> Response {
    let mut response = match instance
        .and_then(|instance| player::prepare_error_document(instance, status.as_u16()).ok())
    {
        Some(html) => (status, Html(html)).into_response(),
        None => (status, "NFT unavailable").into_response(),
    };
    response.extensions_mut().insert(StaticError);
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    if matches!(
        status,
        StatusCode::SERVICE_UNAVAILABLE | StatusCode::TOO_MANY_REQUESTS
    ) {
        response
            .headers_mut()
            .insert(header::RETRY_AFTER, HeaderValue::from_static("2"));
    }
    response
}

fn upstream_error(error: ContentError, instance: Option<&str>) -> Response {
    let mut response = failure(
        StatusCode::from_u16(error.http_status()).unwrap_or(StatusCode::BAD_GATEWAY),
        instance,
    );
    if let Some(delay) = error.retry_after {
        if let Ok(value) = HeaderValue::from_str(&delay.as_secs().clamp(1, 3600).to_string()) {
            response.headers_mut().insert(header::RETRY_AFTER, value);
        }
    }
    response
}

#[derive(Debug, PartialEq, Eq)]
struct Location {
    scheme: String,
    cid: String,
    path: String,
}
impl Location {
    fn parse(value: &str) -> Result<Self, ContentError> {
        let (scheme, rest) = value
            .split_once("://")
            .ok_or_else(|| ContentError::new(ErrorKind::BadRequest, "unsupported URI"))?;
        if !matches!(scheme, "ipfs" | "onchfs") {
            return Err(ContentError::new(
                ErrorKind::BadRequest,
                "unsupported scheme",
            ));
        }
        let rest = rest.split(['?', '#']).next().unwrap_or_default();
        let rest = if scheme == "ipfs" {
            rest.strip_prefix("ipfs/").unwrap_or(rest)
        } else {
            let (authority, path) = rest.split_once('/').unwrap_or((rest, ""));
            match authority {
                "tezos"
                | "tezos:NetXdQprcVkpaWU"
                | "tezos:NetXdQprcVkpaWU:KT1Ae7dT1gsLw2tRnUMXSCmEyF74KVkM6LUo" => path,
                _ if authority.contains(':') => {
                    return Err(ContentError::new(
                        ErrorKind::BadRequest,
                        "unsupported ONCHFS authority",
                    ));
                }
                _ => rest,
            }
        };
        let (cid, path) = rest.split_once('/').unwrap_or((rest, ""));
        if cid.is_empty() || cid.len() > 256 || !cid.bytes().all(|b| b.is_ascii_alphanumeric()) {
            return Err(ContentError::new(ErrorKind::BadRequest, "invalid CID"));
        }
        crate::content::path::split(path)?;
        Ok(Self {
            scheme: scheme.into(),
            cid: cid.into(),
            path: path.into(),
        })
    }
    async fn resolve(
        &self,
        content: &ContentStore,
        work: &Work,
        passive: bool,
    ) -> Result<Content, ContentError> {
        if let Some(hit) = content
            .get_local(&self.scheme, &self.cid, &self.path)
            .await?
        {
            return Ok(hit);
        }
        work.upstream().await?;
        content
            .get_after_local_miss(&self.scheme, &self.cid, &self.path, passive)
            .await
    }
    fn base(&self, origin: &str, directory: bool) -> String {
        let path = if directory {
            self.path.trim_end_matches('/')
        } else {
            self.path.rsplit_once('/').map_or("", |(parent, _)| parent)
        };
        format!(
            "{origin}/content/{}/{}/{}",
            self.scheme,
            self.cid,
            if path.is_empty() {
                String::new()
            } else {
                format!("{path}/")
            }
        )
    }
}

pub(super) fn preview_url(origin: &str, uri: &str) -> Option<String> {
    let parsed = origin.parse::<axum::http::Uri>().ok()?;
    if !matches!(parsed.scheme_str(), Some("http" | "https"))
        || parsed.host().is_none_or(str::is_empty)
        || parsed.authority()?.as_str().contains('@')
        || parsed.path() != "/"
        || parsed.query().is_some()
        || origin.contains(['#', '\\'])
        || origin.chars().any(char::is_control)
    {
        return None;
    }
    let location = Location::parse(uri).ok()?;
    Some(format!(
        "{}/content/{}/{}/{}",
        origin.trim_end_matches('/'),
        location.scheme,
        crate::content::path::uri_component(&location.cid),
        location.path
    ))
}

async fn resource(
    State(state): State<ArtworkState>,
    OriginalUri(uri): OriginalUri,
    Extension(work): Extension<Work>,
    headers: HeaderMap,
) -> Response {
    let raw = uri.path().strip_prefix("/content/").unwrap_or_default();
    let Some((scheme, rest)) = raw.split_once('/') else {
        return failure(StatusCode::BAD_REQUEST, None);
    };
    let location = match Location::parse(&format!("{scheme}://{rest}")) {
        Ok(location) => location,
        Err(error) => return upstream_error(error, None),
    };
    let scheme = location.scheme.clone();
    let cid = location.cid.clone();
    let indexed = crate::store::canonical_content_root(&scheme, &cid)
        .is_ok_and(|canonical| state.roots.known(&scheme, &canonical));
    if !indexed {
        match state
            .query(move |store| store.content_root_known(&scheme, &cid))
            .await
        {
            Ok(true) => {}
            Ok(false) => return failure(StatusCode::NOT_FOUND, None),
            Err(error) => return failure(error.status, None),
        }
    }
    let content = match location.resolve(&state.content, &work, false).await {
        Ok(content) => content,
        Err(error) => return upstream_error(error, None),
    };
    if content.directory_index && !uri.path().ends_with('/') {
        let url = format!(
            "{}/{}",
            uri.path(),
            uri.query().map_or(String::new(), |q| format!("?{q}"))
        );
        let Ok(value) = HeaderValue::from_str(&url) else {
            return failure(StatusCode::BAD_REQUEST, None);
        };
        let mut response = StatusCode::PERMANENT_REDIRECT.into_response();
        response.headers_mut().insert(header::LOCATION, value);
        return response;
    }
    content_response(&state, content, headers, work, true).await
}

async fn content_response(
    state: &ArtworkState,
    mut content: Content,
    headers: HeaderMap,
    work: Work,
    immutable: bool,
) -> Response {
    let gzip = content.content_encoding.as_deref() == Some("gzip");
    let accepts_gzip = headers
        .get(header::ACCEPT_ENCODING)
        .and_then(|v| v.to_str().ok())
        .is_some_and(accepts_gzip);
    if gzip && !accepts_gzip {
        let permit = match work.transform().await {
            Ok(permit) => permit,
            Err(error) => return upstream_error(error, None),
        };
        content.body = match inflate(content.body, state.max_bytes, permit).await {
            Ok(bytes) => bytes,
            Err(error) => return upstream_error(error, None),
        };
        content.content_encoding = None;
        content.delivery_accounted = false;
        content.etag = format!("\"{}-identity\"", content.etag.trim_matches('"'));
    }
    let matched = headers
        .get(header::IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|values| {
            values.split(',').any(|value| {
                let value = value.trim();
                value == "*" || value.trim_start_matches("W/") == content.etag
            })
        });
    let verified = content.verification.is_verified();
    let size = content.body.len();
    let range = if verified
        && !matched
        && headers.get(header::IF_RANGE).is_none_or(|value| {
            value
                .to_str()
                .is_ok_and(|value| value == content.etag && !value.starts_with("W/"))
        }) {
        headers
            .get(header::RANGE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| byte_range(value, size))
    } else {
        None
    };
    let mut accounted = content.delivery_accounted;
    let mut response = if verified && matched {
        StatusCode::NOT_MODIFIED.into_response()
    } else if let Some(range) = range {
        match range {
            Ok((start, end)) => {
                // Do not retain a whole object behind a tiny slice charged to delivery.
                let body = Bytes::copy_from_slice(&content.body[start..=end]);
                accounted = false;
                let mut response = (StatusCode::PARTIAL_CONTENT, body).into_response();
                response.headers_mut().insert(
                    header::CONTENT_RANGE,
                    HeaderValue::from_str(&format!("bytes {start}-{end}/{size}")).unwrap(),
                );
                response
            }
            Err(()) => {
                let mut response = StatusCode::RANGE_NOT_SATISFIABLE.into_response();
                response.headers_mut().insert(
                    header::CONTENT_RANGE,
                    HeaderValue::from_str(&format!("bytes */{size}")).unwrap(),
                );
                response
            }
        }
    } else {
        content.body.into_response()
    };
    if accounted && response.status() == StatusCode::OK {
        response.extensions_mut().insert(AccountedBody);
    }
    let unsatisfied = response.status() == StatusCode::RANGE_NOT_SATISFIABLE;
    let headers = response.headers_mut();
    headers.insert(header::VARY, HeaderValue::from_static("Accept-Encoding"));
    if verified {
        headers.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    }
    if let Ok(value) = HeaderValue::from_str(&content.etag) {
        headers.insert(header::ETAG, value);
    }
    if let Some(value) = content
        .content_type
        .and_then(|v| HeaderValue::from_str(&v).ok())
    {
        headers.insert(header::CONTENT_TYPE, value);
    }
    if let Some(value) = content
        .content_encoding
        .filter(|_| !unsatisfied)
        .and_then(|v| HeaderValue::from_str(&v).ok())
    {
        headers.insert(header::CONTENT_ENCODING, value);
    }
    if verified {
        headers.insert(
            "x-seeded-verification",
            HeaderValue::from_static(content.verification.as_str()),
        );
    }
    headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static(if verified && immutable && !unsatisfied {
            "public, max-age=31536000, immutable"
        } else {
            "no-store"
        }),
    );
    response
}

/// Unsupported/malformed ranges are ignored; a valid but empty range is 416.
fn byte_range(value: &str, size: usize) -> Option<Result<(usize, usize), ()>> {
    let (unit, value) = value.split_once('=')?;
    if !unit.eq_ignore_ascii_case("bytes") || size == 0 {
        return None;
    }
    let value = value.trim();
    if value.contains(',') {
        return None;
    }
    let (start, end) = value.split_once('-')?;
    let number = |value: &str| {
        if value.is_empty() || !value.bytes().all(|b| b.is_ascii_digit()) {
            None
        } else {
            value.parse::<u64>().ok()
        }
    };
    let size = size as u64;
    let (start, end) = if start.is_empty() {
        let suffix = number(end)?;
        if suffix == 0 || size == 0 {
            return Some(Err(()));
        }
        (size.saturating_sub(suffix), size - 1)
    } else {
        let start = number(start)?;
        let end = if end.is_empty() {
            size.saturating_sub(1)
        } else {
            number(end)?
        };
        if !value.ends_with('-') && end < start {
            return None;
        }
        if start >= size {
            return Some(Err(()));
        }
        (start, end.min(size - 1))
    };
    Some(Ok((start as usize, end as usize)))
}

async fn article_media(
    State(state): State<ArtworkState>,
    Path((id, slot)): Path<(String, usize)>,
    Extension(work): Extension<Work>,
    headers: HeaderMap,
) -> Response {
    let Ok(id) = super::numeric_id(&id) else {
        return failure(StatusCode::BAD_REQUEST, None);
    };
    let Some(path) = state.app.articles_database.clone() else {
        return failure(StatusCode::SERVICE_UNAVAILABLE, None);
    };
    let uri = state
        .query(move |_| {
            let articles = crate::articles::store::ArticleStore::open_read_only(&path)
                .map_err(super::articles::map_error)?;
            let article = articles.article(id).map_err(super::articles::map_error)?;
            Ok(match slot {
                0 => article.summary.thumbnail_uri,
                1 => article.display_uri,
                n => article
                    .media
                    .get(n - 2)
                    .map(|m| m.ipfs.clone().unwrap_or_else(|| m.src.clone())),
            })
        })
        .await;
    match uri {
        Ok(Some(uri)) => passive_media(&state, &uri, headers, work).await,
        Ok(None) => failure(StatusCode::NOT_FOUND, None),
        Err(error) => failure(error.status, None),
    }
}

async fn profile_media(
    State(state): State<ArtworkState>,
    Path(address): Path<String>,
    Extension(work): Extension<Work>,
    headers: HeaderMap,
) -> Response {
    match state
        .query(move |store| store.profile(&address).map(|p| p.avatar_uri))
        .await
    {
        Ok(Some(uri)) if avatar_supported(&uri) => {
            if let Err(error) = work.upstream().await {
                return upstream_error(error, None);
            }
            match state.avatars.get(&uri, state.max_bytes).await {
                Ok(content) => {
                    let mut response =
                        content_response(&state, content, headers, work, false).await;
                    response.extensions_mut().insert(PassiveMedia);
                    response
                }
                Err(error) => upstream_error(error, None),
            }
        }
        Ok(Some(uri)) => passive_media(&state, &uri, headers, work).await,
        Ok(None) => failure(StatusCode::NOT_FOUND, None),
        Err(error) => failure(error.status, None),
    }
}

async fn passive_media(
    state: &ArtworkState,
    uri: &str,
    headers: HeaderMap,
    work: Work,
) -> Response {
    let normalized = crate::articles::parse::ipfs_uri(uri).unwrap_or_else(|| uri.to_owned());
    let location = match Location::parse(&normalized) {
        Ok(location) => location,
        Err(_) => return failure(StatusCode::NOT_FOUND, None),
    };
    let mut content = match location.resolve(&state.content, &work, true).await {
        Ok(content) => content,
        Err(error) => return upstream_error(error, None),
    };
    let kind = content.content_type.as_deref().unwrap_or("");
    if !kind.starts_with("image/") && !kind.starts_with("audio/") && !kind.starts_with("video/") {
        let Some(kind) = sniff_media(&content.body) else {
            return failure(StatusCode::UNSUPPORTED_MEDIA_TYPE, None);
        };
        content.content_type = Some(kind.into());
    }
    // The route points at mutable profile/article metadata: always recheck its visibility.
    let mut response = content_response(state, content, headers, work, false).await;
    response.extensions_mut().insert(PassiveMedia);
    response
}

use crate::content::media::sniff_media;

fn accepts_gzip(value: &str) -> bool {
    value.split(',').any(|entry| {
        let mut fields = entry.trim().split(';');
        fields
            .next()
            .is_some_and(|name| name.trim().eq_ignore_ascii_case("gzip"))
            && fields.all(|field| {
                field
                    .trim()
                    .strip_prefix("q=")
                    .is_none_or(|q| q.parse::<f32>().is_ok_and(|q| q > 0.0 && q <= 1.0))
            })
    })
}

async fn inflate(
    body: Bytes,
    maximum: u64,
    permit: Arc<OwnedSemaphorePermit>,
) -> Result<Bytes, ContentError> {
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let decoder = flate2::read::MultiGzDecoder::new(body.as_ref());
        let mut output = Vec::new();
        decoder
            .take(maximum + 1)
            .read_to_end(&mut output)
            .map_err(|_| ContentError::new(ErrorKind::Invalid, "invalid gzip"))?;
        if output.len() as u64 > maximum {
            return Err(ContentError::new(
                ErrorKind::TooLarge,
                "decoded object limit",
            ));
        }
        Ok(Bytes::from(output))
    })
    .await
    .map_err(|_| ContentError::new(ErrorKind::Unavailable, "decoder unavailable"))?
}

async fn render(
    State(state): State<ArtworkState>,
    Path((contract, id)): Path<(String, String)>,
    OriginalUri(uri): OriginalUri,
    Extension(PlaybackClient(client)): Extension<PlaybackClient>,
    Extension(work): Extension<Work>,
) -> Response {
    let Some((canonical_query, instance)) = uri
        .query()
        .and_then(|query| query.rsplit_once("&__seeded_instance="))
    else {
        return failure(StatusCode::BAD_REQUEST, None);
    };
    if instance.len() != 32 || !instance.bytes().all(|b| b.is_ascii_hexdigit()) {
        return failure(StatusCode::BAD_REQUEST, None);
    }
    let Ok(id) = super::numeric_id(&id) else {
        return failure(StatusCode::BAD_REQUEST, Some(instance));
    };
    let token = match state.query(move |store| store.token(&contract, id)).await {
        Ok(token) => token,
        Err(error) => return failure(error.status, Some(instance)),
    };
    let expected = match player::render_url(&state.content_origin, &token) {
        Ok(url) => url,
        Err(_) => return failure(StatusCode::BAD_GATEWAY, Some(instance)),
    };
    let expected_query = expected
        .split('#')
        .next()
        .and_then(|url| url.split_once('?'))
        .map(|(_, query)| query);
    if expected_query != Some(canonical_query) {
        return failure(StatusCode::BAD_REQUEST, Some(instance));
    }
    let location = match Location::parse(&token.artifact_uri) {
        Ok(location) => location,
        Err(error) => return upstream_error(error, Some(instance)),
    };
    let content = match location.resolve(&state.content, &work, false).await {
        Ok(content) => content,
        Err(error) => return upstream_error(error, Some(instance)),
    };
    let base = location.base(&state.content_origin, content.directory_index);
    let verified = content.verification.is_verified();
    let permit = match work.transform().await {
        Ok(permit) => permit,
        Err(error) => return upstream_error(error, Some(instance)),
    };
    let bytes = match content.content_encoding.as_deref() {
        Some("gzip") => match inflate(content.body, state.max_bytes, permit.clone()).await {
            Ok(body) => body,
            Err(error) => return upstream_error(error, Some(instance)),
        },
        None | Some("identity") => content.body,
        _ => return failure(StatusCode::BAD_GATEWAY, Some(instance)),
    };
    let Ok(html) = std::str::from_utf8(&bytes) else {
        return failure(StatusCode::BAD_GATEWAY, Some(instance));
    };
    match player::prepare_html(player::PrepareRequest {
        html,
        base_url: &base,
        instance,
    }) {
        Ok(document) => {
            if let Ok(mut records) = state.playback.lock() {
                records.record(
                    client,
                    &token.contract,
                    token.token_id,
                    instance,
                    verified,
                    Instant::now(),
                );
            }
            Html(document.html).into_response()
        }
        Err(_) => failure(StatusCode::BAD_GATEWAY, Some(instance)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct DatabaseFixture(std::path::PathBuf);
    impl Drop for DatabaseFixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn database_pool_fixture() -> (DatabaseFixture, AppState, ArtworkState) {
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        let fixture = DatabaseFixture(root.join(format!(
            "tmp/artwork-pools-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        )));
        std::fs::create_dir_all(&fixture.0).unwrap();
        let mut config = Config::from_values(&BTreeMap::new()).unwrap();
        config.database = fixture.0.join("archive.sqlite");
        config.assets = root.join("assets");
        let mut store = crate::store::Store::open(&config.database).unwrap();
        crate::import::import_fixtures(&mut store, &config.assets.join("fixtures")).unwrap();
        drop(store);
        let app = AppState::new(&config.database, &config.assets).configured(&config);
        let mut content = crate::content::ContentConfig::mainnet(fixture.0.join("cache"));
        content.tzkt_base = "http://127.0.0.1:1".into();
        content.ipfs_gateways.clear();
        let art = ArtworkState::new(app.clone(), ContentStore::new(content).unwrap(), &config);
        (fixture, app, art)
    }

    fn work_for(
        counters: Arc<ArtworkCounters>,
        responses: usize,
        waiting: usize,
        transforms: usize,
    ) -> Work {
        Work {
            responses: Arc::new(Semaphore::new(responses)),
            waiting: Arc::new(Semaphore::new(waiting)),
            transforms: Arc::new(Semaphore::new(transforms)),
            counters,
            upstream: Arc::new(tokio::sync::OnceCell::new()),
        }
    }

    #[tokio::test]
    async fn ninth_metadata_query_waits_past_the_old_inner_timeout() {
        let (_fixture, _, state) = database_pool_fixture();
        let held = state
            .metadata_reads
            .clone()
            .acquire_many_owned(8)
            .await
            .unwrap();
        let next = state.clone();
        let query = tokio::spawn(async move { next.query(|_| Ok(42)).await });
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert!(!query.is_finished());
        drop(held);
        assert_eq!(
            query
                .await
                .unwrap()
                .unwrap_or_else(|_| panic!("queued lookup failed")),
            42
        );
        assert_eq!(
            state.metrics_handle().snapshot().metadata_waiting_timeout,
            0
        );
    }

    #[tokio::test]
    async fn cancelled_artwork_query_holds_its_worker_slot_until_the_worker_finishes() {
        let (_fixture, _, state) = database_pool_fixture();
        let (started, ready) = tokio::sync::oneshot::channel();
        let (release, blocked) = std::sync::mpsc::channel();
        let querying = state.clone();
        let request = tokio::spawn(async move {
            querying
                .query(move |_| {
                    started.send(()).unwrap();
                    blocked.recv().unwrap();
                    Ok(())
                })
                .await
        });
        ready.await.unwrap();
        request.abort();
        assert!(matches!(request.await, Err(error) if error.is_cancelled()));
        assert_eq!(state.metadata_reads.available_permits(), 7);
        release.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while state.metadata_reads.available_permits() != 8 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(state.query(|_| Ok(())).await.is_ok());
    }

    #[tokio::test]
    async fn reused_artwork_connections_see_commits_and_refuse_a_missing_database() {
        let (fixture, _, state) = database_pool_fixture();
        let pool = &state.app.reused_reads;
        for _ in 0..3 {
            assert!(state.query(|store| store.project(29437)).await.is_ok());
        }
        assert_eq!(pool.opened(), 1);
        let writer = rusqlite::Connection::open(&*state.app.database).unwrap();
        writer
            .execute("UPDATE projects SET hidden=1 WHERE id=29437", [])
            .unwrap();
        let error = state
            .query(|store| store.project(29437))
            .await
            .err()
            .unwrap();
        assert_eq!(error.status, StatusCode::NOT_FOUND);
        writer
            .execute("UPDATE projects SET hidden=0 WHERE id=29437", [])
            .unwrap();
        assert!(state.query(|store| store.project(29437)).await.is_ok());
        let moved = fixture.0.join("moved.sqlite");
        std::fs::rename(&*state.app.database, &moved).unwrap();
        assert_eq!(
            state.query(|_| Ok(())).await.err().unwrap().status,
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            state.metrics_handle().snapshot().metadata_query_unavailable,
            1
        );
        std::fs::rename(&moved, &*state.app.database).unwrap();
        assert!(state.query(|store| store.project(29437)).await.is_ok());
    }

    #[tokio::test]
    async fn handler_deadline_counts_once_without_charging_delivery() {
        use tower::ServiceExt;
        let (_fixture, _, mut state) = database_pool_fixture();
        state.handler_timeout = Duration::from_millis(10);
        let routes = Router::new()
            .route("/stalled", get(std::future::pending::<StatusCode>))
            .layer(axum::middleware::from_fn_with_state(
                state.clone(),
                bounded_response,
            ));
        let response = routes
            .oneshot(Request::get("/stalled").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let counters = state.metrics_handle().snapshot();
        assert_eq!(counters.handler_timeout, 1);
        assert_eq!(counters.delivery_global_bytes_full, 0);
        assert_eq!(counters.delivery_client_bytes_full, 0);
    }

    #[tokio::test]
    async fn metadata_worker_timeout_is_counted_and_keeps_its_slot_until_completion() {
        let (_fixture, _, mut state) = database_pool_fixture();
        state.app.query_timeout = Duration::from_millis(20);
        let (started, ready) = tokio::sync::oneshot::channel();
        let (release, blocked) = std::sync::mpsc::channel();
        let querying = state.clone();
        let request = tokio::spawn(async move {
            querying
                .query(move |_| {
                    let _ = started.send(());
                    let _ = blocked.recv_timeout(Duration::from_secs(2));
                    Ok(())
                })
                .await
        });
        ready.await.unwrap();
        assert_eq!(
            request.await.unwrap().err().unwrap().status,
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(state.metrics_handle().snapshot().metadata_query_timeout, 1);
        assert_eq!(state.metadata_reads.available_permits(), 7);
        release.send(()).unwrap();
        let all = tokio::time::timeout(
            Duration::from_secs(2),
            state.metadata_reads.clone().acquire_many_owned(8),
        )
        .await
        .unwrap()
        .unwrap();
        drop(all);
        state.app.query_timeout = Duration::from_secs(5);
        assert!(state.query(|_| Ok(())).await.is_ok());
        assert_eq!(state.metrics_handle().snapshot().metadata_query_timeout, 1);
    }

    #[tokio::test]
    async fn app_database_rejections_are_measured_separately_and_recover() {
        let (_fixture, mut app, art) = database_pool_fixture();
        let counters = app.database_metrics_handle();
        let waiting = app
            .waiting
            .clone()
            .acquire_many_owned(super::super::MAX_WAITING_READS as u32)
            .await
            .unwrap();
        assert_eq!(
            app.query(|_| Ok(())).await.err().unwrap().status,
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(counters.snapshot().waiting_full, 1);
        drop(waiting);
        let reads = app.reads.clone().acquire_many_owned(8).await.unwrap();
        assert_eq!(
            app.query(|_| Ok(())).await.err().unwrap().status,
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(counters.snapshot().waiting_timeout, 1);
        drop(reads);
        app.query_timeout = Duration::from_millis(20);
        let (started, ready) = tokio::sync::oneshot::channel();
        let (release, blocked) = std::sync::mpsc::channel();
        let next = app.clone();
        let request = tokio::spawn(async move {
            next.query(move |_| {
                let _ = started.send(());
                let _ = blocked.recv_timeout(Duration::from_secs(2));
                Ok(())
            })
            .await
        });
        ready.await.unwrap();
        assert_eq!(
            request.await.unwrap().err().unwrap().status,
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(counters.snapshot().query_timeout, 1);
        release.send(()).unwrap();
        let all = tokio::time::timeout(
            Duration::from_secs(2),
            app.reads.clone().acquire_many_owned(8),
        )
        .await
        .unwrap()
        .unwrap();
        drop(all);
        app.query_timeout = Duration::from_secs(5);
        assert!(app.query(|_| Ok(())).await.is_ok());
        assert!(
            app.query::<(), _>(|_| Err(crate::store::StoreError::Unavailable("test".into())))
                .await
                .is_err()
        );
        assert_eq!(counters.snapshot().query_unavailable, 1);
        assert_eq!(art.metrics_handle().snapshot().metadata_waiting_timeout, 0);
        assert_eq!(
            art.app.database_metrics_handle().snapshot().waiting_timeout,
            0
        );
    }

    #[tokio::test]
    async fn fixed_errors_keep_status_when_artwork_delivery_is_full() {
        use tower::ServiceExt;
        let (_fixture, _, state) = database_pool_fixture();
        let held: Vec<_> = (1..=8)
            .map(|n| {
                state
                    .delivery
                    .reserve(std::net::Ipv4Addr::new(192, 0, 2, n).into(), 64 << 20)
                    .unwrap()
            })
            .collect();
        let routes = Router::new()
            .route(
                "/reject",
                get(|| async { failure(StatusCode::SERVICE_UNAVAILABLE, None) }),
            )
            .layer(axum::middleware::from_fn_with_state(
                state.clone(),
                bounded_response,
            ));
        let response = routes
            .oneshot(Request::get("/reject").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers()[header::ACCESS_CONTROL_ALLOW_ORIGIN], "*");
        assert!(response.headers().contains_key(header::RETRY_AFTER));
        assert_eq!(
            state.metrics_handle().snapshot().delivery_global_bytes_full,
            0
        );
        drop(held);
    }

    #[tokio::test]
    async fn known_roots_skip_the_metadata_pool_and_unknown_roots_still_use_it() {
        use tower::ServiceExt;
        const KNOWN: &str = "bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy";
        const UNKNOWN: &str = "bafkreidivzimqfqtoqxkrpge6bjyhlvxqs3rhe73owtmdulaxr5do5in7u";
        let (_fixture, _, mut state) = database_pool_fixture();
        rusqlite::Connection::open(&*state.app.database)
            .unwrap()
            .execute(
                "INSERT INTO content_roots(scheme,cid,project_id) VALUES('ipfs',?1,29437)",
                [KNOWN],
            )
            .unwrap();
        let get = |state: ArtworkState, cid: &str| {
            let request = Request::get(format!("/content/ipfs/{cid}"))
                .extension(crate::auth::TrustedClientIp(
                    std::net::Ipv4Addr::new(192, 0, 2, 1).into(),
                ))
                .body(Body::empty())
                .unwrap();
            async move { router(state).oneshot(request).await.unwrap().status() }
        };
        // The first request answers from the database while the index loads.
        get(state.clone(), KNOWN).await;
        state.roots.settle().await;
        state.metadata_waiting = Arc::new(Semaphore::new(0));
        let known = get(state.clone(), KNOWN).await;
        assert_eq!(
            state.counters.metadata_waiting_full.load(Ordering::Relaxed),
            0
        );
        assert_ne!(known, StatusCode::NOT_FOUND);
        assert_eq!(
            get(state.clone(), UNKNOWN).await,
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            state.counters.metadata_waiting_full.load(Ordering::Relaxed),
            1
        );
    }

    #[tokio::test]
    async fn metadata_limiter_counters_measure_full_and_timeout_then_recover() {
        let (_fixture, _, mut state) = database_pool_fixture();
        let handle = state.metrics_handle();
        state.metadata_waiting = Arc::new(Semaphore::new(0));
        assert!(
            state
                .query_with_timeout(
                    |_| Ok::<(), crate::store::StoreError>(()),
                    Duration::from_millis(1),
                )
                .await
                .is_err()
        );
        let full = handle.snapshot();
        assert_eq!(full.metadata_waiting_full, 1);
        assert_eq!(full.metadata_waiting_timeout, 0);

        state.metadata_waiting.add_permits(1);
        state
            .query_with_timeout(
                |_| Ok::<(), crate::store::StoreError>(()),
                Duration::from_millis(1),
            )
            .await
            .unwrap_or_else(|_| panic!("metadata read should recover"));
        assert_eq!(handle.snapshot().metadata_waiting_full, 1);

        let reads = state
            .metadata_reads
            .clone()
            .acquire_many_owned(super::super::MAX_DATABASE_READS as u32)
            .await
            .unwrap_or_else(|_| panic!("metadata read should recover"));
        assert!(
            state
                .query_with_timeout(
                    |_| Ok::<(), crate::store::StoreError>(()),
                    Duration::from_millis(1),
                )
                .await
                .is_err()
        );
        assert_eq!(handle.snapshot().metadata_waiting_timeout, 1);
        drop(reads);
        state
            .query_with_timeout(
                |_| Ok::<(), crate::store::StoreError>(()),
                Duration::from_millis(1),
            )
            .await
            .unwrap_or_else(|_| panic!("metadata read should recover"));
        assert_eq!(handle.snapshot().metadata_waiting_timeout, 1);
        let json = serde_json::to_value(handle.snapshot()).unwrap();
        assert_eq!(json["metadata_waiting_full"], 1);
        assert_eq!(json["metadata_waiting_timeout"], 1);
    }

    #[tokio::test]
    async fn upstream_queue_and_transform_counters_measure_rejections_then_recover() {
        let counters = Arc::new(ArtworkCounters::default());
        let handle = ArtworkMetricsHandle {
            counters: counters.clone(),
        };

        let full = work_for(counters.clone(), 1, 0, 1);
        assert!(
            full.upstream_with_timeout(Duration::from_millis(1))
                .await
                .is_err()
        );
        assert_eq!(handle.snapshot().upstream_waiting_full, 1);
        full.waiting.add_permits(1);
        drop(
            full.upstream_with_timeout(Duration::from_millis(1))
                .await
                .unwrap(),
        );
        assert_eq!(handle.snapshot().upstream_waiting_full, 1);

        let timed_out = work_for(counters.clone(), 0, 1, 1);
        assert!(
            timed_out
                .upstream_with_timeout(Duration::from_millis(1))
                .await
                .is_err()
        );
        assert_eq!(handle.snapshot().upstream_waiting_timeout, 1);
        timed_out.responses.add_permits(1);
        drop(
            timed_out
                .upstream_with_timeout(Duration::from_millis(1))
                .await
                .unwrap(),
        );
        assert_eq!(handle.snapshot().upstream_waiting_timeout, 1);

        let transform = work_for(counters, 1, 1, 0);
        assert!(
            transform
                .transform_with_timeout(Duration::from_millis(1))
                .await
                .is_err()
        );
        assert_eq!(handle.snapshot().transform_timeout, 1);
        transform.transforms.add_permits(1);
        drop(
            transform
                .transform_with_timeout(Duration::from_millis(1))
                .await
                .unwrap(),
        );
        assert_eq!(handle.snapshot().transform_timeout, 1);
    }

    #[tokio::test]
    async fn artwork_resolution_counts_a_local_miss_once_before_upstream_failure() {
        use sha2::{Digest, Sha256};
        let (_fixture, _, state) = database_pool_fixture();
        let cid = cid::Cid::new_v1(
            0x55,
            cid::multihash::Multihash::wrap(0x12, &Sha256::digest(b"not cached")).unwrap(),
        );
        let location = Location {
            scheme: "ipfs".into(),
            cid: cid.to_string(),
            path: String::new(),
        };
        for (index, passive) in [false, true].into_iter().enumerate() {
            let work = Work {
                responses: state.responses.clone(),
                waiting: state.waiting.clone(),
                transforms: state.transforms.clone(),
                counters: state.counters.clone(),
                upstream: Arc::new(tokio::sync::OnceCell::new()),
            };
            assert!(
                location
                    .resolve(&state.content, &work, passive)
                    .await
                    .is_err()
            );
            let metrics = state.content.metrics();
            assert_eq!(metrics.cache.misses, index as u64 + 1);
            assert_eq!(metrics.cache.hits, 0);
        }
    }

    #[tokio::test]
    async fn cancelled_upstream_wait_releases_admission_and_local_preparation_stays_available() {
        let work = Work {
            responses: Arc::new(Semaphore::new(1)),
            waiting: Arc::new(Semaphore::new(1)),
            transforms: Arc::new(Semaphore::new(1)),
            counters: Arc::new(ArtworkCounters::default()),
            upstream: Arc::new(tokio::sync::OnceCell::new()),
        };
        let occupied = work.responses.clone().acquire_owned().await.unwrap();
        let pending = work.clone();
        let task = tokio::spawn(async move { pending.upstream().await });
        tokio::time::timeout(Duration::from_secs(2), async {
            while work.waiting.available_permits() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        drop(work.transform().await.unwrap());
        assert_eq!(work.transforms.available_permits(), 1);
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(work.waiting.available_permits(), 1);
        drop(occupied);
        let permit = work.upstream().await.unwrap();
        assert!(Arc::ptr_eq(&permit, &work.upstream().await.unwrap()));
        let responses = work.responses.clone();
        drop(work);
        assert_eq!(responses.available_permits(), 0);
        drop(permit);
        assert_eq!(responses.available_permits(), 1);
    }

    #[tokio::test]
    async fn twelve_viewers_receive_one_large_retained_body_and_ranges_stay_separately_charged() {
        use sha2::{Digest, Sha256};
        use tower::ServiceExt;
        let (fixture, _, mut state) = database_pool_fixture();
        let body = vec![7; 68 << 20];
        let cid = cid::Cid::new_v1(
            0x55,
            cid::multihash::Multihash::wrap(0x12, &Sha256::digest(&body)).unwrap(),
        );
        let key = hex::encode(cid.to_bytes());
        let mirror = fixture.0.join("large-mirror");
        let file = mirror.join("ipfs").join(&key[key.len() - 2..]).join(key);
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        std::fs::write(file, body).unwrap();
        let mut config = crate::content::ContentConfig::mainnet(fixture.0.join("large-cache"));
        config.max_object_bytes = 96 << 20;
        config.mirror_dir = Some(mirror);
        config.ipfs_gateways.clear();
        state.content = ContentStore::new(config).unwrap();
        state.delivery = delivery::Budget::new(96 << 20, state.counters.clone());
        let connection = rusqlite::Connection::open(&*state.app.database).unwrap();
        connection
            .execute(
                "INSERT INTO content_roots(scheme,cid,project_id) VALUES('ipfs',?1,29437)",
                [cid.to_string()],
            )
            .unwrap();
        let app = router(state.clone());
        let uri = format!("/content/ipfs/{cid}");
        let mut responses = Vec::new();
        for n in 1..=12 {
            let response = app
                .clone()
                .oneshot(
                    Request::get(&uri)
                        .extension(crate::auth::TrustedClientIp(
                            std::net::Ipv4Addr::new(192, 0, 2, n).into(),
                        ))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            responses.push(response);
        }
        assert_eq!(state.content.metrics().local_delivery.bytes_used, 68 << 20);
        assert_eq!(
            state
                .counters
                .delivery_global_bytes_full
                .load(Ordering::Relaxed),
            0
        );
        let range = app
            .oneshot(
                Request::get(&uri)
                    .header(header::RANGE, "bytes=0-4095")
                    .extension(crate::auth::TrustedClientIp(
                        std::net::Ipv4Addr::new(192, 0, 2, 13).into(),
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(range.status(), StatusCode::PARTIAL_CONTENT);
        assert!(range.extensions().get::<AccountedBody>().is_none());
        drop(responses);
        assert_eq!(state.content.metrics().local_delivery.bytes_used, 0);
    }

    #[tokio::test]
    async fn mirror_and_cache_hits_bypass_saturated_assembly_and_waiting_pools() {
        use crate::content::{ContentConfig, onchfs};
        use http_body_util::BodyExt;
        use sha2::{Digest, Sha256};
        use tower::ServiceExt;
        const BODY: &[u8] = b"<!doctype html><html><body>local</body></html>";
        for (scheme, mirrored) in [("onchfs", true), ("onchfs", false), ("ipfs", true)] {
            let (fixture, _, mut state) = database_pool_fixture();
            let mut config = ContentConfig::mainnet(fixture.0.join("cache"));
            config.tzkt_base = "http://127.0.0.1:1".into();
            config.ipfs_gateways.clear();
            config.max_concurrent_assemblies = 1;
            let (cid, key, record) = if scheme == "onchfs" {
                let cid = hex::encode(onchfs::file_cid(&onchfs::keccak(BODY), &[]));
                let record = onchfs::Record::File {
                    metadata: vec![],
                    content: Bytes::from_static(BODY),
                }
                .encode()
                .into_iter()
                .flatten()
                .collect::<Vec<_>>();
                (cid.clone(), cid, record)
            } else {
                let hash = cid::multihash::Multihash::wrap(0x12, &Sha256::digest(BODY)).unwrap();
                let cid = cid::Cid::new_v1(0x55, hash);
                (cid.to_string(), hex::encode(cid.to_bytes()), BODY.to_vec())
            };
            let path = if mirrored {
                let mirror = fixture.0.join("mirror");
                config.mirror_dir = Some(mirror.clone());
                mirror.join(scheme).join(&key[key.len() - 2..]).join(&key)
            } else {
                config.cache_dir.join(scheme).join(&key[..2]).join(&key)
            };
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path, record).unwrap();
            state.content = ContentStore::new(config.clone()).unwrap();
            let conn = rusqlite::Connection::open(fixture.0.join("archive.sqlite")).unwrap();
            conn.execute(
                "INSERT INTO content_roots(scheme,cid,project_id) VALUES(?1,?2,29437)",
                [scheme, &cid],
            )
            .unwrap();
            let responses = state
                .responses
                .clone()
                .acquire_many_owned(state.responses.available_permits() as u32)
                .await
                .unwrap();
            let waiting = state
                .waiting
                .clone()
                .acquire_many_owned(state.waiting.available_permits() as u32)
                .await
                .unwrap();
            let app = router(state.clone());
            let uri = format!("/content/{scheme}/{cid}");
            // First access is a cold local read, while every upstream slot is held.
            let barrier = Arc::new(tokio::sync::Barrier::new(150));
            let mut tasks = tokio::task::JoinSet::new();
            for _ in 0..150 {
                let (app, uri, barrier) = (app.clone(), uri.clone(), barrier.clone());
                tasks.spawn(async move {
                    barrier.wait().await;
                    let response = app
                        .oneshot(Request::get(uri).body(Body::empty()).unwrap())
                        .await
                        .unwrap();
                    assert_eq!(response.status(), StatusCode::OK);
                    assert!(response.headers().contains_key("x-seeded-verification"));
                    assert_eq!(
                        response.into_body().collect().await.unwrap().to_bytes(),
                        BODY
                    );
                });
            }
            tokio::time::timeout(Duration::from_secs(4), async {
                while let Some(task) = tasks.join_next().await {
                    task.unwrap();
                }
            })
            .await
            .unwrap();
            assert_eq!(state.responses.available_permits(), 0);
            assert_eq!(state.waiting.available_permits(), 0);
            std::fs::write(&path, b"corrupt disk bytes").unwrap();
            // Retained verified bytes cannot be changed by later disk writes.
            assert_eq!(
                state
                    .content
                    .get_local(scheme, &cid, "")
                    .await
                    .unwrap()
                    .unwrap()
                    .body,
                BODY
            );
            let fresh = ContentStore::new(config).unwrap();
            let local = fresh.get_local(scheme, &cid, "").await.unwrap();
            match (scheme, mirrored) {
                ("ipfs", true) => {
                    let content = local.expect("mirrored IPFS bytes are served as stored");
                    assert_eq!(content.body.as_ref(), b"corrupt disk bytes");
                    assert_eq!(content.verification, crate::content::Verification::IpfsCar);
                }
                ("onchfs", true) => {
                    assert!(local.is_none(), "malformed mirror record is rejected");
                }
                (_, false) => {
                    assert!(local.is_none(), "modified cached bytes fail validation");
                }
                _ => unreachable!(),
            }
            conn.execute("UPDATE projects SET hidden=1 WHERE id=29437", [])
                .unwrap();
            // Hiding takes effect at the root index's next check.
            state.roots.settle().await;
            state.roots.expire();
            expect_pool_response(&app, &uri, StatusCode::NOT_FOUND).await;
            state.roots.settle().await;
            expect_pool_response(&app, &uri, StatusCode::NOT_FOUND).await;
            drop((responses, waiting));
        }
    }

    #[tokio::test]
    async fn indexed_https_profile_media_uses_only_the_bounded_avatar_transport() {
        use http_body_util::BodyExt;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tower::ServiceExt;
        let (fixture, _, mut state) = database_pool_fixture();
        let address = "tz1gL9H53BzjDqmMi2JkKALeCzT7UAoopzEL";
        let uri =
            format!("https://assets.objkt.media/file/assets-004/h/{address}/logo?cb=0123abcd");
        let conn = rusqlite::Connection::open(fixture.0.join("archive.sqlite")).unwrap();
        conn.execute("INSERT INTO profiles_imported(address,source,avatar_uri,fetched_at) VALUES(?1,'objkt',?2,'2026-10-05')",[address,&uri]).unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let counter = calls.clone();
        let mock = Router::new().fallback(move || {
            let counter = counter.clone();
            async move {
                counter.fetch_add(1, Ordering::Relaxed);
                (StatusCode::OK, &b"\x89PNG\r\n\x1a\n"[..])
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        state.avatars = avatar::AvatarClient::mock(
            format!("http://{}", listener.local_addr().unwrap())
                .parse()
                .unwrap(),
        );
        let server = tokio::spawn(async move {
            axum::serve(listener, mock).await.unwrap();
        });
        let app = router(state);
        let path = format!("/profile-media/{address}");
        let response = app
            .clone()
            .oneshot(Request::get(&path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[header::CONTENT_TYPE], "image/png");
        assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
        assert!(
            response.headers()[header::CONTENT_SECURITY_POLICY]
                .to_str()
                .unwrap()
                .contains("script-src 'none'")
        );
        assert!(!response.headers().contains_key("x-seeded-verification"));
        let etag = response.headers()[header::ETAG].clone();
        assert_eq!(
            &response.into_body().collect().await.unwrap().to_bytes()[..],
            b"\x89PNG\r\n\x1a\n"
        );
        for unsupported in [
            None,
            Some("https://127.0.0.1/private"),
            Some("https://other.example/avatar"),
        ] {
            conn.execute(
                "UPDATE profiles_imported SET avatar_uri=?1 WHERE address=?2",
                rusqlite::params![unsupported, address],
            )
            .unwrap();
            let response = app
                .clone()
                .oneshot(
                    Request::get(&path)
                        .header(header::IF_NONE_MATCH, &etag)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::NOT_FOUND);
        }
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        server.abort();
        let _ = server.await;
    }

    async fn expect_pool_response(router: &Router, path: &str, expected: StatusCode) {
        use tower::ServiceExt;
        // Longer than the two-second page wait for a database slot.
        let response = tokio::time::timeout(
            Duration::from_secs(5),
            router
                .clone()
                .oneshot(Request::get(path).body(Body::empty()).unwrap()),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(response.status(), expected);
    }

    async fn check_pool_isolation(block_artwork: bool) {
        let (_fixture, app, mut art) = database_pool_fixture();
        art.metadata_timeout = Duration::from_millis(50);
        let app_router = crate::web::router(app.clone());
        let art_router = router(art.clone());
        let resource = format!("/content/onchfs/{}", "0".repeat(64));
        let (blocked, blocked_router, blocked_path, normal, other_router, other_path, other_status) =
            if block_artwork {
                (
                    &art.app,
                    &art_router,
                    resource.as_str(),
                    StatusCode::NOT_FOUND,
                    &app_router,
                    "/projects",
                    StatusCode::OK,
                )
            } else {
                (
                    &app,
                    &app_router,
                    "/projects",
                    StatusCode::OK,
                    &art_router,
                    resource.as_str(),
                    StatusCode::NOT_FOUND,
                )
            };
        // Exhaust worker slots and waiting slots separately, without relying on
        // SQL timing or locking the database shared by both origins.
        for slots in [&blocked.reads, &blocked.waiting] {
            let held = slots
                .clone()
                .acquire_many_owned(slots.available_permits() as u32)
                .await
                .unwrap();
            expect_pool_response(other_router, other_path, other_status).await;
            expect_pool_response(
                blocked_router,
                blocked_path,
                StatusCode::SERVICE_UNAVAILABLE,
            )
            .await;
            drop(held);
            expect_pool_response(blocked_router, blocked_path, normal).await;
        }
    }

    #[tokio::test]
    async fn catalogue_database_saturation_does_not_block_artwork_admission() {
        check_pool_isolation(false).await;
    }

    #[tokio::test]
    async fn artwork_database_saturation_does_not_block_catalogue_reads() {
        check_pool_isolation(true).await;
    }

    #[test]
    fn playback_records_expire_and_stay_bounded_without_cross_frame_claims() {
        let mut records = PlaybackStatus::default();
        let now = Instant::now();
        let ip = "2001:db8:1::1".parse().unwrap();
        let victim = "2001:db8:2::1".parse().unwrap();
        records.record(ip, "contract", 1, "one", true, now);
        records.record(ip, "contract", 1, "two", false, now);
        assert_eq!(records.get("contract", 1, "one", now), Some(true));
        assert_eq!(records.get("contract", 1, "two", now), Some(false));
        assert_eq!(records.get("other", 1, "one", now), None);
        assert_eq!(records.get("contract", 2, "one", now), None);
        assert_eq!(records.get("contract", 1, "one", now + PLAYBACK_TTL), None);
        records.record(victim, "contract", 999, "victim", true, now);
        for token in 0..256 {
            records.record(
                ip,
                "contract",
                token,
                "one",
                true,
                now + Duration::from_millis(token),
            );
        }
        assert_eq!(records.records.len(), PLAYBACK_PER_PREFIX + 1);
        assert_eq!(
            records.get("contract", 999, "victim", now + Duration::from_secs(1)),
            Some(true)
        );
        records.record(ip, "contract", 999, "victim", false, now);
        assert_eq!(records.get("contract", 999, "victim", now), Some(true));
        // Rotating an IPv6 host within the /64 shares the same quota.
        records.record(
            "2001:db8:1::2".parse().unwrap(),
            "contract",
            300,
            "one",
            true,
            now,
        );
        assert_eq!(records.records.len(), PLAYBACK_PER_PREFIX + 1);
        assert_eq!(
            records.get("contract", 0, "one", now + Duration::from_secs(1)),
            None
        );
        assert_eq!(
            records.get("contract", 255, "one", now + Duration::from_secs(1)),
            Some(true)
        );
    }
    #[test]
    fn preview_urls_reuse_resolver_validation_without_rewriting_paths() {
        assert_eq!(
            preview_url(
                "https://art.example/",
                "ipfs://ipfs/QmX/art%20work.png?size=2#preview"
            ),
            Some("https://art.example/content/ipfs/QmX/art%20work.png".into())
        );
        assert_eq!(
            preview_url(
                "https://art.example",
                "onchfs://tezos:NetXdQprcVkpaWU/abcd/nested/image.svg"
            ),
            Some("https://art.example/content/onchfs/abcd/nested/image.svg".into())
        );
        for uri in [
            "https://elsewhere/image.png",
            "file:///image.png",
            "ipfs://user@host/x",
            "ipfs://host:80/x",
            "ipfs:///missing",
            "ipfs://QmX/../x",
            "ipfs://QmX/%2e%2e/x",
            "ipfs://QmX/a%2fb",
            "ipfs://QmX/%zz",
            "onchfs://tezos:other/abcd/x",
        ] {
            assert!(preview_url("https://art.example", uri).is_none(), "{uri}");
        }
        for origin in [
            "//art.example",
            "https://name@art.example",
            "https://art.example/path",
            "https://art.example/?q=x",
            "https://art.example/#x",
            "https://art.example\\evil",
        ] {
            assert!(
                preview_url(origin, "ipfs://QmX/image.png").is_none(),
                "{origin}"
            );
        }
    }

    #[test]
    fn artifacts_only_resolve_content_addresses_with_safe_paths() {
        assert_eq!(
            Location::parse("onchfs://abcd/dir/index.html?fxhash=oo#ff")
                .unwrap()
                .base("https://art.example", false),
            "https://art.example/content/onchfs/abcd/dir/"
        );
        assert_eq!(
            Location::parse("ipfs://abcd/path")
                .unwrap()
                .base("https://art.example", true),
            "https://art.example/content/ipfs/abcd/path/"
        );
        for uri in [
            "https://example.org",
            "file:///secret",
            "ipfs://x/../etc",
            "onchfs://a/%2fetc",
            "ipfs://x@host/",
        ] {
            assert!(Location::parse(uri).is_err(), "{uri}");
        }
    }

    #[test]
    fn explicit_onchfs_authorities_resolve_only_the_supported_tezos_contract() {
        for authority in [
            "tezos",
            "tezos:NetXdQprcVkpaWU",
            "tezos:NetXdQprcVkpaWU:KT1Ae7dT1gsLw2tRnUMXSCmEyF74KVkM6LUo",
        ] {
            assert_eq!(
                Location::parse(&format!("onchfs://{authority}/abcd/index.html")).unwrap(),
                Location::parse("onchfs://abcd/index.html").unwrap()
            );
        }
        for authority in [
            "tezos:NetXnHfVqm9iesp",
            "tezos:NetXdQprcVkpaWU:other",
            "eip155:1",
        ] {
            assert!(Location::parse(&format!("onchfs://{authority}/abcd")).is_err());
        }
    }
    #[test]
    fn gzip_negotiation_honours_zero_quality() {
        assert!(accepts_gzip("br, gzip;q=0.5"));
        assert!(!accepts_gzip("gzip;q=0, deflate"));
        assert!(!accepts_gzip("gzip;q=wrong"));
    }
}
