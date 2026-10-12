mod agent;
pub mod articles;
pub mod artwork;
pub mod auth;
pub mod claims;
mod database;
pub use database::{DatabaseMetricsHandle, DatabaseMetricsSnapshot};
pub mod history;
pub mod market;
mod oauth;
pub mod release;
mod search;
mod seo;
mod sitemap;
pub mod views;

use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use axum::{
    Router,
    extract::{Path, Query, State, rejection::QueryRejection},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    middleware::{self, Next},
    response::{Html, IntoResponse, Redirect, Response},
    routing::get,
};
use serde::Deserialize;
use tokio::sync::Semaphore;
use tower_http::services::ServeDir;

use crate::{
    model::{ArtistRef, ProjectSummary, Token},
    store::{
        Cursor, KeysetPage, ListingFilter, Page, ProjectFilter, ProjectSort, Store, StoreError,
    },
};
use views::{
    AboutPage, ArtistsPage, CollectorPage, ErrorPage, PageInfo, ProfilePage, ProjectPage,
    ProjectsPage, TokenPage, encode,
};

const PAGE_SIZE: u32 = 24;
const MAX_DATABASE_READS: usize = 8;
const MAX_WAITING_READS: usize = 512;
/// A page waits this long for a database read slot before answering 503.
const READ_WAIT: Duration = Duration::from_secs(2);
/// Archive totals and title collisions for page metadata are refreshed this often.
const SITE_FACTS_TTL: Duration = Duration::from_secs(3600);
const SITE_FACTS_RETRY: Duration = Duration::from_secs(60);

#[derive(Default)]
struct SiteCache {
    facts: Arc<seo::SiteFacts>,
    /// When a refresh may next start; `None` before the first.
    due: Option<std::time::Instant>,
    refreshing: bool,
}

#[derive(Clone)]
pub struct AppState {
    pub(crate) database: Arc<PathBuf>,
    pub(crate) auth: Option<crate::auth::AuthState>,
    pub(crate) oauth: Option<Arc<crate::auth::oauth::OAuthState>>,
    pub(crate) claims: Option<claims::ClaimRegistry>,
    pub(crate) trading: Option<Arc<crate::trading::TradingState>>,
    release: Option<Arc<crate::release::ReleaseClient>>,
    pub(super) sales_database: Option<PathBuf>,
    pub(super) articles_database: Option<PathBuf>,
    assets: PathBuf,
    app_origin: String,
    reads: Arc<Semaphore>,
    waiting: Arc<Semaphore>,
    reused_reads: Arc<database::ReadPool>,
    read_counters: Arc<database::ReadCounters>,
    query_timeout: Duration,
    content_origin: Option<String>,
    content: Option<crate::content::ContentStore>,
    site: Arc<std::sync::Mutex<SiteCache>>,
}

impl AppState {
    pub fn new(database: impl Into<PathBuf>, assets: impl Into<PathBuf>) -> Self {
        Self {
            database: Arc::new(database.into()),
            auth: None,
            oauth: None,
            claims: None,
            trading: None,
            release: None,
            sales_database: None,
            articles_database: None,
            assets: assets.into(),
            app_origin: "http://127.0.0.1:3918".into(),
            reads: Arc::new(Semaphore::new(MAX_DATABASE_READS)),
            waiting: Arc::new(Semaphore::new(MAX_WAITING_READS)),
            reused_reads: Arc::new(database::ReadPool::default()),
            read_counters: Arc::new(database::ReadCounters::default()),
            query_timeout: Duration::from_secs(5),
            content_origin: None,
            content: None,
            site: Arc::default(),
        }
    }

    /// The current site facts, never waiting: a due refresh runs in the background and the
    /// previous facts (empty before the first load) answer meanwhile.
    pub(super) fn site_facts(&self) -> Arc<seo::SiteFacts> {
        let mut cache = self.site.lock().unwrap_or_else(|e| e.into_inner());
        let now = std::time::Instant::now();
        if !cache.refreshing && cache.due.is_none_or(|due| now >= due) {
            cache.refreshing = true;
            let state = self.clone();
            tokio::spawn(async move {
                let loaded = state.load_site_facts().await;
                let mut cache = state.site.lock().unwrap_or_else(|e| e.into_inner());
                cache.refreshing = false;
                let now = std::time::Instant::now();
                match loaded {
                    Ok(facts) => {
                        cache.facts = Arc::new(facts);
                        cache.due = Some(now + SITE_FACTS_TTL);
                    }
                    Err(_) => cache.due = Some(now + SITE_FACTS_RETRY),
                }
            });
        }
        cache.facts.clone()
    }

    async fn load_site_facts(&self) -> Result<seo::SiteFacts, WebError> {
        let (status, project_suffixes) = self
            .query(|store| Ok((store.index_status()?, store.project_title_suffixes()?)))
            .await?;
        let article_ids = match self.articles_database.clone() {
            Some(path) => tokio::task::spawn_blocking(move || article_title_ids(&path))
                .await
                .map_err(|_| WebError::internal())?
                .map_err(|_| WebError::unavailable())?,
            None => Default::default(),
        };
        Ok(seo::SiteFacts {
            projects: status.projects_total,
            editions: status.tokens_total,
            project_suffixes,
            article_ids,
        })
    }

    pub fn configured(mut self, config: &crate::config::Config) -> Self {
        self.content_origin = Some(config.content_origin.clone());
        self.app_origin = config.app_origin.clone();
        self.sales_database = Some(config.sales_database.clone());
        self.articles_database = Some(config.articles_database.clone());
        self
    }

    pub fn with_release(mut self, release: Arc<crate::release::ReleaseClient>) -> Self {
        self.release = Some(release);
        self
    }

    pub fn with_trading(mut self, trading: Arc<crate::trading::TradingState>) -> Self {
        self.trading = Some(trading);
        self
    }

    pub fn with_auth(mut self, auth: crate::auth::AuthState) -> Self {
        self.auth = Some(auth);
        self
    }

    /// Enables the `/oauth` endpoints; they need `with_auth` to sign anyone in.
    pub fn with_oauth(mut self, oauth: Arc<crate::auth::oauth::OAuthState>) -> Self {
        self.oauth = Some(oauth);
        self
    }

    pub fn with_claim_registry(mut self, claims: claims::ClaimRegistry) -> Self {
        self.claims = Some(claims);
        self
    }

    pub fn with_content(mut self, content: crate::content::ContentStore) -> Self {
        self.content = Some(content);
        self
    }

    fn isolated_database_pool(self) -> Self {
        Self {
            reads: Arc::new(Semaphore::new(MAX_DATABASE_READS)),
            waiting: Arc::new(Semaphore::new(MAX_WAITING_READS)),
            reused_reads: Arc::new(database::ReadPool::default()),
            read_counters: Arc::new(database::ReadCounters::default()),
            sales_database: None,
            ..self
        }
    }

    pub fn database_metrics_handle(&self) -> DatabaseMetricsHandle {
        DatabaseMetricsHandle(self.read_counters.clone())
    }

    pub(super) async fn query<T, F>(&self, operation: F) -> Result<T, WebError>
    where
        T: Send + 'static,
        F: FnOnce(&Store) -> Result<T, StoreError> + Send + 'static,
    {
        let waiting = self.waiting.clone().try_acquire_owned().map_err(|_| {
            database::ReadCounters::increment(&self.read_counters.waiting_full);
            WebError::unavailable()
        })?;
        let permit = tokio::time::timeout(READ_WAIT, self.reads.clone().acquire_owned())
            .await
            .map_err(|_| {
                database::ReadCounters::increment(&self.read_counters.waiting_timeout);
                WebError::unavailable()
            })?
            .map_err(|_| WebError::unavailable())?;
        drop(waiting);
        self.query_reserved(operation, permit)
            .await
            .map_err(|error| {
                match error {
                    QueryError::Timeout => {
                        database::ReadCounters::increment(&self.read_counters.query_timeout)
                    }
                    QueryError::Store(StoreError::Unavailable(_)) => {
                        database::ReadCounters::increment(&self.read_counters.query_unavailable)
                    }
                    _ => {}
                }
                error.into_web()
            })
    }

    pub(super) async fn query_reserved<T, F>(
        &self,
        operation: F,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> Result<T, QueryError>
    where
        T: Send + 'static,
        F: FnOnce(&Store) -> Result<T, StoreError> + Send + 'static,
    {
        let database = self.database.clone();
        let sales_database = self.sales_database.clone();
        let pool = self.reused_reads.clone();
        let task = tokio::task::spawn_blocking(move || {
            // The worker owns its slot even if the request times out or disconnects.
            let _permit = permit;
            let read = |store: &Store| {
                store.read_snapshot(|store| {
                    match store.snapshot() {
                        Ok(_) => {}
                        Err(StoreError::NotFound { .. }) => {
                            return Err(StoreError::Unavailable(
                                "Snapshot not imported yet".into(),
                            ));
                        }
                        Err(error) => return Err(error),
                    }
                    operation(store)
                })
            };
            pool.read(&database, sales_database.as_deref(), read)
        });
        match tokio::time::timeout(self.query_timeout, task).await {
            Ok(Ok(result)) => result.map_err(QueryError::Store),
            Ok(Err(_)) => Err(QueryError::Worker),
            Err(_) => Err(QueryError::Timeout),
        }
    }
}

pub(super) enum QueryError {
    Store(StoreError),
    Worker,
    Timeout,
}
impl QueryError {
    fn into_web(self) -> WebError {
        match self {
            Self::Store(error) => WebError::from(error),
            Self::Worker => WebError::internal(),
            Self::Timeout => WebError::unavailable(),
        }
    }
}

pub fn router(state: AppState) -> Router {
    let routes = if state.oauth.is_some() {
        Router::new().merge(oauth::router())
    } else {
        Router::new()
    };
    routes
        .route("/health/live", get(|| async { StatusCode::NO_CONTENT }))
        .route("/health/ready", get(ready))
        .route(
            "/favicon.ico",
            get(|| async { Redirect::permanent("/static/favicon.png") }),
        )
        .route("/", get(projects))
        .route("/projects", get(projects))
        .merge(search::router())
        .merge(agent::router())
        .merge(sitemap::router())
        .route("/project/{id}", get(project))
        .route("/token/{contract}/{id}", get(token))
        .route("/token/{contract}/{id}/full", get(full_artwork))
        .route("/artists", get(artists))
        .route("/profile/{id}", get(profile))
        .route("/artist/{id}", get(artist_redirect))
        .route("/collectors", get(collectors))
        .route("/collector/{id}", get(collector_redirect))
        .merge(articles::router())
        .merge(auth::router())
        .merge(market::trading_router())
        .route("/about", get(about))
        .nest_service("/static", ServeDir::new(state.assets.join("static")))
        .nest_service(
            "/fixtures/previews",
            ServeDir::new(state.assets.join("fixtures/previews")),
        )
        .fallback(not_found)
        .layer(middleware::from_fn_with_state(
            state.clone(),
            response_headers,
        ))
        .with_state(state)
}

async fn ready(State(state): State<AppState>) -> Result<Response, WebError> {
    state.query(|store| store.snapshot().map(|_| ())).await?;
    if let Some(content) = &state.content {
        tokio::time::timeout(Duration::from_secs(2), content.check_cache())
            .await
            .map_err(|_| WebError::unavailable())?
            .map_err(|_| WebError::unavailable())?;
    }
    let mut response = StatusCode::NO_CONTENT.into_response();
    response.headers_mut().insert(
        "x-seeded-cache",
        HeaderValue::from_static(
            if state
                .content
                .as_ref()
                .is_some_and(|content| content.cache_enabled())
            {
                "enabled"
            } else {
                "bypassed"
            },
        ),
    );
    Ok(response)
}

async fn response_headers(
    State(state): State<AppState>,
    request: axum::extract::Request,
    next: Next,
) -> Response {
    let json_requested = agent::wants_json(request.headers());
    let address_page = request.uri().path() == "/auth"
        || request.uri().path().starts_with("/auth/")
        || (request.uri().path().starts_with("/profile/")
            && request.uri().path().ends_with("/market"));
    let sitemap =
        request.uri().path() == "/sitemap.xml" || request.uri().path().starts_with("/sitemap/");
    let static_file = request.uri().path().starts_with("/static/")
        || request.uri().path().starts_with("/fixtures/previews/");
    let mut response = next.run(request).await;
    if json_requested
        && (response.status().is_client_error() || response.status().is_server_error())
    {
        let is_json = response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.starts_with("application/json"));
        if !is_json {
            let old_headers = response.headers().clone();
            response = agent::error(response.status());
            for name in [header::RETRY_AFTER, header::ALLOW] {
                if let Some(value) = old_headers.get(&name) {
                    response.headers_mut().insert(name, value.clone());
                }
            }
        }
    }
    response
        .headers_mut()
        .append(header::VARY, HeaderValue::from_static("Accept"));
    let cacheable = static_file
        && (response.status().is_success() || response.status() == StatusCode::NOT_MODIFIED);
    let response_status_success = response.status().is_success();
    let headers = response.headers_mut();
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
    let address_connect = if address_page {
        "https://beacon-node-1.octez.io https://beacon-node-2.octez.io https://beacon-node-3.octez.io https://beacon-node-4.octez.io https://beacon-node-5.octez.io https://beacon-node-6.octez.io https://beacon-node-7.octez.io https://beacon-node-8.octez.io wss://relay.walletconnect.com wss://relay.walletconnect.org wss://us-east-1.relay.walletconnect.com wss://eu-central-1.relay.walletconnect.com wss://ap-southeast-1.relay.walletconnect.com https://rpc.walletconnect.org https://verify.walletconnect.org https://verify.walletconnect.com"
    } else {
        ""
    };
    let address_frames = if address_page {
        "https://verify.walletconnect.org https://verify.walletconnect.com"
    } else {
        ""
    };
    let address_images = if address_page { " https:" } else { "" };
    let address_styles = if address_page { " 'unsafe-inline'" } else { "" };
    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_str(&format!(
            "default-src 'self'; img-src 'self' data:{address_images} {}; media-src 'self' {}; style-src 'self'{address_styles}; script-src 'self'; connect-src 'self' {} {address_connect}; frame-src {} {address_frames}; object-src 'none'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'",
            state.content_origin.as_deref().unwrap_or("'none'"),
            state.content_origin.as_deref().unwrap_or(""),
            state.content_origin.as_deref().unwrap_or(""),
            state.content_origin.as_deref().unwrap_or("'none'")
        )).unwrap_or_else(|_| HeaderValue::from_static("default-src 'none'")),
    );
    // Unversioned files can change on deploy/import; revalidate instead of promising immutability.
    headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static(if sitemap && response_status_success {
            "public, max-age=300"
        } else if cacheable {
            "public, max-age=0, must-revalidate"
        } else {
            "no-store"
        }),
    );
    response
}

pub(super) struct WebError {
    pub(super) status: StatusCode,
    pub(super) message: &'static str,
}

impl WebError {
    pub(super) fn bad_request() -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: "Check the search, link or NFT and try again.",
        }
    }

    pub(super) fn unavailable() -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            message: "Temporarily unavailable. Try again shortly.",
        }
    }

    pub(super) fn internal() -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: "Page could not be loaded. Try again.",
        }
    }
}

impl From<StoreError> for WebError {
    fn from(error: StoreError) -> Self {
        match error {
            StoreError::NotFound { .. } => Self {
                status: StatusCode::NOT_FOUND,
                message: "This item is not in the local archive.",
            },
            StoreError::BadRequest(_) => Self::bad_request(),
            StoreError::Unavailable(_) => Self::unavailable(),
            StoreError::Internal(_) => Self::internal(),
        }
    }
}

impl IntoResponse for WebError {
    fn into_response(self) -> Response {
        let html = views::render_error(ErrorPage {
            status: self.status.as_u16(),
            title: self.status.canonical_reason().unwrap_or("Archive error").into(),
            message: self.message.into(),
        })
        .unwrap_or_else(|_| "<!doctype html><title>Archive error</title><h1>Archive error</h1><a href=\"/projects\">Projects</a>".into());
        let mut response = (self.status, Html(html)).into_response();
        if self.status == StatusCode::SERVICE_UNAVAILABLE {
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from_static("2"));
        }
        response
    }
}

async fn not_found() -> WebError {
    WebError::from(StoreError::NotFound {
        what: "page",
        key: String::new(),
    })
}

pub(super) fn render(result: askama::Result<String>) -> Result<String, StoreError> {
    result.map_err(|_| StoreError::Internal("Template rendering failed".into()))
}

pub(super) fn query<T>(value: Result<Query<T>, QueryRejection>) -> Result<T, WebError> {
    value
        .map(|Query(value)| value)
        .map_err(|_| WebError::bad_request())
}

pub(super) fn validate_text(text: &str) -> Result<(), WebError> {
    if text.chars().count() > 200 {
        Err(WebError::bad_request())
    } else {
        Ok(())
    }
}

pub(super) fn cursor_request(
    page: Option<u32>,
    after: Option<&str>,
) -> Result<Option<Cursor>, WebError> {
    if page.is_some_and(|page| page != 1) || (page.is_some() && after.is_some()) {
        return Err(WebError::bad_request());
    }
    after.map(Cursor::parse).transpose().map_err(WebError::from)
}

pub(super) fn page_data<T>(
    data: KeysetPage<T>,
    after: Option<&Cursor>,
    base: &str,
) -> (Page<T>, PageInfo) {
    let pagination = PageInfo {
        // Cursor positions have no stable page number; templates only show navigation.
        page: 1,
        previous: after.map(|_| base.to_owned()),
        next: data
            .next
            .as_ref()
            .map(|cursor| with_cursor(base.to_owned(), "after", Some(cursor.as_str()))),
    };
    (keyset_page(data.items, data.total), pagination)
}

/// One page of keyset results; offsets are unused.
pub(super) fn keyset_page<T>(items: Vec<T>, total: u64) -> Page<T> {
    Page {
        items,
        total,
        offset: 0,
        limit: PAGE_SIZE,
    }
}

pub(super) fn count_page<T>(total: u64) -> Page<T> {
    keyset_page(Vec::new(), total)
}

/// Navigation for a result that fits on one page.
pub(super) fn single_page() -> PageInfo {
    PageInfo {
        page: 1,
        previous: None,
        next: None,
    }
}

fn curated_artist_address(store: &Store, id: &str) -> Result<Option<String>, StoreError> {
    use rusqlite::OptionalExtension;
    store
        .conn()
        .query_row(
            "SELECT address FROM identities WHERE slug=?1",
            [id],
            |row| row.get(0),
        )
        .optional()
        .map_err(|_| StoreError::Internal("Artist identity unavailable".into()))
}

pub(super) fn normalized_artists(
    store: &Store,
    artists: &mut [ArtistRef],
) -> Result<(), StoreError> {
    for artist in artists.iter_mut() {
        if artist.address.is_none() {
            artist.address = if crate::store::is_tezos_address(&artist.id) {
                Some(artist.id.clone())
            } else {
                curated_artist_address(store, &artist.id)?
            };
        }
    }
    let addresses: Vec<_> = artists
        .iter()
        .filter_map(|a| a.address.as_deref())
        .collect();
    let mut names = std::collections::HashMap::new();
    for chunk in addresses.chunks(crate::store::profile::MAX_DISPLAY_NAMES) {
        names.extend(store.display_names(chunk)?);
    }
    for artist in artists {
        artist.name = artist.address.as_ref().map_or_else(
            || "Address not yet indexed".into(),
            |address| {
                names
                    .get(address)
                    .cloned()
                    .unwrap_or_else(|| address.clone())
            },
        );
    }
    Ok(())
}

pub(super) fn normalized_tokens(store: &Store, tokens: &mut [Token]) -> Result<(), StoreError> {
    let addresses: BTreeSet<_> = tokens
        .iter()
        .filter_map(|token| token.owner.as_ref())
        .flat_map(|owner| {
            std::iter::once(owner.owner.address.as_str()).chain(
                owner
                    .listed_by
                    .as_ref()
                    .map(|seller| seller.address.as_str()),
            )
        })
        .collect();
    let addresses: Vec<_> = addresses.into_iter().collect();
    let mut names = std::collections::HashMap::new();
    for chunk in addresses.chunks(crate::store::profile::MAX_DISPLAY_NAMES) {
        names.extend(store.display_names(chunk)?);
    }
    for owner in tokens.iter_mut().filter_map(|token| token.owner.as_mut()) {
        owner.owner.alias = names.get(&owner.owner.address).cloned();
        if let Some(seller) = &mut owner.listed_by {
            seller.alias = names.get(&seller.address).cloned();
        }
    }
    Ok(())
}

pub(super) fn normalized_projects(
    store: &Store,
    projects: &mut [ProjectSummary],
) -> Result<(), StoreError> {
    for project in projects {
        normalized_artists(store, &mut project.artists)?;
    }
    Ok(())
}

/// Shown names for directory artists; an artist with a known address is listed by it.
fn normalized_artist_summaries(
    store: &Store,
    artists: &mut [crate::model::ArtistSummary],
) -> Result<(), StoreError> {
    for artist in artists {
        let mut refs = [ArtistRef {
            id: artist.id.clone(),
            name: artist.name.clone(),
            address: store.artist(&artist.id)?.address,
        }];
        normalized_artists(store, &mut refs)?;
        let [artist_ref] = refs;
        artist.name = artist_ref.name;
        if let Some(address) = artist_ref.address {
            artist.id = address;
        }
    }
    Ok(())
}

/// Recorded volume of each listed project, read only for the volume sort.
fn project_volumes(
    store: &Store,
    projects: &[ProjectSummary],
    sort: ProjectSort,
) -> Result<BTreeMap<u64, Option<u64>>, StoreError> {
    let mut volumes = BTreeMap::new();
    if sort == ProjectSort::VolumeDesc {
        for project in projects {
            volumes.insert(
                project.id,
                store
                    .project_volume(project.id)?
                    .map(|volume| volume.volume_mutez),
            );
        }
    }
    Ok(volumes)
}

fn search_url(mut base: String, q: &str, name: &str, value: &str) -> String {
    for (key, value) in [("q", q), ("trait_name", name), ("trait_value", value)] {
        base = with_cursor(base, key, (!value.is_empty()).then_some(value));
    }
    base
}

/// Visible articles whose title is shared (ignoring case) or shorter than 8 characters.
fn article_title_ids(path: &std::path::Path) -> rusqlite::Result<std::collections::HashSet<u64>> {
    let conn = rusqlite::Connection::open_with_flags(
        path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    let mut st = conn.prepare(
        "SELECT token_id, title FROM articles WHERE hidden = 0 AND title IS NOT NULL AND title <> ''",
    )?;
    let rows: Vec<(u64, String)> = st
        .query_map([], |r| Ok((r.get::<_, i64>(0)? as u64, r.get(1)?)))?
        .collect::<rusqlite::Result<_>>()?;
    let mut counts = std::collections::HashMap::new();
    for (_, title) in &rows {
        *counts.entry(title.trim().to_lowercase()).or_insert(0_u32) += 1;
    }
    Ok(rows
        .into_iter()
        .filter(|(_, title)| {
            title.trim().chars().count() < 8 || counts[&title.trim().to_lowercase()] > 1
        })
        .map(|(id, _)| id)
        .collect())
}

fn listing_filter<'a>(q: &'a str, name: &'a str, value: &'a str) -> ListingFilter<'a> {
    ListingFilter {
        text: (!q.trim().is_empty()).then_some(q.trim()),
        trait_filter: (!name.is_empty()).then_some((name, value)),
    }
}

fn validate_search(q: &str, name: &str, value: &str) -> Result<(), WebError> {
    for text in [q, name, value] {
        validate_text(text)?;
    }
    if name.is_empty() != value.is_empty() {
        return Err(WebError::bad_request());
    }
    Ok(())
}

fn profile_artist(
    store: &Store,
    address: &str,
) -> Result<Option<crate::model::Artist>, StoreError> {
    match store.artist_by_address(address) {
        Ok(artist) => Ok(Some(artist)),
        Err(StoreError::NotFound { .. }) => {
            use rusqlite::OptionalExtension;
            let id: Option<String> = store.conn().query_row(
                "SELECT a.id FROM identities i JOIN artists a ON a.id=i.slug WHERE i.address=?1 AND a.address IS NULL ORDER BY a.seq LIMIT 1",
                [address], |row| row.get(0),
            ).optional().map_err(|_| StoreError::Internal("Profile attribution unavailable".into()))?;
            id.map(|id| store.artist(&id)).transpose()
        }
        Err(error) => Err(error),
    }
}

fn resolve_profile(store: &Store, id: &str) -> Result<crate::model::Profile, StoreError> {
    if crate::store::is_tezos_address(id) {
        store.profile(id)
    } else {
        store.profile_by_name(id)
    }
}

/// The profile as its header shows it, with its attributed artist: an artist without a
/// recorded address counts its attributed projects.
pub(super) fn displayed_profile(
    store: &Store,
    id: &str,
) -> Result<(crate::model::Profile, Option<crate::model::Artist>), StoreError> {
    let mut profile = resolve_profile(store, id)?;
    profile.website = views::website(profile.website.as_deref());
    let artist = profile_artist(store, &profile.address)?;
    if let Some(artist) = &artist {
        if artist.address.is_none() {
            profile.project_count = store
                .artist_projects_page(&artist.id, None, 1)?
                .total
                .try_into()
                .unwrap_or(u32::MAX);
        }
    }
    Ok((profile, artist))
}

#[derive(Default, Deserialize)]
struct ProjectsParams {
    #[serde(default)]
    q: String,
    #[serde(default)]
    artist: String,
    #[serde(default)]
    sort: String,
    page: Option<u32>,
    after: Option<String>,
}

async fn projects(
    State(state): State<AppState>,
    request: agent::Request,
    params: Result<Query<ProjectsParams>, QueryRejection>,
) -> Result<Response, WebError> {
    let mut params = query(params)?;
    if let Some(moved) = search::moved_query(&params.q, search::SearchScope::All)? {
        return Ok(moved);
    }
    validate_text(&params.artist)?;
    if matches!(params.sort.as_str(), "title" | "artist") {
        params.page = None;
        params.after = None;
    }
    let after = cursor_request(params.page, params.after.as_deref())?;
    // Retired sorts: `id` is oldest first; title and artist restart at highest volume.
    let requested = match params.sort.as_str() {
        "id" => "oldest",
        "title" | "artist" => "volume",
        other => other,
    };
    let (sort, sort_name) = search::project_sort(requested).ok_or_else(WebError::bad_request)?;
    params.sort = sort_name.into();
    let content_origin = state.content_origin.clone();
    state
        .query(move |store| {
            let projects = store.projects_page(
                &ProjectFilter {
                    text: None,
                    artist: (!params.artist.is_empty()).then(|| params.artist.clone()),
                    sort,
                },
                after.as_ref(),
                PAGE_SIZE,
            )?;
            let base = format!(
                "/projects?artist={}&sort={}",
                encode(&params.artist),
                params.sort
            );
            let (mut projects, pagination) = page_data(projects, after.as_ref(), &base);
            normalized_projects(store, &mut projects.items)?;
            let project_volumes = project_volumes(store, &projects.items, sort)?;
            let page = ProjectsPage {
                content_origin,
                projects,
                artist: params.artist,
                sort: params.sort,
                project_volumes,
                pagination,
                snapshot: store.snapshot()?,
            };
            let data = serde_json::json!(&page);
            request.respond("projects", data, None, None, || {
                views::render_projects(page)
            })
        })
        .await
}

#[derive(Default, Deserialize, serde::Serialize)]
struct ProjectParams {
    #[serde(default)]
    q: String,
    #[serde(default)]
    trait_name: String,
    #[serde(default)]
    trait_value: String,
    #[serde(default)]
    view: String,
    page: Option<u32>,
    after: Option<String>,
    articles_after: Option<String>,
}

pub(super) fn numeric_id(id: &str) -> Result<u64, WebError> {
    id.parse::<u64>()
        .ok()
        .filter(|id| *id <= i64::MAX as u64)
        .ok_or_else(WebError::bad_request)
}

async fn project(
    State(state): State<AppState>,
    request: agent::Request,
    Path(id): Path<String>,
    params: Result<Query<ProjectParams>, QueryRejection>,
) -> Result<Response, WebError> {
    let id = numeric_id(&id)?;
    let params = query(params)?;
    validate_search(&params.q, &params.trait_name, &params.trait_value)?;
    // The edition comparison view was removed; old links land on the project.
    if params.view == "compare" {
        return Ok(Redirect::permanent(&format!("/project/{id}")).into_response());
    }
    if !matches!(params.view.as_str(), "" | "editions") {
        return Err(WebError::bad_request());
    }
    let after = cursor_request(params.page, params.after.as_deref())?;
    let content_origin = state.content_origin.clone();
    let articles_path = state.articles_database.clone();
    let json_requested = request.json;
    let (mut page, mut supplements) = state
        .query(move |store| {
            let mut project = store.project(id)?;
            normalized_artists(store, &mut project.artists)?;
            let history = if json_requested { String::new() } else { history::project(store.conn(), &project) };
            let tokens = store.tokens_for_project_search(
                id,
                &listing_filter(&params.q, &params.trait_name, &params.trait_value),
                after.as_ref(),
                PAGE_SIZE,
            )?;
            let base = format!("/project/{id}");
            let base = search_url(base, &params.q, &params.trait_name, &params.trait_value);
            let panel_base = with_cursor(base.clone(), "after", after.as_ref().map(Cursor::as_str));
            let base = with_cursor(base, "articles_after", params.articles_after.as_deref());
            let (mut tokens, pagination) = page_data(tokens, after.as_ref(), &base);
            normalized_tokens(store, &mut tokens.items)?;
            let articles = articles::section_page(store, articles_path.as_deref(), articles::Scope::Project(id), None, content_origin.as_deref(), params.articles_after.as_deref(), &panel_base)?;
            let supplements = serde_json::json!({"articles": articles.data, "history": json_requested.then(|| history::project_data(store.conn(), id))});
            Ok((ProjectPage {
                articles: articles.html,
                metadata: store.project_meta(id)?,
                release: String::new(),
                history,
                query: params.q,
                trait_name: params.trait_name,
                trait_value: params.trait_value,
                project,
                tokens,
                snapshot: store.snapshot()?,
                pagination,
                content_origin,
            }, supplements))
        })
        .await?;
    let current = match &state.release {
        Some(client) => client.get(&page.project).await,
        None => None,
    };
    if !request.json {
        page.release = release::render(current.as_ref());
    }
    let mut data = serde_json::json!(&page);
    data["articles"] = supplements["articles"].take();
    data["history"] = supplements["history"].take();
    data["release"] = serde_json::json!({"available":current.is_some(),"data":current});
    request
        .respond(
            "project",
            data,
            Some(&format!("/project/{id}")),
            None,
            || views::render_project(page),
        )
        .map_err(WebError::from)
}

// Unknown keys (utm_*) are ignored; serde still rejects a repeated `seed`.
#[derive(Deserialize)]
struct FullArtworkParams {
    seed: String,
}

async fn full_artwork(
    State(state): State<AppState>,
    request: agent::Request,
    Path((contract, id)): Path<(String, String)>,
    params: Result<Query<FullArtworkParams>, QueryRejection>,
) -> Result<Response, WebError> {
    let Query(params) = params.map_err(|_| WebError::bad_request())?;
    let id = numeric_id(&id)?;
    let (token, title) = state
        .query(move |store| {
            let token = store.token(&contract, id)?;
            let project = store.project(token.project_id)?;
            Ok((token, project.title))
        })
        .await?;
    if params.seed.is_empty() || params.seed != token.seed {
        return Err(WebError {
            status: StatusCode::BAD_REQUEST,
            message: "The seed does not match this NFT.",
        });
    }
    let origin = state
        .content_origin
        .as_deref()
        .ok_or_else(WebError::unavailable)?;
    let render_url = crate::player::render_url(origin, &token).map_err(|_| WebError {
        status: StatusCode::CONFLICT,
        message: "Live artwork is unavailable for this NFT.",
    })?;
    request
        .respond(
            "artwork",
            serde_json::json!({"contract": token.contract, "token_id": token.token_id,
            "seed": token.seed, "render_url": render_url}),
            Some(&format!("/token/{}/{}", token.contract, token.token_id)),
            None,
            || views::render_full_artwork(&token, &title, origin),
        )
        .map_err(WebError::from)
}

async fn token(
    State(state): State<AppState>,
    request: agent::Request,
    Path((contract, id)): Path<(String, String)>,
) -> Result<Response, WebError> {
    let id = numeric_id(&id)?;
    let content_origin = state.content_origin.clone();
    state
        .query(move |store| {
            let mut token = store.token(&contract, id)?;
            normalized_tokens(store, std::slice::from_mut(&mut token))?;
            let mut project = store.project(token.project_id)?;
            normalized_artists(store, &mut project.artists)?;
            let history = if request.json {
                String::new()
            } else {
                history::token(store.conn(), &contract, id)
            };
            let instructions: Option<String> = store
                .conn()
                .query_row(
                    "SELECT iteration_description FROM projects WHERE id = ?1",
                    [project.id as i64],
                    |row| row.get(0),
                )
                .map_err(|_| StoreError::Internal("Project instructions unavailable".into()))?;
            let neighbor = |iteration| match store.token_by_iteration(project.id, iteration) {
                Ok(token) => Ok(Some(token)),
                Err(StoreError::NotFound { .. }) => Ok(None),
                Err(error) => Err(error),
            };
            let previous = if token.iteration > 1 {
                neighbor(token.iteration - 1)?
            } else {
                None
            };
            let next = match token.iteration.checked_add(1) {
                Some(i) => neighbor(i)?,
                None => None,
            };
            let shares = store.feature_shares(project.id)?;
            let page = TokenPage {
                history,
                instructions: instructions.unwrap_or_default(),
                token,
                project,
                shares,
                snapshot: store.snapshot()?,
                previous,
                next,
                content_origin,
            };
            let data = serde_json::to_value(agent::TokenData {
                instructions: &page.instructions,
                content_origin: page.content_origin.as_deref(),
                token: &page.token,
                project: &page.project,
                shares: &page.shares,
                snapshot: &page.snapshot,
                previous: page.previous.as_ref().map(agent::TokenRef::from),
                next: page.next.as_ref().map(agent::TokenRef::from),
                history: request
                    .json
                    .then(|| history::token_data(store.conn(), &contract, id)),
            })
            .map_err(|_| StoreError::Internal("Token serialization failed".into()))?;
            request.respond(
                "token",
                data,
                Some(&format!("/token/{contract}/{id}")),
                None,
                || views::render_token(page),
            )
        })
        .await
}

#[derive(Default, Deserialize)]
struct ArtistsParams {
    #[serde(default)]
    q: String,
    page: Option<u32>,
    after: Option<String>,
}

async fn artists(
    State(state): State<AppState>,
    request: agent::Request,
    params: Result<Query<ArtistsParams>, QueryRejection>,
) -> Result<Response, WebError> {
    let params = query(params)?;
    if let Some(moved) = search::moved_query(&params.q, search::SearchScope::People)? {
        return Ok(moved);
    }
    let after = cursor_request(params.page, params.after.as_deref())?;
    let content_origin = state.content_origin.clone();
    let articles_path = state.articles_database.clone();
    state
        .query(move |store| {
            let artists = store.artists_page(None, after.as_ref(), PAGE_SIZE)?;
            let (mut artists, pagination) = page_data(artists, after.as_ref(), "/artists");
            normalized_artist_summaries(store, &mut artists.items)?;
            let addresses: Vec<_> = artists
                .items
                .iter()
                .map(|artist| artist.id.as_str())
                .filter(|id| crate::store::is_tezos_address(id))
                .collect();
            let avatars = store.avatars(&addresses)?;
            let article_counts = articles::author_counts(articles_path.as_deref(), &addresses);
            let page = ArtistsPage {
                artists,
                avatars,
                article_counts,
                content_origin,
                pagination,
                snapshot: store.snapshot()?,
            };
            let data = serde_json::json!(&page);
            request.respond("artists", data, None, None, || views::render_artists(page))
        })
        .await
}

#[derive(Default, Deserialize, serde::Serialize)]
struct ProfileParams {
    #[serde(default)]
    view: String,
    #[serde(default)]
    q: String,
    #[serde(default)]
    trait_name: String,
    #[serde(default)]
    trait_value: String,
    page: Option<u32>,
    after: Option<String>,
    articles_after: Option<String>,
}

async fn profile(
    State(state): State<AppState>,
    request: agent::Request,
    Path(id): Path<String>,
    headers: HeaderMap,
    params: Result<Query<ProfileParams>, QueryRejection>,
) -> Result<Response, WebError> {
    validate_text(&id)?;
    let mut params = query(params)?;
    validate_search(&params.q, &params.trait_name, &params.trait_value)?;
    if !matches!(params.view.as_str(), "" | "projects" | "collected") {
        return Err(WebError::bad_request());
    }
    if params.view == "projects" && !params.trait_name.is_empty() {
        return Err(WebError::bad_request());
    }
    let after = cursor_request(params.page, params.after.as_deref())?;
    let connected = state
        .auth
        .as_ref()
        .and_then(|auth| auth.identity(&headers).ok().flatten())
        .map(|identity| identity.address().to_owned());
    // The owner sees their submitted operations as done until the index has them.
    let overlay = connected
        .as_deref()
        .zip(state.trading.as_ref())
        .map(|(address, trading)| trading.overlay(address));
    let content_origin = state.content_origin.clone();
    let articles_path = state.articles_database.clone();
    state
        .query(move |store| {
            let (profile, artist) = displayed_profile(store, &id)?;
            // Artists open on their projects, collectors on their collection.
            if params.view.is_empty() {
                params.view = if profile.project_count > 0 {
                    "projects"
                } else {
                    "collected"
                }
                .into();
                if params.view == "projects" && !params.trait_name.is_empty() {
                    return Err(StoreError::BadRequest(
                        "Trait filters need view=collected".into(),
                    ));
                }
            }
            let own_profile = connected.as_deref() == Some(&profile.address);
            let pending = if own_profile {
                market::pending(store, &profile.address, overlay.as_ref())?
            } else {
                market::Pending::default()
            };
            let market_count = if own_profile {
                pending
                    .counts(store.market_counts(&profile.address)?)
                    .total()
            } else {
                0
            };
            let mut profile = profile;
            profile.held = profile.held.saturating_sub(pending.tokens.len() as u64);
            let base = search_url(
                format!("/profile/{}?view={}", encode(&profile.address), params.view),
                &params.q,
                &params.trait_name,
                &params.trait_value,
            );
            let panel_base = with_cursor(base.clone(), "after", after.as_ref().map(Cursor::as_str));
            let base = with_cursor(base, "articles_after", params.articles_after.as_deref());
            let collected = params.view == "collected";
            let (mut projects, mut tokens, pagination) = if collected {
                let mut data = store.collector_holdings_search(
                    &profile.address,
                    &listing_filter(&params.q, &params.trait_name, &params.trait_value),
                    after.as_ref(),
                    PAGE_SIZE,
                )?;
                let before = data.items.len();
                data.items.retain(|t| {
                    !pending.tokens.contains(&crate::trading::Token {
                        contract: t.contract.clone(),
                        token_id: t.token_id,
                    })
                });
                data.total = data
                    .total
                    .saturating_sub((before - data.items.len()) as u64);
                let (tokens, pagination) = page_data(data, after.as_ref(), &base);
                (
                    count_page(u64::from(profile.project_count)),
                    tokens,
                    pagination,
                )
            } else {
                let data = match artist {
                    Some(artist) => store.projects_page(
                        &ProjectFilter {
                            text: (!params.q.is_empty()).then(|| params.q.clone()),
                            artist: Some(artist.id),
                            sort: ProjectSort::IdDesc,
                        },
                        after.as_ref(),
                        PAGE_SIZE,
                    )?,
                    None if after.is_none() => KeysetPage {
                        items: vec![],
                        total: 0,
                        next: None,
                    },
                    None => {
                        return Err(StoreError::BadRequest(
                            "No project cursor for this profile".into(),
                        ));
                    }
                };
                let (projects, pagination) = page_data(data, after.as_ref(), &base);
                (projects, count_page(profile.held), pagination)
            };
            normalized_projects(store, &mut projects.items)?;
            normalized_tokens(store, &mut tokens.items)?;
            let (project_titles, project_artists) = token_project_titles(store, &tokens)?;
            let articles = articles::section_page(
                store,
                articles_path.as_deref(),
                if collected {
                    articles::Scope::Holder(profile.address.clone())
                } else {
                    articles::Scope::Author(profile.address.clone())
                },
                (!params.q.trim().is_empty()).then_some(params.q.as_str()),
                content_origin.as_deref(),
                params.articles_after.as_deref(),
                &panel_base,
            )?;

            let page = ProfilePage {
                articles: articles.html,
                profile,
                projects,
                tokens,
                project_titles,
                project_artists,
                own_profile,
                market_count,
                snapshot: store.snapshot()?,
                view: params.view,
                query: params.q,
                trait_name: params.trait_name,
                trait_value: params.trait_value,
                pagination,
                content_origin,
            };
            let mut data = serde_json::json!(&page);
            data["articles"] = articles.data;
            request.respond(
                "profile",
                data,
                Some(&format!("/profile/{}", encode(&page.profile.address))),
                None,
                || views::render_profile(page),
            )
        })
        .await
}

async fn legacy_profile(
    state: AppState,
    id: String,
    params: ProfileParams,
    default_view: &'static str,
) -> Result<Redirect, WebError> {
    validate_text(&id)?;
    validate_search(&params.q, &params.trait_name, &params.trait_value)?;
    let view = if params.view.is_empty() {
        default_view.to_owned()
    } else {
        params.view
    };
    if !matches!(view.as_str(), "projects" | "collected") {
        return Err(WebError::bad_request());
    }
    let after = cursor_request(params.page, params.after.as_deref())?;
    let profile = state
        .query(move |store| {
            // Legacy artist IDs refer to recorded attribution, never a later name claim.
            if default_view == "projects" {
                match store.artist(&id) {
                    Ok(artist) => {
                        let address = artist
                            .address
                            .or(curated_artist_address(store, &artist.id)?);
                        return match address {
                            Some(address) => store.profile(&address),
                            None => Err(StoreError::NotFound {
                                what: "artist profile",
                                key: id,
                            }),
                        };
                    }
                    Err(StoreError::NotFound { .. }) => {}
                    Err(error) => return Err(error),
                }
            }
            resolve_profile(store, &id)
        })
        .await?;
    let mut url = search_url(
        format!("/profile/{}?view={view}", encode(&profile.address)),
        &params.q,
        &params.trait_name,
        &params.trait_value,
    );
    if let Some(after) = after {
        url.push_str(&format!("&after={}", after.as_str()));
    }
    Ok(Redirect::permanent(&url))
}

async fn artist_redirect(
    State(state): State<AppState>,
    Path(id): Path<String>,
    params: Result<Query<ProfileParams>, QueryRejection>,
) -> Result<Redirect, WebError> {
    legacy_profile(state, id, query(params)?, "projects").await
}
async fn collector_redirect(
    State(state): State<AppState>,
    Path(id): Path<String>,
    params: Result<Query<ProfileParams>, QueryRejection>,
) -> Result<Redirect, WebError> {
    legacy_profile(state, id, query(params)?, "collected").await
}

/// Title and artists of each project on a page of tokens.
type ProjectLabels = (
    BTreeMap<u64, String>,
    BTreeMap<u64, Vec<crate::model::ArtistRef>>,
);

fn token_project_titles(
    store: &Store,
    tokens: &Page<crate::model::Token>,
) -> Result<ProjectLabels, StoreError> {
    let (mut titles, mut artists) = (BTreeMap::new(), BTreeMap::new());
    for token in &tokens.items {
        if let std::collections::btree_map::Entry::Vacant(entry) = titles.entry(token.project_id) {
            let mut project = store.project(token.project_id)?;
            normalized_artists(store, &mut project.artists)?;
            entry.insert(project.title);
            artists.insert(token.project_id, project.artists);
        }
    }
    Ok((titles, artists))
}

#[derive(Default, Deserialize)]
struct CollectorParams {
    #[serde(default)]
    address: String,
    page: Option<u32>,
    after: Option<String>,
}

async fn collectors(
    State(state): State<AppState>,
    request: agent::Request,
    params: Result<Query<CollectorParams>, QueryRejection>,
) -> Result<Response, WebError> {
    let params = query(params)?;
    validate_text(&params.address)?;
    if cursor_request(params.page, params.after.as_deref())?.is_some() {
        return Err(WebError::bad_request());
    }
    let address = params.address.trim();
    if !address.is_empty() {
        return Ok(
            Redirect::to(&format!("/profile/{}?view=collected", encode(address))).into_response(),
        );
    }
    state
        .query(move |store| {
            let page = CollectorPage {
                collector: None,
                tokens: count_page(0),
                project_titles: BTreeMap::new(),
                artist: None,
                snapshot: store.snapshot()?,
                content_origin: None,
                pagination: single_page(),
            };
            request.respond(
                "collectors",
                serde_json::json!(&page),
                Some("/collectors"),
                None,
                || views::render_collector(page),
            )
        })
        .await
}

async fn about(
    State(state): State<AppState>,
    request: agent::Request,
) -> Result<Response, WebError> {
    state
        .query(move |store| {
            let page = AboutPage {
                snapshot: store.snapshot()?,
                status: store.index_status()?,
            };
            request.respond(
                "about",
                serde_json::json!(&page),
                Some("/about"),
                None,
                || views::render_about(page),
            )
        })
        .await
}

/// `base` with the parameter `name=value` (encoded) appended, if there is a value.
pub(super) fn with_cursor(mut base: String, name: &str, cursor: Option<&str>) -> String {
    if let Some(cursor) = cursor {
        base.push(if base.contains('?') { '&' } else { '?' });
        base.push_str(name);
        base.push('=');
        base.push_str(&encode(cursor));
    }
    base
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn exhausted_database_slots_wait_briefly_then_fail_and_recover() {
        let state = AppState::new("missing.sqlite", "assets");
        let held = state
            .reads
            .clone()
            .acquire_many_owned(MAX_DATABASE_READS as u32)
            .await
            .unwrap();
        let error = state.query(|_| Ok(String::new())).await.err().unwrap();
        assert_eq!(error.status, StatusCode::SERVICE_UNAVAILABLE);
        drop(held);
        assert_eq!(state.reads.available_permits(), MAX_DATABASE_READS);
    }

    fn archive(name: &str) -> PathBuf {
        let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join(format!("tmp/{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let database = dir.join("archive.sqlite");
        let mut store = Store::open(&database).unwrap();
        crate::import::import_fixtures(
            &mut store,
            &PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("assets/fixtures"),
        )
        .unwrap();
        database
    }

    fn sales_with(path: &std::path::Path, marketplaces: usize) {
        let conn = crate::sales::schema::open_writer(path).unwrap();
        conn.execute("DELETE FROM marketplaces", []).unwrap();
        for n in 0..marketplaces {
            conn.execute(
                "INSERT INTO marketplaces (contract, name, role) VALUES (?1, ?1, 'market')",
                [format!("KT1test{n}")],
            )
            .unwrap();
        }
    }

    async fn marketplaces(state: &AppState) -> Option<i64> {
        state
            .query(|store| {
                Ok(store
                    .conn()
                    .query_row("SELECT count(*) FROM sales.marketplaces", [], |r| r.get(0))
                    .ok())
            })
            .await
            .ok()
            .flatten()
    }

    #[tokio::test]
    async fn page_reads_wait_two_seconds_for_a_slot_before_failing() {
        let database = archive("page-wait");
        let state = AppState::new(&database, "assets");
        let held = state
            .reads
            .clone()
            .acquire_many_owned(MAX_DATABASE_READS as u32)
            .await
            .unwrap();
        let waiter = state.clone();
        let request = tokio::spawn(async move { waiter.query(|_| Ok(1)).await });
        tokio::time::sleep(Duration::from_millis(1_000)).await;
        drop(held);
        assert_eq!(request.await.unwrap().ok(), Some(1));
        let held = state
            .reads
            .clone()
            .acquire_many_owned(MAX_DATABASE_READS as u32)
            .await
            .unwrap();
        let started = std::time::Instant::now();
        let error = state.query(|_| Ok(1)).await.err().unwrap();
        assert_eq!(error.status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(started.elapsed() >= READ_WAIT);
        drop(held);
        let counters = state.database_metrics_handle().snapshot();
        assert_eq!((counters.waiting_timeout, counters.waiting_full), (1, 0));
        std::fs::remove_dir_all(database.parent().unwrap()).unwrap();
    }

    #[tokio::test]
    async fn page_connections_are_reused_and_reattach_a_replaced_or_new_sales_file() {
        let database = archive("page-pool");
        let sales = database.with_file_name("sales.sqlite");
        let mut state = AppState::new(&database, "assets");
        state.sales_database = Some(sales.clone());
        assert_eq!(marketplaces(&state).await, None, "no sales file yet");
        sales_with(&sales, 1);
        let opened = state.reused_reads.opened();
        assert_eq!(
            marketplaces(&state).await,
            Some(1),
            "a new sales file is attached"
        );
        assert_eq!(state.reused_reads.opened(), opened + 1);
        for _ in 0..3 {
            assert_eq!(marketplaces(&state).await, Some(1));
        }
        assert_eq!(
            state.reused_reads.opened(),
            opened + 1,
            "connections are reused"
        );
        let next = database.with_file_name("sales-next.sqlite");
        sales_with(&next, 2);
        std::fs::rename(&next, &sales).unwrap();
        assert_eq!(
            marketplaces(&state).await,
            Some(2),
            "a replaced sales file is reopened"
        );
        assert_eq!(state.reused_reads.opened(), opened + 2);
        std::fs::remove_file(&sales).unwrap();
        assert_eq!(
            marketplaces(&state).await,
            None,
            "a deleted sales file is dropped"
        );
        assert_eq!(state.reused_reads.opened(), opened + 3);
        std::fs::remove_dir_all(database.parent().unwrap()).unwrap();
    }

    #[tokio::test]
    async fn a_reused_connection_retries_a_failed_sales_attach() {
        let database = archive("page-attach");
        let sales = database.with_file_name("sales.sqlite");
        // The sales job creates the file before it writes the schema into it.
        std::fs::write(&sales, b"").unwrap();
        let mut state = AppState::new(&database, "assets");
        state.sales_database = Some(sales.clone());
        assert_eq!(marketplaces(&state).await, None);
        let opened = state.reused_reads.opened();
        sales_with(&sales, 3);
        assert_eq!(marketplaces(&state).await, Some(3));
        assert_eq!(
            state.reused_reads.opened(),
            opened,
            "same connection, same file"
        );
        std::fs::remove_dir_all(database.parent().unwrap()).unwrap();
    }

    #[tokio::test]
    async fn a_sales_file_migrated_in_place_is_detached_from_reused_connections() {
        let database = archive("page-migrated");
        let sales = database.with_file_name("sales.sqlite");
        sales_with(&sales, 2);
        let mut state = AppState::new(&database, "assets");
        state.sales_database = Some(sales.clone());
        assert_eq!(marketplaces(&state).await, Some(2));
        let version = crate::sales::schema::SCHEMA_VERSION + 1;
        rusqlite::Connection::open(&sales)
            .unwrap()
            .pragma_update(None, "user_version", version)
            .unwrap();
        assert_eq!(
            marketplaces(&state).await,
            None,
            "an unknown schema is not read"
        );
        std::fs::remove_dir_all(database.parent().unwrap()).unwrap();
    }

    #[tokio::test]
    async fn a_full_page_waiting_queue_fails_at_once() {
        let state = AppState::new("missing.sqlite", "assets");
        let _reads = state
            .reads
            .clone()
            .acquire_many_owned(MAX_DATABASE_READS as u32)
            .await
            .unwrap();
        let _waiting = state
            .waiting
            .clone()
            .acquire_many_owned(MAX_WAITING_READS as u32)
            .await
            .unwrap();
        let started = std::time::Instant::now();
        let error = state.query(|_| Ok(1)).await.err().unwrap();
        assert_eq!(error.status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(started.elapsed() < Duration::from_millis(100));
        let counters = state.database_metrics_handle().snapshot();
        assert_eq!((counters.waiting_full, counters.waiting_timeout), (1, 0));
    }

    #[tokio::test]
    async fn cancelled_request_keeps_worker_slot_until_read_finishes() {
        let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join(format!("tmp/cancel-read-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let database = dir.join("archive.sqlite");
        let mut store = Store::open(&database).unwrap();
        crate::import::import_fixtures(
            &mut store,
            &PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("assets/fixtures"),
        )
        .unwrap();
        drop(store);
        let state = AppState::new(database, "assets");
        let worker_state = state.clone();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (finish_tx, finish_rx) = std::sync::mpsc::channel();
        let request = tokio::spawn(async move {
            worker_state
                .query(move |_| {
                    let _ = started_tx.send(());
                    let _ = finish_rx.recv_timeout(Duration::from_secs(2));
                    Ok(String::new())
                })
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), started_rx)
            .await
            .unwrap()
            .unwrap();
        request.abort();
        let _ = request.await;
        let remaining = state.reads.available_permits();
        let _ = finish_tx.send(());
        let all = tokio::time::timeout(
            Duration::from_secs(2),
            state
                .reads
                .clone()
                .acquire_many_owned(MAX_DATABASE_READS as u32),
        )
        .await
        .unwrap()
        .unwrap();
        drop(all);
        std::fs::remove_dir_all(dir).unwrap();
        assert_eq!(remaining, MAX_DATABASE_READS - 1);
    }
}

#[cfg(test)]
mod parameter_contract_tests {
    #[test]
    fn openapi_documents_every_project_and_profile_query_field() {
        let spec: serde_json::Value =
            serde_json::from_str(include_str!("../../docs/openapi.json")).unwrap();
        for (route, fields) in [
            (
                "/project/{id}",
                serde_json::to_value(super::ProjectParams::default()).unwrap(),
            ),
            (
                "/profile/{id}",
                serde_json::to_value(super::ProfileParams::default()).unwrap(),
            ),
        ] {
            let params = spec["paths"][route]["get"]["parameters"]
                .as_array()
                .unwrap();
            for key in fields.as_object().unwrap().keys() {
                assert!(
                    params.iter().any(|p| p["name"] == *key
                        || p["$ref"]
                            .as_str()
                            .is_some_and(|r| spec["components"]["parameters"]
                                [r.rsplit('/').next().unwrap()]["name"]
                                == *key)),
                    "{route} missing {key}"
                );
            }
        }
    }
}
