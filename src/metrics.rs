//! Bounded, in-process HTTP metrics. Paths and request metadata are never retained.
//!
//! Request totals, status, route class, and latency record when response headers
//! arrive; latency therefore ends at headers. Active requests remain counted
//! through body consumption or drop. `body.bytes_emitted` counts body bytes
//! yielded to the server stack, not bytes confirmed on the network.
//! Cancellation before headers and body errors are separate request outcomes.
use std::{
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use axum::{
    body::{Body, HttpBody},
    extract::{Request, State},
    http::{Method, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
};
use bytes::Bytes;
use serde_json::{Value, json};

const LATENCY_LIMITS_MS: [u64; 11] = [5, 10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000];
const ROUTES: [&str; 7] = [
    "page", "api", "static", "render", "content", "preview", "other",
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Origin {
    App,
    Artwork,
}

impl Origin {
    fn name(self) -> &'static str {
        match self {
            Self::App => "app",
            Self::Artwork => "artwork",
        }
    }
}

/// Per-process counters. `Default` starts a new process epoch.
pub struct Metrics {
    started: Instant,
    process_start: String,
    total: AtomicU64,
    by_class: [AtomicU64; 4],
    by_status: Vec<AtomicU64>,
    by_route: [AtomicU64; 7],
    active: AtomicU64,
    max_seen: AtomicU64,
    cancelled_before_headers: AtomicU64,
    body_errors: AtomicU64,
    body_bytes: AtomicU64,
    latency_buckets: [AtomicU64; 12],
    latency_sum_us: AtomicU64,
    latency_count: AtomicU64,
    content: Option<crate::content::ContentStore>,
    artwork: Option<crate::web::artwork::ArtworkMetricsHandle>,
    request_limits: Option<crate::request_limits::RequestLimitMetricsHandle>,
    database: Option<crate::web::DatabaseMetricsHandle>,
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            started: Instant::now(),
            process_start: rfc3339_now(),
            total: AtomicU64::new(0),
            by_class: std::array::from_fn(|_| AtomicU64::new(0)),
            by_status: (0..900).map(|_| AtomicU64::new(0)).collect(),
            by_route: std::array::from_fn(|_| AtomicU64::new(0)),
            active: AtomicU64::new(0),
            max_seen: AtomicU64::new(0),
            cancelled_before_headers: AtomicU64::new(0),
            body_errors: AtomicU64::new(0),
            body_bytes: AtomicU64::new(0),
            latency_buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            latency_sum_us: AtomicU64::new(0),
            latency_count: AtomicU64::new(0),
            content: None,
            artwork: None,
            request_limits: None,
            database: None,
        }
    }
}

impl Metrics {
    pub fn with_database(mut self, database: crate::web::DatabaseMetricsHandle) -> Self {
        self.database = Some(database);
        self
    }
    /// Include bounded cache and mirror snapshots from the content store.
    pub fn with_content(mut self, content: crate::content::ContentStore) -> Self {
        self.content = Some(content);
        self
    }

    /// Include bounded artwork admission counters.
    pub fn with_artwork(mut self, artwork: crate::web::artwork::ArtworkMetricsHandle) -> Self {
        self.artwork = Some(artwork);
        self
    }

    /// Include bounded per-listener request admission counters.
    pub fn with_request_limits(
        mut self,
        request_limits: crate::request_limits::RequestLimitMetricsHandle,
    ) -> Self {
        self.request_limits = Some(request_limits);
        self
    }

    fn record(&self, status: StatusCode, route: usize, elapsed_ns: u64) {
        self.total.fetch_add(1, Ordering::Relaxed);
        if let Some(class) = class_index(status) {
            self.by_class[class].fetch_add(1, Ordering::Relaxed);
        }
        self.by_status[usize::from(status.as_u16() - 100)].fetch_add(1, Ordering::Relaxed);
        self.by_route[route].fetch_add(1, Ordering::Relaxed);
        let bucket = LATENCY_LIMITS_MS
            .iter()
            .position(|limit| elapsed_ns <= limit.saturating_mul(1_000_000))
            .unwrap_or(LATENCY_LIMITS_MS.len());
        self.latency_buckets[bucket].fetch_add(1, Ordering::Relaxed);
        self.latency_sum_us
            .fetch_add(elapsed_ns / 1_000, Ordering::Relaxed);
        self.latency_count.fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self, origin: Origin) -> Value {
        let status: serde_json::Map<String, Value> = self
            .by_status
            .iter()
            .enumerate()
            .filter_map(|(index, count)| {
                let count = count.load(Ordering::Relaxed);
                (count != 0).then(|| ((index + 100).to_string(), json!(count)))
            })
            .collect();
        let classes = ["2xx", "3xx", "4xx", "5xx"];
        let by_class: serde_json::Map<String, Value> = classes
            .into_iter()
            .zip(self.by_class.iter())
            .map(|(name, count)| (name.to_owned(), json!(count.load(Ordering::Relaxed))))
            .collect();
        let by_route: serde_json::Map<String, Value> = ROUTES
            .into_iter()
            .zip(self.by_route.iter())
            .map(|(name, count)| (name.to_owned(), json!(count.load(Ordering::Relaxed))))
            .collect();
        let active = self.active.load(Ordering::Relaxed);
        let latency_count = self.latency_count.load(Ordering::Relaxed);
        let latency_sum_ms = self.latency_sum_us.load(Ordering::Relaxed) as f64 / 1_000.0;
        let mut snapshot = json!({
            "schema": 1,
            "origin": origin.name(),
            "process_start": self.process_start,
            "uptime_s": self.started.elapsed().as_secs_f64(),
            "requests": {
                "total": self.total.load(Ordering::Relaxed),
                "by_class": by_class,
                "by_status": status,
                "by_route": by_route,
                "cancelled_before_headers": self.cancelled_before_headers.load(Ordering::Relaxed),
            },
            "active": {
                "requests": active,
                "max_seen": self.max_seen.load(Ordering::Relaxed),
            },
            "latency_ms": {
                "buckets": [5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000],
                "counts": self.latency_buckets.iter().map(|count| count.load(Ordering::Relaxed)).collect::<Vec<_>>(),
                "sum_ms": latency_sum_ms,
                "count": latency_count,
            },
            "body": {
                "errors": self.body_errors.load(Ordering::Relaxed),
                "bytes_emitted": self.body_bytes.load(Ordering::Relaxed),
            },
        });
        if let Some(content) = &self.content {
            let content = content.metrics();
            snapshot["cache"] = json!(content.cache);
            snapshot["mirror"] = json!(content.mirror);
            snapshot["local_delivery"] = json!(content.local_delivery);
            snapshot["local_scratch"] = json!(content.local_scratch);
            snapshot["local_reads"] = json!(content.local_reads);
            snapshot["resolution"] = json!(content.resolution);
        }
        if let Some(artwork) = &self.artwork {
            snapshot["artwork_admission"] = json!(artwork.snapshot());
        }
        if let Some(request_limits) = &self.request_limits {
            snapshot["request_limits"] = json!(request_limits.snapshot());
        }
        if let Some(database) = &self.database {
            snapshot["database_admission"] = json!(database.snapshot());
        }
        snapshot
    }
}

/// Add the private endpoint and wrap the completed router. Install outside
/// `request_limits::protect` so admission responses are counted too.
pub fn instrument(router: axum::Router, metrics: Arc<Metrics>, origin: Origin) -> axum::Router {
    let state = (metrics.clone(), origin);
    router
        .route(
            "/health/metrics",
            get(move || metrics_handler(metrics.clone(), origin)),
        )
        .layer(middleware::from_fn_with_state(state, observe))
}

async fn metrics_handler(metrics: Arc<Metrics>, origin: Origin) -> Response {
    let mut response = axum::Json(metrics.snapshot(origin)).into_response();
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        "no-store".parse().expect("header value"),
    );
    response.headers_mut().insert(
        header::X_CONTENT_TYPE_OPTIONS,
        "nosniff".parse().expect("header value"),
    );
    response
}

async fn observe(
    State((metrics, _origin)): State<(Arc<Metrics>, Origin)>,
    request: Request,
    next: Next,
) -> Response {
    let path = request.uri().path();
    if path.starts_with("/health/") {
        return next.run(request).await;
    }
    let route = route_index(request.method(), path);
    let started = Instant::now();
    let mut active = Active::new(metrics.clone());
    let response = next.run(request).await;
    active.headers_received = true;
    metrics.record(
        response.status(),
        route,
        started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64,
    );
    response.map(|body| track(body, active))
}

struct Active {
    metrics: Arc<Metrics>,
    headers_received: bool,
}

impl Active {
    fn new(metrics: Arc<Metrics>) -> Self {
        let current = metrics.active.fetch_add(1, Ordering::Relaxed) + 1;
        metrics.max_seen.fetch_max(current, Ordering::Relaxed);
        Self {
            metrics,
            headers_received: false,
        }
    }
}

impl Drop for Active {
    fn drop(&mut self) {
        if !self.headers_received {
            self.metrics
                .cancelled_before_headers
                .fetch_add(1, Ordering::Relaxed);
        }
        self.metrics.active.fetch_sub(1, Ordering::Relaxed);
    }
}

fn track(body: Body, active: Active) -> Body {
    if body.is_end_stream() {
        drop(active);
        body
    } else {
        Body::new(TrackedBody {
            inner: body,
            active: Some(active),
            errored: false,
        })
    }
}

struct TrackedBody {
    inner: Body,
    active: Option<Active>,
    errored: bool,
}

impl HttpBody for TrackedBody {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, Self::Error>>> {
        let result = Pin::new(&mut self.inner).poll_frame(cx);
        match result {
            Poll::Ready(Some(Ok(frame))) => match frame.into_data() {
                Ok(bytes) => {
                    if let Some(active) = &self.active {
                        active
                            .metrics
                            .body_bytes
                            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
                    }
                    if !self.inner.is_end_stream() {
                        return Poll::Ready(Some(Ok(http_body::Frame::data(bytes))));
                    }
                    let owned = Bytes::from_owner(TrackedBytes {
                        bytes,
                        _active: self.active.take(),
                    });
                    Poll::Ready(Some(Ok(http_body::Frame::data(owned))))
                }
                Err(frame) => {
                    if self.inner.is_end_stream() {
                        self.active.take();
                    }
                    Poll::Ready(Some(Ok(frame)))
                }
            },
            Poll::Ready(Some(Err(error))) => {
                if !self.errored {
                    if let Some(active) = &self.active {
                        active.metrics.body_errors.fetch_add(1, Ordering::Relaxed);
                    }
                    self.errored = true;
                }
                self.active.take();
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(None) => {
                self.active.take();
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}

struct TrackedBytes {
    bytes: Bytes,
    _active: Option<Active>,
}

impl AsRef<[u8]> for TrackedBytes {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

fn class_index(status: StatusCode) -> Option<usize> {
    match status.as_u16() / 100 {
        2 => Some(0),
        3 => Some(1),
        4 => Some(2),
        5 => Some(3),
        _ => None,
    }
}

fn route_index(method: &Method, path: &str) -> usize {
    let is_page = (method == Method::GET || method == Method::HEAD)
        && (matches!(
            path,
            "/" | "/projects"
                | "/artists"
                | "/collectors"
                | "/articles"
                | "/about"
                | "/search"
                | "/auth"
        ) || [
            "/project/",
            "/token/",
            "/profile/",
            "/artist/",
            "/collector/",
            "/article/",
        ]
        .iter()
        .any(|prefix| path.starts_with(prefix)));
    if path.starts_with("/content/") {
        4
    } else if path.starts_with("/render/") {
        3
    } else if path.starts_with("/fixtures/previews/") || path.starts_with("/preview/") {
        5
    } else if path.starts_with("/static/") {
        2
    } else if path.starts_with("/api/") {
        1
    } else if is_page {
        0
    } else {
        6
    }
}

fn rfc3339_now() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let days = (now.as_secs() / 86_400) as i64;
    let seconds = now.as_secs() % 86_400;
    let (year, month, day) = civil_from_days(days);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{:09}Z",
        seconds / 3_600,
        (seconds / 60) % 60,
        seconds % 60,
        now.subsec_nanos()
    )
}

fn civil_from_days(days_since_epoch: i64) -> (i64, u32, u32) {
    let z = days_since_epoch + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    (year + i64::from(month <= 2), month as u32, day as u32)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn latency_boundaries_are_inclusive_and_buckets_are_exclusive() {
        let metrics = Metrics::default();
        for limit_ms in LATENCY_LIMITS_MS.iter().copied() {
            let limit_ns = limit_ms * 1_000_000;
            metrics.record(StatusCode::OK, 0, limit_ns);
            metrics.record(StatusCode::OK, 0, limit_ns + 1);
        }
        assert_eq!(metrics.latency_count.load(Ordering::Relaxed), 22);
        let counts = metrics
            .latency_buckets
            .iter()
            .map(|count| count.load(Ordering::Relaxed))
            .collect::<Vec<_>>();
        assert_eq!(counts, [1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1]);
    }
}
