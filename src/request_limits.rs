//! Shared per-address limits for the app and content listeners.
use std::{
    collections::HashMap,
    net::IpAddr,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll},
    time::{Duration, Instant},
};

use axum::{
    body::{Body, HttpBody},
    extract::{Request, State},
    http::{Method, StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use bytes::Bytes;
use serde::Serialize;

use crate::auth::TrustedClientIp;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const MAX_ADDRESSES: usize = 8192;
const DELIVERY_CHUNK: usize = 64 * 1024;
const MAX_IN_FLIGHT: u8 = 16;
const STREAM_WAIT: Duration = Duration::from_secs(10);
const TOKEN_INTERVAL: Duration = Duration::from_millis(50);
const BURST_CREDIT: Duration = Duration::from_secs(2);
const CONTENT_BURST: Duration = Duration::from_millis(50 * 256);
const CONTENT_WAITERS_PER_CLIENT: u16 = 256;
const CONTENT_WAITERS: usize = 2048;
const IDLE_TTL: Duration = Duration::from_secs(60);

#[derive(Clone)]
pub struct RequestLimits {
    registry: Arc<Mutex<Registry>>,
    metrics: Arc<RequestLimitCounters>,
    content_limit: usize,
    artwork: bool,
    waiting: Arc<Semaphore>,
}

#[derive(Default)]
struct RequestLimitCounters {
    global_content_wait_full: AtomicU64,
    client_content_wait_full: AtomicU64,
    content_wait_timeout: AtomicU64,
    noncontent_bucket_rejected: AtomicU64,
    family_new_client_rejected: AtomicU64,
    noncontent_stream_timeout: AtomicU64,
    client_table_full: AtomicU64,
    missing_client_ip: AtomicU64,
}

impl RequestLimitCounters {
    fn increment(counter: &AtomicU64) {
        let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
            Some(value.saturating_add(1))
        });
    }
}

/// Cloneable handle for bounded artwork-listener rejection counters.
#[derive(Clone)]
pub struct RequestLimitMetricsHandle {
    counters: Arc<RequestLimitCounters>,
}

/// Fixed-cardinality snapshot; fields count each admission rejection reason once.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RequestLimitMetrics {
    pub global_content_wait_full: u64,
    pub client_content_wait_full: u64,
    /// Content token-refill and stream-capacity waits that exceed the shared 10 s deadline.
    pub content_wait_timeout: u64,
    pub noncontent_bucket_rejected: u64,
    /// Retained for metrics compatibility; family-wide admission was removed.
    pub family_new_client_rejected: u64,
    pub noncontent_stream_timeout: u64,
    pub client_table_full: u64,
    pub missing_client_ip: u64,
}

impl RequestLimitMetricsHandle {
    pub fn snapshot(&self) -> RequestLimitMetrics {
        let counter = &self.counters;
        RequestLimitMetrics {
            global_content_wait_full: counter.global_content_wait_full.load(Ordering::Relaxed),
            client_content_wait_full: counter.client_content_wait_full.load(Ordering::Relaxed),
            content_wait_timeout: counter.content_wait_timeout.load(Ordering::Relaxed),
            noncontent_bucket_rejected: counter.noncontent_bucket_rejected.load(Ordering::Relaxed),
            family_new_client_rejected: counter.family_new_client_rejected.load(Ordering::Relaxed),
            noncontent_stream_timeout: counter.noncontent_stream_timeout.load(Ordering::Relaxed),
            client_table_full: counter.client_table_full.load(Ordering::Relaxed),
            missing_client_ip: counter.missing_client_ip.load(Ordering::Relaxed),
        }
    }
}

struct Registry {
    addresses: HashMap<IpAddr, Entry>,
    idle_head: Option<IpAddr>,
    idle_tail: Option<IpAddr>,
}

struct Entry {
    credit: Duration,
    refilled: Instant,
    last_seen: Instant,
    in_flight: u8,
    streams: Arc<Semaphore>,
    content_credit: Duration,
    content_refilled: Instant,
    waiting: u16,
    idle: bool,
    previous: Option<IpAddr>,
    next: Option<IpAddr>,
}

struct Permit {
    registry: Arc<Mutex<Registry>>,
    ip: IpAddr,
    streams: Arc<Semaphore>,
    metrics: Arc<RequestLimitCounters>,
}

struct Waiting {
    registry: Arc<Mutex<Registry>>,
    ip: IpAddr,
    _slot: OwnedSemaphorePermit,
}

impl Default for RequestLimits {
    fn default() -> Self {
        Self::new()
    }
}

impl RequestLimits {
    pub fn new() -> Self {
        Self::with_content_limit(4)
    }

    pub fn with_content_limit(limit: usize) -> Self {
        Self {
            registry: Arc::new(Mutex::new(Registry {
                addresses: HashMap::new(),
                idle_head: None,
                idle_tail: None,
            })),
            metrics: Arc::new(RequestLimitCounters::default()),
            content_limit: limit.clamp(1, 4),
            artwork: false,
            waiting: Arc::new(Semaphore::new(CONTENT_WAITERS)),
        }
    }

    pub fn artwork(&self) -> Self {
        let mut artwork = Self::with_content_limit(self.content_limit);
        artwork.artwork = true;
        artwork
    }

    pub fn metrics_handle(&self) -> RequestLimitMetricsHandle {
        RequestLimitMetricsHandle {
            counters: self.metrics.clone(),
        }
    }

    fn acquire(&self, ip: IpAddr, now: Instant) -> Result<Arc<Permit>, StatusCode> {
        self.acquire_kind(ip, now, false)
    }

    fn acquire_kind(
        &self,
        ip: IpAddr,
        now: Instant,
        content: bool,
    ) -> Result<Arc<Permit>, StatusCode> {
        let streams = self.client(ip, now, |client| {
            if content {
                client.content_credit = client
                    .content_credit
                    .saturating_add(now.saturating_duration_since(client.content_refilled))
                    .min(CONTENT_BURST);
                client.content_refilled = client.content_refilled.max(now);
                if client.in_flight >= MAX_IN_FLIGHT || client.content_credit < TOKEN_INTERVAL {
                    return Err(StatusCode::TOO_MANY_REQUESTS);
                }
                client.content_credit -= TOKEN_INTERVAL;
            } else {
                if let Err(status) = client.check(now, TOKEN_INTERVAL, MAX_IN_FLIGHT) {
                    if status == StatusCode::TOO_MANY_REQUESTS {
                        RequestLimitCounters::increment(&self.metrics.noncontent_bucket_rejected);
                    }
                    return Err(status);
                }
                client.credit -= TOKEN_INTERVAL;
            }
            client.in_flight += 1;
            Ok(client.streams.clone())
        })?;
        Ok(Arc::new(Permit {
            registry: self.registry.clone(),
            ip: crate::client_ip::rate_ip(ip),
            streams,
            metrics: self.metrics.clone(),
        }))
    }

    fn enqueue(&self, ip: IpAddr, now: Instant) -> Result<Waiting, StatusCode> {
        let slot = self.waiting.clone().try_acquire_owned().map_err(|_| {
            RequestLimitCounters::increment(&self.metrics.global_content_wait_full);
            StatusCode::SERVICE_UNAVAILABLE
        })?;
        self.client(ip, now, |client| {
            if client.waiting >= CONTENT_WAITERS_PER_CLIENT {
                RequestLimitCounters::increment(&self.metrics.client_content_wait_full);
                return Err(StatusCode::TOO_MANY_REQUESTS);
            }
            client.waiting += 1;
            Ok(())
        })?;
        Ok(Waiting {
            registry: self.registry.clone(),
            ip: crate::client_ip::rate_ip(ip),
            _slot: slot,
        })
    }

    async fn content_permit(
        &self,
        ip: IpAddr,
    ) -> Result<(Arc<Permit>, Vec<OwnedSemaphorePermit>), StatusCode> {
        let _waiting = self.enqueue(ip, Instant::now())?;
        // Wait before running the handler or allocating content. The same deadline
        // covers stream capacity, active admission and token refill.
        let result = tokio::time::timeout(STREAM_WAIT, async {
            let permit = loop {
                match self.acquire_kind(ip, Instant::now(), true) {
                    Ok(permit) => break permit,
                    Err(StatusCode::TOO_MANY_REQUESTS) => tokio::time::sleep(TOKEN_INTERVAL).await,
                    Err(status) => return Err(status),
                }
            };
            let stream = permit
                .streams
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
            Ok((permit, vec![stream]))
        })
        .await;
        match result {
            Ok(result) => result,
            Err(_) => {
                RequestLimitCounters::increment(&self.metrics.content_wait_timeout);
                Err(StatusCode::TOO_MANY_REQUESTS)
            }
        }
    }

    fn client<T>(
        &self,
        ip: IpAddr,
        now: Instant,
        visit: impl FnOnce(&mut Entry) -> Result<T, StatusCode>,
    ) -> Result<T, StatusCode> {
        let ip = crate::client_ip::rate_ip(ip);
        let mut registry = self
            .registry
            .lock()
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        registry.expire_idle(now);
        if !registry.addresses.contains_key(&ip) {
            if registry.addresses.len() >= MAX_ADDRESSES {
                let oldest = registry.idle_head.ok_or_else(|| {
                    RequestLimitCounters::increment(&self.metrics.client_table_full);
                    StatusCode::TOO_MANY_REQUESTS
                })?;
                registry.remove_idle(oldest);
                registry.addresses.remove(&oldest);
            }
            registry
                .addresses
                .insert(ip, Entry::new(now, self.content_limit));
        }
        registry.remove_idle(ip);
        let client = registry.addresses.get_mut(&ip).expect("registered client");
        client.last_seen = client.last_seen.max(now);
        let result = visit(client);
        registry.append_idle(ip);
        result
    }
}

impl Registry {
    fn remove_idle(&mut self, ip: IpAddr) {
        let entry = self.addresses.get_mut(&ip).expect("registered client");
        if !entry.idle {
            return;
        }
        let previous = entry.previous.take();
        let next = entry.next.take();
        entry.idle = false;
        if let Some(previous) = previous {
            self.addresses
                .get_mut(&previous)
                .expect("previous idle client")
                .next = next;
        } else {
            self.idle_head = next;
        }
        if let Some(next) = next {
            self.addresses
                .get_mut(&next)
                .expect("next idle client")
                .previous = previous;
        } else {
            self.idle_tail = previous;
        }
    }

    fn append_idle(&mut self, ip: IpAddr) {
        let entry = self.addresses.get_mut(&ip).expect("registered client");
        if entry.in_flight != 0 || entry.waiting != 0 || entry.idle {
            return;
        }
        entry.idle = true;
        entry.previous = self.idle_tail;
        if let Some(tail) = self.idle_tail {
            self.addresses.get_mut(&tail).expect("idle tail").next = Some(ip);
        } else {
            self.idle_head = Some(ip);
        }
        self.idle_tail = Some(ip);
    }

    fn expire_idle(&mut self, now: Instant) {
        while let Some(ip) = self.idle_head {
            if now.saturating_duration_since(self.addresses[&ip].last_seen) < IDLE_TTL {
                break;
            }
            self.remove_idle(ip);
            self.addresses.remove(&ip);
        }
    }
}

impl Entry {
    fn new(now: Instant, limit: usize) -> Self {
        Self {
            credit: BURST_CREDIT,
            refilled: now,
            last_seen: now,
            in_flight: 0,
            streams: Arc::new(Semaphore::new(limit)),
            content_credit: CONTENT_BURST,
            content_refilled: now,
            waiting: 0,
            idle: false,
            previous: None,
            next: None,
        }
    }
    fn check(&mut self, now: Instant, interval: Duration, max: u8) -> Result<(), StatusCode> {
        self.credit = self
            .credit
            .saturating_add(now.saturating_duration_since(self.refilled))
            .min(BURST_CREDIT);
        self.refilled = self.refilled.max(now);
        self.last_seen = self.last_seen.max(now);
        if self.in_flight >= max || self.credit < interval {
            return Err(StatusCode::TOO_MANY_REQUESTS);
        }
        Ok(())
    }
}

impl Permit {
    async fn stream_slots(&self) -> Result<Vec<OwnedSemaphorePermit>, StatusCode> {
        let result = tokio::time::timeout(STREAM_WAIT, async {
            let mut slots = Vec::with_capacity(2);
            slots.push(
                self.streams
                    .clone()
                    .acquire_owned()
                    .await
                    .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?,
            );
            Ok(slots)
        })
        .await;
        match result {
            Ok(result) => result,
            Err(_) => {
                RequestLimitCounters::increment(&self.metrics.noncontent_stream_timeout);
                Err(StatusCode::TOO_MANY_REQUESTS)
            }
        }
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        let Ok(mut registry) = self.registry.lock() else {
            return;
        };
        if let Some(entry) = registry.addresses.get_mut(&self.ip) {
            entry.in_flight -= 1;
            entry.last_seen = entry.last_seen.max(Instant::now());
            registry.append_idle(self.ip);
        }
    }
}

impl Drop for Waiting {
    fn drop(&mut self) {
        if let Ok(mut registry) = self.registry.lock() {
            if let Some(entry) = registry.addresses.get_mut(&self.ip) {
                entry.waiting -= 1;
                entry.last_seen = entry.last_seen.max(Instant::now());
                registry.append_idle(self.ip);
            }
        }
    }
}

pub fn protect(router: axum::Router, limits: RequestLimits, trust_proxy: bool) -> axum::Router {
    let artwork = limits.artwork;
    let identity = (trust_proxy, limits.metrics.clone());
    let router = router
        .layer(axum::middleware::from_fn_with_state(limits, admit))
        .layer(axum::middleware::from_fn_with_state(identity, identify));
    if artwork {
        router.layer(axum::middleware::from_fn(content_errors))
    } else {
        router
    }
}

async fn identify(
    State((trust_proxy, metrics)): State<(bool, Arc<RequestLimitCounters>)>,
    request: Request,
    next: Next,
) -> Response {
    if !request.uri().path().starts_with("/health/")
        && crate::client_ip::peer_addr(&request).is_none()
    {
        RequestLimitCounters::increment(&metrics.missing_client_ip);
    }
    crate::client_ip::identify(State(trust_proxy), request, next).await
}

async fn content_errors(request: Request, next: Next) -> Response {
    content_error_headers(next.run(request).await)
}

fn content_error_headers(mut response: Response) -> Response {
    if matches!(
        response.status(),
        StatusCode::TOO_MANY_REQUESTS | StatusCode::SERVICE_UNAVAILABLE
    ) {
        response
            .headers_mut()
            .insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, "*".parse().unwrap());
        response
            .headers_mut()
            .entry(header::RETRY_AFTER)
            .or_insert("1".parse().unwrap());
    }
    response
}

pub async fn admit(State(limits): State<RequestLimits>, request: Request, next: Next) -> Response {
    let Some(TrustedClientIp(ip)) = request.extensions().get::<TrustedClientIp>().copied() else {
        RequestLimitCounters::increment(&limits.metrics.missing_client_ip);
        return failure(StatusCode::SERVICE_UNAVAILABLE, limits.artwork);
    };
    let content = limits.artwork
        && request.method() == Method::GET
        && ["/content/ipfs/", "/content/onchfs/"]
            .iter()
            .any(|path| request.uri().path().starts_with(path));
    if content {
        let (permit, slots) = match limits.content_permit(ip).await {
            Ok(admission) => admission,
            Err(status) => return failure(status, true),
        };
        return hold_response(next.run(request).await, Arc::new((permit, slots)));
    }
    let permit = match limits.acquire(ip, Instant::now()) {
        Ok(permit) => permit,
        Err(status) => return failure(status, limits.artwork),
    };
    let slots = if limits.artwork {
        match permit.stream_slots().await {
            Ok(slots) => slots,
            Err(status) => return failure(status, limits.artwork),
        }
    } else {
        Vec::new()
    };
    let response = next.run(request).await;
    hold_response(response, Arc::new((permit, slots)))
}

fn failure(status: StatusCode, artwork: bool) -> Response {
    let mut response = (
        status,
        [
            ("cache-control", "no-store"),
            ("x-content-type-options", "nosniff"),
        ],
    )
        .into_response();
    if matches!(
        status,
        StatusCode::TOO_MANY_REQUESTS | StatusCode::SERVICE_UNAVAILABLE
    ) {
        response
            .headers_mut()
            .insert("retry-after", "1".parse().unwrap());
    }
    if artwork {
        content_error_headers(response)
    } else {
        response
    }
}

fn hold_response(response: Response, permit: Arc<dyn Send + Sync>) -> Response {
    let connection = response
        .extensions()
        .get::<crate::delivery_control::Connection>()
        .cloned();
    response.map(|body| hold_delivery(body, permit, connection))
}

#[cfg(test)]
fn hold(body: Body, permit: Arc<dyn Send + Sync>) -> Body {
    hold_delivery(body, permit, None)
}

pub(crate) fn hold_delivery(
    body: Body,
    permit: Arc<dyn Send + Sync>,
    connection: Option<crate::delivery_control::Connection>,
) -> Body {
    if body.is_end_stream() {
        return body;
    }
    Body::new(HeldBody {
        inner: body,
        permit: Some(permit),
        pending: None,
        connection,
    })
}

struct HeldBody {
    inner: Body,
    permit: Option<Arc<dyn Send + Sync>>,
    pending: Option<Bytes>,
    connection: Option<crate::delivery_control::Connection>,
}

// Hyper may retain the final frame after dropping the body.
struct HeldBytes {
    bytes: Bytes,
    _permit: Arc<dyn Send + Sync>,
}

impl AsRef<[u8]> for HeldBytes {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl HeldBody {
    fn frame(&mut self, mut bytes: Bytes) -> http_body::Frame<Bytes> {
        // Hyper may copy a single oversized frame before the socket drains it.
        // Keep the remainder here so its admission survives that copy.
        let chunk = bytes.split_to(bytes.len().min(DELIVERY_CHUNK));
        self.pending = (!bytes.is_empty()).then_some(bytes);
        let chunk = match &self.permit {
            Some(permit) => Bytes::from_owner(HeldBytes {
                bytes: chunk,
                _permit: permit.clone(),
            }),
            None => chunk,
        };
        if self.pending.is_none() && self.inner.is_end_stream() {
            self.permit.take();
        }
        http_body::Frame::data(chunk)
    }
}

impl HttpBody for HeldBody {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, Self::Error>>> {
        if self
            .connection
            .as_ref()
            .is_some_and(|connection| connection.poll_aborted(cx))
        {
            self.pending = None;
            self.inner = Body::empty();
            self.permit = None;
            self.connection = None;
            return Poll::Ready(Some(Err(axum::Error::new(std::io::Error::new(
                std::io::ErrorKind::ConnectionAborted,
                "stalled delivery reclaimed",
            )))));
        }
        if let Some(bytes) = self.pending.take() {
            return Poll::Ready(Some(Ok(self.frame(bytes))));
        }
        let result = match Pin::new(&mut self.inner).poll_frame(cx) {
            Poll::Ready(Some(Ok(frame))) => match frame.into_data() {
                Ok(bytes) => return Poll::Ready(Some(Ok(self.frame(bytes)))),
                Err(frame) => Poll::Ready(Some(Ok(frame))),
            },
            other => other,
        };
        if matches!(result, Poll::Ready(None | Some(Err(_)))) || self.inner.is_end_stream() {
            self.permit.take();
        }
        result
    }

    fn is_end_stream(&self) -> bool {
        self.pending.is_none() && self.inner.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        let inner = self.inner.size_hint();
        let buffered = self.pending.as_ref().map_or(0, |bytes| bytes.len() as u64);
        let mut hint = http_body::SizeHint::new();
        hint.set_lower(inner.lower().saturating_add(buffered));
        if let Some(upper) = inner.upper().and_then(|upper| upper.checked_add(buffered)) {
            hint.set_upper(upper);
        }
        hint
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{convert::Infallible, net::Ipv4Addr, task::Waker};

    use axum::{Router, http::Request as HttpRequest, middleware, routing::get};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    fn ip(n: u32) -> IpAddr {
        Ipv4Addr::from(n).into()
    }

    fn active(limits: &RequestLimits, ip: IpAddr) -> u8 {
        limits.registry.lock().unwrap().addresses[&ip].in_flight
    }

    fn request(address: Option<IpAddr>) -> Request {
        let mut request = HttpRequest::builder().uri("/").body(Body::empty()).unwrap();
        if let Some(ip) = address {
            request.extensions_mut().insert(TrustedClientIp(ip));
        }
        request
    }

    fn content_request(address: IpAddr, file: usize) -> Request {
        let mut request = request(Some(address));
        *request.uri_mut() = format!("/content/ipfs/package/sample-{file}.wav")
            .parse()
            .unwrap();
        request
    }

    fn content_router(limits: RequestLimits) -> Router {
        Router::new()
            .route("/content/ipfs/{*path}", get(|| async { "sample" }))
            .layer(middleware::from_fn_with_state(limits, admit))
    }

    async fn sample_burst(visitors: u32) {
        let limits = RequestLimits::with_content_limit(4).artwork();
        let app = content_router(limits.clone());
        let barrier = Arc::new(tokio::sync::Barrier::new(visitors as usize * 150));
        let mut tasks = tokio::task::JoinSet::new();
        for visitor in 1..=visitors {
            for file in 0..150 {
                let (app, barrier) = (app.clone(), barrier.clone());
                tasks.spawn(async move {
                    barrier.wait().await;
                    let response = app
                        .oneshot(content_request(ip(visitor), file))
                        .await
                        .unwrap();
                    assert_eq!(response.status(), StatusCode::OK);
                    assert_eq!(
                        response.into_body().collect().await.unwrap().to_bytes(),
                        "sample"
                    );
                });
            }
        }
        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
        for visitor in 1..=visitors {
            assert_eq!(active(&limits, ip(visitor)), 0);
            assert_eq!(
                limits.registry.lock().unwrap().addresses[&ip(visitor)].waiting,
                0
            );
        }
        assert_eq!(limits.waiting.available_permits(), CONTENT_WAITERS);
    }

    #[tokio::test]
    async fn one_visitor_can_fetch_150_samples_at_once() {
        sample_burst(1).await;
    }

    #[tokio::test]
    async fn two_visitors_can_fetch_150_samples_each_independently() {
        sample_burst(2).await;
    }

    #[test]
    fn content_burst_is_larger_but_active_limit_and_other_routes_stay_bounded() {
        let limits = RequestLimits::new().artwork();
        let now = Instant::now();
        let held: Vec<_> = (0..MAX_IN_FLIGHT)
            .map(|_| limits.acquire_kind(ip(1), now, true).unwrap())
            .collect();
        assert_eq!(
            limits.acquire_kind(ip(1), now, true).err(),
            Some(StatusCode::TOO_MANY_REQUESTS)
        );
        drop(held);
        for _ in usize::from(MAX_IN_FLIGHT)..256 {
            drop(limits.acquire_kind(ip(1), now, true).unwrap());
        }
        assert!(limits.acquire_kind(ip(1), now, true).is_err());
        for _ in 0..40 {
            drop(limits.acquire(ip(1), now).unwrap());
        }
        assert!(limits.acquire(ip(1), now).is_err());
        drop(
            limits
                .acquire_kind(ip(1), now + TOKEN_INTERVAL, true)
                .unwrap(),
        );
        assert!(
            limits
                .acquire_kind(ip(1), now + TOKEN_INTERVAL, true)
                .is_err()
        );
    }

    #[tokio::test]
    async fn exhausted_content_tokens_wait_for_refill_instead_of_returning_429() {
        let limits = RequestLimits::new().artwork();
        let now = Instant::now();
        for _ in 0..256 {
            drop(limits.acquire_kind(ip(1), now, true).unwrap());
        }
        let response = content_router(limits.clone())
            .oneshot(content_request(ip(1), 0))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        drop(response);
        assert_eq!(active(&limits, ip(1)), 0);
        assert_eq!(limits.waiting.available_permits(), CONTENT_WAITERS);
    }

    #[tokio::test]
    async fn full_client_queue_refuses_excess_but_not_another_visitor() {
        let limits = RequestLimits::new().artwork();
        let held: Vec<_> = (0..CONTENT_WAITERS_PER_CLIENT)
            .map(|_| limits.enqueue(ip(1), Instant::now()).unwrap())
            .collect();
        let app = content_router(limits.clone());
        let refused = app
            .clone()
            .oneshot(content_request(ip(1), 0))
            .await
            .unwrap();
        assert_eq!(refused.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(refused.headers()[header::ACCESS_CONTROL_ALLOW_ORIGIN], "*");
        assert_eq!(refused.headers()[header::RETRY_AFTER], "1");
        let metrics = limits.metrics_handle();
        assert_eq!(metrics.snapshot().client_content_wait_full, 1);
        assert_eq!(active(&limits, ip(1)), 0);
        let other = app
            .clone()
            .oneshot(content_request(ip(2), 0))
            .await
            .unwrap();
        assert_eq!(other.status(), StatusCode::OK);
        drop(other);
        drop(held);
        assert_eq!(limits.waiting.available_permits(), CONTENT_WAITERS);
        assert_eq!(metrics.snapshot().client_content_wait_full, 1);
        assert_eq!(
            app.oneshot(content_request(ip(1), 0))
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
    }

    #[test]
    fn total_waiters_are_bounded_and_waiting_entries_cannot_be_evicted() {
        let limits = RequestLimits::new().artwork();
        let now = Instant::now();
        let held: Vec<_> = (0..CONTENT_WAITERS)
            .map(|n| limits.enqueue(ip((n / 256) as u32), now).unwrap())
            .collect();
        assert_eq!(
            limits.enqueue(ip(100), now).err(),
            Some(StatusCode::SERVICE_UNAVAILABLE)
        );
        let metrics = limits.metrics_handle();
        assert_eq!(metrics.snapshot().global_content_wait_full, 1);
        assert_eq!(limits.waiting.available_permits(), 0);
        for n in 8..MAX_ADDRESSES as u32 + 1 {
            drop(limits.acquire(ip(n), now).unwrap());
        }
        drop(limits.acquire(ip(100), now + IDLE_TTL * 2).unwrap());
        assert_eq!(
            limits.registry.lock().unwrap().addresses[&ip(0)].waiting,
            CONTENT_WAITERS_PER_CLIENT
        );
        drop(held);
        assert_eq!(limits.waiting.available_permits(), CONTENT_WAITERS);
        assert!(limits.enqueue(ip(100), Instant::now()).is_ok());
        assert_eq!(metrics.snapshot().global_content_wait_full, 1);
    }

    #[tokio::test]
    async fn cancelled_content_waiters_release_all_queue_and_active_slots() {
        let limits = RequestLimits::with_content_limit(1).artwork();
        let app = content_router(limits.clone());
        let held = app
            .clone()
            .oneshot(content_request(ip(1), 0))
            .await
            .unwrap();
        let mut tasks = tokio::task::JoinSet::new();
        for n in 0..24 {
            tasks.spawn(app.clone().oneshot(content_request(ip(1), n)));
        }
        tokio::time::timeout(Duration::from_secs(2), async {
            while limits.registry.lock().unwrap().addresses[&ip(1)].waiting != 24 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(active(&limits, ip(1)) <= MAX_IN_FLIGHT);
        tasks.abort_all();
        while let Some(result) = tasks.join_next().await {
            assert!(result.unwrap_err().is_cancelled());
        }
        assert_eq!(active(&limits, ip(1)), 1);
        assert_eq!(limits.registry.lock().unwrap().addresses[&ip(1)].waiting, 0);
        assert_eq!(limits.waiting.available_permits(), CONTENT_WAITERS);
        drop(held);
        assert_eq!(active(&limits, ip(1)), 0);
        assert_eq!(
            app.oneshot(content_request(ip(1), 0))
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
    }

    #[tokio::test]
    async fn content_wait_deadline_refunds_slots_and_exposes_retry_headers() {
        let limits = RequestLimits::with_content_limit(1).artwork();
        let app = content_router(limits.clone());
        let held = app
            .clone()
            .oneshot(content_request(ip(1), 0))
            .await
            .unwrap();
        let response = tokio::time::timeout(
            STREAM_WAIT + Duration::from_secs(2),
            app.clone().oneshot(content_request(ip(1), 1)),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(response.headers()[header::ACCESS_CONTROL_ALLOW_ORIGIN], "*");
        assert_eq!(response.headers()[header::RETRY_AFTER], "1");
        let metrics = limits.metrics_handle();
        assert_eq!(metrics.snapshot().content_wait_timeout, 1);
        assert_eq!(active(&limits, ip(1)), 1);
        assert_eq!(limits.registry.lock().unwrap().addresses[&ip(1)].waiting, 0);
        assert_eq!(limits.waiting.available_permits(), CONTENT_WAITERS);
        drop(held);
        assert_eq!(metrics.snapshot().content_wait_timeout, 1);
        assert_eq!(
            app.oneshot(content_request(ip(1), 0))
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
    }

    #[tokio::test]
    async fn content_errors_include_cors_even_before_identity_or_after_admission() {
        for artwork in [true, false] {
            let limits = RequestLimits::new();
            let app = protect(
                Router::new().route(
                    "/",
                    get(|| async { (StatusCode::SERVICE_UNAVAILABLE, [("retry-after", "7")]) }),
                ),
                if artwork { limits.artwork() } else { limits },
                false,
            );
            let missing = app.clone().oneshot(request(None)).await.unwrap();
            assert_eq!(missing.status(), StatusCode::SERVICE_UNAVAILABLE);
            assert_eq!(
                missing
                    .headers()
                    .contains_key(header::ACCESS_CONTROL_ALLOW_ORIGIN),
                artwork
            );
            if artwork {
                assert_eq!(missing.headers()[header::RETRY_AFTER], "1");
            }
            let mut req = request(None);
            req.extensions_mut()
                .insert(axum::extract::ConnectInfo(crate::client_ip::PeerAddr(
                    "192.0.2.1:1234".parse().unwrap(),
                )));
            let response = app.oneshot(req).await.unwrap();
            assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
            assert_eq!(response.headers()[header::RETRY_AFTER], "7");
            assert_eq!(
                response
                    .headers()
                    .contains_key(header::ACCESS_CONTROL_ALLOW_ORIGIN),
                artwork
            );
        }
    }

    #[test]
    fn addresses_have_independent_bursts_and_exact_bounded_refills() {
        let limits = RequestLimits::new();
        let now = Instant::now();
        for _ in 0..40 {
            drop(limits.acquire(ip(1), now).unwrap());
        }
        assert_eq!(
            limits.acquire(ip(1), now).err(),
            Some(StatusCode::TOO_MANY_REQUESTS)
        );
        drop(limits.acquire(ip(2), now).unwrap());
        assert!(
            limits
                .acquire(ip(1), now + Duration::from_millis(49))
                .is_err()
        );
        drop(
            limits
                .acquire(ip(1), now + Duration::from_millis(50))
                .unwrap(),
        );
        assert!(
            limits
                .acquire(ip(1), now + Duration::from_millis(50))
                .is_err()
        );
        for _ in 0..40 {
            drop(limits.acquire(ip(1), now + Duration::from_secs(3)).unwrap());
        }
        assert!(limits.acquire(ip(1), now + Duration::from_secs(3)).is_err());
    }

    #[test]
    fn noncontent_bucket_rejection_counts_once_and_refills_without_incrementing() {
        let limits = RequestLimits::new();
        let metrics = limits.metrics_handle();
        let now = Instant::now();
        for _ in 0..40 {
            drop(limits.acquire(ip(1), now).unwrap());
        }
        assert_eq!(
            limits.acquire(ip(1), now).err(),
            Some(StatusCode::TOO_MANY_REQUESTS)
        );
        assert_eq!(metrics.snapshot().noncontent_bucket_rejected, 1);
        drop(limits.acquire(ip(1), now + TOKEN_INTERVAL).unwrap());
        assert_eq!(metrics.snapshot().noncontent_bucket_rejected, 1);
    }

    #[test]
    fn sixteenth_active_request_is_allowed_and_seventeenth_rejected() {
        let limits = RequestLimits::new();
        let now = Instant::now();
        let mut held: Vec<_> = (0..16)
            .map(|_| limits.acquire(ip(1), now).unwrap())
            .collect();
        assert_eq!(active(&limits, ip(1)), 16);
        assert_eq!(
            limits.acquire(ip(1), now).err(),
            Some(StatusCode::TOO_MANY_REQUESTS)
        );
        drop(limits.acquire(ip(2), now).unwrap());
        held.pop();
        drop(limits.acquire(ip(1), now).unwrap());
        drop(held);
        assert_eq!(active(&limits, ip(1)), 0);
    }

    #[test]
    fn ipv6_prefixes_and_mapped_ipv4_share_rate_and_active_budgets() {
        for (first, same, separate) in [
            ("2001:db8:1:2::1", "2001:db8:1:2::abcd", "2001:db8:1:3::1"),
            ("192.0.2.1", "::ffff:192.0.2.1", "192.0.2.2"),
        ] {
            let first = first.parse().unwrap();
            let same = same.parse().unwrap();
            let separate = separate.parse().unwrap();
            let limits = RequestLimits::new();
            let now = Instant::now();
            let held: Vec<_> = (0..16)
                .map(|n| {
                    limits
                        .acquire(if n % 2 == 0 { first } else { same }, now)
                        .unwrap()
                })
                .collect();
            assert!(limits.acquire(same, now).is_err());
            drop(limits.acquire(separate, now).unwrap());
            drop(held);
            for _ in 16..40 {
                drop(limits.acquire(first, now).unwrap());
            }
            assert!(limits.acquire(same, now).is_err());
            drop(limits.acquire(separate, now).unwrap());
            assert_eq!(limits.registry.lock().unwrap().addresses.len(), 2);
        }
    }

    #[test]
    fn subnet_churn_never_blocks_new_neighbours_or_other_host() {
        let limits = RequestLimits::new();
        let metrics = limits.metrics_handle();
        let artwork = limits.artwork();
        let now = Instant::now();
        let subnet = |n| format!("2001:db8:1234:{n:x}::1").parse().unwrap();
        for n in 0..MAX_ADDRESSES + 100 {
            drop(limits.acquire(subnet(n), now).unwrap());
        }
        drop(limits.acquire(subnet(MAX_ADDRESSES + 101), now).unwrap());
        assert_eq!(metrics.snapshot().family_new_client_rejected, 0);
        let held: Vec<_> = (0..16)
            .map(|_| limits.acquire(subnet(0), now).unwrap())
            .collect();
        drop(limits.acquire(subnet(1), now).unwrap());
        drop(artwork.acquire(subnet(8), now).unwrap());
        drop(limits.acquire(subnet(8), now).unwrap());
        assert_eq!(metrics.snapshot().family_new_client_rejected, 0);
        assert_eq!(
            limits.registry.lock().unwrap().addresses.len(),
            MAX_ADDRESSES
        );
        drop(held);
    }

    #[test]
    fn failed_admission_and_last_drop_keep_idle_lru_consistent() {
        let limits = RequestLimits::new();
        let now = Instant::now();
        for _ in 0..40 {
            drop(limits.acquire(ip(1), now).unwrap());
        }
        drop(limits.acquire(ip(2), now).unwrap());
        assert!(limits.acquire(ip(1), now).is_err());
        {
            let registry = limits.registry.lock().unwrap();
            assert_eq!(registry.idle_head, Some(ip(2)));
            assert_eq!(registry.idle_tail, Some(ip(1)));
            assert_eq!(registry.addresses[&ip(2)].next, Some(ip(1)));
            assert_eq!(registry.addresses[&ip(1)].previous, Some(ip(2)));
        }
        let queued = limits.enqueue(ip(2), now).unwrap();
        let active = limits.acquire(ip(2), now).unwrap();
        drop(queued);
        assert!(!limits.registry.lock().unwrap().addresses[&ip(2)].idle);
        drop(active);
        let mut registry = limits.registry.lock().unwrap();
        assert_eq!(registry.idle_head, Some(ip(1)));
        assert_eq!(registry.idle_tail, Some(ip(2)));
        registry.expire_idle(now + IDLE_TTL * 2);
        assert!(registry.addresses.is_empty());
        assert_eq!(registry.idle_head, None);
        assert_eq!(registry.idle_tail, None);
    }

    #[tokio::test]
    async fn artwork_stream_caps_are_per_prefix_not_shared_with_neighbours() {
        let limits = RequestLimits::with_content_limit(1).artwork();
        let now = Instant::now();
        let first = limits
            .acquire("2001:db8:1:1::1".parse().unwrap(), now)
            .unwrap();
        let second = limits
            .acquire("2001:db8:1:2::1".parse().unwrap(), now)
            .unwrap();
        let held = first.stream_slots().await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(20), first.stream_slots())
                .await
                .is_err()
        );
        drop(second.stream_slots().await.unwrap());
        drop(held);
        drop(first.stream_slots().await.unwrap());
    }

    #[tokio::test]
    async fn expired_stream_wait_returns_retryable_status_and_does_not_leak_admission() {
        let limits = RequestLimits::with_content_limit(1).artwork();
        let app = Router::new()
            .route("/", get(|| async { "payload" }))
            .layer(middleware::from_fn_with_state(limits.clone(), admit));
        let held = app.clone().oneshot(request(Some(ip(1)))).await.unwrap();
        let metrics = limits.metrics_handle();
        let expired = tokio::time::timeout(
            STREAM_WAIT + Duration::from_secs(2),
            app.clone().oneshot(request(Some(ip(1)))),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(expired.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(metrics.snapshot().noncontent_stream_timeout, 1);
        assert_eq!(expired.headers()["retry-after"], "1");
        assert_eq!(active(&limits, ip(1)), 1);
        drop(held);
        let response = app.oneshot(request(Some(ip(1)))).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(metrics.snapshot().noncontent_stream_timeout, 1);
    }

    #[tokio::test]
    async fn streamed_frames_hold_client_slots_until_their_final_bytes_drop() {
        let limits = RequestLimits::with_content_limit(1).artwork();
        let app = Router::new()
            .route("/", get(|| async { "payload" }))
            .layer(middleware::from_fn_with_state(limits.clone(), admit));
        let response = app.clone().oneshot(request(Some(ip(1)))).await.unwrap();
        let mut body = response.into_body();
        let frame = body.frame().await.unwrap().unwrap();
        drop(body);
        assert_eq!(
            limits.registry.lock().unwrap().addresses[&ip(1)]
                .streams
                .available_permits(),
            0
        );
        let other = app.clone().oneshot(request(Some(ip(2)))).await.unwrap();
        assert_eq!(other.status(), StatusCode::OK);
        drop(other);
        let waiting = tokio::spawn(app.clone().oneshot(request(Some(ip(1)))));
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());
        waiting.abort();
        let _ = waiting.await;
        assert_eq!(active(&limits, ip(1)), 1);
        drop(frame);
        assert_eq!(active(&limits, ip(1)), 0);
        let response = app.oneshot(request(Some(ip(1)))).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        drop(response);
    }

    #[test]
    fn full_registry_reclaims_oldest_idle_entries_but_preserves_active_entries() {
        let limits = RequestLimits::new();
        let now = Instant::now();
        let held = limits.acquire(ip(0), now).unwrap();
        for address in 1..MAX_ADDRESSES as u32 {
            drop(limits.acquire(ip(address), now).unwrap());
        }
        assert_eq!(
            limits.registry.lock().unwrap().addresses.len(),
            MAX_ADDRESSES
        );
        drop(limits.acquire(ip(MAX_ADDRESSES as u32), now).unwrap());
        assert_eq!(
            limits.registry.lock().unwrap().addresses.len(),
            MAX_ADDRESSES
        );
        assert_eq!(active(&limits, ip(0)), 1);
        drop(limits.acquire(ip(1), now).unwrap());
        let expired = now + IDLE_TTL + Duration::from_secs(1);
        drop(limits.acquire(ip(MAX_ADDRESSES as u32), expired).unwrap());
        assert_eq!(limits.registry.lock().unwrap().addresses.len(), 2);
        assert_eq!(active(&limits, ip(0)), 1);
        drop(held);
        assert_eq!(active(&limits, ip(0)), 0);
    }

    #[test]
    fn wholly_active_registry_refuses_new_keys_then_recovers_after_drop() {
        let limits = RequestLimits::new();
        let now = Instant::now();
        let mut held: Vec<_> = (0..MAX_ADDRESSES as u32)
            .map(|n| limits.acquire(ip(n), now).unwrap())
            .collect();
        let metrics = limits.metrics_handle();
        assert!(
            limits
                .acquire(ip(MAX_ADDRESSES as u32), now + IDLE_TTL)
                .is_err()
        );
        assert_eq!(metrics.snapshot().client_table_full, 1);
        held.pop();
        drop(
            limits
                .acquire(ip(MAX_ADDRESSES as u32), now + IDLE_TTL)
                .unwrap(),
        );
        assert_eq!(
            limits.registry.lock().unwrap().addresses.len(),
            MAX_ADDRESSES
        );
        assert_eq!(metrics.snapshot().client_table_full, 1);
        assert_eq!(active(&limits, ip(0)), 1);
    }

    #[tokio::test]
    async fn missing_identity_fails_closed_and_empty_responses_keep_body_metadata() {
        let limits = RequestLimits::default();
        let app = Router::new()
            .route("/", get(|| async { StatusCode::NO_CONTENT }))
            .layer(middleware::from_fn_with_state(limits.clone(), admit));
        let response = app.clone().oneshot(request(None)).await.unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers()["cache-control"], "no-store");
        assert_eq!(response.headers()["x-content-type-options"], "nosniff");
        assert!(limits.registry.lock().unwrap().addresses.is_empty());
        let metrics = limits.metrics_handle();
        assert_eq!(metrics.snapshot().missing_client_ip, 1);
        let response = app.oneshot(request(Some(ip(1)))).await.unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert!(response.body().is_end_stream());
        assert_eq!(response.body().size_hint().exact(), Some(0));
        assert_eq!(active(&limits, ip(1)), 0);
        assert_eq!(metrics.snapshot().missing_client_ip, 1);
    }

    #[test]
    fn metrics_snapshot_serializes_and_counters_saturate() {
        let counters = Arc::new(RequestLimitCounters::default());
        counters
            .global_content_wait_full
            .store(u64::MAX, Ordering::Relaxed);
        RequestLimitCounters::increment(&counters.global_content_wait_full);
        let snapshot = RequestLimitMetricsHandle { counters }.snapshot();
        assert_eq!(snapshot.global_content_wait_full, u64::MAX);
        let json = serde_json::to_value(snapshot).unwrap();
        assert_eq!(json["global_content_wait_full"], u64::MAX);
        assert_eq!(json["missing_client_ip"], 0);
    }

    #[tokio::test]
    async fn cloned_listener_states_share_active_body_limits() {
        let limits = RequestLimits::new();
        let app = Router::new()
            .route("/", get(|| async { "app" }))
            .layer(middleware::from_fn_with_state(limits.clone(), admit));
        let other_listener = Router::new()
            .route("/", get(|| async { "content" }))
            .layer(middleware::from_fn_with_state(limits.clone(), admit));
        let mut responses = Vec::new();
        for n in 0..16 {
            let router = if n % 2 == 0 { &app } else { &other_listener };
            let response = router.clone().oneshot(request(Some(ip(1)))).await.unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            responses.push(response);
        }
        let response = app.oneshot(request(Some(ip(1)))).await.unwrap();
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(response.headers()["retry-after"], "1");
        assert_eq!(response.headers()["cache-control"], "no-store");
        assert_eq!(response.headers()["x-content-type-options"], "nosniff");
        drop(responses);
        assert_eq!(active(&limits, ip(1)), 0);
        assert_eq!(
            other_listener
                .oneshot(request(Some(ip(1))))
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
    }

    #[tokio::test]
    async fn content_length_and_body_metadata_survive_until_the_last_frame_drops() {
        let limits = RequestLimits::new();
        let app = Router::new()
            .route("/", get(|| async { "hello" }))
            .layer(middleware::from_fn_with_state(limits.clone(), admit));
        let response = app.oneshot(request(Some(ip(1)))).await.unwrap();
        assert_eq!(response.headers()["content-length"], "5");
        let mut body = response.into_body();
        assert!(!body.is_end_stream());
        assert_eq!(body.size_hint().exact(), Some(5));
        let frame = body.frame().await.unwrap().unwrap();
        assert_eq!(frame.data_ref().unwrap(), "hello");
        assert!(body.is_end_stream());
        assert_eq!(body.size_hint().exact(), Some(0));
        assert_eq!(active(&limits, ip(1)), 1);
        assert!(body.frame().await.is_none());
        drop(body);
        assert_eq!(active(&limits, ip(1)), 1);
        drop(frame);
        assert_eq!(active(&limits, ip(1)), 0);
    }

    #[tokio::test]
    async fn copying_large_frames_cannot_release_delivery_before_the_remainder_drains() {
        let limits = RequestLimits::new();
        let length = DELIVERY_CHUNK * 3 + 1;
        let mut body = hold(
            Body::from(vec![42; length]),
            limits.acquire(ip(1), Instant::now()).unwrap(),
        );
        let mut read = 0;
        while let Some(frame) = body.frame().await {
            let bytes = frame.unwrap().into_data().unwrap();
            assert!(bytes.len() <= DELIVERY_CHUNK);
            read += bytes.len();
            let copied = Bytes::copy_from_slice(&bytes);
            drop(bytes);
            if read < length {
                assert_eq!(active(&limits, ip(1)), 1);
                assert!(!body.is_end_stream());
            }
            assert_eq!(body.size_hint().exact(), Some((length - read) as u64));
            assert!(copied.iter().all(|byte| *byte == 42));
        }
        assert_eq!(read, length);
        assert_eq!(active(&limits, ip(1)), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn transport_cancellation_clears_outer_pending_bytes_but_preserves_held_frame_slots() {
        use crate::delivery_control::{Client, Connection, Registry};
        let limits = RequestLimits::new();
        let connection = Connection::new();
        connection.enable_reclamation();
        let client = Client::new(ip(1), connection.clone());
        client.ready();
        let registry = Registry::default();
        let allocation = registry.allocate(DELIVERY_CHUNK * 3).unwrap();
        let _stream = allocation.register(client).unwrap();
        let mut response = Response::new(Body::from(vec![42; DELIVERY_CHUNK * 3]));
        response.extensions_mut().insert(connection);
        let permit = limits.acquire(ip(1), Instant::now()).unwrap();
        let mut body = hold_response(response, permit).into_body();
        let frame = body.frame().await.unwrap().unwrap();
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(registry.reclaim());
        assert!(body.frame().await.unwrap().is_err());
        assert!(body.is_end_stream());
        assert_eq!(body.size_hint().exact(), Some(0));
        drop(body);
        assert_eq!(active(&limits, ip(1)), 1);
        drop(frame);
        assert_eq!(active(&limits, ip(1)), 0);
    }

    struct PendingBody;
    impl HttpBody for PendingBody {
        type Data = Bytes;
        type Error = Infallible;
        fn poll_frame(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
        ) -> Poll<Option<Result<http_body::Frame<Bytes>, Self::Error>>> {
            Poll::Pending
        }
    }

    struct FailingBody;
    impl HttpBody for FailingBody {
        type Data = Bytes;
        type Error = std::io::Error;
        fn poll_frame(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
        ) -> Poll<Option<Result<http_body::Frame<Bytes>, Self::Error>>> {
            Poll::Ready(Some(Err(std::io::Error::other("fixture failure"))))
        }
    }

    struct EofBody;
    impl HttpBody for EofBody {
        type Data = Bytes;
        type Error = Infallible;
        fn poll_frame(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
        ) -> Poll<Option<Result<http_body::Frame<Bytes>, Self::Error>>> {
            Poll::Ready(None)
        }
    }

    #[tokio::test]
    async fn pending_drop_error_and_eof_release_slots() {
        let limits = RequestLimits::new();
        let mut pending = hold(
            Body::new(PendingBody),
            limits.acquire(ip(1), Instant::now()).unwrap(),
        );
        assert_eq!(pending.size_hint().upper(), None);
        let mut cx = Context::from_waker(Waker::noop());
        assert!(Pin::new(&mut pending).poll_frame(&mut cx).is_pending());
        assert_eq!(active(&limits, ip(1)), 1);
        drop(pending);
        assert_eq!(active(&limits, ip(1)), 0);
        let mut failing = hold(
            Body::new(FailingBody),
            limits.acquire(ip(1), Instant::now()).unwrap(),
        );
        assert!(failing.frame().await.unwrap().is_err());
        assert_eq!(active(&limits, ip(1)), 0);
        let mut eof = hold(
            Body::new(EofBody),
            limits.acquire(ip(1), Instant::now()).unwrap(),
        );
        assert!(!eof.is_end_stream());
        assert!(eof.frame().await.is_none());
        assert_eq!(active(&limits, ip(1)), 0);
    }

    #[tokio::test]
    async fn cancellation_during_handler_releases_the_slot() {
        let limits = RequestLimits::new();
        let entered = Arc::new(tokio::sync::Notify::new());
        let signal = entered.clone();
        let app = Router::new()
            .route(
                "/",
                get(move || async move {
                    signal.notify_one();
                    std::future::pending::<StatusCode>().await
                }),
            )
            .layer(middleware::from_fn_with_state(limits.clone(), admit));
        let task = tokio::spawn(app.oneshot(request(Some(ip(1)))));
        entered.notified().await;
        assert_eq!(active(&limits, ip(1)), 1);
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(active(&limits, ip(1)), 0);
    }
}
