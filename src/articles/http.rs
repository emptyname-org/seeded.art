//! HTTP for the articles sync: a pluggable blocking transport (tests script it) and a JSON
//! client with bounded retries for TzKT. Pattern copied from `ingest::http`, extended with an
//! `Accept` header, a per-request body cap and the response content type.
//!
//! JSON outcome classes:
//! - `Ok(Some(value))`: HTTP 200 with JSON.
//! - `Ok(None)`: HTTP 404 or 204, a confirmed absence.
//! - `Err(Transient)`: connection trouble, timeouts, 408, 429 or 5xx after every retry.
//! - `Err(Permanent)`: other statuses or an unusable body.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use serde::de::DeserializeOwned;

use super::USER_AGENT;

/// Largest accepted TzKT response body.
pub const MAX_JSON_BODY: u64 = 256 << 20;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpResponse {
    pub status: u16,
    /// Media type without parameters, lowercased.
    pub content_type: Option<String>,
    pub body: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TransportError {
    #[error("transient transport error: {0}")]
    Transient(String),
    #[error("transport error: {0}")]
    Permanent(String),
    /// The body exceeded the request's cap; nothing beyond the cap was read.
    #[error("response body larger than {0} bytes")]
    TooLarge(u64),
}

/// One GET. Implementations must be safe to share between worker threads.
pub trait Fetch: Send + Sync {
    fn get(&self, url: &str, accept: &str, cap: u64) -> Result<HttpResponse, TransportError>;
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ApiError {
    #[error("transient API failure: {0}")]
    Transient(String),
    #[error("API failure: {0}")]
    Permanent(String),
    #[error("interrupted")]
    Interrupted,
}

/// Production transport: async reqwest (rustls) driven from blocking threads through a tokio
/// runtime handle. Must not be called from async code.
pub struct ReqwestFetch {
    client: reqwest::Client,
    runtime: tokio::runtime::Handle,
}

impl ReqwestFetch {
    pub fn new(runtime: tokio::runtime::Handle, timeout: Duration) -> Result<ReqwestFetch, String> {
        let client = reqwest::Client::builder()
            .user_agent(USER_AGENT)
            .timeout(timeout)
            .connect_timeout(Duration::from_secs(15).min(timeout))
            .redirect(reqwest::redirect::Policy::limited(3))
            .build()
            .map_err(|e| e.to_string())?;
        Ok(ReqwestFetch { client, runtime })
    }
}

fn classify_reqwest(e: reqwest::Error) -> TransportError {
    if e.is_builder() || e.is_redirect() {
        TransportError::Permanent(e.to_string())
    } else {
        TransportError::Transient(e.to_string())
    }
}

/// `type/subtype` of a Content-Type header, lowercased.
pub fn media_type(header: &str) -> String {
    header
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
}

impl Fetch for ReqwestFetch {
    fn get(&self, url: &str, accept: &str, cap: u64) -> Result<HttpResponse, TransportError> {
        self.runtime.block_on(async {
            let mut resp = self
                .client
                .get(url)
                .header(reqwest::header::ACCEPT, accept)
                .send()
                .await
                .map_err(classify_reqwest)?;
            let status = resp.status().as_u16();
            let content_type = resp
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .map(media_type);
            if resp.content_length().is_some_and(|n| n > cap) {
                return Err(TransportError::TooLarge(cap));
            }
            let mut body = Vec::new();
            while let Some(chunk) = resp.chunk().await.map_err(classify_reqwest)? {
                if (body.len() + chunk.len()) as u64 > cap {
                    return Err(TransportError::TooLarge(cap));
                }
                body.extend_from_slice(&chunk);
            }
            Ok(HttpResponse {
                status,
                content_type,
                body,
            })
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Total attempts per request, at least 1.
    pub attempts: u32,
    /// Delay before the second attempt; doubles up to `max_delay`.
    pub base_delay: Duration,
    pub max_delay: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        RetryPolicy {
            attempts: 6,
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(30),
        }
    }
}

impl RetryPolicy {
    /// One attempt, no delay: for tests.
    pub fn none() -> Self {
        RetryPolicy {
            attempts: 1,
            base_delay: Duration::ZERO,
            max_delay: Duration::ZERO,
        }
    }
}

/// JSON over HTTP with retries, a shared request pace and a stop flag.
pub struct JsonClient {
    fetch: Arc<dyn Fetch>,
    base: String,
    retry: RetryPolicy,
    pace: Duration,
    next_start: Mutex<Instant>,
    stop: Arc<AtomicBool>,
    pub requests: AtomicU64,
    pub retries: AtomicU64,
}

impl JsonClient {
    pub fn new(
        fetch: Arc<dyn Fetch>,
        base: &str,
        retry: RetryPolicy,
        pace: Duration,
        stop: Arc<AtomicBool>,
    ) -> JsonClient {
        JsonClient {
            fetch,
            base: base.trim_end_matches('/').to_owned(),
            retry,
            pace,
            next_start: Mutex::new(Instant::now()),
            stop,
            requests: AtomicU64::new(0),
            retries: AtomicU64::new(0),
        }
    }

    pub fn stopped(&self) -> bool {
        self.stop.load(Ordering::Relaxed)
    }

    fn wait_turn(&self) {
        let mut next = self
            .next_start
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let now = Instant::now();
        if *next > now {
            thread::sleep(*next - now);
        }
        *next = Instant::now() + self.pace;
    }

    fn pause(&self, d: Duration) -> Result<(), ApiError> {
        let until = Instant::now() + d;
        while Instant::now() < until {
            if self.stopped() {
                return Err(ApiError::Interrupted);
            }
            thread::sleep((until - Instant::now()).min(Duration::from_millis(100)));
        }
        Ok(())
    }

    /// GET `base + path`; see the module docs for outcome classes.
    pub fn get_json<T: DeserializeOwned>(&self, path: &str) -> Result<Option<T>, ApiError> {
        let url = format!("{}{path}", self.base);
        let attempts = self.retry.attempts.max(1);
        let mut delay = self.retry.base_delay;
        let mut last = String::new();
        for attempt in 1..=attempts {
            if self.stopped() {
                return Err(ApiError::Interrupted);
            }
            if attempt > 1 {
                self.retries.fetch_add(1, Ordering::Relaxed);
                self.pause(delay)?;
                delay = (delay * 2).min(self.retry.max_delay);
            }
            self.wait_turn();
            self.requests.fetch_add(1, Ordering::Relaxed);
            match self.fetch.get(&url, "application/json", MAX_JSON_BODY) {
                Ok(resp) => match resp.status {
                    200 => {
                        return serde_json::from_slice(&resp.body).map(Some).map_err(|e| {
                            ApiError::Permanent(format!("unusable JSON from {path}: {e}"))
                        });
                    }
                    204 | 404 => return Ok(None),
                    408 | 429 | 500..=599 => last = format!("HTTP {} from {path}", resp.status),
                    s => return Err(ApiError::Permanent(format!("HTTP {s} from {path}"))),
                },
                Err(TransportError::Transient(e)) => last = format!("{e} ({path})"),
                Err(e) => return Err(ApiError::Permanent(format!("{e} ({path})"))),
            }
        }
        Err(ApiError::Transient(format!(
            "{last}; gave up after {attempts} attempts"
        )))
    }
}

/// TzKT API base ending in `/v1`: accepts `https://api.tzkt.io` or `https://api.tzkt.io/v1`,
/// with or without a trailing slash. `None` unless http(s) with a host.
pub fn tzkt_api_base(url: &str) -> Option<String> {
    let url = url.trim().trim_end_matches('/');
    let rest = url
        .strip_prefix("https://")
        .or_else(|| url.strip_prefix("http://"))?;
    if rest.is_empty() || rest.starts_with('/') || url.contains(['?', '#', ' ']) {
        return None;
    }
    Some(if url.ends_with("/v1") {
        url.to_owned()
    } else {
        format!("{url}/v1")
    })
}
