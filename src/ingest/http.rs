//! Polite JSON over HTTP: a pluggable transport, bounded retries with backoff for transient
//! failures, a shared request pace, and a stop flag checked between attempts.
//!
//! Outcome classes:
//! - `Ok(Some(value))`: HTTP 200 with JSON.
//! - `Ok(None)`: HTTP 404 or 204, a confirmed absence.
//! - `Err(Transient)`: connection trouble, timeouts, 408, 429 or 5xx after every retry.
//! - `Err(Permanent)`: other statuses or an unusable body; retrying cannot help.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use serde::de::DeserializeOwned;

use super::USER_AGENT;

/// Largest accepted response body.
const MAX_BODY: u64 = 256 << 20;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpResponse {
    pub status: u16,
    pub body: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TransportError {
    #[error("transient transport error: {0}")]
    Transient(String),
    #[error("transport error: {0}")]
    Permanent(String),
}

/// One request. Implementations must be safe to share between worker threads.
pub trait Fetch: Send + Sync {
    fn get(&self, url: &str) -> Result<HttpResponse, TransportError>;

    /// One POST with a JSON body. A transport that cannot post refuses permanently.
    fn post_json(&self, url: &str, body: &[u8]) -> Result<HttpResponse, TransportError> {
        let _ = (url, body);
        Err(TransportError::Permanent(
            "this transport cannot POST".into(),
        ))
    }
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

/// Production transport: the crate's async reqwest client (rustls), driven from blocking
/// worker threads through a tokio runtime handle. Must not be called from async code.
pub struct ReqwestFetch {
    client: reqwest::Client,
    runtime: tokio::runtime::Handle,
    max_body: u64,
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
        Ok(ReqwestFetch {
            client,
            runtime,
            max_body: MAX_BODY,
        })
    }

    /// Refuses (permanently) bodies larger than `bytes`; the default is 256 MiB.
    pub fn with_max_body(mut self, bytes: u64) -> ReqwestFetch {
        self.max_body = bytes.min(MAX_BODY);
        self
    }

    fn send(&self, request: reqwest::RequestBuilder) -> Result<HttpResponse, TransportError> {
        let max_body = self.max_body;
        self.runtime.block_on(async {
            let mut resp = request
                .header(reqwest::header::ACCEPT, "application/json")
                .send()
                .await
                .map_err(classify_reqwest)?;
            let status = resp.status().as_u16();
            if resp.content_length().is_some_and(|n| n > max_body) {
                return Err(TransportError::Permanent(format!(
                    "response body larger than {max_body} bytes"
                )));
            }
            let mut body = Vec::new();
            while let Some(chunk) = resp.chunk().await.map_err(classify_reqwest)? {
                if (body.len() + chunk.len()) as u64 > max_body {
                    return Err(TransportError::Permanent(format!(
                        "response body larger than {max_body} bytes"
                    )));
                }
                body.extend_from_slice(&chunk);
            }
            Ok(HttpResponse { status, body })
        })
    }
}

fn classify_reqwest(e: reqwest::Error) -> TransportError {
    if e.is_builder() || e.is_redirect() {
        TransportError::Permanent(e.to_string())
    } else {
        TransportError::Transient(e.to_string())
    }
}

impl Fetch for ReqwestFetch {
    fn get(&self, url: &str) -> Result<HttpResponse, TransportError> {
        self.send(self.client.get(url))
    }

    fn post_json(&self, url: &str, body: &[u8]) -> Result<HttpResponse, TransportError> {
        self.send(
            self.client
                .post(url)
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(body.to_vec()),
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Total attempts per request, at least 1.
    pub attempts: u32,
    /// Delay before the second attempt; doubles each time up to `max_delay`.
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

/// Request counters, for logs and tests.
#[derive(Debug, Default)]
pub struct Counters {
    pub requests: AtomicU64,
    pub retries: AtomicU64,
}

pub struct Client {
    fetch: Arc<dyn Fetch>,
    base: String,
    retry: RetryPolicy,
    /// Minimum spacing between request starts, across all threads.
    pace: Duration,
    next_start: Mutex<Instant>,
    stop: Arc<AtomicBool>,
    pub counters: Counters,
}

impl Client {
    pub fn new(
        fetch: Arc<dyn Fetch>,
        base: &str,
        retry: RetryPolicy,
        pace: Duration,
        stop: Arc<AtomicBool>,
    ) -> Client {
        Client {
            fetch,
            base: base.trim_end_matches('/').to_owned(),
            retry,
            pace,
            next_start: Mutex::new(Instant::now()),
            stop,
            counters: Counters::default(),
        }
    }

    pub fn stopped(&self) -> bool {
        self.stop.load(Ordering::Relaxed)
    }

    pub fn base(&self) -> &str {
        &self.base
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

    /// Sleeps up to `d`, waking early when stopped.
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
        self.request(path, |url| self.fetch.get(url))
    }

    /// POST a JSON body to `base + path`, with the same outcome classes as [`Client::get_json`].
    /// Only for idempotent reads (a GraphQL query), since transient failures are retried.
    pub fn post_json<T: DeserializeOwned>(
        &self,
        path: &str,
        body: &serde_json::Value,
    ) -> Result<Option<T>, ApiError> {
        let bytes = serde_json::to_vec(body)
            .map_err(|e| ApiError::Permanent(format!("unencodable request body: {e}")))?;
        self.request(path, |url| self.fetch.post_json(url, &bytes))
    }

    fn request<T: DeserializeOwned>(
        &self,
        path: &str,
        send: impl Fn(&str) -> Result<HttpResponse, TransportError>,
    ) -> Result<Option<T>, ApiError> {
        let url = format!("{}{path}", self.base);
        let attempts = self.retry.attempts.max(1);
        let mut delay = self.retry.base_delay;
        let mut last = String::new();
        for attempt in 1..=attempts {
            if self.stopped() {
                return Err(ApiError::Interrupted);
            }
            if attempt > 1 {
                self.counters.retries.fetch_add(1, Ordering::Relaxed);
                self.pause(delay)?;
                delay = (delay * 2).min(self.retry.max_delay);
            }
            self.wait_turn();
            self.counters.requests.fetch_add(1, Ordering::Relaxed);
            match send(&url) {
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
                Err(TransportError::Permanent(e)) => {
                    return Err(ApiError::Permanent(format!("{e} ({path})")));
                }
            }
        }
        Err(ApiError::Transient(format!(
            "{last}; gave up after {attempts} attempts"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;

    struct Script(Mutex<VecDeque<Result<HttpResponse, TransportError>>>);

    impl Fetch for Script {
        fn get(&self, _url: &str) -> Result<HttpResponse, TransportError> {
            self.0
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected request")
        }
    }

    fn client(script: Vec<Result<HttpResponse, TransportError>>) -> Client {
        let retry = RetryPolicy {
            attempts: 3,
            base_delay: Duration::ZERO,
            max_delay: Duration::ZERO,
        };
        Client::new(
            Arc::new(Script(Mutex::new(script.into()))),
            "http://x/v1/",
            retry,
            Duration::ZERO,
            Arc::new(AtomicBool::new(false)),
        )
    }

    fn status(status: u16, body: &str) -> Result<HttpResponse, TransportError> {
        Ok(HttpResponse {
            status,
            body: body.as_bytes().to_vec(),
        })
    }

    #[test]
    fn transient_failures_are_retried_until_success() {
        let c = client(vec![
            Err(TransportError::Transient("reset".into())),
            status(503, ""),
            status(200, "[1]"),
        ]);
        assert_eq!(c.get_json::<Vec<u8>>("/a").unwrap(), Some(vec![1]));
        assert_eq!(c.counters.requests.load(Ordering::Relaxed), 3);
        assert_eq!(c.counters.retries.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn exhausted_retries_stay_transient() {
        let c = client(vec![status(429, ""), status(502, ""), status(500, "")]);
        assert!(matches!(
            c.get_json::<Vec<u8>>("/a"),
            Err(ApiError::Transient(_))
        ));
    }

    #[test]
    fn not_found_is_a_confirmed_absence_and_bad_requests_are_permanent() {
        assert_eq!(client(vec![status(404, "")]).get_json::<u8>("/a"), Ok(None));
        assert_eq!(client(vec![status(204, "")]).get_json::<u8>("/a"), Ok(None));
        assert!(matches!(
            client(vec![status(400, "")]).get_json::<u8>("/a"),
            Err(ApiError::Permanent(_))
        ));
        assert!(matches!(
            client(vec![status(200, "{")]).get_json::<u8>("/a"),
            Err(ApiError::Permanent(_))
        ));
        assert!(matches!(
            client(vec![Err(TransportError::Permanent("bad uri".into()))]).get_json::<u8>("/a"),
            Err(ApiError::Permanent(_))
        ));
    }

    #[test]
    fn a_stopped_client_makes_no_request() {
        let c = client(vec![]);
        c.stop.store(true, Ordering::Relaxed);
        assert_eq!(c.get_json::<u8>("/a"), Err(ApiError::Interrupted));
    }
}
