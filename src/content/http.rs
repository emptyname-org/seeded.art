//! Shared outbound HTTP: one client, bounded concurrency, capped bodies.

use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use reqwest::header::{ACCEPT, CONTENT_LENGTH, CONTENT_TYPE, RETRY_AFTER};
use tokio::sync::Semaphore;

use super::stats::ContentCounters;
use super::{ContentConfig, ContentError};

pub(crate) struct Reply {
    pub status: u16,
    pub content_type: Option<String>,
    pub content_encoding: Option<String>,
    pub final_url: String,
    pub retry_after: Option<Duration>,
    pub body: Bytes,
}

#[derive(Debug)]
pub(crate) enum FetchError {
    Busy,
    Timeout,
    Connect(String),
    Network(String),
    TooLarge,
}

impl FetchError {
    pub fn to_content_error(&self, what: &str, cap: u64) -> ContentError {
        match self {
            FetchError::Busy => ContentError::unavailable(format!("{what}: HTTP workers busy")),
            FetchError::Timeout => ContentError::unavailable(format!("{what}: timeout")),
            FetchError::Connect(e) | FetchError::Network(e) => {
                ContentError::unavailable(format!("{what}: {e}"))
            }
            FetchError::TooLarge => ContentError::too_large(cap),
        }
    }
}

pub(crate) struct Http {
    client: reqwest::Client,
    /// For [`Http::stream`], whose bodies are verified against a CID.
    trustless: reqwest::Client,
    permits: Arc<Semaphore>,
    timeout: Duration,
    counters: Arc<ContentCounters>,
}

/// Redirects [`Http::stream`] follows.
const REDIRECTS: usize = 4;

impl Http {
    pub(super) fn deadline(&self, message: &str) -> ContentError {
        self.counters
            .upstream_timeouts
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        ContentError::unavailable(message)
    }

    pub(super) fn busy(&self, message: &str) -> ContentError {
        self.counters
            .upstream_busy
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        ContentError::unavailable(message)
    }

    fn busy_fetch(&self) -> FetchError {
        self.counters
            .upstream_busy
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        FetchError::Busy
    }
    pub fn new(cfg: &ContentConfig) -> Result<Self, ContentError> {
        // Follow redirects only within the same host or its subdomains
        // (path gateways redirect to subdomain gateways); never elsewhere.
        let policy = reqwest::redirect::Policy::custom(|attempt| {
            let origin = attempt.previous().first();
            let next = attempt.url();
            match origin {
                _ if attempt.previous().len() > 3 => attempt.stop(),
                Some(origin) if safe_redirect(origin, next) => attempt.follow(),
                _ => attempt.stop(),
            }
        });
        let trustless =
            reqwest::redirect::Policy::custom(|attempt| match attempt.previous().first() {
                _ if attempt.previous().len() > REDIRECTS => attempt.stop(),
                Some(origin) if trustless_redirect(origin, attempt.url()) => attempt.follow(),
                _ => attempt.stop(),
            });
        let client = |policy| {
            reqwest::Client::builder()
                .user_agent(cfg.user_agent.clone())
                .connect_timeout(cfg.connect_timeout)
                .redirect(policy)
                .build()
                .map_err(|e| ContentError::unavailable(format!("http client: {e}")))
        };
        Ok(Self {
            client: client(policy)?,
            trustless: client(trustless)?,
            permits: Arc::new(Semaphore::new(cfg.max_concurrent_requests)),
            timeout: cfg.request_timeout,
            counters: Arc::new(ContentCounters::default()),
        })
    }

    pub(super) fn with_counters(mut self, counters: Arc<ContentCounters>) -> Self {
        self.counters = counters;
        self
    }

    /// GET with the store-wide concurrency limit, a whole-request timeout and
    /// a body cap. Never decodes `Content-Encoding`.
    pub async fn get(
        &self,
        url: &str,
        accept: Option<&str>,
        cap: u64,
    ) -> Result<Reply, FetchError> {
        self.get_until(url, accept, cap, tokio::time::Instant::now() + self.timeout)
            .await
    }

    pub async fn get_until(
        &self,
        url: &str,
        accept: Option<&str>,
        cap: u64,
        deadline: tokio::time::Instant,
    ) -> Result<Reply, FetchError> {
        let deadline = deadline.min(tokio::time::Instant::now() + self.timeout);
        let _permit = tokio::time::timeout_at(deadline, self.permits.acquire())
            .await
            .map_err(|_| self.busy_fetch())?
            .map_err(|_| self.busy_fetch())?;
        self.counters
            .upstream_fetches
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let result = match tokio::time::timeout_at(deadline, self.get_inner(url, accept, cap)).await
        {
            Ok(r) => r,
            Err(_) => Err(FetchError::Timeout),
        };
        if result.as_ref().is_err()
            || result
                .as_ref()
                .is_ok_and(|reply| upstream_status_error(reply.status))
        {
            self.counters
                .upstream_errors
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        result
    }

    async fn get_inner(
        &self,
        url: &str,
        accept: Option<&str>,
        cap: u64,
    ) -> Result<Reply, FetchError> {
        let mut req = self.client.get(url);
        if let Some(a) = accept {
            req = req.header(ACCEPT, a);
        }
        let mut resp = req.send().await.map_err(net)?;
        let status = resp.status().as_u16();
        let final_url = resp.url().to_string();
        let header = |name| {
            resp.headers()
                .get(name)
                .and_then(|v: &reqwest::header::HeaderValue| v.to_str().ok())
                .map(str::to_owned)
        };
        let content_type = header(CONTENT_TYPE);
        let content_encoding = header(reqwest::header::CONTENT_ENCODING);
        let retry_after = header(RETRY_AFTER).and_then(|v| parse_retry_after(&v));
        // Failure bodies are unused. A stalled 404 body must not turn a miss into
        // a transport timeout and poison a gateway's breaker.
        if status != 200 {
            return Ok(Reply {
                status,
                content_type,
                content_encoding,
                final_url,
                retry_after,
                body: Bytes::new(),
            });
        }
        if header(CONTENT_LENGTH)
            .and_then(|v| v.parse::<u64>().ok())
            .is_some_and(|n| n > cap)
        {
            return Err(FetchError::TooLarge);
        }
        let mut body = BytesMut::new();
        while let Some(chunk) = resp.chunk().await.map_err(net)? {
            if body.len() as u64 + chunk.len() as u64 > cap {
                return Err(FetchError::TooLarge);
            }
            body.extend_from_slice(&chunk);
        }
        Ok(Reply {
            status,
            content_type,
            content_encoding,
            final_url,
            retry_after,
            body: body.freeze(),
        })
    }
}

/// A response whose body is read chunk by chunk. It holds one request slot until dropped.
pub(crate) struct Streaming {
    pub status: u16,
    pub content_type: Option<String>,
    pub content_encoding: Option<String>,
    pub retry_after: Option<Duration>,
    response: reqwest::Response,
    idle: Duration,
    counters: Arc<ContentCounters>,
    error_counted: bool,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl Streaming {
    /// The next body chunk; a stall longer than the request timeout is a timeout.
    pub async fn chunk(&mut self) -> Result<Option<Bytes>, FetchError> {
        let result = match tokio::time::timeout(self.idle, self.response.chunk()).await {
            Ok(chunk) => chunk.map_err(net),
            Err(_) => Err(FetchError::Timeout),
        };
        if result.is_err() && !self.error_counted {
            self.counters
                .upstream_errors
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.error_counted = true;
        }
        result
    }
}

impl Http {
    /// GET for bodies larger than one object: headers within the request timeout, then
    /// [`Streaming::chunk`]. Never decodes `Content-Encoding`. The caller verifies the body
    /// against a CID, so redirects may lead to another host, never off the scheme (see
    /// [`trustless_redirect`]).
    pub async fn stream(&self, url: &str, accept: Option<&str>) -> Result<Streaming, FetchError> {
        let deadline = tokio::time::Instant::now() + self.timeout;
        let permit = tokio::time::timeout_at(deadline, self.permits.clone().acquire_owned())
            .await
            .map_err(|_| self.busy_fetch())?
            .map_err(|_| self.busy_fetch())?;
        self.counters
            .upstream_fetches
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut request = self.trustless.get(url);
        if let Some(accept) = accept {
            request = request.header(ACCEPT, accept);
        }
        let response = match tokio::time::timeout_at(deadline, request.send()).await {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => {
                self.counters
                    .upstream_errors
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                return Err(net(error));
            }
            Err(_) => {
                self.counters
                    .upstream_errors
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                return Err(FetchError::Timeout);
            }
        };
        let header = |name| {
            response
                .headers()
                .get(name)
                .and_then(|v: &reqwest::header::HeaderValue| v.to_str().ok())
                .map(str::to_owned)
        };
        let status = response.status().as_u16();
        let status_error = upstream_status_error(status);
        if status_error {
            self.counters
                .upstream_errors
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        Ok(Streaming {
            status,
            content_type: header(CONTENT_TYPE),
            content_encoding: header(reqwest::header::CONTENT_ENCODING),
            retry_after: header(RETRY_AFTER).and_then(|v| parse_retry_after(&v)),
            response,
            idle: self.timeout,
            counters: self.counters.clone(),
            error_counted: status_error,
            _permit: permit,
        })
    }
}

fn upstream_status_error(status: u16) -> bool {
    !(200..300).contains(&status) && status != 404
}

fn safe_redirect(origin: &reqwest::Url, next: &reqwest::Url) -> bool {
    let (Some(original_host), Some(next_host)) = (origin.host_str(), next.host_str()) else {
        return false;
    };
    origin.scheme() == next.scheme()
        && origin.port_or_known_default() == next.port_or_known_default()
        && next.username().is_empty()
        && next.password().is_none()
        && (next_host == original_host || next_host.ends_with(&format!(".{original_host}")))
}

/// A redirect of a body verified against its CID: any host on the same scheme (HTTPS stays
/// HTTPS), without credentials, and private addresses only from a private origin.
fn trustless_redirect(origin: &reqwest::Url, next: &reqwest::Url) -> bool {
    origin.scheme() == next.scheme()
        && next.username().is_empty()
        && next.password().is_none()
        && next.host_str().is_some()
        && (public_host(next) || !public_host(origin))
}

fn public_host(url: &reqwest::Url) -> bool {
    use std::net::{IpAddr, Ipv4Addr};
    fn public_v4(ip: Ipv4Addr) -> bool {
        let [a, b, ..] = ip.octets();
        !(ip.is_loopback()
            || ip.is_private()
            || ip.is_link_local()
            || ip.is_unspecified()
            || ip.is_broadcast()
            || ip.is_documentation()
            || ip.is_multicast()
            || (a == 100 && b & 0xc0 == 64))
    }
    let Some(host) = url.host_str() else {
        return false;
    };
    let host = host.trim_start_matches('[').trim_end_matches(']');
    match host.parse::<IpAddr>() {
        Ok(IpAddr::V4(ip)) => public_v4(ip),
        Ok(IpAddr::V6(ip)) => match ip.to_ipv4_mapped() {
            Some(ip) => public_v4(ip),
            None => {
                !(ip.is_loopback()
                    || ip.is_unspecified()
                    || ip.is_unique_local()
                    || ip.is_unicast_link_local()
                    || ip.is_multicast())
            }
        },
        Err(_) => {
            let name = host.trim_end_matches('.').to_ascii_lowercase();
            name != "localhost" && !name.ends_with(".localhost")
        }
    }
}

fn parse_retry_after(value: &str) -> Option<Duration> {
    let delay = value
        .trim()
        .parse::<u64>()
        .ok()
        .map(Duration::from_secs)
        .or_else(|| {
            httpdate::parse_http_date(value.trim()).ok().map(|date| {
                date.duration_since(std::time::SystemTime::now())
                    .unwrap_or_default()
            })
        })?;
    Some(delay.min(Duration::from_secs(3600)))
}

fn net(e: reqwest::Error) -> FetchError {
    if e.is_connect() {
        FetchError::Connect(e.without_url().to_string())
    } else if e.is_timeout() {
        FetchError::Timeout
    } else {
        FetchError::Network(e.without_url().to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn stalled_failure_bodies_preserve_status_without_holding_http_capacity() {
        use tokio::io::AsyncWriteExt;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/missing", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            socket
                .write_all(
                    b"HTTP/1.1 404 Not Found\r\nContent-Length: 100\r\nRetry-After: 60\r\n\r\n",
                )
                .await
                .unwrap();
            std::future::pending::<()>().await;
        });
        let mut config = ContentConfig::mainnet("tmp/http-test-unused");
        config.request_timeout = Duration::from_millis(200);
        config.max_concurrent_requests = 1;
        let http = Http::new(&config).unwrap();
        let reply = http.get(&url, None, 1).await.unwrap();
        assert_eq!(reply.status, 404);
        assert_eq!(reply.retry_after, Some(Duration::from_secs(60)));
        assert!(reply.body.is_empty());
        assert_eq!(http.permits.available_permits(), 1);
        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn saturated_http_capacity_is_local_busy_not_a_gateway_timeout() {
        let mut config = ContentConfig::mainnet("tmp/http-test-unused");
        config.max_concurrent_requests = 1;
        config.request_timeout = Duration::from_millis(10);
        let http = Http::new(&config).unwrap();
        let held = http.permits.acquire().await.unwrap();
        assert!(matches!(
            http.get("http://127.0.0.1:1/unused", None, 1).await,
            Err(FetchError::Busy)
        ));
        assert_eq!(
            http.counters
                .upstream_busy
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
        assert_eq!(
            http.counters
                .upstream_fetches
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );
        drop(held);
        assert_eq!(http.permits.available_permits(), 1);
    }

    #[test]
    fn redirects_cannot_change_scheme_port_or_authority() {
        let origin = reqwest::Url::parse("https://gateway.example/ipfs/root").unwrap();
        for url in [
            "http://gateway.example/x",
            "https://gateway.example:444/x",
            "https://other.example/x",
            "https://user@gateway.example/x",
        ] {
            assert!(!safe_redirect(&origin, &reqwest::Url::parse(url).unwrap()));
        }
        assert!(safe_redirect(
            &origin,
            &reqwest::Url::parse("https://cid.gateway.example/x").unwrap()
        ));
    }
    #[test]
    fn trustless_redirects_change_host_but_never_scheme_credentials_or_privacy() {
        let url = |text| reqwest::Url::parse(text).unwrap();
        let origin = url("https://gateway.example/ipfs/root?format=car");
        for next in [
            "https://other.example/ipfs/root?format=car",
            "https://root.ipfs.gateway.example/?format=car",
            "https://gateway.example/ipfs/root/?format=car",
            "https://8.8.8.8/ipfs/root",
        ] {
            assert!(trustless_redirect(&origin, &url(next)), "{next}");
        }
        for next in [
            "http://other.example/ipfs/root",
            "https://user@other.example/ipfs/root",
            "https://127.0.0.1/ipfs/root",
            "https://10.1.2.3/ipfs/root",
            "https://100.64.0.1/ipfs/root",
            "https://169.254.169.254/latest",
            "https://[::1]/ipfs/root",
            "https://[fd00::1]/ipfs/root",
            "https://[::ffff:192.168.0.1]/ipfs/root",
            "https://localhost/ipfs/root",
            "https://api.localhost./ipfs/root",
        ] {
            assert!(!trustless_redirect(&origin, &url(next)), "{next}");
        }
        // A loopback gateway (tests, local nodes) may move to another local port.
        assert!(trustless_redirect(
            &url("http://127.0.0.1:8080/ipfs/root"),
            &url("http://127.0.0.1:9090/ipfs/root")
        ));
    }

    #[test]
    fn retry_after_supports_seconds_dates_and_a_bound() {
        assert_eq!(parse_retry_after("12"), Some(Duration::from_secs(12)));
        assert_eq!(parse_retry_after("999999"), Some(Duration::from_secs(3600)));
        assert_eq!(
            parse_retry_after("Thu, 01 Jan 1970 00:00:00 GMT"),
            Some(Duration::ZERO)
        );
        assert_eq!(parse_retry_after("not a date"), None);
    }
}
