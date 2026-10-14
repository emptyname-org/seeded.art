use super::types::TradingError;
use reqwest::{Client, Url};
use serde_json::Value;
use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

pub type Reply = Pin<Box<dyn Future<Output = Result<Value, TradingError>> + Send>>;
#[derive(Clone, Copy)]
pub enum Endpoint {
    Rpc,
    Indexer,
}

/// Default response body limit: simulations, confirmation and reconciliation reads.
pub const LARGE: usize = 4 * 1024 * 1024;

/// Mockable, bounded read/simulation transport. No injection or signing method exists.
pub trait Transport: Send + Sync {
    fn request(&self, endpoint: Endpoint, path: String, body: Option<Value>) -> Reply;
    /// As `request`, with a response body limit of `limit` bytes.
    fn request_capped(
        &self,
        endpoint: Endpoint,
        path: String,
        body: Option<Value>,
        limit: usize,
    ) -> Reply {
        let _ = limit;
        self.request(endpoint, path, body)
    }
}

const RETRIES: usize = 2;
const BACKOFF: [Duration; RETRIES] = [Duration::from_millis(250), Duration::from_millis(750)];
fn endpoint_name(endpoint: Endpoint) -> &'static str {
    match endpoint {
        Endpoint::Rpc => "rpc",
        Endpoint::Indexer => "indexer",
    }
}

pub struct Http {
    client: Client,
    rpc: Url,
    indexer: Url,
}
impl Http {
    pub fn new(rpc: &str, indexer: &str) -> Result<Arc<Self>, TradingError> {
        fn base(raw: &str) -> Result<Url, TradingError> {
            let url = Url::parse(raw).map_err(|_| TradingError::Invalid)?;
            let local = matches!(url.host_str(), Some("127.0.0.1" | "[::1]" | "localhost"));
            if (url.scheme() != "https" && !(url.scheme() == "http" && local))
                || !url.username().is_empty()
                || url.password().is_some()
                || url.query().is_some()
                || url.fragment().is_some()
            {
                return Err(TradingError::Invalid);
            }
            Ok(url)
        }
        let client = Client::builder()
            .timeout(Duration::from_secs(8))
            .connect_timeout(Duration::from_secs(3))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| TradingError::Unavailable)?;
        Ok(Arc::new(Self {
            client,
            rpc: base(rpc)?,
            indexer: base(indexer)?,
        }))
    }
}
impl Transport for Http {
    fn request(&self, endpoint: Endpoint, path: String, body: Option<Value>) -> Reply {
        self.request_capped(endpoint, path, body, LARGE)
    }
    fn request_capped(
        &self,
        endpoint: Endpoint,
        path: String,
        body: Option<Value>,
        limit: usize,
    ) -> Reply {
        let base = match endpoint {
            Endpoint::Rpc => &self.rpc,
            Endpoint::Indexer => &self.indexer,
        };
        let url = format!("{}{}", base.as_str().trim_end_matches('/'), path);
        let client = self.client.clone();
        Box::pin(async move {
            // Paths are generated internally; POST is restricted to read-only helpers.
            if body.is_some()
                && !(path.contains("/helpers/scripts/")
                    || path.ends_with("/helpers/forge/operations"))
            {
                return Err(TradingError::Invalid);
            }
            // A read refused for rate (429) is retried twice, after Retry-After when it is
            // at most two seconds, otherwise after a short backoff.
            let mut attempt = 0;
            let mut response = loop {
                let request = match &body {
                    Some(value) => client
                        .post(&url)
                        .header("content-type", "application/json")
                        .body(value.to_string()),
                    None => client.get(&url),
                };
                let response = request
                    .send()
                    .await
                    .map_err(|_| TradingError::Unavailable)?;
                if response.status().as_u16() != 429 || body.is_some() || attempt == RETRIES {
                    break response;
                }
                let wait = match response.headers().get(reqwest::header::RETRY_AFTER) {
                    None => BACKOFF[attempt],
                    Some(value) => match value.to_str().ok().and_then(|v| v.parse::<u64>().ok()) {
                        Some(seconds) if seconds <= 2 => Duration::from_secs(seconds),
                        _ => break response,
                    },
                };
                eprintln!("trading_upstream_rate_limited {}", endpoint_name(endpoint));
                tokio::time::sleep(wait).await;
                attempt += 1;
            };
            if response.status().as_u16() == 404 {
                return Err(TradingError::Changed);
            }
            if !response.status().is_success() {
                return Err(
                    if response.status().is_server_error()
                        || matches!(response.status().as_u16(), 408 | 425 | 429)
                    {
                        TradingError::Unavailable
                    } else {
                        TradingError::Simulation
                    },
                );
            }
            if response.content_length().is_some_and(|n| n > limit as u64) {
                return Err(TradingError::TooLarge);
            }
            let mut bytes = Vec::new();
            while let Some(chunk) = response
                .chunk()
                .await
                .map_err(|_| TradingError::Unavailable)?
            {
                if bytes.len().saturating_add(chunk.len()) > limit {
                    return Err(TradingError::TooLarge);
                }
                bytes.extend_from_slice(&chunk);
            }
            serde_json::from_slice(&bytes).map_err(|_| TradingError::Unavailable)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// Serves one JSON string of `size` bytes per connection.
    async fn server(size: usize) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let mut request = [0u8; 1024];
                let _ = socket.read(&mut request).await;
                let body = format!("\"{}\"", "x".repeat(size - 2));
                let head = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                    body.len()
                );
                let _ = socket.write_all(head.as_bytes()).await;
                let _ = socket.write_all(body.as_bytes()).await;
            }
        });
        base
    }

    /// Answers 429 (with `retry_after` when given) to the first `refusals` requests, then
    /// `"ok"`; counts requests.
    async fn limited(
        refusals: usize,
        retry_after: Option<&'static str>,
    ) -> (String, Arc<std::sync::atomic::AtomicUsize>) {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let seen = Arc::new(AtomicUsize::new(0));
        let counter = seen.clone();
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let mut request = [0u8; 1024];
                let _ = socket.read(&mut request).await;
                let n = counter.fetch_add(1, Ordering::SeqCst);
                let reply = if n < refusals {
                    format!(
                        "HTTP/1.1 429 Too Many Requests\r\n{}content-length: 0\r\nconnection: close\r\n\r\n",
                        retry_after
                            .map(|s| format!("retry-after: {s}\r\n"))
                            .unwrap_or_default()
                    )
                } else {
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 4\r\nconnection: close\r\n\r\n\"ok\"".into()
                };
                let _ = socket.write_all(reply.as_bytes()).await;
            }
        });
        (base, seen)
    }

    #[tokio::test]
    async fn rate_limited_reads_are_retried_twice_within_a_short_wait() {
        use std::sync::atomic::Ordering;
        let (base, seen) = limited(2, None).await;
        let http = Http::new(&base, &base).unwrap();
        assert!(
            http.request(Endpoint::Indexer, "/x".into(), None)
                .await
                .is_ok()
        );
        assert_eq!(seen.load(Ordering::SeqCst), 3);
        let (base, seen) = limited(3, Some("1")).await;
        let http = Http::new(&base, &base).unwrap();
        assert!(matches!(
            http.request(Endpoint::Indexer, "/x".into(), None).await,
            Err(TradingError::Unavailable)
        ));
        assert_eq!(seen.load(Ordering::SeqCst), 3);
        // A longer Retry-After is not waited for.
        let (base, seen) = limited(1, Some("30")).await;
        let http = Http::new(&base, &base).unwrap();
        assert!(
            http.request(Endpoint::Indexer, "/x".into(), None)
                .await
                .is_err()
        );
        assert_eq!(seen.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn responses_above_the_call_limit_are_too_large() {
        let base = server(64 * 1024 + 1).await;
        let http = Http::new(&base, &base).unwrap();
        assert!(matches!(
            http.request_capped(Endpoint::Rpc, "/x".into(), None, 64 * 1024)
                .await,
            Err(TradingError::TooLarge)
        ));
        assert!(
            http.request_capped(Endpoint::Rpc, "/x".into(), None, 256 * 1024)
                .await
                .is_ok()
        );
        assert!(
            http.request(Endpoint::Indexer, "/x".into(), None)
                .await
                .is_ok()
        );
    }
}
