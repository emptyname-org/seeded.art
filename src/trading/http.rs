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

/// Mockable, bounded read/simulation transport. No injection or signing method exists.
pub trait Transport: Send + Sync {
    fn request(&self, endpoint: Endpoint, path: String, body: Option<Value>) -> Reply;
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
            let request = match body {
                Some(value) => client
                    .post(url)
                    .header("content-type", "application/json")
                    .body(value.to_string()),
                None => client.get(url),
            };
            let mut response = request
                .send()
                .await
                .map_err(|_| TradingError::Unavailable)?;
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
            const LIMIT: usize = 4 * 1024 * 1024;
            if response.content_length().is_some_and(|n| n > LIMIT as u64) {
                return Err(TradingError::TooLarge);
            }
            let mut bytes = Vec::new();
            while let Some(chunk) = response
                .chunk()
                .await
                .map_err(|_| TradingError::Unavailable)?
            {
                if bytes.len().saturating_add(chunk.len()) > LIMIT {
                    return Err(TradingError::TooLarge);
                }
                bytes.extend_from_slice(&chunk);
            }
            serde_json::from_slice(&bytes).map_err(|_| TradingError::Unavailable)
        })
    }
}
