//! Only indexed objkt logo URLs are proxied; this is not a general HTTPS fetcher.
use crate::content::{Content, ContentError, ErrorKind, Verification};
use bytes::Bytes;
use reqwest::{Client, Url};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, VecDeque},
    future::Future,
    net::{IpAddr, SocketAddr, ToSocketAddrs},
    sync::{Arc, Mutex, Weak},
    time::{Duration, Instant},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const HOST: &str = "assets.objkt.media";
const MAX_BYTES: usize = 2 * 1024 * 1024;
const TIMEOUT: Duration = Duration::from_secs(6);
const CACHE_TTL: Duration = Duration::from_secs(300);
const CACHE_BYTES: usize = 16 * 1024 * 1024;
const CACHE_ENTRIES: usize = 128;

struct CachedAvatar {
    uri: String,
    content: Content,
    inserted: Instant,
}

#[derive(Default)]
struct AvatarCache {
    entries: VecDeque<CachedAvatar>,
    bytes: usize,
}
impl AvatarCache {
    fn expire(&mut self, now: Instant) {
        self.entries.retain(|entry| {
            if now.saturating_duration_since(entry.inserted) >= CACHE_TTL {
                self.bytes -= entry.content.body.len();
                false
            } else {
                true
            }
        });
    }

    fn get(&mut self, uri: &str, now: Instant) -> Option<Content> {
        self.expire(now);
        let position = self.entries.iter().position(|entry| entry.uri == uri)?;
        let entry = self.entries.remove(position)?;
        let mut content = entry.content.clone();
        content.from_cache = true;
        self.entries.push_back(entry);
        Some(content)
    }

    fn insert(&mut self, uri: String, content: Content, now: Instant) {
        self.expire(now);
        if content.body.len() > CACHE_BYTES {
            return;
        }
        if let Some(position) = self.entries.iter().position(|entry| entry.uri == uri) {
            if let Some(previous) = self.entries.remove(position) {
                self.bytes -= previous.content.body.len();
            }
        }
        while self.entries.len() >= CACHE_ENTRIES || self.bytes + content.body.len() > CACHE_BYTES {
            if let Some(oldest) = self.entries.pop_front() {
                self.bytes -= oldest.content.body.len();
            }
        }
        self.bytes += content.body.len();
        self.entries.push_back(CachedAvatar {
            uri,
            content,
            inserted: now,
        });
    }
}

#[derive(Clone)]
pub(super) struct AvatarClient {
    slots: Arc<Semaphore>,
    requests: Arc<Semaphore>,
    cache: Arc<Mutex<AvatarCache>>,
    flights: Arc<Mutex<HashMap<String, Weak<tokio::sync::Mutex<()>>>>>,
    timeout: Duration,
    #[cfg(test)]
    mock_origin: Option<Url>,
}
impl Default for AvatarClient {
    fn default() -> Self {
        Self {
            slots: Arc::new(Semaphore::new(4)),
            requests: Arc::new(Semaphore::new(64)),
            cache: Arc::new(Mutex::new(AvatarCache::default())),
            flights: Arc::new(Mutex::new(HashMap::new())),
            timeout: TIMEOUT,
            #[cfg(test)]
            mock_origin: None,
        }
    }
}

pub(super) fn allowed(uri: &str) -> Option<Url> {
    if uri.len() > 512 || uri.contains(['\\', '%']) {
        return None;
    }
    let url = Url::parse(uri).ok()?;
    if url.scheme() != "https"
        || url.host_str() != Some(HOST)
        || url.port().is_some()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return None;
    }
    let address = url
        .path()
        .strip_prefix("/file/assets-004/h/")?
        .strip_suffix("/logo")?;
    if !crate::store::is_tezos_address(address) {
        return None;
    }
    // Require the exact path too: URL parsing must not hide dot-segment traversal.
    if uri.split('?').next()? != format!("https://{HOST}{}", url.path()) {
        return None;
    }
    if let Some(query) = url.query() {
        let value = query.strip_prefix("cb=")?;
        if value.len() != 8 || !value.bytes().all(|b| b.is_ascii_hexdigit()) {
            return None;
        }
    }
    Some(url)
}
fn unavailable() -> ContentError {
    ContentError::new(ErrorKind::Unavailable, "avatar unavailable")
}
fn public_address(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(ip) => {
            let [a, b, c, _] = ip.octets();
            !ip.is_private()
                && !ip.is_loopback()
                && !ip.is_link_local()
                && !ip.is_multicast()
                && !ip.is_unspecified()
                && !ip.is_broadcast()
                && !ip.is_documentation()
                && a != 0
                && a < 224
                && !(a == 100 && (64..=127).contains(&b))
                && !(a == 192 && b == 0 && c == 0)
                && !(a == 192 && b == 88 && c == 99)
                && !(a == 198 && (b == 18 || b == 19))
        }
        IpAddr::V6(ip) => {
            let segments = ip.segments();
            (segments[0] & 0xe000) == 0x2000
                && segments[0] != 0x2002
                && !(segments[0] == 0x2001 && (segments[1] < 0x200 || segments[1] == 0xdb8))
                && !(segments[0] == 0x3fff && segments[1] < 0x1000)
        }
    }
}
fn validate_addresses(addresses: &[SocketAddr]) -> Result<(), ContentError> {
    if addresses.is_empty()
        || addresses.len() > 16
        || addresses
            .iter()
            .any(|a| !public_address(a.ip()) || a.port() != 443)
    {
        return Err(unavailable());
    }
    Ok(())
}
impl AvatarClient {
    #[cfg(test)]
    pub(super) fn mock(origin: Url) -> Self {
        Self {
            mock_origin: Some(origin),
            ..Self::default()
        }
    }

    async fn bounded<F, Task>(&self, operation: F) -> Result<Content, ContentError>
    where
        F: FnOnce(Arc<OwnedSemaphorePermit>) -> Task,
        Task: Future<Output = Result<Content, ContentError>>,
    {
        let permit = Arc::new(
            self.slots
                .clone()
                .try_acquire_owned()
                .map_err(|_| unavailable())?,
        );
        tokio::time::timeout(self.timeout, operation(permit.clone()))
            .await
            .map_err(|_| unavailable())?
    }
    pub(super) async fn get(&self, uri: &str, maximum: u64) -> Result<Content, ContentError> {
        let url = allowed(uri)
            .ok_or_else(|| ContentError::new(ErrorKind::NotFound, "avatar source unsupported"))?;
        let maximum = usize::try_from(maximum)
            .unwrap_or(usize::MAX)
            .min(MAX_BYTES);
        let _request = self.requests.try_acquire().map_err(|_| unavailable())?;
        if let Some(content) = self.cached(uri, maximum)? {
            return Ok(content);
        }
        let flight = {
            let mut flights = self.flights.lock().map_err(|_| unavailable())?;
            flights.retain(|_, pending| pending.strong_count() > 0);
            if let Some(pending) = flights.get(uri).and_then(Weak::upgrade) {
                pending
            } else {
                if flights.len() >= 4 {
                    return Err(unavailable());
                }
                let pending = Arc::new(tokio::sync::Mutex::new(()));
                flights.insert(uri.to_owned(), Arc::downgrade(&pending));
                pending
            }
        };
        // Same-source readers share one fetch. Cancelled/failed work leaves no cache entry.
        tokio::time::timeout(self.timeout, async {
            let _flight = flight.lock().await;
            if let Some(content) = self.cached(uri, maximum)? {
                return Ok(content);
            }
            let content = self.fetch(url, maximum).await?;
            self.cache.lock().map_err(|_| unavailable())?.insert(
                uri.to_owned(),
                content.clone(),
                Instant::now(),
            );
            Ok(content)
        })
        .await
        .map_err(|_| unavailable())?
    }

    fn cached(&self, uri: &str, maximum: usize) -> Result<Option<Content>, ContentError> {
        let content = self
            .cache
            .lock()
            .map_err(|_| unavailable())?
            .get(uri, Instant::now());
        if content.as_ref().is_some_and(|c| c.body.len() > maximum) {
            return Err(ContentError::new(ErrorKind::TooLarge, "avatar size limit"));
        }
        Ok(content)
    }

    async fn fetch(&self, url: Url, maximum: usize) -> Result<Content, ContentError> {
        #[cfg(test)]
        if let Some(origin) = &self.mock_origin {
            return self
                .bounded(|_| async {
                    let client = Client::builder()
                        .no_proxy()
                        .redirect(reqwest::redirect::Policy::none())
                        .build()
                        .map_err(|_| unavailable())?;
                    read_response(&client, origin.clone(), maximum).await
                })
                .await;
        }
        self.bounded(|permit| async move {
            // System DNS may outlive cancellation. Its worker keeps the same bounded slot.
            let addresses = tokio::task::spawn_blocking(move || {
                let _permit = permit;
                (HOST, 443)
                    .to_socket_addrs()
                    .map(|a| a.take(17).collect::<Vec<_>>())
                    .map_err(|_| unavailable())
            })
            .await
            .map_err(|_| unavailable())??;
            validate_addresses(&addresses)?;
            let client = Client::builder()
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .https_only(true)
                .connect_timeout(Duration::from_secs(2))
                .timeout(self.timeout)
                .resolve_to_addrs(HOST, &addresses)
                .build()
                .map_err(|_| unavailable())?;
            read_response(&client, url, maximum).await
        })
        .await
    }
}
async fn read_response(client: &Client, url: Url, maximum: usize) -> Result<Content, ContentError> {
    let mut response = client
        .get(url)
        .header("Accept", "image/png,image/jpeg,image/webp,image/gif")
        .header("Accept-Encoding", "identity")
        .send()
        .await
        .map_err(|_| unavailable())?;
    if response.status() != reqwest::StatusCode::OK {
        return Err(unavailable());
    }
    if response
        .headers()
        .get("content-encoding")
        .is_some_and(|v| v != "identity")
    {
        return Err(ContentError::new(
            ErrorKind::Invalid,
            "avatar encoding unsupported",
        ));
    }
    if response
        .content_length()
        .is_some_and(|n| n > maximum as u64)
    {
        return Err(ContentError::new(ErrorKind::TooLarge, "avatar size limit"));
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| unavailable())? {
        if chunk.len() > maximum.saturating_sub(bytes.len()) {
            return Err(ContentError::new(ErrorKind::TooLarge, "avatar size limit"));
        }
        bytes.extend_from_slice(&chunk);
    }
    let kind = super::sniff_media(&bytes)
        .filter(|k| matches!(*k, "image/png" | "image/jpeg" | "image/gif" | "image/webp"))
        .ok_or_else(|| ContentError::new(ErrorKind::Invalid, "avatar image unsupported"))?;
    Ok(Content {
        etag: format!("\"{}\"", hex::encode(Sha256::digest(&bytes))),
        body: Bytes::from(bytes.into_boxed_slice()),
        content_type: Some(kind.into()),
        content_encoding: None,
        verification: Verification::Unverified,
        directory_index: false,
        from_cache: false,
        delivery_accounted: false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    const ADDRESS: &str = "tz1gL9H53BzjDqmMi2JkKALeCzT7UAoopzEL";
    fn uri() -> String {
        format!("https://{HOST}/file/assets-004/h/{ADDRESS}/logo?cb=0123abcd")
    }

    fn raster(size: usize) -> Content {
        Content {
            body: Bytes::from(vec![0; size]),
            content_type: Some("image/png".into()),
            content_encoding: None,
            etag: "\"fixture\"".into(),
            verification: Verification::Unverified,
            directory_index: false,
            from_cache: false,
            delivery_accounted: false,
        }
    }

    #[test]
    fn success_cache_expires_without_sliding_and_bounds_bytes_entries_and_lru() {
        let mut cache = AvatarCache::default();
        let now = Instant::now();
        for n in 0..CACHE_ENTRIES {
            cache.insert(n.to_string(), raster(8), now);
        }
        assert!(cache.get("0", now).unwrap().from_cache);
        cache.insert("extra".into(), raster(8), now);
        assert_eq!(cache.entries.len(), CACHE_ENTRIES);
        assert!(cache.get("1", now).is_none());
        assert!(cache.get("0", now + CACHE_TTL / 2).is_some());
        assert!(cache.get("0", now + CACHE_TTL).is_none());
        assert_eq!(cache.bytes, 0);
        assert!(cache.entries.is_empty());
        for n in 0..10 {
            cache.insert(n.to_string(), raster(MAX_BYTES), now);
        }
        assert_eq!(cache.bytes, CACHE_BYTES);
        assert_eq!(cache.entries.len(), 8);
        assert!(cache.get("0", now).is_none());
        cache.insert("9".into(), raster(8), now);
        assert_eq!(cache.bytes, CACHE_BYTES - MAX_BYTES + 8);
    }

    async fn cache_server() -> (
        AvatarClient,
        tokio::task::JoinHandle<()>,
        Arc<std::sync::atomic::AtomicUsize>,
        Arc<std::sync::atomic::AtomicUsize>,
    ) {
        use axum::{Router, body::Body, http::Response};
        use std::sync::atomic::{AtomicUsize, Ordering};
        let calls = Arc::new(AtomicUsize::new(0));
        let mode = Arc::new(AtomicUsize::new(0));
        let counter = calls.clone();
        let setting = mode.clone();
        let app = Router::new().fallback(move || {
            let counter = counter.clone();
            let setting = setting.clone();
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                match setting.load(Ordering::SeqCst) {
                    1 => Response::builder().status(503).body(Body::empty()).unwrap(),
                    2 => Response::new(Body::from("<svg xmlns='http://www.w3.org/2000/svg'/>")),
                    3 => Response::builder()
                        .header("content-type", "image/png")
                        .body(Body::from("<html>not raster</html>"))
                        .unwrap(),
                    5 => Response::builder()
                        .status(206)
                        .body(Body::from(&b"\x89PNG\r\n\x1a\n"[..]))
                        .unwrap(),
                    mode => {
                        if mode == 4 {
                            let _ = tokio::time::timeout(Duration::from_secs(2), async {
                                while setting.load(Ordering::SeqCst) == 4 {
                                    tokio::time::sleep(Duration::from_millis(1)).await;
                                }
                            })
                            .await;
                        }
                        Response::new(Body::from(&b"\x89PNG\r\n\x1a\n"[..]))
                    }
                }
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = AvatarClient::mock(
            format!("http://{}", listener.local_addr().unwrap())
                .parse()
                .unwrap(),
        );
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (client, task, calls, mode)
    }

    #[tokio::test]
    async fn cache_hits_coalesce_cold_readers_and_do_not_need_upstream_slots() {
        use std::sync::atomic::Ordering;
        let (client, server, calls, mode) = cache_server().await;
        mode.store(4, Ordering::SeqCst);
        let mut readers = tokio::task::JoinSet::new();
        for _ in 0..8 {
            let client = client.clone();
            readers.spawn(async move { client.get(&uri(), MAX_BYTES as u64).await });
        }
        tokio::time::timeout(Duration::from_secs(1), async {
            while client.requests.available_permits() != 56 || calls.load(Ordering::SeqCst) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        mode.store(0, Ordering::SeqCst);
        while let Some(result) = readers.join_next().await {
            assert!(result.unwrap().is_ok());
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let _slots = client.slots.acquire_many(4).await.unwrap();
        let content = client.get(&uri(), MAX_BYTES as u64).await.unwrap();
        assert!(content.from_cache);
        assert!(!content.verification.is_verified());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            client.get(&uri(), 4).await.unwrap_err().kind,
            ErrorKind::TooLarge
        );
        assert!(
            client
                .get(&uri().replace(HOST, "127.0.0.1"), 1024)
                .await
                .is_err()
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn expired_invalid_and_transient_responses_do_not_poison_cache_or_refresh_ttl() {
        use std::sync::atomic::Ordering;
        let (client, server, calls, mode) = cache_server().await;
        client.get(&uri(), MAX_BYTES as u64).await.unwrap();
        client
            .cache
            .lock()
            .unwrap()
            .entries
            .front_mut()
            .unwrap()
            .inserted -= CACHE_TTL;
        for invalid in [1, 2, 3, 5] {
            mode.store(invalid, Ordering::SeqCst);
            assert!(client.get(&uri(), MAX_BYTES as u64).await.is_err());
            assert!(client.cache.lock().unwrap().entries.is_empty());
        }
        mode.store(0, Ordering::SeqCst);
        assert!(
            !client
                .get(&uri(), MAX_BYTES as u64)
                .await
                .unwrap()
                .from_cache
        );
        assert_eq!(calls.load(Ordering::SeqCst), 6);
        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn cancelled_or_timed_out_fetch_releases_flight_and_does_not_cache() {
        use std::sync::atomic::Ordering;
        let (client, server, calls, mode) = cache_server().await;
        mode.store(4, Ordering::SeqCst);
        let operation = {
            let client = client.clone();
            tokio::spawn(async move { client.get(&uri(), MAX_BYTES as u64).await })
        };
        tokio::time::timeout(Duration::from_secs(1), async {
            while calls.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        operation.abort();
        let _ = operation.await;
        assert!(client.cache.lock().unwrap().entries.is_empty());
        assert_eq!(client.slots.available_permits(), 4);
        assert_eq!(client.requests.available_permits(), 64);
        let mut short = client.clone();
        short.timeout = Duration::from_millis(5);
        assert!(short.get(&uri(), MAX_BYTES as u64).await.is_err());
        assert!(short.cache.lock().unwrap().entries.is_empty());
        assert_eq!(client.slots.available_permits(), 4);
        mode.store(0, Ordering::SeqCst);
        assert!(client.get(&uri(), MAX_BYTES as u64).await.is_ok());
        assert_eq!(client.flights.lock().unwrap().len(), 1);
        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn distinct_cold_sources_and_waiters_have_finite_admission_and_recover() {
        use std::sync::atomic::Ordering;
        let (client, server, calls, mode) = cache_server().await;
        mode.store(4, Ordering::SeqCst);
        let mut tasks = Vec::new();
        for n in 0..4 {
            let client = client.clone();
            let uri = uri().replace("0123abcd", &format!("{n:08x}"));
            tasks.push(tokio::spawn(async move {
                client.get(&uri, MAX_BYTES as u64).await
            }));
        }
        tokio::time::timeout(Duration::from_secs(1), async {
            while calls.load(Ordering::SeqCst) != 4 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(client.get(&uri(), MAX_BYTES as u64).await.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 4);
        for task in tasks {
            task.abort();
            let _ = task.await;
        }
        assert_eq!(client.requests.available_permits(), 64);
        assert_eq!(client.slots.available_permits(), 4);
        {
            let _requests = client.requests.acquire_many(64).await.unwrap();
            assert!(client.get(&uri(), MAX_BYTES as u64).await.is_err());
            assert_eq!(calls.load(Ordering::SeqCst), 4);
        }
        mode.store(0, Ordering::SeqCst);
        assert!(client.get(&uri(), MAX_BYTES as u64).await.is_ok());
        assert_eq!(client.flights.lock().unwrap().len(), 1);
        server.abort();
        let _ = server.await;
    }
    #[test]
    fn logo_allowlist_rejects_other_hosts_paths_credentials_and_queries() {
        assert!(allowed(&uri()).is_some());
        assert!(allowed(uri().split('?').next().unwrap()).is_some());
        for value in [
            uri().replace(HOST, "127.0.0.1"),
            uri().replace(HOST, "assets.objkt.media.evil.test"),
            uri().replace(HOST, "user@assets.objkt.media"),
            uri().replace("https:", "http:"),
            uri().replace("/logo", "/file"),
            uri().replace("/h/", "/h/../h/"),
            uri().replace("/logo", "/logo/../logo"),
            uri().replace(ADDRESS, "%2e%2e"),
            uri().replace("?cb=0123abcd", "?cb=0123abcd&url=https://evil.test"),
            format!("{}#x", uri()),
            uri().replace("assets-004", "assets-999"),
        ] {
            assert!(allowed(&value).is_none(), "{value}");
        }
    }
    #[test]
    fn dns_must_be_entirely_public_and_pinned_to_https_port() {
        for ip in [
            "127.0.0.1",
            "10.0.0.1",
            "172.16.0.1",
            "192.168.0.1",
            "169.254.169.254",
            "100.64.0.1",
            "0.0.0.0",
            "198.18.0.1",
            "192.0.2.1",
            "224.0.0.1",
            "240.0.0.1",
            "::1",
            "fc00::1",
            "fe80::1",
            "::ffff:127.0.0.1",
            "64:ff9b::7f00:1",
            "2001:db8::1",
            "2002:7f00:1::1",
            "3fff::1",
        ] {
            assert!(!public_address(ip.parse().unwrap()), "{ip}");
        }
        let good = SocketAddr::new("1.1.1.1".parse().unwrap(), 443);
        assert!(validate_addresses(&[good]).is_ok());
        assert!(public_address("2606:4700::1111".parse().unwrap()));
        assert!(
            validate_addresses(&[good, SocketAddr::new("127.0.0.1".parse().unwrap(), 443)])
                .is_err()
        );
        assert!(validate_addresses(&[]).is_err());
        assert!(validate_addresses(&[SocketAddr::new(good.ip(), 80)]).is_err());
        assert!(validate_addresses(&[good; 17]).is_err());
    }
    struct Chunked(u8);
    impl http_body::Body for Chunked {
        type Data = Bytes;
        type Error = std::convert::Infallible;
        fn poll_frame(
            mut self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Result<http_body::Frame<Bytes>, Self::Error>>> {
            let next = self.0;
            self.0 += 1;
            std::task::Poll::Ready(match next {
                0 => Some(Ok(http_body::Frame::data(Bytes::from_static(
                    b"\x89PNG\r\n\x1a\n",
                )))),
                1 => Some(Ok(http_body::Frame::data(Bytes::from_static(&[0; 64])))),
                _ => None,
            })
        }
    }
    async fn mock() -> (
        String,
        tokio::task::JoinHandle<()>,
        Arc<std::sync::atomic::AtomicUsize>,
    ) {
        use axum::{Router, body::Body, http::Response};
        let followed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let count = followed.clone();
        let app = Router::new().fallback(move |request: axum::extract::Request| {
            let count = count.clone();
            async move {
                match request.uri().path() {
                    "/redirect" => Response::builder()
                        .status(302)
                        .header("location", "/followed")
                        .body(Body::empty())
                        .unwrap(),
                    "/followed" => {
                        count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        Response::new(Body::from("followed"))
                    }
                    "/chunked" => Response::new(Body::new(Chunked(0))),
                    "/large" => Response::builder()
                        .header("content-length", MAX_BYTES + 1)
                        .body(Body::from(vec![0; MAX_BYTES + 1]))
                        .unwrap(),
                    "/encoded" => Response::builder()
                        .header("content-encoding", "gzip")
                        .body(Body::from("not identity"))
                        .unwrap(),
                    "/html" => Response::builder()
                        .header("content-type", "image/png")
                        .body(Body::from("<html><script>bad()</script>"))
                        .unwrap(),
                    "/fail" => Response::builder().status(503).body(Body::empty()).unwrap(),
                    "/slow" => {
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        Response::new(Body::from("late"))
                    }
                    _ => Response::new(Body::from(&b"\x89PNG\r\n\x1a\n"[..])),
                }
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (base, task, followed)
    }
    #[tokio::test]
    async fn transport_bounds_status_redirect_body_encoding_type_and_retry() {
        let (base, task, followed) = mock().await;
        let client = Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap();
        for path in ["redirect", "large", "encoded", "html", "fail"] {
            assert!(
                read_response(
                    &client,
                    Url::parse(&format!("{base}/{path}")).unwrap(),
                    MAX_BYTES
                )
                .await
                .is_err(),
                "{path}"
            );
        }
        assert_eq!(followed.load(std::sync::atomic::Ordering::Relaxed), 0);
        assert!(
            read_response(&client, Url::parse(&base).unwrap(), 4)
                .await
                .is_err()
        );
        assert!(
            read_response(&client, Url::parse(&format!("{base}/chunked")).unwrap(), 16)
                .await
                .is_err()
        );
        let bounded = AvatarClient {
            timeout: Duration::from_millis(5),
            ..AvatarClient::default()
        };
        assert!(
            bounded
                .bounded(|_| read_response(
                    &client,
                    Url::parse(&format!("{base}/slow")).unwrap(),
                    MAX_BYTES
                ))
                .await
                .is_err()
        );
        assert_eq!(bounded.slots.available_permits(), 4);
        let content = read_response(&client, Url::parse(&base).unwrap(), MAX_BYTES)
            .await
            .unwrap();
        assert_eq!(content.content_type.as_deref(), Some("image/png"));
        assert!(!content.verification.is_verified());
        task.abort();
        let _ = task.await;
    }
    #[tokio::test]
    async fn cancelled_dns_worker_retains_its_slot_until_the_worker_exits() {
        use std::sync::{Condvar, Mutex};
        let client = AvatarClient::default();
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let entered = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let worker = {
            let client = client.clone();
            let gate = gate.clone();
            let entered = entered.clone();
            tokio::spawn(async move {
                client
                    .bounded(|permit| async move {
                        tokio::task::spawn_blocking(move || {
                            let _permit = permit;
                            entered.store(true, std::sync::atomic::Ordering::SeqCst);
                            let _wait = gate
                                .1
                                .wait_timeout_while(
                                    gate.0.lock().unwrap(),
                                    Duration::from_secs(2),
                                    |released| !*released,
                                )
                                .unwrap();
                            Err(unavailable())
                        })
                        .await
                        .unwrap()
                    })
                    .await
            })
        };
        tokio::time::timeout(Duration::from_secs(1), async {
            while !entered.load(std::sync::atomic::Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        worker.abort();
        let _ = worker.await;
        assert_eq!(client.slots.available_permits(), 3);
        *gate.0.lock().unwrap() = true;
        gate.1.notify_one();
        tokio::time::timeout(Duration::from_secs(1), async {
            while client.slots.available_permits() != 4 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    #[ignore = "bounded public objkt connectivity smoke"]
    async fn live_allowlisted_objkt_logo_is_read_through_validated_public_addresses() {
        let uri = "https://assets.objkt.media/file/assets-004/h/KT1CZMurPAjSfZqcn6LBUNUhG4byE6AJgDT6/logo?cb=aeaa26d6";
        let content = AvatarClient::default()
            .get(uri, MAX_BYTES as u64)
            .await
            .unwrap();
        assert!(
            content
                .content_type
                .as_deref()
                .unwrap()
                .starts_with("image/")
        );
        assert!(content.body.len() <= MAX_BYTES);
        assert!(!content.verification.is_verified());
    }

    #[tokio::test]
    async fn cancellation_timeout_and_exhaustion_release_avatar_slots() {
        let client = AvatarClient::default();
        let mut tasks = vec![];
        for _ in 0..4 {
            let client = client.clone();
            tasks.push(tokio::spawn(async move {
                client.bounded(|_| std::future::pending()).await
            }));
        }
        tokio::time::timeout(Duration::from_secs(1), async {
            while client.slots.available_permits() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(client.bounded(|_| std::future::pending()).await.is_err());
        for task in tasks {
            task.abort();
            let _ = task.await;
        }
        assert_eq!(client.slots.available_permits(), 4);
        let mut short = client.clone();
        short.timeout = Duration::from_millis(5);
        assert!(short.bounded(|_| std::future::pending()).await.is_err());
        assert_eq!(client.slots.available_permits(), 4);
    }
}
