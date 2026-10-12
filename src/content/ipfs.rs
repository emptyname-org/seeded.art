//! IPFS gateway fallback, verified CAR caching and optional bounded Kubo reads.

use std::collections::HashMap;
use std::process::Stdio;
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use bytes::Bytes;
use cid::Cid;
use sha2::{Digest, Sha256};
use tokio::io::AsyncReadExt;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use super::cache::DiskCache;
use super::car::{self, Outcome};
use super::http::{FetchError, Http};
use super::media::sniff_media;
use super::path::{self, Segment};
use super::{
    BreakerConfig, Content, ContentConfig, ContentError, ErrorKind, KuboConfig, Verification,
};

pub(super) const CAR_TYPE: &str = "application/vnd.ipld.car";
const CACHE_NS: &str = "ipfs-car-v1";
const KUBO_BUDGET: Duration = Duration::from_secs(3);
const KUBO_STDERR_CAP: u64 = 4096;
const MAX_IN_FLIGHT: usize = 128;
type FlightMap = Mutex<HashMap<String, Weak<tokio::sync::Mutex<()>>>>;

pub(crate) struct IpfsResolver {
    pub(super) gateways: Vec<String>,
    pub(super) http: Arc<Http>,
    cache: Arc<DiskCache>,
    assemblies: Arc<Semaphore>,
    cache_reads: Arc<Semaphore>,
    requests: Arc<Semaphore>,
    flights: Arc<FlightMap>,
    breakers: Arc<Mutex<Vec<Breaker>>>,
    breaker_config: BreakerConfig,
    kubo: Option<KuboConfig>,
    pub(super) cap: u64,
    pub(super) wait: Duration,
}

#[derive(Default)]
struct Breaker {
    failures: u32,
    until: Option<Instant>,
    probing: bool,
    generation: u64,
}

pub(super) struct Lease {
    breakers: Arc<Mutex<Vec<Breaker>>>,
    index: usize,
    probe: bool,
    generation: u64,
}

enum GatewayResult {
    Ready(Content, Option<Bytes>),
    PlainRequired,
}

struct GatewayError {
    error: ContentError,
    breaker_failure: bool,
}

impl From<ContentError> for GatewayError {
    fn from(error: ContentError) -> Self {
        Self {
            error,
            breaker_failure: false,
        }
    }
}

impl GatewayError {
    fn fetch(error: FetchError, cap: u64) -> Self {
        Self {
            breaker_failure: matches!(error, FetchError::Connect(_)),
            error: error.to_content_error("IPFS gateway", cap),
        }
    }
}

enum KuboRead {
    File(Bytes),
    Directory,
}

struct Flight {
    lock: Arc<tokio::sync::Mutex<()>>,
    key: String,
    registry: Arc<FlightMap>,
}

impl Drop for Flight {
    fn drop(&mut self) {
        if let Ok(mut registry) = self.registry.lock() {
            if Arc::strong_count(&self.lock) == 1 {
                registry.remove(&self.key);
            }
        }
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        if self.probe {
            if let Ok(mut breakers) = self.breakers.lock() {
                let state = &mut breakers[self.index];
                if state.generation == self.generation {
                    state.probing = false;
                }
            }
        }
    }
}

impl IpfsResolver {
    pub(crate) fn new(
        config: &ContentConfig,
        http: Arc<Http>,
        cache: Arc<DiskCache>,
        assemblies: Arc<Semaphore>,
    ) -> Result<Self, ContentError> {
        for gateway in &config.ipfs_gateways {
            let url = reqwest::Url::parse(gateway)
                .map_err(|_| ContentError::bad_request("invalid IPFS gateway URL"))?;
            if !matches!(url.scheme(), "http" | "https")
                || url.host_str().is_none()
                || !url.username().is_empty()
                || url.password().is_some()
                || url.query().is_some()
                || url.fragment().is_some()
                || !url.path().ends_with("/ipfs/")
            {
                return Err(ContentError::bad_request("IPFS gateway must end in /ipfs/"));
            }
        }
        if config.breaker.failure_threshold == 0
            || config.breaker.base_cooldown.is_zero()
            || config.breaker.max_cooldown < config.breaker.base_cooldown
        {
            return Err(ContentError::bad_request("invalid IPFS breaker limits"));
        }
        Ok(Self {
            gateways: config.ipfs_gateways.clone(),
            http,
            cache,
            assemblies,
            cache_reads: Arc::new(Semaphore::new(config.max_concurrent_assemblies.max(1))),
            requests: Arc::new(Semaphore::new(
                config.max_concurrent_assemblies.max(1).saturating_mul(8),
            )),
            flights: Arc::new(Mutex::new(HashMap::new())),
            breakers: Arc::new(Mutex::new(
                (0..config.ipfs_gateways.len())
                    .map(|_| Breaker::default())
                    .collect(),
            )),
            breaker_config: config.breaker.clone(),
            kubo: config.kubo.clone(),
            cap: config.max_object_bytes,
            wait: config.request_timeout,
        })
    }

    pub(crate) async fn resolve(
        &self,
        cid: &str,
        request_path: &str,
    ) -> Result<Content, ContentError> {
        let _request = self
            .requests
            .clone()
            .try_acquire_owned()
            .map_err(|_| self.http.busy("too many IPFS requests"))?;
        let deadline =
            tokio::time::Instant::now() + self.wait.saturating_mul(4).min(Duration::from_secs(35));
        let mut fallback = None;
        match tokio::time::timeout_at(
            deadline,
            self.resolve_inner(cid, request_path, deadline, &mut fallback),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => self.resolution_deadline(fallback),
        }
    }

    fn resolution_deadline(&self, fallback: Option<Content>) -> Result<Content, ContentError> {
        let error = self.http.deadline("IPFS resolution timed out");
        fallback.ok_or(error)
    }

    pub(crate) async fn local(
        &self,
        cid: &str,
        request_path: &str,
        permit: Arc<OwnedSemaphorePermit>,
        body_cap: u64,
    ) -> Result<Option<Content>, ContentError> {
        let root = car::parse_cid(cid)?;
        let segments = path::split(request_path)?;
        let encoded_path = segments
            .iter()
            .map(Segment::url_encoded)
            .collect::<Vec<_>>()
            .join("/");
        let key = hex::encode(Sha256::digest(format!("{root}/{encoded_path}").as_bytes()));
        let cache = self.cache.clone();
        let cap = self.cap.min(body_cap);
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let Some(data) = cache.get_local_capped(CACHE_NS, &key, car::transport_cap(cap))?
            else {
                return Ok(None);
            };
            // A stale hint or damaged cache entry is a miss, never a permanent absence.
            Ok(decode_content(&data, root, &segments, cap, true)
                .ok()
                .flatten())
        })
        .await
        .map_err(|_| ContentError::unavailable("local IPFS reader failed"))?
    }

    pub(crate) async fn probe_local(
        &self,
        cid: &str,
        request_path: &str,
        permit: Arc<OwnedSemaphorePermit>,
    ) -> Result<Option<super::LocalHint>, ContentError> {
        let root = car::parse_cid(cid)?;
        let segments = path::split(request_path)?;
        let encoded_path = segments
            .iter()
            .map(Segment::url_encoded)
            .collect::<Vec<_>>()
            .join("/");
        let key = hex::encode(Sha256::digest(format!("{root}/{encoded_path}").as_bytes()));
        let cache = self.cache.clone();
        let cap = self.cap;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let Some((prefix, size)) = cache.peek_local(CACHE_NS, &key, 1 << 20)? else {
                return Ok(None);
            };
            if size > car::transport_cap(cap) {
                return Ok(None);
            }
            // A complete file node narrows the bound; the verified reader enforces it.
            // Keep enough room for the encoded CAR, or fall back when the node is absent.
            let body_bytes = car::probe_car_prefix(&prefix, root, &segments, cap)
                .map(|file| file.max(size.min(cap)))
                .unwrap_or_else(|| {
                    if root.codec() == 0x55 {
                        cap.min(size)
                    } else {
                        cap
                    }
                });
            Ok(Some(super::LocalHint {
                body_bytes,
                scratch_bytes: car::transport_cap(body_bytes)
                    .saturating_add(body_bytes.saturating_mul(2))
                    .saturating_add(8 << 20),
                cache_keys: vec![key],
                mirror_leaf: None,
            }))
        })
        .await
        .map_err(|_| ContentError::unavailable("local IPFS probe failed"))?
    }

    pub(crate) fn touch(&self, cid: &str, request_path: &str) {
        let (Ok(root), Ok(segments)) = (car::parse_cid(cid), path::split(request_path)) else {
            return;
        };
        let encoded_path = segments
            .iter()
            .map(Segment::url_encoded)
            .collect::<Vec<_>>()
            .join("/");
        let key = hex::encode(Sha256::digest(format!("{root}/{encoded_path}").as_bytes()));
        self.cache.touch(CACHE_NS, &key);
    }

    async fn resolve_inner(
        &self,
        cid: &str,
        request_path: &str,
        deadline: tokio::time::Instant,
        fallback: &mut Option<Content>,
    ) -> Result<Content, ContentError> {
        let root = car::parse_cid(cid)?;
        let segments = path::split(request_path)?;
        let canonical = root.to_string();
        let encoded_path = segments
            .iter()
            .map(Segment::url_encoded)
            .collect::<Vec<_>>()
            .join("/");
        let key = hex::encode(Sha256::digest(
            format!("{canonical}/{encoded_path}").as_bytes(),
        ));
        // Warm reads have an independent bound: cold upstream work cannot monopolise
        // cache access, and verification workers retain their permit on cancellation.
        let cache_permit = self.acquire(&self.cache_reads).await?;
        if let Some(content) = self
            .cached(&key, root, &segments, cache_permit.clone())
            .await
        {
            return Ok(content);
        }
        drop(cache_permit);
        let flight = self.flight(&key)?;
        let _same_key = tokio::time::timeout(self.wait, flight.lock.lock())
            .await
            .map_err(|_| self.http.busy("IPFS object is already being resolved"))?;
        // A prior resolution may have filled the cache while this one waited.
        let cache_permit = self.acquire(&self.cache_reads).await?;
        if let Some(content) = self
            .cached(&key, root, &segments, cache_permit.clone())
            .await
        {
            return Ok(content);
        }
        drop(cache_permit);
        let permit = self.acquire(&self.assemblies).await?;
        let mut last_error = ContentError::unavailable("no IPFS gateway is available");
        if let Some(kubo) = &self.kubo {
            let decoded_path = segments
                .iter()
                .map(|s| s.decoded.as_str())
                .collect::<Vec<_>>()
                .join("/");
            let budget = kubo.timeout.min(KUBO_BUDGET).min(self.wait / 4);
            let kubo_deadline = (tokio::time::Instant::now() + budget).min(deadline);
            let result = async {
                match self
                    .kubo_read(
                        kubo,
                        &canonical,
                        &decoded_path,
                        kubo_deadline,
                        permit.clone(),
                    )
                    .await?
                {
                    KuboRead::File(body) => {
                        let kind = mime(&decoded_path, &body);
                        Ok(content(
                            body,
                            kind,
                            None,
                            Verification::IpfsKubo,
                            false,
                            false,
                        ))
                    }
                    KuboRead::Directory => {
                        let index_path = if decoded_path.is_empty() {
                            "index.html".to_owned()
                        } else {
                            format!("{decoded_path}/index.html")
                        };
                        match self
                            .kubo_read(kubo, &canonical, &index_path, kubo_deadline, permit.clone())
                            .await?
                        {
                            KuboRead::File(body) => Ok(content(
                                body,
                                mime("index.html", &[]),
                                None,
                                Verification::IpfsKubo,
                                true,
                                false,
                            )),
                            KuboRead::Directory => {
                                Err(ContentError::unavailable("Kubo index is a directory"))
                            }
                        }
                    }
                }
            }
            .await;
            match result {
                Ok(content) => return Ok(content),
                Err(error) if error.kind == ErrorKind::TooLarge => return Err(error),
                Err(error) => last_error = error,
            }
        }
        let mut plain_gateways = Vec::new();
        // Exhaust eligible proof sources before requesting or serving plain bytes.
        for (index, base) in self.gateways.iter().enumerate() {
            let _lease = match self.lease(index) {
                Ok(lease) => lease,
                Err(error) => {
                    last_error = error;
                    continue;
                }
            };
            let url = format!("{base}{canonical}/{encoded_path}");
            let remaining_attempts = (self.gateways.len() - index) * 2 + plain_gateways.len();
            let attempt = self.attempt_deadline(deadline, remaining_attempts);
            match self
                .gateway(&url, root, &segments, permit.clone(), true, attempt)
                .await
            {
                Ok(GatewayResult::Ready(content, car)) => {
                    self.success(index);
                    if let Some(car) = car {
                        *fallback = Some(content.clone());
                        let _ = self.cache.put_parts(CACHE_NS, &key, vec![car]).await;
                        return Ok(content);
                    }
                    if fallback.is_none() {
                        *fallback = Some(content);
                    }
                }
                Ok(GatewayResult::PlainRequired) => plain_gateways.push(index),
                Err(failure) => {
                    if failure.breaker_failure {
                        self.failure(index, failure.error.retry_after);
                    }
                    if failure.error.kind == ErrorKind::NotFound {
                        self.success(index);
                    }
                    if matches!(
                        failure.error.kind,
                        ErrorKind::NotFound | ErrorKind::TooLarge
                    ) {
                        return Err(failure.error);
                    }
                    last_error = failure.error;
                }
            }
        }
        if let Some(content) = fallback.take() {
            return Ok(content);
        }
        for (position, index) in plain_gateways.iter().copied().enumerate() {
            let _lease = match self.lease(index) {
                Ok(lease) => lease,
                Err(error) => {
                    last_error = error;
                    continue;
                }
            };
            let url = format!("{}{canonical}/{encoded_path}", self.gateways[index]);
            let attempt = self.attempt_deadline(deadline, plain_gateways.len() - position);
            match self
                .gateway(&url, root, &segments, permit.clone(), false, attempt)
                .await
            {
                Ok(GatewayResult::Ready(content, car)) => {
                    self.success(index);
                    if let Some(car) = car {
                        *fallback = Some(content.clone());
                        let _ = self.cache.put_parts(CACHE_NS, &key, vec![car]).await;
                    }
                    return Ok(content);
                }
                Ok(GatewayResult::PlainRequired) => {
                    unreachable!("plain request cannot negotiate CAR")
                }
                Err(failure) => {
                    if failure.breaker_failure {
                        self.failure(index, failure.error.retry_after);
                    }
                    if failure.error.kind == ErrorKind::NotFound {
                        self.success(index);
                    }
                    if matches!(
                        failure.error.kind,
                        ErrorKind::NotFound | ErrorKind::TooLarge
                    ) {
                        return Err(failure.error);
                    }
                    last_error = failure.error;
                }
            }
        }
        Err(last_error)
    }

    fn flight(&self, key: &str) -> Result<Flight, ContentError> {
        let mut flights = self
            .flights
            .lock()
            .map_err(|_| ContentError::unavailable("IPFS flights unavailable"))?;
        flights.retain(|_, flight| flight.strong_count() != 0);
        if let Some(flight) = flights.get(key).and_then(Weak::upgrade) {
            return Ok(Flight {
                lock: flight,
                key: key.to_owned(),
                registry: self.flights.clone(),
            });
        }
        if flights.len() >= MAX_IN_FLIGHT {
            return Err(self.http.busy("too many IPFS objects in flight"));
        }
        let flight = Arc::new(tokio::sync::Mutex::new(()));
        flights.insert(key.to_owned(), Arc::downgrade(&flight));
        Ok(Flight {
            lock: flight,
            key: key.to_owned(),
            registry: self.flights.clone(),
        })
    }

    fn attempt_deadline(
        &self,
        deadline: tokio::time::Instant,
        attempts: usize,
    ) -> tokio::time::Instant {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        tokio::time::Instant::now()
            + self
                .wait
                .min(remaining / u32::try_from(attempts.max(1)).unwrap_or(u32::MAX))
    }

    async fn acquire(
        &self,
        slots: &Arc<Semaphore>,
    ) -> Result<Arc<OwnedSemaphorePermit>, ContentError> {
        Ok(Arc::new(
            tokio::time::timeout(self.wait, slots.clone().acquire_owned())
                .await
                .map_err(|_| self.http.busy("content resolver is busy"))?
                .map_err(|_| ContentError::unavailable("content resolver is closed"))?,
        ))
    }

    async fn cached(
        &self,
        key: &str,
        root: Cid,
        segments: &[Segment],
        permit: Arc<OwnedSemaphorePermit>,
    ) -> Option<Content> {
        if let Ok(Some(data)) = self.cache.get(CACHE_NS, key).await {
            if data.len() as u64 > car::transport_cap(self.cap) {
                let _ = self.cache.remove(CACHE_NS, key).await;
                return None;
            }
            match self
                .verified(Bytes::from(data), root, segments, true, permit)
                .await
            {
                Ok(Some(content)) => return Some(content),
                _ => {
                    let _ = self.cache.remove(CACHE_NS, key).await;
                }
            }
        }
        None
    }

    /// A directory response needs a second entity CAR including index.html.
    async fn gateway(
        &self,
        url: &str,
        root: Cid,
        segments: &[Segment],
        permit: Arc<OwnedSemaphorePermit>,
        proof: bool,
        deadline: tokio::time::Instant,
    ) -> Result<GatewayResult, GatewayError> {
        let request = if proof {
            format!("{url}?format=car&dag-scope=entity")
        } else {
            url.to_owned()
        };
        let transfer_cap = if proof {
            car::transport_cap(self.cap)
        } else {
            self.cap
        };
        let reply = self
            .http
            .get_until(
                &request,
                Some(if proof { CAR_TYPE } else { "*/*" }),
                transfer_cap,
                deadline,
            )
            .await
            .map_err(|e| GatewayError::fetch(e, transfer_cap))?;
        if proof && matches!(reply.status, 406 | 415 | 501) {
            return Ok(GatewayResult::PlainRequired);
        }
        if reply.status != 200 {
            return Err(status_error(reply.status, reply.retry_after));
        }
        let is_car = reply
            .content_type
            .as_deref()
            .is_some_and(|s| s.split(';').next().unwrap_or_default().trim() == CAR_TYPE);
        if !is_car {
            if reply.body.len() as u64 > self.cap {
                return Err(ContentError::too_large(self.cap).into());
            }
            let directory_index = reqwest::Url::parse(&reply.final_url)
                .ok()
                .is_some_and(|u| u.path().ends_with('/') && !url.ends_with('/'));
            return Ok(GatewayResult::Ready(
                content(
                    reply.body,
                    reply.content_type,
                    reply.content_encoding,
                    Verification::Unverified,
                    directory_index,
                    false,
                ),
                None,
            ));
        }
        if reply.content_encoding.is_some() {
            return Err(ContentError::invalid("compressed CAR transport is unsupported").into());
        }
        if let Some(result) = tokio::time::timeout_at(
            deadline,
            self.verified(reply.body.clone(), root, segments, false, permit.clone()),
        )
        .await
        .map_err(|_| ContentError::unavailable("CAR verification deadline exceeded"))??
        {
            return Ok(GatewayResult::Ready(result, Some(reply.body)));
        }
        let index_url = format!(
            "{}/index.html?format=car&dag-scope=entity",
            url.trim_end_matches('/')
        );
        let index = self
            .http
            .get_until(
                &index_url,
                Some(CAR_TYPE),
                car::transport_cap(self.cap),
                deadline,
            )
            .await
            .map_err(|e| GatewayError::fetch(e, car::transport_cap(self.cap)))?;
        if index.status != 200 {
            return Err(status_error(index.status, index.retry_after));
        }
        if index
            .content_type
            .as_deref()
            .is_none_or(|s| s.split(';').next().unwrap_or_default().trim() != CAR_TYPE)
            || index.content_encoding.is_some()
        {
            return Err(ContentError::invalid("gateway omitted directory index proof").into());
        }
        let result = tokio::time::timeout_at(
            deadline,
            self.verified(index.body.clone(), root, segments, false, permit),
        )
        .await
        .map_err(|_| ContentError::unavailable("CAR verification deadline exceeded"))??
        .ok_or_else(|| ContentError::unavailable("CAR is missing directory index blocks"))?;
        Ok(GatewayResult::Ready(result, Some(index.body)))
    }

    async fn verified(
        &self,
        data: Bytes,
        root: Cid,
        segments: &[Segment],
        from_cache: bool,
        permit: Arc<OwnedSemaphorePermit>,
    ) -> Result<Option<Content>, ContentError> {
        let cap = self.cap;
        let segments = segments.to_vec();
        tokio::task::spawn_blocking(move || {
            // Cancellation keeps this permit alive until CPU work and its buffers end.
            let _permit = permit;
            decode_content(&data, root, &segments, cap, from_cache)
        })
        .await
        .map_err(|_| ContentError::unavailable("CAR verifier failed"))?
    }

    pub(super) fn lease(&self, index: usize) -> Result<Lease, ContentError> {
        let mut breakers = self
            .breakers
            .lock()
            .map_err(|_| ContentError::unavailable("IPFS breaker is unavailable"))?;
        let state = &mut breakers[index];
        let mut probe = false;
        if let Some(until) = state.until {
            if until > Instant::now() || state.probing {
                let mut error = self.http.busy("IPFS gateway is cooling down");
                error.retry_after = Some(
                    until
                        .saturating_duration_since(Instant::now())
                        .max(Duration::from_secs(1)),
                );
                return Err(error);
            }
            state.probing = true;
            state.generation = state.generation.wrapping_add(1);
            probe = true;
        }
        Ok(Lease {
            breakers: self.breakers.clone(),
            index,
            probe,
            generation: state.generation,
        })
    }

    pub(super) fn success(&self, index: usize) {
        if let Ok(mut breakers) = self.breakers.lock() {
            let generation = breakers[index].generation.wrapping_add(1);
            breakers[index] = Breaker {
                generation,
                ..Breaker::default()
            };
        }
    }

    pub(super) fn failure(&self, index: usize, retry: Option<Duration>) {
        if let Ok(mut breakers) = self.breakers.lock() {
            let state = &mut breakers[index];
            state.failures = state.failures.saturating_add(1);
            if state.failures >= self.breaker_config.failure_threshold || retry.is_some() {
                let multiplier = 1_u32
                    << state
                        .failures
                        .saturating_sub(self.breaker_config.failure_threshold)
                        .min(16);
                let cooldown = self
                    .breaker_config
                    .base_cooldown
                    .saturating_mul(multiplier)
                    .min(self.breaker_config.max_cooldown)
                    .max(retry.unwrap_or_default().min(Duration::from_secs(86_400)));
                state.until = Some(Instant::now() + cooldown);
            }
        }
    }

    async fn kubo_read(
        &self,
        config: &KuboConfig,
        cid: &str,
        path: &str,
        deadline: tokio::time::Instant,
        permit: Arc<OwnedSemaphorePermit>,
    ) -> Result<KuboRead, ContentError> {
        let config = config.clone();
        let target = format!("/ipfs/{cid}/{path}");
        let cap = self.cap;
        let (cancel_guard, cancelled) = tokio::sync::oneshot::channel::<()>();
        let worker = tokio::spawn(async move {
            // A cancelled caller releases admission, but the worker retains object
            // capacity until its process has been killed and reaped.
            let _permit = permit;
            kubo_process(config, target, cap, deadline, cancelled).await
        });
        let result = worker
            .await
            .map_err(|_| ContentError::unavailable("Kubo worker failed"))?;
        drop(cancel_guard);
        result
    }
}

fn decode_content(
    data: &[u8],
    root: Cid,
    segments: &[Segment],
    cap: u64,
    from_cache: bool,
) -> Result<Option<Content>, ContentError> {
    let (body, directory) = match car::resolve(data, root, segments, cap)? {
        Outcome::File(body) => (body, false),
        Outcome::Directory => {
            let mut index = segments.to_vec();
            index.push(Segment {
                raw: "index.html".into(),
                decoded: "index.html".into(),
            });
            match car::resolve(data, root, &index, cap) {
                Ok(Outcome::File(body)) => (body, true),
                Ok(Outcome::Directory) => {
                    return Err(ContentError::invalid("directory index is not a file"));
                }
                Err(e) if e.kind == ErrorKind::Unavailable => return Ok(None),
                Err(e) => return Err(e),
            }
        }
    };
    let filename = if directory {
        "index.html"
    } else {
        segments
            .last()
            .map(|s| s.decoded.as_str())
            .unwrap_or_default()
    };
    let content_type = mime(filename, &body);
    Ok(Some(content(
        Bytes::from(body),
        content_type,
        None,
        Verification::IpfsCar,
        directory,
        from_cache,
    )))
}

async fn kubo_process(
    config: KuboConfig,
    target: String,
    cap: u64,
    deadline: tokio::time::Instant,
    cancelled: tokio::sync::oneshot::Receiver<()>,
) -> Result<KuboRead, ContentError> {
    if tokio::time::Instant::now() >= deadline {
        return Err(ContentError::unavailable("Kubo timed out"));
    }
    let mut child = tokio::process::Command::new(&config.binary)
        .arg("cat")
        .arg(target)
        .env("IPFS_PATH", &config.ipfs_path)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(|_| ContentError::unavailable("Kubo could not start"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| ContentError::unavailable("Kubo stdout unavailable"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| ContentError::unavailable("Kubo stderr unavailable"))?;
    let operation = async {
        let output = async {
            let mut data = Vec::new();
            stdout
                .take(cap.saturating_add(1))
                .read_to_end(&mut data)
                .await
                .map_err(|_| ContentError::unavailable("Kubo output failed"))?;
            if data.len() as u64 > cap {
                return Err(ContentError::too_large(cap));
            }
            Ok::<_, ContentError>(data)
        };
        let errors = async {
            let mut data = Vec::new();
            stderr
                .take(KUBO_STDERR_CAP + 1)
                .read_to_end(&mut data)
                .await
                .map_err(|_| ContentError::unavailable("Kubo stderr failed"))?;
            if data.len() as u64 > KUBO_STDERR_CAP {
                return Err(ContentError::unavailable("Kubo stderr exceeds limit"));
            }
            Ok::<_, ContentError>(data)
        };
        let (output, errors) = tokio::try_join!(output, errors)?;
        let status = child
            .wait()
            .await
            .map_err(|_| ContentError::unavailable("Kubo wait failed"))?;
        if !status.success() {
            if String::from_utf8_lossy(&errors)
                .lines()
                .any(|line| line.trim_end().ends_with("is a directory"))
            {
                return Ok(KuboRead::Directory);
            }
            return Err(ContentError::unavailable("Kubo content unavailable"));
        }
        Ok(KuboRead::File(Bytes::from(output)))
    };
    let outcome = tokio::select! {
        outcome = tokio::time::timeout_at(deadline, operation) => outcome,
        _ = cancelled => Ok(Err(ContentError::unavailable("Kubo cancelled"))),
    };
    match outcome {
        Ok(Ok(result)) => Ok(result),
        result => {
            // Size, I/O and deadline failures kill and reap the child; cancellation
            // retains Tokio's kill-on-drop guard until the process is reaped.
            let _ = child.kill().await;
            let _ = child.wait().await;
            match result {
                Ok(Err(error)) => Err(error),
                _ => Err(ContentError::unavailable("Kubo timed out")),
            }
        }
    }
}

fn status_error(status: u16, retry_after: Option<Duration>) -> GatewayError {
    let mut error = ContentError::unavailable(format!("IPFS gateway returned {status}"));
    // Gateway 404 is not proof of absence in the requested immutable DAG.
    error.retry_after = retry_after;
    GatewayError {
        error,
        breaker_failure: status == 429 || (500..600).contains(&status),
    }
}

pub(super) fn content(
    body: Bytes,
    content_type: Option<String>,
    content_encoding: Option<String>,
    verification: Verification,
    directory_index: bool,
    from_cache: bool,
) -> Content {
    Content {
        delivery_accounted: false,
        etag: format!("\"{}\"", hex::encode(Sha256::digest(&body))),
        body,
        content_type,
        content_encoding,
        verification,
        directory_index,
        from_cache,
    }
}

pub(super) fn mime(path: &str, body: &[u8]) -> Option<String> {
    let suffix = path
        .rsplit('.')
        .next()
        .unwrap_or_default()
        .to_ascii_lowercase();
    let kind = match suffix.as_str() {
        "html" | "htm" => "text/html; charset=utf-8",
        "js" | "mjs" => "text/javascript; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "json" | "map" => "application/json",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "woff" => "font/woff",
        "woff2" => "font/woff2",
        "ttf" => "font/ttf",
        "wasm" => "application/wasm",
        "mp3" => "audio/mpeg",
        "wav" => "audio/wav",
        "mp4" => "video/mp4",
        "webm" => "video/webm",
        _ => {
            let prefix = std::str::from_utf8(&body[..body.len().min(256)])
                .unwrap_or_default()
                .trim_start()
                .to_ascii_lowercase();
            if let Some(kind) = sniff_media(body) {
                kind
            } else if prefix.starts_with("<!doctype html") || prefix.starts_with("<html") {
                "text/html; charset=utf-8"
            } else {
                "application/octet-stream"
            }
        }
    };
    Some(kind.to_owned())
}

#[cfg(test)]
mod local_probe_tests {
    use super::*;

    #[tokio::test]
    async fn upstream_busy_and_other_errors_are_distinct_and_recovery_is_counted_once() {
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join(format!("tmp/ipfs-error-counters-{}", std::process::id()));
        let mut cfg = ContentConfig::mainnet(&path);
        cfg.ipfs_gateways.clear();
        let store = super::super::ContentStore::new(cfg).unwrap();
        let root = cid(0x55, b"test").to_string();
        let slots = store.ipfs.requests.available_permits() as u32;
        let held = store
            .ipfs
            .requests
            .clone()
            .acquire_many_owned(slots)
            .await
            .unwrap();
        let error = store
            .get_after_local_miss("ipfs", &root, "", false)
            .await
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::Unavailable);
        let counters = store.metrics().resolution;
        assert_eq!(counters.upstream_failures, 1);
        assert_eq!(counters.upstream_busy, 1);
        assert_eq!(counters.upstream_other_failures, 0);
        drop(held);
        assert!(
            store
                .get_after_local_miss("ipfs", &root, "", false)
                .await
                .is_err()
        );
        assert_eq!(store.metrics().resolution.upstream_failures, 2);
        assert_eq!(store.metrics().resolution.upstream_busy, 1);
        assert!(
            store
                .get_after_local_miss("ipfs", "invalid", "", false)
                .await
                .is_err()
        );
        assert!(store.get_local("ipfs", "invalid", "").await.is_err());
        let counters = store.metrics().resolution;
        assert_eq!(counters.upstream_failures, 2);
        assert_eq!(counters.upstream_other_failures, 1);
        assert_eq!(counters.local_failures, 0);
        assert_eq!(counters.local_other_failures, 1);
        drop(store);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn resolver_deadline_is_counted_even_when_fallback_succeeds() {
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join(format!("tmp/ipfs-deadline-fallback-{}", std::process::id()));
        let mut cfg = ContentConfig::mainnet(&path);
        cfg.ipfs_gateways.clear();
        let store = super::super::ContentStore::new(cfg).unwrap();
        let fallback = content(
            Bytes::from_static(b"fallback"),
            None,
            None,
            Verification::IpfsCar,
            false,
            false,
        );

        let result = store.ipfs.resolution_deadline(Some(fallback)).unwrap();
        assert_eq!(result.body, "fallback");
        let metrics = store.metrics().resolution;
        assert_eq!(metrics.upstream_timeouts, 1);
        assert_eq!(metrics.upstream_failures, 0);

        drop(store);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cooldown_skip_can_be_followed_by_success_without_counting_failure() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join(format!("tmp/ipfs-cooldown-recovery-{}", std::process::id()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0u8; 4096];
            let _ = stream.read(&mut request).await.unwrap();
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 5\r\nConnection: close\r\n\r\nhello",
                )
                .await
                .unwrap();
        });
        let mut cfg = ContentConfig::mainnet(&path);
        cfg.ipfs_gateways = vec![
            "http://127.0.0.1:1/ipfs/".into(),
            format!("http://{address}/ipfs/"),
        ];
        cfg.connect_timeout = Duration::from_millis(50);
        cfg.request_timeout = Duration::from_secs(1);
        cfg.breaker.failure_threshold = 1;
        cfg.breaker.base_cooldown = Duration::from_secs(60);
        cfg.breaker.max_cooldown = Duration::from_secs(60);
        let store = super::super::ContentStore::new(cfg).unwrap();
        store.ipfs.failure(0, None);

        let root = cid(0x55, b"root").to_string();
        let result = store
            .get_after_local_miss("ipfs", &root, "", false)
            .await
            .unwrap();
        assert_eq!(result.body, "hello");
        server.await.unwrap();
        let metrics = store.metrics().resolution;
        assert_eq!(metrics.upstream_busy, 1);
        assert_eq!(metrics.upstream_failures, 0);
        assert_eq!(metrics.upstream_other_failures, 0);

        drop(store);
        std::fs::remove_dir_all(path).unwrap();
    }

    fn varint(mut n: usize) -> Vec<u8> {
        let mut out = Vec::new();
        while n >= 128 {
            out.push((n as u8) | 128);
            n >>= 7;
        }
        out.push(n as u8);
        out
    }
    fn field(tag: u8, bytes: &[u8]) -> Vec<u8> {
        let mut out = vec![tag];
        out.extend(varint(bytes.len()));
        out.extend(bytes);
        out
    }
    fn cid(codec: u64, bytes: &[u8]) -> Cid {
        Cid::new_v1(
            codec,
            cid::multihash::Multihash::wrap(0x12, &Sha256::digest(bytes)).unwrap(),
        )
    }

    #[tokio::test]
    async fn cached_small_file_under_directory_uses_less_than_sixteen_mib_scratch() {
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join(format!("tmp/ipfs-small-probe-{}", std::process::id()));
        let body = vec![19; 4096];
        let leaf = cid(0x55, &body);
        let mut link = field(10, &leaf.to_bytes());
        link.extend(field(18, b"file.js"));
        let mut directory = field(10, &[8, 1]);
        directory.extend(field(18, &link));
        let root = cid(0x70, &directory);
        let mut header = vec![0xa2, 0x65];
        header.extend(b"roots");
        header.extend([0x81, 0xd8, 0x2a, 0x58, root.to_bytes().len() as u8 + 1, 0]);
        header.extend(root.to_bytes());
        header.push(0x67);
        header.extend(b"version");
        header.push(1);
        let mut data = varint(header.len());
        data.extend(header);
        for (cid, bytes) in [(root, directory), (leaf, body.clone())] {
            data.extend(varint(cid.to_bytes().len() + bytes.len()));
            data.extend(cid.to_bytes());
            data.extend(bytes);
        }
        let mut cfg = ContentConfig::mainnet(&path);
        cfg.max_object_bytes = 96 << 20;
        let store = super::super::ContentStore::new(cfg).unwrap();
        let key = hex::encode(Sha256::digest(format!("{root}/file.js").as_bytes()));
        store.cache.put(CACHE_NS, &key, data).await.unwrap();
        let permit = Arc::new(tokio::sync::Semaphore::new(1))
            .acquire_owned()
            .await
            .unwrap();
        let hint = store
            .ipfs
            .probe_local(&root.to_string(), "file.js", Arc::new(permit))
            .await
            .unwrap()
            .unwrap();
        assert!(hint.scratch_bytes < (16 << 20), "{}", hint.scratch_bytes);
        assert!(hint.body_bytes < (1 << 20));
        let served = store
            .get_local("ipfs", &root.to_string(), "file.js")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&served.body[..], &body);
        assert!(served.delivery_accounted);
        assert_eq!(store.metrics().cache.upstream_fetches, 0);
        drop(store);
        std::fs::remove_dir_all(path).unwrap();
    }
}
