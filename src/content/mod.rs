//! Artwork content layer: serves `onchfs://` and `ipfs://` resources.
//!
//! ONCHFS is read from the Tezos ONCHFS contracts' big maps through TzKT and
//! every inode and chunk is checked against its keccak CID. IPFS goes through
//! an optional local Kubo node, then an ordered gateway list. Gateways are asked
//! for a trustless CAR first so the bytes can be checked against the CID; a
//! plain gateway response is served but marked [`Verification::Unverified`].
//!
//! An optional preservation mirror ([`MirrorWriter`], `seeded-index mirror`) is read first;
//! verified local bytes are retained in a bounded memory cache.
//!
//! Object size, HTTP concurrency and assemblies are bounded. Parsing, hash
//! verification and encoded records also require memory beyond object bytes.

pub mod cache;
pub(crate) mod car;
mod flight;
pub mod hpack;
mod http;
pub(crate) mod ipfs;
mod local;
pub(crate) mod media;
mod mirror;
pub mod onchfs;
pub mod path;
mod stats;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;

pub use mirror::{MirrorWriter, PackageError, PackageStats};
pub use onchfs::Resolved;
pub use stats::{CacheMetrics, ContentMetrics, LocalDeliveryMetrics, MirrorMetrics};

/// IPFS gateways in fallback order, checked 2026-10-03: the first two serve
/// trustless CARs (`?format=car&dag-scope=entity`); ipfs.io and dweb.link
/// answered scripted requests with HTTP 429.
pub const DEFAULT_IPFS_GATEWAYS: [&str; 4] = [
    "https://gateway.pinit.io/ipfs/",
    "https://ipfs.raribleuserdata.com/ipfs/",
    "https://dweb.link/ipfs/",
    "https://ipfs.io/ipfs/",
];

/// Optional local Kubo node.
#[derive(Debug, Clone)]
pub struct KuboConfig {
    pub binary: PathBuf,
    /// Its own repo, e.g. under `tmp/` or `var/`.
    pub ipfs_path: PathBuf,
    pub timeout: Duration,
}

/// Mainnet TzKT API.
pub const TZKT_MAINNET: &str = "https://api.tzkt.io";
/// User-Agent sent to every upstream.
pub const USER_AGENT: &str = "seeded.art-archive-dev";

/// What went wrong, as far as a caller needs to know.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// Confirmed absent: the chain or a verified directory says it is not there.
    NotFound,
    /// Transient upstream failure; retry later.
    Unavailable,
    /// Hash mismatch, malformed CID or undecodable verified data.
    Invalid,
    /// Malformed request path (traversal, bad escapes).
    BadRequest,
    /// The object exceeds `max_object_bytes`.
    TooLarge,
}

/// Content-layer error.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{kind:?}: {message}")]
pub struct ContentError {
    pub kind: ErrorKind,
    pub message: String,
    /// Upstream hint for when a retry may succeed.
    pub retry_after: Option<Duration>,
}

impl ContentError {
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            retry_after: None,
        }
    }

    pub(crate) fn not_found(m: impl Into<String>) -> Self {
        Self::new(ErrorKind::NotFound, m)
    }

    pub(crate) fn unavailable(m: impl Into<String>) -> Self {
        Self::new(ErrorKind::Unavailable, m)
    }

    pub(crate) fn invalid(m: impl Into<String>) -> Self {
        Self::new(ErrorKind::Invalid, m)
    }

    pub(crate) fn bad_request(m: impl Into<String>) -> Self {
        Self::new(ErrorKind::BadRequest, m)
    }

    pub(crate) fn too_large(limit: u64) -> Self {
        Self::new(ErrorKind::TooLarge, format!("object exceeds {limit} bytes"))
    }

    /// Suggested HTTP status for the web layer.
    pub fn http_status(&self) -> u16 {
        match self.kind {
            ErrorKind::NotFound => 404,
            ErrorKind::Unavailable => 503,
            ErrorKind::Invalid | ErrorKind::TooLarge => 502,
            ErrorKind::BadRequest => 400,
        }
    }
}

impl From<std::io::Error> for ContentError {
    fn from(e: std::io::Error) -> Self {
        Self::unavailable(format!("cache i/o: {e}"))
    }
}

/// How the bytes were checked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verification {
    /// ONCHFS: every inode and chunk matched its keccak CID.
    OnchfsKeccak,
    /// IPFS: every block of a trustless CAR matched its CID.
    IpfsCar,
    /// IPFS: read through the local Kubo node, which verifies blocks.
    IpfsKubo,
    /// IPFS: plain gateway bytes, not checked against the CID.
    Unverified,
}

impl Verification {
    pub fn is_verified(self) -> bool {
        !matches!(self, Verification::Unverified)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Verification::OnchfsKeccak => "onchfs-keccak",
            Verification::IpfsCar => "ipfs-car",
            Verification::IpfsKubo => "ipfs-kubo",
            Verification::Unverified => "unverified",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "onchfs-keccak" => Verification::OnchfsKeccak,
            "ipfs-car" => Verification::IpfsCar,
            "ipfs-kubo" => Verification::IpfsKubo,
            "unverified" => Verification::Unverified,
            _ => return None,
        })
    }
}

/// A resolved resource, ready to serve.
#[derive(Debug, Clone)]
pub struct Content {
    /// The body retains a shared local byte charge until its last clone drops.
    pub delivery_accounted: bool,
    /// Bytes exactly as stored; decode per `content_encoding` only if needed.
    pub body: Bytes,
    pub content_type: Option<String>,
    /// E.g. `gzip` for most ONCHFS files. Serve as `Content-Encoding`.
    pub content_encoding: Option<String>,
    pub verification: Verification,
    /// The request named a directory and its `index.html` was served. If the
    /// request path did not end in `/`, redirect to the slash form so relative
    /// URLs resolve inside the directory.
    pub directory_index: bool,
    /// Served from the disk cache without touching the network.
    pub from_cache: bool,
    /// Strong validator: mirror file CID, otherwise sha256 of the IPFS body.
    pub etag: String,
}

/// Circuit breaker settings, per IPFS gateway.
#[derive(Debug, Clone)]
pub struct BreakerConfig {
    /// Consecutive failures that open the breaker.
    pub failure_threshold: u32,
    /// First open period; doubles with each further failure.
    pub base_cooldown: Duration,
    pub max_cooldown: Duration,
}

impl Default for BreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 3,
            base_cooldown: Duration::from_secs(30),
            max_cooldown: Duration::from_secs(600),
        }
    }
}

/// Store configuration. [`ContentConfig::mainnet`] gives working defaults.
#[derive(Debug, Clone)]
pub struct ContentConfig {
    pub cache_dir: PathBuf,
    /// Preservation mirror read before the cache; `None` disables it.
    pub mirror_dir: Option<PathBuf>,
    /// TzKT base URL, without a trailing slash.
    pub tzkt_base: String,
    /// ONCHFS file-objects `inodes` big map id.
    pub onchfs_inodes_bigmap: u64,
    /// ONCHFS content-store chunk big map id.
    pub onchfs_chunks_bigmap: u64,
    /// Ordered gateway prefixes, each ending in `/ipfs/`.
    pub ipfs_gateways: Vec<String>,
    pub kubo: Option<KuboConfig>,
    pub user_agent: String,
    pub connect_timeout: Duration,
    /// Per request, headers and body included.
    pub request_timeout: Duration,
    pub max_object_bytes: u64,
    /// Maximum temporary cache bytes; least-recently-used objects may be evicted.
    pub max_cache_bytes: u64,
    /// Outbound HTTP requests in flight across the store.
    pub max_concurrent_requests: usize,
    /// Network resolutions holding object bytes in memory at once.
    pub max_concurrent_assemblies: usize,
    /// ONCHFS chunks requested per TzKT call.
    pub chunk_batch: usize,
    /// How long an upstream-confirmed absence is remembered.
    pub absence_ttl: Duration,
    pub breaker: BreakerConfig,
}

impl ContentConfig {
    /// Tezos mainnet ONCHFS via TzKT, and the gateway order verified on 2026-10-03.
    pub fn mainnet(cache_dir: impl Into<PathBuf>) -> Self {
        Self {
            cache_dir: cache_dir.into(),
            mirror_dir: None,
            tzkt_base: TZKT_MAINNET.to_string(),
            onchfs_inodes_bigmap: onchfs::MAINNET_INODES_BIGMAP,
            onchfs_chunks_bigmap: onchfs::MAINNET_CHUNKS_BIGMAP,
            ipfs_gateways: DEFAULT_IPFS_GATEWAYS
                .iter()
                .map(|s| s.to_string())
                .collect(),
            kubo: None,
            user_agent: USER_AGENT.to_string(),
            connect_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(30),
            max_object_bytes: 32 * 1024 * 1024,
            max_cache_bytes: 2 * 1024 * 1024 * 1024,
            max_concurrent_requests: 4,
            max_concurrent_assemblies: 8,
            chunk_batch: 16,
            absence_ttl: Duration::from_secs(3600),
            breaker: BreakerConfig::default(),
        }
    }
}

/// Async, framework-agnostic content store. Cheap to clone.
#[derive(Clone)]
pub struct ContentStore {
    onchfs: Arc<onchfs::OnchfsResolver>,
    ipfs: Arc<ipfs::IpfsResolver>,
    cache: Arc<cache::DiskCache>,
    passive_onchfs: Arc<onchfs::OnchfsResolver>,
    passive_ipfs: Arc<ipfs::IpfsResolver>,
    mirror: Option<Arc<mirror::Mirror>>,
    verified: Arc<local::Verified>,
    counters: Arc<stats::ContentCounters>,
    local_reads: Arc<tokio::sync::Semaphore>,
    local_flights: Arc<flight::Flights>,
    local_read_wait: Arc<local::Delivery>,
    local_leader_wait: Arc<local::Delivery>,
    local_delivery: Arc<local::Delivery>,
    local_scratch: Arc<local::Delivery>,
    upstream_copies: Arc<tokio::sync::Semaphore>,
    #[cfg(test)]
    local_read_delay: Duration,
    #[cfg(test)]
    local_lookup_count: Arc<std::sync::atomic::AtomicU64>,
}

/// Bounded metadata from a local backing object; no body has been allocated yet.
#[derive(Debug, Clone)]
pub(crate) struct LocalHint {
    pub body_bytes: u64,
    pub scratch_bytes: u64,
    pub cache_keys: Vec<String>,
    pub mirror_leaf: Option<car::LeafPlan>,
}

struct LocalBytes {
    bytes: Bytes,
    _permit: Arc<local::BytePermit>,
}
impl AsRef<[u8]> for LocalBytes {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

fn onchfs_content(r: Resolved) -> Content {
    Content {
        delivery_accounted: false,
        etag: format!("\"{}\"", hex::encode(r.file_cid)),
        body: r.bytes,
        content_type: r.content_type,
        content_encoding: r.content_encoding,
        verification: Verification::OnchfsKeccak,
        directory_index: r.directory_index,
        from_cache: r.from_cache,
    }
}

impl ContentStore {
    /// Opens the disk cache and builds the HTTP client. No network access.
    pub fn new(config: ContentConfig) -> Result<Self, ContentError> {
        let base = reqwest::Url::parse(&config.tzkt_base)
            .map_err(|_| ContentError::bad_request("invalid TzKT URL"))?;
        if !matches!(base.scheme(), "http" | "https")
            || base.host_str().is_none()
            || !base.username().is_empty()
            || base.password().is_some()
            || base.query().is_some()
            || base.fragment().is_some()
            || config.ipfs_gateways.len() > 8
            || config.connect_timeout.is_zero()
            || config.connect_timeout > Duration::from_secs(300)
            || config.kubo.as_ref().is_some_and(|kubo| {
                kubo.timeout.is_zero()
                    || kubo.timeout > Duration::from_secs(300)
                    || kubo.binary.as_os_str().is_empty()
                    || kubo.ipfs_path.as_os_str().is_empty()
            })
        {
            return Err(ContentError::bad_request("invalid upstream configuration"));
        }
        if config.max_concurrent_requests == 0
            || config.max_concurrent_assemblies == 0
            || config.chunk_batch == 0
        {
            return Err(ContentError::bad_request(
                "concurrency limits must be positive",
            ));
        }
        if config.max_object_bytes == 0
            || config.max_object_bytes > 256 * 1024 * 1024
            || config.max_concurrent_requests > 64
            || config.max_concurrent_assemblies > 16
            || config.chunk_batch > 100
            || config.request_timeout.is_zero()
            || config.request_timeout > Duration::from_secs(300)
            || config.absence_ttl > Duration::from_secs(86_400)
        {
            return Err(ContentError::bad_request(
                "content limits outside supported bounds",
            ));
        }
        if let Some(mirror_dir) = &config.mirror_dir {
            let cache_path = resolved_path(&config.cache_dir)
                .map_err(|_| ContentError::bad_request("invalid cache directory"))?;
            let mirror_path = resolved_path(mirror_dir)
                .map_err(|_| ContentError::bad_request("invalid mirror directory"))?;
            if cache_path.starts_with(&mirror_path) || mirror_path.starts_with(&cache_path) {
                return Err(ContentError::bad_request(
                    "cache and mirror directories must not overlap",
                ));
            }
        }
        let cache = Arc::new(cache::DiskCache::open_evicting(
            &config.cache_dir,
            config.max_cache_bytes,
            (config.max_object_bytes + 1024 * 1024)
                .max(car::transport_cap(config.max_object_bytes)),
        )?);
        let counters = Arc::new(stats::ContentCounters::default());
        let http = Arc::new(http::Http::new(&config)?.with_counters(counters.clone()));
        let assemblies = Arc::new(tokio::sync::Semaphore::new(
            config.max_concurrent_assemblies,
        ));
        let media_cache = Arc::new(cache.read_only());
        let passive_onchfs = Arc::new(onchfs::OnchfsResolver::new(
            &config,
            http.clone(),
            media_cache.clone(),
            assemblies.clone(),
        ));
        let passive_ipfs = Arc::new(ipfs::IpfsResolver::new(
            &config,
            http.clone(),
            media_cache,
            assemblies.clone(),
        )?);
        let onchfs = Arc::new(onchfs::OnchfsResolver::new(
            &config,
            http.clone(),
            cache.clone(),
            assemblies.clone(),
        ));
        let ipfs = Arc::new(ipfs::IpfsResolver::new(
            &config,
            http,
            cache.clone(),
            assemblies,
        )?);
        let mirror = config.mirror_dir.clone().map(|dir| {
            Arc::new(mirror::Mirror::new(
                dir,
                config.max_object_bytes,
                config.max_concurrent_assemblies,
            ))
        });
        Ok(Self {
            onchfs,
            ipfs,
            cache,
            passive_onchfs,
            passive_ipfs,
            mirror,
            verified: Arc::new(local::Verified::default()),
            counters,
            local_reads: Arc::new(tokio::sync::Semaphore::new(16)),
            local_flights: Arc::new(flight::Flights::default()),
            local_read_wait: Arc::new(local::Delivery::new(0, 256, Duration::from_secs(2))),
            local_leader_wait: Arc::new(local::Delivery::new(
                0,
                flight::MAX_FOLLOWERS,
                Duration::from_secs(5),
            )),
            local_delivery: Arc::new(local::Delivery::default()),
            local_scratch: Arc::new(local::Delivery::new(
                (car::transport_cap(config.max_object_bytes)
                    .saturating_add(config.max_object_bytes.saturating_mul(2))
                    .saturating_add(8 << 20)
                    .max(512 << 20) as usize)
                    .div_ceil(local::BYTE_UNIT),
                256,
                Duration::from_secs(2),
            )),
            upstream_copies: Arc::new(tokio::sync::Semaphore::new(2)),
            #[cfg(test)]
            local_read_delay: Duration::ZERO,
            #[cfg(test)]
            local_lookup_count: Arc::default(),
        })
    }

    /// Verified memory, mirror or disk-cache reads only; never waits on upstream assembly.
    /// Read workers and retained bytes have separate bounds from upstream assembly.
    pub async fn get_local(
        &self,
        scheme: &str,
        cid: &str,
        path: &str,
    ) -> Result<Option<Content>, ContentError> {
        let mut shared_failure = false;
        let result = self
            .get_local_inner(scheme, cid, path, &mut shared_failure)
            .await;
        let result = result.and_then(|content| {
            content
                .map(|mut content| {
                    content.body = self.local_delivery.track(content.body)?;
                    Ok(content)
                })
                .transpose()
        });
        if let Err(error) = &result {
            let counter = if error.kind == ErrorKind::Unavailable {
                &self.counters.local_failures
            } else {
                &self.counters.local_other_failures
            };
            counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if shared_failure {
                self.counters
                    .local_shared_failures
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            } else if local::AdmissionBudget::exhausted(error) {
                self.counters
                    .local_deadline_expired
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
        result
    }

    async fn get_local_inner(
        &self,
        scheme: &str,
        cid: &str,
        path: &str,
        shared_failure: &mut bool,
    ) -> Result<Option<Content>, ContentError> {
        match scheme {
            "onchfs" => {
                onchfs::parse_cid(cid)?;
            }
            "ipfs" => {
                car::parse_cid(cid)?;
            }
            _ => return Err(ContentError::bad_request("unsupported content scheme")),
        }
        path::split(path)?;
        let key = format!("{scheme}://{cid}/{path}");
        let mut admission = self.local_delivery.admission();
        if let Some(content) = self
            .memory_hit(&key, scheme, cid, path, &mut admission)
            .await?
        {
            return Ok(Some(content));
        }
        let flight = self.local_flights.register(key.clone()).inspect_err(|_| {
            self.counters
                .local_coordination_rejected
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        })?;
        if flight.joined {
            self.counters
                .local_coalesced
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        // Leader disk work spends no follower admission budget. This independent
        // wait is bounded to 5s and 256 followers without reader/scratch slots.
        let turn = self
            .local_leader_wait
            .acquire_budget(flight.turn(), 1, &mut self.local_leader_wait.admission())
            .await
            .map_err(|mut error| {
                // A ready permit polled after expiry is also a leader timeout,
                // never exhaustion of this request's separate admission budget.
                if local::AdmissionBudget::exhausted(&error) {
                    self.counters
                        .local_leader_deadline_expired
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    error.message = "local leader deadline elapsed".into();
                }
                error
            })?;
        if let Some(outcome) = flight.outcome() {
            match &outcome {
                Ok(Some(_)) => self.record_hit(false),
                Ok(None) => {
                    self.counters
                        .cache_misses
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                Err(_) => *shared_failure = true,
            }
            return outcome;
        }
        if let Some(content) = self
            .memory_hit(&key, scheme, cid, path, &mut admission)
            .await
            .inspect_err(|error| flight.complete_busy(error))?
        {
            let outcome = Some(content);
            flight.complete(&outcome);
            return Ok(outcome);
        }
        let permit = Arc::new(
            self.local_read_wait
                .acquire_budget(self.local_reads.clone(), 1, &mut admission)
                .await
                .inspect_err(|error| flight.complete_busy(error))?,
        );
        if let Some(content) = self
            .memory_hit(&key, scheme, cid, path, &mut admission)
            .await
            .inspect_err(|error| flight.complete_busy(error))?
        {
            let outcome = Some(content);
            flight.complete(&outcome);
            return Ok(outcome);
        }
        admission
            .check()
            .inspect_err(|error| flight.complete_busy(error))?;
        let reader = self.clone();
        let scheme = scheme.to_owned();
        let cid = cid.to_owned();
        let path = path.to_owned();
        // Cancellation detaches this bounded task; its reader slot and scratch
        // reservations survive until blocking disk work and result buffers end.
        tokio::spawn(async move {
            let _turn = turn;
            let result = reader
                .read_local((&scheme, &cid, &path), key, permit, admission)
                .await;
            match &result {
                Ok(outcome) => flight.complete(outcome),
                Err(error) => flight.complete_busy(error),
            }
            // I/O failure or cancellation leaves one follower to retry.
            result
        })
        .await
        .map_err(|_| ContentError::unavailable("local reader failed"))?
    }

    async fn memory_hit(
        &self,
        key: &str,
        scheme: &str,
        cid: &str,
        path: &str,
        admission: &mut local::AdmissionBudget,
    ) -> Result<Option<Content>, ContentError> {
        let Some(content) = self
            .verified
            .deliver_budget(key, &self.local_delivery, admission)
            .await?
        else {
            return Ok(None);
        };
        self.touch_memory(key, scheme, cid, path, &content);
        self.record_hit(false);
        Ok(Some(content))
    }

    fn touch_memory(&self, key: &str, scheme: &str, cid: &str, path: &str, content: &Content) {
        if let Some(keys) = self.verified.touch_due(key) {
            match scheme {
                "ipfs" => self.ipfs.touch(cid, path),
                "onchfs" => {
                    self.onchfs.touch(cid);
                    for key in keys {
                        self.onchfs.touch(&key);
                    }
                    self.onchfs.touch(content.etag.trim_matches('"'));
                }
                _ => {}
            }
        }
    }

    async fn read_local(
        &self,
        request: (&str, &str, &str),
        key: String,
        permit: Arc<tokio::sync::OwnedSemaphorePermit>,
        mut admission: local::AdmissionBudget,
    ) -> Result<Option<Content>, ContentError> {
        let (scheme, cid, path) = request;
        #[cfg(test)]
        self.local_lookup_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        for mirrored in [true, false] {
            admission.check()?;
            let hint = match (scheme, mirrored) {
                ("ipfs", true) => match &self.mirror {
                    Some(mirror) => mirror.probe_ipfs(cid, path).await.transpose()?,
                    None => None,
                },
                ("onchfs", true) => match &self.mirror {
                    Some(mirror) => mirror.probe_onchfs(cid, path).await.transpose()?,
                    None => None,
                },
                ("ipfs", false) => self.ipfs.probe_local(cid, path, permit.clone()).await?,
                ("onchfs", false) => self.onchfs.probe_local(cid, path, permit.clone()).await?,
                _ => return Err(ContentError::bad_request("unsupported content scheme")),
            };
            let Some(hint) = hint else { continue };
            let scratch_bytes = usize::try_from(hint.scratch_bytes)
                .map_err(|_| ContentError::unavailable("local scratch size unsupported"))?;
            // A miss reserves no body memory. Decode and compact buffers use their
            // own budget, including while waiting to admit the exact output body.
            let scratch = self
                .local_scratch
                .reserve_budget(scratch_bytes, &mut admission)
                .await?;
            // An upstream response may have populated memory during scratch admission.
            let _scratch = if self.verified.contains(&key) {
                // Cached bytes need no decode memory while waiting for delivery.
                drop(scratch);
                if let Some(content) = self
                    .memory_hit(&key, scheme, cid, path, &mut admission)
                    .await?
                {
                    return Ok(Some(content));
                }
                // An eviction raced delivery; reserve again before reading the hint.
                self.local_scratch
                    .reserve_budget(scratch_bytes, &mut admission)
                    .await?
            } else {
                scratch
            };
            admission.check()?;
            #[cfg(test)]
            if !self.local_read_delay.is_zero() {
                tokio::time::sleep(self.local_read_delay).await;
            }
            let content = match (scheme, mirrored) {
                ("ipfs", true) => self
                    .mirror
                    .as_ref()
                    .expect("probed mirror")
                    .ipfs_capped(cid, path, hint.body_bytes, hint.mirror_leaf.clone())
                    .await
                    .transpose()?,
                ("onchfs", true) => self
                    .mirror
                    .as_ref()
                    .expect("probed mirror")
                    .onchfs_capped(cid, path, hint.body_bytes)
                    .await
                    .transpose()?
                    .map(onchfs_content),
                ("ipfs", false) => {
                    self.ipfs
                        .local(cid, path, permit.clone(), hint.body_bytes)
                        .await?
                }
                ("onchfs", false) => self
                    .onchfs
                    .local(cid, path, permit.clone(), hint.body_bytes)
                    .await?
                    .map(onchfs_content),
                _ => return Err(ContentError::bad_request("unsupported content scheme")),
            };
            let Some(content) = content else { continue };
            if content.body.len() as u64 > hint.body_bytes {
                return Err(ContentError::unavailable(
                    "local object changed after probe",
                ));
            }
            admission.check()?;
            let content = self.remember(key.clone(), content, hint.cache_keys).await?;
            if let Some(delivered) = self
                .verified
                .deliver_budget(&key, &self.local_delivery, &mut admission)
                .await?
            {
                self.record_hit(mirrored);
                return Ok(Some(delivered));
            }
            // Bodies too large for memory-cache retention still have scratch coverage
            // until their exact delivery charge is acquired.
            admission.check()?;
            let content = tokio::task::spawn_blocking(move || {
                let mut content = content;
                content.body = Bytes::copy_from_slice(&content.body);
                content.delivery_accounted = false;
                content
            })
            .await
            .map_err(|_| ContentError::unavailable("local body copy failed"))?;
            let reservation = self
                .local_delivery
                .reserve_budget(content.body.len(), &mut admission)
                .await?;
            return self.finish_local(content, reservation, mirrored).map(Some);
        }
        self.counters
            .cache_misses
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(None)
    }

    /// Returns a bounded snapshot of content cache, mirror, and upstream counters.
    pub fn metrics(&self) -> ContentMetrics {
        let reads = self.local_read_wait.snapshot();
        let followers = self.local_leader_wait.snapshot();
        self.counters.snapshot(
            self.cache.is_enabled(),
            self.cache.stats().ok(),
            self.local_delivery.snapshot(),
            self.local_scratch.snapshot(),
            stats::LocalReadMetrics {
                active: (16 - self.local_reads.available_permits()) as u64,
                waiting: reads.waiting,
                wait_capacity: reads.wait_capacity,
                waits: reads.waits,
                timeouts: reads.timeouts,
                rejected: reads.rejected,
                leader_waiting: followers.waiting,
                leader_wait_capacity: followers.wait_capacity,
                leader_waits: followers.waits,
                leader_timeouts: followers.timeouts.saturating_add(
                    self.counters
                        .local_leader_deadline_expired
                        .load(std::sync::atomic::Ordering::Relaxed),
                ),
                leader_rejected: followers.rejected,
                coalesced: self
                    .counters
                    .local_coalesced
                    .load(std::sync::atomic::Ordering::Relaxed),
                coordination_rejected: self
                    .counters
                    .local_coordination_rejected
                    .load(std::sync::atomic::Ordering::Relaxed),
                deadline_expired: self
                    .counters
                    .local_deadline_expired
                    .load(std::sync::atomic::Ordering::Relaxed),
                shared_failures: self
                    .counters
                    .local_shared_failures
                    .load(std::sync::atomic::Ordering::Relaxed),
            },
            self.mirror.as_ref().map_or(0, |mirror| mirror.busy_count()),
        )
    }

    fn finish_local(
        &self,
        mut content: Content,
        mut reservation: local::BytePermit,
        mirror_hit: bool,
    ) -> Result<Content, ContentError> {
        let units = content.body.len().div_ceil(local::BYTE_UNIT).max(1);
        let permit = reservation
            .split(units)
            .ok_or_else(|| ContentError::unavailable("local body exceeds reservation"))?;
        drop(reservation);
        let permit = Arc::new(permit);
        permit.bind(&content.body)?;
        content.body = Bytes::from_owner(LocalBytes {
            bytes: content.body,
            _permit: permit,
        });
        content.delivery_accounted = true;
        self.record_hit(mirror_hit);
        Ok(content)
    }

    fn record_hit(&self, mirror_hit: bool) {
        self.counters
            .cache_hits
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if mirror_hit {
            self.counters
                .mirror_hits
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    /// Resolves `onchfs://<cid>/<path>`. `path` is the URL path after the CID,
    /// percent-encoded as a browser sends it, without query or fragment.
    pub async fn get_onchfs(&self, cid: &str, path: &str) -> Result<Content, ContentError> {
        if let Some(content) = self.get_local("onchfs", cid, path).await? {
            return Ok(content);
        }
        self.get_after_local_miss("onchfs", cid, path, false).await
    }

    /// Resolves `ipfs://<cid>/<path>`, same path rules as [`Self::get_onchfs`].
    pub async fn get_ipfs(&self, cid: &str, path: &str) -> Result<Content, ContentError> {
        if let Some(content) = self.get_local("ipfs", cid, path).await? {
            return Ok(content);
        }
        self.get_after_local_miss("ipfs", cid, path, false).await
    }

    /// The caller already performed the local lookup; avoid counting it twice.
    pub(crate) async fn get_after_local_miss(
        &self,
        scheme: &str,
        cid: &str,
        path: &str,
        passive: bool,
    ) -> Result<Content, ContentError> {
        let result = self.resolve_after_miss(scheme, cid, path, passive).await;
        if let Err(error) = &result {
            let counter = if error.kind == ErrorKind::Unavailable {
                &self.counters.upstream_failures
            } else {
                &self.counters.upstream_other_failures
            };
            counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        result
    }

    async fn resolve_after_miss(
        &self,
        scheme: &str,
        cid: &str,
        path: &str,
        passive: bool,
    ) -> Result<Content, ContentError> {
        let (content, cache_keys) = match scheme {
            "ipfs" => {
                let resolver = if passive {
                    &self.passive_ipfs
                } else {
                    &self.ipfs
                };
                (resolver.resolve(cid, path).await?, Vec::new())
            }
            "onchfs" => {
                let resolver = if passive {
                    &self.passive_onchfs
                } else {
                    &self.onchfs
                };
                let resolved = resolver.resolve(cid, path).await?;
                let keys = resolved.record_cids.clone();
                (onchfs_content(resolved), keys)
            }
            _ => return Err(ContentError::bad_request("unsupported content scheme")),
        };
        self.remember_upstream(format!("{scheme}://{cid}/{path}"), content, cache_keys)
            .await
    }

    async fn remember_upstream(
        &self,
        key: String,
        content: Content,
        cache_keys: Vec<String>,
    ) -> Result<Content, ContentError> {
        // Some IPFS outputs no longer own their assembly permit. Bound detached
        // copies independently; a busy cache never rejects an upstream result.
        if !content.verification.is_verified() {
            return Ok(content);
        }
        let Ok(slot) = self.upstream_copies.clone().try_acquire_owned() else {
            self.counters
                .retention_skipped
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return Ok(content);
        };
        // This worker retains the body and its copy, plus bounded ONCHFS record
        // metadata (<= 1 MiB). Transport/cache workers have separate limits.
        let Some(scratch) = self
            .local_scratch
            .try_reserve_bytes(content.body.len().saturating_mul(2).saturating_add(2 << 20))
        else {
            self.counters
                .retention_skipped
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return Ok(content);
        };
        let verified = self.verified.clone();
        tokio::task::spawn_blocking(move || {
            let _slot = slot;
            let _scratch = scratch;
            verified.put_with_keys(key, &content, cache_keys);
            content
        })
        .await
        .map_err(|_| ContentError::unavailable("local cache copy failed"))
    }

    async fn remember(
        &self,
        key: String,
        content: Content,
        cache_keys: Vec<String>,
    ) -> Result<Content, ContentError> {
        let verified = self.verified.clone();
        // The detached local reader holds scratch until this copy completes.
        tokio::task::spawn_blocking(move || {
            verified.put_with_keys(key, &content, cache_keys);
            content
        })
        .await
        .map_err(|_| ContentError::unavailable("local cache copy failed"))
    }

    /// Passive media gets no persistent write budget. Existing cache/mirror hits
    /// remain usable; upstream object bytes and concurrency use the normal limits.
    pub async fn get_passive(
        &self,
        scheme: &str,
        cid: &str,
        path: &str,
    ) -> Result<Content, ContentError> {
        if let Some(content) = self.get_local(scheme, cid, path).await? {
            return Ok(content);
        }
        self.get_after_local_miss(scheme, cid, path, true).await
    }

    /// Copies the complete package under `uri` (`ipfs://<cid>` or `onchfs://<cid>`) into
    /// `writer`'s mirror. Only verified objects are stored, and only once every file of the
    /// package verified.
    pub async fn mirror_package(
        &self,
        uri: &str,
        writer: &Arc<MirrorWriter>,
    ) -> Result<PackageStats, PackageError> {
        mirror::package(&self.ipfs, &self.onchfs, writer, uri).await
    }

    /// Charged cache bytes, including allocation rounding and metadata reserve.
    pub fn cache_disk_usage(&self) -> std::io::Result<u64> {
        self.cache.disk_usage()
    }

    pub async fn check_cache(&self) -> std::io::Result<()> {
        if self.cache.is_enabled() {
            self.cache.check_writable().await
        } else {
            Ok(())
        }
    }

    pub fn cache_enabled(&self) -> bool {
        self.cache.is_enabled()
    }
}

/// Resolve a path without creating it, canonicalizing its nearest existing
/// ancestor so symlink aliases are caught before the cache can evict mirror data.
fn resolved_path(path: &std::path::Path) -> std::io::Result<PathBuf> {
    use std::path::Component;

    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    let mut resolved = PathBuf::new();
    let mut missing_base: Option<PathBuf> = None;
    for component in absolute.components() {
        match component {
            Component::Prefix(prefix) => resolved.push(prefix.as_os_str()),
            Component::RootDir => resolved.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                resolved.pop();
                if missing_base.as_ref().is_some_and(|base| base == &resolved) {
                    missing_base = None;
                }
            }
            Component::Normal(part) => {
                if missing_base.is_some() {
                    resolved.push(part);
                    continue;
                }
                let candidate = resolved.join(part);
                match std::fs::canonicalize(&candidate) {
                    Ok(canonical) => resolved = canonical,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        // A dangling symlink must fail closed rather than be treated as a
                        // missing directory that can later be created under another target.
                        match std::fs::symlink_metadata(&candidate) {
                            Ok(_) => return Err(error),
                            Err(metadata_error)
                                if metadata_error.kind() == std::io::ErrorKind::NotFound => {}
                            Err(metadata_error) => return Err(metadata_error),
                        }
                        if !resolved.is_dir() {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::NotADirectory,
                                "existing path ancestor is not a directory",
                            ));
                        }
                        missing_base = Some(resolved.clone());
                        resolved.push(part);
                    }
                    Err(error) => return Err(error),
                }
            }
        }
    }
    Ok(resolved)
}

#[cfg(test)]
mod local_tests {
    use super::*;
    use sha2::Digest as _;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[tokio::test]
    async fn metrics_count_successful_local_hits_and_completed_misses() {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(format!(
            "tmp/content-metrics-local-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let store = ContentStore::new(ContentConfig::mainnet(&path)).unwrap();
        let body = Bytes::from_static(b"verified content");
        let cid = cid::Cid::new_v1(
            0x55,
            cid::multihash::Multihash::wrap(0x12, &sha2::Sha256::digest(&body)).unwrap(),
        )
        .to_string();
        let content = Content {
            delivery_accounted: false,
            body,
            content_type: None,
            content_encoding: None,
            verification: Verification::IpfsCar,
            directory_index: false,
            from_cache: false,
            etag: "etag".into(),
        };
        // A true local miss remains usable when slow deliveries occupy the whole pool.
        let held = store
            .local_delivery
            .reserve(512 << 20, store.local_delivery.deadline())
            .await
            .unwrap();
        assert!(store.get_local("ipfs", &cid, "").await.unwrap().is_none());
        assert_eq!(store.metrics().local_delivery.waits, 0);
        drop(held);
        store.verified.put(format!("ipfs://{cid}/"), &content);
        assert!(store.get_local("ipfs", &cid, "").await.unwrap().is_some());

        let metrics = store.metrics();
        assert_eq!(metrics.cache.hits, 1);
        assert_eq!(metrics.cache.misses, 1);
        assert_eq!(metrics.mirror.hits, 0);
        let snapshot = serde_json::to_string(&metrics).unwrap();
        assert!(!snapshot.contains(&cid));
        drop(store);
        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn cache_directory_may_not_overlap_preservation_mirror() {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(format!(
            "tmp/content-metrics-overlap-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let mirror = root.join("mirror");
        std::fs::create_dir_all(&mirror).unwrap();
        let mut config = ContentConfig::mainnet(&root);
        config.mirror_dir = Some(mirror.clone());
        let error = ContentStore::new(config).err().unwrap();
        assert_eq!(error.kind, ErrorKind::BadRequest);
        assert!(!root.join("tmp").exists());
        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;
            let child = mirror.join("child");
            std::fs::create_dir(&child).unwrap();
            let alias = root.join("mirror-alias");
            symlink(&child, &alias).unwrap();
            let cache = alias.join("..").join("cache");
            let mut config = ContentConfig::mainnet(cache.clone());
            config.mirror_dir = Some(mirror.clone());
            assert_eq!(
                ContentStore::new(config).err().unwrap().kind,
                ErrorKind::BadRequest
            );
            assert!(!cache.exists());
        }
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn retained_local_bytes_are_bounded_until_the_last_clone_drops_and_hot_hits_skip_readers()
    {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(format!(
            "tmp/local-bounds-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let mut store = ContentStore::new(ContentConfig::mainnet(&path)).unwrap();
        store.local_delivery = Arc::new(local::Delivery::new(2, 2, Duration::from_millis(20)));
        let content = Content {
            delivery_accounted: false,
            body: Bytes::from_static(b"verified"),
            content_type: None,
            content_encoding: None,
            verification: Verification::IpfsCar,
            directory_index: false,
            from_cache: true,
            etag: "etag".into(),
        };
        let reservation = store
            .local_delivery
            .reserve(8, store.local_delivery.deadline())
            .await
            .unwrap();
        let first = store
            .finish_local(content.clone(), reservation, false)
            .unwrap();
        let last = first.body.clone();
        let reservation = store
            .local_delivery
            .reserve(8, store.local_delivery.deadline())
            .await
            .unwrap();
        let second = store
            .finish_local(
                Content {
                    // Each uncached read owns a distinct compact allocation.
                    body: Bytes::copy_from_slice(&content.body),
                    ..content.clone()
                },
                reservation,
                false,
            )
            .unwrap();
        assert_eq!(
            store
                .local_delivery
                .reserve(8, store.local_delivery.deadline())
                .await
                .unwrap_err()
                .kind,
            ErrorKind::Unavailable
        );
        drop(first);
        assert_eq!(
            store.local_delivery.snapshot().bytes_used,
            2 * local::BYTE_UNIT as u64
        );
        drop(last);
        assert_eq!(
            store.local_delivery.snapshot().bytes_used,
            local::BYTE_UNIT as u64
        );
        drop(second);
        assert_eq!(store.local_delivery.snapshot().bytes_used, 0);
        let cid = hex::encode(onchfs::file_cid(&onchfs::keccak(&content.body), &[]));
        store.verified.put(format!("onchfs://{cid}/"), &content);
        store
            .verified
            .put(format!("onchfs://{cid}/child/"), &content);
        assert_eq!(
            store
                .get_local("onchfs", &format!("{cid}/child"), "")
                .await
                .unwrap_err()
                .kind,
            ErrorKind::Invalid
        );
        assert!(store.get_local("onchfs", &cid, "../child").await.is_err());
        let busy = store
            .local_reads
            .clone()
            .acquire_many_owned(16)
            .await
            .unwrap();
        let hit = tokio::time::timeout(Duration::from_secs(1), store.get_local("onchfs", &cid, ""))
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(hit.body, content.body);
        drop(hit);
        assert_eq!(store.local_delivery.snapshot().bytes_used, 0);
        drop((busy, store));
        std::fs::remove_dir_all(path).unwrap();
    }
    #[tokio::test]
    async fn cold_read_cancellation_keeps_scratch_work_bounded_until_completion() {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(format!(
            "tmp/local-cold-wait-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let cid = cid::Cid::new_v1(
            0x55,
            cid::multihash::Multihash::wrap(0x12, &sha2::Sha256::digest(b"cold")).unwrap(),
        );
        let key = hex::encode(cid.to_bytes());
        let object = root
            .join("mirror/ipfs")
            .join(&key[key.len() - 2..])
            .join(&key);
        std::fs::create_dir_all(object.parent().unwrap()).unwrap();
        std::fs::write(&object, b"cold").unwrap();
        let mut config = ContentConfig::mainnet(root.join("cache"));
        config.max_object_bytes = (2 * local::BYTE_UNIT) as u64;
        config.mirror_dir = Some(root.join("mirror"));
        let mut store = ContentStore::new(config).unwrap();
        store.local_scratch = Arc::new(local::Delivery::new(
            (512 << 20) / local::BYTE_UNIT,
            256,
            Duration::from_secs(30),
        ));
        let held = store
            .local_scratch
            .reserve(512 << 20, store.local_scratch.deadline())
            .await
            .unwrap();
        let cid = cid.to_string();
        let reader = store.clone();
        let request_cid = cid.clone();
        let request = tokio::spawn(async move { reader.get_local("ipfs", &request_cid, "").await });
        tokio::time::timeout(Duration::from_secs(5), async {
            while store.metrics().local_scratch.waiting != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(store.metrics().local_delivery.bytes_used, 0);
        assert_eq!(store.metrics().mirror.hits, 0);
        assert_eq!(store.local_reads.available_permits(), 15);
        request.abort();
        assert!(request.await.unwrap_err().is_cancelled());
        assert_eq!(store.local_reads.available_permits(), 15);
        std::fs::remove_file(&object).unwrap();
        drop(held);
        tokio::time::timeout(Duration::from_secs(5), async {
            while store.local_reads.available_permits() != 16 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(store.metrics().cache.misses, 1);
        assert_eq!(store.metrics().local_scratch.bytes_used, 0);
        assert_eq!(store.metrics().local_scratch.waiting, 0);
        std::fs::write(&object, b"cold").unwrap();
        // Cold tiny files and shared hot hits still work with 450 MiB delivered.
        let held = store
            .local_delivery
            .reserve(450 << 20, store.local_delivery.deadline())
            .await
            .unwrap();
        let recovered = store.get_local("ipfs", &cid, "").await.unwrap().unwrap();
        assert_eq!(recovered.body, "cold");
        let hot = store.get_local("ipfs", &cid, "").await.unwrap().unwrap();
        assert_eq!(
            store.metrics().local_delivery.bytes_used,
            (450 << 20) + local::BYTE_UNIT as u64
        );
        assert_eq!(store.metrics().local_scratch.bytes_used, 0);
        drop(recovered);
        assert_eq!(
            store.metrics().local_delivery.bytes_used,
            (450 << 20) + local::BYTE_UNIT as u64
        );
        drop(hot);
        drop(held);
        assert_eq!(store.metrics().local_delivery.bytes_used, 0);
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn scratch_pool_fits_the_largest_supported_cached_decode() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join(format!("tmp/local-largest-scratch-{}", std::process::id()));
        let mut config = ContentConfig::mainnet(&root);
        config.max_object_bytes = 256 << 20;
        let expected =
            car::transport_cap(config.max_object_bytes) + 2 * config.max_object_bytes + (8 << 20);
        let store = ContentStore::new(config).unwrap();
        assert!(store.metrics().local_scratch.bytes_quota >= expected);
        assert_eq!(store.metrics().local_delivery.bytes_quota, 512 << 20);
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn local_reader_wait_timeout_is_counted_separately_from_byte_admission() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join(format!("tmp/local-reader-counter-{}", std::process::id()));
        let mut store = ContentStore::new(ContentConfig::mainnet(&root)).unwrap();
        store.local_delivery = Arc::new(local::Delivery::new(
            (512 << 20) / local::BYTE_UNIT,
            256,
            Duration::from_millis(20),
        ));
        let held = store
            .local_reads
            .clone()
            .acquire_many_owned(16)
            .await
            .unwrap();
        let cid = hex::encode([1; 32]);
        assert!(store.get_local("onchfs", &cid, "").await.is_err());
        let metrics = store.metrics();
        assert_eq!(metrics.local_reads.timeouts, 1);
        assert_eq!(metrics.local_reads.waiting, 0);
        assert_eq!(metrics.local_delivery.timeouts, 0);
        assert_eq!(metrics.local_scratch.waits, 0);
        assert_eq!(metrics.resolution.local_failures, 1);
        drop(held);
        assert!(store.get_local("onchfs", &cid, "").await.unwrap().is_none());
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn admitted_mirror_read_longer_than_two_seconds_still_succeeds() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join(format!("tmp/local-slow-admitted-{}", std::process::id()));
        let cid = cid::Cid::new_v1(
            0x55,
            cid::multihash::Multihash::wrap(0x12, &sha2::Sha256::digest(b"slow mirror")).unwrap(),
        );
        let key = hex::encode(cid.to_bytes());
        let object = root
            .join("mirror/ipfs")
            .join(&key[key.len() - 2..])
            .join(key);
        std::fs::create_dir_all(object.parent().unwrap()).unwrap();
        std::fs::write(&object, b"slow mirror").unwrap();
        let mut config = ContentConfig::mainnet(root.join("cache"));
        config.mirror_dir = Some(root.join("mirror"));
        let mut store = ContentStore::new(config).unwrap();
        store.local_read_delay = Duration::from_millis(2500);
        let content = store
            .get_local("ipfs", &cid.to_string(), "")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(content.body, "slow mirror");
        assert!(content.delivery_accounted);
        let metrics = store.metrics();
        assert_eq!(metrics.local_reads.timeouts, 0);
        assert_eq!(metrics.local_reads.deadline_expired, 0);
        assert_eq!(metrics.local_scratch.timeouts, 0);
        assert_eq!(metrics.local_delivery.timeouts, 0);
        assert_eq!(metrics.local_scratch.bytes_used, 0);
        assert_eq!(metrics.resolution.local_failures, 0);
        drop(content);
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn exhausted_admission_starts_no_local_work_and_counts_once() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(format!(
            "tmp/local-expired-admission-{}",
            std::process::id()
        ));
        let mut store = ContentStore::new(ContentConfig::mainnet(&root)).unwrap();
        store.local_delivery = Arc::new(local::Delivery::new(1, 1, Duration::ZERO));
        let error = store
            .get_local("onchfs", &hex::encode([1; 32]), "")
            .await
            .unwrap_err();
        assert!(local::AdmissionBudget::exhausted(&error));
        let metrics = store.metrics();
        assert_eq!(metrics.local_reads.deadline_expired, 1);
        assert_eq!(metrics.local_reads.active, 0);
        assert_eq!(metrics.local_reads.waits, 0);
        assert_eq!(metrics.local_reads.timeouts, 0);
        assert_eq!(metrics.local_scratch.waits, 0);
        assert_eq!(metrics.local_scratch.timeouts, 0);
        assert_eq!(metrics.local_delivery.waits, 0);
        assert_eq!(metrics.local_delivery.timeouts, 0);
        assert_eq!(metrics.resolution.local_failures, 1);
        assert_eq!(metrics.cache.misses, 0);
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn cancelled_upstream_copy_keeps_scratch_and_worker_capacity_until_done() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join(format!("tmp/upstream-copy-bounds-{}", std::process::id()));
        let store = ContentStore::new(ContentConfig::mainnet(&root)).unwrap();
        let verified = store.verified.clone();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let held = tokio::task::spawn_blocking(move || {
            verified.holding_lock(|| {
                started_tx.send(()).unwrap();
                release_rx.recv().unwrap();
            })
        });
        started_rx.await.unwrap();
        let content = Content {
            delivery_accounted: false,
            body: Bytes::from_static(b"upstream"),
            content_type: None,
            content_encoding: None,
            verification: Verification::IpfsCar,
            directory_index: false,
            from_cache: false,
            etag: "etag".into(),
        };
        let reader = store.clone();
        let request = tokio::spawn(async move {
            reader
                .remember_upstream("key".into(), content, Vec::new())
                .await
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            while store.upstream_copies.available_permits() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let charged = store.local_scratch.snapshot().bytes_used;
        assert_eq!(charged, (2 << 20) + local::BYTE_UNIT as u64);
        request.abort();
        assert!(request.await.unwrap_err().is_cancelled());
        assert_eq!(store.upstream_copies.available_permits(), 1);
        assert_eq!(store.local_scratch.snapshot().bytes_used, charged);
        release_tx.send(()).unwrap();
        held.await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while store.upstream_copies.available_permits() != 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(store.local_scratch.snapshot().bytes_used, 0);
        assert_eq!(store.verified.get("key").unwrap().body, "upstream");
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn upstream_retention_skips_full_scratch_or_copy_pool_without_failing() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join(format!("tmp/upstream-copy-skip-{}", std::process::id()));
        let store = ContentStore::new(ContentConfig::mainnet(&root)).unwrap();
        let content = Content {
            delivery_accounted: false,
            body: Bytes::from_static(b"upstream"),
            content_type: None,
            content_encoding: None,
            verification: Verification::IpfsCar,
            directory_index: false,
            from_cache: false,
            etag: "etag".into(),
        };
        let held = store
            .upstream_copies
            .clone()
            .acquire_many_owned(2)
            .await
            .unwrap();
        assert_eq!(
            store
                .remember_upstream("worker-full".into(), content.clone(), Vec::new())
                .await
                .unwrap()
                .body,
            "upstream"
        );
        assert!(store.verified.get("worker-full").is_none());
        assert_eq!(store.metrics().cache.retention_skipped, 1);
        drop(held);
        let held = store.local_scratch.try_reserve_bytes(512 << 20).unwrap();
        assert_eq!(
            store
                .remember_upstream("scratch-full".into(), content.clone(), Vec::new())
                .await
                .unwrap()
                .body,
            "upstream"
        );
        assert!(store.verified.get("scratch-full").is_none());
        assert_eq!(store.metrics().cache.retention_skipped, 2);
        assert_eq!(store.upstream_copies.available_permits(), 2);
        drop(held);
        store
            .remember_upstream("retry".into(), content, Vec::new())
            .await
            .unwrap();
        assert_eq!(store.verified.get("retry").unwrap().body, "upstream");
        assert_eq!(store.metrics().cache.retention_skipped, 2);
        assert_eq!(store.local_scratch.snapshot().bytes_used, 0);
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }

    fn cold_fixture(name: &str) -> (PathBuf, String, ContentStore) {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join(format!("tmp/{name}-{}", std::process::id()));
        let cid = cid::Cid::new_v1(
            0x55,
            cid::multihash::Multihash::wrap(0x12, &sha2::Sha256::digest(b"cold")).unwrap(),
        );
        let key = hex::encode(cid.to_bytes());
        let object = root
            .join("mirror/ipfs")
            .join(&key[key.len() - 2..])
            .join(key);
        std::fs::create_dir_all(object.parent().unwrap()).unwrap();
        std::fs::write(object, b"cold").unwrap();
        let mut config = ContentConfig::mainnet(root.join("cache"));
        config.mirror_dir = Some(root.join("mirror"));
        (root, cid.to_string(), ContentStore::new(config).unwrap())
    }

    async fn wait_for(mut ready: impl FnMut() -> bool) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while !ready() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn concurrent_cold_reads_share_work_even_when_the_first_caller_cancels() {
        let (root, cid, store) = cold_fixture("coalesced-cold");
        // Only one probed read's scratch fits. Followers must not reserve it.
        let mut store = store;
        let hint = store
            .mirror
            .as_ref()
            .unwrap()
            .probe_ipfs(&cid, "")
            .await
            .unwrap()
            .unwrap();
        let units = (hint.scratch_bytes as usize).div_ceil(local::BYTE_UNIT);
        store.local_scratch = Arc::new(local::Delivery::new(units, 256, Duration::from_secs(2)));
        let held = store
            .local_scratch
            .try_reserve_bytes(units * local::BYTE_UNIT)
            .unwrap();
        let reader = store.clone();
        let key = cid.clone();
        let first = tokio::spawn(async move { reader.get_local("ipfs", &key, "").await });
        wait_for(|| store.metrics().local_scratch.waiting == 1).await;
        let reader = store.clone();
        let key = cid.clone();
        let second = tokio::spawn(async move { reader.get_local("ipfs", &key, "").await });
        wait_for(|| store.metrics().local_reads.leader_waiting == 1).await;
        assert_eq!(store.metrics().local_reads.active, 1);
        assert_eq!(store.metrics().local_scratch.waiting, 1);
        assert_eq!(store.metrics().local_reads.coalesced, 1);
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        // The detached leader retains its registration until its disk work ends.
        assert_eq!(store.metrics().local_reads.active, 1);
        drop(held);
        let delivered = second.await.unwrap().unwrap().unwrap();
        assert_eq!(delivered.body, "cold");
        assert_eq!(store.metrics().mirror.hits, 1);
        assert_eq!(store.metrics().cache.hits, 2);
        assert_eq!(store.metrics().local_reads.active, 0);
        assert_eq!(store.metrics().local_scratch.bytes_used, 0);
        assert!(
            !store
                .local_flights
                .register(format!("ipfs://{cid}/"))
                .unwrap()
                .joined
        );
        drop((delivered, store));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn scratch_waiter_uses_memory_populated_by_another_resolver() {
        let (root, cid, store) = cold_fixture("scratch-memory-recheck");
        let held = store.local_scratch.try_reserve_bytes(512 << 20).unwrap();
        let reader = store.clone();
        let request_cid = cid.clone();
        let request = tokio::spawn(async move { reader.get_local("ipfs", &request_cid, "").await });
        wait_for(|| store.metrics().local_scratch.waiting == 1).await;
        let content = ipfs::content(
            Bytes::from_static(b"cold"),
            None,
            None,
            Verification::IpfsCar,
            false,
            false,
        );
        store.verified.put(format!("ipfs://{cid}/"), &content);
        let delivery = store.local_delivery.try_reserve_bytes(512 << 20).unwrap();
        drop(held);
        wait_for(|| store.metrics().local_delivery.waiting == 1).await;
        assert_eq!(store.metrics().local_scratch.bytes_used, 0);
        drop(delivery);
        let result = request.await.unwrap().unwrap().unwrap();
        assert_eq!(result.body, "cold");
        assert_eq!(store.metrics().cache.hits, 1);
        assert_eq!(store.metrics().mirror.hits, 0);
        assert_eq!(store.metrics().local_scratch.bytes_used, 0);
        drop((result, store));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn failed_cold_flight_does_not_cache_a_transient_miss() {
        let (root, cid, store) = cold_fixture("cold-flight-retry");
        let key = hex::encode(car::parse_cid(&cid).unwrap().to_bytes());
        let object = root
            .join("mirror/ipfs")
            .join(&key[key.len() - 2..])
            .join(key);
        let held = store.local_scratch.try_reserve_bytes(512 << 20).unwrap();
        let reader = store.clone();
        let key = cid.clone();
        let first = tokio::spawn(async move { reader.get_local("ipfs", &key, "").await });
        wait_for(|| store.metrics().local_scratch.waiting == 1).await;
        let reader = store.clone();
        let key = cid.clone();
        let second = tokio::spawn(async move { reader.get_local("ipfs", &key, "").await });
        wait_for(|| store.metrics().local_reads.leader_waiting == 1).await;
        std::fs::remove_file(&object).unwrap();
        drop(held);
        assert!(first.await.unwrap().unwrap().is_none());
        assert!(second.await.unwrap().unwrap().is_none());
        assert_eq!(store.metrics().cache.misses, 2);
        std::fs::write(object, b"cold").unwrap();
        assert_eq!(
            store
                .get_local("ipfs", &cid, "")
                .await
                .unwrap()
                .unwrap()
                .body,
            "cold"
        );
        assert_eq!(store.metrics().mirror.hits, 1);
        assert_eq!(store.metrics().local_reads.active, 0);
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn cold_flight_registry_exhaustion_is_counted_and_recovers() {
        let (root, cid, store) = cold_fixture("cold-flight-cap");
        let held: Vec<_> = (0..272)
            .map(|i| store.local_flights.register(format!("held/{i}")).unwrap())
            .collect();
        assert_eq!(
            store.get_local("ipfs", &cid, "").await.unwrap_err().kind,
            ErrorKind::Unavailable
        );
        assert_eq!(store.metrics().local_reads.coordination_rejected, 1);
        assert_eq!(store.metrics().resolution.local_failures, 1);
        assert_eq!(store.metrics().local_reads.active, 0);
        drop(held);
        assert!(store.get_local("ipfs", &cid, "").await.unwrap().is_some());
        assert_eq!(store.metrics().local_reads.coordination_rejected, 1);
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn small_upstream_body_retains_with_small_scratch_at_maximum_object_cap() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join(format!("tmp/tiny-retention-{}", std::process::id()));
        let mut cfg = ContentConfig::mainnet(&root);
        cfg.max_object_bytes = 256 << 20;
        let mut store = ContentStore::new(cfg).unwrap();
        store.local_scratch = Arc::new(local::Delivery::new(
            (4 << 20) / local::BYTE_UNIT,
            1,
            Duration::from_secs(1),
        ));
        let content = ipfs::content(
            Bytes::from_static(b"tiny"),
            None,
            None,
            Verification::IpfsCar,
            false,
            false,
        );
        let returned = store
            .remember_upstream("tiny".into(), content, Vec::new())
            .await
            .unwrap();
        assert_eq!(returned.body, "tiny");
        assert_eq!(store.verified.get("tiny").unwrap().body, "tiny");
        assert_eq!(store.metrics().cache.retention_skipped, 0);
        assert_eq!(store.metrics().local_scratch.bytes_used, 0);
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn expired_reader_admission_releases_permit_before_any_disk_work() {
        let (root, cid, mut store) = cold_fixture("expired-reader-stage");
        store.local_delivery = Arc::new(local::Delivery::new(1, 256, Duration::from_millis(1)));
        let held = store
            .local_reads
            .clone()
            .acquire_many_owned(16)
            .await
            .unwrap();
        let mut request = Box::pin(store.get_local("ipfs", &cid, ""));
        std::future::poll_fn(|cx| {
            assert!(std::future::Future::poll(request.as_mut(), cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        // Let the permit become ready while the current-thread runtime cannot poll.
        std::thread::sleep(Duration::from_millis(10));
        drop(held);
        assert!(local::AdmissionBudget::exhausted(
            &request.await.unwrap_err()
        ));
        let metrics = store.metrics();
        assert_eq!(metrics.local_reads.deadline_expired, 1);
        assert_eq!(metrics.local_reads.timeouts, 0);
        assert_eq!(metrics.local_reads.active, 0);
        assert_eq!(metrics.local_scratch.bytes_used, 0);
        assert_eq!(metrics.mirror.hits, 0);
        assert!(
            !store
                .local_flights
                .register(format!("ipfs://{cid}/"))
                .unwrap()
                .joined
        );
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn cancelled_follower_releases_queue_capacity_without_starting_a_reader() {
        let (root, cid, store) = cold_fixture("cancelled-cold-follower");
        let key = format!("ipfs://{cid}/");
        let leader = store.local_flights.register(key.clone()).unwrap();
        let turn = leader.turn().acquire_owned().await.unwrap();
        let mut follower = Box::pin(store.get_local("ipfs", &cid, ""));
        std::future::poll_fn(|cx| {
            assert!(std::future::Future::poll(follower.as_mut(), cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        assert_eq!(store.metrics().local_reads.leader_waiting, 1);
        assert_eq!(store.metrics().local_reads.active, 0);
        drop(follower);
        assert_eq!(store.metrics().local_reads.leader_waiting, 0);
        drop((turn, leader));
        assert!(!store.local_flights.register(key).unwrap().joined);
        assert!(store.get_local("ipfs", &cid, "").await.unwrap().is_some());
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn missing_leader_shares_one_lookup_without_follower_reader_admission() {
        let (root, cid, store) = cold_fixture("cold-missing-cohort");
        let raw_key = hex::encode(car::parse_cid(&cid).unwrap().to_bytes());
        let object = root
            .join("mirror/ipfs")
            .join(&raw_key[raw_key.len() - 2..])
            .join(raw_key);
        std::fs::remove_file(&object).unwrap();
        let held = store
            .local_reads
            .clone()
            .acquire_many_owned(16)
            .await
            .unwrap();
        let mut requests = Vec::new();
        for index in 0..20 {
            let reader = store.clone();
            let cid = cid.clone();
            requests.push(tokio::spawn(async move {
                reader.get_local("ipfs", &cid, "").await
            }));
            wait_for(|| store.metrics().local_reads.coalesced == index).await;
        }
        wait_for(|| store.metrics().local_reads.leader_waiting == 19).await;
        assert_eq!(store.metrics().local_reads.waiting, 1);
        drop(held);
        for request in requests {
            assert!(request.await.unwrap().unwrap().is_none());
        }
        assert_eq!(
            store
                .local_lookup_count
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
        assert_eq!(store.metrics().local_reads.waits, 1);
        assert_eq!(store.metrics().cache.misses, 20);
        assert_eq!(store.metrics().local_reads.active, 0);
        assert_eq!(store.metrics().local_reads.leader_waiting, 0);
        // A later request starts a new generation instead of reusing the miss.
        std::fs::write(object, b"cold").unwrap();
        assert_eq!(
            store
                .get_local("ipfs", &cid, "")
                .await
                .unwrap()
                .unwrap()
                .body,
            "cold"
        );
        assert_eq!(
            store
                .local_lookup_count
                .load(std::sync::atomic::Ordering::Relaxed),
            2
        );
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn leader_cancelled_before_spawn_leaves_one_follower_to_retry() {
        let (root, cid, store) = cold_fixture("cold-pre-spawn-cancel");
        let held = store
            .local_reads
            .clone()
            .acquire_many_owned(16)
            .await
            .unwrap();
        let reader = store.clone();
        let key = cid.clone();
        let first = tokio::spawn(async move { reader.get_local("ipfs", &key, "").await });
        wait_for(|| store.metrics().local_reads.waiting == 1).await;
        let mut followers = Vec::new();
        for _ in 0..10 {
            let reader = store.clone();
            let key = cid.clone();
            followers.push(tokio::spawn(async move {
                reader.get_local("ipfs", &key, "").await
            }));
        }
        wait_for(|| store.metrics().local_reads.leader_waiting == 10).await;
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        wait_for(|| {
            store.metrics().local_reads.leader_waiting == 9
                && store.metrics().local_reads.waiting == 1
        })
        .await;
        assert_eq!(
            store
                .local_lookup_count
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );
        drop(held);
        for follower in followers {
            assert_eq!(follower.await.unwrap().unwrap().unwrap().body, "cold");
        }
        assert_eq!(
            store
                .local_lookup_count
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
        assert_eq!(store.metrics().mirror.hits, 1);
        assert_eq!(store.metrics().local_reads.waits, 2);
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn follower_wait_excludes_slow_disk_from_its_two_second_admission_budget() {
        let (root, cid, mut store) = cold_fixture("cold-slow-cohort");
        store.local_read_delay = Duration::from_millis(2200);
        let reader = store.clone();
        let key = cid.clone();
        let leader = tokio::spawn(async move { reader.get_local("ipfs", &key, "").await });
        wait_for(|| store.metrics().local_scratch.bytes_used > 0).await;
        let reader = store.clone();
        let key = cid.clone();
        let follower = tokio::spawn(async move { reader.get_local("ipfs", &key, "").await });
        wait_for(|| store.metrics().local_reads.leader_waiting == 1).await;
        let first = leader.await.unwrap().unwrap().unwrap();
        let second = follower.await.unwrap().unwrap().unwrap();
        assert_eq!(first.body, second.body);
        assert_eq!(first.body.as_ptr(), second.body.as_ptr());
        let reads = store.metrics().local_reads;
        assert_eq!(reads.timeouts, 0);
        assert_eq!(reads.deadline_expired, 0);
        assert_eq!(reads.leader_waits, 1);
        assert_eq!(reads.leader_timeouts, 0);
        assert_eq!(store.metrics().mirror.hits, 1);
        drop((first, second, store));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn follower_wait_has_distinct_saturation_timeout_and_cancellation_metrics() {
        let (root, cid, mut store) = cold_fixture("cold-follower-bounds");
        store.local_leader_wait = Arc::new(local::Delivery::new(0, 1, Duration::from_millis(20)));
        let leader = store
            .local_flights
            .register(format!("ipfs://{cid}/"))
            .unwrap();
        let turn = leader.turn().acquire_owned().await.unwrap();
        let mut first = Box::pin(store.get_local("ipfs", &cid, ""));
        std::future::poll_fn(|cx| {
            assert!(std::future::Future::poll(first.as_mut(), cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        assert!(store.get_local("ipfs", &cid, "").await.is_err());
        assert_eq!(store.metrics().local_reads.leader_rejected, 1);
        assert!(first.await.is_err());
        assert_eq!(store.metrics().local_reads.leader_timeouts, 1);
        let mut cancelled = Box::pin(store.get_local("ipfs", &cid, ""));
        std::future::poll_fn(|cx| {
            assert!(std::future::Future::poll(cancelled.as_mut(), cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        drop(cancelled);
        let reads = store.metrics().local_reads;
        assert_eq!(reads.leader_waiting, 0);
        assert_eq!(reads.leader_wait_capacity, 1);
        assert_eq!(reads.waits, 0);
        assert_eq!(reads.timeouts, 0);
        assert_eq!(reads.rejected, 0);
        assert_eq!(reads.active, 0);
        assert_eq!(store.metrics().local_scratch.bytes_used, 0);
        drop((turn, leader));
        assert!(store.get_local("ipfs", &cid, "").await.unwrap().is_some());
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn failed_leader_allows_one_follower_retry_without_reusing_the_error() {
        let (root, cid, store) = cold_fixture("cold-failed-leader-cohort");
        let mut failing_reader = store.clone();
        // Only the first caller's reader pool is unavailable. Its followers have
        // fresh admission budgets but share the same lookup generation.
        failing_reader.local_reads = Arc::new(tokio::sync::Semaphore::new(0));
        let failed_pool = failing_reader.local_reads.clone();
        let key = cid.clone();
        let first = tokio::spawn(async move { failing_reader.get_local("ipfs", &key, "").await });
        wait_for(|| store.metrics().local_reads.waiting == 1).await;
        let mut followers = Vec::new();
        for _ in 0..10 {
            let reader = store.clone();
            let key = cid.clone();
            followers.push(tokio::spawn(async move {
                reader.get_local("ipfs", &key, "").await
            }));
        }
        wait_for(|| store.metrics().local_reads.leader_waiting == 10).await;
        failed_pool.close();
        assert_eq!(
            first.await.unwrap().unwrap_err().kind,
            ErrorKind::Unavailable
        );
        for follower in followers {
            assert_eq!(follower.await.unwrap().unwrap().unwrap().body, "cold");
        }
        assert_eq!(
            store
                .local_lookup_count
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
        assert_eq!(store.metrics().local_reads.timeouts, 0);
        assert_eq!(store.metrics().local_reads.leader_timeouts, 0);
        assert_eq!(store.metrics().mirror.hits, 1);
        assert_eq!(store.metrics().local_reads.active, 0);
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test(start_paused = true)]
    async fn coalesced_unretained_body_keeps_one_charge_until_its_last_clone_drops() {
        use crate::delivery_control::{CLIENT, Client, Connection};
        let (root, cid, mut store) = cold_fixture("cold-unretained-cohort");
        store.local_delivery = Arc::new(local::Delivery::new(1, 32, Duration::from_secs(2)));
        let key = format!("ipfs://{cid}/");
        let leader = store.local_flights.register(key.clone()).unwrap();
        let turn = leader.turn().acquire_owned().await.unwrap();
        let mut requests = Vec::new();
        let mut clients = Vec::new();
        for n in 0..10 {
            let reader = store.clone();
            let cid = cid.clone();
            let client = Client::new(
                format!("2001:db8:1:{n}::1").parse().unwrap(),
                Connection::new(),
            );
            client.connection.enable_reclamation();
            client.ready();
            clients.push(client.clone());
            requests.push(tokio::spawn(async move {
                CLIENT
                    .scope(Some(client), reader.get_local("ipfs", &cid, ""))
                    .await
            }));
        }
        wait_for(|| store.metrics().local_reads.leader_waiting == 10).await;
        // Publish the same charged result used by read_local's unretained path.
        let reservation = store.local_delivery.try_reserve_bytes(4).unwrap();
        let delivered = store
            .finish_local(
                ipfs::content(
                    Bytes::from_static(b"cold"),
                    None,
                    None,
                    Verification::IpfsCar,
                    false,
                    true,
                ),
                reservation,
                true,
            )
            .unwrap();
        leader.complete(&Some(delivered));
        drop(turn);
        let mut bodies = Vec::new();
        for request in requests {
            bodies.push(request.await.unwrap().unwrap().unwrap());
        }
        assert!(!store.verified.contains(&key));
        assert_eq!(
            store
                .local_lookup_count
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );
        assert_eq!(store.metrics().local_reads.waits, 0);
        assert!(
            bodies.iter().all(
                |body| body.delivery_accounted && body.body.as_ptr() == bodies[0].body.as_ptr()
            )
        );
        assert_eq!(
            store.metrics().local_delivery.bytes_used,
            local::BYTE_UNIT as u64
        );
        tokio::time::advance(Duration::from_secs(1)).await;
        clients[0].connection.wrote(64 * 1024);
        let mut waiting = Box::pin(
            store
                .local_delivery
                .reserve(local::BYTE_UNIT, store.local_delivery.deadline()),
        );
        std::future::poll_fn(|cx| {
            assert!(std::future::Future::poll(waiting.as_mut(), cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        let cx = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(
            clients
                .iter()
                .all(|client| !client.connection.poll_aborted(&cx))
        );
        drop(waiting);
        drop(bodies.remove(0));
        tokio::time::advance(crate::delivery_control::PRESSURE_TICK).await;
        let mut waiting = Box::pin(
            store
                .local_delivery
                .reserve(local::BYTE_UNIT, store.local_delivery.deadline()),
        );
        std::future::poll_fn(|cx| {
            assert!(std::future::Future::poll(waiting.as_mut(), cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        assert_eq!(
            clients
                .iter()
                .filter(|client| client.connection.poll_aborted(&cx))
                .count(),
            8
        );
        assert!(!clients[0].connection.poll_aborted(&cx));
        assert_eq!(
            store.metrics().local_delivery.bytes_used,
            local::BYTE_UNIT as u64
        );
        drop(waiting);
        drop(leader);
        let last = bodies.pop().unwrap();
        drop(bodies);
        assert_eq!(
            store.metrics().local_delivery.bytes_used,
            local::BYTE_UNIT as u64
        );
        drop(last);
        assert_eq!(store.metrics().local_delivery.bytes_used, 0);
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn detached_local_reader_registers_returned_bytes_for_the_calling_connection() {
        use crate::delivery_control::{CLIENT, Client, Connection};
        let (root, cid, mut store) = cold_fixture("cold-delivery-registration");
        store.local_delivery = Arc::new(local::Delivery::new(1, 2, Duration::from_secs(2)));
        let client = Client::new("2001:db8:1:1::1".parse().unwrap(), Connection::new());
        client.connection.enable_reclamation();
        let content = CLIENT
            .scope(Some(client.clone()), store.get_local("ipfs", &cid, ""))
            .await
            .unwrap()
            .unwrap();
        client.ready();
        assert_eq!(store.local_lookup_count.load(Ordering::Relaxed), 1);
        tokio::time::advance(Duration::from_secs(1)).await;
        let mut waiting = Box::pin(
            store
                .local_delivery
                .reserve(local::BYTE_UNIT, store.local_delivery.deadline()),
        );
        std::future::poll_fn(|cx| {
            assert!(std::future::Future::poll(waiting.as_mut(), cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        assert!(
            client
                .connection
                .poll_aborted(&std::task::Context::from_waker(std::task::Waker::noop()))
        );
        assert_eq!(
            store.metrics().local_delivery.bytes_used,
            local::BYTE_UNIT as u64
        );
        drop(content);
        drop(waiting.await.unwrap());
        assert_eq!(store.metrics().local_delivery.bytes_used, 0);
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn expired_ready_leader_turn_counts_only_as_leader_timeout() {
        let (root, cid, mut store) = cold_fixture("cold-expired-leader-turn");
        store.local_leader_wait = Arc::new(local::Delivery::new(0, 1, Duration::from_millis(1)));
        let leader = store
            .local_flights
            .register(format!("ipfs://{cid}/"))
            .unwrap();
        let turn = leader.turn().acquire_owned().await.unwrap();
        let mut request = Box::pin(store.get_local("ipfs", &cid, ""));
        std::future::poll_fn(|cx| {
            assert!(std::future::Future::poll(request.as_mut(), cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        std::thread::sleep(Duration::from_millis(10));
        drop(turn);
        assert!(request.await.is_err());
        let reads = store.metrics().local_reads;
        assert_eq!(reads.leader_timeouts, 1);
        assert_eq!(reads.leader_waiting, 0);
        assert_eq!(reads.deadline_expired, 0);
        assert_eq!(reads.timeouts, 0);
        assert_eq!(reads.waits, 0);
        assert_eq!(
            store
                .local_lookup_count
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );
        drop((leader, store));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn default_follower_deadline_is_counted_before_the_outer_handler_timeout() {
        let (root, cid, store) = cold_fixture("follower-handler-headroom");
        let leader = store
            .local_flights
            .register(format!("ipfs://{cid}/"))
            .unwrap();
        let turn = leader.turn().acquire_owned().await.unwrap();
        let started = tokio::time::Instant::now();
        let result =
            tokio::time::timeout(Duration::from_secs(40), store.get_local("ipfs", &cid, ""))
                .await
                .expect("follower must fail before the handler deadline");
        assert_eq!(result.unwrap_err().kind, ErrorKind::Unavailable);
        assert!(started.elapsed() < Duration::from_secs(10));
        let reads = store.metrics().local_reads;
        assert_eq!(reads.leader_timeouts, 1);
        assert_eq!(reads.leader_waiting, 0);
        assert_eq!(reads.timeouts, 0);
        assert_eq!(reads.active, 0);
        drop((turn, leader));
        assert!(store.get_local("ipfs", &cid, "").await.unwrap().is_some());
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn busy_leader_shares_one_admission_failure_and_late_requests_retry() {
        let (root, cid, mut store) = cold_fixture("busy-leader-cohort");
        store.local_delivery = Arc::new(local::Delivery::new(128, 256, Duration::from_millis(20)));
        let held = store
            .local_reads
            .clone()
            .acquire_many_owned(16)
            .await
            .unwrap();
        let mut requests = Vec::new();
        for _ in 0..11 {
            let mut request = Box::pin(store.get_local("ipfs", &cid, ""));
            std::future::poll_fn(|cx| {
                assert!(std::future::Future::poll(request.as_mut(), cx).is_pending());
                std::task::Poll::Ready(())
            })
            .await;
            requests.push(request);
        }
        assert_eq!(store.metrics().local_reads.leader_waiting, 10);
        let first = requests.remove(0).await.unwrap_err();
        assert_eq!(first.retry_after, Some(Duration::from_secs(1)));
        for mut request in requests {
            // One ready poll proves followers do not begin another timed wait.
            let result =
                std::future::poll_fn(|cx| match std::future::Future::poll(request.as_mut(), cx) {
                    std::task::Poll::Ready(result) => std::task::Poll::Ready(result),
                    std::task::Poll::Pending => {
                        panic!("busy followers must share the completed error")
                    }
                })
                .await;
            assert_eq!(result.unwrap_err(), first);
        }
        let reads = store.metrics().local_reads;
        assert_eq!(reads.waits, 1);
        assert_eq!(reads.timeouts, 1);
        assert_eq!(reads.shared_failures, 10);
        assert_eq!(reads.deadline_expired, 0);
        assert_eq!(store.metrics().resolution.local_failures, 11);
        assert_eq!(reads.leader_waiting, 0);
        assert_eq!(reads.leader_timeouts, 0);
        assert_eq!(
            store
                .local_lookup_count
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );
        drop(held);
        assert_eq!(
            store
                .get_local("ipfs", &cid, "")
                .await
                .unwrap()
                .unwrap()
                .body,
            "cold"
        );
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn shared_exhaustion_counts_followers_without_recounting_admission_expiry() {
        let (root, cid, store) = cold_fixture("shared-admission-metrics");
        let leader = store
            .local_flights
            .register(format!("ipfs://{cid}/"))
            .unwrap();
        let turn = leader.turn().try_acquire_owned().unwrap();
        let mut followers = Vec::new();
        for _ in 0..10 {
            let mut request = Box::pin(store.get_local("ipfs", &cid, ""));
            std::future::poll_fn(|cx| {
                assert!(std::future::Future::poll(request.as_mut(), cx).is_pending());
                std::task::Poll::Ready(())
            })
            .await;
            followers.push(request);
        }
        let error = local::Delivery::new(0, 1, Duration::ZERO)
            .admission()
            .check()
            .unwrap_err();
        assert!(local::AdmissionBudget::exhausted(&error));
        leader.complete_busy(&error);
        drop(turn);
        for follower in followers {
            assert_eq!(follower.await.unwrap_err(), error);
        }
        let metrics = store.metrics();
        assert_eq!(metrics.local_reads.shared_failures, 10);
        assert_eq!(metrics.local_reads.deadline_expired, 0);
        assert_eq!(metrics.local_reads.waits, 0);
        assert_eq!(metrics.local_reads.leader_waiting, 0);
        assert_eq!(metrics.resolution.local_failures, 10);
        assert!(store.get_local("ipfs", &cid, "").await.unwrap().is_some());
        assert_eq!(store.metrics().local_reads.shared_failures, 10);
        assert_eq!(store.metrics().local_reads.deadline_expired, 0);
        drop((leader, store));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn scratch_busy_after_spawn_is_shared_without_more_readers() {
        let (root, cid, mut store) = cold_fixture("scratch-busy-cohort");
        let scratch = store.local_scratch.clone();
        store.local_scratch = Arc::new(local::Delivery::new(0, 256, Duration::from_secs(2)));
        let mut requests = Vec::new();
        for _ in 0..11 {
            let mut request = Box::pin(store.get_local("ipfs", &cid, ""));
            std::future::poll_fn(|cx| {
                assert!(std::future::Future::poll(request.as_mut(), cx).is_pending());
                std::task::Poll::Ready(())
            })
            .await;
            requests.push(request);
        }
        for request in requests {
            let error = request.await.unwrap_err();
            assert_eq!(error.message, "local body exceeds byte budget");
            assert_eq!(error.retry_after, Some(Duration::from_secs(1)));
        }
        assert_eq!(
            store
                .local_lookup_count
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
        assert_eq!(store.metrics().local_reads.active, 0);
        assert_eq!(store.metrics().local_reads.leader_waiting, 0);
        assert_eq!(store.metrics().local_reads.shared_failures, 10);
        assert_eq!(store.metrics().local_reads.deadline_expired, 0);
        assert_eq!(store.metrics().resolution.local_failures, 11);
        store.local_scratch = scratch;
        assert!(store.get_local("ipfs", &cid, "").await.unwrap().is_some());
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }
}
