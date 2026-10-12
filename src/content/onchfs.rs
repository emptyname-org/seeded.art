//! ONCHFS resolver over the Tezos contracts' big maps, read through TzKT.
//!
//! Format, as implemented by the Tezos contracts:
//! - content store `KT1JySSNRfQeRzrkZJypyoHDBRDaTbtTYAq1`: big map `keccak(chunk) -> chunk`;
//! - file objects `KT1Ae7dT1gsLw2tRnUMXSCmEyF74KVkM6LUo`: big map `cid -> inode`, where an
//!   inode is a directory `{name -> cid}` or a file `{chunk_pointers, metadata}`;
//! - file cid = `keccak(0x01 || keccak(concat(chunks)) || keccak(metadata))`, metadata
//!   being HPACK-encoded HTTP headers;
//! - directory cid = `keccak(0x00 || for names descending: cid || keccak(name))`.
//!
//! Upstream and temporary-cache records are hash-checked. Preservation-mirror
//! playback trusts the mirror writer's verification.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use serde_json::Value;
use sha3::{Digest, Keccak256};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use super::cache::DiskCache;
use super::http::{Http, Reply};
use super::{ContentConfig, ContentError, LocalHint, hpack, path};

pub const MAINNET_FILE_OBJECTS: &str = "KT1Ae7dT1gsLw2tRnUMXSCmEyF74KVkM6LUo";
pub const MAINNET_CONTENT_STORE: &str = "KT1JySSNRfQeRzrkZJypyoHDBRDaTbtTYAq1";
/// `inodes` big map of the file-objects contract.
pub const MAINNET_INODES_BIGMAP: u64 = 549145;
/// Chunk big map of the content-store contract.
pub const MAINNET_CHUNKS_BIGMAP: u64 = 549144;

const NS: &str = "onchfs";
const CHUNK_NS: &str = "onchfs-chunk";
const RESOLUTION_LIMIT: Duration = Duration::from_secs(35);
/// Upper bound for one inode's JSON (directories are small).
const INODE_REPLY_CAP: u64 = 1024 * 1024;
const MAX_DIR_ENTRIES: usize = 10_000;
const PROBE_PREFIX_CAP: usize = INODE_REPLY_CAP as usize + 5;

pub type Hash = [u8; 32];

pub fn keccak(data: &[u8]) -> Hash {
    Keccak256::digest(data).into()
}

/// File CID from the keccak of its full content and its raw metadata.
pub fn file_cid(content_hash: &Hash, metadata: &[u8]) -> Hash {
    let mut h = Keccak256::new();
    h.update([1u8]);
    h.update(content_hash);
    h.update(keccak(metadata));
    h.finalize().into()
}

/// Directory CID; `BTreeMap` iterates names in byte order, as Michelson maps do.
pub fn directory_cid(entries: &BTreeMap<String, Hash>) -> Hash {
    let mut h = Keccak256::new();
    h.update([0u8]);
    for (name, cid) in entries.iter().rev() {
        h.update(cid);
        h.update(keccak(name.as_bytes()));
    }
    h.finalize().into()
}

/// Parses a 64-hex-digit ONCHFS CID.
pub fn parse_cid(s: &str) -> Result<Hash, ContentError> {
    let mut out = [0u8; 32];
    if s.len() != 64 || hex::decode_to_slice(s, &mut out).is_err() {
        return Err(ContentError::invalid("ONCHFS CID must be 64 hex digits"));
    }
    Ok(out)
}

/// A decoded, not yet verified inode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Inode {
    Directory(BTreeMap<String, Hash>),
    File {
        metadata: Vec<u8>,
        chunks: Vec<Hash>,
    },
}

impl Inode {
    /// Parses a TzKT big-map value: `{"directory":{..}}` or `{"file":{..}}`.
    pub fn from_tzkt(value: &Value) -> Result<Self, ContentError> {
        let bad = |m: &str| ContentError::unavailable(format!("TzKT inode: {m}"));
        if let Some(dir) = value.get("directory") {
            let map = dir
                .as_object()
                .ok_or_else(|| bad("directory is not an object"))?;
            if map.len() > MAX_DIR_ENTRIES {
                return Err(bad("directory too large"));
            }
            let mut entries = BTreeMap::new();
            for (name, cid) in map {
                let cid = cid.as_str().ok_or_else(|| bad("entry is not a string"))?;
                entries.insert(name.clone(), parse_cid(cid).map_err(|_| bad("entry cid"))?);
            }
            Ok(Inode::Directory(entries))
        } else if let Some(file) = value.get("file") {
            let metadata = file
                .get("metadata")
                .and_then(Value::as_str)
                .and_then(|m| hex::decode(m).ok())
                .ok_or_else(|| bad("file metadata"))?;
            let chunks = file
                .get("chunk_pointers")
                .and_then(Value::as_array)
                .ok_or_else(|| bad("chunk_pointers"))?
                .iter()
                .map(|p| {
                    p.as_str()
                        .and_then(|p| parse_cid(p).ok())
                        .ok_or_else(|| bad("chunk pointer"))
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Inode::File { metadata, chunks })
        } else {
            Err(bad("neither directory nor file"))
        }
    }
}

/// A resolved ONCHFS file.
#[derive(Debug, Clone)]
pub struct Resolved {
    /// Stored bytes (still `content_encoding`-encoded).
    pub bytes: Bytes,
    pub content_type: Option<String>,
    pub content_encoding: Option<String>,
    pub file_cid: Hash,
    /// Canonical lowercase hex keys for every record traversed, including the root.
    pub record_cids: Vec<String>,
    /// A directory was requested and its `index.html` served.
    pub directory_index: bool,
    pub from_cache: bool,
}

/// A verified node on the walk.
enum Node {
    Directory(BTreeMap<String, Hash>),
    File {
        metadata: Vec<u8>,
        chunks: Vec<Hash>,
    },
    CachedFile {
        metadata: Vec<u8>,
        content: Bytes,
    },
}

type SharedResult = tokio::sync::Mutex<Option<Result<Resolved, ContentError>>>;

struct Flight<'a> {
    registry: &'a Mutex<HashMap<String, Arc<SharedResult>>>,
    key: String,
    result: Arc<SharedResult>,
}

impl Drop for Flight<'_> {
    fn drop(&mut self) {
        if let Ok(mut registry) = self.registry.lock() {
            if Arc::strong_count(&self.result) == 2 {
                registry.remove(&self.key);
            }
        }
    }
}

#[derive(Default)]
struct Resolution {
    network: bool,
    permit: Option<Arc<OwnedSemaphorePermit>>,
}

struct HeldBytes {
    bytes: Bytes,
    _permit: Arc<OwnedSemaphorePermit>,
}

impl AsRef<[u8]> for HeldBytes {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl Resolution {
    fn for_write(&self, bytes: Bytes) -> Bytes {
        match &self.permit {
            Some(permit) => Bytes::from_owner(HeldBytes {
                bytes,
                _permit: permit.clone(),
            }),
            None => bytes,
        }
    }
}

pub struct OnchfsResolver {
    http: Arc<Http>,
    cache: Arc<DiskCache>,
    assemblies: Arc<Semaphore>,
    cache_reads: Arc<Semaphore>,
    requests: Semaphore,
    flights: Mutex<HashMap<String, Arc<SharedResult>>>,
    tzkt: String,
    inodes: u64,
    chunks: u64,
    cap: u64,
    batch: usize,
    absence_ttl: Duration,
    resolution_timeout: Duration,
    request_timeout: Duration,
}

impl OnchfsResolver {
    pub(crate) fn touch(&self, file_cid: &str) {
        self.cache.touch(NS, file_cid);
    }

    pub(crate) fn new(
        cfg: &ContentConfig,
        http: Arc<Http>,
        cache: Arc<DiskCache>,
        assemblies: Arc<Semaphore>,
    ) -> Self {
        Self {
            http,
            cache,
            assemblies,
            cache_reads: Arc::new(Semaphore::new(cfg.max_concurrent_assemblies)),
            requests: Semaphore::new(cfg.max_concurrent_assemblies * 8),
            flights: Mutex::new(HashMap::new()),
            tzkt: cfg.tzkt_base.trim_end_matches('/').to_string(),
            inodes: cfg.onchfs_inodes_bigmap,
            chunks: cfg.onchfs_chunks_bigmap,
            cap: cfg.max_object_bytes,
            batch: cfg.chunk_batch,
            absence_ttl: cfg.absence_ttl,
            resolution_timeout: cfg.request_timeout.saturating_mul(4).min(RESOLUTION_LIMIT),
            request_timeout: cfg.request_timeout,
        }
    }

    /// Resolves `onchfs://<cid>/<path>`, serving `index.html` for directories.
    pub(crate) async fn local(
        &self,
        cid: &str,
        path: &str,
        permit: Arc<OwnedSemaphorePermit>,
        body_cap: u64,
    ) -> Result<Option<Resolved>, ContentError> {
        let root = parse_cid(cid)?;
        let segments = path::split(path)?;
        let cache = self.cache.clone();
        let cap = self.cap.min(body_cap);
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let mut load = |cid: &Hash| {
                cache
                    .get_local_capped(
                        NS,
                        &hex::encode(cid),
                        cap.saturating_add(INODE_REPLY_CAP).saturating_add(5),
                    )
                    .map(|bytes| bytes.map(Bytes::from))
            };
            match resolve_records(root, &segments, cap, &mut load) {
                Err(error) if error.kind == super::ErrorKind::NotFound => Err(error),
                Err(_) => Ok(None),
                result => result,
            }
        })
        .await
        .map_err(|_| ContentError::unavailable("cache reader failed"))?
    }

    /// Inspects cached record headers and directories without allocating file bodies.
    pub(crate) async fn probe_local(
        &self,
        cid: &str,
        path: &str,
        permit: Arc<OwnedSemaphorePermit>,
    ) -> Result<Option<LocalHint>, ContentError> {
        let root = parse_cid(cid)?;
        let segments = path::split(path)?;
        let cache = self.cache.clone();
        let cap = self.cap;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let mut load = |cid: &Hash| cache.peek_local(NS, &hex::encode(cid), PROBE_PREFIX_CAP);
            match probe_records(root, &segments, cap, &mut load) {
                Err(_) => Ok(None),
                result => result,
            }
        })
        .await
        .map_err(|_| ContentError::unavailable("cache probe failed"))?
    }

    /// Resolves `onchfs://<cid>/<path>`, serving `index.html` for directories.
    pub async fn resolve(&self, cid: &str, path: &str) -> Result<Resolved, ContentError> {
        let root = parse_cid(cid)?;
        let segments = path::split(path)?;
        let _request = self
            .requests
            .try_acquire()
            .map_err(|_| self.http.busy("ONCHFS request queue full"))?;
        let key = format!("{}/{path}", hex::encode(root));
        let result = {
            let mut flights = self
                .flights
                .lock()
                .map_err(|_| ContentError::unavailable("ONCHFS coordination unavailable"))?;
            flights
                .entry(key.clone())
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(None)))
                .clone()
        };
        let flight = Flight {
            registry: &self.flights,
            key,
            result,
        };
        tokio::time::timeout(self.resolution_timeout, async {
            let mut shared = flight.result.lock().await;
            if let Some(result) = &*shared {
                return result.clone();
            }
            // Cancellation drops the guard; a waiting request can become leader.
            // Completed errors are shared only with current waiters, never persisted.
            let result = self.resolve_inner(root, &segments).await;
            *shared = Some(result.clone());
            result
        })
        .await
        .map_err(|_| self.http.deadline("ONCHFS resolution timed out"))?
    }

    async fn fetch(&self, url: &str, cap: u64) -> Result<Reply, ContentError> {
        tokio::time::timeout(self.request_timeout, async {
            for attempt in 0..3 {
                let reply = self
                    .http
                    .get(url, Some("application/json"), cap)
                    .await
                    .map_err(|error| error.to_content_error("TzKT request", cap))?;
                if attempt == 2 || !matches!(reply.status, 429 | 500 | 502 | 503 | 504) {
                    return Ok(reply);
                }
                let delay = reply
                    .retry_after
                    .unwrap_or_else(|| Duration::from_millis(250 << attempt));
                if delay >= self.request_timeout {
                    return Ok(reply);
                }
                tokio::time::sleep(delay).await;
            }
            Err(ContentError::unavailable("TzKT retry exhausted"))
        })
        .await
        .map_err(|_| ContentError::unavailable("TzKT retry deadline exceeded"))?
    }

    async fn network(&self, state: &mut Resolution) -> Result<(), ContentError> {
        if state.permit.is_none() {
            let permit = tokio::time::timeout(
                self.request_timeout,
                self.assemblies.clone().acquire_owned(),
            )
            .await
            .map_err(|_| self.http.busy("ONCHFS assembly workers busy"))?
            .map_err(|_| ContentError::unavailable("store closed"))?;
            state.permit = Some(Arc::new(permit));
        }
        state.network = true;
        Ok(())
    }

    async fn resolve_inner(
        &self,
        root: Hash,
        segments: &[path::Segment],
    ) -> Result<Resolved, ContentError> {
        let mut state = Resolution::default();
        let mut cid = root;
        let mut record_cids = vec![hex::encode(root)];
        let mut node = self.node(root, true, &mut state).await?;
        for seg in segments {
            if let Node::File { metadata, chunks } = node {
                let content = self.assemble(&cid, &metadata, &chunks, &state).await?;
                node = Node::CachedFile { metadata, content };
            }
            let Node::Directory(entries) = &node else {
                return Err(ContentError::not_found(format!(
                    "{} is not a directory",
                    seg.raw
                )));
            };
            let child = seg
                .candidates()
                .iter()
                .find_map(|c| entries.get(c).copied())
                .ok_or_else(|| ContentError::not_found(format!("no entry {}", seg.raw)))?;
            node = self.node(child, false, &mut state).await?;
            cid = child;
            record_cids.push(hex::encode(child));
        }
        let mut directory_index = false;
        if let Node::Directory(entries) = &node {
            let index = *entries
                .get("index.html")
                .ok_or_else(|| ContentError::not_found("directory has no index.html"))?;
            node = self.node(index, false, &mut state).await?;
            cid = index;
            record_cids.push(hex::encode(index));
            directory_index = true;
        }
        let (metadata, bytes) = match node {
            Node::Directory(_) => return Err(ContentError::not_found("index.html is a directory")),
            Node::CachedFile { metadata, content } => (metadata, content),
            Node::File { metadata, chunks } => {
                let content = self.assemble(&cid, &metadata, &chunks, &state).await?;
                (metadata, content)
            }
        };
        if bytes.len() as u64 > self.cap {
            return Err(ContentError::too_large(self.cap));
        }
        let headers = hpack::decode(&metadata)
            .map_err(|e| ContentError::invalid(format!("file metadata: {e}")))?;
        let header = |name: &str| {
            headers
                .iter()
                .find(|(n, _)| n == name)
                .map(|(_, v)| v.clone())
        };
        Ok(Resolved {
            content_type: header("content-type"),
            content_encoding: header("content-encoding"),
            bytes,
            file_cid: cid,
            record_cids,
            directory_index,
            from_cache: !state.network,
        })
    }

    /// One verified inode for the mirror: a directory's entries, or a file's metadata and
    /// assembled content (at most the object limit).
    pub(crate) async fn record(&self, cid: Hash, is_root: bool) -> Result<Record, ContentError> {
        let mut state = Resolution::default();
        match self.node(cid, is_root, &mut state).await? {
            Node::Directory(entries) => Ok(Record::Directory(entries)),
            Node::CachedFile { metadata, content } => Ok(Record::File { metadata, content }),
            Node::File { metadata, chunks } => {
                let content = self.assemble(&cid, &metadata, &chunks, &state).await?;
                Ok(Record::File { metadata, content })
            }
        }
    }

    /// Returns a verified node from the cache or TzKT.
    async fn node(
        &self,
        cid: Hash,
        is_root: bool,
        state: &mut Resolution,
    ) -> Result<Node, ContentError> {
        let key = hex::encode(cid);
        if let Some(node) = self.cached(&cid, &key).await? {
            return Ok(node);
        }
        if is_root && self.cache.is_absent(NS, &key).await.unwrap_or(false) {
            return Err(ContentError::not_found("ONCHFS CID not on chain (cached)"));
        }
        self.network(state).await?;
        let url = format!("{}/v1/bigmaps/{}/keys/{key}", self.tzkt, self.inodes);
        let reply = self.fetch(&url, INODE_REPLY_CAP).await?;
        let value = match reply.status {
            200 => serde_json::from_slice::<Value>(&reply.body)
                .map_err(|e| ContentError::unavailable(format!("TzKT inode json: {e}")))?,
            204 => Value::Null,
            s => {
                let mut e = ContentError::unavailable(format!("TzKT inode: HTTP {s}"));
                e.retry_after = reply.retry_after;
                return Err(e);
            }
        };
        let active = if reply.status == 204 {
            false
        } else {
            if value.get("value").is_none() {
                return Err(ContentError::unavailable("TzKT inode missing value"));
            }
            value
                .get("active")
                .and_then(Value::as_bool)
                .ok_or_else(|| ContentError::unavailable("TzKT inode missing active state"))?
        };
        let Some(inode_value) = value.get("value").filter(|_| active) else {
            if is_root {
                // An inode can still be inscribed later: remember, but not forever.
                let _ = self
                    .cache
                    .mark_absent(NS, &key, Some(self.absence_ttl))
                    .await;
                return Err(ContentError::not_found("ONCHFS CID not on chain"));
            }
            // A verified directory points here and the contract only links
            // existing inodes, so the indexer is behind or inconsistent.
            return Err(ContentError::unavailable(
                "TzKT lacks an inode its parent references",
            ));
        };
        match Inode::from_tzkt(inode_value)? {
            Inode::Directory(entries) => {
                if directory_cid(&entries) != cid {
                    return Err(ContentError::invalid(format!(
                        "directory {key} hash mismatch"
                    )));
                }
                let data = state.for_write(Bytes::from(encode_directory(&entries)));
                let _ = self.cache.put_parts(NS, &key, vec![data]).await;
                Ok(Node::Directory(entries))
            }
            Inode::File { metadata, chunks } => Ok(Node::File { metadata, chunks }),
        }
    }

    async fn cached(&self, cid: &Hash, key: &str) -> Result<Option<Node>, ContentError> {
        let _permit = self
            .cache_reads
            .acquire()
            .await
            .map_err(|_| ContentError::unavailable("cache readers closed"))?;
        let Ok(Some(raw)) = self.cache.get(NS, key).await else {
            return Ok(None);
        };
        match decode_cached(Bytes::from(raw)) {
            Some(node) if node_matches(&node, cid) => Ok(Some(node)),
            _ => {
                let _ = self.cache.remove(NS, key).await;
                Ok(None)
            }
        }
    }

    /// Fetches, verifies and caches a file's chunks.
    async fn assemble(
        &self,
        cid: &Hash,
        metadata: &[u8],
        chunks: &[Hash],
        state: &Resolution,
    ) -> Result<Bytes, ContentError> {
        let mut content = BytesMut::new();
        for group in chunks.chunks(self.batch) {
            let fetched = self
                .fetch_chunks(group, self.cap - content.len() as u64, state)
                .await?;
            for ptr in group {
                let data = fetched
                    .get(ptr)
                    .ok_or_else(|| ContentError::unavailable("TzKT lacks a referenced chunk"))?;
                if content.len() as u64 + data.len() as u64 > self.cap {
                    return Err(ContentError::too_large(self.cap));
                }
                content.extend_from_slice(data);
            }
        }
        if file_cid(&keccak(&content), metadata) != *cid {
            return Err(ContentError::invalid(format!(
                "file {} hash mismatch",
                hex::encode(cid)
            )));
        }
        let content = content.freeze();
        let header = file_header(metadata);
        let _ = self
            .cache
            .put_parts(
                NS,
                &hex::encode(cid),
                vec![Bytes::from(header), state.for_write(content.clone())],
            )
            .await;
        Ok(content)
    }

    /// One TzKT call for up to `batch` distinct chunks; each is hash-checked.
    async fn fetch_chunks(
        &self,
        group: &[Hash],
        remaining: u64,
        state: &Resolution,
    ) -> Result<HashMap<Hash, Bytes>, ContentError> {
        let mut all: Vec<Hash> = group.to_vec();
        all.sort();
        all.dedup();
        let mut out = HashMap::new();
        let mut keys = Vec::new();
        let mut total = 0u64;
        for pointer in all {
            let key = hex::encode(pointer);
            if let Ok(Some(bytes)) = self.cache.get(CHUNK_NS, &key).await {
                if keccak(&bytes) == pointer {
                    total += bytes.len() as u64;
                    if total > remaining {
                        return Err(ContentError::too_large(self.cap));
                    }
                    out.insert(pointer, Bytes::from(bytes));
                    continue;
                }
                let _ = self.cache.remove(CHUNK_NS, &key).await;
            }
            keys.push(key);
        }
        if keys.is_empty() {
            return Ok(out);
        }
        keys.sort();
        let filter = if keys.len() == 1 {
            format!("key={}", keys[0])
        } else {
            format!("key.in={}", keys.join(","))
        };
        let url = format!(
            "{}/v1/bigmaps/{}/keys?{filter}&select=key,value,active&limit={}",
            self.tzkt,
            self.chunks,
            keys.len()
        );
        // Hex doubles only the remaining payload; bound JSON overhead per row.
        let cap = (remaining - total)
            .saturating_mul(2)
            .saturating_add(keys.len() as u64 * 256 + 1024);
        let reply = self.fetch(&url, cap).await?;
        if reply.status != 200 {
            let mut e = ContentError::unavailable(format!("TzKT chunks: HTTP {}", reply.status));
            e.retry_after = reply.retry_after;
            return Err(e);
        }
        let rows = chunk_rows(&reply.body, keys.len())?;
        let mut fresh = Vec::new();
        for row in rows {
            let key = row.key;
            if !keys.iter().any(|expected| expected == key) || out.contains_key(&parse_cid(key)?) {
                return Err(ContentError::unavailable(
                    "TzKT returned an unrequested or duplicate chunk",
                ));
            }
            if !row.active {
                continue;
            }
            let ptr = parse_cid(key).map_err(|_| ContentError::unavailable("TzKT chunk key"))?;
            if row.value.len() as u64 / 2 > remaining - total {
                return Err(ContentError::too_large(self.cap));
            }
            let data =
                hex::decode(row.value).map_err(|_| ContentError::unavailable("TzKT chunk hex"))?;
            if keccak(&data) != ptr {
                return Err(ContentError::invalid(format!("chunk {key} hash mismatch")));
            }
            total += data.len() as u64;
            out.insert(ptr, Bytes::from(data));
            fresh.push(ptr);
        }
        drop(reply);
        for pointer in fresh {
            let data = state.for_write(out[&pointer].clone());
            let _ = self
                .cache
                .put_parts(CHUNK_NS, &hex::encode(pointer), vec![data])
                .await;
        }
        Ok(out)
    }
}

#[derive(serde::Deserialize)]
struct ChunkRow<'a> {
    key: &'a str,
    value: &'a str,
    active: bool,
}

fn chunk_rows(data: &[u8], limit: usize) -> Result<Vec<ChunkRow<'_>>, ContentError> {
    use serde::de::{DeserializeSeed, SeqAccess, Visitor};
    struct Batch(usize);
    impl<'de> DeserializeSeed<'de> for Batch {
        type Value = Vec<ChunkRow<'de>>;
        fn deserialize<D: serde::Deserializer<'de>>(
            self,
            deserializer: D,
        ) -> Result<Self::Value, D::Error> {
            deserializer.deserialize_seq(self)
        }
    }
    impl<'de> Visitor<'de> for Batch {
        type Value = Vec<ChunkRow<'de>>;
        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("bounded chunk rows")
        }
        fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
            let mut rows = Vec::with_capacity(self.0);
            while rows.len() < self.0 {
                match seq.next_element()? {
                    Some(row) => rows.push(row),
                    None => return Ok(rows),
                }
            }
            if seq.next_element::<serde::de::IgnoredAny>()?.is_some() {
                return Err(serde::de::Error::custom("too many chunk rows"));
            }
            Ok(rows)
        }
    }
    let mut decoder = serde_json::Deserializer::from_slice(data);
    let rows = Batch(limit)
        .deserialize(&mut decoder)
        .map_err(|_| ContentError::unavailable("TzKT chunk rows malformed or excessive"))?;
    decoder
        .end()
        .map_err(|_| ContentError::unavailable("TzKT chunk rows trailing data"))?;
    Ok(rows)
}

/// A verified inode as the mirror stores it, in the cache record encoding.
pub(crate) enum Record {
    Directory(BTreeMap<String, Hash>),
    File { metadata: Vec<u8>, content: Bytes },
}

impl Record {
    pub(crate) fn encode(&self) -> Vec<Bytes> {
        match self {
            Record::Directory(entries) => vec![Bytes::from(encode_directory(entries))],
            Record::File { metadata, content } => {
                vec![Bytes::from(file_header(metadata)), content.clone()]
            }
        }
    }

    /// Decodes a stored record; `None` unless it matches `cid`.
    pub(crate) fn decode(raw: Bytes, cid: &Hash) -> Option<Record> {
        Self::decode_stored(raw, Some(cid))
    }

    fn decode_stored(raw: Bytes, verify: Option<&Hash>) -> Option<Record> {
        match decode_cached(raw)? {
            node if verify.is_some_and(|cid| !node_matches(&node, cid)) => None,
            Node::Directory(entries) => Some(Record::Directory(entries)),
            Node::CachedFile { metadata, content } => Some(Record::File { metadata, content }),
            Node::File { .. } => None,
        }
    }
}

/// Loads a stored record by CID; `None` when the store lacks it.
pub(crate) type RecordLoader<'a> = dyn FnMut(&Hash) -> std::io::Result<Option<Bytes>> + 'a;

/// Loads a bounded prefix and the complete stored record length.
pub(crate) type ProbeLoader<'a> = dyn FnMut(&Hash) -> std::io::Result<Option<(Vec<u8>, u64)>> + 'a;

enum ProbeRecord {
    Directory(BTreeMap<String, Hash>),
    File { body_bytes: u64 },
}

fn decode_probe_record(prefix: &[u8], total_bytes: u64) -> Option<ProbeRecord> {
    match *prefix.first()? {
        TAG_DIRECTORY => {
            if total_bytes > INODE_REPLY_CAP + 1 || prefix.len() as u64 != total_bytes {
                return None;
            }
            let map: BTreeMap<String, String> = serde_json::from_slice(&prefix[1..]).ok()?;
            if map.len() > MAX_DIR_ENTRIES {
                return None;
            }
            let entries = map
                .into_iter()
                .map(|(name, value)| parse_cid(&value).ok().map(|cid| (name, cid)))
                .collect::<Option<BTreeMap<_, _>>>()?;
            Some(ProbeRecord::Directory(entries))
        }
        TAG_FILE => {
            let metadata_len = u32::from_be_bytes(prefix.get(1..5)?.try_into().ok()?) as usize;
            let header_len = 5usize.checked_add(metadata_len)?;
            if metadata_len as u64 > INODE_REPLY_CAP
                || prefix.len() < header_len
                || total_bytes < header_len as u64
            {
                return None;
            }
            hpack::decode(&prefix[5..header_len]).ok()?;
            Some(ProbeRecord::File {
                body_bytes: total_bytes - header_len as u64,
            })
        }
        _ => None,
    }
}

/// Walks cached inode headers only. Record keys are the exact traversed CIDs.
pub(crate) fn probe_records(
    root: Hash,
    segments: &[path::Segment],
    cap: u64,
    load: &mut ProbeLoader<'_>,
) -> Result<Option<LocalHint>, ContentError> {
    let mut record_cids = Vec::with_capacity(segments.len().saturating_add(2));
    let mut read = |cid: &Hash| -> Result<Option<ProbeRecord>, ContentError> {
        let Some((prefix, total_bytes)) = load(cid)? else {
            return Ok(None);
        };
        if prefix.len() > PROBE_PREFIX_CAP {
            return Ok(None);
        }
        let Some(record) = decode_probe_record(&prefix, total_bytes) else {
            return Ok(None);
        };
        record_cids.push(hex::encode(cid));
        Ok(Some(record))
    };
    let Some(mut record) = read(&root)? else {
        return Ok(None);
    };
    for segment in segments {
        let ProbeRecord::Directory(entries) = &record else {
            return Err(ContentError::not_found(format!(
                "{} is not a directory",
                segment.raw
            )));
        };
        let child = segment
            .candidates()
            .iter()
            .find_map(|candidate| entries.get(candidate).copied())
            .ok_or_else(|| ContentError::not_found(format!("no entry {}", segment.raw)))?;
        let Some(next) = read(&child)? else {
            return Ok(None);
        };
        record = next;
    }
    if let ProbeRecord::Directory(entries) = &record {
        let index = *entries
            .get("index.html")
            .ok_or_else(|| ContentError::not_found("directory has no index.html"))?;
        let Some(next) = read(&index)? else {
            return Ok(None);
        };
        record = next;
    }
    let ProbeRecord::File { body_bytes } = record else {
        return Err(ContentError::not_found("index.html is a directory"));
    };
    if body_bytes > cap {
        return Err(ContentError::too_large(cap));
    }
    // At peak the raw file and retained verification-cache copy coexist. Four
    // bounded inode expansions cover decode buffers and a directory being
    // replaced by its child; path candidates fit in 32 KiB and the trace has
    // at most 66 strings, each charged 128 bytes including allocation overhead.
    let scratch_bytes = body_bytes
        .saturating_mul(2)
        .saturating_add(INODE_REPLY_CAP.saturating_mul(4))
        .saturating_add(32 * 1024)
        .saturating_add((segments.len().saturating_add(2) * 128) as u64);
    Ok(Some(LocalHint {
        body_bytes,
        scratch_bytes,
        cache_keys: record_cids,
        mirror_leaf: None,
    }))
}

/// Resolves a path through stored records with the network resolver's rules. `Ok(None)`: a
/// record is absent; a record that fails verification is an `Invalid` error.
pub(crate) fn resolve_records(
    root: Hash,
    segments: &[path::Segment],
    cap: u64,
    load: &mut RecordLoader<'_>,
) -> Result<Option<Resolved>, ContentError> {
    resolve_stored(root, segments, cap, load, true)
}

/// Playback-only read of records verified when the mirror writer committed them.
/// Encoding, metadata, path and object-size checks remain; no hashes are computed.
pub(super) fn resolve_mirrored_records(
    root: Hash,
    segments: &[path::Segment],
    cap: u64,
    load: &mut RecordLoader<'_>,
) -> Result<Option<Resolved>, ContentError> {
    resolve_stored(root, segments, cap, load, false)
}

fn resolve_stored(
    root: Hash,
    segments: &[path::Segment],
    cap: u64,
    load: &mut RecordLoader<'_>,
    verify: bool,
) -> Result<Option<Resolved>, ContentError> {
    let mut read = |cid: &Hash| -> Result<Option<Record>, ContentError> {
        let Some(raw) = load(cid)? else {
            return Ok(None);
        };
        Record::decode_stored(raw, verify.then_some(cid))
            .map(Some)
            .ok_or_else(|| ContentError::invalid("invalid stored ONCHFS record"))
    };
    let mut record_cids = vec![hex::encode(root)];
    let mut cid = root;
    let Some(mut record) = read(&cid)? else {
        return Ok(None);
    };
    for seg in segments {
        let Record::Directory(entries) = &record else {
            return Err(ContentError::not_found(format!(
                "{} is not a directory",
                seg.raw
            )));
        };
        let child = seg
            .candidates()
            .iter()
            .find_map(|c| entries.get(c).copied())
            .ok_or_else(|| ContentError::not_found(format!("no entry {}", seg.raw)))?;
        let Some(next) = read(&child)? else {
            return Ok(None);
        };
        record = next;
        cid = child;
        record_cids.push(hex::encode(child));
    }
    let mut directory_index = false;
    if let Record::Directory(entries) = &record {
        let index = *entries
            .get("index.html")
            .ok_or_else(|| ContentError::not_found("directory has no index.html"))?;
        let Some(next) = read(&index)? else {
            return Ok(None);
        };
        record = next;
        cid = index;
        record_cids.push(hex::encode(index));
        directory_index = true;
    }
    let Record::File { metadata, content } = record else {
        return Err(ContentError::not_found("index.html is a directory"));
    };
    if content.len() as u64 > cap {
        return Err(ContentError::too_large(cap));
    }
    let headers = hpack::decode(&metadata)
        .map_err(|e| ContentError::invalid(format!("file metadata: {e}")))?;
    let header = |name: &str| {
        headers
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.clone())
    };
    Ok(Some(Resolved {
        content_type: header("content-type"),
        content_encoding: header("content-encoding"),
        bytes: content,
        file_cid: cid,
        record_cids,
        directory_index,
        from_cache: true,
    }))
}

// Cache record: tag byte, then a directory as JSON, or a file as
// u32 BE metadata length, metadata, content.
const TAG_DIRECTORY: u8 = 0;
const TAG_FILE: u8 = 1;

fn encode_directory(entries: &BTreeMap<String, Hash>) -> Vec<u8> {
    let map: BTreeMap<&str, String> = entries
        .iter()
        .map(|(k, v)| (k.as_str(), hex::encode(v)))
        .collect();
    let mut out = vec![TAG_DIRECTORY];
    out.extend(serde_json::to_vec(&map).unwrap_or_default());
    out
}

fn file_header(metadata: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(5 + metadata.len());
    out.push(TAG_FILE);
    out.extend((metadata.len() as u32).to_be_bytes());
    out.extend_from_slice(metadata);
    out
}

#[cfg(test)]
fn encode_file(metadata: &[u8], content: &[u8]) -> Bytes {
    let mut out = file_header(metadata);
    out.extend_from_slice(content);
    Bytes::from(out)
}

fn decode_cached(raw: Bytes) -> Option<Node> {
    let tag = *raw.first()?;
    let rest = raw.slice(1..);
    match tag {
        TAG_DIRECTORY => {
            if rest.len() as u64 > INODE_REPLY_CAP {
                return None;
            }
            let map: BTreeMap<String, String> = serde_json::from_slice(&rest).ok()?;
            if map.len() > MAX_DIR_ENTRIES {
                return None;
            }
            let entries = map
                .into_iter()
                .map(|(k, v)| parse_cid(&v).ok().map(|c| (k, c)))
                .collect::<Option<BTreeMap<_, _>>>()?;
            Some(Node::Directory(entries))
        }
        TAG_FILE => {
            let len = u32::from_be_bytes(rest.get(..4)?.try_into().ok()?) as usize;
            if len as u64 > INODE_REPLY_CAP {
                return None;
            }
            let end = 4usize.checked_add(len)?;
            let metadata = rest.get(4..end)?.to_vec();
            rest.get(end..)?;
            let content = rest.slice(end..);
            Some(Node::CachedFile { metadata, content })
        }
        _ => None,
    }
}

/// Re-verifies a cache record against its CID (guards against disk corruption).
fn node_matches(node: &Node, cid: &Hash) -> bool {
    match node {
        Node::Directory(entries) => directory_cid(entries) == *cid,
        Node::CachedFile { metadata, content } => file_cid(&keccak(content), metadata) == *cid,
        Node::File { .. } => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keccak_matches_known_vector() {
        assert_eq!(
            hex::encode(keccak(b"")),
            "c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470"
        );
    }

    #[test]
    fn directory_cid_hashes_names_descending() {
        let a = [1u8; 32];
        let b = [2u8; 32];
        let entries = BTreeMap::from([("a".to_string(), a), ("b".to_string(), b)]);
        let mut manual = vec![0u8];
        manual.extend(b);
        manual.extend(keccak(b"b"));
        manual.extend(a);
        manual.extend(keccak(b"a"));
        assert_eq!(directory_cid(&entries), keccak(&manual));
    }

    #[test]
    fn cache_records_roundtrip_and_reject_garbage() {
        let entries = BTreeMap::from([("x".to_string(), [7u8; 32])]);
        let cid = directory_cid(&entries);
        let node = decode_cached(Bytes::from(encode_directory(&entries))).unwrap();
        assert!(node_matches(&node, &cid));
        let meta = b"\x5f\x87".to_vec();
        let fcid = file_cid(&keccak(b"body"), &meta);
        let node = decode_cached(encode_file(&meta, b"body")).unwrap();
        assert!(node_matches(&node, &fcid));
        assert!(!node_matches(
            &decode_cached(encode_file(&meta, b"bodx")).unwrap(),
            &fcid
        ));
        assert!(decode_cached(Bytes::from_static(&[TAG_FILE, 0, 0, 0, 9])).is_none());
        assert!(decode_cached(Bytes::from_static(&[9])).is_none());
        let oversized_metadata = file_header(&vec![0; INODE_REPLY_CAP as usize + 1]);
        assert!(decode_cached(Bytes::from(oversized_metadata)).is_none());
    }

    #[test]
    fn probe_tracks_nested_record_keys_canonically_without_loading_body() {
        let body = b"index body";
        let file = file_cid(&keccak(body), b"");
        let mut child_entries = BTreeMap::new();
        child_entries.insert("index.html".to_string(), file);
        let child = directory_cid(&child_entries);
        let mut root_entries = BTreeMap::new();
        root_entries.insert("nested".to_string(), child);
        let root = directory_cid(&root_entries);
        let records = HashMap::from([
            (root, encode_directory(&root_entries)),
            (child, encode_directory(&child_entries)),
            (file, encode_file(b"", body).to_vec()),
        ]);
        let path = path::split("nested").unwrap();
        let mut load = |cid: &Hash| {
            Ok(records.get(cid).map(|record| {
                (
                    record[..record.len().min(PROBE_PREFIX_CAP)].to_vec(),
                    record.len() as u64,
                )
            }))
        };
        let hint = probe_records(root, &path, 1024, &mut load)
            .unwrap()
            .unwrap();
        assert_eq!(hint.body_bytes, body.len() as u64);
        assert_eq!(
            hint.cache_keys,
            [hex::encode(root), hex::encode(child), hex::encode(file)]
        );
        let upper_root = hex::encode(root).to_uppercase();
        assert_eq!(parse_cid(&upper_root).unwrap(), root);
        assert_eq!(
            hint.cache_keys[0],
            hex::encode(parse_cid(&upper_root).unwrap())
        );
        assert!(hint.scratch_bytes >= body.len() as u64 * 2);
    }

    #[test]
    fn cached_file_body_is_a_shared_slice_of_the_disk_read() {
        let record = encode_file(b"", b"artwork");
        let Node::CachedFile { content, .. } = decode_cached(record.clone()).unwrap() else {
            panic!("file expected");
        };
        assert_eq!(content.as_ptr(), record.as_ptr().wrapping_add(5));
        assert_eq!(content, b"artwork"[..]);
    }

    #[tokio::test]
    async fn cold_queue_and_flight_identities_are_bounded_and_cancellation_removes_them() {
        let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tmp")
            .join(format!("onchfs-flight-{}", std::process::id()));
        let mut config = ContentConfig::mainnet(&root);
        config.max_concurrent_assemblies = 1;
        config.request_timeout = Duration::from_secs(60);
        let cache = Arc::new(DiskCache::open(&root).unwrap());
        let http = Arc::new(Http::new(&config).unwrap());
        let assemblies = Arc::new(Semaphore::new(1));
        let held = assemblies.clone().acquire_owned().await.unwrap();
        let resolver = Arc::new(OnchfsResolver::new(
            &config,
            http,
            cache,
            assemblies.clone(),
        ));
        assert!(resolver.resolution_timeout < Duration::from_secs(40));
        let mut tasks = Vec::new();
        for number in 0..8u8 {
            let resolver = resolver.clone();
            tasks.push(tokio::spawn(async move {
                resolver.resolve(&hex::encode([number; 32]), "").await
            }));
        }
        tokio::time::timeout(Duration::from_secs(2), async {
            while resolver.flights.lock().unwrap().len() != 8 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(
            resolver
                .resolve(&hex::encode([9; 32]), "")
                .await
                .unwrap_err()
                .message
                .contains("queue full")
        );
        for task in tasks {
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
        }
        assert!(resolver.flights.lock().unwrap().is_empty());
        assert_eq!(resolver.requests.available_permits(), 8);
        drop(held);
        assemblies.close();
        assert!(resolver.resolve(&hex::encode([10; 32]), "").await.is_err());
        assert!(resolver.flights.lock().unwrap().is_empty());
        drop(resolver);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn assembly_admission_timeout_is_counted_as_busy() {
        let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tmp")
            .join(format!("onchfs-assembly-timeout-{}", std::process::id()));
        let mut config = ContentConfig::mainnet(&root);
        config.request_timeout = Duration::from_millis(10);
        let cache = Arc::new(DiskCache::open(&root).unwrap());
        let counters = Arc::new(super::super::stats::ContentCounters::default());
        let http = Arc::new(Http::new(&config).unwrap().with_counters(counters.clone()));
        let assemblies = Arc::new(Semaphore::new(1));
        let resolver = OnchfsResolver::new(&config, http, cache, assemblies.clone());
        let held = assemblies.acquire_owned().await.unwrap();

        let error = resolver
            .network(&mut Resolution::default())
            .await
            .unwrap_err();
        assert_eq!(error.kind, super::super::ErrorKind::Unavailable);
        assert_eq!(error.message, "ONCHFS assembly workers busy");
        assert_eq!(
            counters
                .upstream_busy
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
        drop(held);
        drop(resolver);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn resolution_deadline_is_counted_once() {
        let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tmp")
            .join(format!("onchfs-resolution-timeout-{}", std::process::id()));
        let mut config = ContentConfig::mainnet(&root);
        config.request_timeout = Duration::from_secs(1);
        let cache = Arc::new(DiskCache::open(&root).unwrap());
        let counters = Arc::new(super::super::stats::ContentCounters::default());
        let http = Arc::new(Http::new(&config).unwrap().with_counters(counters.clone()));
        let mut resolver = OnchfsResolver::new(&config, http, cache, Arc::new(Semaphore::new(1)));
        resolver.resolution_timeout = Duration::from_millis(1);
        let cid = [7; 32];
        let key = format!("{}/", hex::encode(cid));
        let shared = Arc::new(tokio::sync::Mutex::new(None));
        resolver
            .flights
            .lock()
            .unwrap()
            .insert(key.clone(), shared.clone());
        let guard = shared.lock().await;

        let error = resolver.resolve(&hex::encode(cid), "").await.unwrap_err();
        assert_eq!(error.kind, super::super::ErrorKind::Unavailable);
        assert_eq!(error.message, "ONCHFS resolution timed out");
        assert_eq!(
            counters
                .upstream_timeouts
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );

        drop(guard);
        resolver.flights.lock().unwrap().remove(&key);
        drop(resolver);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn parse_cid_rejects_bad_input() {
        assert!(parse_cid(&"a".repeat(63)).is_err());
        assert!(parse_cid(&"g".repeat(64)).is_err());
        assert!(parse_cid(&"a".repeat(64)).is_ok());
    }
}
