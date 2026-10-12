use std::collections::{BTreeMap, HashMap, VecDeque};
use std::io::Read;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::{
    Router,
    extract::State,
    http::{StatusCode, Uri, header},
    response::Response,
};
use seeded_archive::content::onchfs::{Hash, directory_cid, file_cid, keccak, parse_cid};
use seeded_archive::content::{ContentConfig, ContentStore, ErrorKind, Verification};
use serde_json::{Value, json};

static SEQUENCE: AtomicU64 = AtomicU64::new(0);

struct TestDir(PathBuf);
impl TestDir {
    fn new() -> Self {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tmp/content_onchfs")
            .join(format!(
                "{}-{}",
                std::process::id(),
                SEQUENCE.fetch_add(1, Ordering::Relaxed)
            ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
    fn object(&self, cid: &str) -> PathBuf {
        self.0.join("onchfs").join(&cid[..2]).join(cid)
    }
    fn absent(&self, cid: &str) -> PathBuf {
        self.0.join("absent/onchfs").join(&cid[..2]).join(cid)
    }
}
impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[derive(Clone)]
struct Reply {
    status: u16,
    body: Option<Value>,
    delay: Duration,
}
impl Reply {
    fn status(status: u16) -> Self {
        Self {
            status,
            body: None,
            delay: Duration::ZERO,
        }
    }
    fn json(body: Value) -> Self {
        Self {
            status: 200,
            body: Some(body),
            delay: Duration::ZERO,
        }
    }
}

#[derive(Default)]
struct Data {
    inodes: BTreeMap<String, Value>,
    chunks: BTreeMap<String, String>,
    replies: HashMap<String, VecDeque<Reply>>,
    calls: Vec<String>,
    delay: Duration,
}
#[derive(Clone)]
struct MockState {
    data: Arc<Mutex<Data>>,
    changed: Arc<tokio::sync::Notify>,
}
struct Mock {
    base: String,
    state: MockState,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Mock {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Mock {
    async fn new(data: Data) -> Self {
        let state = MockState {
            data: Arc::new(Mutex::new(data)),
            changed: Arc::new(tokio::sync::Notify::new()),
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let router = Router::new().fallback(respond).with_state(state.clone());
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        Self { base, state, task }
    }
    fn calls(&self) -> usize {
        self.state.data.lock().unwrap().calls.len()
    }
    fn once(&self, key: &str, reply: Reply) {
        self.state
            .data
            .lock()
            .unwrap()
            .replies
            .entry(key.into())
            .or_default()
            .push_back(reply);
    }
    async fn called(&self) {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if self.calls() > 0 {
                    return;
                }
                self.state.changed.notified().await;
            }
        })
        .await
        .unwrap();
    }
    fn config(&self, dir: &TestDir) -> ContentConfig {
        let mut config = ContentConfig::mainnet(&dir.0);
        config.tzkt_base = self.base.clone();
        config.request_timeout = Duration::from_secs(2);
        config.connect_timeout = Duration::from_millis(200);
        config.max_object_bytes = 1024 * 1024;
        config
    }
}

async fn respond(State(state): State<MockState>, uri: Uri) -> Response {
    let reply = {
        let mut data = state.data.lock().unwrap();
        data.calls.push(uri.to_string());
        let inode = uri.path().strip_prefix("/v1/bigmaps/549145/keys/");
        let route = inode.map_or_else(|| "chunks".into(), |cid| format!("inode:{cid}"));
        let overridden = data.replies.get_mut(&route).and_then(VecDeque::pop_front);
        overridden.unwrap_or_else(|| {
            let mut reply = if let Some(cid) = inode {
                data.inodes
                    .get(cid)
                    .cloned()
                    .map(Reply::json)
                    .unwrap_or_else(|| Reply::status(204))
            } else if uri.path() == "/v1/bigmaps/549144/keys" {
                let query: HashMap<_, _> = uri
                    .query()
                    .unwrap_or("")
                    .split('&')
                    .filter_map(|pair| pair.split_once('='))
                    .collect();
                let keys = query.get("key").or_else(|| query.get("key.in")).unwrap();
                let rows: Vec<Value> = keys
                    .split(',')
                    .filter_map(|key| {
                        data.chunks
                            .get(key)
                            .map(|value| json!({"key":key,"value":value,"active":true}))
                    })
                    .collect();
                Reply::json(json!(rows))
            } else {
                Reply::status(404)
            };
            reply.delay = data.delay;
            reply
        })
    };
    state.changed.notify_one();
    tokio::time::sleep(reply.delay).await;
    let body = reply
        .body
        .map_or_else(Vec::new, |body| serde_json::to_vec(&body).unwrap());
    Response::builder()
        .status(StatusCode::from_u16(reply.status).unwrap())
        .header(header::CONTENT_TYPE, "application/json")
        .body(axum::body::Body::from(body))
        .unwrap()
}

fn add_file(data: &mut Data, chunks: &[&[u8]], metadata: &[u8]) -> String {
    let pointers: Vec<String> = chunks
        .iter()
        .map(|chunk| {
            let pointer = hex::encode(keccak(chunk));
            data.chunks.insert(pointer.clone(), hex::encode(chunk));
            pointer
        })
        .collect();
    let body: Vec<u8> = chunks
        .iter()
        .flat_map(|chunk| chunk.iter().copied())
        .collect();
    let cid = hex::encode(file_cid(&keccak(&body), metadata));
    data.inodes.insert(cid.clone(), json!({"active":true,"value":{"file":{"metadata":hex::encode(metadata),"chunk_pointers":pointers}}}));
    cid
}

fn add_directory(data: &mut Data, entries: &[(&str, &str)]) -> String {
    let hashed: BTreeMap<String, Hash> = entries
        .iter()
        .map(|(name, cid)| (name.to_string(), parse_cid(cid).unwrap()))
        .collect();
    let cid = hex::encode(directory_cid(&hashed));
    let named: BTreeMap<_, _> = entries.iter().copied().collect();
    data.inodes.insert(
        cid.clone(),
        json!({"active":true,"value":{"directory":named}}),
    );
    cid
}

fn small_fixture() -> (Data, String, String) {
    let mut data = Data::default();
    let file = add_file(&mut data, &[b"archive fixture"], &[]);
    let root = add_directory(&mut data, &[("index.html", &file)]);
    (data, root, file)
}

#[tokio::test]
async fn cache_read_and_write_failures_do_not_hide_verified_artwork_or_real_absences() {
    let (data, root, _) = small_fixture();
    let mock = Mock::new(data).await;
    let dir = TestDir::new();
    let store = ContentStore::new(mock.config(&dir)).unwrap();
    std::fs::remove_dir(dir.0.join("tmp")).unwrap();
    std::fs::write(dir.0.join("tmp"), b"blocked staging").unwrap();
    std::fs::write(dir.0.join("onchfs"), b"blocked cache reads").unwrap();
    let result = store.get_onchfs(&root, "").await.unwrap();
    assert_eq!(&result.body[..], b"archive fixture");
    assert_eq!(result.verification, Verification::OnchfsKeccak);
    assert!(!result.from_cache);
    assert_eq!(
        store
            .get_onchfs(&"f".repeat(64), "")
            .await
            .unwrap_err()
            .kind,
        ErrorKind::NotFound
    );
    assert_eq!(mock.calls(), 4);
}

#[tokio::test]
async fn duplicate_cold_requests_share_work_even_when_persistent_cache_is_disabled() {
    let (mut data, root, _) = small_fixture();
    data.delay = Duration::from_millis(30);
    let mock = Mock::new(data).await;
    let dir = TestDir::new();
    let mut config = mock.config(&dir);
    config.max_cache_bytes = 0;
    let store = ContentStore::new(config).unwrap();
    let barrier = Arc::new(tokio::sync::Barrier::new(8));
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let store = store.clone();
        let root = root.clone();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            store.get_onchfs(&root, "").await.unwrap()
        }));
    }
    for task in tasks {
        assert_eq!(&task.await.unwrap().body[..], b"archive fixture");
    }
    assert_eq!(
        mock.calls(),
        3,
        "one inode walk and chunk request for all waiters"
    );
    store.get_onchfs(&root, "").await.unwrap();
    assert_eq!(
        mock.calls(),
        3,
        "verified results use the bounded memory cache even without disk writes"
    );
}

#[tokio::test]
async fn warm_hit_does_not_wait_for_a_saturated_network_assembly_slot() {
    let (mut data, _, warm) = small_fixture();
    let cold = add_file(&mut data, &[b"cold"], &[]);
    let delayed = data.inodes[&cold].clone();
    let mock = Mock::new(data).await;
    let dir = TestDir::new();
    let mut config = mock.config(&dir);
    config.max_concurrent_assemblies = 1;
    let store = ContentStore::new(config).unwrap();
    store.get_onchfs(&warm, "").await.unwrap();
    let previous = mock.calls();
    mock.once(
        &format!("inode:{cold}"),
        Reply {
            delay: Duration::from_millis(300),
            ..Reply::json(delayed)
        },
    );
    let worker = store.clone();
    let pending = tokio::spawn(async move { worker.get_onchfs(&cold, "").await });
    tokio::time::timeout(Duration::from_secs(1), async {
        while mock.calls() == previous {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let hit = tokio::time::timeout(Duration::from_millis(150), store.get_onchfs(&warm, ""))
        .await
        .unwrap()
        .unwrap();
    assert!(hit.from_cache);
    assert_eq!(mock.calls(), previous + 1);
    pending.abort();
    assert!(pending.await.unwrap_err().is_cancelled());
}

#[tokio::test]
async fn verified_shared_chunks_are_reused_and_corrupt_chunks_are_refetched() {
    let mut data = Data::default();
    let first = add_file(&mut data, &[b"shared", b"first"], &[]);
    let second = add_file(&mut data, &[b"shared", b"second"], &[]);
    let third = add_file(&mut data, &[b"shared", b"third"], &[]);
    let mock = Mock::new(data).await;
    let dir = TestDir::new();
    let mut config = mock.config(&dir);
    config.chunk_batch = 1;
    let store = ContentStore::new(config).unwrap();
    store.get_onchfs(&first, "").await.unwrap();
    assert_eq!(mock.calls(), 3);
    assert_eq!(
        &store.get_onchfs(&second, "").await.unwrap().body[..],
        b"sharedsecond"
    );
    assert_eq!(mock.calls(), 5);
    let hash = hex::encode(keccak(b"shared"));
    let path = dir.0.join("onchfs-chunk").join(&hash[..2]).join(&hash);
    std::fs::write(&path, b"corrupt").unwrap();
    assert_eq!(
        &store.get_onchfs(&third, "").await.unwrap().body[..],
        b"sharedthird"
    );
    assert_eq!(mock.calls(), 8);
    assert_eq!(std::fs::read(path).unwrap(), b"shared");
}

#[tokio::test]
async fn sequential_successful_fetches_share_one_resolution_deadline_and_can_retry() {
    let mut data = Data {
        delay: Duration::from_millis(35),
        ..Data::default()
    };
    let file = add_file(
        &mut data,
        &[b"a", b"b", b"c", b"d", b"e", b"f", b"g", b"h"],
        &[],
    );
    let mock = Mock::new(data).await;
    let dir = TestDir::new();
    let mut config = mock.config(&dir);
    config.chunk_batch = 1;
    config.request_timeout = Duration::from_millis(60);
    let store = ContentStore::new(config).unwrap();
    let started = std::time::Instant::now();
    let error = store.get_onchfs(&file, "").await.unwrap_err();
    assert_eq!(error.kind, ErrorKind::Unavailable);
    assert!(error.message.contains("resolution timed out"), "{error}");
    assert!(started.elapsed() < Duration::from_secs(1));
    assert!(!dir.object(&file).exists());
    mock.state.data.lock().unwrap().delay = Duration::ZERO;
    assert_eq!(
        &store.get_onchfs(&file, "").await.unwrap().body[..],
        b"abcdefgh"
    );
}

#[tokio::test]
async fn real_arboreal_index_hashes_gzip_headers_and_directory_are_verified() {
    let mut data = Data::default();
    let root: Value = serde_json::from_str(ARBOREAL_ROOT_INODE).unwrap();
    data.inodes.insert(ARBOREAL_ROOT.into(), root);
    let metadata = hex::decode("5f87497ca589d34d1f5a839bd9ab").unwrap();
    let body = hex::decode(ARBOREAL_INDEX_HEX).unwrap();
    let file = add_file(&mut data, &[&body], &metadata);
    assert_eq!(file, ARBOREAL_INDEX);
    let mock = Mock::new(data).await;
    let dir = TestDir::new();
    let store = ContentStore::new(mock.config(&dir)).unwrap();
    let result = store.get_onchfs(ARBOREAL_ROOT, "").await.unwrap();
    assert_eq!(&result.body[..], body);
    assert_eq!(result.content_type.as_deref(), Some("text/html"));
    assert_eq!(result.content_encoding.as_deref(), Some("gzip"));
    assert_eq!(result.verification, Verification::OnchfsKeccak);
    assert_eq!(result.etag, format!("\"{ARBOREAL_INDEX}\""));
    assert!(result.directory_index);
    assert!(!result.from_cache);
    let mut html = String::new();
    flate2::read::GzDecoder::new(&result.body[..])
        .read_to_string(&mut html)
        .unwrap();
    assert!(html.contains("index.js"));
    let index = store.get_onchfs(ARBOREAL_ROOT, "index.html").await.unwrap();
    assert!(!index.directory_index);
    assert!(index.from_cache);
    assert_eq!(mock.calls(), 3);
}

#[tokio::test]
async fn nested_encoded_names_and_repeated_chunks_preserve_file_order() {
    let mut data = Data::default();
    let file = add_file(&mut data, &[b"first", b"second", b"first"], &[]);
    let folder = add_directory(&mut data, &[("my%20file.js", &file)]);
    let root = add_directory(&mut data, &[("scripts", &folder)]);
    let mock = Mock::new(data).await;
    let dir = TestDir::new();
    let store = ContentStore::new(mock.config(&dir)).unwrap();
    let result = store
        .get_onchfs(&root, "scripts/my%20file.js")
        .await
        .unwrap();
    assert_eq!(&result.body[..], b"firstsecondfirst");
    assert!(!result.directory_index);
    let calls = mock.state.data.lock().unwrap().calls.clone();
    assert_eq!(calls.len(), 4);
    assert!(calls.last().unwrap().contains("key.in="));
    assert!(calls.last().unwrap().ends_with("limit=2"));
    let decoded = store.get_onchfs(&root, "scripts/my file.js").await.unwrap();
    assert!(decoded.from_cache);
    assert_eq!(mock.calls(), 4);
}

#[tokio::test]
async fn nested_memory_hit_refreshes_onchfs_record_recency_with_uppercase_root() {
    let mut data = Data::default();
    let file = add_file(&mut data, &[b"hot object"], &[]);
    let nested = add_directory(&mut data, &[("index.html", &file)]);
    let root = add_directory(&mut data, &[("nested", &nested)]);
    let filler = add_file(&mut data, &[], &[]);
    let mock = Mock::new(data).await;
    let dir = TestDir::new();
    let mut config = mock.config(&dir);
    config.max_cache_bytes = 32 * 1024;

    let store = ContentStore::new(config.clone()).unwrap();
    let upper_root = root.to_uppercase();
    assert_eq!(
        &store.get_onchfs(&upper_root, "nested").await.unwrap().body[..],
        b"hot object"
    );
    let requests = mock.calls();
    assert!(
        store
            .get_onchfs(&upper_root, "nested")
            .await
            .unwrap()
            .from_cache
    );
    assert_eq!(mock.calls(), requests, "second read must be a memory hit");
    store.get_onchfs(&filler, "").await.unwrap();
    assert!(store.cache_disk_usage().unwrap() <= config.max_cache_bytes);
    assert!(store.metrics().cache.evictions.unwrap() > 0);
    let requests_after_filler = mock.calls();
    drop(store);

    mock.task.abort();
    let restarted = ContentStore::new(config).unwrap();
    let recovered = restarted.get_onchfs(&upper_root, "nested").await.unwrap();
    assert!(recovered.from_cache);
    assert_eq!(&recovered.body[..], b"hot object");
    assert_eq!(
        mock.calls(),
        requests_after_filler,
        "recovery must stay offline"
    );
}

#[tokio::test]
async fn corrupt_cached_directory_miss_falls_back_to_verified_resolution() {
    let (data, root, _) = small_fixture();
    let mock = Mock::new(data).await;
    let dir = TestDir::new();
    let store = ContentStore::new(mock.config(&dir)).unwrap();
    assert!(store.get_onchfs(&root, "").await.is_ok());
    drop(store);

    // This parses as an empty directory but does not match the root CID.
    std::fs::write(dir.object(&root), b"\0{}").unwrap();
    let requests = mock.calls();
    let restarted = ContentStore::new(mock.config(&dir)).unwrap();
    let repaired = restarted.get_onchfs(&root, "").await.unwrap();
    assert!(!repaired.from_cache);
    assert_eq!(&repaired.body[..], b"archive fixture");
    assert!(
        mock.calls() > requests,
        "corrupt probe must permit upstream repair"
    );
}

#[tokio::test]
async fn verified_cache_survives_restart_and_recovers_from_file_and_directory_corruption() {
    let (data, root, file) = small_fixture();
    let mock = Mock::new(data).await;
    let dir = TestDir::new();
    let store = ContentStore::new(mock.config(&dir)).unwrap();
    store.get_onchfs(&root, "").await.unwrap();
    drop(store);
    let restarted = ContentStore::new(mock.config(&dir)).unwrap();
    assert!(restarted.get_onchfs(&root, "").await.unwrap().from_cache);
    assert_eq!(mock.calls(), 3);
    let mut damaged = std::fs::read(dir.object(&file)).unwrap();
    *damaged.last_mut().unwrap() ^= 1;
    std::fs::write(dir.object(&file), damaged).unwrap();
    assert!(restarted.get_onchfs(&root, "").await.unwrap().from_cache);
    let restarted = ContentStore::new(mock.config(&dir)).unwrap();
    assert!(!restarted.get_onchfs(&root, "").await.unwrap().from_cache);
    assert_eq!(
        mock.calls(),
        4,
        "verified chunks survive whole-file corruption"
    );
    let damaged = String::from_utf8(std::fs::read(dir.object(&root)).unwrap())
        .unwrap()
        .replace(&file, &"0".repeat(64));
    std::fs::write(dir.object(&root), damaged).unwrap();
    let restarted = ContentStore::new(mock.config(&dir)).unwrap();
    assert!(!restarted.get_onchfs(&root, "").await.unwrap().from_cache);
    assert_eq!(mock.calls(), 5);
    assert!(restarted.get_onchfs(&root, "").await.unwrap().from_cache);
}

#[tokio::test]
async fn upstream_404_and_500_are_retryable_and_never_absence_cached() {
    for status in [404, 500] {
        let (data, root, _) = small_fixture();
        let mock = Mock::new(data).await;
        let dir = TestDir::new();
        let attempts = if status == 500 { 3 } else { 1 };
        for _ in 0..attempts {
            mock.once(&format!("inode:{root}"), Reply::status(status));
        }
        let store = ContentStore::new(mock.config(&dir)).unwrap();
        assert_eq!(
            store.get_onchfs(&root, "").await.unwrap_err().kind,
            ErrorKind::Unavailable
        );
        assert_eq!(mock.calls(), attempts);
        assert!(!dir.absent(&root).exists());
        assert_eq!(
            &store.get_onchfs(&root, "").await.unwrap().body[..],
            b"archive fixture"
        );
        assert_eq!(mock.calls(), attempts + 3);
    }
}

#[tokio::test]
async fn transient_rate_limit_and_service_failure_retry_inodes_and_chunks() {
    let (data, root, _) = small_fixture();
    let mock = Mock::new(data).await;
    let dir = TestDir::new();
    mock.once(&format!("inode:{root}"), Reply::status(429));
    mock.once(&format!("inode:{root}"), Reply::status(503));
    mock.once("chunks", Reply::status(503));
    let store = ContentStore::new(mock.config(&dir)).unwrap();
    let content = store.get_onchfs(&root, "").await.unwrap();
    assert_eq!(&content.body[..], b"archive fixture");
    assert_eq!(content.verification, Verification::OnchfsKeccak);
    assert_eq!(mock.calls(), 6);
    assert!(!dir.absent(&root).exists());
    assert!(store.get_onchfs(&root, "").await.unwrap().from_cache);
    assert_eq!(mock.calls(), 6);
}

#[tokio::test]
async fn cancelling_retry_releases_slots_and_leaves_no_cached_failure() {
    let (data, _, file) = small_fixture();
    let mock = Mock::new(data).await;
    let dir = TestDir::new();
    let mut config = mock.config(&dir);
    config.max_concurrent_requests = 1;
    config.max_concurrent_assemblies = 1;
    mock.once(&format!("inode:{file}"), Reply::status(503));
    let store = ContentStore::new(config).unwrap();
    let pending_store = store.clone();
    let pending_file = file.clone();
    let mut pending =
        tokio::spawn(async move { pending_store.get_onchfs(&pending_file, "").await });
    mock.called().await;
    // The immediate response has arrived, but the first retry waits 250 ms.
    assert!(
        tokio::time::timeout(Duration::from_millis(80), &mut pending)
            .await
            .is_err()
    );
    assert_eq!(mock.calls(), 1);
    pending.abort();
    assert!(pending.await.unwrap_err().is_cancelled());
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        mock.calls(),
        1,
        "cancelled retry must not fetch in background"
    );
    assert!(!dir.absent(&file).exists());
    assert!(!dir.object(&file).exists());
    let recovered = tokio::time::timeout(Duration::from_secs(1), store.get_onchfs(&file, ""))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&recovered.body[..], b"archive fixture");
    assert_eq!(mock.calls(), 3);
    assert!(store.get_onchfs(&file, "").await.unwrap().from_cache);
}

#[tokio::test]
async fn confirmed_absence_survives_restart_but_expiry_allows_recovery() {
    for reply in [
        Reply::status(204),
        Reply::json(json!({"active":false,"value":null})),
    ] {
        let (data, root, _) = small_fixture();
        let mock = Mock::new(data).await;
        let dir = TestDir::new();
        mock.once(&format!("inode:{root}"), reply);
        let store = ContentStore::new(mock.config(&dir)).unwrap();
        assert_eq!(
            store.get_onchfs(&root, "").await.unwrap_err().kind,
            ErrorKind::NotFound
        );
        drop(store);
        let restarted = ContentStore::new(mock.config(&dir)).unwrap();
        assert_eq!(
            restarted.get_onchfs(&root, "").await.unwrap_err().kind,
            ErrorKind::NotFound
        );
        assert_eq!(mock.calls(), 1);
        std::fs::write(dir.absent(&root), b"until=0\n").unwrap();
        assert!(restarted.get_onchfs(&root, "").await.is_ok());
        assert_eq!(mock.calls(), 4);
        assert!(!dir.absent(&root).exists());
    }
}

#[tokio::test]
async fn missing_referenced_inode_or_chunk_is_transient_and_recoverable() {
    for child_missing in [true, false] {
        let (data, root, file) = small_fixture();
        let mock = Mock::new(data).await;
        let dir = TestDir::new();
        if child_missing {
            mock.once(&format!("inode:{file}"), Reply::status(204));
        } else {
            mock.once("chunks", Reply::json(json!([])));
        }
        let store = ContentStore::new(mock.config(&dir)).unwrap();
        assert_eq!(
            store.get_onchfs(&root, "").await.unwrap_err().kind,
            ErrorKind::Unavailable
        );
        assert!(!dir.absent(&file).exists());
        assert!(!dir.object(&file).exists());
        assert_eq!(
            &store.get_onchfs(&root, "").await.unwrap().body[..],
            b"archive fixture"
        );
    }
}

#[tokio::test]
async fn excess_duplicate_and_unrequested_chunk_rows_fail_without_poisoning_cache() {
    let row = |bytes: &[u8]| json!({"key":hex::encode(keccak(bytes)),"value":hex::encode(bytes),"active":true});
    for (fault, rows) in [
        (
            "excess",
            json!([row(b"first"), row(b"second"), row(b"extra")]),
        ),
        ("duplicate", json!([row(b"first"), row(b"first")])),
        ("unrequested", json!([row(b"first"), row(b"extra")])),
    ] {
        let mut data = Data::default();
        let file = add_file(&mut data, &[b"first", b"second"], &[]);
        let mock = Mock::new(data).await;
        let dir = TestDir::new();
        mock.once("chunks", Reply::json(rows));
        let store = ContentStore::new(mock.config(&dir)).unwrap();
        assert_eq!(
            store.get_onchfs(&file, "").await.unwrap_err().kind,
            ErrorKind::Unavailable,
            "{fault}"
        );
        assert!(!dir.object(&file).exists(), "{fault}");
        assert!(!dir.absent(&file).exists(), "{fault}");
        let recovered = store.get_onchfs(&file, "").await.unwrap();
        assert_eq!(&recovered.body[..], b"firstsecond", "{fault}");
        assert!(!recovered.from_cache, "{fault}");
        let calls = mock.calls();
        assert!(store.get_onchfs(&file, "").await.unwrap().from_cache);
        assert_eq!(mock.calls(), calls);
    }
}

#[tokio::test]
async fn forged_file_inode_cannot_turn_directory_path_into_confirmed_absence() {
    for nested in [false, true] {
        let (mut data, directory, file) = small_fixture();
        let forged = data.inodes[&file].clone();
        let root = if nested {
            add_directory(&mut data, &[("folder", &directory)])
        } else {
            directory.clone()
        };
        let path = if nested {
            "folder/index.html"
        } else {
            "index.html"
        };
        let mock = Mock::new(data).await;
        let dir = TestDir::new();
        mock.once(&format!("inode:{directory}"), Reply::json(forged));
        let store = ContentStore::new(mock.config(&dir)).unwrap();
        assert_eq!(
            store.get_onchfs(&root, path).await.unwrap_err().kind,
            ErrorKind::Invalid
        );
        assert!(!dir.object(&directory).exists());
        assert!(!dir.object(&file).exists());
        assert!(!dir.absent(&directory).exists());
        assert!(!dir.absent(&root).exists());
        let recovered = store.get_onchfs(&root, path).await.unwrap();
        assert_eq!(&recovered.body[..], b"archive fixture");
        assert_eq!(recovered.verification, Verification::OnchfsKeccak);
        assert!(!recovered.from_cache);
        assert!(store.get_onchfs(&root, path).await.unwrap().from_cache);
    }
}

#[tokio::test]
async fn corrupt_directory_chunk_and_file_hashes_never_become_cache_hits() {
    for fault in ["directory", "chunk", "file"] {
        let (data, root, file) = small_fixture();
        let mock = Mock::new(data).await;
        let dir = TestDir::new();
        match fault {
            "directory" => mock.once(&format!("inode:{root}"), Reply::json(json!({"active":true,"value":{"directory":{}}}))),
            "chunk" => mock.once("chunks", Reply::json(json!([{"key":hex::encode(keccak(b"archive fixture")),"value":hex::encode(b"corrupt"),"active":true}]))),
            _ => { let mut inode = mock.state.data.lock().unwrap().inodes[&file].clone(); inode["value"]["file"]["metadata"] = json!("88"); mock.once(&format!("inode:{file}"), Reply::json(inode)); }
        }
        let store = ContentStore::new(mock.config(&dir)).unwrap();
        assert_eq!(
            store.get_onchfs(&root, "").await.unwrap_err().kind,
            ErrorKind::Invalid,
            "{fault}"
        );
        assert!(!dir.object(&file).exists());
        assert!(!dir.absent(&root).exists());
        assert!(store.get_onchfs(&root, "").await.is_ok());
    }
}

#[tokio::test]
async fn malformed_incomplete_indexer_records_remain_retryable() {
    for malformed in [
        json!({}),
        json!({"active":true}),
        json!({"value":{"directory":{}}}),
        json!({"active":true,"value":{"file":{"metadata":"zz","chunk_pointers":[]}}}),
    ] {
        let (data, root, _) = small_fixture();
        let mock = Mock::new(data).await;
        let dir = TestDir::new();
        mock.once(&format!("inode:{root}"), Reply::json(malformed));
        let store = ContentStore::new(mock.config(&dir)).unwrap();
        assert_eq!(
            store.get_onchfs(&root, "").await.unwrap_err().kind,
            ErrorKind::Unavailable
        );
        assert!(!dir.absent(&root).exists());
        assert!(store.get_onchfs(&root, "").await.is_ok());
    }
}

#[tokio::test]
async fn invalid_paths_and_cids_fail_before_network_access() {
    let (data, root, _) = small_fixture();
    let mock = Mock::new(data).await;
    let dir = TestDir::new();
    let store = ContentStore::new(mock.config(&dir)).unwrap();
    for path in [
        "../index.html",
        "%2e%2e",
        "a%2fb",
        "a\\b",
        "%zz",
        "index.html?fxhash=other",
        "index.html#params",
    ] {
        assert_eq!(
            store.get_onchfs(&root, path).await.unwrap_err().kind,
            ErrorKind::BadRequest
        );
    }
    assert_eq!(
        store.get_onchfs("not-a-cid", "").await.unwrap_err().kind,
        ErrorKind::Invalid
    );
    assert_eq!(mock.calls(), 0);
}

#[tokio::test]
async fn verified_missing_path_and_directory_without_index_are_not_found() {
    let (mut data, root, file) = small_fixture();
    let no_index = add_directory(&mut data, &[("other.js", &file)]);
    let mock = Mock::new(data).await;
    let dir = TestDir::new();
    let store = ContentStore::new(mock.config(&dir)).unwrap();
    assert_eq!(
        store
            .get_onchfs(&root, "missing.js")
            .await
            .unwrap_err()
            .kind,
        ErrorKind::NotFound
    );
    assert_eq!(
        store.get_onchfs(&no_index, "").await.unwrap_err().kind,
        ErrorKind::NotFound
    );
    assert_eq!(mock.calls(), 2);
    assert!(!dir.absent(&root).exists());
    assert!(!dir.absent(&no_index).exists());
}

#[tokio::test]
async fn timeout_and_cancellation_release_request_and_assembly_slots() {
    let (data, _, file) = small_fixture();
    let mock = Mock::new(data).await;
    let dir = TestDir::new();
    let mut config = mock.config(&dir);
    config.request_timeout = Duration::from_millis(80);
    config.max_concurrent_requests = 1;
    config.max_concurrent_assemblies = 1;
    mock.once(
        &format!("inode:{file}"),
        Reply {
            delay: Duration::from_millis(300),
            ..Reply::status(500)
        },
    );
    let store = ContentStore::new(config).unwrap();
    assert_eq!(
        store.get_onchfs(&file, "").await.unwrap_err().kind,
        ErrorKind::Unavailable
    );
    assert!(
        tokio::time::timeout(Duration::from_secs(1), store.get_onchfs(&file, ""))
            .await
            .unwrap()
            .is_ok()
    );

    let (data, _, file) = small_fixture();
    let mock = Mock::new(data).await;
    let dir = TestDir::new();
    let mut config = mock.config(&dir);
    config.max_concurrent_requests = 1;
    config.max_concurrent_assemblies = 1;
    mock.once(
        &format!("inode:{file}"),
        Reply {
            delay: Duration::from_millis(500),
            ..Reply::status(500)
        },
    );
    let store = ContentStore::new(config).unwrap();
    let first_store = store.clone();
    let first_file = file.clone();
    let first = tokio::spawn(async move { first_store.get_onchfs(&first_file, "").await });
    mock.called().await;
    let next_store = store.clone();
    let next_file = file.clone();
    let queued = tokio::spawn(async move { next_store.get_onchfs(&next_file, "").await });
    tokio::task::yield_now().await;
    assert_eq!(mock.calls(), 1);
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    assert!(
        tokio::time::timeout(Duration::from_secs(1), queued)
            .await
            .unwrap()
            .unwrap()
            .is_ok()
    );
    assert!(!dir.absent(&file).exists());
    assert!(store.get_onchfs(&file, "").await.unwrap().from_cache);
}

#[tokio::test]
async fn object_limits_and_cache_saturation_fail_or_bypass_without_poisoning() {
    let (data, root, file) = small_fixture();
    let mock = Mock::new(data).await;
    let dir = TestDir::new();
    let mut config = mock.config(&dir);
    config.max_object_bytes = 5;
    let limited = ContentStore::new(config).unwrap();
    assert_eq!(
        limited.get_onchfs(&root, "").await.unwrap_err().kind,
        ErrorKind::TooLarge
    );
    assert!(!dir.object(&file).exists());
    let recovered = ContentStore::new(mock.config(&dir)).unwrap();
    assert!(recovered.get_onchfs(&root, "").await.is_ok());
    let dir = TestDir::new();
    let mut config = mock.config(&dir);
    config.max_cache_bytes = 1;
    let no_cache = ContentStore::new(config).unwrap();
    assert!(!no_cache.get_onchfs(&root, "").await.unwrap().from_cache);
    assert!(no_cache.get_onchfs(&root, "").await.unwrap().from_cache);
    assert_eq!(no_cache.cache_disk_usage().unwrap(), 0);
}

#[tokio::test]
async fn stricter_object_limit_is_enforced_on_warm_cache_after_restart() {
    let (data, root, _) = small_fixture();
    let mock = Mock::new(data).await;
    let dir = TestDir::new();
    let store = ContentStore::new(mock.config(&dir)).unwrap();
    store.get_onchfs(&root, "").await.unwrap();
    drop(store);
    let mut config = mock.config(&dir);
    config.max_object_bytes = 5;
    let limited = ContentStore::new(config).unwrap();
    assert_eq!(
        limited.get_onchfs(&root, "").await.unwrap_err().kind,
        ErrorKind::TooLarge
    );
}

#[tokio::test]
async fn corrupt_absence_marker_is_discarded_and_refetched() {
    let (data, root, _) = small_fixture();
    let mock = Mock::new(data).await;
    let dir = TestDir::new();
    mock.once(&format!("inode:{root}"), Reply::status(204));
    let store = ContentStore::new(mock.config(&dir)).unwrap();
    assert_eq!(
        store.get_onchfs(&root, "").await.unwrap_err().kind,
        ErrorKind::NotFound
    );
    std::fs::write(dir.absent(&root), [0xff, 0xfe]).unwrap();
    assert!(store.get_onchfs(&root, "").await.is_ok());
}

#[tokio::test]
#[ignore = "Explicit live TzKT smoke test; bounded to 25 seconds"]
async fn live_arboreal_root_and_index() {
    let dir = TestDir::new();
    let mut config = ContentConfig::mainnet(&dir.0);
    config.request_timeout = Duration::from_secs(8);
    config.connect_timeout = Duration::from_secs(3);
    let store = ContentStore::new(config).unwrap();
    tokio::time::timeout(Duration::from_secs(25), async {
        let root = store.get_onchfs(ARBOREAL_ROOT, "").await.unwrap();
        assert_eq!(root.etag, format!("\"{ARBOREAL_INDEX}\""));
        assert_eq!(root.content_encoding.as_deref(), Some("gzip"));
        assert_eq!(root.body.as_ref(), hex::decode(ARBOREAL_INDEX_HEX).unwrap());
        let index = store.get_onchfs(ARBOREAL_ROOT, "index.html").await.unwrap();
        assert!(index.from_cache);
        assert_eq!(root.body, index.body);
    })
    .await
    .expect("bounded live ONCHFS check");
}

// Public Arboreal TzKT inode and oracle index bytes, captured 2026-10-03.
const ARBOREAL_ROOT: &str = "a71e98fced01c865f33be138100b5ea5486f32fc333ad1503f814b201234130c";
const ARBOREAL_INDEX: &str = "cd702710cf098b0d4ff8e8d1c7153845da428239fd7291e98dd101fe8485654e";
const ARBOREAL_ROOT_INODE: &str = r#"{"id":55992735,"active":true,"hash":"exprtqKbVoUiRuiMFjoE7w1Qr9ABbaVuxRB1UgXh2QT5GLX8E29PqV","key":"a71e98fced01c865f33be138100b5ea5486f32fc333ad1503f814b201234130c","value":{"directory":{".vscode":"cf146f09d212a4cc22fab6c1037f63a23e3897a3a06027a08aadb5a533a6feb1","LICENSE":"90af53b0dda1d81a49200f4e29e160990a84a99507af16bd5d7ed954d4219012","Tree.js":"101e717ea785cf8bf0d8a528e900e0fb122399a1f09dfc126daabb938eb81509","Curve.js":"813f35c60b425f96e2f89d2e4137939d3e6c70af235d29f7864573b7a893863a","index.js":"50927bcb6a978f7f97489b8cd6c4c651dbe70141ee0b94e1eb1aa7c2d957bff1","utils.js":"6ce0d4eeed8222eb007e1185e633f3af1af3bc1a22172b65ed6c61fafbc0ebdc","Branch.js":"7a32138c5cf00ef8bae071c643dd63ee9def04a9c784a247937e1f3ebfa2d36b","fxhash.js":"5c9b0f3896af633991cac20a6968c637baf46e8fc99393fc006b247b46e94e29","p5.min.js":"165c6f1e5ae4fd0c2b30e773b7d6bdc00a9d5981880ef2fb5313d5a3ceb442fd","style.css":"d8addd514e84bb226d7ffb48d4e522d3871395376ce69cf065ecce4670b34a1a",".gitignore":"01564c7eb083224a3693380db7d9ce8fafd53c015cd432bc70ce4db7209ca0b2","index.html":"cd702710cf098b0d4ff8e8d1c7153845da428239fd7291e98dd101fe8485654e",".prettierrc":"7f78b355a1961ee42b3f3798465e3ed73e7a30287dc8ab8ec1c6cec4e6282c8c","geometry.js":"eaa50ac0e0cb9fef4aec8ecaa1f831f36b5bf834422487f758b1ba1e5b9d205d","painting.js":"d8ed644890406df03fc9bc70d46f78bc9a6d26f8c5ac60951de117a9afeaa60b",".eslintrc.json":"bab4d48707b58ed0f6fc9dcd95034dfb442d136e1a39adc0914f736b42f144a8"}},"firstLevel":4556419,"lastLevel":4556419,"updates":1}"#;
const ARBOREAL_INDEX_HEX: &str = "1f8b08000000000000038d923f6f833010c5f77c8a8b7760aad4c144cabfa9433344aa3a1a73c44e8d41e7230ddfbe8093a1121299fc7cef77a7679de5faf0b93f7f9f8e60b8769b958c078034a8ca510c922d3bdc6ca96808959359bc47af4656a08da2809c8b8eabe45d3c2c67fd0f10ba5c04ee1d0683c8020c61958b349b6aa90ee189074db66508a447bb7d4b6bebd3eb60cb2c5ab35c75372a9865ae63ebc23276c1667811f5cbe4bea31b2e633b525ebf90ef4cf8c2b45659cfd65ffe9300e3c2b2e7c664d194fd6cbbf525de677a07709d24f075dc9db6fb0ff8b5ce81f557d40c6c108ace976e4c07060921491ec3b5f23715c096b988729c1bd594238b418664d3a75afd0158f5cc196d020000";
