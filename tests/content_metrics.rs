use std::path::PathBuf;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use axum::{Router, extract::State, http::StatusCode, response::Response};
use cid::Cid;
use seeded_archive::content::{ContentConfig, ContentStore};
use sha2::{Digest, Sha256};

struct Mock {
    base: String,
    calls: Arc<AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Mock {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn failing_gateway() -> Mock {
    async fn respond(State(calls): State<Arc<AtomicUsize>>) -> Response {
        calls.fetch_add(1, Ordering::Relaxed);
        Response::builder()
            .status(StatusCode::BAD_GATEWAY)
            .body(axum::body::Body::empty())
            .unwrap()
    }
    let calls = Arc::new(AtomicUsize::new(0));
    let router = Router::new().fallback(respond).with_state(calls.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}/ipfs/", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    Mock { base, calls, task }
}

fn temp() -> PathBuf {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(format!(
        "tmp/content_metrics-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&path);
    std::fs::create_dir_all(&path).unwrap();
    path
}

#[tokio::test]
async fn failed_upstream_attempts_are_counted_without_exposing_request_identifiers() {
    let mock = failing_gateway().await;
    let root = temp();
    let mut config = ContentConfig::mainnet(&root);
    config.ipfs_gateways = vec![mock.base.clone()];
    config.connect_timeout = Duration::from_millis(200);
    config.request_timeout = Duration::from_secs(1);
    let store = ContentStore::new(config).unwrap();
    let digest = Sha256::digest(b"private-test-object");
    let cid = Cid::new_v1(
        0x55,
        cid::multihash::Multihash::wrap(0x12, &digest).unwrap(),
    )
    .to_string();

    assert!(store.get_ipfs(&cid, "private-path").await.is_err());
    let metrics = store.metrics();
    assert_eq!(metrics.cache.upstream_fetches, 1);
    assert_eq!(metrics.cache.upstream_errors, 1);
    assert_eq!(metrics.local_delivery.bytes_used, 0);
    assert_eq!(metrics.local_delivery.waiting, 0);
    assert_eq!(metrics.local_scratch.bytes_used, 0);
    assert_eq!(metrics.local_scratch.waiting, 0);
    assert_eq!(metrics.local_scratch.bytes_quota, 512 << 20);
    assert_eq!(metrics.local_delivery.bytes_quota, 512 << 20);
    assert_eq!(metrics.local_delivery.wait_capacity, 256);
    assert_eq!(mock.calls.load(Ordering::Relaxed), 1);
    let json = serde_json::to_string(&metrics).unwrap();
    assert!(!json.contains(&cid));
    assert!(!json.contains("private-path"));

    drop(store);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn direct_mirror_reads_increment_local_and_mirror_hits() {
    let root = temp();
    let body = b"direct mirror object";
    let digest = Sha256::digest(body);
    let cid = Cid::new_v1(
        0x55,
        cid::multihash::Multihash::wrap(0x12, &digest).unwrap(),
    );
    let key = hex::encode(cid.to_bytes());
    let object = root
        .join("mirror/ipfs")
        .join(&key[key.len() - 2..])
        .join(&key);
    std::fs::create_dir_all(object.parent().unwrap()).unwrap();
    std::fs::write(&object, body).unwrap();

    let mut config = ContentConfig::mainnet(root.join("cache"));
    config.mirror_dir = Some(root.join("mirror"));
    let store = ContentStore::new(config).unwrap();
    let content = store
        .get_local("ipfs", &cid.to_string(), "")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(content.body.as_ref(), body);
    let metrics = store.metrics();
    assert_eq!(metrics.cache.hits, 1);
    assert_eq!(metrics.cache.misses, 0);
    assert_eq!(metrics.mirror.hits, 1);
    assert_eq!(metrics.cache.upstream_fetches, 0);
    assert_eq!(metrics.local_delivery.bytes_used, 4096);
    assert_eq!(metrics.local_delivery.waiting, 0);
    assert_eq!(metrics.local_scratch.bytes_used, 0);
    assert_eq!(metrics.local_scratch.waiting, 0);
    assert_eq!(metrics.local_scratch.bytes_quota, 512 << 20);
    drop(content);
    assert_eq!(store.metrics().local_delivery.bytes_used, 0);

    drop(store);
    std::fs::remove_dir_all(root).unwrap();
}
