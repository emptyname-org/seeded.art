use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use axum::{
    Router,
    extract::State,
    http::{StatusCode, header},
    response::Response,
};
use cid::Cid;
use seeded_archive::content::{ContentConfig, ContentStore, ErrorKind, Verification};
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

#[derive(Clone)]
struct Reply {
    status: u16,
    kind: &'static str,
    body: Vec<u8>,
    delay: Duration,
    retry: Option<&'static str>,
}

fn car_reply(body: Vec<u8>) -> Reply {
    Reply {
        status: 200,
        kind: "application/vnd.ipld.car",
        body,
        delay: Duration::ZERO,
        retry: None,
    }
}

async fn mock(replies: Vec<Reply>) -> Mock {
    let calls = Arc::new(AtomicUsize::new(0));
    async fn respond(
        State((calls, replies)): State<(Arc<AtomicUsize>, Arc<Vec<Reply>>)>,
    ) -> Response {
        let n = calls.fetch_add(1, Ordering::SeqCst).min(replies.len() - 1);
        let reply = &replies[n];
        tokio::time::sleep(reply.delay).await;
        let mut response = Response::builder()
            .status(StatusCode::from_u16(reply.status).unwrap())
            .header(header::CONTENT_TYPE, reply.kind);
        if let Some(retry) = reply.retry {
            response = response.header(header::RETRY_AFTER, retry);
        }
        response
            .body(axum::body::Body::from(reply.body.clone()))
            .unwrap()
    }
    let router = Router::new()
        .fallback(respond)
        .with_state((calls.clone(), Arc::new(replies)));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}/ipfs/", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    Mock { base, calls, task }
}

fn temp(name: &str) -> PathBuf {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tmp/content_ipfs")
        .join(format!("{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);
    std::fs::create_dir_all(&path).unwrap();
    path
}

fn config(path: &Path, mocks: &[&Mock]) -> ContentConfig {
    let mut config = ContentConfig::mainnet(path);
    config.ipfs_gateways = mocks.iter().map(|m| m.base.clone()).collect();
    config.request_timeout = Duration::from_millis(300);
    config.connect_timeout = Duration::from_millis(100);
    config.breaker.failure_threshold = 20;
    config.breaker.base_cooldown = Duration::from_millis(30);
    config.breaker.max_cooldown = Duration::from_secs(1);
    config
}

fn varint(mut n: u64) -> Vec<u8> {
    let mut out = Vec::new();
    while n >= 128 {
        out.push(n as u8 | 128);
        n >>= 7;
    }
    out.push(n as u8);
    out
}

fn field(tag: u8, bytes: &[u8]) -> Vec<u8> {
    let mut out = vec![tag];
    out.extend(varint(bytes.len() as u64));
    out.extend(bytes);
    out
}

fn block(codec: u64, bytes: Vec<u8>) -> (Cid, Vec<u8>) {
    let digest = Sha256::digest(&bytes);
    (
        Cid::new_v1(
            codec,
            cid::multihash::Multihash::wrap(0x12, &digest).unwrap(),
        ),
        bytes,
    )
}

fn car(root: Cid, blocks: &[(Cid, Vec<u8>)]) -> Vec<u8> {
    let mut header = vec![0xa2, 0x65];
    header.extend(b"roots");
    header.extend([0x81, 0xd8, 0x2a]);
    let length = root.to_bytes().len() + 1;
    if length < 24 {
        header.push(0x40 + length as u8);
    } else {
        header.extend([0x58, length as u8]);
    }
    header.push(0);
    header.extend(root.to_bytes());
    header.push(0x67);
    header.extend(b"version");
    header.push(1);
    let mut out = varint(header.len() as u64);
    out.extend(header);
    for (cid, data) in blocks {
        out.extend(varint((cid.to_bytes().len() + data.len()) as u64));
        out.extend(cid.to_bytes());
        out.extend(data);
    }
    out
}

fn directory(names: &[&str], child: Cid) -> (Cid, Vec<u8>) {
    let mut node = field(10, &[8, 1]);
    for name in names {
        let mut link = field(10, &child.to_bytes());
        link.extend(field(18, name.as_bytes()));
        node.extend(field(18, &link));
    }
    block(0x70, node)
}

#[tokio::test]
async fn verified_car_survives_restart_and_corruption_triggers_refetch() {
    let raw = block(0x55, b"<!doctype html><p>archive</p>".to_vec());
    let mock = mock(vec![car_reply(car(raw.0, std::slice::from_ref(&raw)))]).await;
    let root = temp("cache_restart");
    let cfg = config(&root, &[&mock]);
    let first = ContentStore::new(cfg.clone())
        .unwrap()
        .get_ipfs(&raw.0.to_string(), "")
        .await
        .unwrap();
    assert_eq!(first.verification, Verification::IpfsCar);
    assert_eq!(first.body.as_ref(), raw.1);
    assert_eq!(
        first.content_type.as_deref(),
        Some("text/html; charset=utf-8")
    );
    let second = ContentStore::new(cfg.clone())
        .unwrap()
        .get_ipfs(&raw.0.to_string(), "")
        .await
        .unwrap();
    assert!(second.from_cache);
    assert_eq!(mock.calls.load(Ordering::SeqCst), 1);
    for dir in std::fs::read_dir(root.join("ipfs-car-v1")).unwrap() {
        for file in std::fs::read_dir(dir.unwrap().path()).unwrap() {
            std::fs::write(file.unwrap().path(), b"corrupt").unwrap();
        }
    }
    let third = ContentStore::new(cfg)
        .unwrap()
        .get_ipfs(&raw.0.to_string(), "")
        .await
        .unwrap();
    assert!(!third.from_cache);
    assert_eq!(mock.calls.load(Ordering::SeqCst), 2);
    assert_eq!(std::fs::read_dir(root.join("tmp")).unwrap().count(), 0);
}

#[tokio::test]
async fn full_preview_cache_rotates_verified_images_and_serves_them_offline_after_restart() {
    let first = block(0x55, b"\x89PNG\r\n\x1a\nfirst".to_vec());
    let second = block(0x55, b"\x89PNG\r\n\x1a\nsecond".to_vec());
    let upstream = mock(vec![
        car_reply(car(first.0, std::slice::from_ref(&first))),
        car_reply(car(second.0, std::slice::from_ref(&second))),
    ])
    .await;
    let root = temp("rotating_previews");
    let mut cfg = config(&root, &[&upstream]);
    cfg.max_cache_bytes = 8192;
    let store = ContentStore::new(cfg.clone()).unwrap();
    store.get_ipfs(&first.0.to_string(), "").await.unwrap();
    let image = store.get_ipfs(&second.0.to_string(), "").await.unwrap();
    assert_eq!(image.body.as_ref(), second.1);
    assert_eq!(image.verification, Verification::IpfsCar);
    assert_eq!(store.metrics().cache.evictions, Some(1));
    assert_eq!(store.cache_disk_usage().unwrap(), 8192);
    drop(store);
    upstream.task.abort();
    let store = ContentStore::new(cfg).unwrap();
    let image = store
        .get_local("ipfs", &second.0.to_string(), "")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(image.body.as_ref(), second.1);
    assert_eq!(image.verification, Verification::IpfsCar);
    assert!(image.from_cache);
    assert!(
        store
            .get_local("ipfs", &first.0.to_string(), "")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(upstream.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn memory_hit_refreshes_disk_recency_before_lru_eviction() {
    let cold = block(0x55, b"\x89PNG\r\ncold".to_vec());
    let hot = block(0x55, b"\x89PNG\r\nhot".to_vec());
    let newcomer = block(0x55, b"\x89PNG\r\nnew".to_vec());
    let upstream = mock(vec![
        car_reply(car(hot.0, std::slice::from_ref(&hot))),
        car_reply(car(cold.0, std::slice::from_ref(&cold))),
        car_reply(car(newcomer.0, std::slice::from_ref(&newcomer))),
    ])
    .await;
    let root = temp("memory_hit_lru");
    let mut cfg = config(&root, &[&upstream]);
    cfg.max_cache_bytes = 16_384;
    let store = ContentStore::new(cfg.clone()).unwrap();

    store.get_ipfs(&hot.0.to_string(), "").await.unwrap();
    store.get_ipfs(&cold.0.to_string(), "").await.unwrap();
    let hot_hit = store.get_ipfs(&hot.0.to_string(), "").await.unwrap();
    assert!(hot_hit.from_cache);
    assert_eq!(hot_hit.body.as_ref(), hot.1);
    assert_eq!(upstream.calls.load(Ordering::SeqCst), 2);
    assert_eq!(store.metrics().cache.hits, 1);
    assert_eq!(store.metrics().cache.misses, 2);

    let third = store.get_ipfs(&newcomer.0.to_string(), "").await.unwrap();
    assert_eq!(third.body.as_ref(), newcomer.1);
    assert_eq!(store.metrics().cache.evictions, Some(1));
    assert_eq!(store.cache_disk_usage().unwrap(), 16_384);
    drop(store);
    upstream.task.abort();

    let restarted = ContentStore::new(cfg).unwrap();
    let hot = restarted.get_ipfs(&hot.0.to_string(), "").await.unwrap();
    assert!(hot.from_cache);
    assert_eq!(hot.body.as_ref(), b"\x89PNG\r\nhot");
    assert_eq!(restarted.metrics().cache.hits, 1);
    assert!(
        restarted
            .get_local("ipfs", &cold.0.to_string(), "")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(restarted.metrics().cache.misses, 1);
    assert_eq!(upstream.calls.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn directory_path_and_index_are_anchored_to_requested_root() {
    let file = block(0x55, b"<html>index</html>".to_vec());
    let dir = directory(&["index.html", "other.html"], file.0);
    // Real trustless gateways may name the resolved file in CAR header roots.
    // The request's root block and directory links must still be proven.
    let proof = car(file.0, &[dir.clone(), file.clone()]);
    let mock = mock(vec![car_reply(proof)]).await;
    let store = ContentStore::new(config(&temp("directory"), &[&mock])).unwrap();
    let page = store.get_ipfs(&dir.0.to_string(), "").await.unwrap();
    assert!(page.directory_index);
    assert_eq!(page.body.as_ref(), file.1);
    let other = store
        .get_ipfs(&dir.0.to_string(), "other.html")
        .await
        .unwrap();
    assert!(!other.directory_index);
    let error = store
        .get_ipfs(&dir.0.to_string(), "missing.html")
        .await
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::NotFound);
}

#[tokio::test]
async fn extensionless_verified_objects_get_sniffed_media_types() {
    let cases: &[(&str, &[u8], &str)] = &[
        ("png", b"\x89PNG\r\n\x1a\nrest", "image/png"),
        ("jpeg", b"\xff\xd8\xffrest", "image/jpeg"),
        ("gif", b"GIF89a", "image/gif"),
        ("webp", b"RIFFxxxxWEBP", "image/webp"),
        (
            "svg",
            b"<?xml version=\"1.0\"?><svg></svg>",
            "image/svg+xml",
        ),
        ("mp4", b"xxxxftypisom", "video/mp4"),
        ("ogg", b"OggS", "audio/ogg"),
        ("mp3", b"ID3", "audio/mpeg"),
    ];
    for (name, bytes, content_type) in cases {
        let file = block(0x55, bytes.to_vec());
        let gateway = mock(vec![car_reply(car(file.0, std::slice::from_ref(&file)))]).await;
        let store = ContentStore::new(config(&temp(&format!("mime_{name}")), &[&gateway])).unwrap();
        let resolved = store.get_ipfs(&file.0.to_string(), "").await.unwrap();
        assert_eq!(
            resolved.content_type.as_deref(),
            Some(*content_type),
            "{name}"
        );
    }
}

#[tokio::test]
async fn car_framing_headroom_preserves_near_limit_objects_and_cached_proofs() {
    let bytes = vec![0x5a; 4096];
    let file = block(0x55, bytes.clone());
    let proof = car(file.0, std::slice::from_ref(&file));
    assert!(proof.len() as u64 > bytes.len() as u64);
    let gateway = mock(vec![car_reply(proof)]).await;
    let root = temp("car_framing_headroom");
    let mut cfg = config(&root, &[&gateway]);
    cfg.max_object_bytes = bytes.len() as u64;

    let first = ContentStore::new(cfg.clone())
        .unwrap()
        .get_ipfs(&file.0.to_string(), "")
        .await
        .unwrap();
    assert_eq!(first.body.as_ref(), bytes);
    assert_eq!(first.verification, Verification::IpfsCar);
    assert!(!first.from_cache);

    let cached = ContentStore::new(cfg)
        .unwrap()
        .get_ipfs(&file.0.to_string(), "")
        .await
        .unwrap();
    assert_eq!(cached.body.as_ref(), bytes);
    assert!(cached.from_cache);
    assert_eq!(gateway.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn car_headroom_does_not_raise_the_cap_for_unverified_plain_replies() {
    let file = block(0x55, b"cid data".to_vec());
    let gateway = mock(vec![Reply {
        kind: "application/octet-stream",
        body: vec![0; 128],
        ..car_reply(vec![])
    }])
    .await;
    let mut cfg = config(&temp("plain_headroom"), &[&gateway]);
    cfg.max_object_bytes = 8;
    let error = ContentStore::new(cfg)
        .unwrap()
        .get_ipfs(&file.0.to_string(), "")
        .await
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::TooLarge);
}

#[tokio::test]
async fn directory_entity_fetches_a_second_car_for_index_content() {
    let file = block(0x55, b"<html>index</html>".to_vec());
    let dir = directory(&["index.html"], file.0);
    let mock = mock(vec![
        car_reply(car(dir.0, std::slice::from_ref(&dir))),
        car_reply(car(dir.0, &[dir.clone(), file])),
    ])
    .await;
    let store = ContentStore::new(config(&temp("second_car"), &[&mock])).unwrap();
    assert!(
        store
            .get_ipfs(&dir.0.to_string(), "")
            .await
            .unwrap()
            .directory_index
    );
    assert_eq!(mock.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn retry_after_opens_breaker_and_falls_back_without_negative_cache() {
    let file = block(0x55, b"ok".to_vec());
    let dir = directory(&["a", "b"], file.0);
    let first = mock(vec![Reply {
        status: 429,
        retry: Some("60"),
        ..car_reply(vec![])
    }])
    .await;
    let second = mock(vec![car_reply(car(dir.0, &[dir.clone(), file]))]).await;
    let store = ContentStore::new(config(&temp("breaker"), &[&first, &second])).unwrap();
    for path in ["a", "b"] {
        assert_eq!(
            store
                .get_ipfs(&dir.0.to_string(), path)
                .await
                .unwrap()
                .verification,
            Verification::IpfsCar
        );
    }
    assert_eq!(first.calls.load(Ordering::SeqCst), 1);
    assert_eq!(second.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn gateway_404_is_transient_and_retry_can_succeed() {
    let file = block(0x55, b"retry".to_vec());
    let mock = mock(vec![
        Reply {
            status: 404,
            ..car_reply(vec![])
        },
        car_reply(car(file.0, std::slice::from_ref(&file))),
    ])
    .await;
    let store = ContentStore::new(config(&temp("not_found"), &[&mock])).unwrap();
    assert_eq!(
        store
            .get_ipfs(&file.0.to_string(), "")
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Unavailable
    );
    assert!(store.get_ipfs(&file.0.to_string(), "").await.is_ok());
    assert_eq!(mock.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn timeout_falls_back_and_dropped_resolution_releases_capacity() {
    let file = block(0x55, b"bounded".to_vec());
    let proof = car(file.0, std::slice::from_ref(&file));
    let slow = mock(vec![Reply {
        delay: Duration::from_millis(200),
        ..car_reply(proof.clone())
    }])
    .await;
    let fast = mock(vec![car_reply(proof)]).await;
    let mut cfg = config(&temp("timeout"), &[&slow, &fast]);
    cfg.request_timeout = Duration::from_millis(40);
    cfg.max_concurrent_assemblies = 1;
    let store = ContentStore::new(cfg).unwrap();
    let task_store = store.clone();
    let cid = file.0.to_string();
    let task_cid = cid.clone();
    let task = tokio::spawn(async move { task_store.get_ipfs(&task_cid, "").await });
    while slow.calls.load(Ordering::SeqCst) == 0 {
        tokio::task::yield_now().await;
    }
    task.abort();
    let _ = task.await;
    assert_eq!(
        store.get_ipfs(&cid, "").await.unwrap().body.as_ref(),
        file.1
    );
}

#[tokio::test]
async fn hanging_object_does_not_open_gateway_breaker_for_the_next_object() {
    let missing = block(0x55, b"unavailable-object".to_vec());
    let present = block(0x55, b"available-object".to_vec());
    let gateway = mock(vec![
        Reply {
            delay: Duration::from_secs(2),
            ..car_reply(vec![])
        },
        car_reply(car(present.0, std::slice::from_ref(&present))),
    ])
    .await;
    let mut cfg = config(&temp("object_timeout_neutral"), &[&gateway]);
    cfg.request_timeout = Duration::from_millis(100);
    cfg.breaker.failure_threshold = 1;
    cfg.breaker.base_cooldown = Duration::from_secs(1);
    let store = ContentStore::new(cfg).unwrap();
    assert_eq!(
        store
            .get_ipfs(&missing.0.to_string(), "")
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Unavailable
    );
    let result = store.get_ipfs(&present.0.to_string(), "").await.unwrap();
    assert_eq!(result.body.as_ref(), present.1);
    assert_eq!(result.verification, Verification::IpfsCar);
    assert_eq!(gateway.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn cancelled_half_open_probe_does_not_stick_the_breaker() {
    let file = block(0x55, b"recovered".to_vec());
    let proof = car(file.0, std::slice::from_ref(&file));
    let mock = mock(vec![
        Reply {
            status: 503,
            ..car_reply(vec![])
        },
        Reply {
            delay: Duration::from_secs(1),
            ..car_reply(proof.clone())
        },
        car_reply(proof),
    ])
    .await;
    let mut cfg = config(&temp("cancel_probe"), &[&mock]);
    cfg.breaker.failure_threshold = 1;
    let store = ContentStore::new(cfg).unwrap();
    let cid = file.0.to_string();
    assert_eq!(
        store.get_ipfs(&cid, "").await.unwrap_err().kind,
        ErrorKind::Unavailable
    );
    tokio::time::sleep(Duration::from_millis(40)).await;
    let task_store = store.clone();
    let task_cid = cid.clone();
    let task = tokio::spawn(async move { task_store.get_ipfs(&task_cid, "").await });
    tokio::time::timeout(Duration::from_secs(1), async {
        while mock.calls.load(Ordering::SeqCst) != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    task.abort();
    let _ = task.await;
    assert_eq!(
        store.get_ipfs(&cid, "").await.unwrap().body.as_ref(),
        file.1
    );
    assert_eq!(mock.calls.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn concurrent_requests_share_bounded_capacity_and_atomic_cache() {
    let file = block(0x55, b"shared".to_vec());
    let mock = mock(vec![Reply {
        delay: Duration::from_millis(10),
        ..car_reply(car(file.0, std::slice::from_ref(&file)))
    }])
    .await;
    let root = temp("concurrent");
    let mut cfg = config(&root, &[&mock]);
    cfg.max_concurrent_assemblies = 2;
    let store = ContentStore::new(cfg).unwrap();
    let mut tasks = Vec::new();
    for _ in 0..16 {
        let store = store.clone();
        let cid = file.0.to_string();
        tasks.push(tokio::spawn(async move { store.get_ipfs(&cid, "").await }));
    }
    for task in tasks {
        assert_eq!(task.await.unwrap().unwrap().body.as_ref(), file.1);
    }
    assert!(mock.calls.load(Ordering::SeqCst) <= 2);
    assert_eq!(std::fs::read_dir(root.join("tmp")).unwrap().count(), 0);
}

#[tokio::test]
async fn malformed_proofs_never_receive_verified_status() {
    let raw = block(0x55, b"proof".to_vec());
    let wrong = block(0x55, b"other".to_vec());
    let mut bad_hash = car(raw.0, std::slice::from_ref(&raw));
    *bad_hash.last_mut().unwrap() ^= 1;
    let mut truncated = car(raw.0, std::slice::from_ref(&raw));
    truncated.pop();
    let mut noncanonical = vec![0xff; 11];
    noncanonical.push(0);
    let cases = [
        car(wrong.0, std::slice::from_ref(&wrong)),
        bad_hash,
        truncated,
        noncanonical,
    ];
    for (n, proof) in cases.into_iter().enumerate() {
        let mock = mock(vec![car_reply(proof)]).await;
        let store = ContentStore::new(config(&temp(&format!("invalid_{n}")), &[&mock])).unwrap();
        assert_eq!(
            store
                .get_ipfs(&raw.0.to_string(), "")
                .await
                .unwrap_err()
                .kind,
            ErrorKind::Invalid
        );
    }
}

#[tokio::test]
async fn repeated_blocks_are_deduplicated_and_reconstructed_file_size_is_bounded() {
    let raw = block(0x55, b"proof".to_vec());
    let proof = car(raw.0, &[raw.clone(), raw.clone()]);
    let mock = mock(vec![car_reply(proof.clone())]).await;
    let cfg = config(&temp("duplicates"), &[&mock]);
    assert!(
        ContentStore::new(cfg)
            .unwrap()
            .get_ipfs(&raw.0.to_string(), "")
            .await
            .is_ok()
    );
    let mut cfg = config(&temp("size_limit"), &[&mock]);
    cfg.max_object_bytes = b"proof".len() as u64 - 1;
    assert_eq!(
        ContentStore::new(cfg)
            .unwrap()
            .get_ipfs(&raw.0.to_string(), "")
            .await
            .unwrap_err()
            .kind,
        ErrorKind::TooLarge
    );
}

#[tokio::test]
async fn plain_gateway_body_is_explicitly_unverified_and_not_persisted() {
    let raw = block(0x55, b"original".to_vec());
    let mock = mock(vec![Reply {
        kind: "text/plain",
        body: b"unverified".to_vec(),
        ..car_reply(vec![])
    }])
    .await;
    let store = ContentStore::new(config(&temp("plain"), &[&mock])).unwrap();
    for _ in 0..2 {
        let reply = store.get_ipfs(&raw.0.to_string(), "").await.unwrap();
        assert_eq!(reply.verification, Verification::Unverified);
        assert!(!reply.from_cache);
    }
    assert_eq!(mock.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn invalid_cid_and_traversal_never_reach_gateway() {
    let mock = mock(vec![car_reply(vec![])]).await;
    let store = ContentStore::new(config(&temp("bad_request"), &[&mock])).unwrap();
    let valid = block(0x55, vec![]).0.to_string();
    let mut trailing = block(0x55, vec![]).0.to_bytes();
    trailing.push(0);
    let trailing = cid::multibase::encode(cid::multibase::Base::Base32Lower, trailing);
    for (cid, path) in [
        ("Qmnotacid", ""),
        (&valid, "%2e%2e/secret"),
        (&valid, "a%2fb"),
        (&trailing, ""),
    ] {
        assert_eq!(
            store.get_ipfs(cid, path).await.unwrap_err().kind,
            ErrorKind::BadRequest
        );
    }
    assert_eq!(mock.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn hamt_and_symlinks_fail_closed() {
    for kind in [4_u8, 5] {
        let node = block(0x70, field(10, &[8, kind]));
        let mock = mock(vec![car_reply(car(node.0, std::slice::from_ref(&node)))]).await;
        let store =
            ContentStore::new(config(&temp(&format!("unsupported_{kind}")), &[&mock])).unwrap();
        assert_eq!(
            store
                .get_ipfs(&node.0.to_string(), "")
                .await
                .unwrap_err()
                .kind,
            ErrorKind::Invalid
        );
    }
}

#[tokio::test]
async fn multi_block_unixfs_file_checks_every_child_and_declared_size() {
    let a = block(0x55, b"hello ".to_vec());
    let b = block(0x55, b"world".to_vec());
    let mut unix = vec![8, 2, 24, 11, 32, 6, 32, 5];
    for (n, expected) in [Ok(b"hello world".as_slice()), Err(ErrorKind::Invalid)]
        .into_iter()
        .enumerate()
    {
        let mut node = field(10, &unix);
        for child in [&a, &b] {
            node.extend(field(18, &field(10, &child.0.to_bytes())));
        }
        let file = block(0x70, node);
        let mock = mock(vec![car_reply(car(
            file.0,
            &[file.clone(), a.clone(), b.clone()],
        ))])
        .await;
        let store =
            ContentStore::new(config(&temp(&format!("multi_block_{n}")), &[&mock])).unwrap();
        let result = store.get_ipfs(&file.0.to_string(), "").await;
        match expected {
            Ok(bytes) => assert_eq!(result.unwrap().body.as_ref(), bytes),
            Err(kind) => assert_eq!(result.unwrap_err().kind, kind),
        }
        unix[5] = 5;
    }
}

#[tokio::test]
async fn zero_length_shared_subgraphs_have_a_cpu_work_budget() {
    fn empty_file(children: &[Cid]) -> (Cid, Vec<u8>) {
        let mut unix = vec![8, 2, 24, 0];
        for _ in children {
            unix.extend([32, 0]);
        }
        let mut data = field(10, &unix);
        for child in children {
            data.extend(field(18, &field(10, &child.to_bytes())));
        }
        block(0x70, data)
    }
    let empty = block(0x55, vec![]);
    let shared = empty_file(&vec![empty.0; 1_000]);
    let root = empty_file(&[shared.0; 10]);
    let proof = car(root.0, &[root.clone(), shared, empty]);
    let cap = proof.len() as u64 + 100;
    let mock = mock(vec![car_reply(proof)]).await;
    let mut cfg = config(&temp("cpu_budget"), &[&mock]);
    cfg.max_object_bytes = cap;
    let error = ContentStore::new(cfg)
        .unwrap()
        .get_ipfs(&root.0.to_string(), "")
        .await
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Invalid);
    assert!(error.message.contains("work budget"));
}

#[tokio::test]
async fn cid_v0_and_v1_share_the_same_verified_block_identity() {
    let file = block(0x70, field(10, &[8, 2, 18, 1, b'x', 24, 1]));
    let v0 = Cid::new_v0(*file.0.hash()).unwrap();
    let mock = mock(vec![car_reply(car(file.0, std::slice::from_ref(&file)))]).await;
    let store = ContentStore::new(config(&temp("cid_versions"), &[&mock])).unwrap();
    assert_eq!(
        store
            .get_ipfs(&v0.to_string(), "")
            .await
            .unwrap()
            .body
            .as_ref(),
        b"x"
    );
}

#[tokio::test]
async fn missing_linked_block_is_retryable_and_identity_hash_is_checked() {
    let file = block(0x55, b"identity".to_vec());
    let dir = directory(&["a"], file.0);
    let missing = mock(vec![car_reply(car(dir.0, std::slice::from_ref(&dir)))]).await;
    let store = ContentStore::new(config(&temp("missing_block"), &[&missing])).unwrap();
    assert_eq!(
        store
            .get_ipfs(&dir.0.to_string(), "a")
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Unavailable
    );
    let cid = Cid::new_v1(0x55, cid::multihash::Multihash::wrap(0, &file.1).unwrap());
    let mock = mock(vec![car_reply(car(cid, &[]))]).await;
    let store = ContentStore::new(config(&temp("identity"), &[&mock])).unwrap();
    assert_eq!(
        store
            .get_ipfs(&cid.to_string(), "")
            .await
            .unwrap()
            .body
            .as_ref(),
        file.1
    );
}

#[cfg(unix)]
#[tokio::test]
async fn kubo_is_optional_bounded_and_falls_back_after_timeout() {
    use seeded_archive::content::KuboConfig;
    use std::os::unix::fs::PermissionsExt;
    let raw = block(0x55, b"kubo-data".to_vec());
    let mock = mock(vec![car_reply(car(raw.0, std::slice::from_ref(&raw)))]).await;
    let root = temp("kubo");
    let executable = root.join("kubo_mock");
    std::fs::write(&executable, b"#!/bin/sh\nprintf 'kubo-data'\n").unwrap();
    std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
    let mut cfg = config(&root.join("cache"), &[&mock]);
    cfg.kubo = Some(KuboConfig {
        binary: executable.clone(),
        ipfs_path: root.join("repo"),
        timeout: Duration::from_millis(40),
    });
    let result = ContentStore::new(cfg.clone())
        .unwrap()
        .get_ipfs(&raw.0.to_string(), "")
        .await
        .unwrap();
    assert_eq!(result.verification, Verification::IpfsKubo);
    assert_eq!(mock.calls.load(Ordering::SeqCst), 0);
    std::fs::write(&executable, b"#!/bin/sh\nexec sleep 5\n").unwrap();
    let result = ContentStore::new(cfg)
        .unwrap()
        .get_ipfs(&raw.0.to_string(), "")
        .await
        .unwrap();
    assert_eq!(result.verification, Verification::IpfsCar);
    assert_eq!(mock.calls.load(Ordering::SeqCst), 1);
}

#[cfg(unix)]
#[tokio::test]
async fn kubo_output_limit_is_preserved_without_any_gateway() {
    use seeded_archive::content::KuboConfig;
    use std::os::unix::fs::PermissionsExt;
    let root = temp("kubo_limit");
    let executable = root.join("kubo_mock");
    std::fs::write(&executable, b"#!/bin/sh\nprintf '%0200d' 0\n").unwrap();
    std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
    let mut cfg = config(&root.join("cache"), &[]);
    cfg.max_object_bytes = 128;
    cfg.kubo = Some(KuboConfig {
        binary: executable,
        ipfs_path: root.join("repo"),
        timeout: Duration::from_millis(100),
    });
    let store = ContentStore::new(cfg).unwrap();
    assert_eq!(
        store
            .get_ipfs(&block(0x55, b"x".to_vec()).0.to_string(), "")
            .await
            .unwrap_err()
            .kind,
        ErrorKind::TooLarge
    );
}

#[cfg(unix)]
#[tokio::test]
async fn kubo_only_mode_resolves_directory_index_with_slash_hint() {
    use seeded_archive::content::KuboConfig;
    use std::os::unix::fs::PermissionsExt;
    let root = temp("kubo_directory");
    let executable = root.join("kubo_mock");
    std::fs::write(
        &executable,
        b"#!/bin/sh\ncase \"$2\" in */index.html) printf '<html>local</html>';; *) printf 'Error: this dag node is a directory\\n' >&2; exit 1;; esac\n",
    )
    .unwrap();
    std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
    let mut cfg = config(&root.join("cache"), &[]);
    cfg.kubo = Some(KuboConfig {
        binary: executable,
        ipfs_path: root.join("repo"),
        timeout: Duration::from_millis(100),
    });
    let store = ContentStore::new(cfg).unwrap();
    let result = store
        .get_ipfs(
            &block(0x55, b"configured-local-node".to_vec()).0.to_string(),
            "",
        )
        .await
        .unwrap();
    assert_eq!(result.verification, Verification::IpfsKubo);
    assert!(result.directory_index);
    assert_eq!(
        result.content_type.as_deref(),
        Some("text/html; charset=utf-8")
    );
}

#[tokio::test]
#[ignore = "bounded public gateway probe; requires network"]
async fn public_gateway_serves_verified_artwork() {
    let mut cfg = ContentConfig::mainnet(temp("live_probe"));
    cfg.ipfs_gateways.truncate(1);
    cfg.request_timeout = Duration::from_secs(10);
    cfg.connect_timeout = Duration::from_secs(3);
    let store = ContentStore::new(cfg).unwrap();
    let result = store
        .get_ipfs("QmbVXKkiuqjHgDgzmMf6811VuUWL3k9RJhokR8vZv1Phw1", "")
        .await
        .unwrap();
    assert_eq!(result.verification, Verification::IpfsCar);
    assert!(!result.body.is_empty());
}

#[tokio::test]
async fn repeated_misses_and_invalid_proofs_do_not_open_gateway_breakers() {
    let file = block(0x55, b"recovery".to_vec());
    let upstream = mock(vec![
        Reply {
            status: 404,
            retry: Some("60"),
            ..car_reply(vec![])
        },
        Reply {
            status: 404,
            ..car_reply(vec![])
        },
        Reply {
            status: 400,
            ..car_reply(vec![])
        },
        car_reply(b"invalid CAR".to_vec()),
        car_reply(car(file.0, std::slice::from_ref(&file))),
    ])
    .await;
    let mut cfg = config(&temp("neutral_breakers"), &[&upstream]);
    cfg.breaker.failure_threshold = 1;
    cfg.breaker.base_cooldown = Duration::from_secs(1);
    let store = ContentStore::new(cfg).unwrap();
    for expected in [
        ErrorKind::Unavailable,
        ErrorKind::Unavailable,
        ErrorKind::Unavailable,
        ErrorKind::Invalid,
    ] {
        assert_eq!(
            store
                .get_ipfs(&file.0.to_string(), "")
                .await
                .unwrap_err()
                .kind,
            expected
        );
    }
    assert_eq!(
        store
            .get_ipfs(&file.0.to_string(), "")
            .await
            .unwrap()
            .body
            .as_ref(),
        file.1
    );
    assert_eq!(upstream.calls.load(Ordering::SeqCst), 5);
}

#[tokio::test]
async fn oversized_content_stops_fallback_without_poisoning_the_gateway() {
    let file = block(0x55, b"small".to_vec());
    let first = mock(vec![
        car_reply(vec![0; 70 * 1024]),
        car_reply(car(file.0, std::slice::from_ref(&file))),
    ])
    .await;
    let second = mock(vec![car_reply(car(file.0, std::slice::from_ref(&file)))]).await;
    let mut cfg = config(&temp("terminal_size"), &[&first, &second]);
    cfg.max_object_bytes = 256;
    cfg.breaker.failure_threshold = 1;
    cfg.breaker.base_cooldown = Duration::from_secs(1);
    let store = ContentStore::new(cfg).unwrap();
    assert_eq!(
        store
            .get_ipfs(&file.0.to_string(), "")
            .await
            .unwrap_err()
            .kind,
        ErrorKind::TooLarge
    );
    assert_eq!(second.calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        store
            .get_ipfs(&file.0.to_string(), "")
            .await
            .unwrap()
            .verification,
        Verification::IpfsCar
    );
    assert_eq!(first.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn later_gateway_proof_wins_over_plain_bytes_or_unsupported_negotiation() {
    for status in [200, 406, 415, 501] {
        let file = block(0x55, b"verified bytes".to_vec());
        let first = mock(vec![Reply {
            status,
            kind: "text/html",
            body: b"untrusted bytes".to_vec(),
            delay: Duration::ZERO,
            retry: None,
        }])
        .await;
        let second = mock(vec![car_reply(car(file.0, std::slice::from_ref(&file)))]).await;
        let store = ContentStore::new(config(
            &temp(&format!("proof_first_{status}")),
            &[&first, &second],
        ))
        .unwrap();
        let result = store.get_ipfs(&file.0.to_string(), "").await.unwrap();
        assert_eq!(result.verification, Verification::IpfsCar);
        assert_eq!(result.body.as_ref(), file.1);
        assert_eq!(
            first.calls.load(Ordering::SeqCst),
            1,
            "no plain fetch before the second proof"
        );
        assert_eq!(second.calls.load(Ordering::SeqCst), 1);
        assert!(
            store
                .get_ipfs(&file.0.to_string(), "")
                .await
                .unwrap()
                .from_cache
        );
    }
}

#[tokio::test]
async fn retained_plain_bytes_survive_later_proof_timeouts_without_becoming_verified() {
    let file = block(0x55, b"known".to_vec());
    let first = mock(vec![Reply {
        kind: "text/html",
        ..car_reply(b"fallback".to_vec())
    }])
    .await;
    let slow = mock(vec![Reply {
        delay: Duration::from_secs(2),
        ..car_reply(car(file.0, std::slice::from_ref(&file)))
    }])
    .await;
    let mut cfg = config(&temp("retained_plain"), &[&first, &slow]);
    cfg.request_timeout = Duration::from_millis(100);
    let store = ContentStore::new(cfg).unwrap();
    let result = tokio::time::timeout(
        Duration::from_millis(700),
        store.get_ipfs(&file.0.to_string(), ""),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(result.verification, Verification::Unverified);
    assert_eq!(result.body.as_ref(), b"fallback");
    assert!(!result.from_cache);
    assert_eq!(slow.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cold_upstream_saturation_does_not_block_verified_cache_hits() {
    let warm = block(0x55, b"warm".to_vec());
    let cold = block(0x55, b"cold".to_vec());
    let upstream = mock(vec![
        car_reply(car(warm.0, std::slice::from_ref(&warm))),
        Reply {
            delay: Duration::from_secs(2),
            ..car_reply(car(cold.0, std::slice::from_ref(&cold)))
        },
    ])
    .await;
    let mut cfg = config(&temp("independent_cache"), &[&upstream]);
    cfg.max_concurrent_assemblies = 1;
    cfg.max_concurrent_requests = 1;
    cfg.request_timeout = Duration::from_millis(500);
    let store = ContentStore::new(cfg).unwrap();
    store.get_ipfs(&warm.0.to_string(), "").await.unwrap();
    let task_store = store.clone();
    let task = tokio::spawn(async move { task_store.get_ipfs(&cold.0.to_string(), "").await });
    tokio::time::timeout(Duration::from_secs(1), async {
        while upstream.calls.load(Ordering::SeqCst) < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let cached = tokio::time::timeout(
        Duration::from_millis(100),
        store.get_ipfs(&warm.0.to_string(), ""),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(cached.from_cache);
    assert_eq!(cached.body.as_ref(), warm.1);
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
}

#[cfg(unix)]
fn kubo_script(
    root: &Path,
    script: &str,
    timeout: Duration,
) -> seeded_archive::content::KuboConfig {
    use std::os::unix::fs::PermissionsExt;
    let binary = root.join("kubo_mock");
    let repo = root.join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    std::fs::write(
        &binary,
        format!("#!/bin/sh\nprintf x >> \"$IPFS_PATH/calls\"\n{script}\n"),
    )
    .unwrap();
    std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o700)).unwrap();
    seeded_archive::content::KuboConfig {
        binary,
        ipfs_path: repo,
        timeout,
    }
}

#[cfg(unix)]
#[tokio::test]
async fn kubo_non_directory_errors_and_large_stderr_go_straight_to_gateways() {
    for (name, script) in [
        ("miss", "printf 'Error: unavailable\\n' >&2; exit 1"),
        ("stderr", "printf '%06000d' 0 >&2; exit 1"),
    ] {
        let file = block(0x55, b"gateway".to_vec());
        let gateway = mock(vec![car_reply(car(file.0, std::slice::from_ref(&file)))]).await;
        let root = temp(&format!("kubo_error_{name}"));
        let mut cfg = config(&root.join("cache"), &[&gateway]);
        cfg.kubo = Some(kubo_script(&root, script, Duration::from_secs(30)));
        let store = ContentStore::new(cfg).unwrap();
        assert_eq!(
            store
                .get_ipfs(&file.0.to_string(), "")
                .await
                .unwrap()
                .verification,
            Verification::IpfsCar
        );
        assert_eq!(std::fs::read(root.join("repo/calls")).unwrap(), b"x");
    }
}

#[cfg(unix)]
#[tokio::test]
async fn kubo_directory_attempts_share_one_short_deadline() {
    let file = block(0x55, b"gateway".to_vec());
    let gateway = mock(vec![car_reply(car(file.0, std::slice::from_ref(&file)))]).await;
    let root = temp("kubo_shared_deadline");
    let script = "sleep 0.15\ncase \"$2\" in */index.html) printf 'local';; *) printf 'Error: this dag node is a directory\\n' >&2; exit 1;; esac";
    let mut cfg = config(&root.join("cache"), &[&gateway]);
    cfg.request_timeout = Duration::from_secs(1);
    cfg.kubo = Some(kubo_script(&root, script, Duration::from_millis(240)));
    let store = ContentStore::new(cfg).unwrap();
    let result = tokio::time::timeout(
        Duration::from_millis(600),
        store.get_ipfs(&file.0.to_string(), ""),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(result.verification, Verification::IpfsCar);
    assert_eq!(std::fs::read(root.join("repo/calls")).unwrap(), b"xx");
}

#[cfg(unix)]
#[tokio::test]
async fn kubo_oversize_does_not_redownload_from_gateways() {
    let file = block(0x55, b"gateway".to_vec());
    let gateway = mock(vec![car_reply(car(file.0, std::slice::from_ref(&file)))]).await;
    let root = temp("kubo_terminal_size");
    let mut cfg = config(&root.join("cache"), &[&gateway]);
    cfg.max_object_bytes = 128;
    cfg.kubo = Some(kubo_script(
        &root,
        "printf '%0200d' 0",
        Duration::from_secs(1),
    ));
    let store = ContentStore::new(cfg).unwrap();
    assert_eq!(
        store
            .get_ipfs(&file.0.to_string(), "")
            .await
            .unwrap_err()
            .kind,
        ErrorKind::TooLarge
    );
    assert_eq!(gateway.calls.load(Ordering::SeqCst), 0);
    assert_eq!(std::fs::read(root.join("repo/calls")).unwrap(), b"x");
}

#[cfg(unix)]
#[tokio::test]
async fn cancelled_kubo_process_is_reaped_and_resolution_slots_recover() {
    let file = block(0x55, b"recovered".to_vec());
    let root = temp("kubo_cancel");
    let mut cfg = config(&root.join("cache"), &[]);
    cfg.max_concurrent_assemblies = 1;
    cfg.request_timeout = Duration::from_secs(2);
    cfg.kubo = Some(kubo_script(
        &root,
        "echo $$ > \"$IPFS_PATH/pid\"\nexec sleep 5",
        Duration::from_secs(30),
    ));
    let store = ContentStore::new(cfg).unwrap();
    let task_store = store.clone();
    let cid = file.0.to_string();
    let task = tokio::spawn(async move { task_store.get_ipfs(&cid, "").await });
    let pid_path = root.join("repo/pid");
    let pid = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if let Some(pid) = std::fs::read_to_string(&pid_path)
                .ok()
                .and_then(|text| text.strip_suffix('\n')?.parse::<u32>().ok())
                .filter(|pid| *pid > 0)
            {
                break pid;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    tokio::time::timeout(Duration::from_secs(1), async {
        while PathBuf::from(format!("/proc/{pid}")).exists() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    std::fs::write(root.join("kubo_mock"), "#!/bin/sh\nprintf recovered\n").unwrap();
    assert_eq!(
        store
            .get_ipfs(&file.0.to_string(), "")
            .await
            .unwrap()
            .body
            .as_ref(),
        file.1
    );
}

#[tokio::test]
async fn admitted_waiters_are_bounded_and_cancellation_clears_the_flight() {
    let file = block(0x55, b"retry".to_vec());
    let proof = car(file.0, std::slice::from_ref(&file));
    let upstream = mock(vec![
        Reply {
            delay: Duration::from_secs(2),
            ..car_reply(proof.clone())
        },
        car_reply(proof),
    ])
    .await;
    let mut cfg = config(&temp("bounded_flights"), &[&upstream]);
    cfg.max_concurrent_assemblies = 1;
    cfg.request_timeout = Duration::from_millis(500);
    let store = ContentStore::new(cfg).unwrap();
    let barrier = Arc::new(tokio::sync::Barrier::new(9));
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let store = store.clone();
        let cid = file.0.to_string();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            store.get_ipfs(&cid, "").await
        }));
    }
    barrier.wait().await;
    tokio::time::timeout(Duration::from_secs(1), async {
        while upstream.calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    let overflow = tokio::time::timeout(
        Duration::from_millis(100),
        store.get_ipfs(&file.0.to_string(), ""),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert_eq!(overflow.kind, ErrorKind::Unavailable);
    assert_eq!(upstream.calls.load(Ordering::SeqCst), 1);
    for task in &tasks {
        task.abort();
    }
    for task in tasks {
        assert!(task.await.unwrap_err().is_cancelled());
    }
    let recovered = store.get_ipfs(&file.0.to_string(), "").await.unwrap();
    assert_eq!(recovered.verification, Verification::IpfsCar);
    assert_eq!(upstream.calls.load(Ordering::SeqCst), 2);
}
