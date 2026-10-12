//! Preservation mirror: packages copied from local mock gateways and TzKT, then served with
//! upstream down; cap, transient, not-found, resume and verification transitions.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::{
    Router,
    extract::State,
    http::{StatusCode, Uri, header},
    response::Response,
};
use cid::Cid;
use rusqlite::{Connection, params};
use seeded_archive::content::onchfs::{Hash, directory_cid, file_cid, keccak};
use seeded_archive::content::{
    ContentConfig, ContentStore, ErrorKind, MirrorWriter, PackageError, Verification,
};
use seeded_archive::ingest::mirror::{
    ListedProject, MirrorOptions, MirrorRun, MirrorState, MirrorStatus, ProjectList, Tiers,
    backoff, read_list, run_mirror, write_list,
};
use seeded_archive::store::Store;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

const CAR: &str = "application/vnd.ipld.car";
/// Nothing listens on port 1: upstream is down.
const DEAD: &str = "http://127.0.0.1:1";

fn temp(name: &str) -> PathBuf {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tmp/mirror-tests")
        .join(format!("{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);
    std::fs::create_dir_all(&path).unwrap();
    path
}

#[derive(Clone)]
struct Reply {
    status: u16,
    kind: &'static str,
    body: Vec<u8>,
    delay: Duration,
    location: Option<String>,
}

impl Reply {
    fn status(status: u16) -> Self {
        Self {
            status,
            kind: "text/plain",
            body: Vec::new(),
            delay: Duration::ZERO,
            location: None,
        }
    }

    fn car(body: Vec<u8>) -> Self {
        Self {
            kind: CAR,
            body,
            ..Self::status(200)
        }
    }

    fn json(value: Value) -> Self {
        Self {
            kind: "application/json",
            body: serde_json::to_vec(&value).unwrap(),
            ..Self::status(200)
        }
    }

    fn redirect(location: String) -> Self {
        Self {
            location: Some(location),
            ..Self::status(301)
        }
    }
}

type Handler = Arc<dyn Fn(&Uri) -> Reply + Send + Sync>;
type Calls = Arc<Mutex<Vec<String>>>;

struct Server {
    base: String,
    calls: Calls,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Server {
    fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }
}

async fn server(suffix: &str, handler: Handler) -> Server {
    async fn respond(State((calls, handler)): State<(Calls, Handler)>, uri: Uri) -> Response {
        calls.lock().unwrap().push(uri.to_string());
        let reply = handler(&uri);
        tokio::time::sleep(reply.delay).await;
        let mut response = Response::builder()
            .status(StatusCode::from_u16(reply.status).unwrap())
            .header(header::CONTENT_TYPE, reply.kind);
        if let Some(location) = reply.location {
            response = response.header(header::LOCATION, location);
        }
        response.body(axum::body::Body::from(reply.body)).unwrap()
    }
    let calls: Calls = Arc::new(Mutex::new(Vec::new()));
    let router = Router::new()
        .fallback(respond)
        .with_state((calls.clone(), handler));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}{suffix}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    Server { base, calls, task }
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

fn car(root: Cid, blocks: &[(Cid, Vec<u8>)]) -> Vec<u8> {
    let mut header = vec![0xa2, 0x65];
    header.extend(b"roots");
    header.extend([0x81, 0xd8, 0x2a, 0x58, root.to_bytes().len() as u8 + 1, 0]);
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

/// An IPFS DAG with UnixFS raw leaves, chunked files and directories.
#[derive(Clone, Default)]
struct Dag {
    blocks: HashMap<String, (Cid, Vec<u8>)>,
    children: HashMap<Cid, Vec<Cid>>,
    directories: HashSet<Cid>,
}

type Node = (Cid, u64);

impl Dag {
    fn add(&mut self, codec: u64, bytes: Vec<u8>, children: Vec<Cid>) -> Cid {
        let digest = Sha256::digest(&bytes);
        let cid = Cid::new_v1(
            codec,
            cid::multihash::Multihash::wrap(0x12, &digest).unwrap(),
        );
        self.blocks.insert(cid.to_string(), (cid, bytes));
        self.children.insert(cid, children);
        cid
    }

    fn file(&mut self, data: &[u8]) -> Node {
        (self.add(0x55, data.to_vec(), vec![]), data.len() as u64)
    }

    /// A node held in its identity CID; gateways never send it.
    fn inline(node: &[u8]) -> Node {
        let hash = cid::multihash::Multihash::wrap(0x00, node).unwrap();
        (Cid::new_v1(0x70, hash), 0)
    }

    fn chunked(&mut self, chunks: &[&[u8]]) -> Node {
        let leaves: Vec<Node> = chunks.iter().map(|chunk| self.file(chunk)).collect();
        let mut unixfs = vec![8, 2, 24];
        unixfs.extend(varint(leaves.iter().map(|leaf| leaf.1).sum()));
        for leaf in &leaves {
            unixfs.push(32);
            unixfs.extend(varint(leaf.1));
        }
        let mut node = Vec::new();
        for (cid, size) in &leaves {
            let mut link = field(10, &cid.to_bytes());
            link.push(24);
            link.extend(varint(*size));
            node.extend(field(18, &link));
        }
        node.extend(field(10, &unixfs));
        let tsize = node.len() as u64 + leaves.iter().map(|leaf| leaf.1).sum::<u64>();
        let cid = self.add(0x70, node, leaves.iter().map(|leaf| leaf.0).collect());
        (cid, tsize)
    }

    fn dir(&mut self, entries: &[(&str, Node)], tsize: bool) -> Node {
        let mut sorted = entries.to_vec();
        sorted.sort_by_key(|entry| entry.0);
        let mut node = Vec::new();
        for (name, (cid, size)) in &sorted {
            let mut link = field(10, &cid.to_bytes());
            link.extend(field(18, name.as_bytes()));
            if tsize {
                link.push(24);
                link.extend(varint(*size));
            }
            node.extend(field(18, &link));
        }
        node.extend(field(10, &[8, 1]));
        let total = node.len() as u64 + sorted.iter().map(|entry| entry.1.1).sum::<u64>();
        let cid = self.add(0x70, node, sorted.iter().map(|entry| entry.1.0).collect());
        self.directories.insert(cid);
        (cid, total)
    }

    fn block(&self, cid: &Cid) -> Vec<u8> {
        self.blocks[&cid.to_string()].1.clone()
    }

    fn subtree(&self, root: Cid) -> Vec<(Cid, Vec<u8>)> {
        let mut out = Vec::new();
        let mut pending = vec![root];
        let mut seen = HashSet::new();
        while let Some(cid) = pending.pop() {
            if cid.hash().code() != 0x00 && seen.insert(cid) {
                out.push((cid, self.block(&cid)));
                pending.extend(self.children[&cid].iter().rev());
            }
        }
        out
    }

    /// A trustless gateway: `dag-scope=all` sends the whole DAG, `entity` a directory block
    /// or a file's blocks.
    fn serve(&self, uri: &Uri) -> Reply {
        let text = uri.path().trim_start_matches("/ipfs/");
        let Some((cid, _)) = self.blocks.get(text.split('/').next().unwrap()) else {
            return Reply::status(404);
        };
        let query = uri.query().unwrap_or_default();
        let blocks = if query.contains("dag-scope=block") {
            vec![(*cid, self.block(cid))]
        } else if query.contains("dag-scope=all") || !self.directories.contains(cid) {
            self.subtree(*cid)
        } else {
            vec![(*cid, self.block(cid))]
        };
        Reply::car(car(*cid, &blocks))
    }
}

const INDEX: &[u8] = b"<!doctype html><script src=js/sketch.js></script>";

/// `index.html`, `js/sketch.js` in two chunks and `js/lib.js`.
fn artwork() -> (Dag, Cid, Vec<u8>) {
    let mut dag = Dag::default();
    let sketch = [b"function setup() {".as_slice(), b" createCanvas(9, 9) }"].concat();
    let index = dag.file(INDEX);
    let chunks = dag.chunked(&[&sketch[..18], &sketch[18..]]);
    let lib = dag.file(b"// library");
    let js = dag.dir(&[("sketch.js", chunks), ("lib.js", lib)], true);
    let root = dag.dir(&[("index.html", index), ("js", js)], true);
    (dag, root.0, sketch)
}

fn gateway_handler(dag: Dag) -> Handler {
    Arc::new(move |uri| dag.serve(uri))
}

fn fetch_config(dir: &Path, gateways: &[&Server], tzkt: Option<&Server>) -> ContentConfig {
    let mut config = ContentConfig::mainnet(dir.join("job-cache"));
    config.max_cache_bytes = 0;
    config.ipfs_gateways = gateways.iter().map(|g| g.base.clone()).collect();
    config.tzkt_base = tzkt.map_or_else(|| DEAD.to_string(), |t| t.base.clone());
    config.request_timeout = Duration::from_secs(2);
    config.connect_timeout = Duration::from_millis(200);
    config.max_object_bytes = 1 << 20;
    config.breaker.failure_threshold = 20;
    config.breaker.base_cooldown = Duration::from_millis(30);
    config.breaker.max_cooldown = Duration::from_secs(1);
    config
}

fn serve_config(dir: &Path, mirror: &Path) -> ContentConfig {
    let mut config = ContentConfig::mainnet(dir.join("serve-cache"));
    config.mirror_dir = Some(mirror.to_path_buf());
    config.ipfs_gateways = vec![format!("{DEAD}/ipfs/")];
    config.tzkt_base = DEAD.to_string();
    config.request_timeout = Duration::from_millis(500);
    config.connect_timeout = Duration::from_millis(200);
    config.max_object_bytes = 1 << 20;
    config
}

fn writer(mirror: &Path, max: u64) -> Arc<MirrorWriter> {
    Arc::new(MirrorWriter::open(mirror, max).unwrap())
}

fn stored(mirror: &Path, ns: &str) -> usize {
    let Ok(shards) = std::fs::read_dir(mirror.join(ns)) else {
        return 0;
    };
    shards
        .map(|shard| std::fs::read_dir(shard.unwrap().path()).unwrap().count())
        .sum()
}

fn replace_mirror_bytes(mirror: &Path, ns: &str, original: &[u8], replacement: &[u8]) -> PathBuf {
    assert_eq!(original.len(), replacement.len());
    assert!(!original.is_empty());
    for shard in std::fs::read_dir(mirror.join(ns)).unwrap() {
        for entry in std::fs::read_dir(shard.unwrap().path()).unwrap() {
            let path = entry.unwrap().path();
            let mut bytes = std::fs::read(&path).unwrap();
            let Some(offset) = bytes
                .windows(original.len())
                .position(|window| window == original)
            else {
                continue;
            };
            bytes[offset..offset + replacement.len()].copy_from_slice(replacement);
            std::fs::write(&path, bytes).unwrap();
            return path;
        }
    }
    panic!("mirror {ns} object did not contain target bytes");
}

fn staging(mirror: &Path) -> usize {
    std::fs::read_dir(mirror.join("tmp")).map_or(0, |entries| entries.count())
}

fn list(projects: &[(u64, &str, u64)]) -> ProjectList {
    ProjectList {
        version: 1,
        generated_at: "2026-10-07T00:00:00Z".into(),
        projects: projects
            .iter()
            .map(|(id, root_uri, volume_mutez)| ListedProject {
                id: *id,
                root_uri: root_uri.to_string(),
                volume_mutez: *volume_mutez,
            })
            .collect(),
    }
}

async fn run(
    list: &ProjectList,
    store: &ContentStore,
    writer: &Arc<MirrorWriter>,
    opts: MirrorOptions,
) -> MirrorRun {
    let mut status = MirrorStatus::open(writer.root()).unwrap();
    run_mirror(
        list,
        &mut status,
        store,
        writer,
        &opts,
        &AtomicBool::new(false),
        &|_| {},
    )
    .await
    .unwrap()
}

/// Requests for `dag-scope=<scope>` CARs.
fn scoped(server: &Server, scope: &str) -> usize {
    let query = format!("dag-scope={scope}");
    server
        .calls()
        .iter()
        .filter(|call| call.contains(&query))
        .count()
}

fn tiered(kib: u64) -> MirrorOptions {
    MirrorOptions {
        concurrency: 1,
        tiers: Some(Tiers {
            start: kib << 10,
            step: kib << 10,
        }),
        ..MirrorOptions::default()
    }
}

/// A run and its log lines.
async fn logged(
    list: &ProjectList,
    store: &ContentStore,
    writer: &Arc<MirrorWriter>,
    opts: MirrorOptions,
    stop: &AtomicBool,
) -> (MirrorRun, Vec<String>) {
    let mut status = MirrorStatus::open(writer.root()).unwrap();
    let lines = Mutex::new(Vec::new());
    let run = run_mirror(list, &mut status, store, writer, &opts, stop, &|line| {
        lines.lock().unwrap().push(line.to_owned())
    })
    .await
    .unwrap();
    (run, lines.into_inner().unwrap())
}

fn passes(lines: &[String]) -> usize {
    lines
        .iter()
        .filter(|line| line.starts_with("mirror: pass at"))
        .count()
}

fn row(mirror: &Path, id: u64) -> Option<seeded_archive::ingest::mirror::StatusRow> {
    MirrorStatus::open(mirror).unwrap().row(id).unwrap()
}

#[tokio::test]
async fn car_package_is_mirrored_once_then_served_from_the_mirror_with_upstream_down() {
    let (mut dag, root, sketch) = artwork();
    let gateway = server("/ipfs/", gateway_handler(dag.clone())).await;
    let dir = temp("car-serve");
    let mirror = dir.join("mirror");
    let writer = writer(&mirror, 1 << 30);
    let fetcher = ContentStore::new(fetch_config(&dir, &[&gateway], None)).unwrap();
    let uri = format!("ipfs://{root}");
    let stats = fetcher.mirror_package(&uri, &writer).await.unwrap();
    assert_eq!((stats.files, stats.local), (3, false));
    assert_eq!(stats.bytes, (INDEX.len() + sketch.len() + 10) as u64);
    assert_eq!(stats.new_bytes, writer.used_bytes());
    assert_eq!(
        stored(&mirror, "ipfs"),
        7,
        "every block once, the chunked file's node too"
    );
    assert_eq!(gateway.calls().len(), 1, "one dag-scope=all CAR");
    assert!(gateway.calls()[0].contains("dag-scope=all"));
    let again = fetcher.mirror_package(&uri, &writer).await.unwrap();
    assert!(again.local);
    assert_eq!((again.files, again.new_bytes), (3, 0));
    assert_eq!(
        gateway.calls().len(),
        1,
        "a stored package needs no request"
    );
    assert_eq!(staging(&mirror), 0);
    drop(gateway);

    let server = ContentStore::new(serve_config(&dir, &mirror)).unwrap();
    let page = server.get_ipfs(&root.to_string(), "").await.unwrap();
    assert_eq!(page.body.as_ref(), INDEX);
    assert_eq!(page.verification, Verification::IpfsCar);
    assert!(page.directory_index && page.from_cache);
    assert_eq!(
        page.content_type.as_deref(),
        Some("text/html; charset=utf-8")
    );
    let script = server
        .get_ipfs(&root.to_string(), "js/sketch.js")
        .await
        .unwrap();
    assert_eq!(script.body.as_ref(), sketch.as_slice());
    assert_eq!(
        script.content_type.as_deref(),
        Some("text/javascript; charset=utf-8")
    );
    let js_dir = dag.children[&root][1];
    let sketch_file = dag.children[&js_dir][1];
    assert_eq!(script.etag, format!("\"ipfs-{sketch_file}\""));
    let missing = server.get_ipfs(&root.to_string(), "js/missing.js").await;
    assert_eq!(missing.unwrap_err().kind, ErrorKind::NotFound);

    // The mirror trusts its stored blocks on first read. A fresh store sees the changed
    // bytes, while the stored file CID keeps the validator stable.
    let mut changed_sketch = sketch.clone();
    changed_sketch[0] ^= 1;
    replace_mirror_bytes(&mirror, "ipfs", &sketch[..18], &changed_sketch[..18]);
    let fresh = ContentStore::new(serve_config(&dir, &mirror)).unwrap();
    let changed = fresh
        .get_ipfs(&root.to_string(), "js/sketch.js")
        .await
        .unwrap();
    assert_eq!(changed.body.as_ref(), changed_sketch.as_slice());
    assert_eq!(changed.verification, Verification::IpfsCar);
    assert_eq!(changed.etag, script.etag);
    assert_eq!(changed.content_encoding, script.content_encoding);
    assert_eq!(changed.body.len(), sketch.len());
    assert!(
        !dir.join("serve-cache/ipfs-car-v1").exists(),
        "mirror hits never fill the playback cache"
    );
    // Something the mirror lacks still goes upstream, which is down.
    let other = dag.file(b"unmirrored").0;
    let error = server.get_ipfs(&other.to_string(), "").await.unwrap_err();
    assert_eq!(error.kind, ErrorKind::Unavailable);
}

#[tokio::test]
async fn gateway_refusing_dag_scope_all_falls_back_to_entity_traversal() {
    let (dag, root, _) = artwork();
    let handler: Handler = Arc::new(move |uri| {
        if uri.query().unwrap_or_default().contains("dag-scope=all") {
            Reply::status(400)
        } else {
            dag.serve(uri)
        }
    });
    let gateway = server("/ipfs/", handler).await;
    let dir = temp("entity");
    let mirror = dir.join("mirror");
    let writer = writer(&mirror, 1 << 30);
    let fetcher = ContentStore::new(fetch_config(&dir, &[&gateway], None)).unwrap();
    let stats = fetcher
        .mirror_package(&format!("ipfs://{root}"), &writer)
        .await
        .unwrap();
    assert_eq!(stats.files, 3);
    let calls = gateway.calls();
    assert_eq!(
        calls.len(),
        6,
        "one refused CAR, then root, js and three files"
    );
    assert!(
        calls[1..]
            .iter()
            .all(|call| call.contains("dag-scope=entity"))
    );
    let server = ContentStore::new(serve_config(&dir, &mirror)).unwrap();
    let lib = server
        .get_ipfs(&root.to_string(), "js/lib.js")
        .await
        .unwrap();
    assert_eq!(lib.body.as_ref(), b"// library");
}

#[derive(Clone, Default)]
struct Chain {
    inodes: HashMap<String, Value>,
    chunks: HashMap<String, String>,
}

impl Chain {
    fn file(&mut self, chunks: &[&[u8]], metadata: &[u8]) -> String {
        let pointers: Vec<String> = chunks
            .iter()
            .map(|chunk| {
                let pointer = hex::encode(keccak(chunk));
                self.chunks.insert(pointer.clone(), hex::encode(chunk));
                pointer
            })
            .collect();
        let body = chunks.concat();
        let cid = hex::encode(file_cid(&keccak(&body), metadata));
        self.inodes.insert(
            cid.clone(),
            json!({"active": true, "value": {"file": {
                "metadata": hex::encode(metadata), "chunk_pointers": pointers}}}),
        );
        cid
    }

    fn dir(&mut self, entries: &[(&str, &str)]) -> String {
        let hashed: std::collections::BTreeMap<String, Hash> = entries
            .iter()
            .map(|(name, cid)| {
                let mut hash = [0u8; 32];
                hex::decode_to_slice(cid, &mut hash).unwrap();
                (name.to_string(), hash)
            })
            .collect();
        let cid = hex::encode(directory_cid(&hashed));
        let named: HashMap<_, _> = entries.iter().copied().collect();
        self.inodes.insert(
            cid.clone(),
            json!({"active": true, "value": {"directory": named}}),
        );
        cid
    }

    fn serve(&self, uri: &Uri) -> Reply {
        if let Some(cid) = uri.path().strip_prefix("/v1/bigmaps/549145/keys/") {
            return self
                .inodes
                .get(cid)
                .cloned()
                .map_or_else(|| Reply::status(204), Reply::json);
        }
        let query: HashMap<_, _> = uri
            .query()
            .unwrap_or_default()
            .split('&')
            .filter_map(|pair| pair.split_once('='))
            .collect();
        let keys = query.get("key").or_else(|| query.get("key.in")).unwrap();
        Reply::json(json!(
            keys.split(',')
                .filter_map(|key| self
                    .chunks
                    .get(key)
                    .map(|value| json!({"key": key, "value": value, "active": true})))
                .collect::<Vec<_>>()
        ))
    }
}

#[tokio::test]
async fn onchfs_package_is_mirrored_and_served_from_the_mirror_with_tzkt_down() {
    let mut chain = Chain::default();
    // content-type: text/html, content-encoding: gzip (a real fxhash index's headers).
    let headers = hex::decode("5f87497ca589d34d1f5a839bd9ab").unwrap();
    let index = chain.file(&[b"<html>", b"onchain"], &headers);
    let script = chain.file(&[b"draw()"], &[]);
    let lib = chain.dir(&[("sketch.js", &script)]);
    let root = chain.dir(&[("index.html", &index), ("lib", &lib)]);
    let handler: Handler = {
        let chain = chain.clone();
        Arc::new(move |uri| chain.serve(uri))
    };
    let tzkt = server("", handler).await;
    let dir = temp("onchfs");
    let mirror = dir.join("mirror");
    let writer = writer(&mirror, 1 << 30);
    let fetcher = ContentStore::new(fetch_config(&dir, &[], Some(&tzkt))).unwrap();
    let uri = format!("onchfs://{root}");
    let stats = fetcher.mirror_package(&uri, &writer).await.unwrap();
    assert_eq!((stats.files, stats.bytes, stats.local), (2, 19, false));
    assert_eq!(stored(&mirror, "onchfs"), 4);
    assert!(fetcher.mirror_package(&uri, &writer).await.unwrap().local);
    drop(tzkt);
    let server = ContentStore::new(serve_config(&dir, &mirror)).unwrap();
    let page = server.get_onchfs(&root, "").await.unwrap();
    assert_eq!(page.body.as_ref(), b"<html>onchain");
    assert_eq!(page.verification, Verification::OnchfsKeccak);
    assert_eq!(page.content_encoding.as_deref(), Some("gzip"));
    assert!(page.directory_index && page.from_cache);
    assert_eq!(page.etag, format!("\"{index}\""));
    let sketch = server.get_onchfs(&root, "lib/sketch.js").await.unwrap();
    assert_eq!(sketch.body.as_ref(), b"draw()");
    assert_eq!(sketch.etag, format!("\"{script}\""));

    // A fresh store serves same-length changed record content without hashing its body.
    let changed_sketch = b"drAw()";
    replace_mirror_bytes(&mirror, "onchfs", b"draw()", changed_sketch);
    let fresh = ContentStore::new(serve_config(&dir, &mirror)).unwrap();
    let changed = fresh.get_onchfs(&root, "lib/sketch.js").await.unwrap();
    assert_eq!(changed.body.as_ref(), changed_sketch);
    assert_eq!(changed.verification, Verification::OnchfsKeccak);
    assert_eq!(changed.etag, sketch.etag);
    assert_eq!(changed.content_encoding, sketch.content_encoding);

    let absent = server.get_onchfs(&root, "lib/other.js").await.unwrap_err();
    assert_eq!(absent.kind, ErrorKind::NotFound);
}

#[tokio::test]
async fn playback_object_limit_applies_to_mirrored_ipfs_and_onchfs_files() {
    let oversized = vec![b'x'; 64];

    let mut dag = Dag::default();
    let index = dag.file(&oversized);
    let root = dag.dir(&[("index.html", index)], true).0;
    let gateway = server("/ipfs/", gateway_handler(dag)).await;
    let dir = temp("mirror-playback-limit");
    let ipfs_mirror = dir.join("ipfs-mirror");
    let ipfs_writer = writer(&ipfs_mirror, 1 << 30);
    let ipfs_fetcher = ContentStore::new(fetch_config(&dir, &[&gateway], None)).unwrap();
    ipfs_fetcher
        .mirror_package(&format!("ipfs://{root}"), &ipfs_writer)
        .await
        .unwrap();
    drop(gateway);

    let normal_ipfs = ContentStore::new(serve_config(&dir, &ipfs_mirror)).unwrap();
    assert_eq!(
        normal_ipfs
            .get_ipfs(&root.to_string(), "")
            .await
            .unwrap()
            .body
            .as_ref(),
        oversized.as_slice()
    );
    let mut ipfs_config = serve_config(&dir, &ipfs_mirror);
    ipfs_config.max_object_bytes = 16;
    let ipfs_playback = ContentStore::new(ipfs_config).unwrap();
    assert!(
        ipfs_playback
            .get_local("ipfs", &root.to_string(), "")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        ipfs_playback
            .get_ipfs(&root.to_string(), "")
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Unavailable
    );

    let mut chain = Chain::default();
    let index = chain.file(&[oversized.as_slice()], &[]);
    let root = chain.dir(&[("index.html", &index)]);
    let handler: Handler = {
        let chain = chain.clone();
        Arc::new(move |uri| chain.serve(uri))
    };
    let tzkt = server("", handler).await;
    let onchfs_mirror = dir.join("onchfs-mirror");
    let onchfs_writer = writer(&onchfs_mirror, 1 << 30);
    let onchfs_fetcher = ContentStore::new(fetch_config(&dir, &[], Some(&tzkt))).unwrap();
    onchfs_fetcher
        .mirror_package(&format!("onchfs://{root}"), &onchfs_writer)
        .await
        .unwrap();
    drop(tzkt);

    let normal_onchfs = ContentStore::new(serve_config(&dir, &onchfs_mirror)).unwrap();
    assert_eq!(
        normal_onchfs
            .get_onchfs(&root, "")
            .await
            .unwrap()
            .body
            .as_ref(),
        oversized.as_slice()
    );
    let mut onchfs_config = serve_config(&dir, &onchfs_mirror);
    onchfs_config.max_object_bytes = 16;
    let onchfs_playback = ContentStore::new(onchfs_config).unwrap();
    assert!(
        onchfs_playback
            .get_local("onchfs", &root, "")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        onchfs_playback
            .get_onchfs(&root, "")
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Unavailable
    );
}

#[tokio::test]
async fn cap_skips_a_too_big_package_and_the_run_continues_with_smaller_ones() {
    let mut dag = Dag::default();
    let big_file = dag.chunked(&[&[1u8; 150_000], &[2u8; 150_000]]);
    let big = dag.dir(&[("index.html", big_file)], true).0;
    let mut undeclared = dag.chunked(&[&[3u8; 150_000], &[4u8; 150_000]]);
    undeclared.1 = 0;
    let hidden_size = dag.dir(&[("index.html", undeclared)], false).0;
    let small_file = dag.file(INDEX);
    let small = dag.dir(&[("index.html", small_file)], true).0;
    let gateway = server("/ipfs/", gateway_handler(dag)).await;
    let dir = temp("cap");
    let mirror = dir.join("mirror");
    let writer = writer(&mirror, 256 << 10);
    let fetcher = ContentStore::new(fetch_config(&dir, &[&gateway], None)).unwrap();
    let projects = list(&[
        (1, &format!("ipfs://{big}"), 900),
        (2, &format!("ipfs://{hidden_size}"), 800),
        (3, &format!("ipfs://{small}"), 700),
    ]);
    let untiered = || MirrorOptions {
        tiers: None,
        ..MirrorOptions::default()
    };
    let result = run(&projects, &fetcher, &writer, untiered()).await;
    assert_eq!((result.ok, result.started), (1, 3));
    // Only blocks actually staged count; a package in flight may defer the other to the
    // next run until it fails and releases them.
    assert_eq!(result.too_big + result.deferred, 2);
    let again = run(&projects, &fetcher, &writer, untiered()).await;
    assert_eq!(
        (again.started, again.too_big),
        (result.deferred, result.deferred)
    );
    let declared = row(&mirror, 1).unwrap();
    assert_eq!(declared.state, MirrorState::TooBig);
    assert!(declared.bytes > 256 << 10, "the bytes it needed are kept");
    assert_eq!(row(&mirror, 2).unwrap().state, MirrorState::TooBig);
    assert_eq!(row(&mirror, 3).unwrap().state, MirrorState::Ok);
    assert_eq!(stored(&mirror, "ipfs"), 2, "only the small package");
    assert_eq!(staging(&mirror), 0);
    assert!(writer.used_bytes() <= 256 << 10);
    // A file beyond the object limit is too big whatever the cap.
    let mut dag = Dag::default();
    let huge = dag.chunked(&[&[5u8; 600_000], &[6u8; 600_000]]);
    let package = dag.dir(&[("video.mp4", huge)], true).0;
    let gateway = server("/ipfs/", gateway_handler(dag)).await;
    let roomy = Arc::new(MirrorWriter::open(&dir.join("roomy"), 1 << 30).unwrap());
    let fetcher = ContentStore::new(fetch_config(&dir, &[&gateway], None)).unwrap();
    let error = fetcher
        .mirror_package(&format!("ipfs://{package}"), &roomy)
        .await
        .unwrap_err();
    assert!(matches!(error, PackageError::TooBig { .. }), "{error:?}");
    assert_eq!(stored(&dir.join("roomy"), "ipfs"), 0);
}

static RETRY_NOW: AtomicI64 = AtomicI64::new(1_800_000_000);

fn retry_clock() -> i64 {
    RETRY_NOW.load(Ordering::SeqCst)
}

#[tokio::test]
async fn transient_failure_stays_pending_and_is_retried_after_its_backoff() {
    let (dag, root, _) = artwork();
    let healthy = Arc::new(AtomicBool::new(false));
    let handler: Handler = {
        let healthy = healthy.clone();
        Arc::new(move |uri| {
            if healthy.load(Ordering::SeqCst) {
                dag.serve(uri)
            } else {
                Reply::status(503)
            }
        })
    };
    let first = server("/ipfs/", handler.clone()).await;
    let second = server("/ipfs/", handler).await;
    let dir = temp("transient");
    let mirror = dir.join("mirror");
    let writer = writer(&mirror, 1 << 30);
    let fetcher = ContentStore::new(fetch_config(&dir, &[&first, &second], None)).unwrap();
    let projects = list(&[(7, &format!("ipfs://{root}"), 1)]);
    let opts = MirrorOptions {
        clock: retry_clock,
        ..MirrorOptions::default()
    };
    let result = run(&projects, &fetcher, &writer, opts.clone()).await;
    assert_eq!((result.pending, result.ok), (1, 0));
    let pending = row(&mirror, 7).unwrap();
    assert_eq!(pending.state, MirrorState::Pending);
    assert_eq!((pending.attempts, pending.not_found), (1, 0));
    assert!(pending.last_error.unwrap().contains("503"));
    assert_eq!(stored(&mirror, "ipfs"), 0);
    // Not due again before the backoff.
    let early = run(&projects, &fetcher, &writer, opts.clone()).await;
    assert_eq!((early.due, early.started), (0, 0));
    RETRY_NOW.fetch_add(backoff(1), Ordering::SeqCst);
    healthy.store(true, Ordering::SeqCst);
    let retried = run(&projects, &fetcher, &writer, opts).await;
    assert_eq!(retried.ok, 1);
    let ok = row(&mirror, 7).unwrap();
    assert_eq!((ok.state, ok.attempts, ok.files), (MirrorState::Ok, 0, 3));
    assert_eq!(ok.last_error, None);
}

static MISSING_NOW: AtomicI64 = AtomicI64::new(1_800_000_000);

fn missing_clock() -> i64 {
    MISSING_NOW.load(Ordering::SeqCst)
}

#[tokio::test]
async fn repeated_not_found_across_gateways_and_runs_becomes_missing() {
    let absent = Dag::default().file(b"never uploaded").0;
    let (dag, half, _) = artwork();
    let gone: Handler = Arc::new(|_| Reply::status(404));
    let first = server("/ipfs/", gone.clone()).await;
    let second = server("/ipfs/", gone).await;
    let busy = server("/ipfs/", Arc::new(|_| Reply::status(502))).await;
    let dir = temp("missing");
    let mirror = dir.join("mirror");
    let writer = writer(&mirror, 1 << 30);
    let fetcher = ContentStore::new(fetch_config(&dir, &[&first, &second], None)).unwrap();
    let projects = list(&[(5, &format!("ipfs://{absent}"), 1)]);
    let opts = MirrorOptions {
        clock: missing_clock,
        ..MirrorOptions::default()
    };
    for attempt in 1..=3u32 {
        let result = run(&projects, &fetcher, &writer, opts.clone()).await;
        assert_eq!(result.started, 1);
        let state = row(&mirror, 5).unwrap();
        assert_eq!(state.not_found, attempt);
        let expected = if attempt < 3 {
            MirrorState::Pending
        } else {
            MirrorState::Missing
        };
        assert_eq!(state.state, expected);
        MISSING_NOW.fetch_add(backoff(attempt), Ordering::SeqCst);
    }
    let after = run(&projects, &fetcher, &writer, opts.clone()).await;
    assert_eq!(after.due, 0, "missing is final unless --recheck");
    let rechecked = run(
        &projects,
        &fetcher,
        &writer,
        MirrorOptions {
            recheck: true,
            ..opts.clone()
        },
    )
    .await;
    assert_eq!(rechecked.started, 1);
    // One 404 while the other gateway fails is no answer.
    let mixed = ContentStore::new(fetch_config(&dir, &[&first, &busy], None)).unwrap();
    let projects = list(&[(6, &format!("ipfs://{half}"), 1)]);
    run(&projects, &mixed, &writer, opts).await;
    let state = row(&mirror, 6).unwrap();
    assert_eq!((state.state, state.not_found), (MirrorState::Pending, 0));
    drop(dag);
}

#[tokio::test]
async fn interrupted_and_limited_runs_resume_without_recording_partial_packages() {
    let mut dag = Dag::default();
    let mut roots = Vec::new();
    for n in 0..3u8 {
        let file = dag.file(&[n; 64]);
        roots.push(dag.dir(&[("index.html", file)], true).0);
    }
    let slow = Arc::new(AtomicBool::new(true));
    let handler: Handler = {
        let dag = dag.clone();
        let slow = slow.clone();
        Arc::new(move |uri| {
            let mut reply = dag.serve(uri);
            if slow.load(Ordering::SeqCst) {
                reply.delay = Duration::from_secs(1);
            }
            reply
        })
    };
    let gateway = server("/ipfs/", handler).await;
    let dir = temp("resume");
    let mirror = dir.join("mirror");
    // A crash left staging behind; opening clears it.
    std::fs::create_dir_all(mirror.join("tmp/999-0")).unwrap();
    std::fs::write(mirror.join("tmp/999-0/ipfs-00"), b"partial").unwrap();
    let writer = writer(&mirror, 1 << 30);
    assert_eq!(staging(&mirror), 0);
    assert_eq!(
        MirrorWriter::open(&mirror, 1 << 30).err().unwrap().kind(),
        std::io::ErrorKind::WouldBlock,
        "one writer per directory"
    );
    let fetcher = ContentStore::new(fetch_config(&dir, &[&gateway], None)).unwrap();
    let projects = list(&[
        (1, &format!("ipfs://{}", roots[0]), 30),
        (2, &format!("ipfs://{}", roots[1]), 20),
        (3, &format!("ipfs://{}", roots[2]), 10),
    ]);
    let stop = Arc::new(AtomicBool::new(false));
    let trigger = stop.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        trigger.store(true, Ordering::SeqCst);
    });
    let mut status = MirrorStatus::open(&mirror).unwrap();
    let interrupted = run_mirror(
        &projects,
        &mut status,
        &fetcher,
        &writer,
        &MirrorOptions::default(),
        &stop,
        &|_| {},
    )
    .await
    .unwrap();
    assert!(interrupted.interrupted);
    assert_eq!((interrupted.started, interrupted.ok), (2, 0));
    assert!(
        status.rows().unwrap().is_empty(),
        "nothing recorded in flight"
    );
    drop(status);
    tokio::time::timeout(Duration::from_secs(3), async {
        while staging(&mirror) != 0 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("cancelled staging is removed");
    assert_eq!(stored(&mirror, "ipfs"), 0);
    slow.store(false, Ordering::SeqCst);
    let limited = run(
        &projects,
        &fetcher,
        &writer,
        MirrorOptions {
            limit: Some(1),
            ..MirrorOptions::default()
        },
    )
    .await;
    assert!(limited.limited);
    assert_eq!((limited.due, limited.ok), (3, 1));
    assert_eq!(row(&mirror, 1).unwrap().state, MirrorState::Ok);
    let calls = scoped(&gateway, "all");
    let rest = run(&projects, &fetcher, &writer, MirrorOptions::default()).await;
    assert_eq!((rest.due, rest.ok, rest.limited), (2, 2, false));
    assert_eq!(
        scoped(&gateway, "all"),
        calls + 2,
        "the ok package is skipped"
    );
    assert_eq!(stored(&mirror, "ipfs"), 6);
    let report = MirrorStatus::open_read_only(&mirror)
        .unwrap()
        .report(5)
        .unwrap();
    assert_eq!((report.listed, report.unvisited), (3, 0));
    assert_eq!(report.ok_volume_mutez, 60);
    assert_eq!(report.states[0], (MirrorState::Ok, 3, 192, 3));
}

#[tokio::test]
async fn upstream_tampering_is_rejected_and_mirror_tampering_is_repaired() {
    let (dag, root, _) = artwork();
    let tampered: Handler = {
        let dag = dag.clone();
        Arc::new(move |uri| {
            let mut reply = dag.serve(uri);
            if reply.status == 200 {
                *reply.body.last_mut().unwrap() ^= 1;
            }
            reply
        })
    };
    let plain: Handler = Arc::new(|_| Reply {
        kind: "text/html",
        body: INDEX.to_vec(),
        ..Reply::status(200)
    });
    let bad = server("/ipfs/", tampered).await;
    let unproven = server("/ipfs/", plain).await;
    let dir = temp("unverified");
    let mirror = dir.join("mirror");
    let writer = writer(&mirror, 1 << 30);
    let fetcher = ContentStore::new(fetch_config(&dir, &[&bad, &unproven], None)).unwrap();
    let projects = list(&[(9, &format!("ipfs://{root}"), 1)]);
    let result = run(&projects, &fetcher, &writer, MirrorOptions::default()).await;
    assert_eq!(result.failed, 1);
    assert_eq!(row(&mirror, 9).unwrap().state, MirrorState::Failed);
    assert_eq!(stored(&mirror, "ipfs"), 0);
    assert_eq!(writer.used_bytes(), 0);
    assert_eq!(staging(&mirror), 0);

    // Upstream tampering was rejected above. For an already mirrored object, serve the
    // stored bytes as-is on a fresh first read; the writer still verifies and repairs it.
    let good = server("/ipfs/", gateway_handler(dag.clone())).await;
    let healthy = ContentStore::new(fetch_config(&dir, &[&good], None)).unwrap();
    healthy
        .mirror_package(&format!("ipfs://{root}"), &writer)
        .await
        .unwrap();
    let pristine = ContentStore::new(serve_config(&dir, &mirror)).unwrap();
    let original = pristine.get_ipfs(&root.to_string(), "").await.unwrap();
    let mut changed_index = INDEX.to_vec();
    changed_index[0] ^= 1;
    replace_mirror_bytes(&mirror, "ipfs", INDEX, &changed_index);
    let server = ContentStore::new(serve_config(&dir, &mirror)).unwrap();
    let damaged_page = server.get_ipfs(&root.to_string(), "").await.unwrap();
    assert_eq!(damaged_page.body.as_ref(), changed_index.as_slice());
    assert_eq!(damaged_page.verification, Verification::IpfsCar);
    assert_eq!(damaged_page.etag, original.etag);
    let repaired = healthy
        .mirror_package(&format!("ipfs://{root}"), &writer)
        .await
        .unwrap();
    assert!(!repaired.local);
    let fresh = ContentStore::new(serve_config(&dir, &mirror)).unwrap();
    let page = fresh.get_ipfs(&root.to_string(), "").await.unwrap();
    assert_eq!(page.body.as_ref(), INDEX);
    assert_eq!(page.etag, original.etag);
    assert_eq!(damaged_page.body.as_ref(), changed_index.as_slice());
}

#[test]
fn status_database_is_created_once_and_refuses_other_databases() {
    let dir = temp("status-db");
    assert!(MirrorStatus::open_read_only(&dir).is_err());
    let mut status = MirrorStatus::open(&dir).unwrap();
    let project = ListedProject {
        id: 4,
        root_uri: "ipfs://a".into(),
        volume_mutez: 1,
    };
    let outcome = seeded_archive::ingest::mirror::Outcome::Transient("timeout".into());
    status.record(&project, &outcome, 100).unwrap();
    status.record(&project, &outcome, 200).unwrap();
    drop(status);
    let reopened = MirrorStatus::open(&dir).unwrap();
    let kept = reopened.row(4).unwrap().unwrap();
    assert_eq!((kept.attempts, kept.checked_unix), (2, 200));
    assert_eq!(kept.checked_at, "1970-01-01T00:03:20Z");
    // A new root starts over.
    let mut reopened = reopened;
    let moved = ListedProject {
        root_uri: "ipfs://b".into(),
        ..project
    };
    reopened.record(&moved, &outcome, 300).unwrap();
    assert_eq!(reopened.row(4).unwrap().unwrap().attempts, 1);
    let other = temp("status-db-other");
    Store::open(&other.join("status.sqlite")).unwrap();
    assert!(MirrorStatus::open(&other).is_err());
    assert!(MirrorStatus::open_read_only(&other).is_err());
}

#[test]
fn mirror_list_holds_visible_projects_by_volume_then_id() {
    let dir = temp("list");
    let db = dir.join("main.sqlite");
    Store::open(&db).unwrap();
    let c = Connection::open(&db).unwrap();
    for (id, hidden, root) in [
        (10, 0, Some("ipfs://QmA")),
        (11, 0, Some("onchfs://0a")),
        (12, 1, Some("ipfs://QmHidden")),
        (13, 0, Some("https://example.org/x")),
        (14, 0, None),
        (15, 0, Some("ipfs://QmB")),
    ] {
        c.execute(
            "INSERT INTO projects (id, title, title_key, artist_key, index_state, hidden, \
             generative_uri, artifact_uri) VALUES (?1, 't', 't', 'a', 'indexed', ?2, ?3, ?3)",
            params![id, hidden, root],
        )
        .unwrap();
    }
    for (id, volume) in [(11, 5), (12, 99), (15, 7)] {
        c.execute(
            "INSERT INTO project_volume VALUES (?1, ?2, ?2, 0, 1, 0, 'now', NULL)",
            params![id, volume],
        )
        .unwrap();
    }
    drop(c);
    let out = dir.join("list.json");
    let (listed, skipped) = write_list(&Store::open_read_only(&db).unwrap(), &out).unwrap();
    assert_eq!((listed, skipped), (3, 1));
    let read = read_list(&out).unwrap();
    let order: Vec<(u64, u64)> = read
        .projects
        .iter()
        .map(|p| (p.id, p.volume_mutez))
        .collect();
    assert_eq!(order, [(15, 7), (11, 5), (10, 0)]);
    std::fs::write(&out, br#"{"version":2,"generated_at":"x","projects":[]}"#).unwrap();
    assert!(read_list(&out).is_err());
}

fn section(cid: Cid, data: &[u8]) -> Vec<u8> {
    let mut out = varint((cid.to_bytes().len() + data.len()) as u64);
    out.extend(cid.to_bytes());
    out.extend(data);
    out
}

/// A gateway sending `prefix`, then `unit` until the client hangs up.
struct Endless {
    base: String,
    requests: Arc<AtomicU64>,
    sent: Arc<AtomicU64>,
    closed: Arc<AtomicU64>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Endless {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn endless(prefix: Vec<u8>, unit: Vec<u8>) -> Endless {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}/ipfs/", listener.local_addr().unwrap());
    let counters = (
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
    );
    let (requests, sent, closed) = counters.clone();
    let chunk = unit.repeat((64 << 10) / unit.len() + 1);
    let task = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let (requests, sent, closed) = counters.clone();
            let (prefix, chunk) = (prefix.clone(), chunk.clone());
            tokio::spawn(async move {
                let mut head = Vec::new();
                let mut buffer = [0u8; 4096];
                while !head.windows(4).any(|end| end == b"\r\n\r\n") {
                    match socket.read(&mut buffer).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => head.extend_from_slice(&buffer[..n]),
                    }
                }
                requests.fetch_add(1, Ordering::SeqCst);
                let mut out =
                    format!("HTTP/1.1 200 OK\r\nContent-Type: {CAR}\r\nConnection: close\r\n\r\n")
                        .into_bytes();
                out.extend(prefix);
                if socket.write_all(&out).await.is_ok() {
                    while socket.write_all(&chunk).await.is_ok() {
                        sent.fetch_add(chunk.len() as u64, Ordering::SeqCst);
                    }
                }
                closed.fetch_add(1, Ordering::SeqCst);
            });
        }
    });
    Endless {
        base,
        requests,
        sent,
        closed,
        task,
    }
}

#[tokio::test]
async fn endless_duplicate_or_empty_blocks_are_cut_off_and_the_next_gateway_serves() {
    let (mut dag, root, _) = artwork();
    let second = dag.file(b"second");
    let other = dag.dir(&[("index.html", second)], true).0;
    let root_block = dag.block(&root);
    // The root, then the root again for ever.
    let mut prefix = car(root, &[]);
    prefix.extend(section(root, &root_block));
    let duplicates = endless(prefix, section(root, &root_block)).await;
    // Empty blocks nothing links, for ever.
    let empty = Dag::default().file(b"").0;
    let tiny = endless(car(root, &[]), section(empty, b"")).await;
    let good = server("/ipfs/", gateway_handler(dag)).await;
    let dir = temp("endless");
    let mirror = dir.join("mirror");
    let writer = writer(&mirror, 1 << 30);
    let mut config = fetch_config(&dir, &[&good], None);
    config.ipfs_gateways = vec![
        duplicates.base.clone(),
        tiny.base.clone(),
        good.base.clone(),
    ];
    config.breaker.failure_threshold = 1;
    config.breaker.base_cooldown = Duration::from_secs(60);
    config.breaker.max_cooldown = Duration::from_secs(60);
    let fetcher = ContentStore::new(config).unwrap();
    let uri = format!("ipfs://{root}");
    let stats = tokio::time::timeout(
        Duration::from_secs(60),
        fetcher.mirror_package(&uri, &writer),
    )
    .await
    .expect("wasteful streams are cut off")
    .unwrap();
    assert_eq!((stats.files, stats.local), (3, false));
    assert_eq!(stored(&mirror, "ipfs"), 7);
    assert_eq!(staging(&mirror), 0);
    assert_eq!(good.calls().len(), 1);
    for gateway in [&duplicates, &tiny] {
        assert_eq!(gateway.requests.load(Ordering::SeqCst), 1);
        tokio::time::timeout(Duration::from_secs(10), async {
            while gateway.closed.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the transfer is cancelled");
        let sent = gateway.sent.load(Ordering::SeqCst);
        // A few MiB read before the cut, plus socket buffers; without it, endless.
        assert!(sent < 64 << 20, "bounded work, {sent} bytes sent");
    }
    // Both were marked failed: the next package does not ask them.
    fetcher
        .mirror_package(&format!("ipfs://{other}"), &writer)
        .await
        .unwrap();
    assert_eq!(duplicates.requests.load(Ordering::SeqCst), 1);
    assert_eq!(tiny.requests.load(Ordering::SeqCst), 1);
    assert_eq!(good.calls().len(), 2);
}

#[tokio::test]
async fn an_oversized_car_section_fails_that_gateway_not_the_package() {
    let (dag, root, _) = artwork();
    let mut oversized = car(root, &[]);
    oversized.extend(varint(2 << 20));
    let bad = server("/ipfs/", Arc::new(move |_| Reply::car(oversized.clone()))).await;
    let good = server("/ipfs/", gateway_handler(dag)).await;
    let dir = temp("oversized-section");
    let mirror = dir.join("mirror");
    let writer = writer(&mirror, 1 << 30);
    let uri = format!("ipfs://{root}");
    let alone = ContentStore::new(fetch_config(&dir, &[&bad], None)).unwrap();
    let error = alone.mirror_package(&uri, &writer).await.unwrap_err();
    assert!(matches!(error, PackageError::Transient { .. }), "{error:?}");
    let fetcher = ContentStore::new(fetch_config(&dir, &[&bad, &good], None)).unwrap();
    let stats = fetcher.mirror_package(&uri, &writer).await.unwrap();
    assert_eq!(stats.files, 3);
    assert_eq!(good.calls().len(), 1);
    assert_eq!(stored(&mirror, "ipfs"), 7);
}

#[tokio::test]
async fn shared_blocks_count_once_against_the_cap() {
    let mut dag = Dag::default();
    let file = dag.file(&[b'x'; 10_000]);
    let first = dag.dir(&[("first.txt", file)], true).0;
    let second = dag.dir(&[("second.txt", file)], true).0;
    let gateway = server("/ipfs/", gateway_handler(dag)).await;
    let dir = temp("shared-cap");
    let mirror = dir.join("mirror");
    let writer = writer(&mirror, 5 * 4096);
    let fetcher = ContentStore::new(fetch_config(&dir, &[&gateway], None)).unwrap();
    fetcher
        .mirror_package(&format!("ipfs://{first}"), &writer)
        .await
        .unwrap();
    assert_eq!(writer.remaining(), 4096);
    // Its root declares the shared file again; only the root is new, and it fits.
    let stats = fetcher
        .mirror_package(&format!("ipfs://{second}"), &writer)
        .await
        .unwrap();
    assert_eq!((stats.files, stats.new_bytes), (1, 4096));
    assert_eq!(writer.remaining(), 0);
    assert_eq!(stored(&mirror, "ipfs"), 3);
}

#[tokio::test]
async fn inline_nodes_are_read_from_their_cid_and_never_fetched() {
    // An empty UnixFS file held in its identity CID.
    let empty = Dag::inline(&[10, 4, 8, 2, 24, 0]);
    let mut dag = Dag::default();
    let index = dag.file(INDEX);
    let root = dag
        .dir(&[("index.html", index), ("empty.txt", empty)], true)
        .0;
    let gateway = server("/ipfs/", gateway_handler(dag)).await;
    let dir = temp("inline");
    let mirror = dir.join("mirror");
    let writer = writer(&mirror, 1 << 30);
    let fetcher = ContentStore::new(fetch_config(&dir, &[&gateway], None)).unwrap();
    let alone = fetcher
        .mirror_package(&format!("ipfs://{}", empty.0), &writer)
        .await
        .unwrap();
    assert_eq!((alone.files, alone.bytes), (1, 0));
    assert!(gateway.calls().is_empty());
    let stats = fetcher
        .mirror_package(&format!("ipfs://{root}"), &writer)
        .await
        .unwrap();
    assert_eq!((stats.files, stats.bytes), (2, INDEX.len() as u64));
    assert_eq!(
        gateway.calls().len(),
        1,
        "one CAR; the inline file is not asked for"
    );
    assert_eq!(stored(&mirror, "ipfs"), 2);
    drop(gateway);
    let server = ContentStore::new(serve_config(&dir, &mirror)).unwrap();
    let page = server
        .get_ipfs(&root.to_string(), "empty.txt")
        .await
        .unwrap();
    assert!(page.body.is_empty());
    assert_eq!(page.verification, Verification::IpfsCar);
}

#[tokio::test]
async fn redirects_are_followed_and_an_unfollowable_one_fails_only_that_gateway() {
    let (dag, root, _) = artwork();
    let good = server("/ipfs/", gateway_handler(dag)).await;
    // To another authority, as a path gateway sending to a subdomain gateway.
    let target = good.base.clone();
    let mover = server(
        "/ipfs/",
        Arc::new(move |uri: &Uri| {
            let rest = uri.path().trim_start_matches("/ipfs/");
            let query = uri.query().unwrap_or_default();
            Reply::redirect(format!("{target}{rest}?{query}"))
        }),
    )
    .await;
    let dir = temp("redirect");
    let writer = writer(&dir.join("mirror"), 1 << 30);
    let fetcher = ContentStore::new(fetch_config(&dir, &[&mover], None)).unwrap();
    let stats = fetcher
        .mirror_package(&format!("ipfs://{root}"), &writer)
        .await
        .unwrap();
    assert_eq!(stats.files, 3);
    assert_eq!((mover.calls().len(), good.calls().len()), (1, 1));
    assert!(good.calls()[0].contains("dag-scope=all"));

    // A loop is cut after four hops, and the other gateway's answer decides.
    let absent = Dag::default().file(b"never uploaded").0;
    let looping = server(
        "/ipfs/",
        Arc::new(|uri: &Uri| Reply::redirect(uri.to_string())),
    )
    .await;
    let gone = server("/ipfs/", Arc::new(|_| Reply::status(404))).await;
    let fetcher = ContentStore::new(fetch_config(&dir, &[&looping, &gone], None)).unwrap();
    let error = fetcher
        .mirror_package(&format!("ipfs://{absent}"), &writer)
        .await
        .unwrap_err();
    assert!(matches!(error, PackageError::NotFound(_)), "{error:?}");
    assert_eq!(
        looping.calls().len(),
        10,
        "the whole DAG, then the entity; five requests each"
    );
}

/// Small packages of `n` bytes each, as directories with `index.html`.
fn small_packages(dag: &mut Dag, count: u8) -> Vec<Cid> {
    (0..count)
        .map(|n| {
            let file = dag.file(&[n; 64]);
            dag.dir(&[("index.html", file)], true).0
        })
        .collect()
}

#[tokio::test]
async fn small_packages_complete_before_a_larger_earlier_listed_one() {
    let mut dag = Dag::default();
    let big_file = dag.chunked(&[&[1u8; 100_000], &[2u8; 100_000]]);
    let big = dag.dir(&[("index.html", big_file)], true).0;
    let small = small_packages(&mut dag, 2);
    let gateway = server("/ipfs/", gateway_handler(dag)).await;
    let dir = temp("tier-order");
    let mirror = dir.join("mirror");
    let writer = writer(&mirror, 1 << 30);
    let fetcher = ContentStore::new(fetch_config(&dir, &[&gateway], None)).unwrap();
    let projects = list(&[
        (1, &format!("ipfs://{big}"), 900),
        (2, &format!("ipfs://{}", small[0]), 800),
        (3, &format!("ipfs://{}", small[1]), 700),
    ]);
    let (result, lines) = logged(
        &projects,
        &fetcher,
        &writer,
        tiered(64),
        &AtomicBool::new(false),
    )
    .await;
    let done: Vec<&str> = lines
        .iter()
        .filter(|line| line.contains(" ok files "))
        .map(|line| line.split(' ').nth(1).unwrap())
        .collect();
    assert_eq!(
        done,
        ["2", "3", "1"],
        "smallest first, list order within a pass"
    );
    assert_eq!((result.ok, result.waiting, result.left), (3, 1, 0));
    assert_eq!(
        scoped(&gateway, "all"),
        3,
        "the estimate kept the big package from a partial download"
    );
    assert_eq!(result.threshold, 256 << 10, "64 KiB, then 256 KiB");
    assert_eq!(passes(&lines), 2);
    assert_eq!(row(&mirror, 1).unwrap().state, MirrorState::Ok);
    assert!(lines.last().unwrap().ends_with("; threshold 0.25 MiB"));
}

/// A package whose links declare no sizes: its estimate is too small to keep it back.
fn undeclared(dag: &mut Dag) -> Cid {
    let mut file = dag.chunked(&[&[3u8; 100_000], &[4u8; 100_000]]);
    file.1 = 0;
    dag.dir(&[("index.html", file)], false).0
}

#[tokio::test]
async fn package_over_the_threshold_waits_without_a_breaker_failure_and_completes_later() {
    let mut dag = Dag::default();
    let root = undeclared(&mut dag);
    let gateway = server("/ipfs/", gateway_handler(dag)).await;
    let dir = temp("tier-wait");
    let mirror = dir.join("mirror");
    let writer = writer(&mirror, 1 << 30);
    let mut config = fetch_config(&dir, &[&gateway], None);
    // One gateway failure would refuse every later request of this test.
    config.breaker.failure_threshold = 1;
    config.breaker.base_cooldown = Duration::from_secs(600);
    config.breaker.max_cooldown = Duration::from_secs(600);
    let fetcher = ContentStore::new(config).unwrap();
    let projects = list(&[(1, &format!("ipfs://{root}"), 1)]);
    let first = run(
        &projects,
        &fetcher,
        &writer,
        MirrorOptions {
            limit: Some(1),
            ..tiered(64)
        },
    )
    .await;
    assert_eq!((first.waiting, first.pending, first.left), (1, 0, 1));
    let waiting = row(&mirror, 1).unwrap();
    assert_eq!(waiting.state, MirrorState::Waiting);
    assert!(
        waiting.bytes > 64 << 10,
        "bytes seen: a verified lower bound"
    );
    assert_eq!(waiting.attempts, 0, "not a failure");
    assert_eq!((stored(&mirror, "ipfs"), staging(&mirror)), (0, 0));
    let rest = run(&projects, &fetcher, &writer, tiered(64)).await;
    assert_eq!((rest.ok, rest.pending), (1, 0));
    assert_eq!(row(&mirror, 1).unwrap().state, MirrorState::Ok);
    assert_eq!(stored(&mirror, "ipfs"), 4);
}

#[tokio::test]
async fn package_is_never_downloaded_twice_below_its_known_size_and_empty_passes_are_skipped() {
    let mut dag = Dag::default();
    let root = undeclared(&mut dag);
    let gateway = server("/ipfs/", gateway_handler(dag)).await;
    let dir = temp("tier-skip");
    let mirror = dir.join("mirror");
    let writer = writer(&mirror, 1 << 30);
    let fetcher = ContentStore::new(fetch_config(&dir, &[&gateway], None)).unwrap();
    let projects = list(&[(1, &format!("ipfs://{root}"), 1)]);
    let (result, lines) = logged(
        &projects,
        &fetcher,
        &writer,
        tiered(64),
        &AtomicBool::new(false),
    )
    .await;
    // 64 KiB stops after the first 100 kB chunk, 128 KiB after the second; 192 KiB is below
    // the 200 kB seen and is skipped; 256 KiB completes.
    assert_eq!((result.ok, result.waiting), (1, 2));
    assert_eq!(passes(&lines), 3);
    assert_eq!(result.threshold, 256 << 10);
    assert_eq!(
        scoped(&gateway, "all"),
        3,
        "one transfer per pass it ran in"
    );
    assert_eq!(scoped(&gateway, "block"), 1, "estimated once");
    let waited = lines
        .iter()
        .filter(|line| line.starts_with("mirror 1 waiting package exceeds the limit"))
        .count();
    assert_eq!(waited, 2);
}

#[tokio::test]
async fn resume_after_interruption_keeps_the_threshold_and_ok_rows() {
    let mut dag = Dag::default();
    let big_file = dag.chunked(&[&[1u8; 100_000], &[2u8; 100_000]]);
    let big = dag.dir(&[("index.html", big_file)], true).0;
    let small = small_packages(&mut dag, 1);
    let slow = Arc::new(AtomicBool::new(true));
    let handler: Handler = {
        let dag = dag.clone();
        let slow = slow.clone();
        let big = big.to_string();
        Arc::new(move |uri| {
            let mut reply = dag.serve(uri);
            let whole = uri.query().unwrap_or_default().contains("dag-scope=all");
            if slow.load(Ordering::SeqCst) && whole && uri.path().contains(&big) {
                reply.delay = Duration::from_secs(2);
            }
            reply
        })
    };
    let gateway = server("/ipfs/", handler).await;
    let dir = temp("tier-resume");
    let mirror = dir.join("mirror");
    let writer = writer(&mirror, 1 << 30);
    let fetcher = ContentStore::new(fetch_config(&dir, &[&gateway], None)).unwrap();
    let projects = list(&[
        (1, &format!("ipfs://{big}"), 900),
        (2, &format!("ipfs://{}", small[0]), 800),
    ]);
    let stop = Arc::new(AtomicBool::new(false));
    let trigger = stop.clone();
    let watched = gateway.calls.clone();
    tokio::spawn(async move {
        // Stops once the big package's transfer is in flight.
        while scoped_calls(&watched) < 2 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        trigger.store(true, Ordering::SeqCst);
    });
    let (interrupted, _) = logged(&projects, &fetcher, &writer, tiered(64), &stop).await;
    assert!(interrupted.interrupted);
    assert_eq!((interrupted.ok, interrupted.waiting), (1, 1));
    assert_eq!(interrupted.threshold, 256 << 10);
    let status = MirrorStatus::open_read_only(&mirror).unwrap();
    assert_eq!(status.meta("threshold").unwrap().as_deref(), Some("262144"));
    assert_eq!(status.row(2).unwrap().unwrap().state, MirrorState::Ok);
    assert_eq!(status.row(1).unwrap().unwrap().state, MirrorState::Waiting);
    drop(status);
    slow.store(false, Ordering::SeqCst);
    let calls = gateway.calls().len();
    let (resumed, lines) = logged(
        &projects,
        &fetcher,
        &writer,
        tiered(64),
        &AtomicBool::new(false),
    )
    .await;
    assert_eq!((resumed.due, resumed.started, resumed.ok), (1, 1, 1));
    assert_eq!(passes(&lines), 1, "straight to the stored threshold");
    assert_eq!(
        gateway.calls().len(),
        calls + 1,
        "the ok package is skipped and the known size needs no estimate"
    );
    let status = MirrorStatus::open_read_only(&mirror).unwrap();
    assert_eq!(status.meta("run_state").unwrap().as_deref(), Some("done"));
}

/// `dag-scope=all` requests so far.
fn scoped_calls(calls: &Calls) -> usize {
    calls
        .lock()
        .unwrap()
        .iter()
        .filter(|call| call.contains("dag-scope=all"))
        .count()
}

#[tokio::test]
async fn existing_ok_rows_of_a_version_1_status_database_stay_ok_after_the_upgrade() {
    let dir = temp("tier-upgrade");
    let mirror = dir.join("mirror");
    std::fs::create_dir_all(&mirror).unwrap();
    {
        let conn = Connection::open(mirror.join("status.sqlite")).unwrap();
        conn.execute_batch(
            "CREATE TABLE mirror_status (
              project_id INTEGER PRIMARY KEY CHECK (project_id >= 0),
              root_uri TEXT NOT NULL,
              state TEXT NOT NULL CHECK (state IN ('pending', 'ok', 'missing', 'too_big', 'failed')),
              bytes INTEGER NOT NULL DEFAULT 0 CHECK (bytes >= 0),
              files INTEGER NOT NULL DEFAULT 0 CHECK (files >= 0),
              attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
              not_found INTEGER NOT NULL DEFAULT 0 CHECK (not_found >= 0),
              last_error TEXT,
              checked_at TEXT NOT NULL
            ) STRICT;
            CREATE INDEX mirror_status_state ON mirror_status (state);
            CREATE TABLE mirror_list (
              position INTEGER PRIMARY KEY,
              project_id INTEGER NOT NULL UNIQUE,
              root_uri TEXT NOT NULL,
              volume_mutez INTEGER NOT NULL CHECK (volume_mutez >= 0)
            ) STRICT;
            CREATE TABLE mirror_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL) STRICT,
              WITHOUT ROWID;
            PRAGMA application_id = 1397050706;
            PRAGMA user_version = 1;",
        )
        .unwrap();
        conn.execute(
            "INSERT INTO mirror_status VALUES (?1, ?2, 'ok', 4096, 3, 0, 0, NULL, ?3), \
             (?4, ?5, 'too_big', 9000, 0, 1, 0, 'a file exceeds the object limit', ?3)",
            params![
                7,
                "ipfs://bafyok",
                "2026-10-01T00:00:00Z",
                8,
                "ipfs://bafybig"
            ],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO mirror_meta VALUES ('run_state', 'cap reached')",
            [],
        )
        .unwrap();
    }
    let before = MirrorStatus::open_read_only(&mirror)
        .unwrap()
        .rows()
        .unwrap();
    assert_eq!(before.len(), 2, "version 1 still reads");
    let mut status = MirrorStatus::open(&mirror).unwrap();
    assert_eq!(status.rows().unwrap(), before, "every row unchanged");
    let version: i64 = Connection::open(mirror.join("status.sqlite"))
        .unwrap()
        .pragma_query_value(None, "user_version", |r| r.get(0))
        .unwrap();
    assert_eq!(version, 4);
    let project = ListedProject {
        id: 9,
        root_uri: "ipfs://bafywait".into(),
        volume_mutez: 1,
    };
    let waiting = seeded_archive::ingest::mirror::Outcome::Waiting {
        bytes: 5 << 20,
        error: "estimated".into(),
        blocked: None,
    };
    assert_eq!(
        status.record(&project, &waiting, 100).unwrap(),
        MirrorState::Waiting
    );
    drop(status);
    // Reopening keeps version 2 and the rows; a run skips the ok and too_big rows.
    let writer = writer(&mirror, 1 << 30);
    let fetcher = ContentStore::new(fetch_config(&dir, &[], None)).unwrap();
    let projects = list(&[(7, "ipfs://bafyok", 2), (8, "ipfs://bafybig", 1)]);
    let result = run(&projects, &fetcher, &writer, MirrorOptions::default()).await;
    assert_eq!((result.due, result.started), (0, 0));
    let after = MirrorStatus::open(&mirror).unwrap().rows().unwrap();
    assert_eq!(after[&7], before[&7]);
    assert_eq!(after[&8], before[&8]);
}

#[tokio::test]
async fn cap_full_defers_the_rest_as_waiting_and_too_big_stays_for_over_limit_objects() {
    let mut dag = Dag::default();
    let big_file = dag.chunked(&[&[1u8; 150_000], &[2u8; 150_000]]);
    let big = dag.dir(&[("index.html", big_file)], true).0;
    let hidden = {
        let mut file = dag.chunked(&[&[3u8; 150_000], &[4u8; 150_000]]);
        file.1 = 0;
        dag.dir(&[("index.html", file)], false).0
    };
    let huge_file = dag.chunked(&[&[5u8; 600_000], &[6u8; 600_000]]);
    let huge = dag.dir(&[("video.mp4", huge_file)], false).0;
    let small = small_packages(&mut dag, 1);
    let gateway = server("/ipfs/", gateway_handler(dag)).await;
    let dir = temp("tier-cap");
    let mirror = dir.join("mirror");
    let writer = writer(&mirror, 256 << 10);
    let fetcher = ContentStore::new(fetch_config(&dir, &[&gateway], None)).unwrap();
    let projects = list(&[
        (1, &format!("ipfs://{big}"), 900),
        (2, &format!("ipfs://{hidden}"), 800),
        (3, &format!("ipfs://{huge}"), 700),
        (4, &format!("ipfs://{}", small[0]), 600),
    ]);
    let (result, _) = logged(
        &projects,
        &fetcher,
        &writer,
        tiered(1024),
        &AtomicBool::new(false),
    )
    .await;
    assert!(result.cap_reached);
    assert_eq!((result.ok, result.too_big, result.deferred), (1, 1, 0));
    assert_eq!(result.left, 2);
    let big_row = row(&mirror, 1).unwrap();
    assert_eq!(
        (big_row.state, big_row.blocked_used.is_some()),
        (MirrorState::Waiting, true),
        "an estimate never blocks: staging measured it"
    );
    let hidden_row = row(&mirror, 2).unwrap();
    assert_eq!(
        hidden_row.state,
        MirrorState::Waiting,
        "seen beyond the room left"
    );
    assert!(hidden_row.bytes > writer.remaining());
    assert_eq!(
        row(&mirror, 3).unwrap().state,
        MirrorState::TooBig,
        "a file beyond the object limit"
    );
    assert_eq!(staging(&mirror), 0);
    assert!(writer.used_bytes() <= 256 << 10);
    // Package 4 stored blocks after both were stopped: they are tried once more.
    let again = run(&projects, &fetcher, &writer, tiered(1024)).await;
    assert!(again.cap_reached);
    assert_eq!((again.started, again.waiting, again.left), (2, 2, 2));
    let calls = gateway.calls().len();
    let parked = run(&projects, &fetcher, &writer, tiered(1024)).await;
    assert!(parked.cap_reached);
    assert_eq!((parked.started, parked.left), (0, 2));
    assert_eq!(gateway.calls().len(), calls, "nothing changed: no request");
    assert_eq!(row(&mirror, 3).unwrap().state, MirrorState::TooBig);
}

/// A gateway sending `body` for every request in paced 64 KiB writes, counting requests
/// and the bytes the client accepted.
struct Paced {
    base: String,
    requests: Arc<AtomicU64>,
    sent: Arc<AtomicU64>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Paced {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn paced(body: Vec<u8>) -> Paced {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}/ipfs/", listener.local_addr().unwrap());
    let (requests, sent) = (Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)));
    let counters = (requests.clone(), sent.clone());
    let task = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let (requests, sent) = counters.clone();
            let body = body.clone();
            tokio::spawn(async move {
                let mut head = Vec::new();
                let mut buffer = [0u8; 4096];
                while !head.windows(4).any(|end| end == b"\r\n\r\n") {
                    match socket.read(&mut buffer).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => head.extend_from_slice(&buffer[..n]),
                    }
                }
                requests.fetch_add(1, Ordering::SeqCst);
                let headers = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: {CAR}\r\nContent-Length: {}\r\n\
                     Connection: close\r\n\r\n",
                    body.len()
                );
                if socket.write_all(headers.as_bytes()).await.is_err() {
                    return;
                }
                for chunk in body.chunks(64 << 10) {
                    if socket.write_all(chunk).await.is_err() {
                        break;
                    }
                    sent.fetch_add(chunk.len() as u64, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(3)).await;
                }
            });
        }
    });
    Paced {
        base,
        requests,
        sent,
        task,
    }
}

fn paced_config(dir: &Path, gateway: &Paced, copies: usize) -> ContentConfig {
    let mut config = fetch_config(dir, &[], None);
    config.ipfs_gateways = vec![gateway.base.clone(); copies];
    config.max_object_bytes = 4 << 20;
    config
}

#[tokio::test]
async fn estimate_probe_reads_at_most_two_mib_of_an_oversized_root_from_two_requests() {
    let mut dag = Dag::default();
    let root = dag.file(&vec![7; 3 << 20]).0;
    let gateway = paced(car(root, &[(root, dag.block(&root))])).await;
    let dir = temp("tier-probe-root");
    let writer = writer(&dir.join("mirror"), 1 << 30);
    let fetcher = ContentStore::new(paced_config(&dir, &gateway, 3)).unwrap();
    let estimate = tokio::time::timeout(
        Duration::from_secs(10),
        fetcher.mirror_estimate(&format!("ipfs://{root}"), &writer),
    )
    .await
    .expect("bounded termination");
    assert_eq!(estimate, None, "beyond the probe bound: unknown");
    assert_eq!(
        gateway.requests.load(Ordering::SeqCst),
        2,
        "two requests of three gateways"
    );
    let sent = gateway.sent.load(Ordering::SeqCst);
    assert!(sent < 2 << 20, "{sent} bytes accepted");
    assert_eq!((writer.used_bytes(), staging(&dir.join("mirror"))), (0, 0));
}

#[tokio::test]
async fn estimate_probe_stops_at_the_root_when_a_gateway_ignores_the_block_scope() {
    let mut dag = Dag::default();
    let files: Vec<Node> = (0..3u8).map(|n| dag.file(&vec![n; 900_000])).collect();
    let root = dag
        .dir(
            &[
                ("a.bin", files[0]),
                ("b.bin", files[1]),
                ("c.bin", files[2]),
            ],
            true,
        )
        .0;
    let gateway = paced(car(root, &dag.subtree(root))).await;
    let dir = temp("tier-probe");
    let mirror = dir.join("mirror");
    let writer = writer(&mirror, 1 << 30);
    let fetcher = ContentStore::new(paced_config(&dir, &gateway, 1)).unwrap();
    let projects = list(&[(1, &format!("ipfs://{root}"), 1)]);
    let result = run(
        &projects,
        &fetcher,
        &writer,
        MirrorOptions {
            limit: Some(1),
            ..tiered(64)
        },
    )
    .await;
    assert_eq!((result.waiting, result.limited), (1, true));
    let waiting = row(&mirror, 1).unwrap();
    assert_eq!(waiting.state, MirrorState::Waiting);
    assert!(waiting.bytes >= 2_700_000, "{}", waiting.bytes);
    assert_eq!(gateway.requests.load(Ordering::SeqCst), 1, "only the probe");
    let sent = gateway.sent.load(Ordering::SeqCst);
    assert!(sent < 2 << 20, "{sent} of 2.7 MB accepted");
    assert_eq!((stored(&mirror, "ipfs"), staging(&mirror)), (0, 0));
}

#[tokio::test]
async fn shared_blocks_fit_under_the_cap_and_a_waiting_package_that_became_local_completes() {
    let mut dag = Dag::default();
    let common = dag.file(&vec![5; 900_000]);
    let first = dag.dir(&[("index.html", common)], true).0;
    let second = dag.dir(&[("other.html", common)], true).0;
    let third = dag.dir(&[("third.html", common)], true).0;
    let gateway = server("/ipfs/", gateway_handler(dag)).await;
    let dir = temp("tier-shared");
    let mirror = dir.join("mirror");
    let writer = writer(&mirror, 1 << 20);
    let fetcher = ContentStore::new(fetch_config(&dir, &[&gateway], None)).unwrap();
    fetcher
        .mirror_package(&format!("ipfs://{first}"), &writer)
        .await
        .unwrap();
    assert!(writer.remaining() < 900_000);
    // Its 900 kB estimate exceeds the room left, but only its root is missing.
    let projects = list(&[(2, &format!("ipfs://{second}"), 2)]);
    let result = run(&projects, &fetcher, &writer, tiered(1024)).await;
    assert_eq!(
        (result.ok, result.waiting, result.cap_reached),
        (1, 0, false)
    );
    // A package left waiting on its hint, then stored by other means, completes locally.
    let projects = list(&[(3, &format!("ipfs://{third}"), 1)]);
    let held = run(
        &projects,
        &fetcher,
        &writer,
        MirrorOptions {
            limit: Some(1),
            ..tiered(64)
        },
    )
    .await;
    assert_eq!(held.waiting, 1);
    assert_eq!(
        row(&mirror, 3).unwrap().blocked_used,
        None,
        "a hint, not the cap"
    );
    fetcher
        .mirror_package(&format!("ipfs://{third}"), &writer)
        .await
        .unwrap();
    let calls = scoped(&gateway, "all");
    let resumed = run(&projects, &fetcher, &writer, tiered(64)).await;
    assert_eq!((resumed.started, resumed.ok, resumed.local), (1, 1, 1));
    assert_eq!(
        scoped(&gateway, "all"),
        calls,
        "complete from stored objects"
    );
    assert_eq!(row(&mirror, 3).unwrap().state, MirrorState::Ok);
}

#[test]
fn version_2_status_database_upgrades_in_place_keeping_its_rows() {
    let dir = temp("tier-upgrade-v2");
    {
        let conn = Connection::open(dir.join("status.sqlite")).unwrap();
        conn.execute_batch(
            "CREATE TABLE mirror_status (
              project_id INTEGER PRIMARY KEY CHECK (project_id >= 0),
              root_uri TEXT NOT NULL,
              state TEXT NOT NULL
                CHECK (state IN ('pending', 'ok', 'missing', 'too_big', 'failed', 'waiting')),
              bytes INTEGER NOT NULL DEFAULT 0 CHECK (bytes >= 0),
              files INTEGER NOT NULL DEFAULT 0 CHECK (files >= 0),
              attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
              not_found INTEGER NOT NULL DEFAULT 0 CHECK (not_found >= 0),
              last_error TEXT,
              checked_at TEXT NOT NULL
            ) STRICT;
            CREATE INDEX mirror_status_state ON mirror_status (state);
            CREATE TABLE mirror_list (
              position INTEGER PRIMARY KEY,
              project_id INTEGER NOT NULL UNIQUE,
              root_uri TEXT NOT NULL,
              volume_mutez INTEGER NOT NULL CHECK (volume_mutez >= 0)
            ) STRICT;
            CREATE TABLE mirror_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL) STRICT,
              WITHOUT ROWID;
            INSERT INTO mirror_status VALUES
              (1, 'ipfs://bafyok', 'ok', 4096, 3, 0, 0, NULL, '2026-10-01T00:00:00Z'),
              (2, 'ipfs://bafywait', 'waiting', 9000, 0, 0, 0, 'estimated',
               '2026-10-01T00:00:00Z');
            INSERT INTO mirror_meta VALUES ('threshold', '3145728');
            PRAGMA application_id = 1397050706;
            PRAGMA user_version = 2;",
        )
        .unwrap();
    }
    let before = MirrorStatus::open_read_only(&dir).unwrap().rows().unwrap();
    assert_eq!(before.len(), 2, "version 2 still reads");
    assert!(before.values().all(|row| row.blocked_used.is_none()));
    let status = MirrorStatus::open(&dir).unwrap();
    assert_eq!(status.rows().unwrap(), before, "every row unchanged");
    assert_eq!(
        status.meta("threshold").unwrap().as_deref(),
        Some("3145728")
    );
    drop(status);
    let version: i64 = Connection::open(dir.join("status.sqlite"))
        .unwrap()
        .pragma_query_value(None, "user_version", |r| r.get(0))
        .unwrap();
    assert_eq!(version, 4);
    assert_eq!(MirrorStatus::open(&dir).unwrap().rows().unwrap(), before);
}

#[tokio::test]
async fn probe_budget_is_shared_across_fallback_responses() {
    let mut dag = Dag::default();
    let root = dag.file(&[9; 4096]).0;
    let unrelated = |size: usize, dag: &mut Dag| dag.file(&vec![8; size]).0;
    let estimate = |first_waste: usize| {
        let mut dag = dag.clone();
        let waste = unrelated(first_waste, &mut dag);
        let first_body = car(root, &[(waste, dag.block(&waste))]);
        let second_body = car(root, &[(root, dag.block(&root))]);
        async move {
            let first = server("/ipfs/", Arc::new(move |_| Reply::car(first_body.clone()))).await;
            let second = server("/ipfs/", Arc::new(move |_| Reply::car(second_body.clone()))).await;
            let dir = temp(&format!("tier-probe-budget-{first_waste}"));
            let writer = writer(&dir.join("mirror"), 1 << 30);
            let mut config = fetch_config(&dir, &[&first, &second], None);
            config.max_object_bytes = 4 << 20;
            let fetcher = ContentStore::new(config).unwrap();
            let estimate = fetcher
                .mirror_estimate(&format!("ipfs://{root}"), &writer)
                .await;
            (estimate, first.calls().len(), second.calls().len())
        }
    };
    // The first answer leaves 1 MiB of the budget: the second gateway's root fits.
    assert_eq!(estimate(1 << 20).await, (Some(4096), 1, 1));
    // It leaves under 1 kB: the root would pass the 2 MiB budget, so the size is unknown.
    assert_eq!(estimate((2 << 20) - 1000).await, (None, 1, 1));
}

#[tokio::test]
async fn parked_package_is_tried_again_when_the_cap_grows_or_on_recheck() {
    let mut dag = Dag::default();
    let roots: Vec<Cid> = (0..2u8)
        .map(|n| {
            let file = dag.chunked(&[&[n; 150_000], &[n + 10; 150_000]]);
            dag.dir(&[("index.html", file)], true).0
        })
        .collect();
    let gateway = server("/ipfs/", gateway_handler(dag)).await;
    let dir = temp("tier-parked");
    let mirror = dir.join("mirror");
    let fetcher = ContentStore::new(fetch_config(&dir, &[&gateway], None)).unwrap();
    let projects = list(&[
        (1, &format!("ipfs://{}", roots[0]), 2),
        (2, &format!("ipfs://{}", roots[1]), 1),
    ]);
    let small = writer(&mirror, 256 << 10);
    let parked = run(&projects, &fetcher, &small, tiered(1024)).await;
    assert_eq!((parked.waiting, parked.cap_reached), (2, true));
    let blocked = row(&mirror, 1).unwrap();
    assert_eq!(
        (blocked.blocked_used, blocked.blocked_cap),
        (Some(0), Some(256 << 10))
    );
    let calls = gateway.calls().len();
    let unchanged = run(&projects, &fetcher, &small, tiered(1024)).await;
    assert_eq!((unchanged.started, unchanged.cap_reached), (0, true));
    assert_eq!(
        gateway.calls().len(),
        calls,
        "same usage and cap: no request"
    );
    let rechecked = run(
        &projects,
        &fetcher,
        &small,
        MirrorOptions {
            recheck: true,
            ..tiered(1024)
        },
    )
    .await;
    assert_eq!(
        (rechecked.started, rechecked.waiting),
        (2, 2),
        "--recheck tries them"
    );
    drop(small);
    let larger = writer(&mirror, 1 << 20);
    let grown = run(&projects, &fetcher, &larger, tiered(1024)).await;
    assert_eq!((grown.started, grown.ok, grown.cap_reached), (2, 2, false));
    assert_eq!(row(&mirror, 2).unwrap().state, MirrorState::Ok);
}

#[test]
fn version_3_status_database_upgrades_in_place_keeping_its_rows() {
    let dir = temp("tier-upgrade-v3");
    // A version 4 database without `blocked_cap` is the version 3 layout.
    drop(MirrorStatus::open(&dir).unwrap());
    {
        let conn = Connection::open(dir.join("status.sqlite")).unwrap();
        conn.execute_batch(
            "ALTER TABLE mirror_status DROP COLUMN blocked_cap;
            INSERT INTO mirror_status (project_id, root_uri, state, bytes, files, attempts,
              not_found, last_error, checked_at, blocked_used) VALUES
              (1, 'ipfs://bafyok', 'ok', 4096, 3, 0, 0, NULL, '2026-10-01T00:00:00Z', NULL),
              (2, 'ipfs://bafyparked', 'waiting', 9000, 0, 0, 0, 'cap',
               '2026-10-01T00:00:00Z', 4096);
            PRAGMA user_version = 3;",
        )
        .unwrap();
    }
    let before = MirrorStatus::open_read_only(&dir).unwrap().rows().unwrap();
    assert_eq!(before[&2].blocked_used, Some(4096), "version 3 still reads");
    assert_eq!(before[&2].blocked_cap, None);
    let status = MirrorStatus::open(&dir).unwrap();
    assert_eq!(status.rows().unwrap(), before, "every row unchanged");
    drop(status);
    let version: i64 = Connection::open(dir.join("status.sqlite"))
        .unwrap()
        .pragma_query_value(None, "user_version", |r| r.get(0))
        .unwrap();
    assert_eq!(version, 4);
}
