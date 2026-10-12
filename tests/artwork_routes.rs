use axum::{
    Router,
    body::Body,
    extract::{OriginalUri, State},
    http::{Request, StatusCode},
    response::IntoResponse,
};
use http_body_util::BodyExt;
use seeded_archive::{
    config::Config,
    content::{
        ContentConfig, ContentStore,
        onchfs::{file_cid, keccak},
    },
    import::import_fixtures,
    player,
    store::Store,
    web::{self, AppState, artwork},
};
use serde_json::json;
use std::{
    collections::{BTreeMap, BTreeSet},
    io::Write,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};
use tower::ServiceExt;

const CONTRACT: &str = "KT1EfsNuqwLAWDd3o4pvfUx1CAh5GMdTrRvr";
static NEXT: AtomicU64 = AtomicU64::new(0);
#[derive(Clone)]
struct Source {
    cid: String,
    chunk: String,
    body: Vec<u8>,
    metadata: Vec<u8>,
    calls: Arc<AtomicU64>,
    car: Arc<Mutex<Option<Vec<u8>>>>,
}
struct Archive {
    root: PathBuf,
    app: Router,
    art: Router,
    source: Source,
    render: String,
    upstream: tokio::task::JoinHandle<()>,
}
impl Drop for Archive {
    fn drop(&mut self) {
        self.upstream.abort();
        let _ = std::fs::remove_dir_all(&self.root);
    }
}
impl Archive {
    async fn new(html: &[u8], object_limit: u64) -> Self {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(format!(
            "tmp/artwork-routes-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&root).unwrap();
        let mut gzip = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        gzip.write_all(html).unwrap();
        let body = gzip.finish().unwrap();
        // Literal HPACK metadata from ONCHFS's HTML/gzip encoding.
        let metadata = vec![
            0x5f, 9, b't', b'e', b'x', b't', b'/', b'h', b't', b'm', b'l', 0x5a, 4, b'g', b'z',
            b'i', b'p',
        ];
        let source = Source {
            cid: hex::encode(file_cid(&keccak(&body), &metadata)),
            chunk: hex::encode(keccak(&body)),
            body,
            metadata,
            calls: Arc::new(AtomicU64::new(0)),
            car: Arc::new(Mutex::new(None)),
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut content_config = ContentConfig::mainnet(root.join("cache"));
        content_config.tzkt_base = format!("http://{}", listener.local_addr().unwrap());
        content_config.ipfs_gateways = vec![format!("{}/ipfs/", content_config.tzkt_base)];
        content_config.max_object_bytes = object_limit;
        let mock = Router::new().fallback(upstream).with_state(source.clone());
        let upstream = tokio::spawn(async move {
            axum::serve(listener, mock).await.unwrap();
        });
        let mut config = Config::from_values(&BTreeMap::new()).unwrap();
        config.database = root.join("archive.sqlite");
        config.articles_database = root.join("articles.sqlite");
        config.assets = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("assets");
        config.object_bytes = object_limit;
        config.assemblies = 1;
        let mut store = Store::open(&config.database).unwrap();
        import_fixtures(&mut store, &config.assets.join("fixtures")).unwrap();
        let connection = rusqlite::Connection::open(&config.database).unwrap();
        connection
            .execute(
                "UPDATE tokens SET artifact_uri=?1 WHERE contract=?2 AND token_id=133459",
                (&format!("onchfs://{}", source.cid), CONTRACT),
            )
            .unwrap();
        seeded_archive::ingest::upgrade::upgrade(&mut store).unwrap();
        let token = store.token(CONTRACT, 133459).unwrap();
        let render = format!(
            "{}&__seeded_instance={}",
            player::render_url(&config.content_origin, &token)
                .unwrap()
                .strip_prefix(&config.content_origin)
                .unwrap(),
            "a".repeat(32)
        );
        let app = AppState::new(&config.database, &config.assets).configured(&config);
        let art = artwork::router(artwork::ArtworkState::new(
            app.clone(),
            ContentStore::new(content_config).unwrap(),
            &config,
        ));
        Self {
            root,
            app: web::router(app),
            art,
            source,
            render,
            upstream,
        }
    }
    fn resource(&self) -> String {
        format!("/content/onchfs/{}", self.source.cid)
    }
    fn set_artifact(&self, uri: &str) {
        let database = self.root.join("archive.sqlite");
        let connection = rusqlite::Connection::open(&database).unwrap();
        connection
            .execute(
                "UPDATE tokens SET artifact_uri=?1 WHERE contract=?2 AND token_id=133459",
                (uri, CONTRACT),
            )
            .unwrap();
        let mut store = Store::open(&database).unwrap();
        seeded_archive::ingest::upgrade::upgrade(&mut store).unwrap();
    }
}

#[tokio::test]
async fn verified_content_burst_passes_the_listener_and_artwork_limits() {
    let archive = Archive::new(b"<html><body>sample</body></html>", 1024).await;
    let app = seeded_archive::request_limits::protect(
        archive.art.clone(),
        seeded_archive::request_limits::RequestLimits::with_content_limit(1).artwork(),
        false,
    );
    let barrier = Arc::new(tokio::sync::Barrier::new(150));
    let mut tasks = tokio::task::JoinSet::new();
    for n in 0..150 {
        let (app, barrier) = (app.clone(), barrier.clone());
        let path = format!("{}?sample={n}", archive.resource());
        tasks.spawn(async move {
            barrier.wait().await;
            let mut request = Request::builder().uri(path).body(Body::empty()).unwrap();
            request.extensions_mut().insert(axum::extract::ConnectInfo(
                seeded_archive::client_ip::PeerAddr("192.0.2.1:1234".parse().unwrap()),
            ));
            let response = app.oneshot(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(response.headers()["access-control-allow-origin"], "*");
            assert_eq!(response.headers()["x-seeded-verification"], "onchfs-keccak");
            assert_eq!(
                response.into_body().collect().await.unwrap().to_bytes(),
                b"<html><body>sample</body></html>".as_slice()
            );
        });
    }
    while let Some(result) = tasks.join_next().await {
        result.unwrap();
    }
}

async fn upstream(
    State(source): State<Source>,
    OriginalUri(uri): OriginalUri,
) -> impl IntoResponse {
    source.calls.fetch_add(1, Ordering::Relaxed);
    if uri.path().starts_with("/ipfs/") {
        return match source.car.lock().unwrap().clone() {
            Some(car) => ([("content-type", "application/vnd.ipld.car")], car).into_response(),
            None => StatusCode::NOT_FOUND.into_response(),
        };
    }
    if uri.path().ends_with(&format!("/{}", source.cid)) {
        axum::Json(json!({"active":true,"value":{"file":{"metadata":hex::encode(source.metadata),"chunk_pointers":[source.chunk]}}})).into_response()
    } else if uri.query().is_some() {
        axum::Json(json!([{"active":true,"key":source.chunk,"value":hex::encode(source.body)}]))
            .into_response()
    } else {
        StatusCode::NO_CONTENT.into_response()
    }
}
async fn get(app: &Router, path: &str) -> axum::response::Response {
    app.clone()
        .oneshot(Request::get(path).body(Body::empty()).unwrap())
        .await
        .unwrap()
}
async fn text(response: axum::response::Response) -> String {
    String::from_utf8(
        response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap()
}

fn assert_artwork_csp(response: &axum::response::Response) {
    let csp = response.headers()["content-security-policy"]
        .to_str()
        .unwrap();
    let directives = csp
        .split(';')
        .map(str::trim)
        .map(|directive| directive.split_whitespace().collect::<Vec<_>>())
        .filter(|directive| !directive.is_empty())
        .map(|directive| (directive[0], directive))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        directives["connect-src"],
        ["connect-src", "http://127.0.0.1:3919", "data:", "blob:"]
    );
    assert_eq!(
        directives["frame-src"],
        ["frame-src", "http://127.0.0.1:3919"]
    );
    assert!(!directives.contains_key("frame-ancestors"));
    assert_eq!(directives["sandbox"], ["sandbox", "allow-scripts"]);
}

#[tokio::test]
async fn derived_render_preserves_art_and_seed_while_separating_origins() {
    let original = b"<!doctype html><script>window.originalArtwork=true</script>";
    let archive = Archive::new(original, 1024).await;
    let response = get(&archive.art, &archive.render).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["cache-control"], "no-store");
    assert_eq!(response.headers()["access-control-allow-origin"], "*");
    assert_artwork_csp(&response);
    let html = text(response).await;
    assert!(html.contains("window.originalArtwork=true"));
    assert!(html.contains("data-seeded-instance=\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\""));
    assert_eq!(
        get(&archive.app, &archive.render).await.status(),
        StatusCode::NOT_FOUND
    );
    let page = get(&archive.app, &format!("/token/{CONTRACT}/133459")).await;
    let app_csp = page.headers()["content-security-policy"].to_str().unwrap();
    assert!(app_csp.contains("frame-src http://127.0.0.1:3919"));
    assert!(app_csp.contains("frame-ancestors 'none'"));
    let raw = get(&archive.art, &archive.resource()).await;
    assert_eq!(raw.status(), StatusCode::OK);
    assert_artwork_csp(&raw);
}

#[tokio::test]
async fn frozen_chain_tokens_keep_canonical_seed_minter_iteration_and_params() {
    use seeded_archive::ingest::{
        frank::bootstrap,
        sync::{SyncOptions, sync},
    };
    let archive = Archive::new(b"<html>recorded inputs</html>", 1024).await;
    let mut store = Store::open(&archive.root.join("chain.sqlite")).unwrap();
    bootstrap(&mut store, &ingest_support::frank()).unwrap();
    let fake = ingest_support::FakeTzkt::recorded();
    let stop = Arc::new(AtomicBool::new(false));
    let api = ingest_support::tzkt(&fake, &stop);
    let report = sync(
        &mut store,
        &api,
        &SyncOptions {
            contracts: Vec::new(),
            limit: None,
            concurrency: 3,
            batch: 10_000,
        },
        &|_| {},
    )
    .unwrap();
    assert_eq!(report.tokens_written, 21);
    let mut played = 0;
    let mut with_params = 0;
    let mut contracts = BTreeSet::new();
    for row in fake.with(|state| state.tokens.clone()) {
        let contract = row["_contract"].as_str().unwrap();
        let id = row["tokenId"].as_str().unwrap().parse().unwrap();
        let token = match store.token(contract, id) {
            Ok(token) => token,
            Err(seeded_archive::store::StoreError::NotFound { .. }) => continue,
            Err(error) => panic!("unexpected token lookup failure: {error}"),
        };
        contracts.insert(token.contract.clone());
        assert_eq!(token.source, seeded_archive::model::Source::Tzkt);
        let url = player::render_url("https://art.example", &token).unwrap();
        let parsed = reqwest::Url::parse(&url).unwrap();
        let query: BTreeMap<_, _> = parsed.query_pairs().collect();
        assert_eq!(query.get("fxhash").unwrap(), &token.seed);
        assert_eq!(query.get("fxminter").unwrap(), &token.minter);
        assert_eq!(
            query.get("fxiteration").unwrap(),
            &token.iteration.to_string()
        );
        let (artifact, fragment) = token
            .artifact_uri
            .split_once('#')
            .map_or((token.artifact_uri.as_str(), None), |(uri, hash)| {
                (uri, Some(hash))
            });
        if let Some((_, original_query)) = artifact.split_once('?') {
            assert!(parsed.query().unwrap().starts_with(original_query));
        }
        assert_eq!(parsed.fragment(), fragment);
        if let Some(params) = token.params {
            assert_eq!(parsed.fragment(), Some(format!("0x{params}").as_str()));
            with_params += 1;
        }
        played += 1;
    }
    assert_eq!(played, 19, "two signed tokens belong to hidden projects");
    assert_eq!(contracts.len(), 3);
    assert_eq!(with_params, 1);
}

#[tokio::test]
async fn chain_params_reach_frames_without_changing_original_queries_or_cached_art() {
    let original = b"<!doctype html><script>window.params=location.hash</script>";
    let archive = Archive::new(original, 1024).await;
    let connection = rusqlite::Connection::open(archive.root.join("archive.sqlite")).unwrap();
    let artifact = format!("onchfs://{}?custom=a+b&opaque=%26%2f", archive.source.cid);
    connection.execute("UPDATE tokens SET artifact_uri=?1, params='00a1Ff' WHERE contract=?2 AND token_id=133459", (&artifact, CONTRACT)).unwrap();
    let store = Store::open(&archive.root.join("archive.sqlite")).unwrap();
    let token = store.token(CONTRACT, 133459).unwrap();
    let url = player::render_url("http://127.0.0.1:3919", &token).unwrap();
    assert!(url.ends_with("#0x00a1Ff"));
    let path = url
        .strip_prefix("http://127.0.0.1:3919")
        .unwrap()
        .split('#')
        .next()
        .unwrap();
    let request = format!("{path}&__seeded_instance={}", "a".repeat(32));
    let page = text(get(&archive.app, &format!("/token/{CONTRACT}/133459")).await).await;
    assert!(page.contains(&format!(
        "data-render-url=\"{}\"",
        askama::filters::escape(&url, askama::filters::Html).unwrap()
    )));
    let response = get(&archive.art, &request).await;
    assert_eq!(response.status(), StatusCode::OK);
    let prepared = text(response).await;
    assert!(prepared.ends_with("<script>window.params=location.hash</script>"));
    assert_eq!(
        text(get(&archive.art, &archive.resource()).await)
            .await
            .as_bytes(),
        original
    );
    assert_eq!(
        store.token(CONTRACT, 133459).unwrap().artifact_uri,
        artifact
    );
    let calls = archive.source.calls.load(Ordering::Relaxed);
    for altered in [
        request.replace("custom=a+b", "custom=a%20b"),
        request.replace("%26%2f", "%26%2F"),
        request.replace("fxiteration=1", "fxiteration=2"),
        request.replace("&__seeded_instance=", "&fxparams=other&__seeded_instance="),
    ] {
        assert_eq!(
            get(&archive.art, &altered).await.status(),
            StatusCode::BAD_REQUEST
        );
    }
    assert_eq!(archive.source.calls.load(Ordering::Relaxed), calls);
}

#[tokio::test]
async fn conflicting_params_fail_before_fetch_and_recover_after_metadata_correction() {
    let archive = Archive::new(b"<html>params</html>", 1024).await;
    let connection = rusqlite::Connection::open(archive.root.join("archive.sqlite")).unwrap();
    connection.execute("UPDATE tokens SET artifact_uri=?1, params='aabb' WHERE contract=?2 AND token_id=133459", (&format!("onchfs://{}#0xccdd", archive.source.cid), CONTRACT)).unwrap();
    let response = get(&archive.art, &archive.render).await;
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    assert_eq!(response.headers()["cache-control"], "no-store");
    assert!(text(response).await.contains("state:'unavailable'"));
    assert_eq!(archive.source.calls.load(Ordering::Relaxed), 0);
    connection
        .execute(
            "UPDATE tokens SET artifact_uri=?1 WHERE contract=?2 AND token_id=133459",
            (&format!("onchfs://{}#0xAaBb", archive.source.cid), CONTRACT),
        )
        .unwrap();
    let recovered = get(&archive.art, &archive.render).await;
    assert_eq!(recovered.status(), StatusCode::OK);
    assert!(text(recovered).await.ends_with("params</html>"));
}

#[tokio::test]
async fn forged_seed_duplicate_instance_and_unknown_token_never_fetch_artwork() {
    let archive = Archive::new(b"<html>art</html>", 1024).await;
    for path in [
        archive.render.replace("fxiteration=1", "fxiteration=2"),
        format!("{}&__seeded_instance={}", archive.render, "b".repeat(32)),
        archive.render.replace("/133459?", "/999999?"),
    ] {
        let response = get(&archive.art, &path).await;
        assert!(matches!(
            response.status(),
            StatusCode::BAD_REQUEST | StatusCode::NOT_FOUND
        ));
        assert_eq!(response.headers()["cache-control"], "no-store");
    }
    assert_eq!(archive.source.calls.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn verified_resources_negotiate_gzip_validate_etags_and_ignore_upstream_headers() {
    let archive = Archive::new(b"<html>original</html>", 1024).await;
    let response = get(&archive.art, &archive.resource()).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["x-seeded-verification"], "onchfs-keccak");
    assert!(
        response.headers()["cache-control"]
            .to_str()
            .unwrap()
            .contains("immutable")
    );
    assert!(!response.headers().contains_key("content-encoding"));
    let etag = response.headers()["etag"].clone();
    assert_eq!(text(response).await, "<html>original</html>");
    let cached = archive
        .art
        .clone()
        .oneshot(
            Request::get(archive.resource())
                .header("if-none-match", etag)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(cached.status(), StatusCode::NOT_MODIFIED);
    assert!(text(cached).await.is_empty());
    let compressed = archive
        .art
        .clone()
        .oneshot(
            Request::get(archive.resource())
                .header("accept-encoding", "gzip")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(compressed.headers()["content-encoding"], "gzip");
    assert_eq!(
        compressed
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .as_ref(),
        archive.source.body.as_slice()
    );
}

#[tokio::test]
async fn unknown_roots_and_invalid_identifiers_never_fetch_content() {
    let archive = Archive::new(b"<html>known</html>", 1024).await;
    let identity = cid::Cid::new_v1(
        0x55,
        cid::multihash::Multihash::<64>::wrap(0, b"<html>unindexed</html>").unwrap(),
    );
    for (path, status) in [
        (
            format!("/content/onchfs/{}", "0".repeat(64)),
            StatusCode::NOT_FOUND,
        ),
        (format!("/content/ipfs/{identity}"), StatusCode::NOT_FOUND),
        ("/content/ipfs/notACid".into(), StatusCode::BAD_REQUEST),
        ("/content/onchfs/abcd".into(), StatusCode::BAD_REQUEST),
        ("/content/https/abcd".into(), StatusCode::BAD_REQUEST),
    ] {
        let response = get(&archive.art, &path).await;
        assert_eq!(response.status(), status, "{path}");
        assert_eq!(response.headers()["cache-control"], "no-store");
        assert!(!response.headers().contains_key("x-seeded-verification"));
    }
    assert_eq!(archive.source.calls.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn flagged_roots_cannot_bypass_admission_with_cached_content_or_etags() {
    let archive = Archive::new(b"<html>moderated</html>", 1024).await;
    let response = get(&archive.art, &archive.resource()).await;
    assert_eq!(response.status(), StatusCode::OK);
    let etag = response.headers()["etag"].clone();
    drop(response);
    let calls = archive.source.calls.load(Ordering::Relaxed);
    let connection = rusqlite::Connection::open(archive.root.join("archive.sqlite")).unwrap();
    connection
        .execute("UPDATE projects SET hidden=1 WHERE id=29437", [])
        .unwrap();
    for path in [archive.resource(), archive.render.clone()] {
        let response = archive
            .art
            .clone()
            .oneshot(
                Request::get(path)
                    .header("if-none-match", &etag)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(response.headers()["cache-control"], "no-store");
        assert!(!response.headers().contains_key("x-seeded-verification"));
    }
    assert_eq!(archive.source.calls.load(Ordering::Relaxed), calls);
    connection
        .execute("UPDATE projects SET hidden=0 WHERE id=29437", [])
        .unwrap();
    assert_eq!(
        text(get(&archive.art, &archive.resource()).await).await,
        "<html>moderated</html>"
    );
    assert_eq!(archive.source.calls.load(Ordering::Relaxed), calls);
}

#[tokio::test]
async fn database_outage_blocks_cached_resources_and_recovers_without_refetching() {
    let archive = Archive::new(b"<html>recover</html>", 1024).await;
    assert_eq!(
        get(&archive.art, &archive.resource()).await.status(),
        StatusCode::OK
    );
    let calls = archive.source.calls.load(Ordering::Relaxed);
    let database = archive.root.join("archive.sqlite");
    let held = archive.root.join("archive-held.sqlite");
    std::fs::rename(&database, &held).unwrap();
    let response = get(&archive.art, &archive.resource()).await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response.headers()["cache-control"], "no-store");
    assert_eq!(response.headers()["retry-after"], "2");
    drop(response);
    assert_eq!(archive.source.calls.load(Ordering::Relaxed), calls);
    std::fs::rename(held, database).unwrap();
    assert_eq!(
        text(get(&archive.art, &archive.resource()).await).await,
        "<html>recover</html>"
    );
    assert_eq!(archive.source.calls.load(Ordering::Relaxed), calls);
}

#[tokio::test]
async fn profile_collected_view_discloses_listings_from_its_recorded_address() {
    let archive = Archive::new(b"<html>artist</html>", 1024).await;
    let database = archive.root.join("archive.sqlite");
    let store = Store::open(&database).unwrap();
    let holder = store
        .token(CONTRACT, 133459)
        .unwrap()
        .owner
        .unwrap()
        .owner
        .address;
    drop(store);
    let connection = rusqlite::Connection::open(&database).unwrap();
    connection
        .execute(
            "UPDATE artists SET address=?1 WHERE id='emptyname'",
            [&holder],
        )
        .unwrap();
    connection.execute(
        "UPDATE tokens SET owner_address=?1, owner_alias=NULL, owner_alias_key=NULL, listed_by=?2, holder_address=?2 WHERE contract=?3 AND token_id=133459",
        (seeded_archive::ingest::escrow::ESCROWS[0].address, &holder, CONTRACT),
    ).unwrap();
    let response = get(&archive.app, &format!("/profile/{holder}?view=collected")).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert!(text(response).await.contains("Includes 1 listing."));
    let projects = get(&archive.app, &format!("/profile/{holder}?view=projects")).await;
    assert_eq!(projects.status(), StatusCode::OK);
    assert!(!text(projects).await.contains("Includes 1 listing."));
    connection
        .execute("UPDATE artists SET address=NULL WHERE id='emptyname'", [])
        .unwrap();
    let alias = get(&archive.app, &format!("/profile/{holder}?view=collected")).await;
    assert_eq!(alias.status(), StatusCode::OK);
    assert!(text(alias).await.contains("Includes 1 listing."));
}

#[tokio::test]
async fn indexed_identity_cids_are_allowed_in_valid_multibase_forms() {
    let archive = Archive::new(b"unused", 1024).await;
    let bytes = b"<!doctype html><html>indexed identity content is valid</html>";
    let cid = cid::Cid::new_v1(
        0x55,
        cid::multihash::Multihash::<64>::wrap(0, bytes).unwrap(),
    );
    archive.set_artifact(&format!("ipfs://{cid}"));
    *archive.source.car.lock().unwrap() = Some(b"\x11\xa2\x65roots\x80\x67version\x01".to_vec());
    let base16 = cid
        .to_string_of_base(cid::multibase::Base::Base16Lower)
        .unwrap();
    assert!(base16.len() > 128);
    let response = get(&archive.art, &format!("/content/ipfs/{base16}")).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["x-seeded-verification"], "ipfs-car");
    assert_eq!(text(response).await.as_bytes(), bytes);
    assert_eq!(archive.source.calls.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn cid_v0_and_v1_resources_use_the_same_catalogue_admission() {
    use sha2::Digest;
    let archive = Archive::new(b"unused", 1024).await;
    let bytes = b"canonical";
    let mut unixfs = vec![8, 2, 18, bytes.len() as u8];
    unixfs.extend(bytes);
    unixfs.extend([24, bytes.len() as u8]);
    let mut node = vec![10, unixfs.len() as u8];
    node.extend(unixfs);
    let hash = cid::multihash::Multihash::<64>::wrap(0x12, &sha2::Sha256::digest(&node)).unwrap();
    let v0 = cid::Cid::new_v0(hash).unwrap();
    let v1 = cid::Cid::new_v1(0x70, hash);
    archive.set_artifact(&format!("ipfs://{v0}"));
    let mut proof = b"\x11\xa2\x65roots\x80\x67version\x01".to_vec();
    let section = v0.to_bytes().len() + node.len();
    assert!(section < 128);
    proof.push(section as u8);
    proof.extend(v0.to_bytes());
    proof.extend(node);
    *archive.source.car.lock().unwrap() = Some(proof);
    for cid in [v1, v0] {
        let response = get(&archive.art, &format!("/content/ipfs/{cid}")).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["x-seeded-verification"], "ipfs-car");
        assert_eq!(text(response).await.as_bytes(), bytes);
    }
}

#[tokio::test]
async fn gzip_expansion_is_bounded_and_errors_remain_retryable() {
    let archive = Archive::new(&[b'x'; 4096], 256).await;
    for path in [archive.resource(), archive.render.clone()] {
        let response = get(&archive.art, &path).await;
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        assert_eq!(response.headers()["cache-control"], "no-store");
    }
}

#[tokio::test]
async fn small_retained_responses_do_not_exhaust_artwork_assembly_slots() {
    let archive = Archive::new(b"<html>art</html>", 1024).await;
    let mut held = Vec::new();
    for _ in 0..32 {
        let response = get(&archive.art, "/unknown").await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        held.push(response);
    }
    assert_eq!(
        get(&archive.art, &archive.resource()).await.status(),
        StatusCode::OK
    );
}

#[tokio::test]
async fn retained_data_frames_keep_the_byte_budget_until_the_last_reference_drops() {
    let archive = Archive::new(&vec![b'x'; 32 << 20], 32 << 20).await;
    let first = get(&archive.art, &archive.resource()).await;
    let second = get(&archive.art, &archive.resource()).await;
    assert_eq!(first.status(), StatusCode::OK);
    assert_eq!(second.status(), StatusCode::OK);
    let mut body = first.into_body();
    let mut last = None;
    let mut received = 0;
    while let Some(frame) = body.frame().await {
        let frame = frame.unwrap();
        received += frame.data_ref().unwrap().len();
        last = Some(frame);
    }
    assert_eq!(received, 32 << 20);
    let frame = last.unwrap();
    drop(body);
    assert_eq!(
        get(&archive.art, "/unknown").await.status(),
        StatusCode::NOT_FOUND
    );
    let refused = get(&archive.art, &archive.resource()).await;
    assert_eq!(refused.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(refused.headers()["retry-after"], "2");
    drop(frame);
    assert_eq!(
        get(&archive.art, &archive.resource()).await.status(),
        StatusCode::OK
    );
    drop(second);
    assert_eq!(
        get(&archive.art, &archive.resource()).await.status(),
        StatusCode::OK
    );
}
mod ingest_support;

#[tokio::test]
async fn playback_integrity_status_is_bound_to_prepared_token_and_instance() {
    let archive = Archive::new(b"<html>verified</html>", 1024).await;
    let status_url = format!(
        "/playback-status/{CONTRACT}/133459?instance={}",
        "a".repeat(32)
    );
    let before = get(&archive.art, &status_url).await;
    assert_eq!(before.status(), StatusCode::ACCEPTED);
    assert_eq!(before.headers()["cache-control"], "no-store");
    assert_eq!(archive.source.calls.load(Ordering::Relaxed), 0);
    assert_eq!(
        get(&archive.art, &archive.render).await.status(),
        StatusCode::OK
    );
    let verified = get(&archive.art, &status_url).await;
    assert_eq!(verified.status(), StatusCode::OK);
    assert_eq!(verified.headers()["access-control-allow-origin"], "*");
    assert_eq!(text(verified).await, r#"{"verification":"verified"}"#);
    for url in [
        status_url.replace("instance=aaaa", "instance=bbbb"),
        status_url.replace("/133459?", "/133460?"),
    ] {
        assert_eq!(get(&archive.art, &url).await.status(), StatusCode::ACCEPTED);
    }
    for query in [
        "",
        "?instance=short",
        "?instance=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa&instance=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    ] {
        assert_eq!(
            get(
                &archive.art,
                &format!("/playback-status/{CONTRACT}/133459{query}")
            )
            .await
            .status(),
            StatusCode::BAD_REQUEST
        );
    }
}

#[tokio::test]
async fn plain_gateway_playback_is_reported_unverified_by_the_server() {
    use sha2::Digest;
    let original = b"<html>unverified gateway</html>";
    let archive = Archive::new(original, 1024).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let upstream = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new().fallback(|| async {
                (
                    [("content-type", "text/html")],
                    "<html>unverified gateway</html>",
                )
            }),
        )
        .await
        .unwrap();
    });
    let cid = cid::Cid::new_v1(
        0x55,
        cid::multihash::Multihash::<64>::wrap(0x12, &sha2::Sha256::digest(original)).unwrap(),
    )
    .to_string();
    archive.set_artifact(&format!("ipfs://{cid}"));
    let mut config = Config::from_values(&BTreeMap::new()).unwrap();
    config.database = archive.root.join("archive.sqlite");
    config.cache = archive.root.join("gateway-cache");
    config.ipfs_gateways = vec![format!("http://{address}/ipfs/")];
    let state = AppState::new(&config.database, &config.assets).configured(&config);
    let art = artwork::router(artwork::ArtworkState::new(
        state,
        ContentStore::new(config.content_config()).unwrap(),
        &config,
    ));
    assert_eq!(get(&art, &archive.render).await.status(), StatusCode::OK);
    let status = get(
        &art,
        &format!(
            "/playback-status/{CONTRACT}/133459?instance={}&verification=verified",
            "a".repeat(32)
        ),
    )
    .await;
    assert_eq!(status.status(), StatusCode::OK);
    assert_eq!(text(status).await, r#"{"verification":"unverified"}"#);
    upstream.abort();
    let _ = upstream.await;
}

#[tokio::test]
async fn profile_media_is_scoped_to_current_profile_and_rejects_active_documents() {
    let archive = Archive::new(b"unused", 4096).await;
    let png = b"\x89PNG\r\n\x1a\n";
    let cid = cid::Cid::new_v1(0x55, cid::multihash::Multihash::<64>::wrap(0, png).unwrap());
    *archive.source.car.lock().unwrap() = Some(b"\x11\xa2\x65roots\x80\x67version\x01".to_vec());
    let connection = rusqlite::Connection::open(archive.root.join("archive.sqlite")).unwrap();
    let address: String = connection
        .query_row(
            "SELECT owner_address FROM tokens WHERE token_id=133459",
            [],
            |r| r.get(0),
        )
        .unwrap();
    connection.execute("INSERT INTO profiles_imported(address,source,avatar_uri,fetched_at) VALUES (?,'fxhash',?,'2026-10-05')", (&address, format!("ipfs://{cid}"))).unwrap();
    let path = format!("/profile-media/{address}");
    assert_eq!(
        get(&archive.art, &format!("/content/ipfs/{cid}"))
            .await
            .status(),
        StatusCode::NOT_FOUND
    );
    let response = get(&archive.art, &path).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["content-type"], "image/png");
    assert_eq!(response.headers()["cache-control"], "no-store");
    assert_eq!(
        response.headers()["content-security-policy"],
        "default-src 'none'; script-src 'none'; object-src 'none'; frame-ancestors 'none'; sandbox"
    );
    let etag = response.headers()["etag"].clone();
    drop(response);
    let media_cache =
        seeded_archive::content::cache::DiskCache::open(&archive.root.join("cache")).unwrap();
    assert_eq!(
        media_cache.disk_usage().unwrap(),
        0,
        "profile media must not populate persistent cache"
    );
    let calls = archive.source.calls.load(Ordering::Relaxed);
    connection
        .execute(
            "UPDATE profiles_imported SET avatar_uri=NULL WHERE address=?",
            [&address],
        )
        .unwrap();
    let response = archive
        .art
        .clone()
        .oneshot(
            Request::get(&path)
                .header("if-none-match", etag)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(archive.source.calls.load(Ordering::Relaxed), calls);
    let html_cid = cid::Cid::new_v1(
        0x55,
        cid::multihash::Multihash::<64>::wrap(0, b"<!doctype html><script>bad()</script>").unwrap(),
    );
    connection
        .execute(
            "UPDATE profiles_imported SET avatar_uri=? WHERE address=?",
            (format!("ipfs://{html_cid}"), &address),
        )
        .unwrap();
    assert_eq!(
        get(&archive.art, &path).await.status(),
        StatusCode::UNSUPPORTED_MEDIA_TYPE
    );
    assert_eq!(
        seeded_archive::content::cache::DiskCache::open(&archive.root.join("cache"))
            .unwrap()
            .disk_usage()
            .unwrap(),
        0,
        "rejected HTML must not populate the cache"
    );
    let calls = archive.source.calls.load(Ordering::Relaxed);
    connection
        .execute(
            "UPDATE profiles_imported SET avatar_uri='https://127.0.0.1/private' WHERE address=?",
            [&address],
        )
        .unwrap();
    assert_eq!(
        get(&archive.art, &path).await.status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(archive.source.calls.load(Ordering::Relaxed), calls);
}

#[tokio::test]
async fn only_images_on_the_artwork_origin_are_indexable() {
    let archive = Archive::new(b"<!doctype html><script>art()</script>", 4096).await;
    let status = format!(
        "/playback-status/{CONTRACT}/133459?instance={}",
        "a".repeat(32)
    );
    for path in [
        archive.render.clone(),
        archive.resource(),
        status,
        format!("/render/{CONTRACT}/999999"),
        "/unknown".into(),
    ] {
        let response = get(&archive.art, &path).await;
        assert_eq!(response.headers()["x-robots-tag"], "noindex", "{path}");
    }
    let post = archive
        .art
        .clone()
        .oneshot(Request::post(&archive.render).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(post.status(), StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(post.headers()["x-robots-tag"], "noindex");
    let png = b"\x89PNG\r\n\x1a\n";
    let cid = cid::Cid::new_v1(0x55, cid::multihash::Multihash::<64>::wrap(0, png).unwrap());
    *archive.source.car.lock().unwrap() = Some(b"\x11\xa2\x65roots\x80\x67version\x01".to_vec());
    let connection = rusqlite::Connection::open(archive.root.join("archive.sqlite")).unwrap();
    let address: String = connection
        .query_row(
            "SELECT owner_address FROM tokens WHERE token_id=133459",
            [],
            |r| r.get(0),
        )
        .unwrap();
    connection.execute("INSERT INTO profiles_imported(address,source,avatar_uri,fetched_at) VALUES (?,'fxhash',?,'2026-10-05')", (&address, format!("ipfs://{cid}"))).unwrap();
    let image = get(&archive.art, &format!("/profile-media/{address}")).await;
    assert_eq!(image.status(), StatusCode::OK);
    assert_eq!(image.headers()["content-type"], "image/png");
    assert!(!image.headers().contains_key("x-robots-tag"));
}

#[tokio::test]
async fn article_media_visibility_is_rechecked_even_with_a_matching_etag() {
    let archive = Archive::new(b"unused", 4096).await;
    let cid = cid::Cid::new_v1(
        0x55,
        cid::multihash::Multihash::<64>::wrap(0, b"GIF89a").unwrap(),
    );
    *archive.source.car.lock().unwrap() = Some(b"\x11\xa2\x65roots\x80\x67version\x01".to_vec());
    let database = archive.root.join("articles.sqlite");
    let _articles = seeded_archive::articles::store::ArticleStore::open(&database).unwrap();
    let connection = rusqlite::Connection::open(&database).unwrap();
    connection.execute("INSERT INTO articles(token_id,author,created_level,created_at,editions,minted,burned,thumbnail_uri,chain_level) VALUES (1,'tz1gL9H53BzjDqmMi2JkKALeCzT7UAoopzEL',1,'2026-10-05',1,1,0,?,1)", [format!("ipfs://{cid}")]).unwrap();
    let response = get(&archive.art, "/article-media/1/0").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["cache-control"], "no-store");
    let etag = response.headers()["etag"].clone();
    drop(response);
    let calls = archive.source.calls.load(Ordering::Relaxed);
    connection
        .execute("UPDATE articles SET hidden=1 WHERE token_id=1", [])
        .unwrap();
    let response = archive
        .art
        .clone()
        .oneshot(
            Request::get("/article-media/1/0")
                .header("if-none-match", etag)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(archive.source.calls.load(Ordering::Relaxed), calls);
    connection
        .execute("UPDATE articles SET hidden=0 WHERE token_id=1", [])
        .unwrap();
    assert_eq!(
        get(&archive.art, "/article-media/1/999").await.status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        get(&archive.art, "/article-media/1/0").await.status(),
        StatusCode::OK
    );
    assert_eq!(
        archive.source.calls.load(Ordering::Relaxed),
        calls,
        "verified media stays in bounded memory without persistent writes"
    );
    assert_eq!(
        seeded_archive::content::cache::DiskCache::open(&archive.root.join("cache"))
            .unwrap()
            .disk_usage()
            .unwrap(),
        0,
        "article media gets no persistent cache budget"
    );
}

#[tokio::test]
async fn verified_byte_ranges_follow_encoding_validators_and_head_semantics() {
    let body = b"<html>range test</html>";
    let archive = Archive::new(body, 4096).await;
    let path = archive.resource();
    let initial = get(&archive.art, &path).await;
    let identity_tag = initial.headers()["etag"].clone();
    assert_eq!(initial.headers()["accept-ranges"], "bytes");
    drop(initial);
    for (range, start, end) in [
        ("bytes=0-1", 0, 1),
        ("bytes=6-", 6, body.len() - 1),
        ("bytes=-4", body.len() - 4, body.len() - 1),
        ("BYTES=0-9999", 0, body.len() - 1),
    ] {
        let response = archive
            .art
            .clone()
            .oneshot(
                Request::get(&path)
                    .header("range", range)
                    .header("if-range", &identity_tag)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT, "{range}");
        assert_eq!(
            response.headers()["content-range"],
            format!("bytes {start}-{end}/{}", body.len())
        );
        assert_eq!(response.headers()["x-seeded-verification"], "onchfs-keccak");
        assert!(
            response.headers()["cache-control"]
                .to_str()
                .unwrap()
                .contains("immutable")
        );
        assert_eq!(
            response
                .into_body()
                .collect()
                .await
                .unwrap()
                .to_bytes()
                .as_ref(),
            &body[start..=end]
        );
    }
    for validator in [
        "\"wrong\"".to_owned(),
        format!("W/{}", identity_tag.to_str().unwrap()),
        "Thu, 01 Jan 1970 00:00:00 GMT".to_owned(),
    ] {
        let response = archive
            .art
            .clone()
            .oneshot(
                Request::get(&path)
                    .header("range", "bytes=0-1")
                    .header("if-range", validator)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .into_body()
                .collect()
                .await
                .unwrap()
                .to_bytes()
                .as_ref(),
            body
        );
    }
    for range in [
        "items=0-1",
        "bytes=nope",
        "bytes=8-1",
        "bytes=0-1,3-4",
        "bytes=184467440737095516160-",
    ] {
        let response = archive
            .art
            .clone()
            .oneshot(
                Request::get(&path)
                    .header("range", range)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{range}");
        drop(response);
    }
    for range in ["bytes=9999-", "bytes=-0"] {
        let response = archive
            .art
            .clone()
            .oneshot(
                Request::get(&path)
                    .header("range", range)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::RANGE_NOT_SATISFIABLE);
        assert_eq!(
            response.headers()["content-range"],
            format!("bytes */{}", body.len())
        );
        assert_eq!(response.headers()["cache-control"], "no-store");
    }
    let response = archive
        .art
        .clone()
        .oneshot(
            Request::get(&path)
                .header("range", "bytes=0-1")
                .header("if-none-match", &identity_tag)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
    assert!(!response.headers().contains_key("content-range"));
    drop(response);
    let response = archive
        .art
        .clone()
        .oneshot(
            Request::head(&path)
                .header("range", "bytes=0-1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(!response.headers().contains_key("content-range"));
    assert!(
        response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .is_empty()
    );
    let response = archive
        .art
        .clone()
        .oneshot(
            Request::get(&path)
                .header("range", "bytes=0-1")
                .header("accept-encoding", "gzip")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(response.headers()["content-encoding"], "gzip");
    assert_ne!(response.headers()["etag"], identity_tag);
    assert_eq!(
        response.headers()["content-range"],
        format!("bytes 0-1/{}", archive.source.body.len())
    );
    assert_eq!(
        response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .as_ref(),
        &archive.source.body[..2]
    );
}
