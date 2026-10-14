use std::{
    convert::Infallible,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use axum::{
    Router,
    body::{Body, HttpBody},
    http::{Request, StatusCode, header},
    response::Response,
    routing::get,
};
use bytes::Bytes;
use http_body_util::BodyExt;
use seeded_archive::metrics::{Metrics, Origin, instrument};
use tower::ServiceExt;

fn request(path: &str) -> Request<Body> {
    Request::builder().uri(path).body(Body::empty()).unwrap()
}

async fn snapshot(router: Router) -> serde_json::Value {
    let response = router.oneshot(request("/health/metrics")).await.unwrap();
    assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
    assert_eq!(
        response.headers()[header::X_CONTENT_TYPE_OPTIONS],
        "nosniff"
    );
    assert_eq!(response.headers()[header::CONTENT_TYPE], "application/json");
    serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap()
}

#[tokio::test]
async fn records_bounded_route_status_and_latency_counts() {
    let metrics = Arc::new(Metrics::default());
    let app = Router::new()
        .route("/", get(|| async { StatusCode::OK }))
        .route("/api/x", get(|| async { StatusCode::TOO_MANY_REQUESTS }))
        .route("/static/x", get(|| async { StatusCode::NOT_FOUND }))
        .route(
            "/render/x",
            get(|| async { StatusCode::SERVICE_UNAVAILABLE }),
        )
        .route("/content/x", get(|| async { StatusCode::OK }))
        .route("/fixtures/previews/x", get(|| async { StatusCode::FOUND }))
        .route(
            "/unknown",
            get(|| async { StatusCode::INTERNAL_SERVER_ERROR }),
        )
        .fallback(|| async { StatusCode::NOT_FOUND });
    let app = instrument(app, metrics.clone(), Origin::Artwork);

    for path in [
        "/",
        "/api/x?private=query",
        "/static/x",
        "/render/x",
        "/content/x",
        "/fixtures/previews/x",
        "/unknown",
    ] {
        app.clone().oneshot(request(path)).await.unwrap();
    }
    let data = snapshot(app.clone()).await;
    assert_eq!(data["schema"], 1);
    assert_eq!(data["origin"], "artwork");
    assert!(data["process_start"].as_str().unwrap().ends_with('Z'));
    assert!(!data.to_string().contains("private"));
    assert_eq!(data["requests"]["total"], 7);
    assert_eq!(data["requests"]["by_class"]["2xx"], 2);
    assert_eq!(data["requests"]["by_class"]["3xx"], 1);
    assert_eq!(data["requests"]["by_class"]["4xx"], 2);
    assert_eq!(data["requests"]["by_class"]["5xx"], 2);
    assert_eq!(data["requests"]["by_status"]["200"], 2);
    assert_eq!(data["requests"]["by_status"]["302"], 1);
    assert_eq!(data["requests"]["by_status"]["404"], 1);
    assert_eq!(data["requests"]["by_status"]["429"], 1);
    assert_eq!(data["requests"]["by_status"]["503"], 1);
    assert_eq!(data["requests"]["by_status"]["500"], 1);
    assert_eq!(data["requests"]["by_route"]["page"], 1);
    assert_eq!(data["requests"]["by_route"]["api"], 1);
    assert_eq!(data["requests"]["by_route"]["static"], 1);
    assert_eq!(data["requests"]["by_route"]["render"], 1);
    assert_eq!(data["requests"]["by_route"]["content"], 1);
    assert_eq!(data["requests"]["by_route"]["preview"], 1);
    assert_eq!(data["requests"]["by_route"]["other"], 1);
    assert_eq!(data["latency_ms"]["count"], 7);
    assert_eq!(data["latency_ms"]["counts"].as_array().unwrap().len(), 12);
    assert_eq!(
        data["latency_ms"]["counts"]
            .as_array()
            .unwrap()
            .iter()
            .map(|n| n.as_u64().unwrap())
            .sum::<u64>(),
        7
    );
    assert_eq!(data["active"]["requests"], 0);
    assert!(data["active"]["max_seen"].as_u64().unwrap() >= 1);
}

#[tokio::test]
async fn keeps_unusual_but_valid_status_codes_in_the_bounded_status_map() {
    let metrics = Arc::new(Metrics::default());
    let status = StatusCode::from_u16(700).unwrap();
    let app = instrument(
        Router::new().route("/status", get(move || async move { status })),
        metrics,
        Origin::App,
    );
    app.clone().oneshot(request("/status")).await.unwrap();
    let data = snapshot(app).await;
    assert_eq!(data["requests"]["by_status"]["700"], 1);
    assert_eq!(data["requests"]["by_class"]["5xx"], 0);
}

#[tokio::test]
async fn observes_admission_rejections_and_health_remains_available_when_full() {
    use axum::extract::ConnectInfo;
    use seeded_archive::{
        client_ip::PeerAddr,
        request_limits::{self, RequestLimits},
    };
    let protected = request_limits::protect(
        Router::new().route("/", get(|| async { "held body" })),
        RequestLimits::default(),
        false,
    );
    let app = instrument(protected, Arc::new(Metrics::default()), Origin::App);
    // Real identify middleware rejects a request without transport identity.
    assert_eq!(
        app.clone().oneshot(request("/")).await.unwrap().status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    let identified = || {
        let mut request = request("/");
        request
            .extensions_mut()
            .insert(ConnectInfo(PeerAddr("192.0.2.1:3000".parse().unwrap())));
        request
    };
    let mut held = Vec::new();
    for _ in 0..16 {
        let response = app.clone().oneshot(identified()).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        held.push(response);
    }
    assert_eq!(
        app.clone().oneshot(identified()).await.unwrap().status(),
        StatusCode::TOO_MANY_REQUESTS
    );
    let data = snapshot(app.clone()).await;
    assert_eq!(data["requests"]["by_status"]["429"], 1);
    assert_eq!(data["requests"]["by_status"]["503"], 1);
    assert_eq!(data["active"]["requests"], 16);
    drop(held);
    assert_eq!(snapshot(app).await["active"]["requests"], 0);
}

#[tokio::test]
async fn excludes_all_health_requests_and_keeps_origins_separate() {
    use seeded_archive::request_limits::{self, RequestLimits};

    let app_limits = RequestLimits::new();
    let app_limit_metrics = app_limits.metrics_handle();
    let art_limits = RequestLimits::new().artwork();
    let art_limit_metrics = art_limits.metrics_handle();
    let app = instrument(
        request_limits::protect(
            Router::new().route("/", get(|| async { StatusCode::OK })),
            app_limits,
            false,
        ),
        Arc::new(Metrics::default().with_request_limits(app_limit_metrics)),
        Origin::App,
    );
    let art = instrument(
        request_limits::protect(
            Router::new().route("/", get(|| async { StatusCode::OK })),
            art_limits,
            false,
        ),
        Arc::new(Metrics::default().with_request_limits(art_limit_metrics)),
        Origin::Artwork,
    );

    app.clone().oneshot(request("/health/live")).await.unwrap();
    assert_eq!(
        app.clone().oneshot(request("/")).await.unwrap().status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    let app_data = snapshot(app).await;
    let art_data = snapshot(art).await;
    assert_eq!(app_data["origin"], "app");
    assert_eq!(app_data["requests"]["total"], 1);
    assert!(app_data.get("artwork_admission").is_none());
    let app_limits = app_data["request_limits"].as_object().unwrap();
    let art_limits = art_data["request_limits"].as_object().unwrap();
    assert_eq!(app_limits.len(), 8);
    assert_eq!(app_limits["missing_client_ip"], 1);
    assert_eq!(art_limits.len(), 8);
    assert_eq!(art_limits["missing_client_ip"], 0);
    for name in [
        "global_content_wait_full",
        "client_content_wait_full",
        "content_wait_timeout",
        "noncontent_bucket_rejected",
        "family_new_client_rejected",
        "noncontent_stream_timeout",
        "client_table_full",
        "missing_client_ip",
    ] {
        assert!(app_limits.contains_key(name));
        assert!(art_limits.contains_key(name));
    }
    assert!(!app_data.to_string().contains("192.0.2."));
    assert_eq!(art_data["origin"], "artwork");
    assert_eq!(art_data["requests"]["total"], 0);
}

struct PendingBody;

impl HttpBody for PendingBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, Self::Error>>> {
        Poll::Pending
    }
}

#[tokio::test]
async fn active_request_lives_through_body_and_releases_when_dropped() {
    let metrics = Arc::new(Metrics::default());
    let app = instrument(
        Router::new().route(
            "/stream",
            get(|| async { Response::new(Body::new(PendingBody)) }),
        ),
        metrics,
        Origin::App,
    );
    let response = app.clone().oneshot(request("/stream")).await.unwrap();
    assert_eq!(snapshot(app.clone()).await["active"]["requests"], 1);
    drop(response);
    assert_eq!(snapshot(app).await["active"]["requests"], 0);
}

#[tokio::test]
async fn final_data_frame_keeps_active_request_until_its_bytes_drop() {
    let metrics = Arc::new(Metrics::default());
    let app = instrument(
        Router::new().route("/finite", get(|| async { "payload" })),
        metrics,
        Origin::App,
    );
    let response = app.clone().oneshot(request("/finite")).await.unwrap();
    let mut body = response.into_body();
    let frame = body.frame().await.unwrap().unwrap();
    drop(body);
    assert_eq!(snapshot(app.clone()).await["active"]["requests"], 1);
    assert_eq!(snapshot(app.clone()).await["body"]["bytes_emitted"], 7);
    drop(frame);
    assert_eq!(snapshot(app).await["active"]["requests"], 0);
}

#[tokio::test]
async fn cancelled_handler_releases_active_request() {
    let metrics = Arc::new(Metrics::default());
    let entered = Arc::new(tokio::sync::Notify::new());
    let handler_entered = entered.clone();
    let app = instrument(
        Router::new().route(
            "/wait",
            get(move || {
                let entered = handler_entered.clone();
                async move {
                    entered.notify_one();
                    std::future::pending::<StatusCode>().await
                }
            }),
        ),
        metrics,
        Origin::App,
    );
    let task = tokio::spawn(app.clone().oneshot(request("/wait")));
    entered.notified().await;
    assert_eq!(snapshot(app.clone()).await["active"]["requests"], 1);
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    let data = snapshot(app).await;
    assert_eq!(data["active"]["requests"], 0);
    assert_eq!(data["requests"]["cancelled_before_headers"], 1);
}

struct FailingBody;

impl HttpBody for FailingBody {
    type Data = Bytes;
    type Error = std::io::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, Self::Error>>> {
        Poll::Ready(Some(Err(std::io::Error::other("fixture body failure"))))
    }
}

#[tokio::test]
async fn records_body_errors_and_bytes_yielded() {
    let metrics = Arc::new(Metrics::default());
    let app = instrument(
        Router::new().route(
            "/failure",
            get(|| async { Response::new(Body::new(FailingBody)) }),
        ),
        metrics,
        Origin::App,
    );
    let response = app.clone().oneshot(request("/failure")).await.unwrap();
    assert!(response.into_body().frame().await.unwrap().is_err());
    let data = snapshot(app).await;
    assert_eq!(data["body"]["errors"], 1);
    assert_eq!(data["body"]["bytes_emitted"], 0);
    assert_eq!(data["active"]["requests"], 0);
}

#[tokio::test]
async fn content_metrics_are_merged_into_snapshot() {
    use seeded_archive::content::{ContentConfig, ContentStore};

    let cache_dir = std::env::current_dir()
        .unwrap()
        .join("tmp")
        .join(format!("metrics-content-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&cache_dir);
    let content = ContentStore::new(ContentConfig::mainnet(&cache_dir)).unwrap();
    let metrics = Arc::new(Metrics::default().with_content(content));
    let app = instrument(Router::new(), metrics, Origin::Artwork);
    let data = snapshot(app).await;
    assert_eq!(data["cache"]["enabled"], true);
    assert_eq!(data["cache"]["hits"], 0);
    assert_eq!(data["cache"]["retention_skipped"], 0);
    assert_eq!(data["mirror"]["hits"], 0);
    assert_eq!(data["local_delivery"]["bytes_quota"], 512 << 20);
    assert_eq!(data["local_delivery"]["waiting"], 0);
    assert_eq!(data["local_delivery"]["wait_capacity"], 256);
    assert_eq!(data["local_scratch"]["bytes_quota"], 512 << 20);
    assert_eq!(data["local_scratch"]["waiting"], 0);
    assert_eq!(data["local_reads"]["active"], 0);
    assert_eq!(data["local_reads"]["waiting"], 0);
    assert_eq!(data["local_reads"]["wait_capacity"], 256);
    assert_eq!(data["local_reads"]["deadline_expired"], 0);
    assert_eq!(data["local_reads"]["shared_failures"], 0);
    assert_eq!(data["local_reads"]["coalesced"], 0);
    assert_eq!(data["local_reads"]["coordination_rejected"], 0);
    assert_eq!(data["local_reads"]["leader_waiting"], 0);
    assert_eq!(data["local_reads"]["leader_wait_capacity"], 256);
    assert_eq!(data["local_reads"]["leader_waits"], 0);
    assert_eq!(data["local_reads"]["leader_timeouts"], 0);
    assert_eq!(data["local_reads"]["leader_rejected"], 0);
    assert_eq!(data["resolution"]["upstream_failures"], 0);
    assert_eq!(data["resolution"]["local_failures"], 0);
    assert_eq!(data["resolution"]["upstream_other_failures"], 0);
    assert_eq!(data["resolution"]["local_other_failures"], 0);
    assert_eq!(data["resolution"]["upstream_busy"], 0);
    assert_eq!(data["resolution"]["upstream_timeouts"], 0);
    assert_eq!(data["resolution"]["mirror_busy"], 0);
    std::fs::remove_dir_all(cache_dir).unwrap();
}

#[tokio::test]
async fn app_database_admission_is_reported_without_error_text() {
    use seeded_archive::web::{self, AppState};
    let state = AppState::new("tmp/missing-metrics-database/absent.sqlite", "assets");
    let app = instrument(
        web::router(state.clone()),
        Arc::new(Metrics::default().with_database(state.database_metrics_handle())),
        Origin::App,
    );
    assert_eq!(
        app.clone()
            .oneshot(request("/projects"))
            .await
            .unwrap()
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    // The page and its background site-facts refresh are both refused; the refresh may
    // finish after the response.
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    let data = loop {
        let data = snapshot(app.clone()).await;
        if data["database_admission"]["query_unavailable"] == 2
            || tokio::time::Instant::now() >= deadline
        {
            break data;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    };
    let counters = data["database_admission"].as_object().unwrap();
    assert_eq!(counters.len(), 4);
    assert_eq!(counters["query_unavailable"], 2);
    assert_eq!(counters["query_timeout"], 0);
    assert!(!data.to_string().contains("absent.sqlite"));
    assert!(!data.to_string().contains("Archive"));
}

#[tokio::test]
async fn artwork_admission_metrics_are_bounded_and_private() {
    use seeded_archive::{
        config::Config,
        content::{ContentConfig, ContentStore},
        web::{AppState, artwork::ArtworkState},
    };
    use std::collections::BTreeMap;

    let root = std::env::current_dir()
        .unwrap()
        .join("tmp")
        .join(format!("metrics-artwork-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let mut config = Config::from_values(&BTreeMap::new()).unwrap();
    config.database = root.join("archive.sqlite");
    config.assets = std::env::current_dir().unwrap().join("assets");
    let mut content_config = ContentConfig::mainnet(root.join("cache"));
    content_config.ipfs_gateways.clear();
    let content = ContentStore::new(content_config).unwrap();
    let state = ArtworkState::new(
        AppState::new(&config.database, &config.assets).configured(&config),
        content.clone(),
        &config,
    );
    let app = instrument(
        Router::new(),
        Arc::new(
            Metrics::default()
                .with_content(content)
                .with_artwork(state.metrics_handle()),
        ),
        Origin::Artwork,
    );

    let data = snapshot(app).await;
    let admission = data["artwork_admission"].as_object().unwrap();
    for name in [
        "metadata_query_timeout",
        "metadata_query_unavailable",
        "handler_timeout",
        "delivery_client_table_full",
        "metadata_waiting_full",
        "metadata_waiting_timeout",
        "upstream_waiting_full",
        "upstream_waiting_timeout",
        "transform_timeout",
        "delivery_global_bytes_full",
        "delivery_client_bytes_full",
        "delivery_reclaim_scans",
        "delivery_reclaimed",
        "delivery_reclaim_no_victim",
    ] {
        assert_eq!(admission[name], 0);
    }
    assert_eq!(admission.len(), 14);
    for pool in ["local_delivery", "local_scratch"] {
        for counter in ["reclaim_scans", "reclaimed", "reclaim_no_victim"] {
            assert_eq!(data[pool][counter], 0);
        }
    }
    assert_eq!(data["local_scratch"]["wait_capacity"], 256);
    for forbidden in ["address", "contract", "token_id", "seed"] {
        assert!(!data.to_string().contains(forbidden));
    }
    std::fs::remove_dir_all(root).unwrap();
}
