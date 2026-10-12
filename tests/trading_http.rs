use axum::{Json, Router, extract::State, http::StatusCode, response::IntoResponse, routing::get};
use seeded_archive::trading::{
    TradingError,
    http::{Endpoint, Http, Transport},
};
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicU16, Ordering},
};
#[tokio::test]
async fn transient_http_statuses_retry_without_becoming_permanent_simulation_failures() {
    let status = Arc::new(AtomicU16::new(200));
    let app = Router::new()
        .route(
            "/probe",
            get(|State(status): State<Arc<AtomicU16>>| async move {
                (
                    StatusCode::from_u16(status.load(Ordering::SeqCst)).unwrap(),
                    Json(json!({"ok":true})),
                )
                    .into_response()
            }),
        )
        .with_state(status.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let base = format!("http://{address}");
    let http = Http::new(&base, &base).unwrap();
    for code in [408, 425, 429, 503] {
        status.store(code, Ordering::SeqCst);
        assert!(matches!(
            http.request(Endpoint::Rpc, "/probe".into(), None).await,
            Err(TradingError::Unavailable)
        ));
        status.store(200, Ordering::SeqCst);
        assert_eq!(
            http.request(Endpoint::Rpc, "/probe".into(), None)
                .await
                .unwrap()["ok"],
            true
        );
    }
    status.store(400, Ordering::SeqCst);
    assert!(matches!(
        http.request(Endpoint::Rpc, "/probe".into(), None).await,
        Err(TradingError::Simulation)
    ));
    status.store(404, Ordering::SeqCst);
    assert!(matches!(
        http.request(Endpoint::Rpc, "/probe".into(), None).await,
        Err(TradingError::Changed)
    ));
    server.abort();
    let _ = server.await;
}
