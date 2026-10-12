use std::{
    collections::BTreeMap,
    error::Error,
    io::Write,
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};

use axum::{
    Json, Router,
    extract::{OriginalUri, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use flate2::{Compression, write::GzEncoder};
use seeded_archive::{
    config::Config,
    content::{
        ContentStore,
        onchfs::{file_cid, keccak},
    },
    import::import_fixtures,
    request_limits::{self, RequestLimits},
    store::Store,
    transport::BoundedListener,
    web::{self, artwork},
};
use serde_json::json;
use tokio::{net::TcpListener, sync::watch, task::JoinHandle};

const CONTRACT: &str = "KT1EfsNuqwLAWDd3o4pvfUx1CAh5GMdTrRvr";
const LARGE_FILE_BYTES: u64 = 32 << 20;
const ARTWORK_BYTES: usize = 31 << 20;

#[derive(Clone)]
struct MockOnchfs {
    cid: String,
    chunk: String,
    metadata: Vec<u8>,
    body: Vec<u8>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    run().await
}

async fn run() -> Result<(), Box<dyn Error>> {
    let output_root = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .ok_or("usage: proxy_fixture TEMP_DIRECTORY")?;
    std::fs::create_dir_all(&output_root)?;
    let runtime_root = output_root.join(format!("proxy-fixture-{}", std::process::id()));
    std::fs::create_dir(&runtime_root)?;
    let _cleanup = Cleanup(runtime_root.clone());

    let assets = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("assets");
    let fixture_assets = runtime_root.join("assets");
    prepare_assets(&assets, &fixture_assets)?;
    write_large_file(&fixture_assets.join("static/large.bin"))?;

    let mut gzip = GzEncoder::new(Vec::new(), Compression::default());
    gzip.write_all(&vec![b'x'; ARTWORK_BYTES])?;
    let body = gzip.finish()?;
    let metadata = vec![
        0x5f, 9, b't', b'e', b'x', b't', b'/', b'h', b't', b'm', b'l', 0x5a, 4, b'g', b'z', b'i',
        b'p',
    ];
    let fixture = MockOnchfs {
        cid: hex::encode(file_cid(&keccak(&body), &metadata)),
        chunk: hex::encode(keccak(&body)),
        metadata,
        body,
    };

    let app_listener = TcpListener::bind("127.0.0.1:0").await?;
    let art_listener = TcpListener::bind("127.0.0.1:0").await?;
    let upstream_listener = TcpListener::bind("127.0.0.1:0").await?;
    let app_addr = app_listener.local_addr()?;
    let art_addr = art_listener.local_addr()?;
    let upstream_addr = upstream_listener.local_addr()?;
    let config = config(
        app_addr,
        art_addr,
        upstream_addr,
        runtime_root.join("archive.sqlite"),
        fixture_assets.clone(),
        runtime_root.join("cache"),
    )?;

    let mut store = Store::open(&config.database)?;
    import_fixtures(&mut store, &fixture_assets.join("fixtures"))?;
    drop(store);
    let connection = rusqlite::Connection::open(&config.database)?;
    connection.execute(
        "UPDATE tokens SET artifact_uri=?1 WHERE contract=?2 AND token_id=133459",
        (format!("onchfs://{}", fixture.cid), CONTRACT),
    )?;
    drop(connection);
    let mut store = Store::open(&config.database)?;
    seeded_archive::ingest::upgrade::upgrade(&mut store)?;
    drop(store);

    let content = ContentStore::new(config.content_config())?;
    let app_state = web::AppState::new(&config.database, &config.assets).configured(&config);
    let limits = RequestLimits::with_content_limit(4);
    let app = request_limits::protect(web::router(app_state.clone()), limits.clone(), true);
    let art_state = artwork::ArtworkState::new(app_state, content, &config);
    let art = request_limits::protect(artwork::router(art_state), limits.artwork(), true);
    let upstream = Router::new()
        .fallback(mock_onchfs)
        .with_state(fixture.clone());

    let (stop, receiver) = watch::channel(false);
    let mut app_task = serve(app_listener, app, receiver.clone());
    let mut art_task = serve(art_listener, art, receiver.clone());
    let upstream_task = tokio::spawn(async move {
        axum::serve(upstream_listener, upstream)
            .with_graceful_shutdown(wait_stop(receiver))
            .await
    });
    tokio::pin!(upstream_task);

    println!(
        "{}",
        json!({
            "app_port": app_addr.port(),
            "art_port": art_addr.port(),
            "art_path": format!("/content/onchfs/{}", fixture.cid),
        })
    );
    let result: Result<(), Box<dyn Error>> = tokio::select! {
        signal = shutdown_signal() => signal.map_err(|error| Box::new(error) as Box<dyn Error>),
        _ = &mut app_task => Err(Box::new(std::io::Error::other("app listener stopped unexpectedly")) as Box<dyn Error>),
        _ = &mut art_task => Err(Box::new(std::io::Error::other("artwork listener stopped unexpectedly")) as Box<dyn Error>),
        _ = &mut upstream_task => Err(Box::new(std::io::Error::other("mock upstream stopped unexpectedly")) as Box<dyn Error>),
    };
    let _ = stop.send(true);
    let drained = tokio::time::timeout(Duration::from_secs(10), async {
        let _ = (&mut app_task).await;
        let _ = (&mut art_task).await;
        let _ = (&mut upstream_task).await;
    })
    .await
    .is_ok();
    if !drained {
        app_task.abort();
        art_task.abort();
        upstream_task.as_mut().abort();
        let _ = app_task.await;
        let _ = art_task.await;
        let _ = upstream_task.await;
    }
    result
}

struct Cleanup(PathBuf);

impl Drop for Cleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn config(
    app: SocketAddr,
    art: SocketAddr,
    upstream: SocketAddr,
    database: PathBuf,
    assets: PathBuf,
    cache: PathBuf,
) -> Result<Config, Box<dyn Error>> {
    let mut values = BTreeMap::new();
    for (key, value) in [
        ("SEEDED_BIND", app.to_string()),
        ("SEEDED_CONTENT_BIND", art.to_string()),
        ("SEEDED_DATABASE", database.display().to_string()),
        ("SEEDED_ASSETS", assets.display().to_string()),
        ("SEEDED_CACHE", cache.display().to_string()),
        ("SEEDED_TZKT_URL", format!("http://{upstream}/v1")),
        ("SEEDED_IPFS_GATEWAYS", format!("http://{upstream}/ipfs/")),
        ("SEEDED_TRUST_LOOPBACK_PROXY", "true".into()),
        ("SEEDED_MAX_OBJECT_BYTES", (32_u64 << 20).to_string()),
        ("SEEDED_CONTENT_CONCURRENCY", "4".into()),
        ("SEEDED_CACHE_BYTES", (64_u64 << 20).to_string()),
        ("SEEDED_TZKT_TIMEOUT_SECS", "10".into()),
    ] {
        values.insert(key.to_owned(), value);
    }
    Ok(Config::from_values(&values)?)
}

fn prepare_assets(source: &Path, destination: &Path) -> Result<(), Box<dyn Error>> {
    std::fs::create_dir_all(destination.join("static"))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;
        symlink(source.join("fixtures"), destination.join("fixtures"))?;
        for entry in std::fs::read_dir(source.join("static"))? {
            let entry = entry?;
            if entry.file_name() != "large.bin" {
                symlink(
                    entry.path(),
                    destination.join("static").join(entry.file_name()),
                )?;
            }
        }
    }
    #[cfg(not(unix))]
    {
        return Err("proxy_fixture requires Unix symlink support".into());
    }
    Ok(())
}

fn write_large_file(path: &Path) -> Result<(), Box<dyn Error>> {
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)?;
    let chunk = [0_u8; 64 * 1024];
    for _ in 0..(LARGE_FILE_BYTES / chunk.len() as u64) {
        file.write_all(&chunk)?;
    }
    Ok(())
}

fn serve(listener: TcpListener, router: Router, stop: watch::Receiver<bool>) -> JoinHandle<()> {
    tokio::spawn(async move {
        axum::serve(
            BoundedListener::new(listener).with_delivery_progress(),
            router
                .into_make_service_with_connect_info::<seeded_archive::transport::ConnectionInfo>(),
        )
        .with_graceful_shutdown(wait_stop(stop))
        .await
        .expect("proxy fixture listener failed");
    })
}

async fn wait_stop(mut stop: watch::Receiver<bool>) {
    if !*stop.borrow() {
        let _ = stop.changed().await;
    }
}

async fn shutdown_signal() -> std::io::Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result,
            _ = terminate.recv() => Ok(()),
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await
}

async fn mock_onchfs(State(fixture): State<MockOnchfs>, OriginalUri(uri): OriginalUri) -> Response {
    if uri.path().ends_with(&fixture.cid) {
        Json(json!({
            "active": true,
            "value": {"file": {
                "metadata": hex::encode(&fixture.metadata),
                "chunk_pointers": [&fixture.chunk],
            }}
        }))
        .into_response()
    } else if uri.query().is_some() {
        Json(json!([{
            "active": true,
            "key": fixture.chunk,
            "value": hex::encode(&fixture.body),
        }]))
        .into_response()
    } else {
        StatusCode::NOT_FOUND.into_response()
    }
}
