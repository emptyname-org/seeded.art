use std::{
    env,
    error::Error,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use seeded_archive::{
    backup::{BackupOptions, create_backup},
    config::{Config, Role},
    content::ContentStore,
    import::import_fixtures,
    metrics::{self, Metrics, Origin},
    request_limits::{RequestLimits, protect},
    store::{Store, StoreError},
    trading::TradingState,
    transport::BoundedListener,
    web::{AppState, artwork, router},
};

const USAGE: &str =
    "Usage: seeded_archive [serve [--role all|app|content]|dev|import-demo|backup DESTINATION]";

enum Command {
    Serve,
    Dev,
    ImportDemo,
    Backup(PathBuf),
}

fn main() -> Result<(), Box<dyn Error>> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let result = runtime.block_on(run());
    runtime.shutdown_timeout(Duration::from_secs(5));
    result
}

/// The command and its `--role` flag; the role itself is resolved with `SEEDED_ROLE`.
fn parse_args(args: &[String]) -> Result<(Command, Option<&str>), Box<dyn Error>> {
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    Ok(match args.as_slice() {
        [] | ["serve"] => (Command::Serve, None),
        ["serve", "--role", role] => (Command::Serve, Some(*role)),
        ["dev"] => (Command::Dev, None),
        ["import-demo"] => (Command::ImportDemo, None),
        ["backup", destination] => (Command::Backup(PathBuf::from(destination)), None),
        _ => return Err(USAGE.into()),
    })
}

async fn run() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().skip(1).collect();
    let (command, flag) = parse_args(&args)?;
    let role = match command {
        Command::Serve | Command::Dev => {
            let environment = env::var_os("SEEDED_ROLE")
                .map(|value| value.into_string())
                .transpose()
                .map_err(|_| "SEEDED_ROLE must be UTF-8")?;
            Role::resolve(flag, environment.as_deref())?
        }
        Command::ImportDemo | Command::Backup(_) => Role::All,
    };
    if matches!(command, Command::Dev) && role != Role::All {
        return Err("dev serves both listeners; split processes use serve --role".into());
    }
    let config = Config::from_env_for(role)?;
    let (import, dev) = match command {
        Command::Backup(destination) => return backup(&config, &destination).await,
        Command::ImportDemo => (true, false),
        Command::Dev => (false, true),
        Command::Serve => (false, false),
    };
    if import || dev {
        prepare_database(&config.database)?;
    } else if !config.database.is_file() {
        return Err(
            "Archive database is missing; import data first, or use dev for the demo".into(),
        );
    }
    if role.serves_app() {
        let mut store = Store::open(&config.database)?;
        let empty = match store.snapshot() {
            Ok(_) => false,
            Err(StoreError::NotFound { .. }) => true,
            Err(error) => return Err(error.into()),
        };
        if import && !empty {
            return Err(
                "Demo import requires an empty database; use a separate SEEDED_DATABASE".into(),
            );
        }
        if empty && !dev && !import {
            return Err("Archive database has no snapshot; import data first".into());
        }
        if (empty && dev) || import {
            let report = import_fixtures(&mut store, &config.assets.join("fixtures"))?;
            println!(
                "Snapshot: {} projects, {} editions{}.",
                report.projects,
                report.tokens,
                if report.changed {
                    " imported"
                } else {
                    " unchanged"
                }
            );
        }
        if dev || import {
            seeded_archive::ingest::identity::apply_to(&mut store)?;
        }
    } else {
        // Artwork only reads; creating and migrating the archive is the app's job.
        match Store::open_read_only(&config.database)?.snapshot() {
            Ok(_) => {}
            Err(StoreError::NotFound { .. }) => {
                return Err("Archive database has no snapshot; import data first".into());
            }
            Err(error) => return Err(error.into()),
        }
    }
    if import {
        return Ok(());
    }
    if role.serves_app() {
        for path in [
            config.assets.join("static"),
            config.assets.join("fixtures/previews"),
        ] {
            if !path.is_dir() {
                return Err("Assets are missing; set SEEDED_ASSETS".into());
            }
        }
    }
    serve(config).await
}

/// One listener of this process.
struct Listener {
    name: &'static str,
    bind: SocketAddr,
    router: axum::Router,
}

type AppRouter = (
    axum::Router,
    Arc<TradingState>,
    seeded_archive::request_limits::RequestLimitMetricsHandle,
    seeded_archive::web::DatabaseMetricsHandle,
);

/// Application side: pages, sessions, sign-in, market. Its fee audit runs as a worker.
/// `content` is set only when this process also serves artwork, for cache readiness.
fn app_router(config: &Config, content: Option<ContentStore>) -> Result<AppRouter, Box<dyn Error>> {
    let auth = seeded_archive::auth::AuthState::new(&config.app_origin)?;
    let trading = Arc::new(
        TradingState::new(seeded_archive::trading::TradingConfig {
            rpc_url: config.trading_rpc.clone(),
            tzkt_url: config.trading_tzkt.clone(),
            app_origin: config.app_origin.clone(),
            platform_fee_bps: config.platform_fee_bps,
            platform_fee_address: config.platform_fee_address.clone(),
        })?
        .with_optional_fee_audit(&config.trading_database)
        .with_sync_request(config.database.with_file_name("sync.request")),
    );
    let release = Arc::new(seeded_archive::release::ReleaseClient::new(
        &config.tzkt_url,
    )?);
    let mut app = AppState::new(&config.database, &config.assets)
        .configured(config)
        .with_auth(auth)
        .with_claim_registry(seeded_archive::web::claims::ClaimRegistry::new(
            &config.tzkt_url,
        )?)
        .with_trading(trading.clone())
        .with_release(release);
    if let Some(content) = content {
        app = app.with_content(content);
    }
    if let Some(client) = config.oauth.clone() {
        app = app.with_oauth(Arc::new(seeded_archive::auth::oauth::OAuthState::new(
            client,
        )));
    }
    let limits = RequestLimits::with_content_limit(config.assemblies);
    let request_limit_metrics = limits.metrics_handle();
    let database_metrics = app.database_metrics_handle();
    let router = protect(router(app), limits, config.trust_loopback_proxy);
    Ok((router, trading, request_limit_metrics, database_metrics))
}

/// Artwork side: render, content, media and playback status. Shares no memory with the app;
/// archive reads use its own read-only database pool.
fn content_router(
    config: &Config,
    content: ContentStore,
) -> (
    axum::Router,
    artwork::ArtworkMetricsHandle,
    seeded_archive::request_limits::RequestLimitMetricsHandle,
) {
    let archive = AppState::new(&config.database, &config.assets).configured(config);
    let state = artwork::ArtworkState::new(archive, content, config);
    let metrics = state.metrics_handle();
    let limits = RequestLimits::with_content_limit(config.assemblies).artwork();
    let request_limit_metrics = limits.metrics_handle();
    let router = protect(artwork::router(state), limits, config.trust_loopback_proxy);
    (router, metrics, request_limit_metrics)
}

async fn serve(config: Config) -> Result<(), Box<dyn Error>> {
    let role = config.role;
    // Only a process that serves artwork opens the cache: one writer per cache directory.
    let content = if role.serves_content() {
        Some(ContentStore::new(config.content_config())?)
    } else {
        None
    };
    // Each role's routers are built here, once per listener; per-origin instrumentation
    // wraps each `router` before it is pushed.
    let mut listeners = Vec::new();
    let mut fee_audit = None;
    if role.serves_app() {
        let (router, trading, request_limit_metrics, database_metrics) =
            app_router(&config, content.clone())?;
        fee_audit = Some(trading);
        listeners.push(Listener {
            name: "Application",
            bind: config.bind,
            router: metrics::instrument(
                router,
                Arc::new(
                    Metrics::default()
                        .with_request_limits(request_limit_metrics)
                        .with_database(database_metrics),
                ),
                Origin::App,
            ),
        });
    }
    if let Some(content) = content {
        let (router, artwork_metrics, request_limit_metrics) =
            content_router(&config, content.clone());
        let observed = Arc::new(
            Metrics::default()
                .with_content(content)
                .with_artwork(artwork_metrics)
                .with_request_limits(request_limit_metrics),
        );
        listeners.push(Listener {
            name: "Artwork",
            bind: config.content_bind,
            router: metrics::instrument(router, observed, Origin::Artwork),
        });
    }
    let mut bound = Vec::with_capacity(listeners.len());
    for listener in listeners {
        let socket = tokio::net::TcpListener::bind(listener.bind).await?;
        bound.push((listener, socket));
    }
    match role {
        Role::All => println!(
            "seeded.art: {} (artwork: {})",
            config.app_origin, config.content_origin
        ),
        Role::App => println!("seeded.art app: {}", config.app_origin),
        Role::Content => println!("seeded.art artwork: {}", config.content_origin),
    }
    let proxy_delivery = config.trust_loopback_proxy;
    let (shutdown, stop) = tokio::sync::watch::channel(false);
    let audit_task = fee_audit.map(|trading| {
        let mut audit_stop = stop.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = audit_stop.changed() => break,
                    _ = interval.tick() => {
                        tokio::select! {
                            _ = audit_stop.changed() => break,
                            result = trading.reconcile_fees() => {
                                if result.is_err() { eprintln!("platform_fee_audit_unavailable"); }
                            }
                        }
                    }
                }
            }
        })
    });
    let mut tasks = tokio::task::JoinSet::new();
    let mut names = std::collections::HashMap::new();
    for (listener, socket) in bound {
        let stop = stop.clone();
        let task = tasks.spawn(async move {
            axum::serve(
                if proxy_delivery {
                    BoundedListener::new(socket).with_delivery_progress()
                } else {
                    BoundedListener::new(socket)
                },
                listener
                    .router
                    .into_make_service_with_connect_info::<seeded_archive::transport::ConnectionInfo>(),
            )
            .with_graceful_shutdown(wait_stop(stop))
            .await
        });
        names.insert(task.id(), listener.name);
    }
    drop(stop);
    let result = tokio::select! {
        signal = shutdown_signal() => signal.map_err(|error| error.to_string()),
        Some(stopped) = tasks.join_next_with_id() => {
            let (id, result) = match stopped {
                Ok((id, result)) => (id, Ok(result)),
                Err(error) => (error.id(), Err(error)),
            };
            let name = names.get(&id).copied().unwrap_or("Unknown");
            Err(format!("{name} listener stopped: {result:?}"))
        }
    };
    let _ = shutdown.send(true);
    if let Some(audit_task) = audit_task {
        let _ = audit_task.await;
    }
    let drain = async { while tasks.join_next().await.is_some() {} };
    if tokio::time::timeout(Duration::from_secs(45), drain)
        .await
        .is_err()
    {
        tasks.abort_all();
        eprintln!("Shutdown drain expired; remaining requests cancelled");
    }
    result.map_err(Into::into)
}

async fn wait_stop(mut stop: tokio::sync::watch::Receiver<bool>) {
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

async fn backup(config: &Config, destination: &Path) -> Result<(), Box<dyn Error>> {
    let cancelled = Arc::new(AtomicBool::new(false));
    let options = BackupOptions {
        timeout: config.backup_timeout,
        cancellation: Some(cancelled.clone()),
        ..BackupOptions::default()
    };
    let source = config.database.clone();
    let destination = destination.to_path_buf();
    let mut worker =
        tokio::task::spawn_blocking(move || create_backup(&source, &destination, options));
    let report = tokio::select! {
        result = &mut worker => result??,
        signal = shutdown_signal() => {
            cancelled.store(true, Ordering::Release);
            let result = worker.await?;
            signal?;
            result?
        }
    };
    println!("Backup: {} pages, {} bytes.", report.pages, report.bytes);
    Ok(())
}

fn prepare_database(database: &Path) -> std::io::Result<()> {
    if let Some(parent) = database
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
    {
        let mut builder = std::fs::DirBuilder::new();
        builder.recursive(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder.create(parent)?;
    }
    let mut file = std::fs::OpenOptions::new();
    file.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        file.mode(0o600);
    }
    match file.open(database) {
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
        Err(error) => Err(error),
    }
}
