#![cfg(unix)]
use std::{
    collections::BTreeMap,
    net::TcpListener,
    path::{Path, PathBuf},
    process::Stdio,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};
use tokio::process::Command;

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture {
    root: PathBuf,
    env: BTreeMap<String, String>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}
impl Fixture {
    fn new() -> Self {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(format!(
            "tmp/lifecycle-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&root).unwrap();
        let a = TcpListener::bind("127.0.0.1:0").unwrap();
        let b = TcpListener::bind("127.0.0.1:0").unwrap();
        let app = a.local_addr().unwrap().to_string();
        let art = b.local_addr().unwrap().to_string();
        let env = [
            ("SEEDED_BIND", app.clone()),
            ("SEEDED_CONTENT_BIND", art.clone()),
            ("SEEDED_APP_ORIGIN", format!("http://{app}")),
            ("SEEDED_CONTENT_ORIGIN", format!("http://{art}")),
            (
                "SEEDED_DATABASE",
                root.join("state/archive.sqlite").display().to_string(),
            ),
            (
                "SEEDED_TRADING_DATABASE",
                root.join("state/trading.sqlite").display().to_string(),
            ),
            ("SEEDED_CACHE", root.join("cache").display().to_string()),
            (
                "SEEDED_ASSETS",
                Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("assets")
                    .display()
                    .to_string(),
            ),
        ]
        .into_iter()
        .map(|(key, value)| (key.to_string(), value))
        .collect();
        Self { root, env }
    }
    fn command(&self) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_seeded_archive"));
        command
            .env_clear()
            .envs(&self.env)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        command
    }
}

#[tokio::test]
async fn local_server_handles_sigterm_backup_and_restart_without_reimporting() {
    let fixture = Fixture::new();
    assert!(
        fixture
            .command()
            .arg("import-demo")
            .status()
            .await
            .unwrap()
            .success()
    );
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(1))
        .build()
        .unwrap();
    let ready = format!("{}/health/ready", fixture.env["SEEDED_APP_ORIGIN"]);
    for pass in 0..2 {
        let mut child = fixture.command().spawn().unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if client
                    .get(&ready)
                    .send()
                    .await
                    .is_ok_and(|response| response.status() == 204)
                {
                    break;
                }
                assert!(
                    child.try_wait().unwrap().is_none(),
                    "server exited before readiness"
                );
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .unwrap();
        let database = Path::new(&fixture.env["SEEDED_DATABASE"]);
        let audit = Path::new(&fixture.env["SEEDED_TRADING_DATABASE"]);
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(audit).unwrap().permissions().mode() & 0o077,
                0
            );
            let conn = rusqlite::Connection::open(audit).unwrap();
            if pass == 0 {
                conn.execute("INSERT INTO outcomes(operation_hash,binding_id,evidence,recorded_at) VALUES('fixture','fixture','{}',unixepoch())",[]).unwrap();
            } else {
                assert_eq!(
                    conn.query_row("SELECT count(*) FROM outcomes", [], |r| r.get::<_, i64>(0))
                        .unwrap(),
                    1
                );
            }
        }
        if pass == 0 {
            let connection = rusqlite::Connection::open(database).unwrap();
            connection
                .execute("CREATE TABLE retained_marker(value INTEGER)", [])
                .unwrap();
            connection
                .execute("INSERT INTO retained_marker VALUES(123)", [])
                .unwrap();
            let backup = fixture.root.join("backup.sqlite");
            let status = tokio::time::timeout(
                Duration::from_secs(5),
                fixture.command().arg("backup").arg(&backup).status(),
            )
            .await
            .unwrap()
            .unwrap();
            assert!(status.success());
            let backup = rusqlite::Connection::open(backup).unwrap();
            assert_eq!(
                backup
                    .query_row("SELECT value FROM retained_marker", [], |row| row
                        .get::<_, i64>(0))
                    .unwrap(),
                123
            );
        } else {
            let connection = rusqlite::Connection::open(database).unwrap();
            assert_eq!(
                connection
                    .query_row("SELECT value FROM retained_marker", [], |row| row
                        .get::<_, i64>(0))
                    .unwrap(),
                123
            );
        }
        let pid = child.id().unwrap();
        let signal = Command::new("/bin/kill")
            .args(["-TERM", &pid.to_string()])
            .status()
            .await
            .unwrap();
        assert!(signal.success());
        let status = tokio::time::timeout(Duration::from_secs(16), child.wait())
            .await
            .unwrap()
            .unwrap();
        assert!(status.success(), "SIGTERM must exit cleanly: {status}");
    }
}

#[tokio::test]
async fn invalid_cli_and_origin_configuration_do_not_create_a_database() {
    let fixture = Fixture::new();
    assert!(
        !fixture
            .command()
            .arg("unknown-command")
            .status()
            .await
            .unwrap()
            .success()
    );
    assert!(
        !fixture
            .command()
            .env("SEEDED_CONTENT_ORIGIN", &fixture.env["SEEDED_APP_ORIGIN"])
            .status()
            .await
            .unwrap()
            .success()
    );
    assert!(!Path::new(&fixture.env["SEEDED_DATABASE"]).exists());
}

#[tokio::test]
async fn demo_import_refuses_to_replace_an_existing_archive() {
    let fixture = Fixture::new();
    assert!(
        fixture
            .command()
            .arg("import-demo")
            .status()
            .await
            .unwrap()
            .success()
    );
    let connection = rusqlite::Connection::open(&fixture.env["SEEDED_DATABASE"]).unwrap();
    connection
        .execute(
            "UPDATE projects SET title='Preserved archive title' WHERE id=29437",
            [],
        )
        .unwrap();
    assert!(
        !fixture
            .command()
            .arg("import-demo")
            .status()
            .await
            .unwrap()
            .success()
    );
    assert!(
        !fixture
            .command()
            .arg("import")
            .status()
            .await
            .unwrap()
            .success()
    );
    assert_eq!(
        connection
            .query_row("SELECT title FROM projects WHERE id=29437", [], |row| {
                row.get::<_, String>(0)
            })
            .unwrap(),
        "Preserved archive title"
    );
}

#[tokio::test]
async fn serving_missing_or_empty_database_fails_without_creating_demo_data() {
    let fixture = Fixture::new();
    assert!(
        !fixture
            .command()
            .arg("serve")
            .status()
            .await
            .unwrap()
            .success()
    );
    let database = Path::new(&fixture.env["SEEDED_DATABASE"]);
    assert!(!database.exists());
    std::fs::create_dir_all(database.parent().unwrap()).unwrap();
    drop(seeded_archive::store::Store::open(database).unwrap());
    assert!(!fixture.command().status().await.unwrap().success());
    let connection = rusqlite::Connection::open(database).unwrap();
    assert_eq!(
        connection
            .query_row("SELECT count(*) FROM projects", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn sigterm_drains_an_in_flight_artwork_request_before_clean_exit() {
    use std::sync::Arc;
    let mut fixture = Fixture::new();
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let handler_entered = entered.clone();
    let handler_release = release.clone();
    let upstream = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    fixture.env.insert(
        "SEEDED_TZKT_URL".into(),
        format!("http://{}", upstream.local_addr().unwrap()),
    );
    let mock = tokio::spawn(async move {
        axum::serve(
            upstream,
            axum::Router::new().fallback(move || {
                let entered = handler_entered.clone();
                let release = handler_release.clone();
                async move {
                    entered.notify_one();
                    release.notified().await;
                    axum::http::StatusCode::NO_CONTENT
                }
            }),
        )
        .await
        .unwrap();
    });
    assert!(
        fixture
            .command()
            .arg("import-demo")
            .status()
            .await
            .unwrap()
            .success()
    );
    let content_root = {
        let database = rusqlite::Connection::open(&fixture.env["SEEDED_DATABASE"]).unwrap();
        database
            .query_row(
                "SELECT r.cid FROM content_roots r JOIN projects p ON p.id=r.project_id WHERE r.scheme='onchfs' AND p.hidden=0 LIMIT 1",
                [],
                |row| row.get::<_, String>(0),
            )
            .unwrap()
    };
    let mut child = fixture.command().spawn().unwrap();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(8))
        .build()
        .unwrap();
    let ready = format!("{}/health/ready", fixture.env["SEEDED_APP_ORIGIN"]);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if client
                .get(&ready)
                .send()
                .await
                .is_ok_and(|r| r.status() == 204)
            {
                break;
            }
            assert!(child.try_wait().unwrap().is_none());
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let url = format!(
        "{}/content/onchfs/{content_root}/",
        fixture.env["SEEDED_CONTENT_ORIGIN"]
    );
    let request = tokio::spawn(async move { client.get(url).send().await });
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    assert!(
        Command::new("/bin/kill")
            .args(["-TERM", &child.id().unwrap().to_string()])
            .status()
            .await
            .unwrap()
            .success()
    );
    release.notify_one();
    assert_eq!(request.await.unwrap().unwrap().status(), 404);
    let status = tokio::time::timeout(Duration::from_secs(5), child.wait())
        .await
        .unwrap()
        .unwrap();
    mock.abort();
    let _ = mock.await;
    assert!(status.success());
}

#[tokio::test]
async fn unavailable_fee_database_keeps_the_archive_ready() {
    let fixture = Fixture::new();
    assert!(
        fixture
            .command()
            .arg("import-demo")
            .status()
            .await
            .unwrap()
            .success()
    );
    std::fs::create_dir(&fixture.env["SEEDED_TRADING_DATABASE"]).unwrap();
    let mut child = fixture
        .command()
        .env(
            "SEEDED_PLATFORM_FEE_ADDRESS",
            "tz1P719ymGCtNwavZKh2LmhMs53dGsEMzjnm",
        )
        .spawn()
        .unwrap();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(1))
        .build()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if client
                .get(format!("{}/health/ready", fixture.env["SEEDED_APP_ORIGIN"]))
                .send()
                .await
                .is_ok_and(|response| response.status() == 204)
            {
                break;
            }
            assert!(
                child.try_wait().unwrap().is_none(),
                "audit failure must not stop the archive"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    assert!(
        client
            .get(format!("{}/projects", fixture.env["SEEDED_APP_ORIGIN"]))
            .send()
            .await
            .unwrap()
            .status()
            .is_success()
    );
    assert!(
        Command::new("/bin/kill")
            .args(["-TERM", &child.id().unwrap().to_string()])
            .status()
            .await
            .unwrap()
            .success()
    );
    assert!(
        tokio::time::timeout(Duration::from_secs(16), child.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
}
