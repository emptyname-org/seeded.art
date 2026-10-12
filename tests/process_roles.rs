#![cfg(unix)]
//! `serve --role`: split app and artwork processes, and the unchanged combined default.
use std::{
    collections::BTreeMap,
    net::TcpListener,
    path::{Path, PathBuf},
    process::Stdio,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};
use tokio::process::{Child, Command};

static NEXT: AtomicU64 = AtomicU64::new(0);
const PLAYBACK: &str = "/playback-status/KT1EfsNuqwLAWDd3o4pvfUx1CAh5GMdTrRvr/1?instance=0123456789abcdef0123456789abcdef";

struct Fixture {
    root: PathBuf,
    env: BTreeMap<String, String>,
    client: reqwest::Client,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}
impl Fixture {
    async fn imported() -> Self {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(format!(
            "tmp/roles-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&root).unwrap();
        let a = TcpListener::bind("127.0.0.1:0").unwrap();
        let b = TcpListener::bind("127.0.0.1:0").unwrap();
        let app = a.local_addr().unwrap().to_string();
        let art = b.local_addr().unwrap().to_string();
        let path = |name: &str| root.join(name).display().to_string();
        let env = [
            ("SEEDED_BIND", app.clone()),
            ("SEEDED_CONTENT_BIND", art.clone()),
            ("SEEDED_APP_ORIGIN", format!("http://{app}")),
            ("SEEDED_CONTENT_ORIGIN", format!("http://{art}")),
            // Release lookups get a local 404, independent of mainnet latency.
            ("SEEDED_TZKT_URL", format!("http://{app}/fixture-tzkt")),
            ("SEEDED_DATABASE", path("state/archive.sqlite")),
            ("SEEDED_TRADING_DATABASE", path("state/trading.sqlite")),
            ("SEEDED_CACHE", path("cache")),
            ("SEEDED_MIRROR", path("mirror")),
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
        let fixture = Self {
            root,
            env,
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(2))
                .build()
                .unwrap(),
        };
        assert!(
            fixture
                .command()
                .arg("import-demo")
                .status()
                .await
                .unwrap()
                .success()
        );
        fixture
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
    fn serve(&self, role: &str) -> Child {
        self.command()
            .args(["serve", "--role", role])
            .spawn()
            .unwrap()
    }
    fn app(&self, path: &str) -> String {
        format!("{}{path}", self.env["SEEDED_APP_ORIGIN"])
    }
    fn art(&self, path: &str) -> String {
        format!("{}{path}", self.env["SEEDED_CONTENT_ORIGIN"])
    }
    async fn status(&self, url: &str) -> Option<u16> {
        self.client
            .get(url)
            .send()
            .await
            .ok()
            .map(|response| response.status().as_u16())
    }
    async fn wait_for(&self, child: &mut Child, url: &str, expected: u16) {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if self.status(url).await == Some(expected) {
                    break;
                }
                assert!(
                    child.try_wait().unwrap().is_none(),
                    "server exited before {url} answered"
                );
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .unwrap();
    }
    fn refused(&self, bind: &str) -> bool {
        std::net::TcpStream::connect(&self.env[bind])
            .is_err_and(|error| error.kind() == std::io::ErrorKind::ConnectionRefused)
    }
}

async fn terminate(mut child: Child) {
    let pid = child.id().unwrap().to_string();
    assert!(
        Command::new("/bin/kill")
            .args(["-TERM", &pid])
            .status()
            .await
            .unwrap()
            .success()
    );
    let status = tokio::time::timeout(Duration::from_secs(16), child.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(status.success(), "SIGTERM must exit cleanly: {status}");
}

#[tokio::test]
async fn app_role_serves_pages_without_artwork_listener_or_cache() {
    let fixture = Fixture::imported().await;
    let mut app = fixture.serve("app");
    fixture
        .wait_for(&mut app, &fixture.app("/health/ready"), 204)
        .await;
    assert_eq!(fixture.status(&fixture.app("/projects")).await, Some(200));
    assert_eq!(fixture.status(&fixture.app(PLAYBACK)).await, Some(404));
    assert!(fixture.refused("SEEDED_CONTENT_BIND"));
    // The artwork process is the only writer of the content cache.
    assert!(!fixture.root.join("cache").exists());
    assert!(Path::new(&fixture.env["SEEDED_TRADING_DATABASE"]).is_file());
    terminate(app).await;
}

#[tokio::test]
async fn content_role_serves_artwork_from_a_read_only_archive_without_app_state() {
    use std::os::unix::fs::PermissionsExt;
    let fixture = Fixture::imported().await;
    let database = Path::new(&fixture.env["SEEDED_DATABASE"]);
    std::fs::set_permissions(database, std::fs::Permissions::from_mode(0o400)).unwrap();
    // Settings only the app reads are not validated by the artwork process.
    let mut content = fixture
        .command()
        .env("SEEDED_OAUTH_CLIENT_ID", "partial")
        .env("SEEDED_PLATFORM_FEE_BPS", "invalid")
        .args(["serve", "--role", "content"])
        .spawn()
        .unwrap();
    fixture
        .wait_for(&mut content, &fixture.art(PLAYBACK), 202)
        .await;
    assert_eq!(fixture.status(&fixture.art("/projects")).await, Some(404));
    assert!(fixture.refused("SEEDED_BIND"));
    assert!(fixture.root.join("cache").is_dir());
    assert!(!Path::new(&fixture.env["SEEDED_TRADING_DATABASE"]).exists());
    terminate(content).await;
}

#[tokio::test]
async fn app_keeps_serving_when_the_artwork_process_dies() {
    let fixture = Fixture::imported().await;
    let mut app = fixture
        .command()
        .env("SEEDED_ROLE", "app")
        .arg("serve")
        .spawn()
        .unwrap();
    let mut content = fixture
        .command()
        .env("SEEDED_ROLE", "content")
        .spawn()
        .unwrap();
    fixture
        .wait_for(&mut app, &fixture.app("/health/ready"), 204)
        .await;
    fixture
        .wait_for(&mut content, &fixture.art(PLAYBACK), 202)
        .await;
    content.start_kill().unwrap();
    content.wait().await.unwrap();
    assert!(fixture.refused("SEEDED_CONTENT_BIND"));
    for (path, status) in [
        ("/health/ready", 204),
        ("/projects", 200),
        ("/project/29437", 200),
    ] {
        assert_eq!(
            fixture.status(&fixture.app(path)).await,
            Some(status),
            "{path}"
        );
    }
    let mut restarted = fixture.serve("content");
    fixture
        .wait_for(&mut restarted, &fixture.art(PLAYBACK), 202)
        .await;
    terminate(restarted).await;
    terminate(app).await;
}

#[tokio::test]
async fn all_role_keeps_both_listeners_in_one_process() {
    let fixture = Fixture::imported().await;
    let mut all = fixture.serve("all");
    fixture
        .wait_for(&mut all, &fixture.app("/health/ready"), 204)
        .await;
    assert_eq!(fixture.status(&fixture.art(PLAYBACK)).await, Some(202));
    assert_eq!(fixture.status(&fixture.app("/projects")).await, Some(200));
    terminate(all).await;
}

#[tokio::test]
async fn invalid_roles_are_refused_before_serving_or_creating_data() {
    let fixture = Fixture::imported().await;
    let empty = fixture
        .root
        .join("state/empty.sqlite")
        .display()
        .to_string();
    for (args, role) in [
        (vec!["serve", "--role", "both"], None),
        (vec!["serve", "--role"], None),
        (vec!["serve", "--role", "app", "extra"], None),
        (vec!["--role", "app"], None),
        (vec!["import-demo", "--role", "app"], None),
        (vec!["serve"], Some("APP")),
        (vec!["dev"], Some("content")),
    ] {
        let mut command = fixture.command();
        command.env("SEEDED_DATABASE", &empty).args(&args);
        if let Some(role) = role {
            command.env("SEEDED_ROLE", role);
        }
        let status = tokio::time::timeout(Duration::from_secs(10), command.status())
            .await
            .unwrap()
            .unwrap();
        assert!(!status.success(), "{args:?} {role:?}");
        assert!(!Path::new(&empty).exists(), "{args:?} {role:?}");
    }
    assert!(fixture.refused("SEEDED_BIND") && fixture.refused("SEEDED_CONTENT_BIND"));
}
