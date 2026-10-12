"""Local template/runtime checks. Never install units or alter host services."""

import contextlib
import http.client
import http.server
import json
import os
import re
import shutil
import signal
import socket
import sqlite3
import ssl
import subprocess
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import configure
import measure_state

ROOT = Path(__file__).resolve().parent.parent
# Parser fixtures only: these values are not hardware recommendations.
VALID = {
    "APP_HOST": "app.invalid",
    "CONTENT_HOST": "artwork.invalid",
    "APP_MEMORY_HIGH": "128M",
    "APP_MEMORY_MAX": "256M",
    "APP_CPU_QUOTA": "100%",
    "APP_TASKS_MAX": "64",
    "APP_OPEN_FILES": "4096",
    "PROXY_MEMORY_MAX": "1536M",
}
PROJECT_CADDY = ROOT / "tmp/caddy_tool/caddy"
CADDY = os.environ.get("SEEDED_TEST_CADDY") or (
    str(PROJECT_CADDY)
    if PROJECT_CADDY.is_file()
    else (shutil.which("caddy") or "caddy")
)


class DeploymentTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        caddy_available = Path(CADDY).is_file() or shutil.which(CADDY)
        if os.environ.get("SEEDED_REQUIRE_NATIVE_PROXY") == "1" and not caddy_available:
            raise RuntimeError(
                "Native proxy gate requires standard Caddy >=2.11.7 and OpenSSL; set SEEDED_TEST_CADDY or install the project-local test tool."
            )
        if os.environ.get("SEEDED_REQUIRE_NATIVE_PROXY") == "1" and not shutil.which(
            "openssl"
        ):
            raise RuntimeError("Native proxy gate requires OpenSSL")
        if caddy_available:
            version = subprocess.run(
                [CADDY, "version"],
                capture_output=True,
                text=True,
                timeout=10,
                check=False,
            )
            if version.returncode == 0:
                match = re.search(r"v?(\d+)\.(\d+)\.(\d+)", version.stdout)
                if not match or tuple(map(int, match.groups())) < (2, 11, 7):
                    raise RuntimeError("Native proxy tests require Caddy >=2.11.7")

    def setUp(self):
        (ROOT / "tmp/caddy-review").mkdir(parents=True, exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(
            prefix="deploy_check_", dir=ROOT / "tmp/caddy-review"
        )
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)

    def settings(self, values):
        path = self.directory / "settings.json"
        path.write_text(json.dumps(values))
        return configure.settings(path)

    def test_every_app_api_route_passes_the_proxy(self):
        # A route the proxy does not list answers 404 in production only.
        root = Path(__file__).resolve().parent.parent
        routes = set()
        for source in (root / "src" / "web").glob("*.rs"):
            routes.update(re.findall(r'\.route\("(/api/[^"]+)"', source.read_text()))
        self.assertIn("/api/market/submitted", routes)
        caddy = (root / "deploy" / "Caddyfile.in").read_text()
        allowed = set(re.search(r"not path ([^\n]+)", caddy).group(1).split())
        self.assertEqual(routes - allowed, set())

    def test_placeholders_and_unsafe_origins_are_rejected(self):
        with self.assertRaises(ValueError):
            configure.settings(ROOT / "deploy/settings.example.json")
        for value in [
            "app.invalid",
            "https://artwork.invalid",
            "bad.invalid;",
            "bad.invalid\n",
            "127.0.0.1",
        ]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.settings({**VALID, "CONTENT_HOST": value})

    def test_content_host_redirects_are_optional_validated_hosts(self):
        forum = {
            **VALID,
            "FORUM_HOST": "forum.invalid",
            "FORUM_ROOT": "/var/www/forum/public",
            "FORUM_PHP_SOCKET": "/run/php/php8.4-fpm.sock",
        }
        for value in (
            "BAD.invalid",
            "127.0.0.1",
            "nodot",
            "https://old.invalid",
            "old.invalid;",
            "*.old.invalid",
            "old.invalid/path",
            "old.invalid old.invalid",
            "old.invalid app.invalid",
            "artwork.invalid",
            "forum.invalid",
        ):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.settings({**forum, "CONTENT_HOST_REDIRECTS": value})
        absent = self.settings(VALID)
        self.assertEqual(absent["CONTENT_HOST_REDIRECTS"], "")
        configure.render(absent, self.directory / "absent")
        configure.render(
            self.settings({**VALID, "CONTENT_HOST_REDIRECTS": ""}),
            self.directory / "empty",
        )
        caddy = (self.directory / "absent/Caddyfile").read_text()
        self.assertEqual(caddy, (self.directory / "empty/Caddyfile").read_text())
        self.assertNotIn(" 302\n", caddy)
        self.assertTrue(caddy.endswith("\t}\n}\n"))
        configure.render(
            self.settings(
                {**VALID, "CONTENT_HOST_REDIRECTS": "old.invalid www.artwork.invalid"}
            ),
            self.directory / "redirects",
        )
        caddy = (self.directory / "redirects/Caddyfile").read_text()
        self.assertIn(
            "http://app.invalid, http://artwork.invalid, "
            "http://old.invalid, http://www.artwork.invalid {",
            caddy,
        )
        for host in ("old.invalid", "www.artwork.invalid"):
            self.assertIn(
                f"\n\n{host} {{\n\timport site_policy\n"
                "\tredir https://artwork.invalid{uri} 302\n}",
                caddy,
            )
        self.assertTrue(caddy.endswith("302\n}\n"))

    def test_resource_budgets_are_finite_and_high_is_below_maximum(self):
        for key, value in [
            ("APP_MEMORY_MAX", "infinity"),
            ("APP_MEMORY_HIGH", "256M"),
            ("APP_CPU_QUOTA", "0%"),
            ("APP_TASKS_MAX", "-1"),
            ("APP_OPEN_FILES", "4095"),
            ("APP_OPEN_FILES", "not-a-number"),
            ("PROXY_MEMORY_MAX", "256M"),
            ("PROXY_MEMORY_MAX", "1279M"),
        ]:
            with self.subTest(key=key), self.assertRaises(ValueError):
                self.settings({**VALID, key: value})

    def test_proxy_open_files_defaults_renders_and_requires_matching_memory(self):
        values = self.settings(VALID)
        self.assertEqual(values["PROXY_OPEN_FILES"], "16384")
        self.assertEqual(
            self.settings({**VALID, "PROXY_MEMORY_MAX": "1280M"})["PROXY_OPEN_FILES"],
            "16384",
        )
        rendered = self.directory / "proxy-default"
        configure.render(values, rendered)
        self.assertIn(
            "LimitNOFILE=16384", (rendered / "seeded-proxy.service").read_text()
        )

        for open_files, memory in (("16385", "1281M"), ("65536", "4352M")):
            with self.subTest(open_files=open_files):
                self.assertEqual(
                    self.settings(
                        {
                            **VALID,
                            "PROXY_OPEN_FILES": open_files,
                            "PROXY_MEMORY_MAX": memory,
                        }
                    )["PROXY_OPEN_FILES"],
                    open_files,
                )
        rendered = self.directory / "proxy-override"
        configure.render(
            self.settings(
                {
                    **VALID,
                    "PROXY_OPEN_FILES": "65536",
                    "PROXY_MEMORY_MAX": "4352M",
                }
            ),
            rendered,
        )
        proxy = (rendered / "seeded-proxy.service").read_text()
        self.assertIn("LimitNOFILE=65536", proxy)
        self.assertIn("MemoryMax=4352M", proxy)

        for value in ("16383", "65537", "0", "1.5", "many", "16384\nUser=root"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.settings({**VALID, "PROXY_OPEN_FILES": value})
        for memory in ("1G", "4351M"):
            with self.subTest(memory=memory), self.assertRaises(ValueError):
                self.settings(
                    {
                        **VALID,
                        "PROXY_OPEN_FILES": "65536",
                        "PROXY_MEMORY_MAX": memory,
                    }
                )

    def test_app_open_files_accepts_the_4096_minimum(self):
        self.assertEqual(self.settings(VALID)["APP_OPEN_FILES"], "4096")

    def test_mirror_defaults_and_safe_overrides(self):
        values = self.settings(VALID)
        self.assertEqual(values["MIRROR"], "/var/lib/seeded/mirror")
        self.assertEqual(values["MIRROR_BYTES"], "53687091200")
        overridden = self.settings(
            {**VALID, "MIRROR": "/srv/seeded/archive", "MIRROR_BYTES": "123456"}
        )
        self.assertEqual(overridden["MIRROR"], "/srv/seeded/archive")
        self.assertEqual(overridden["MIRROR_BYTES"], "123456")
        self.assertEqual(self.settings({**VALID, "MIRROR": ""})["MIRROR"], "")

    def test_mirror_rejects_unsafe_paths_and_caps(self):
        for value in (
            "relative/path",
            "/var/lib/seeded/../other",
            "/var/lib/seeded//mirror",
            "/var/lib/seeded/mirror;touch",
            "/var/lib/seeded/__APP_HOST__",
            "/var/lib/seeded/mirror\nINJECTED=yes",
        ):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.settings({**VALID, "MIRROR": value})
        for value in ("0", "-1", "1G", "1125899906842625", "1\nBAD=1"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.settings({**VALID, "MIRROR_BYTES": value})

    def test_mirror_values_are_rendered_to_the_private_environment_file(self):
        rendered = self.directory / "mirror-rendered"
        configure.render(
            self.settings(
                {**VALID, "MIRROR": "/srv/seeded/mirror", "MIRROR_BYTES": "123456"}
            ),
            rendered,
        )
        env = (rendered / "seeded.env").read_text()
        self.assertIn("SEEDED_MIRROR=/srv/seeded/mirror", env)
        self.assertIn("SEEDED_MIRROR_BYTES=123456", env)
        self.assertEqual((rendered / "seeded.env").stat().st_mode & 0o777, 0o600)

    def test_content_concurrency_defaults_renders_and_validates_overrides(self):
        values = self.settings(VALID)
        self.assertEqual(values["CONTENT_CONCURRENCY"], "4")
        rendered = self.directory / "content-concurrency-rendered"
        configure.render(values, rendered)
        env = (rendered / "seeded.env").read_text()
        self.assertIn("SEEDED_CONTENT_CONCURRENCY=4", env)

        values = self.settings({**VALID, "CONTENT_CONCURRENCY": "16"})
        self.assertEqual(values["CONTENT_CONCURRENCY"], "16")
        rendered = self.directory / "content-concurrency-override"
        configure.render(values, rendered)
        self.assertIn(
            "SEEDED_CONTENT_CONCURRENCY=16",
            (rendered / "seeded.env").read_text(),
        )
        for value in ("0", "17", "-1", "1.5", "four", "4\nSEEDED_BIND=0.0.0.0:80"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.settings({**VALID, "CONTENT_CONCURRENCY": value})

    def test_max_object_bytes_defaults_renders_and_validates_overrides(self):
        values = self.settings(VALID)
        self.assertEqual(values["MAX_OBJECT_BYTES"], str(32 * 1024**2))
        values = self.settings({**VALID, "MAX_OBJECT_BYTES": str(96 * 1024**2)})
        rendered = self.directory / "max-object-rendered"
        configure.render(values, rendered)
        self.assertIn(
            f"SEEDED_MAX_OBJECT_BYTES={96 * 1024**2}",
            (rendered / "seeded.env").read_text(),
        )
        for value in ("0", str(256 * 1024**2 + 1), "-1", "1.5", "big", "1\nX=1"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.settings({**VALID, "MAX_OBJECT_BYTES": value})

    def test_optional_job_budgets_default_safely_and_validate_overrides(self):
        values = self.settings(VALID)
        self.assertEqual(values["JOB_MEMORY_MAX"], VALID["APP_MEMORY_MAX"])
        self.assertEqual(values["JOB_CPU_QUOTA"], VALID["APP_CPU_QUOTA"])
        self.assertEqual(values["JOB_TASKS_MAX"], VALID["APP_TASKS_MAX"])
        for key, value in [
            ("JOB_MEMORY_MAX", "infinity"),
            ("JOB_CPU_QUOTA", "0%"),
            ("JOB_TASKS_MAX", "0"),
        ]:
            with self.subTest(key=key), self.assertRaises(ValueError):
                self.settings({**VALID, key: value})
        values = self.settings({**VALID, "JOB_MEMORY_MAX": "512M"})
        self.assertEqual(values["JOB_MEMORY_MAX"], "512M")

    def test_content_budget_defaults_to_app_budget_and_validates_overrides(self):
        values = self.settings(VALID)
        for key, fallback in configure.CONTENT_BUDGET.items():
            self.assertEqual(values[key], VALID[fallback])
        for key, value in [
            ("CONTENT_MEMORY_MAX", "infinity"),
            ("CONTENT_MEMORY_HIGH", "256M"),
            ("CONTENT_MEMORY_MAX", "128M"),
            ("CONTENT_CPU_QUOTA", "0%"),
            ("CONTENT_TASKS_MAX", "0"),
            ("CONTENT_OPEN_FILES", "4095"),
            ("CONTENT_OPEN_FILES", "4096\nUser=root"),
        ]:
            with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                self.settings({**VALID, key: value})
        rendered = self.directory / "content-budget"
        configure.render(
            self.settings(
                {
                    **VALID,
                    "CONTENT_MEMORY_HIGH": "768M",
                    "CONTENT_MEMORY_MAX": "1G",
                    "CONTENT_CPU_QUOTA": "150%",
                    "CONTENT_TASKS_MAX": "128",
                    "CONTENT_OPEN_FILES": "8192",
                }
            ),
            rendered,
        )
        content = (rendered / "seeded-content.service").read_text()
        for line in (
            "MemoryHigh=768M",
            "MemoryMax=1G",
            "CPUQuota=150%",
            "TasksMax=128",
            "LimitNOFILE=8192",
        ):
            self.assertIn(line, content)
        for unit in ("seeded-app.service", "seeded.service"):
            text = (rendered / unit).read_text()
            for line in (
                "MemoryHigh=128M",
                "MemoryMax=256M",
                "CPUQuota=100%",
                "TasksMax=64",
                "LimitNOFILE=4096",
            ):
                self.assertIn(line, text)

    def test_split_units_keep_combined_hardening_and_exclude_the_combined_unit(self):
        rendered = self.directory / "split"
        configure.render(self.settings(VALID), rendered)
        combined = (rendered / "seeded.service").read_text().splitlines()
        differing = ("Description=", "After=", "ExecStart=")
        for role in ("app", "content"):
            unit = (rendered / f"seeded-{role}.service").read_text()
            lines = unit.splitlines()
            with self.subTest(role=role):
                self.assertIn(
                    f"ExecStart=/opt/seeded/current/seeded_archive serve --role {role}",
                    lines,
                )
                self.assertIn("Conflicts=seeded.service", lines)
                self.assertIn("After=network-online.target seeded.service", lines)
                self.assertNotRegex(unit, r"(?m)^(Wants|Requires|BindsTo)=.*seeded")
                for line in combined:
                    if not line.startswith(differing):
                        self.assertIn(line, lines)
        # Starting the proxy must not pull in the combined unit over the split ones.
        proxy = (rendered / "seeded-proxy.service").read_text()
        self.assertNotRegex(proxy, r"(?m)^(Wants|Requires|BindsTo)=.*seeded")
        self.assertIn(
            "After=network-online.target seeded.service seeded-app.service "
            "seeded-content.service",
            proxy,
        )
        caddy = (rendered / "Caddyfile").read_text()
        self.assertIn("127.0.0.1:3918", caddy)
        self.assertIn("127.0.0.1:3919", caddy)
        deployment = (ROOT / "docs/deployment.md").read_text()
        self.assertIn("sudo systemctl stop seeded.service", deployment)
        self.assertIn(
            "sudo systemctl start seeded-app.service seeded-content.service",
            deployment,
        )

    def test_optional_acme_email_is_omitted_or_rendered_and_validated(self):
        self.assertEqual(self.settings(VALID)["ACME_EMAIL"], "")
        rendered = self.directory / "email-rendered"
        configure.render(
            self.settings({**VALID, "ACME_EMAIL": "ops@example.invalid"}), rendered
        )
        caddy = (rendered / "Caddyfile").read_text()
        self.assertIn("email ops@example.invalid", caddy)
        for email in (
            "ops@example.invalid\nemail attacker.invalid",
            "ops@@example.invalid",
        ):
            with self.subTest(email=email), self.assertRaises(ValueError):
                self.settings({**VALID, "ACME_EMAIL": email})

    def test_optional_jobs_share_runner_and_daily_jobs_retry_transient_failures(self):
        rendered = self.directory / "rendered"
        configure.render(self.settings(VALID), rendered)
        self.assertEqual(len(list(rendered.iterdir())), len(configure.TEMPLATES))
        for job, start_limit in [
            ("sync", "9min"),
            ("verify", "31min"),
            ("backup", "42min"),
            ("profiles", "41min"),
        ]:
            service = (rendered / f"seeded-{job}.service").read_text()
            timer = (rendered / f"seeded-{job}.timer").read_text()
            self.assertIn(f"/opt/seeded/current/deploy/maintain.py {job}", service)
            self.assertIn(f"TimeoutStartSec={start_limit}", service)
            self.assertIn("TimeoutStopSec=25s", service)
            self.assertIn("KillMode=control-group", service)
            self.assertIn("User=seeded", service)
            self.assertIn("NoNewPrivileges=true", service)
            self.assertIn("MemoryMax=256M", service)
            self.assertIn("WantedBy=timers.target", timer)
            self.assertIn("Persistent=true", timer)
            if job != "sync":
                self.assertIn("Restart=on-failure", service)
                self.assertIn("RestartPreventExitStatus=1 64 130", service)
                self.assertIn("RestartSec=5min", service)
                self.assertIn("StartLimitBurst=4", service)
                self.assertIn("StartLimitIntervalSec=6h", service)
        self.assertIn(
            "RestartMode=direct",
            (rendered / "seeded-profiles.service").read_text(),
        )
        self.assertIn(
            "RestartMode=direct",
            (rendered / "seeded-verify.service").read_text(),
        )
        # Trades show within 10 minutes; a busy or partial run is retried by the next one.
        self.assertIn(
            "OnCalendar=*-*-* *:05/10:00", (rendered / "seeded-sync.timer").read_text()
        )
        self.assertIn(
            "SuccessExitStatus=75", (rendered / "seeded-sync.service").read_text()
        )
        # A confirmed trade's request file starts the sync; the sync clears it first.
        self.assertIn(
            "PathExists=/var/lib/seeded/sync.request",
            (rendered / "seeded-sync.path").read_text(),
        )
        self.assertIn(
            "ExecStartPre=/usr/bin/rm -f /var/lib/seeded/sync.request",
            (rendered / "seeded-sync.service").read_text(),
        )
        # Articles and verification change rarely since fxhash closed: weekly.
        self.assertIn(
            "OnCalendar=Mon *-*-* 00:50:00",
            (rendered / "seeded-articles.timer").read_text(),
        )
        self.assertIn(
            "OnCalendar=Mon *-*-* 02:00:00",
            (rendered / "seeded-verify.timer").read_text(),
        )
        self.assertIn(
            "PrivateNetwork=true", (rendered / "seeded-backup.service").read_text()
        )

    def test_daily_profile_verify_backup_ordering_keeps_backup_independent(self):
        rendered = self.directory / "maintenance-chain"
        configure.render(
            self.settings(
                {
                    **VALID,
                    "FORUM_HOST": "forum.invalid",
                    "FORUM_ROOT": "/var/www/forum/public",
                    "FORUM_PHP_SOCKET": "/run/php/php8.4-fpm.sock",
                }
            ),
            rendered,
        )
        backup_timer = (rendered / "seeded-backup.timer").read_text()
        verify = (rendered / "seeded-verify.service").read_text()
        backup = (rendered / "seeded-backup.service").read_text()
        self.assertIn("OnCalendar=*-*-* 03:00:00", backup_timer)
        self.assertIn("Requires=seeded-profiles.service", verify)
        self.assertIn("After=seeded-profiles.service", verify)
        self.assertIn("After=seeded-profiles.service seeded-verify.service", backup)
        for dependency in ("seeded-profiles.service", "seeded-verify.service"):
            self.assertNotIn(f"Requires={dependency}", backup)
            self.assertNotIn(f"Wants={dependency}", backup)
        forum_backup = (rendered / "seeded-forum-backup.service").read_text()
        self.assertIn("StateDirectory=seeded-forum-backup", forum_backup)
        self.assertIn("StateDirectoryMode=0700", forum_backup)
        self.assertIn("WorkingDirectory=/var/lib/seeded-forum-backup", forum_backup)
        self.assertNotIn("RuntimeDirectory=", forum_backup)
        self.assertNotIn("BindPaths=", forum_backup)
        deployment = (ROOT / "docs/deployment.md").read_text()
        self.assertIn(
            "Enable `seeded-verify.timer` and `seeded-backup.timer` independently",
            deployment,
        )

    def test_render_refuses_overwrite_and_keeps_two_listener_boundaries(self):
        destination = self.directory / "rendered"
        configure.render(self.settings(VALID), destination)
        with self.assertRaises(FileExistsError):
            configure.render(self.settings(VALID), destination)
        caddy = (destination / "Caddyfile").read_text()
        self.assertIn("127.0.0.1:3918", caddy)
        self.assertIn("127.0.0.1:3919", caddy)
        self.assertIn("admin off", caddy)
        self.assertIn("persist_config off", caddy)
        self.assertNotIn("response_buffers", caddy)
        self.assertIn("dir https://acme-v02.api.letsencrypt.org/directory", caddy)
        self.assertNotIn("__ACME_EMAIL_DIRECTIVE__", caddy)
        self.assertNotRegex(caddy, r"(?m)^\s*email\s+")
        self.assertNotIn("__APP_", caddy)
        self.assertEqual((destination / "seeded.env").stat().st_mode & 0o777, 0o600)
        self.assertIn(
            "SEEDED_TRADING_DATABASE=/var/lib/seeded/trading.sqlite",
            (destination / "seeded.env").read_text(),
        )

    def test_streaming_and_service_hardening_are_bounded(self):
        destination = self.directory / "rendered"
        configure.render(self.settings(VALID), destination)
        caddy = (destination / "Caddyfile").read_text()
        for directive in (
            "max_size",
            "write_idle",
            "read_header",
            "protocols h1 h2",
        ):
            self.assertIn(directive, caddy)
        self.assertIn("keepalive off", caddy)
        self.assertIn("dir https://acme-v02.api.letsencrypt.org/directory", caddy)
        self.assertNotRegex(caddy, r"(?m)^\s*email\s+")
        self.assertNotIn("unhealthy_request_count", caddy)
        self.assertNotIn("response_buffers", caddy)
        logging = self.adapted().get("logging", {}).get("logs", {}).get("default", {})
        self.assertEqual(logging.get("encoder", {}).get("format"), "filter")
        self.assertEqual(
            logging.get("encoder", {})
            .get("fields", {})
            .get("request", {})
            .get("filter"),
            "delete",
        )
        self.assertIn("http.log.error", logging.get("exclude", []))
        error_log = self.adapted()["logging"]["logs"]["upstream_errors"]
        self.assertEqual(error_log["include"], ["http.log.error"])
        self.assertEqual(error_log["level"], "DEBUG")
        self.assertEqual(error_log["encoder"]["fields"]["request"]["filter"], "delete")
        for unit in (
            "seeded.service",
            "seeded-app.service",
            "seeded-content.service",
            "seeded-proxy.service",
        ):
            text = (destination / unit).read_text()
            for directive in (
                "RestrictNamespaces=true",
                "SystemCallFilter=@system-service",
                "ProtectClock=true",
                "ProtectHostname=true",
                "ProtectProc=invisible",
                "ProcSubset=pid",
                "RemoveIPC=true",
            ):
                self.assertIn(directive, text)
        proxy = (destination / "seeded-proxy.service").read_text()
        self.assertIn("/usr/bin/caddy run", proxy)
        self.assertIn("MemoryMax=1536M", proxy)
        self.assertIn("LimitNOFILE=16384", proxy)
        self.assertNotIn("StartLimitBurst=", proxy)
        self.assertIn("StartLimitIntervalSec=0", proxy)

    def test_measurement_cannot_escape_project_or_follow_directory_links(self):
        with self.assertRaises(ValueError):
            measure_state.local_path("../other-project")
        real = self.directory / "real"
        real.mkdir()
        (real / "data").write_bytes(b"1234")
        self.assertEqual(measure_state.files(real)["logical_bytes"], 4)
        (real / "link").symlink_to(real / "data")
        with self.assertRaises(ValueError):
            measure_state.files(real)

    def test_measurement_bounds_empty_directories_and_pending_entries(self):
        tree = self.directory / "tree"
        tree.mkdir()
        for number in range(5):
            (tree / str(number)).mkdir()
        with (
            patch.object(measure_state, "MAX_ENTRIES", 4),
            self.assertRaises(ValueError),
        ):
            measure_state.files(tree)

    def test_measurement_counts_secondary_databases_without_creating_missing_files(
        self,
    ):
        missing = self.directory / "missing.sqlite"
        self.assertEqual(
            measure_state.database(missing, "sales"), {"status": "missing"}
        )
        self.assertFalse(missing.exists())
        for kind in ("sales", "articles"):
            path = self.directory / f"{kind}.sqlite"
            with contextlib.closing(sqlite3.connect(path)) as connection, connection:
                for table in measure_state.TABLES[kind]:
                    connection.execute(f"CREATE TABLE {table}(id INTEGER)")
                    connection.execute(f"INSERT INTO {table} VALUES (1)")
            report = measure_state.database(path, kind)
            self.assertEqual(
                report["indexed_rows"], dict.fromkeys(measure_state.TABLES[kind], 1)
            )
            self.assertEqual(report["main"]["logical_bytes"], path.stat().st_size)
            link = self.directory / f"{kind}-link.sqlite"
            link.symlink_to(path)
            with self.assertRaises(ValueError):
                measure_state.database(link, kind)

    def adapted(self):
        rendered = self.directory / "rendered"
        if not rendered.exists():
            configure.render(self.settings(VALID), rendered)
        environment = dict(
            os.environ,
            XDG_DATA_HOME=str(self.directory / "data"),
            XDG_CONFIG_HOME=str(self.directory / "config"),
        )
        result = subprocess.run(
            [
                CADDY,
                "adapt",
                "--config",
                str(rendered / "Caddyfile"),
                "--adapter",
                "caddyfile",
            ],
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
            env=environment,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        return json.loads(result.stdout)

    @contextlib.contextmanager
    def running_proxy(
        self, upstream, *, short_deadline=False, tls_files=None, redirect_only=False
    ):
        config = self.adapted()
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        servers = config["apps"]["http"]["servers"]

        def contains_proxy(value):
            if isinstance(value, dict):
                return value.get("handler") == "reverse_proxy" or any(
                    contains_proxy(child) for child in value.values()
                )
            if isinstance(value, list):
                return any(contains_proxy(child) for child in value)
            return False

        servers = {
            key: value
            for key, value in servers.items()
            if contains_proxy(value) != redirect_only
        }
        self.assertEqual(len(servers), 1, "one selected listener server")
        config["apps"]["http"]["servers"] = servers
        for server in servers.values():
            server["listen"] = [f"127.0.0.1:{port}"]
            server["automatic_https"] = {"disable": True}
            if tls_files:
                server["tls_connection_policies"] = [{}]
            else:
                server.pop("tls_connection_policies", None)
            if short_deadline:
                server["write_timeout"] = 3_000_000_000
                server["write_idle_timeout"] = 2_000_000_000
        config["apps"].pop("tls", None)
        if tls_files:
            config["apps"]["tls"] = {
                "certificates": {
                    "load_files": [
                        {"certificate": str(tls_files[0]), "key": str(tls_files[1])}
                    ]
                }
            }
        config["admin"] = {"disabled": True}

        def visit(value):
            if isinstance(value, dict):
                if value.get("dial") in ("127.0.0.1:3918", "127.0.0.1:3919"):
                    value["dial"] = f"127.0.0.1:{upstream.server_port}"
                for child in value.values():
                    visit(child)
            elif isinstance(value, list):
                for child in value:
                    visit(child)

        visit(config)
        path = self.directory / (
            "short-runtime.json" if short_deadline else "runtime.json"
        )
        path.write_text(json.dumps(config))
        environment = dict(
            os.environ,
            XDG_DATA_HOME=str(self.directory / "data"),
            XDG_CONFIG_HOME=str(self.directory / "config"),
        )
        validated = subprocess.run(
            [CADDY, "validate", "--config", str(path)],
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
            env=environment,
        )
        self.assertEqual(validated.returncode, 0, validated.stderr)
        log_path = path.with_suffix(".log")
        with log_path.open("w") as log:
            process = subprocess.Popen(
                [CADDY, "run", "--config", str(path)],
                stdout=log,
                stderr=log,
                env=environment,
            )
            try:
                deadline = time.monotonic() + 5
                while True:
                    self.assertIsNone(process.poll(), log_path.read_text())
                    try:
                        with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                            break
                    except OSError:
                        self.assertLess(
                            time.monotonic(), deadline, log_path.read_text()
                        )
                        time.sleep(0.02)
                yield port, process
            finally:
                process.send_signal(signal.SIGTERM)
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)

    @staticmethod
    def connect_proxy(port, host, path, method="GET", body=b"", chunked=False):
        sock = socket.socket()
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024)
        sock.settimeout(5)
        sock.connect(("127.0.0.1", port))
        length = (
            "Transfer-Encoding: chunked" if chunked else f"Content-Length: {len(body)}"
        )
        if chunked:
            body = f"{len(body):x}\r\n".encode() + body + b"\r\n0\r\n\r\n"
        sock.sendall(
            (
                f"{method} {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n"
                f"{length}\r\nContent-Type: application/json\r\n"
                "Cookie: seeded_session=private-fixture\r\nAuthorization: Bearer private-fixture\r\n"
                "X-Forwarded-For: 198.51.100.99\r\nForwarded: for=198.51.100.88\r\n"
                "Origin: https://app.invalid\r\nX-CSRF-Token: fixture\r\n\r\n"
            ).encode()
            + body
        )
        return sock

    def request_proxy(self, port, host, path, method="GET", body=b"", chunked=False):
        with self.connect_proxy(port, host, path, method, body, chunked) as sock:
            response = http.client.HTTPResponse(sock, method=method)
            response.begin()
            result = response.status, dict(response.getheaders()), response.read()
            response.close()
            return result

    def upstream(self, body_mib=12):
        records = {
            "completed": [],
            "requests": [],
            "posts": [],
            "received_posts": [],
            "peak_active": 0,
            "active": 0,
        }
        condition = threading.Condition()
        slots = threading.BoundedSemaphore(64)

        class Upstream(http.server.BaseHTTPRequestHandler):
            def log_message(self, *_args):
                pass

            def do_POST(self):
                # Model the app's complete-body JSON extractor: no mutation until
                # framing, size and parsing have all succeeded.
                self.connection.settimeout(3)
                with condition:
                    records["received_posts"].append(self.path)
                body = bytearray()
                try:
                    if self.headers.get("Transfer-Encoding") == "chunked":
                        while True:
                            line = self.rfile.readline(128)
                            if not line.endswith(b"\r\n"):
                                raise ValueError("incomplete chunk header")
                            size = int(line.strip().split(b";", 1)[0], 16)
                            if size == 0:
                                if self.rfile.read(2) != b"\r\n":
                                    raise ValueError("incomplete body")
                                break
                            if len(body) + size > 16 * 1024:
                                raise OverflowError
                            chunk = self.rfile.read(size)
                            if len(chunk) != size or self.rfile.read(2) != b"\r\n":
                                raise ValueError("incomplete chunk")
                            body.extend(chunk)
                    else:
                        size = int(self.headers.get("Content-Length", "0"))
                        if size > 16 * 1024:
                            raise OverflowError
                        body.extend(self.rfile.read(size))
                        if len(body) != size:
                            raise ValueError("incomplete body")
                    json.loads(body)
                except (ValueError, OverflowError, OSError):
                    try:
                        self.send_response(413)
                        self.send_header("Content-Length", "0")
                        self.end_headers()
                    except OSError:
                        pass
                    return
                with condition:
                    records["posts"].append(
                        (self.path, bytes(body), dict(self.headers))
                    )
                self.send_response(204)
                self.send_header("Cache-Control", "public, max-age=3600")
                self.send_header("Set-Cookie", "server_session=fixture; HttpOnly")
                self.send_header("Content-Security-Policy", "default-src 'none'")
                self.end_headers()

            def do_GET(self):
                if not slots.acquire(timeout=3):
                    self.send_error(503)
                    return
                try:
                    with condition:
                        records["active"] += 1
                        records["peak_active"] = max(
                            records["peak_active"], records["active"]
                        )
                        records["requests"].append((self.path, dict(self.headers)))
                    slow = self.path.startswith("/slow/")
                    status = (
                        304
                        if "304" in self.path
                        else 206
                        if "partial" in self.path
                        else 429
                        if "rate-limited" in self.path
                        else 503
                        if "unavailable" in self.path
                        else 500
                        if "error" in self.path
                        else 200
                    )
                    size = body_mib * 1024 * 1024 if slow else 2 if status == 206 else 0
                    self.send_response(status)
                    self.send_header("Content-Length", str(size))
                    self.send_header(
                        "Cache-Control",
                        "public, max-age=0, must-revalidate"
                        if self.path.startswith(("/static/", "/fixtures/"))
                        else "public, max-age=31536000, immutable",
                    )
                    self.send_header("Expires", "Thu, 01 Jan 2099 00:00:00 GMT")
                    self.send_header(
                        "Content-Security-Policy", "default-src 'none'; sandbox"
                    )
                    if status == 206:
                        self.send_header("Content-Range", "bytes 0-1/5")
                        self.send_header("Accept-Ranges", "bytes")
                    self.send_header("ETag", '"fixture"')
                    if "no-retry" not in self.path:
                        self.send_header("Retry-After", "7")
                    self.send_header("Set-Cookie", "server_session=fixture; HttpOnly")
                    if self.path.startswith("/content/verified"):
                        self.send_header("X-Seeded-Verification", "onchfs-keccak")
                    self.end_headers()
                    if status == 206:
                        self.wfile.write(b"xy")
                    if slow:
                        self.connection.settimeout(8)
                        chunk = b"x" * (64 * 1024)
                        for _ in range(size // len(chunk)):
                            self.wfile.write(chunk)
                        with condition:
                            records["completed"].append(self.path)
                            condition.notify_all()
                except (BrokenPipeError, ConnectionResetError, TimeoutError):
                    pass
                finally:
                    with condition:
                        records["active"] -= 1
                        condition.notify_all()
                    slots.release()

            do_HEAD = do_GET

        upstream = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Upstream)
        threading.Thread(target=upstream.serve_forever, daemon=True).start()
        self.addCleanup(upstream.server_close)
        self.addCleanup(upstream.shutdown)
        return upstream, records, condition

    @unittest.skipUnless(Path(CADDY).is_file(), "native Caddy unavailable")
    def test_caddy_adapts_bounded_production_configuration(self):
        config = self.adapted()
        deadline = re.search(
            r"PROXY_WRITE_TIMEOUT_SECONDS:\s*u64\s*=\s*(\d+)",
            (ROOT / "src/transport.rs").read_text(),
        )
        self.assertIsNotNone(deadline)
        assert deadline is not None
        write_timeout = int(deadline[1]) * 1_000_000_000
        self.assertTrue(config["admin"]["disabled"])
        servers = config["apps"]["http"]["servers"].values()
        self.assertGreaterEqual(len(config["apps"]["http"]["servers"]), 2)
        redirects = []
        for server in servers:
            self.assertEqual(server["read_header_timeout"], 5_000_000_000)
            self.assertFalse(server.get("automatic_https", {}).get("disable"))
            self.assertFalse(
                server.get("automatic_https", {}).get("disable_certificates")
            )
            self.assertGreater(server["write_timeout"], 0)
            self.assertEqual(server["write_timeout"], write_timeout)
            self.assertGreater(server["write_idle_timeout"], 0)
            self.assertEqual(server["max_header_bytes"], 128 * 1024)
            self.assertEqual(server["protocols"], ["h1", "h2"])
            if server.get("tls_connection_policies"):
                self.assertTrue(server["strict_sni_host"])

            def contains_redirect(value):
                if isinstance(value, dict):
                    if (
                        value.get("handler") == "static_response"
                        and value.get("status_code") == 308
                    ):
                        return True
                    return any(contains_redirect(child) for child in value.values())
                if isinstance(value, list):
                    return any(contains_redirect(child) for child in value)
                return False

            if contains_redirect(server.get("routes", [])):
                redirects.append(server)
        self.assertTrue(redirects, "HTTP redirect server has explicit redirect route")
        redirect = redirects[0]
        self.assertIn(":80", redirect["listen"])
        for timeout, expected in (
            ("read_header_timeout", 5_000_000_000),
            ("read_timeout", 10_000_000_000),
            ("write_timeout", 180_000_000_000),
            ("write_idle_timeout", 30_000_000_000),
            ("idle_timeout", 15_000_000_000),
        ):
            self.assertEqual(redirect[timeout], expected, timeout)
        upstream, _, _ = self.upstream()
        with self.running_proxy(upstream, redirect_only=True) as (port, _):
            status, headers, _ = self.request_proxy(port, "app.invalid", "/projects")
            self.assertEqual(status, 308)
            self.assertEqual(headers["Location"], "https://app.invalid/projects")
            self.assertEqual(headers["X-Content-Type-Options"], "nosniff")
            self.assertEqual(headers["Referrer-Policy"], "no-referrer")
            self.assertNotIn("Server", headers)

    @unittest.skipUnless(Path(CADDY).is_file(), "native Caddy unavailable")
    def test_content_host_redirects_keep_path_and_query_on_both_schemes(self):
        configure.render(
            self.settings(
                {**VALID, "CONTENT_HOST_REDIRECTS": "old.invalid www.artwork.invalid"}
            ),
            self.directory / "rendered",
        )
        target = "/content/ipfs/bafy/index.html?fxhash=oo1&x=%20"
        upstream, records, _ = self.upstream()
        # running_proxy validates the adapted configuration before starting Caddy.
        with self.running_proxy(upstream) as (port, _):
            for host in ("old.invalid", "www.artwork.invalid"):
                for method, path in (("GET", target), ("HEAD", "/"), ("POST", "/x")):
                    with self.subTest(host=host, method=method):
                        status, headers, _ = self.request_proxy(
                            port, host, path, method
                        )
                        self.assertEqual(status, 302)
                        self.assertEqual(
                            headers["Location"], f"https://artwork.invalid{path}"
                        )
                        self.assertEqual(headers["Cache-Control"], "no-store")
                        self.assertEqual(
                            headers["Strict-Transport-Security"], "max-age=31536000"
                        )
                        self.assertNotIn("Server", headers)
            self.assertEqual((records["requests"], records["posts"]), ([], []))
            self.assertEqual(
                self.request_proxy(port, "artwork.invalid", "/content/verified")[0],
                200,
            )
        with self.running_proxy(upstream, redirect_only=True) as (port, _):
            status, headers, _ = self.request_proxy(port, "old.invalid", target)
            self.assertEqual(status, 308)
            self.assertEqual(headers["Location"], f"https://old.invalid{target}")

    @unittest.skipUnless(Path(CADDY).is_file(), "native Caddy unavailable")
    def test_native_proxy_closes_incomplete_headers_after_five_seconds(self):
        upstream, _, _ = self.upstream()
        with (
            self.running_proxy(upstream, redirect_only=True) as (port, _),
            socket.create_connection(("127.0.0.1", port), timeout=3) as client,
        ):
            client.settimeout(2)
            client.sendall(b"GET / HTTP/1.1\r\nHost: app.invalid\r\nX-Incomplete:")
            started = time.monotonic()
            time.sleep(4)
            client.settimeout(0.05)
            with self.assertRaises(socket.timeout):
                client.recv(1)
            time.sleep(max(0, started + 5.5 - time.monotonic()))
            client.settimeout(2)
            try:
                self.assertEqual(client.recv(1), b"")
            except ConnectionResetError:
                pass

    @unittest.skipUnless(Path(CADDY).is_file(), "native Caddy unavailable")
    def test_proxy_preserves_security_headers_methods_body_and_origin_isolation(self):
        upstream, records, _ = self.upstream()
        with self.running_proxy(upstream) as (port, _):
            auth_cap = 16 * 1024
            market_cap = 24 * 1024
            proxy_body_limit = 32 * 1024
            self.assertIn(
                "DefaultBodyLimit::max(16 * 1024)",
                (ROOT / "src/web/auth.rs").read_text(),
            )
            self.assertIn(
                "DefaultBodyLimit::max(24 * 1024)",
                (ROOT / "src/web/market.rs").read_text(),
            )
            self.assertLess(
                auth_cap, proxy_body_limit, "auth JSON cap is below proxy cap"
            )
            self.assertLess(
                market_cap, proxy_body_limit, "market JSON cap is below proxy cap"
            )
            for host in ("app.invalid", "artwork.invalid"):
                for path in ("/health", "/health/", "/health/ready", "/health/metrics"):
                    status, headers, _ = self.request_proxy(port, host, path)
                    self.assertEqual(status, 404)
                    self.assertEqual(headers["Cache-Control"], "no-store")
            self.assertEqual(records["requests"], [])
            payload = json.dumps({"bio": "x" * 6000}).encode()
            for path in (
                "/api/auth/challenge",
                "/api/auth/verify",
                "/api/auth/logout",
                "/api/auth/profile",
                "/api/market/prepare",
                "/api/market/check",
                "/api/market/confirm",
                "/api/market/submitted",
                "/api/market/discard",
            ):
                status, headers, _ = self.request_proxy(
                    port, "app.invalid", path, "POST", payload
                )
                self.assertEqual(status, 204, path)
                self.assertEqual(headers["Cache-Control"], "no-store")
                self.assertEqual(
                    headers["Content-Security-Policy"], "default-src 'none'"
                )
                self.assertIn("Set-Cookie", headers)
                self.assertEqual(records["posts"][-1][:2], (path, payload))
                sent = records["posts"][-1][2]
                self.assertEqual(sent["Origin"], "https://app.invalid")
                self.assertEqual(sent["X-Csrf-Token"], "fixture")
                self.assertEqual(sent["X-Forwarded-For"], "127.0.0.1")
                self.assertNotIn("Forwarded", sent)
            self.assertEqual(
                self.request_proxy(port, "app.invalid", "/api/auth/session")[0], 200
            )
            for host, path, method, expected in (
                ("app.invalid", "/api/auth/challenge", "GET", 405),
                ("app.invalid", "/api/auth/profile", "PUT", 405),
                ("app.invalid", "/api/auth/session", "POST", 405),
                ("app.invalid", "/api/unknown", "POST", 404),
                ("app.invalid", "/projects", "POST", 405),
                ("artwork.invalid", "/api/auth/challenge", "POST", 405),
            ):
                self.assertEqual(
                    self.request_proxy(port, host, path, method)[0], expected
                )
            self.assertEqual(
                self.request_proxy(
                    port, "app.invalid", "/api/auth/profile", "POST", b"x" * 32769
                )[0],
                413,
            )
            self.assertEqual(len(records["posts"]), 9)
            self.assertEqual(
                len(records["received_posts"]),
                9,
                "declared oversize never reaches backend",
            )
            excess = b'{"bio":"valid prefix"}' + b" " * 40000
            self.assertEqual(
                self.request_proxy(
                    port,
                    "app.invalid",
                    "/api/auth/profile",
                    "POST",
                    excess,
                    chunked=True,
                )[0],
                413,
            )
            self.assertEqual(
                len(records["posts"]), 9, "chunked prefix must not cause a mutation"
            )
            before = len(records["requests"])
            for host in ("app.invalid", "artwork.invalid"):
                self.assertEqual(
                    self.request_proxy(
                        port, host, "/oversized-get", "GET", b"x" * 1025
                    )[0],
                    413,
                )
            self.assertEqual(len(records["requests"]), before)
            for path in (
                "/static/site.css",
                "/static/304.css",
                "/fixtures/preview.png",
            ):
                _, headers, _ = self.request_proxy(port, "app.invalid", path)
                self.assertEqual(
                    headers["Cache-Control"], "public, max-age=0, must-revalidate"
                )
            for path in ("/content/verified", "/content/verified304"):
                _, headers, _ = self.request_proxy(port, "artwork.invalid", path)
                self.assertEqual(
                    headers["Cache-Control"], "public, max-age=31536000, immutable"
                )
            for path, cache in (
                ("/content/verified-partial", "public, max-age=31536000, immutable"),
                ("/content/unverified-partial", "no-store"),
            ):
                status, headers, body = self.request_proxy(
                    port, "artwork.invalid", path
                )
                self.assertEqual(status, 206)
                self.assertEqual(headers["Cache-Control"], cache)
                self.assertEqual(headers["Content-Range"], "bytes 0-1/5")
                self.assertEqual(headers["Accept-Ranges"], "bytes")
                self.assertEqual(body, b"xy")
            for host, path in (
                ("app.invalid", "/projects"),
                ("app.invalid", "/static/error.css"),
                ("artwork.invalid", "/render/demo"),
                ("artwork.invalid", "/content/unverified"),
                ("artwork.invalid", "/content/verified-error"),
            ):
                _, headers, _ = self.request_proxy(port, host, path)
                self.assertEqual(headers["Cache-Control"], "no-store", (host, path))
                self.assertNotIn("Expires", headers)
                self.assertEqual(
                    headers["Content-Security-Policy"], "default-src 'none'; sandbox"
                )
                self.assertEqual(headers["Retry-After"], "7")
                self.assertEqual(headers["Etag"], '"fixture"')
                sent = records["requests"][-1][1]
                if host == "artwork.invalid":
                    self.assertNotIn("Cookie", sent)
                    self.assertNotIn("Authorization", sent)
                    self.assertNotIn("Set-Cookie", headers)
                else:
                    self.assertIn("Cookie", sent)

    @unittest.skipUnless(Path(CADDY).is_file(), "native Caddy unavailable")
    def test_content_host_robots_txt_keeps_crawlers_off_generator_pages(self):
        upstream, records, _ = self.upstream()
        with self.running_proxy(upstream) as (port, _):
            status, headers, body = self.request_proxy(
                port, "artwork.invalid", "/robots.txt"
            )
            self.assertEqual(status, 200)
            self.assertEqual(
                body,
                b"User-agent: *\nDisallow: /render/\nDisallow: /playback-status/\n",
            )
            self.assertEqual(headers["Cache-Control"], "no-store")
            self.assertEqual(headers["X-Content-Type-Options"], "nosniff")
            self.assertNotIn("Server", headers)
            self.assertEqual(
                self.request_proxy(port, "artwork.invalid", "/robots.txt", "POST")[0],
                405,
            )
            self.assertEqual(records["requests"], [])
            # Public crawler groups are served even if the application is down.
            status, _, body = self.request_proxy(port, "app.invalid", "/robots.txt")
            self.assertEqual(status, 200)
            self.assertIn(b"User-agent: OAI-SearchBot", body)
            self.assertEqual(records["requests"], [])

    @unittest.skipUnless(Path(CADDY).is_file(), "native Caddy unavailable")
    def test_security_headers_cover_normal_refusal_and_upstream_failure_on_both_hosts(
        self,
    ):
        upstream, _, _ = self.upstream()
        with self.running_proxy(upstream) as (port, _):
            cases = (
                ("app.invalid", "/projects", "GET", 200),
                ("artwork.invalid", "/content/verified", "GET", 200),
                ("app.invalid", "/health", "GET", 404),
                ("artwork.invalid", "/health", "GET", 404),
                ("app.invalid", "/projects", "POST", 405),
                ("artwork.invalid", "/projects", "POST", 405),
                ("app.invalid", "/oversized", "GET", 413),
                ("artwork.invalid", "/oversized", "GET", 413),
                ("app.invalid", "/static/error.css", "GET", 500),
                ("app.invalid", "/rate-limited", "GET", 429),
                ("app.invalid", "/unavailable", "GET", 503),
                ("artwork.invalid", "/rate-limited", "GET", 429),
                ("artwork.invalid", "/unavailable", "GET", 503),
                ("artwork.invalid", "/unavailable-no-retry", "GET", 503),
                ("artwork.invalid", "/content/error", "GET", 500),
            )
            for host, path, method, expected in cases:
                with self.subTest(host=host, path=path):
                    status, headers, _ = self.request_proxy(
                        port,
                        host,
                        path,
                        method,
                        b"x" * 1025 if path == "/oversized" else b"",
                    )
                    self.assertEqual(status, expected)
                    self.assertEqual(
                        headers["Strict-Transport-Security"], "max-age=31536000"
                    )
                    self.assertEqual(headers["X-Content-Type-Options"], "nosniff")
                    self.assertEqual(headers["Referrer-Policy"], "no-referrer")
                    self.assertNotIn("Server", headers)
                    if expected >= 400:
                        self.assertEqual(headers["Cache-Control"], "no-store")
                    if path in ("/rate-limited", "/unavailable"):
                        self.assertEqual(headers["Retry-After"], "7")
                    if host == "artwork.invalid" and expected in (429, 503):
                        self.assertEqual(headers["Access-Control-Allow-Origin"], "*")
                    else:
                        self.assertNotIn("Access-Control-Allow-Origin", headers)
                    if path == "/unavailable-no-retry":
                        self.assertEqual(headers["Retry-After"], "2")

        with socket.socket() as unavailable:
            unavailable.bind(("127.0.0.1", 0))
            unused_port = unavailable.getsockname()[1]
        target = type("UnavailableUpstream", (), {"server_port": unused_port})()
        with self.running_proxy(target) as (port, _):
            for host in ("app.invalid", "artwork.invalid"):
                status, headers, _ = self.request_proxy(
                    port, host, "/upstream-down?private=fixture-secret"
                )
                self.assertEqual(status, 503)
                self.assertEqual(headers["Retry-After"], "2")
                if host == "artwork.invalid":
                    self.assertEqual(headers["Access-Control-Allow-Origin"], "*")
                else:
                    self.assertNotIn("Access-Control-Allow-Origin", headers)
                self.assertEqual(headers["Cache-Control"], "no-store")
                self.assertEqual(
                    headers["Strict-Transport-Security"], "max-age=31536000"
                )
                self.assertEqual(headers["X-Content-Type-Options"], "nosniff")
                self.assertEqual(headers["Referrer-Policy"], "no-referrer")
                self.assertNotIn("Server", headers)
        # Exercise the unmodified production error handlers and filtered logger.
        with self.running_proxy(target) as (port, _):
            self.assertEqual(
                self.request_proxy(port, "app.invalid", "/down?private=fixture-secret")[
                    0
                ],
                503,
            )
        errors = (self.directory / "runtime.log").read_text()
        self.assertIn('"logger":"http.log.error"', errors)
        self.assertNotIn("fixture-secret", errors)
        self.assertNotIn("private-fixture", errors)
        self.assertNotIn('"request":', errors)

    @unittest.skipUnless(
        Path(CADDY).is_file() and shutil.which("openssl"), "Caddy/OpenSSL unavailable"
    )
    def test_private_tls_rejects_unknown_sni_and_mismatched_host(self):
        certificate = self.directory / "certificate.pem"
        key = self.directory / "key.pem"
        created = subprocess.run(
            [
                "openssl",
                "req",
                "-x509",
                "-newkey",
                "ed25519",
                "-nodes",
                "-keyout",
                str(key),
                "-out",
                str(certificate),
                "-days",
                "1",
                "-subj",
                "/CN=app.invalid",
                "-addext",
                "subjectAltName=DNS:app.invalid,DNS:artwork.invalid",
            ],
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
            umask=0o077,
        )
        self.assertEqual(created.returncode, 0, created.stderr)
        context = ssl.create_default_context(cafile=str(certificate))
        upstream, records, _ = self.upstream()
        with self.running_proxy(upstream, tls_files=(certificate, key)) as (port, _):
            for hostname in ("app.invalid", "artwork.invalid"):
                with (
                    socket.create_connection(("127.0.0.1", port), timeout=3) as raw,
                    context.wrap_socket(raw, server_hostname=hostname) as sock,
                ):
                    sock.sendall(
                        f"GET /tls-check HTTP/1.1\r\nHost: {hostname}\r\nConnection: close\r\n\r\n".encode()
                    )
                    with http.client.HTTPResponse(sock) as response:
                        response.begin()
                        self.assertEqual(response.status, 200)
                        response.read()
            before = len(records["requests"])
            with (
                socket.create_connection(("127.0.0.1", port), timeout=3) as raw,
                context.wrap_socket(raw, server_hostname="app.invalid") as sock,
            ):
                sock.sendall(
                    b"GET /wrong-host HTTP/1.1\r\nHost: artwork.invalid\r\nConnection: close\r\n\r\n"
                )
                with http.client.HTTPResponse(sock) as response:
                    response.begin()
                    self.assertEqual(response.status, 421)
                    self.assertEqual(response.getheader("Cache-Control"), "no-store")
                    self.assertEqual(
                        response.getheader("X-Content-Type-Options"), "nosniff"
                    )
                    self.assertEqual(
                        response.getheader("Referrer-Policy"), "no-referrer"
                    )
                    self.assertEqual(
                        response.getheader("Strict-Transport-Security"),
                        "max-age=31536000",
                    )
                    self.assertIsNone(response.getheader("Server"))
                    response.read()
            unknown = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            unknown.check_hostname = False
            unknown.verify_mode = ssl.CERT_NONE
            with (
                self.assertRaises(ssl.SSLError),
                socket.create_connection(("127.0.0.1", port), timeout=3) as raw,
            ):
                # The server must refuse unknown SNI even without client-side
                # certificate checks; this context is private test-only.
                unknown.wrap_socket(raw, server_hostname="unknown.invalid")
            self.assertEqual(len(records["requests"]), before)

    @staticmethod
    def rss_kib(process, field="VmRSS"):
        values = dict(
            line.split(":", 1)
            for line in Path(f"/proc/{process.pid}/status").read_text().splitlines()
            if ":" in line
        )
        return int(values[field].split()[0])

    @unittest.skipUnless(Path(CADDY).is_file(), "native Caddy unavailable")
    def test_streaming_proxy_has_no_shared_host_rejection_threshold(self):
        upstream, _, _ = self.upstream(12)
        held = []
        with self.running_proxy(upstream) as (port, _):
            try:
                for index in range(16):
                    reader = self.connect_proxy(
                        port, "artwork.invalid", f"/slow/stalled-{index}"
                    )
                    held.append(reader)
                    response = http.client.HTTPResponse(reader)
                    response.begin()
                    self.assertEqual(response.status, 200)
                self.assertEqual(
                    self.request_proxy(port, "artwork.invalid", "/other-client")[0],
                    200,
                )
            finally:
                for reader in held:
                    reader.close()

    @unittest.skipUnless(Path(CADDY).is_file(), "native Caddy unavailable")
    def test_write_deadlines_release_stalled_downstream(self):
        upstream, _, _ = self.upstream()
        with self.running_proxy(upstream, short_deadline=True) as (port, _):
            started = time.monotonic()
            with self.connect_proxy(port, "artwork.invalid", "/slow/trickle") as reader:
                time.sleep(3.5)
                reader.settimeout(3)
                size = 0
                try:
                    while chunk := reader.recv(64 * 1024):
                        size += len(chunk)
                except ConnectionResetError:
                    pass
                self.assertLess(size, 12 * 1024 * 1024)
            self.assertLess(time.monotonic() - started, 8)
            self.assertEqual(
                self.request_proxy(port, "artwork.invalid", "/after-timeout")[0], 200
            )

    @unittest.skipUnless(shutil.which("systemd-analyze"), "systemd-analyze unavailable")
    def test_systemd_accepts_units_in_an_isolated_validation_root(self):
        rendered = self.directory / "rendered"
        configure.render(
            self.settings(
                {
                    **VALID,
                    "FORUM_HOST": "forum.invalid",
                    "FORUM_ROOT": "/var/www/forum/public",
                    "FORUM_PHP_SOCKET": "/run/php/php8.4-fpm.sock",
                }
            ),
            rendered,
        )
        stage = self.directory / "root"
        units = stage / "etc/systemd/system"
        units.mkdir(parents=True)
        unit_names = sorted(
            path.name
            for path in rendered.iterdir()
            if path.name.endswith((".service", ".timer", ".path"))
        )
        for name in unit_names:
            shutil.copyfile(rendered / name, units / name)
        for name in (
            "basic.target",
            "sysinit.target",
            "network-online.target",
            "multi-user.target",
            "shutdown.target",
            "timers.target",
            "paths.target",
            "time-set.target",
            "time-sync.target",
        ):
            (units / name).write_text(
                "[Unit]\nDescription=Validation stub\nDefaultDependencies=no\n"
            )
        for executable in (
            "usr/bin/mkdir",
            "usr/bin/rm",
            "usr/bin/kill",
            "usr/bin/python3",
            "usr/bin/caddy",
            "opt/seeded/current/seeded_archive",
        ):
            path = stage / executable
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("#!/bin/sh\nexit 0\n")
            path.chmod(0o755)
        result = subprocess.run(
            [
                "systemd-analyze",
                f"--root={stage}",
                "--man=no",
                "--generators=no",
                "verify",
                *unit_names,
            ],
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
