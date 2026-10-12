"""Optional forum hosting and OAuth proxy boundaries, without host changes."""

import json
import socketserver
import struct
import subprocess
import threading
import unittest

import configure
import test_deployment as deployment


class ForumTests(unittest.TestCase):
    def setUp(self):
        self.fixture = deployment.DeploymentTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.directory = self.fixture.directory
        self.values = {
            **deployment.VALID,
            "FORUM_HOST": "forum.invalid",
            "FORUM_ROOT": "/var/www/forum/public",
            "FORUM_PHP_SOCKET": "/run/php/php8.4-fpm.sock",
        }

    def render(self, values):
        configure.render(self.fixture.settings(values), self.directory / "rendered")
        return self.directory / "rendered"

    def test_forum_backup_is_optional_and_isolated_from_archive_jobs(self):
        plain = self.directory / "plain"
        configure.render(self.fixture.settings(deployment.VALID), plain)
        self.assertFalse((plain / "seeded-forum-backup.service").exists())
        rendered = self.render(self.values)
        unit = (rendered / "seeded-forum-backup.service").read_text()
        timer = (rendered / "seeded-forum-backup.timer").read_text()
        for expected in (
            "User=seeded-forum-backup",
            "Group=seeded-forum-backup",
            "StateDirectory=seeded-forum-backup",
            "StateDirectoryMode=0700",
            "WorkingDirectory=/var/lib/seeded-forum-backup",
            "PrivateNetwork=true",
            "RestrictAddressFamilies=AF_UNIX",
            "UMask=0077",
            "ProtectSystem=strict",
            "NoNewPrivileges=true",
            "TimeoutStartSec=11min",
            "KillMode=control-group",
        ):
            self.assertIn(expected, unit)
        for forbidden in (
            "EnvironmentFile=",
            "SupplementaryGroups=",
            "Requires=",
            "Wants=",
            "seeded-verify",
            "seeded-profiles",
            "BindPaths=",
            "RuntimeDirectory=",
            "/var/lib/seeded/",
        ):
            self.assertNotIn(forbidden, unit)
        self.assertIn("OnCalendar=*-*-* 03:30:00", timer)
        self.assertIn("Persistent=true", timer)
        self.assertIn("Unit=seeded-forum-backup.service", timer)
        self.assertEqual(
            (rendered / "seeded-forum-backup.service").stat().st_mode & 0o777,
            0o644,
        )

    def fastcgi(self):
        """A local PHP responder that records scripts and emits unsafe headers."""
        scripts = []

        class PHP(socketserver.StreamRequestHandler):
            def handle(self):
                self.connection.settimeout(5)
                params = bytearray()
                while header := self.rfile.read(8):
                    _, kind, request, size, padding, _ = struct.unpack(
                        "!BBHHBB", header
                    )
                    data = self.rfile.read(size)
                    self.rfile.read(padding)
                    if kind == 4:
                        params.extend(data)
                    if kind != 5 or size:
                        continue
                    fields = {}
                    position = 0

                    def length():
                        nonlocal position
                        value = params[position]
                        if value & 128:
                            value = (
                                int.from_bytes(params[position : position + 4])
                                & 0x7FFFFFFF
                            )
                            position += 4
                        else:
                            position += 1
                        return value

                    while position < len(params):
                        name_size, value_size = length(), length()
                        name = bytes(params[position : position + name_size])
                        position += name_size
                        fields[name] = bytes(params[position : position + value_size])
                        position += value_size
                    scripts.append(fields[b"SCRIPT_FILENAME"].decode())
                    body = (
                        b"Status: 200 OK\r\nContent-Type: text/plain\r\n"
                        b"X-Powered-By: Flarum\r\nX-Frame-Options: SAMEORIGIN\r\n"
                        b"Content-Security-Policy: frame-ancestors *\r\n\r\nforum php fixture"
                    )
                    for record, payload in ((6, body), (6, b""), (3, bytes(8))):
                        self.wfile.write(
                            struct.pack(
                                "!BBHHBB", 1, record, request, len(payload), 0, 0
                            )
                            + payload
                        )
                    return

        path = self.directory / "php.sock"
        server = socketserver.ThreadingUnixStreamServer(str(path), PHP)
        server.daemon_threads = True
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()

        def stop():
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

        self.addCleanup(stop)
        return path, scripts

    def assert_forum_headers(self, headers):
        self.assertEqual(headers["X-Frame-Options"], "DENY")
        self.assertEqual(headers["Content-Security-Policy"], "frame-ancestors 'none'")
        self.assertNotIn("X-Powered-By", headers)

    def test_absent_forum_has_no_vhost_or_extra_permissions(self):
        rendered = self.render(deployment.VALID)
        self.assertNotIn("php_fastcgi", (rendered / "Caddyfile").read_text())
        unit = (rendered / "seeded-proxy.service").read_text()
        self.assertNotIn("SupplementaryGroups", unit)
        self.assertNotIn("ReadOnlyPaths", unit)

    def test_forum_settings_are_all_or_none_and_separate_from_archive_hosts(self):
        for missing in ("FORUM_HOST", "FORUM_ROOT", "FORUM_PHP_SOCKET"):
            values = dict(self.values)
            del values[missing]
            with self.subTest(missing=missing), self.assertRaises(ValueError):
                self.fixture.settings(values)
        for host in ("app.invalid", "artwork.invalid", "BAD.invalid", "127.0.0.1"):
            with self.subTest(host=host), self.assertRaises(ValueError):
                self.fixture.settings({**self.values, "FORUM_HOST": host})

    def test_forum_paths_cannot_inject_configuration_or_use_hidden_mounts(self):
        for key in ("FORUM_ROOT", "FORUM_PHP_SOCKET"):
            for path in (
                "/",
                "relative",
                "/var/www/../private",
                "/var/www//forum",
                "/var/www/forum\nReadWritePaths=/",
                "/var/www/__APP_HOST__",
                "/home/forum/public",
                "/root/forum",
                "/tmp/forum",
                "/var/tmp/forum",
                "/run/user/1000/forum",
                "/var/www/%h",
            ):
                with self.subTest(key=key, path=path), self.assertRaises(ValueError):
                    self.fixture.settings({**self.values, key: path})

    def test_forum_service_keeps_hardening_with_read_only_root_and_socket_group(self):
        rendered = self.render(self.values)
        unit = (rendered / "seeded-proxy.service").read_text()
        for line in (
            "SupplementaryGroups=www-data",
            f"ReadOnlyPaths={self.values['FORUM_ROOT']}",
            "ProtectSystem=strict",
            "ProtectHome=true",
            "PrivateTmp=true",
            "RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6",
            "Conflicts=caddy.service",
        ):
            self.assertIn(line, unit)
        self.assertNotIn("ReadWritePaths", unit)
        config = self.fixture.adapted()
        transports = []

        def visit(value):
            if isinstance(value, dict):
                if value.get("transport", {}).get("protocol") == "fastcgi":
                    transports.append(value)
                for item in value.values():
                    visit(item)
            elif isinstance(value, list):
                for item in value:
                    visit(item)

        visit(config)
        self.assertEqual(len(transports), 1)
        self.assertEqual(
            transports[0]["upstreams"][0]["dial"],
            "unix/" + self.values["FORUM_PHP_SOCKET"],
        )
        self.assertEqual(transports[0]["transport"]["dial_timeout"], 2_000_000_000)

    def test_native_forum_static_files_and_private_paths_stay_separate(self):
        root = self.directory / "public"
        root.mkdir()
        (root / "visible.txt").write_text("forum fixture")
        for name in (
            "composer.json",
            "composer.lock",
            "config.php",
            "flarum",
            "site.php",
            "extend.php",
            ".htaccess",
            "web.config",
            "stray.php",
        ):
            (root / name).write_text("must not be served")
        for name in ("vendor", "storage"):
            (root / name).mkdir()
            (root / name / "secret.txt").write_text("must not be served")
        rendered = self.render(self.values)
        caddyfile = rendered / "Caddyfile"
        # Native process tests use project-local files, outside the service namespace.
        caddyfile.write_text(
            caddyfile.read_text().replace(self.values["FORUM_ROOT"], str(root))
        )
        upstream, _, _ = self.fixture.upstream()
        with self.fixture.running_proxy(upstream) as (port, _):
            status, headers, body = self.fixture.request_proxy(
                port, "forum.invalid", "/visible.txt"
            )
            self.assertEqual((status, body), (200, b"forum fixture"))
            self.assertEqual(headers["X-Content-Type-Options"], "nosniff")
            self.assert_forum_headers(headers)
            for path in (
                "/vendor",
                "/vendor/secret.txt",
                "/storage",
                "/storage/secret.txt",
                "/composer.json",
                "/composer.lock",
                "/config.php",
                "/config.php/info",
                "/flarum",
                "/flarum/info",
                "/site.php",
                "/extend.php",
                "/%76endor/secret.txt",
                "/x/../storage/secret.txt",
                "/.htaccess",
                "/web.config",
                "/stray.php",
            ):
                with self.subTest(path=path):
                    status, headers, _ = self.fixture.request_proxy(
                        port, "forum.invalid", path
                    )
                    self.assertEqual(status, 404)
                    self.assert_forum_headers(headers)
            # Agents asking the forum for /llms.txt are sent to the app's guide index.
            status, headers, _ = self.fixture.request_proxy(
                port, "forum.invalid", "/llms.txt"
            )
            self.assertEqual(status, 302)
            self.assertEqual(headers["Location"], "https://app.invalid/llms.txt")
            # Crawlers may fetch every forum page.
            status, headers, body = self.fixture.request_proxy(
                port, "forum.invalid", "/robots.txt"
            )
            self.assertEqual(status, 200)
            self.assertTrue(body.startswith(b"User-agent: *\nAllow: /\n"), body)
            self.assertIn(b"User-agent: Claude-SearchBot", body)
            self.assertNotIn(b"Disallow:", body)
            self.assert_forum_headers(headers)
            self.assertEqual(
                self.fixture.request_proxy(port, "app.invalid", "/visible.txt")[2], b""
            )
            self.assertEqual(
                self.fixture.request_proxy(port, "artwork.invalid", "/visible.txt")[2],
                b"",
            )
        with self.fixture.running_proxy(upstream, redirect_only=True) as (port, _):
            status, headers, _ = self.fixture.request_proxy(
                port, "forum.invalid", "/discussion"
            )
            self.assertEqual(status, 308)
            self.assertEqual(headers["Location"], "https://forum.invalid/discussion")

    def test_native_forum_runs_only_its_front_controller_and_strips_php_headers(self):
        root = self.directory / "public"
        root.mkdir()
        (root / "index.php").write_text("front controller fixture")
        (root / "assets").mkdir()
        (root / "assets" / "avatar.svg").write_text("<svg/>")
        for name in ("stray.php", "stray.PHP", "index.php", ".htaccess", "web.config"):
            (root / "assets" / name).write_text("must not be served")
        socket, scripts = self.fastcgi()
        rendered = self.render(self.values)
        caddyfile = rendered / "Caddyfile"
        caddyfile.write_text(
            caddyfile.read_text()
            .replace(self.values["FORUM_ROOT"], str(root))
            .replace(self.values["FORUM_PHP_SOCKET"], str(socket))
        )
        upstream, _, _ = self.fixture.upstream()
        with self.fixture.running_proxy(upstream) as (port, _):
            for path in ("/", "/index.php", "/admin", "/auth/seeded", "/d/123"):
                with self.subTest(path=path):
                    status, headers, body = self.fixture.request_proxy(
                        port, "forum.invalid", path
                    )
                    self.assertEqual((status, body), (200, b"forum php fixture"))
                    self.assert_forum_headers(headers)
            self.assertEqual(scripts, [str(root / "index.php")] * 5)
            for path in (
                "/assets/stray.php",
                "/assets/stray.PHP",
                "/assets/stray.php/info",
                "/assets/index.php",
                "/assets/index.php/info",
                "/assets/%73tray.php",
                "/assets/stray%2ephp",
                "/assets/.htaccess",
                "/assets/web.config",
                "/%2ehtaccess",
                "/WEB.CONFIG",
                "/assets/../stray.php",
                "/missing.php",
                "/index.php/info",
            ):
                with self.subTest(path=path):
                    status, headers, _ = self.fixture.request_proxy(
                        port, "forum.invalid", path
                    )
                    self.assertEqual(status, 404)
                    self.assert_forum_headers(headers)
                    self.assertEqual(len(scripts), 5)
            status, headers, body = self.fixture.request_proxy(
                port, "forum.invalid", "/assets/avatar.svg"
            )
            self.assertEqual((status, body), (200, b"<svg/>"))
            self.assert_forum_headers(headers)
            socket.unlink()
            status, headers, _ = self.fixture.request_proxy(
                port, "forum.invalid", "/admin"
            )
            self.assertEqual(status, 503)
            self.assert_forum_headers(headers)

    def test_oauth_post_is_bounded_and_never_forwarded_on_artwork_host(self):
        # Transport fixture parses complete JSON; form parsing remains covered by tests/oauth.rs.
        self.render(deployment.VALID)
        upstream, records, _ = self.fixture.upstream()
        with self.fixture.running_proxy(upstream) as (port, _):
            for chunked in (False, True):
                status, headers, _ = self.fixture.request_proxy(
                    port, "app.invalid", "/oauth/token", "POST", b"{}", chunked
                )
                self.assertEqual(status, 204)
                self.assertEqual(headers["Cache-Control"], "no-store")
                before = len(records["posts"])
                body = json.dumps({"padding": "x" * 4096}).encode()
                self.assertEqual(
                    self.fixture.request_proxy(
                        port, "app.invalid", "/oauth/token", "POST", body, chunked
                    )[0],
                    413,
                )
                self.assertEqual(len(records["posts"]), before)
            for host, path in (
                ("artwork.invalid", "/oauth/token"),
                ("app.invalid", "/oauth/authorize"),
                ("app.invalid", "/oauth/user"),
                ("app.invalid", "/oauth/token/extra"),
            ):
                with self.subTest(host=host, path=path):
                    self.assertEqual(
                        self.fixture.request_proxy(port, host, path, "POST", b"{}")[0],
                        405,
                    )
            self.assertEqual(
                records["posts"][0][2]["Authorization"], "Bearer private-fixture"
            )

    def test_oauth_secret_is_literal_private_and_never_written_to_proxy_config(self):
        secret = 'fixture-__APP_HOST__-"-\\-$-%'
        rendered = self.render(
            {
                **deployment.VALID,
                "OAUTH_CLIENT_ID": "forum",
                "OAUTH_CLIENT_SECRET": secret,
                "OAUTH_REDIRECT_URI": "https://forum.invalid/auth/seeded",
            }
        )
        env = (rendered / "seeded.env").read_text()
        self.assertIn("__APP_HOST__", env)
        self.assertIn('SEEDED_OAUTH_CLIENT_ID="forum"', env)
        self.assertEqual((rendered / "seeded.env").stat().st_mode & 0o777, 0o600)
        for path in rendered.iterdir():
            if path.name != "seeded.env":
                self.assertNotIn("fixture-", path.read_text())
        settings = self.directory / "bad.json"
        settings.write_text(json.dumps({**self.values, "OAUTH_CLIENT_SECRET": secret}))
        result = subprocess.run(
            [
                "python3",
                "deploy/configure.py",
                str(settings),
                str(self.directory / "bad-output"),
            ],
            cwd=deployment.ROOT,
            capture_output=True,
            text=True,
            check=False,
            timeout=5,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn(secret, result.stdout + result.stderr)
        self.assertNotIn("fixture-", result.stdout + result.stderr)
