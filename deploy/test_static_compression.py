"""Native Caddy compression stays inside the app static handler."""

import gzip
import http.client
import http.server
import threading
import unittest

import test_deployment as deployment

BODY = b"seeded static compression fixture\n" * 256


class StaticCompressionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        deployment.DeploymentTests.setUpClass()

    def setUp(self):
        self.fixture = deployment.DeploymentTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.fixture.settings(deployment.VALID)

    def upstream(self):
        body = BODY

        class Upstream(http.server.BaseHTTPRequestHandler):
            def log_message(self, *_args):
                pass

            def reply(self):
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                content_type = (
                    "text/html"
                    if self.path == "/auth"
                    else "application/json"
                    if self.path.startswith("/api/")
                    else "image/svg+xml"
                    if self.path.startswith(("/fixtures/", "/ipfs/"))
                    else "text/css"
                )
                self.send_header("Content-Type", content_type)
                if self.path.startswith(("/static/", "/fixtures/")):
                    self.send_header(
                        "Cache-Control", "public, max-age=0, must-revalidate"
                    )
                elif self.path.startswith("/ipfs/"):
                    self.send_header(
                        "Cache-Control", "public, max-age=31536000, immutable"
                    )
                    self.send_header("X-Seeded-Verification", "onchfs-keccak")
                else:
                    self.send_header("Cache-Control", "no-store")
                self.end_headers()
                self.wfile.write(body)

            do_GET = reply

            def do_POST(self):
                self.rfile.read(int(self.headers.get("Content-Length", "0")))
                self.reply()

        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Upstream)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()

        def stop_server():
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

        self.addCleanup(stop_server)
        return server

    @staticmethod
    def request(port, host, path, accept_encoding=None, method="GET", body=b""):
        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
        headers = {"Host": host, "Connection": "close"}
        if accept_encoding:
            headers["Accept-Encoding"] = accept_encoding
        if method == "POST":
            headers["Content-Type"] = "application/json"
        connection.request(method, path, body=body, headers=headers)
        response = connection.getresponse()
        result = response.status, dict(response.getheaders()), response.read()
        connection.close()
        return result

    def test_static_paths_compress_only_when_requested_and_keep_cache_policy(self):
        upstream = self.upstream()
        with self.fixture.running_proxy(upstream) as (port, _):
            for path in ("/static/site.css", "/fixtures/preview.svg"):
                for encoding in ("gzip", "zstd"):
                    with self.subTest(path=path, encoding=encoding):
                        status, headers, body = self.request(
                            port, "app.invalid", path, encoding
                        )
                        self.assertEqual(status, 200)
                        self.assertEqual(headers["Content-Encoding"], encoding)
                        self.assertLess(len(body), len(BODY))
                        self.assertEqual(
                            headers["Cache-Control"],
                            "public, max-age=0, must-revalidate",
                        )
                        if encoding == "gzip":
                            self.assertEqual(gzip.decompress(body), BODY)
                        else:
                            self.assertTrue(body.startswith(b"\x28\xb5\x2f\xfd"))

                status, headers, body = self.request(port, "app.invalid", path)
                self.assertEqual((status, body), (200, BODY))
                self.assertNotIn("Content-Encoding", headers)
                self.assertEqual(
                    headers["Cache-Control"], "public, max-age=0, must-revalidate"
                )

    def test_app_html_and_auth_api_stay_uncompressed(self):
        upstream = self.upstream()
        with self.fixture.running_proxy(upstream) as (port, _):
            for path, method, request_body in (
                ("/auth", "GET", b""),
                ("/api/auth/session", "GET", b""),
                ("/api/auth/profile", "POST", b'{"bio":"fixture"}'),
            ):
                with self.subTest(path=path, method=method):
                    status, headers, body = self.request(
                        port,
                        "app.invalid",
                        path,
                        "zstd, gzip",
                        method,
                        request_body,
                    )
                    self.assertEqual((status, body), (200, BODY))
                    self.assertNotIn("Content-Encoding", headers)
                    self.assertEqual(headers["Cache-Control"], "no-store")

    def test_content_host_static_and_verified_ipfs_routes_stay_uncompressed(self):
        upstream = self.upstream()
        with self.fixture.running_proxy(upstream) as (port, _):
            for path, cache in (
                ("/static/site.css", "no-store"),
                ("/fixtures/preview.svg", "no-store"),
                ("/ipfs/fixture/artwork.svg", "public, max-age=31536000, immutable"),
            ):
                with self.subTest(path=path):
                    status, headers, body = self.request(
                        port, "artwork.invalid", path, "zstd, gzip"
                    )
                    self.assertEqual((status, body), (200, BODY))
                    self.assertNotIn("Content-Encoding", headers)
                    self.assertEqual(headers["Cache-Control"], cache)
