"""Native HTTP/2 isolation against real Rust app/content routers and Caddy."""

import contextlib
import json
import os
import selectors
import shutil
import signal
import socket
import subprocess
import threading
import time
import unittest
from pathlib import Path

import test_deployment as deployment

ROOT = Path(__file__).resolve().parent.parent


def memory_scope():
    try:
        line = next(
            line
            for line in Path("/proc/self/cgroup").read_text().splitlines()
            if line.startswith("0::")
        )
        group = Path("/sys/fs/cgroup") / line[3:].lstrip("/")
        maximum = (group / "memory.max").read_text().strip()
        events = dict(
            line.split() for line in (group / "memory.events").read_text().splitlines()
        )
        return {
            "limited": maximum != "max",
            "max_bytes": int(maximum) if maximum != "max" else None,
            "peak_bytes": int((group / "memory.peak").read_text()),
            "oom_kill": int(events.get("oom_kill", "0")),
        }
    except (OSError, ValueError, StopIteration):
        return {"limited": False}


class ProxyIsolationTests(unittest.TestCase):
    def test_stalled_http2_prefix_cannot_starve_another_client(self):
        required = os.environ.get("SEEDED_REQUIRE_NATIVE_PROXY") == "1"
        if os.environ.get("SEEDED_REQUIRE_CGROUP") == "1":
            self.assertTrue(memory_scope()["limited"], "Finite test cgroup required")
        binary = (
            Path(os.environ.get("CARGO_TARGET_DIR", ROOT / "target"))
            / "debug/examples/proxy_fixture"
        )
        if (
            not binary.is_file()
            or not Path(deployment.CADDY).is_file()
            or not shutil.which("node")
        ):
            if required:
                self.fail("Native isolation gate needs proxy_fixture, Caddy and Node")
            self.skipTest("Build proxy_fixture and provide Caddy/Node")
        fixture = deployment.DeploymentTests()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        work = fixture.directory
        with (work / "app.log").open("w") as errors:
            app = subprocess.Popen(
                [str(binary), str(work / "archive")],
                stdout=subprocess.PIPE,
                stderr=errors,
                text=True,
            )
            self.addCleanup(self.stop, app)
            assert app.stdout is not None
            with selectors.DefaultSelector() as selector:
                selector.register(app.stdout, selectors.EVENT_READ)
                self.assertTrue(selector.select(30), "fixture startup timed out")
                ports = json.loads(app.stdout.readline())
            cert, key = work / "cert.pem", work / "key.pem"
            subprocess.run(
                [
                    "openssl",
                    "req",
                    "-x509",
                    "-newkey",
                    "rsa:2048",
                    "-nodes",
                    "-days",
                    "1",
                    "-keyout",
                    str(key),
                    "-out",
                    str(cert),
                    "-subj",
                    "/CN=app.invalid",
                    "-addext",
                    "subjectAltName=DNS:app.invalid,DNS:artwork.invalid",
                ],
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=15,
            )
            config = fixture.adapted()
            with socket.socket() as probe:
                probe.bind(("127.0.0.1", 0))
                port = probe.getsockname()[1]
            servers = config["apps"]["http"]["servers"]
            # Test only the HTTPS server. Explicit port80 policy has its own native gate.
            config["apps"]["http"]["servers"] = {
                key: server
                for key, server in servers.items()
                if ":443" in server["listen"]
            }
            self.assertEqual(len(config["apps"]["http"]["servers"]), 1)
            for server in config["apps"]["http"]["servers"].values():
                server["listen"] = [f"127.0.0.1:{port}"]
                server["automatic_https"] = {"disable": True}
                server["tls_connection_policies"] = [{}]
            config["apps"]["tls"] = {
                "certificates": {
                    "load_files": [{"certificate": str(cert), "key": str(key)}]
                }
            }

            def visit(value):
                if isinstance(value, dict):
                    if value.get("dial") == "127.0.0.1:3918":
                        value["dial"] = f"127.0.0.1:{ports['app_port']}"
                    if value.get("dial") == "127.0.0.1:3919":
                        value["dial"] = f"127.0.0.1:{ports['art_port']}"
                    for child in value.values():
                        visit(child)
                elif isinstance(value, list):
                    for child in value:
                        visit(child)

            visit(config)
            runtime = work / "isolation.json"
            runtime.write_text(json.dumps(config))
            with (work / "proxy.log").open("w") as log:
                proxy = subprocess.Popen(
                    [deployment.CADDY, "run", "--config", str(runtime)],
                    stdout=log,
                    stderr=log,
                    env=dict(
                        os.environ,
                        XDG_DATA_HOME=str(work / "data"),
                        XDG_CONFIG_HOME=str(work / "config"),
                    ),
                )
                self.addCleanup(self.stop, proxy)
                deadline = time.monotonic() + 10
                while True:
                    self.assertIsNone(proxy.poll(), "Caddy failed to start")
                    try:
                        with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                            break
                    except OSError:
                        self.assertLess(time.monotonic(), deadline)
                        time.sleep(0.02)
                stopped = threading.Event()
                samples = []

                def sample():
                    while not stopped.wait(0.02):
                        with contextlib.suppress(OSError, KeyError):
                            samples.append(
                                {
                                    "app_kib": fixture.rss_kib(app),
                                    "proxy_kib": fixture.rss_kib(proxy),
                                }
                            )

                sampler = threading.Thread(target=sample)
                sampler.start()
                try:
                    result = subprocess.run(
                        [
                            "node",
                            "tests/proxy_streams.mjs",
                            str(port),
                            ports["art_path"],
                        ],
                        cwd=ROOT,
                        text=True,
                        capture_output=True,
                        timeout=75,
                        check=False,
                    )
                    self.assertEqual(result.returncode, 0, result.stderr)
                    report = {
                        "checks": json.loads(result.stdout),
                        "cgroup_limited": memory_scope()["limited"],
                        "cgroup": memory_scope(),
                        "peak_combined_rss_kib": max(
                            row["app_kib"] + row["proxy_kib"] for row in samples
                        ),
                        "peak_app_rss_kib": max(row["app_kib"] for row in samples),
                        "peak_proxy_rss_kib": max(row["proxy_kib"] for row in samples),
                    }
                    self.assertEqual(report["cgroup"].get("oom_kill", 0), 0)
                    (ROOT / "tmp/caddy-review/http2-isolation.json").write_text(
                        json.dumps(report, indent=2) + "\n"
                    )
                finally:
                    stopped.set()
                    sampler.join(timeout=2)
                self.stop(proxy)
            self.stop(app)

    @staticmethod
    def stop(process):
        if process.poll() is None:
            process.send_signal(signal.SIGTERM)
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        if process.stdout:
            process.stdout.close()
