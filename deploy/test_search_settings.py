"""Crawler exclusions and verification files on their own host only."""

import unittest
import urllib.robotparser

import configure
import search_settings
import test_deployment as deployment

KEY = "a1b2c3d4" * 8
FORUM_KEY = "f5e6d7c8" * 8


class SearchSettingsTests(unittest.TestCase):
    def setUp(self):
        self.fixture = deployment.DeploymentTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)

    def test_every_named_crawler_keeps_auth_and_artwork_exclusions(self):
        robots = search_settings.robots("https://app.invalid")
        policy = urllib.robotparser.RobotFileParser()
        policy.parse(robots.splitlines())
        agents = ["bingbot", "unknown"] + [
            agent for _, agents in search_settings.GROUPS for agent in agents
        ]
        for agent in agents:
            for path in ("/", "/projects", "/article/10", "/profile/artist"):
                self.assertTrue(policy.can_fetch(agent, path), (agent, path))
            for path in (
                "/auth",
                "/api/auth/challenge",
                "/api/market/prepare",
                "/health/ready",
                "/oauth/token",
            ):
                self.assertFalse(policy.can_fetch(agent, path), (agent, path))
        # Python's basic parser does not implement REP wildcard matching.
        self.assertEqual(robots.count("Disallow: /token/*/full"), 4)
        self.assertEqual(policy.site_maps(), ["https://app.invalid/sitemap.xml"])

    def test_forum_keeps_its_existing_allow_policy(self):
        text = search_settings.robots("https://forum.invalid", forum=True)
        policy = urllib.robotparser.RobotFileParser()
        policy.parse(text.splitlines())
        self.assertTrue(policy.can_fetch("GPTBot", "/d/1"))
        self.assertNotIn("Sitemap:", text)  # No forum sitemap exists yet.

    def test_invalid_tokens_and_forum_settings_without_forum_are_rejected(self):
        for key, value in (
            ("INDEXNOW_KEY", "short"),
            ("INDEXNOW_KEY", "../12345678"),
            ("INDEXNOW_KEY", "x" * 129),
            ("INDEXNOW_KEY", 'abcdefgh"\n}'),
            ("FORUM_INDEXNOW_KEY", FORUM_KEY),
        ):
            with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                self.fixture.settings({**deployment.VALID, key: value})

    def test_search_console_settings_are_refused(self):
        for key in (
            "BING_SITE_VERIFICATION",
            "FORUM_BING_SITE_VERIFICATION",
            "GOOGLE_SITE_VERIFICATION",
        ):
            with self.subTest(key=key), self.assertRaises(ValueError):
                self.fixture.settings({**deployment.VALID, key: "ABCDEF0123456789" * 2})

    def test_optional_routes_are_host_specific_and_answer_without_upstreams(self):
        values = self.fixture.settings(
            {
                **deployment.VALID,
                "INDEXNOW_KEY": KEY,
                "FORUM_HOST": "forum.invalid",
                "FORUM_ROOT": "/var/www/forum/public",
                "FORUM_PHP_SOCKET": "/run/php/php8.4-fpm.sock",
                "FORUM_INDEXNOW_KEY": FORUM_KEY,
            }
        )
        configure.render(values, self.fixture.directory / "rendered")
        upstream, records, _ = self.fixture.upstream()
        with self.fixture.running_proxy(upstream) as (port, _):
            for host, forum in (("app.invalid", False), ("forum.invalid", True)):
                status, headers, body = self.fixture.request_proxy(
                    port, host, "/robots.txt"
                )
                self.assertEqual(status, 200)
                self.assertEqual(
                    body.decode(),
                    search_settings.robots(f"https://{host}", forum=forum),
                )
                self.assertEqual(headers["Cache-Control"], "public, max-age=300")
            for host, key in (("app.invalid", KEY), ("forum.invalid", FORUM_KEY)):
                status, headers, body = self.fixture.request_proxy(
                    port, host, f"/{key}.txt"
                )
                self.assertEqual((status, body), (200, key.encode()))
                self.assertTrue(headers["Content-Type"].startswith("text/plain"))
                self.assertNotIn("Set-Cookie", headers)
                self.assertNotIn("Server", headers)
            self.assertEqual(records["requests"], [])
            self.assertEqual(
                self.fixture.request_proxy(port, "app.invalid", f"/{KEY}.txt", "POST")[
                    0
                ],
                405,
            )
            self.assertEqual(
                self.fixture.request_proxy(port, "app.invalid", f"/{KEY}.txt", "HEAD")[
                    2
                ],
                b"",
            )
            for host, path in (
                ("app.invalid", f"/{FORUM_KEY}.txt"),
                ("artwork.invalid", f"/{KEY}.txt"),
            ):
                self.fixture.request_proxy(port, host, path)
                self.assertEqual(records["requests"][-1][0], path)

    def test_unconfigured_keys_do_not_create_verification_routes(self):
        routes = search_settings.routes(self.fixture.settings(deployment.VALID))
        self.assertNotIn("BingSiteAuth", routes)
        self.assertNotIn(KEY, routes)


if __name__ == "__main__":
    unittest.main()
