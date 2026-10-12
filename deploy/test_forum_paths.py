"""Forum path isolation in rendered systemd units."""

import unittest

import configure
import test_deployment as deployment


class ForumPathTests(unittest.TestCase):
    # Reuse the deployment suite's isolated project-local temporary directory.
    setUp = deployment.DeploymentTests.setUp

    def render_units(self, forum_root="/var/www/forum/public"):
        values = {
            **deployment.VALID,
            "FORUM_HOST": "forum.invalid",
            "FORUM_ROOT": forum_root,
            "FORUM_PHP_SOCKET": "/run/php/forum.sock",
        }
        destination = self.directory / "rendered"
        configure.render(deployment.DeploymentTests.settings(self, values), destination)
        return {
            name: (destination / name).read_text()
            for name in (
                "seeded.service",
                "seeded-app.service",
                "seeded-content.service",
                "seeded-proxy.service",
            )
        }

    def test_configured_forum_hides_private_siblings_from_both_services(self):
        units = self.render_units()
        expected = (
            "InaccessiblePaths=-/var/www/forum/config.php -/var/www/forum/storage"
        )
        for service in units.values():
            with self.subTest(service=service.splitlines()[1]):
                self.assertIn(expected, service)
                self.assertIn("ProtectSystem=strict", service)
                self.assertIn("ProtectHome=true", service)

        proxy = units["seeded-proxy.service"]
        for name in ("seeded.service", "seeded-app.service", "seeded-content.service"):
            self.assertNotIn("SupplementaryGroups=www-data", units[name])
            self.assertNotIn("ReadOnlyPaths=/var/www/forum/public", units[name])
        self.assertIn("SupplementaryGroups=www-data", proxy)
        self.assertIn("ReadOnlyPaths=/var/www/forum/public", proxy)

    def test_forum_private_siblings_follow_normalized_public_path_override(self):
        units = self.render_units("/srv/community/web")
        expected = (
            "InaccessiblePaths=-/srv/community/config.php -/srv/community/storage"
        )
        for service in units.values():
            self.assertIn(expected, service)
        self.assertIn("ReadOnlyPaths=/srv/community/web", units["seeded-proxy.service"])

    def test_absent_forum_adds_no_forum_paths_or_proxy_access(self):
        destination = self.directory / "without-forum"
        configure.render(
            deployment.DeploymentTests.settings(self, deployment.VALID), destination
        )
        apps = [
            (destination / name).read_text()
            for name in (
                "seeded.service",
                "seeded-app.service",
                "seeded-content.service",
            )
        ]
        proxy = (destination / "seeded-proxy.service").read_text()
        for service in (*apps, proxy):
            self.assertNotIn("InaccessiblePaths=", service)
            self.assertIn("ProtectSystem=strict", service)
            self.assertIn("ProtectHome=true", service)
        for app in apps:
            self.assertNotIn("SupplementaryGroups=www-data", app)
            self.assertNotIn("ReadOnlyPaths=", app)
        self.assertNotIn("SupplementaryGroups=www-data", proxy)
        self.assertNotIn("ReadOnlyPaths=", proxy)
