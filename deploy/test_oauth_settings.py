"""Focused tests for optional OAuth deployment settings."""

import unittest

import oauth_settings


class OAuthSettingsTests(unittest.TestCase):
    def valid(self):
        return {
            "OAUTH_CLIENT_ID": "forum-client_1",
            "OAUTH_CLIENT_SECRET": "0123456789abcdef",
            "OAUTH_REDIRECT_URI": "https://forum.example/auth/seeded",
        }

    def test_absent_settings_are_inserted_and_disabled(self):
        values = {}
        oauth_settings.validate(values)
        self.assertEqual(values, {key: "" for key in oauth_settings.SETTINGS})
        self.assertEqual(oauth_settings.environment({}), "")

    def test_all_or_none_treats_whitespace_as_unset(self):
        for missing in oauth_settings.SETTINGS:
            values = self.valid()
            values[missing] = " \t "
            with (
                self.subTest(missing=missing),
                self.assertRaisesRegex(ValueError, "set all OAuth settings or none"),
            ):
                oauth_settings.validate(values)
        values = {key: " \t " for key in oauth_settings.SETTINGS}
        oauth_settings.validate(values)

    def test_client_id_byte_limits_and_character_set_match_rust(self):
        for value in ("a" * 129, "has space", "é"):
            values = self.valid()
            values["OAUTH_CLIENT_ID"] = value
            with (
                self.subTest(value=value),
                self.assertRaisesRegex(ValueError, "invalid OAuth client ID"),
            ):
                oauth_settings.validate(values)
        values = self.valid()
        values["OAUTH_CLIENT_ID"] = "a" * 128
        oauth_settings.validate(values)

    def test_secret_byte_limits_and_control_characters_match_rust(self):
        for value in ("short", "s" * 513, "s" * 15 + "\x85"):
            values = self.valid()
            values["OAUTH_CLIENT_SECRET"] = value
            with (
                self.subTest(length=len(value)),
                self.assertRaisesRegex(ValueError, "invalid OAuth client secret"),
            ):
                oauth_settings.validate(values)
        for value in ("s" * 16, "s" * 512):
            values = self.valid()
            values["OAUTH_CLIENT_SECRET"] = value
            oauth_settings.validate(values)

    def test_redirect_requires_https_or_loopback_http_and_rejects_unsafe_parts(self):
        for value in (
            "http://forum.example/callback",
            "https://user:pass@forum.example/callback",
            "https://forum.example/callback#",
            "https://forum.example/callback\\unsafe",
            "https://forum.example:bad/callback",
            "https://forum.example/\x7fcallback",
            "https://forum.example/" + "a" * 500,
        ):
            values = self.valid()
            values["OAUTH_REDIRECT_URI"] = value
            with (
                self.subTest(uri=value),
                self.assertRaisesRegex(ValueError, "invalid OAuth redirect URI"),
            ):
                oauth_settings.validate(values)
        for value in (
            "https://forum.example/callback",
            "http://localhost:3918/callback",
            "http://[::1]:3918/callback",
        ):
            values = self.valid()
            values["OAUTH_REDIRECT_URI"] = value
            oauth_settings.validate(values)

    def test_errors_never_include_secret_values(self):
        secret = "do-not-report-this-secret"
        values = self.valid()
        values["OAUTH_CLIENT_SECRET"] = secret
        values["OAUTH_CLIENT_ID"] = "invalid id"
        with self.assertRaises(ValueError) as error:
            oauth_settings.validate(values)
        self.assertNotIn(secret, str(error.exception))

    def test_environment_quotes_shell_and_systemd_metacharacters_literally(self):
        values = self.valid()
        values["OAUTH_CLIENT_SECRET"] = 'ab"cd\\ef$g%h123456'
        rendered = oauth_settings.environment(values)
        self.assertEqual(
            rendered.splitlines(),
            [
                'SEEDED_OAUTH_CLIENT_ID="forum-client_1"',
                'SEEDED_OAUTH_CLIENT_SECRET="ab\\"cd\\\\ef$g%h123456"',
                'SEEDED_OAUTH_REDIRECT_URI="https://forum.example/auth/seeded"',
            ],
        )
        self.assertNotIn(values["OAUTH_CLIENT_SECRET"], rendered)

    def test_control_characters_are_never_rendered(self):
        values = self.valid()
        values["OAUTH_CLIENT_SECRET"] = "s" * 16 + "\n"
        with self.assertRaises(ValueError) as error:
            oauth_settings.environment(values)
        self.assertNotIn(values["OAUTH_CLIENT_SECRET"], str(error.exception))


if __name__ == "__main__":
    unittest.main()
