"""IndexNow sends only reviewed, public, same-host URLs; failures retain the input."""

import json
import unittest
from unittest.mock import patch

import indexnow

SITE = "https://app.invalid"
KEY = "12345678abcdef90" * 4


class IndexNowTests(unittest.TestCase):
    def data(self, urls=None, forum=False):
        return indexnow.payload(SITE, KEY, urls or [SITE + "/projects"], forum=forum)

    def test_canonical_urls_are_deduplicated_and_use_a_root_key(self):
        data = self.data([SITE + "/projects", SITE + "/projects", SITE + "/article/42"])
        self.assertEqual(data["urlList"], [SITE + "/projects", SITE + "/article/42"])
        self.assertEqual(data["keyLocation"], SITE + "/" + KEY + ".txt")

    def test_private_foreign_query_redirect_and_oversized_inputs_are_rejected(self):
        for url in (
            SITE + "/auth",
            SITE + "/api/auth/session",
            SITE + "/health/metrics",
            SITE + "/projects?q=private",
            SITE + "/projects#fragment",
            SITE + "/project/1/full",
            SITE + "/../projects",
            SITE + "/%70rojects",
            SITE + "/.keys/key",
            "https://app.invalid.evil/projects",
            "https://app.invalid@evil.invalid/projects",
            "http://app.invalid/projects",
            "https://app.invalid:443/projects",
            SITE + "/profile/me",
            SITE + "/projects\n",
            SITE + "/d/4",
            SITE + "/sitemap.xml",
        ):
            with self.subTest(url=url), self.assertRaises(indexnow.SubmissionError):
                self.data([url])
        with self.assertRaises(indexnow.SubmissionError):
            self.data([SITE + "/projects"] * 101)

    def test_forum_routes_require_explicit_forum_mode(self):
        self.data([SITE + "/d/1-welcome"], forum=True)
        with self.assertRaises(indexnow.SubmissionError):
            self.data([SITE + "/projects"], forum=True)

    def test_submission_checks_key_and_public_pages_before_posting(self):
        replies = [(200, {}, KEY.encode()), (200, {}, b""), (202, {}, b"")]
        with patch("indexnow.request", side_effect=replies) as request:
            self.assertEqual(indexnow.submit(self.data()), 202)
            calls = request.call_args_list
            self.assertEqual(calls[1].kwargs["method"], "HEAD")
            self.assertEqual(calls[2].args[0], "https://api.indexnow.org/indexnow")
            self.assertEqual(json.loads(calls[2].kwargs["body"]), self.data())

    def test_missing_wrong_key_or_nonpublic_page_prevents_external_submission(self):
        for replies in (
            [(200, {}, b"wrong")],
            [(404, {}, b"")],
            [(200, {}, KEY.encode()), (404, {}, b"")],
            [(200, {}, KEY.encode()), (200, {"x-robots-tag": "noindex, follow"}, b"")],
            [(200, {}, KEY.encode()), (200, {"x-robots-tag": "none"}, b"")],
        ):
            with patch("indexnow.request", side_effect=replies) as request:
                with self.assertRaises(indexnow.SubmissionError):
                    indexnow.submit(self.data())
                self.assertTrue(
                    all(
                        call.args[0] != indexnow.ENDPOINT
                        for call in request.call_args_list
                    )
                )

    def test_deadline_and_retryable_failure_never_claim_success(self):
        with self.assertRaises(indexnow.SubmissionError) as error:
            indexnow.request(SITE, 0)
        self.assertEqual(error.exception.code, 75)
        with patch(
            "indexnow.request", side_effect=indexnow.SubmissionError("retry", 75)
        ):
            with self.assertRaises(indexnow.SubmissionError) as error:
                indexnow.submit(self.data())
            self.assertEqual(error.exception.code, 75)

    def test_http_errors_are_bounded_and_redirects_are_never_followed(self):
        import io
        import urllib.error

        for status in (302, 403, 429, 500):
            error = urllib.error.HTTPError(SITE, status, "fixture", {}, io.BytesIO())
            with patch("urllib.request.OpenerDirector.open", side_effect=error):
                with self.assertRaises(indexnow.SubmissionError) as caught:
                    indexnow.request(SITE, indexnow.time.monotonic() + 10)
                self.assertEqual(caught.exception.code, 75 if status >= 429 else 64)
        self.assertIsNone(
            indexnow.NoRedirect().redirect_request(
                None, None, 302, "", {}, "https://other.invalid"
            )
        )


if __name__ == "__main__":
    unittest.main()
