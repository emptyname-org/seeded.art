"""The forum extension's PHP: syntax, JSON sign-in helpers and page metadata."""

import base64
import json
import shutil
import subprocess
import unittest
from html.parser import HTMLParser
from pathlib import Path

FORUM = Path(__file__).resolve().parent / "forum"
PHP = shutil.which("php")
# Flarum 1.8 ResponseFactory::makeResponse, as rendered for a new address.
POPUP = (
    "<script>window.close(); window.opener.app.authenticationComplete("
    '{"email":"tz1abc@seeded.invalid","username":"tz1abc","token":"t0k\\/en",'
    '"provided":["email"]});</script>'
)
DISCUSSION = 'Fees "&" <royalties>'
FORUM_ATTRIBUTES = {
    "title": "seeded.art forum",
    "description": "Talk\n about  editions",
}
CANONICAL = "https://forum.seeded.art/d/12-fees"
# The rendered first post; the escaped script is text a member typed.
FIRST_POST = (
    "<p>Hello <strong>wor</strong>ld &amp; friends</p>\n<p>"
    + "lorem ipsum " * 15
    + "&lt;/script&gt;&lt;script&gt;alert(1)&lt;/script&gt;</p>"
    + "<script>dropped()</script>"
)
FIRST_POST_TEXT = (
    "Hello world & friends "
    + "lorem ipsum " * 15
    + "</script><script>alert(1)</script>"
)
# A plain object standing in for Flarum's Frontend\Document.
FAKE_DOCUMENT = """
$doc = new class {
    public $title; public $canonicalUrl; public $page; public $payload = [];
    public $meta = ['description' => 'core description']; public $head = [];
    public $forum = [];
    public function getForumApiDocument(): array { return $this->forum; }
    public static function setPageParam($url, $page) {
        if ($page === 'fail') { throw new RuntimeException('broken'); }
        return $page > 1 ? "$url?page=$page" : $url;
    }
};
"""


def php(code: str, source: str = "SeededJson.php") -> str:
    assert PHP is not None
    script = f"require {json.dumps(str(FORUM / 'seeded' / source))};\n{code}"
    return subprocess.run(
        [PHP, "-r", script], check=True, capture_output=True, text=True, timeout=30
    ).stdout


def value(data: object, assoc: bool = True) -> str:
    """A PHP expression decoding data; assoc=False keeps objects as stdClass."""
    encoded = base64.b64encode(json.dumps(data).encode()).decode()
    return f"json_decode(base64_decode('{encoded}'), {'true' if assoc else 'false'})"


def seo(method: str, *args: object) -> object:
    call = ", ".join(value(arg) for arg in args)
    code = f"echo json_encode(Seeded\\Forum\\SeededSeo::{method}({call}));"
    return json.loads(php(code, "SeededSeo.php"))


def first_post(html: str = FIRST_POST, discussion: str = "12", **extra: object) -> dict:
    return {
        "type": "posts",
        "id": "30",
        "attributes": {
            "number": 1,
            "contentType": "comment",
            "contentHtml": html,
            **extra,
        },
        "relationships": {
            "discussion": {"data": {"type": "discussions", "id": discussion}}
        },
    }


def discussion_api(posts: list[dict]) -> dict:
    return {
        "data": {
            "type": "discussions",
            "id": "12",
            "attributes": {
                "title": DISCUSSION,
                "slug": "12-fees",
                "commentCount": 4,
                "createdAt": "2026-01-02T03:04:05+00:00",
            },
            "relationships": {"user": {"data": {"type": "users", "id": "7"}}},
        },
        "included": [
            {
                "type": "users",
                "id": "7",
                "attributes": {"username": "tz1abc", "displayName": "Alice"},
            },
            *posts,
        ],
    }


def render(path: str, route: object, api: object = None, **document: object) -> dict:
    """Attach, fill the document as Flarum's route content does, render the head."""
    fill = "".join(f"$doc->{key} = {value(item)};\n" for key, item in document.items())
    code = (
        FAKE_DOCUMENT
        + f"$doc->forum = {value({'data': {'attributes': FORUM_ATTRIBUTES}})};\n"
        + f"Seeded\\Forum\\SeededSeo::attach($doc, {value(path)}, {value(route)});\n"
        + f"$doc->canonicalUrl = {value(CANONICAL)};\n"
        + f"$doc->payload['apiDocument'] = {value(api, assoc=False)};\n"
        + fill
        + "echo json_encode(['meta' => $doc->meta, 'head' => implode(\"\\n\", $doc->head)]);"
    )
    result: dict = json.loads(php(code, "SeededSeo.php"))
    return result


class HeadTags(HTMLParser):
    def __init__(self, html: str) -> None:
        super().__init__()
        self.tags: list[tuple[str, str]] = []
        self.scripts: list[str] = []
        self.in_script = False
        self.feed(html)

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        found = dict(attrs)
        if tag == "meta":
            key = found.get("name") or found.get("property") or ""
            self.tags.append((key, found.get("content") or ""))
        self.in_script = tag == "script"

    def handle_endtag(self, tag: str) -> None:
        self.in_script = False

    def handle_data(self, data: str) -> None:
        if self.in_script:
            self.scripts.append(data)


@unittest.skipIf(PHP is None, "php is not installed")
class ForumExtensionTests(unittest.TestCase):
    def test_every_php_file_parses(self) -> None:
        files = sorted(FORUM.rglob("*.php"))
        self.assertEqual(
            sorted(path.name for path in files),
            ["SeededAuth.php", "SeededJson.php", "SeededSeo.php", "extend.php"],
        )
        for path in files:
            assert PHP is not None
            subprocess.run(
                [PHP, "-l", str(path)], check=True, capture_output=True, timeout=30
            )

    def test_only_clients_asking_for_json_get_json(self) -> None:
        cases = {
            "application/json": True,
            "Application/JSON; charset=utf-8": True,
            "text/html,application/xhtml+xml,*/*;q=0.8": False,
            "": False,
        }
        for accept, expected in cases.items():
            out = php(
                f"echo json_encode(Seeded\\Forum\\SeededJson::wanted({json.dumps(accept)}));"
            )
            self.assertEqual(json.loads(out), expected, accept)

    def test_popup_payload_becomes_the_json_result(self) -> None:
        out = php(
            f"echo json_encode(Seeded\\Forum\\SeededJson::payload({json.dumps(POPUP)}));"
        )
        self.assertEqual(
            json.loads(out),
            {
                "email": "tz1abc@seeded.invalid",
                "username": "tz1abc",
                "token": "t0k/en",
                "provided": ["email"],
            },
        )
        logged_in = (
            "<script>window.close(); window.opener.app.authenticationComplete("
            '{"loggedIn":true});</script>'
        )
        out = php(
            f"echo json_encode(Seeded\\Forum\\SeededJson::payload({json.dumps(logged_in)}));"
        )
        self.assertEqual(json.loads(out), {"loggedIn": True})

    def test_other_pages_yield_no_payload(self) -> None:
        for html in ["<p>Sign-in expired.</p>", POPUP + "<p>extra</p>", ""]:
            out = php(
                f"var_export(Seeded\\Forum\\SeededJson::payload({json.dumps(html)}));"
            )
            self.assertEqual(out, "NULL", html)

    def test_plain_text_drops_markup_and_collapses_whitespace(self) -> None:
        html = (
            "<p>One <strong>wo</strong>rd &amp;\n\t more</p><p>Two&nbsp;&nbsp;lines<br>"
            "end</p><script>x()</script><style>p{}</style>"
        )
        self.assertEqual(seo("plain", html), "One word & more Two lines end")
        self.assertEqual(seo("plain", FIRST_POST), FIRST_POST_TEXT)

    def test_excerpt_cuts_long_text_at_a_word_boundary(self) -> None:
        short = "a" * 160
        self.assertEqual(seo("excerpt", short), short)
        text = "lorem ipsum, " * 20
        cut = seo("excerpt", text.strip())
        assert isinstance(cut, str)
        self.assertLessEqual(len(cut), 160)
        self.assertTrue(cut.endswith("ipsum…"), cut)
        self.assertTrue(text.startswith(cut[:-1] + ", "))
        self.assertEqual(seo("excerpt", "é" * 200), "é" * 159 + "…")

    def test_attribute_values_are_escaped(self) -> None:
        self.assertEqual(seo("attr", "\"<a>&'"), "&quot;&lt;a&gt;&amp;&#039;")
        self.assertEqual(
            seo("meta", "description", 'x"><script>'),
            '<meta name="description" content="x&quot;&gt;&lt;script&gt;">',
        )

    def test_json_ld_cannot_close_its_script(self) -> None:
        data = {"text": "</script><script>alert(1)</script> & <!--"}
        out = seo("jsonLd", data)
        assert isinstance(out, str)
        prefix, suffix = '<script type="application/ld+json">', "</script>"
        self.assertTrue(out.startswith(prefix) and out.endswith(suffix))
        inner = out[len(prefix) : -len(suffix)]
        for char in "<>&":
            self.assertNotIn(char, inner)
        self.assertIn("\\u003C/script\\u003E", inner)
        self.assertEqual(json.loads(inner), data)

    def test_discussion_page_gets_excerpt_open_graph_and_posting(self) -> None:
        other = first_post("<p>Quoted from another discussion</p>", discussion="5")
        api = discussion_api([first_post(), other])
        out = render("/d/12-fees", "discussion", api, page=1)
        self.assertNotIn("description", out["meta"])
        head = HeadTags(out["head"])
        excerpt = seo("excerpt", FIRST_POST_TEXT)
        self.assertEqual(
            head.tags,
            [
                ("description", excerpt),
                ("og:site_name", "seeded.art forum"),
                ("og:type", "article"),
                ("og:title", DISCUSSION),
                ("og:description", excerpt),
                ("og:url", CANONICAL),
                ("twitter:card", "summary"),
            ],
        )
        self.assertNotIn("<royalties>", out["head"])
        self.assertEqual(out["head"].count("</script>"), 1)
        self.assertEqual(
            json.loads(head.scripts[0]),
            {
                "@context": "https://schema.org",
                "@type": "DiscussionForumPosting",
                "headline": DISCUSSION,
                "url": CANONICAL,
                "datePublished": "2026-01-02T03:04:05+00:00",
                "author": {"@type": "Person", "name": "Alice"},
                "text": FIRST_POST_TEXT,
                "interactionStatistic": {
                    "@type": "InteractionCounter",
                    "interactionType": "https://schema.org/CommentAction",
                    "userInteractionCount": 3,
                },
            },
        )

    def test_discussion_page_without_its_first_post_uses_forum_description(
        self,
    ) -> None:
        cases = {
            "later page": [],
            "hidden": [first_post(isHidden=True)],
            "render failed": [first_post(renderFailed=True)],
            "other discussion": [first_post(discussion="5")],
            "no text": [first_post("<p><img src=x></p>")],
        }
        for case, posts in cases.items():
            out = render("/d/12-fees", "discussion", discussion_api(posts), page=2)
            head = HeadTags(out["head"])
            tags = dict(head.tags)
            self.assertEqual(tags["description"], "Talk about editions", case)
            self.assertEqual(tags["og:url"], CANONICAL + "?page=2", case)
            self.assertEqual(head.scripts, [], case)

    def test_home_page_gets_forum_description_and_website_tags(self) -> None:
        out = render("/", "default")
        self.assertEqual(out["meta"]["description"], "Talk about editions")
        self.assertEqual(
            HeadTags(out["head"]).tags,
            [
                ("og:site_name", "seeded.art forum"),
                ("og:type", "website"),
                ("og:title", "seeded.art forum"),
                ("og:description", "Talk about editions"),
                ("og:url", CANONICAL),
                ("twitter:card", "summary"),
            ],
        )

    def test_home_page_without_a_forum_description_uses_the_fallback(self) -> None:
        code = (
            FAKE_DOCUMENT
            + "Seeded\\Forum\\SeededSeo::attach($doc, '', 'default');"
            + "echo json_encode($doc->meta);"
        )
        meta = json.loads(php(code, "SeededSeo.php"))
        self.assertEqual(meta["description"], "seeded.art forum")

    def test_other_pages_are_left_alone(self) -> None:
        out = render("/u/alice", "user")
        self.assertEqual(out["meta"], {"description": "core description"})
        self.assertEqual(out["head"], "")

    def test_failed_rendering_keeps_one_description(self) -> None:
        api = discussion_api([first_post()])
        out = render("/d/12-fees", "discussion", api, page="fail")
        self.assertEqual(
            out["head"], '<meta name="description" content="Talk about editions">'
        )
        self.assertEqual(render("/", "default", page="fail")["head"], "")
        out = render("/d/12-fees", "discussion", "not a document")
        self.assertEqual(
            HeadTags(out["head"]).tags, [("description", "Talk about editions")]
        )


if __name__ == "__main__":
    unittest.main()
