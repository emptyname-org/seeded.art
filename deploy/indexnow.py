"""Preview or submit up to 100 public page URLs to IndexNow. No Google integration."""

import argparse
import http.client
import json
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

from search_settings import valid_key

ENDPOINT = "https://api.indexnow.org/indexnow"
MAX_URLS = 100
DEADLINE = 120.0
ADDRESS = r"(?:tz[1-4]|KT1)[1-9A-HJ-NP-Za-km-z]{33}"
APP_PATH = re.compile(
    rf"/(?:projects|artists|collectors|articles|about|project/[0-9]+|article/[0-9]+|"
    rf"profile/{ADDRESS}|token/KT1[1-9A-HJ-NP-Za-km-z]{{33}}/[0-9]+)?"
)
FORUM_PATH = re.compile(r"/(?:d/[0-9]+(?:-[a-z0-9-]+)?)?")


class SubmissionError(Exception):
    def __init__(self, message: str, code: int = 64):
        super().__init__(message)
        self.code = code


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[no-untyped-def]
        return None


def origin(value: str) -> str:
    parsed = urllib.parse.urlsplit(value)
    host = parsed.hostname or ""
    if (
        parsed.scheme != "https"
        or parsed.netloc != host
        or not re.fullmatch(r"[a-z0-9]+(?:[.-][a-z0-9]+)+", host)
        or parsed.path not in ("", "/")
        or parsed.query
        or parsed.fragment
        or value not in (f"https://{host}", f"https://{host}/")
    ):
        raise SubmissionError("site must be a bare HTTPS origin")
    return f"https://{host}"


def payload(
    site: str, key: str, urls: list[str], *, forum: bool = False
) -> dict[str, object]:
    site = origin(site)
    if not valid_key(key):
        raise SubmissionError("invalid IndexNow key")
    if not 1 <= len(urls) <= MAX_URLS:
        raise SubmissionError("provide 1 to 100 URLs per submission")
    allowed = FORUM_PATH if forum else APP_PATH
    unique = []
    for url in urls:
        parsed = urllib.parse.urlsplit(url)
        if (
            not url.startswith(site + "/")
            or parsed.scheme != "https"
            or parsed.netloc != site.removeprefix("https://")
            or parsed.query
            or parsed.fragment
            or not allowed.fullmatch(parsed.path)
            or url != site + parsed.path
        ):
            raise SubmissionError(
                "only canonical public pages on the selected site may be submitted"
            )
        if url not in unique:
            unique.append(url)
    return {
        "host": site.removeprefix("https://"),
        "key": key,
        "keyLocation": f"{site}/{key}.txt",
        "urlList": unique,
    }


def read_small(path: Path, limit: int) -> str:
    with path.open("rb") as source:
        data = source.read(limit + 1)
    if len(data) > limit:
        raise SubmissionError("input file exceeds its size limit")
    return data.decode("utf-8")


def request(
    url: str, deadline: float, *, method: str = "GET", body: bytes | None = None
) -> tuple[int, dict[str, str], bytes]:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise SubmissionError("submission deadline reached; retry the same URLs", 75)
    headers = {"User-Agent": "seeded-indexnow/1.0"}
    if body is not None:
        headers["Content-Type"] = "application/json; charset=utf-8"
    req = urllib.request.Request(url, data=body, headers=headers, method=method)
    # No ambient proxy credentials, cookies, redirects or account authentication.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
    try:
        with opener.open(req, timeout=min(10.0, remaining)) as response:
            result = response.read(4097)
            if len(result) > 4096:
                raise SubmissionError("verification response exceeds its size limit")
            return (
                response.status,
                {k.lower(): v for k, v in response.headers.items()},
                result,
            )
    except urllib.error.HTTPError as error:
        code = error.code
        error.close()
        if code == 429 or code >= 500:
            raise SubmissionError(
                f"HTTP {code}; retry later with the same URLs", 75
            ) from None
        raise SubmissionError(f"HTTP {code}; submission stopped") from None
    except (urllib.error.URLError, TimeoutError, OSError, http.client.HTTPException):
        raise SubmissionError(
            "network unavailable; retry later with the same URLs", 75
        ) from None


def submit(data: dict[str, object]) -> int:
    deadline = time.monotonic() + DEADLINE
    status, _, key = request(str(data["keyLocation"]), deadline)
    if status != 200 or key.decode("utf-8").strip() != data["key"]:
        raise SubmissionError("hosted IndexNow key does not match; nothing submitted")
    urls = data["urlList"]
    assert isinstance(urls, list)
    for url in urls:
        status, headers, _ = request(str(url), deadline, method="HEAD")
        if status != 200 or re.search(
            r"\b(?:noindex|none)\b", headers.get("x-robots-tag", ""), re.IGNORECASE
        ):
            raise SubmissionError("page is not publicly indexable; nothing submitted")
    status, _, _ = request(
        ENDPOINT, deadline, method="POST", body=json.dumps(data).encode()
    )
    if status not in (200, 202):
        raise SubmissionError("IndexNow did not acknowledge the submission", 75)
    return status


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--site", required=True)
    parser.add_argument("--key-file", required=True, type=Path)
    parser.add_argument("--urls-file", required=True, type=Path)
    parser.add_argument("--forum", action="store_true")
    parser.add_argument(
        "--submit",
        action="store_true",
        help="send after public key and page checks; default previews only",
    )
    args = parser.parse_args()
    try:
        key = read_small(args.key_file, 256).strip()
        urls = read_small(args.urls_file, 64 * 1024).splitlines()
        data = payload(args.site, key, urls, forum=args.forum)
        if not args.submit:
            print(json.dumps(data, indent=2))
            return 0
        status = submit(data)
        print(
            "URLs received; key verification pending."
            if status == 202
            else "URLs received. Indexing is not guaranteed."
        )
        return 0
    except SubmissionError as error:
        print(str(error), file=sys.stderr)
        return error.code
    except (OSError, UnicodeError, ValueError):
        print("invalid or unreadable input", file=sys.stderr)
        return 64


if __name__ == "__main__":
    sys.exit(main())
