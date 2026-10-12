#!/usr/bin/env python3
"""Render reviewed deployment settings without shell interpolation or host changes."""

import argparse
import hashlib
import ipaddress
import json
import re
from pathlib import Path
from typing import cast

import oauth_settings
import search_settings

TEMPLATES = {
    "seeded-profiles.service.in": "seeded-profiles.service",
    "seeded-profiles.timer.in": "seeded-profiles.timer",
    "seeded-sales.service.in": "seeded-sales.service",
    "seeded-sales.timer.in": "seeded-sales.timer",
    "seeded-articles.service.in": "seeded-articles.service",
    "seeded-articles.timer.in": "seeded-articles.timer",
    "seeded.env.in": "seeded.env",
    "seeded.service.in": "seeded.service",
    "seeded-app.service.in": "seeded-app.service",
    "seeded-content.service.in": "seeded-content.service",
    "seeded-proxy.service.in": "seeded-proxy.service",
    "Caddyfile.in": "Caddyfile",
    "seeded-sync.service.in": "seeded-sync.service",
    "seeded-sync.timer.in": "seeded-sync.timer",
    "seeded-sync.path.in": "seeded-sync.path",
    "seeded-verify.service.in": "seeded-verify.service",
    "seeded-verify.timer.in": "seeded-verify.timer",
    "seeded-backup.service.in": "seeded-backup.service",
    "seeded-backup.timer.in": "seeded-backup.timer",
    "seeded-monitor.service.in": "seeded-monitor.service",
}
FORUM_TEMPLATES = {
    "seeded-forum-backup.service.in": "seeded-forum-backup.service",
    "seeded-forum-backup.timer.in": "seeded-forum-backup.timer",
}
OPTIONAL = {
    "ACME_EMAIL",
    "JOB_MEMORY_MAX",
    "JOB_CPU_QUOTA",
    "JOB_TASKS_MAX",
    "MIRROR",
    "MIRROR_BYTES",
    "CONTENT_CONCURRENCY",
    "MAX_OBJECT_BYTES",
    "CONTENT_HOST_REDIRECTS",
    "FORUM_HOST",
    "FORUM_ROOT",
    "FORUM_PHP_SOCKET",
    "PLATFORM_FEE_ADDRESS",
    "PROXY_OPEN_FILES",
}
# seeded-content.service budget; each defaults to its APP_* value.
CONTENT_BUDGET = {
    "CONTENT_MEMORY_HIGH": "APP_MEMORY_HIGH",
    "CONTENT_MEMORY_MAX": "APP_MEMORY_MAX",
    "CONTENT_CPU_QUOTA": "APP_CPU_QUOTA",
    "CONTENT_TASKS_MAX": "APP_TASKS_MAX",
    "CONTENT_OPEN_FILES": "APP_OPEN_FILES",
}
OPTIONAL |= set(CONTENT_BUDGET)
OPTIONAL |= oauth_settings.SETTINGS
OPTIONAL |= search_settings.SETTINGS
KEYS = {
    "APP_HOST",
    "CONTENT_HOST",
    "APP_MEMORY_HIGH",
    "APP_MEMORY_MAX",
    "APP_CPU_QUOTA",
    "APP_TASKS_MAX",
    "APP_OPEN_FILES",
    "PROXY_MEMORY_MAX",
}


BASE58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def fee_address(address: str) -> bool:
    """Base58Check tz1/tz2/tz3/KT1, as the app checks it (src/trading/types.rs)."""
    if len(address) != 36 or any(character not in BASE58 for character in address):
        return False
    number = 0
    for character in address:
        number = number * 58 + BASE58.index(character)
    raw = number.to_bytes(27, "big")
    payload, checksum = raw[:-4], raw[-4:]
    if hashlib.sha256(hashlib.sha256(payload).digest()).digest()[:4] != checksum:
        return False
    return payload[:3] in (
        bytes([6, 161, 159]),
        bytes([6, 161, 161]),
        bytes([6, 161, 164]),
        bytes([2, 90, 121]),
    )


def check_hostname(key: str, host: str) -> None:
    if (
        len(host) > 253
        or "." not in host
        or not all(
            re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", label)
            for label in host.split(".")
        )
    ):
        raise ValueError(f"{key} must be a lowercase DNS hostname")
    try:
        ipaddress.ip_address(host)
    except ValueError:
        pass
    else:
        raise ValueError(f"{key} must not be an IP address")


def settings(path: Path) -> dict[str, str]:
    parsed = json.loads(path.read_text())
    if (
        not isinstance(parsed, dict)
        or not KEYS.issubset(parsed)
        or set(parsed) - KEYS - OPTIONAL
        or any(not isinstance(value, str) for value in parsed.values())
    ):
        raise ValueError("settings require the documented string fields")
    values = cast(dict[str, str], parsed)
    values.setdefault("ACME_EMAIL", "")
    values.setdefault("MIRROR", "/var/lib/seeded/mirror")
    values.setdefault("MIRROR_BYTES", "53687091200")
    values.setdefault("CONTENT_CONCURRENCY", "4")
    values.setdefault("MAX_OBJECT_BYTES", str(32 * 1024**2))
    values.setdefault("PLATFORM_FEE_ADDRESS", "")
    values.setdefault("PROXY_OPEN_FILES", "16384")
    # Empty keeps offer accepts disabled; the app re-checks a KT1 recipient on chain.
    if values["PLATFORM_FEE_ADDRESS"] and not fee_address(
        values["PLATFORM_FEE_ADDRESS"]
    ):
        raise ValueError(
            "PLATFORM_FEE_ADDRESS must be empty or a tz1/tz2/tz3/KT1 address"
        )
    forum_keys = ("FORUM_HOST", "FORUM_ROOT", "FORUM_PHP_SOCKET")
    for key in forum_keys:
        values.setdefault(key, "")
    provided = [bool(values[key]) for key in forum_keys]
    if any(provided) != all(provided):
        raise ValueError("set FORUM_HOST, FORUM_ROOT and FORUM_PHP_SOCKET together")
    forum = all(provided)
    oauth_settings.validate(values)
    search_settings.validate(values)
    for optional, fallback in (
        ("JOB_MEMORY_MAX", "APP_MEMORY_MAX"),
        ("JOB_CPU_QUOTA", "APP_CPU_QUOTA"),
        ("JOB_TASKS_MAX", "APP_TASKS_MAX"),
        *CONTENT_BUDGET.items(),
    ):
        values.setdefault(optional, values[fallback])
    for key in ("APP_HOST", "CONTENT_HOST", *(("FORUM_HOST",) if forum else ())):
        check_hostname(key, values[key])
    # Old or alias artwork hosts that redirect to CONTENT_HOST.
    redirects = values.setdefault("CONTENT_HOST_REDIRECTS", "").split()
    for host in redirects:
        check_hostname("CONTENT_HOST_REDIRECTS", host)
    if len(set(redirects)) != len(redirects):
        raise ValueError("CONTENT_HOST_REDIRECTS must not repeat a host")
    if {values["APP_HOST"], values["CONTENT_HOST"], values["FORUM_HOST"]} & set(
        redirects
    ):
        raise ValueError(
            "CONTENT_HOST_REDIRECTS must differ from app, artwork and forum hosts"
        )
    if values["APP_HOST"] == values["CONTENT_HOST"]:
        raise ValueError("production app and artwork hosts must differ")
    if forum and values["FORUM_HOST"] in (values["APP_HOST"], values["CONTENT_HOST"]):
        raise ValueError("FORUM_HOST must differ from app and artwork hosts")
    for key in ("FORUM_ROOT", "FORUM_PHP_SOCKET"):
        path_value = values[key]
        if forum and (
            not re.fullmatch(r"/[A-Za-z0-9._/-]+", path_value)
            or any(part in ("", ".", "..") for part in path_value.split("/")[1:])
            or re.search(r"__[A-Z0-9_]+__", path_value)
            or path_value.split("/")[1] in ("home", "root", "tmp")
            or path_value in ("/var/tmp", "/run/user")
            or path_value.startswith(("/var/tmp/", "/run/user/"))
        ):
            raise ValueError(
                f"{key} requires an absolute path outside private home/tmp mounts"
            )
    email = values["ACME_EMAIL"]
    if email and (
        not re.fullmatch(
            r"[A-Za-z0-9][A-Za-z0-9._%+-]{0,63}@[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?(?:\.[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?)+",
            email,
        )
        or ".." in email.split("@", maxsplit=1)[0]
        or email.split("@", maxsplit=1)[0].endswith(".")
    ):
        raise ValueError("ACME_EMAIL must be empty or a valid ASCII email address")
    sizes = {}
    for key in (
        "APP_MEMORY_HIGH",
        "APP_MEMORY_MAX",
        "PROXY_MEMORY_MAX",
        "JOB_MEMORY_MAX",
        "CONTENT_MEMORY_HIGH",
        "CONTENT_MEMORY_MAX",
    ):
        match = re.fullmatch(r"([1-9][0-9]*)([KMGT])", values[key])
        if not match:
            raise ValueError(
                f"{key} requires a finite measured budget, e.g. a number and M"
            )
        sizes[key] = int(match[1]) * 1024 ** ("KMGT".index(match[2]) + 1)
    if not re.fullmatch(r"[1-9][0-9]*", values["PROXY_OPEN_FILES"]):
        raise ValueError("PROXY_OPEN_FILES requires an integer from 16384 to 65536")
    proxy_open_files = int(values["PROXY_OPEN_FILES"])
    if not 16384 <= proxy_open_files <= 65536:
        raise ValueError("PROXY_OPEN_FILES requires an integer from 16384 to 65536")
    proxy_memory_minimum = max(1024**3, proxy_open_files * 64 * 1024 + 256 * 1024**2)
    if sizes["PROXY_MEMORY_MAX"] < proxy_memory_minimum:
        raise ValueError(
            "PROXY_MEMORY_MAX must cover PROXY_OPEN_FILES at 64 KiB each plus 256 MiB"
        )
    for unit in ("APP", "CONTENT"):
        if sizes[f"{unit}_MEMORY_HIGH"] >= sizes[f"{unit}_MEMORY_MAX"]:
            raise ValueError(f"{unit}_MEMORY_HIGH must be below {unit}_MEMORY_MAX")
    for key in ("APP_CPU_QUOTA", "JOB_CPU_QUOTA", "CONTENT_CPU_QUOTA"):
        if not re.fullmatch(r"[1-9][0-9]*(?:\.[0-9]+)?%", values[key]):
            raise ValueError(f"{key} requires a positive percentage")
    for key in (
        "APP_TASKS_MAX",
        "APP_OPEN_FILES",
        "JOB_TASKS_MAX",
        "CONTENT_TASKS_MAX",
        "CONTENT_OPEN_FILES",
    ):
        if not re.fullmatch(r"[1-9][0-9]*", values[key]):
            raise ValueError(f"{key} requires a positive integer")
    for key in ("APP_OPEN_FILES", "CONTENT_OPEN_FILES"):
        if int(values[key]) < 4096:
            raise ValueError(f"{key} requires at least 4096")
    mirror = values["MIRROR"]
    if mirror and (
        not mirror.startswith("/")
        or not re.fullmatch(r"/[A-Za-z0-9._/-]+", mirror)
        or any(part in ("", ".", "..") for part in mirror.split("/")[1:])
        or re.search(r"__[A-Z0-9_]+__", mirror)
    ):
        raise ValueError("MIRROR must be empty or a normalized absolute path")
    if not re.fullmatch(r"[1-9][0-9]*", values["MIRROR_BYTES"]):
        raise ValueError("MIRROR_BYTES requires a positive integer")
    if int(values["MIRROR_BYTES"]) > 1 << 50:
        raise ValueError("MIRROR_BYTES must not exceed 1 PiB")
    if not re.fullmatch(r"[1-9][0-9]*", values["CONTENT_CONCURRENCY"]):
        raise ValueError("CONTENT_CONCURRENCY requires a positive integer")
    if int(values["CONTENT_CONCURRENCY"]) > 16:
        raise ValueError("CONTENT_CONCURRENCY must not exceed 16")
    # Largest single artwork file served and mirrored (the app accepts 1 B-256 MiB).
    if (
        not re.fullmatch(r"[1-9][0-9]*", values["MAX_OBJECT_BYTES"])
        or int(values["MAX_OBJECT_BYTES"]) > 256 * 1024**2
    ):
        raise ValueError("MAX_OBJECT_BYTES requires 1 to 268435456 bytes")
    return values


def render(values: dict[str, str], destination: Path) -> None:
    rendered = {}
    forum = bool(values["FORUM_HOST"])
    redirects = values["CONTENT_HOST_REDIRECTS"].split()
    replacements = {
        **values,
        "APP_SEARCH_ROUTES": search_settings.routes(values),
        "FORUM_SEARCH_ROUTES": search_settings.routes(values, forum=True)
        if forum
        else "",
        "ACME_EMAIL_DIRECTIVE": (
            f"email {values['ACME_EMAIL']}" if values["ACME_EMAIL"] else ""
        ),
        "HTTP_HOSTS": ", ".join(
            f"http://{host}"
            for host in (
                values["APP_HOST"],
                values["CONTENT_HOST"],
                values["FORUM_HOST"],
                *redirects,
            )
            if host
        ),
        # Appended after the last block, so no redirects keep the file unchanged.
        "CONTENT_REDIRECT_BLOCKS": "".join(
            f"\n\n{host} {{\n\timport site_policy\n"
            f"\tredir https://{values['CONTENT_HOST']}{{uri}} 302\n}}"
            for host in redirects
        ),
        "FORUM_SERVICE": (
            f"SupplementaryGroups=www-data\nReadOnlyPaths={values['FORUM_ROOT']}"
            if forum
            else ""
        ),
        "FORUM_PRIVATE_PATHS": (
            f"InaccessiblePaths=-{Path(values['FORUM_ROOT']).parent}/config.php "
            f"-{Path(values['FORUM_ROOT']).parent}/storage"
            if forum
            else ""
        ),
        "FORUM_ERROR_HEADERS": (
            f"@forum_error host {values['FORUM_HOST']}\n"
            "\t\theader @forum_error {\n"
            "\t\t\t>X-Frame-Options DENY\n"
            "\t\t\t>Content-Security-Policy \"frame-ancestors 'none'\"\n"
            "\t\t\t-X-Powered-By\n"
            "\t\t}"
            if forum
            else ""
        ),
    }
    forum_text = Path(__file__).with_name("forum.caddy.in").read_text() if forum else ""
    replacements["FORUM_BLOCK"] = re.sub(
        r"__([A-Z0-9_]+)__", lambda match: replacements[match[1]], forum_text
    )
    replacements["OAUTH_ENVIRONMENT"] = oauth_settings.environment(values)
    templates = TEMPLATES | (FORUM_TEMPLATES if forum else {})
    for source, name in templates.items():
        text = Path(__file__).with_name(source).read_text()

        def substitute(match: re.Match[str], name: str = name) -> str:
            if match[1] not in replacements:
                raise ValueError(f"unresolved placeholder in {name}")
            return replacements[match[1]]

        # One pass: literal secret contents must never become template directives.
        text = re.sub(r"__([A-Z0-9_]+)__", substitute, text)
        rendered[name] = text
    destination.mkdir(mode=0o700, parents=False, exist_ok=False)
    for name, text in rendered.items():
        path = destination / name
        with path.open("x") as output:
            output.write(text)
        path.chmod(0o600 if name == "seeded.env" else 0o644)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("settings", type=Path)
    parser.add_argument(
        "output", type=Path, help="new output directory; existing paths are refused"
    )
    args = parser.parse_args()
    try:
        render(settings(args.settings), args.output)
    except (ValueError, OSError) as error:
        parser.exit(1, f"Configuration rejected: {error}\n")
    print("Deployment files rendered. No host changes made.")


if __name__ == "__main__":
    main()
