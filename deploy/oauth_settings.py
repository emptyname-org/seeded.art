"""Validate and render optional OAuth settings for systemd EnvironmentFile."""

from __future__ import annotations

import ipaddress
from urllib.parse import urlsplit

SETTINGS = {
    "OAUTH_CLIENT_ID",
    "OAUTH_CLIENT_SECRET",
    "OAUTH_REDIRECT_URI",
}
_ENVIRONMENT_NAMES = {
    "OAUTH_CLIENT_ID": "SEEDED_OAUTH_CLIENT_ID",
    "OAUTH_CLIENT_SECRET": "SEEDED_OAUTH_CLIENT_SECRET",
    "OAUTH_REDIRECT_URI": "SEEDED_OAUTH_REDIRECT_URI",
}


def validate(values: dict[str, str]) -> None:
    """Insert absent optional settings and validate the complete OAuth tuple."""
    if any(
        not isinstance(value, str) for key, value in values.items() if key in SETTINGS
    ):
        raise ValueError("OAuth settings must be strings")
    for key in SETTINGS:
        values.setdefault(key, "")

    present = {key: bool(values[key].strip()) for key in SETTINGS}
    if any(present.values()) and not all(present.values()):
        raise ValueError("set all OAuth settings or none")
    if not any(present.values()):
        return

    client_id = values["OAUTH_CLIENT_ID"]
    if (
        not 1 <= len(client_id.encode("utf-8")) <= 128
        or not client_id.isascii()
        or any(not (char.isalnum() or char in "._~-") for char in client_id)
    ):
        raise ValueError("invalid OAuth client ID")

    secret = values["OAUTH_CLIENT_SECRET"]
    if not 16 <= len(secret.encode("utf-8")) <= 512 or any(
        ord(char) < 32 or 127 <= ord(char) <= 159 for char in secret
    ):
        raise ValueError("invalid OAuth client secret")

    redirect = values["OAUTH_REDIRECT_URI"]
    if (
        len(redirect.encode("utf-8")) > 512
        or any(ord(char) < 32 or 127 <= ord(char) <= 159 for char in redirect)
        or "\\" in redirect
    ):
        raise ValueError("invalid OAuth redirect URI")
    try:
        parsed = urlsplit(redirect)
        host = parsed.hostname
        # Accessing .port also rejects malformed ports.
        _ = parsed.port
    except ValueError:
        raise ValueError("invalid OAuth redirect URI") from None
    loopback = host == "localhost"
    if host:
        try:
            loopback = loopback or ipaddress.ip_address(host).is_loopback
        except ValueError:
            pass
    if (
        parsed.scheme not in {"https", "http"}
        or not host
        or parsed.username is not None
        or parsed.password is not None
        or "#" in redirect
        or not (parsed.scheme == "https" or (parsed.scheme == "http" and loopback))
    ):
        raise ValueError("invalid OAuth redirect URI")


def _quote_environment_value(value: str) -> str:
    if any(ord(char) < 32 or 127 <= ord(char) <= 159 for char in value):
        raise ValueError("OAuth settings cannot contain control characters")
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def environment(values: dict[str, str]) -> str:
    """Return quoted systemd EnvironmentFile lines, or empty when disabled."""
    validated = dict(values)
    validate(validated)
    if not any(validated[key].strip() for key in SETTINGS):
        return ""
    return "\n".join(
        f"{_ENVIRONMENT_NAMES[key]}={_quote_environment_value(validated[key])}"
        for key in ("OAUTH_CLIENT_ID", "OAUTH_CLIENT_SECRET", "OAUTH_REDIRECT_URI")
    )
