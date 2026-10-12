"""Public crawler policy and optional search-engine verification routes."""

import re

SETTINGS = {
    "INDEXNOW_KEY",
    "FORUM_INDEXNOW_KEY",
}
GROUPS = (
    ("AI search", ("OAI-SearchBot", "Claude-SearchBot", "PerplexityBot")),
    ("Training and datasets", ("GPTBot", "ClaudeBot", "CCBot")),
    ("Requested fetches", ("ChatGPT-User", "Claude-User", "Perplexity-User")),
)
APP_EXCLUSIONS = (
    "/auth",
    "/api/auth/",
    "/api/market/",
    "/health/",
    "/oauth/",
    "/token/*/full",
)


def valid_key(key: str) -> bool:
    return re.fullmatch(r"[A-Za-z0-9-]{8,128}", key) is not None


def validate(values: dict[str, str]) -> None:
    for name in SETTINGS:
        value = values.setdefault(name, "")
        if value and name.startswith("FORUM_") and not values["FORUM_HOST"]:
            raise ValueError(f"{name} requires a forum host")
        if value and not valid_key(value):
            raise ValueError(f"invalid {name}")


def robots(origin: str, *, forum: bool = False) -> str:
    # Named groups do not inherit wildcard restrictions. Repeat the full policy.
    rules = (
        "".join(f"Disallow: {path}\n" for path in (() if forum else APP_EXCLUSIONS))
        + "Allow: /\n"
    )
    text = (
        ""
        if forum
        else f"# Disallow lines are for crawlers. Agents may sign in with their own address; see {origin}/agents.md\n"
    )
    text += "User-agent: *\n" + rules
    for label, agents in GROUPS:
        text += f"\n# {label}\n"
        text += "".join(f"User-agent: {agent}\n" for agent in agents) + rules
    if not forum:
        text += f"\nSitemap: {origin}/sitemap.xml\n"
    return text


def routes(values: dict[str, str], *, forum: bool = False) -> str:
    prefix = "FORUM_" if forum else ""
    host = values["FORUM_HOST" if forum else "APP_HOST"]
    records = [("/robots.txt", "text/plain", robots(f"https://{host}", forum=forum))]
    if key := values[prefix + "INDEXNOW_KEY"]:
        records.append((f"/{key}.txt", "text/plain", key))
    blocks = []
    for number, (path, kind, body) in enumerate(records):
        # Caddy quoted strings keep \n literal. Raw strings preserve REP newlines.
        blocks.append(
            f"@search_file_{number} {{\n\t\t\tmethod GET HEAD\n\t\t\tpath {path}\n\t\t}}\n"
            f"\t\thandle @search_file_{number} {{\n"
            f'\t\t\theader Content-Type "{kind}; charset=utf-8"\n'
            '\t\t\theader Cache-Control "public, max-age=300"\n'
            f"\t\t\trespond `{body}` 200\n\t\t}}"
        )
    return "\n\t\t".join(blocks)
