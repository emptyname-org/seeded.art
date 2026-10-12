//! fx(text) metadata JSON and the media an article body embeds.
//!
//! Metadata (TZIP-21 style, `type: "article"`): `name`, `description`, `tags`, `language`,
//! `artifactUri` (the fx-markdown body, on IPFS), `displayUri`, `thumbnailUri`,
//! `thumbnailCaption`, `minter` (self-declared; the chain's `token_data.minter` is
//! authoritative), `platforms`.
//!
//! Body media (listed, never downloaded here): markdown images `![alt](uri)`, HTML `<img>`,
//! `<video>`, `<source>` and `<audio>` sources, and fx-markdown leaf directives such as
//! `::video[]{src="…"}` and `::embed-media[]{href="…"}`. Fenced code blocks are skipped.

use serde_json::Value;

/// Longest kept metadata string, in characters; longer values are cut.
const MAX_FIELD_CHARS: usize = 20_000;
const MAX_TAGS: usize = 64;
const MAX_MEDIA: usize = 2_000;
const MAX_URI_CHARS: usize = 4_096;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ArticleMeta {
    pub name: Option<String>,
    pub description: Option<String>,
    pub tags: Vec<String>,
    pub language: Option<String>,
    pub artifact_uri: Option<String>,
    pub display_uri: Option<String>,
    pub thumbnail_uri: Option<String>,
    pub thumbnail_caption: Option<String>,
    pub minter: Option<String>,
}

fn field(v: &Value, key: &str) -> Option<String> {
    let s = v.get(key)?.as_str()?.trim();
    (!s.is_empty()).then(|| s.chars().take(MAX_FIELD_CHARS).collect())
}

/// Reads the fields we keep; `None` unless `v` is a JSON object. Non-string values are ignored.
pub fn metadata(v: &Value) -> Option<ArticleMeta> {
    if !v.is_object() {
        return None;
    }
    let mut tags: Vec<String> = Vec::new();
    for t in v
        .get("tags")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        if let Some(t) = t.as_str().map(str::trim).filter(|t| !t.is_empty()) {
            let t: String = t.chars().take(200).collect();
            if !tags.contains(&t) && tags.len() < MAX_TAGS {
                tags.push(t);
            }
        }
    }
    Some(ArticleMeta {
        name: field(v, "name"),
        description: field(v, "description"),
        tags,
        language: field(v, "language"),
        artifact_uri: field(v, "artifactUri"),
        display_uri: field(v, "displayUri"),
        thumbnail_uri: field(v, "thumbnailUri"),
        thumbnail_caption: field(v, "thumbnailCaption"),
        minter: field(v, "minter"),
    })
}

/// One embedded media reference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Media {
    /// `image`, `video`, `audio`, or an fx-markdown directive name (`embed-media`, …).
    pub kind: String,
    /// As written in the body.
    pub src: String,
    /// `ipfs://<cid>[/path]` when `src` names IPFS content (directly or via a gateway URL).
    pub ipfs: Option<String>,
}

/// `ipfs://<cid>[/path]` for `ipfs://…`, `https://<host>/ipfs/<cid>/…` and
/// `https://<cid>.ipfs.<host>/…`; `None` otherwise. The CID is not validated here.
pub fn ipfs_uri(src: &str) -> Option<String> {
    let clean = |rest: &str| -> Option<String> {
        let rest = rest.split(['?', '#']).next().unwrap_or_default();
        let rest = rest.trim_end_matches('/');
        let cid = rest.split('/').next().unwrap_or_default();
        (!cid.is_empty() && cid.bytes().all(|b| b.is_ascii_alphanumeric()))
            .then(|| format!("ipfs://{rest}"))
    };
    if let Some(rest) = src.strip_prefix("ipfs://") {
        return clean(rest.strip_prefix("ipfs/").unwrap_or(rest));
    }
    let rest = src
        .strip_prefix("https://")
        .or_else(|| src.strip_prefix("http://"))?;
    let (host, path) = rest.split_once('/').unwrap_or((rest, ""));
    if let Some(p) = path.strip_prefix("ipfs/") {
        return clean(p);
    }
    let (cid, gateway) = host.split_once(".ipfs.")?;
    if gateway.is_empty() {
        return None;
    }
    clean(&if path.is_empty() {
        cid.to_owned()
    } else {
        format!("{cid}/{path}")
    })
}

fn push(out: &mut Vec<Media>, kind: &str, src: &str) {
    let src = src
        .trim()
        .trim_start_matches('<')
        .trim_end_matches('>')
        .trim();
    if src.is_empty() || src.chars().count() > MAX_URI_CHARS || out.len() >= MAX_MEDIA {
        return;
    }
    if out.iter().any(|m| m.kind == kind && m.src == src) {
        return;
    }
    out.push(Media {
        kind: kind.to_owned(),
        src: src.to_owned(),
        ipfs: ipfs_uri(src),
    });
}

/// Value of `name="…"` / `name='…'` / `name=bare` inside an attribute list.
pub(crate) fn attr<'a>(attrs: &'a str, name: &str) -> Option<&'a str> {
    let lower = attrs.to_ascii_lowercase();
    let mut from = 0;
    while let Some(i) = lower[from..].find(name) {
        let at = from + i;
        from = at + name.len();
        let before_ok = at == 0
            || !lower.as_bytes()[at - 1].is_ascii_alphanumeric()
                && lower.as_bytes()[at - 1] != b'-';
        let rest = attrs[from..].trim_start();
        if !before_ok || !rest.starts_with('=') {
            continue;
        }
        let rest = rest[1..].trim_start();
        let value = match rest.chars().next()? {
            q @ ('"' | '\'') => rest[1..].split(q).next()?,
            _ => rest
                .split(|c: char| c.is_whitespace() || c == '>' || c == '}')
                .next()?,
        };
        return Some(value);
    }
    None
}

/// Markdown images on one line: `![alt](src "title")`.
fn images(line: &str, out: &mut Vec<Media>) {
    let mut rest = line;
    while let Some(i) = rest.find("![") {
        rest = &rest[i + 2..];
        let Some(close) = rest.find("](") else {
            return;
        };
        let after = &rest[close + 2..];
        let end = after.find(')').unwrap_or(after.len());
        let target = after[..end].trim();
        let src = if let Some(inner) = target.strip_prefix('<') {
            inner.split('>').next().unwrap_or_default()
        } else {
            target.split_whitespace().next().unwrap_or_default()
        };
        push(out, "image", src);
        rest = &after[end..];
    }
}

/// HTML media tags on one line.
fn html(line: &str, out: &mut Vec<Media>) {
    let lower = line.to_ascii_lowercase();
    for (tag, kind) in [
        ("<img", "image"),
        ("<video", "video"),
        ("<audio", "audio"),
        ("<source", "media"),
    ] {
        let mut from = 0;
        while let Some(i) = lower[from..].find(tag) {
            let start = from + i + tag.len();
            let end = lower[start..].find('>').map_or(line.len(), |j| start + j);
            if let Some(src) = attr(&line[start..end], "src") {
                push(out, kind, src);
            }
            from = end.min(line.len());
            if from >= line.len() {
                break;
            }
        }
    }
}

/// fx-markdown leaf directive: `::name[label]{attrs}` at line start.
fn directive(line: &str, out: &mut Vec<Media>) {
    let Some(rest) = line.trim_start().strip_prefix("::") else {
        return;
    };
    if rest.starts_with(':') {
        return;
    }
    let name: String = rest
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || *c == '-')
        .collect();
    if name.is_empty() {
        return;
    }
    let Some(open) = rest.find('{') else {
        return;
    };
    let close = rest.rfind('}').filter(|c| *c > open).unwrap_or(rest.len());
    let attrs = &rest[open + 1..close];
    if let Some(src) = attr(attrs, "src").or_else(|| attr(attrs, "href")) {
        push(out, &name, src);
    }
}

/// Media referenced by an fx-markdown body, in order of first appearance, deduplicated.
pub fn media(body: &str) -> Vec<Media> {
    let mut out = Vec::new();
    let mut fence: Option<&str> = None;
    for line in body.lines() {
        let t = line.trim_start();
        if let Some(f) = fence {
            if t.starts_with(f) {
                fence = None;
            }
            continue;
        }
        if t.starts_with("```") {
            fence = Some("```");
            continue;
        }
        if t.starts_with("~~~") {
            fence = Some("~~~");
            continue;
        }
        images(line, &mut out);
        html(line, &mut out);
        directive(line, &mut out);
    }
    out
}
