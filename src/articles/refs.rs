//! References from article bodies to fxhash projects and tokens, and their resolution
//! against the catalogue (`seeded.sqlite`, opened read-only).
//!
//! Only genuine references count (rules checked against every stored body; counts in
//! `tmp/claude-articles/ARTICLES.md`):
//!
//! - `::tezos-storage-pointer[…]{contract path}`, matched as fxhash-website's TezosStorage
//!   renderers match it: an fxhash issuer ([`ISSUER`], [`ISSUER_V3`]) with `ledger::<id>` is
//!   project `<id>` (one id sequence across issuers); a GENTK contract with
//!   `token_metadata::<id>` is that token. A `.network` suffix on the contract is ignored and
//!   `data_spec` is not needed. Other contracts (older issuers, EVM, foreign FA2) are not
//!   references: fxhash showed them as unknown.
//! - fxhash.xyz URLs (`http(s)://[www.]fxhash.xyz/…` or bare `www.fxhash.xyz/…`):
//!   `/generative/<id>`, `/marketplace/generative/<id>` (project); `/generative/slug/<slug>`,
//!   `/project/<slug>` (project by slug; an unmatched all-digit slug is tried as an id);
//!   `/gentk/<id>` and
//!   `/gentk/FX0-<id>` (GENTK v1/v2, one id sequence, v2 from [`GENTK_V2_FIRST_ID`]);
//!   `/gentk/FX1-<id>` (GENTK v3); `/gentk/<GENTK contract>-<id>`;
//!   `/gentk/slug/<project slug>-<iteration>` (token by slug). EVM ids (`0x…`), profiles,
//!   articles and docs are not references.
//! - Marketplace links to one GENTK token: objkt.com `/asset|tokens/<contract>/<id>` (also
//!   objkt's `fxhash` and `fxparams` aliases) and tzkt.io `/<contract>/tokens/<id>`.
//! - Any `ipfs://`, IPFS gateway or `onchfs://` URL whose root is a project's generator
//!   (root of its `artifact_uri` or `generative_uri`): that project. Needs the catalogue,
//!   so these are added when references are resolved.
//!
//! Fenced code blocks and inline code spans are skipped. One row per distinct target per
//! article, at its first appearance.

use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

use rusqlite::{Connection, OpenFlags, OptionalExtension};

use super::parse;
use super::store::ArticleStoreError;

/// fxhash contracts (fxhash-website `.env`).
pub const GENTK_V1: &str = "KT1KEa8z6vWXDJrVqtMrAeDVzsvxat3kHaCE";
pub const GENTK_V2: &str = "KT1U6EHmNxJTkvaWJ4ThczG4FSDaHC21ssvi";
pub const GENTK_V3: &str = "KT1EfsNuqwLAWDd3o4pvfUx1CAh5GMdTrRvr";
pub const ISSUER: &str = "KT1BJC12dG17CVvPKJ1VYaNnaT5mzfnUTwXv";
pub const ISSUER_V3: &str = "KT1Xpmp15KfqoePNW9HczFmqaGNHwadV2a3b";
pub const GENTK_CONTRACTS: [&str; 3] = [GENTK_V1, GENTK_V2, GENTK_V3];
/// First GENTK v2 token id. v1 and v2 share one id sequence (fxhash's `FX0-<id>`); the
/// catalogue has v1 ids 0–589145 and v2 ids from 589146.
pub const GENTK_V2_FIRST_ID: u64 = 589_146;
/// Most references kept per article.
pub const MAX_REFS: usize = 2_000;
/// Bump when the rules change: every article's references are then extracted again.
pub const RULES_VERSION: i64 = 1;
const MAX_SOURCE_CHARS: usize = 300;
const MAX_SLUG_CHARS: usize = 200;
const MAX_URL_BYTES: usize = 4_096;
/// Punctuation that may trail a URL in prose or markdown.
const TRAILING: &[char] = &['.', ',', ';', ':', '!', '?', '*', '\'', '"', '_', '~'];

pub const FORM_POINTER: &str = "storage-pointer";
pub const FORM_FXHASH: &str = "fxhash-url";
pub const FORM_MARKET: &str = "marketplace-url";
pub const FORM_GENERATOR: &str = "generator";

/// What a reference names.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Target {
    Project(u64),
    ProjectSlug(String),
    Token {
        contract: &'static str,
        token_id: u64,
    },
    /// `<project slug>-<iteration>`.
    TokenSlug(String),
    /// `ipfs` (canonical CIDv1) or `onchfs` (hex) root; a reference only if it is a
    /// project's generator.
    Root {
        scheme: &'static str,
        cid: String,
    },
}

impl Target {
    /// `project` or `token`.
    pub fn kind(&self) -> &'static str {
        match self {
            Target::Project(_) | Target::ProjectSlug(_) | Target::Root { .. } => "project",
            Target::Token { .. } | Target::TokenSlug(_) => "token",
        }
    }

    /// Stable text key, unique per target (the `article_refs.target` column).
    pub fn key(&self) -> String {
        match self {
            Target::Project(id) => format!("project:{id}"),
            Target::ProjectSlug(s) => format!("project-slug:{s}"),
            Target::Token { contract, token_id } => format!("token:{contract}:{token_id}"),
            Target::TokenSlug(s) => format!("token-slug:{s}"),
            Target::Root { scheme, cid } => format!("{scheme}:{cid}"),
        }
    }

    /// Inverse of [`Target::key`].
    pub fn from_key(key: &str) -> Option<Target> {
        let (kind, rest) = key.split_once(':')?;
        Some(match kind {
            "project" => Target::Project(rest.parse().ok()?),
            "project-slug" => Target::ProjectSlug(rest.to_owned()),
            "token" => {
                let (contract, id) = rest.split_once(':')?;
                Target::Token {
                    contract: gentk(contract)?,
                    token_id: id.parse().ok()?,
                }
            }
            "token-slug" => Target::TokenSlug(rest.to_owned()),
            "ipfs" => Target::Root {
                scheme: "ipfs",
                cid: rest.to_owned(),
            },
            "onchfs" => Target::Root {
                scheme: "onchfs",
                cid: rest.to_owned(),
            },
            _ => return None,
        })
    }
}

/// One reference as found in a body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reference {
    pub target: Target,
    /// [`FORM_POINTER`], [`FORM_FXHASH`], [`FORM_MARKET`] or [`FORM_GENERATOR`].
    pub form: &'static str,
    /// The directive or URL as written, capped.
    pub source: String,
}

fn gentk(contract: &str) -> Option<&'static str> {
    GENTK_CONTRACTS.iter().copied().find(|c| *c == contract)
}

fn pre_v3(token_id: u64) -> Target {
    Target::Token {
        contract: if token_id >= GENTK_V2_FIRST_ID {
            GENTK_V2
        } else {
            GENTK_V1
        },
        token_id,
    }
}

fn cap(s: &str, chars: usize) -> String {
    s.chars().take(chars).collect()
}

/// An id the stores can hold: at most `i64::MAX`.
fn storable(n: u64) -> Option<u64> {
    i64::try_from(n).is_ok().then_some(n)
}

/// Leading decimal number of a path segment, at most `i64::MAX`; anything after it must be
/// trailing punctuation.
fn number(seg: &str) -> Option<u64> {
    let end = seg.bytes().take_while(u8::is_ascii_digit).count();
    if end == 0 || end > 19 || !seg[end..].chars().all(|c| TRAILING.contains(&c)) {
        return None;
    }
    seg[..end].parse().ok().and_then(storable)
}

/// A percent-decoded slug without markdown escapes; `None` when empty or too long.
fn slug(seg: &str) -> Option<String> {
    let decoded = percent_encoding::percent_decode_str(seg)
        .decode_utf8()
        .ok()?;
    let s: String = decoded.chars().filter(|c| *c != '\\').collect();
    // No catalogue slug ends with these; prose punctuation does.
    let s = s.trim().trim_end_matches([',', ';', '?']);
    (!s.is_empty() && s.chars().count() <= MAX_SLUG_CHARS).then(|| s.to_owned())
}

/// `<project slug>-<iteration>` split at the last dash.
pub fn token_slug_parts(s: &str) -> Option<(&str, u64)> {
    let (project, iteration) = s.rsplit_once('-')?;
    let iteration = number(iteration)?;
    (!project.is_empty()).then_some((project, iteration))
}

/// Slug spellings to try, most literal first: as written, then without trailing punctuation,
/// then lowercase.
pub fn slug_candidates(s: &str) -> Vec<String> {
    let mut out = vec![s.to_owned()];
    for c in [
        s.trim_end_matches(TRAILING).to_owned(),
        s.to_lowercase(),
        s.trim_end_matches(TRAILING).to_lowercase(),
    ] {
        if !c.is_empty() && !out.contains(&c) {
            out.push(c);
        }
    }
    out
}

fn gentk_segment(seg: &str) -> Option<Target> {
    if let Some(n) = number(seg) {
        return Some(pre_v3(n));
    }
    let (prefix, id) = seg.rsplit_once('-')?;
    let n = number(id)?;
    match prefix {
        "FX0" | "fx0" => Some(pre_v3(n)),
        "FX1" | "fx1" => Some(Target::Token {
            contract: GENTK_V3,
            token_id: n,
        }),
        c => gentk(c).map(|contract| Target::Token {
            contract,
            token_id: n,
        }),
    }
}

fn segments(path: &str) -> Vec<&str> {
    path.split(['?', '#'])
        .next()
        .unwrap_or_default()
        .split('/')
        .filter(|s| !s.is_empty())
        .collect()
}

fn fxhash_path(path: &str) -> Option<Target> {
    match segments(path).as_slice() {
        ["generative", "slug", s, ..] => slug(s).map(Target::ProjectSlug),
        ["generative", n, ..] | ["marketplace", "generative", n, ..] => {
            number(n).map(Target::Project)
        }
        ["project", x, ..] => slug(x).map(Target::ProjectSlug),
        ["gentk", "slug", s, ..] => slug(s)
            .filter(|s| token_slug_parts(s).is_some())
            .map(Target::TokenSlug),
        ["gentk", x, ..] => gentk_segment(x),
        _ => None,
    }
}

fn objkt_path(path: &str) -> Option<Target> {
    match segments(path).as_slice() {
        ["asset" | "tokens", c, id, ..] => {
            let n = number(id)?;
            match *c {
                "fxhash" => Some(pre_v3(n)),
                "fxparams" => Some(Target::Token {
                    contract: GENTK_V3,
                    token_id: n,
                }),
                c => gentk(c).map(|contract| Target::Token {
                    contract,
                    token_id: n,
                }),
            }
        }
        _ => None,
    }
}

fn tzkt_path(path: &str) -> Option<Target> {
    match segments(path).as_slice() {
        [c, "tokens", id, ..] => Some(Target::Token {
            contract: gentk(c)?,
            token_id: number(id)?,
        }),
        _ => None,
    }
}

/// A generator candidate: the content root of an `ipfs://`, gateway or `onchfs://` URL.
fn root(url: &str) -> Option<Target> {
    let url = url.trim_end_matches(TRAILING);
    let uri = if url.starts_with("ipfs://") || url.starts_with("onchfs://") {
        url.to_owned()
    } else {
        parse::ipfs_uri(url)?
    };
    let (scheme, cid) = crate::store::content_root(&uri)?;
    Some(Target::Root { scheme, cid })
}

fn classify(url: &str) -> Option<(Target, &'static str)> {
    let lower = url.to_ascii_lowercase();
    let rest = ["https://", "http://"]
        .iter()
        .find_map(|p| lower.strip_prefix(p).map(|_| &url[p.len()..]))
        .or_else(|| lower.starts_with("www.").then_some(url));
    if let Some(rest) = rest {
        let (host, path) = rest.split_once('/').unwrap_or((rest, ""));
        let host = host.to_ascii_lowercase();
        let host = host.strip_prefix("www.").unwrap_or(&host);
        let found = match host {
            "fxhash.xyz" => fxhash_path(path).map(|t| (t, FORM_FXHASH)),
            "objkt.com" => objkt_path(path).map(|t| (t, FORM_MARKET)),
            "tzkt.io" => tzkt_path(path).map(|t| (t, FORM_MARKET)),
            _ => None,
        };
        if found.is_some() {
            return found;
        }
    }
    root(url).map(|t| (t, FORM_GENERATOR))
}

/// End of a URL starting at `start`: stops at whitespace, markup delimiters, non-ASCII, or an
/// unbalanced `)`.
fn url_end(line: &str, start: usize) -> usize {
    let b = line.as_bytes();
    let mut depth = 0usize;
    let mut i = start;
    while i < b.len() && i - start < MAX_URL_BYTES {
        match b[i] {
            c if !c.is_ascii() || c.is_ascii_whitespace() || c.is_ascii_control() => break,
            b'<' | b'>' | b'"' | b'`' | b'[' | b']' | b'{' | b'}' | b'|' => break,
            b'(' => depth += 1,
            b')' if depth == 0 => break,
            b')' => depth -= 1,
            _ => {}
        }
        i += 1;
    }
    i
}

/// URLs on one line: `http(s)://`, `ipfs://`, `onchfs://`, and bare `www.fxhash.xyz/`.
fn urls(line: &str) -> Vec<&str> {
    let lower = line.to_ascii_lowercase();
    let l = lower.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < l.len() {
        let after_word = i == 0 || !l[i - 1].is_ascii_alphanumeric();
        let scheme = after_word
            && [&b"https://"[..], b"http://", b"ipfs://", b"onchfs://"]
                .iter()
                .any(|p| l[i..].starts_with(p));
        let bare = l[i..].starts_with(b"www.fxhash.xyz/")
            && (i == 0 || !(l[i - 1].is_ascii_alphanumeric() || b"./:".contains(&l[i - 1])));
        if scheme || bare {
            let end = url_end(line, i);
            if end > i {
                out.push(&line[i..end]);
            }
            i = end.max(i + 1);
        } else {
            i += 1;
        }
    }
    out
}

/// The line without inline code spans (an unmatched backtick is kept as text).
fn without_code(line: &str) -> std::borrow::Cow<'_, str> {
    let ticks = line.bytes().filter(|b| *b == b'`').count();
    if ticks < 2 {
        return line.into();
    }
    let usable = ticks - ticks % 2;
    let mut out = String::with_capacity(line.len());
    let (mut seen, mut inside) = (0usize, false);
    for c in line.chars() {
        if c == '`' && seen < usable {
            seen += 1;
            inside = !inside;
            out.push(' ');
        } else if !inside {
            out.push(c);
        }
    }
    out.into()
}

/// `::tezos-storage-pointer[…]{contract="…" path="…"}` at line start.
/// The fxhash project or token an `::tezos-storage-pointer` line points to.
pub fn pointer(line: &str) -> Option<Target> {
    let rest = line.trim_start().strip_prefix("::tezos-storage-pointer")?;
    if !(rest.starts_with('[') || rest.starts_with('{')) {
        return None;
    }
    let open = rest.find('{')?;
    let close = rest.rfind('}').filter(|c| *c > open).unwrap_or(rest.len());
    let attrs = &rest[open + 1..close];
    let contract = parse::attr(attrs, "contract")?.split('.').next()?.trim();
    let path = parse::attr(attrs, "path")?;
    let mut parts = path.split("::");
    let map = parts.next()?.trim();
    let id: u64 = storable(parts.next()?.trim().parse().ok()?)?;
    if map == "ledger" && [ISSUER, ISSUER_V3].contains(&contract) {
        Some(Target::Project(id))
    } else if map == "token_metadata" {
        gentk(contract).map(|contract| Target::Token {
            contract,
            token_id: id,
        })
    } else {
        None
    }
}

/// References in an fx-markdown body, in order of first appearance, one per target, at most
/// [`MAX_REFS`]. Generator candidates ([`Target::Root`]) are included; whether they are
/// references depends on the catalogue.
pub fn references(body: &str) -> Vec<Reference> {
    let mut out: Vec<Reference> = Vec::new();
    let mut seen = std::collections::HashSet::new();
    let mut push = |out: &mut Vec<Reference>, target: Target, form: &'static str, src: &str| {
        if out.len() < MAX_REFS && seen.insert(target.clone()) {
            out.push(Reference {
                target,
                form,
                source: cap(src.trim(), MAX_SOURCE_CHARS),
            });
        }
    };
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
        if let Some(target) = pointer(line) {
            push(&mut out, target, FORM_POINTER, line);
        }
        let text = without_code(line);
        for url in urls(&text) {
            if let Some((target, form)) = classify(url) {
                push(&mut out, target, form, url);
            }
        }
        if out.len() >= MAX_REFS {
            break;
        }
    }
    out
}

// ---- the catalogue ----

const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

/// Opens the catalogue (`seeded.sqlite`) read-only and checks it has what resolution reads.
pub fn open_archive(path: &Path) -> Result<Connection, ArticleStoreError> {
    if !path.is_file() {
        return Err(ArticleStoreError::BadRequest(format!(
            "{} does not exist",
            path.display()
        )));
    }
    let conn = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|e| ArticleStoreError::Unavailable(format!("{}: {e}", path.display())))?;
    conn.busy_timeout(BUSY_TIMEOUT)
        .map_err(|e| ArticleStoreError::Unavailable(e.to_string()))?;
    check_archive(&conn)?;
    Ok(conn)
}

/// The catalogue columns resolution reads exist.
pub fn check_archive(conn: &Connection) -> Result<(), ArticleStoreError> {
    for sql in [
        "SELECT id, slug, hidden, artifact_uri, generative_uri FROM projects LIMIT 0",
        "SELECT contract, token_id, project_id, iteration FROM tokens LIMIT 0",
    ] {
        conn.prepare(sql).map_err(|e| {
            ArticleStoreError::BadRequest(format!("not a seeded.art catalogue: {e}"))
        })?;
    }
    Ok(())
}

/// `(project_id, Some((contract, token_id)))` of a gentk slug.
pub(crate) type SlugToken = (u64, Option<(String, u64)>);

/// Catalogue facts resolution needs, read in one snapshot.
pub(crate) struct Catalogue<'c> {
    conn: &'c Connection,
    /// Project id → hidden.
    projects: HashMap<u64, bool>,
    slugs: HashMap<String, u64>,
    /// Generator root → projects using it, visible first, then by id.
    roots: HashMap<(&'static str, String), Vec<u64>>,
}

fn store_err(e: rusqlite::Error) -> ArticleStoreError {
    use rusqlite::ffi::ErrorCode as C;
    match &e {
        rusqlite::Error::SqliteFailure(f, _)
            if matches!(
                f.code,
                C::DatabaseBusy | C::DatabaseLocked | C::SystemIoFailure | C::CannotOpen
            ) =>
        {
            ArticleStoreError::Unavailable(format!("catalogue: {e}"))
        }
        _ => ArticleStoreError::Internal(format!("catalogue: {e}")),
    }
}

impl<'c> Catalogue<'c> {
    /// Loads projects (about 27,000 rows) from `conn`, which should be inside a read
    /// transaction so later token lookups see the same snapshot.
    pub(crate) fn load(conn: &'c Connection) -> Result<Catalogue<'c>, ArticleStoreError> {
        let mut projects = HashMap::new();
        let mut slugs = HashMap::new();
        let mut roots: HashMap<(&'static str, String), Vec<(bool, u64)>> = HashMap::new();
        let mut stmt = conn
            .prepare("SELECT id, slug, hidden, artifact_uri, generative_uri FROM projects")
            .map_err(store_err)?;
        let mut rows = stmt.query([]).map_err(store_err)?;
        while let Some(r) = rows.next().map_err(store_err)? {
            let id: i64 = r.get(0).map_err(store_err)?;
            let Ok(id) = u64::try_from(id) else {
                continue;
            };
            let hidden = r.get::<_, i64>(2).map_err(store_err)? != 0;
            projects.insert(id, hidden);
            if let Some(s) = r.get::<_, Option<String>>(1).map_err(store_err)? {
                slugs.insert(s, id);
            }
            for i in [3, 4] {
                if let Some(uri) = r.get::<_, Option<String>>(i).map_err(store_err)? {
                    if let Some(key) = crate::store::content_root(&uri) {
                        let list = roots.entry(key).or_default();
                        if !list.iter().any(|(_, p)| *p == id) {
                            list.push((hidden, id));
                        }
                    }
                }
            }
        }
        let roots = roots
            .into_iter()
            .map(|(k, mut v)| {
                v.sort_unstable();
                (k, v.into_iter().map(|(_, id)| id).collect())
            })
            .collect();
        Ok(Catalogue {
            conn,
            projects,
            slugs,
            roots,
        })
    }

    /// `Some(hidden)` for a project the catalogue has.
    pub(crate) fn hidden(&self, project_id: u64) -> Option<bool> {
        self.projects.get(&project_id).copied()
    }

    /// By slug; an all-digit text no slug matches is tried as a project id (`/project/<x>`
    /// takes slugs, and 137 catalogue slugs are all digits).
    pub(crate) fn project_by_slug(&self, s: &str) -> Option<u64> {
        slug_candidates(s)
            .iter()
            .find_map(|c| self.slugs.get(c).copied())
            .or_else(|| {
                let id: u64 = s.parse().ok()?;
                self.projects.contains_key(&id).then_some(id)
            })
    }

    /// The project whose generator has this root: the lowest visible id, else the lowest id.
    pub(crate) fn generator(&self, scheme: &'static str, cid: &str) -> Option<u64> {
        self.roots
            .get(&(scheme, cid.to_owned()))
            .and_then(|v| v.first().copied())
    }

    pub(crate) fn token_project(
        &self,
        contract: &str,
        token_id: u64,
    ) -> Result<Option<u64>, ArticleStoreError> {
        let Ok(id) = i64::try_from(token_id) else {
            return Ok(None);
        };
        self.conn
            .prepare_cached("SELECT project_id FROM tokens WHERE contract = ?1 AND token_id = ?2")
            .map_err(store_err)?
            .query_row(rusqlite::params![contract, id], |r| r.get::<_, i64>(0))
            .optional()
            .map_err(store_err)
            .map(|p| p.and_then(|p| u64::try_from(p).ok()))
    }

    /// `(project_id, Some((contract, token_id)))` for a gentk slug; the token part is `None`
    /// when the project is known but that iteration is not.
    pub(crate) fn token_by_slug(&self, s: &str) -> Result<Option<SlugToken>, ArticleStoreError> {
        for c in slug_candidates(s) {
            let Some((project_slug, iteration)) = token_slug_parts(&c) else {
                continue;
            };
            let Some(project) = self.slugs.get(project_slug).copied() else {
                continue;
            };
            let token = self
                .conn
                .prepare_cached(
                    "SELECT contract, token_id FROM tokens WHERE project_id = ?1 AND iteration = ?2",
                )
                .map_err(store_err)?
                .query_row(
                    rusqlite::params![project as i64, iteration as i64],
                    |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)),
                )
                .optional()
                .map_err(store_err)?
                .and_then(|(c, t)| u64::try_from(t).ok().map(|t| (c, t)));
            return Ok(Some((project, token)));
        }
        Ok(None)
    }
}
