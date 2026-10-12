//! The articles database: schema, sync writes and the read API.
//!
//! A separate SQLite file (default [`super::DEFAULT_DB_PATH`]), WAL, marked with its own
//! `application_id` and `user_version`, so it is never mistaken for the catalogue. One writer
//! (`seeded-articles sync`); the web opens it read-only with [`ArticleStore::open_read_only`].
//!
//! Public reads exclude articles hidden by fxhash moderation (REPORTED, AUTO_DETECT_COPY,
//! MALICIOUS, HIDDEN and unknown states, as the catalogue's `HIDDEN_FLAGS`); a direct lookup
//! of one is NotFound.
//!
//! Bodies are returned raw (fx-markdown). They are untrusted: sanitize before rendering.
//!
//! Schema 2 adds `article_refs` (references from each article's current body to fxhash
//! projects and tokens, see [`super::refs`]) and `ref_projects` (catalogue visibility of the
//! referenced projects). References are re-extracted in the same transaction whenever an
//! article's body changes; resolution against the catalogue is a separate, positive-only
//! step ([`ArticleStore::resolve_refs`]). Reads return a reference only when its project is
//! known to the catalogue and not flagged.
//!
//! Schema 3 makes `names` `(address, name)`: the name the site displays for each article
//! author, copied from the catalogue by [`ArticleStore::resolve_names`] (see [`super::names`]);
//! no row means the site shows the address. The search index's `authors` column is the address
//! plus that name. The migration drops the fxhash registry names schema 2 kept, since the site
//! may hide them; the next run with a catalogue fills the table.

use std::path::Path;
use std::time::Duration;

use rusqlite::types::Value as SqlValue;
use rusqlite::{Connection, OpenFlags, OptionalExtension, Row, TransactionBehavior, params};
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::ipfs::{Outcome, Verification, ipfs_ref};
use super::parse::{self, ArticleMeta};
use super::refs::{self, Catalogue, Target};
use super::tzkt::ChainSnapshot;
use super::{MARKETPLACE_V3, unix_now};

/// Marks the file as an articles database (`PRAGMA application_id`).
pub const APPLICATION_ID: i64 = 0x5EED_A271;
pub const SCHEMA_VERSION: i64 = 3;
pub const MAX_LIMIT: u32 = 100;
pub const MAX_QUERY_CHARS: usize = 200;
/// Most search terms per query; more is BadRequest.
pub const MAX_QUERY_TERMS: usize = 8;
/// Most authors [`ArticleStore::counts_by_author`] takes in one call.
pub const MAX_AUTHOR_COUNTS: usize = 500;
/// Runs in which every gateway said "not found" (or served only unverified bytes that are not
/// text) before an object counts as absent.
pub const ABSENT_AFTER_RUNS: i64 = 3;
/// Seconds after which an object absent for not-found runs is fetched again by a plain sync.
pub const ABSENT_RECHECK_SECS: i64 = 7 * 86_400;
const BUSY_TIMEOUT: Duration = Duration::from_secs(2);
const CURSOR_DOMAIN: &[u8] = b"seeded-articles-cursor-v1\0";
const MAX_CURSOR_CHARS: usize = 512;

const SCHEMA_V1: &str = "
CREATE TABLE articles (
  token_id INTEGER PRIMARY KEY,
  author TEXT NOT NULL,
  created_level INTEGER NOT NULL,
  created_at TEXT NOT NULL,
  editions INTEGER NOT NULL,
  minted INTEGER NOT NULL,
  burned INTEGER NOT NULL,
  royalties INTEGER,
  metadata_locked INTEGER NOT NULL DEFAULT 0,
  metadata_uri TEXT,
  metadata_key TEXT,
  metadata_level INTEGER,
  revisions INTEGER NOT NULL DEFAULT 0,
  tzkt_metadata TEXT,
  metadata_source TEXT CHECK (metadata_source IN ('ipfs', 'tzkt')),
  title TEXT,
  description TEXT,
  language TEXT,
  tags TEXT NOT NULL DEFAULT '[]',
  thumbnail_uri TEXT,
  display_uri TEXT,
  thumbnail_caption TEXT,
  declared_minter TEXT,
  body_uri TEXT,
  body_key TEXT,
  flag TEXT NOT NULL DEFAULT 'NONE',
  moderation_reason TEXT,
  hidden INTEGER NOT NULL DEFAULT 0 CHECK (hidden IN (0, 1)),
  chain_level INTEGER NOT NULL
);
CREATE INDEX articles_visible ON articles (hidden, token_id);
CREATE INDEX articles_author ON articles (author, hidden, token_id);
CREATE INDEX articles_body ON articles (body_key);
CREATE INDEX articles_metadata ON articles (metadata_key);
CREATE TABLE article_splits (
  token_id INTEGER NOT NULL REFERENCES articles (token_id) ON DELETE CASCADE,
  position INTEGER NOT NULL,
  address TEXT NOT NULL,
  share INTEGER NOT NULL,
  PRIMARY KEY (token_id, position)
) WITHOUT ROWID;
CREATE INDEX article_splits_address ON article_splits (address);
CREATE TABLE holders (
  token_id INTEGER NOT NULL,
  address TEXT NOT NULL,
  amount INTEGER NOT NULL CHECK (amount > 0),
  last_level INTEGER NOT NULL,
  PRIMARY KEY (token_id, address)
) WITHOUT ROWID;
CREATE INDEX holders_address ON holders (address);
CREATE TABLE listings (
  listing_id INTEGER PRIMARY KEY,
  token_id INTEGER NOT NULL,
  seller TEXT NOT NULL,
  amount INTEGER NOT NULL CHECK (amount > 0),
  price_mutez INTEGER,
  last_level INTEGER NOT NULL
);
CREATE INDEX listings_token ON listings (token_id);
CREATE INDEX listings_seller ON listings (seller);
CREATE TABLE moderation (
  token_id INTEGER PRIMARY KEY,
  state INTEGER NOT NULL,
  reason_id INTEGER,
  last_level INTEGER NOT NULL
);
CREATE TABLE moderation_reasons (id INTEGER PRIMARY KEY, text TEXT NOT NULL);
CREATE TABLE names (
  address TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  last_level INTEGER NOT NULL
) WITHOUT ROWID;
CREATE TABLE objects (
  uri TEXT PRIMARY KEY,
  kind TEXT NOT NULL CHECK (kind IN ('metadata', 'body')),
  state TEXT NOT NULL CHECK (state IN ('ok', 'transient', 'absent')),
  verification TEXT CHECK (verification IN ('ipfs-car', 'sha256', 'identity', 'unverified')),
  format TEXT CHECK (format IN ('markdown', 'json')),
  data TEXT,
  sha256 TEXT,
  bytes INTEGER,
  gateway TEXT,
  attempts INTEGER NOT NULL DEFAULT 0,
  not_found_runs INTEGER NOT NULL DEFAULT 0,
  error TEXT,
  updated_at INTEGER NOT NULL,
  CHECK ((state = 'ok') = (data IS NOT NULL)),
  CHECK ((state = 'ok') = (verification IS NOT NULL))
);
CREATE INDEX objects_state ON objects (kind, state);
CREATE TABLE object_media (
  uri TEXT NOT NULL REFERENCES objects (uri) ON DELETE CASCADE,
  position INTEGER NOT NULL,
  kind TEXT NOT NULL,
  src TEXT NOT NULL,
  ipfs TEXT,
  PRIMARY KEY (uri, position)
) WITHOUT ROWID;
CREATE TABLE sync_state (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  chain_level INTEGER,
  chain_synced_at INTEGER,
  fetch_run_at INTEGER
);
INSERT INTO sync_state (id) VALUES (1);
CREATE VIRTUAL TABLE articles_fts USING fts5 (
  title, authors, tags, tokenize = 'unicode61 remove_diacritics 2'
);
";

/// v1 -> v2, additive: article references and the visibility of referenced projects.
const SCHEMA_V2: &str = "
ALTER TABLE articles ADD COLUMN refs_key TEXT;
ALTER TABLE articles ADD COLUMN refs_rules INTEGER;
CREATE TABLE article_refs (
  article_id INTEGER NOT NULL REFERENCES articles (token_id) ON DELETE CASCADE,
  target TEXT NOT NULL,
  position INTEGER NOT NULL,
  kind TEXT NOT NULL CHECK (kind IN ('project', 'token')),
  form TEXT NOT NULL,
  project_id INTEGER CHECK (project_id IS NULL OR project_id >= 0),
  contract TEXT,
  token_id INTEGER CHECK (token_id IS NULL OR token_id >= 0),
  source_text TEXT NOT NULL,
  PRIMARY KEY (article_id, target),
  CHECK ((contract IS NULL) = (token_id IS NULL)),
  CHECK (kind = 'token' OR contract IS NULL)
) WITHOUT ROWID;
CREATE INDEX article_refs_project ON article_refs (project_id, article_id)
  WHERE project_id IS NOT NULL;
CREATE INDEX article_refs_token ON article_refs (contract, token_id) WHERE token_id IS NOT NULL;
CREATE INDEX article_refs_target ON article_refs (target);
CREATE TABLE ref_projects (
  project_id INTEGER PRIMARY KEY CHECK (project_id >= 0),
  hidden INTEGER NOT NULL CHECK (hidden IN (0, 1)),
  checked_at INTEGER NOT NULL
);
";

/// v2 -> v3: `names` holds the names the site displays, nothing else. Registry names are
/// dropped (the site may hide them) and the index loses them in the same transaction.
const SCHEMA_V3: &str = "
DROP TABLE names;
CREATE TABLE names (address TEXT PRIMARY KEY, name TEXT NOT NULL) WITHOUT ROWID;
";

/// Rebuilds the search index from articles and `names`. `authors` is the address plus the
/// displayed name; [`super::names`] rewrites it for authors whose name changed.
const INDEX_ARTICLES: &str = "
DELETE FROM articles_fts;
INSERT INTO articles_fts (rowid, title, authors, tags)
  SELECT a.token_id, COALESCE(a.title, ''), a.author || ' ' || COALESCE(n.name, ''),
    COALESCE((SELECT group_concat(value, ' ') FROM json_each(a.tags)), '')
  FROM articles a LEFT JOIN names n ON n.address = a.author;
";

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ArticleStoreError {
    #[error("{what} not found: {key}")]
    NotFound { what: &'static str, key: String },
    #[error("bad request: {0}")]
    BadRequest(String),
    /// Retryable: busy, locked, I/O, or not yet initialised.
    #[error("articles store unavailable: {0}")]
    Unavailable(String),
    #[error("internal articles store error: {0}")]
    Internal(String),
}

type Result<T> = std::result::Result<T, ArticleStoreError>;

fn classify(e: &rusqlite::Error) -> ArticleStoreError {
    use rusqlite::ffi::ErrorCode as C;
    match e {
        rusqlite::Error::SqliteFailure(f, _)
            if matches!(
                f.code,
                C::DatabaseBusy | C::DatabaseLocked | C::SystemIoFailure | C::CannotOpen
            ) =>
        {
            ArticleStoreError::Unavailable(e.to_string())
        }
        _ => ArticleStoreError::Internal(e.to_string()),
    }
}

pub(super) trait Db<T> {
    fn db(self) -> Result<T>;
}

impl<T> Db<T> for rusqlite::Result<T> {
    fn db(self) -> Result<T> {
        self.map_err(|e| classify(&e))
    }
}

/// What an object holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectKind {
    Metadata,
    Body,
}

impl ObjectKind {
    pub fn as_str(self) -> &'static str {
        match self {
            ObjectKind::Metadata => "metadata",
            ObjectKind::Body => "body",
        }
    }
}

/// Fetch state of an article's body (or metadata) object.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum FetchState {
    Ok,
    /// Failed for now; the next sync retries.
    Transient,
    /// Permanent: no usable URI, a directory, over the size cap, not text, or not found on
    /// every gateway in [`ABSENT_AFTER_RUNS`] runs (tried again after
    /// [`ABSENT_RECHECK_SECS`]).
    Absent,
    /// Not attempted yet.
    Pending,
}

impl FetchState {
    fn parse(s: &str) -> FetchState {
        match s {
            "ok" => FetchState::Ok,
            "absent" => FetchState::Absent,
            _ => FetchState::Transient,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            FetchState::Ok => "ok",
            FetchState::Transient => "transient",
            FetchState::Absent => "absent",
            FetchState::Pending => "pending",
        }
    }
}

/// One article in a listing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ArticleSummary {
    pub token_id: u64,
    /// `None` when no metadata could be read.
    pub title: Option<String>,
    pub description: Option<String>,
    /// The minting address (`token_data.minter`).
    pub author: String,
    /// The name the site displayed for the author at the last run with a catalogue (see
    /// [`super::names`]); `None` means the address is shown. The web labels authors from the
    /// catalogue itself, so this is what search matched.
    pub author_name: Option<String>,
    pub created_level: u64,
    /// ISO 8601 UTC.
    pub created_at: String,
    /// Editions in existence (minted minus burned).
    pub editions: u64,
    pub thumbnail_uri: Option<String>,
    pub tags: Vec<String>,
    pub language: Option<String>,
    pub body_state: FetchState,
}

/// A royalty split recipient (fx(text) has no on-chain co-author field; the split is the
/// minter's own declaration of who shares royalties).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Split {
    pub address: String,
    /// Per mille of the royalties.
    pub share: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MediaRef {
    pub kind: String,
    pub src: String,
    pub ipfs: Option<String>,
}

/// The article text and how it was obtained.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ArticleBody {
    /// `artifactUri` as written in the metadata.
    pub uri: Option<String>,
    pub state: FetchState,
    /// `ipfs-car`, `sha256`, `identity` or `unverified`; `None` unless `state` is Ok.
    pub verification: Option<String>,
    /// Raw fx-markdown; untrusted, sanitize before rendering.
    pub text: Option<String>,
    pub format: &'static str,
    pub sha256: Option<String>,
    pub bytes: Option<u64>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Article {
    #[serde(flatten)]
    pub summary: ArticleSummary,
    pub splits: Vec<Split>,
    /// Per mille.
    pub royalties: Option<u64>,
    pub metadata_locked: bool,
    /// Metadata updates after the mint.
    pub revisions: u64,
    pub metadata_uri: Option<String>,
    /// `ipfs` (fetched here) or `tzkt` (TzKT's copy, until ours is fetched).
    pub metadata_source: Option<String>,
    pub display_uri: Option<String>,
    pub thumbnail_caption: Option<String>,
    /// `NONE` or `CLEAN` for visible articles.
    pub flag: String,
    pub body: ArticleBody,
    pub media: Vec<MediaRef>,
    pub holders: u64,
    /// Editions currently listed on the fxhash marketplace.
    pub listed: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ArticleHolder {
    pub address: String,
    /// The displayed name when the holder is also an article author (only authors are copied).
    pub name: Option<String>,
    /// Editions held, listed ones included.
    pub amount: u64,
    /// Of `amount`, editions in marketplace escrow listed by this address.
    pub listed: u64,
    /// The marketplace escrow itself (editions no listing accounts for).
    pub escrow: bool,
}

/// Opaque, URL-safe position after an item of one listing; only resumes the listing that
/// issued it.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct ArticleCursor(String);

fn bad_cursor(why: &str) -> ArticleStoreError {
    ArticleStoreError::BadRequest(format!("invalid cursor: {why}"))
}

fn cursor_sum(payload: &[u8]) -> [u8; 8] {
    let mut h = Sha256::new();
    h.update(CURSOR_DOMAIN);
    h.update(payload);
    let d = h.finalize();
    let mut out = [0u8; 8];
    out.copy_from_slice(&d[..8]);
    out
}

impl ArticleCursor {
    pub fn parse(s: &str) -> Result<ArticleCursor> {
        let c = ArticleCursor(s.to_owned());
        c.payload()?;
        Ok(c)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn payload(&self) -> Result<(String, i64)> {
        let s = &self.0;
        if s.is_empty() || s.len() > MAX_CURSOR_CHARS {
            return Err(bad_cursor("length"));
        }
        if !s
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(bad_cursor("not lowercase hex"));
        }
        let bytes = hex::decode(s).map_err(|_| bad_cursor("not hex"))?;
        if bytes.len() < 9 {
            return Err(bad_cursor("too short"));
        }
        let (sum, payload) = bytes.split_at(8);
        if sum != cursor_sum(payload) {
            return Err(bad_cursor("checksum"));
        }
        serde_json::from_slice(payload).map_err(|_| bad_cursor("payload"))
    }

    fn encode(listing: &str, token_id: i64) -> ArticleCursor {
        let bytes = serde_json::to_vec(&(listing, token_id)).unwrap_or_default();
        let mut all = cursor_sum(&bytes).to_vec();
        all.extend_from_slice(&bytes);
        ArticleCursor(hex::encode(all))
    }

    fn key(&self, listing: &str) -> Result<i64> {
        let (l, id) = self.payload()?;
        if l != listing {
            return Err(bad_cursor("issued for another listing"));
        }
        Ok(id)
    }
}

impl std::fmt::Display for ArticleCursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ArticlePage {
    pub items: Vec<ArticleSummary>,
    pub next: Option<ArticleCursor>,
    /// Items matching the filter, all pages.
    pub total: u64,
}

/// An article an address holds editions of.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HeldArticle {
    #[serde(flatten)]
    pub summary: ArticleSummary,
    /// Editions held, listed ones included.
    pub amount: u64,
    /// Of `amount`, editions in marketplace escrow listed by this address.
    pub listed: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HeldArticlePage {
    pub items: Vec<HeldArticle>,
    pub next: Option<ArticleCursor>,
    /// Articles held, all pages.
    pub total: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum RefKind {
    Project,
    Token,
}

/// A reference from an article to a visible fxhash project or one of its tokens.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ArticleRef {
    pub kind: RefKind,
    /// How it was written: `storage-pointer`, `fxhash-url`, `marketplace-url` or `generator`.
    pub form: String,
    pub project_id: u64,
    /// GENTK contract; `Some` for token references.
    pub contract: Option<String>,
    pub token_id: Option<u64>,
    /// The directive or URL as written (capped). Untrusted.
    pub source_text: String,
}

/// What [`ArticleStore::resolve_refs`] changed.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct ResolveReport {
    /// Distinct targets looked up in the catalogue.
    pub looked_up: u64,
    /// Reference rows that gained a project this run.
    pub resolved: u64,
    /// Generator references added this run.
    pub generators: u64,
    /// Referenced projects whose visibility was read from the catalogue.
    pub projects_checked: u64,
    /// Rows still without a project.
    pub unresolved: u64,
}

/// Builds an FTS5 query exactly as the catalogue store does: each alphanumeric run becomes a
/// quoted term (a prefix term unless one character long), terms are ANDed; at most
/// [`MAX_QUERY_TERMS`] terms and [`MAX_QUERY_CHARS`] characters.
pub fn fts_query(text: &str) -> Result<Option<String>> {
    let text = text.trim();
    if text.chars().count() > MAX_QUERY_CHARS {
        return Err(ArticleStoreError::BadRequest(format!(
            "search text longer than {MAX_QUERY_CHARS} characters"
        )));
    }
    let terms: Vec<&str> = text
        .split(|c: char| !c.is_alphanumeric())
        .filter(|t| !t.is_empty())
        .collect();
    if terms.len() > MAX_QUERY_TERMS {
        return Err(ArticleStoreError::BadRequest(
            "too many search terms".into(),
        ));
    }
    let terms: Vec<String> = terms
        .into_iter()
        .map(|t| {
            let quoted = format!("\"{}\"", t.replace('"', "\"\""));
            if t.chars().count() == 1 {
                quoted
            } else {
                quoted + "*"
            }
        })
        .collect();
    Ok((!terms.is_empty()).then(|| terms.join(" ")))
}

/// Restricts a listing to articles whose search row matches the bound FTS5 query.
const FTS_FILTER: &str =
    " AND a.token_id IN (SELECT rowid FROM articles_fts WHERE articles_fts MATCH ?)";

/// The FTS5 query for optional search text; blank text matches everything.
fn text_filter(text: Option<&str>) -> Result<Option<String>> {
    match text {
        Some(t) => fts_query(t),
        None => Ok(None),
    }
}

fn listing_id(parts: &[&str]) -> String {
    let mut h = Sha256::new();
    for p in parts {
        h.update(p.as_bytes());
        h.update([0u8]);
    }
    hex::encode(&h.finalize()[..8])
}

fn check_limit(limit: u32) -> Result<()> {
    if (1..=MAX_LIMIT).contains(&limit) {
        Ok(())
    } else {
        Err(ArticleStoreError::BadRequest(format!(
            "limit must be 1..={MAX_LIMIT}"
        )))
    }
}

fn check_address(address: &str) -> Result<()> {
    let ok = address.len() == 36
        && ["tz1", "tz2", "tz3", "tz4", "KT1"]
            .iter()
            .any(|p| address.starts_with(p))
        && address.bytes().all(|b| b.is_ascii_alphanumeric());
    if ok {
        Ok(())
    } else {
        Err(ArticleStoreError::BadRequest(
            "invalid Tezos address".into(),
        ))
    }
}

fn sql_id(id: u64) -> Result<i64> {
    i64::try_from(id).map_err(|_| ArticleStoreError::NotFound {
        what: "article",
        key: id.to_string(),
    })
}

fn get_u64(r: &Row<'_>, i: usize) -> rusqlite::Result<u64> {
    let v: i64 = r.get(i)?;
    u64::try_from(v).map_err(|_| rusqlite::Error::IntegralValueOutOfRange(i, v))
}

fn tags_of(json: &str) -> Vec<String> {
    serde_json::from_str::<Vec<String>>(json).unwrap_or_default()
}

/// Columns read into an [`ArticleSummary`]; `a` = articles, `n` = names, `o` = body object.
const SUMMARY_COLUMNS: &str = "a.token_id, a.title, a.description, a.author, n.name, \
     a.created_level, a.created_at, a.editions, a.thumbnail_uri, a.tags, a.language, \
     a.body_uri, a.body_key, o.state";
const SUMMARY_FROM: &str = "FROM articles a LEFT JOIN names n ON n.address = a.author \
     LEFT JOIN objects o ON o.uri = a.body_key";

fn body_state(uri: Option<String>, key: Option<String>, state: Option<String>) -> FetchState {
    match (uri, key, state) {
        (None, _, _) | (_, None, _) => FetchState::Absent,
        (_, _, None) => FetchState::Pending,
        (_, _, Some(s)) => FetchState::parse(&s),
    }
}

fn summary(r: &Row<'_>) -> rusqlite::Result<ArticleSummary> {
    Ok(ArticleSummary {
        token_id: get_u64(r, 0)?,
        title: r.get(1)?,
        description: r.get(2)?,
        author: r.get(3)?,
        author_name: r.get(4)?,
        created_level: get_u64(r, 5)?,
        created_at: r.get(6)?,
        editions: get_u64(r, 7)?,
        thumbnail_uri: r.get(8)?,
        tags: tags_of(&r.get::<_, String>(9)?),
        language: r.get(10)?,
        body_state: body_state(r.get(11)?, r.get(12)?, r.get(13)?),
    })
}

/// Counts for `stats` and operations.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct ArticleStats {
    pub articles: u64,
    pub visible: u64,
    pub hidden: u64,
    pub metadata_ipfs: u64,
    pub metadata_tzkt: u64,
    pub metadata_none: u64,
    /// `(kind, state, verification, count, bytes)`.
    pub objects: Vec<(String, String, Option<String>, u64, u64)>,
    pub articles_body_ok: u64,
    pub articles_body_pending: u64,
    pub articles_body_transient: u64,
    pub articles_body_absent: u64,
    pub body_bytes: u64,
    pub body_max_bytes: u64,
    pub media: u64,
    pub media_ipfs: u64,
    pub holder_rows: u64,
    pub distinct_holders: u64,
    pub listings: u64,
    pub listed_editions: u64,
    pub authors: u64,
    pub named_authors: u64,
    pub chain_level: Option<u64>,
    pub chain_synced_at: Option<i64>,
    /// Reference rows, hidden articles included.
    pub refs: u64,
    /// Rows without a project yet (token or slug not in the catalogue at the last run).
    pub refs_unresolved: u64,
    /// Articles with at least one reference row.
    pub ref_articles: u64,
    /// Visible articles with at least one reference to a visible project.
    pub ref_articles_visible: u64,
    /// Distinct projects referenced (resolved rows).
    pub ref_projects: u64,
    /// Of those, known to the catalogue and visible / flagged; the rest were never seen.
    pub ref_projects_visible: u64,
    pub ref_projects_hidden: u64,
    /// Distinct GENTK tokens referenced.
    pub ref_tokens: u64,
    /// `(kind, form, rows, articles)`.
    pub refs_by_form: Vec<(String, String, u64, u64)>,
}

/// What [`ArticleStore::apply_snapshot`] wrote.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SnapshotReport {
    pub articles: usize,
    /// Tokens TzKT listed without `token_data` yet (minted mid-snapshot): next run.
    pub skipped: usize,
    pub holders: usize,
    pub listings: usize,
    pub moderation: usize,
}

/// One object needing a fetch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingObject {
    pub key: String,
    pub kind: ObjectKind,
}

pub struct ArticleStore {
    pub(super) conn: Connection,
}

impl ArticleStore {
    /// Opens or creates a database file (read-write) and migrates it.
    pub fn open(path: &Path) -> Result<ArticleStore> {
        let conn = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .db()?;
        conn.busy_timeout(BUSY_TIMEOUT).db()?;
        let mode: String = conn
            .pragma_update_and_check(None, "journal_mode", "WAL", |r| r.get(0))
            .db()?;
        if !mode.eq_ignore_ascii_case("wal") {
            return Err(ArticleStoreError::Internal(format!(
                "journal_mode is {mode}, expected wal"
            )));
        }
        conn.pragma_update(None, "synchronous", "NORMAL").db()?;
        Self::finish_open(conn)
    }

    /// Read-only, for the web. Unavailable until a sync has initialised the file.
    pub fn open_read_only(path: &Path) -> Result<ArticleStore> {
        let conn = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .db()?;
        conn.busy_timeout(BUSY_TIMEOUT).db()?;
        conn.pragma_update(None, "foreign_keys", true).db()?;
        let store = ArticleStore { conn };
        match store.versions()? {
            (APPLICATION_ID, SCHEMA_VERSION) => Ok(store),
            (0, 0) => Err(ArticleStoreError::Unavailable(
                "articles database is not initialised".into(),
            )),
            (APPLICATION_ID, v) if v < SCHEMA_VERSION => {
                Err(ArticleStoreError::Unavailable(format!(
                    "articles database is at schema {v}; `seeded-articles sync` or `refs` \
                     migrates it to {SCHEMA_VERSION}"
                )))
            }
            (app, v) => Err(ArticleStoreError::Internal(format!(
                "not an articles database at schema {SCHEMA_VERSION} \
                 (application_id {app:#x}, user_version {v})"
            ))),
        }
    }

    pub fn open_in_memory() -> Result<ArticleStore> {
        Self::finish_open(Connection::open_in_memory().db()?)
    }

    fn versions(&self) -> Result<(i64, i64)> {
        let app: i64 = self
            .conn
            .pragma_query_value(None, "application_id", |r| r.get(0))
            .db()?;
        let v: i64 = self
            .conn
            .pragma_query_value(None, "user_version", |r| r.get(0))
            .db()?;
        Ok((app, v))
    }

    fn finish_open(conn: Connection) -> Result<ArticleStore> {
        conn.pragma_update(None, "foreign_keys", true).db()?;
        let mut store = ArticleStore { conn };
        match store.versions()? {
            (APPLICATION_ID, SCHEMA_VERSION) => {}
            (APPLICATION_ID, 1 | 2) => {
                // v2: references are backfilled from stored bodies, no network. v3: names
                // wait for the next run with a catalogue.
                let tx = store
                    .conn
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .db()?;
                // Another writer may have migrated while we waited for the lock.
                let v: i64 = tx
                    .pragma_query_value(None, "user_version", |r| r.get(0))
                    .db()?;
                if v == 1 {
                    tx.execute_batch(SCHEMA_V2).db()?;
                    extract_refs(&tx)?;
                }
                if v == 1 || v == 2 {
                    tx.execute_batch(SCHEMA_V3).db()?;
                    tx.execute_batch(INDEX_ARTICLES).db()?;
                    tx.pragma_update(None, "user_version", SCHEMA_VERSION)
                        .db()?;
                }
                tx.commit().db()?;
            }
            (0, 0) => {
                let tx = store
                    .conn
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .db()?;
                let empty: i64 = tx
                    .query_row("SELECT count(*) FROM sqlite_schema", [], |r| r.get(0))
                    .db()?;
                if empty != 0 {
                    return Err(ArticleStoreError::Internal(
                        "refusing to initialise a non-empty database that is not an \
                         articles database"
                            .into(),
                    ));
                }
                tx.execute_batch(SCHEMA_V1).db()?;
                tx.execute_batch(SCHEMA_V2).db()?;
                tx.execute_batch(SCHEMA_V3).db()?;
                tx.pragma_update(None, "application_id", APPLICATION_ID)
                    .db()?;
                tx.pragma_update(None, "user_version", SCHEMA_VERSION)
                    .db()?;
                tx.commit().db()?;
            }
            (app, v) => {
                return Err(ArticleStoreError::Internal(format!(
                    "not an articles database at schema {SCHEMA_VERSION} \
                     (application_id {app:#x}, user_version {v})"
                )));
            }
        }
        Ok(store)
    }

    /// Runs `f` in one read transaction, so multi-query reads see one state.
    fn read<T>(&self, f: impl FnOnce(&Connection) -> Result<T>) -> Result<T> {
        let tx = self.conn.unchecked_transaction().db()?;
        let out = f(&tx)?;
        tx.finish().db()?;
        Ok(out)
    }

    // ---- sync writes ----

    /// Replaces chain facts with `snap` in one transaction, then re-derives articles. Author
    /// names are kept: only [`ArticleStore::resolve_names`] changes them.
    pub fn apply_snapshot(&mut self, snap: &ChainSnapshot) -> Result<SnapshotReport> {
        let mut report = SnapshotReport::default();
        let level = i64::try_from(snap.level)
            .map_err(|_| ArticleStoreError::Internal("head level overflows".into()))?;
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .db()?;
        {
            let data: std::collections::HashMap<u64, &super::tzkt::TokenData> =
                snap.token_data.iter().map(|d| (d.token_id, d)).collect();
            let pointers: std::collections::HashMap<u64, &super::tzkt::MetadataPointer> =
                snap.pointers.iter().map(|p| (p.token_id, p)).collect();
            let mut upsert = tx
                .prepare(
                    "INSERT INTO articles (token_id, author, created_level, created_at, editions, \
                     minted, burned, royalties, metadata_locked, metadata_uri, metadata_key, \
                     metadata_level, revisions, tzkt_metadata, chain_level)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)
                     ON CONFLICT (token_id) DO UPDATE SET author = excluded.author,
                       created_level = excluded.created_level, created_at = excluded.created_at,
                       editions = excluded.editions, minted = excluded.minted,
                       burned = excluded.burned, royalties = excluded.royalties,
                       metadata_locked = excluded.metadata_locked,
                       metadata_uri = excluded.metadata_uri, metadata_key = excluded.metadata_key,
                       metadata_level = excluded.metadata_level, revisions = excluded.revisions,
                       tzkt_metadata = excluded.tzkt_metadata, chain_level = excluded.chain_level",
                )
                .db()?;
            let mut split = tx
                .prepare(
                    "INSERT INTO article_splits (token_id, position, address, share)
                     VALUES (?1, ?2, ?3, ?4)",
                )
                .db()?;
            tx.execute("DELETE FROM article_splits", []).db()?;
            for t in &snap.tokens {
                let Some(d) = data.get(&t.token_id) else {
                    report.skipped += 1;
                    continue;
                };
                let p = pointers.get(&t.token_id);
                let uri = p.and_then(|p| p.uri.clone());
                let key = uri.as_deref().and_then(|u| ipfs_ref(u).ok()).map(|r| r.key);
                let id = sql_id(t.token_id)?;
                upsert
                    .execute(params![
                        id,
                        d.minter,
                        t.first_level as i64,
                        t.first_time,
                        t.supply,
                        t.minted,
                        t.burned,
                        d.royalties,
                        d.metadata_locked,
                        uri,
                        key,
                        p.map(|p| p.last_level as i64),
                        p.map_or(0, |p| p.updates.saturating_sub(1) as i64),
                        t.metadata.as_ref().map(Value::to_string),
                        level,
                    ])
                    .db()?;
                for (i, (address, share)) in d.splits.iter().enumerate() {
                    split.execute(params![id, i as i64, address, share]).db()?;
                }
                report.articles += 1;
            }
        }
        tx.execute("DELETE FROM holders", []).db()?;
        {
            let mut ins = tx
                .prepare(
                    "INSERT INTO holders (token_id, address, amount, last_level)
                     VALUES (?1, ?2, ?3, ?4)
                     ON CONFLICT (token_id, address) DO UPDATE SET amount = excluded.amount,
                       last_level = excluded.last_level",
                )
                .db()?;
            for b in &snap.balances {
                ins.execute(params![
                    b.token_id as i64,
                    b.address,
                    b.amount,
                    b.last_level as i64
                ])
                .db()?;
                report.holders += 1;
            }
        }
        tx.execute("DELETE FROM listings", []).db()?;
        {
            let mut ins = tx
                .prepare(
                    "INSERT INTO listings (listing_id, token_id, seller, amount, price_mutez, \
                     last_level) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                )
                .db()?;
            for l in snap.listings.iter().filter(|l| l.amount > 0) {
                ins.execute(params![
                    l.listing_id as i64,
                    l.token_id as i64,
                    l.seller,
                    l.amount,
                    l.price_mutez,
                    l.last_level as i64
                ])
                .db()?;
                report.listings += 1;
            }
        }
        tx.execute("DELETE FROM moderation", []).db()?;
        tx.execute("DELETE FROM moderation_reasons", []).db()?;
        {
            let mut ins = tx
                .prepare(
                    "INSERT OR REPLACE INTO moderation (token_id, state, reason_id, last_level)
                     VALUES (?1, ?2, ?3, ?4)",
                )
                .db()?;
            for m in &snap.moderation {
                ins.execute(params![
                    m.token_id as i64,
                    m.state,
                    m.reason_id,
                    m.last_level as i64
                ])
                .db()?;
                report.moderation += 1;
            }
            let mut ins = tx
                .prepare("INSERT OR REPLACE INTO moderation_reasons (id, text) VALUES (?1, ?2)")
                .db()?;
            for (id, text) in &snap.reasons {
                ins.execute(params![id, text]).db()?;
            }
        }
        tx.execute(
            "UPDATE sync_state SET chain_level = ?1, chain_synced_at = ?2 WHERE id = 1",
            params![level, unix_now()],
        )
        .db()?;
        derive(&tx)?;
        extract_refs(&tx)?;
        tx.commit().db()?;
        Ok(report)
    }

    /// Objects referenced by articles that are not stored yet or failed transiently, and
    /// those absent for not-found runs whose last try is [`ABSENT_RECHECK_SECS`] old; with
    /// `recheck`, also every absent one and unverified copies. Newest articles first.
    pub fn pending_objects(&self, kind: ObjectKind, recheck: bool) -> Result<Vec<PendingObject>> {
        let column = match kind {
            ObjectKind::Metadata => "metadata_key",
            ObjectKind::Body => "body_key",
        };
        let sql = format!(
            "SELECT a.{column}, MAX(a.token_id) AS newest FROM articles a
             LEFT JOIN objects o ON o.uri = a.{column}
             WHERE a.{column} IS NOT NULL
               AND (o.uri IS NULL OR o.state = 'transient'
                    OR (o.state = 'absent' AND o.not_found_runs >= ?2 AND o.updated_at <= ?3)
                    OR (?1 AND (o.state = 'absent' OR o.verification = 'unverified')))
             GROUP BY a.{column} ORDER BY newest DESC"
        );
        let mut stmt = self.conn.prepare(&sql).db()?;
        let due = unix_now().saturating_sub(ABSENT_RECHECK_SECS);
        let rows = stmt
            .query_map(params![recheck, ABSENT_AFTER_RUNS, due], |r| {
                r.get::<_, String>(0)
            })
            .db()?;
        rows.map(|r| r.map(|key| PendingObject { key, kind }))
            .collect::<rusqlite::Result<_>>()
            .db()
    }

    /// Records fetch outcomes in one transaction. A stored `ok` object is only ever replaced
    /// by a verified copy of an unverified one. Returns the state written for each (None when
    /// the stored row was kept).
    pub fn record_objects(
        &mut self,
        results: &[(PendingObject, Outcome)],
    ) -> Result<Vec<Option<FetchState>>> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .db()?;
        let mut out = Vec::with_capacity(results.len());
        for (item, outcome) in results {
            out.push(record_object(&tx, item, outcome)?);
        }
        if results.iter().any(|(i, _)| i.kind == ObjectKind::Metadata) {
            derive(&tx)?;
        }
        extract_refs(&tx)?;
        tx.execute(
            "UPDATE sync_state SET fetch_run_at = ?1 WHERE id = 1",
            [unix_now()],
        )
        .db()?;
        tx.commit().db()?;
        Ok(out)
    }

    /// Articles stored, hidden ones included.
    pub fn article_count(&self) -> Result<u64> {
        self.conn
            .query_row("SELECT count(*) FROM articles", [], |r| r.get::<_, i64>(0))
            .db()
            .map(|n| n.max(0) as u64)
    }

    // ---- reads ----

    /// Visible articles, newest first, optionally matching `text` (title, author address or
    /// displayed name, tags; see [`fts_query`]).
    pub fn articles_page(
        &self,
        text: Option<&str>,
        after: Option<&ArticleCursor>,
        limit: u32,
    ) -> Result<ArticlePage> {
        check_limit(limit)?;
        let fts = text_filter(text)?;
        let listing = listing_id(&["articles", fts.as_deref().unwrap_or("")]);
        let mut filter = String::from("a.hidden = 0");
        let mut args: Vec<SqlValue> = Vec::new();
        if let Some(q) = &fts {
            filter.push_str(FTS_FILTER);
            args.push(SqlValue::Text(q.clone()));
        }
        self.page(&listing, &filter, args, after, limit)
    }

    /// Visible articles minted by `address`, newest first, optionally matching `text` as in
    /// [`ArticleStore::articles_page`].
    pub fn articles_by_author(
        &self,
        address: &str,
        text: Option<&str>,
        after: Option<&ArticleCursor>,
        limit: u32,
    ) -> Result<ArticlePage> {
        check_limit(limit)?;
        check_address(address)?;
        let fts = text_filter(text)?;
        let listing = listing_id(&["author", address, fts.as_deref().unwrap_or("")]);
        let mut filter = String::from("a.hidden = 0 AND a.author = ?");
        let mut args = vec![SqlValue::Text(address.to_owned())];
        if let Some(q) = fts {
            filter.push_str(FTS_FILTER);
            args.push(SqlValue::Text(q));
        }
        self.page(&listing, &filter, args, after, limit)
    }

    /// Visible article counts for `addresses` (at most [`MAX_AUTHOR_COUNTS`]); authors
    /// without any are absent.
    pub fn counts_by_author(
        &self,
        addresses: &[&str],
    ) -> Result<std::collections::HashMap<String, u64>> {
        if addresses.len() > MAX_AUTHOR_COUNTS {
            return Err(ArticleStoreError::BadRequest(format!(
                "at most {MAX_AUTHOR_COUNTS} authors per call"
            )));
        }
        if addresses.is_empty() {
            return Ok(std::collections::HashMap::new());
        }
        let sql = format!(
            "SELECT author, count(*) FROM articles WHERE hidden = 0 AND author IN ({}) \
             GROUP BY author",
            vec!["?"; addresses.len()].join(", ")
        );
        let mut stmt = self.conn.prepare(&sql).db()?;
        stmt.query_map(rusqlite::params_from_iter(addresses), |r| {
            Ok((r.get(0)?, get_u64(r, 1)?))
        })
        .db()?
        .collect::<rusqlite::Result<_>>()
        .db()
    }

    /// Visible articles `address` holds editions of, newest first, optionally matching `text`
    /// as in [`ArticleStore::articles_page`]. Editions in marketplace escrow count for the
    /// seller of the listing, as in [`ArticleStore::article_holders`]; the escrow contract
    /// itself holds only what no listing explains.
    pub fn articles_by_holder(
        &self,
        address: &str,
        text: Option<&str>,
        after: Option<&ArticleCursor>,
        limit: u32,
    ) -> Result<HeldArticlePage> {
        check_limit(limit)?;
        check_address(address)?;
        let fts = text_filter(text)?;
        let listing = listing_id(&["holder", address, fts.as_deref().unwrap_or("")]);
        // `?5` is NULL without search text.
        let matching = "(?5 IS NULL OR a.token_id IN \
             (SELECT rowid FROM articles_fts WHERE articles_fts MATCH ?5))";
        let after = after.map(|c| c.key(&listing)).transpose()?;
        const HELD: &str = "WITH held AS (
               SELECT token_id, amount AS held, 0 AS listed FROM holders
               WHERE address = ?1 AND address <> ?2
               UNION ALL
               SELECT token_id, 0, amount FROM listings WHERE seller = ?1
               UNION ALL
               SELECT h.token_id, h.amount - COALESCE((SELECT SUM(l.amount) FROM listings l
                 WHERE l.token_id = h.token_id), 0), 0
               FROM holders h WHERE h.address = ?1 AND ?1 = ?2
             ),
             agg AS (
               SELECT token_id, SUM(held) + SUM(listed) AS amount, SUM(listed) AS listed
               FROM held GROUP BY token_id HAVING SUM(held) + SUM(listed) > 0
             )";
        self.read(|conn| {
            let total: i64 = conn
                .query_row(
                    &format!(
                        "{HELD} SELECT count(*) FROM agg g JOIN articles a \
                         ON a.token_id = g.token_id WHERE a.hidden = 0 AND {matching}"
                    ),
                    params![address, MARKETPLACE_V3, None::<i64>, None::<i64>, fts],
                    |r| r.get(0),
                )
                .db()?;
            let mut stmt = conn
                .prepare(&format!(
                    "{HELD} SELECT {SUMMARY_COLUMNS}, g.amount, g.listed FROM agg g \
                     JOIN articles a ON a.token_id = g.token_id \
                     LEFT JOIN names n ON n.address = a.author \
                     LEFT JOIN objects o ON o.uri = a.body_key \
                     WHERE a.hidden = 0 AND a.token_id < ?3 AND {matching} \
                     ORDER BY a.token_id DESC LIMIT ?4"
                ))
                .db()?;
            let mut items = stmt
                .query_map(
                    params![
                        address,
                        MARKETPLACE_V3,
                        after.unwrap_or(i64::MAX),
                        i64::from(limit) + 1,
                        fts
                    ],
                    |r| {
                        Ok(HeldArticle {
                            summary: summary(r)?,
                            amount: get_u64(r, 14)?,
                            listed: get_u64(r, 15)?,
                        })
                    },
                )
                .db()?
                .collect::<rusqlite::Result<Vec<_>>>()
                .db()?;
            let next = if items.len() > limit as usize {
                items.truncate(limit as usize);
                items
                    .last()
                    .map(|h| ArticleCursor::encode(&listing, h.summary.token_id as i64))
            } else {
                None
            };
            Ok(HeldArticlePage {
                items,
                next,
                total: total.max(0) as u64,
            })
        })
    }

    /// Visible articles referencing project `project_id` (directly or through one of its
    /// tokens), newest first. Empty unless the catalogue knows the project and it is not
    /// flagged.
    pub fn articles_for_project(
        &self,
        project_id: u64,
        after: Option<&ArticleCursor>,
        limit: u32,
    ) -> Result<ArticlePage> {
        check_limit(limit)?;
        let id = i64::try_from(project_id)
            .map_err(|_| ArticleStoreError::BadRequest("invalid project id".into()))?;
        let listing = listing_id(&["project", &project_id.to_string()]);
        self.page(
            &listing,
            "a.hidden = 0 AND a.token_id IN (SELECT r.article_id FROM article_refs r \
             WHERE r.project_id = ?) AND EXISTS (SELECT 1 FROM ref_projects p \
             WHERE p.project_id = ? AND p.hidden = 0)",
            vec![SqlValue::Integer(id), SqlValue::Integer(id)],
            after,
            limit,
        )
    }

    /// References of a visible article to visible projects and their tokens, in order of
    /// appearance, one per distinct project or token. Hidden or unknown article: NotFound.
    pub fn article_refs(&self, token_id: u64) -> Result<Vec<ArticleRef>> {
        let id = sql_id(token_id)?;
        self.read(|conn| {
            let visible: Option<i64> = conn
                .query_row(
                    "SELECT token_id FROM articles WHERE token_id = ?1 AND hidden = 0",
                    [id],
                    |r| r.get(0),
                )
                .optional()
                .db()?;
            if visible.is_none() {
                return Err(ArticleStoreError::NotFound {
                    what: "article",
                    key: token_id.to_string(),
                });
            }
            let mut stmt = conn
                .prepare(
                    "SELECT r.kind, r.form, r.project_id, r.contract, r.token_id, r.source_text \
                     FROM article_refs r JOIN ref_projects p \
                       ON p.project_id = r.project_id AND p.hidden = 0 \
                     WHERE r.article_id = ?1 ORDER BY r.position LIMIT ?2",
                )
                .db()?;
            let rows = stmt
                .query_map(params![id, refs::MAX_REFS as i64], |r| {
                    let token_id: Option<i64> = r.get(4)?;
                    Ok(ArticleRef {
                        kind: if r.get::<_, String>(0)? == "token" {
                            RefKind::Token
                        } else {
                            RefKind::Project
                        },
                        form: r.get(1)?,
                        project_id: get_u64(r, 2)?,
                        contract: r.get(3)?,
                        token_id: token_id.map(|t| t.max(0) as u64),
                        source_text: r.get(5)?,
                    })
                })
                .db()?
                .collect::<rusqlite::Result<Vec<_>>>()
                .db()?;
            let mut seen = std::collections::HashSet::new();
            Ok(rows
                .into_iter()
                .filter(|r| {
                    seen.insert((
                        r.kind,
                        r.project_id,
                        r.contract.clone(),
                        r.token_id.filter(|_| r.kind == RefKind::Token),
                    ))
                })
                .collect())
        })
    }

    /// Extracts references for articles whose body changed since the last extraction (or
    /// whose rules are older). Returns how many articles were (re)extracted. Local only.
    pub fn refresh_refs(&mut self) -> Result<u64> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .db()?;
        let n = extract_refs(&tx)?;
        tx.commit().db()?;
        Ok(n)
    }

    /// Resolves references against the catalogue (`archive`, a read-only connection from
    /// [`refs::open_archive`]): token and slug references get their project, generator roots
    /// become project references, and referenced projects get their visibility. Positive
    /// only: a target the catalogue lacks keeps what an earlier run found.
    pub fn resolve_refs(&mut self, archive: &Connection) -> Result<ResolveReport> {
        let snapshot = archive.unchecked_transaction().db()?;
        let catalogue = Catalogue::load(&snapshot)?;
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .db()?;
        extract_refs(&tx)?;
        let mut report = ResolveReport::default();
        let targets: Vec<String> = {
            let mut stmt = tx
                .prepare(
                    "SELECT DISTINCT target FROM article_refs \
                     WHERE kind = 'token' OR target LIKE 'project-slug:%'",
                )
                .db()?;
            stmt.query_map([], |r| r.get(0))
                .db()?
                .collect::<rusqlite::Result<_>>()
                .db()?
        };
        {
            let mut set_project = tx
                .prepare_cached(
                    "UPDATE article_refs SET project_id = ?2 WHERE target = ?1 \
                     AND project_id IS NOT ?2",
                )
                .db()?;
            let mut set_token = tx
                .prepare_cached(
                    "UPDATE article_refs SET project_id = ?2, contract = ?3, token_id = ?4 \
                     WHERE target = ?1 AND (project_id IS NOT ?2 OR contract IS NOT ?3 \
                       OR token_id IS NOT ?4)",
                )
                .db()?;
            for key in &targets {
                let Some(target) = Target::from_key(key) else {
                    continue;
                };
                report.looked_up += 1;
                let changed = match target {
                    Target::Token { contract, token_id } => {
                        match catalogue.token_project(contract, token_id)? {
                            Some(p) => set_project.execute(params![key, p as i64]).db()?,
                            None => 0,
                        }
                    }
                    Target::ProjectSlug(s) => match catalogue.project_by_slug(&s) {
                        Some(p) => set_project.execute(params![key, p as i64]).db()?,
                        None => 0,
                    },
                    Target::TokenSlug(s) => match catalogue.token_by_slug(&s)? {
                        Some((p, Some((contract, token)))) => set_token
                            .execute(params![key, p as i64, contract, token as i64])
                            .db()?,
                        Some((p, None)) => set_project.execute(params![key, p as i64]).db()?,
                        None => 0,
                    },
                    Target::Project(_) | Target::Root { .. } => 0,
                };
                report.resolved += changed as u64;
            }
        }
        report.generators = add_generators(&tx, &catalogue)?;
        {
            let ids: Vec<i64> = {
                let mut stmt = tx
                    .prepare(
                        "SELECT DISTINCT project_id FROM article_refs \
                         WHERE project_id IS NOT NULL",
                    )
                    .db()?;
                stmt.query_map([], |r| r.get(0))
                    .db()?
                    .collect::<rusqlite::Result<_>>()
                    .db()?
            };
            let mut upsert = tx
                .prepare_cached(
                    "INSERT INTO ref_projects (project_id, hidden, checked_at) VALUES (?1, ?2, ?3)
                     ON CONFLICT (project_id) DO UPDATE SET hidden = excluded.hidden,
                       checked_at = excluded.checked_at",
                )
                .db()?;
            let now = unix_now();
            for id in ids {
                if let Some(hidden) = u64::try_from(id).ok().and_then(|p| catalogue.hidden(p)) {
                    upsert.execute(params![id, hidden, now]).db()?;
                    report.projects_checked += 1;
                }
            }
        }
        report.unresolved = tx
            .query_row(
                "SELECT count(*) FROM article_refs WHERE project_id IS NULL",
                [],
                |r| r.get::<_, i64>(0),
            )
            .db()?
            .max(0) as u64;
        tx.commit().db()?;
        drop(catalogue);
        snapshot.finish().db()?;
        Ok(report)
    }

    fn page(
        &self,
        listing: &str,
        filter: &str,
        args: Vec<SqlValue>,
        after: Option<&ArticleCursor>,
        limit: u32,
    ) -> Result<ArticlePage> {
        let after = after.map(|c| c.key(listing)).transpose()?;
        self.read(|conn| {
            let total: i64 = conn
                .query_row(
                    &format!("SELECT count(*) FROM articles a WHERE {filter}"),
                    rusqlite::params_from_iter(args.iter()),
                    |r| r.get(0),
                )
                .db()?;
            let mut page_args = args.clone();
            let mut sql = format!("SELECT {SUMMARY_COLUMNS} {SUMMARY_FROM} WHERE {filter}");
            if let Some(after) = after {
                sql.push_str(" AND a.token_id < ?");
                page_args.push(SqlValue::Integer(after));
            }
            sql.push_str(" ORDER BY a.token_id DESC LIMIT ?");
            page_args.push(SqlValue::Integer(i64::from(limit) + 1));
            let mut stmt = conn.prepare(&sql).db()?;
            let mut items = stmt
                .query_map(rusqlite::params_from_iter(page_args.iter()), summary)
                .db()?
                .collect::<rusqlite::Result<Vec<_>>>()
                .db()?;
            let next = if items.len() > limit as usize {
                items.truncate(limit as usize);
                items
                    .last()
                    .map(|s| ArticleCursor::encode(listing, s.token_id as i64))
            } else {
                None
            };
            Ok(ArticlePage {
                items,
                next,
                total: total as u64,
            })
        })
    }

    /// One visible article with its body. Hidden or unknown: NotFound.
    pub fn article(&self, token_id: u64) -> Result<Article> {
        let id = sql_id(token_id)?;
        self.read(|conn| {
            let not_found = || ArticleStoreError::NotFound {
                what: "article",
                key: token_id.to_string(),
            };
            let row: Option<(ArticleSummary, Detail)> = conn
                .query_row(
                    &format!(
                        "SELECT {SUMMARY_COLUMNS}, a.royalties, a.metadata_locked, a.revisions, \
                         a.metadata_uri, a.metadata_source, a.display_uri, a.thumbnail_caption, \
                         a.flag, a.body_uri, a.body_key {SUMMARY_FROM} \
                         WHERE a.token_id = ?1 AND a.hidden = 0"
                    ),
                    [id],
                    |r| {
                        Ok((
                            summary(r)?,
                            Detail {
                                royalties: r.get(14)?,
                                metadata_locked: r.get(15)?,
                                revisions: r.get(16)?,
                                metadata_uri: r.get(17)?,
                                metadata_source: r.get(18)?,
                                display_uri: r.get(19)?,
                                thumbnail_caption: r.get(20)?,
                                flag: r.get(21)?,
                                body_uri: r.get(22)?,
                                body_key: r.get(23)?,
                            },
                        ))
                    },
                )
                .optional()
                .db()?;
            let Some((
                summary,
                Detail {
                    royalties,
                    metadata_locked,
                    revisions,
                    metadata_uri,
                    metadata_source,
                    display_uri,
                    thumbnail_caption,
                    flag,
                    body_uri,
                    body_key,
                },
            )) = row
            else {
                return Err(not_found());
            };
            let mut stmt = conn
                .prepare(
                    "SELECT address, share FROM article_splits WHERE token_id = ?1 \
                     ORDER BY position",
                )
                .db()?;
            let splits = stmt
                .query_map([id], |r| {
                    Ok(Split {
                        address: r.get(0)?,
                        share: r.get::<_, i64>(1)?.max(0) as u64,
                    })
                })
                .db()?
                .collect::<rusqlite::Result<Vec<_>>>()
                .db()?;
            let object = match &body_key {
                Some(key) => conn
                    .query_row(
                        "SELECT state, verification, data, sha256, bytes, error FROM objects \
                         WHERE uri = ?1",
                        [key],
                        |r| {
                            Ok((
                                r.get::<_, String>(0)?,
                                r.get::<_, Option<String>>(1)?,
                                r.get::<_, Option<String>>(2)?,
                                r.get::<_, Option<String>>(3)?,
                                r.get::<_, Option<i64>>(4)?,
                                r.get::<_, Option<String>>(5)?,
                            ))
                        },
                    )
                    .optional()
                    .db()?,
                None => None,
            };
            let body = match (&body_uri, &body_key, object) {
                (None, _, _) => ArticleBody {
                    uri: None,
                    state: FetchState::Absent,
                    verification: None,
                    text: None,
                    format: "markdown",
                    sha256: None,
                    bytes: None,
                    error: Some(if metadata_source.is_some() {
                        "metadata names no body".into()
                    } else {
                        "no readable metadata".into()
                    }),
                },
                (Some(_), None, _) => ArticleBody {
                    uri: body_uri.clone(),
                    state: FetchState::Absent,
                    verification: None,
                    text: None,
                    format: "markdown",
                    sha256: None,
                    bytes: None,
                    error: Some("body URI is not a usable ipfs:// URI".into()),
                },
                (Some(_), Some(_), None) => ArticleBody {
                    uri: body_uri.clone(),
                    state: FetchState::Pending,
                    verification: None,
                    text: None,
                    format: "markdown",
                    sha256: None,
                    bytes: None,
                    error: None,
                },
                (Some(_), Some(_), Some((state, verification, data, sha256, bytes, error))) => {
                    ArticleBody {
                        uri: body_uri.clone(),
                        state: FetchState::parse(&state),
                        verification,
                        text: data,
                        format: "markdown",
                        sha256,
                        bytes: bytes.map(|b| b.max(0) as u64),
                        error,
                    }
                }
            };
            let media = match &body_key {
                Some(key) => {
                    let mut stmt = conn
                        .prepare(
                            "SELECT kind, src, ipfs FROM object_media WHERE uri = ?1 \
                             ORDER BY position",
                        )
                        .db()?;
                    stmt.query_map([key], |r| {
                        Ok(MediaRef {
                            kind: r.get(0)?,
                            src: r.get(1)?,
                            ipfs: r.get(2)?,
                        })
                    })
                    .db()?
                    .collect::<rusqlite::Result<Vec<_>>>()
                    .db()?
                }
                None => Vec::new(),
            };
            let holders = holders_of(conn, id)?;
            let listed: i64 = conn
                .query_row(
                    "SELECT COALESCE(SUM(amount), 0) FROM listings WHERE token_id = ?1",
                    [id],
                    |r| r.get(0),
                )
                .db()?;
            Ok(Article {
                summary,
                splits,
                royalties: royalties.map(|r| r.max(0) as u64),
                metadata_locked,
                revisions: revisions.max(0) as u64,
                metadata_uri,
                metadata_source,
                display_uri,
                thumbnail_caption,
                flag,
                body,
                media,
                holders: holders.len() as u64,
                listed: listed.max(0) as u64,
            })
        })
    }

    /// Current holders of a visible article, largest first. Editions in marketplace escrow are
    /// credited to the sellers of the active listings; any escrow balance no listing explains
    /// stays with the escrow contract (`escrow: true`).
    pub fn article_holders(&self, token_id: u64) -> Result<Vec<ArticleHolder>> {
        let id = sql_id(token_id)?;
        self.read(|conn| {
            let visible: Option<i64> = conn
                .query_row(
                    "SELECT token_id FROM articles WHERE token_id = ?1 AND hidden = 0",
                    [id],
                    |r| r.get(0),
                )
                .optional()
                .db()?;
            if visible.is_none() {
                return Err(ArticleStoreError::NotFound {
                    what: "article",
                    key: token_id.to_string(),
                });
            }
            holders_of(conn, id)
        })
    }

    pub fn stats(&self) -> Result<ArticleStats> {
        self.read(|conn| {
            let count = |sql: &str| -> Result<u64> {
                conn.query_row(sql, [], |r| r.get::<_, Option<i64>>(0))
                    .db()
                    .map(|v| v.unwrap_or(0).max(0) as u64)
            };
            let mut s = ArticleStats {
                articles: count("SELECT count(*) FROM articles")?,
                visible: count("SELECT count(*) FROM articles WHERE hidden = 0")?,
                hidden: count("SELECT count(*) FROM articles WHERE hidden = 1")?,
                metadata_ipfs: count(
                    "SELECT count(*) FROM articles WHERE metadata_source = 'ipfs'",
                )?,
                metadata_tzkt: count(
                    "SELECT count(*) FROM articles WHERE metadata_source = 'tzkt'",
                )?,
                metadata_none: count(
                    "SELECT count(*) FROM articles WHERE metadata_source IS NULL",
                )?,
                articles_body_ok: count(
                    "SELECT count(*) FROM articles a JOIN objects o ON o.uri = a.body_key \
                     WHERE o.state = 'ok'",
                )?,
                articles_body_pending: count(
                    "SELECT count(*) FROM articles a LEFT JOIN objects o ON o.uri = a.body_key \
                     WHERE a.body_key IS NOT NULL AND o.uri IS NULL",
                )?,
                articles_body_transient: count(
                    "SELECT count(*) FROM articles a JOIN objects o ON o.uri = a.body_key \
                     WHERE o.state = 'transient'",
                )?,
                articles_body_absent: count(
                    "SELECT count(*) FROM articles a LEFT JOIN objects o ON o.uri = a.body_key \
                     WHERE a.body_key IS NULL OR o.state = 'absent'",
                )?,
                body_bytes: count(
                    "SELECT SUM(bytes) FROM objects WHERE kind = 'body' AND state = 'ok'",
                )?,
                body_max_bytes: count(
                    "SELECT MAX(bytes) FROM objects WHERE kind = 'body' AND state = 'ok'",
                )?,
                media: count("SELECT count(*) FROM object_media")?,
                media_ipfs: count("SELECT count(*) FROM object_media WHERE ipfs IS NOT NULL")?,
                holder_rows: count("SELECT count(*) FROM holders")?,
                distinct_holders: count("SELECT count(DISTINCT address) FROM holders")?,
                listings: count("SELECT count(*) FROM listings")?,
                listed_editions: count("SELECT SUM(amount) FROM listings")?,
                authors: count("SELECT count(DISTINCT author) FROM articles")?,
                named_authors: count(
                    "SELECT count(DISTINCT a.author) FROM articles a \
                     JOIN names n ON n.address = a.author",
                )?,
                ..ArticleStats::default()
            };
            let mut stmt = conn
                .prepare(
                    "SELECT kind, state, verification, count(*), COALESCE(SUM(bytes), 0) \
                     FROM objects GROUP BY kind, state, verification ORDER BY 1, 2, 3",
                )
                .db()?;
            s.objects = stmt
                .query_map([], |r| {
                    Ok((
                        r.get(0)?,
                        r.get(1)?,
                        r.get(2)?,
                        get_u64(r, 3)?,
                        get_u64(r, 4)?,
                    ))
                })
                .db()?
                .collect::<rusqlite::Result<Vec<_>>>()
                .db()?;
            let (level, at): (Option<i64>, Option<i64>) = conn
                .query_row(
                    "SELECT chain_level, chain_synced_at FROM sync_state WHERE id = 1",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .db()?;
            s.chain_level = level.map(|l| l.max(0) as u64);
            s.chain_synced_at = at;
            s.refs = count("SELECT count(*) FROM article_refs")?;
            s.refs_unresolved =
                count("SELECT count(*) FROM article_refs WHERE project_id IS NULL")?;
            s.ref_articles = count("SELECT count(DISTINCT article_id) FROM article_refs")?;
            s.ref_articles_visible = count(
                "SELECT count(DISTINCT r.article_id) FROM article_refs r \
                 JOIN articles a ON a.token_id = r.article_id AND a.hidden = 0 \
                 JOIN ref_projects p ON p.project_id = r.project_id AND p.hidden = 0",
            )?;
            s.ref_projects = count(
                "SELECT count(DISTINCT project_id) FROM article_refs \
                 WHERE project_id IS NOT NULL",
            )?;
            s.ref_projects_visible = count(
                "SELECT count(*) FROM ref_projects p WHERE p.hidden = 0 AND EXISTS \
                 (SELECT 1 FROM article_refs r WHERE r.project_id = p.project_id)",
            )?;
            s.ref_projects_hidden = count(
                "SELECT count(*) FROM ref_projects p WHERE p.hidden = 1 AND EXISTS \
                 (SELECT 1 FROM article_refs r WHERE r.project_id = p.project_id)",
            )?;
            s.ref_tokens = count(
                "SELECT count(*) FROM (SELECT DISTINCT contract, token_id FROM article_refs \
                 WHERE token_id IS NOT NULL)",
            )?;
            let mut stmt = conn
                .prepare(
                    "SELECT kind, form, count(*), count(DISTINCT article_id) FROM article_refs \
                     GROUP BY kind, form ORDER BY 1, 2",
                )
                .db()?;
            s.refs_by_form = stmt
                .query_map([], |r| {
                    Ok((r.get(0)?, r.get(1)?, get_u64(r, 2)?, get_u64(r, 3)?))
                })
                .db()?
                .collect::<rusqlite::Result<Vec<_>>>()
                .db()?;
            Ok(s)
        })
    }
}

/// Article columns beyond the summary.
struct Detail {
    royalties: Option<i64>,
    metadata_locked: bool,
    revisions: i64,
    metadata_uri: Option<String>,
    metadata_source: Option<String>,
    display_uri: Option<String>,
    thumbnail_caption: Option<String>,
    flag: String,
    body_uri: Option<String>,
    body_key: Option<String>,
}

fn holders_of(conn: &Connection, id: i64) -> Result<Vec<ArticleHolder>> {
    let mut stmt = conn
        .prepare(
            "WITH listed AS (
               SELECT seller AS address, SUM(amount) AS amount FROM listings
               WHERE token_id = ?1 GROUP BY seller
             ),
             escrow AS (
               SELECT h.amount - COALESCE((SELECT SUM(amount) FROM listed), 0) AS rest
               FROM holders h WHERE h.token_id = ?1 AND h.address = ?2
             ),
             merged AS (
               SELECT address, amount AS held, 0 AS listed, 0 AS escrow FROM holders
               WHERE token_id = ?1 AND address <> ?2
               UNION ALL
               SELECT address, 0, amount, 0 FROM listed
               UNION ALL
               SELECT ?2, rest, 0, 1 FROM escrow WHERE rest > 0
             )
             SELECT m.address, n.name, SUM(m.held) + SUM(m.listed), SUM(m.listed), MAX(m.escrow)
             FROM merged m LEFT JOIN names n ON n.address = m.address
             GROUP BY m.address
             ORDER BY SUM(m.held) + SUM(m.listed) DESC, m.address",
        )
        .db()?;
    stmt.query_map(params![id, MARKETPLACE_V3], |r| {
        Ok(ArticleHolder {
            address: r.get(0)?,
            name: r.get(1)?,
            amount: get_u64(r, 2)?,
            listed: get_u64(r, 3)?,
            escrow: r.get::<_, i64>(4)? == 1,
        })
    })
    .db()?
    .collect::<rusqlite::Result<Vec<_>>>()
    .db()
}

fn record_object(
    tx: &Connection,
    item: &PendingObject,
    outcome: &Outcome,
) -> Result<Option<FetchState>> {
    let existing: Option<(String, Option<String>, i64)> = tx
        .query_row(
            "SELECT state, verification, not_found_runs FROM objects WHERE uri = ?1",
            [&item.key],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()
        .db()?;
    if *outcome == Outcome::Interrupted {
        return Ok(None);
    }
    let stored_ok = existing.as_ref().is_some_and(|(s, _, _)| s == "ok");
    let stored_unverified = existing
        .as_ref()
        .is_some_and(|(_, v, _)| v.as_deref() == Some(Verification::Unverified.as_str()));
    let runs = existing.as_ref().map_or(0, |(_, _, n)| *n);
    let (state, verification, data, error, runs, gateway) = match outcome {
        Outcome::Ok {
            data,
            verification,
            gateway,
        } => match String::from_utf8(data.clone()) {
            Ok(text) => (
                FetchState::Ok,
                Some(*verification),
                Some(text),
                None,
                runs,
                Some(gateway.clone()),
            ),
            Err(_) if verification.is_verified() => (
                FetchState::Absent,
                None,
                None,
                Some("verified bytes are not UTF-8 text".to_owned()),
                runs,
                Some(gateway.clone()),
            ),
            // No verified copy anywhere either: counted like a not-found run.
            Err(_) => {
                let runs = runs + 1;
                (
                    if runs >= ABSENT_AFTER_RUNS {
                        FetchState::Absent
                    } else {
                        FetchState::Transient
                    },
                    None,
                    None,
                    Some(format!("unverified bytes are not UTF-8 text ({runs} runs)")),
                    runs,
                    Some(gateway.clone()),
                )
            }
        },
        Outcome::NotFound(e) => {
            let runs = runs + 1;
            let state = if runs >= ABSENT_AFTER_RUNS {
                FetchState::Absent
            } else {
                FetchState::Transient
            };
            (
                state,
                None,
                None,
                Some(format!("not found on every gateway ({runs} runs): {e}")),
                runs,
                None,
            )
        }
        Outcome::Absent(e) => (FetchState::Absent, None, None, Some(e.clone()), runs, None),
        Outcome::Transient(e) => (
            FetchState::Transient,
            None,
            None,
            Some(e.clone()),
            runs,
            None,
        ),
        Outcome::Interrupted => return Ok(None),
    };
    let verified_ok =
        state == FetchState::Ok && verification.is_some_and(Verification::is_verified);
    if stored_ok && !(stored_unverified && verified_ok) {
        return Ok(None);
    }
    let (sha, bytes) = match &data {
        Some(d) => (
            Some(hex::encode(Sha256::digest(d.as_bytes()))),
            Some(d.len() as i64),
        ),
        None => (None, None),
    };
    let format = data.as_ref().map(|_| match item.kind {
        ObjectKind::Metadata => "json",
        ObjectKind::Body => "markdown",
    });
    tx.execute(
        "INSERT INTO objects (uri, kind, state, verification, format, data, sha256, bytes, \
         gateway, attempts, not_found_runs, error, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 1, ?10, ?11, ?12)
         ON CONFLICT (uri) DO UPDATE SET state = excluded.state,
           verification = excluded.verification, format = excluded.format, data = excluded.data,
           sha256 = excluded.sha256, bytes = excluded.bytes, gateway = excluded.gateway,
           attempts = objects.attempts + 1, not_found_runs = excluded.not_found_runs,
           error = excluded.error, updated_at = excluded.updated_at
         WHERE objects.state <> 'ok' OR objects.verification = 'unverified'",
        params![
            item.key,
            item.kind.as_str(),
            state.as_str(),
            verification.map(Verification::as_str),
            format,
            data,
            sha,
            bytes,
            gateway.filter(|g| !g.is_empty()),
            runs,
            error,
            unix_now(),
        ],
    )
    .db()?;
    tx.execute("DELETE FROM object_media WHERE uri = ?1", [&item.key])
        .db()?;
    if let (ObjectKind::Body, Some(text)) = (item.kind, &data) {
        let mut ins = tx
            .prepare(
                "INSERT INTO object_media (uri, position, kind, src, ipfs) \
                 VALUES (?1, ?2, ?3, ?4, ?5)",
            )
            .db()?;
        for (i, m) in parse::media(text).iter().enumerate() {
            ins.execute(params![item.key, i as i64, m.kind, m.src, m.ipfs])
                .db()?;
        }
    }
    Ok(Some(state))
}

/// Recomputes metadata-derived columns, moderation flags and the search index for every
/// article. Our own fetched metadata wins; TzKT's copy fills in until then.
fn derive(tx: &Connection) -> Result<()> {
    let rows: Vec<(i64, Option<String>, Option<String>)> = {
        let mut stmt = tx
            .prepare(
                "SELECT a.token_id, o.data, a.tzkt_metadata FROM articles a \
                 LEFT JOIN objects o ON o.uri = a.metadata_key AND o.state = 'ok'",
            )
            .db()?;
        stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
            .db()?
            .collect::<rusqlite::Result<Vec<_>>>()
            .db()?
    };
    let mut update = tx
        .prepare(
            "UPDATE articles SET metadata_source = ?2, title = ?3, description = ?4, \
             language = ?5, tags = ?6, thumbnail_uri = ?7, display_uri = ?8, \
             thumbnail_caption = ?9, declared_minter = ?10, body_uri = ?11, body_key = ?12 \
             WHERE token_id = ?1",
        )
        .db()?;
    let parsed = |json: &Option<String>| -> Option<ArticleMeta> {
        json.as_deref()
            .and_then(|j| serde_json::from_str::<Value>(j).ok())
            .and_then(|v| parse::metadata(&v))
    };
    for (id, ours, tzkt) in &rows {
        let (source, meta) = match (parsed(ours), parsed(tzkt)) {
            (Some(m), _) => (Some("ipfs"), m),
            (None, Some(m)) => (Some("tzkt"), m),
            (None, None) => (None, ArticleMeta::default()),
        };
        let body_key = meta
            .artifact_uri
            .as_deref()
            .and_then(|u| ipfs_ref(u).ok())
            .map(|r| r.key);
        update
            .execute(params![
                id,
                source,
                meta.name,
                meta.description,
                meta.language,
                serde_json::to_string(&meta.tags).unwrap_or_else(|_| "[]".into()),
                meta.thumbnail_uri,
                meta.display_uri,
                meta.thumbnail_caption,
                meta.minter,
                meta.artifact_uri,
                body_key,
            ])
            .db()?;
    }
    tx.execute_batch(
        "UPDATE articles SET
           flag = COALESCE((SELECT CASE m.state WHEN 0 THEN 'NONE' WHEN 1 THEN 'CLEAN'
               WHEN 2 THEN 'REPORTED' WHEN 3 THEN 'AUTO_DETECT_COPY' WHEN 4 THEN 'MALICIOUS'
               WHEN 5 THEN 'HIDDEN' ELSE 'STATE_' || m.state END
             FROM moderation m WHERE m.token_id = articles.token_id), 'NONE'),
           moderation_reason = (SELECT r.text FROM moderation m
             JOIN moderation_reasons r ON r.id = m.reason_id
             WHERE m.token_id = articles.token_id),
           hidden = COALESCE((SELECT m.state NOT IN (0, 1) FROM moderation m
             WHERE m.token_id = articles.token_id), 0);",
    )
    .db()?;
    tx.execute_batch(INDEX_ARTICLES).db()?;
    Ok(())
}

/// Which stored body version references were extracted from (`articles.refs_key`): the body
/// key and its sha256, so a verified copy replacing an unverified one counts as a change.
const BODY_VERSION: &str = "o.uri || '#' || COALESCE(o.sha256, '')";

/// Articles whose current body is stored but whose references were extracted from another
/// body version (or by older rules): `(token_id, body_key, body version)`.
fn stale_refs(tx: &Connection) -> Result<Vec<(i64, String, String)>> {
    let mut stmt = tx
        .prepare(&format!(
            "SELECT a.token_id, a.body_key, {BODY_VERSION} FROM articles a \
             JOIN objects o ON o.uri = a.body_key AND o.state = 'ok' \
             WHERE a.refs_key IS NOT ({BODY_VERSION}) OR a.refs_rules IS NOT ?1 \
             ORDER BY a.body_key"
        ))
        .db()?;
    stmt.query_map([refs::RULES_VERSION], |r| {
        Ok((r.get(0)?, r.get(1)?, r.get(2)?))
    })
    .db()?
    .collect::<rusqlite::Result<_>>()
    .db()
}

/// Keeps `article_refs` equal to what each article's current body references (generator
/// rows excepted: [`add_generators`] adds those). Articles without a stored body have none.
/// Resolved projects of re-extracted rows are cleared; the next resolution restores them.
/// Returns how many articles were (re)extracted.
fn extract_refs(tx: &Connection) -> Result<u64> {
    tx.execute_batch(
        "DELETE FROM article_refs WHERE article_id IN (
           SELECT a.token_id FROM articles a
           LEFT JOIN objects o ON o.uri = a.body_key AND o.state = 'ok'
           WHERE o.uri IS NULL);
         UPDATE articles SET refs_key = NULL, refs_rules = NULL
         WHERE refs_key IS NOT NULL AND NOT EXISTS (
           SELECT 1 FROM objects o WHERE o.uri = articles.body_key AND o.state = 'ok');",
    )
    .db()?;
    let stale = stale_refs(tx)?;
    if stale.is_empty() {
        return Ok(0);
    }
    let mut body = tx
        .prepare_cached("SELECT data FROM objects WHERE uri = ?1 AND state = 'ok'")
        .db()?;
    let mut clear = tx
        .prepare_cached("DELETE FROM article_refs WHERE article_id = ?1")
        .db()?;
    let mut ins = tx
        .prepare_cached(
            "INSERT INTO article_refs (article_id, target, position, kind, form, project_id, \
             contract, token_id, source_text) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        )
        .db()?;
    let mut mark = tx
        .prepare_cached("UPDATE articles SET refs_key = ?2, refs_rules = ?3 WHERE token_id = ?1")
        .db()?;
    // Sorted by body, so a shared body is parsed once.
    let mut current: Option<(&str, Vec<refs::Reference>)> = None;
    for (id, key, version) in &stale {
        if current.as_ref().is_none_or(|(k, _)| k != key) {
            let text: Option<String> = body.query_row([key], |r| r.get(0)).optional().db()?;
            current = Some((
                key.as_str(),
                text.as_deref().map(refs::references).unwrap_or_default(),
            ));
        }
        let Some((_, found)) = &current else {
            continue;
        };
        clear.execute([id]).db()?;
        for (position, r) in found.iter().enumerate() {
            // Ids that do not fit a column are no reference (`refs` never yields them).
            let (project, contract, token) = match &r.target {
                Target::Root { .. } => continue,
                Target::Project(p) => match i64::try_from(*p) {
                    Ok(p) => (Some(p), None, None),
                    Err(_) => continue,
                },
                Target::Token { contract, token_id } => match i64::try_from(*token_id) {
                    Ok(t) => (None, Some(*contract), Some(t)),
                    Err(_) => continue,
                },
                Target::ProjectSlug(_) | Target::TokenSlug(_) => (None, None, None),
            };
            ins.execute(params![
                id,
                r.target.key(),
                position as i64,
                r.target.kind(),
                r.form,
                project,
                contract,
                token,
                r.source,
            ])
            .db()?;
        }
        mark.execute(params![id, version, refs::RULES_VERSION])
            .db()?;
    }
    Ok(stale.len() as u64)
}

/// `(position, reference, project)` of a generator root the catalogue knows.
type Generator = (usize, refs::Reference, u64);

/// Adds a project reference for every generator root in an extracted body that the
/// catalogue knows. Returns rows added.
fn add_generators(tx: &Connection, catalogue: &Catalogue<'_>) -> Result<u64> {
    let articles: Vec<(i64, String)> = {
        let mut stmt = tx
            .prepare(&format!(
                "SELECT a.token_id, a.body_key FROM articles a \
                 JOIN objects o ON o.uri = a.body_key AND o.state = 'ok' \
                 WHERE a.refs_key = ({BODY_VERSION}) AND a.refs_rules = ?1 \
                 ORDER BY a.body_key"
            ))
            .db()?;
        stmt.query_map([refs::RULES_VERSION], |r| Ok((r.get(0)?, r.get(1)?)))
            .db()?
            .collect::<rusqlite::Result<_>>()
            .db()?
    };
    let mut body = tx
        .prepare_cached("SELECT data FROM objects WHERE uri = ?1 AND state = 'ok'")
        .db()?;
    let mut ins = tx
        .prepare_cached(
            "INSERT OR IGNORE INTO article_refs (article_id, target, position, kind, form, \
             project_id, source_text) VALUES (?1, ?2, ?3, 'project', ?4, ?5, ?6)",
        )
        .db()?;
    let mut added = 0u64;
    // Sorted by body, so a shared body is parsed once.
    let mut current: Option<(String, Vec<Generator>)> = None;
    for (id, key) in &articles {
        if current.as_ref().is_none_or(|(k, _)| k != key) {
            let text: Option<String> = body.query_row([key], |r| r.get(0)).optional().db()?;
            let found = text
                .as_deref()
                .map(refs::references)
                .unwrap_or_default()
                .into_iter()
                .enumerate()
                .filter_map(|(i, r)| match &r.target {
                    Target::Root { scheme, cid } => {
                        catalogue.generator(scheme, cid).map(|p| (i, r, p))
                    }
                    _ => None,
                })
                .collect();
            current = Some((key.clone(), found));
        }
        let Some((_, found)) = &current else {
            continue;
        };
        for (position, r, project) in found {
            added += ins
                .execute(params![
                    id,
                    r.target.key(),
                    *position as i64,
                    r.form,
                    *project as i64,
                    r.source,
                ])
                .db()? as u64;
        }
    }
    Ok(added)
}
