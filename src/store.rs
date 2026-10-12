//! SQLite read store. All reads of one call share a snapshot (one read transaction).
//!
//! Projects flagged by fxhash (`hidden = 1`) and their tokens are excluded from every public
//! read; they appear only in [`Store::index_status`] counts.

use std::path::Path;
use std::time::Duration;

use rusqlite::{Connection, OpenFlags, OptionalExtension, Row, ToSql, TransactionBehavior};
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

pub mod market;
mod meta;
mod names;
pub mod profile;
pub(crate) mod search;
pub mod volume;

pub use names::NamesNumbered;
pub(crate) use names::assign as assign_names;
pub use profile::name_key;

use crate::model::{
    Artist, ArtistRef, ArtistSummary, Collector, ContractStatus, FeatureShare, IndexState,
    IndexStatus, Owner, OwnerSnapshot, Project, ProjectSummary, Snapshot, Source, Token,
    Verification,
};

pub const MAX_LIMIT: u32 = 100;
pub const MAX_OFFSET: u32 = 10_000;
pub const MAX_QUERY_CHARS: usize = 200;
/// Most search terms per query; more is BadRequest.
pub const MAX_QUERY_TERMS: usize = 8;
pub(crate) const SCHEMA_VERSION: i64 = 10;
const BUSY_TIMEOUT: Duration = Duration::from_secs(2);
/// Covers every distinct read statement (sort x filter variants included).
const STATEMENT_CACHE: usize = 192;

/// fxhash token contracts on Tezos: GENTK v1, GENTK v2, fx(params).
pub const TOKEN_CONTRACTS: [&str; 3] = [
    "KT1KEa8z6vWXDJrVqtMrAeDVzsvxat3kHaCE",
    "KT1U6EHmNxJTkvaWJ4ThczG4FSDaHC21ssvi",
    "KT1EfsNuqwLAWDd3o4pvfUx1CAh5GMdTrRvr",
];

const SCHEMA_V1: &str = "
CREATE TABLE snapshot (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  as_of TEXT NOT NULL,
  owner_source TEXT NOT NULL,
  catalogue_source TEXT NOT NULL,
  fixture_sha256 TEXT NOT NULL,
  imported_at TEXT NOT NULL
) STRICT;
CREATE TABLE artists (
  seq INTEGER PRIMARY KEY,
  id TEXT NOT NULL UNIQUE,
  name TEXT NOT NULL,
  name_key TEXT NOT NULL,
  website TEXT
) STRICT;
CREATE INDEX artists_name_key ON artists (name_key, id);
CREATE TABLE artist_aliases (
  artist_id TEXT NOT NULL REFERENCES artists (id) ON DELETE CASCADE,
  position INTEGER NOT NULL,
  alias TEXT NOT NULL,
  alias_key TEXT NOT NULL,
  PRIMARY KEY (artist_id, position),
  UNIQUE (artist_id, alias_key)
) STRICT, WITHOUT ROWID;
CREATE INDEX artist_aliases_key ON artist_aliases (alias_key);
CREATE TABLE projects (
  id INTEGER PRIMARY KEY CHECK (id >= 0),
  title TEXT NOT NULL,
  title_key TEXT NOT NULL,
  artist_key TEXT NOT NULL,
  description TEXT,
  preview TEXT,
  contract TEXT,
  issuer TEXT,
  artifact_uri TEXT,
  editions INTEGER CHECK (editions IS NULL OR editions BETWEEN 1 AND 4294967295),
  first_mint TEXT,
  last_mint TEXT,
  index_state TEXT NOT NULL CHECK (index_state IN ('indexed', 'not_indexed'))
) STRICT;
CREATE INDEX projects_title_key ON projects (title_key, id);
CREATE INDEX projects_artist_key ON projects (artist_key, id);
CREATE TABLE project_artists (
  project_id INTEGER NOT NULL REFERENCES projects (id) ON DELETE CASCADE,
  position INTEGER NOT NULL,
  artist_id TEXT NOT NULL REFERENCES artists (id),
  PRIMARY KEY (project_id, position),
  UNIQUE (project_id, artist_id)
) STRICT, WITHOUT ROWID;
CREATE INDEX project_artists_artist ON project_artists (artist_id, project_id);
CREATE TABLE tokens (
  contract TEXT NOT NULL,
  token_id INTEGER NOT NULL CHECK (token_id >= 0),
  project_id INTEGER NOT NULL REFERENCES projects (id) ON DELETE CASCADE,
  iteration INTEGER NOT NULL CHECK (iteration BETWEEN 1 AND 4294967295),
  seed TEXT NOT NULL,
  minter TEXT NOT NULL,
  minted TEXT NOT NULL,
  artifact_uri TEXT NOT NULL,
  preview TEXT,
  owner_address TEXT,
  owner_alias TEXT,
  owner_alias_key TEXT,
  owner_as_of TEXT,
  owner_source TEXT,
  PRIMARY KEY (contract, token_id),
  UNIQUE (project_id, iteration),
  CHECK ((owner_address IS NULL) = (owner_as_of IS NULL)),
  CHECK ((owner_address IS NULL) = (owner_source IS NULL)),
  CHECK (owner_address IS NOT NULL OR owner_alias IS NULL)
) STRICT, WITHOUT ROWID;
CREATE INDEX tokens_owner_alias_key ON tokens (owner_alias_key) WHERE owner_alias_key IS NOT NULL;
CREATE TABLE token_features (
  contract TEXT NOT NULL,
  token_id INTEGER NOT NULL,
  position INTEGER NOT NULL,
  name TEXT NOT NULL,
  value TEXT NOT NULL,
  PRIMARY KEY (contract, token_id, position),
  UNIQUE (contract, token_id, name),
  FOREIGN KEY (contract, token_id) REFERENCES tokens (contract, token_id) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;
CREATE VIRTUAL TABLE project_fts USING fts5 (title, artists, tokenize = 'unicode61 remove_diacritics 2');
CREATE VIRTUAL TABLE artist_fts USING fts5 (name, aliases, tokenize = 'unicode61 remove_diacritics 2');
";

/// v1 -> v2, additive only: catalogue and chain columns, collectors, ingest bookkeeping.
const SCHEMA_V2: &str = "
ALTER TABLE projects ADD COLUMN slug TEXT;
ALTER TABLE projects ADD COLUMN supply INTEGER
  CHECK (supply IS NULL OR supply BETWEEN 0 AND 4294967295);
ALTER TABLE projects ADD COLUMN flag TEXT;
ALTER TABLE projects ADD COLUMN hidden INTEGER NOT NULL DEFAULT 0 CHECK (hidden IN (0, 1));
ALTER TABLE projects ADD COLUMN source TEXT NOT NULL DEFAULT 'fixture'
  CHECK (source IN ('fixture', 'frank'));
ALTER TABLE projects ADD COLUMN author_address TEXT;
ALTER TABLE projects ADD COLUMN thumbnail_uri TEXT;
ALTER TABLE projects ADD COLUMN display_uri TEXT;
ALTER TABLE projects ADD COLUMN generative_uri TEXT;
ALTER TABLE projects ADD COLUMN tags TEXT;
ALTER TABLE projects ADD COLUMN iteration_description TEXT;
ALTER TABLE projects ADD COLUMN created_at TEXT;
ALTER TABLE projects ADD COLUMN mint_opens_at TEXT;
ALTER TABLE projects ADD COLUMN catalogue_source TEXT;
CREATE INDEX projects_hidden ON projects (hidden) WHERE hidden = 1;
ALTER TABLE artists ADD COLUMN address TEXT;
ALTER TABLE artists ADD COLUMN description TEXT;
ALTER TABLE artists ADD COLUMN avatar_uri TEXT;
ALTER TABLE artists ADD COLUMN catalogue_source TEXT;
CREATE INDEX artists_address ON artists (address, seq) WHERE address IS NOT NULL;
ALTER TABLE tokens ADD COLUMN display_uri TEXT;
ALTER TABLE tokens ADD COLUMN thumbnail_uri TEXT;
ALTER TABLE tokens ADD COLUMN params TEXT;
ALTER TABLE tokens ADD COLUMN first_time TEXT;
ALTER TABLE tokens ADD COLUMN attributes TEXT;
ALTER TABLE tokens ADD COLUMN chain_level INTEGER;
ALTER TABLE tokens ADD COLUMN owner_level INTEGER;
ALTER TABLE tokens ADD COLUMN source TEXT NOT NULL DEFAULT 'fixture'
  CHECK (source IN ('fixture', 'tzkt'));
CREATE INDEX tokens_owner ON tokens (owner_address, project_id, iteration)
  WHERE owner_address IS NOT NULL;
CREATE INDEX tokens_fixture ON tokens (project_id) WHERE source = 'fixture';
CREATE TABLE users (
  address TEXT PRIMARY KEY,
  name TEXT NOT NULL
) STRICT, WITHOUT ROWID;
CREATE TABLE collaborations (
  contract TEXT NOT NULL,
  position INTEGER NOT NULL,
  member TEXT NOT NULL,
  share INTEGER,
  PRIMARY KEY (contract, position),
  UNIQUE (contract, member)
) STRICT, WITHOUT ROWID;
CREATE TABLE catalogue_tokens (
  contract TEXT NOT NULL,
  token_id INTEGER NOT NULL CHECK (token_id >= 0),
  project_id INTEGER NOT NULL,
  seed TEXT,
  artifact_uri TEXT,
  chain_state TEXT NOT NULL DEFAULT 'unknown'
    CHECK (chain_state IN ('unknown', 'present', 'absent')),
  verify_state TEXT NOT NULL DEFAULT 'unverified'
    CHECK (verify_state IN ('unverified', 'match', 'mismatch', 'unsigned', 'absent')),
  verify_detail TEXT,
  verified_level INTEGER,
  PRIMARY KEY (contract, token_id)
) STRICT, WITHOUT ROWID;
CREATE INDEX catalogue_tokens_open ON catalogue_tokens (verify_state, contract, token_id)
  WHERE verify_state <> 'match';
CREATE TABLE catalogue_snapshot (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  source TEXT NOT NULL,
  revision TEXT,
  generated_at TEXT,
  content_sha256 TEXT NOT NULL,
  imported_at TEXT NOT NULL
) STRICT;
CREATE TABLE sync_state (
  contract TEXT PRIMARY KEY,
  synced_level INTEGER,
  synced_at TEXT,
  full_level INTEGER,
  full_lo INTEGER,
  full_hi INTEGER,
  full_batch INTEGER,
  incr_level INTEGER,
  incr_mint_id INTEGER NOT NULL DEFAULT 0,
  incr_token_id INTEGER NOT NULL DEFAULT 0,
  incr_balance_id INTEGER NOT NULL DEFAULT 0,
  updated_at TEXT NOT NULL
) STRICT, WITHOUT ROWID;
CREATE TABLE sync_chunks (
  contract TEXT NOT NULL,
  lo INTEGER NOT NULL,
  hi INTEGER NOT NULL,
  tokens INTEGER NOT NULL,
  done_at TEXT NOT NULL,
  PRIMARY KEY (contract, lo)
) STRICT, WITHOUT ROWID;
CREATE TABLE ingest_issues (
  contract TEXT NOT NULL,
  token_id INTEGER NOT NULL,
  kind TEXT NOT NULL,
  detail TEXT,
  level INTEGER,
  seen_at TEXT NOT NULL,
  PRIMARY KEY (contract, token_id, kind)
) STRICT, WITHOUT ROWID;
";

/// v2 -> v3: marketplace escrow, burn address, collector counts, identities, content roots.
/// `holder_address` is who a collector page credits: the ledger owner, the seller of an
/// escrowed listing, or nobody (unresolved escrow, burn). Collector reads move from
/// `owner_address` to `holder_address`, so the v2 owner index is replaced. `holder_counts`
/// keeps per-collector totals of visible tokens, maintained by triggers.
const SCHEMA_V3: &str = "
ALTER TABLE tokens ADD COLUMN listed_by TEXT;
ALTER TABLE tokens ADD COLUMN holder_address TEXT;
UPDATE tokens SET holder_address = owner_address;
DROP INDEX tokens_owner;
CREATE INDEX tokens_holder ON tokens (holder_address, project_id, iteration)
  WHERE holder_address IS NOT NULL;
CREATE TABLE known_addresses (
  address TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  kind TEXT NOT NULL CHECK (kind IN ('escrow', 'burn'))
) STRICT, WITHOUT ROWID;
CREATE TABLE holder_counts (
  holder_address TEXT PRIMARY KEY,
  held INTEGER NOT NULL,
  listed INTEGER NOT NULL
) STRICT, WITHOUT ROWID;
INSERT INTO holder_counts (holder_address, held, listed)
  SELECT t.holder_address, count(*), sum(t.listed_by IS NOT NULL) FROM tokens t
  JOIN projects p ON p.id = t.project_id AND p.hidden = 0
  WHERE t.holder_address IS NOT NULL GROUP BY t.holder_address;
CREATE TRIGGER tokens_holder_insert AFTER INSERT ON tokens
WHEN NEW.holder_address IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM projects WHERE id = NEW.project_id AND hidden = 1)
BEGIN
  INSERT INTO holder_counts (holder_address, held, listed)
    VALUES (NEW.holder_address, 1, NEW.listed_by IS NOT NULL)
    ON CONFLICT (holder_address) DO UPDATE SET held = held + 1, listed = listed + excluded.listed;
END;
CREATE TRIGGER tokens_holder_delete AFTER DELETE ON tokens
WHEN OLD.holder_address IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM projects WHERE id = OLD.project_id AND hidden = 1)
BEGIN
  UPDATE holder_counts SET held = held - 1, listed = listed - (OLD.listed_by IS NOT NULL)
    WHERE holder_address = OLD.holder_address;
END;
CREATE TRIGGER tokens_holder_update AFTER UPDATE OF holder_address, listed_by, project_id ON tokens
WHEN OLD.holder_address IS NOT NEW.holder_address OR OLD.listed_by IS NOT NEW.listed_by
  OR OLD.project_id IS NOT NEW.project_id
BEGIN
  UPDATE holder_counts SET held = held - 1, listed = listed - (OLD.listed_by IS NOT NULL)
    WHERE holder_address = OLD.holder_address
    AND NOT EXISTS (SELECT 1 FROM projects WHERE id = OLD.project_id AND hidden = 1);
  INSERT INTO holder_counts (holder_address, held, listed)
    SELECT NEW.holder_address, 1, NEW.listed_by IS NOT NULL
    WHERE NEW.holder_address IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM projects WHERE id = NEW.project_id AND hidden = 1)
    ON CONFLICT (holder_address) DO UPDATE SET held = held + 1, listed = listed + excluded.listed;
END;
CREATE TRIGGER projects_hidden_counts AFTER UPDATE OF hidden ON projects
WHEN OLD.hidden IS NOT NEW.hidden
BEGIN
  INSERT INTO holder_counts (holder_address, held, listed)
    SELECT holder_address,
      count(*) * (CASE WHEN NEW.hidden = 1 THEN -1 ELSE 1 END),
      sum(listed_by IS NOT NULL) * (CASE WHEN NEW.hidden = 1 THEN -1 ELSE 1 END)
    FROM tokens WHERE project_id = NEW.id AND holder_address IS NOT NULL
    GROUP BY holder_address
    ON CONFLICT (holder_address) DO UPDATE SET held = held + excluded.held,
      listed = listed + excluded.listed;
END;
CREATE TABLE escrow_listings (
  escrow TEXT NOT NULL,
  listing TEXT NOT NULL,
  contract TEXT NOT NULL,
  token_id INTEGER NOT NULL,
  seller TEXT NOT NULL,
  level INTEGER NOT NULL,
  PRIMARY KEY (escrow, listing)
) STRICT, WITHOUT ROWID;
CREATE INDEX escrow_listings_token ON escrow_listings (contract, token_id, escrow, seller);
ALTER TABLE sync_state ADD COLUMN incr_phase INTEGER NOT NULL DEFAULT 0;
CREATE TABLE identities (
  address TEXT PRIMARY KEY,
  slug TEXT UNIQUE,
  name TEXT NOT NULL,
  website TEXT,
  aliases TEXT NOT NULL,
  provenance TEXT NOT NULL
) STRICT, WITHOUT ROWID;
CREATE TABLE content_roots (
  scheme TEXT NOT NULL CHECK (scheme IN ('ipfs', 'onchfs')),
  cid TEXT NOT NULL,
  project_id INTEGER NOT NULL,
  PRIMARY KEY (scheme, cid, project_id)
) STRICT, WITHOUT ROWID;
CREATE TABLE escrow_state (
  escrow TEXT PRIMARY KEY,
  synced_level INTEGER,
  synced_at TEXT,
  pass_level INTEGER,
  pass_full INTEGER,
  cursor_id INTEGER NOT NULL DEFAULT 0,
  updated_at TEXT NOT NULL
) STRICT, WITHOUT ROWID;
";

/// v3 -> v4, additive: the retry phase's token-id cursor, so each open issue gets one attempt
/// per pass however short the runs.
const SCHEMA_V4: &str = "
ALTER TABLE sync_state ADD COLUMN incr_retry_after INTEGER NOT NULL DEFAULT 0;
";

/// v4 -> v5, additive: per-project feature value counts, so `feature_shares` reads a few
/// hundred rows instead of every token's attributes. Rows are keyed by the feature's position
/// too, which keeps "first-seen feature order" exact. Triggers keep it equal to a recount:
/// chain attributes (`tokens.attributes`, JSON `[name, value]` pairs) on token insert, update
/// and delete; fixture rows (`token_features`) on their insert and delete. A token delete
/// subtracts its fixture rows before the cascade removes them, and the cascade's own trigger
/// then finds no parent and adds nothing. Counts ignore moderation; reads check visibility.
const SCHEMA_V5: &str = "
CREATE TABLE feature_counts (
  project_id INTEGER NOT NULL,
  name TEXT NOT NULL,
  value TEXT NOT NULL,
  position INTEGER NOT NULL,
  count INTEGER NOT NULL,
  PRIMARY KEY (project_id, name, value, position)
) STRICT, WITHOUT ROWID;
INSERT INTO feature_counts (project_id, name, value, position, count)
  SELECT project_id, name, value, position, count(*) FROM (
    SELECT t.project_id, j.value ->> 0 AS name, j.value ->> 1 AS value,
      CAST(j.key AS INTEGER) AS position
    FROM tokens t, json_each(t.attributes) j WHERE t.attributes IS NOT NULL
    UNION ALL
    SELECT t.project_id, tf.name, tf.value, tf.position FROM token_features tf
    JOIN tokens t ON t.contract = tf.contract AND t.token_id = tf.token_id
  ) GROUP BY project_id, name, value, position;
CREATE TRIGGER tokens_features_insert AFTER INSERT ON tokens
WHEN NEW.attributes IS NOT NULL
BEGIN
  INSERT INTO feature_counts (project_id, name, value, position, count)
    SELECT NEW.project_id, j.value ->> 0, j.value ->> 1, CAST(j.key AS INTEGER), 1
    FROM json_each(NEW.attributes) j WHERE true
    ON CONFLICT (project_id, name, value, position) DO UPDATE SET count = count + 1;
END;
CREATE TRIGGER tokens_features_update AFTER UPDATE OF attributes, project_id ON tokens
WHEN OLD.attributes IS NOT NEW.attributes OR OLD.project_id IS NOT NEW.project_id
BEGIN
  UPDATE feature_counts SET count = count - 1 WHERE project_id = OLD.project_id
    AND (name, value, position) IN (SELECT j.value ->> 0, j.value ->> 1, CAST(j.key AS INTEGER)
      FROM json_each(OLD.attributes) j);
  INSERT INTO feature_counts (project_id, name, value, position, count)
    SELECT NEW.project_id, j.value ->> 0, j.value ->> 1, CAST(j.key AS INTEGER), 1
    FROM json_each(NEW.attributes) j WHERE true
    ON CONFLICT (project_id, name, value, position) DO UPDATE SET count = count + 1;
  UPDATE feature_counts SET count = count - 1 WHERE OLD.project_id IS NOT NEW.project_id
    AND project_id = OLD.project_id AND (name, value, position) IN
      (SELECT name, value, position FROM token_features
       WHERE contract = OLD.contract AND token_id = OLD.token_id);
  INSERT INTO feature_counts (project_id, name, value, position, count)
    SELECT NEW.project_id, name, value, position, 1 FROM token_features
    WHERE OLD.project_id IS NOT NEW.project_id AND contract = NEW.contract
      AND token_id = NEW.token_id
    ON CONFLICT (project_id, name, value, position) DO UPDATE SET count = count + 1;
END;
CREATE TRIGGER tokens_features_delete BEFORE DELETE ON tokens
BEGIN
  UPDATE feature_counts SET count = count - 1 WHERE project_id = OLD.project_id
    AND (name, value, position) IN (SELECT j.value ->> 0, j.value ->> 1, CAST(j.key AS INTEGER)
      FROM json_each(OLD.attributes) j);
  UPDATE feature_counts SET count = count - 1 WHERE project_id = OLD.project_id
    AND (name, value, position) IN (SELECT name, value, position FROM token_features
      WHERE contract = OLD.contract AND token_id = OLD.token_id);
END;
CREATE TRIGGER token_features_insert AFTER INSERT ON token_features
BEGIN
  INSERT INTO feature_counts (project_id, name, value, position, count)
    SELECT t.project_id, NEW.name, NEW.value, NEW.position, 1 FROM tokens t
    WHERE t.contract = NEW.contract AND t.token_id = NEW.token_id
    ON CONFLICT (project_id, name, value, position) DO UPDATE SET count = count + 1;
END;
CREATE TRIGGER token_features_delete AFTER DELETE ON token_features
BEGIN
  UPDATE feature_counts SET count = count - 1
    WHERE project_id = (SELECT project_id FROM tokens
      WHERE contract = OLD.contract AND token_id = OLD.token_id)
    AND name = OLD.name AND value = OLD.value AND position = OLD.position;
END;
";

/// v5 -> v6, additive: profiles. `name_key` columns (trimmed, Unicode-lowercased name, see
/// [`name_key`]) index every name source so the duplicate-name rule is a few index probes;
/// they are backfilled in Rust because SQLite's `lower` is ASCII-only. `profile_edits` is
/// user data that cannot be rebuilt: nothing but [`Store::save_profile_edit`] writes it, and
/// no import or bootstrap deletes it. `profiles_imported` keeps each source's values with
/// provenance; `profile_fetch` and `profile_sync` are the resumable state of
/// `seeded-index profiles`. `feature_counts_trait` finds the projects carrying a trait.
const SCHEMA_V6: &str = "
ALTER TABLE users ADD COLUMN name_key TEXT;
CREATE INDEX users_name_key ON users (name_key) WHERE name_key IS NOT NULL;
ALTER TABLE identities ADD COLUMN name_key TEXT;
CREATE INDEX identities_name_key ON identities (name_key) WHERE name_key IS NOT NULL;
CREATE TABLE profiles_imported (
  address TEXT NOT NULL,
  source TEXT NOT NULL CHECK (source IN ('fxhash', 'objkt', 'tzprofiles')),
  name TEXT,
  name_key TEXT,
  bio TEXT,
  website TEXT,
  links TEXT NOT NULL DEFAULT '[]' CHECK (json_valid(links) AND json_type(links) = 'array'),
  avatar_uri TEXT,
  fetched_at TEXT NOT NULL,
  PRIMARY KEY (address, source),
  CHECK ((name IS NULL) = (name_key IS NULL))
) STRICT, WITHOUT ROWID;
CREATE INDEX profiles_imported_name_key ON profiles_imported (name_key)
  WHERE name_key IS NOT NULL;
CREATE TABLE profile_edits (
  address TEXT PRIMARY KEY,
  name TEXT,
  name_key TEXT UNIQUE,
  bio TEXT,
  links TEXT NOT NULL DEFAULT '[]' CHECK (json_valid(links) AND json_type(links) = 'array'),
  updated_at TEXT NOT NULL,
  signed_message TEXT NOT NULL,
  signature TEXT NOT NULL,
  public_key TEXT NOT NULL,
  CHECK ((name IS NULL) = (name_key IS NULL))
) STRICT, WITHOUT ROWID;
CREATE TABLE profile_fetch (
  source TEXT NOT NULL,
  address TEXT NOT NULL,
  uri TEXT NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('pending', 'done', 'invalid')),
  attempts INTEGER NOT NULL DEFAULT 0,
  detail TEXT,
  updated_at TEXT NOT NULL,
  PRIMARY KEY (source, address)
) STRICT, WITHOUT ROWID;
CREATE INDEX profile_fetch_pending ON profile_fetch (source, address) WHERE state = 'pending';
CREATE TABLE profile_sync (
  source TEXT PRIMARY KEY,
  cursor TEXT,
  synced_level INTEGER,
  pass_started_at TEXT,
  pass_done_at TEXT,
  updated_at TEXT NOT NULL
) STRICT, WITHOUT ROWID;
CREATE INDEX feature_counts_trait ON feature_counts (name, value, project_id) WHERE count > 0;
";

/// Feature shares of one project, read from `feature_counts` only (one primary-key range).
pub(crate) const FEATURE_SHARES: &str = "WITH f AS (SELECT name, value, position, count \
     FROM feature_counts WHERE project_id = ?1 AND count > 0), \
     n AS (SELECT name, min(position) AS pos FROM f GROUP BY name) \
     SELECT f.name, f.value, sum(f.count) AS c FROM f JOIN n ON n.name = f.name \
     GROUP BY n.pos, f.name, f.value ORDER BY n.pos, f.name, c DESC, f.value";

/// Projects whose title or shown artist names match `:fts` (see [`search`]).
pub(crate) const PROJECT_TEXT_FILTER: &str =
    " AND p.id IN (SELECT rowid FROM project_fts WHERE project_fts MATCH :fts)";

/// Artists whose shown name, address or curated aliases match `:fts` (see [`search`]).
pub(crate) const ARTIST_TEXT_FILTER: &str =
    " AND a.seq IN (SELECT rowid FROM artist_fts WHERE artist_fts MATCH :fts)";

/// Projects credited to `:artist`: driven by the `project_artists_artist` index, never a scan
/// of projects (checked by `artist_filter_is_driven_by_the_artist_index`).
pub(crate) const ARTIST_FILTER: &str =
    " AND p.id IN (SELECT project_id FROM project_artists WHERE artist_id = :artist)";

/// `index_status` counts. Every one starts from a small table or partial index (issues, open
/// catalogue rows, fixture rows, projects) and never scans `tokens`; `CROSS JOIN` pins that
/// order. Checked by `status_queries_never_scan_tokens`.
pub(crate) const STATUS_FIXTURE: &str = "SELECT count(*) FROM tokens t \
     INDEXED BY tokens_fixture CROSS JOIN projects p ON p.id = t.project_id AND p.hidden = 0 \
     WHERE t.source = 'fixture'";
pub(crate) const STATUS_MISMATCH: &str = "SELECT count(*) FROM catalogue_tokens ct \
     CROSS JOIN tokens t ON t.contract = ct.contract AND t.token_id = ct.token_id \
     CROSS JOIN projects p ON p.id = t.project_id AND p.hidden = 0 \
     WHERE ct.verify_state <> 'match' AND ct.verify_state = 'mismatch' AND t.source = 'tzkt'";
/// Awaiting re-comparison. Unsigned and absent rows have no token row, so only `unverified`
/// can count, and the partial index narrows to it.
pub(crate) const STATUS_PENDING: &str = "SELECT count(*) FROM catalogue_tokens ct \
     CROSS JOIN tokens t ON t.contract = ct.contract AND t.token_id = ct.token_id \
     CROSS JOIN projects p ON p.id = t.project_id AND p.hidden = 0 \
     WHERE ct.verify_state <> 'match' AND ct.verify_state = 'unverified' AND t.source = 'tzkt'";
pub(crate) const STATUS_UNCATALOGUED: &str = "SELECT count(*) FROM ingest_issues i \
     CROSS JOIN tokens t ON t.contract = i.contract AND t.token_id = i.token_id \
     CROSS JOIN projects p ON p.id = t.project_id AND p.hidden = 0 \
     WHERE i.kind = 'not_catalogued' AND t.source = 'tzkt'";
pub(crate) const STATUS_PER_CONTRACT: &str = "SELECT contract, sum(editions) FROM projects \
     WHERE hidden = 0 AND index_state = 'indexed' AND contract IS NOT NULL GROUP BY contract";

const SUMMARY_COLS: &str = "p.id, p.title, p.preview, p.editions, p.index_state, p.slug, \
     p.supply, p.thumbnail_uri, p.display_uri, p.created_at";
const TOKEN_COLS: &str = "t.contract, t.token_id, t.project_id, t.iteration, t.seed, t.minter, \
     t.minted, t.artifact_uri, t.preview, t.owner_address, t.owner_alias, t.owner_as_of, \
     t.owner_source, t.attributes, t.display_uri, t.thumbnail_uri, t.params, t.source, \
     ct.verify_state, t.listed_by, coalesce(il.name, ul.name), e.name, e.kind";
/// Visible tokens only, with catalogue verification, seller alias and escrow name.
const TOKEN_FROM: &str = "FROM tokens t JOIN projects p ON p.id = t.project_id AND p.hidden = 0 \
     LEFT JOIN catalogue_tokens ct ON ct.contract = t.contract AND ct.token_id = t.token_id \
     LEFT JOIN users ul ON ul.address = t.listed_by \
     LEFT JOIN identities il ON il.address = t.listed_by \
     LEFT JOIN known_addresses e ON e.address = t.owner_address";
const TOKEN_ORDER: &str = "t.project_id, t.iteration, t.contract, t.token_id";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageRequest {
    pub offset: u32,
    pub limit: u32,
}

impl PageRequest {
    fn validate(self) -> Result<(), StoreError> {
        if !(1..=MAX_LIMIT).contains(&self.limit) {
            return Err(StoreError::BadRequest(format!(
                "limit must be 1..={MAX_LIMIT}"
            )));
        }
        if self.offset > MAX_OFFSET {
            return Err(StoreError::BadRequest(format!(
                "offset must be at most {MAX_OFFSET}"
            )));
        }
        Ok(())
    }

    fn empty<T>(self) -> Page<T> {
        Page {
            items: Vec::new(),
            total: 0,
            offset: self.offset,
            limit: self.limit,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub total: u64,
    pub offset: u32,
    pub limit: u32,
}

/// Tiebreak is always id ascending.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectSort {
    TitleAsc,
    ArtistAsc,
    IdAsc,
    IdDesc,
    /// Highest trading volume first (see [`volume`]); projects without volume come last, by
    /// id.
    VolumeDesc,
}

impl ProjectSort {
    /// `ORDER BY` of one listing statement. [`ProjectSort::VolumeDesc`] lists in two parts
    /// (see [`volume`]); this is the order of its ranked part.
    fn order_by(self) -> &'static str {
        match self {
            ProjectSort::TitleAsc => "p.title_key, p.id",
            ProjectSort::ArtistAsc => "p.artist_key, p.id",
            ProjectSort::IdAsc => "p.id",
            ProjectSort::IdDesc => "p.id DESC",
            ProjectSort::VolumeDesc => volume::RANKED_ORDER,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectQuery {
    pub text: Option<String>,
    pub artist: Option<String>,
    pub sort: ProjectSort,
    pub page: PageRequest,
}

/// Sorted by name (case-insensitive), then id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtistQuery {
    pub text: Option<String>,
    pub page: PageRequest,
}

/// A project listing without its page: the keyset counterpart of [`ProjectQuery`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectFilter {
    pub text: Option<String>,
    pub artist: Option<String>,
    pub sort: ProjectSort,
}

/// Longest trait name or value in a [`ListingFilter`].
pub const MAX_TRAIT_CHARS: usize = 200;

/// Narrows a token listing; every part given must match. What text matches depends on the
/// listing ([`Store::collector_holdings_search`], [`Store::tokens_for_project_search`]);
/// blank text is no filter.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ListingFilter<'a> {
    pub text: Option<&'a str>,
    /// Exact feature name and value.
    pub trait_filter: Option<(&'a str, &'a str)>,
}

impl ListingFilter<'_> {
    fn text(&self) -> Option<&str> {
        self.text.map(str::trim).filter(|t| !t.is_empty())
    }

    fn is_empty(&self) -> bool {
        self.text().is_none() && self.trait_filter.is_none()
    }

    fn validate(&self) -> Result<(), StoreError> {
        if self
            .text()
            .is_some_and(|t| t.chars().count() > MAX_QUERY_CHARS)
        {
            return Err(StoreError::BadRequest(format!(
                "search text longer than {MAX_QUERY_CHARS} characters"
            )));
        }
        if let Some((name, value)) = self.trait_filter {
            if name.is_empty()
                || name.chars().count() > MAX_TRAIT_CHARS
                || value.chars().count() > MAX_TRAIT_CHARS
            {
                return Err(StoreError::BadRequest(format!(
                    "trait name must be 1..={MAX_TRAIT_CHARS} and value 0..={MAX_TRAIT_CHARS} \
                     characters"
                )));
            }
        }
        Ok(())
    }

    /// Listing-id parts: equal filters give equal parts, different filters different ones.
    fn parts(&self) -> [String; 3] {
        [
            opt_part(self.text()),
            opt_part(self.trait_filter.map(|(n, _)| n)),
            opt_part(self.trait_filter.map(|(_, v)| v)),
        ]
    }
}

/// The token carries feature `:tn` = `:tv`, in its chain attributes or fixture features.
pub(crate) const TOKEN_HAS_TRAIT: &str = " AND (EXISTS (SELECT 1 FROM json_each(t.attributes) j \
     WHERE j.value ->> 0 = :tn AND CAST(j.value ->> 1 AS TEXT) = :tv) \
     OR EXISTS (SELECT 1 FROM token_features tf WHERE tf.contract = t.contract \
     AND tf.token_id = t.token_id AND tf.name = :tn AND tf.value = :tv))";

/// Some token of project `?1` has feature `?2` = `?3` (one primary-key range).
pub(crate) const PROJECT_HAS_TRAIT: &str = "SELECT 1 FROM feature_counts \
     WHERE project_id = ?1 AND name = ?2 AND value = ?3 AND count > 0";

/// Visible tokens without the display joins: what a count needs, so it reads only the index.
pub(crate) const TOKEN_COUNT_FROM: &str =
    "FROM tokens t JOIN projects p ON p.id = t.project_id AND p.hidden = 0";

/// How holdings search text is matched.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HoldingsText {
    None,
    /// Title and artist words (`:fts`).
    Words,
    /// A number alone: edition number or token id (`:n`), or the words (`:fts`).
    Number,
    /// Words then a number ("Gap 28", "The Gap #28"): edition `:n` of a project matching the
    /// words (`:ftsw`), or the whole text as words (`:fts`).
    WordsAndNumber,
}

/// Holdings `WHERE` for `filter` parts. Text and trait each restrict the project ids the
/// holder index is probed with, so a page comes out in order without a sort. `after` starts
/// the project lists at the cursor's project (`:k0`).
pub(crate) fn holdings_where(text: HoldingsText, trait_filter: bool, after: bool) -> String {
    let from = if after { " AND rowid >= :k0" } else { "" };
    let words = |param: &str| {
        format!(
            "t.project_id IN (SELECT rowid FROM project_fts WHERE project_fts MATCH {param}{from})"
        )
    };
    let edition = "(t.iteration = :n OR t.token_id = :n)";
    let mut w = String::from(" WHERE t.holder_address = :owner");
    match text {
        HoldingsText::None => {}
        HoldingsText::Words => w.push_str(&format!(" AND {}", words(":fts"))),
        HoldingsText::Number => w.push_str(&format!(" AND ({edition} OR {})", words(":fts"))),
        HoldingsText::WordsAndNumber => w.push_str(&format!(
            " AND (({edition} AND {}) OR {})",
            words(":ftsw"),
            words(":fts")
        )),
    }
    if trait_filter {
        w.push_str(&format!(
            " AND t.project_id IN (SELECT project_id FROM feature_counts WHERE name = :tn \
             AND value = :tv AND count > 0{})",
            if after { " AND project_id >= :k0" } else { "" }
        ));
        w.push_str(TOKEN_HAS_TRAIT);
    }
    w
}

/// Holdings search text: the whole text as words, a trailing edition number (`28` or `#28`),
/// and the words before that number.
struct HoldingsQuery {
    words: Option<String>,
    number: Option<i64>,
    head: Option<String>,
}

fn holdings_query(text: &str) -> Result<HoldingsQuery, StoreError> {
    let words = fts_query(text)?;
    let trimmed = text.trim();
    let (head, last) = match trimmed.rsplit_once(char::is_whitespace) {
        Some((head, last)) => (head.trim(), last),
        None => ("", trimmed),
    };
    let digits = last.strip_prefix('#').unwrap_or(last);
    let number = (!digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()))
        .then(|| digits.parse().ok())
        .flatten();
    let head = match number {
        Some(_) if !head.is_empty() => fts_query(head)?,
        _ => None,
    };
    Ok(HoldingsQuery {
        words,
        number,
        head,
    })
}

/// What a project-editions search text is.
enum EditionText<'a> {
    /// An edition number or token id; `None` when too large to be either.
    Number(Option<i64>),
    /// An owner address or shown name.
    Owner(&'a str),
}

fn edition_text(text: &str) -> EditionText<'_> {
    let digits = text.strip_prefix('#').unwrap_or(text);
    if !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()) {
        EditionText::Number(digits.parse().ok())
    } else {
        EditionText::Owner(text)
    }
}

/// One keyset page. `next` resumes after the last item; `total` counts the whole listing.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct KeysetPage<T> {
    pub items: Vec<T>,
    pub next: Option<Cursor>,
    pub total: u64,
}

const CURSOR_DOMAIN: &[u8] = b"seeded-cursor-v1\0";
const MAX_CURSOR_CHARS: usize = 1024;

/// Opaque, URL-safe (lowercase hex) position after an item of one listing. It carries the
/// listing's sort key and a checksum, and only resumes the listing that issued it: anything
/// else is BadRequest. Rows inserted before the position are skipped, never repeated.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct Cursor(String);

fn bad_cursor(why: &str) -> StoreError {
    StoreError::BadRequest(format!("invalid cursor: {why}"))
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

impl Cursor {
    /// Accepts a cursor from a request; BadRequest unless well-formed and intact.
    pub fn parse(s: &str) -> Result<Cursor, StoreError> {
        Cursor(s.to_owned()).payload()?;
        Ok(Cursor(s.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn payload(&self) -> Result<Vec<Value>, StoreError> {
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

    fn encode(listing: &str, key: Vec<Value>) -> Cursor {
        let mut payload = vec![Value::String(listing.to_owned())];
        payload.extend(key);
        let bytes = serde_json::to_vec(&payload).unwrap_or_default();
        let mut all = cursor_sum(&bytes).to_vec();
        all.extend_from_slice(&bytes);
        Cursor(hex::encode(all))
    }

    /// The sort key, if this cursor belongs to `listing` and has `arity` key values.
    fn key(&self, listing: &str, arity: usize) -> Result<Vec<Value>, StoreError> {
        let mut v = self.payload()?;
        if v.first().and_then(Value::as_str) != Some(listing) {
            return Err(bad_cursor("issued for another listing"));
        }
        v.remove(0);
        if v.len() != arity || !v.iter().all(|x| x.is_string() || x.is_i64()) {
            return Err(bad_cursor("key shape"));
        }
        Ok(v)
    }
}

impl std::fmt::Display for Cursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Identifies a listing (kind and filters) so a cursor cannot resume another one.
fn listing_id(parts: &[&str]) -> String {
    let mut h = Sha256::new();
    for p in parts {
        h.update(p.as_bytes());
        h.update([0u8]);
    }
    hex::encode(&h.finalize()[..8])
}

fn check_limit(limit: u32) -> Result<(), StoreError> {
    if (1..=MAX_LIMIT).contains(&limit) {
        Ok(())
    } else {
        Err(StoreError::BadRequest(format!(
            "limit must be 1..={MAX_LIMIT}"
        )))
    }
}

/// Keyset values as SQL parameters.
fn key_params(key: &[Value]) -> Vec<Box<dyn ToSql>> {
    key.iter()
        .map(|v| -> Box<dyn ToSql> {
            match v {
                Value::String(s) => Box::new(s.clone()),
                other => Box::new(other.as_i64().unwrap_or_default()),
            }
        })
        .collect()
}

fn sql_value(r: &Row<'_>, i: usize) -> rusqlite::Result<Value> {
    use rusqlite::types::ValueRef;
    Ok(match r.get_ref(i)? {
        ValueRef::Integer(n) => Value::from(n),
        ValueRef::Text(t) => Value::String(String::from_utf8_lossy(t).into_owned()),
        _ => Value::Null,
    })
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StoreError {
    #[error("{what} not found: {key}")]
    NotFound { what: &'static str, key: String },
    #[error("bad request: {0}")]
    BadRequest(String),
    /// Retryable: busy, locked or I/O failure.
    #[error("store unavailable: {0}")]
    Unavailable(String),
    #[error("internal store error: {0}")]
    Internal(String),
}

/// Maps a SQLite error: lock and I/O classes are retryable, everything else is internal.
pub(crate) fn classify(e: &rusqlite::Error) -> StoreError {
    use rusqlite::ffi::ErrorCode as C;
    match e {
        rusqlite::Error::SqliteFailure(f, _) => match f.code {
            C::DatabaseBusy
            | C::DatabaseLocked
            | C::SystemIoFailure
            | C::CannotOpen
            | C::DiskFull
            | C::FileLockingProtocolFailed
            | C::OutOfMemory
            | C::OperationInterrupted => StoreError::Unavailable(e.to_string()),
            _ => StoreError::Internal(e.to_string()),
        },
        _ => StoreError::Internal(e.to_string()),
    }
}

pub(crate) trait Db<T> {
    fn db(self) -> Result<T, StoreError>;
}

impl<T> Db<T> for rusqlite::Result<T> {
    fn db(self) -> Result<T, StoreError> {
        self.map_err(|e| classify(&e))
    }
}

fn not_found(what: &'static str, key: impl ToString) -> StoreError {
    StoreError::NotFound {
        what,
        key: key.to_string(),
    }
}

const BASE58: &str = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

pub(crate) fn is_base58(s: &str) -> bool {
    !s.is_empty() && s.chars().all(|c| BASE58.contains(c))
}

/// `^KT1[1-9A-HJ-NP-Za-km-z]{33}$`
pub(crate) fn is_contract(s: &str) -> bool {
    s.len() == 36 && s.starts_with("KT1") && is_base58(&s[3..])
}

/// Implicit (tz1..tz4) or originated (KT1) Tezos address.
pub(crate) fn is_tezos_address(s: &str) -> bool {
    s.len() == 36
        && ["tz1", "tz2", "tz3", "tz4", "KT1"]
            .iter()
            .any(|p| s.starts_with(p))
        && is_base58(&s[3..])
}

fn require_address(address: &str) -> Result<(), StoreError> {
    if is_tezos_address(address) {
        Ok(())
    } else {
        Err(StoreError::BadRequest(
            "address must be a tz1..tz4 or KT1 address".into(),
        ))
    }
}

/// Builds an FTS5 query: each alphanumeric run becomes a quoted prefix term, terms are ANDed.
/// `Ok(None)` means no terms, so nothing can match.
pub(crate) fn fts_query(text: &str) -> Result<Option<String>, StoreError> {
    let text = text.trim();
    if text.chars().count() > MAX_QUERY_CHARS {
        return Err(StoreError::BadRequest(format!(
            "search text longer than {MAX_QUERY_CHARS} characters"
        )));
    }
    let terms: Vec<&str> = text
        .split(|c: char| !c.is_alphanumeric())
        .filter(|t| !t.is_empty())
        .collect();
    if terms.len() > MAX_QUERY_TERMS {
        return Err(StoreError::BadRequest("too many search terms".into()));
    }
    // A one-character prefix would expand to a large slice of the index: match it exactly.
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

pub(crate) fn sql_id(id: u64) -> Option<i64> {
    i64::try_from(id).ok()
}

fn get_u64(r: &Row<'_>, i: usize) -> rusqlite::Result<u64> {
    let v: i64 = r.get(i)?;
    u64::try_from(v).map_err(|_| rusqlite::Error::IntegralValueOutOfRange(i, v))
}

fn get_opt_u64(r: &Row<'_>, i: usize) -> rusqlite::Result<Option<u64>> {
    match r.get::<_, Option<i64>>(i)? {
        None => Ok(None),
        Some(v) => u64::try_from(v)
            .map(Some)
            .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(i, v)),
    }
}

pub struct Store {
    conn: Connection,
}

impl Store {
    /// Opens or creates a database file and migrates it.
    pub fn open(path: &Path) -> Result<Store, StoreError> {
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
            return Err(StoreError::Internal(format!(
                "journal_mode is {mode}, expected wal"
            )));
        }
        conn.pragma_update(None, "synchronous", "NORMAL").db()?;
        Self::finish_open(conn)
    }

    pub fn open_read_only(path: &Path) -> Result<Store, StoreError> {
        let conn = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .db()?;
        conn.busy_timeout(BUSY_TIMEOUT).db()?;
        conn.pragma_update(None, "foreign_keys", true).db()?;
        conn.set_prepared_statement_cache_capacity(STATEMENT_CACHE);
        match user_version(&conn)? {
            SCHEMA_VERSION => Ok(Store { conn }),
            0 => Err(StoreError::Unavailable(
                "database is not initialised".into(),
            )),
            v => Err(StoreError::Internal(format!(
                "schema version {v}, expected {SCHEMA_VERSION}"
            ))),
        }
    }

    /// Opens an existing, current database read-write without creating or migrating it, for
    /// writes from the web such as profile edits. Like [`Store::open`] it requires WAL and sets
    /// `synchronous=NORMAL`. A missing, uninitialised or other-schema file is refused with
    /// its reason; `seeded-index upgrade` migrates offline.
    pub fn open_existing_write(path: &Path) -> Result<Store, StoreError> {
        let conn = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .db()?;
        conn.busy_timeout(BUSY_TIMEOUT).db()?;
        match user_version(&conn)? {
            SCHEMA_VERSION => {}
            0 => {
                return Err(StoreError::Unavailable(
                    "database is not initialised; run `seeded-index upgrade` offline".into(),
                ));
            }
            v => {
                return Err(StoreError::Unavailable(format!(
                    "schema version {v}, expected {SCHEMA_VERSION}; run `seeded-index upgrade` offline"
                )));
            }
        }
        let mode: String = conn
            .pragma_query_value(None, "journal_mode", |r| r.get(0))
            .db()?;
        if !mode.eq_ignore_ascii_case("wal") {
            return Err(StoreError::Unavailable(format!(
                "journal_mode is {mode}, expected wal"
            )));
        }
        conn.pragma_update(None, "synchronous", "NORMAL").db()?;
        conn.pragma_update(None, "foreign_keys", true).db()?;
        conn.set_prepared_statement_cache_capacity(STATEMENT_CACHE);
        Ok(Store { conn })
    }

    pub fn open_in_memory() -> Result<Store, StoreError> {
        Self::finish_open(Connection::open_in_memory().db()?)
    }

    fn finish_open(conn: Connection) -> Result<Store, StoreError> {
        conn.pragma_update(None, "foreign_keys", true).db()?;
        conn.set_prepared_statement_cache_capacity(STATEMENT_CACHE);
        let mut store = Store { conn };
        store.migrate()?;
        Ok(store)
    }

    /// Migrates to the current version (from 0 to 10) in one immediate transaction.
    fn migrate(&mut self) -> Result<(), StoreError> {
        if user_version(&self.conn)? == SCHEMA_VERSION {
            return Ok(());
        }
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .db()?;
        let from = user_version(&tx)?;
        if from == SCHEMA_VERSION {
            return Ok(());
        }
        if !(0..SCHEMA_VERSION).contains(&from) {
            return Err(StoreError::Internal(format!(
                "unsupported schema version {from}, expected {SCHEMA_VERSION}"
            )));
        }
        if from < 1 {
            tx.execute_batch(SCHEMA_V1).db()?;
        }
        if from < 2 {
            tx.execute_batch(SCHEMA_V2).db()?;
        }
        if from < 3 {
            tx.execute_batch(SCHEMA_V3).db()?;
            // A fresh database has nothing to backfill.
            if from > 0 {
                backfill_content_roots(&tx)?;
            }
        }
        if from < 4 {
            tx.execute_batch(SCHEMA_V4).db()?;
        }
        if from < 5 {
            tx.execute_batch(SCHEMA_V5).db()?;
        }
        if from < 6 {
            tx.execute_batch(SCHEMA_V6).db()?;
            profile::backfill_name_keys(&tx)?;
        }
        if from < 10 {
            // Created first: shown names, resolved by the rebuilds below, read it.
            tx.execute_batch(names::SCHEMA_V10).db()?;
        }
        if from < 7 {
            market::migrate_v7(&tx)?;
        }
        if from < 8 {
            tx.execute_batch(profile::SCHEMA_V8).db()?;
            search::migrate_v8(&tx)?;
        }
        if from < 9 {
            tx.execute_batch(volume::SCHEMA_V9).db()?;
            search::migrate_v9(&tx)?;
        }
        if from < 10 {
            names::migrate_v10(&tx)?;
        }
        tx.pragma_update(None, "user_version", SCHEMA_VERSION)
            .db()?;
        tx.commit().db()
    }

    /// Rewrites the artist names of the search indexes from shown names (see [`search`]) in
    /// one immediate transaction; returns the number of artists indexed. Writers keep them in
    /// sync already; this repairs rows written around the store (raw SQL).
    pub fn rebuild_search_names(&mut self) -> Result<usize, StoreError> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .db()?;
        let n = search::rebuild(&tx)?;
        tx.commit().db()?;
        Ok(n)
    }

    pub(crate) fn conn_mut(&mut self) -> &mut Connection {
        &mut self.conn
    }

    pub(crate) fn conn(&self) -> &Connection {
        &self.conn
    }

    /// Runs `f` inside one deferred read transaction so multi-statement reads are consistent.
    /// Inside an active transaction (see [`Store::read_snapshot`]) it joins that one.
    fn read<T>(
        &self,
        f: impl FnOnce(&Connection) -> Result<T, StoreError>,
    ) -> Result<T, StoreError> {
        if !self.conn.is_autocommit() {
            return f(&self.conn);
        }
        let tx = self.conn.unchecked_transaction().db()?;
        let out = f(&tx)?;
        tx.commit().db()?;
        Ok(out)
    }

    /// Runs `f` inside one deferred read transaction: every read method `f` calls on this store
    /// sees the same snapshot, taken at its first read. An error from `f` rolls back.
    /// Nested calls join the outer snapshot.
    pub fn read_snapshot<T>(
        &self,
        f: impl FnOnce(&Self) -> Result<T, StoreError>,
    ) -> Result<T, StoreError> {
        if !self.conn.is_autocommit() {
            return f(self);
        }
        // Dropping the transaction on the error path rolls it back.
        let tx = self.conn.unchecked_transaction().db()?;
        let out = f(self)?;
        tx.commit().db()?;
        Ok(out)
    }

    /// Text matches words of the title or of the credited artists' shown names (see
    /// [`search`]).
    pub fn projects(&self, q: &ProjectQuery) -> Result<Page<ProjectSummary>, StoreError> {
        q.page.validate()?;
        let fts = match q.text.as_deref().map(fts_query).transpose()? {
            Some(None) => return Ok(q.page.empty()),
            Some(Some(s)) => Some(s),
            None => None,
        };
        let mut from = String::from("FROM projects p WHERE p.hidden = 0");
        let mut params: Vec<(&str, &dyn ToSql)> = Vec::new();
        if let Some(fts) = &fts {
            from.push_str(PROJECT_TEXT_FILTER);
            params.push((":fts", fts));
        }
        if let Some(artist) = &q.artist {
            from.push_str(ARTIST_FILTER);
            params.push((":artist", artist));
        }
        if q.sort == ProjectSort::VolumeDesc {
            return self.read(|c| volume::volume_page(c, &from, &params, q.page));
        }
        self.read(|c| summaries_page(c, &from, q.sort.order_by(), &params, q.page))
    }

    pub fn project(&self, id: u64) -> Result<Project, StoreError> {
        let key = sql_id(id).ok_or_else(|| not_found("project", id))?;
        self.read(|c| {
            let row = c
                .prepare_cached(
                    "SELECT title, description, preview, contract, issuer, artifact_uri, \
                     editions, first_mint, last_mint, index_state, slug, supply, thumbnail_uri, \
                     display_uri, source FROM projects WHERE id = ?1 AND hidden = 0",
                )
                .db()?
                .query_row([key], |r| {
                    Ok(ProjectRow {
                        title: r.get(0)?,
                        description: r.get(1)?,
                        preview: r.get(2)?,
                        contract: r.get(3)?,
                        issuer: r.get(4)?,
                        artifact_uri: r.get(5)?,
                        editions: r.get(6)?,
                        first_mint: r.get(7)?,
                        last_mint: r.get(8)?,
                        index_state: r.get(9)?,
                        slug: r.get(10)?,
                        supply: r.get(11)?,
                        thumbnail_uri: r.get(12)?,
                        display_uri: r.get(13)?,
                        source: r.get(14)?,
                    })
                })
                .optional()
                .db()?
                .ok_or_else(|| not_found("project", id))?;
            Ok(Project {
                id,
                title: row.title,
                artists: project_artists(c, key)?,
                description: row.description,
                preview: row.preview,
                contract: row.contract,
                issuer: row.issuer,
                artifact_uri: row.artifact_uri,
                editions: row.editions,
                first_mint: row.first_mint,
                last_mint: row.last_mint,
                index_state: index_state(&row.index_state)?,
                slug: row.slug,
                supply: row.supply,
                thumbnail_uri: row.thumbnail_uri,
                display_uri: row.display_uri,
                source: source(&row.source)?,
            })
        })
    }

    /// Ordered by iteration.
    pub fn tokens_for_project(
        &self,
        project_id: u64,
        page: PageRequest,
    ) -> Result<Page<Token>, StoreError> {
        page.validate()?;
        let key = sql_id(project_id).ok_or_else(|| not_found("project", project_id))?;
        self.read(|c| {
            require_project(c, key, project_id)?;
            let total = project_token_total(c, key)?;
            tokens_page_counted(
                c,
                &format!("{TOKEN_FROM} WHERE t.project_id = :project"),
                &[(":project", &key)],
                page,
                total,
            )
        })
    }

    pub fn token(&self, contract: &str, token_id: u64) -> Result<Token, StoreError> {
        if !is_contract(contract) {
            return Err(StoreError::BadRequest(
                "contract must be a KT1 address".into(),
            ));
        }
        let missing = || not_found("token", format!("{contract}/{token_id}"));
        let key = sql_id(token_id).ok_or_else(missing)?;
        self.read(|c| {
            let sql = format!(
                "SELECT {TOKEN_COLS} {TOKEN_FROM} WHERE t.contract = ?1 AND t.token_id = ?2"
            );
            let row = c
                .prepare_cached(&sql)
                .db()?
                .query_row((contract, key), token_row)
                .optional()
                .db()?
                .ok_or_else(missing)?;
            finish_token(c, row)
        })
    }

    pub fn token_by_iteration(&self, project_id: u64, iteration: u32) -> Result<Token, StoreError> {
        let key = sql_id(project_id).ok_or_else(|| not_found("project", project_id))?;
        self.read(|c| {
            let sql = format!(
                "SELECT {TOKEN_COLS} {TOKEN_FROM} WHERE t.project_id = ?1 AND t.iteration = ?2"
            );
            let row = c
                .prepare_cached(&sql)
                .db()?
                .query_row((key, iteration), token_row)
                .optional()
                .db()?;
            match row {
                Some(row) => finish_token(c, row),
                None => {
                    require_project(c, key, project_id)?;
                    Err(not_found("token", format!("{project_id}#{iteration}")))
                }
            }
        })
    }

    /// Feature value counts over the project's indexed tokens, in first-seen feature order.
    /// Reads `feature_counts`; `total` is `editions`, which sync and import write equal to
    /// the indexed token count in the same transaction as the tokens.
    pub fn feature_shares(&self, project_id: u64) -> Result<Vec<FeatureShare>, StoreError> {
        let key = sql_id(project_id).ok_or_else(|| not_found("project", project_id))?;
        self.read(|c| {
            require_project(c, key, project_id)?;
            let total = u32::try_from(project_token_total(c, key)?)
                .map_err(|_| StoreError::Internal("token total out of range".into()))?;
            let mut st = c.prepare_cached(FEATURE_SHARES).db()?;
            let rows = st
                .query_map([key], |r| {
                    Ok(FeatureShare {
                        name: r.get(0)?,
                        value: r.get(1)?,
                        count: r.get(2)?,
                        total,
                    })
                })
                .db()?;
            rows.collect::<rusqlite::Result<Vec<_>>>().db()
        })
    }

    pub fn artists(&self, q: &ArtistQuery) -> Result<Page<ArtistSummary>, StoreError> {
        q.page.validate()?;
        let fts = match q.text.as_deref().map(fts_query).transpose()? {
            Some(None) => return Ok(q.page.empty()),
            Some(Some(s)) => Some(s),
            None => None,
        };
        // An artist whose every project is hidden is not listed.
        let mut from = format!("FROM artists a WHERE {ARTIST_VISIBLE}");
        let mut params: Vec<(&str, &dyn ToSql)> = Vec::new();
        if let Some(fts) = &fts {
            from.push_str(ARTIST_TEXT_FILTER);
            params.push((":fts", fts));
        }
        let page = q.page;
        self.read(|c| {
            let total = count(c, &from, &params)?;
            let sql = format!(
                "SELECT a.id, a.name, \
                 (SELECT count(*) FROM project_artists pa JOIN projects p ON p.id = pa.project_id \
                  WHERE pa.artist_id = a.id AND p.hidden = 0) \
                 {from} ORDER BY a.name_key, a.id LIMIT :limit OFFSET :offset"
            );
            let mut all = params.clone();
            all.push((":limit", &page.limit));
            all.push((":offset", &page.offset));
            let mut st = c.prepare_cached(&sql).db()?;
            let items = st
                .query_map(all.as_slice(), |r| {
                    Ok(ArtistSummary {
                        id: r.get(0)?,
                        name: r.get(1)?,
                        project_count: r.get(2)?,
                    })
                })
                .db()?
                .collect::<rusqlite::Result<Vec<_>>>()
                .db()?;
            Ok(Page {
                items,
                total,
                offset: page.offset,
                limit: page.limit,
            })
        })
    }

    pub fn artist(&self, id: &str) -> Result<Artist, StoreError> {
        self.read(|c| artist_by_id(c, id))
    }

    /// The artist whose Tezos address is `address`.
    pub fn artist_by_address(&self, address: &str) -> Result<Artist, StoreError> {
        require_address(address)?;
        self.read(|c| {
            let id: String = c
                .prepare_cached(&format!(
                    "SELECT a.id FROM artists a WHERE a.address = ?1 AND {ARTIST_VISIBLE} \
                     ORDER BY a.seq LIMIT 1"
                ))
                .db()?
                .query_row([address], |r| r.get(0))
                .optional()
                .db()?
                .ok_or_else(|| not_found("artist", address))?;
            artist_by_id(c, &id)
        })
    }

    /// Ordered by project id.
    pub fn artist_projects(
        &self,
        id: &str,
        page: PageRequest,
    ) -> Result<Page<ProjectSummary>, StoreError> {
        page.validate()?;
        self.read(|c| {
            let id = require_artist(c, id)?;
            summaries_page(
                c,
                "FROM projects p JOIN project_artists pa ON pa.project_id = p.id \
                 WHERE pa.artist_id = :artist AND p.hidden = 0",
                "p.id",
                &[(":artist", &id)],
                page,
            )
        })
    }

    /// Fallback only: tokens whose snapshot owner alias equals one of the artist's aliases
    /// (case-insensitive) that no other artist uses. Partial by nature: unaliased addresses are
    /// not matched; addresses are authoritative (see [`Store::collector_holdings`]).
    pub fn artist_known_holdings(
        &self,
        id: &str,
        page: PageRequest,
    ) -> Result<Page<Token>, StoreError> {
        page.validate()?;
        self.read(|c| {
            let id = require_artist(c, id)?;
            tokens_page(
                c,
                &format!(
                    "{TOKEN_FROM} WHERE t.owner_alias_key IN \
                     (SELECT alias_key FROM artist_aliases WHERE artist_id = :artist \
                      AND alias_key NOT IN (SELECT alias_key FROM artist_aliases \
                      WHERE artist_id <> :artist))"
                ),
                &[(":artist", &id)],
                page,
            )
        })
    }

    /// An address's fxhash user name and visible holdings: tokens it holds plus tokens it listed
    /// in a marketplace escrow. NotFound when it holds nothing in the index, and for escrow
    /// contracts themselves. Holdings come only from ledger balances and listing storage,
    /// never from minters or aliases.
    pub fn collector(&self, address: &str) -> Result<Collector, StoreError> {
        require_address(address)?;
        self.read(|c| {
            if is_known_address(c, address)? {
                return Err(not_found(
                    "collector",
                    format!("{address} (escrow or burn address)"),
                ));
            }
            let (token_count, listed) = holder_totals(c, address)?;
            if token_count == 0 {
                return Err(not_found("collector", address));
            }
            let alias = c
                .prepare_cached(
                    "SELECT coalesce((SELECT name FROM identities WHERE address = ?1), \
                     (SELECT name FROM users WHERE address = ?1))",
                )
                .db()?
                .query_row([address], |r| r.get(0))
                .optional()
                .db()?
                .flatten();
            Ok(Collector {
                address: address.to_owned(),
                alias,
                token_count,
                listed,
            })
        })
    }

    /// Visible tokens held by `address`, including its escrowed listings (see
    /// [`OwnerSnapshot::listed_by`]), ordered by project id and iteration. An address without
    /// holdings, or an escrow contract, is an empty page.
    pub fn collector_holdings(
        &self,
        address: &str,
        page: PageRequest,
    ) -> Result<Page<Token>, StoreError> {
        page.validate()?;
        require_address(address)?;
        self.read(|c| {
            let (total, _) = holder_totals(c, address)?;
            tokens_page_counted(
                c,
                &format!("{TOKEN_FROM} WHERE t.holder_address = :owner"),
                &[(":owner", &address)],
                page,
                total,
            )
        })
    }

    /// Coverage counts; see [`IndexStatus`].
    /// Visible projects whose title and artists match another visible project, with the
    /// suffix that tells them apart: the first-mint year, or the project id when that is
    /// shared or missing.
    pub fn project_title_suffixes(
        &self,
    ) -> Result<std::collections::HashMap<u64, String>, StoreError> {
        self.read(|c| {
            let mut st = c
                .prepare(
                    "WITH k AS (SELECT p.id, lower(p.title) AS t, \
                     coalesce((SELECT group_concat(artist_id, ',') FROM (SELECT artist_id \
                       FROM project_artists WHERE project_id = p.id ORDER BY artist_id)), '') AS a, \
                     substr(coalesce(p.first_mint, ''), 1, 4) AS y \
                     FROM projects p WHERE p.hidden = 0) \
                     SELECT id, t, a, y FROM k WHERE (t, a) IN \
                     (SELECT t, a FROM k GROUP BY t, a HAVING count(*) > 1) ORDER BY t, a, id",
                )
                .db()?;
            let rows = st
                .query_map([], |r| {
                    Ok((
                        get_u64(r, 0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, String>(2)?,
                        r.get::<_, String>(3)?,
                    ))
                })
                .db()?
                .collect::<Result<Vec<_>, _>>()
                .db()?;
            let mut groups: std::collections::BTreeMap<(String, String), Vec<(u64, String)>> =
                std::collections::BTreeMap::new();
            for (id, title, artists, year) in rows {
                groups.entry((title, artists)).or_default().push((id, year));
            }
            let mut suffixes = std::collections::HashMap::new();
            for members in groups.values() {
                for (id, year) in members {
                    let unique_year =
                        !year.is_empty() && members.iter().filter(|(_, y)| y == year).count() == 1;
                    let suffix = if unique_year {
                        format!("({year})")
                    } else {
                        format!("(project {id})")
                    };
                    suffixes.insert(*id, suffix);
                }
            }
            Ok(suffixes)
        })
    }

    pub fn index_status(&self) -> Result<IndexStatus, StoreError> {
        self.read(|c| {
            let one = |sql: &str| -> Result<u64, StoreError> {
                c.prepare_cached(sql)
                    .db()?
                    .query_row([], |r| get_u64(r, 0))
                    .db()
            };
            let projects_total = one("SELECT count(*) FROM projects WHERE hidden = 0")?;
            let projects_indexed =
                one("SELECT count(*) FROM projects WHERE hidden = 0 AND index_state = 'indexed'")?;
            let projects_flagged_hidden = one("SELECT count(*) FROM projects WHERE hidden = 1")?;
            let fixture = one(STATUS_FIXTURE)?;
            let tokens_mismatch = one(STATUS_MISMATCH)?;
            // Chain tokens whose catalogue record awaits re-comparison are not verified.
            let pending = one(STATUS_PENDING)?;
            // Chain tokens without a catalogue record: never compared, so not verified.
            let uncatalogued = one(STATUS_UNCATALOGUED)?;
            let mut contracts: Vec<ContractStatus> = TOKEN_CONTRACTS
                .iter()
                .map(|c| ContractStatus {
                    contract: (*c).to_owned(),
                    tokens: 0,
                    last_level: None,
                    last_sync: None,
                })
                .collect();
            let per_contract = |sql: &str| -> Result<Vec<(String, u64)>, StoreError> {
                c.prepare_cached(sql)
                    .db()?
                    .query_map([], |r| Ok((r.get(0)?, get_u64(r, 1)?)))
                    .db()?
                    .collect::<rusqlite::Result<Vec<_>>>()
                    .db()
            };
            // `editions` is each indexed project's token count, kept by import and sync; summing
            // it avoids scanning the wide tokens table.
            let visible = per_contract(STATUS_PER_CONTRACT)?;
            for (contract, n) in visible {
                match contracts.iter_mut().find(|s| s.contract == contract) {
                    Some(s) => s.tokens = n,
                    None => contracts.push(ContractStatus {
                        contract,
                        tokens: n,
                        last_level: None,
                        last_sync: None,
                    }),
                }
            }
            let mut st = c
                .prepare_cached("SELECT contract, synced_level, synced_at FROM sync_state")
                .db()?;
            let synced = st
                .query_map([], |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        get_opt_u64(r, 1)?,
                        r.get::<_, Option<String>>(2)?,
                    ))
                })
                .db()?
                .collect::<rusqlite::Result<Vec<_>>>()
                .db()?;
            for (contract, level, at) in synced {
                if let Some(s) = contracts.iter_mut().find(|s| s.contract == contract) {
                    s.last_level = level;
                    s.last_sync = at;
                }
            }
            let tokens_total = contracts.iter().map(|s| s.tokens).sum::<u64>();
            Ok(IndexStatus {
                projects_total,
                projects_indexed,
                projects_flagged_hidden,
                tokens_total,
                tokens_verified: tokens_total
                    .saturating_sub(fixture + tokens_mismatch + pending + uncatalogued),
                tokens_mismatch,
                contracts,
            })
        })
    }

    /// Keyset counterpart of [`Store::projects`]: stable for every [`ProjectSort`], no
    /// offset limit.
    pub fn projects_page(
        &self,
        q: &ProjectFilter,
        after: Option<&Cursor>,
        limit: u32,
    ) -> Result<KeysetPage<ProjectSummary>, StoreError> {
        check_limit(limit)?;
        let (name, key_cols, keyset) = match q.sort {
            ProjectSort::TitleAsc => (
                "title",
                "p.title_key, p.id",
                "(p.title_key, p.id) > (:k0, :k1)",
            ),
            ProjectSort::ArtistAsc => (
                "artist",
                "p.artist_key, p.id",
                "(p.artist_key, p.id) > (:k0, :k1)",
            ),
            ProjectSort::IdAsc => ("id", "p.id", "p.id > :k0"),
            ProjectSort::IdDesc => ("id-desc", "p.id", "p.id < :k0"),
            ProjectSort::VolumeDesc => ("volume", volume::RANKED_KEY, ""),
        };
        let arity = key_cols.split(',').count();
        let listing = listing_id(&[
            "projects",
            name,
            &opt_part(q.text.as_deref()),
            &opt_part(q.artist.as_deref()),
        ]);
        let key = after.map(|c| c.key(&listing, arity)).transpose()?;
        let fts = match q.text.as_deref().map(fts_query).transpose()? {
            Some(None) => return Ok(empty_keyset()),
            Some(Some(s)) => Some(s),
            None => None,
        };
        let mut from = String::from("FROM projects p WHERE p.hidden = 0");
        let mut params: Vec<(&str, &dyn ToSql)> = Vec::new();
        if let Some(fts) = &fts {
            from.push_str(PROJECT_TEXT_FILTER);
            params.push((":fts", fts));
        }
        if let Some(artist) = &q.artist {
            from.push_str(ARTIST_FILTER);
            params.push((":artist", artist));
        }
        let order = q.sort.order_by();
        self.read(|c| {
            let total = count(c, &from, &params)?;
            let (items, next) = if q.sort == ProjectSort::VolumeDesc {
                volume::volume_keyset(c, &from, &params, key.as_deref(), limit)?
            } else {
                summaries_keyset(
                    c,
                    &from,
                    &params,
                    key_cols,
                    keyset,
                    order,
                    key.as_deref(),
                    limit,
                )?
            };
            Ok(KeysetPage {
                items,
                next: next.map(|k| Cursor::encode(&listing, k)),
                total,
            })
        })
    }

    /// Keyset counterpart of [`Store::tokens_for_project`] (iteration order).
    pub fn tokens_for_project_page(
        &self,
        project_id: u64,
        after: Option<&Cursor>,
        limit: u32,
    ) -> Result<KeysetPage<Token>, StoreError> {
        self.tokens_for_project_search(project_id, &ListingFilter::default(), after, limit)
    }

    /// [`Store::tokens_for_project_page`] narrowed by `filter`. Text is an edition number or
    /// token id (`12` or `#12`), an owner address (holder or ledger owner), or an owner's
    /// shown name (exact, case-insensitive, valid names only; see [`Store::profile_by_name`]).
    /// A cursor resumes only the listing with the same filter.
    pub fn tokens_for_project_search(
        &self,
        project_id: u64,
        filter: &ListingFilter<'_>,
        after: Option<&Cursor>,
        limit: u32,
    ) -> Result<KeysetPage<Token>, StoreError> {
        check_limit(limit)?;
        filter.validate()?;
        let key_id = sql_id(project_id).ok_or_else(|| not_found("project", project_id))?;
        let id_text = project_id.to_string();
        let listing = if filter.is_empty() {
            listing_id(&["project-tokens", &id_text])
        } else {
            let [a, b, c] = filter.parts();
            listing_id(&["project-tokens-search", &id_text, &a, &b, &c])
        };
        let key = after.map(|c| c.key(&listing, 3)).transpose()?;
        self.read(|c| {
            require_project(c, key_id, project_id)?;
            let number: i64;
            let who: String;
            let mut filters = String::from(" WHERE t.project_id = :project");
            let mut params: Vec<(&str, &dyn ToSql)> = vec![(":project", &key_id)];
            match filter.text().map(edition_text) {
                None => {}
                Some(EditionText::Number(None)) => return Ok(empty_keyset()),
                Some(EditionText::Number(Some(n))) => {
                    number = n;
                    filters.push_str(" AND (t.iteration = :n OR t.token_id = :n)");
                    params.push((":n", &number));
                }
                Some(EditionText::Owner(text)) => {
                    who = if is_tezos_address(text) {
                        text.to_owned()
                    } else {
                        match profile::owner_of_name(c, text)? {
                            Some(a) => a,
                            None => return Ok(empty_keyset()),
                        }
                    };
                    filters.push_str(" AND (t.holder_address = :who OR t.owner_address = :who)");
                    params.push((":who", &who));
                }
            }
            if let Some((name, value)) = &filter.trait_filter {
                // The maintained counts rule out a trait the project lacks before any scan.
                let carried = c
                    .prepare_cached(PROJECT_HAS_TRAIT)
                    .db()?
                    .exists(rusqlite::params![key_id, name, value])
                    .db()?;
                if !carried {
                    return Ok(empty_keyset());
                }
                filters.push_str(TOKEN_HAS_TRAIT);
                params.push((":tn", name));
                params.push((":tv", value));
            }
            let total = if filter.is_empty() {
                project_token_total(c, key_id)?
            } else {
                count(c, &format!("{TOKEN_COUNT_FROM}{filters}"), &params)?
            };
            let (items, next) = tokens_keyset(
                c,
                &format!("{TOKEN_FROM}{filters}"),
                &params,
                "(t.iteration, t.contract, t.token_id) > (:k0, :k1, :k2)",
                "t.iteration, t.contract, t.token_id",
                key.as_deref(),
                limit,
                |t| {
                    vec![
                        Value::from(t.iteration),
                        Value::from(t.contract.clone()),
                        Value::from(t.token_id),
                    ]
                },
            )?;
            Ok(KeysetPage {
                items,
                next: next.map(|k| Cursor::encode(&listing, k)),
                total,
            })
        })
    }

    /// Keyset counterpart of [`Store::collector_holdings`].
    pub fn collector_holdings_page(
        &self,
        address: &str,
        after: Option<&Cursor>,
        limit: u32,
    ) -> Result<KeysetPage<Token>, StoreError> {
        self.collector_holdings_search(address, &ListingFilter::default(), after, limit)
    }

    /// [`Store::collector_holdings_page`] narrowed by `filter`. Text matches words of the
    /// project title or shown artist names (the project search index, same escaping and term
    /// cap as [`Store::projects`]). Driven by the holder index probed per matching project, so it
    /// stays indexed for any holding size (`holdings_search_is_driven_by_the_holder_index`).
    /// A cursor resumes only the listing with the same filter.
    pub fn collector_holdings_search(
        &self,
        address: &str,
        filter: &ListingFilter<'_>,
        after: Option<&Cursor>,
        limit: u32,
    ) -> Result<KeysetPage<Token>, StoreError> {
        check_limit(limit)?;
        require_address(address)?;
        filter.validate()?;
        let listing = if filter.is_empty() {
            listing_id(&["holdings", address])
        } else {
            let [a, b, c] = filter.parts();
            listing_id(&["holdings-search", address, &a, &b, &c])
        };
        let key = after.map(|c| c.key(&listing, 4)).transpose()?;
        let HoldingsQuery {
            words: fts,
            number,
            head,
        } = match filter.text() {
            Some(text) => holdings_query(text)?,
            None => HoldingsQuery {
                words: None,
                number: None,
                head: None,
            },
        };
        let text = match (&fts, number, &head) {
            (None, None, _) if filter.text().is_some() => return Ok(empty_keyset()),
            (None, None, _) => HoldingsText::None,
            (_, Some(_), Some(_)) => HoldingsText::WordsAndNumber,
            (_, Some(_), None) => HoldingsText::Number,
            (Some(_), None, _) => HoldingsText::Words,
        };
        // A number alone always has words too ("28"); `:fts` is bound whenever text is set.
        let fts = fts.unwrap_or_default();
        let trait_filter = filter.trait_filter.is_some();
        let count_from = format!(
            "{TOKEN_COUNT_FROM}{}",
            holdings_where(text, trait_filter, false)
        );
        let page_from = format!(
            "{TOKEN_FROM}{}",
            holdings_where(text, trait_filter, key.is_some())
        );
        let mut params: Vec<(&str, &dyn ToSql)> = vec![(":owner", &address)];
        if text != HoldingsText::None {
            params.push((":fts", &fts));
        }
        if let Some(n) = &number {
            params.push((":n", n));
        }
        if let Some(head) = &head {
            params.push((":ftsw", head));
        }
        if let Some((name, value)) = &filter.trait_filter {
            params.push((":tn", name));
            params.push((":tv", value));
        }
        self.read(|c| {
            let total = if filter.is_empty() {
                holder_totals(c, address)?.0
            } else {
                count(c, &count_from, &params)?
            };
            let (items, next) = tokens_keyset(
                c,
                &page_from,
                &params,
                "(t.project_id, t.iteration, t.contract, t.token_id) > (:k0, :k1, :k2, :k3)",
                TOKEN_ORDER,
                key.as_deref(),
                limit,
                |t| {
                    vec![
                        Value::from(t.project_id),
                        Value::from(t.iteration),
                        Value::from(t.contract.clone()),
                        Value::from(t.token_id),
                    ]
                },
            )?;
            Ok(KeysetPage {
                items,
                next: next.map(|k| Cursor::encode(&listing, k)),
                total,
            })
        })
    }

    /// Keyset counterpart of [`Store::artist_projects`] (project id order).
    pub fn artist_projects_page(
        &self,
        id: &str,
        after: Option<&Cursor>,
        limit: u32,
    ) -> Result<KeysetPage<ProjectSummary>, StoreError> {
        check_limit(limit)?;
        let listing = listing_id(&["artist-projects", id]);
        let key = after.map(|c| c.key(&listing, 1)).transpose()?;
        self.read(|c| {
            let id = require_artist(c, id)?;
            let from = "FROM projects p JOIN project_artists pa ON pa.project_id = p.id \
                        WHERE pa.artist_id = :artist AND p.hidden = 0";
            let params: [(&str, &dyn ToSql); 1] = [(":artist", &id)];
            let total = count(c, from, &params)?;
            let (items, next) = summaries_keyset(
                c,
                from,
                &params,
                "p.id",
                "p.id > :k0",
                "p.id",
                key.as_deref(),
                limit,
            )?;
            Ok(KeysetPage {
                items,
                next: next.map(|k| Cursor::encode(&listing, k)),
                total,
            })
        })
    }

    /// Keyset counterpart of [`Store::artists`] (name, then id). Text matches the shown name,
    /// the address, or curated aliases while the curated name is shown (see [`search`]).
    pub fn artists_page(
        &self,
        text: Option<&str>,
        after: Option<&Cursor>,
        limit: u32,
    ) -> Result<KeysetPage<ArtistSummary>, StoreError> {
        check_limit(limit)?;
        let listing = listing_id(&["artists", &opt_part(text)]);
        let key = after.map(|c| c.key(&listing, 2)).transpose()?;
        let fts = match text.map(fts_query).transpose()? {
            Some(None) => return Ok(empty_keyset()),
            Some(Some(s)) => Some(s),
            None => None,
        };
        let mut from = format!("FROM artists a WHERE {ARTIST_VISIBLE}");
        let mut params: Vec<(&str, &dyn ToSql)> = Vec::new();
        if let Some(fts) = &fts {
            from.push_str(ARTIST_TEXT_FILTER);
            params.push((":fts", fts));
        }
        let kp = key.as_deref().map(key_params);
        self.read(|c| {
            let total = count(c, &from, &params)?;
            let mut all = params.clone();
            let mut sql_from = from.clone();
            if let Some(kp) = &kp {
                sql_from.push_str(" AND (a.name_key, a.id) > (:k0, :k1)");
                for (i, v) in kp.iter().enumerate() {
                    all.push((KEY_NAMES[i], v.as_ref()));
                }
            }
            let fetch = limit + 1;
            all.push((":limit", &fetch));
            let sql = format!(
                "SELECT a.id, a.name, \
                 (SELECT count(*) FROM project_artists pa JOIN projects p ON p.id = pa.project_id \
                  WHERE pa.artist_id = a.id AND p.hidden = 0), a.name_key \
                 {sql_from} ORDER BY a.name_key, a.id LIMIT :limit"
            );
            let mut rows = c
                .prepare_cached(&sql)
                .db()?
                .query_map(all.as_slice(), |r| {
                    Ok((
                        ArtistSummary {
                            id: r.get(0)?,
                            name: r.get(1)?,
                            project_count: r.get(2)?,
                        },
                        r.get::<_, String>(3)?,
                    ))
                })
                .db()?
                .collect::<rusqlite::Result<Vec<_>>>()
                .db()?;
            let more = rows.len() > limit as usize;
            rows.truncate(limit as usize);
            let next = if more {
                rows.last().map(|(a, name_key)| {
                    Cursor::encode(
                        &listing,
                        vec![Value::from(name_key.clone()), Value::from(a.id.clone())],
                    )
                })
            } else {
                None
            };
            Ok(KeysetPage {
                items: rows.into_iter().map(|(a, _)| a).collect(),
                next,
                total,
            })
        })
    }

    /// Whether `cid` is the root of any artifact, display or thumbnail URI of a visible project
    /// or its tokens. `scheme` is `ipfs` (any CID text form; compared canonically as CIDv1
    /// base32) or `onchfs` (64 hex digits). One primary-key lookup.
    pub fn content_root_known(&self, scheme: &str, cid: &str) -> Result<bool, StoreError> {
        let canonical = canonical_content_root(scheme, cid)?;
        self.read(|c| {
            c.prepare_cached(
                "SELECT EXISTS (SELECT 1 FROM content_roots r JOIN projects p \
                 ON p.id = r.project_id AND p.hidden = 0 WHERE r.scheme = ?1 AND r.cid = ?2)",
            )
            .db()?
            .query_row([scheme, canonical.as_str()], |r| r.get(0))
            .db()
        })
    }

    /// The snapshot row, rewritten by every import and sync pass, and the hidden project ids.
    /// Roots are removed only by imports, which also rewrite the snapshot. Missing snapshot:
    /// unavailable.
    pub fn content_root_fingerprint(&self) -> Result<(String, String), StoreError> {
        self.read(|c| {
            c.query_row(
                "SELECT quote(as_of) || quote(owner_source) || quote(catalogue_source) \
                 || quote(fixture_sha256) || quote(imported_at), \
                 coalesce((SELECT group_concat(id) FROM \
                    (SELECT id FROM projects WHERE hidden = 1 ORDER BY id)), '') \
                 FROM snapshot WHERE id = 1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()
            .db()?
            .ok_or_else(|| StoreError::Unavailable("Snapshot not imported yet".into()))
        })
    }

    /// Calls `f` with the scheme and canonical CID of every root of a visible project, stopping
    /// at its first error.
    pub fn each_visible_content_root(
        &self,
        mut f: impl FnMut(&str, &str) -> Result<(), StoreError>,
    ) -> Result<(), StoreError> {
        self.read(|c| {
            let mut st = c
                .prepare(
                    "SELECT r.scheme, r.cid FROM content_roots r JOIN projects p \
                     ON p.id = r.project_id AND p.hidden = 0",
                )
                .db()?;
            let mut rows = st.query([]).db()?;
            let text = |e: rusqlite::types::FromSqlError| StoreError::Internal(e.to_string());
            while let Some(row) = rows.next().db()? {
                let scheme = row.get_ref(0).db()?.as_str().map_err(text)?;
                let cid = row.get_ref(1).db()?.as_str().map_err(text)?;
                f(scheme, cid)?;
            }
            Ok(())
        })
    }

    pub fn snapshot(&self) -> Result<Snapshot, StoreError> {
        self.read(|c| {
            c.prepare_cached(
                "SELECT as_of, owner_source, catalogue_source, fixture_sha256, imported_at \
                 FROM snapshot WHERE id = 1",
            )
            .db()?
            .query_row([], |r| {
                Ok(Snapshot {
                    as_of: r.get(0)?,
                    owner_source: r.get(1)?,
                    catalogue_source: r.get(2)?,
                    fixture_sha256: r.get(3)?,
                    imported_at: r.get(4)?,
                })
            })
            .optional()
            .db()?
            .ok_or_else(|| not_found("snapshot", "current"))
        })
    }
}

struct ProjectRow {
    title: String,
    description: Option<String>,
    preview: Option<String>,
    contract: Option<String>,
    issuer: Option<String>,
    artifact_uri: Option<String>,
    editions: Option<u32>,
    first_mint: Option<String>,
    last_mint: Option<String>,
    index_state: String,
    slug: Option<String>,
    supply: Option<u32>,
    thumbnail_uri: Option<String>,
    display_uri: Option<String>,
    source: String,
}

/// Canonical text of an IPFS CID: CIDv1, lowercase base32 (what the content layer serves).
/// `None` unless the text is exactly one canonical binary CID in some multibase (or CIDv0).
pub(crate) fn canonical_ipfs(text: &str) -> Option<String> {
    if text.is_empty() || text.len() > 256 || !text.bytes().all(|b| b.is_ascii_alphanumeric()) {
        return None;
    }
    let bytes = if text.starts_with("Qm") {
        cid::multibase::Base::Base58Btc.decode(text).ok()?
    } else {
        cid::multibase::decode(text).ok()?.1
    };
    let parsed = cid::Cid::try_from(bytes.as_slice()).ok()?;
    if parsed.to_bytes() != bytes {
        return None;
    }
    Some(cid::Cid::new_v1(parsed.codec(), *parsed.hash()).to_string())
}

/// Canonical ONCHFS CID: 64 lowercase hex digits.
pub(crate) fn canonical_onchfs(text: &str) -> Option<String> {
    (text.len() == 64 && text.bytes().all(|b| b.is_ascii_hexdigit()))
        .then(|| text.to_ascii_lowercase())
}

/// `(scheme, canonical root CID)` of an `ipfs://` or `onchfs://` URI, parsed like the
/// content layer: optional `ipfs/` prefix, ONCHFS mainnet authorities, query and fragment
/// ignored. `None` for anything else.
pub(crate) fn content_root(uri: &str) -> Option<(&'static str, String)> {
    let (scheme, rest) = uri.split_once("://")?;
    let rest = rest.split(['?', '#']).next().unwrap_or_default();
    match scheme {
        "ipfs" => {
            let rest = rest.strip_prefix("ipfs/").unwrap_or(rest);
            let root = rest.split('/').next()?;
            Some(("ipfs", canonical_ipfs(root)?))
        }
        "onchfs" => {
            let (authority, path) = rest.split_once('/').unwrap_or((rest, ""));
            let rest = match authority {
                "tezos"
                | "tezos:NetXdQprcVkpaWU"
                | "tezos:NetXdQprcVkpaWU:KT1Ae7dT1gsLw2tRnUMXSCmEyF74KVkM6LUo" => path,
                a if a.contains(':') => return None,
                _ => rest,
            };
            let root = rest.split('/').next()?;
            Some(("onchfs", canonical_onchfs(root)?))
        }
        _ => None,
    }
}

/// The stored form of a content root: `ipfs` CIDs as CIDv1 base32, `onchfs` as lowercase hex.
pub fn canonical_content_root(scheme: &str, cid: &str) -> Result<String, StoreError> {
    match scheme {
        "ipfs" => canonical_ipfs(cid),
        "onchfs" => canonical_onchfs(cid),
        _ => {
            return Err(StoreError::BadRequest(
                "scheme must be ipfs or onchfs".into(),
            ));
        }
    }
    .ok_or_else(|| StoreError::BadRequest(format!("invalid {scheme} CID")))
}

/// Records the roots of `uris` as referenced by `project_id`.
pub(crate) fn add_content_roots(
    c: &Connection,
    project_id: u64,
    uris: &[Option<&str>],
) -> Result<(), StoreError> {
    let mut st = c
        .prepare_cached(
            "INSERT OR IGNORE INTO content_roots (scheme, cid, project_id) VALUES (?1, ?2, ?3)",
        )
        .db()?;
    for (scheme, cid) in uris.iter().flatten().filter_map(|u| content_root(u)) {
        st.execute(rusqlite::params![scheme, cid, project_id as i64])
            .db()?;
    }
    Ok(())
}

/// Fills `content_roots` from stored tokens and projects (v3 migration, `upgrade`).
/// Idempotent.
pub(crate) fn backfill_content_roots(c: &Connection) -> Result<(), StoreError> {
    for sql in [
        "SELECT project_id, artifact_uri, display_uri, thumbnail_uri, NULL FROM tokens",
        "SELECT id, artifact_uri, display_uri, thumbnail_uri, generative_uri FROM projects",
    ] {
        let mut st = c.prepare(sql).db()?;
        let mut rows = st.query([]).db()?;
        while let Some(r) = rows.next().db()? {
            let id: i64 = r.get(0).db()?;
            let uris: [Option<String>; 4] = [
                r.get(1).db()?,
                r.get(2).db()?,
                r.get(3).db()?,
                r.get(4).db()?,
            ];
            let refs: Vec<Option<&str>> = uris.iter().map(Option::as_deref).collect();
            add_content_roots(c, id as u64, &refs)?;
        }
    }
    Ok(())
}

pub(crate) fn current_fixture_sha256(c: &Connection) -> Result<Option<String>, StoreError> {
    c.query_row(
        "SELECT fixture_sha256 FROM snapshot WHERE id = 1",
        [],
        |r| r.get(0),
    )
    .optional()
    .db()
}

fn user_version(c: &Connection) -> Result<i64, StoreError> {
    c.pragma_query_value(None, "user_version", |r| r.get(0))
        .db()
}

fn index_state(s: &str) -> Result<IndexState, StoreError> {
    IndexState::parse(s).ok_or_else(|| StoreError::Internal(format!("unknown index_state {s:?}")))
}

fn source(s: &str) -> Result<Source, StoreError> {
    Source::parse(s).ok_or_else(|| StoreError::Internal(format!("unknown source {s:?}")))
}

fn is_known_address(c: &Connection, address: &str) -> Result<bool, StoreError> {
    c.prepare_cached("SELECT 1 FROM known_addresses WHERE address = ?1")
        .db()?
        .exists([address])
        .db()
}

/// `(held, listed)` visible tokens credited to a collector, from `holder_counts` (kept by
/// triggers on every holder change): one primary-key read.
fn holder_totals(c: &Connection, address: &str) -> Result<(u64, u64), StoreError> {
    Ok(
        c.prepare_cached("SELECT held, listed FROM holder_counts WHERE holder_address = ?1")
            .db()?
            .query_row([address], |r| Ok((get_u64(r, 0)?, get_u64(r, 1)?)))
            .optional()
            .db()?
            .unwrap_or((0, 0)),
    )
}

/// A project's token count: `editions` (kept by import and sync), counted only when unset.
fn project_token_total(c: &Connection, key: i64) -> Result<u64, StoreError> {
    let editions: Option<i64> = c
        .prepare_cached("SELECT editions FROM projects WHERE id = ?1")
        .db()?
        .query_row([key], |r| r.get(0))
        .optional()
        .db()?
        .flatten();
    match editions {
        Some(n) => Ok(n as u64),
        None => c
            .prepare_cached("SELECT count(*) FROM tokens WHERE project_id = ?1")
            .db()?
            .query_row([key], |r| get_u64(r, 0))
            .db(),
    }
}

fn require_project(c: &Connection, key: i64, id: u64) -> Result<(), StoreError> {
    let found = c
        .prepare_cached("SELECT 1 FROM projects WHERE id = ?1 AND hidden = 0")
        .db()?
        .exists([key])
        .db()?;
    if found {
        Ok(())
    } else {
        Err(not_found("project", id))
    }
}

/// An artist is visible unless every one of their projects is hidden.
const ARTIST_VISIBLE: &str = "(NOT EXISTS (SELECT 1 FROM project_artists pa \
     WHERE pa.artist_id = a.id) OR EXISTS (SELECT 1 FROM project_artists pa \
     JOIN projects p ON p.id = pa.project_id WHERE pa.artist_id = a.id AND p.hidden = 0))";

/// The visible artist's canonical id: `id` itself, or the address of the curated identity
/// whose slug is `id` (e.g. "emptyname").
fn require_artist(c: &Connection, id: &str) -> Result<String, StoreError> {
    let direct = c
        .prepare_cached(&format!(
            "SELECT 1 FROM artists a WHERE a.id = ?1 AND {ARTIST_VISIBLE}"
        ))
        .db()?
        .exists([id])
        .db()?;
    if direct {
        return Ok(id.to_owned());
    }
    c.prepare_cached(&format!(
        "SELECT a.id FROM identities i JOIN artists a ON a.id = i.address \
         WHERE i.slug = ?1 AND {ARTIST_VISIBLE}"
    ))
    .db()?
    .query_row([id], |r| r.get(0))
    .optional()
    .db()?
    .ok_or_else(|| not_found("artist", id))
}

fn artist_by_id(c: &Connection, id: &str) -> Result<Artist, StoreError> {
    let id = require_artist(c, id)?;
    let id = id.as_str();
    let (name, website, address) = c
        .prepare_cached(&format!(
            "SELECT name, website, address FROM artists a WHERE a.id = ?1 AND {ARTIST_VISIBLE}"
        ))
        .db()?
        .query_row([id], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, Option<String>>(1)?,
                r.get::<_, Option<String>>(2)?,
            ))
        })
        .optional()
        .db()?
        .ok_or_else(|| not_found("artist", id))?;
    let aliases = c
        .prepare_cached("SELECT alias FROM artist_aliases WHERE artist_id = ?1 ORDER BY position")
        .db()?
        .query_map([id], |r| r.get(0))
        .db()?
        .collect::<rusqlite::Result<Vec<String>>>()
        .db()?;
    Ok(Artist {
        id: id.to_owned(),
        name,
        website,
        aliases,
        address,
    })
}

fn count(c: &Connection, from: &str, params: &[(&str, &dyn ToSql)]) -> Result<u64, StoreError> {
    c.prepare_cached(&format!("SELECT count(*) {from}"))
        .db()?
        .query_row(params, |r| get_u64(r, 0))
        .db()
}

fn project_artists(c: &Connection, project: i64) -> Result<Vec<ArtistRef>, StoreError> {
    c.prepare_cached(
        "SELECT a.id, a.name, a.address FROM project_artists pa \
         JOIN artists a ON a.id = pa.artist_id WHERE pa.project_id = ?1 ORDER BY pa.position",
    )
    .db()?
    .query_map([project], |r| {
        Ok(ArtistRef {
            id: r.get(0)?,
            name: r.get(1)?,
            address: r.get(2)?,
        })
    })
    .db()?
    .collect::<rusqlite::Result<Vec<_>>>()
    .db()
}

fn summaries_page(
    c: &Connection,
    from: &str,
    order: &str,
    params: &[(&str, &dyn ToSql)],
    page: PageRequest,
) -> Result<Page<ProjectSummary>, StoreError> {
    let total = count(c, from, params)?;
    let sql = format!("SELECT {SUMMARY_COLS} {from} ORDER BY {order} LIMIT :limit OFFSET :offset");
    let mut all = params.to_vec();
    all.push((":limit", &page.limit));
    all.push((":offset", &page.offset));
    let rows = summary_rows(c, &sql, &all, 0)?;
    Ok(Page {
        items: finish_summaries(c, rows.into_iter().map(|(raw, _)| raw))?,
        total,
        offset: page.offset,
        limit: page.limit,
    })
}

/// A summary row before its artists are read: [`SUMMARY_COLS`] in order.
type SummaryRaw = (
    u64,
    String,
    Option<String>,
    Option<u32>,
    String,
    Option<String>,
    Option<u32>,
    Option<String>,
    Option<String>,
    Option<String>,
);

/// Rows of `sql`, which selects [`SUMMARY_COLS`] and then `arity` sort-key columns.
fn summary_rows(
    c: &Connection,
    sql: &str,
    params: &[(&str, &dyn ToSql)],
    arity: usize,
) -> Result<Vec<(SummaryRaw, Vec<Value>)>, StoreError> {
    c.prepare_cached(sql)
        .db()?
        .query_map(params, |r| {
            let raw: SummaryRaw = (
                get_u64(r, 0)?,
                r.get(1)?,
                r.get(2)?,
                r.get(3)?,
                r.get(4)?,
                r.get(5)?,
                r.get(6)?,
                r.get(7)?,
                r.get(8)?,
                r.get(9)?,
            );
            let key = (0..arity)
                .map(|i| sql_value(r, 10 + i))
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok((raw, key))
        })
        .db()?
        .collect::<rusqlite::Result<Vec<_>>>()
        .db()
}

/// Summaries with their credited artists.
fn finish_summaries(
    c: &Connection,
    rows: impl IntoIterator<Item = SummaryRaw>,
) -> Result<Vec<ProjectSummary>, StoreError> {
    rows.into_iter()
        .map(
            |(
                id,
                title,
                preview,
                editions,
                state,
                slug,
                supply,
                thumbnail_uri,
                display_uri,
                published,
            )| {
                let key = sql_id(id)
                    .ok_or_else(|| StoreError::Internal("project id out of range".into()))?;
                Ok(ProjectSummary {
                    id,
                    title,
                    artists: project_artists(c, key)?,
                    preview,
                    editions,
                    index_state: index_state(&state)?,
                    slug,
                    supply,
                    thumbnail_uri,
                    display_uri,
                    published,
                })
            },
        )
        .collect()
}

struct TokenRow {
    contract: String,
    token_id: u64,
    project_id: u64,
    iteration: u32,
    seed: String,
    minter: String,
    minted: String,
    artifact_uri: String,
    preview: Option<String>,
    owner_address: Option<String>,
    owner_alias: Option<String>,
    owner_as_of: Option<String>,
    owner_source: Option<String>,
    attributes: Option<String>,
    display_uri: Option<String>,
    thumbnail_uri: Option<String>,
    params: Option<String>,
    source: String,
    verify_state: Option<String>,
    listed_by: Option<String>,
    listed_by_alias: Option<String>,
    known_name: Option<String>,
    known_kind: Option<String>,
}

fn token_row(r: &Row<'_>) -> rusqlite::Result<TokenRow> {
    Ok(TokenRow {
        contract: r.get(0)?,
        token_id: get_u64(r, 1)?,
        project_id: get_u64(r, 2)?,
        iteration: r.get(3)?,
        seed: r.get(4)?,
        minter: r.get(5)?,
        minted: r.get(6)?,
        artifact_uri: r.get(7)?,
        preview: r.get(8)?,
        owner_address: r.get(9)?,
        owner_alias: r.get(10)?,
        owner_as_of: r.get(11)?,
        owner_source: r.get(12)?,
        attributes: r.get(13)?,
        display_uri: r.get(14)?,
        thumbnail_uri: r.get(15)?,
        params: r.get(16)?,
        source: r.get(17)?,
        verify_state: r.get(18)?,
        listed_by: r.get(19)?,
        listed_by_alias: r.get(20)?,
        known_name: r.get(21)?,
        known_kind: r.get(22)?,
    })
}

/// Chain attributes are stored compactly as a JSON array of `[name, value]` pairs.
pub(crate) fn parse_attributes(json: &str) -> Result<Vec<(String, String)>, StoreError> {
    serde_json::from_str(json)
        .map_err(|e| StoreError::Internal(format!("stored attributes are not [name, value]: {e}")))
}

fn finish_token(c: &Connection, row: TokenRow) -> Result<Token, StoreError> {
    let token_key =
        sql_id(row.token_id).ok_or_else(|| StoreError::Internal("token id out of range".into()))?;
    let features = match &row.attributes {
        Some(json) => parse_attributes(json)?,
        None => c
            .prepare_cached(
                "SELECT name, value FROM token_features WHERE contract = ?1 AND token_id = ?2 \
                 ORDER BY position",
            )
            .db()?
            .query_map((&row.contract, token_key), |r| Ok((r.get(0)?, r.get(1)?)))
            .db()?
            .collect::<rusqlite::Result<Vec<(String, String)>>>()
            .db()?,
    };
    let owner = match (row.owner_address, row.owner_as_of, row.owner_source) {
        (Some(address), Some(as_of), Some(source)) => Some(OwnerSnapshot {
            owner: Owner {
                address,
                alias: row.owner_alias,
            },
            as_of,
            source,
            listed_by: row.listed_by.map(|address| Owner {
                address,
                alias: row.listed_by_alias,
            }),
            escrow: row
                .known_name
                .filter(|_| row.known_kind.as_deref() == Some("escrow")),
            burned: row.known_kind.as_deref() == Some("burn"),
        }),
        (None, None, None) if row.owner_alias.is_none() => None,
        _ => return Err(StoreError::Internal("inconsistent owner columns".into())),
    };
    let source = source(&row.source)?;
    // The comparison result, not the data source: only a recorded match is a chain match.
    let verification = match (source, row.verify_state.as_deref()) {
        (Source::Tzkt, Some("match")) => Verification::ChainMatch,
        (Source::Tzkt, Some("mismatch")) => Verification::Mismatch,
        _ => Verification::Unverified,
    };
    Ok(Token {
        contract: row.contract,
        token_id: row.token_id,
        project_id: row.project_id,
        iteration: row.iteration,
        seed: row.seed,
        minter: row.minter,
        minted: row.minted,
        artifact_uri: row.artifact_uri,
        preview: row.preview,
        features,
        owner,
        display_uri: row.display_uri,
        thumbnail_uri: row.thumbnail_uri,
        params: row.params,
        verification,
        source,
    })
}

fn tokens_page(
    c: &Connection,
    from: &str,
    params: &[(&str, &dyn ToSql)],
    page: PageRequest,
) -> Result<Page<Token>, StoreError> {
    let total = count(c, from, params)?;
    tokens_page_counted(c, from, params, page, total)
}

/// Like [`tokens_page`] with a total already known from maintained counts.
fn tokens_page_counted(
    c: &Connection,
    from: &str,
    params: &[(&str, &dyn ToSql)],
    page: PageRequest,
    total: u64,
) -> Result<Page<Token>, StoreError> {
    let sql =
        format!("SELECT {TOKEN_COLS} {from} ORDER BY {TOKEN_ORDER} LIMIT :limit OFFSET :offset");
    let mut all = params.to_vec();
    all.push((":limit", &page.limit));
    all.push((":offset", &page.offset));
    let rows = c
        .prepare_cached(&sql)
        .db()?
        .query_map(all.as_slice(), token_row)
        .db()?
        .collect::<rusqlite::Result<Vec<_>>>()
        .db()?;
    let items = rows
        .into_iter()
        .map(|row| finish_token(c, row))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Page {
        items,
        total,
        offset: page.offset,
        limit: page.limit,
    })
}

const KEY_NAMES: [&str; 4] = [":k0", ":k1", ":k2", ":k3"];

fn opt_part(v: Option<&str>) -> String {
    v.map_or_else(|| "-".to_owned(), |t| format!("+{t}"))
}

fn empty_keyset<T>() -> KeysetPage<T> {
    KeysetPage {
        items: Vec::new(),
        next: None,
        total: 0,
    }
}

/// Up to `limit` summaries after `key`, plus the key of the last one when more follow.
#[allow(clippy::too_many_arguments)] // one call shape shared by two listings
fn summaries_keyset(
    c: &Connection,
    from: &str,
    params: &[(&str, &dyn ToSql)],
    key_cols: &str,
    keyset: &str,
    order: &str,
    key: Option<&[Value]>,
    limit: u32,
) -> Result<(Vec<ProjectSummary>, Option<Vec<Value>>), StoreError> {
    let arity = key_cols.split(',').count();
    let kp = key.map(key_params);
    let mut all = params.to_vec();
    let mut sql_from = from.to_owned();
    if let Some(kp) = &kp {
        sql_from.push_str(" AND ");
        sql_from.push_str(keyset);
        for (i, v) in kp.iter().enumerate() {
            all.push((KEY_NAMES[i], v.as_ref()));
        }
    }
    let fetch = limit + 1;
    all.push((":limit", &fetch));
    let sql = format!("SELECT {SUMMARY_COLS}, {key_cols} {sql_from} ORDER BY {order} LIMIT :limit");
    let mut rows = summary_rows(c, &sql, &all, arity)?;
    let more = rows.len() > limit as usize;
    rows.truncate(limit as usize);
    let next = if more {
        rows.last().map(|(_, k)| k.clone())
    } else {
        None
    };
    let items = finish_summaries(c, rows.into_iter().map(|(raw, _)| raw))?;
    Ok((items, next))
}

/// Items and the sort key of the last one when more follow.
type TokensKeyset = (Vec<Token>, Option<Vec<Value>>);

/// Up to `limit` tokens after `key`, and the key of the last one when more follow. Totals
/// come from maintained counts (see [`holder_totals`], [`project_token_total`]).
#[allow(clippy::too_many_arguments)] // one call shape shared by two listings
fn tokens_keyset(
    c: &Connection,
    from: &str,
    params: &[(&str, &dyn ToSql)],
    keyset: &str,
    order: &str,
    key: Option<&[Value]>,
    limit: u32,
    key_of: impl Fn(&Token) -> Vec<Value>,
) -> Result<TokensKeyset, StoreError> {
    let kp = key.map(key_params);
    let mut all = params.to_vec();
    let mut sql_from = from.to_owned();
    if let Some(kp) = &kp {
        sql_from.push_str(" AND ");
        sql_from.push_str(keyset);
        for (i, v) in kp.iter().enumerate() {
            all.push((KEY_NAMES[i], v.as_ref()));
        }
    }
    let fetch = limit + 1;
    all.push((":limit", &fetch));
    let sql = format!("SELECT {TOKEN_COLS} {sql_from} ORDER BY {order} LIMIT :limit");
    let mut rows = c
        .prepare_cached(&sql)
        .db()?
        .query_map(all.as_slice(), token_row)
        .db()?
        .collect::<rusqlite::Result<Vec<_>>>()
        .db()?;
    let more = rows.len() > limit as usize;
    rows.truncate(limit as usize);
    let items = rows
        .into_iter()
        .map(|row| finish_token(c, row))
        .collect::<Result<Vec<_>, _>>()?;
    let next = if more {
        items.last().map(&key_of)
    } else {
        None
    };
    Ok((items, next))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::ffi;

    fn failure(code: i32) -> rusqlite::Error {
        rusqlite::Error::SqliteFailure(ffi::Error::new(code), None)
    }

    #[test]
    fn lock_and_io_errors_are_unavailable() {
        for code in [
            ffi::SQLITE_BUSY,
            ffi::SQLITE_LOCKED,
            ffi::SQLITE_IOERR,
            ffi::SQLITE_CANTOPEN,
            ffi::SQLITE_FULL,
            ffi::SQLITE_PROTOCOL,
            ffi::SQLITE_NOMEM,
            ffi::SQLITE_INTERRUPT,
        ] {
            assert!(
                matches!(classify(&failure(code)), StoreError::Unavailable(_)),
                "{code}"
            );
        }
    }

    #[test]
    fn schema_and_decode_errors_are_internal() {
        for code in [
            ffi::SQLITE_CORRUPT,
            ffi::SQLITE_NOTADB,
            ffi::SQLITE_ERROR,
            ffi::SQLITE_CONSTRAINT,
        ] {
            assert!(
                matches!(classify(&failure(code)), StoreError::Internal(_)),
                "{code}"
            );
        }
        assert!(matches!(
            classify(&rusqlite::Error::QueryReturnedNoRows),
            StoreError::Internal(_)
        ));
        assert!(matches!(
            classify(&rusqlite::Error::InvalidColumnType(
                0,
                "x".into(),
                rusqlite::types::Type::Null
            )),
            StoreError::Internal(_)
        ));
    }

    #[test]
    fn fts_query_quotes_terms_and_neutralises_operators() {
        assert_eq!(
            fts_query("  zancan ").unwrap().as_deref(),
            Some("\"zancan\"*")
        );
        assert_eq!(
            fts_query("a OR b").unwrap().as_deref(),
            Some("\"a\" \"OR\"* \"b\""),
            "one-character terms are exact"
        );
        assert_eq!(fts_query("é").unwrap().as_deref(), Some("\"é\""));
        assert_eq!(
            fts_query("1 2 3 4 5 6 7 8").unwrap().as_deref(),
            Some("\"1\" \"2\" \"3\" \"4\" \"5\" \"6\" \"7\" \"8\"")
        );
        match fts_query("1 2 3 4 5 6 7 8 9") {
            Err(StoreError::BadRequest(m)) => assert_eq!(m, "too many search terms"),
            other => panic!("{other:?}"),
        }
        // Separators alone are not terms.
        assert!(
            fts_query("a - b - c - d - e - f - g - h")
                .unwrap()
                .is_some()
        );
        assert_eq!(
            fts_query("garden\"").unwrap().as_deref(),
            Some("\"garden\"*")
        );
        assert_eq!(
            fts_query("(kinder)").unwrap().as_deref(),
            Some("\"kinder\"*")
        );
        for empty in ["", "   ", "*", "-", "^", ":", "§§", "\"\""] {
            assert_eq!(fts_query(empty).unwrap(), None, "{empty:?}");
        }
        assert!(fts_query(&"a".repeat(200)).unwrap().is_some());
        assert!(matches!(
            fts_query(&"a".repeat(201)),
            Err(StoreError::BadRequest(_))
        ));
        // Length counts characters, not bytes, after trimming.
        assert!(
            fts_query(&format!("  {}  ", "é".repeat(200)))
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn status_queries_never_scan_tokens() {
        let store = Store::open_in_memory().unwrap();
        for sql in [
            STATUS_FIXTURE,
            STATUS_MISMATCH,
            STATUS_PENDING,
            STATUS_UNCATALOGUED,
            STATUS_PER_CONTRACT,
        ] {
            let mut st = store
                .conn
                .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
                .unwrap();
            let plan: Vec<String> = st
                .query_map([], |r| r.get::<_, String>(3))
                .unwrap()
                .collect::<rusqlite::Result<_>>()
                .unwrap();
            for step in &plan {
                let scans_tokens = step.starts_with("SCAN t ")
                    || step == "SCAN t"
                    || step.starts_with("SCAN tokens");
                assert!(
                    !scans_tokens || step.contains("tokens_fixture"),
                    "{sql}\n{plan:?}"
                );
            }
            // The first step reads the small side.
            let first = plan.first().unwrap();
            assert!(
                first.contains(" i")
                    || first.contains(" ct ")
                    || first.contains("tokens_fixture")
                    || first.contains("projects"),
                "{sql}\n{plan:?}"
            );
        }
    }

    fn plan(store: &Store, sql: &str) -> Vec<String> {
        let mut st = store
            .conn
            .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
            .unwrap();
        // Parameters stay unbound (NULL): only the plan matters.
        let mut rows = st.raw_query();
        let mut plan = Vec::new();
        while let Some(r) = rows.next().unwrap() {
            plan.push(r.get::<_, String>(3).unwrap());
        }
        plan
    }

    #[test]
    fn feature_shares_read_only_feature_counts() {
        let store = Store::open_in_memory().unwrap();
        let plan = plan(&store, FEATURE_SHARES);
        assert!(
            plan.iter().any(|s| s.contains("feature_counts")),
            "{plan:?}"
        );
        for step in &plan {
            assert!(
                !step.contains("tokens")
                    && !step.contains("token_features")
                    && !step.starts_with("SCAN feature_counts"),
                "{plan:?}"
            );
        }
    }

    #[test]
    fn artist_filter_is_driven_by_the_artist_index() {
        let store = Store::open_in_memory().unwrap();
        let from = format!("FROM projects p WHERE p.hidden = 0{ARTIST_FILTER}");
        let mut statements = vec![format!("SELECT count(*) {from}")];
        for (sort, keyset) in [
            (ProjectSort::TitleAsc, "(p.title_key, p.id) > (:k0, :k1)"),
            (ProjectSort::ArtistAsc, "(p.artist_key, p.id) > (:k0, :k1)"),
            (ProjectSort::IdAsc, "p.id > :k0"),
            (ProjectSort::IdDesc, "p.id < :k0"),
        ] {
            let order = sort.order_by();
            statements.push(format!(
                "SELECT {SUMMARY_COLS} {from} ORDER BY {order} LIMIT :limit OFFSET :offset"
            ));
            statements.push(format!(
                "SELECT {SUMMARY_COLS}, {order} {from} ORDER BY {order} LIMIT :limit"
            ));
            statements.push(format!(
                "SELECT {SUMMARY_COLS}, {order} {from} AND {keyset} ORDER BY {order} LIMIT :limit"
            ));
        }
        for sql in &statements {
            let plan = plan(&store, sql);
            assert!(
                plan.iter().any(|s| s.contains("project_artists_artist")),
                "{sql}\n{plan:?}"
            );
            assert!(
                !plan.iter().any(|s| s.starts_with("SCAN")),
                "{sql}\n{plan:?}"
            );
        }
    }

    /// No step scans `tokens` or `feature_counts`, and none sorts.
    fn assert_indexed(sql: &str, plan: &[String]) {
        for step in plan {
            let scans = step.starts_with("SCAN t ")
                || step == "SCAN t"
                || step.starts_with("SCAN tokens")
                || step.starts_with("SCAN feature_counts");
            assert!(!scans, "{sql}\n{plan:?}");
            assert!(!step.contains("TEMP B-TREE"), "{sql}\n{plan:?}");
        }
    }

    #[test]
    fn holdings_search_is_driven_by_the_holder_index() {
        let store = Store::open_in_memory().unwrap();
        let keyset = " AND (t.project_id, t.iteration, t.contract, t.token_id) > \
                      (:k0, :k1, :k2, :k3)";
        for (text, trait_filter) in [
            (HoldingsText::Words, false),
            (HoldingsText::None, true),
            (HoldingsText::Words, true),
            (HoldingsText::Number, false),
            (HoldingsText::WordsAndNumber, true),
        ] {
            let first = holdings_where(text, trait_filter, false);
            let after = holdings_where(text, trait_filter, true);
            for sql in [
                format!("SELECT count(*) {TOKEN_COUNT_FROM}{first}"),
                format!(
                    "SELECT {TOKEN_COLS} {TOKEN_FROM}{first} ORDER BY {TOKEN_ORDER} LIMIT :limit"
                ),
                format!(
                    "SELECT {TOKEN_COLS} {TOKEN_FROM}{after}{keyset} ORDER BY {TOKEN_ORDER} \
                     LIMIT :limit"
                ),
            ] {
                let plan = plan(&store, &sql);
                assert!(
                    plan.iter().any(|s| s.contains("tokens_holder")),
                    "{sql}\n{plan:?}"
                );
                assert_indexed(&sql, &plan);
                if text != HoldingsText::None {
                    assert!(plan.iter().any(|s| s.contains("project_fts")), "{plan:?}");
                }
                if trait_filter {
                    assert!(
                        plan.iter().any(|s| s.contains("feature_counts_trait")),
                        "{sql}\n{plan:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn edition_search_stays_inside_the_project() {
        let store = Store::open_in_memory().unwrap();
        let pre = plan(&store, PROJECT_HAS_TRAIT);
        assert!(
            pre.iter()
                .all(|s| s.starts_with("SEARCH feature_counts USING PRIMARY KEY")),
            "{pre:?}"
        );
        for filter in [
            " AND (t.iteration = :n OR t.token_id = :n)".to_owned(),
            " AND (t.holder_address = :who OR t.owner_address = :who)".to_owned(),
            TOKEN_HAS_TRAIT.to_owned(),
        ] {
            let filters = format!(" WHERE t.project_id = :project{filter}");
            for sql in [
                format!("SELECT count(*) {TOKEN_COUNT_FROM}{filters}"),
                format!(
                    "SELECT {TOKEN_COLS} {TOKEN_FROM}{filters} AND (t.iteration, t.contract, \
                     t.token_id) > (:k0, :k1, :k2) ORDER BY t.iteration, t.contract, t.token_id \
                     LIMIT :limit"
                ),
            ] {
                let plan = plan(&store, &sql);
                assert_indexed(&sql, &plan);
                assert!(
                    plan.iter()
                        .any(|s| s.starts_with("SEARCH t ") && s.contains("project_id=?")),
                    "{sql}\n{plan:?}"
                );
            }
        }
    }

    #[test]
    fn v5_database_gains_profile_tables_and_name_keys() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
        for v in [SCHEMA_V1, SCHEMA_V2, SCHEMA_V3, SCHEMA_V4, SCHEMA_V5] {
            conn.execute_batch(v).unwrap();
        }
        conn.execute_batch(
            "PRAGMA user_version = 5;
             INSERT INTO users (address, name) VALUES
               ('tz1N64TWNCZR3yJZmmzBE4DRidN1tCaZCw8Q', ' Ünïque '),
               ('tz1PkLH6efZifS8NtKiZBWFWgDoHDmDwN9f1', 'Twin'),
               ('tz1Kq8NtEjSXkpswLmgod1tPFPVH2TZc8dwM', 'TWIN');
             INSERT INTO identities (address, slug, name, aliases, provenance) VALUES
               ('tz1gL9H53BzjDqmMi2JkKALeCzT7UAoopzEL', 'emptyname', 'EmptyName', '[]', 'test');",
        )
        .unwrap();
        let mut store = Store { conn };
        store.migrate().unwrap();
        assert_eq!(user_version(&store.conn).unwrap(), SCHEMA_VERSION);
        let keys: Vec<String> = store
            .conn
            .prepare("SELECT name_key FROM users ORDER BY address")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(keys, vec!["twin", "ünïque", "twin"]);
        let p = store
            .profile("tz1N64TWNCZR3yJZmmzBE4DRidN1tCaZCw8Q")
            .unwrap();
        assert_eq!(p.name.as_deref(), Some("Ünïque"));
        assert_eq!(
            store.profile_by_name("üNÏQUE").unwrap().address,
            "tz1N64TWNCZR3yJZmmzBE4DRidN1tCaZCw8Q",
            "non-ASCII names fold case too"
        );
        // Shared "twin", nobody credited or seen: the lower address is plain.
        for (address, shown) in [
            ("tz1Kq8NtEjSXkpswLmgod1tPFPVH2TZc8dwM", "TWIN"),
            ("tz1PkLH6efZifS8NtKiZBWFWgDoHDmDwN9f1", "Twin-2"),
        ] {
            assert_eq!(store.profile(address).unwrap().name.as_deref(), Some(shown));
        }
        assert_eq!(
            store.profile_by_name("emptyname").unwrap().address,
            "tz1gL9H53BzjDqmMi2JkKALeCzT7UAoopzEL"
        );
        // Migrating again is a no-op.
        store.migrate().unwrap();
    }

    #[test]
    fn v4_database_backfills_feature_counts() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
        for v in [SCHEMA_V1, SCHEMA_V2, SCHEMA_V3, SCHEMA_V4] {
            conn.execute_batch(v).unwrap();
        }
        conn.execute_batch(
            "PRAGMA user_version = 4;
             INSERT INTO projects (id, title, title_key, artist_key, index_state)
               VALUES (1, 'P', 'p', 'a', 'indexed');
             INSERT INTO tokens (contract, token_id, project_id, iteration, seed, minter, minted,
               artifact_uri, source, attributes) VALUES
               ('KT1EfsNuqwLAWDd3o4pvfUx1CAh5GMdTrRvr', 1, 1, 1, 'oo', 'tz1m', '2023-01-01',
                'ipfs://x', 'tzkt', '[[\"Palette\",\"Gold\"],[\"Wind\",\"0.1\"]]'),
               ('KT1EfsNuqwLAWDd3o4pvfUx1CAh5GMdTrRvr', 2, 1, 2, 'oo', 'tz1m', '2023-01-01',
                'ipfs://x', 'tzkt', '[[\"Palette\",\"Gold\"]]'),
               ('KT1EfsNuqwLAWDd3o4pvfUx1CAh5GMdTrRvr', 3, 1, 3, 'oo', 'tz1m', '2023-01-01',
                'ipfs://x', 'fixture', NULL);
             INSERT INTO token_features (contract, token_id, position, name, value) VALUES
               ('KT1EfsNuqwLAWDd3o4pvfUx1CAh5GMdTrRvr', 3, 0, 'Palette', 'Red');",
        )
        .unwrap();
        let mut store = Store { conn };
        store.migrate().unwrap();
        assert_eq!(user_version(&store.conn).unwrap(), SCHEMA_VERSION);
        let shares: Vec<(String, String, u32, u32)> = store
            .feature_shares(1)
            .unwrap()
            .into_iter()
            .map(|s| (s.name, s.value, s.count, s.total))
            .collect();
        let row = |n: &str, v: &str, c| (n.to_owned(), v.to_owned(), c, 3);
        assert_eq!(
            shares,
            vec![
                row("Palette", "Gold", 2),
                row("Palette", "Red", 1),
                row("Wind", "0.1", 1)
            ]
        );
        // Deleting a token through the cascade leaves exact counts.
        store
            .conn
            .execute_batch("DELETE FROM tokens WHERE token_id IN (1, 3)")
            .unwrap();
        let left: Vec<(String, String, i64)> = store
            .conn
            .prepare("SELECT name, value, count FROM feature_counts WHERE count > 0")
            .unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(left, vec![("Palette".into(), "Gold".into(), 1)]);
    }

    #[test]
    fn v2_database_migrates_to_v3_with_holders_from_owners() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(SCHEMA_V1).unwrap();
        conn.execute_batch(SCHEMA_V2).unwrap();
        conn.execute_batch(
            "PRAGMA user_version = 2;
             INSERT INTO projects (id, title, title_key, artist_key, index_state)
               VALUES (1, 'P', 'p', 'a', 'indexed');
             INSERT INTO tokens (contract, token_id, project_id, iteration, seed, minter, minted,
               artifact_uri, owner_address, owner_as_of, owner_source, source)
               VALUES ('KT1EfsNuqwLAWDd3o4pvfUx1CAh5GMdTrRvr', 7, 1, 1, 'oo', 'tz1m',
               '2023-01-01', 'ipfs://x', 'tz1N64TWNCZR3yJZmmzBE4DRidN1tCaZCw8Q', 'now', 'tzkt',
               'tzkt');",
        )
        .unwrap();
        let mut store = Store { conn };
        store.migrate().unwrap();
        assert_eq!(user_version(&store.conn).unwrap(), SCHEMA_VERSION);
        let holder: String = store
            .conn
            .query_row("SELECT holder_address FROM tokens", [], |r| r.get(0))
            .unwrap();
        assert_eq!(holder, "tz1N64TWNCZR3yJZmmzBE4DRidN1tCaZCw8Q");
        let c = store
            .collector("tz1N64TWNCZR3yJZmmzBE4DRidN1tCaZCw8Q")
            .unwrap();
        assert_eq!((c.token_count, c.listed), (1, 0));
    }

    /// Every step reads an index: no table is scanned. The only `SCAN`s allowed are FTS5
    /// lookups (a MATCH or rowid constraint, never a full virtual-table scan) and the
    /// handful of queued values in the `v` CTE (including its constant row).
    fn assert_no_table_scan(sql: &str, plan: &[String]) {
        for step in plan {
            if !step.starts_with("SCAN ") {
                continue;
            }
            let fts_lookup = step.contains("VIRTUAL TABLE INDEX")
                && !step.trim_end().ends_with(':')
                && (step.contains(":M") || step.contains(":="));
            let queued = step == "SCAN v" || step == "SCAN CONSTANT ROW";
            assert!(fts_lookup || queued, "{sql}\n{plan:?}");
        }
    }

    #[test]
    fn name_searches_and_their_upkeep_never_scan_tables() {
        let store = Store::open_in_memory().unwrap();
        let mut statements = Vec::new();
        let artists = format!("FROM artists a WHERE {ARTIST_VISIBLE}{ARTIST_TEXT_FILTER}");
        statements.push(format!("SELECT count(*) {artists}"));
        statements.push(format!(
            "SELECT a.id, a.name, a.name_key {artists} ORDER BY a.name_key, a.id LIMIT :limit"
        ));
        statements.push(format!(
            "SELECT a.id, a.name, a.name_key {artists} AND (a.name_key, a.id) > (:k0, :k1) \
             ORDER BY a.name_key, a.id LIMIT :limit"
        ));
        let projects = format!("FROM projects p WHERE p.hidden = 0{PROJECT_TEXT_FILTER}");
        statements.push(format!("SELECT count(*) {projects}"));
        for (sort, keyset) in [
            (ProjectSort::TitleAsc, "(p.title_key, p.id) > (:k0, :k1)"),
            (ProjectSort::ArtistAsc, "(p.artist_key, p.id) > (:k0, :k1)"),
            (ProjectSort::IdAsc, "p.id > :k0"),
            (ProjectSort::IdDesc, "p.id < :k0"),
        ] {
            let order = sort.order_by();
            statements.push(format!(
                "SELECT {SUMMARY_COLS} {projects} ORDER BY {order} LIMIT :limit OFFSET :offset"
            ));
            statements.push(format!(
                "SELECT {SUMMARY_COLS}, {order} {projects} AND {keyset} ORDER BY {order} \
                 LIMIT :limit"
            ));
        }
        for after in [false, true] {
            statements.push(format!(
                "SELECT {TOKEN_COLS} {TOKEN_FROM}{} ORDER BY {TOKEN_ORDER} LIMIT :limit",
                holdings_where(HoldingsText::Words, false, after)
            ));
        }
        statements.push(format!(
            "SELECT count(*) {TOKEN_COUNT_FROM}{}",
            holdings_where(HoldingsText::Words, false, false)
        ));
        for sql in [
            search::TOUCHED_ARTISTS,
            search::ARTIST_PROJECTS,
            search::PROJECT_CREDITS,
            "SELECT seq, id, name, address FROM artists WHERE seq = ?1",
            "SELECT name, aliases FROM artist_fts WHERE rowid = ?1",
            "SELECT title, artists FROM project_fts WHERE rowid = ?1",
            "SELECT address FROM identities WHERE slug = ?1",
            "SELECT slug FROM identities WHERE address = ?1",
            "SELECT alias FROM artist_aliases WHERE artist_id = ?1 ORDER BY position",
            profile::FOOTPRINT,
        ] {
            statements.push(sql.to_owned());
        }
        for sql in &statements {
            let plan = plan(&store, sql);
            assert_no_table_scan(sql, &plan);
        }
        // Text filters are driven by the FTS match, not by a walk of the sort index.
        for sql in &statements[..statements.len() - 10] {
            let plan = plan(&store, sql);
            assert!(
                plan.iter()
                    .any(|s| s.contains("VIRTUAL TABLE INDEX") && s.contains(":M")),
                "{sql}\n{plan:?}"
            );
        }
        // Every name source is probed by its index.
        let touched = plan(&store, search::TOUCHED_ARTISTS);
        for index in [
            "users_name_key",
            "artists_name_key",
            "profiles_imported_name_key",
            "identities_name_key",
            "artists_address",
        ] {
            assert!(
                touched.iter().any(|s| s.contains(index)),
                "{index}\n{touched:?}"
            );
        }
    }

    #[test]
    fn v7_database_rebuilds_search_names_from_shown_names() {
        const CLAIMED: &str = "tz1PoDdN2oyRyF6DA73zTWAWYhNL4UGr3Egj";
        const SHARED: &str = "tz1Kq8NtEjSXkpswLmgod1tPFPVH2TZc8dwM";
        const SHARER: &str = "tz1TEL6xFJx1hJKukx21NCufAfGEEr9yYoKX";
        const KEEPER: &str = "tz1gL9H53BzjDqmMi2JkKALeCzT7UAoopzEL";
        const UNIQUE: &str = "tz1WMa1cAKk4JjFGZCpXNkTDi5o5AqCsFF6E";
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
        for v in [
            SCHEMA_V1,
            SCHEMA_V2,
            SCHEMA_V3,
            SCHEMA_V4,
            SCHEMA_V5,
            SCHEMA_V6,
            market::SCHEMA_V7,
        ] {
            conn.execute_batch(v).unwrap();
        }
        // v7 indexes carried imported names.
        conn.execute_batch(&format!(
            "PRAGMA user_version = 7;
             INSERT INTO artists (id, name, name_key, address) VALUES
               ('{CLAIMED}', 'Old Name', 'old name', '{CLAIMED}'),
               ('{SHARED}', 'Mirror Hand', 'mirror hand', '{SHARED}'),
               ('{KEEPER}', '{KEEPER}', lower('{KEEPER}'), '{KEEPER}'),
               ('{UNIQUE}', 'Tyler Hobbs', 'tyler hobbs', '{UNIQUE}'),
               ('fixture-artist', 'Fixture Person', 'fixture person', NULL);
             INSERT INTO artist_aliases (artist_id, position, alias, alias_key) VALUES
               ('fixture-artist', 0, 'fixpers', 'fixpers');
             INSERT INTO users (address, name, name_key) VALUES
               ('{SHARER}', 'MIRROR hand', 'mirror hand');
             INSERT INTO identities (address, slug, name, aliases, provenance, name_key) VALUES
               ('{KEEPER}', 'emptyname', 'EmptyName', '[]', 'test', 'emptyname');
             INSERT INTO profile_edits (address, name, name_key, links, updated_at,
               signed_message, signature, public_key) VALUES
               ('{CLAIMED}', 'Seeded One', 'seeded one', '[]', 'now', 'm', 's', 'p');
             INSERT INTO projects (id, title, title_key, artist_key, index_state) VALUES
               (1, 'Lantern Fields', 'lantern fields', 'old name', 'indexed'),
               (2, 'Glass Orchard', 'glass orchard', 'mirror hand', 'indexed'),
               (3, 'Arbor', 'arbor', lower('{KEEPER}'), 'indexed'),
               (4, 'Duet Lines', 'duet lines', 'tyler hobbs', 'indexed'),
               (5, 'Fixture Work', 'fixture work', 'fixture person', 'indexed');
             INSERT INTO project_artists (project_id, position, artist_id) VALUES
               (1, 0, '{CLAIMED}'), (2, 0, '{SHARED}'), (3, 0, '{KEEPER}'),
               (4, 0, '{UNIQUE}'), (4, 1, '{CLAIMED}'), (5, 0, 'fixture-artist');
             INSERT INTO project_fts (rowid, title, artists) SELECT p.id, p.title,
               (SELECT group_concat(a.name, ' ') FROM project_artists pa
                JOIN artists a ON a.id = pa.artist_id WHERE pa.project_id = p.id)
               FROM projects p;
             INSERT INTO artist_fts (rowid, name, aliases) SELECT seq, name, coalesce(address, '')
               FROM artists;"
        ))
        .unwrap();
        let mut store = Store { conn };
        store.migrate().unwrap();
        assert_eq!(user_version(&store.conn).unwrap(), SCHEMA_VERSION);
        let rows = |sql: &str| -> Vec<(i64, String, String)> {
            store
                .conn
                .prepare(sql)
                .unwrap()
                .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
                .unwrap()
                .collect::<rusqlite::Result<_>>()
                .unwrap()
        };
        let row = |n: i64, a: &str, b: &str| (n, a.to_owned(), b.to_owned());
        assert_eq!(
            rows("SELECT rowid, name, aliases FROM artist_fts ORDER BY rowid"),
            vec![
                row(1, "Seeded One", CLAIMED),
                row(2, "Mirror Hand", SHARED),
                row(3, "EmptyName", &format!("{KEEPER} emptyname")),
                row(4, "Tyler Hobbs", UNIQUE),
                row(5, "Fixture Person", "fixpers"),
            ]
        );
        assert_eq!(
            rows("SELECT rowid, title, artists FROM project_fts ORDER BY rowid"),
            vec![
                row(1, "Lantern Fields", "Seeded One"),
                row(2, "Glass Orchard", "Mirror Hand"),
                row(3, "Arbor", "EmptyName"),
                row(4, "Duet Lines", "Tyler Hobbs Seeded One"),
                row(5, "Fixture Work", "Fixture Person"),
            ]
        );
        let found = |store: &Store, text: &str| -> Vec<u64> {
            let q = ProjectFilter {
                text: Some(text.into()),
                artist: None,
                sort: ProjectSort::IdAsc,
            };
            let page = store.projects_page(&q, None, 100).unwrap();
            page.items.iter().map(|p| p.id).collect()
        };
        assert_eq!(found(&store, "seeded one"), vec![1, 4]);
        assert_eq!(found(&store, "old name"), Vec::<u64>::new());
        // The shared name is numbered: the credited artist keeps it plain.
        assert_eq!(found(&store, "mirror"), vec![2]);
        assert_eq!(found(&store, "emptyname"), vec![3]);
        assert_eq!(found(&store, "lantern"), vec![1]);
        let count = |sql: &str| -> i64 { store.conn.query_row(sql, [], |r| r.get(0)).unwrap() };
        assert_eq!(count("SELECT count(*) FROM search_dirty"), 0);
        assert_eq!(
            count(
                "SELECT count(*) FROM sqlite_master WHERE type = 'trigger' \
                 AND name LIKE '%_search_%'"
            ),
            18
        );
        // Migrating again is a no-op; a hand rollback to v7 migrates again.
        store.migrate().unwrap();
        store.conn.execute_batch("PRAGMA user_version = 7").unwrap();
        store.migrate().unwrap();
        assert_eq!(user_version(&store.conn).unwrap(), SCHEMA_VERSION);
        assert_eq!(found(&store, "seeded one"), vec![1, 4]);
    }

    #[test]
    fn address_checks_follow_base58() {
        assert!(is_contract("KT1EfsNuqwLAWDd3o4pvfUx1CAh5GMdTrRvr"));
        assert!(!is_contract("KT1EfsNuqwLAWDd3o4pvfUx1CAh5GMdTrRv0"));
        assert!(!is_contract("KT1EfsNuqwLAWDd3o4pvfUx1CAh5GMdTrRv"));
        assert!(!is_contract("tz1N64TWNCZR3yJZmmzBE4DRidN1tCaZCw8Q"));
        assert!(is_tezos_address("tz1N64TWNCZR3yJZmmzBE4DRidN1tCaZCw8Q"));
        assert!(is_tezos_address("tz2RXosjAZdS85oCrguy7Eom757144dcU1FT"));
        assert!(!is_tezos_address("tz5N64TWNCZR3yJZmmzBE4DRidN1tCaZCw8Q"));
        assert!(!is_tezos_address("tz1N64TWNCZR3yJZmmzBE4DRidN1tCaZCwéQ"));
    }
}
