-- Schema v1 exactly as in src/store.rs SCHEMA_V1 before v2, for the v1 -> v2 migration test.
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

-- A small v1 database: one artist, one indexed project, one token with an owner snapshot.
INSERT INTO snapshot VALUES (1, '2026-10-02', 'objkt data.objkt.com/v3/graphql', 'fixture', 'v1-fixture', '2026-10-02T00:00:00Z');
INSERT INTO artists (seq, id, name, name_key, website) VALUES (1, 'zancan', 'zancan', 'zancan', NULL);
INSERT INTO artist_aliases VALUES ('zancan', 0, 'zancan', 'zancan');
INSERT INTO artist_fts (rowid, name, aliases) VALUES (1, 'zancan', 'zancan');
INSERT INTO projects VALUES (2969, 'Garden, Monoliths', 'garden, monoliths', 'zancan', NULL, NULL, 'KT1KEa8z6vWXDJrVqtMrAeDVzsvxat3kHaCE', NULL, NULL, 1, '2021-12-01', '2021-12-01', 'indexed');
INSERT INTO project_artists VALUES (2969, 0, 'zancan');
INSERT INTO project_fts (rowid, title, artists) VALUES (2969, 'Garden, Monoliths', 'zancan');
INSERT INTO tokens VALUES ('KT1KEa8z6vWXDJrVqtMrAeDVzsvxat3kHaCE', 100000, 2969, 1, 'ooTestSeed', 'tz1gBXG9fg8RMDH69KfKqwoTH5sFDmzt5yzm', '2021-12-01', 'ipfs://QmTest?fxhash=ooTestSeed', NULL, 'tz2RXosjAZdS85oCrguy7Eom757144dcU1FT', 'zancan', 'zancan', '2026-10-02', 'objkt data.objkt.com/v3/graphql');
INSERT INTO token_features VALUES ('KT1KEa8z6vWXDJrVqtMrAeDVzsvxat3kHaCE', 100000, 0, 'Palette', 'Stone');
PRAGMA user_version = 1;
