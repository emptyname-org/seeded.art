//! Fixture import: validate everything, then replace all data in one transaction. A database
//! holding indexed data (Frank catalogue, TzKT sync) is refused, never wiped.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs::File;
use std::io::Read;
use std::path::{Component, Path};

use rusqlite::{TransactionBehavior, params};
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::model::IndexState;
use crate::store::{
    Db, Store, StoreError, add_content_roots, current_fixture_sha256, is_base58, is_contract,
    is_tezos_address,
};

const ARTISTS: &str = "artists.json";
const PROJECTS: &str = "projects.json";
const TOKENS: &str = "tokens.json";
const SNAPSHOT: &str = "snapshot.json";
const MAX_JSON_BYTES: u64 = 64 << 20;
const MAX_PREVIEW_BYTES: u64 = 32 << 20;
const PREVIEW_EXTENSIONS: [&str; 3] = ["webp", "png", "jpg"];
const HASH_DOMAIN: &[u8] = b"seeded-fixtures-v1\0";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportReport {
    pub projects: usize,
    pub tokens: usize,
    pub artists: usize,
    pub changed: bool,
    pub fixture_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ImportError {
    #[error("invalid fixture {file}: {reason}")]
    Invalid { file: String, reason: String },
    #[error("fixture io error: {0}")]
    Io(String),
    #[error(transparent)]
    Store(#[from] StoreError),
}

fn invalid(file: &str, reason: impl Into<String>) -> ImportError {
    ImportError::Invalid {
        file: file.to_owned(),
        reason: reason.into(),
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ArtistIn {
    id: String,
    name: String,
    website: Option<String>,
    aliases: Vec<String>,
}

#[derive(Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum IndexStateIn {
    Indexed,
    NotIndexed,
}

impl From<IndexStateIn> for IndexState {
    fn from(s: IndexStateIn) -> Self {
        match s {
            IndexStateIn::Indexed => IndexState::Indexed,
            IndexStateIn::NotIndexed => IndexState::NotIndexed,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProjectIn {
    id: u64,
    title: String,
    artists: Vec<String>,
    description: Option<String>,
    preview: Option<String>,
    contract: Option<String>,
    issuer: Option<String>,
    artifact_uri: Option<String>,
    editions: Option<u32>,
    first_mint: Option<String>,
    last_mint: Option<String>,
    index_state: IndexStateIn,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FeatureIn {
    name: String,
    value: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OwnerIn {
    address: String,
    alias: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TokenIn {
    contract: String,
    token_id: u64,
    project_id: u64,
    iteration: u32,
    seed: String,
    minter: String,
    minted: String,
    artifact_uri: String,
    preview: Option<String>,
    features: Vec<FeatureIn>,
    owner: Option<OwnerIn>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SnapshotIn {
    as_of: String,
    owner_source: String,
    catalogue_source: String,
}

struct Fixtures {
    artists: Vec<ArtistIn>,
    projects: Vec<ProjectIn>,
    tokens: Vec<TokenIn>,
    snapshot: SnapshotIn,
    sha256: String,
}

/// Imports `dir`. An unchanged fixture hash is a no-op; any failure leaves the previous data intact.
pub fn import_fixtures(store: &mut Store, dir: &Path) -> Result<ImportReport, ImportError> {
    let fx = load(dir)?;
    let report = |changed| ImportReport {
        projects: fx.projects.len(),
        tokens: fx.tokens.len(),
        artists: fx.artists.len(),
        changed,
        fixture_sha256: fx.sha256.clone(),
    };
    // Cheap pre-check without the write lock; repeated under the lock below.
    if current_fixture_sha256(store.conn_mut())?.as_deref() == Some(fx.sha256.as_str()) {
        return Ok(report(false));
    }
    let tx = store
        .conn_mut()
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .db()?;
    if current_fixture_sha256(&tx)?.as_deref() == Some(fx.sha256.as_str()) {
        return Ok(report(false));
    }
    let indexed: bool = tx
        .query_row(
            "SELECT EXISTS (SELECT 1 FROM sync_state) \
             OR EXISTS (SELECT 1 FROM projects WHERE source <> 'fixture') \
             OR EXISTS (SELECT 1 FROM tokens WHERE source <> 'fixture') \
             OR EXISTS (SELECT 1 FROM catalogue_tokens)",
            [],
            |r| r.get(0),
        )
        .db()?;
    if indexed {
        return Err(invalid(
            "database",
            "refusing to replace an indexed archive with fixtures",
        ));
    }
    write_all(&tx, &fx)?;
    tx.commit().db()?;
    Ok(report(true))
}

fn write_all(tx: &rusqlite::Connection, fx: &Fixtures) -> Result<(), StoreError> {
    tx.execute_batch(
        "DELETE FROM token_features; DELETE FROM tokens; DELETE FROM project_artists; \
         DELETE FROM projects; DELETE FROM artist_aliases; DELETE FROM artists; \
         DELETE FROM project_fts; DELETE FROM artist_fts; DELETE FROM snapshot; \
         DELETE FROM users; DELETE FROM collaborations; DELETE FROM catalogue_tokens; \
         DELETE FROM catalogue_snapshot; DELETE FROM sync_state; DELETE FROM sync_chunks; \
         DELETE FROM ingest_issues; DELETE FROM escrow_listings; DELETE FROM escrow_state; \
         DELETE FROM content_roots; DELETE FROM holder_counts; DELETE FROM feature_counts;",
    )
    .db()?;

    let mut names: HashMap<&str, &str> = HashMap::new();
    {
        let mut artist = tx
            .prepare("INSERT INTO artists (id, name, name_key, website) VALUES (?1, ?2, ?3, ?4)")
            .db()?;
        let mut alias = tx
            .prepare(
                "INSERT INTO artist_aliases (artist_id, position, alias, alias_key) \
                 VALUES (?1, ?2, ?3, ?4)",
            )
            .db()?;
        let mut fts = tx
            .prepare("INSERT INTO artist_fts (rowid, name, aliases) VALUES (?1, ?2, ?3)")
            .db()?;
        for a in &fx.artists {
            artist
                .execute(params![a.id, a.name, a.name.to_lowercase(), a.website])
                .db()?;
            let seq = tx.last_insert_rowid();
            for (i, al) in a.aliases.iter().enumerate() {
                alias
                    .execute(params![a.id, i as i64, al, al.to_lowercase()])
                    .db()?;
            }
            fts.execute(params![seq, a.name, a.aliases.join(" ")])
                .db()?;
            names.insert(&a.id, &a.name);
        }
    }
    {
        let mut project = tx
            .prepare(
                "INSERT INTO projects (id, title, title_key, artist_key, description, preview, \
                 contract, issuer, artifact_uri, editions, first_mint, last_mint, index_state) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
            )
            .db()?;
        let mut link = tx
            .prepare(
                "INSERT INTO project_artists (project_id, position, artist_id) VALUES (?1, ?2, ?3)",
            )
            .db()?;
        let mut fts = tx
            .prepare("INSERT INTO project_fts (rowid, title, artists) VALUES (?1, ?2, ?3)")
            .db()?;
        for p in &fx.projects {
            let id = p.id as i64; // validated <= i64::MAX
            let artist_names = p
                .artists
                .iter()
                .map(|a| names.get(a.as_str()).copied())
                .collect::<Option<Vec<&str>>>()
                .filter(|n| !n.is_empty())
                .ok_or_else(|| {
                    StoreError::Internal(format!("project {id}: unvalidated artists"))
                })?;
            project
                .execute(params![
                    id,
                    p.title,
                    p.title.to_lowercase(),
                    artist_names[0].to_lowercase(),
                    p.description,
                    p.preview,
                    p.contract,
                    p.issuer,
                    p.artifact_uri,
                    p.editions,
                    p.first_mint,
                    p.last_mint,
                    IndexState::from(p.index_state).as_str(),
                ])
                .db()?;
            for (i, a) in p.artists.iter().enumerate() {
                link.execute(params![id, i as i64, a]).db()?;
            }
            add_content_roots(tx, p.id, &[p.artifact_uri.as_deref()])?;
            fts.execute(params![id, p.title, artist_names.join(" ")])
                .db()?;
        }
    }
    {
        let mut token = tx
            .prepare(
                "INSERT INTO tokens (contract, token_id, project_id, iteration, seed, minter, \
                 minted, artifact_uri, preview, owner_address, owner_alias, owner_alias_key, \
                 owner_as_of, owner_source, holder_address) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?10)",
            )
            .db()?;
        let mut feature = tx
            .prepare(
                "INSERT INTO token_features (contract, token_id, position, name, value) \
                 VALUES (?1, ?2, ?3, ?4, ?5)",
            )
            .db()?;
        let snap = &fx.snapshot;
        for t in &fx.tokens {
            let token_id = t.token_id as i64; // validated <= i64::MAX
            let owner = t.owner.as_ref();
            token
                .execute(params![
                    t.contract,
                    token_id,
                    t.project_id as i64,
                    t.iteration,
                    t.seed,
                    t.minter,
                    t.minted,
                    t.artifact_uri,
                    t.preview,
                    owner.map(|o| &o.address),
                    owner.and_then(|o| o.alias.as_ref()),
                    owner
                        .and_then(|o| o.alias.as_ref())
                        .map(|a| a.to_lowercase()),
                    owner.map(|_| &snap.as_of),
                    owner.map(|_| &snap.owner_source),
                ])
                .db()?;
            add_content_roots(tx, t.project_id, &[Some(t.artifact_uri.as_str())])?;
            for (i, f) in t.features.iter().enumerate() {
                feature
                    .execute(params![t.contract, token_id, i as i64, f.name, f.value])
                    .db()?;
            }
        }
    }
    tx.execute(
        "INSERT INTO snapshot (id, as_of, owner_source, catalogue_source, fixture_sha256, \
         imported_at) VALUES (1, ?1, ?2, ?3, ?4, strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))",
        params![
            fx.snapshot.as_of,
            fx.snapshot.owner_source,
            fx.snapshot.catalogue_source,
            fx.sha256
        ],
    )
    .db()?;
    Ok(())
}

fn load(dir: &Path) -> Result<Fixtures, ImportError> {
    let root = dir
        .canonicalize()
        .map_err(|e| ImportError::Io(format!("fixture directory: {e}")))?;
    let raw: BTreeMap<&str, Vec<u8>> = [ARTISTS, PROJECTS, TOKENS, SNAPSHOT]
        .into_iter()
        .map(|name| read_capped(&root, name).map(|b| (name, b)))
        .collect::<Result<_, _>>()?;
    let artists: Vec<ArtistIn> = parse(ARTISTS, &raw[ARTISTS])?;
    let projects: Vec<ProjectIn> = parse(PROJECTS, &raw[PROJECTS])?;
    let tokens: Vec<TokenIn> = parse(TOKENS, &raw[TOKENS])?;
    let snapshot: SnapshotIn = parse(SNAPSHOT, &raw[SNAPSHOT])?;

    validate_snapshot(&snapshot)?;
    validate_artists(&artists)?;
    validate_projects(&projects, &artists)?;
    validate_tokens(&tokens, &projects)?;

    let mut previews = BTreeSet::new();
    let refs = projects
        .iter()
        .map(|p| (PROJECTS, p.preview.as_deref()))
        .chain(tokens.iter().map(|t| (TOKENS, t.preview.as_deref())));
    for (file, preview) in refs {
        if let Some(rel) = preview {
            check_preview(&root, rel).map_err(|reason| invalid(file, reason))?;
            previews.insert(rel.to_owned());
        }
    }
    let sha256 = fixture_hash(&root, &raw, &previews)?;
    Ok(Fixtures {
        artists,
        projects,
        tokens,
        snapshot,
        sha256,
    })
}

fn read_capped(root: &Path, name: &str) -> Result<Vec<u8>, ImportError> {
    let file = File::open(root.join(name)).map_err(|e| ImportError::Io(format!("{name}: {e}")))?;
    let mut buf = Vec::new();
    file.take(MAX_JSON_BYTES + 1)
        .read_to_end(&mut buf)
        .map_err(|e| ImportError::Io(format!("{name}: {e}")))?;
    if buf.len() as u64 > MAX_JSON_BYTES {
        return Err(invalid(name, format!("larger than {MAX_JSON_BYTES} bytes")));
    }
    Ok(buf)
}

fn parse<T: for<'de> Deserialize<'de>>(file: &str, bytes: &[u8]) -> Result<T, ImportError> {
    serde_json::from_slice(bytes).map_err(|e| invalid(file, e.to_string()))
}

/// SHA-256 over a domain tag, then for each file sorted by relative path:
/// path, NUL, big-endian u64 length, bytes. Files: the four JSON files and every referenced preview.
fn fixture_hash(
    root: &Path,
    json: &BTreeMap<&str, Vec<u8>>,
    previews: &BTreeSet<String>,
) -> Result<String, ImportError> {
    let mut names: Vec<&str> = json
        .keys()
        .copied()
        .chain(previews.iter().map(String::as_str))
        .collect();
    names.sort_unstable();
    let mut h = Sha256::new();
    h.update(HASH_DOMAIN);
    let mut buf = vec![0u8; 64 << 10];
    for name in names {
        h.update(name.as_bytes());
        h.update([0u8]);
        if let Some(bytes) = json.get(name) {
            h.update((bytes.len() as u64).to_be_bytes());
            h.update(bytes);
            continue;
        }
        let io = |e: std::io::Error| ImportError::Io(format!("{name}: {e}"));
        let mut file = File::open(root.join(name)).map_err(io)?;
        let len = file.metadata().map_err(io)?.len();
        h.update(len.to_be_bytes());
        let mut seen = 0u64;
        loop {
            let n = file.read(&mut buf).map_err(io)?;
            if n == 0 {
                break;
            }
            seen += n as u64;
            if seen > len {
                break;
            }
            h.update(&buf[..n]);
        }
        if seen != len {
            return Err(ImportError::Io(format!("{name}: changed while hashing")));
        }
    }
    Ok(hex::encode(h.finalize()))
}

fn check_preview(root: &Path, rel: &str) -> Result<(), String> {
    let path = Path::new(rel);
    if rel.is_empty() || rel.contains('\\') || rel.contains('\0') {
        return Err(format!("preview {rel:?} is not a plain relative path"));
    }
    if !path.components().all(|c| matches!(c, Component::Normal(_))) {
        return Err(format!(
            "preview {rel:?} must be relative without '.' or '..'"
        ));
    }
    let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
    if !PREVIEW_EXTENSIONS.contains(&ext) {
        return Err(format!("preview {rel:?} must end in .webp, .png or .jpg"));
    }
    let full = root
        .join(path)
        .canonicalize()
        .map_err(|_| format!("preview {rel:?} does not exist"))?;
    if !full.starts_with(root) {
        return Err(format!(
            "preview {rel:?} resolves outside the fixture directory"
        ));
    }
    let meta = full
        .metadata()
        .map_err(|e| format!("preview {rel:?}: {e}"))?;
    if !meta.is_file() {
        return Err(format!("preview {rel:?} is not a file"));
    }
    if meta.len() > MAX_PREVIEW_BYTES {
        return Err(format!(
            "preview {rel:?} is larger than {MAX_PREVIEW_BYTES} bytes"
        ));
    }
    Ok(())
}

/// Strict `YYYY-MM-DD` with a real calendar day.
fn is_date(s: &str) -> bool {
    let b = s.as_bytes();
    if b.len() != 10 || b[4] != b'-' || b[7] != b'-' {
        return false;
    }
    let num = |r: std::ops::Range<usize>| -> Option<u32> {
        b[r].iter().try_fold(0u32, |acc, &c| {
            c.is_ascii_digit().then(|| acc * 10 + u32::from(c - b'0'))
        })
    };
    let (Some(y), Some(m), Some(d)) = (num(0..4), num(5..7), num(8..10)) else {
        return false;
    };
    let leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
    let days = match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap => 29,
        2 => 28,
        _ => return false,
    };
    (1..=days).contains(&d)
}

fn is_slug(s: &str) -> bool {
    !s.is_empty()
        && s.split('-').all(|part| {
            !part.is_empty()
                && part
                    .bytes()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
        })
}

fn blank(s: &str) -> bool {
    s.trim().is_empty()
}

fn validate_snapshot(s: &SnapshotIn) -> Result<(), ImportError> {
    if !is_date(&s.as_of) {
        return Err(invalid(
            SNAPSHOT,
            format!("as_of {:?} is not YYYY-MM-DD", s.as_of),
        ));
    }
    if blank(&s.owner_source) || blank(&s.catalogue_source) {
        return Err(invalid(SNAPSHOT, "sources must not be empty"));
    }
    Ok(())
}

fn validate_artists(artists: &[ArtistIn]) -> Result<(), ImportError> {
    let mut ids = HashSet::new();
    let mut aliases: HashMap<String, &str> = HashMap::new();
    for a in artists {
        let bad = |reason: String| invalid(ARTISTS, format!("artist {:?}: {reason}", a.id));
        if !is_slug(&a.id) {
            return Err(bad("id must be a lowercase slug".into()));
        }
        if !ids.insert(a.id.as_str()) {
            return Err(bad("duplicate id".into()));
        }
        if blank(&a.name) {
            return Err(bad("empty name".into()));
        }
        if let Some(w) = &a.website {
            if !(w.starts_with("https://") || w.starts_with("http://")) {
                return Err(bad(format!("website {w:?} is not an http(s) URL")));
            }
        }
        for al in &a.aliases {
            if blank(al) {
                return Err(bad("empty alias".into()));
            }
            if let Some(other) = aliases.insert(al.to_lowercase(), &a.id) {
                return Err(bad(format!("alias {al:?} already used by {other:?}")));
            }
        }
    }
    Ok(())
}

fn validate_projects(projects: &[ProjectIn], artists: &[ArtistIn]) -> Result<(), ImportError> {
    let known: HashSet<&str> = artists.iter().map(|a| a.id.as_str()).collect();
    let mut ids = HashSet::new();
    for p in projects {
        let bad = |reason: String| invalid(PROJECTS, format!("project {}: {reason}", p.id));
        if i64::try_from(p.id).is_err() {
            return Err(bad("id out of range".into()));
        }
        if !ids.insert(p.id) {
            return Err(bad("duplicate id".into()));
        }
        if blank(&p.title) {
            return Err(bad("empty title".into()));
        }
        if p.artists.is_empty() {
            return Err(bad("no artists".into()));
        }
        let mut seen = HashSet::new();
        for a in &p.artists {
            if !known.contains(a.as_str()) {
                return Err(bad(format!("unknown artist {a:?}")));
            }
            if !seen.insert(a.as_str()) {
                return Err(bad(format!("artist {a:?} listed twice")));
            }
        }
        for (field, addr) in [("contract", &p.contract), ("issuer", &p.issuer)] {
            if let Some(addr) = addr {
                if !is_contract(addr) {
                    return Err(bad(format!("{field} {addr:?} is not a KT1 address")));
                }
            }
        }
        if let Some(uri) = &p.artifact_uri {
            if !has_artifact_scheme(uri) {
                return Err(bad(
                    "artifact_uri must start with onchfs:// or ipfs://".into()
                ));
            }
        }
        if p.editions == Some(0) {
            return Err(bad(
                "editions must be positive; use null when unknown".into()
            ));
        }
        for (field, date) in [("first_mint", &p.first_mint), ("last_mint", &p.last_mint)] {
            if let Some(d) = date {
                if !is_date(d) {
                    return Err(bad(format!("{field} {d:?} is not YYYY-MM-DD")));
                }
            }
        }
        if let (Some(first), Some(last)) = (&p.first_mint, &p.last_mint) {
            if first > last {
                return Err(bad("first_mint is after last_mint".into()));
            }
        }
        if p.index_state == IndexStateIn::Indexed && (p.editions.is_none() || p.contract.is_none())
        {
            return Err(bad("indexed projects need editions and contract".into()));
        }
    }
    Ok(())
}

fn has_artifact_scheme(uri: &str) -> bool {
    uri.starts_with("onchfs://") || uri.starts_with("ipfs://")
}

/// Query parameters of an artifact URI; `None` on a duplicate key.
fn query_params(uri: &str) -> Option<HashMap<&str, &str>> {
    let uri = uri.split('#').next().unwrap_or(uri);
    let mut out = HashMap::new();
    let Some((_, query)) = uri.split_once('?') else {
        return Some(out);
    };
    for pair in query.split('&').filter(|p| !p.is_empty()) {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        if out.insert(k, v).is_some() {
            return None;
        }
    }
    Some(out)
}

fn validate_tokens(tokens: &[TokenIn], projects: &[ProjectIn]) -> Result<(), ImportError> {
    let by_id: HashMap<u64, &ProjectIn> = projects.iter().map(|p| (p.id, p)).collect();
    let mut keys = HashSet::new();
    let mut iterations = HashSet::new();
    let mut per_project: HashMap<u64, (usize, &str, &str)> = HashMap::new();
    for t in tokens {
        let bad = |reason: String| {
            invalid(
                TOKENS,
                format!("token {}/{}: {reason}", t.contract, t.token_id),
            )
        };
        if !is_contract(&t.contract) {
            return Err(bad("contract is not a KT1 address".into()));
        }
        if i64::try_from(t.token_id).is_err() {
            return Err(bad("token_id out of range".into()));
        }
        if !keys.insert((t.contract.as_str(), t.token_id)) {
            return Err(bad("duplicate (contract, token_id)".into()));
        }
        let Some(p) = by_id.get(&t.project_id) else {
            return Err(bad(format!("unknown project {}", t.project_id)));
        };
        if p.index_state != IndexStateIn::Indexed {
            return Err(bad(format!("project {} is not indexed", p.id)));
        }
        if p.contract.as_deref() != Some(t.contract.as_str()) {
            return Err(bad(format!("contract differs from project {}", p.id)));
        }
        let editions = p.editions.unwrap_or(0);
        if !(1..=editions).contains(&t.iteration) {
            return Err(bad(format!(
                "iteration {} outside 1..={editions}",
                t.iteration
            )));
        }
        if !iterations.insert((t.project_id, t.iteration)) {
            return Err(bad(format!("duplicate iteration {}", t.iteration)));
        }
        if !is_base58(&t.seed) {
            return Err(bad("seed must be non-empty base58".into()));
        }
        if !is_tezos_address(&t.minter) {
            return Err(bad(format!("minter {:?} is not a Tezos address", t.minter)));
        }
        if !is_date(&t.minted) {
            return Err(bad(format!("minted {:?} is not YYYY-MM-DD", t.minted)));
        }
        if !has_artifact_scheme(&t.artifact_uri) {
            return Err(bad(
                "artifact_uri must start with onchfs:// or ipfs://".into()
            ));
        }
        let q = query_params(&t.artifact_uri)
            .ok_or_else(|| bad("artifact_uri repeats a query parameter".into()))?;
        if q.get("fxhash") != Some(&t.seed.as_str()) {
            return Err(bad("artifact_uri fxhash does not match seed".into()));
        }
        if q.get("fxiteration") != Some(&t.iteration.to_string().as_str()) {
            return Err(bad(
                "artifact_uri fxiteration does not match iteration".into()
            ));
        }
        if let Some(m) = q.get("fxminter") {
            if *m != t.minter {
                return Err(bad("artifact_uri fxminter does not match minter".into()));
            }
        }
        let mut names = HashSet::new();
        for f in &t.features {
            if blank(&f.name) || !names.insert(f.name.as_str()) {
                return Err(bad(format!("feature name {:?} empty or repeated", f.name)));
            }
        }
        if let Some(o) = &t.owner {
            if !is_tezos_address(&o.address) {
                return Err(bad(format!("owner {:?} is not a Tezos address", o.address)));
            }
            if o.alias.as_deref().is_some_and(blank) {
                return Err(bad("owner alias is empty; use null".into()));
            }
        }
        let e =
            per_project
                .entry(t.project_id)
                .or_insert((0, t.minted.as_str(), t.minted.as_str()));
        e.0 += 1;
        e.1 = e.1.min(t.minted.as_str());
        e.2 = e.2.max(t.minted.as_str());
    }
    for p in projects
        .iter()
        .filter(|p| p.index_state == IndexStateIn::Indexed)
    {
        let bad = |reason: String| invalid(PROJECTS, format!("project {}: {reason}", p.id));
        let (count, first, last) = per_project.get(&p.id).copied().unwrap_or((0, "", ""));
        let editions = p.editions.unwrap_or(0) as usize;
        if count != editions {
            return Err(bad(format!("{count} tokens for {editions} editions")));
        }
        if p.first_mint.as_deref() != Some(first) || p.last_mint.as_deref() != Some(last) {
            return Err(bad(format!("first/last mint must be {first}/{last}")));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dates_are_strict_calendar_days() {
        for ok in ["2023-11-10", "2024-02-29", "2000-02-29", "2026-10-02"] {
            assert!(is_date(ok), "{ok}");
        }
        for bad in [
            "2023-02-29",
            "1900-02-29",
            "2023-13-01",
            "2023-00-10",
            "2023-1-10",
            "2023-11-10T00",
            "２０２３-11-10",
            "",
        ] {
            assert!(!is_date(bad), "{bad}");
        }
    }

    #[test]
    fn query_params_rejects_duplicates_and_ignores_fragment() {
        let q = query_params("onchfs://x/?fxhash=oo1&fxiteration=2#0xab").unwrap();
        assert_eq!(q.get("fxhash"), Some(&"oo1"));
        assert_eq!(q.get("fxiteration"), Some(&"2"));
        assert!(query_params("ipfs://x?fxhash=a&fxhash=b").is_none());
        assert!(query_params("ipfs://x").unwrap().is_empty());
    }

    #[test]
    fn slugs_are_lowercase_dash_separated() {
        assert!(is_slug("zancan-and-yazid"));
        assert!(!is_slug("Zancan"));
        assert!(!is_slug("-a"));
        assert!(!is_slug("a--b"));
        assert!(!is_slug(""));
    }
}
