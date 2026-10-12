//! Bootstrap from Frank Force's fxhash archive, https://github.com/KilledByAPixel/fxhashArchive
//! (`public/data`, captured from fxhash's API and TzKT; the repository's code is MIT, the
//! record data is not authored there).
//!
//! Reads: `tokens/index-*.json` (projects), `users.json` (address -> on-chain user name),
//! `collaborations.json` (collab contract -> members), `descriptions/*.json`,
//! `artists/index.json` (profiles), `iterations/contracts.json` + `map-*.json` (project ->
//! token ids), `seeds/meta.json` + `seeds/<contract>/*.json` (token id -> seed, artifact URI),
//! and the repository's `data/artist-names.json` (names changed at an artist's request).
//!
//! Writes projects, artists, users, collaborations and `catalogue_tokens` in one transaction.
//! Never writes `tokens`: Frank's records lack minter, mint time and most iterations, so a
//! token is materialised only from chain data (see `sync`). An unchanged input hash is a
//! no-op; a failure leaves the previous state intact.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};

use rusqlite::{OptionalExtension, TransactionBehavior, params};
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::{IngestError, TOKEN_CONTRACTS, now_iso, source_err};
use crate::store::{Db, Store, add_content_roots, is_tezos_address, name_key};

/// Flags fxhash moderated away; same set as Frank's `HIDDEN_FLAGS`.
pub const HIDDEN_FLAGS: [&str; 4] = ["MALICIOUS", "HIDDEN", "REPORTED", "AUTO_DETECT_COPY"];
pub const SOURCE_NAME: &str = "KilledByAPixel/fxhashArchive";
/// Bump when the mapping changes so an unchanged archive is re-imported.
const HASH_DOMAIN: &[u8] = b"seeded-frank-bootstrap-v1\0";
const MAX_FILE: u64 = 64 << 20;
const NONE: u32 = u32::MAX;
const CONFLICT: u32 = u32::MAX - 1;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct BootstrapReport {
    pub changed: bool,
    pub content_sha256: String,
    pub source: String,
    pub revision: Option<String>,
    pub projects: usize,
    pub hidden: usize,
    pub artists: usize,
    pub users: usize,
    pub collaborations: usize,
    pub catalogue_tokens: usize,
    /// Catalogue tokens without a seed (unsigned mints) or without a seed record.
    pub without_seed: usize,
    /// Rejected input records by reason.
    pub rejected: BTreeMap<&'static str, usize>,
    /// Changed catalogue rows re-compared with stored chain tokens, by resulting state.
    pub recompared: BTreeMap<String, u64>,
    pub identities: super::identity::IdentityReport,
}

struct Layout {
    data: PathBuf,
    root: Option<PathBuf>,
}

fn layout(dir: &Path) -> Result<Layout, IngestError> {
    let dir = dir
        .canonicalize()
        .map_err(|e| IngestError::Io(format!("{}: {e}", dir.display())))?;
    if dir.join("public/data/tokens").is_dir() {
        return Ok(Layout {
            data: dir.join("public/data"),
            root: Some(dir),
        });
    }
    if dir.join("tokens").is_dir() {
        let root = dir
            .parent()
            .and_then(Path::parent)
            .filter(|r| r.join("data").is_dir())
            .map(Path::to_path_buf);
        return Ok(Layout { data: dir, root });
    }
    Err(source_err(
        dir.display().to_string(),
        "neither <dir>/public/data/tokens nor <dir>/tokens exists",
    ))
}

fn list(dir: &Path, keep: impl Fn(&str) -> bool) -> Result<Vec<PathBuf>, IngestError> {
    let mut out = Vec::new();
    let entries = match fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
        Err(e) => return Err(IngestError::Io(format!("{}: {e}", dir.display()))),
    };
    for entry in entries {
        let entry = entry.map_err(|e| IngestError::Io(e.to_string()))?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if keep(name) && entry.path().is_file() {
            out.push(entry.path());
        }
    }
    out.sort();
    Ok(out)
}

fn numbered(name: &str, prefix: &str) -> bool {
    name.strip_prefix(prefix)
        .and_then(|r| r.strip_suffix(".json"))
        .is_some_and(|n| !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()))
}

struct Inputs {
    projects: Vec<PathBuf>,
    descriptions: Vec<PathBuf>,
    maps: Vec<PathBuf>,
    /// (contract index, file)
    seeds: Vec<(usize, PathBuf)>,
    users: PathBuf,
    collaborations: PathBuf,
    meta: PathBuf,
    contracts: PathBuf,
    seeds_meta: PathBuf,
    artists: Option<PathBuf>,
    renames: Option<PathBuf>,
}

impl Inputs {
    fn find(l: &Layout) -> Result<Inputs, IngestError> {
        let d = &l.data;
        let required = |rel: &str| -> Result<PathBuf, IngestError> {
            let p = d.join(rel);
            if p.is_file() {
                Ok(p)
            } else {
                Err(source_err(rel, "missing"))
            }
        };
        let mut seeds = Vec::new();
        for ci in 0..TOKEN_CONTRACTS.len() {
            for f in list(&d.join(format!("seeds/{ci}")), |n| numbered(n, ""))? {
                seeds.push((ci, f));
            }
        }
        let projects = list(&d.join("tokens"), |n| numbered(n, "index-"))?;
        if projects.is_empty() {
            return Err(source_err("tokens/index-*.json", "no project shards"));
        }
        Ok(Inputs {
            projects,
            descriptions: list(&d.join("descriptions"), |n| numbered(n, ""))?,
            maps: list(&d.join("iterations"), |n| numbered(n, "map-"))?,
            seeds,
            users: required("users.json")?,
            collaborations: required("collaborations.json")?,
            meta: required("meta.json")?,
            contracts: required("iterations/contracts.json")?,
            seeds_meta: required("seeds/meta.json")?,
            artists: Some(d.join("artists/index.json")).filter(|p| p.is_file()),
            renames: l
                .root
                .as_ref()
                .map(|r| r.join("data/artist-names.json"))
                .filter(|p| p.is_file()),
        })
    }

    fn all(&self) -> Vec<&Path> {
        let mut v: Vec<&Path> = vec![
            &self.users,
            &self.collaborations,
            &self.meta,
            &self.contracts,
            &self.seeds_meta,
        ];
        v.extend(self.artists.as_deref());
        v.extend(self.renames.as_deref());
        v.extend(self.projects.iter().map(PathBuf::as_path));
        v.extend(self.descriptions.iter().map(PathBuf::as_path));
        v.extend(self.maps.iter().map(PathBuf::as_path));
        v.extend(self.seeds.iter().map(|(_, p)| p.as_path()));
        v
    }
}

fn read_capped(path: &Path) -> Result<Vec<u8>, IngestError> {
    let io = |e: std::io::Error| IngestError::Io(format!("{}: {e}", path.display()));
    let mut buf = Vec::new();
    File::open(path)
        .map_err(io)?
        .take(MAX_FILE + 1)
        .read_to_end(&mut buf)
        .map_err(io)?;
    if buf.len() as u64 > MAX_FILE {
        return Err(source_err(
            path.display().to_string(),
            format!("larger than {MAX_FILE} bytes"),
        ));
    }
    Ok(buf)
}

fn parse<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T, IngestError> {
    serde_json::from_slice(&read_capped(path)?)
        .map_err(|e| source_err(path.display().to_string(), e.to_string()))
}

/// SHA-256 over a domain tag, then per file (sorted by path relative to the data or repo
/// root): path, NUL, big-endian u64 length, bytes.
fn content_hash(l: &Layout, files: &[&Path]) -> Result<String, IngestError> {
    let mut named: Vec<(String, &Path)> = files
        .iter()
        .map(|p| {
            let rel = p
                .strip_prefix(&l.data)
                .ok()
                .or_else(|| l.root.as_ref().and_then(|r| p.strip_prefix(r).ok()))
                .unwrap_or(p);
            (rel.to_string_lossy().into_owned(), *p)
        })
        .collect();
    named.sort();
    let mut h = Sha256::new();
    h.update(HASH_DOMAIN);
    for (rel, path) in named {
        let bytes = read_capped(path)?;
        h.update(rel.as_bytes());
        h.update([0u8]);
        h.update((bytes.len() as u64).to_be_bytes());
        h.update(&bytes);
    }
    Ok(hex::encode(h.finalize()))
}

/// The checked-out commit of a git clone at `root`, if readable.
fn git_revision(root: &Path) -> Option<String> {
    let git = root.join(".git");
    let head = fs::read_to_string(git.join("HEAD")).ok()?;
    let head = head.trim();
    let Some(r) = head.strip_prefix("ref: ") else {
        return Some(head.to_owned()).filter(|h| h.len() == 40);
    };
    if let Ok(id) = fs::read_to_string(git.join(r)) {
        return Some(id.trim().to_owned());
    }
    fs::read_to_string(git.join("packed-refs"))
        .ok()?
        .lines()
        .find_map(|l| l.strip_suffix(r).map(|id| id.trim().to_owned()))
}

#[derive(Deserialize)]
struct AuthorIn {
    id: String,
    name: Option<String>,
}

#[derive(Deserialize)]
struct ProjectIn {
    id: u64,
    slug: Option<String>,
    name: Option<String>,
    flag: Option<String>,
    supply: Option<u64>,
    #[serde(rename = "createdAt")]
    created_at: Option<String>,
    #[serde(rename = "mintOpensAt")]
    mint_opens_at: Option<String>,
    #[serde(rename = "thumbnailUri")]
    thumbnail_uri: Option<String>,
    #[serde(rename = "displayUri")]
    display_uri: Option<String>,
    #[serde(rename = "generativeUri")]
    generative_uri: Option<String>,
    #[serde(default)]
    tags: Option<Value>,
    author: Option<AuthorIn>,
}

#[derive(Deserialize)]
struct UsersIn {
    names: HashMap<String, String>,
}

#[derive(Deserialize)]
struct CollaboratorIn {
    id: String,
    name: Option<String>,
    share: Option<i64>,
}

#[derive(Deserialize)]
struct CollabProjectIn {
    contract: String,
    collaborators: Vec<CollaboratorIn>,
}

#[derive(Deserialize)]
struct CollaborationsIn {
    #[serde(rename = "byProject")]
    by_project: BTreeMap<String, CollabProjectIn>,
}

#[derive(Deserialize)]
struct DescriptionIn {
    d: Option<String>,
    c: Option<String>,
}

#[derive(Deserialize)]
struct ArtistIn {
    id: String,
    description: Option<String>,
    #[serde(rename = "avatarUri")]
    avatar_uri: Option<String>,
}

#[derive(Deserialize)]
struct RenameIn {
    name: String,
}

#[derive(Deserialize)]
struct RenamesIn {
    names: HashMap<String, RenameIn>,
}

#[derive(Deserialize)]
struct MetaIn {
    #[serde(rename = "generatedAt")]
    generated_at: Option<String>,
}

#[derive(Deserialize)]
struct ContractsIn {
    contracts: Vec<String>,
    #[serde(rename = "byProject")]
    by_project: HashMap<String, usize>,
}

#[derive(Deserialize)]
struct SeedContractIn {
    index: usize,
    address: String,
    min: u64,
    max: u64,
}

#[derive(Deserialize)]
struct SeedsMetaIn {
    contracts: Vec<SeedContractIn>,
}

#[derive(Deserialize)]
struct SeedChunkIn {
    contract: usize,
    address: String,
    from: u64,
    size: usize,
    seeds: Vec<Option<String>>,
    artifacts: Vec<Option<String>>,
}

fn non_blank(s: Option<String>) -> Option<String> {
    s.map(|s| s.trim().to_owned()).filter(|s| !s.is_empty())
}

fn artifact_scheme(uri: &str) -> bool {
    uri.starts_with("ipfs://") || uri.starts_with("onchfs://")
}

/// Token id -> project id, one dense vector per token contract.
struct TokenMap {
    min: u64,
    project: Vec<u32>,
    covered: Vec<bool>,
}

/// Imports the archive at `dir` (repository root or its `public/data`).
pub fn bootstrap(store: &mut Store, dir: &Path) -> Result<BootstrapReport, IngestError> {
    let l = layout(dir)?;
    let inputs = Inputs::find(&l)?;
    let sha = content_hash(&l, &inputs.all())?;
    let revision = l.root.as_deref().and_then(git_revision);
    let source = match &revision {
        Some(r) => format!("{SOURCE_NAME}@{} public/data", &r[..r.len().min(12)]),
        None => format!("{SOURCE_NAME} public/data"),
    };
    let mut report = BootstrapReport {
        content_sha256: sha.clone(),
        source: source.clone(),
        revision: revision.clone(),
        ..BootstrapReport::default()
    };
    if current_hash(store.conn_mut())?.as_deref() == Some(sha.as_str()) {
        return Ok(report);
    }

    let meta: MetaIn = parse(&inputs.meta)?;
    let users: UsersIn = parse(&inputs.users)?;
    let collabs: CollaborationsIn = parse(&inputs.collaborations)?;
    let renames: HashMap<String, String> = match &inputs.renames {
        Some(p) => parse::<RenamesIn>(p)?
            .names
            .into_iter()
            .map(|(k, v)| (k, v.name))
            .collect(),
        None => HashMap::new(),
    };
    let profiles: HashMap<String, ArtistIn> = match &inputs.artists {
        Some(p) => parse::<Vec<ArtistIn>>(p)?
            .into_iter()
            .map(|a| (a.id.clone(), a))
            .collect(),
        None => HashMap::new(),
    };
    let mut descriptions: HashMap<u64, DescriptionIn> = HashMap::new();
    for f in &inputs.descriptions {
        let chunk: HashMap<String, DescriptionIn> = parse(f)?;
        for (k, v) in chunk {
            if let Ok(id) = k.parse() {
                descriptions.insert(id, v);
            }
        }
    }
    let mut projects: Vec<ProjectIn> = Vec::new();
    for f in &inputs.projects {
        projects.extend(parse::<Vec<ProjectIn>>(f)?);
    }
    projects.sort_by_key(|p| p.id);
    let contracts: ContractsIn = parse(&inputs.contracts)?;
    let seeds_meta: SeedsMetaIn = parse(&inputs.seeds_meta)?;
    if contracts.contracts != TOKEN_CONTRACTS {
        return Err(source_err(
            "iterations/contracts.json",
            "contracts are not [GENTK v1, GENTK v2, fx(params)]",
        ));
    }

    let tx = store
        .conn_mut()
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .db()?;
    if current_hash(&tx)?.as_deref() == Some(sha.as_str()) {
        return Ok(report);
    }
    let now = now_iso();
    let reject = |report: &mut BootstrapReport, why: &'static str, n: usize| {
        if n > 0 {
            *report.rejected.entry(why).or_insert(0) += n;
        }
    };

    // Users: the on-chain registry, replaced whole.
    tx.execute("DELETE FROM users", []).db()?;
    {
        let mut ins = tx
            .prepare("INSERT INTO users (address, name, name_key) VALUES (?1, ?2, ?3)")
            .db()?;
        let mut sorted: Vec<(&String, &String)> = users.names.iter().collect();
        sorted.sort();
        for (address, name) in sorted {
            if !is_tezos_address(address) || name.trim().is_empty() {
                reject(&mut report, "user_invalid", 1);
                continue;
            }
            ins.execute(params![address, name.trim(), name_key(name)])
                .db()?;
            report.users += 1;
        }
    }
    let name_of = |address: &str, fallback: Option<&str>| -> String {
        renames
            .get(address)
            .or_else(|| users.names.get(address))
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .or(fallback.map(str::trim).filter(|s| !s.is_empty()))
            .unwrap_or(address)
            .to_owned()
    };

    // Collaboration contracts and their members, replaced whole.
    tx.execute("DELETE FROM collaborations", []).db()?;
    let mut members_of: HashMap<&str, Vec<(&str, Option<&str>)>> = HashMap::new();
    let mut collab_by_project: HashMap<u64, &str> = HashMap::new();
    {
        let mut ins = tx
            .prepare(
                "INSERT INTO collaborations (contract, position, member, share) \
                 VALUES (?1, ?2, ?3, ?4)",
            )
            .db()?;
        for (pid, c) in &collabs.by_project {
            let Ok(pid) = pid.parse::<u64>() else {
                reject(&mut report, "collaboration_bad_project", 1);
                continue;
            };
            if !is_tezos_address(&c.contract) {
                reject(&mut report, "collaboration_bad_contract", 1);
                continue;
            }
            collab_by_project.insert(pid, c.contract.as_str());
            if members_of.contains_key(c.contract.as_str()) {
                continue;
            }
            let mut members: Vec<(&str, Option<&str>)> = Vec::new();
            for m in &c.collaborators {
                if !is_tezos_address(&m.id) {
                    reject(&mut report, "collaborator_invalid", 1);
                    continue;
                }
                if members.iter().any(|(id, _)| *id == m.id) {
                    continue;
                }
                ins.execute(params![c.contract, members.len() as i64, m.id, m.share])
                    .db()?;
                members.push((&m.id, m.name.as_deref()));
            }
            members_of.insert(&c.contract, members);
        }
        report.collaborations = members_of.len();
    }

    // Artists and projects.
    let mut artists_done: HashSet<String> = HashSet::new();
    {
        let mut artist = tx
            .prepare(
                "INSERT INTO artists (id, name, name_key, website, address, description, \
                 avatar_uri, catalogue_source) VALUES (?1, ?2, ?3, NULL, ?1, ?4, ?5, ?6) \
                 ON CONFLICT (id) DO UPDATE SET name = excluded.name, \
                 name_key = excluded.name_key, address = excluded.address, \
                 description = excluded.description, avatar_uri = excluded.avatar_uri, \
                 catalogue_source = excluded.catalogue_source RETURNING seq",
            )
            .db()?;
        let mut project = tx
            .prepare(
                "INSERT INTO projects (id, title, title_key, artist_key, description, preview, \
                 contract, issuer, artifact_uri, editions, first_mint, last_mint, index_state, \
                 slug, supply, flag, hidden, source, author_address, thumbnail_uri, display_uri, \
                 generative_uri, tags, iteration_description, created_at, mint_opens_at, \
                 catalogue_source) VALUES (?1, ?2, ?3, ?4, ?5, NULL, ?6, NULL, ?7, NULL, NULL, \
                 NULL, 'not_indexed', ?8, ?9, ?10, ?11, 'frank', ?12, ?13, ?14, ?15, ?16, ?17, \
                 ?18, ?19, ?20) \
                 ON CONFLICT (id) DO UPDATE SET title = excluded.title, \
                 title_key = excluded.title_key, artist_key = excluded.artist_key, \
                 description = excluded.description, \
                 contract = coalesce(projects.contract, excluded.contract), \
                 artifact_uri = coalesce(projects.artifact_uri, excluded.artifact_uri), \
                 slug = excluded.slug, supply = excluded.supply, flag = excluded.flag, \
                 hidden = excluded.hidden, source = 'frank', \
                 author_address = excluded.author_address, \
                 thumbnail_uri = excluded.thumbnail_uri, display_uri = excluded.display_uri, \
                 generative_uri = excluded.generative_uri, tags = excluded.tags, \
                 iteration_description = excluded.iteration_description, \
                 created_at = excluded.created_at, mint_opens_at = excluded.mint_opens_at, \
                 catalogue_source = excluded.catalogue_source",
            )
            .db()?;
        let mut links_del = tx
            .prepare("DELETE FROM project_artists WHERE project_id = ?1")
            .db()?;
        let mut link = tx
            .prepare(
                "INSERT INTO project_artists (project_id, position, artist_id) VALUES (?1, ?2, ?3)",
            )
            .db()?;
        let mut seen_ids: HashSet<u64> = HashSet::new();
        for p in &projects {
            let Ok(id) = i64::try_from(p.id) else {
                reject(&mut report, "project_id_out_of_range", 1);
                continue;
            };
            if !seen_ids.insert(p.id) {
                reject(&mut report, "project_duplicate", 1);
                continue;
            }
            let Some(author) = p.author.as_ref().filter(|a| is_tezos_address(&a.id)) else {
                reject(&mut report, "project_without_author", 1);
                continue;
            };
            let collab: Option<&str> = collab_by_project.get(&p.id).copied().or_else(|| {
                members_of
                    .contains_key(author.id.as_str())
                    .then_some(author.id.as_str())
            });
            let members: Vec<(&str, Option<&str>)> = collab
                .and_then(|c| members_of.get(c))
                .filter(|m| !m.is_empty())
                .cloned()
                .unwrap_or_else(|| vec![(author.id.as_str(), author.name.as_deref())]);
            let mut names = Vec::with_capacity(members.len());
            for (address, fallback) in &members {
                let name = name_of(address, *fallback);
                if artists_done.insert((*address).to_owned()) {
                    let profile = profiles.get(*address);
                    artist
                        .query_row(
                            params![
                                address,
                                name,
                                name.to_lowercase(),
                                profile.and_then(|a| non_blank(a.description.clone())),
                                profile.and_then(|a| non_blank(a.avatar_uri.clone())),
                                source,
                            ],
                            |r| r.get::<_, i64>(0),
                        )
                        .db()?;
                }
                names.push(name);
            }
            let title = non_blank(p.name.clone()).unwrap_or_else(|| "Untitled".to_owned());
            let flag = non_blank(p.flag.clone());
            let hidden = flag.as_deref().is_some_and(|f| HIDDEN_FLAGS.contains(&f));
            let contract = contracts
                .by_project
                .get(&p.id.to_string())
                .and_then(|&ci| TOKEN_CONTRACTS.get(ci));
            let generative = non_blank(p.generative_uri.clone());
            let tags = match &p.tags {
                Some(Value::Array(items)) => Some(
                    serde_json::to_string(
                        &items
                            .iter()
                            .filter_map(Value::as_str)
                            .map(str::trim)
                            .filter(|t| !t.is_empty())
                            .collect::<Vec<_>>(),
                    )
                    .map_err(|e| IngestError::Io(e.to_string()))?,
                ),
                _ => None,
            };
            let desc = descriptions.remove(&p.id);
            project
                .execute(params![
                    id,
                    title,
                    title.to_lowercase(),
                    names[0].to_lowercase(),
                    desc.as_ref().and_then(|d| non_blank(d.d.clone())),
                    contract,
                    generative.as_deref().filter(|g| artifact_scheme(g)),
                    non_blank(p.slug.clone()),
                    p.supply.and_then(|s| u32::try_from(s).ok()),
                    flag,
                    hidden,
                    author.id,
                    non_blank(p.thumbnail_uri.clone()),
                    non_blank(p.display_uri.clone()),
                    generative,
                    tags,
                    desc.as_ref().and_then(|d| non_blank(d.c.clone())),
                    non_blank(p.created_at.clone()),
                    non_blank(p.mint_opens_at.clone()),
                    source,
                ])
                .db()?;
            links_del.execute([id]).db()?;
            for (i, (address, _)) in members.iter().enumerate() {
                link.execute(params![id, i as i64, address]).db()?;
            }
            add_content_roots(
                &tx,
                p.id,
                &[
                    generative.as_deref(),
                    p.thumbnail_uri.as_deref(),
                    p.display_uri.as_deref(),
                ],
            )?;
            report.projects += 1;
            report.hidden += usize::from(hidden);
        }
        report.artists = artists_done.len();
    }
    tx.execute(
        "UPDATE projects SET index_state = CASE WHEN EXISTS \
         (SELECT 1 FROM tokens t WHERE t.project_id = projects.id) \
         THEN 'indexed' ELSE 'not_indexed' END WHERE source = 'frank'",
        [],
    )
    .db()?;

    // Catalogue tokens: project from the iteration maps, seed and artifact from the seeds.
    let mut maps: Vec<TokenMap> = Vec::new();
    for (ci, address) in TOKEN_CONTRACTS.iter().enumerate() {
        let c = seeds_meta
            .contracts
            .iter()
            .find(|c| c.index == ci)
            .filter(|c| c.address == *address && c.min <= c.max && c.max - c.min < 1 << 26)
            .ok_or_else(|| source_err("seeds/meta.json", format!("contract {ci} range")))?;
        let len = (c.max - c.min + 1) as usize;
        maps.push(TokenMap {
            min: c.min,
            project: vec![NONE; len],
            covered: vec![false; len],
        });
    }
    for f in &inputs.maps {
        let map: BTreeMap<String, Vec<String>> = parse(f)?;
        for (pid, objkts) in map {
            if objkts.is_empty() {
                continue;
            }
            let (Ok(project), Some(&ci)) = (
                pid.parse::<u32>(),
                contracts
                    .by_project
                    .get(&pid)
                    .filter(|&&ci| ci < maps.len()),
            ) else {
                reject(&mut report, "token_without_contract", objkts.len());
                continue;
            };
            for o in objkts {
                let parsed = o
                    .split_once('-')
                    .and_then(|(prefix, n)| Some((prefix, n.parse::<u64>().ok()?)));
                let Some((prefix, token)) = parsed else {
                    reject(&mut report, "token_malformed", 1);
                    continue;
                };
                let prefix_ok = match prefix {
                    "FX0" => ci < 2,
                    "FX1" => ci == 2,
                    _ => false,
                };
                let m = &mut maps[ci];
                let slot = token
                    .checked_sub(m.min)
                    .and_then(|i| usize::try_from(i).ok())
                    .filter(|&i| i < m.project.len());
                match (prefix_ok, slot) {
                    (false, _) => reject(&mut report, "token_prefix_mismatch", 1),
                    (true, None) => reject(&mut report, "token_out_of_range", 1),
                    (true, Some(i)) if m.project[i] == NONE => m.project[i] = project,
                    (true, Some(i)) => {
                        if m.project[i] != CONFLICT {
                            m.project[i] = CONFLICT;
                            reject(&mut report, "token_in_two_projects", 2);
                        } else {
                            reject(&mut report, "token_in_two_projects", 1);
                        }
                    }
                }
            }
        }
    }
    {
        let mut upsert = tx
            .prepare(
                "INSERT INTO catalogue_tokens (contract, token_id, project_id, seed, artifact_uri) \
                 VALUES (?1, ?2, ?3, ?4, ?5) \
                 ON CONFLICT (contract, token_id) DO UPDATE SET \
                 project_id = excluded.project_id, seed = excluded.seed, \
                 artifact_uri = excluded.artifact_uri, \
                 verify_state = CASE WHEN project_id = excluded.project_id \
                   AND seed IS excluded.seed AND artifact_uri IS excluded.artifact_uri \
                   THEN verify_state WHEN chain_state = 'absent' THEN 'absent' \
                   ELSE 'unverified' END, \
                 verify_detail = CASE WHEN project_id = excluded.project_id \
                   AND seed IS excluded.seed AND artifact_uri IS excluded.artifact_uri \
                   THEN verify_detail ELSE NULL END",
            )
            .db()?;
        for (ci, f) in &inputs.seeds {
            let chunk: SeedChunkIn = parse(f)?;
            let file = f.display().to_string();
            if chunk.contract != *ci
                || chunk.address != TOKEN_CONTRACTS[*ci]
                || chunk.seeds.len() != chunk.size
                || chunk.artifacts.len() != chunk.size
            {
                return Err(source_err(
                    file,
                    "contract, address or array sizes disagree",
                ));
            }
            let m = &mut maps[*ci];
            for (i, (seed, artifact)) in chunk.seeds.into_iter().zip(chunk.artifacts).enumerate() {
                let token = chunk.from + i as u64;
                let slot = token
                    .checked_sub(m.min)
                    .and_then(|i| usize::try_from(i).ok())
                    .filter(|&i| i < m.project.len());
                let Some(slot) = slot else {
                    reject(&mut report, "seed_out_of_range", 1);
                    continue;
                };
                let project = m.project[slot];
                if project == NONE {
                    if seed.is_some() {
                        reject(&mut report, "seed_without_project", 1);
                    }
                    continue;
                }
                if project == CONFLICT || m.covered[slot] {
                    continue;
                }
                m.covered[slot] = true;
                let seed = non_blank(seed);
                report.without_seed += usize::from(seed.is_none());
                upsert
                    .execute(params![
                        TOKEN_CONTRACTS[*ci],
                        token as i64,
                        project,
                        seed,
                        non_blank(artifact)
                    ])
                    .db()?;
                report.catalogue_tokens += 1;
            }
        }
        for (ci, m) in maps.iter().enumerate() {
            for (slot, &project) in m.project.iter().enumerate() {
                if project == NONE || project == CONFLICT || m.covered[slot] {
                    continue;
                }
                upsert
                    .execute(params![
                        TOKEN_CONTRACTS[ci],
                        (m.min + slot as u64) as i64,
                        project,
                        None::<String>,
                        None::<String>
                    ])
                    .db()?;
                report.catalogue_tokens += 1;
                report.without_seed += 1;
            }
        }
    }

    // Changed catalogue rows are re-compared with stored chain data before anyone reads them,
    // the search indexes are rebuilt from shown names, and curated identities are re-applied
    // over the refreshed artists (they drain their own search changes).
    report.recompared = super::verify::recompare_all(&tx)?;
    crate::store::assign_names(&tx)?;
    crate::store::search::rebuild(&tx)?;
    report.identities = super::identity::apply(&tx)?;
    tx.execute(
        "INSERT INTO catalogue_snapshot (id, source, revision, generated_at, content_sha256, \
         imported_at) VALUES (1, ?1, ?2, ?3, ?4, ?5) ON CONFLICT (id) DO UPDATE SET \
         source = excluded.source, revision = excluded.revision, \
         generated_at = excluded.generated_at, content_sha256 = excluded.content_sha256, \
         imported_at = excluded.imported_at",
        params![source, revision, meta.generated_at, sha, now],
    )
    .db()?;
    let as_of = meta
        .generated_at
        .as_deref()
        .and_then(|g| g.get(..10))
        .unwrap_or(&now[..10])
        .to_owned();
    let has_snapshot = tx
        .query_row("SELECT 1 FROM snapshot WHERE id = 1", [], |_| Ok(()))
        .optional()
        .db()?
        .is_some();
    if has_snapshot {
        tx.execute(
            "UPDATE snapshot SET catalogue_source = ?1 WHERE id = 1",
            [&source],
        )
        .db()?;
    } else {
        tx.execute(
            "INSERT INTO snapshot (id, as_of, owner_source, catalogue_source, fixture_sha256, \
             imported_at) VALUES (1, ?1, 'none', ?2, ?3, ?4)",
            params![as_of, source, sha, now],
        )
        .db()?;
    }
    tx.commit().db()?;
    report.changed = true;
    Ok(report)
}

fn current_hash(c: &rusqlite::Connection) -> Result<Option<String>, IngestError> {
    Ok(c.query_row(
        "SELECT content_sha256 FROM catalogue_snapshot WHERE id = 1",
        [],
        |r| r.get(0),
    )
    .optional()
    .db()?)
}
