//! Escaped, server-rendered views for the light SEEDED archive.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use askama::Template;
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};

use super::search::SearchScope;
use crate::model::{
    AddressKind, Artist, ArtistRef, ArtistSummary, Collector, FeatureShare, IndexState,
    IndexStatus, Profile, ProfileHit, Project, ProjectMeta, ProjectSummary, Snapshot, Source,
    Token, Verification,
};
use crate::store::Page;

#[derive(serde::Serialize)]
pub struct PageInfo {
    pub page: u32,
    #[serde(rename = "first")]
    pub previous: Option<String>,
    pub next: Option<String>,
}

#[derive(serde::Serialize)]
pub struct ProjectsPage {
    pub content_origin: Option<String>,
    pub projects: Page<ProjectSummary>,
    pub artist: String,
    pub sort: String,
    pub project_volumes: BTreeMap<u64, Option<u64>>,
    pub pagination: PageInfo,
    pub snapshot: Snapshot,
}

#[derive(serde::Serialize)]
pub struct ProjectPage {
    #[serde(skip)]
    pub articles: String,
    pub metadata: ProjectMeta,
    #[serde(skip)]
    pub release: String,
    #[serde(skip)]
    pub history: String,
    pub query: String,
    pub trait_name: String,
    pub trait_value: String,
    pub project: Project,
    pub tokens: Page<Token>,
    pub snapshot: Snapshot,
    pub content_origin: Option<String>,
    pub pagination: PageInfo,
}

#[derive(serde::Serialize)]
pub struct TokenPage {
    #[serde(skip)]
    pub history: String,
    pub instructions: String,
    pub content_origin: Option<String>,
    pub token: Token,
    pub project: Project,
    pub shares: Vec<FeatureShare>,
    pub snapshot: Snapshot,
    pub previous: Option<Token>,
    pub next: Option<Token>,
}

#[derive(serde::Serialize)]
pub struct ArtistsPage {
    pub artists: Page<ArtistSummary>,
    pub avatars: HashMap<String, String>,
    /// Visible articles per artist address.
    pub article_counts: HashMap<String, u64>,
    pub content_origin: Option<String>,
    pub pagination: PageInfo,
    pub snapshot: Snapshot,
}

#[derive(serde::Serialize)]
pub(super) struct SearchPage {
    pub query: String,
    #[serde(serialize_with = "serialize_scope")]
    pub scope: SearchScope,
    pub sort: String,
    pub article_counts: HashMap<String, u64>,
    pub articles: serde_json::Value,
    #[serde(skip)]
    pub articles_html: String,
    pub project_volumes: BTreeMap<u64, Option<u64>>,
    pub projects: Page<ProjectSummary>,
    pub artists: Page<ArtistSummary>,
    pub collectors: Page<ProfileHit>,
    pub projects_pagination: PageInfo,
    pub artists_pagination: PageInfo,
    pub collectors_pagination: PageInfo,
    pub avatars: HashMap<String, String>,
    pub content_origin: Option<String>,
    pub snapshot: Snapshot,
}

#[derive(serde::Serialize)]
pub struct ProfilePage {
    #[serde(skip)]
    pub articles: String,
    pub profile: Profile,
    pub projects: Page<ProjectSummary>,
    pub tokens: Page<Token>,
    pub project_titles: BTreeMap<u64, String>,
    #[serde(skip)]
    pub project_artists: BTreeMap<u64, Vec<ArtistRef>>,
    pub snapshot: Snapshot,
    pub view: String,
    pub query: String,
    pub trait_name: String,
    pub trait_value: String,
    pub pagination: PageInfo,
    pub content_origin: Option<String>,
    pub own_profile: bool,
    /// Listings and offers of the own profile; 0 for other visitors.
    #[serde(skip)]
    pub market_count: u64,
}

#[derive(serde::Serialize)]
pub struct AboutPage {
    pub snapshot: Snapshot,
    pub status: IndexStatus,
}
#[derive(serde::Serialize)]
pub struct CollectorPage {
    pub collector: Option<Collector>,
    pub tokens: Page<Token>,
    pub project_titles: BTreeMap<u64, String>,
    pub artist: Option<Artist>,
    pub snapshot: Snapshot,
    pub content_origin: Option<String>,
    pub pagination: PageInfo,
}
pub struct ErrorPage {
    pub status: u16,
    pub title: String,
    pub message: String,
}

pub(super) struct Meta {
    pub(super) title: String,
    pub(super) active: &'static str,
    /// The header search text; only search results fill it.
    pub(super) query: String,
    /// The header search scope (`scope` parameter); empty searches every section.
    pub(super) scope: &'static str,
    pub(super) snapshot: String,
}

impl Meta {
    pub(super) fn new(title: impl Into<String>, active: &'static str, snapshot: &Snapshot) -> Self {
        Self {
            title: title.into(),
            active,
            query: String::new(),
            scope: "",
            snapshot: date(&snapshot.as_of),
        }
    }

    /// Header searches from this page cover `scope`'s sections.
    pub(super) fn scoped(mut self, scope: SearchScope) -> Self {
        self.scope = scope.name();
        self
    }
}

fn serialize_scope<S: serde::Serializer>(
    scope: &SearchScope,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(scope.name())
}

struct ArtistLink {
    name: String,
    url: String,
}
struct ProjectCard {
    id: u64,
    title: String,
    url: String,
    preview: Option<String>,
    artists: Vec<ArtistLink>,
    editions: String,
    published: String,
    volume: Option<String>,
}
struct TokenCard {
    autoplay: bool,
    fullscreen: bool,
    fullsize_url: String,
    render_url: Option<String>,
    title: String,
    url: String,
    preview: Option<String>,
    iteration: u32,
    token_id: u64,
    palette: String,
    minted: String,
    owner: String,
    owner_address: String,
    owner_url: Option<String>,
    seed: String,
    artists: Vec<ArtistLink>,
}
struct Trait {
    name: String,
    value: String,
    share: String,
}
/// One artist or collector in a people list (`person_cards.html`).
struct DirectoryEntry {
    name: String,
    url: String,
    /// Shown under the name for collectors; empty for artists.
    address: String,
    detail: String,
    avatar: Option<String>,
    initial: String,
}

/// The profile header and its view tabs (`profile_header.html`), shared by the profile and
/// market pages.
pub(super) struct ProfileHeader {
    pub(super) name: String,
    pub(super) name_source: &'static str,
    pub(super) address: String,
    pub(super) kind: &'static str,
    pub(super) url: String,
    pub(super) initial: String,
    pub(super) avatar: Option<String>,
    pub(super) bio: String,
    pub(super) links: Vec<String>,
    pub(super) project_count: u64,
    pub(super) token_count: u64,
    pub(super) own_profile: bool,
    /// Records on the Listings and offers page; 0 unless `own_profile`.
    pub(super) market_count: u64,
    /// The active tab: `projects`, `collected`, `market` or `edit`.
    pub(super) tab: &'static str,
}

/// An edition or project search form with optional trait filters (`collection_search.html`).
struct CollectionSearch {
    action: String,
    aria_label: &'static str,
    /// Prefix of the form's element ids.
    id: &'static str,
    label: &'static str,
    placeholder: &'static str,
    /// The `view` parameter to keep; empty for none.
    view: &'static str,
    query: String,
    traits: bool,
    trait_name: String,
    trait_value: String,
    filtered: bool,
    clear_url: String,
}

pub(super) fn encode(value: &str) -> String {
    utf8_percent_encode(value, NON_ALPHANUMERIC).to_string()
}
fn project_url(id: u64) -> String {
    format!("/project/{id}")
}
fn artist_url(id: &str) -> String {
    format!("/profile/{}", encode(id))
}
/// The profile URL of a Tezos address; empty for anything else.
fn profile_link(address: &str) -> String {
    if crate::store::is_tezos_address(address) {
        artist_url(address)
    } else {
        String::new()
    }
}
fn profile_initial(name: &str) -> String {
    name.chars()
        .next()
        .map_or_else(|| "?".into(), |letter| letter.to_uppercase().to_string())
}
fn profile_avatar(address: &str, uri: Option<&str>, origin: Option<&str>) -> Option<String> {
    let uri = uri?;
    if crate::store::is_tezos_address(address)
        && (crate::articles::parse::ipfs_uri(uri).is_some()
            || uri.starts_with("onchfs://")
            || super::artwork::avatar_supported(uri))
    {
        origin.map(|origin| format!("{origin}/profile-media/{}", encode(address)))
    } else {
        None
    }
}
fn token_url(token: &Token) -> String {
    format!("/token/{}/{}", encode(&token.contract), token.token_id)
}
fn token_source(token: &Token) -> String {
    format!(
        "https://objkt.com/tokens/{}/{}",
        encode(&token.contract),
        token.token_id
    )
}
fn project_source(id: u64) -> String {
    format!("https://objkt.com/collections/fxhash/projects/{id}")
}
pub(super) fn date(value: &str) -> String {
    value.split('T').next().unwrap_or(value).to_owned()
}
fn missing(value: Option<&str>) -> String {
    value
        .filter(|v| !v.is_empty())
        .unwrap_or("Not yet indexed")
        .to_owned()
}

fn preview(value: Option<&str>) -> Option<String> {
    let value = value?;
    if value.is_empty()
        || value.starts_with('/')
        || value.contains('\\')
        || value
            .split('/')
            .any(|part| part == "." || part == ".." || part.is_empty())
    {
        return None;
    }
    Some(format!(
        "/fixtures/{}",
        value.split('/').map(encode).collect::<Vec<_>>().join("/")
    ))
}

pub(super) fn image_preview(
    local: Option<&str>,
    remote: Option<&str>,
    origin: Option<&str>,
) -> Option<String> {
    preview(local).or_else(|| super::artwork::preview_url(origin?, remote?))
}

pub(super) fn website(value: Option<&str>) -> Option<String> {
    let value = value?;
    let uri = value.parse::<axum::http::Uri>().ok()?;
    if !matches!(uri.scheme_str(), Some("http" | "https"))
        || uri.host().is_none_or(str::is_empty)
        || uri.authority()?.as_str().contains('@')
    {
        return None;
    }
    Some(value.to_owned())
}

fn artists(artists: &[ArtistRef]) -> Vec<ArtistLink> {
    artists
        .iter()
        .map(|artist| ArtistLink {
            name: artist.name.clone(),
            url: artist
                .address
                .as_deref()
                .map(artist_url)
                .unwrap_or_default(),
        })
        .collect()
}

fn project_card(
    project: &ProjectSummary,
    origin: Option<&str>,
    volume: Option<String>,
) -> ProjectCard {
    ProjectCard {
        id: project.id,
        title: project.title.clone(),
        url: project_url(project.id),
        preview: image_preview(
            project.preview.as_deref(),
            project
                .thumbnail_uri
                .as_deref()
                .or(project.display_uri.as_deref()),
            origin,
        ),
        artists: artists(&project.artists),
        editions: project
            .editions
            .map_or_else(|| "Not yet indexed".into(), |count| count.to_string()),
        published: project
            .published
            .as_deref()
            .filter(|s| !s.is_empty())
            .map_or_else(|| "Not yet indexed".into(), date),
        volume,
    }
}

/// Project cards; the volume metric shows only under the volume sort.
fn project_cards(
    projects: &[ProjectSummary],
    sort: &str,
    volumes: &BTreeMap<u64, Option<u64>>,
    origin: Option<&str>,
) -> Vec<ProjectCard> {
    projects
        .iter()
        .map(|project| {
            let volume = (sort == "volume").then(|| volume_label(volumes, project.id));
            project_card(project, origin, volume)
        })
        .collect()
}

fn plural(count: u64) -> &'static str {
    if count == 1 { "" } else { "s" }
}

fn artist_entries(
    artists: &[ArtistSummary],
    avatars: &HashMap<String, String>,
    article_counts: &HashMap<String, u64>,
    origin: Option<&str>,
) -> Vec<DirectoryEntry> {
    artists
        .iter()
        .map(|artist| {
            let projects = u64::from(artist.project_count);
            let articles = article_counts.get(&artist.id).copied().unwrap_or(0);
            let mut detail = format!("{projects} project{}", plural(projects));
            if articles > 0 {
                detail.push_str(&format!(" · {articles} article{}", plural(articles)));
            }
            DirectoryEntry {
                name: artist.name.clone(),
                url: profile_link(&artist.id),
                address: String::new(),
                detail,
                avatar: profile_avatar(
                    &artist.id,
                    avatars.get(&artist.id).map(String::as_str),
                    origin,
                ),
                initial: profile_initial(&artist.name),
            }
        })
        .collect()
}

fn collector_entries(
    collectors: &[ProfileHit],
    avatars: &HashMap<String, String>,
    origin: Option<&str>,
) -> Vec<DirectoryEntry> {
    collectors
        .iter()
        .map(|profile| DirectoryEntry {
            name: profile.name.clone(),
            url: format!("{}?view=collected", artist_url(&profile.address)),
            address: profile.address.clone(),
            detail: format!("{} held", profile.held),
            avatar: profile_avatar(
                &profile.address,
                avatars.get(&profile.address).map(String::as_str),
                origin,
            ),
            initial: profile_initial(&profile.name),
        })
        .collect()
}

pub(super) fn profile_header(
    profile: &Profile,
    origin: Option<&str>,
    // Some(number of listings and offers) only on the connected address's own profile.
    market_count: Option<u64>,
    tab: &'static str,
) -> ProfileHeader {
    let name = profile
        .name
        .clone()
        .unwrap_or_else(|| short_address(&profile.address));
    let mut links = BTreeSet::new();
    for url in profile.website.iter().chain(profile.links.iter()) {
        if let Some(url) = website(Some(url)) {
            links.insert(url);
        }
    }
    ProfileHeader {
        name_source: match profile.name_source.as_deref() {
            Some("claimed") => "Name set locally by this address",
            Some("curated") => "Name curated by the archive",
            Some("fxhash") => "Name from the fxhash registry",
            Some("objkt") => "Name from objkt",
            Some("tzprofiles") => "Name from Tezos Profiles",
            Some("known") => "Known address label",
            _ => "",
        },
        address: profile.address.clone(),
        kind: match profile.kind {
            AddressKind::Account => "Profile",
            AddressKind::Contract => "Contract holder",
            AddressKind::Escrow => "Marketplace contract",
            AddressKind::Burn => "Burn address",
        },
        url: artist_url(&profile.address),
        initial: profile_initial(&name),
        avatar: profile_avatar(&profile.address, profile.avatar_uri.as_deref(), origin),
        bio: crate::markup::render(profile.bio.as_deref().unwrap_or("")),
        links: links.into_iter().collect(),
        project_count: u64::from(profile.project_count),
        token_count: profile.held,
        own_profile: market_count.is_some(),
        market_count: market_count.unwrap_or(0),
        tab,
        name,
    }
}

/// The owner shown on token pages and cards: name (or address) and address. A listed
/// edition's owner is its seller.
struct OwnershipView {
    name: String,
    address: String,
    url: Option<String>,
}

fn ownership(token: &Token) -> OwnershipView {
    let Some(snapshot) = &token.owner else {
        return OwnershipView {
            name: "Not yet indexed".into(),
            address: String::new(),
            url: None,
        };
    };
    let person = if snapshot.burned {
        return OwnershipView {
            name: "Burned".into(),
            address: snapshot.owner.address.clone(),
            url: None,
        };
    } else if snapshot.escrow.is_some() {
        snapshot.listed_by.as_ref()
    } else {
        Some(&snapshot.owner)
    };
    OwnershipView {
        name: person.map_or_else(
            || "Seller not yet resolved".into(),
            |person| {
                person
                    .alias
                    .clone()
                    .unwrap_or_else(|| person.address.clone())
            },
        ),
        address: person.map_or_else(
            || snapshot.owner.address.clone(),
            |person| person.address.clone(),
        ),
        url: person.map(|person| artist_url(&person.address)),
    }
}

fn token_card(
    token: &Token,
    project_title: Option<&str>,
    project_artists: &[ArtistRef],
    origin: Option<&str>,
) -> TokenCard {
    let owner = ownership(token);
    let title = project_title.map_or_else(
        || format!("NFT #{}", token.iteration),
        |title| format!("{title} #{}", token.iteration),
    );
    TokenCard {
        autoplay: false,
        fullscreen: false,
        fullsize_url: format!("{}/full?seed={}", token_url(token), encode(&token.seed)),
        render_url: None,
        title,
        url: token_url(token),
        preview: image_preview(
            token.preview.as_deref(),
            token
                .display_uri
                .as_deref()
                .or(token.thumbnail_uri.as_deref()),
            origin,
        ),
        iteration: token.iteration,
        token_id: token.token_id,
        palette: token
            .features
            .iter()
            .find(|(name, _)| name == "Palette")
            .map_or_else(String::new, |(_, value)| value.clone()),
        minted: date(&token.minted),
        owner: owner.name,
        owner_address: owner.address,
        owner_url: owner.url,
        seed: missing(Some(&token.seed)),
        artists: artists(project_artists),
    }
}

fn short_address(address: &str) -> String {
    let chars: Vec<_> = address.chars().collect();
    if chars.len() <= 20 {
        address.into()
    } else {
        format!(
            "{}…{}",
            chars[..8].iter().collect::<String>(),
            chars[chars.len() - 6..].iter().collect::<String>()
        )
    }
}

fn playable_card(
    token: &Token,
    title: &str,
    project_artists: &[ArtistRef],
    origin: Option<&str>,
) -> TokenCard {
    let mut card = token_card(token, Some(title), project_artists, origin);
    card.render_url = origin
        .filter(|_| !token.seed.is_empty())
        .and_then(|origin| crate::player::render_url(origin, token).ok());
    card
}

#[derive(Template)]
#[template(path = "artwork_full.html")]
struct FullArtworkTemplate {
    card: TokenCard,
}

pub(super) fn render_full_artwork(
    token: &Token,
    title: &str,
    origin: &str,
) -> askama::Result<String> {
    let mut card = playable_card(token, title, &[], Some(origin));
    card.autoplay = true;
    card.fullscreen = true;
    FullArtworkTemplate { card }.render()
}

/// One project or edition card embedded in an article body, as listings show it.
#[derive(Template)]
#[template(path = "article_embed.html")]
struct EmbedTemplate {
    projects: Vec<ProjectCard>,
    tokens: Vec<TokenCard>,
}

pub(super) fn render_embedded_project(
    project: &ProjectSummary,
    origin: Option<&str>,
) -> askama::Result<String> {
    EmbedTemplate {
        projects: vec![project_card(project, origin, None)],
        tokens: vec![],
    }
    .render()
}

pub(super) fn render_embedded_token(
    token: &Token,
    project_title: &str,
    project_artists: &[ArtistRef],
    origin: Option<&str>,
) -> askama::Result<String> {
    EmbedTemplate {
        projects: vec![],
        tokens: vec![token_card(
            token,
            Some(project_title),
            project_artists,
            origin,
        )],
    }
    .render()
}

#[derive(Template)]
#[template(path = "projects.html")]
struct ProjectsTemplate {
    meta: Meta,
    cards: Vec<ProjectCard>,
    total: u64,
    artist: String,
    sort: String,
    pagination: PageInfo,
}

fn volume_label(volumes: &BTreeMap<u64, Option<u64>>, id: u64) -> String {
    volumes
        .get(&id)
        .and_then(|volume| *volume)
        .map_or_else(|| "Not recorded".into(), whole_tez)
}

fn whole_tez(mutez: u64) -> String {
    let digits = (mutez / 1_000_000).to_string();
    let mut result = String::new();
    for (index, digit) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index) % 3 == 0 {
            result.push(',');
        }
        result.push(digit);
    }
    result.push_str(" tez");
    result
}

pub fn render_projects(page: ProjectsPage) -> askama::Result<String> {
    ProjectsTemplate {
        meta: Meta::new("Projects", "projects", &page.snapshot),
        cards: project_cards(
            &page.projects.items,
            &page.sort,
            &page.project_volumes,
            page.content_origin.as_deref(),
        ),
        total: page.projects.total,
        artist: page.artist,
        sort: page.sort,
        pagination: page.pagination,
    }
    .render()
}

#[derive(Template)]
#[template(path = "project.html")]
struct ProjectTemplate {
    articles: String,
    release: String,
    history: String,
    search: CollectionSearch,
    meta: Meta,
    project: Project,
    preview: Option<String>,
    source: String,
    artists: Vec<ArtistLink>,
    minted: String,
    published: String,
    issuer: String,
    artist_addresses: Vec<ArtistLink>,
    runtime: String,
    tags: Vec<String>,
    description: String,
    contract: String,
    artifact: String,
    indexed: bool,
    cards: Vec<TokenCard>,
    total: u64,
    pagination: PageInfo,
}

pub fn render_project(page: ProjectPage) -> askama::Result<String> {
    let url = project_url(page.project.id);
    ProjectTemplate {
        articles: page.articles,
        release: page.release,
        history: page.history,
        search: CollectionSearch {
            action: format!("{url}#editions"),
            aria_label: "Search project NFTs",
            id: "edition",
            label: "NFT number or owner",
            placeholder: "#12 or owner name",
            view: "",
            filtered: !page.query.trim().is_empty() || !page.trait_name.is_empty(),
            clear_url: format!("{url}#editions"),
            query: page.query,
            traits: true,
            trait_name: page.trait_name,
            trait_value: page.trait_value,
        },
        meta: Meta::new(&page.project.title, "projects", &page.snapshot),
        source: project_source(page.project.id),
        artists: artists(&page.project.artists),
        minted: match (
            &page.project.index_state,
            page.project.editions,
            page.project.supply,
        ) {
            (IndexState::Indexed, Some(count), Some(supply)) => format!("{count} / {supply}"),
            (IndexState::Indexed, Some(count), None) => count.to_string(),
            _ => "Not yet indexed".into(),
        },
        published: page
            .metadata
            .published
            .as_deref()
            .filter(|s| !s.is_empty())
            .map_or_else(|| "Not yet indexed".into(), date),
        issuer: missing(page.project.issuer.as_deref()),
        artist_addresses: page
            .metadata
            .artist_addresses
            .iter()
            .map(|address| ArtistLink {
                name: address.clone(),
                url: profile_link(address),
            })
            .collect(),
        runtime: match page.project.contract.as_deref() {
            Some(crate::ingest::FX_PARAMS) => "fxhash snippet · params: yes",
            Some(crate::ingest::GENTK_V1 | crate::ingest::GENTK_V2) => {
                "fxhash snippet · params: no"
            }
            _ => "Not yet indexed",
        }
        .into(),
        tags: page.metadata.tags,
        description: crate::markup::render(&missing(page.project.description.as_deref())),
        contract: missing(page.project.contract.as_deref()),
        artifact: missing(
            page.metadata
                .generative_uri
                .as_deref()
                .or(page.project.artifact_uri.as_deref()),
        ),
        indexed: page.project.index_state == IndexState::Indexed,
        cards: page
            .tokens
            .items
            .iter()
            .map(|token| {
                token_card(
                    token,
                    Some(&page.project.title),
                    &page.project.artists,
                    page.content_origin.as_deref(),
                )
            })
            .collect(),
        total: page.tokens.total,
        pagination: page.pagination,
        preview: image_preview(
            page.project.preview.as_deref(),
            page.project
                .display_uri
                .as_deref()
                .or(page.project.thumbnail_uri.as_deref()),
            page.content_origin.as_deref(),
        ),
        project: page.project,
    }
    .render()
}

#[derive(Template)]
#[template(path = "token.html")]
struct TokenTemplate {
    history: String,
    description: String,
    instructions: String,
    meta: Meta,
    token: Token,
    card: TokenCard,
    project_title: String,
    project_url: String,
    artists: Vec<ArtistLink>,
    previous: Option<TokenCard>,
    next: Option<TokenCard>,
    source: String,
    traits: Vec<Trait>,
    record_source: &'static str,
    verification: &'static str,
}

pub fn render_token(page: TokenPage) -> askama::Result<String> {
    let mut card = playable_card(
        &page.token,
        &page.project.title,
        &page.project.artists,
        page.content_origin.as_deref(),
    );
    card.autoplay = true;
    let traits = page
        .token
        .features
        .iter()
        .map(|(name, value)| {
            let share = page
                .shares
                .iter()
                .find(|share| {
                    &share.name == name
                        && &share.value == value
                        && share.total > 0
                        && share.count <= share.total
                })
                .map_or_else(
                    || "Share not yet indexed".into(),
                    |share| {
                        format!(
                            "{} of {} indexed NFTs · {:.0}%",
                            share.count,
                            share.total,
                            f64::from(share.count) / f64::from(share.total) * 100.0
                        )
                    },
                );
            Trait {
                name: name.clone(),
                value: value.clone(),
                share,
            }
        })
        .collect();
    TokenTemplate {
        history: page.history,
        description: crate::markup::render(page.project.description.as_deref().unwrap_or("")),
        instructions: crate::markup::render(&page.instructions),
        record_source: match page.token.source {
            Source::Fixture => "Fixture snapshot",
            Source::Frank => "Frank Force catalogue",
            Source::Tzkt => "Tezos via TzKT",
        },
        verification: match page.token.verification {
            Verification::Unverified => "Not yet checked",
            Verification::ChainMatch => "Catalogue matches chain record",
            Verification::Mismatch => "Catalogue and chain differ",
        },
        meta: Meta::new(&card.title, "projects", &page.snapshot),
        card,
        project_url: project_url(page.project.id),
        project_title: page.project.title.clone(),
        artists: artists(&page.project.artists),
        previous: page.previous.as_ref().map(|token| {
            token_card(
                token,
                Some(&page.project.title),
                &page.project.artists,
                page.content_origin.as_deref(),
            )
        }),
        next: page.next.as_ref().map(|token| {
            token_card(
                token,
                Some(&page.project.title),
                &page.project.artists,
                page.content_origin.as_deref(),
            )
        }),
        source: token_source(&page.token),
        traits,
        token: page.token,
    }
    .render()
}

#[derive(Template)]
#[template(path = "artists.html")]
struct ArtistsTemplate {
    meta: Meta,
    people: Vec<DirectoryEntry>,
    total: u64,
    pagination: PageInfo,
}

pub fn render_artists(page: ArtistsPage) -> askama::Result<String> {
    ArtistsTemplate {
        meta: Meta::new("Artists", "artists", &page.snapshot).scoped(SearchScope::People),
        people: artist_entries(
            &page.artists.items,
            &page.avatars,
            &page.article_counts,
            page.content_origin.as_deref(),
        ),
        total: page.artists.total,
        pagination: page.pagination,
    }
    .render()
}

#[derive(Template)]
#[template(path = "search.html")]
struct SearchTemplate {
    meta: Meta,
    query: String,
    sort: String,
    articles_html: String,
    articles_total: u64,
    articles_available: bool,
    cards: Vec<ProjectCard>,
    artists: Vec<DirectoryEntry>,
    collectors: Vec<DirectoryEntry>,
    projects_total: u64,
    artists_total: u64,
    collectors_total: u64,
    projects_pagination: PageInfo,
    artists_pagination: PageInfo,
    collectors_pagination: PageInfo,
}

pub(super) fn render_search(page: SearchPage) -> askama::Result<String> {
    let mut meta = Meta::new("Search", page.scope.tab(), &page.snapshot).scoped(page.scope);
    meta.query = page.query.clone();
    SearchTemplate {
        meta,
        query: page.query,
        cards: project_cards(
            &page.projects.items,
            &page.sort,
            &page.project_volumes,
            page.content_origin.as_deref(),
        ),
        sort: page.sort,
        articles_total: page.articles["total"].as_u64().unwrap_or(0),
        articles_available: page.articles["available"].as_bool().unwrap_or(false),
        articles_html: page.articles_html,
        artists: artist_entries(
            &page.artists.items,
            &page.avatars,
            &page.article_counts,
            page.content_origin.as_deref(),
        ),
        collectors: collector_entries(
            &page.collectors.items,
            &page.avatars,
            page.content_origin.as_deref(),
        ),
        projects_total: page.projects.total,
        artists_total: page.artists.total,
        collectors_total: page.collectors.total,
        projects_pagination: page.projects_pagination,
        artists_pagination: page.artists_pagination,
        collectors_pagination: page.collectors_pagination,
    }
    .render()
}

#[derive(Template)]
#[template(path = "artist.html")]
struct ProfileTemplate {
    articles: String,
    meta: Meta,
    header: ProfileHeader,
    search: CollectionSearch,
    collected_url: String,
    cards: Vec<ProjectCard>,
    tokens: Vec<TokenCard>,
    result_count: u64,
    listed: u64,
    collected: bool,
    pagination: PageInfo,
}

pub fn render_profile(page: ProfilePage) -> askama::Result<String> {
    let collected = page.view == "collected";
    let view = if collected { "collected" } else { "projects" };
    let header = profile_header(
        &page.profile,
        page.content_origin.as_deref(),
        page.own_profile.then_some(page.market_count),
        view,
    );
    let search = CollectionSearch {
        action: format!("{}#generative-work", header.url),
        aria_label: if collected {
            "Search collected NFTs"
        } else {
            "Search profile projects"
        },
        id: "profile",
        label: "Title or artist",
        placeholder: if collected {
            "Search NFTs"
        } else {
            "Search projects"
        },
        view,
        filtered: !page.query.is_empty()
            || !page.trait_name.is_empty()
            || !page.trait_value.is_empty(),
        clear_url: format!("{}?view={view}#generative-work", header.url),
        query: page.query,
        traits: collected,
        trait_name: page.trait_name,
        trait_value: page.trait_value,
    };
    ProfileTemplate {
        articles: page.articles,
        meta: Meta::new(&header.name, "artists", &page.snapshot),
        collected_url: format!("{}?view=collected", header.url),
        cards: page
            .projects
            .items
            .iter()
            .map(|project| project_card(project, page.content_origin.as_deref(), None))
            .collect(),
        tokens: page
            .tokens
            .items
            .iter()
            .map(|token| {
                token_card(
                    token,
                    page.project_titles
                        .get(&token.project_id)
                        .map(String::as_str),
                    page.project_artists
                        .get(&token.project_id)
                        .map_or(&[][..], Vec::as_slice),
                    page.content_origin.as_deref(),
                )
            })
            .collect(),
        result_count: if collected {
            page.tokens.total
        } else {
            page.projects.total
        },
        listed: page.profile.listed,
        collected,
        pagination: page.pagination,
        header,
        search,
    }
    .render()
}

/// The address form; collections render on profiles.
#[derive(Template)]
#[template(path = "collector.html")]
struct CollectorTemplate {
    meta: Meta,
}

pub fn render_collector(page: CollectorPage) -> askama::Result<String> {
    CollectorTemplate {
        meta: Meta::new("Collectors", "", &page.snapshot).scoped(SearchScope::People),
    }
    .render()
}

#[derive(Template)]
#[template(path = "about.html")]
struct AboutTemplate {
    meta: Meta,
    status: IndexStatus,
}

pub fn render_about(page: AboutPage) -> askama::Result<String> {
    AboutTemplate {
        status: page.status,
        meta: Meta::new("About seeded.art", "about", &page.snapshot),
    }
    .render()
}

#[derive(Template)]
#[template(path = "error.html")]
struct ErrorTemplate {
    meta: Meta,
    status: u16,
    title: String,
    message: String,
}

pub fn render_error(page: ErrorPage) -> askama::Result<String> {
    ErrorTemplate {
        meta: Meta {
            title: page.title.clone(),
            active: "",
            query: String::new(),
            scope: "",
            snapshot: String::new(),
        },
        status: page.status,
        title: page.title,
        message: page.message,
    }
    .render()
}

#[cfg(test)]
mod tests {
    use super::{ErrorPage, preview, render_error, website};

    #[test]
    fn external_text_is_escaped_in_html_and_titles() {
        let html = render_error(ErrorPage {
            status: 404,
            title: "<script>alert(1)</script>".into(),
            message: "<img src=x onerror=alert(1)> & missing".into(),
        })
        .unwrap();
        assert!(!html.contains("<script>"));
        assert!(!html.contains("<img src=x"));
        assert!(html.contains("&#60;script&#62;"));
        assert!(html.contains("&#38; missing"));
    }

    #[test]
    fn previews_stay_under_fixture_route() {
        assert_eq!(
            preview(Some("previews/a b.webp")),
            Some("/fixtures/previews/a%20b%2Ewebp".into())
        );
        for path in [
            "../secret",
            "/absolute",
            "a/./b",
            "a\\b",
            "https://remote/a",
        ] {
            assert!(preview(Some(path)).is_none());
        }
    }

    #[test]
    fn websites_accept_only_absolute_http_without_credentials() {
        assert_eq!(
            website(Some("https://emptyname.org")),
            Some("https://emptyname.org".into())
        );
        for url in [
            "javascript:alert(1)",
            "//example.org",
            "https://",
            "https://name:password@example.org",
            "https://bad host/",
        ] {
            assert!(website(Some(url)).is_none());
        }
    }
}
