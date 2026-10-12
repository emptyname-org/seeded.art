//! Read-side domain types returned by the store.

use serde::Serialize;

/// Whether a project's tokens are in the archive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IndexState {
    Indexed,
    NotIndexed,
}

impl IndexState {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            IndexState::Indexed => "indexed",
            IndexState::NotIndexed => "not_indexed",
        }
    }

    pub(crate) fn parse(s: &str) -> Option<IndexState> {
        match s {
            "indexed" => Some(IndexState::Indexed),
            "not_indexed" => Some(IndexState::NotIndexed),
            _ => None,
        }
    }
}

/// Where a record came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Source {
    /// The bundled fixture snapshot.
    Fixture,
    /// Frank Force's fxhash archive (KilledByAPixel/fxhashArchive).
    Frank,
    /// Tezos chain data read through TzKT.
    Tzkt,
}

impl Source {
    pub(crate) fn parse(s: &str) -> Option<Source> {
        match s {
            "fixture" => Some(Source::Fixture),
            "frank" => Some(Source::Frank),
            "tzkt" => Some(Source::Tzkt),
            _ => None,
        }
    }
}

/// A token's standing against the chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Verification {
    /// Not read from chain (fixture data).
    Unverified,
    /// Read from chain; where the catalogue has a record, it agrees.
    ChainMatch,
    /// Read from chain, and the catalogue record disagrees (see `seeded-index verify`).
    Mismatch,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ArtistRef {
    pub id: String,
    pub name: String,
    pub address: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ProjectSummary {
    pub id: u64,
    pub title: String,
    pub artists: Vec<ArtistRef>,
    pub preview: Option<String>,
    /// Indexed (minted) tokens; `None` when not indexed.
    pub editions: Option<u32>,
    pub index_state: IndexState,
    pub slug: Option<String>,
    /// Catalogued maximum supply.
    pub supply: Option<u32>,
    /// Original `ipfs://` or `onchfs://` URI.
    pub thumbnail_uri: Option<String>,
    /// Original `ipfs://` or `onchfs://` URI.
    pub display_uri: Option<String>,
    /// When the project was published on fxhash (UTC, ISO 8601).
    pub published: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Project {
    pub id: u64,
    pub title: String,
    pub artists: Vec<ArtistRef>,
    pub description: Option<String>,
    pub preview: Option<String>,
    pub contract: Option<String>,
    pub issuer: Option<String>,
    pub artifact_uri: Option<String>,
    pub editions: Option<u32>,
    pub first_mint: Option<String>,
    pub last_mint: Option<String>,
    pub index_state: IndexState,
    pub slug: Option<String>,
    pub supply: Option<u32>,
    pub thumbnail_uri: Option<String>,
    pub display_uri: Option<String>,
    pub source: Source,
}

/// Catalogue metadata of a visible project, beside [`Project`].
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ProjectMeta {
    pub id: u64,
    /// When the project was published on fxhash (UTC, ISO 8601).
    pub published: Option<String>,
    pub mint_opens_at: Option<String>,
    pub tags: Vec<String>,
    /// Original `ipfs://` or `onchfs://` URI.
    pub generative_uri: Option<String>,
    /// Catalogue author: an artist, or a collaboration contract (KT1).
    pub author_address: Option<String>,
    /// Credited artists in order, collaboration members included; artists without an
    /// address (fixture data) are left out.
    pub artist_addresses: Vec<String>,
}

/// Trading volume of a visible project from chain sales (`seeded-index volumes`): every
/// trade priced in tez or wXTZ (1:1), primary mints plus secondary sales, offer accepts and
/// auctions. Amounts in mutez.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProjectVolume {
    pub project_id: u64,
    /// `primary_mutez + secondary_mutez`.
    pub volume_mutez: u64,
    pub primary_mutez: u64,
    pub secondary_mutez: u64,
    /// Priced trades, free mints included.
    pub sales_count: u64,
    /// Trades paid in another token, or without an attributable price; not in the volume.
    pub unpriced_count: u64,
    /// When it was computed (UTC, `YYYY-MM-DDTHH:MM:SSZ`).
    pub computed_at: String,
    /// Chain level every sales stream had completed then; `None` if one had not.
    pub sales_level: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Owner {
    pub address: String,
    pub alias: Option<String>,
}

/// An owner as seen in one snapshot; not live chain state. For chain data `as_of` is the
/// sync time the balance was read and `source` is `tzkt`.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct OwnerSnapshot {
    /// The ledger owner; a marketplace contract while the token is listed there.
    pub owner: Owner,
    pub as_of: String,
    pub source: String,
    /// The seller, when `owner` is a marketplace escrow and its listing storage names exactly
    /// one live listing for the token. `None` while unresolved: never guessed.
    pub listed_by: Option<Owner>,
    /// Name of the marketplace escrow contract when `owner` is one, e.g. "fxhash marketplace v2".
    pub escrow: Option<String>,
    /// `owner` is the burn address: the token is out of circulation and credits no collector.
    pub burned: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Token {
    pub contract: String,
    pub token_id: u64,
    pub project_id: u64,
    pub iteration: u32,
    pub seed: String,
    pub minter: String,
    pub minted: String,
    pub artifact_uri: String,
    pub preview: Option<String>,
    pub features: Vec<(String, String)>,
    pub owner: Option<OwnerSnapshot>,
    pub display_uri: Option<String>,
    pub thumbnail_uri: Option<String>,
    /// fx(params) input bytes as hex without `0x`; `None` for GENTK v1/v2.
    pub params: Option<String>,
    pub verification: Verification,
    pub source: Source,
}

/// `count` of `total` indexed tokens carry feature `name` = `value`.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct FeatureShare {
    pub name: String,
    pub value: String,
    pub count: u32,
    pub total: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Artist {
    pub id: String,
    pub name: String,
    pub website: Option<String>,
    pub aliases: Vec<String>,
    /// Chain artists use their address as id; fixture ids have none.
    pub address: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ArtistSummary {
    pub id: String,
    pub name: String,
    pub project_count: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Snapshot {
    pub as_of: String,
    pub owner_source: String,
    pub catalogue_source: String,
    pub fixture_sha256: String,
    pub imported_at: String,
}

/// An address with visible holdings in the index.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Collector {
    pub address: String,
    /// fxhash user name from the on-chain user registry.
    pub alias: Option<String>,
    /// Held directly plus listed in a marketplace escrow by this address.
    pub token_count: u64,
    /// Of `token_count`, tokens listed in an escrow by this address.
    pub listed: u64,
}

/// Index coverage. Flagged (hidden) projects appear only in `projects_flagged_hidden`.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct IndexStatus {
    /// Visible projects.
    pub projects_total: u64,
    /// Visible projects with tokens in the index.
    pub projects_indexed: u64,
    pub projects_flagged_hidden: u64,
    /// Visible tokens.
    pub tokens_total: u64,
    /// Visible tokens with [`Verification::ChainMatch`].
    pub tokens_verified: u64,
    /// Visible tokens with [`Verification::Mismatch`].
    pub tokens_mismatch: u64,
    pub contracts: Vec<ContractStatus>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ContractStatus {
    pub contract: String,
    /// Visible tokens.
    pub tokens: u64,
    /// TzKT level up to which every change is applied.
    pub last_level: Option<u64>,
    /// When that level was reached (UTC, ISO 8601).
    pub last_sync: Option<String>,
}

/// What an address is, for labelling a profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AddressKind {
    /// An implicit account (tz1..tz4).
    Account,
    /// An originated contract (KT1) that is not a known escrow.
    Contract,
    /// A marketplace contract holding listed tokens.
    Escrow,
    /// The burn address.
    Burn,
}

/// One address: artist and collector merged. Every field is resolved field by field:
/// local edit > curated identity > fxhash > objkt > Tezos Profiles. Imported names that more
/// than one address carries (case-insensitively) are never returned.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Profile {
    pub address: String,
    pub name: Option<String>,
    /// Where `name` came from: `claimed` (local edit), `curated` (identity), `fxhash`,
    /// `objkt`, `tzprofiles`, or `known` (escrow and burn addresses).
    pub name_source: Option<String>,
    pub bio: Option<String>,
    pub website: Option<String>,
    /// Absolute http(s) URLs.
    pub links: Vec<String>,
    /// Original URI (`ipfs://` or `https://`); not fetched by the store.
    pub avatar_uri: Option<String>,
    pub kind: AddressKind,
    /// Visible projects credited to the address.
    pub project_count: u32,
    /// Visible tokens credited to the address: held directly plus listed in an escrow.
    pub held: u64,
    /// Of `held`, tokens listed in a marketplace escrow by the address.
    pub listed: u64,
}

/// One profile search result (see `Store::profiles_search`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProfileHit {
    pub address: String,
    /// The shown name; the address itself when an address typed exactly shows none.
    pub name: String,
    /// `Account` or `Contract`: escrow and burn addresses are never found.
    pub kind: AddressKind,
    /// Visible projects credited to the address.
    pub project_count: u32,
    /// Visible tokens credited to the address (see [`Profile::held`]).
    pub held: u64,
}

/// A profile edit by the address owner. `None` and empty `links` keep the imported value;
/// `bio: Some("")` hides the imported bio.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct ProfileEdit {
    pub name: Option<String>,
    pub bio: Option<String>,
    pub links: Vec<String>,
}

/// The signed message behind an edit, stored for audit only. The store checks that
/// `public_key` belongs to the address; verifying `signature` is the caller's job.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EditProof {
    pub message: String,
    pub signature: String,
    pub public_key: String,
}
