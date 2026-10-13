//! Profiles: one page per address, the duplicate-name rule and local edits.
//!
//! Name rule:
//! - A name is compared by [`name_key`]: trimmed and Unicode-lowercased.
//! - A key carried by two or more addresses (users, artists, imported profiles, curated
//!   identities, claims) is numbered, by [`Store::assign_name_numbers`], persisted in
//!   `name_numbers`. The plain name is number 1; the others show `<own name>-N`.
//! - Number 1 goes to the top address by class (claimed, curated, credited on a visible
//!   project, others), then for credited ones by total volume of their visible projects, then
//!   first seen (earliest project created or token minted), then address. Others are numbered
//!   in first-seen order with the smallest N >= 2 never used for the key whose generated key
//!   no address carries and no row holds.
//! - Rows are stable: they keep their numbers; number 1 changes hands only to a higher class
//!   (the displaced holder takes the number the new holder leaves, else the next unused);
//!   rows of addresses that stopped carrying the name stay as tombstones, so numbers are never
//!   reused; a single remaining carrier shows the plain name.
//! - A key that a row holds as another key's generated name (a later real `foo-2`) has no
//!   plain holder: its carriers, even a single one, are numbered from 2 (`foo-2-2`).
//! - Only an address's candidate name counts: claimed, curated, else its first usable imported
//!   name (fxhash artist, user, registry, objkt, tzprofiles). Lower aliases it never shows
//!   neither number nor join a name. A shared name whose address has no row yet is hidden (no
//!   fall-through to a lower name), curated and claimed names excepted.
//! - Claims win over imported names and show plain. A claim must not collide with any name
//!   another address carries or any generated name assigned to another address.
//! - Only an address with history in the archive can claim a name (see [`FOOTPRINT`]); bio and
//!   links edits are open to every tz1/tz2/tz3 address. A claim already held is kept.
//!
//! Search indexes artists by these shown names (see `store::search`).
//!
//! Every other field resolves field by field: edit > identity > fxhash > objkt > tzprofiles.
//! Within fxhash, the API snapshot (`artists`, `users`, from Frank's archive) precedes the
//! live registry read (`profiles_imported`), which has not changed since 2023-10-26.

use std::collections::HashMap;

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use sha2::{Digest, Sha256};

use super::{
    Cursor, Db, KeysetPage, Store, StoreError, check_limit, empty_keyset, fts_query, holder_totals,
    is_tezos_address, listing_id, not_found, require_address,
};
use crate::model::{AddressKind, EditProof, Profile, ProfileEdit, ProfileHit};

/// Longest claimed name, in characters.
pub const MAX_NAME_CHARS: usize = 40;
pub const MAX_BIO_CHARS: usize = 1000;
pub const MAX_LINKS: usize = 5;
pub const MAX_LINK_CHARS: usize = 300;
/// Most addresses per [`Store::display_names`] call.
pub const MAX_DISPLAY_NAMES: usize = 500;
/// Most addresses per [`Store::avatars`] call.
pub const MAX_AVATARS: usize = 500;
/// Longest name accepted for lookup or import, in characters.
pub const MAX_IMPORTED_NAME_CHARS: usize = 100;
const MAX_MESSAGE_CHARS: usize = 2000;
const MAX_SIGNATURE_CHARS: usize = 256;
const MAX_PUBLIC_KEY_CHARS: usize = 128;

/// v7 -> v8, additive: minters are found by index, for [`FOOTPRINT`].
pub(crate) const SCHEMA_V8: &str = "CREATE INDEX IF NOT EXISTS tokens_minter ON tokens (minter);";

/// Avatar sources of each address in the JSON array `?1`, in [`Profile`] precedence (fxhash API
/// snapshot, fxhash registry, objkt, Tezos Profiles); escrow and burn addresses are left out.
/// Each source is a primary-key or `artists_address` probe per address
/// (`avatars_probe_each_source_by_index`).
pub(crate) const AVATARS: &str = "SELECT q.value, \
     (SELECT a.avatar_uri FROM artists a WHERE a.address = q.value ORDER BY a.seq LIMIT 1), \
     (SELECT avatar_uri FROM profiles_imported WHERE address = q.value AND source = 'fxhash'), \
     (SELECT avatar_uri FROM profiles_imported WHERE address = q.value AND source = 'objkt'), \
     (SELECT avatar_uri FROM profiles_imported \
       WHERE address = q.value AND source = 'tzprofiles') \
     FROM json_each(?1) q \
     WHERE NOT EXISTS (SELECT 1 FROM known_addresses k WHERE k.address = q.value)";

/// Whether address `?1` has history in the archive: credited artist (collaborators included)
/// of a visible project, minter of a visible token, holder, seller of a live listing, or buyer
/// of a live offer. Index probes only (`name_searches_and_their_upkeep_never_scan_tables`).
pub(crate) const FOOTPRINT: &str = "SELECT EXISTS (SELECT 1 FROM artists a \
     JOIN project_artists pa ON pa.artist_id = a.id \
     JOIN projects p ON p.id = pa.project_id AND p.hidden = 0 WHERE a.address = ?1) \
     OR EXISTS (SELECT 1 FROM tokens t JOIN projects p ON p.id = t.project_id AND p.hidden = 0 \
       WHERE t.minter = ?1) \
     OR EXISTS (SELECT 1 FROM holder_counts WHERE holder_address = ?1 AND held > 0) \
     OR EXISTS (SELECT 1 FROM escrow_listings WHERE seller = ?1) \
     OR EXISTS (SELECT 1 FROM market_offers WHERE buyer = ?1 AND active = 1)";

/// The comparison form of a name: trimmed, Unicode-lowercased.
pub fn name_key(name: &str) -> String {
    name.trim().to_lowercase()
}

/// Whether `name` can be a profile name at all: non-blank, not an address.
pub(super) fn usable(name: &str) -> bool {
    let n = name.trim();
    !n.is_empty() && !is_tezos_address(n)
}

/// Fills `users.name_key` and `identities.name_key` (v6 migration). Idempotent.
pub(crate) fn backfill_name_keys(c: &Connection) -> Result<(), StoreError> {
    for table in ["users", "identities"] {
        let rows: Vec<(String, String)> = c
            .prepare(&format!(
                "SELECT address, name FROM {table} WHERE name_key IS NULL"
            ))
            .db()?
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .db()?
            .collect::<rusqlite::Result<_>>()
            .db()?;
        let mut st = c
            .prepare(&format!(
                "UPDATE {table} SET name_key = ?2 WHERE address = ?1"
            ))
            .db()?;
        for (address, name) in rows {
            st.execute(params![address, name_key(&name)]).db()?;
        }
    }
    Ok(())
}

/// One source's imported values for an address.
#[derive(Debug, Default)]
struct Imported {
    name: Option<String>,
    bio: Option<String>,
    website: Option<String>,
    links: Vec<String>,
    avatar_uri: Option<String>,
}

/// Everything stored about one address, before resolution.
#[derive(Debug, Default)]
struct Sources {
    known: Option<(String, String)>,
    edit: Option<ProfileEdit>,
    identity: Option<(String, Option<String>)>,
    /// fxhash API snapshot (Frank's `artists`), then the registry name snapshot (`users`).
    artist: Option<Imported>,
    user_name: Option<String>,
    fxhash: Option<Imported>,
    objkt: Option<Imported>,
    tzprofiles: Option<Imported>,
}

fn links_of(json: &str) -> Result<Vec<String>, StoreError> {
    serde_json::from_str(json)
        .map_err(|e| StoreError::Internal(format!("stored links are not a string list: {e}")))
}

fn sources(c: &Connection, address: &str) -> Result<Sources, StoreError> {
    let mut s = Sources {
        known: c
            .prepare_cached("SELECT name, kind FROM known_addresses WHERE address = ?1")
            .db()?
            .query_row([address], |r| Ok((r.get(0)?, r.get(1)?)))
            .optional()
            .db()?,
        identity: c
            .prepare_cached("SELECT name, website FROM identities WHERE address = ?1")
            .db()?
            .query_row([address], |r| Ok((r.get(0)?, r.get(1)?)))
            .optional()
            .db()?,
        user_name: c
            .prepare_cached("SELECT name FROM users WHERE address = ?1")
            .db()?
            .query_row([address], |r| r.get(0))
            .optional()
            .db()?,
        ..Sources::default()
    };
    let edit: Option<(Option<String>, Option<String>, String)> = c
        .prepare_cached("SELECT name, bio, links FROM profile_edits WHERE address = ?1")
        .db()?
        .query_row([address], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .optional()
        .db()?;
    if let Some((name, bio, links)) = edit {
        s.edit = Some(ProfileEdit {
            name,
            bio,
            links: links_of(&links)?,
        });
    }
    s.artist = c
        .prepare_cached(
            "SELECT name, description, avatar_uri, website FROM artists \
             WHERE address = ?1 ORDER BY seq LIMIT 1",
        )
        .db()?
        .query_row([address], |r| {
            Ok(Imported {
                name: r.get(0)?,
                bio: r.get(1)?,
                avatar_uri: r.get(2)?,
                website: r.get(3)?,
                links: Vec::new(),
            })
        })
        .optional()
        .db()?;
    let rows: Vec<(String, Imported, String)> = c
        .prepare_cached(
            "SELECT source, name, bio, website, links, avatar_uri FROM profiles_imported \
             WHERE address = ?1",
        )
        .db()?
        .query_map([address], |r| {
            Ok((
                r.get(0)?,
                Imported {
                    name: r.get(1)?,
                    bio: r.get(2)?,
                    website: r.get(3)?,
                    links: Vec::new(),
                    avatar_uri: r.get(5)?,
                },
                r.get(4)?,
            ))
        })
        .db()?
        .collect::<rusqlite::Result<_>>()
        .db()?;
    for (source, mut row, links) in rows {
        row.links = links_of(&links)?;
        match source.as_str() {
            "fxhash" => s.fxhash = Some(row),
            "objkt" => s.objkt = Some(row),
            "tzprofiles" => s.tzprofiles = Some(row),
            other => return Err(StoreError::Internal(format!("unknown source {other:?}"))),
        }
    }
    Ok(s)
}

/// The imported name `s` shows, if nothing above it does: the first usable one in precedence
/// order (fxhash artist, fxhash user, fxhash registry, objkt, tzprofiles), with its source.
fn imported_candidate(s: &Sources) -> Option<(&String, &'static str)> {
    [
        (s.artist.as_ref().and_then(|a| a.name.as_ref()), "fxhash"),
        (s.user_name.as_ref(), "fxhash"),
        (s.fxhash.as_ref().and_then(|a| a.name.as_ref()), "fxhash"),
        (s.objkt.as_ref().and_then(|a| a.name.as_ref()), "objkt"),
        (
            s.tzprofiles.as_ref().and_then(|a| a.name.as_ref()),
            "tzprofiles",
        ),
    ]
    .into_iter()
    .find_map(|(name, source)| name.filter(|n| usable(n)).map(|n| (n, source)))
}

/// The key of the name an address would show before numbering: claimed, curated, else its
/// first usable imported name. Only this name makes an address a carrier. Known addresses
/// carry none.
fn candidate_key(s: &Sources) -> Option<String> {
    if s.known.is_some() {
        return None;
    }
    if let Some(name) = s.edit.as_ref().and_then(|e| e.name.as_ref()) {
        return Some(name_key(name));
    }
    if let Some((name, _)) = &s.identity {
        return Some(name_key(name));
    }
    imported_candidate(s).map(|(n, _)| name_key(n))
}

/// Addresses whose candidate name has the key `key`.
fn carriers_of(c: &Connection, key: &str) -> Result<Vec<String>, StoreError> {
    let any: Vec<String> = c
        .prepare_cached(
            "SELECT address FROM users WHERE name_key = ?1 \
             UNION SELECT address FROM artists WHERE name_key = ?1 AND address IS NOT NULL \
             UNION SELECT address FROM profiles_imported WHERE name_key = ?1 \
             UNION SELECT address FROM profile_edits WHERE name_key = ?1 \
             UNION SELECT address FROM identities WHERE name_key = ?1",
        )
        .db()?
        .query_map([key], |r| r.get(0))
        .db()?
        .collect::<rusqlite::Result<_>>()
        .db()?;
    let mut out = Vec::new();
    for a in any {
        if candidate_key(&sources(c, &a)?).as_deref() == Some(key) {
            out.push(a);
        }
    }
    Ok(out)
}

/// Whether an address other than `address` carries the name `key` (see [`candidate_key`]).
fn carried_by_other(c: &Connection, key: &str, address: &str) -> Result<bool, StoreError> {
    Ok(carriers_of(c, key)?.iter().any(|a| a != address))
}

/// Whether another address holds `key` as an assigned shown name (`name_numbers`).
fn shown_by_other(c: &Connection, key: &str, address: &str) -> Result<bool, StoreError> {
    c.prepare_cached("SELECT 1 FROM name_numbers WHERE shown_key = ?1 AND address <> ?2")
        .db()?
        .exists([key, address])
        .db()
}

/// Rows holding `?1` as another key's generated name; a `shown_key` index probe.
const GENERATED_ELSEWHERE: &str =
    "SELECT 1 FROM name_numbers WHERE shown_key = ?1 AND name_key <> ?1";

/// Whether a row holds `key` as another key's generated name (`<key>-N` of some key).
fn generated_elsewhere(c: &Connection, key: &str) -> Result<bool, StoreError> {
    c.prepare_cached(GENERATED_ELSEWHERE)
        .db()?
        .exists([key])
        .db()
}

/// `name` of `address` as shown: plain when no other address carries it and it is not another
/// key's generated name, or the address holds number 1 (or none and `plain_unnumbered`),
/// `name-N` for number N >= 2, `None` (hidden) for a shared name without a number.
fn numbered_name(
    c: &Connection,
    name: &str,
    address: &str,
    plain_unnumbered: bool,
) -> Result<Option<String>, StoreError> {
    let plain = name.trim().to_owned();
    let key = name_key(name);
    if !generated_elsewhere(c, &key)? && !carried_by_other(c, &key, address)? {
        return Ok(Some(plain));
    }
    let number: Option<i64> = c
        .prepare_cached("SELECT number FROM name_numbers WHERE name_key = ?1 AND address = ?2")
        .db()?
        .query_row([&key, address], |r| r.get(0))
        .optional()
        .db()?;
    Ok(match number {
        Some(n) if n >= 2 => Some(format!("{plain}-{n}")),
        Some(_) => Some(plain),
        None => plain_unnumbered.then_some(plain),
    })
}

/// The name shown for an address and where it came from.
fn resolve_name(
    c: &Connection,
    address: &str,
    s: &Sources,
) -> Result<Option<(String, &'static str)>, StoreError> {
    if let Some((name, _)) = &s.known {
        return Ok(Some((name.clone(), "known")));
    }
    if let Some(name) = s.edit.as_ref().and_then(|e| e.name.as_ref()) {
        return Ok(Some((name.clone(), "claimed")));
    }
    if let Some((name, _)) = &s.identity {
        let shown = numbered_name(c, name, address, true)?.unwrap_or_else(|| name.clone());
        return Ok(Some((shown, "curated")));
    }
    // Only the candidate name is shown or numbered; a hidden one does not fall through.
    match imported_candidate(s) {
        Some((name, source)) => Ok(numbered_name(c, name, address, false)?.map(|n| (n, source))),
        None => Ok(None),
    }
}

/// The name shown for `address` and its source, exactly as [`Store::profile`] resolves it.
pub(crate) fn shown_name(
    c: &Connection,
    address: &str,
) -> Result<Option<(String, &'static str)>, StoreError> {
    let s = sources(c, address)?;
    resolve_name(c, address, &s)
}

fn first_text<'a>(values: impl IntoIterator<Item = Option<&'a String>>) -> Option<String> {
    values
        .into_iter()
        .flatten()
        .map(|v| v.trim())
        .find(|v| !v.is_empty())
        .map(str::to_owned)
}

/// Visible projects credited to `address`.
fn project_count(c: &Connection, address: &str) -> Result<u32, StoreError> {
    c.prepare_cached(PROJECT_COUNT)
        .db()?
        .query_row([address], |r| r.get(0))
        .db()
}

pub(crate) const PROJECT_COUNT: &str = "SELECT count(DISTINCT pa.project_id) FROM artists a \
     JOIN project_artists pa ON pa.artist_id = a.id \
     JOIN projects p ON p.id = pa.project_id AND p.hidden = 0 WHERE a.address = ?1";

/// Profile search matches (`:fts`, see [`fts_query`]): driven by the FTS match, then each
/// entry by rowid; matches are sorted by `(name_key, address)`.
pub(crate) const PROFILE_MATCHES: &str = "FROM profile_names n \
     WHERE n.id IN (SELECT rowid FROM profile_fts WHERE profile_fts MATCH :fts)";
pub(crate) const PROFILE_AFTER: &str = " AND (n.name_key, n.address) > (:k0, :k1)";

fn is_visible_artist(c: &Connection, address: &str) -> Result<bool, StoreError> {
    c.prepare_cached(&format!(
        "SELECT EXISTS (SELECT 1 FROM artists a WHERE a.address = ?1 AND {})",
        super::ARTIST_VISIBLE
    ))
    .db()?
    .query_row([address], |r| r.get(0))
    .db()
}

fn hit(c: &Connection, address: String, name: String) -> Result<ProfileHit, StoreError> {
    Ok(ProfileHit {
        kind: if address.starts_with("KT1") {
            AddressKind::Contract
        } else {
            AddressKind::Account
        },
        project_count: project_count(c, &address)?,
        held: holder_totals(c, &address)?.0,
        address,
        name,
    })
}

fn build_profile(c: &Connection, address: &str) -> Result<Profile, StoreError> {
    let s = sources(c, address)?;
    let (name, name_source) = match resolve_name(c, address, &s)? {
        Some((n, src)) => (Some(n), Some(src.to_owned())),
        None => (None, None),
    };
    let imported = [&s.artist, &s.fxhash, &s.objkt, &s.tzprofiles];
    let bio = match s.edit.as_ref().and_then(|e| e.bio.as_ref()) {
        // An edited bio, possibly empty to hide the imported one.
        Some(b) => Some(b.trim().to_owned()).filter(|b| !b.is_empty()),
        None => first_text(
            imported
                .iter()
                .map(|i| i.as_ref().and_then(|i| i.bio.as_ref())),
        ),
    };
    let website = first_text(
        std::iter::once(s.identity.as_ref().and_then(|(_, w)| w.as_ref())).chain(
            imported
                .iter()
                .map(|i| i.as_ref().and_then(|i| i.website.as_ref())),
        ),
    );
    let links = match s.edit.as_ref().filter(|e| !e.links.is_empty()) {
        Some(e) => e.links.clone(),
        None => imported
            .iter()
            .filter_map(|i| i.as_ref())
            .map(|i| i.links.clone())
            .find(|l| !l.is_empty())
            .unwrap_or_default(),
    };
    let avatar_uri = first_text(
        imported
            .iter()
            .map(|i| i.as_ref().and_then(|i| i.avatar_uri.as_ref())),
    );
    let kind = match s.known.as_ref().map(|(_, k)| k.as_str()) {
        Some("escrow") => AddressKind::Escrow,
        Some("burn") => AddressKind::Burn,
        _ if address.starts_with("KT1") => AddressKind::Contract,
        _ => AddressKind::Account,
    };
    let project_count = project_count(c, address)?;
    let (held, listed) = holder_totals(c, address)?;
    Ok(Profile {
        address: address.to_owned(),
        name,
        name_source,
        bio,
        website,
        links,
        avatar_uri,
        kind,
        project_count,
        held,
        listed,
    })
}

/// The address whose shown name is `key`, if any.
fn address_for_name(c: &Connection, key: &str) -> Result<Option<String>, StoreError> {
    let shows = |address: &str| -> Result<bool, StoreError> {
        let s = sources(c, address)?;
        Ok(resolve_name(c, address, &s)?.is_some_and(|(n, _)| name_key(&n) == key))
    };
    // A claim is shown whenever it exists, and claims are unique.
    if let Some(a) = c
        .prepare_cached("SELECT address FROM profile_edits WHERE name_key = ?1")
        .db()?
        .query_row([key], |r| r.get::<_, String>(0))
        .optional()
        .db()?
    {
        return Ok(Some(a));
    }
    let curated: Vec<String> = c
        .prepare_cached("SELECT address FROM identities WHERE name_key = ?1 ORDER BY address")
        .db()?
        .query_map([key], |r| r.get(0))
        .db()?
        .collect::<rusqlite::Result<_>>()
        .db()?;
    for a in curated {
        if shows(&a)? {
            return Ok(Some(a));
        }
    }
    // A numbered name resolves through its generated key, if the address shows it now.
    if let Some(a) = c
        .prepare_cached("SELECT address FROM name_numbers WHERE shown_key = ?1")
        .db()?
        .query_row([key], |r| r.get::<_, String>(0))
        .optional()
        .db()?
    {
        if shows(&a)? {
            return Ok(Some(a));
        }
    }
    let carriers = carriers_of(c, key)?;
    match carriers.as_slice() {
        [only] if shows(only)? => Ok(Some(only.clone())),
        _ => Ok(None),
    }
}

/// Validated, normalised form of a lookup name; BadRequest when it cannot be a name.
fn lookup_key(name: &str) -> Result<String, StoreError> {
    let n = name.trim();
    if n.is_empty() || n.chars().count() > MAX_IMPORTED_NAME_CHARS {
        return Err(StoreError::BadRequest(format!(
            "name must be 1..={MAX_IMPORTED_NAME_CHARS} characters"
        )));
    }
    Ok(name_key(n))
}

/// The address a shown name belongs to, for listing filters; `None` when no profile shows it.
pub(crate) fn owner_of_name(c: &Connection, name: &str) -> Result<Option<String>, StoreError> {
    match lookup_key(name) {
        Ok(key) => address_for_name(c, &key),
        Err(_) => Ok(None),
    }
}

/// A claimed name: 1..=40 characters of ASCII letters, digits, `_`, `-`, `.` and single
/// spaces, starting and ending with a letter or digit, and not an address.
pub fn valid_claimed_name(name: &str) -> bool {
    let chars: Vec<char> = name.chars().collect();
    let (Some(first), Some(last)) = (chars.first(), chars.last()) else {
        return false;
    };
    chars.len() <= MAX_NAME_CHARS
        && first.is_ascii_alphanumeric()
        && last.is_ascii_alphanumeric()
        && chars
            .iter()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, ' ' | '_' | '-' | '.'))
        && !name.contains("  ")
        && !is_tezos_address(name)
}

/// An absolute `https` URL with a host and no credentials, normalised; `None` otherwise.
pub fn https_link(text: &str) -> Option<String> {
    if text.is_empty()
        || text.chars().count() > MAX_LINK_CHARS
        || text.chars().any(|c| c.is_whitespace() || c.is_control())
    {
        return None;
    }
    let url = reqwest::Url::parse(text).ok()?;
    let host = url.host_str()?;
    (url.scheme() == "https"
        && !host.is_empty()
        && url.username().is_empty()
        && url.password().is_none())
    .then(|| url.to_string())
    .filter(|s| s.chars().count() <= MAX_LINK_CHARS)
}

/// A claimed name another address shows or carries; shown to the user as is.
pub const NAME_TAKEN: &str = "That name is taken.";

fn bad(msg: impl Into<String>) -> StoreError {
    StoreError::BadRequest(msg.into())
}

/// An edit after validation: what gets stored.
struct CheckedEdit {
    name: Option<(String, String)>,
    bio: Option<String>,
    links: Vec<String>,
}

fn check_edit(address: &str, edit: &ProfileEdit) -> Result<CheckedEdit, StoreError> {
    require_account(address)?;
    let name = match &edit.name {
        None => None,
        Some(n) => {
            let n = n.trim();
            if !valid_claimed_name(n) {
                return Err(bad(format!(
                    "name must be 1..={MAX_NAME_CHARS} characters of letters, digits, \
                     space, '_', '-' or '.', starting and ending with a letter or digit"
                )));
            }
            Some((n.to_owned(), name_key(n)))
        }
    };
    let bio = match &edit.bio {
        None => None,
        Some(b) => {
            let b = b.trim();
            if b.chars().count() > MAX_BIO_CHARS {
                return Err(bad(format!(
                    "bio must be at most {MAX_BIO_CHARS} characters"
                )));
            }
            if b.chars()
                .any(|c| c.is_control() && !matches!(c, '\n' | '\r' | '\t'))
            {
                return Err(bad("bio contains control characters"));
            }
            Some(b.to_owned())
        }
    };
    if edit.links.len() > MAX_LINKS {
        return Err(bad(format!("at most {MAX_LINKS} links")));
    }
    let mut links = Vec::with_capacity(edit.links.len());
    for l in &edit.links {
        let link = https_link(l.trim()).ok_or_else(|| {
            bad(format!(
                "links must be absolute https URLs without credentials, at most \
                 {MAX_LINK_CHARS} characters"
            ))
        })?;
        if links.contains(&link) {
            return Err(bad("duplicate link"));
        }
        links.push(link);
    }
    Ok(CheckedEdit { name, bio, links })
}

/// Only implicit accounts can sign, so only they can edit (tz4 is not supported yet).
fn require_account(address: &str) -> Result<(), StoreError> {
    require_address(address)?;
    if ["tz1", "tz2", "tz3"].iter().any(|p| address.starts_with(p)) {
        Ok(())
    } else {
        Err(bad("only tz1, tz2 and tz3 addresses can edit a profile"))
    }
}

fn check_claim(c: &Connection, address: &str, edit: &CheckedEdit) -> Result<(), StoreError> {
    if let Some((_, key)) = &edit.name {
        // Keeping a claim already held: a name imported since cannot take it back.
        let held: bool = c
            .prepare_cached("SELECT 1 FROM profile_edits WHERE address = ?1 AND name_key = ?2")
            .db()?
            .exists([address, key.as_str()])
            .db()?;
        if held {
            return Ok(());
        }
        let footprint: bool = c
            .prepare_cached(FOOTPRINT)
            .db()?
            .query_row([address], |r| r.get(0))
            .db()?;
        if !footprint {
            return Err(bad(
                "name claims need an address with history in the archive",
            ));
        }
        if carried_by_other(c, key, address)? || shown_by_other(c, key, address)? {
            return Err(bad(NAME_TAKEN));
        }
    }
    Ok(())
}

fn check_proof(address: &str, proof: &EditProof) -> Result<(), StoreError> {
    let len_ok = |s: &str, max: usize| !s.trim().is_empty() && s.chars().count() <= max;
    if !len_ok(&proof.message, MAX_MESSAGE_CHARS) {
        return Err(bad(format!(
            "signed message must be 1..={MAX_MESSAGE_CHARS} characters"
        )));
    }
    if !len_ok(&proof.signature, MAX_SIGNATURE_CHARS)
        || !proof.signature.bytes().all(|b| b.is_ascii_alphanumeric())
    {
        return Err(bad("signature must be base58 text"));
    }
    if proof.public_key.chars().count() > MAX_PUBLIC_KEY_CHARS
        || !public_key_matches_address(&proof.public_key, address)
    {
        return Err(bad("public key does not belong to the address"));
    }
    Ok(())
}

impl Store {
    /// The profile of `address`: BadRequest unless a tz1..tz4 or KT1 address. An address the
    /// archive knows nothing about is still a profile, with zero counts.
    pub fn profile(&self, address: &str) -> Result<Profile, StoreError> {
        require_address(address)?;
        self.read(|c| build_profile(c, address))
    }

    /// The profile whose shown name is `name`, compared case-insensitively. A shared name resolves to
    /// its plain holder, `name-N` to the address numbered N (see the module docs), if it shows
    /// it now; anything else is NotFound.
    pub fn profile_by_name(&self, name: &str) -> Result<Profile, StoreError> {
        let key = lookup_key(name)?;
        self.read(|c| match address_for_name(c, &key)? {
            Some(address) => build_profile(c, &address),
            None => Err(not_found("profile", name.trim())),
        })
    }

    /// Shown names of up to [`MAX_DISPLAY_NAMES`] addresses, for owner and artist labels.
    /// Addresses without a valid name (and malformed ones) are absent from the map.
    pub fn display_names(&self, addresses: &[&str]) -> Result<HashMap<String, String>, StoreError> {
        if addresses.len() > MAX_DISPLAY_NAMES {
            return Err(bad(format!("at most {MAX_DISPLAY_NAMES} addresses")));
        }
        self.read(|c| {
            let mut out = HashMap::new();
            for &a in addresses {
                if !is_tezos_address(a) || out.contains_key(a) {
                    continue;
                }
                let s = sources(c, a)?;
                if let Some((name, _)) = resolve_name(c, a, &s)? {
                    out.insert(a.to_owned(), name);
                }
            }
            Ok(out)
        })
    }

    /// Shown avatar URIs of up to [`MAX_AVATARS`] addresses, as [`Store::profile`] resolves
    /// `avatar_uri`, in one query. Addresses without one, escrow and burn addresses, and
    /// malformed ones are absent from the map.
    pub fn avatars(&self, addresses: &[&str]) -> Result<HashMap<String, String>, StoreError> {
        if addresses.len() > MAX_AVATARS {
            return Err(bad(format!("at most {MAX_AVATARS} addresses")));
        }
        let mut wanted: Vec<&str> = addresses
            .iter()
            .copied()
            .filter(|a| is_tezos_address(a))
            .collect();
        wanted.sort_unstable();
        wanted.dedup();
        if wanted.is_empty() {
            return Ok(HashMap::new());
        }
        let list = serde_json::to_string(&wanted)
            .map_err(|e| StoreError::Internal(format!("address list: {e}")))?;
        self.read(|c| {
            let mut out = HashMap::new();
            let mut st = c.prepare_cached(AVATARS).db()?;
            let mut rows = st.query([list]).db()?;
            while let Some(r) = rows.next().db()? {
                let sources: [Option<String>; 4] = [
                    r.get(1).db()?,
                    r.get(2).db()?,
                    r.get(3).db()?,
                    r.get(4).db()?,
                ];
                if let Some(uri) = first_text(sources.iter().map(Option::as_ref)) {
                    out.insert(r.get(0).db()?, uri);
                }
            }
            Ok(out)
        })
    }

    /// Addresses whose shown name matches `text` (artists and collectors alike), ordered by
    /// name case-insensitively, then address. Words match as prefixes, at most
    /// [`MAX_QUERY_TERMS`](super::MAX_QUERY_TERMS) of them, as in the project search; an
    /// address typed exactly returns that profile alone. Escrow and burn addresses are never
    /// found; a shared name is found plain, then as `name-N`. The cursor only resumes the same text.
    pub fn profiles_search(
        &self,
        text: &str,
        after: Option<&Cursor>,
        limit: u32,
    ) -> Result<KeysetPage<ProfileHit>, StoreError> {
        self.profile_search(text, after, limit, false)
    }

    /// Like [`Store::profiles_search`] without visible artists' addresses, so a search page can
    /// list artists and collectors without repeating anyone. Its cursors are its own.
    pub fn collectors_search(
        &self,
        text: &str,
        after: Option<&Cursor>,
        limit: u32,
    ) -> Result<KeysetPage<ProfileHit>, StoreError> {
        self.profile_search(text, after, limit, true)
    }

    fn profile_search(
        &self,
        text: &str,
        after: Option<&Cursor>,
        limit: u32,
        collectors_only: bool,
    ) -> Result<KeysetPage<ProfileHit>, StoreError> {
        check_limit(limit)?;
        let text = text.trim();
        let listing = listing_id(&[
            if collectors_only {
                "collectors"
            } else {
                "profiles"
            },
            text,
        ]);
        let key = after.map(|c| c.key(&listing, 2)).transpose()?;
        let not_artist = if collectors_only {
            format!(
                " AND NOT EXISTS (SELECT 1 FROM artists a WHERE a.address = n.address AND {})",
                super::ARTIST_VISIBLE
            )
        } else {
            String::new()
        };
        if is_tezos_address(text) {
            return self.read(|c| {
                if super::is_known_address(c, text)? {
                    return Ok(empty_keyset());
                }
                if collectors_only && is_visible_artist(c, text)? {
                    return Ok(empty_keyset());
                }
                let name = shown_name(c, text)?.map_or_else(|| text.to_owned(), |(n, _)| n);
                let found = hit(c, text.to_owned(), name)?;
                Ok(KeysetPage {
                    // A cursor of this listing sits after its only item.
                    items: if key.is_some() { vec![] } else { vec![found] },
                    next: None,
                    total: 1,
                })
            });
        }
        let Some(fts) = fts_query(text)? else {
            return Ok(empty_keyset());
        };
        let kp = key.as_deref().map(super::key_params);
        self.read(|c| {
            let fts_param: (&str, &dyn rusqlite::ToSql) = (":fts", &fts);
            let matches = format!("{PROFILE_MATCHES}{not_artist}");
            let total = super::count(c, &matches, &[fts_param])?;
            let mut all = vec![fts_param];
            let mut from = matches;
            if let Some(kp) = &kp {
                from.push_str(PROFILE_AFTER);
                all.push((":k0", kp[0].as_ref()));
                all.push((":k1", kp[1].as_ref()));
            }
            let fetch = limit + 1;
            all.push((":limit", &fetch));
            let sql = format!(
                "SELECT n.address, n.name, n.name_key {from} \
                 ORDER BY n.name_key, n.address LIMIT :limit"
            );
            let mut rows: Vec<(String, String, String)> = c
                .prepare_cached(&sql)
                .db()?
                .query_map(all.as_slice(), |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
                .db()?
                .collect::<rusqlite::Result<_>>()
                .db()?;
            let more = rows.len() > limit as usize;
            rows.truncate(limit as usize);
            let next = if more {
                rows.last().map(|(address, _, key)| {
                    Cursor::encode(
                        &listing,
                        vec![
                            serde_json::Value::from(key.clone()),
                            serde_json::Value::from(address.clone()),
                        ],
                    )
                })
            } else {
                None
            };
            let items = rows
                .into_iter()
                .map(|(address, name, _)| hit(c, address, name))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(KeysetPage { items, next, total })
        })
    }

    /// The stored edit of `address`, if any (for prefilling an edit form).
    pub fn profile_edit(&self, address: &str) -> Result<Option<ProfileEdit>, StoreError> {
        require_address(address)?;
        self.read(|c| Ok(sources(c, address)?.edit))
    }

    /// Everything [`Store::save_profile_edit`] checks except the proof, so a form can be
    /// rejected before the owner signs it.
    pub fn check_profile_edit(&self, address: &str, edit: &ProfileEdit) -> Result<(), StoreError> {
        let checked = check_edit(address, edit)?;
        self.read(|c| check_claim(c, address, &checked))
    }

    /// Stores the owner's edit, replacing any earlier one. BadRequest on invalid fields, a
    /// name another address carries, or a public key that is not the address's. The proof is
    /// kept for audit; its signature must already be verified by the caller.
    pub fn save_profile_edit(
        &mut self,
        address: &str,
        edit: ProfileEdit,
        proof: EditProof,
    ) -> Result<(), StoreError> {
        let checked = check_edit(address, &edit)?;
        check_proof(address, &proof)?;
        let links = serde_json::to_string(&checked.links)
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        let tx = self
            .conn_mut()
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .db()?;
        // Checked under the write lock, so no concurrent claim can slip in between.
        check_claim(&tx, address, &checked)?;
        let (name, key) = checked.name.unzip();
        tx.execute(
            "INSERT INTO profile_edits (address, name, name_key, bio, links, updated_at, \
             signed_message, signature, public_key) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9) ON CONFLICT (address) DO UPDATE SET \
             name = excluded.name, name_key = excluded.name_key, bio = excluded.bio, \
             links = excluded.links, updated_at = excluded.updated_at, \
             signed_message = excluded.signed_message, signature = excluded.signature, \
             public_key = excluded.public_key",
            params![
                address,
                name,
                key,
                checked.bio,
                links,
                crate::ingest::now_iso(),
                proof.message,
                proof.signature,
                proof.public_key
            ],
        )
        .db()?;
        // The search indexes follow the shown name in the same transaction.
        super::search::drain(&tx)?;
        tx.commit().db()
    }
}

/// Base58check payload of `text` (version prefix included), if the checksum holds.
fn base58check(text: &str) -> Option<Vec<u8>> {
    if text.is_empty() || text.len() > 200 {
        return None;
    }
    let raw = cid::multibase::Base::Base58Btc.decode(text).ok()?;
    if raw.len() < 5 {
        return None;
    }
    let (payload, sum) = raw.split_at(raw.len() - 4);
    let digest = Sha256::digest(Sha256::digest(payload));
    (digest[..4] == *sum).then(|| payload.to_vec())
}

/// (address prefix, public-key prefix, key length) per curve: Ed25519, secp256k1, P-256.
const KEY_KINDS: [([u8; 3], [u8; 4], usize); 3] = [
    ([6, 161, 159], [13, 15, 37, 217], 32),
    ([6, 161, 161], [3, 254, 226, 86], 33),
    ([6, 161, 164], [3, 178, 139, 127], 33),
];

/// Whether `public_key` (edpk, sppk or p2pk) hashes to the tz1, tz2 or tz3 `address`.
pub fn public_key_matches_address(public_key: &str, address: &str) -> bool {
    let (Some(key), Some(addr)) = (base58check(public_key), base58check(address)) else {
        return false;
    };
    KEY_KINDS.iter().any(|(addr_prefix, key_prefix, len)| {
        key.len() == 4 + len
            && key.starts_with(key_prefix)
            && addr.len() == 23
            && addr.starts_with(addr_prefix)
            && blake2b(20, &key[4..]) == addr[3..]
    })
}

const IV: [u64; 8] = [
    0x6a09_e667_f3bc_c908,
    0xbb67_ae85_84ca_a73b,
    0x3c6e_f372_fe94_f82b,
    0xa54f_f53a_5f1d_36f1,
    0x510e_527f_ade6_82d1,
    0x9b05_688c_2b3e_6c1f,
    0x1f83_d9ab_fb41_bd6b,
    0x5be0_cd19_137e_2179,
];

const SIGMA: [[usize; 16]; 10] = [
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
    [14, 10, 4, 8, 9, 15, 13, 6, 1, 12, 0, 2, 11, 7, 5, 3],
    [11, 8, 12, 0, 5, 2, 15, 13, 10, 14, 3, 6, 7, 1, 9, 4],
    [7, 9, 3, 1, 13, 12, 11, 14, 2, 6, 5, 10, 4, 0, 15, 8],
    [9, 0, 5, 7, 2, 4, 10, 15, 14, 1, 11, 12, 6, 8, 3, 13],
    [2, 12, 6, 10, 0, 11, 8, 3, 4, 13, 7, 5, 15, 14, 1, 9],
    [12, 5, 1, 15, 14, 13, 4, 10, 0, 7, 6, 3, 9, 2, 8, 11],
    [13, 11, 7, 14, 12, 1, 3, 9, 5, 0, 15, 4, 8, 6, 2, 10],
    [6, 15, 14, 9, 11, 3, 0, 8, 12, 2, 13, 7, 1, 4, 10, 5],
    [10, 2, 8, 4, 7, 6, 1, 5, 15, 11, 9, 14, 3, 12, 13, 0],
];

#[allow(clippy::many_single_char_names)] // RFC 7693 names
fn mix(v: &mut [u64; 16], a: usize, b: usize, c: usize, d: usize, x: u64, y: u64) {
    v[a] = v[a].wrapping_add(v[b]).wrapping_add(x);
    v[d] = (v[d] ^ v[a]).rotate_right(32);
    v[c] = v[c].wrapping_add(v[d]);
    v[b] = (v[b] ^ v[c]).rotate_right(24);
    v[a] = v[a].wrapping_add(v[b]).wrapping_add(y);
    v[d] = (v[d] ^ v[a]).rotate_right(16);
    v[c] = v[c].wrapping_add(v[d]);
    v[b] = (v[b] ^ v[c]).rotate_right(63);
}

fn compress(h: &mut [u64; 8], block: &[u8; 128], t: u128, last: bool) {
    let mut m = [0u64; 16];
    for (i, w) in m.iter_mut().enumerate() {
        let mut b = [0u8; 8];
        b.copy_from_slice(&block[i * 8..i * 8 + 8]);
        *w = u64::from_le_bytes(b);
    }
    let mut v = [0u64; 16];
    v[..8].copy_from_slice(h);
    v[8..].copy_from_slice(&IV);
    v[12] ^= t as u64;
    v[13] ^= (t >> 64) as u64;
    if last {
        v[14] = !v[14];
    }
    for round in 0..12 {
        let s = &SIGMA[round % 10];
        mix(&mut v, 0, 4, 8, 12, m[s[0]], m[s[1]]);
        mix(&mut v, 1, 5, 9, 13, m[s[2]], m[s[3]]);
        mix(&mut v, 2, 6, 10, 14, m[s[4]], m[s[5]]);
        mix(&mut v, 3, 7, 11, 15, m[s[6]], m[s[7]]);
        mix(&mut v, 0, 5, 10, 15, m[s[8]], m[s[9]]);
        mix(&mut v, 1, 6, 11, 12, m[s[10]], m[s[11]]);
        mix(&mut v, 2, 7, 8, 13, m[s[12]], m[s[13]]);
        mix(&mut v, 3, 4, 9, 14, m[s[14]], m[s[15]]);
    }
    for i in 0..8 {
        h[i] ^= v[i] ^ v[i + 8];
    }
}

/// Unkeyed BLAKE2b (RFC 7693) with a 1..=64-byte digest; Tezos hashes keys to 20 bytes.
pub(crate) fn blake2b(out_len: usize, data: &[u8]) -> Vec<u8> {
    debug_assert!((1..=64).contains(&out_len));
    let mut h = IV;
    h[0] ^= 0x0101_0000 ^ out_len as u64;
    let mut t: u128 = 0;
    let mut rest = data;
    while rest.len() > 128 {
        let (block, tail) = rest.split_at(128);
        t += 128;
        let mut b = [0u8; 128];
        b.copy_from_slice(block);
        compress(&mut h, &b, t, false);
        rest = tail;
    }
    let mut b = [0u8; 128];
    b[..rest.len()].copy_from_slice(rest);
    t += rest.len() as u128;
    compress(&mut h, &b, t, true);
    h.iter()
        .flat_map(|w| w.to_le_bytes())
        .take(out_len)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plan_of(sql: &str) -> Vec<String> {
        let store = Store::open_in_memory().unwrap();
        let mut st = store
            .conn()
            .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
            .unwrap();
        let mut rows = st.raw_query();
        let mut plan = Vec::new();
        while let Some(r) = rows.next().unwrap() {
            plan.push(r.get::<_, String>(3).unwrap());
        }
        plan
    }

    /// Profile search reads the FTS match, then entries by rowid; its upkeep and the per-hit
    /// counts are index probes. Only sorting the matches uses a temporary B-tree.
    #[test]
    fn profile_search_and_its_upkeep_never_scan_tables() {
        let page = format!(
            "SELECT n.address, n.name, n.name_key {PROFILE_MATCHES} \
             ORDER BY n.name_key, n.address LIMIT :limit"
        );
        let after = format!(
            "SELECT n.address, n.name, n.name_key {PROFILE_MATCHES}{PROFILE_AFTER} \
             ORDER BY n.name_key, n.address LIMIT :limit"
        );
        let count = format!("SELECT count(*) {PROFILE_MATCHES}");
        for sql in [&page, &after, &count] {
            let plan = plan_of(sql);
            assert!(
                plan.iter()
                    .any(|s| s.contains("VIRTUAL TABLE INDEX") && s.contains(":M")),
                "{sql}\n{plan:?}"
            );
            assert!(
                plan.iter()
                    .any(|s| s == "SEARCH n USING INTEGER PRIMARY KEY (rowid=?)"),
                "{sql}\n{plan:?}"
            );
            for step in &plan {
                if step.starts_with("SCAN") {
                    assert!(step.contains("VIRTUAL TABLE INDEX"), "{sql}\n{plan:?}");
                }
            }
        }
        for sql in [
            super::super::search::TOUCHED_ADDRESSES,
            PROJECT_COUNT,
            GENERATED_ELSEWHERE,
            "SELECT id, name FROM profile_names WHERE address = ?1",
            "SELECT 1 FROM known_addresses WHERE address = ?1",
            "SELECT held, listed FROM holder_counts WHERE holder_address = ?1",
        ] {
            let plan = plan_of(sql);
            for step in &plan {
                if step.starts_with("SCAN") {
                    assert_eq!(step, "SCAN CONSTANT ROW", "{sql}\n{plan:?}");
                }
            }
        }
    }

    #[test]
    fn avatars_probe_each_source_by_index() {
        let store = Store::open_in_memory().unwrap();
        let mut st = store
            .conn()
            .prepare(&format!("EXPLAIN QUERY PLAN {AVATARS}"))
            .unwrap();
        let mut rows = st.raw_query();
        let mut plan = Vec::new();
        while let Some(r) = rows.next().unwrap() {
            plan.push(r.get::<_, String>(3).unwrap());
        }
        for step in &plan {
            // The only scan is over the request's own address list.
            if step.starts_with("SCAN") {
                assert!(step.starts_with("SCAN q VIRTUAL TABLE"), "{plan:?}");
            }
            assert!(!step.contains("TEMP B-TREE"), "{plan:?}");
        }
        for probe in [
            "SEARCH a USING INDEX artists_address (address=?)",
            "SEARCH profiles_imported USING PRIMARY KEY (address=? AND source=?)",
            "SEARCH k USING PRIMARY KEY (address=?)",
        ] {
            assert!(
                plan.iter().any(|s| s.starts_with(probe)),
                "{probe}\n{plan:?}"
            );
        }
    }

    #[test]
    fn blake2b_matches_rfc_7693_vectors() {
        assert_eq!(
            hex::encode(blake2b(64, b"abc")),
            "ba80a53f981c4d0d6a2797b69f12f6e94c212f14685ac4b74b12bb6fdbffa2d1\
             7d87c5392aab792dc252d5de4533cc9518d38aa8dbf1925ab92386edd4009923"
        );
        assert_eq!(
            hex::encode(blake2b(64, b"")),
            "786a02f742015903c6c6fd852552d272912f4740e15847618a86e217f71f5419\
             d25e1031afee585313896444934eb04b903a685b1448b755d56f701afe9be2ce"
        );
        assert_eq!(
            hex::encode(blake2b(32, b"")),
            "0e5751c026e543b2e8ab2eb06099daa1d1e5df47778f7787faab45cdf12fe3a8"
        );
        // Multi-block input: two full blocks plus a partial one, and an exact block.
        let long = vec![0u8; 300];
        assert_eq!(blake2b(32, &long).len(), 32);
        assert_ne!(blake2b(32, &long[..128]), blake2b(32, &long[..129]));
    }

    #[test]
    fn public_keys_hash_to_their_addresses() {
        // Revealed keys read from TzKT on 2026-10-05.
        for (pk, address) in [
            (
                "edpkukC966pBdnKAYgkRsc7KgxYJwzvj5wwLVFV5t8ozjhyjiyFWEj",
                "tz1PoDdN2oyRyF6DA73zTWAWYhNL4UGr3Egj",
            ),
            (
                "edpktrU1cZyBK4vBU1WSpcmrCfqgUsgC4NYGRcyim6ijn3XBipXYmh",
                "tz1itwvWyP4qd6PY7qrYkn3LBvDC13vjQS9i",
            ),
            (
                "sppk7aEz5GzduJrXvNWQaTbWKRiWHqkUhrnSt3h4dD49PgLsyyKszVr",
                "tz2VtWQNPH2TsDixaShXp5615sxDNZ4uB436",
            ),
            (
                "p2pk67RZCKQ7LwRsGriNZL4YJKankdd24WrUfzYifxcqqQrLb3K2rsN",
                "tz3fPdUoSmoLYNggLXPHedyGiLCPQZM6jwCN",
            ),
        ] {
            assert!(public_key_matches_address(pk, address), "{pk} -> {address}");
        }
        let ed = "edpkukC966pBdnKAYgkRsc7KgxYJwzvj5wwLVFV5t8ozjhyjiyFWEj";
        assert!(!public_key_matches_address(
            ed,
            "tz1itwvWyP4qd6PY7qrYkn3LBvDC13vjQS9i"
        ));
        assert!(!public_key_matches_address(
            ed,
            "tz2VtWQNPH2TsDixaShXp5615sxDNZ4uB436"
        ));
        // A broken checksum or a non-key never matches.
        assert!(!public_key_matches_address(
            "edpkukC966pBdnKAYgkRsc7KgxYJwzvj5wwLVFV5t8ozjhyjiyFWEk",
            "tz1PoDdN2oyRyF6DA73zTWAWYhNL4UGr3Egj"
        ));
        assert!(!public_key_matches_address(
            "",
            "tz1PoDdN2oyRyF6DA73zTWAWYhNL4UGr3Egj"
        ));
        assert!(!public_key_matches_address(
            "tz1PoDdN2oyRyF6DA73zTWAWYhNL4UGr3Egj",
            "tz1PoDdN2oyRyF6DA73zTWAWYhNL4UGr3Egj"
        ));
    }

    #[test]
    fn claimed_names_follow_the_charset() {
        for ok in ["EmptyName", "a", "Che-Yu Wu", "x.y_z-1", &"a".repeat(40)] {
            assert!(valid_claimed_name(ok), "{ok:?}");
        }
        for bad in [
            "",
            " a",
            "a ",
            "-a",
            "a.",
            "a  b",
            "naïve",
            "a/b",
            "<b>",
            &"a".repeat(41),
            "tz1PoDdN2oyRyF6DA73zTWAWYhNL4UGr3Egj",
        ] {
            assert!(!valid_claimed_name(bad), "{bad:?}");
        }
    }

    #[test]
    fn links_must_be_https_without_credentials() {
        assert_eq!(
            https_link("https://emptyname.org").as_deref(),
            Some("https://emptyname.org/")
        );
        assert!(https_link("https://x.com/emptyname_org").is_some());
        for bad in [
            "http://emptyname.org",
            "javascript:alert(1)",
            "https://user:pw@example.org/",
            "https://user@example.org/",
            "//example.org",
            "example.org",
            "https:// example.org",
            "data:text/html,x",
            "",
        ] {
            assert_eq!(https_link(bad), None, "{bad:?}");
        }
        assert!(https_link(&format!("https://e.org/{}", "a".repeat(300))).is_none());
    }

    #[test]
    fn name_keys_trim_and_fold_case() {
        assert_eq!(name_key("  ZanCan "), "zancan");
        assert_eq!(name_key("ÉCOLE"), "école");
        assert!(!usable("   "));
        assert!(!usable("tz1PoDdN2oyRyF6DA73zTWAWYhNL4UGr3Egj"));
    }
}
