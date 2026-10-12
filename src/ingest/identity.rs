//! Curated identities: corrections to who an address is, with provenance.
//!
//! Applied at bootstrap and by `seeded-index identities`. An identity renames the artist with
//! that address (when there is one), adds its aliases and website, and names the address as a
//! collector. Co-authors are untouched: credits stay per address. The search indexes follow in
//! the same transaction (`store::search`).

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};

use crate::store::{Db, Store, StoreError, name_key, search};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Identity {
    pub address: &'static str,
    /// Stable lowercase id that also resolves the artist (e.g. `/artist/emptyname`).
    pub slug: &'static str,
    pub name: &'static str,
    pub website: Option<&'static str>,
    pub aliases: &'static [&'static str],
    pub provenance: &'static str,
}

/// Verified 2026-10-03: objkt (data.objkt.com) lists `tz1gL9H53…` as an Arboreal creator with
/// holder alias "EmptyName" (tmp/arboreal_KT1Efs….json); it is the archive keeper's address.
pub const IDENTITIES: [Identity; 1] = [Identity {
    address: "tz1gL9H53BzjDqmMi2JkKALeCzT7UAoopzEL",
    slug: "emptyname",
    name: "Emptyname",
    website: None,
    aliases: &["EmptyName"],
    provenance: "objkt alias EmptyName (data.objkt.com holder alias) + archive keeper",
}];

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IdentityReport {
    pub identities: usize,
    /// Identities that matched an artist row.
    pub artists: usize,
    /// Projects whose artist sort key was refreshed.
    pub projects: usize,
    /// Owned tokens whose owner alias was set.
    pub tokens: usize,
}

/// Applies [`IDENTITIES`] in its own transaction (`seeded-index identities`).
pub fn apply_to(store: &mut Store) -> Result<IdentityReport, StoreError> {
    let tx = store
        .conn_mut()
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .db()?;
    let report = apply(&tx)?;
    tx.commit().db()?;
    Ok(report)
}

/// Applies [`IDENTITIES`] in the caller's transaction.
pub fn apply(c: &Connection) -> Result<IdentityReport, StoreError> {
    let mut report = IdentityReport::default();
    for id in &IDENTITIES {
        let aliases =
            serde_json::to_string(id.aliases).map_err(|e| StoreError::Internal(e.to_string()))?;
        c.execute(
            "INSERT INTO identities (address, slug, name, website, aliases, provenance, \
             name_key) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) ON CONFLICT (address) DO UPDATE SET \
             slug = excluded.slug, name = excluded.name, website = excluded.website, \
             aliases = excluded.aliases, provenance = excluded.provenance, \
             name_key = excluded.name_key",
            params![
                id.address,
                id.slug,
                id.name,
                id.website,
                aliases,
                id.provenance,
                name_key(id.name)
            ],
        )
        .db()?;
        report.identities += 1;

        let exists: Option<i64> = c
            .query_row("SELECT seq FROM artists WHERE id = ?1", [id.address], |r| {
                r.get(0)
            })
            .optional()
            .db()?;
        if exists.is_some() {
            c.execute(
                // Only identities write artist websites (the bootstrap leaves them empty), so
                // an identity without one clears it.
                "UPDATE artists SET name = ?2, name_key = ?3, website = ?4 WHERE id = ?1",
                params![id.address, id.name, id.name.to_lowercase(), id.website],
            )
            .db()?;
            for alias in id.aliases {
                c.execute(
                    "INSERT OR IGNORE INTO artist_aliases (artist_id, position, alias, alias_key) \
                     SELECT ?1, coalesce(max(position) + 1, 0), ?2, ?3 FROM artist_aliases \
                     WHERE artist_id = ?1",
                    params![id.address, alias, alias.to_lowercase()],
                )
                .db()?;
            }
            report.artists += 1;
            report.projects += refresh_projects(c, id.address)?;
        }
        report.tokens += c
            .execute(
                "UPDATE tokens SET owner_alias = ?2, owner_alias_key = ?3 \
                 WHERE holder_address = ?1 AND owner_address = ?1 \
                 AND owner_alias IS NOT ?2",
                params![id.address, id.name, id.name.to_lowercase()],
            )
            .db()?;
    }
    crate::store::assign_names(c)?;
    search::drain(c)?;
    Ok(report)
}

/// Refreshes the artist sort key of every project credited to `artist`.
fn refresh_projects(c: &Connection, artist: &str) -> Result<usize, StoreError> {
    let projects: Vec<i64> = c
        .prepare("SELECT project_id FROM project_artists WHERE artist_id = ?1")
        .db()?
        .query_map([artist], |r| r.get(0))
        .db()?
        .collect::<rusqlite::Result<_>>()
        .db()?;
    for id in &projects {
        let first: Option<String> = c
            .prepare_cached(
                "SELECT a.name FROM project_artists pa JOIN artists a ON a.id = pa.artist_id \
                 WHERE pa.project_id = ?1 ORDER BY pa.position LIMIT 1",
            )
            .db()?
            .query_row([id], |r| r.get(0))
            .optional()
            .db()?;
        let Some(first) = first else { continue };
        c.execute(
            "UPDATE projects SET artist_key = ?2 WHERE id = ?1",
            params![id, first.to_lowercase()],
        )
        .db()?;
    }
    Ok(projects.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::profile::valid_claimed_name;

    #[test]
    fn curated_names_and_slugs_are_unique_and_claimable_shapes() {
        let mut keys: Vec<String> = IDENTITIES.iter().map(|i| name_key(i.name)).collect();
        let mut slugs: Vec<&str> = IDENTITIES.iter().map(|i| i.slug).collect();
        keys.sort();
        keys.dedup();
        slugs.sort();
        slugs.dedup();
        assert_eq!(keys.len(), IDENTITIES.len());
        assert_eq!(slugs.len(), IDENTITIES.len());
        assert!(IDENTITIES.iter().all(|i| valid_claimed_name(i.name)));
    }
}
