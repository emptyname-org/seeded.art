//! Author names: the name the site displays for each article author, copied for search.
//!
//! The site labels an address with [`Store::display_names`] from the catalogue: claimed >
//! curated > fxhash > objkt > Tezos Profiles, and a name that several addresses carry
//! (compared case-insensitively) is hidden, so the address is shown. A run with a catalogue
//! (`seeded-articles sync|refs --archive-db`) copies that name for every article author into
//! `names (address, name)` and into the `authors` column of the search index (address plus
//! name). No displayed name: no row, and only the address is searchable. Search therefore
//! matches the names the web displays.
//!
//! - Runs without a catalogue keep the copied names.
//! - One immediate transaction per run, writing only authors whose name changed (and rows of
//!   addresses that are no longer authors), so an unchanged run writes nothing.
//! - The web reads names from the catalogue per request; search sees them as of the last run
//!   with a catalogue, so a name edited on the site becomes searchable on the next run.

use std::collections::HashMap;
use std::path::Path;

use rusqlite::{TransactionBehavior, params};
use serde::Serialize;

use super::store::{ArticleStore, ArticleStoreError, Db};
use crate::store::profile::MAX_DISPLAY_NAMES;
use crate::store::{Store, StoreError};

type Result<T> = std::result::Result<T, ArticleStoreError>;

/// What [`ArticleStore::resolve_names`] did.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct NamesReport {
    /// Distinct article authors looked up.
    pub authors: u64,
    /// Of those, with a name the site displays.
    pub named: u64,
    /// Stored names added, changed or removed this run.
    pub changed: u64,
    /// Search index rows rewritten for them.
    pub reindexed: u64,
}

fn catalogue_err(e: StoreError) -> ArticleStoreError {
    match e {
        StoreError::Unavailable(m) => ArticleStoreError::Unavailable(format!("catalogue: {m}")),
        other => ArticleStoreError::Internal(format!("catalogue: {other}")),
    }
}

/// Opens the catalogue read-only through the store API. Its schema must be current;
/// `seeded-index upgrade` migrates an older one.
pub fn open_catalogue(path: &Path) -> Result<Store> {
    Store::open_read_only(path).map_err(|e| match e {
        StoreError::Unavailable(m) => {
            ArticleStoreError::Unavailable(format!("{}: {m}", path.display()))
        }
        other => ArticleStoreError::BadRequest(format!(
            "{}: {other}; `seeded-index upgrade` migrates an older catalogue",
            path.display()
        )),
    })
}

/// Names the site displays for `addresses`, read in one catalogue snapshot with at most
/// [`MAX_DISPLAY_NAMES`] addresses per call. Addresses without one are absent.
pub fn displayed_names(catalogue: &Store, addresses: &[String]) -> Result<HashMap<String, String>> {
    catalogue
        .read_snapshot(|s| {
            let mut out = HashMap::with_capacity(addresses.len());
            for chunk in addresses.chunks(MAX_DISPLAY_NAMES) {
                let chunk: Vec<&str> = chunk.iter().map(String::as_str).collect();
                out.extend(s.display_names(&chunk)?);
            }
            Ok(out)
        })
        .map_err(catalogue_err)
}

impl ArticleStore {
    /// Copies the name the site displays for every article author from `catalogue` into
    /// `names` and the search index (see the module docs).
    pub fn resolve_names(&mut self, catalogue: &Store) -> Result<NamesReport> {
        let authors: Vec<String> = {
            let mut stmt = self
                .conn
                .prepare("SELECT DISTINCT author FROM articles ORDER BY author")
                .db()?;
            stmt.query_map([], |r| r.get(0))
                .db()?
                .collect::<rusqlite::Result<_>>()
                .db()?
        };
        let shown = displayed_names(catalogue, &authors)?;
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .db()?;
        let stored: HashMap<String, String> = {
            let mut stmt = tx.prepare("SELECT address, name FROM names").db()?;
            stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
                .db()?
                .collect::<rusqlite::Result<_>>()
                .db()?
        };
        let mut changed: Vec<&str> = Vec::new();
        {
            let mut upsert = tx
                .prepare(
                    "INSERT INTO names (address, name) VALUES (?1, ?2) \
                     ON CONFLICT (address) DO UPDATE SET name = excluded.name",
                )
                .db()?;
            let mut delete = tx.prepare("DELETE FROM names WHERE address = ?1").db()?;
            for a in &authors {
                match (shown.get(a), stored.get(a)) {
                    (Some(new), old) if old != Some(new) => {
                        upsert.execute(params![a, new]).db()?;
                        changed.push(a);
                    }
                    (None, Some(_)) => {
                        delete.execute([a]).db()?;
                        changed.push(a);
                    }
                    _ => {}
                }
            }
        }
        // An author changed by a chain correction leaves a row nobody displays.
        let orphans: Vec<String> = {
            let mut stmt = tx
                .prepare(
                    "SELECT address FROM names n \
                     WHERE NOT EXISTS (SELECT 1 FROM articles a WHERE a.author = n.address)",
                )
                .db()?;
            stmt.query_map([], |r| r.get(0))
                .db()?
                .collect::<rusqlite::Result<_>>()
                .db()?
        };
        for a in &orphans {
            tx.execute("DELETE FROM names WHERE address = ?1", [a])
                .db()?;
        }
        let mut reindexed = 0u64;
        {
            // The same text as the full rebuild in `store::INDEX_ARTICLES`.
            let mut reindex = tx
                .prepare(
                    "UPDATE articles_fts SET authors = ?1 || ' ' || \
                       COALESCE((SELECT name FROM names WHERE address = ?1), '') \
                     WHERE rowid IN (SELECT token_id FROM articles WHERE author = ?1)",
                )
                .db()?;
            for a in &changed {
                reindexed += reindex.execute([a]).db()? as u64;
            }
        }
        tx.commit().db()?;
        Ok(NamesReport {
            authors: authors.len() as u64,
            named: shown.len() as u64,
            changed: (changed.len() + orphans.len()) as u64,
            reindexed,
        })
    }
}
