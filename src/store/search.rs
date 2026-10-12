//! Artist names in the search indexes: `artist_fts` and `project_fts` hold the names pages
//! show, never raw imported ones.
//!
//! An artist's search label is the shown name of its address, resolved like
//! [`Store::profile`](super::Store::profile) (claimed > curated > fxhash > objkt; a name shared
//! case-insensitively is numbered, see the profile module docs), or the address itself when
//! the address shows no name. The address is the artist's `address`, else its id when that is
//! an address, else the address of the curated identity whose slug is the id (the attribution
//! pages use). An artist without any address (fixture catalogue only) keeps its catalogue
//! name and aliases. `artist_fts.aliases` holds the address, plus the curated slug and
//! aliases while the curated name is the one shown.
//!
//! `profile_names` and `profile_fts` (v9) hold the shown name of every named address, artist
//! or collector, resolved the same way; escrow and burn addresses and addresses without a
//! valid name have no entry. Their search is [`Store::profiles_search`](super::Store).
//!
//! Triggers on every name source queue the addresses, name keys, artist ids and slugs a write
//! touches in `search_dirty`. Writers call [`drain`] in the same transaction: it refreshes
//! every artist and address whose address is queued or carries a queued name key, then the
//! entries of those artists' projects, and empties the queue. Bulk writers (bootstrap,
//! migration, upgrade) call [`rebuild`]. A queue left by a writer that does not drain is taken
//! by the next drain.

use std::collections::{BTreeSet, HashMap};

use rusqlite::{Connection, OptionalExtension, params};

use super::{Db, StoreError, is_known_address, is_tezos_address, profile};

/// Name sources: (table, columns whose values a write queues, other columns that change the
/// shown name). A known address's name only changes that address.
const SOURCES: [(&str, &[&str], &[&str]); 6] = [
    ("users", &["address", "name_key"], &[]),
    ("artists", &["id", "address", "name_key"], &[]),
    ("identities", &["address", "slug", "name_key"], &["aliases"]),
    ("profiles_imported", &["address", "name_key"], &[]),
    ("profile_edits", &["address", "name_key"], &[]),
    ("known_addresses", &["address"], &["name"]),
];

/// v7 -> v8, additive: the `search_dirty` queue and the triggers that fill it. Idempotent, so
/// a database rolled back to v7 by hand migrates again.
pub(crate) fn schema_v8() -> String {
    let mut sql = String::from(
        "CREATE TABLE IF NOT EXISTS search_dirty (value TEXT PRIMARY KEY) STRICT, \
         WITHOUT ROWID;\n",
    );
    for (table, queued, watched) in SOURCES {
        let queue = |row: &str| -> String {
            let mut body = String::new();
            for c in queued {
                // Not `OR IGNORE`: an outer statement's conflict policy would override it.
                body.push_str(&format!(
                    "  INSERT INTO search_dirty (value) SELECT {row}.{c} \
                     WHERE {row}.{c} IS NOT NULL AND NOT EXISTS \
                     (SELECT 1 FROM search_dirty WHERE value = {row}.{c});\n"
                ));
            }
            body
        };
        let columns: Vec<&str> = queued.iter().chain(watched.iter()).copied().collect();
        let changed: Vec<String> = columns
            .iter()
            .map(|c| format!("OLD.{c} IS NOT NEW.{c}"))
            .collect();
        let trigger = |name: &str, event: &str, body: String| {
            format!(
                "CREATE TRIGGER IF NOT EXISTS {table}_search_{name} AFTER {event}\n\
                 BEGIN\n{body}END;\n"
            )
        };
        sql.push_str(&trigger(
            "insert",
            &format!("INSERT ON {table}"),
            queue("NEW"),
        ));
        sql.push_str(&trigger(
            "delete",
            &format!("DELETE ON {table}"),
            queue("OLD"),
        ));
        let update = format!(
            "UPDATE OF {} ON {table} WHEN {}",
            columns.join(", "),
            changed.join(" OR ")
        );
        sql.push_str(&trigger("update", &update, queue("OLD") + &queue("NEW")));
    }
    sql
}

pub(crate) fn migrate_v8(tx: &Connection) -> Result<(), StoreError> {
    tx.execute_batch(&schema_v8()).db()?;
    rebuild_credits(tx).map(|_| ())
}

/// v8 -> v9, additive: shown names of every named address and their FTS index (rowids are
/// `profile_names.id`). Idempotent, like v8.
const SCHEMA_V9: &str = "
CREATE TABLE IF NOT EXISTS profile_names (
  id INTEGER PRIMARY KEY,
  address TEXT NOT NULL UNIQUE,
  name TEXT NOT NULL,
  name_key TEXT NOT NULL
) STRICT;
CREATE VIRTUAL TABLE IF NOT EXISTS profile_fts USING fts5 (name,
  tokenize = 'unicode61 remove_diacritics 2');
";

pub(crate) fn migrate_v9(tx: &Connection) -> Result<(), StoreError> {
    tx.execute_batch(SCHEMA_V9).db()?;
    rebuild_profiles(tx).map(|_| ())
}

/// Addresses a queued value touches: the value as an address, every address carrying it as a
/// name key, the address of the artist with that id, and of the curated identity with that
/// slug. Each step is an index probe.
pub(crate) const TOUCHED_ADDRESSES: &str = "SELECT ?1 \
     UNION SELECT address FROM users WHERE name_key = ?1 \
     UNION SELECT address FROM artists WHERE name_key = ?1 AND address IS NOT NULL \
     UNION SELECT address FROM profiles_imported WHERE name_key = ?1 \
     UNION SELECT address FROM profile_edits WHERE name_key = ?1 \
     UNION SELECT address FROM identities WHERE name_key = ?1 \
     UNION SELECT address FROM artists WHERE id = ?1 AND address IS NOT NULL \
     UNION SELECT address FROM identities WHERE slug = ?1";

/// Artists a queued value touches: as an address, artist id or curated identity address, and
/// through every address carrying it as a name key. Each step is an index probe; `CROSS JOIN`
/// keeps the few values outermost.
pub(crate) const TOUCHED_ARTISTS: &str = "WITH v (x) AS (SELECT ?1 \
     UNION SELECT address FROM users WHERE name_key = ?1 \
     UNION SELECT address FROM artists WHERE name_key = ?1 AND address IS NOT NULL \
     UNION SELECT address FROM profiles_imported WHERE name_key = ?1 \
     UNION SELECT address FROM profile_edits WHERE name_key = ?1 \
     UNION SELECT address FROM identities WHERE name_key = ?1) \
     SELECT a.seq FROM v CROSS JOIN artists a ON a.address = v.x \
     UNION SELECT a.seq FROM v CROSS JOIN artists a ON a.id = v.x \
     UNION SELECT a.seq FROM v CROSS JOIN identities i ON i.address = v.x \
       CROSS JOIN artists a ON a.id = i.slug AND a.address IS NULL";

/// Projects crediting artist `?1`.
pub(crate) const ARTIST_PROJECTS: &str =
    "SELECT project_id FROM project_artists WHERE artist_id = ?1";

/// Credited artists of project `?1`, in credit order.
pub(crate) const PROJECT_CREDITS: &str = "SELECT a.seq, a.id, a.name, a.address \
     FROM project_artists pa JOIN artists a ON a.id = pa.artist_id \
     WHERE pa.project_id = ?1 ORDER BY pa.position";

#[derive(Debug, Clone)]
struct ArtistRow {
    seq: i64,
    id: String,
    name: String,
    address: Option<String>,
}

fn artist_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<ArtistRow> {
    Ok(ArtistRow {
        seq: r.get(0)?,
        id: r.get(1)?,
        name: r.get(2)?,
        address: r.get(3)?,
    })
}

/// What `artist_fts` holds for one artist.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Entry {
    label: String,
    aliases: String,
}

/// Labels and entries computed during one drain or rebuild, by artist id.
#[derive(Default)]
struct Labels {
    entries: HashMap<String, Entry>,
}

impl Labels {
    fn entry(&mut self, c: &Connection, a: &ArtistRow) -> Result<Entry, StoreError> {
        if let Some(e) = self.entries.get(&a.id) {
            return Ok(e.clone());
        }
        let e = compute_entry(c, a)?;
        self.entries.insert(a.id.clone(), e.clone());
        Ok(e)
    }
}

/// The address an artist is shown as (see the module docs).
fn artist_address(c: &Connection, a: &ArtistRow) -> Result<Option<String>, StoreError> {
    if let Some(address) = &a.address {
        return Ok(Some(address.clone()));
    }
    if is_tezos_address(&a.id) {
        return Ok(Some(a.id.clone()));
    }
    c.prepare_cached("SELECT address FROM identities WHERE slug = ?1")
        .db()?
        .query_row([&a.id], |r| r.get(0))
        .optional()
        .db()
}

fn catalogue_aliases(c: &Connection, id: &str) -> Result<Vec<String>, StoreError> {
    c.prepare_cached("SELECT alias FROM artist_aliases WHERE artist_id = ?1 ORDER BY position")
        .db()?
        .query_map([id], |r| r.get(0))
        .db()?
        .collect::<rusqlite::Result<_>>()
        .db()
}

fn compute_entry(c: &Connection, a: &ArtistRow) -> Result<Entry, StoreError> {
    let Some(address) = artist_address(c, a)? else {
        return Ok(Entry {
            label: a.name.clone(),
            aliases: catalogue_aliases(c, &a.id)?.join(" "),
        });
    };
    let shown = profile::shown_name(c, &address)?;
    let mut aliases = vec![address.clone()];
    if matches!(shown, Some((_, "curated"))) {
        let slug: Option<String> = c
            .prepare_cached("SELECT slug FROM identities WHERE address = ?1")
            .db()?
            .query_row([&address], |r| r.get(0))
            .optional()
            .db()?
            .flatten();
        aliases.extend(slug);
        aliases.extend(catalogue_aliases(c, &a.id)?);
    }
    Ok(Entry {
        label: shown.map_or(address, |(name, _)| name),
        aliases: aliases.join(" "),
    })
}

/// Rewrites every artist, project and profile entry from shown names and empties the queue.
/// Returns the number of artists indexed.
pub(crate) fn rebuild(c: &Connection) -> Result<usize, StoreError> {
    let n = rebuild_credits(c)?;
    rebuild_profiles(c)?;
    Ok(n)
}

/// Rewrites every profile entry: each address any name source carries, once. Returns the
/// number of entries written.
pub(crate) fn rebuild_profiles(c: &Connection) -> Result<usize, StoreError> {
    c.execute_batch("DELETE FROM profile_fts; DELETE FROM profile_names;")
        .db()?;
    // Deduplicated here rather than by a SQL UNION, which would sort in a temporary file.
    let mut addresses: BTreeSet<String> = BTreeSet::new();
    for sql in [
        "SELECT address FROM users",
        "SELECT address FROM artists WHERE address IS NOT NULL",
        "SELECT address FROM profiles_imported WHERE name IS NOT NULL",
        "SELECT address FROM profile_edits WHERE name IS NOT NULL",
        "SELECT address FROM identities",
    ] {
        let mut st = c.prepare(sql).db()?;
        let mut rows = st.query([]).db()?;
        while let Some(r) = rows.next().db()? {
            addresses.insert(r.get(0).db()?);
        }
    }
    let mut written = 0;
    for address in &addresses {
        written += usize::from(refresh_address(c, address)?);
    }
    Ok(written)
}

/// The name `address` is found by: its shown name, unless it is an escrow or burn address.
fn searchable_name(c: &Connection, address: &str) -> Result<Option<String>, StoreError> {
    if !is_tezos_address(address) || is_known_address(c, address)? {
        return Ok(None);
    }
    Ok(profile::shown_name(c, address)?.map(|(name, _)| name))
}

/// Makes the entry of `address` match its searchable name; whether anything changed.
fn refresh_address(c: &Connection, address: &str) -> Result<bool, StoreError> {
    let want = searchable_name(c, address)?;
    let stored: Option<(i64, String)> = c
        .prepare_cached("SELECT id, name FROM profile_names WHERE address = ?1")
        .db()?
        .query_row([address], |r| Ok((r.get(0)?, r.get(1)?)))
        .optional()
        .db()?;
    match (stored, want) {
        (None, None) => Ok(false),
        (Some((_, old)), Some(new)) if old == new => Ok(false),
        (Some((id, _)), want) => {
            c.prepare_cached("DELETE FROM profile_fts WHERE rowid = ?1")
                .db()?
                .execute([id])
                .db()?;
            match want {
                Some(name) => {
                    c.prepare_cached(
                        "UPDATE profile_names SET name = ?2, name_key = ?3 WHERE id = ?1",
                    )
                    .db()?
                    .execute(params![id, name, profile::name_key(&name)])
                    .db()?;
                    insert_fts(c, id, &name)?;
                }
                None => {
                    c.prepare_cached("DELETE FROM profile_names WHERE id = ?1")
                        .db()?
                        .execute([id])
                        .db()?;
                }
            }
            Ok(true)
        }
        (None, Some(name)) => {
            c.prepare_cached(
                "INSERT INTO profile_names (address, name, name_key) VALUES (?1, ?2, ?3)",
            )
            .db()?
            .execute(params![address, name, profile::name_key(&name)])
            .db()?;
            insert_fts(c, c.last_insert_rowid(), &name)?;
            Ok(true)
        }
    }
}

fn insert_fts(c: &Connection, id: i64, name: &str) -> Result<(), StoreError> {
    c.prepare_cached("INSERT INTO profile_fts (rowid, name) VALUES (?1, ?2)")
        .db()?
        .execute(params![id, name])
        .db()?;
    Ok(())
}

/// Rewrites every artist and project entry from shown names and empties the queue (the v8
/// indexes). Returns the number of artists indexed.
fn rebuild_credits(c: &Connection) -> Result<usize, StoreError> {
    c.execute_batch("DELETE FROM artist_fts; DELETE FROM project_fts; DELETE FROM search_dirty;")
        .db()?;
    let artists: Vec<ArtistRow> = c
        .prepare("SELECT seq, id, name, address FROM artists ORDER BY seq")
        .db()?
        .query_map([], artist_row)
        .db()?
        .collect::<rusqlite::Result<_>>()
        .db()?;
    let mut labels = Labels::default();
    {
        let mut ins = c
            .prepare_cached("INSERT INTO artist_fts (rowid, name, aliases) VALUES (?1, ?2, ?3)")
            .db()?;
        for a in &artists {
            let e = labels.entry(c, a)?;
            ins.execute(params![a.seq, e.label, e.aliases]).db()?;
        }
    }
    let mut credits: HashMap<i64, Vec<String>> = HashMap::new();
    {
        let mut st = c
            .prepare(
                "SELECT project_id, artist_id FROM project_artists ORDER BY project_id, position",
            )
            .db()?;
        let mut rows = st.query([]).db()?;
        while let Some(r) = rows.next().db()? {
            credits
                .entry(r.get(0).db()?)
                .or_default()
                .push(r.get(1).db()?);
        }
    }
    let mut ins = c
        .prepare_cached("INSERT INTO project_fts (rowid, title, artists) VALUES (?1, ?2, ?3)")
        .db()?;
    let mut st = c
        .prepare("SELECT id, title FROM projects ORDER BY id")
        .db()?;
    let mut rows = st.query([]).db()?;
    while let Some(r) = rows.next().db()? {
        let id: i64 = r.get(0).db()?;
        let title: String = r.get(1).db()?;
        let names: Vec<&str> = credits
            .get(&id)
            .into_iter()
            .flatten()
            .filter_map(|a| labels.entries.get(a).map(|e| e.label.as_str()))
            .collect();
        ins.execute(params![id, title, names.join(" ")]).db()?;
    }
    Ok(artists.len())
}

/// Refreshes the entries the queued writes can have changed and empties the queue. Returns
/// the number of entries rewritten.
pub(crate) fn drain(c: &Connection) -> Result<usize, StoreError> {
    let values: Vec<String> = c
        .prepare_cached("SELECT value FROM search_dirty")
        .db()?
        .query_map([], |r| r.get(0))
        .db()?
        .collect::<rusqlite::Result<_>>()
        .db()?;
    if values.is_empty() {
        return Ok(0);
    }
    let mut seqs: BTreeSet<i64> = BTreeSet::new();
    {
        let mut st = c.prepare_cached(TOUCHED_ARTISTS).db()?;
        for v in &values {
            let rows = st.query_map([v], |r| r.get::<_, i64>(0)).db()?;
            for seq in rows {
                seqs.insert(seq.db()?);
            }
        }
    }
    let mut addresses: BTreeSet<String> = BTreeSet::new();
    {
        let mut st = c.prepare_cached(TOUCHED_ADDRESSES).db()?;
        for v in &values {
            let rows = st.query_map([v], |r| r.get::<_, String>(0)).db()?;
            for a in rows {
                addresses.insert(a.db()?);
            }
        }
    }
    let mut written = 0;
    for address in &addresses {
        written += usize::from(refresh_address(c, address)?);
    }
    let mut labels = Labels::default();
    let mut projects: BTreeSet<i64> = BTreeSet::new();
    for seq in seqs {
        let Some(a) = c
            .prepare_cached("SELECT seq, id, name, address FROM artists WHERE seq = ?1")
            .db()?
            .query_row([seq], artist_row)
            .optional()
            .db()?
        else {
            continue;
        };
        let e = labels.entry(c, &a)?;
        let stored: Option<(String, String)> = c
            .prepare_cached("SELECT name, aliases FROM artist_fts WHERE rowid = ?1")
            .db()?
            .query_row([seq], |r| Ok((r.get(0)?, r.get(1)?)))
            .optional()
            .db()?;
        if stored.as_ref() != Some(&(e.label.clone(), e.aliases.clone())) {
            c.prepare_cached("DELETE FROM artist_fts WHERE rowid = ?1")
                .db()?
                .execute([seq])
                .db()?;
            c.prepare_cached("INSERT INTO artist_fts (rowid, name, aliases) VALUES (?1, ?2, ?3)")
                .db()?
                .execute(params![seq, e.label, e.aliases])
                .db()?;
            written += 1;
        }
        let rows = c
            .prepare_cached(ARTIST_PROJECTS)
            .db()?
            .query_map([&a.id], |r| r.get::<_, i64>(0))
            .db()?
            .collect::<rusqlite::Result<Vec<_>>>()
            .db()?;
        projects.extend(rows);
    }
    for id in projects {
        written += usize::from(refresh_project(c, &mut labels, id)?);
    }
    c.prepare_cached("DELETE FROM search_dirty")
        .db()?
        .execute([])
        .db()?;
    Ok(written)
}

/// Rewrites one project's entry if it differs from its title and credited labels.
fn refresh_project(c: &Connection, labels: &mut Labels, id: i64) -> Result<bool, StoreError> {
    let title: Option<String> = c
        .prepare_cached("SELECT title FROM projects WHERE id = ?1")
        .db()?
        .query_row([id], |r| r.get(0))
        .optional()
        .db()?;
    let Some(title) = title else {
        return Ok(false);
    };
    let credited: Vec<ArtistRow> = c
        .prepare_cached(PROJECT_CREDITS)
        .db()?
        .query_map([id], artist_row)
        .db()?
        .collect::<rusqlite::Result<_>>()
        .db()?;
    let mut names = Vec::with_capacity(credited.len());
    for a in &credited {
        names.push(labels.entry(c, a)?.label);
    }
    let want = (title, names.join(" "));
    let stored: Option<(String, String)> = c
        .prepare_cached("SELECT title, artists FROM project_fts WHERE rowid = ?1")
        .db()?
        .query_row([id], |r| Ok((r.get(0)?, r.get(1)?)))
        .optional()
        .db()?;
    if stored.as_ref() == Some(&want) {
        return Ok(false);
    }
    c.prepare_cached("DELETE FROM project_fts WHERE rowid = ?1")
        .db()?
        .execute([id])
        .db()?;
    c.prepare_cached("INSERT INTO project_fts (rowid, title, artists) VALUES (?1, ?2, ?3)")
        .db()?
        .execute(params![id, want.0, want.1])
        .db()?;
    Ok(true)
}
