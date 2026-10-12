//! Name numbers (v10): which address shows a shared name plain and which show `name-N`.
//! See the rule in the [`profile`](super::profile) module docs.

use std::cmp::Reverse;
use std::collections::{BTreeMap, HashMap, HashSet};

use rusqlite::{Connection, TransactionBehavior, params};

use super::{Db, Store, StoreError, profile, search};
use profile::{name_key, usable};

/// v9 -> v10, additive. Number 1 is the plain name (`shown_key = name_key`). Idempotent.
pub(crate) const SCHEMA_V10: &str = "
CREATE TABLE IF NOT EXISTS name_numbers (
  name_key TEXT NOT NULL,
  address TEXT NOT NULL,
  number INTEGER NOT NULL CHECK (number >= 1),
  shown_key TEXT NOT NULL,
  PRIMARY KEY (name_key, address),
  UNIQUE (name_key, number),
  UNIQUE (shown_key)
) STRICT, WITHOUT ROWID;
";

/// The table is created by the caller (see `Store::migrate`).
pub(crate) fn migrate_v10(tx: &Connection) -> Result<(), StoreError> {
    assign(tx)?;
    search::rebuild(tx).map(|_| ())
}

/// What one [`Store::assign_name_numbers`] run found and changed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NamesNumbered {
    /// Name keys carried by two or more addresses.
    pub shared_names: usize,
    /// Current carriers of those names that hold a number.
    pub numbered: usize,
    /// Rows inserted by this run.
    pub assigned: usize,
    /// Existing rows whose number changed (re-rank).
    pub moved: usize,
    /// Rows of addresses that no longer carry their name, kept so numbers are not reused.
    pub tombstones: usize,
    /// Plain-name holders of the shared names, by class.
    pub plain_claimed: usize,
    pub plain_curated: usize,
    pub plain_credited: usize,
    pub plain_other: usize,
}

#[derive(Debug)]
struct Carrier {
    claimed: bool,
    curated: bool,
}

/// Name key -> address -> what that address carries under the key.
type Carriers = BTreeMap<String, BTreeMap<String, Carrier>>;

#[derive(Debug, Clone, Default)]
struct Standing {
    credited: bool,
    volume: u64,
    first_seen: Option<String>,
}

/// Every name an address stores, by source, before choosing its candidate.
#[derive(Default)]
struct Stored {
    known: bool,
    edit: Option<String>,
    identity: Option<String>,
    /// Imported names in precedence order: artist, user, fxhash, objkt, tzprofiles.
    imported: [Option<String>; 5],
}

/// Carriers of every key: each address counts once, under its candidate name only (claimed,
/// curated, else its first usable imported name; known addresses have none). Also the keys
/// no generated name may take.
fn load_carriers(c: &Connection) -> Result<(Carriers, HashSet<String>), StoreError> {
    let mut all: BTreeMap<String, Stored> = BTreeMap::new();
    let mut read = |sql: &str, f: &mut dyn FnMut(&mut Stored, String)| -> Result<(), StoreError> {
        let mut st = c.prepare(sql).db()?;
        let mut rows = st.query([]).db()?;
        while let Some(r) = rows.next().db()? {
            let address: String = r.get(0).db()?;
            let name: Option<String> = r.get(1).db()?;
            if let Some(name) = name {
                f(all.entry(address).or_default(), name);
            }
        }
        Ok(())
    };
    read("SELECT address, name FROM profile_edits", &mut |s, n| {
        s.edit = Some(n);
    })?;
    read("SELECT address, name FROM identities", &mut |s, n| {
        s.identity = Some(n);
    })?;
    // The first artist row of an address is the one its profile reads.
    read(
        "SELECT address, name FROM artists WHERE address IS NOT NULL ORDER BY seq DESC",
        &mut |s, n| s.imported[0] = Some(n),
    )?;
    read("SELECT address, name FROM users", &mut |s, n| {
        s.imported[1] = Some(n);
    })?;
    for (i, source) in ["fxhash", "objkt", "tzprofiles"].into_iter().enumerate() {
        read(
            &format!("SELECT address, name FROM profiles_imported WHERE source = '{source}'"),
            &mut |s, n| s.imported[2 + i] = Some(n),
        )?;
    }
    let mut taken: HashSet<String> = HashSet::new();
    let mut st = c
        .prepare("SELECT address, name FROM known_addresses")
        .db()?;
    let mut rows = st.query([]).db()?;
    while let Some(r) = rows.next().db()? {
        let (address, name): (String, String) = (r.get(0).db()?, r.get(1).db()?);
        taken.insert(name_key(&name));
        all.entry(address).or_default().known = true;
    }
    let mut carriers = Carriers::new();
    for (address, s) in all {
        if s.known {
            continue;
        }
        let (name, claimed, curated) = if let Some(n) = s.edit {
            (n, true, false)
        } else if let Some(n) = s.identity {
            (n, false, true)
        } else if let Some(n) = s.imported.into_iter().flatten().find(|n| usable(n)) {
            (n, false, false)
        } else {
            continue;
        };
        carriers
            .entry(name_key(&name))
            .or_default()
            .insert(address, Carrier { claimed, curated });
    }
    taken.extend(carriers.keys().cloned());
    Ok((carriers, taken))
}

fn standing(c: &Connection, address: &str) -> Result<Standing, StoreError> {
    let credits: Vec<i64> = c
        .prepare_cached(
            "SELECT coalesce(v.volume_mutez, 0) FROM artists a \
             JOIN project_artists pa ON pa.artist_id = a.id \
             JOIN projects p ON p.id = pa.project_id AND p.hidden = 0 \
             LEFT JOIN project_volume v ON v.project_id = p.id \
             WHERE a.address = ?1 GROUP BY p.id",
        )
        .db()?
        .query_map([address], |r| r.get(0))
        .db()?
        .collect::<rusqlite::Result<_>>()
        .db()?;
    let created: Option<String> = c
        .prepare_cached(
            "SELECT min(p.created_at) FROM artists a \
             JOIN project_artists pa ON pa.artist_id = a.id \
             JOIN projects p ON p.id = pa.project_id WHERE a.address = ?1",
        )
        .db()?
        .query_row([address], |r| r.get(0))
        .db()?;
    let minted: Option<String> = c
        .prepare_cached("SELECT min(minted) FROM tokens WHERE minter = ?1")
        .db()?
        .query_row([address], |r| r.get(0))
        .db()?;
    Ok(Standing {
        credited: !credits.is_empty(),
        volume: credits.iter().fold(0u64, |s, v| {
            s.saturating_add(u64::try_from(*v).unwrap_or(0))
        }),
        first_seen: [created, minted].into_iter().flatten().min(),
    })
}

/// 0 claimed, 1 curated, 2 credited, 3 everyone else.
fn class(carrier: &Carrier, s: &Standing) -> u8 {
    if carrier.claimed {
        0
    } else if carrier.curated {
        1
    } else if s.credited {
        2
    } else {
        3
    }
}

type SeenKey<'a> = (bool, Option<&'a str>, &'a str);

/// First seen ascending, none last, then address.
fn seen_key<'a>(s: &'a Standing, address: &'a str) -> SeenKey<'a> {
    (s.first_seen.is_none(), s.first_seen.as_deref(), address)
}

/// Smallest number >= 2 free for `key` whose generated key nobody carries or holds.
fn next_number(
    key: &str,
    used: &HashSet<i64>,
    taken: &HashSet<String>,
    shown: &HashSet<String>,
) -> (i64, String) {
    (2..)
        .find_map(|n| {
            let generated = format!("{key}-{n}");
            (!used.contains(&n) && !taken.contains(&generated) && !shown.contains(&generated))
                .then_some((n, generated))
        })
        .expect("an unused number exists")
}

fn shown_key(key: &str, number: i64) -> String {
    if number == 1 {
        key.to_owned()
    } else {
        format!("{key}-{number}")
    }
}

/// Assigns numbers to every name carried by two or more addresses or held as another key's
/// generated name, in the caller's transaction, and queues the names and addresses it changed in `search_dirty`; the caller
/// drains. Idempotent.
pub(crate) fn assign(c: &Connection) -> Result<NamesNumbered, StoreError> {
    let (carriers, taken) = load_carriers(c)?;
    let mut rows: BTreeMap<String, BTreeMap<String, i64>> = BTreeMap::new();
    let mut shown: HashSet<String> = HashSet::new();
    // Keys a row holds as another key's generated name: their plain name (number 1) is taken.
    let mut generated: HashSet<String> = HashSet::new();
    {
        let mut st = c
            .prepare("SELECT name_key, address, number, shown_key FROM name_numbers")
            .db()?;
        let mut it = st.query([]).db()?;
        while let Some(r) = it.next().db()? {
            let (key, shown_key): (String, String) = (r.get(0).db()?, r.get(3).db()?);
            if shown_key != key {
                generated.insert(shown_key.clone());
            }
            rows.entry(key)
                .or_default()
                .insert(r.get(1).db()?, r.get(2).db()?);
            shown.insert(shown_key);
        }
    }
    let mut standings: HashMap<String, Standing> = HashMap::new();
    let mut report = NamesNumbered::default();
    let mut dirty: Vec<String> = Vec::new();
    let mut queued_keys: Vec<String> = Vec::new();

    for (key, current) in &carriers {
        // A key that is another address's generated name is numbered from 2, even for a
        // single carrier, so its plain form never shows for two addresses.
        let generated = generated.contains(key);
        if current.len() < 2 && !generated {
            continue;
        }
        if current.len() >= 2 {
            report.shared_names += 1;
        }
        for address in current.keys() {
            if !standings.contains_key(address) {
                standings.insert(address.clone(), standing(c, address)?);
            }
        }
        let st_of = |a: &str| &standings[a];
        let class_of = |a: &str| class(&current[a], st_of(a));
        let mut order: Vec<&str> = current.keys().map(String::as_str).collect();
        order.sort_by_key(|a| {
            let s = st_of(a);
            let cl = class_of(a);
            (
                cl,
                Reverse(if cl == 2 { s.volume } else { 0 }),
                seen_key(s, a),
            )
        });
        let best = order[0];

        let before = rows.get(key).cloned().unwrap_or_default();
        let mut now = before.clone();
        let holder: Option<String> = before
            .iter()
            .find(|(_, n)| **n == 1)
            .map(|(a, _)| a.clone());
        let promote = !generated
            && match holder.as_deref() {
                None => true,
                Some(h) if h == best => false,
                // A holder that stopped carrying the name is outranked by any carrier.
                Some(h) => current.get(h).is_none_or(|_| class_of(best) < class_of(h)),
            };
        let mut used: HashSet<i64> = now.values().copied().collect();
        if promote {
            let old = now.get(best).copied();
            if let Some(h) = &holder {
                // The displaced holder takes the number the new holder leaves, or the next one.
                let n = match old {
                    Some(n) => n,
                    None => {
                        let (n, g) = next_number(key, &used, &taken, &shown);
                        shown.insert(g);
                        n
                    }
                };
                used.insert(n);
                now.insert(h.clone(), n);
            }
            now.insert(best.to_owned(), 1);
            used.insert(1);
        }
        let mut fresh: Vec<&str> = order
            .iter()
            .copied()
            .filter(|a| !now.contains_key(*a))
            .collect();
        fresh.sort_by_key(|a| seen_key(st_of(a), a));
        for a in fresh {
            let (n, g) = next_number(key, &used, &taken, &shown);
            shown.insert(g);
            used.insert(n);
            now.insert(a.to_owned(), n);
        }

        let changed: Vec<&String> = now
            .keys()
            .filter(|a| before.get(*a) != now.get(*a))
            .collect();
        if !changed.is_empty() {
            let mut del = c
                .prepare_cached("DELETE FROM name_numbers WHERE name_key = ?1 AND address = ?2")
                .db()?;
            let mut ins = c
                .prepare_cached(
                    "INSERT INTO name_numbers (name_key, address, number, shown_key) \
                     VALUES (?1, ?2, ?3, ?4)",
                )
                .db()?;
            for a in &changed {
                if let Some(old) = before.get(*a) {
                    del.execute(params![key, a]).db()?;
                    shown.remove(&shown_key(key, *old));
                    report.moved += 1;
                } else {
                    report.assigned += 1;
                }
            }
            for a in &changed {
                let n = now[*a];
                ins.execute(params![key, a, n, shown_key(key, n)]).db()?;
                shown.insert(shown_key(key, n));
            }
            dirty.push(key.clone());
            dirty.extend(changed.iter().map(|a| (*a).clone()));
        }
        rows.insert(key.clone(), now);
    }

    for (key, held) in &rows {
        // A key that lost a carrier: its remaining carriers' entries may change.
        if held
            .keys()
            .any(|a| carriers.get(key).is_none_or(|m| !m.contains_key(a)))
        {
            queued_keys.push(key.clone());
        }
        for (address, number) in held {
            match carriers.get(key).and_then(|m| m.get(address)) {
                None => report.tombstones += 1,
                Some(carrier) if carriers[key].len() >= 2 => {
                    report.numbered += 1;
                    if *number == 1 {
                        match class(carrier, &standings[address]) {
                            0 => report.plain_claimed += 1,
                            1 => report.plain_curated += 1,
                            2 => report.plain_credited += 1,
                            _ => report.plain_other += 1,
                        }
                    }
                }
                Some(_) => {}
            }
        }
    }
    let mut queue = c
        .prepare_cached(
            "INSERT INTO search_dirty (value) SELECT ?1 \
             WHERE NOT EXISTS (SELECT 1 FROM search_dirty WHERE value = ?1)",
        )
        .db()?;
    for v in dirty.into_iter().chain(queued_keys) {
        queue.execute([v]).db()?;
    }
    Ok(report)
}

impl Store {
    /// Numbers the names several addresses carry (see the [`profile`] module docs) in one
    /// immediate transaction, then refreshes the search entries it changed. Idempotent.
    pub fn assign_name_numbers(&mut self) -> Result<NamesNumbered, StoreError> {
        let tx = self
            .conn_mut()
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .db()?;
        let report = assign(&tx)?;
        search::drain(&tx)?;
        tx.commit().db()?;
        Ok(report)
    }
}
