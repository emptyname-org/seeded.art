//! Check new name claims against recent mainnet registry changes.
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use reqwest::{Client, Url};
use rusqlite::OptionalExtension;
use serde_json::Value;

use crate::{
    auth::AuthError,
    model::ProfileEdit,
    store::{Store, StoreError, profile::name_key},
};

const REGISTRY: &str = "KT1Ezht4PDKZri7aVppVGT4Jkw39sesaFnww";
const BODY_LIMIT: usize = 64 * 1024;
const DELTA_PAGE: usize = 100;
const DELTA_LIMIT: usize = 10_000;
const REFRESH_DEADLINE: Duration = Duration::from_secs(8);
const WAIT_DEADLINE: Duration = Duration::from_secs(10);
const HEAD_TTL: Duration = Duration::from_secs(2);

pub(super) fn new_claim_baseline(
    store: &Store,
    address: &str,
    edit: &ProfileEdit,
) -> Result<Option<i64>, StoreError> {
    store.check_profile_edit(address, edit)?;
    let Some(name) = &edit.name else {
        return Ok(None);
    };
    if store
        .profile_edit(address)?
        .and_then(|edit| edit.name)
        .is_some_and(|old| name_key(&old) == name_key(name))
    {
        return Ok(None);
    }
    let level: Option<i64> = store
        .conn()
        .query_row(
            "SELECT synced_level FROM profile_sync WHERE source='fxhash'",
            [],
            |row| row.get(0),
        )
        .optional()
        .map_err(|_| StoreError::Unavailable("Name registry unavailable".into()))?
        .flatten();
    level
        .filter(|level| *level > 0)
        .map(Some)
        .ok_or_else(|| StoreError::Unavailable("Name registry needs an initial import".into()))
}

pub(super) fn guard_write(
    store: &Store,
    baseline: Option<i64>,
    edit: &ProfileEdit,
) -> Result<(), StoreError> {
    if let Some(level) = baseline {
        store
            .conn()
            .execute_batch("CREATE TEMP TABLE claim_baseline(level INTEGER NOT NULL);")
            .map_err(|_| StoreError::Unavailable("Claim check unavailable".into()))?;
        store
            .conn()
            .execute("INSERT INTO claim_baseline VALUES (?1)", [level])
            .map_err(|_| StoreError::Unavailable("Claim check unavailable".into()))?;
        // This connection-local guard runs within the store's immediate transaction.
        // The importer cannot change the baseline between this check and the edit.
        store
            .conn()
            .execute_batch(
                "CREATE TEMP TRIGGER claim_baseline_guard BEFORE INSERT ON main.profile_edits
             WHEN NOT EXISTS (SELECT 1 FROM main.profile_sync, claim_baseline
               WHERE source='fxhash' AND synced_level=claim_baseline.level)
             BEGIN SELECT RAISE(ABORT, 'name registry changed; retry'); END;",
            )
            .map_err(|_| StoreError::Unavailable("Claim check unavailable".into()))?;
    } else if edit.name.is_some() {
        store
            .conn()
            .execute_batch(
                "CREATE TEMP TRIGGER claim_held_guard BEFORE INSERT ON main.profile_edits
             WHEN NOT EXISTS (SELECT 1 FROM main.profile_edits
               WHERE address=NEW.address AND name_key=NEW.name_key)
             BEGIN SELECT RAISE(ABORT, 'name claim changed; retry'); END;",
            )
            .map_err(|_| StoreError::Unavailable("Claim check unavailable".into()))?;
    }
    Ok(())
}

#[derive(Clone)]
pub struct ClaimRegistry {
    client: Client,
    base: Url,
    checked: Arc<tokio::sync::Mutex<RegistryCache>>,
    waiting: Arc<tokio::sync::Semaphore>,
}

#[derive(Default)]
struct RegistryCache {
    head: Option<(Instant, Value)>,
    delta: Option<CheckedDelta>,
}
struct CheckedDelta {
    imported_level: i64,
    head: HeadSnapshot,
    names: Arc<HashMap<String, String>>,
}

#[derive(Clone, PartialEq, Eq)]
struct HeadSnapshot {
    level: i64,
    hash: String,
}

enum FetchError {
    TooLarge,
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimCheckError {
    Conflict,
    Unavailable,
    RateLimited,
}

impl ClaimRegistry {
    pub fn new(base: &str) -> Result<Self, AuthError> {
        let mut base = Url::parse(base).map_err(|_| AuthError::Unavailable)?;
        if !matches!(base.scheme(), "https" | "http")
            || base.host_str().is_none()
            || !base.username().is_empty()
            || base.password().is_some()
            || base.query().is_some()
            || base.fragment().is_some()
        {
            return Err(AuthError::Unavailable);
        }
        let prefix = base.path().trim_end_matches('/');
        let path = if prefix.ends_with("/v1") {
            format!("{prefix}/")
        } else {
            format!("{prefix}/v1/")
        };
        base.set_path(&path);
        Ok(Self {
            client: Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .timeout(Duration::from_secs(2))
                .build()
                .map_err(|_| AuthError::Unavailable)?,
            base,
            checked: Arc::new(tokio::sync::Mutex::new(RegistryCache::default())),
            waiting: Arc::new(tokio::sync::Semaphore::new(32)),
        })
    }

    pub async fn check(
        &self,
        imported_level: i64,
        address: &str,
        candidate_key: &str,
    ) -> Result<(), ClaimCheckError> {
        let _waiting = self
            .waiting
            .clone()
            .try_acquire_owned()
            .map_err(|_| ClaimCheckError::RateLimited)?;
        let mut cache = tokio::time::timeout(WAIT_DEADLINE, self.checked.lock())
            .await
            .map_err(|_| ClaimCheckError::Unavailable)?;
        let names = tokio::time::timeout(REFRESH_DEADLINE, async {
            let head = self.shared_head(&mut cache, imported_level).await?;
            if let Some(delta) = &cache.delta {
                if delta.imported_level == imported_level && delta.head == head {
                    return Ok(Arc::clone(&delta.names));
                }
            }
            let (head, head_value, names) = self.refresh(imported_level, head).await?;
            cache.head = Some((Instant::now(), head_value));
            let names = Arc::new(names);
            cache.delta = Some(CheckedDelta {
                imported_level,
                head,
                names: Arc::clone(&names),
            });
            Ok::<_, ClaimCheckError>(names)
        })
        .await
        .map_err(|_| ClaimCheckError::Unavailable)??;
        drop(cache);
        if names
            .iter()
            .any(|(owner, name)| owner != address && name == candidate_key)
        {
            return Err(ClaimCheckError::Conflict);
        }
        Ok(())
    }

    async fn shared_head(
        &self,
        cache: &mut RegistryCache,
        imported_level: i64,
    ) -> Result<HeadSnapshot, ClaimCheckError> {
        if let Some((at, value)) = &cache.head {
            if at.elapsed() < HEAD_TTL {
                if let Ok(head) = Self::validate_head(value, imported_level) {
                    return Ok(head);
                }
            }
        }
        cache.head = None;
        let value = self.get("head").await?;
        let head = Self::validate_head(&value, imported_level)?;
        cache.head = Some((Instant::now(), value));
        Ok(head)
    }

    fn validate_head(head: &Value, imported_level: i64) -> Result<HeadSnapshot, ClaimCheckError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| ClaimCheckError::Unavailable)?
            .as_secs();
        if head["chainId"] != "NetXdQprcVkpaWU"
            || head["synced"] != true
            || imported_level <= 0
            || !head["timestamp"]
                .as_str()
                .is_some_and(|timestamp| fresh(timestamp, now))
        {
            return Err(ClaimCheckError::Unavailable);
        }
        Ok(HeadSnapshot {
            level: head["level"]
                .as_i64()
                .filter(|level| *level >= imported_level)
                .ok_or(ClaimCheckError::Unavailable)?,
            hash: head["hash"]
                .as_str()
                .ok_or(ClaimCheckError::Unavailable)?
                .to_owned(),
        })
    }

    async fn refresh(
        &self,
        imported_level: i64,
        mut head: HeadSnapshot,
    ) -> Result<(HeadSnapshot, Value, HashMap<String, String>), ClaimCheckError> {
        let mut names = HashMap::new();
        let mut since = imported_level;
        let mut total = 0;
        // Catch changes behind the pagination cursor without restarting a full scan.
        // Repeated head movement still shares the caller's eight-second deadline.
        for _ in 0..4 {
            self.merge_delta(since, &mut names, &mut total).await?;
            let value = self.get("head").await?;
            let latest = Self::validate_head(&value, imported_level)?;
            if latest == head {
                return Ok((head, value, names));
            }
            if latest.level > head.level {
                // Verify the old head survived before trusting an incremental merge.
                let old = self.get(&format!("blocks/{}", head.level)).await?;
                if old["hash"] == head.hash {
                    since = head.level;
                } else {
                    since = imported_level;
                    names.clear();
                }
            } else {
                since = imported_level;
                names.clear();
            }
            head = latest;
        }
        Err(ClaimCheckError::Unavailable)
    }

    async fn merge_delta(
        &self,
        since: i64,
        names: &mut HashMap<String, String>,
        total: &mut usize,
    ) -> Result<(), ClaimCheckError> {
        let mut after = 0_u64;
        let mut limit = DELTA_PAGE;
        loop {
            let path = format!(
                "contracts/{REGISTRY}/bigmaps/users_name/keys?lastLevel.gt={since}&sort.asc=id&offset.cr={after}&limit={limit}&select.fields=id,lastLevel,active,key,value"
            );
            let delta = match self.get_limited(&path).await {
                Ok(delta) => delta,
                Err(FetchError::TooLarge) if limit > 1 => {
                    limit = (limit / 2).max(1);
                    continue;
                }
                Err(_) => return Err(ClaimCheckError::Unavailable),
            };
            let rows = delta
                .as_array()
                .filter(|rows| rows.len() <= limit)
                .ok_or(ClaimCheckError::Unavailable)?;
            *total = total
                .checked_add(rows.len())
                .filter(|count| *count <= DELTA_LIMIT)
                .ok_or(ClaimCheckError::Unavailable)?;
            let mut page_last = after;
            for row in rows {
                let id = row["id"].as_u64().ok_or(ClaimCheckError::Unavailable)?;
                let level = row["lastLevel"]
                    .as_i64()
                    .ok_or(ClaimCheckError::Unavailable)?;
                let active = row["active"]
                    .as_bool()
                    .ok_or(ClaimCheckError::Unavailable)?;
                if id <= page_last || level <= since {
                    return Err(ClaimCheckError::Unavailable);
                }
                page_last = id;
                let address = row["key"].as_str().ok_or(ClaimCheckError::Unavailable)?;
                if !crate::store::is_tezos_address(address) {
                    return Err(ClaimCheckError::Unavailable);
                }
                // Deletions and invalid names remove earlier values during catch-up.
                names.remove(address);
                if active {
                    let encoded = row["value"].as_str().ok_or(ClaimCheckError::Unavailable)?;
                    if let Ok(bytes) = hex::decode(encoded) {
                        if let Ok(value) = String::from_utf8(bytes) {
                            if let Some(name) = crate::ingest::profiles::clean_name(&value) {
                                names.insert(address.to_owned(), name_key(&name));
                            }
                        }
                    }
                }
            }
            if rows.len() < limit {
                return Ok(());
            }
            after = page_last;
        }
    }

    async fn get(&self, path: &str) -> Result<Value, ClaimCheckError> {
        self.get_limited(path)
            .await
            .map_err(|_| ClaimCheckError::Unavailable)
    }
    async fn get_limited(&self, path: &str) -> Result<Value, FetchError> {
        let mut response = self
            .client
            .get(self.base.join(path).map_err(|_| FetchError::Unavailable)?)
            .send()
            .await
            .map_err(|_| FetchError::Unavailable)?;
        if response.status() != reqwest::StatusCode::OK {
            return Err(FetchError::Unavailable);
        }
        if response
            .content_length()
            .is_some_and(|size| size > BODY_LIMIT as u64)
        {
            return Err(FetchError::TooLarge);
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| FetchError::Unavailable)?
        {
            if bytes.len() + chunk.len() > BODY_LIMIT {
                return Err(FetchError::TooLarge);
            }
            bytes.extend_from_slice(&chunk);
        }
        serde_json::from_slice(&bytes).map_err(|_| FetchError::Unavailable)
    }
}

fn fresh(timestamp: &str, now: u64) -> bool {
    let b = timestamp.as_bytes();
    if !(20..=30).contains(&b.len())
        || b[4] != b'-'
        || b[7] != b'-'
        || b[10] != b'T'
        || b[13] != b':'
        || b[16] != b':'
        || b[b.len() - 1] != b'Z'
        || !b[..19]
            .iter()
            .enumerate()
            .all(|(i, c)| [4, 7, 10, 13, 16].contains(&i) || c.is_ascii_digit())
        || (b.len() > 20
            && (b[19] != b'.'
                || b.len() < 22
                || !b[20..b.len() - 1].iter().all(u8::is_ascii_digit)))
    {
        return false;
    }
    let number = |start, end| timestamp[start..end].parse::<u32>().unwrap_or(0);
    let year = number(0, 4);
    let month = number(5, 7);
    let day = number(8, 10);
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let days = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap => 29,
        2 => 28,
        _ => return false,
    };
    if day == 0 || day > days || number(11, 13) > 23 || number(14, 16) > 59 || number(17, 19) > 59 {
        return false;
    }
    let Ok(now) = i64::try_from(now) else {
        return false;
    };
    let lower = crate::ingest::iso_utc(now.saturating_sub(120));
    let upper = crate::ingest::iso_utc(now.saturating_add(30));
    timestamp[..19] >= lower[..19] && timestamp[..19] <= upper[..19]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retaining_a_name_requires_the_claim_to_still_exist_inside_the_write() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tmp");
        let path = root.join(format!(
            "claim-guard-{}.sqlite",
            crate::ingest::now_iso().replace(':', "-") + &std::process::id().to_string()
        ));
        let store = Store::open(&path).unwrap();
        let insert = "INSERT INTO profile_edits(address,name,name_key,bio,links,updated_at,signed_message,signature,public_key)
            VALUES ('holder','HeldName','heldname',NULL,'[]','fixture','proof','signature','key')
            ON CONFLICT(address) DO UPDATE SET name=excluded.name,name_key=excluded.name_key";
        store.conn().execute(insert, []).unwrap();
        guard_write(
            &store,
            None,
            &ProfileEdit {
                name: Some("HeldName".into()),
                bio: None,
                links: vec![],
            },
        )
        .unwrap();
        store.conn().execute(insert, []).unwrap();
        store
            .conn()
            .execute("DELETE FROM profile_edits WHERE address='holder'", [])
            .unwrap();
        assert!(store.conn().execute(insert, []).is_err());
        drop(store);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn freshness_bounds_cover_fractional_seconds_and_reject_stale_or_malformed_heads() {
        let now = 1_759_622_400;
        let current = crate::ingest::iso_utc(now);
        assert!(fresh(&current, now as u64));
        assert!(fresh(&current.replace('Z', ".123Z"), now as u64));
        let past_minute = crate::ingest::iso_utc(now - 60);
        assert!(!fresh(&format!("{}99Z", &past_minute[..17]), now as u64));
        for bad in [
            crate::ingest::iso_utc(now - 121),
            crate::ingest::iso_utc(now + 31),
            current.replace('Z', "+00:00"),
            current.replace('Z', ".Z"),
            current.replace('Z', ".xyzZ"),
            "é".repeat(12),
        ] {
            assert!(!fresh(&bad, now as u64), "{bad}");
        }
    }
}
