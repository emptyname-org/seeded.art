//! Durable accept bindings and canonical fee evidence, independent of browser receipts.
use super::{Operation, Plan, TradingError, confirmation::FeeEvasion, types::V2};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs,
    path::Path,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

const APPLICATION_ID: i64 = 0x5eedfee;
const MAX_PENDING: i64 = 16_384;
const MAX_ACTOR_PENDING: i64 = 32;
const PENDING_AGE: u64 = 7 * 86400;
const OUTCOME_AGE: u64 = 30 * 86400;
const MAX_OUTCOMES: i64 = 16_384;
const MAX_RECORD: usize = 32_768;
const PRUNE_INTERVAL: u64 = 60;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct StoredOperation {
    source: String,
    counter: String,
    destination: String,
    amount: String,
    #[serde(default)]
    parameters: serde_json::Value,
}
impl StoredOperation {
    fn operation(&self) -> Operation {
        Operation {
            kind: "transaction",
            source: self.source.clone(),
            counter: self.counter.clone(),
            destination: self.destination.clone(),
            amount: self.amount.clone(),
            parameters: self.parameters.clone(),
            fee: "0".into(),
            gas_limit: "0".into(),
            storage_limit: "0".into(),
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct Binding {
    pub actor: String,
    pub counter: u64,
    pub recipient: String,
    pub amount: u64,
    pub level: u64,
    pub operations: Vec<StoredOperation>,
}
impl Binding {
    pub fn from_plan(actor: &str, plan: &Plan) -> Result<Option<Self>, TradingError> {
        let Some(fees) = plan.simulation.accept_fees.as_ref() else {
            return Ok(None);
        };
        if fees.platform_fee_mutez == 0 || fees.platform_fee_address == actor {
            return Ok(None);
        }
        let accept = plan
            .simulation
            .operations
            .iter()
            .find(|op| {
                op.destination == V2
                    && matches!(
                        op.parameters["entrypoint"].as_str(),
                        Some("offer_accept" | "collection_offer_accept")
                    )
            })
            .ok_or(TradingError::Invalid)?;
        Ok(Some(Self {
            actor: actor.into(),
            counter: accept.counter.parse().map_err(|_| TradingError::Invalid)?,
            recipient: fees.platform_fee_address.clone(),
            amount: fees.platform_fee_mutez,
            level: plan.checked.head.level,
            operations: plan
                .simulation
                .operations
                .iter()
                .map(|op| StoredOperation {
                    source: op.source.clone(),
                    counter: op.counter.clone(),
                    destination: op.destination.clone(),
                    amount: op.amount.clone(),
                    parameters: op.parameters.clone(),
                })
                .collect(),
        }))
    }
    fn accept_key(&self) -> Result<String, TradingError> {
        let accept = self
            .operations
            .iter()
            .find(|op| {
                op.destination == V2
                    && matches!(
                        op.parameters["entrypoint"].as_str(),
                        Some("offer_accept" | "collection_offer_accept")
                    )
            })
            .ok_or(TradingError::Invalid)?;
        Ok(hex::encode(Sha256::digest(
            serde_json::to_vec(accept).map_err(unavailable)?,
        )))
    }
    pub fn operations(&self) -> Vec<Operation> {
        self.operations
            .iter()
            .map(StoredOperation::operation)
            .collect()
    }
    pub fn id(&self) -> Result<String, TradingError> {
        // Repeated reviews with the same chain counter and terms share an audit.
        let mut binding = self.clone();
        binding.level = 0;
        Ok(hex::encode(Sha256::digest(
            serde_json::to_vec(&binding).map_err(unavailable)?,
        )))
    }
}
pub(super) struct Recorded {
    pub first: bool,
    pub ambiguous: bool,
    pub evasion: Option<FeeEvasion>,
}
#[derive(Clone)]
pub(super) struct Audit(Arc<Mutex<Connection>>);
pub(super) struct Due {
    pub id: String,
    pub binding: Binding,
    pub cursor: u64,
}
fn integer(value: u64) -> Result<i64, TradingError> {
    i64::try_from(value).map_err(unavailable)
}
fn unavailable(_: impl std::fmt::Display) -> TradingError {
    TradingError::Unavailable
}
pub(super) fn now() -> Result<u64, TradingError> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(unavailable)?
        .as_secs())
}
impl Audit {
    pub fn open(path: &Path) -> Result<Self, TradingError> {
        let mut options = fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        match options.open(path) {
            Ok(file) => drop(file),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(unavailable(e)),
        }
        let meta = fs::symlink_metadata(path).map_err(unavailable)?;
        if !meta.is_file() {
            return Err(TradingError::Unavailable);
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if meta.permissions().mode() & 0o077 != 0 {
                return Err(TradingError::Unavailable);
            }
        }
        let mut conn = Connection::open_with_flags(
            path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(unavailable)?;
        conn.busy_timeout(Duration::from_millis(250))
            .map_err(unavailable)?;
        let app: i64 = conn
            .query_row("PRAGMA application_id", [], |r| r.get(0))
            .map_err(unavailable)?;
        let version: i64 = conn
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .map_err(unavailable)?;
        let tables: i64 = conn
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE type='table'",
                [],
                |r| r.get(0),
            )
            .map_err(unavailable)?;
        if !((app == 0 && version == 0 && tables == 0)
            || (app == APPLICATION_ID && matches!(version, 1 | 2)))
        {
            return Err(TradingError::Unavailable);
        }
        let page_size: i64 = conn
            .query_row("PRAGMA page_size", [], |r| r.get(0))
            .map_err(unavailable)?;
        if page_size != 4096 {
            return Err(TradingError::Unavailable);
        }
        // DELETE avoids an unbounded WAL when readers or backup jobs stay open.
        conn.execute_batch(
            "PRAGMA journal_mode=DELETE; PRAGMA synchronous=FULL; PRAGMA max_page_count=32768;",
        )
        .map_err(unavailable)?;
        let tx = conn.transaction().map_err(unavailable)?;
        tx.execute_batch(
            "CREATE TABLE IF NOT EXISTS pending (
            id TEXT PRIMARY KEY, actor TEXT NOT NULL, counter INTEGER NOT NULL,
            binding TEXT NOT NULL CHECK(length(binding)<=32768), next_try INTEGER NOT NULL,
            cursor INTEGER NOT NULL DEFAULT 0, created_at INTEGER NOT NULL DEFAULT 0);
            CREATE INDEX IF NOT EXISTS pending_due ON pending(next_try);
            CREATE INDEX IF NOT EXISTS pending_actor ON pending(actor,counter);
            CREATE TABLE IF NOT EXISTS terms (
            binding_id TEXT PRIMARY KEY, accept_key TEXT NOT NULL,
            recipient TEXT NOT NULL, amount TEXT NOT NULL);
            CREATE INDEX IF NOT EXISTS terms_accept ON terms(accept_key);
            CREATE TABLE IF NOT EXISTS outcomes (
            operation_hash TEXT PRIMARY KEY, binding_id TEXT NOT NULL UNIQUE,
            evidence TEXT NOT NULL, recorded_at INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS settlements (
            binding_id TEXT PRIMARY KEY, operation_hash TEXT NOT NULL);
            CREATE INDEX IF NOT EXISTS settlements_hash ON settlements(operation_hash);
            CREATE INDEX IF NOT EXISTS outcomes_recent ON outcomes(recorded_at DESC,operation_hash ASC);
            CREATE TABLE IF NOT EXISTS audit_maintenance (
            name TEXT PRIMARY KEY, last_run INTEGER NOT NULL);",
        )
        .map_err(unavailable)?;
        if version == 1 {
            tx.execute_batch(
                "ALTER TABLE pending ADD COLUMN created_at INTEGER NOT NULL DEFAULT 0;",
            )
            .map_err(unavailable)?;
            tx.execute("UPDATE pending SET created_at=?1", [integer(now()?)?])
                .map_err(unavailable)?;
        }
        tx.execute_batch(
            "CREATE INDEX IF NOT EXISTS pending_age ON pending(created_at);
             CREATE INDEX IF NOT EXISTS pending_actor_age ON pending(actor,created_at,counter,id);",
        )
        .map_err(unavailable)?;
        tx.execute_batch(
            "INSERT OR IGNORE INTO settlements SELECT binding_id,operation_hash FROM outcomes;",
        )
        .map_err(unavailable)?;
        tx.pragma_update(None, "application_id", APPLICATION_ID)
            .map_err(unavailable)?;
        tx.pragma_update(None, "user_version", 2)
            .map_err(unavailable)?;
        tx.commit().map_err(unavailable)?;
        Ok(Self(Arc::new(Mutex::new(conn))))
    }
    async fn run<T: Send + 'static>(
        &self,
        work: impl FnOnce(&mut Connection) -> Result<T, TradingError> + Send + 'static,
    ) -> Result<T, TradingError> {
        let conn = self.0.clone();
        tokio::task::spawn_blocking(move || work(&mut *conn.lock().map_err(unavailable)?))
            .await
            .map_err(unavailable)?
    }
    /// Capacity is best effort: missing telemetry must not prevent a valid accept.
    pub async fn record(&self, binding: Binding) -> Result<bool, TradingError> {
        let id = binding.id()?;
        let accept_key = binding.accept_key()?;
        let json = serde_json::to_string(&binding).map_err(unavailable)?;
        if json.len() > MAX_RECORD {
            return Err(TradingError::TooLarge);
        }
        let time = now()?;
        let due = time.saturating_add(super::RECEIPT_TTL.as_secs());
        self.run(move |conn| {
            let tx = conn.transaction().map_err(unavailable)?;
            let cutoff = integer(time.saturating_sub(PENDING_AGE))?;
            let active: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM pending WHERE id=?1 AND created_at>?2) OR EXISTS(SELECT 1 FROM settlements WHERE binding_id=?1)",
                params![id, cutoff],
                |r| r.get(0),
            ).map_err(unavailable)?;
            if !active {
                tx.execute("DELETE FROM pending WHERE id=?1", [&id]).map_err(unavailable)?;
                tx.execute(
                    "DELETE FROM terms WHERE binding_id=?1 AND NOT EXISTS(SELECT 1 FROM settlements WHERE binding_id=?1)",
                    [&id],
                ).map_err(unavailable)?;
                let (mut total, mut owned): (i64, i64) = tx.query_row(
                    "SELECT (SELECT count(*) FROM pending WHERE created_at>?1), (SELECT count(*) FROM pending WHERE actor=?2 AND created_at>?1)",
                    params![cutoff, binding.actor],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                ).map_err(unavailable)?;
                if owned >= MAX_ACTOR_PENDING {
                    let oldest: Option<String> = tx.query_row(
                        "SELECT id FROM pending WHERE actor=?1 AND created_at>?2 ORDER BY created_at,counter,id LIMIT 1",
                        params![binding.actor, cutoff],
                        |r| r.get(0),
                    ).optional().map_err(unavailable)?;
                    let Some(oldest) = oldest else {
                        tx.commit().map_err(unavailable)?;
                        return Ok(false);
                    };
                    tx.execute("DELETE FROM pending WHERE id=?1", [&oldest]).map_err(unavailable)?;
                    tx.execute(
                        "DELETE FROM terms WHERE binding_id=?1 AND NOT EXISTS(SELECT 1 FROM settlements WHERE binding_id=?1)",
                        [&oldest],
                    ).map_err(unavailable)?;
                    total -= 1;
                    owned -= 1;
                }
                if total >= MAX_PENDING || owned >= MAX_ACTOR_PENDING {
                    tx.commit().map_err(unavailable)?;
                    return Ok(false);
                }
                tx.execute("INSERT OR IGNORE INTO terms(binding_id,accept_key,recipient,amount) VALUES(?1,?2,?3,?4)", params![id,accept_key,binding.recipient,binding.amount.to_string()]).map_err(unavailable)?;
                tx.execute("INSERT INTO pending(id,actor,counter,binding,next_try,created_at) VALUES(?1,?2,?3,?4,?5,?6)", params![id,binding.actor,integer(binding.counter)?,json,integer(due)?,integer(time)?]).map_err(unavailable)?;
            }
            tx.commit().map_err(unavailable)?;
            Ok(true)
        }).await
    }
    pub async fn due(&self) -> Result<Vec<Due>, TradingError> {
        let time = now()?;
        self.run(move |conn| {
            let tx = conn.transaction().map_err(unavailable)?;
            prune_if_due(&tx, time)?;
            let rows = {
                let mut stmt = tx.prepare("SELECT id,actor,counter,binding,cursor FROM pending WHERE next_try<=?1 ORDER BY next_try,id LIMIT 8").map_err(unavailable)?;
                stmt.query_map([integer(time)?], |r| Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,i64>(2)?,r.get::<_,String>(3)?,r.get::<_,i64>(4)?))).map_err(unavailable)?.collect::<Result<Vec<_>,_>>().map_err(unavailable)?
            };
            let mut result = Vec::new();
            for (id,actor,counter,json,cursor) in rows {
                let binding = decode(&json).filter(|b| b.actor == actor && integer(b.counter).ok() == Some(counter) && b.id().ok().as_ref() == Some(&id));
                let (Some(binding), Ok(cursor)) = (binding, u64::try_from(cursor)) else {
                    tx.execute("DELETE FROM pending WHERE id=?1", [&id]).map_err(unavailable)?;
                    tx.execute("DELETE FROM terms WHERE binding_id=?1", [&id]).map_err(unavailable)?;
                    eprintln!("platform_fee_audit_invalid_row");
                    continue;
                };
                // Cancellation/restart releases this durable lease without discarding work.
                tx.execute("UPDATE pending SET next_try=?1 WHERE id=?2", params![integer(time+300)?,id]).map_err(unavailable)?;
                result.push(Due{id,binding,cursor});
            }
            tx.commit().map_err(unavailable)?;
            Ok(result)
        }).await
    }
    pub async fn progress(&self, id: String, cursor: u64, delay: u64) -> Result<(), TradingError> {
        let next = now()?.saturating_add(delay);
        self.run(move |conn| {
            conn.execute(
                "UPDATE pending SET cursor=max(cursor,?1),next_try=?2 WHERE id=?3",
                params![integer(cursor)?, integer(next)?, id],
            )
            .map_err(unavailable)?;
            Ok(())
        })
        .await
    }
    /// Only call after scanning through the finalized level used for the RPC counter.
    pub async fn consumed(&self, actor: String, counter: u64) -> Result<(), TradingError> {
        self.run(move |conn| {
            let tx = conn.transaction().map_err(unavailable)?;
            tx.execute(
                "DELETE FROM pending WHERE actor=?1 AND counter=?2",
                params![actor, integer(counter)?],
            )
            .map_err(unavailable)?;
            tx.commit().map_err(unavailable)
        })
        .await
    }
    pub async fn finish(
        &self,
        binding: Binding,
        hash: String,
        block: String,
        level: u64,
        contents: Vec<serde_json::Value>,
    ) -> Result<Recorded, TradingError> {
        let id = binding.id()?;
        let time = now()?;
        self.run(move |conn| {
            let tx = conn.transaction().map_err(unavailable)?;
            let previous: Option<String> = tx.query_row("SELECT operation_hash FROM settlements WHERE binding_id=?1", [&id], |r| r.get(0)).optional().map_err(unavailable)?;
            if previous.as_deref().is_some_and(|old| old!=hash) { return Err(TradingError::Changed); }
            let rows = {
                let mut stmt = tx.prepare("SELECT id,binding FROM pending WHERE actor=?1 AND counter=?2 ORDER BY id LIMIT 16384").map_err(unavailable)?;
                stmt.query_map(params![binding.actor,integer(binding.counter)?], |r| Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?))).map_err(unavailable)?.collect::<Result<Vec<_>,_>>().map_err(unavailable)?
            };
            let mut siblings: Vec<Binding> = rows.iter().filter_map(|(id,json)| decode(json).filter(|b| b.actor == binding.actor && b.counter == binding.counter && b.id().ok().as_ref() == Some(id))).collect();
            if !siblings.iter().any(|b| b.id().ok().as_ref() == Some(&id)) {
                siblings.push(binding.clone());
            }
            // All competing reviews see the same canonical group before any are removed.
            let mut evasion = None;
            let mut ambiguous = false;
            let mut matched = false;
            for sibling in &siblings {
                match super::confirmation::classify_operations(&sibling.actor, &sibling.operations(), Some((&sibling.recipient,sibling.amount)), &contents, &hash, &block, level) {
                    Ok(found) => {
                        matched = true;
                        evasion = evasion.or(found);
                        let key = sibling.accept_key()?;
                        ambiguous |= tx.query_row("SELECT EXISTS(SELECT 1 FROM terms WHERE accept_key=?1 AND (recipient<>?2 OR amount<>?3))", params![key,sibling.recipient,sibling.amount.to_string()], |r| r.get::<_,bool>(0)).map_err(unavailable)?;
                    }
                    Err(TradingError::Changed) => {},
                    Err(error) => return Err(error),
                }
            }
            if ambiguous { evasion = None; }
            let status = if ambiguous {"ambiguous_terms"} else if matched {"classified"} else {"unmatched_group"};
            let evidence = serde_json::to_string(&serde_json::json!({"operation_hash":hash,"block_hash":block,"level":level,"actor":binding.actor,"status":status,"fee_evasion":evasion})).map_err(unavailable)?;
            let inserted = tx.execute("INSERT OR IGNORE INTO outcomes(operation_hash,binding_id,evidence,recorded_at) VALUES(?1,?2,?3,?4)", params![hash,id,evidence,integer(time)?]).map_err(unavailable)?;
            // A v1 unmatched sibling may have won the UNIQUE hash first. Upgrade
            // that incomplete evidence, but never overwrite a classified outcome.
            let mut first = inserted == 1;
            if !first {
                let prior: String = tx.query_row("SELECT evidence FROM outcomes WHERE operation_hash=?1", [&hash], |r| r.get(0)).map_err(unavailable)?;
                let prior: serde_json::Value = serde_json::from_str(&prior).map_err(unavailable)?;
                if prior["status"] == "unmatched_group" && matched {
                    tx.execute("UPDATE outcomes SET evidence=?1 WHERE operation_hash=?2", params![evidence,hash]).map_err(unavailable)?;
                    first = true;
                } else {
                    ambiguous = prior["status"] == "ambiguous_terms";
                    evasion = serde_json::from_value(prior["fee_evasion"].clone()).map_err(unavailable)?;
                }
            }
            for sibling in &siblings {
                let sibling_id = sibling.id()?;
                tx.execute("INSERT OR IGNORE INTO settlements(binding_id,operation_hash) VALUES(?1,?2)", params![sibling_id,hash]).map_err(unavailable)?;
            }
            tx.execute("DELETE FROM pending WHERE actor=?1 AND counter=?2", params![binding.actor,integer(binding.counter)?]).map_err(unavailable)?;
            tx.commit().map_err(unavailable)?;
            Ok(Recorded{first, ambiguous, evasion})
        }).await
    }
}
fn decode(json: &str) -> Option<Binding> {
    let binding: Binding = serde_json::from_str(json).ok()?;
    (binding.operations.len() <= 100 && binding.accept_key().is_ok()).then_some(binding)
}
fn prune_if_due(conn: &Connection, time: u64) -> Result<(), TradingError> {
    let last: Option<i64> = conn
        .query_row(
            "SELECT last_run FROM audit_maintenance WHERE name='prune'",
            [],
            |r| r.get(0),
        )
        .optional()
        .map_err(unavailable)?;
    let needed = last
        .is_none_or(|last| last < 0 || time < last as u64 || time - last as u64 >= PRUNE_INTERVAL);
    if needed {
        prune(conn, time)?;
        conn.execute(
            "INSERT INTO audit_maintenance(name,last_run) VALUES('prune',?1) ON CONFLICT(name) DO UPDATE SET last_run=excluded.last_run",
            [integer(time)?],
        )
        .map_err(unavailable)?;
    }
    Ok(())
}
fn prune(conn: &Connection, time: u64) -> Result<(), TradingError> {
    conn.execute(
        "DELETE FROM pending WHERE created_at<=?1",
        [integer(time.saturating_sub(PENDING_AGE))?],
    )
    .map_err(unavailable)?;
    conn.execute("DELETE FROM outcomes WHERE recorded_at<=?1 OR operation_hash IN (SELECT operation_hash FROM outcomes ORDER BY recorded_at DESC,operation_hash LIMIT -1 OFFSET ?2)", params![integer(time.saturating_sub(OUTCOME_AGE))?,MAX_OUTCOMES]).map_err(unavailable)?;
    conn.execute("DELETE FROM settlements WHERE NOT EXISTS(SELECT 1 FROM outcomes WHERE outcomes.operation_hash=settlements.operation_hash)", []).map_err(unavailable)?;
    conn.execute("DELETE FROM terms WHERE NOT EXISTS(SELECT 1 FROM pending WHERE pending.id=terms.binding_id) AND NOT EXISTS(SELECT 1 FROM settlements WHERE settlements.binding_id=terms.binding_id)", []).map_err(unavailable)?;
    Ok(())
}
