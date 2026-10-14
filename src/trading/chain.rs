use super::{
    http::{Endpoint, LARGE, Transport},
    types::*,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    future::Future,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore, oneshot},
    task::JoinHandle,
};

/// Response body limits for review and check reads. Simulations, confirmation and fee
/// reconciliation keep `LARGE`.
pub const SMALL: usize = 64 * 1024;
pub const MEDIUM: usize = 256 * 1024;
/// Upstream requests in flight or answered but not yet taken. Prepare, check and browser
/// confirmation share the foreground window; the confirmation watcher and fee
/// reconciliation share the background one.
pub const FOREGROUND_PERMITS: usize = 12;
pub const BACKGROUND_PERMITS: usize = 2;
/// Indexer requests in flight across all trading work: the public indexer refused 9 of 25
/// parallel requests and none of 12 (2026-10-14).
pub const INDEXER_PERMITS: usize = 6;

tokio::task_local! {
    static BACKGROUND: ();
}

/// Runs `work` with its upstream requests in the background window.
pub async fn background<F: Future>(work: F) -> F::Output {
    BACKGROUND.scope((), work).await
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Head {
    pub hash: String,
    pub level: u64,
}
#[derive(Clone)]
pub struct Checked {
    pub head: Head,
    pub reviews: Vec<ReviewItem>,
    pub proof: String,
    pub operations: Vec<Operation>,
    /// `protocol` and `next_protocol` of the pinned block; a check must see the same pair.
    pub protocols: (String, String),
}
#[derive(Clone, Debug, Serialize)]
pub struct Simulation {
    pub operations: Vec<Operation>,
    #[serde(serialize_with = "super::types::decimal")]
    pub fee_mutez: u64,
    #[serde(serialize_with = "super::types::decimal")]
    pub storage_burn_cap_mutez: u64,
    pub gas: u64,
    pub bytes: usize,
    pub counter: u64,
    pub proceeds_mutez: String,
    pub accept_fees: Option<AcceptFees>,
    /// Storage cost of the reviewed protocol; a check reuses it only for that protocol.
    #[serde(skip)]
    pub cost_per_byte: u64,
}
/// The reads that follow the reviewed records, validated in this order by the caller,
/// which then closes it.
pub struct Tail {
    round: Round,
    /// Check only: whether the reviewed head is within 20 blocks, so its hash was read.
    ancestor: Option<bool>,
    review: bool,
}
/// Fresh simulation inputs from a review or check round.
pub struct Inputs {
    pub counter: u64,
    pub filter: Value,
    pub cost_per_byte: Option<u64>,
}
impl Tail {
    /// The reviewed head is an ancestor of the current one, at most 20 blocks back.
    pub async fn ancestor(&mut self, reviewed: &Head) -> Result<(), TradingError> {
        match self.ancestor {
            None => Err(TradingError::Unavailable),
            Some(false) => Err(TradingError::Changed),
            Some(true) => match self.round.take("ancestor").await? {
                hash if hash == reviewed.hash.as_str() => Ok(()),
                _ => Err(TradingError::Changed),
            },
        }
    }
    pub async fn inputs(&mut self) -> Result<Inputs, TradingError> {
        if self.round.take("manager key").await?.is_null() {
            return Err(TradingError::Unrevealed);
        }
        let counter = number(&self.round.take("counter").await?)?;
        let cost_per_byte = match self.review {
            true => Some(number(
                &self.round.take("constants").await?["cost_per_byte"],
            )?),
            false => None,
        };
        let filter = self.round.take("filter").await?;
        Ok(Inputs {
            counter,
            filter,
            cost_per_byte,
        })
    }
    /// Aborts unused reads and waits for every task to stop.
    pub async fn close(self) {
        self.round.close().await;
    }
}

/// A check's single simulation: whether everything matched the review, and the accept
/// split it produced for the price check.
pub struct AcceptCheck {
    pub unchanged: bool,
    pub accept_fees: Option<AcceptFees>,
}

/// A response that keeps its window permit until it is dropped.
pub struct Held {
    pub value: Value,
    _permit: OwnedSemaphorePermit,
}
struct Read {
    tag: String,
    endpoint: Endpoint,
    path: String,
    limit: usize,
}
impl Read {
    fn rpc(tag: impl Into<String>, path: String, limit: usize) -> Self {
        Self {
            tag: tag.into(),
            endpoint: Endpoint::Rpc,
            path,
            limit,
        }
    }
    fn indexer(tag: impl Into<String>, path: String, limit: usize) -> Self {
        Self {
            tag: tag.into(),
            endpoint: Endpoint::Indexer,
            path,
            limit,
        }
    }
}
/// Reads started together; they acquire window permits in their order. Each task reduces
/// its response to what validation reads before releasing its permit, so at most a
/// window of raw responses exists. Validation takes results by tag when it needs them; a
/// task stays owned by the round while awaited, so dropping the round aborts everything,
/// and `close` also waits until every task has stopped.
pub struct Round {
    reads: HashMap<String, JoinHandle<Result<Value, TradingError>>>,
}
impl Drop for Round {
    fn drop(&mut self) {
        for handle in self.reads.values() {
            handle.abort();
        }
    }
}
impl Round {
    async fn take(&mut self, tag: &str) -> Result<Value, TradingError> {
        let handle = self.reads.get_mut(tag).ok_or(TradingError::Unavailable)?;
        let result = (&mut *handle).await;
        self.reads.remove(tag);
        result.unwrap_or(Err(TradingError::Unavailable))
    }
    fn has(&self, tag: &str) -> bool {
        self.reads.contains_key(tag)
    }
    /// A big-map pair: the indexer row must be active, no newer than the pinned head and
    /// name the locally computed key hash; then the node value at the pinned head.
    async fn pair(
        &mut self,
        head: &Head,
        key: &str,
        tag: &str,
    ) -> Result<(Value, Value), TradingError> {
        let row = self.take(&format!("{tag} row")).await?;
        if row["active"] != true || number(&row["lastLevel"])? > head.level {
            return Err(TradingError::Changed);
        }
        let hash = text(&row["hash"])?;
        if hash != key_hash(key)? {
            return Err(TradingError::Changed);
        }
        if hash.len() != 54 || !hash.bytes().all(|b| b.is_ascii_alphanumeric()) {
            return Err(TradingError::Unavailable);
        }
        let raw = self.take(&format!("{tag} value")).await?;
        Ok((row, raw))
    }
    /// Aborts what remains, unused speculative reads included, and waits until every task
    /// has stopped and released its permit.
    pub async fn close(mut self) {
        let handles: Vec<_> = self.reads.drain().map(|(_, handle)| handle).collect();
        for handle in &handles {
            handle.abort();
        }
        for handle in handles {
            let _ = handle.await;
        }
    }
    /// Closes the round before returning `result`.
    async fn finish<T>(self, result: Result<T, TradingError>) -> Result<T, TradingError> {
        self.close().await;
        result
    }
}
/// Keeps only the fields validation reads from large responses.
fn reduce(tag: &str, value: Value) -> Value {
    if tag.ends_with(" row") {
        json!({
            "active": value["active"],
            "hash": value["hash"],
            "firstLevel": value["firstLevel"],
            "lastLevel": value["lastLevel"],
        })
    } else if tag.starts_with("lambda ") {
        json!({"digest": digest(&value)})
    } else if tag.starts_with("script ") {
        let ty = value["code"]
            .as_array()
            .and_then(|a| a.iter().find(|v| v["prim"] == "storage"))
            .and_then(|v| v["args"].get(0))
            .cloned()
            .unwrap_or(Value::Null);
        json!({"code_digest": digest(&value["code"]), "type": ty, "storage": value["storage"]})
    } else {
        value
    }
}
fn bigmap_reads(head: &Head, map: u64, key: &str, tag: &str) -> Result<[Read; 2], TradingError> {
    if key.len() > 80 || !key.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
        return Err(TradingError::Invalid);
    }
    let hash = key_hash(key)?;
    Ok([
        Read::indexer(
            format!("{tag} row"),
            format!("/bigmaps/{map}/keys/{key}?micheline=2"),
            SMALL,
        ),
        Read::rpc(
            format!("{tag} value"),
            format!(
                "/chains/main/blocks/{}/context/big_maps/{map}/{hash}",
                head.hash
            ),
            SMALL,
        ),
    ])
}
fn lambda_id(entrypoint: &str) -> Result<u64, TradingError> {
    match entrypoint {
        "collection_offer_accept" => Ok(5),
        "collection_offer_cancel" => Ok(6),
        "listing_cancel" => Ok(9),
        "offer_accept" => Ok(11),
        "offer_cancel" => Ok(12),
        _ => Err(TradingError::Invalid),
    }
}
fn record_map(action: &Action) -> u64 {
    match (action.market(), action.kind()) {
        (V1, _) => 22799,
        (_, Kind::Listing) => 149787,
        (_, Kind::Token) => 149789,
        (_, Kind::Collection) => 149783,
    }
}
fn protocols(value: &Value) -> Result<(String, String), TradingError> {
    let field = |name: &str| {
        value[name]
            .as_str()
            .filter(|p| p.len() == 51 && p.bytes().all(|b| b.is_ascii_alphanumeric()))
            .map(str::to_owned)
            .ok_or(TradingError::Unavailable)
    };
    Ok((field("protocol")?, field("next_protocol")?))
}
/// The minimal fee for one operation: the node's minimum, a 100 mutez margin, its gas
/// limit and a fixed upper bound on its encoding (addresses, maximally encoded counters,
/// fees and limits; these fixed ASCII parameters encode shorter than JSON).
fn operation_fee(filter: &Value, op: &Operation, index: usize) -> Result<u64, TradingError> {
    let minimum = number(&filter["minimal_fees"])?;
    let encoded = op.parameters.to_string().len() as u64 + 160 + if index == 0 { 96 } else { 0 };
    minimum
        .checked_add(100)
        .and_then(|n| {
            n.checked_add(
                rate(
                    &filter["minimal_nanotez_per_gas_unit"],
                    number(&json!(op.gas_limit)).ok()?,
                )
                .ok()?,
            )
        })
        .and_then(|n| n.checked_add(rate(&filter["minimal_nanotez_per_byte"], encoded).ok()?))
        .ok_or(TradingError::TooLarge)
}

pub struct Chain {
    pub transport: Arc<dyn Transport>,
    pub fee: PlatformFee,
    foreground: Arc<Semaphore>,
    background: Arc<Semaphore>,
    indexer: Arc<Semaphore>,
}
impl Chain {
    pub fn new(transport: Arc<dyn Transport>, fee: PlatformFee) -> Self {
        Self {
            transport,
            fee,
            foreground: Arc::new(Semaphore::new(FOREGROUND_PERMITS)),
            background: Arc::new(Semaphore::new(BACKGROUND_PERMITS)),
            indexer: Arc::new(Semaphore::new(INDEXER_PERMITS)),
        }
    }
    fn window(&self) -> Arc<Semaphore> {
        if BACKGROUND.try_with(|_| ()).is_ok() {
            self.background.clone()
        } else {
            self.foreground.clone()
        }
    }
    /// One request holding a window permit until the response is dropped.
    async fn call(
        &self,
        endpoint: Endpoint,
        path: String,
        body: Option<Value>,
        limit: usize,
    ) -> Result<Held, TradingError> {
        let permit = self
            .window()
            .acquire_owned()
            .await
            .map_err(|_| TradingError::Unavailable)?;
        let indexer = match endpoint {
            Endpoint::Indexer => Some(
                self.indexer
                    .clone()
                    .acquire_owned()
                    .await
                    .map_err(|_| TradingError::Unavailable)?,
            ),
            Endpoint::Rpc => None,
        };
        let value = self
            .transport
            .request_capped(endpoint, path, body, limit)
            .await?;
        drop(indexer);
        Ok(Held {
            value,
            _permit: permit,
        })
    }
    pub async fn rpc(&self, path: String, body: Option<Value>) -> Result<Value, TradingError> {
        Ok(self.call(Endpoint::Rpc, path, body, LARGE).await?.value)
    }
    pub async fn indexer(&self, path: String) -> Result<Value, TradingError> {
        Ok(self.call(Endpoint::Indexer, path, None, LARGE).await?.value)
    }
    fn round(&self, reads: Vec<Read>) -> Round {
        let window = self.window();
        let indexer = self.indexer.clone();
        let mut previous: Option<oneshot::Receiver<()>> = None;
        let mut tasks = HashMap::with_capacity(reads.len());
        for read in reads {
            let (acquired, next) = oneshot::channel();
            let earlier = previous.replace(next);
            let transport = self.transport.clone();
            let window = window.clone();
            let indexer = indexer.clone();
            let tag = read.tag.clone();
            let handle = tokio::spawn(async move {
                // An aborted or failed earlier read drops its sender, which also releases this one.
                if let Some(earlier) = earlier {
                    let _ = earlier.await;
                }
                let permit = window
                    .acquire_owned()
                    .await
                    .map_err(|_| TradingError::Unavailable)?;
                let _ = acquired.send(());
                let indexer = match read.endpoint {
                    Endpoint::Indexer => Some(
                        indexer
                            .acquire_owned()
                            .await
                            .map_err(|_| TradingError::Unavailable)?,
                    ),
                    Endpoint::Rpc => None,
                };
                let reduced = transport
                    .request_capped(read.endpoint, read.path, None, read.limit)
                    .await
                    .map(|value| reduce(&read.tag, value));
                drop(indexer);
                drop(permit);
                reduced
            });
            tasks.insert(tag, handle);
        }
        Round { reads: tasks }
    }
    pub async fn head(&self) -> Result<Head, TradingError> {
        let mut round = self.round(vec![
            Read::rpc("header", "/chains/main/blocks/head/header".into(), SMALL),
            Read::indexer("indexer head", "/head".into(), SMALL),
        ]);
        let result = Self::head_from(&mut round).await;
        round.finish(result).await
    }
    async fn head_from(round: &mut Round) -> Result<Head, TradingError> {
        let header = round.take("header").await?;
        match header["chain_id"].as_str() {
            Some(MAINNET) => {}
            Some(_) => return Err(TradingError::CodeChanged),
            None => return Err(TradingError::Unavailable),
        }
        let level = number(&header["level"])?;
        let timestamp = timestamp(text(&header["timestamp"])?)?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| TradingError::Unavailable)?
            .as_secs();
        if now.abs_diff(timestamp) > 90 {
            return Err(TradingError::Unavailable);
        }
        let hash = text(&header["hash"])?.to_owned();
        if hash.len() != 51 || !hash.bytes().all(|b| b.is_ascii_alphanumeric()) {
            return Err(TradingError::Unavailable);
        }
        let indexed = round.take("indexer head").await?;
        if indexed["synced"] != true || number(&indexed["level"])?.abs_diff(level) > 2 {
            return Err(TradingError::Unavailable);
        }
        Ok(Head { hash, level })
    }
    /// Validates the actions, then reads their records at the current head in parallel and
    /// returns the later reads for the caller to validate. A check passes the reviewed state, which
    /// fixes the protocols and adds the ancestor read.
    pub async fn inspect(
        &self,
        actor: &str,
        actions: &[Action],
        hint: Option<&Action>,
        reviewed: Option<&Checked>,
    ) -> Result<(Checked, Tail), TradingError> {
        if actions.is_empty() || actions.len() > MAX_ACTIONS {
            return Err(TradingError::Invalid);
        }
        let mut unique = HashSet::new();
        let accepts = actions
            .iter()
            .filter(|a| matches!(a, Action::Accept { .. }))
            .count();
        if accepts > 1 || (accepts == 1 && actions.len() > 2) {
            return Err(TradingError::Invalid);
        }
        if accepts > 0 {
            self.fee.recipient()?;
        }
        for action in actions {
            action.validate()?;
            if !unique.insert((action.market(), action.kind(), action.id())) {
                return Err(TradingError::Invalid);
            }
        }
        let reclaim = match hint {
            None => None,
            Some(hint) => {
                let [Action::Accept { token, .. }] = actions else {
                    return Err(TradingError::Invalid);
                };
                let Action::Cancel {
                    marketplace,
                    kind: Kind::Listing,
                    id,
                } = hint
                else {
                    return Err(TradingError::Invalid);
                };
                hint.validate()?;
                Some((hint, marketplace.as_str(), *id, token))
            }
        };
        let head = self.head().await?;
        let prefix = format!("/chains/main/blocks/{}", head.hash);
        let fee_contract = match accepts {
            0 => None,
            _ => Some(self.fee.recipient()?.to_owned()).filter(|a| a.starts_with("KT1")),
        };
        // Every read is scheduled in the order it is validated. The reclaimed listing's
        // reads are speculative: unused when the listing is no longer active.
        let mut reads = vec![Read::rpc("protocols", format!("{prefix}/protocols"), SMALL)];
        if let Some(address) = &fee_contract {
            reads.push(Read::rpc(
                "fee entrypoint",
                format!("{prefix}/context/contracts/{address}/entrypoints/default"),
                SMALL,
            ));
        }
        if let Some((_, marketplace, id, _)) = reclaim {
            let map = if marketplace == V1 { 22799 } else { 149787 };
            let hash = key_hash(&id.to_string())?;
            reads.push(Read::rpc(
                "hint",
                format!("{prefix}/context/big_maps/{map}/{hash}"),
                SMALL,
            ));
        }
        let candidates: Vec<&Action> = reclaim
            .iter()
            .map(|(hint, ..)| *hint)
            .chain(actions.iter())
            .collect();
        let mut scheduled = HashSet::new();
        for action in &candidates {
            let market = action.market();
            if scheduled.insert(format!("script {market}")) {
                reads.push(Read::rpc(
                    format!("script {market}"),
                    format!("{prefix}/context/contracts/{market}/script"),
                    MEDIUM,
                ));
            }
            if market == V2 && scheduled.insert(format!("entrypoint {}", action.entrypoint())) {
                let id = lambda_id(action.entrypoint())?;
                reads.extend(bigmap_reads(
                    &head,
                    149790,
                    &id.to_string(),
                    &format!("lambda {id}"),
                )?);
                reads.extend(bigmap_reads(
                    &head,
                    149784,
                    action.entrypoint(),
                    &format!("enabled {}", action.entrypoint()),
                )?);
            }
            let map = record_map(action);
            reads.extend(bigmap_reads(
                &head,
                map,
                &action.id().to_string(),
                &format!("record {map} {}", action.id()),
            )?);
            // Version mappings are chosen from the record, so all three are read once,
            // speculatively; only the chosen ones are validated.
            let mapped = matches!(action, Action::Accept { .. })
                || (market == V2 && matches!(action.kind(), Kind::Listing | Kind::Token));
            if mapped && scheduled.insert("mappings".into()) {
                for version in 0..3 {
                    reads.extend(bigmap_reads(
                        &head,
                        149786,
                        &version.to_string(),
                        &format!("mapping {version}"),
                    )?);
                }
            }
            if let Action::Accept { token, .. } = action {
                reads.push(Read::indexer(
                    format!("balance {}", token.token_id),
                    format!(
                        "/tokens/balances?account={actor}&token.contract={}&token.tokenId={}&balance.gt=0&limit=2",
                        token.contract, token.token_id
                    ),
                    SMALL,
                ));
            }
        }
        let ancestor_distance = reviewed.map(|reviewed| {
            head.level
                .checked_sub(reviewed.head.level)
                .filter(|n| *n <= 20)
        });
        if let Some(Some(distance)) = ancestor_distance {
            reads.push(Read::rpc(
                "ancestor",
                format!("/chains/main/blocks/{}~{distance}/hash", head.hash),
                SMALL,
            ));
        }
        reads.push(Read::rpc(
            "manager key",
            format!("{prefix}/context/contracts/{actor}/manager_key"),
            SMALL,
        ));
        reads.push(Read::rpc(
            "counter",
            format!("{prefix}/context/contracts/{actor}/counter"),
            SMALL,
        ));
        reads.push(Read::rpc(
            "filter",
            "/chains/main/mempool/filter".into(),
            SMALL,
        ));
        if reviewed.is_none() {
            reads.push(Read::rpc(
                "constants",
                format!("{prefix}/context/constants"),
                SMALL,
            ));
        }
        let mut round = self.round(reads);
        let validated = self
            .validate(
                &mut round,
                actor,
                actions,
                reclaim,
                fee_contract.is_some(),
                &head,
                reviewed,
            )
            .await;
        let (reviews, proof, operations, protocols) = match validated {
            Ok(validated) => validated,
            Err(error) => return round.finish(Err(error)).await,
        };
        let tail = Tail {
            round,
            ancestor: ancestor_distance.map(|distance| distance.is_some()),
            review: reviewed.is_none(),
        };
        Ok((
            Checked {
                head,
                reviews,
                proof,
                operations,
                protocols,
            },
            tail,
        ))
    }
    /// Validates the round's results in the order the reads were scheduled, awaiting each
    /// only when it is needed.
    #[allow(clippy::too_many_arguments, clippy::type_complexity)]
    async fn validate(
        &self,
        round: &mut Round,
        actor: &str,
        actions: &[Action],
        reclaim: Option<(&Action, &str, u64, &Token)>,
        fee_contract: bool,
        head: &Head,
        reviewed: Option<&Checked>,
    ) -> Result<(Vec<ReviewItem>, String, Vec<Operation>, (String, String)), TradingError> {
        let protocols = protocols(&round.take("protocols").await?)?;
        if reviewed.is_some_and(|reviewed| reviewed.protocols != protocols) {
            return Err(TradingError::Changed);
        }
        if fee_contract {
            let entrypoint = round.take("fee entrypoint").await.map_err(|error| {
                if matches!(error, TradingError::Changed) {
                    TradingError::FeeRecipient
                } else {
                    error
                }
            })?;
            if !unit_default_entrypoint(&entrypoint)? {
                return Err(TradingError::FeeRecipient);
            }
        }
        let mut effective = actions.to_vec();
        if let Some((hint, marketplace, _, token)) = reclaim {
            match round.take("hint").await {
                Err(TradingError::Changed) => {} // Confirmed absent at this fixed block; simulation still proves ownership.
                Err(e) => return Err(e),
                Ok(raw) => {
                    let v = leaves(&raw)?;
                    let active = if marketplace == V1 {
                        if v.len() != 6 {
                            return Err(TradingError::Changed);
                        }
                        number(&v[2])? > 0
                    } else {
                        if v.len() != 4 {
                            return Err(TradingError::Changed);
                        }
                        true
                    };
                    if active {
                        let (owner, listed) = if marketplace == V1 {
                            (
                                text(&v[1])?,
                                Token {
                                    contract: TOKENS[0].into(),
                                    token_id: number(&v[3])?,
                                },
                            )
                        } else {
                            (text(&v[3])?, token_ref(&v[0], &v[1])?)
                        };
                        if owner != actor || listed != *token {
                            return Err(TradingError::Changed);
                        }
                        effective.insert(0, hint.clone());
                    }
                }
            }
        }
        let actions = effective.as_slice();
        let mut proofs = Vec::new();
        let mut reviews = Vec::new();
        let mut operations = Vec::new();
        let mut platform_transfer = None;
        let pins: Value = serde_json::from_str(include_str!("pins.json")).expect("compiled pins");
        let mut checked_markets = HashSet::new();
        let mut checked_eps = HashSet::new();
        let mut checked_versions = HashSet::new();
        for action in actions {
            if checked_markets.insert(action.market().to_owned()) {
                let script = round.take(&format!("script {}", action.market())).await?;
                let expected = if action.market() == V2 {
                    "1dc67ad05ad65658df838f26716f3ae532d301f56096640495f749815133e2d1"
                } else {
                    "5db80ed5a9c23a8b13a240726c1a68a39dc7331b477781019965b3784c6e7153"
                };
                if script["code_digest"] != expected {
                    return Err(TradingError::CodeChanged);
                }
                let ty = &script["type"];
                if ty.is_null() {
                    return Err(TradingError::CodeChanged);
                }
                let fields = fields(ty, &script["storage"])?;
                if fields.get("fees").and_then(|v| v.as_str()) != Some("25")
                    || fields.get("treasury").and_then(|v| v.as_str())
                        != Some("KT1P2BXYb894MekrCcSrnidzQYPVqitLoVLc")
                {
                    return Err(TradingError::CodeChanged);
                }
                if action.market() == V2 {
                    for (name, n) in [
                        ("listings", 149787),
                        ("offers", 149789),
                        ("collection_offers", 149783),
                        ("entrypoints_enabled", 149784),
                        ("gentk_contracts", 149786),
                    ] {
                        if fields.get(name).and_then(|v| v.as_str()) != Some(n.to_string().as_str())
                        {
                            return Err(TradingError::CodeChanged);
                        }
                    }
                    if fields.get("paused") != Some(&json!(false)) {
                        return Err(TradingError::Changed);
                    }
                    // The unannotated final field is the mutable entrypoint big map.
                    if leaves(&script["storage"])?.last() != Some(&json!("149790")) {
                        return Err(TradingError::CodeChanged);
                    }
                } else if fields.get("enabled") != Some(&json!(true))
                    || fields.get("offers") != Some(&json!("22799"))
                    || fields.get("objkts") != Some(&json!(TOKENS[0]))
                {
                    return Err(TradingError::CodeChanged);
                }
            }
            if action.market() == V2 && checked_eps.insert(action.entrypoint()) {
                let id = lambda_id(action.entrypoint())?;
                let (row, lambda) = round
                    .pair(head, &id.to_string(), &format!("lambda {id}"))
                    .await?;
                let pin = &pins[id.to_string()];
                if row["lastLevel"] != pin["level"] || lambda["digest"] != pin["sha256"] {
                    return Err(TradingError::CodeChanged);
                }
                let (_, enabled) = round
                    .pair(
                        head,
                        action.entrypoint(),
                        &format!("enabled {}", action.entrypoint()),
                    )
                    .await?;
                if enabled["prim"] != "True" {
                    return Err(TradingError::Changed);
                }
                proofs.push(json!([id, lambda["digest"]]));
            }
            let map = record_map(action);
            let (row, raw) = round
                .pair(
                    head,
                    &action.id().to_string(),
                    &format!("record {map} {}", action.id()),
                )
                .await?;
            let values = leaves(&raw)?;
            let (counterparty, price, remaining, token, project) = if action.market() == V1 {
                if values.len() != 6 {
                    return Err(TradingError::Changed);
                }
                (
                    text(&values[1])?.to_owned(),
                    number(&values[4])?,
                    number(&values[2])?,
                    Some(Token {
                        contract: TOKENS[0].into(),
                        token_id: number(&values[3])?,
                    }),
                    None,
                )
            } else {
                match action.kind() {
                    Kind::Listing => {
                        if values.len() != 4 {
                            return Err(TradingError::Changed);
                        }
                        (
                            text(&values[3])?.to_owned(),
                            number(&values[2])?,
                            1,
                            Some(token_ref(&values[0], &values[1])?),
                            None,
                        )
                    }
                    Kind::Token => {
                        if values.len() != 4 {
                            return Err(TradingError::Changed);
                        }
                        (
                            text(&values[0])?.to_owned(),
                            number(&values[3])?,
                            1,
                            Some(token_ref(&values[1], &values[2])?),
                            None,
                        )
                    }
                    Kind::Collection => {
                        if values.len() != 4 {
                            return Err(TradingError::Changed);
                        }
                        (
                            text(&values[1])?.to_owned(),
                            number(&values[3])?,
                            number(&values[0])?,
                            None,
                            Some(number(&values[2])?),
                        )
                    }
                }
            };
            if remaining == 0 || price.checked_mul(remaining).is_none() {
                return Err(TradingError::Changed);
            }
            let mapped_token = match action {
                Action::Accept { token, .. } => Some(token),
                _ if action.market() == V2 => token.as_ref(),
                _ => None,
            };
            if let Some(mapped) = mapped_token {
                let selected = TOKENS
                    .iter()
                    .position(|c| *c == mapped.contract)
                    .ok_or(TradingError::Invalid)?;
                let versions: Vec<usize> = if matches!(action, Action::Accept { .. }) {
                    vec![0, 1, 2]
                } else {
                    vec![selected]
                };
                for version in versions {
                    if checked_versions.insert(version) {
                        let tag = format!("mapping {version}");
                        if !round.has(&format!("{tag} row")) {
                            return Err(TradingError::Unavailable);
                        }
                        let (_, mapping) = round.pair(head, &version.to_string(), &tag).await?;
                        if leaves(&mapping)? != vec![json!(TOKENS[version]), json!(VIEWS[version])]
                        {
                            return Err(TradingError::CodeChanged);
                        }
                        proofs.push(mapping);
                    }
                }
            }
            match action {
                Action::Cancel { .. } if counterparty != actor => {
                    return Err(TradingError::Unauthorized);
                }
                Action::Accept { token: chosen, .. } => {
                    if counterparty == actor || token.as_ref().is_some_and(|t| t != chosen) {
                        return Err(TradingError::Unauthorized);
                    }
                    // FA2 transfer authorization and collection membership are checked by the
                    // pinned accept lambda and token contract during the complete atomic simulation.
                    let balances = round.take(&format!("balance {}", chosen.token_id)).await?;
                    let owned = balances
                        .as_array()
                        .is_some_and(|rows| rows.len() == 1 && rows[0]["balance"] == "1");
                    let reclaimed = actions.first().is_some_and(|a| {
                        matches!(
                            a,
                            Action::Cancel {
                                kind: Kind::Listing,
                                ..
                            }
                        )
                    }) && reviews
                        .first()
                        .is_some_and(|r: &ReviewItem| r.token.as_ref() == Some(chosen));
                    if !owned && !reclaimed {
                        return Err(TradingError::NotHeld {
                            contract: chosen.contract.clone(),
                            token_id: chosen.token_id,
                        });
                    }
                }
                _ => {}
            }
            let created_level = number(&row["firstLevel"])?;
            proofs.push(json!([action, &raw, row["firstLevel"], row["lastLevel"]]));
            reviews.push(ReviewItem {
                action: action.clone(),
                counterparty,
                price_mutez: price,
                remaining,
                // Filled by `created_dates`; a check doesn't need it.
                created_at: String::new(),
                created_level,
                token,
                project_id: project,
            });
            operations.extend(operation_group(actor, action)?);
            if matches!(action, Action::Accept { .. }) {
                let address = self.fee.recipient()?;
                let amount = self.fee.amount(price)?;
                proofs.push(json!(["platform_fee", self.fee.bps, address, amount]));
                // A rounded zero has no minimum and needs no transfer to an implicit account.
                if amount > 0 {
                    platform_transfer = Some(Operation::transfer(actor, address, amount));
                }
            }
        }
        operations.extend(platform_transfer);
        Ok((reviews, digest(&json!(proofs)), operations, protocols))
    }

    /// The block timestamp each reviewed record was created at, in parallel.
    pub async fn created_dates(&self, reviews: &[ReviewItem]) -> Result<Vec<String>, TradingError> {
        let reads = reviews
            .iter()
            .enumerate()
            .map(|(i, review)| {
                Read::indexer(
                    format!("created {i}"),
                    format!("/blocks/{}?select=timestamp", review.created_level),
                    SMALL,
                )
            })
            .collect();
        let mut round = self.round(reads);
        let mut dates = Vec::with_capacity(reviews.len());
        for i in 0..reviews.len() {
            let block = match round.take(&format!("created {i}")).await {
                Ok(block) => block,
                Err(error) => return round.finish(Err(error)).await,
            };
            match text(block.get("timestamp").unwrap_or(&block)) {
                Ok(date) => dates.push(date.to_owned()),
                Err(error) => return round.finish(Err(error)).await,
            }
        }
        round.finish(Ok(dates)).await
    }
    /// Gas, storage and fee limits for a reviewed group, from a first simulation, then the
    /// final simulation with those limits in parallel with forging for the size bound.
    pub async fn simulate(
        &self,
        actor: &str,
        head: &Head,
        mut operations: Vec<Operation>,
        inputs: &Inputs,
    ) -> Result<Simulation, TradingError> {
        let prefix = format!("/chains/main/blocks/{}", head.hash);
        let counter = inputs.counter;
        let cost = inputs.cost_per_byte.ok_or(TradingError::Unavailable)?;
        if operations.is_empty() || operations.len() > MAX_ACTIONS {
            return Err(TradingError::TooLarge);
        }
        let per_gas = MAX_GAS / operations.len() as u64;
        for (i, op) in operations.iter_mut().enumerate() {
            op.counter = counter
                .checked_add(i as u64 + 1)
                .ok_or(TradingError::TooLarge)?
                .to_string();
            op.gas_limit = per_gas.to_string();
            op.storage_limit = "1000".into();
        }
        let contents = rpc_contents(&operations, actor, counter)?;
        let signature = bs58::encode([vec![4, 130, 43], vec![0; 64]].concat())
            .with_check()
            .into_string();
        let simulated = self
            .call(
                Endpoint::Rpc,
                format!("{prefix}/helpers/scripts/simulate_operation"),
                Some(json!({"chain_id":MAINNET,"latency":0,"operation":{"branch":head.hash,"contents":contents,"signature":signature}})),
                LARGE,
            )
            .await?
            .value;
        let results = simulated["contents"]
            .as_array()
            .filter(|r| r.len() == operations.len())
            .ok_or(TradingError::Simulation)?;
        let accept_fees = self.accept_fees(actor, &operations, results)?;
        let mut gas = 0u64;
        let mut storage = 0u64;
        let mut proceeds = 0u64;
        for (op, result) in operations.iter_mut().zip(results) {
            let mut used = 0u64;
            let mut paid = 0u64;
            collect_result(
                &result["metadata"]["operation_result"],
                &mut used,
                &mut paid,
            )?;
            if let Some(internals) = result["metadata"]["internal_operation_results"].as_array() {
                if internals.len() > 128 {
                    return Err(TradingError::TooLarge);
                }
                for internal in internals {
                    collect_result(&internal["result"], &mut used, &mut paid)?;
                    if internal["kind"] == "transaction" && internal["destination"] == actor {
                        proceeds = proceeds
                            .checked_add(number(&internal["amount"])?)
                            .ok_or(TradingError::TooLarge)?;
                    }
                }
            }
            let limit = used.checked_add(500).ok_or(TradingError::TooLarge)?;
            let storage_limit = paid.checked_add(64).ok_or(TradingError::TooLarge)?;
            if limit > per_gas || storage_limit > 1000 {
                return Err(TradingError::TooLarge);
            }
            op.gas_limit = limit.to_string();
            op.storage_limit = storage_limit.to_string();
            gas = gas.checked_add(limit).ok_or(TradingError::TooLarge)?;
            storage = storage
                .checked_add(storage_limit)
                .ok_or(TradingError::TooLarge)?;
        }
        // The first simulation's response is not needed past this point.
        drop(simulated);
        // Forge the group once; bound each transaction encoding independently so
        // batching does not multiply the entire group size into every fee.
        let forge = json!({"branch":head.hash,"contents":rpc_contents(&operations,actor,counter)?});
        let mut fee = 0u64;
        for (index, op) in operations.iter_mut().enumerate() {
            let n = operation_fee(&inputs.filter, op, index)?;
            op.fee = n.to_string();
            fee = fee.checked_add(n).ok_or(TradingError::TooLarge)?;
        }
        if fee > 2_000_000 || gas > MAX_GAS {
            return Err(TradingError::TooLarge);
        }
        let burn = storage.checked_mul(cost).ok_or(TradingError::TooLarge)?;
        let (forged, final_run) = tokio::join!(
            self.call(
                Endpoint::Rpc,
                format!("{prefix}/helpers/forge/operations"),
                Some(forge),
                MEDIUM,
            ),
            self.call(
                Endpoint::Rpc,
                format!("{prefix}/helpers/scripts/simulate_operation"),
                Some(json!({"chain_id":MAINNET,"latency":0,"operation":{"branch":head.hash,"contents":rpc_contents(&operations,actor,counter)?,"signature":signature}})),
                LARGE,
            ),
        );
        let bytes = text(&forged?.value)?.len() / 2 + 64 + operations.len() * 40;
        if bytes > MAX_BYTES {
            return Err(TradingError::TooLarge);
        }
        let final_run = final_run?.value;
        let final_results = final_run["contents"]
            .as_array()
            .filter(|r| r.len() == operations.len())
            .ok_or(TradingError::Simulation)?;
        if self.accept_fees(actor, &operations, final_results)? != accept_fees {
            return Err(TradingError::Simulation);
        }
        for (result, op) in final_results.iter().zip(&operations) {
            let mut used = 0;
            let mut paid = 0;
            collect_result(
                &result["metadata"]["operation_result"],
                &mut used,
                &mut paid,
            )?;
            if let Some(internals) = result["metadata"]["internal_operation_results"].as_array() {
                if internals.len() > 128 {
                    return Err(TradingError::TooLarge);
                }
                for i in internals {
                    collect_result(&i["result"], &mut used, &mut paid)?;
                }
            }
            if used > number(&json!(op.gas_limit))? || paid > number(&json!(op.storage_limit))? {
                return Err(TradingError::Simulation);
            }
        }
        Ok(Simulation {
            operations,
            fee_mutez: fee,
            storage_burn_cap_mutez: burn,
            gas,
            bytes,
            counter,
            proceeds_mutez: proceeds.to_string(),
            accept_fees,
            cost_per_byte: cost,
        })
    }
    /// One simulation of exactly the reviewed operations at the current head, with every
    /// review comparison: same counter, gas and storage within the reviewed limits after
    /// the same reserves, fee and burn no higher, identical accept split and proceeds.
    pub async fn recheck(
        &self,
        actor: &str,
        current: &Checked,
        reviewed: &Simulation,
        inputs: &Inputs,
    ) -> Result<AcceptCheck, TradingError> {
        // Another operation from the address used the reviewed counters; the node would
        // refuse the simulation, which must not hide that the state changed.
        if inputs.counter != reviewed.counter {
            return Err(TradingError::Changed);
        }
        let operations = &reviewed.operations;
        if operations.is_empty() || operations.len() > MAX_ACTIONS {
            return Err(TradingError::TooLarge);
        }
        let contents = operations
            .iter()
            .map(|op| {
                op.counter
                    .parse::<u64>()
                    .map(|counter| op.rpc(actor, counter))
                    .map_err(|_| TradingError::Unavailable)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let signature = bs58::encode([vec![4, 130, 43], vec![0; 64]].concat())
            .with_check()
            .into_string();
        let run = self
            .call(
                Endpoint::Rpc,
                format!(
                    "/chains/main/blocks/{}/helpers/scripts/simulate_operation",
                    current.head.hash
                ),
                Some(json!({"chain_id":MAINNET,"latency":0,"operation":{"branch":current.head.hash,"contents":contents,"signature":signature}})),
                LARGE,
            )
            .await?
            .value;
        let results = run["contents"]
            .as_array()
            .filter(|r| r.len() == operations.len())
            .ok_or(TradingError::Simulation)?;
        let accept_fees = self.accept_fees(actor, operations, results)?;
        let per_gas = MAX_GAS / operations.len() as u64;
        let mut storage = 0u64;
        let mut proceeds = 0u64;
        let mut fee = 0u64;
        let mut within = true;
        for (index, (op, result)) in operations.iter().zip(results).enumerate() {
            let mut used = 0u64;
            let mut paid = 0u64;
            collect_result(
                &result["metadata"]["operation_result"],
                &mut used,
                &mut paid,
            )?;
            if let Some(internals) = result["metadata"]["internal_operation_results"].as_array() {
                if internals.len() > 128 {
                    return Err(TradingError::TooLarge);
                }
                for internal in internals {
                    collect_result(&internal["result"], &mut used, &mut paid)?;
                    if internal["kind"] == "transaction" && internal["destination"] == actor {
                        proceeds = proceeds
                            .checked_add(number(&internal["amount"])?)
                            .ok_or(TradingError::TooLarge)?;
                    }
                }
            }
            let limit = used.checked_add(500).ok_or(TradingError::TooLarge)?;
            let storage_limit = paid.checked_add(64).ok_or(TradingError::TooLarge)?;
            if limit > per_gas || storage_limit > 1000 {
                return Err(TradingError::TooLarge);
            }
            within &= limit <= number(&json!(op.gas_limit)).unwrap_or(0)
                && storage_limit <= number(&json!(op.storage_limit)).unwrap_or(0);
            storage = storage
                .checked_add(storage_limit)
                .ok_or(TradingError::TooLarge)?;
            let mut estimate = op.clone();
            estimate.gas_limit = limit.to_string();
            fee = fee
                .checked_add(operation_fee(&inputs.filter, &estimate, index)?)
                .ok_or(TradingError::TooLarge)?;
        }
        let burn = storage
            .checked_mul(reviewed.cost_per_byte)
            .ok_or(TradingError::TooLarge)?;
        Ok(AcceptCheck {
            unchanged: within
                && accept_fees == reviewed.accept_fees
                && fee <= reviewed.fee_mutez
                && burn <= reviewed.storage_burn_cap_mutez
                && proceeds.to_string() == reviewed.proceeds_mutez,
            accept_fees,
        })
    }

    fn accept_fees(
        &self,
        actor: &str,
        operations: &[Operation],
        results: &[Value],
    ) -> Result<Option<AcceptFees>, TradingError> {
        let mut accepted = None;
        for (operation, result) in operations.iter().zip(results) {
            if operation.destination != V2
                || !matches!(
                    operation.parameters["entrypoint"].as_str(),
                    Some("offer_accept" | "collection_offer_accept")
                )
            {
                continue;
            }
            if accepted.is_some() {
                return Err(TradingError::Invalid);
            }
            let address = self.fee.recipient()?;
            let internals = result["metadata"]["internal_operation_results"]
                .as_array()
                .filter(|rows| rows.len() <= 128)
                .ok_or(TradingError::Simulation)?;
            let (mut price, mut seller, mut treasury) = (0u64, 0u64, 0u64);
            for row in internals {
                if row["kind"] != "transaction" || row["source"] != V2 {
                    continue;
                }
                let amount = number(&row["amount"])?;
                price = price.checked_add(amount).ok_or(TradingError::TooLarge)?;
                if row["destination"] == actor {
                    seller = seller.checked_add(amount).ok_or(TradingError::TooLarge)?;
                }
                if row["destination"] == FXHASH_TREASURY {
                    treasury = treasury.checked_add(amount).ok_or(TradingError::TooLarge)?;
                }
            }
            let fxhash_fee = (u128::from(price) * 25 / 1000) as u64;
            if treasury < fxhash_fee {
                return Err(TradingError::Simulation);
            }
            let royalties = price
                .checked_sub(fxhash_fee)
                .and_then(|n| n.checked_sub(seller))
                .ok_or(TradingError::Simulation)?;
            let platform_fee = self.fee.amount(price)?;
            let mut returned = if address == actor { platform_fee } else { 0 };
            let transfers: Vec<_> = operations
                .iter()
                .filter(|op| op.parameters.is_null())
                .collect();
            if platform_fee == 0 {
                if !transfers.is_empty() {
                    return Err(TradingError::Simulation);
                }
            } else if transfers.len() != 1
                || operations.last().is_none_or(|op| {
                    op.source != actor
                        || op.destination != address
                        || op.amount != platform_fee.to_string()
                        || !op.parameters.is_null()
                })
            {
                return Err(TradingError::Simulation);
            }
            if platform_fee > 0 {
                if let Some(rows) = results
                    .last()
                    .and_then(|r| r["metadata"]["internal_operation_results"].as_array())
                {
                    if rows.len() > 128 {
                        return Err(TradingError::TooLarge);
                    }
                    for row in rows {
                        if row["kind"] == "transaction" && row["destination"] == actor {
                            returned = returned
                                .checked_add(number(&row["amount"])?)
                                .ok_or(TradingError::TooLarge)?;
                        }
                    }
                }
            }
            let seller_net = seller
                .checked_add(returned)
                .and_then(|n| n.checked_sub(platform_fee))
                .ok_or(TradingError::Simulation)?;
            accepted = Some(AcceptFees {
                price_mutez: price,
                royalties_mutez: royalties,
                fxhash_fee_mutez: fxhash_fee,
                platform_fee_mutez: platform_fee,
                platform_fee_returned_mutez: returned,
                platform_fee_bps: self.fee.bps,
                platform_fee_address: address.to_owned(),
                seller_net_mutez: seller_net,
            });
        }
        if accepted.is_none() && operations.iter().any(|op| op.parameters.is_null()) {
            return Err(TradingError::Invalid);
        }
        Ok(accepted)
    }
}
fn rpc_contents(ops: &[Operation], actor: &str, counter: u64) -> Result<Vec<Value>, TradingError> {
    ops.iter()
        .enumerate()
        .map(|(i, op)| {
            counter
                .checked_add(i as u64 + 1)
                .map(|n| op.rpc(actor, n))
                .ok_or(TradingError::TooLarge)
        })
        .collect()
}
fn collect_result(value: &Value, gas: &mut u64, storage: &mut u64) -> Result<(), TradingError> {
    if value["status"] != "applied" {
        return Err(TradingError::Simulation);
    }
    *gas = gas
        .checked_add(number(&value["consumed_milligas"])?.div_ceil(1000))
        .ok_or(TradingError::TooLarge)?;
    let paid = value
        .get("paid_storage_size_diff")
        .map(number)
        .transpose()?
        .unwrap_or(0);
    *storage = storage
        .checked_add(
            paid.checked_add(if value["allocated_destination_contract"] == true {
                257
            } else {
                0
            })
            .ok_or(TradingError::TooLarge)?,
        )
        .ok_or(TradingError::TooLarge)?;
    Ok(())
}
fn token_ref(id: &Value, version: &Value) -> Result<Token, TradingError> {
    Ok(Token {
        contract: TOKENS
            .get(number(version)? as usize)
            .ok_or(TradingError::CodeChanged)?
            .to_string(),
        token_id: number(id)?,
    })
}
pub fn number(value: &Value) -> Result<u64, TradingError> {
    value
        .as_u64()
        .or_else(|| value.as_str().and_then(|s| s.parse().ok()))
        .ok_or(TradingError::Unavailable)
}
pub fn text(value: &Value) -> Result<&str, TradingError> {
    value.as_str().ok_or(TradingError::Unavailable)
}
pub fn digest(value: &Value) -> String {
    hex::encode(Sha256::digest(value.to_string().as_bytes()))
}
fn leaves(value: &Value) -> Result<Vec<Value>, TradingError> {
    fn visit(v: &Value, out: &mut Vec<Value>) -> Result<(), TradingError> {
        if out.len() > 128 {
            return Err(TradingError::TooLarge);
        }
        if let Some(a) = v.as_array().or_else(|| {
            (v["prim"] == "Pair")
                .then(|| v["args"].as_array())
                .flatten()
        }) {
            for v in a {
                visit(v, out)?;
            }
            return Ok(());
        }
        if let Some(s) = v.get("int").or_else(|| v.get("string")) {
            out.push(s.clone());
            return Ok(());
        }
        if let Some(s) = v["bytes"].as_str() {
            out.push(json!(decode_address(s)?));
            return Ok(());
        }
        if v["prim"] == "True" || v["prim"] == "False" {
            out.push(json!(v["prim"] == "True"));
            return Ok(());
        }
        Err(TradingError::Unavailable)
    }
    let mut out = Vec::new();
    visit(value, &mut out)?;
    Ok(out)
}
fn decode_address(s: &str) -> Result<String, TradingError> {
    let bytes = hex::decode(s).map_err(|_| TradingError::Unavailable)?;
    if bytes.len() != 22 {
        return Err(TradingError::Unavailable);
    }
    let (prefix, payload) = match (bytes[0], bytes[1]) {
        (0, 0) => (vec![6, 161, 159], &bytes[2..]),
        (0, 1) => (vec![6, 161, 161], &bytes[2..]),
        (0, 2) => (vec![6, 161, 164], &bytes[2..]),
        (1, _) if bytes[21] == 0 => (vec![2, 90, 121], &bytes[1..21]),
        _ => return Err(TradingError::Unavailable),
    };
    Ok(bs58::encode([prefix, payload.to_vec()].concat())
        .with_check()
        .into_string())
}
fn fields(ty: &Value, value: &Value) -> Result<BTreeMap<String, Value>, TradingError> {
    fn names(ty: &Value, out: &mut Vec<String>) {
        if ty["prim"] == "pair" {
            if let Some(a) = ty["args"].as_array() {
                for t in a {
                    names(t, out);
                }
            }
        } else {
            out.push(
                ty["annots"]
                    .as_array()
                    .and_then(|a| {
                        a.iter()
                            .filter_map(Value::as_str)
                            .find_map(|s| s.strip_prefix('%'))
                    })
                    .unwrap_or("")
                    .into(),
            );
        }
    }
    let mut keys = Vec::new();
    names(ty, &mut keys);
    let values = leaves(value)?;
    if keys.len() != values.len() {
        return Err(TradingError::CodeChanged);
    }
    Ok(keys.into_iter().zip(values).collect())
}
fn timestamp(raw: &str) -> Result<u64, TradingError> {
    let b = raw.as_bytes();
    if b.len() < 20
        || b[4] != b'-'
        || b[7] != b'-'
        || b[10] != b'T'
        || b[13] != b':'
        || b[16] != b':'
        || !raw.ends_with('Z')
    {
        return Err(TradingError::Unavailable);
    }
    let n = |start, end| {
        raw.get(start..end)
            .and_then(|s| s.parse::<i64>().ok())
            .ok_or(TradingError::Unavailable)
    };
    let y = n(0, 4)?;
    let m = n(5, 7)?;
    let d = n(8, 10)?;
    let h = n(11, 13)?;
    let min = n(14, 16)?;
    let sec = n(17, 19)?;
    if !(1970..=2100).contains(&y)
        || !(1..=12).contains(&m)
        || !(1..=31).contains(&d)
        || h > 23
        || min > 59
        || sec > 60
    {
        return Err(TradingError::Unavailable);
    }
    let y = y - i64::from(m <= 2);
    let era = y / 400;
    let yoe = y - era * 400;
    let mp = m + if m > 2 { -3 } else { 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let days = era * 146097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719468;
    u64::try_from(days * 86400 + h * 3600 + min * 60 + sec).map_err(|_| TradingError::Unavailable)
}

/// Normalize RPC comb pairs and optimized addresses without losing left-pair structure.
pub fn normalized(value: &Value) -> Result<Value, TradingError> {
    if let Some(a) = value.as_array() {
        return a
            .iter()
            .map(normalized)
            .collect::<Result<Vec<_>, _>>()
            .map(Value::Array);
    }
    if let Some(bytes) = value["bytes"].as_str() {
        if bytes.len() == 44 {
            if let Ok(address) = decode_address(bytes) {
                return Ok(json!({"string":address}));
            }
        }
    }
    let mut v = value.clone();
    if let Some(args) = value["args"].as_array() {
        let mut args = args.iter().map(normalized).collect::<Result<Vec<_>, _>>()?;
        if value["prim"] == "Pair" && args.len() > 2 {
            let first = args.remove(0);
            v["args"] = json!([first, normalized(&json!({"prim":"Pair","args":args}))?]);
        } else {
            v["args"] = json!(args);
        }
    }
    Ok(v)
}

pub fn unit_default_entrypoint(value: &Value) -> Result<bool, TradingError> {
    fn strip_annots(value: &mut Value) {
        match value {
            Value::Array(values) => values.iter_mut().for_each(strip_annots),
            Value::Object(fields) => {
                fields.remove("annots");
                fields.values_mut().for_each(strip_annots);
            }
            _ => {}
        }
    }
    let mut value = normalized(value)?;
    strip_annots(&mut value);
    Ok(value == json!({"prim":"unit"}))
}

fn key_hash(key: &str) -> Result<String, TradingError> {
    let mut packed = vec![5];
    if let Ok(mut n) = key.parse::<u64>() {
        packed.push(0);
        let first = (n & 63) as u8;
        n >>= 6;
        packed.push(first | if n > 0 { 128 } else { 0 });
        while n > 0 {
            let next = (n & 127) as u8;
            n >>= 7;
            packed.push(next | if n > 0 { 128 } else { 0 });
        }
    } else {
        packed.push(1);
        let n = u32::try_from(key.len()).map_err(|_| TradingError::Invalid)?;
        packed.extend(n.to_be_bytes());
        packed.extend(key.as_bytes());
    }
    let hash = <blake2::Blake2b<blake2::digest::consts::U32> as blake2::Digest>::digest(packed);
    Ok(bs58::encode([vec![13, 44, 64, 27], hash.to_vec()].concat())
        .with_check()
        .into_string())
}

fn rate(ratio: &Value, units: u64) -> Result<u64, TradingError> {
    let numerator = number(&ratio[0])? as u128;
    let denominator = number(&ratio[1])? as u128;
    if denominator == 0 {
        return Err(TradingError::Unavailable);
    }
    let value = (u128::from(units) * numerator).div_ceil(denominator * 1000);
    u64::try_from(value).map_err(|_| TradingError::TooLarge)
}

#[cfg(test)]
mod window_tests {
    use super::*;
    use crate::trading::http::Reply;
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

    /// Counts started requests and request futures still alive, including aborted ones
    /// until they are dropped.
    #[derive(Default)]
    struct Counting {
        started: AtomicUsize,
        alive: AtomicUsize,
        peak: AtomicUsize,
    }
    struct Alive(Arc<Counting>);
    impl Drop for Alive {
        fn drop(&mut self) {
            self.0.alive.fetch_sub(1, Ordering::SeqCst);
        }
    }
    /// Answers `"ok"`; paths starting with `slow` wait first.
    struct Transport(Arc<Counting>, Duration);
    impl super::Transport for Transport {
        fn request(&self, _: Endpoint, path: String, _: Option<Value>) -> Reply {
            let counting = self.0.clone();
            let delay = self.1;
            Box::pin(async move {
                counting.started.fetch_add(1, Ordering::SeqCst);
                let alive = counting.alive.fetch_add(1, Ordering::SeqCst) + 1;
                counting.peak.fetch_max(alive, Ordering::SeqCst);
                let _alive = Alive(counting);
                if path.starts_with("slow") {
                    tokio::time::sleep(delay).await;
                }
                Ok(json!("ok"))
            })
        }
    }
    fn chain(delay: Duration) -> (Arc<Chain>, Arc<Counting>) {
        let counting = Arc::new(Counting::default());
        let chain = Chain::new(
            Arc::new(Transport(counting.clone(), delay)),
            PlatformFee {
                bps: 0,
                address: None,
            },
        );
        (Arc::new(chain), counting)
    }
    fn reads(n: usize, slow: impl Fn(usize) -> bool) -> Vec<Read> {
        (0..n)
            .map(|i| {
                let path = if slow(i) {
                    format!("slow{i}")
                } else {
                    format!("r{i}")
                };
                Read::rpc(i.to_string(), path, SMALL)
            })
            .collect()
    }
    async fn take_all(round: &mut Round, n: usize) -> usize {
        for i in 0..n {
            round.take(&i.to_string()).await.unwrap();
        }
        n
    }

    #[tokio::test]
    async fn requests_in_flight_never_exceed_the_window() {
        let (chain, counting) = chain(Duration::from_millis(50));
        let mut round = chain.round(reads(50, |_| true));
        assert_eq!(take_all(&mut round, 50).await, 50);
        assert_eq!(counting.peak.load(Ordering::SeqCst), FOREGROUND_PERMITS);
        assert_eq!(counting.started.load(Ordering::SeqCst), 50);
        assert_eq!(chain.foreground.available_permits(), FOREGROUND_PERMITS);
    }

    #[tokio::test]
    async fn a_slow_read_does_not_hold_back_answered_ones() {
        let (chain, counting) = chain(Duration::from_millis(300));
        let mut round = chain.round(reads(50, |i| i == 0));
        // Answered reads release their permits; validation can take any of them.
        round.take("49").await.unwrap();
        assert_eq!(counting.started.load(Ordering::SeqCst), 50);
        round.close().await;
        assert_eq!(counting.alive.load(Ordering::SeqCst), 0);
        assert_eq!(chain.foreground.available_permits(), FOREGROUND_PERMITS);
    }

    #[tokio::test]
    async fn concurrent_rounds_with_slow_reads_all_finish_within_both_windows() {
        let (chain, counting) = chain(Duration::from_millis(20));
        let mut tasks = Vec::new();
        for background_work in [false, false, false, false, true] {
            let chain = chain.clone();
            tasks.push(tokio::spawn(async move {
                let work = async {
                    let mut round = chain.round(reads(50, |i| i % 5 == 0));
                    take_all(&mut round, 50).await
                };
                if background_work {
                    background(work).await
                } else {
                    work.await
                }
            }));
        }
        let all = tokio::time::timeout(Duration::from_secs(10), async {
            let mut total = 0;
            for task in tasks {
                total += task.await.unwrap();
            }
            total
        })
        .await
        .expect("no round starves");
        assert_eq!(all, 250);
        assert!(counting.peak.load(Ordering::SeqCst) <= FOREGROUND_PERMITS + BACKGROUND_PERMITS);
        assert_eq!(chain.foreground.available_permits(), FOREGROUND_PERMITS);
        assert_eq!(chain.background.available_permits(), BACKGROUND_PERMITS);
    }

    #[tokio::test]
    async fn at_most_six_indexer_requests_are_in_flight() {
        let (chain, counting) = chain(Duration::from_millis(50));
        let mut round = chain.round(
            (0..30)
                .map(|i| Read::indexer(i.to_string(), format!("slow{i}"), SMALL))
                .collect(),
        );
        assert_eq!(take_all(&mut round, 30).await, 30);
        assert_eq!(counting.peak.load(Ordering::SeqCst), INDEXER_PERMITS);
        assert_eq!(chain.indexer.available_permits(), INDEXER_PERMITS);
    }

    #[tokio::test]
    async fn background_work_uses_its_own_two_permit_window() {
        let (chain, counting) = chain(Duration::from_millis(300));
        let task = {
            let chain = chain.clone();
            tokio::spawn(background(async move {
                let mut round = chain.round(reads(10, |_| true));
                take_all(&mut round, 10).await
            }))
        };
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(counting.started.load(Ordering::SeqCst), BACKGROUND_PERMITS);
        assert_eq!(chain.foreground.available_permits(), FOREGROUND_PERMITS);
        task.await.unwrap();
    }

    #[tokio::test]
    async fn cancelling_a_take_aborts_the_awaited_and_queued_reads() {
        let (chain, counting) = chain(Duration::from_secs(30));
        let task = {
            let chain = chain.clone();
            tokio::spawn(async move {
                let mut round = chain.round(reads(50, |_| true));
                round.take("0").await
            })
        };
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(counting.alive.load(Ordering::SeqCst), FOREGROUND_PERMITS);
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(counting.alive.load(Ordering::SeqCst), 0);
        assert_eq!(counting.started.load(Ordering::SeqCst), FOREGROUND_PERMITS);
        assert_eq!(chain.foreground.available_permits(), FOREGROUND_PERMITS);
    }

    #[tokio::test]
    async fn closing_a_round_waits_until_every_read_has_stopped() {
        let (chain, counting) = chain(Duration::from_secs(30));
        let round = chain.round(reads(50, |_| true));
        tokio::time::sleep(Duration::from_millis(100)).await;
        round.close().await;
        assert_eq!(counting.alive.load(Ordering::SeqCst), 0);
        assert_eq!(chain.foreground.available_permits(), FOREGROUND_PERMITS);
    }
}
