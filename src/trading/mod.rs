//! Read, review and simulate fxhash operations. Signing stays in the connected signing app.
mod admission;
mod audit;
pub mod chain;
mod confirmation;
pub mod http;
mod submitted;
pub use submitted::{Overlay, RecordKey};
pub mod types;

use chain::{Chain, Checked, Head, Simulation};
use http::{Http, Transport};
use serde::Serialize;
use serde_json::Value;
use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
const REVIEW_TTL: Duration = Duration::from_secs(120);
const SIGNING_LOCK: Duration = Duration::from_secs(120);
/// Re-reads before reporting a changed market: nodes behind the indexer answer 404 or an
/// older head for a few seconds, while a real change persists.
const LAG_RETRIES: u32 = 2;
const LAG_PAUSE: Duration = Duration::from_secs(1);
/// Upstream time for one review or check, re-reads included (the page waits 60 s).
const REVIEW_BUDGET: Duration = Duration::from_secs(50);
/// About an hour of blocks: only a recent receipt-less confirmation starts the index sync.
const RECENT_BLOCKS: u64 = 450;
/// Receipt-less confirmations read the chain, so they are budgeted per address and overall.
const UNBOUND_WINDOW: Duration = Duration::from_secs(600);
const UNBOUND_PER_ADDRESS: usize = 10;
const UNBOUND_TOTAL: usize = 120;
/// One address's receipt-less checks: window start and, per hash, when it was last read.
type UnboundWindow = (Instant, HashMap<String, Instant>);
/// Repeated receipt-less checks of one hash read the chain at most this often.
const UNBOUND_SPACING: Duration = Duration::from_secs(5);
const RECEIPT_TTL: Duration = Duration::from_secs(1800);
const MAX_RECEIPTS: usize = 256;
const MAX_ADDRESS_RECEIPTS: usize = 8;
const MAX_ADDRESS_UNCONFIRMED: usize = 4;
pub use types::{Action, Kind, Operation, ReviewItem, Token, TradingError};

pub struct TradingConfig {
    pub rpc_url: String,
    pub tzkt_url: String,
    pub app_origin: String,
    pub platform_fee_bps: u16,
    pub platform_fee_address: Option<String>,
}
pub struct TradingState {
    chain: Chain,
    slots: Arc<Mutex<HashMap<String, Slot>>>,
    admission: admission::Admission,
    receipts: Mutex<HashMap<String, Receipt>>,
    audit: Option<audit::Audit>,
    fee_audit_unavailable: bool,
    /// Written on each first confirmation; its systemd path unit starts the index sync.
    sync_request: Option<std::path::PathBuf>,
    /// Receipt-less checks per address in the current window: window start, hash → last read.
    unbound: Mutex<HashMap<String, UnboundWindow>>,
    /// Whether the background worker for submitted operations is running.
    watcher: std::sync::atomic::AtomicBool,
    /// Submitted operations by ticket, shown to their sender until confirmed and indexed.
    submitted: Mutex<HashMap<String, submitted::Submitted>>,
}
#[derive(Clone)]
struct Plan {
    session: String,
    ticket: String,
    checked: Checked,
    simulation: Simulation,
}
enum Phase {
    Preparing,
    Reviewed(Box<Plan>),
    Checking,
    Consumed,
}
#[derive(Clone)]
struct Receipt {
    confirmed_hash: Option<String>,
    fee_evasion: Option<confirmation::FeeEvasion>,
    confirming: bool,
    ready: bool,
    actor: String,
    plan: Plan,
    evict_after: Instant,
    expires: Instant,
}
struct Slot {
    ticket: String,
    expires: Instant,
    phase: Phase,
}
#[derive(Serialize)]
pub struct Review {
    pub ticket: String,
    pub expires_seconds: u64,
    pub head: Head,
    pub items: Vec<ReviewItem>,
    pub simulation: Simulation,
}
#[derive(Serialize)]
pub struct Ready {
    pub operations: Vec<Operation>,
    pub branch: String,
    pub chain_id: &'static str,
    pub signing_lock_seconds: u64,
    pub receipt_seconds: u64,
}

struct Reservation {
    slots: Arc<Mutex<HashMap<String, Slot>>>,
    actor: String,
    ticket: String,
    keep: bool,
}

struct ReceiptReservation<'a> {
    receipts: &'a Mutex<HashMap<String, Receipt>>,
    ticket: String,
    keep: bool,
}
impl Drop for ReceiptReservation<'_> {
    fn drop(&mut self) {
        if !self.keep {
            self.receipts
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&self.ticket);
        }
    }
}

struct Confirmation<'a> {
    receipts: &'a Mutex<HashMap<String, Receipt>>,
    ticket: String,
}
impl Drop for Confirmation<'_> {
    fn drop(&mut self) {
        if let Some(receipt) = self
            .receipts
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get_mut(&self.ticket)
        {
            receipt.confirming = false;
        }
    }
}
impl Drop for Reservation {
    fn drop(&mut self) {
        if !self.keep {
            let mut slots = self.slots.lock().unwrap_or_else(|e| e.into_inner());
            if slots
                .get(&self.actor)
                .is_some_and(|s| s.ticket == self.ticket)
            {
                slots.remove(&self.actor);
            }
        }
    }
}

impl TradingState {
    pub fn new(config: TradingConfig) -> Result<Self, TradingError> {
        let _ = reqwest::Url::parse(&config.app_origin).map_err(|_| TradingError::Invalid)?;
        Self::with_transport_and_fee(
            Http::new(&config.rpc_url, &config.tzkt_url)?,
            types::PlatformFee {
                bps: config.platform_fee_bps,
                address: config.platform_fee_address,
            },
        )
    }
    pub fn with_transport(transport: Arc<dyn Transport>) -> Self {
        Self::with_transport_and_fee(transport, types::PlatformFee::default())
            .expect("valid default fee")
    }
    pub fn with_transport_and_fee(
        transport: Arc<dyn Transport>,
        fee: types::PlatformFee,
    ) -> Result<Self, TradingError> {
        fee.validate()?;
        Ok(Self {
            chain: Chain { transport, fee },
            slots: Arc::new(Mutex::new(HashMap::new())),
            admission: admission::Admission::new(),
            receipts: Mutex::new(HashMap::new()),
            audit: None,
            fee_audit_unavailable: false,
            sync_request: None,
            unbound: Mutex::new(HashMap::new()),
            watcher: std::sync::atomic::AtomicBool::new(false),
            submitted: Mutex::new(HashMap::new()),
        })
    }
    /// Admit one receipt-less check of `hash`: `true` to read the chain now, `false` when the
    /// same hash was read moments ago. Each address may check a few distinct hashes per window,
    /// and all addresses together a bounded number; beyond that the caller retries later.
    fn admit_unbound(&self, actor: &str, hash: &str) -> Result<bool, TradingError> {
        let now = Instant::now();
        let mut unbound = self.unbound.lock().unwrap_or_else(|e| e.into_inner());
        unbound.retain(|_, (start, _)| now.duration_since(*start) < UNBOUND_WINDOW);
        let total: usize = unbound.values().map(|(_, hashes)| hashes.len()).sum();
        let (_, hashes) = unbound.entry(actor.into()).or_insert((now, HashMap::new()));
        if let Some(last) = hashes.get_mut(hash) {
            if now.duration_since(*last) < UNBOUND_SPACING {
                return Ok(false);
            }
            *last = now;
            return Ok(true);
        }
        if hashes.len() >= UNBOUND_PER_ADDRESS || total >= UNBOUND_TOTAL {
            return Err(TradingError::RateLimited);
        }
        hashes.insert(hash.into(), now);
        Ok(true)
    }
    /// The signing lock after a check: installed for this ticket even if the checking slot
    /// was lost, so no second review can start while the operation may be signed.
    fn lock_signing(&self, actor: &str, ticket: &str, now: Instant) {
        self.slots.lock().unwrap_or_else(|e| e.into_inner()).insert(
            actor.to_owned(),
            Slot {
                ticket: ticket.to_owned(),
                expires: now + SIGNING_LOCK,
                phase: Phase::Consumed,
            },
        );
    }
    pub fn with_sync_request(mut self, path: std::path::PathBuf) -> Self {
        self.sync_request = Some(path);
        self
    }
    /// Ask the index sync to run now, so a confirmed trade shows without waiting for its timer.
    fn request_sync(&self) {
        if let Some(path) = &self.sync_request {
            if std::fs::write(path, b"").is_err() {
                eprintln!("index_sync_request_unavailable");
            }
        }
    }
    pub fn with_fee_audit(mut self, path: &std::path::Path) -> Result<Self, TradingError> {
        self.audit = Some(audit::Audit::open(path)?);
        self.fee_audit_unavailable = false;
        Ok(self)
    }
    /// Degrade only accepts if durable fee evidence cannot be opened at startup.
    pub fn with_optional_fee_audit(mut self, path: &std::path::Path) -> Self {
        match audit::Audit::open(path) {
            Ok(audit) => {
                self.audit = Some(audit);
                self.fee_audit_unavailable = false;
            }
            Err(_) => {
                self.fee_audit_unavailable = true;
                eprintln!("platform_fee_audit_unavailable_accepts_disabled");
            }
        }
        self
    }
    /// Recover fee evidence without a browser callback. One bounded page per binding.
    pub async fn reconcile_fees(&self) -> Result<(), TradingError> {
        let Some(audit) = &self.audit else {
            return Ok(());
        };
        for due in audit.due().await? {
            // A timeout leaves the leased row available for a later retry.
            if !matches!(
                tokio::time::timeout(Duration::from_secs(30), self.reconcile_binding(audit, &due))
                    .await,
                Ok(Ok(()))
            ) {
                eprintln!("platform_fee_audit_retry");
            }
        }
        Ok(())
    }
    async fn reconcile_binding(
        &self,
        audit: &audit::Audit,
        due: &audit::Due,
    ) -> Result<(), TradingError> {
        let binding = &due.binding;
        let head = self.chain.head().await?;
        let finalized = head.level.checked_sub(2).ok_or(TradingError::Pending)?;
        if finalized < binding.level {
            return Err(TradingError::Pending);
        }
        // TzKT has no counter filter: scan bounded, persistent id pages, then match
        // the manager counter against the canonical RPC operation group.
        let rows = self.chain.indexer(format!("/operations/transactions?sender={}&target={}&entrypoint.in=offer_accept,collection_offer_accept&status=applied&level.ge={}&level.le={}&id.gt={}&sort.asc=id&limit=100", binding.actor, types::V2, binding.level, finalized, due.cursor)).await?;
        let rows = rows
            .as_array()
            .filter(|rows| rows.len() <= 100)
            .ok_or(TradingError::Unavailable)?;
        let mut cursor = due.cursor;
        for row in rows {
            let id = chain::number(&row["id"])?;
            let level = chain::number(&row["level"])?;
            if id <= cursor || level < binding.level || level > finalized {
                return Err(TradingError::Unavailable);
            }
            cursor = id;
            if chain::number(&row["counter"])? != binding.counter {
                continue;
            }
            let hash = chain::text(&row["hash"])?;
            if !valid_operation_hash(hash) {
                return Err(TradingError::Unavailable);
            }
            let (block, contents) = self.canonical_contents(hash, level).await?;
            if !contents.iter().any(|row| {
                row["kind"] == "transaction"
                    && row["source"] == binding.actor
                    && row["counter"].as_str().and_then(|c| c.parse::<u64>().ok())
                        == Some(binding.counter)
                    && row["metadata"]["operation_result"]["status"] == "applied"
            }) {
                return Err(TradingError::Changed);
            }
            let recorded = audit
                .finish(binding.clone(), hash.into(), block, level, contents)
                .await?;
            if recorded.first && !recorded.ambiguous {
                if let Some(event) = &recorded.evasion {
                    confirmation::log(event);
                }
            }
            return Ok(());
        }
        if rows.len() < 100 {
            let value = self
                .chain
                .rpc(
                    format!(
                        "/chains/main/blocks/{finalized}/context/contracts/{}/counter",
                        binding.actor
                    ),
                    None,
                )
                .await?;
            if chain::number(&value)? >= binding.counter {
                return audit.consumed(binding.actor.clone(), binding.counter).await;
            }
        }
        audit
            .progress(
                due.id.clone(),
                cursor,
                if rows.len() == 100 { 60 } else { 3600 },
            )
            .await
    }
    async fn canonical_contents(
        &self,
        hash: &str,
        level: u64,
    ) -> Result<(String, Vec<Value>), TradingError> {
        let value = self
            .chain
            .rpc(format!("/chains/main/blocks/{level}/hash"), None)
            .await?;
        let block = chain::text(&value)?;
        if block.len() != 51 || !block.bytes().all(|b| b.is_ascii_alphanumeric()) {
            return Err(TradingError::Unavailable);
        }
        // Fetch the compact hash list, then only the matching group. A whole
        // manager pass with metadata can exceed the transport's 4 MiB bound.
        let hashes = self
            .chain
            .rpc(
                format!("/chains/main/blocks/{block}/operation_hashes/3"),
                None,
            )
            .await?;
        let index = hashes
            .as_array()
            .filter(|rows| rows.len() <= 65536)
            .and_then(|rows| rows.iter().position(|h| h == hash))
            .ok_or(TradingError::Changed)?;
        let group = self
            .chain
            .rpc(
                format!("/chains/main/blocks/{block}/operations/3/{index}"),
                None,
            )
            .await?;
        if group["hash"] != hash {
            return Err(TradingError::Changed);
        }
        let contents = group["contents"]
            .as_array()
            .filter(|rows| rows.len() <= 100)
            .ok_or(TradingError::Changed)?;
        Ok((block.into(), contents.clone()))
    }
    pub fn platform_fee(&self) -> &types::PlatformFee {
        &self.chain.fee
    }
    pub fn fee_audit_unavailable(&self) -> bool {
        self.fee_audit_unavailable
    }
    pub async fn prepare(
        &self,
        actor: &str,
        session: &str,
        actions: Vec<Action>,
    ) -> Result<Review, TradingError> {
        self.prepare_from(
            actor,
            session,
            actions,
            None,
            IpAddr::V4(Ipv4Addr::LOCALHOST),
        )
        .await
    }
    pub async fn prepare_from(
        &self,
        actor: &str,
        session: &str,
        actions: Vec<Action>,
        hint: Option<Action>,
        client: IpAddr,
    ) -> Result<Review, TradingError> {
        if !valid_actor(actor) || session.is_empty() {
            return Err(TradingError::Unauthorized);
        }
        if self.fee_audit_unavailable
            && actions
                .iter()
                .any(|action| matches!(action, Action::Accept { .. }))
        {
            return Err(TradingError::FeeAuditUnavailable);
        }
        let mut permit = self.admission.acquire(actor, client, 10).await?;
        let mut random = [0u8; 24];
        getrandom::fill(&mut random).map_err(|_| TradingError::Unavailable)?;
        let ticket = hex::encode(random);
        {
            let mut slots = self.slots.lock().unwrap_or_else(|e| e.into_inner());
            let now = Instant::now();
            slots.retain(|_, s| s.expires > now);
            match slots.get(actor).map(|slot| &slot.phase) {
                Some(Phase::Consumed) => {
                    return Err(TradingError::SigningPending(
                        slots[actor]
                            .expires
                            .saturating_duration_since(now)
                            .as_secs()
                            .max(1),
                    ));
                }
                // A review left by another session of this address issued no operation yet,
                // so a new sign-in replaces it instead of waiting for it to expire.
                Some(Phase::Reviewed(plan)) if plan.session != session => {
                    slots.remove(actor);
                }
                Some(_) => return Err(TradingError::Busy),
                None => {}
            }
            if slots.len() >= 128 {
                return Err(TradingError::Busy);
            }
            slots.insert(
                actor.to_owned(),
                Slot {
                    ticket: ticket.clone(),
                    expires: now + REVIEW_TTL,
                    phase: Phase::Preparing,
                },
            );
        }
        let mut reservation = Reservation {
            slots: self.slots.clone(),
            actor: actor.into(),
            ticket: ticket.clone(),
            keep: false,
        };
        permit.start_work();
        let started = Instant::now();
        let outcome = tokio::time::timeout(REVIEW_BUDGET, async {
            settled("prepare", || async {
                let checked = self
                    .chain
                    .inspect_with_hint(actor, &actions, hint.as_ref())
                    .await?;
                let simulation = self
                    .chain
                    .simulate(actor, &checked.head, checked.operations.clone())
                    .await?;
                verify_accept_price(&checked, &simulation)?;
                Ok::<_, TradingError>((checked, simulation))
            })
            .await
        })
        .await
        .map_err(|_| TradingError::Unavailable)
        .and_then(|r| r);
        log_step("prepare", started, &outcome);
        let (checked, simulation) = outcome?;
        let review = Review {
            ticket: ticket.clone(),
            expires_seconds: 120,
            head: checked.head.clone(),
            items: checked.reviews.clone(),
            simulation: simulation.clone(),
        };
        self.slots.lock().unwrap_or_else(|e| e.into_inner()).insert(
            actor.into(),
            Slot {
                ticket: ticket.clone(),
                expires: Instant::now() + REVIEW_TTL,
                phase: Phase::Reviewed(Box::new(Plan {
                    session: session.into(),
                    ticket,
                    checked,
                    simulation,
                })),
            },
        );
        reservation.keep = true;
        Ok(review)
    }
    pub async fn check(
        &self,
        actor: &str,
        session: &str,
        ticket: &str,
    ) -> Result<Ready, TradingError> {
        self.check_from(actor, session, ticket, IpAddr::V4(Ipv4Addr::LOCALHOST))
            .await
    }
    pub async fn check_from(
        &self,
        actor: &str,
        session: &str,
        ticket: &str,
        client: IpAddr,
    ) -> Result<Ready, TradingError> {
        let mut permit = self.admission.acquire(actor, client, 10).await?;

        // Reserve receipt capacity before spending time on upstream chain calls.
        // The reservation also prevents concurrent checks from oversubscribing it.
        let now = Instant::now();
        let plan = {
            let mut slots = self.slots.lock().unwrap_or_else(|e| e.into_inner());
            let slot = slots
                .get_mut(actor)
                .filter(|s| s.expires > now)
                .ok_or(TradingError::Expired)?;
            let Phase::Reviewed(plan) = &slot.phase else {
                return Err(TradingError::Expired);
            };
            if plan.session != session || plan.ticket != ticket {
                return Err(TradingError::Unauthorized);
            }
            (**plan).clone()
        };
        if self.fee_audit_unavailable
            && plan
                .checked
                .reviews
                .iter()
                .any(|review| matches!(review.action, Action::Accept { .. }))
        {
            return Err(TradingError::FeeAuditUnavailable);
        }
        {
            let mut receipts = self.receipts.lock().unwrap_or_else(|e| e.into_inner());
            if receipts
                .get(ticket)
                .is_some_and(|receipt| !receipt.ready || receipt.expires > now)
            {
                return Err(TradingError::Busy);
            }
            receipt_room(&mut receipts, actor, now)?;
            receipts.insert(
                ticket.into(),
                Receipt {
                    confirmed_hash: None,
                    fee_evasion: None,
                    confirming: false,
                    ready: false,
                    actor: actor.into(),
                    plan: plan.clone(),
                    evict_after: now + SIGNING_LOCK + Duration::from_secs(30),
                    expires: now + RECEIPT_TTL,
                },
            );
        }
        let mut receipt_reservation = ReceiptReservation {
            receipts: &self.receipts,
            ticket: ticket.into(),
            keep: false,
        };

        let plan = {
            let mut slots = self.slots.lock().unwrap_or_else(|e| e.into_inner());
            let slot = slots
                .get_mut(actor)
                .filter(|s| s.expires > Instant::now())
                .ok_or(TradingError::Expired)?;
            let Phase::Reviewed(plan) = &slot.phase else {
                return Err(TradingError::Expired);
            };
            if plan.session != session || plan.ticket != ticket {
                return Err(TradingError::Unauthorized);
            }
            let plan = (**plan).clone();
            slot.phase = Phase::Checking;
            // Outlives the whole check, so no other review can prune it meanwhile.
            slot.expires = Instant::now() + REVIEW_BUDGET + Duration::from_secs(10);
            plan
        };
        let mut reservation = Reservation {
            slots: self.slots.clone(),
            actor: actor.into(),
            ticket: ticket.into(),
            keep: false,
        };
        // A definite failure or cancellation before Ready can release the checking
        // slot. Once Ready is issued, a client rejection cannot prove non-submission.
        permit.start_work();
        let started = Instant::now();
        let outcome = tokio::time::timeout(
            REVIEW_BUDGET,
            settled("check", || async {
                let actions: Vec<_> = plan
                    .checked
                    .reviews
                    .iter()
                    .map(|r| r.action.clone())
                    .collect();
                let current = self.chain.inspect(actor, &actions).await?;
                self.chain
                    .ancestor(&plan.checked.head, &current.head)
                    .await?;
                if current.proof != plan.checked.proof {
                    return Err(TradingError::Changed);
                }
                let simulated = self
                    .chain
                    .simulate(actor, &current.head, current.operations.clone())
                    .await?;
                verify_accept_price(&current, &simulated)?;
                if simulated.counter != plan.simulation.counter
                    || simulated.accept_fees != plan.simulation.accept_fees
                    || simulated.fee_mutez > plan.simulation.fee_mutez
                    || simulated.storage_burn_cap_mutez > plan.simulation.storage_burn_cap_mutez
                    || simulated.proceeds_mutez != plan.simulation.proceeds_mutez
                    || simulated
                        .operations
                        .iter()
                        .zip(&plan.simulation.operations)
                        .any(|(new, old)| {
                            new.gas_limit.parse::<u64>().unwrap_or(u64::MAX)
                                > old.gas_limit.parse::<u64>().unwrap_or(0)
                                || new.storage_limit.parse::<u64>().unwrap_or(u64::MAX)
                                    > old.storage_limit.parse::<u64>().unwrap_or(0)
                        })
                {
                    return Err(TradingError::Changed);
                }
                Ok(Ready {
                    operations: plan.simulation.operations.clone(),
                    branch: current.head.hash,
                    chain_id: types::MAINNET,
                    signing_lock_seconds: SIGNING_LOCK.as_secs(),
                    receipt_seconds: RECEIPT_TTL.as_secs(),
                })
            }),
        )
        .await
        .map_err(|_| TradingError::Unavailable)
        .and_then(|r| r);
        log_step("check", started, &outcome);
        let result = outcome?;
        if let (Some(audit), Some(binding)) =
            (&self.audit, audit::Binding::from_plan(actor, &plan)?)
        {
            if !matches!(audit.record(binding).await, Ok(true)) {
                eprintln!("platform_fee_audit_record_unavailable");
            }
        }
        let now = Instant::now();
        if let Some(receipt) = self
            .receipts
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get_mut(ticket)
        {
            receipt.ready = true;
            receipt.evict_after = now + SIGNING_LOCK;
            receipt.expires = now + RECEIPT_TTL;
        }
        receipt_reservation.keep = true;
        self.lock_signing(actor, ticket, now);
        reservation.keep = true;
        Ok(result)
    }
    /// Only an included exact operation group unlocks the next bulk page early.
    pub async fn confirm(
        &self,
        actor: &str,
        session: &str,
        ticket: &str,
        hash: &str,
    ) -> Result<(), TradingError> {
        self.confirm_from(
            actor,
            session,
            ticket,
            hash,
            IpAddr::V4(Ipv4Addr::LOCALHOST),
        )
        .await
    }
    pub async fn confirm_from(
        &self,
        actor: &str,
        session: &str,
        ticket: &str,
        hash: &str,
        client: IpAddr,
    ) -> Result<(), TradingError> {
        self.confirm_inner(actor, session, ticket, hash, Some(client))
            .await
    }
    /// `client` is `None` only for the in-process watcher, which skips request admission.
    async fn confirm_inner(
        &self,
        actor: &str,
        session: &str,
        ticket: &str,
        hash: &str,
        client: Option<IpAddr>,
    ) -> Result<(), TradingError> {
        if !valid_actor(actor) || session.is_empty() {
            return Err(TradingError::Unauthorized);
        }
        if ticket.len() != 48
            || !ticket
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
            || !valid_operation_hash(hash)
        {
            return Err(TradingError::Invalid);
        }
        // The watcher's verdict on a submitted operation answers its page.
        if let Some(verdict) = self.verdict(actor, ticket, hash) {
            return verdict;
        }
        let known = self
            .receipts
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(ticket)
            .is_some_and(|r| r.expires > Instant::now());
        if !known {
            let Some(client) = client else {
                return Err(TradingError::Expired);
            };
            if !self.admit_unbound(actor, hash)? {
                return Err(TradingError::Pending);
            }
            return self.confirm_unbound(actor, hash, client).await;
        }
        let plan = {
            let mut receipts = self.receipts.lock().unwrap_or_else(|e| e.into_inner());
            let receipt = receipts
                .get_mut(ticket)
                .filter(|r| r.expires > Instant::now())
                .ok_or(TradingError::Expired)?;
            if receipt.actor != actor {
                return Err(TradingError::Unauthorized);
            }
            if let Some(confirmed) = &receipt.confirmed_hash {
                return if confirmed == hash {
                    if receipt.fee_evasion.is_some() {
                        Err(TradingError::FeeEvasion)
                    } else {
                        Ok(())
                    }
                } else {
                    Err(TradingError::Changed)
                };
            }
            if receipt.confirming {
                return Err(TradingError::Pending);
            }
            if !receipt.ready {
                return Err(TradingError::Pending);
            }
            receipt.confirming = true;
            // Keep a polled receipt through the next retry without extending its TTL.
            receipt.evict_after = (Instant::now() + Duration::from_secs(60)).min(receipt.expires);
            receipt.plan.clone()
        };
        let _confirmation = Confirmation {
            receipts: &self.receipts,
            ticket: ticket.into(),
        };
        let mut permit = match client {
            Some(client) => Some(self.admission.acquire(actor, client, 1).await?),
            None => None,
        };
        // A queued confirmation may outlive its receipt or follow a completed one.
        {
            let receipts = self.receipts.lock().unwrap_or_else(|e| e.into_inner());
            let receipt = receipts
                .get(ticket)
                .filter(|r| r.expires > Instant::now())
                .ok_or(TradingError::Expired)?;
            if let Some(confirmed) = &receipt.confirmed_hash {
                return if confirmed == hash {
                    if receipt.fee_evasion.is_some() {
                        Err(TradingError::FeeEvasion)
                    } else {
                        Ok(())
                    }
                } else {
                    Err(TradingError::Changed)
                };
            }
        }
        if let Some(permit) = permit.as_mut() {
            permit.start_work();
        }
        let (evasion, durable_log) = tokio::time::timeout(Duration::from_secs(20), async {
            let value = self
                .chain
                .indexer(format!(
                    "/operations/transactions/{hash}?status=applied&limit=100"
                ))
                .await?;
            let rows = value
                .as_array()
                .filter(|a| !a.is_empty())
                .ok_or(TradingError::Pending)?;
            let level = chain::number(&rows[0]["level"])?;
            let head = self.chain.head().await.map_err(lagging)?;
            if head.level < level.checked_add(2).ok_or(TradingError::Changed)? {
                return Err(TradingError::Pending);
            }
            let (block, contents) = self
                .canonical_contents(hash, level)
                .await
                .map_err(lagging)?;
            let mut evasion = confirmation::classify(actor, &plan, &contents, hash, &block, level)?;
            let first = if let (Some(audit), Some(binding)) =
                (&self.audit, audit::Binding::from_plan(actor, &plan)?)
            {
                match audit
                    .finish(binding, hash.into(), block, level, contents)
                    .await
                {
                    Ok(recorded) => {
                        evasion = recorded.evasion;
                        Some(recorded.first)
                    }
                    Err(TradingError::Changed) => return Err(TradingError::Changed),
                    Err(_) => {
                        // Acceptance is already proven. Do not report a retryable
                        // transaction failure or accuse without checking sibling terms.
                        eprintln!("platform_fee_audit_record_unavailable");
                        evasion = None;
                        Some(false)
                    }
                }
            } else {
                None
            };
            Ok::<_, TradingError>((evasion, first))
        })
        .await
        .map_err(|_| TradingError::Unavailable)??;
        let should_log = {
            let mut receipts = self.receipts.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(receipt) = receipts.get_mut(ticket) {
                let first = receipt.confirmed_hash.is_none();
                if first {
                    receipt.confirmed_hash = Some(hash.into());
                    receipt.fee_evasion = evasion.clone();
                }
                first
            } else {
                true
            }
        };
        if durable_log.unwrap_or(should_log) {
            if let Some(event) = &evasion {
                confirmation::log(event);
            }
        }
        // Applied either way (a fee evasion is an applied acceptance): refresh the index once.
        if should_log {
            self.request_sync();
        }
        let mut slots = self.slots.lock().unwrap_or_else(|e| e.into_inner());
        if slots.get(actor).is_some_and(|s| s.ticket == ticket) {
            slots.remove(actor);
        }
        if evasion.is_some() {
            Err(TradingError::FeeEvasion)
        } else {
            Ok(())
        }
    }
    /// Without a receipt (expired, or lost in a restart) the operation is confirmed for its
    /// sender from the chain alone: applied, sent by this address and final. Fee evidence for
    /// it is the audit worker's job, which needs no receipt.
    async fn confirm_unbound(
        &self,
        actor: &str,
        hash: &str,
        client: IpAddr,
    ) -> Result<(), TradingError> {
        let mut permit = self.admission.acquire(actor, client, 1).await?;
        permit.start_work();
        tokio::time::timeout(Duration::from_secs(20), async {
            let value = self
                .chain
                .indexer(format!(
                    "/operations/transactions/{hash}?status=applied&limit=100"
                ))
                .await?;
            let rows = value
                .as_array()
                .filter(|a| !a.is_empty())
                .ok_or(TradingError::Pending)?;
            if rows[0]["sender"]["address"] != actor {
                return Err(TradingError::Unauthorized);
            }
            let level = chain::number(&rows[0]["level"])?;
            let head = self.chain.head().await.map_err(lagging)?;
            if head.level < level.checked_add(2).ok_or(TradingError::Changed)? {
                return Err(TradingError::Pending);
            }
            if head.level - level <= RECENT_BLOCKS {
                self.request_sync();
            }
            Ok(())
        })
        .await
        .map_err(|_| TradingError::Unavailable)?
    }
    pub fn discard_review(
        &self,
        actor: &str,
        session: &str,
        ticket: &str,
    ) -> Result<(), TradingError> {
        let mut slots = self.slots.lock().unwrap_or_else(|e| e.into_inner());
        let slot = slots.get(actor).ok_or(TradingError::Expired)?;
        match &slot.phase {
            Phase::Reviewed(p) if p.ticket == ticket && p.session == session => {
                slots.remove(actor);
                Ok(())
            }
            _ => Err(TradingError::Expired),
        }
    }
}
fn receipt_room(
    receipts: &mut HashMap<String, Receipt>,
    actor: &str,
    now: Instant,
) -> Result<(), TradingError> {
    receipts.retain(|_, r| r.expires > now || r.confirming || !r.ready);
    let mut planned: HashMap<_, _> = receipts.iter().collect();
    let unconfirmed_owned = planned
        .values()
        .filter(|r| r.actor == actor && r.confirmed_hash.is_none() && r.fee_evasion.is_none())
        .count();
    if unconfirmed_owned >= MAX_ADDRESS_UNCONFIRMED {
        evict_oldest_unconfirmed(&mut planned, Some(actor), now)?;
    }
    if planned.values().filter(|r| r.actor == actor).count() >= MAX_ADDRESS_RECEIPTS {
        evict_confirmed(&mut planned, Some(actor))
            .or_else(|_| evict_oldest_unconfirmed(&mut planned, Some(actor), now))?;
    }
    if planned.len() >= MAX_RECEIPTS {
        evict_confirmed(&mut planned, None)
            .or_else(|_| evict_oldest_unconfirmed(&mut planned, None, now))?;
    }
    let removed: Vec<_> = receipts
        .keys()
        .filter(|key| !planned.contains_key(key))
        .cloned()
        .collect();
    drop(planned);
    for key in removed {
        receipts.remove(&key);
    }
    Ok(())
}

fn evict_confirmed(
    receipts: &mut HashMap<&String, &Receipt>,
    actor: Option<&str>,
) -> Result<(), TradingError> {
    let oldest = receipts
        .iter()
        .filter(|(_, r)| {
            !r.confirming
                && r.confirmed_hash.is_some()
                && r.fee_evasion.is_none()
                && actor.is_none_or(|a| r.actor == a)
        })
        .min_by_key(|(_, r)| r.expires)
        .map(|(ticket, _)| *ticket);
    if let Some(ticket) = oldest {
        receipts.remove(&ticket);
        Ok(())
    } else {
        Err(TradingError::Busy)
    }
}

fn evict_oldest_unconfirmed(
    receipts: &mut HashMap<&String, &Receipt>,
    actor: Option<&str>,
    now: Instant,
) -> Result<(), TradingError> {
    let eligible = receipts
        .iter()
        .filter(|(_, r)| {
            !r.confirming
                && r.confirmed_hash.is_none()
                && r.fee_evasion.is_none()
                && r.ready
                && actor.is_none_or(|a| r.actor == a)
                && r.evict_after <= now
        })
        .min_by_key(|(_, r)| r.evict_after)
        .map(|(ticket, _)| *ticket);
    if let Some(ticket) = eligible {
        receipts.remove(&ticket);
        return Ok(());
    }
    let retry = receipts
        .values()
        .filter(|r| {
            r.confirmed_hash.is_none()
                && r.fee_evasion.is_none()
                && actor.is_none_or(|a| r.actor == a)
        })
        .map(|r| {
            if r.confirming || !r.ready {
                r.expires.max(now)
            } else {
                r.evict_after.max(now).min(r.expires.max(now))
            }
        })
        .min()
        .map(|at| {
            let delay = at.saturating_duration_since(now);
            delay
                .as_secs()
                .saturating_add(u64::from(delay.subsec_nanos() > 0))
                .max(1)
        })
        .unwrap_or(1);
    Err(TradingError::ReceiptCapacity(retry))
}
/// Runs `attempt` again after a short pause while it reports a changed market, so a node or
/// indexer that is a few blocks behind is not mistaken for a change.
async fn settled<T, F, Fut>(step: &str, mut attempt: F) -> Result<T, TradingError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, TradingError>>,
{
    for n in 1..=LAG_RETRIES {
        match attempt().await {
            Err(TradingError::Changed) => {
                eprintln!("trading_{step}_reread {n}");
                tokio::time::sleep(LAG_PAUSE).await;
            }
            other => return other,
        }
    }
    attempt().await
}
/// One journal line per review or check: outcome and upstream time, no addresses.
fn log_step<T>(step: &str, started: Instant, outcome: &Result<T, TradingError>) {
    let result = match outcome {
        Ok(_) => "ok".to_owned(),
        Err(error) => format!("{error:?}"),
    };
    eprintln!(
        "trading_{step} {result} {}ms",
        started.elapsed().as_millis()
    );
}
/// A node behind the indexer answers 404 for a block the indexer already reports, or
/// lacks the group there: retry the confirmation instead of reporting a changed market.
fn lagging(error: TradingError) -> TradingError {
    match error {
        TradingError::Changed => TradingError::Pending,
        other => other,
    }
}
fn valid_actor(actor: &str) -> bool {
    actor.len() == 36
        && ["tz1", "tz2", "tz3"].iter().any(|p| actor.starts_with(p))
        && actor.bytes().all(|c| c.is_ascii_alphanumeric())
}

fn verify_accept_price(checked: &Checked, simulation: &Simulation) -> Result<(), TradingError> {
    match (
        checked
            .reviews
            .iter()
            .find(|r| matches!(r.action, Action::Accept { .. })),
        &simulation.accept_fees,
    ) {
        (Some(review), Some(fees)) if review.price_mutez == fees.price_mutez => Ok(()),
        (None, None) => Ok(()),
        _ => Err(TradingError::Simulation),
    }
}

fn same_parameters(actual: &Value, expected: &Value) -> Result<bool, TradingError> {
    if expected.is_null() {
        return Ok(actual.is_null()
            || *actual == serde_json::json!({"entrypoint":"default","value":{"prim":"Unit"}}));
    }
    Ok(actual["entrypoint"] == expected["entrypoint"]
        && chain::normalized(&actual["value"])? == chain::normalized(&expected["value"])?)
}

pub fn action_json(action: &Action) -> String {
    serde_json::to_string(action).expect("serializable action")
}
pub fn parse_actions(value: Value) -> Result<Vec<Action>, TradingError> {
    serde_json::from_value(value).map_err(|_| TradingError::Invalid)
}

fn valid_operation_hash(hash: &str) -> bool {
    hash.len() == 51
        && hash.starts_with('o')
        && hash
            .bytes()
            .all(|b| b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz".contains(&b))
}

#[cfg(test)]
mod lifecycle_tests {
    use super::*;
    struct Unavailable;
    impl Transport for Unavailable {
        fn request(&self, _: http::Endpoint, _: String, _: Option<Value>) -> http::Reply {
            Box::pin(async { Err(TradingError::Unavailable) })
        }
    }
    fn plan() -> Plan {
        Plan {
            session: "s".into(),
            ticket: TICKET.into(),
            checked: Checked {
                head: Head {
                    hash: String::new(),
                    level: 1,
                },
                reviews: vec![],
                proof: String::new(),
                operations: vec![],
            },
            simulation: Simulation {
                operations: vec![],
                fee_mutez: 0,
                storage_burn_cap_mutez: 0,
                gas: 0,
                bytes: 0,
                counter: 0,
                proceeds_mutez: "0".into(),
                accept_fees: None,
            },
        }
    }
    const TICKET: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const ACTOR: &str = "tz1P719ymGCtNwavZKh2LmhMs53dGsEMzjnm";
    #[tokio::test]
    async fn receipt_survives_signing_hold_pruning_but_has_its_own_expiry() {
        let state = TradingState::with_transport(Arc::new(Unavailable));
        let now = Instant::now();
        state.slots.lock().unwrap().insert(
            ACTOR.into(),
            Slot {
                ticket: TICKET.into(),
                expires: now - Duration::from_secs(1),
                phase: Phase::Consumed,
            },
        );
        state.receipts.lock().unwrap().insert(
            TICKET.into(),
            Receipt {
                confirmed_hash: None,
                fee_evasion: None,
                confirming: false,
                ready: true,
                actor: ACTOR.into(),
                plan: plan(),
                evict_after: now + SIGNING_LOCK,
                expires: now + RECEIPT_TTL,
            },
        );
        let _ = state
            .prepare("tz1ewF2P3erZJSP8m35QiNEKGtwj7qsBhF8z", "s", vec![])
            .await;
        assert!(!state.slots.lock().unwrap().contains_key(ACTOR));
        assert!(matches!(
            state
                .confirm(ACTOR, "s", TICKET, &format!("o{}", "a".repeat(50)))
                .await,
            Err(TradingError::Unavailable)
        ));
        state
            .receipts
            .lock()
            .unwrap()
            .get_mut(TICKET)
            .unwrap()
            .expires = now - Duration::from_secs(1);
        // An expired receipt falls back to the chain, which is unavailable here.
        assert!(matches!(
            state
                .confirm(ACTOR, "s", TICKET, &format!("o{}", "a".repeat(50)))
                .await,
            Err(TradingError::Unavailable)
        ));
    }
    #[test]
    fn client_rejection_cannot_discard_an_issued_operation() {
        let state = TradingState::with_transport(Arc::new(Unavailable));
        state.slots.lock().unwrap().insert(
            ACTOR.into(),
            Slot {
                ticket: TICKET.into(),
                expires: Instant::now() + SIGNING_LOCK,
                phase: Phase::Consumed,
            },
        );
        assert!(state.discard_review(ACTOR, "s", TICKET).is_err());
        assert!(state.slots.lock().unwrap().contains_key(ACTOR));
    }
    fn receipt(actor: &str, expires: Instant, confirmed: bool) -> Receipt {
        Receipt {
            confirmed_hash: confirmed.then(|| format!("o{}", "a".repeat(50))),
            fee_evasion: None,
            confirming: false,
            ready: true,
            actor: actor.into(),
            plan: plan(),
            evict_after: expires - SIGNING_LOCK,
            expires,
        }
    }
    #[test]
    fn receipt_caps_preserve_unconfirmed_claims_and_evict_confirmed_first() {
        let now = Instant::now();
        let mut receipts = HashMap::new();
        for n in 0..MAX_ADDRESS_UNCONFIRMED {
            receipts.insert(n.to_string(), receipt(ACTOR, now + RECEIPT_TTL, false));
        }
        assert!(matches!(
            receipt_room(&mut receipts, ACTOR, now),
            Err(TradingError::ReceiptCapacity(_))
        ));
        assert_eq!(receipts.len(), MAX_ADDRESS_UNCONFIRMED);
        receipts.get_mut("0").unwrap().confirmed_hash = Some("confirmed".into());
        for n in MAX_ADDRESS_UNCONFIRMED..MAX_ADDRESS_RECEIPTS {
            receipts.insert(n.to_string(), receipt(ACTOR, now + RECEIPT_TTL, true));
        }
        receipts.get_mut("0").unwrap().expires -= Duration::from_secs(1);
        receipt_room(&mut receipts, ACTOR, now).unwrap();
        assert_eq!(receipts.len(), MAX_ADDRESS_RECEIPTS - 1);
        assert!(!receipts.contains_key("0"));
        assert!(receipts.contains_key("1"));
        assert!(receipts.contains_key("2"));
        assert!(receipts.contains_key("3"));
        receipts.clear();
        for n in 0..MAX_RECEIPTS {
            receipts.insert(
                n.to_string(),
                receipt(&format!("actor-{n}"), now + RECEIPT_TTL, false),
            );
        }
        assert!(matches!(
            receipt_room(&mut receipts, ACTOR, now),
            Err(TradingError::ReceiptCapacity(_))
        ));
        assert_eq!(receipts.len(), MAX_RECEIPTS);
        receipts.get_mut("100").unwrap().confirmed_hash = Some("confirmed".into());
        receipt_room(&mut receipts, ACTOR, now).unwrap();
        assert_eq!(receipts.len(), MAX_RECEIPTS - 1);
        assert!(!receipts.contains_key("100"));
        assert!(receipts.values().all(|r| r.confirmed_hash.is_none()));
        receipts.get_mut("0").unwrap().expires = now;
        receipt_room(&mut receipts, ACTOR, now).unwrap();
        assert!(!receipts.contains_key("0"));
    }
    #[test]
    fn unconfirmed_receipts_evict_only_after_signing_lock_at_both_caps() {
        let now = Instant::now();
        let mut per_address = HashMap::new();
        for n in 0..MAX_ADDRESS_UNCONFIRMED {
            let mut r = receipt(ACTOR, now + RECEIPT_TTL, false);
            r.evict_after = now + SIGNING_LOCK;
            per_address.insert(n.to_string(), r);
        }
        assert!(matches!(
            receipt_room(&mut per_address, ACTOR, now),
            Err(TradingError::ReceiptCapacity(n)) if n > 0 && n <= SIGNING_LOCK.as_secs()
        ));
        assert_eq!(per_address.len(), MAX_ADDRESS_UNCONFIRMED);
        receipt_room(&mut per_address, ACTOR, now + SIGNING_LOCK).unwrap();
        assert_eq!(per_address.len(), MAX_ADDRESS_UNCONFIRMED - 1);

        let mut global = HashMap::new();
        for n in 0..MAX_RECEIPTS {
            let actor = if n == 0 { ACTOR } else { "someone-else" };
            let mut r = receipt(actor, now + RECEIPT_TTL, false);
            r.evict_after = now + SIGNING_LOCK;
            global.insert(n.to_string(), r);
        }
        assert!(matches!(
            receipt_room(&mut global, ACTOR, now),
            Err(TradingError::ReceiptCapacity(_))
        ));
        assert_eq!(global.len(), MAX_RECEIPTS);
        receipt_room(&mut global, ACTOR, now + SIGNING_LOCK).unwrap();
        assert_eq!(global.len(), MAX_RECEIPTS - 1);
    }

    #[test]
    fn active_confirmation_receipt_is_never_evicted() {
        let now = Instant::now();
        let mut receipts = HashMap::new();
        for n in 0..MAX_ADDRESS_UNCONFIRMED {
            let mut r = receipt(ACTOR, now + RECEIPT_TTL, false);
            r.evict_after = if n == 0 {
                now - Duration::from_secs(1)
            } else {
                now + SIGNING_LOCK
            };
            receipts.insert(n.to_string(), r);
        }
        receipts.get_mut("0").unwrap().confirming = true;
        assert!(matches!(
            receipt_room(&mut receipts, ACTOR, now),
            Err(TradingError::ReceiptCapacity(_))
        ));
        assert!(receipts.get("0").unwrap().confirming);
        assert_eq!(receipts.len(), MAX_ADDRESS_UNCONFIRMED);
    }

    #[test]
    fn unready_and_fee_evasion_receipts_are_not_evicted() {
        let now = Instant::now();
        let mut receipts = HashMap::new();
        for n in 0..MAX_ADDRESS_UNCONFIRMED {
            let mut r = receipt(ACTOR, now + RECEIPT_TTL, false);
            r.evict_after = now - Duration::from_secs(1);
            if n == 0 {
                r.ready = false;
            } else if n == 1 {
                r.fee_evasion = Some(confirmation::FeeEvasion {
                    operation_hash: String::new(),
                    block_hash: String::new(),
                    recipient: String::new(),
                    actor: ACTOR.into(),
                    level: 0,
                    expected_mutez: 0,
                    observed_mutez: 0,
                });
                r.confirmed_hash = Some("applied-operation".into());
            } else {
                r.evict_after = now + SIGNING_LOCK;
            }
            receipts.insert(n.to_string(), r);
        }
        for n in MAX_ADDRESS_UNCONFIRMED..MAX_ADDRESS_RECEIPTS {
            let mut r = receipt(ACTOR, now + RECEIPT_TTL, true);
            r.fee_evasion = Some(confirmation::FeeEvasion {
                operation_hash: format!("hash-{n}"),
                block_hash: format!("block-{n}"),
                level: n as u64,
                actor: ACTOR.into(),
                recipient: "recipient".into(),
                expected_mutez: 1,
                observed_mutez: 0,
            });
            receipts.insert(n.to_string(), r);
        }
        assert!(matches!(
            receipt_room(&mut receipts, ACTOR, now),
            Err(TradingError::ReceiptCapacity(_))
        ));
        assert!(receipts.contains_key("0"));
        assert!(receipts.contains_key("1"));
        assert_eq!(receipts.len(), MAX_ADDRESS_RECEIPTS);
    }

    #[test]
    fn receipt_capacity_retry_seconds_round_up() {
        let now = Instant::now();
        let mut receipts = HashMap::new();
        for n in 0..MAX_ADDRESS_UNCONFIRMED {
            let mut r = receipt(ACTOR, now + RECEIPT_TTL, false);
            r.evict_after = now + SIGNING_LOCK + Duration::from_millis(500);
            receipts.insert(n.to_string(), r);
        }
        assert!(matches!(
            receipt_room(&mut receipts, ACTOR, now),
            Err(TradingError::ReceiptCapacity(121))
        ));
    }

    #[test]
    fn terminal_fee_evasion_receipts_survive_capacity_eviction() {
        let now = Instant::now();
        let mut receipts = HashMap::new();
        for n in 0..MAX_RECEIPTS {
            let actor = if n == 0 { ACTOR } else { "someone-else" };
            let mut r = receipt(actor, now + RECEIPT_TTL, true);
            if n == 0 {
                r.fee_evasion = Some(confirmation::FeeEvasion {
                    operation_hash: "hash".into(),
                    block_hash: "block".into(),
                    level: 1,
                    actor: ACTOR.into(),
                    recipient: "recipient".into(),
                    expected_mutez: 1,
                    observed_mutez: 0,
                });
            }
            receipts.insert(n.to_string(), r);
        }
        receipt_room(&mut receipts, ACTOR, now).unwrap();
        assert!(receipts.contains_key("0"));
        assert_eq!(receipts.len(), MAX_RECEIPTS - 1);
    }

    struct CountCalls(std::sync::atomic::AtomicUsize);
    impl Transport for CountCalls {
        fn request(&self, _: http::Endpoint, _: String, _: Option<Value>) -> http::Reply {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Box::pin(async { Err(TradingError::Unavailable) })
        }
    }

    #[tokio::test]
    async fn receipt_exhaustion_is_rejected_before_chain_calls() {
        let transport = Arc::new(CountCalls(std::sync::atomic::AtomicUsize::new(0)));
        let state = TradingState::with_transport(transport.clone());
        let now = Instant::now();
        state.slots.lock().unwrap().insert(
            ACTOR.into(),
            Slot {
                ticket: TICKET.into(),
                expires: now + REVIEW_TTL,
                phase: Phase::Reviewed(Box::new(plan())),
            },
        );
        {
            let mut receipts = state.receipts.lock().unwrap();
            for n in 0..MAX_ADDRESS_UNCONFIRMED {
                let mut r = receipt(ACTOR, now + RECEIPT_TTL, false);
                r.evict_after = now + SIGNING_LOCK;
                receipts.insert(format!("receipt-{n}"), r);
            }
        }
        assert!(matches!(
            state.check(ACTOR, "s", TICKET).await,
            Err(TradingError::ReceiptCapacity(_))
        ));
        assert_eq!(transport.0.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn duplicate_check_does_not_replace_an_existing_receipt_reservation() {
        let transport = Arc::new(CountCalls(std::sync::atomic::AtomicUsize::new(0)));
        let state = TradingState::with_transport(transport.clone());
        let now = Instant::now();
        state.slots.lock().unwrap().insert(
            ACTOR.into(),
            Slot {
                ticket: TICKET.into(),
                expires: now + REVIEW_TTL,
                phase: Phase::Reviewed(Box::new(plan())),
            },
        );
        let mut reserved = receipt(ACTOR, now + RECEIPT_TTL, false);
        reserved.ready = false;
        state
            .receipts
            .lock()
            .unwrap()
            .insert(TICKET.into(), reserved);
        assert!(matches!(
            state.check(ACTOR, "s", TICKET).await,
            Err(TradingError::Busy)
        ));
        assert!(!state.receipts.lock().unwrap()[TICKET].ready);
        assert_eq!(transport.0.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn overlapping_confirmation_is_rejected_without_duplicate_chain_work() {
        let transport = Arc::new(CountCalls(std::sync::atomic::AtomicUsize::new(0)));
        let state = TradingState::with_transport(transport.clone());
        let now = Instant::now();
        let mut active = receipt(ACTOR, now + RECEIPT_TTL, false);
        active.confirming = true;
        state.receipts.lock().unwrap().insert(TICKET.into(), active);
        assert!(matches!(
            state
                .confirm(ACTOR, "s", TICKET, &format!("o{}", "a".repeat(50)))
                .await,
            Err(TradingError::Pending)
        ));
        assert_eq!(transport.0.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn confirmed_receipt_is_address_bound_and_does_not_extend_its_expiry() {
        let state = TradingState::with_transport(Arc::new(Unavailable));
        let now = Instant::now();
        state
            .receipts
            .lock()
            .unwrap()
            .insert(TICKET.into(), receipt(ACTOR, now + RECEIPT_TTL, true));
        let hash = format!("o{}", "a".repeat(50));
        state
            .confirm(ACTOR, "new-session", TICKET, &hash)
            .await
            .unwrap();
        assert_eq!(
            state.receipts.lock().unwrap()[TICKET].expires,
            now + RECEIPT_TTL
        );
        state
            .receipts
            .lock()
            .unwrap()
            .get_mut(TICKET)
            .unwrap()
            .expires = now;
        // An expired receipt falls back to the chain, which is unavailable here.
        assert!(matches!(
            state.confirm(ACTOR, "new-session", TICKET, &hash).await,
            Err(TradingError::Unavailable)
        ));
    }
    #[tokio::test]
    async fn a_check_installs_its_signing_lock_even_after_losing_its_slot() {
        let state = TradingState::with_transport(Arc::new(Unavailable));
        // Another address's review pruned this check's slot meanwhile.
        assert!(state.slots.lock().unwrap().get(ACTOR).is_none());
        state.lock_signing(ACTOR, TICKET, Instant::now());
        {
            let slots = state.slots.lock().unwrap();
            let slot = &slots[ACTOR];
            assert_eq!(slot.ticket, TICKET);
            assert!(matches!(slot.phase, Phase::Consumed));
        }
        assert!(matches!(
            state.prepare(ACTOR, "s", vec![]).await,
            Err(TradingError::SigningPending(_))
        ));
    }
    #[tokio::test]
    async fn recently_polled_receipt_survives_other_checks_until_retry_or_expiry() {
        let state = TradingState::with_transport(Arc::new(Unavailable));
        let now = Instant::now();
        let mut polled = receipt(ACTOR, now + RECEIPT_TTL, false);
        polled.evict_after = now - Duration::from_secs(1);
        state.receipts.lock().unwrap().insert(TICKET.into(), polled);
        let _ = state
            .confirm(ACTOR, "s", TICKET, &format!("o{}", "a".repeat(50)))
            .await;
        let mut receipts = state.receipts.lock().unwrap();
        for n in 1..MAX_ADDRESS_UNCONFIRMED {
            let mut other = receipt(ACTOR, now + RECEIPT_TTL, false);
            other.evict_after = now - Duration::from_secs(1);
            receipts.insert(n.to_string(), other);
        }
        receipt_room(&mut receipts, ACTOR, now).unwrap();
        assert!(receipts.contains_key(TICKET));
        assert_eq!(receipts[TICKET].expires, now + RECEIPT_TTL);
        assert!(!receipts[TICKET].confirming);
        assert!(receipts[TICKET].evict_after > now);
    }
}
