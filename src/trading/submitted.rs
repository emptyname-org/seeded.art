//! Operations a page reported as submitted. Their effects show to the sender at once and are
//! withdrawn unless the operation is confirmed. One background worker checks them, one at a
//! time, oldest due first; only chain evidence makes one a failure.

use super::types::{Action, Kind, Token, TradingError, V2};
use super::{Plan, RECEIPT_TTL, TradingState, valid_actor, valid_operation_hash};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
// The runtime clock, so tests can advance it.
use tokio::time::Instant;

/// Effects stay shown this long after confirmation, while the index sync catches up.
const OVERLAY_HOLD: Duration = Duration::from_secs(180);
/// A failure stays reportable this long after it is known.
const FAILED_KEEP: Duration = Duration::from_secs(600);
/// Unconfirmed this long after submission without proof of failure: unverified, which a later
/// valid confirmation can still turn into confirmed.
const DROP_AFTER: Duration = Duration::from_secs(900);
const MAX_SUBMITTED: usize = 1024;
const MAX_SUBMITTED_PER_ADDRESS: usize = 16;
/// First check after submission, and the longest pause between checks of one operation.
const FIRST_CHECK: Duration = Duration::from_secs(5);
const MAX_PAUSE: Duration = Duration::from_secs(30);

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RecordKey {
    pub marketplace: String,
    pub kind: Kind,
    pub id: u64,
}

/// What an address's own pages show as done while its operations are pending or just confirmed.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Overlay {
    /// Listings and offers that are gone.
    pub records: HashSet<RecordKey>,
    /// NFTs that left the address.
    pub tokens: HashSet<Token>,
    /// Accepted collection offers: offer id → (remaining when reviewed, tokens taken).
    pub taken: HashMap<u64, (u64, u64)>,
}

impl Overlay {
    pub fn is_empty(&self) -> bool {
        self.records.is_empty() && self.tokens.is_empty() && self.taken.is_empty()
    }
    fn add(&mut self, other: &Overlay) {
        self.records.extend(other.records.iter().cloned());
        self.tokens.extend(other.tokens.iter().cloned());
        for (id, (remaining, n)) in &other.taken {
            let entry = self.taken.entry(*id).or_insert((*remaining, 0));
            entry.0 = entry.0.max(*remaining);
            entry.1 += n;
        }
    }
}

pub(super) enum Watch {
    Pending,
    /// Confirmed against its review; `evasion` keeps a fee-evasion verdict.
    Confirmed {
        at: Instant,
        evasion: bool,
    },
    /// A failed status on chain, or unknown to a reachable indexer long after submission.
    Failed(Instant),
    /// Not matched to its review, or never checkable: the index shows what happened.
    Unverified(Instant),
}

pub(super) struct Submitted {
    pub actor: String,
    pub hash: String,
    pub effects: Overlay,
    pub at: Instant,
    pub watch: Watch,
    next_check: Instant,
    pause: Duration,
}

impl Submitted {
    fn current(&self, now: Instant) -> bool {
        match self.watch {
            Watch::Pending => now.duration_since(self.at) < RECEIPT_TTL + MAX_PAUSE,
            Watch::Confirmed { at, .. } => now.duration_since(at) < OVERLAY_HOLD,
            Watch::Failed(at) | Watch::Unverified(at) => now.duration_since(at) < FAILED_KEEP,
        }
    }
}

/// Only these statuses prove failure; a missing, null or unknown one proves nothing.
fn failed_status(status: &serde_json::Value) -> bool {
    matches!(status.as_str(), Some("failed" | "backtracked" | "skipped"))
}

/// The records an operation group ends and the NFTs it moves away from its sender.
fn effects(plan: &Plan) -> Overlay {
    let mut effects = Overlay::default();
    for review in &plan.checked.reviews {
        match &review.action {
            Action::Cancel {
                marketplace,
                kind,
                id,
            } => {
                effects.records.insert(RecordKey {
                    marketplace: marketplace.clone(),
                    kind: *kind,
                    id: *id,
                });
            }
            Action::Accept { kind, id, token } => {
                effects.tokens.insert(token.clone());
                if *kind == Kind::Collection {
                    effects.taken.entry(*id).or_insert((review.remaining, 0)).1 += 1;
                } else {
                    effects.records.insert(RecordKey {
                        marketplace: V2.into(),
                        kind: *kind,
                        id: *id,
                    });
                }
            }
        }
    }
    // An accepted NFT that sat in a listing has that listing cancelled in the same group.
    for op in &plan.simulation.operations {
        if op.destination == V2 && op.parameters["entrypoint"] == "listing_cancel" {
            if let Some(id) = op.parameters["value"]["int"]
                .as_str()
                .and_then(|s| s.parse().ok())
            {
                effects.records.insert(RecordKey {
                    marketplace: V2.into(),
                    kind: Kind::Listing,
                    id,
                });
            }
        }
    }
    effects
}

impl TradingState {
    /// The page reports a submitted operation for a checked ticket: show its effects to the
    /// sender now and confirm it in the background.
    pub fn submitted(
        self: &Arc<Self>,
        actor: &str,
        session: &str,
        ticket: &str,
        hash: &str,
    ) -> Result<(), TradingError> {
        if !valid_actor(actor) || session.is_empty() {
            return Err(TradingError::Unauthorized);
        }
        if ticket.len() != 48 || !valid_operation_hash(hash) {
            return Err(TradingError::Invalid);
        }
        let now = Instant::now();
        let plan = {
            let receipts = self.receipts.lock().unwrap_or_else(|e| e.into_inner());
            let receipt = receipts
                .get(ticket)
                .filter(|r| r.expires > now.into_std())
                .ok_or(TradingError::Expired)?;
            if receipt.actor != actor {
                return Err(TradingError::Unauthorized);
            }
            if !receipt.ready {
                return Err(TradingError::Pending);
            }
            if receipt.confirmed_hash.as_ref().is_some_and(|h| h != hash) {
                return Err(TradingError::Changed);
            }
            receipt.plan.clone()
        };
        if self.record_submitted(actor, ticket, hash, effects(&plan))? {
            self.start_worker();
        }
        Ok(())
    }

    /// Records a validated submission's effects; `true` when it is new. [`Self::submitted`]
    /// validates against the checked receipt and starts the worker.
    pub fn record_submitted(
        &self,
        actor: &str,
        ticket: &str,
        hash: &str,
        effects: Overlay,
    ) -> Result<bool, TradingError> {
        let now = Instant::now();
        {
            let mut submitted = self.submitted.lock().unwrap_or_else(|e| e.into_inner());
            submitted.retain(|_, s| s.current(now));
            if let Some(existing) = submitted.get(ticket) {
                return if existing.hash == hash {
                    Ok(false)
                } else {
                    Err(TradingError::Changed)
                };
            }
            if submitted.len() >= MAX_SUBMITTED
                || submitted.values().filter(|s| s.actor == actor).count()
                    >= MAX_SUBMITTED_PER_ADDRESS
            {
                return Err(TradingError::Busy);
            }
            submitted.insert(
                ticket.into(),
                Submitted {
                    actor: actor.into(),
                    hash: hash.into(),
                    effects,
                    at: now,
                    watch: Watch::Pending,
                    next_check: now + FIRST_CHECK,
                    pause: FIRST_CHECK,
                },
            );
        }
        Ok(true)
    }

    /// Effects of `actor`'s pending and just-confirmed operations.
    pub fn overlay(&self, actor: &str) -> Overlay {
        let now = Instant::now();
        let mut overlay = Overlay::default();
        let submitted = self.submitted.lock().unwrap_or_else(|e| e.into_inner());
        for s in submitted.values().filter(|s| s.actor == actor) {
            let shown = match s.watch {
                Watch::Pending | Watch::Confirmed { .. } => s.current(now),
                Watch::Failed(_) | Watch::Unverified(_) => false,
            };
            if shown {
                overlay.add(&s.effects);
            }
        }
        overlay
    }

    /// The worker's verdict on `ticket` for its page, while it is current.
    pub(super) fn verdict(
        &self,
        actor: &str,
        ticket: &str,
        hash: &str,
    ) -> Option<Result<(), TradingError>> {
        let now = Instant::now();
        let submitted = self.submitted.lock().unwrap_or_else(|e| e.into_inner());
        let s = submitted
            .get(ticket)
            .filter(|s| s.hash == hash && s.actor == actor && s.current(now))?;
        match s.watch {
            Watch::Pending => None,
            Watch::Confirmed { evasion: false, .. } => Some(Ok(())),
            Watch::Confirmed { evasion: true, .. } => Some(Err(TradingError::FeeEvasion)),
            Watch::Failed(_) => Some(Err(TradingError::Failed)),
            Watch::Unverified(_) => Some(Err(TradingError::Unverified)),
        }
    }

    /// A later valid confirmation of an unverified submission.
    pub(super) fn settle_confirmed(&self, ticket: &str, evasion: bool) {
        if let Some(s) = self
            .submitted
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get_mut(ticket)
        {
            s.watch = Watch::Confirmed {
                at: Instant::now(),
                evasion,
            };
        }
    }

    fn start_worker(self: &Arc<Self>) {
        if !self.watcher.swap(true, Ordering::AcqRel) {
            let state = Arc::clone(self);
            tokio::spawn(async move { state.work().await });
        }
    }

    /// The pending operation due first: `Ok` to check now (its next check is booked), `Err` with
    /// the wait until the next one, `None` when nothing is pending.
    fn next_due(&self) -> Option<Result<(String, String, String, Instant), Duration>> {
        let now = Instant::now();
        let mut submitted = self.submitted.lock().unwrap_or_else(|e| e.into_inner());
        let (ticket, s) = submitted
            .iter_mut()
            .filter(|(_, s)| matches!(s.watch, Watch::Pending))
            .min_by_key(|(_, s)| s.next_check)?;
        if s.next_check > now {
            return Some(Err(s.next_check - now));
        }
        s.pause = (s.pause * 3 / 2).min(MAX_PAUSE);
        s.next_check = now + s.pause;
        Some(Ok((ticket.clone(), s.actor.clone(), s.hash.clone(), s.at)))
    }

    async fn work(self: Arc<Self>) {
        loop {
            match self.next_due() {
                Some(Ok((ticket, actor, hash, at))) => {
                    if let Some(watch) = self.check_submitted(&actor, &ticket, &hash, at).await {
                        if let Some(s) = self
                            .submitted
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .get_mut(&ticket)
                        {
                            s.watch = watch;
                        }
                    }
                }
                Some(Err(wait)) => tokio::time::sleep(wait.min(MAX_PAUSE)).await,
                None => {
                    self.watcher.store(false, Ordering::Release);
                    // A submission recorded meanwhile restarts the loop here or in its caller.
                    if self.next_due_pending() && !self.watcher.swap(true, Ordering::AcqRel) {
                        continue;
                    }
                    return;
                }
            }
        }
    }

    fn next_due_pending(&self) -> bool {
        self.submitted
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .any(|s| matches!(s.watch, Watch::Pending))
    }

    /// One check of a pending operation; `None` while it stays pending.
    async fn check_submitted(
        &self,
        actor: &str,
        ticket: &str,
        hash: &str,
        at: Instant,
    ) -> Option<Watch> {
        let now = Instant::now;
        let watch = match self.confirm_inner(actor, "watch", ticket, hash, None).await {
            Ok(()) => Watch::Confirmed {
                at: now(),
                evasion: false,
            },
            Err(TradingError::FeeEvasion) => Watch::Confirmed {
                at: now(),
                evasion: true,
            },
            Err(
                TradingError::Pending
                | TradingError::Unavailable
                | TradingError::Busy
                | TradingError::RateLimited,
            ) => {
                let age = now().duration_since(at);
                match self.final_failure(hash).await {
                    Ok(true) => Watch::Failed(now()),
                    // Absent for long, or still not final: not proof of failure.
                    _ if age > DROP_AFTER => Watch::Unverified(now()),
                    _ => return None,
                }
            }
            // Not the reviewed operation, or no receipt left to compare it with.
            Err(_) => Watch::Unverified(now()),
        };
        match watch {
            Watch::Failed(_) => eprintln!("trading_submitted_failed"),
            Watch::Unverified(_) => {
                eprintln!("trading_submitted_unverified");
                self.request_sync();
            }
            _ => {}
        }
        Some(watch)
    }

    /// Whether the group is in a final block of the canonical chain and did not apply: the only
    /// evidence that makes a submission failed.
    async fn final_failure(&self, hash: &str) -> Result<bool, TradingError> {
        let value = self
            .chain
            .indexer(format!("/operations/transactions/{hash}?limit=100"))
            .await?;
        let Some(row) = value.as_array().and_then(|rows| rows.first()) else {
            return Ok(false);
        };
        if !failed_status(&row["status"]) {
            return Ok(false);
        }
        let level = super::chain::number(&row["level"])?;
        if self.chain.head().await?.level < level.saturating_add(2) {
            return Ok(false);
        }
        let (_, contents) = self.canonical_contents(hash, level).await?;
        Ok(contents
            .iter()
            .any(|c| failed_status(&c["metadata"]["operation_result"]["status"])))
    }
}
