//! Bounded immutable bodies, allocation charges and size-aware admission.
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};

use super::{Content, ContentError, stats::LocalDeliveryMetrics};

const MAX_BYTES: usize = 256 << 20;
const MAX_ENTRIES: usize = 8192;

pub(super) struct Verified {
    state: Mutex<State>,
    max_bytes: usize,
    max_entries: usize,
}
impl Default for Verified {
    fn default() -> Self {
        Self::new(MAX_BYTES, MAX_ENTRIES)
    }
}
#[derive(Default)]
struct State {
    entries: HashMap<String, Entry>,
    bytes: usize,
    clock: u64,
}
struct Entry {
    content: Content,
    charge: usize,
    used: u64,
    touched: Option<Instant>,
    allocation: Arc<Allocation>,
    cache_keys: Vec<String>,
}
#[derive(Default)]
struct Allocation {
    leader: Arc<tokio::sync::Mutex<()>>,
    live: Mutex<Weak<BytePermit>>,
}
impl Verified {
    pub(super) fn contains(&self, key: &str) -> bool {
        self.state
            .lock()
            .is_ok_and(|state| state.entries.contains_key(key))
    }
    fn new(max_bytes: usize, max_entries: usize) -> Self {
        Self {
            state: Mutex::new(State::default()),
            max_bytes,
            max_entries,
        }
    }
    fn allocation(&self, key: &str) -> Option<(usize, Arc<Allocation>)> {
        let state = self.state.lock().ok()?;
        let entry = state.entries.get(key)?;
        Some((entry.content.body.len(), entry.allocation.clone()))
    }
    #[cfg(test)]
    pub async fn deliver(
        &self,
        key: &str,
        budget: &Delivery,
        deadline: tokio::time::Instant,
    ) -> Result<Option<Content>, ContentError> {
        self.deliver_budget(key, budget, &mut AdmissionBudget::until(deadline))
            .await
    }
    /// Waiters keep allocation metadata only; cloning bytes requires a live charge.
    pub async fn deliver_budget(
        &self,
        key: &str,
        budget: &Delivery,
        admission: &mut AdmissionBudget,
    ) -> Result<Option<Content>, ContentError> {
        admission.check()?;
        let Some((size, allocation)) = self.allocation(key) else {
            return Ok(None);
        };
        let live = allocation.live.lock().ok().and_then(|weak| weak.upgrade());
        let permit = if let Some(permit) = live {
            permit
        } else {
            let _leader = match allocation.leader.clone().try_lock_owned() {
                Ok(lock) => lock,
                Err(_) => {
                    budget
                        .wait_impl(admission, allocation.leader.clone().lock_owned(), true)
                        .await?
                }
            };
            let live = allocation.live.lock().ok().and_then(|weak| weak.upgrade());
            if let Some(permit) = live {
                permit
            } else {
                let permit = Arc::new(budget.reserve_budget(size, admission).await?);
                *allocation
                    .live
                    .lock()
                    .map_err(|_| busy("local allocation unavailable"))? = Arc::downgrade(&permit);
                permit
            }
        };
        let mut state = self
            .state
            .lock()
            .map_err(|_| busy("local cache unavailable"))?;
        state.clock = state.clock.saturating_add(1);
        let clock = state.clock;
        let Some(entry) = state.entries.get_mut(key) else {
            return Ok(None);
        };
        if !Arc::ptr_eq(&entry.allocation, &allocation) {
            return Ok(None);
        }
        entry.used = clock;
        let mut content = entry.content.clone();
        content.from_cache = true;
        content.delivery_accounted = true;
        permit.bind(&content.body)?;
        content.body = bytes::Bytes::from_owner(ChargedBytes {
            bytes: content.body,
            _permit: permit,
        });
        Ok(Some(content))
    }
    /// Refresh all probed path records at most once per second.
    pub fn touch_due(&self, key: &str) -> Option<Vec<String>> {
        let mut state = self.state.lock().ok()?;
        let entry = state.entries.get_mut(key)?;
        let now = Instant::now();
        if entry
            .touched
            .is_some_and(|last| now.duration_since(last) < Duration::from_secs(1))
        {
            return None;
        }
        entry.touched = Some(now);
        Some(entry.cache_keys.clone())
    }
    #[cfg(test)]
    pub fn get(&self, key: &str) -> Option<Content> {
        let mut state = self.state.lock().ok()?;
        state.clock = state.clock.saturating_add(1);
        let clock = state.clock;
        let entry = state.entries.get_mut(key)?;
        entry.used = clock;
        let mut content = entry.content.clone();
        content.from_cache = true;
        Some(content)
    }
    #[cfg(test)]
    pub(super) fn holding_lock(&self, work: impl FnOnce()) {
        let _lock = self.state.lock().unwrap();
        work();
    }
    #[cfg(test)]
    pub fn put(&self, key: String, content: &Content) {
        self.put_with_keys(key, content, Vec::new())
    }
    pub fn put_with_keys(&self, key: String, content: &Content, cache_keys: Vec<String>) {
        if !content.verification.is_verified() || key.len() > 8192 {
            return;
        }
        let charge = content
            .body
            .len()
            .saturating_add(key.len())
            .saturating_add(content.etag.len())
            .saturating_add(content.content_type.as_ref().map_or(0, String::len))
            .saturating_add(content.content_encoding.as_ref().map_or(0, String::len))
            .saturating_add(cache_keys.iter().map(String::len).sum::<usize>())
            .saturating_add(4096);
        if charge > self.max_bytes {
            return;
        }
        if self
            .state
            .lock()
            .is_ok_and(|state| state.entries.contains_key(&key))
        {
            return;
        }
        let mut retained = content.clone();
        retained.body = bytes::Bytes::copy_from_slice(&content.body);
        retained.delivery_accounted = false;
        let mut evicted = Vec::new();
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        if state.entries.contains_key(&key) {
            return;
        } // Immutable key; preserve concurrent allocation charges.
        if state.bytes + charge > self.max_bytes || state.entries.len() >= self.max_entries {
            let oldest = state
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.used)
                .map(|(key, entry)| (key.as_str(), entry.charge));
            if let Some((oldest_key, _)) = oldest.filter(|(_, oldest_charge)| {
                state.bytes - *oldest_charge + charge <= self.max_bytes
                    && state.entries.len() - 1 < self.max_entries
            }) {
                let oldest_key = oldest_key.to_owned();
                if let Some(old) = state.entries.remove(&oldest_key) {
                    state.bytes -= old.charge;
                    evicted.push(old);
                }
            } else {
                let mut oldest = state
                    .entries
                    .iter()
                    .map(|(key, entry)| (entry.used, key.as_str()))
                    .collect::<Vec<_>>();
                oldest.sort_by_key(|(used, _)| *used);
                let mut victims = Vec::new();
                let mut remaining_bytes = state.bytes;
                let mut remaining_entries = state.entries.len();
                for (_, oldest_key) in oldest {
                    if remaining_bytes + charge <= self.max_bytes
                        && remaining_entries < self.max_entries
                    {
                        break;
                    }
                    if let Some(entry) = state.entries.get(oldest_key) {
                        remaining_bytes -= entry.charge;
                        remaining_entries -= 1;
                        victims.push(oldest_key.to_owned());
                    }
                }
                for oldest_key in victims {
                    if let Some(old) = state.entries.remove(&oldest_key) {
                        state.bytes -= old.charge;
                        evicted.push(old);
                    }
                }
            }
        }
        state.clock = state.clock.saturating_add(1);
        let used = state.clock;
        state.bytes += charge;
        state.entries.insert(
            key,
            Entry {
                content: retained,
                charge,
                used,
                touched: None,
                allocation: Arc::new(Allocation::default()),
                cache_keys,
            },
        );
        drop(state);
        drop(evicted);
    }
}

/// Only admission waits spend this request budget; filesystem and copy work do not.
pub(super) struct AdmissionBudget {
    remaining: Duration,
}
const ADMISSION_EXHAUSTED: &str = "local admission wait budget exhausted";
impl AdmissionBudget {
    fn new(remaining: Duration) -> Self {
        Self { remaining }
    }
    #[cfg(test)]
    fn until(deadline: tokio::time::Instant) -> Self {
        Self::new(deadline.saturating_duration_since(tokio::time::Instant::now()))
    }
    pub(super) fn check(&self) -> Result<(), ContentError> {
        if self.remaining.is_zero() {
            Err(busy(ADMISSION_EXHAUSTED))
        } else {
            Ok(())
        }
    }
    pub(super) fn exhausted(error: &ContentError) -> bool {
        error.kind == super::ErrorKind::Unavailable && error.message == ADMISSION_EXHAUSTED
    }
}

struct ChargedBytes {
    bytes: bytes::Bytes,
    _permit: Arc<BytePermit>,
}
impl AsRef<[u8]> for ChargedBytes {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

/// A reservation is all-or-nothing: large waiters cannot absorb partial capacity.
pub(super) struct Delivery {
    state: Arc<BudgetState>,
    waiters: Arc<Semaphore>,
    units: usize,
    waiter_limit: usize,
    wait: Duration,
    waits: AtomicU64,
    timeouts: AtomicU64,
    rejected: AtomicU64,
    leader_waits: AtomicU64,
    leader_timeouts: AtomicU64,
    leader_rejected: AtomicU64,
}
struct BudgetState {
    usage: Mutex<Usage>,
    changed: Notify,
    deliveries: crate::delivery_control::Registry,
}
#[derive(Default)]
struct Usage {
    used: usize,
    next_id: u64,
    pending: BTreeMap<u64, (usize, tokio::time::Instant)>,
}
struct WaitingBytes {
    state: Arc<BudgetState>,
    id: u64,
}
impl WaitingBytes {
    fn new(state: Arc<BudgetState>, units: usize) -> Self {
        let id = {
            let mut usage = state.usage.lock().unwrap_or_else(|e| e.into_inner());
            usage.next_id = usage.next_id.wrapping_add(1);
            let id = usage.next_id;
            usage
                .pending
                .insert(id, (units, tokio::time::Instant::now()));
            id
        };
        Self { state, id }
    }
}
impl Drop for WaitingBytes {
    fn drop(&mut self) {
        if let Ok(mut usage) = self.state.usage.lock() {
            usage.pending.remove(&self.id);
        }
        self.state.changed.notify_waiters();
    }
}
#[derive(Debug)]
pub(super) struct BytePermit {
    state: Arc<BudgetState>,
    units: usize,
    allocation: crate::delivery_control::Allocation,
}
impl std::fmt::Debug for BudgetState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BudgetState").finish_non_exhaustive()
    }
}
impl BytePermit {
    pub(super) fn bind(self: &Arc<Self>, bytes: &bytes::Bytes) -> Result<(), ContentError> {
        let owner: Arc<dyn Send + Sync> = self.clone();
        self.allocation
            .bind_owned(bytes, Arc::downgrade(&owner))
            .map_err(|_| busy("local allocation unavailable"))
    }
    pub fn split(&mut self, units: usize) -> Option<Self> {
        if units > self.units {
            return None;
        }
        let allocation = self.state.deliveries.allocate(units * BYTE_UNIT)?;
        self.units -= units;
        Some(Self {
            state: self.state.clone(),
            units,
            allocation,
        })
    }
}
impl Drop for BytePermit {
    fn drop(&mut self) {
        if self.units == 0 {
            return;
        }
        if let Ok(mut usage) = self.state.usage.lock() {
            usage.used -= self.units;
        }
        self.state.changed.notify_waiters();
    }
}
pub(super) const BYTE_UNIT: usize = 4096;
impl Default for Delivery {
    fn default() -> Self {
        Self::new((512 << 20) / BYTE_UNIT, 256, Duration::from_secs(2))
    }
}
impl Delivery {
    pub(super) fn new(units: usize, waiter_limit: usize, wait: Duration) -> Self {
        Self {
            state: Arc::new(BudgetState {
                usage: Mutex::new(Usage::default()),
                changed: Notify::new(),
                deliveries: crate::delivery_control::Registry::default(),
            }),
            waiters: Arc::new(Semaphore::new(waiter_limit)),
            units,
            waiter_limit,
            wait,
            waits: AtomicU64::new(0),
            timeouts: AtomicU64::new(0),
            rejected: AtomicU64::new(0),
            leader_waits: AtomicU64::new(0),
            leader_timeouts: AtomicU64::new(0),
            leader_rejected: AtomicU64::new(0),
        }
    }
    pub(super) fn admission(&self) -> AdmissionBudget {
        AdmissionBudget::new(self.wait)
    }
    pub(super) fn track(&self, bytes: bytes::Bytes) -> Result<bytes::Bytes, ContentError> {
        self.state
            .deliveries
            .track(bytes)
            .map_err(|_| busy("local delivery registry full"))
    }
    #[cfg(test)]
    pub(super) fn deadline(&self) -> tokio::time::Instant {
        tokio::time::Instant::now() + self.wait
    }
    fn try_reserve(&self, units: usize, own: Option<u64>) -> Option<BytePermit> {
        let mut usage = self.state.usage.lock().ok()?;
        let free = self.units.saturating_sub(usage.used);
        if free < units {
            return None;
        }
        if let Some((&id, &(needed, since))) = usage.pending.first_key_value() {
            if own != Some(id)
                && since.elapsed() >= Duration::from_millis(250)
                && free.saturating_sub(units) < needed
            {
                return None;
            }
        }
        let allocation = self.state.deliveries.allocate(units * BYTE_UNIT)?;
        usage.used += units;
        Some(BytePermit {
            state: self.state.clone(),
            units,
            allocation,
        })
    }
    pub(super) fn try_reserve_bytes(&self, bytes: usize) -> Option<BytePermit> {
        self.try_reserve(bytes.div_ceil(BYTE_UNIT).max(1), None)
    }
    #[cfg(test)]
    pub(super) async fn reserve(
        &self,
        bytes: usize,
        deadline: tokio::time::Instant,
    ) -> Result<BytePermit, ContentError> {
        self.reserve_budget(bytes, &mut AdmissionBudget::until(deadline))
            .await
    }
    pub(super) async fn reserve_budget(
        &self,
        bytes: usize,
        admission: &mut AdmissionBudget,
    ) -> Result<BytePermit, ContentError> {
        admission.check()?;
        let units = bytes.div_ceil(BYTE_UNIT).max(1);
        if units > self.units {
            return Err(busy("local body exceeds byte budget"));
        }
        if let Some(permit) = self.try_reserve(units, None) {
            return Ok(permit);
        }
        let deadline = tokio::time::Instant::now() + admission.remaining;
        self.wait_impl(
            admission,
            async {
                let queued = WaitingBytes::new(self.state.clone(), units);
                let mut cancellations = 0;
                loop {
                    let changed = self.state.changed.notified();
                    tokio::pin!(changed);
                    changed.as_mut().enable();
                    if let Some(permit) = self.try_reserve(units, Some(queued.id)) {
                        return permit;
                    }
                    if tokio::time::Instant::now() < deadline
                        && cancellations < 4
                        && self.state.deliveries.reclaim()
                    {
                        cancellations += 1;
                    }
                    tokio::select! {
                        () = changed => {},
                        () = tokio::time::sleep(crate::delivery_control::PRESSURE_TICK) => {},
                    }
                }
            },
            false,
        )
        .await
    }
    async fn wait_impl<F: std::future::Future>(
        &self,
        admission: &mut AdmissionBudget,
        future: F,
        leader: bool,
    ) -> Result<F::Output, ContentError> {
        admission.check()?;
        let (waits, rejected, timeouts) = if leader {
            (
                &self.leader_waits,
                &self.leader_rejected,
                &self.leader_timeouts,
            )
        } else {
            (&self.waits, &self.rejected, &self.timeouts)
        };
        let _waiting = self.waiters.clone().try_acquire_owned().map_err(|_| {
            rejected.fetch_add(1, Ordering::Relaxed);
            busy("local admission queue full")
        })?;
        waits.fetch_add(1, Ordering::Relaxed);
        let started = tokio::time::Instant::now();
        let result = tokio::time::timeout(admission.remaining, future).await;
        admission.remaining = admission.remaining.saturating_sub(started.elapsed());
        match result {
            // Tokio may poll an immediately-ready admission after its timeout.
            // Drop that permit instead of starting work with an exhausted budget.
            Ok(value) => {
                admission.check()?;
                Ok(value)
            }
            Err(_) => {
                admission.remaining = Duration::ZERO;
                timeouts.fetch_add(1, Ordering::Relaxed);
                Err(busy("local admission deadline elapsed"))
            }
        }
    }
    pub(super) async fn acquire_budget(
        &self,
        semaphore: Arc<Semaphore>,
        units: u32,
        admission: &mut AdmissionBudget,
    ) -> Result<OwnedSemaphorePermit, ContentError> {
        admission.check()?;
        if let Ok(permit) = semaphore.clone().try_acquire_many_owned(units) {
            return Ok(permit);
        }
        self.wait_impl(admission, semaphore.acquire_many_owned(units), false)
            .await?
            .map_err(|_| busy("local delivery closed"))
    }
    pub(super) fn snapshot(&self) -> LocalDeliveryMetrics {
        let reclaim = self.state.deliveries.snapshot();
        LocalDeliveryMetrics {
            reclaim_scans: reclaim.scans,
            reclaimed: reclaim.reclaimed,
            reclaim_no_victim: reclaim.no_victim,
            bytes_used: (self
                .state
                .usage
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .used
                * BYTE_UNIT) as u64,
            bytes_quota: (self.units * BYTE_UNIT) as u64,
            waiting: (self.waiter_limit - self.waiters.available_permits()) as u64,
            wait_capacity: self.waiter_limit as u64,
            waits: self.waits.load(Ordering::Relaxed),
            timeouts: self.timeouts.load(Ordering::Relaxed),
            rejected: self.rejected.load(Ordering::Relaxed),
            leader_waits: self.leader_waits.load(Ordering::Relaxed),
            leader_timeouts: self.leader_timeouts.load(Ordering::Relaxed),
            leader_rejected: self.leader_rejected.load(Ordering::Relaxed),
        }
    }
}
fn busy(message: &str) -> ContentError {
    let mut error = ContentError::unavailable(message);
    error.retry_after = Some(Duration::from_secs(1));
    error
}

pub(super) fn cohort_busy(error: &ContentError) -> bool {
    error.kind == super::ErrorKind::Unavailable
        && matches!(
            error.message.as_str(),
            ADMISSION_EXHAUSTED
                | "local admission queue full"
                | "local admission deadline elapsed"
                | "local body exceeds byte budget"
        )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn pressure_cancels_real_transport_but_waits_for_the_last_frame() {
        use crate::{
            delivery_control::{CLIENT, Client},
            request_limits::hold_delivery,
            transport::tests::Transfer,
        };
        use axum::body::Body;
        use http_body_util::BodyExt;
        // Exceed Hyper's bounded coalescing buffer so a blocked transfer retains
        // the original allocation as well as any copied transport frames.
        let size = 1024 * 1024;
        let budget = Delivery::new(8 * size / BYTE_UNIT, 4, Duration::from_secs(2));
        let cache = Verified::default();
        let mut transfers = Vec::new();
        let mut held = None;
        for n in 0..8 {
            let key = n.to_string();
            let mut source = content(b"fixture");
            source.body = Bytes::from(vec![n; size]);
            cache.put(key.clone(), &source);
            let mut transfer = Transfer::new().await;
            let client = Client::new(
                format!("2001:db8:{}:{}::1", n / 2, n % 2).parse().unwrap(),
                transfer.control.clone(),
            );
            let served = cache
                .deliver(&key, &budget, budget.deadline())
                .await
                .unwrap()
                .unwrap();
            let bytes = CLIENT
                .scope(Some(client.clone()), async {
                    budget.track(served.body).unwrap()
                })
                .await;
            client.ready();
            if n == 0 {
                let mut body = hold_delivery(
                    Body::from(bytes.clone()),
                    Arc::new(()),
                    Some(transfer.control.clone()),
                );
                while let Some(frame) = body.frame().await {
                    held = Some(frame.unwrap());
                }
                drop(body);
            }
            transfer
                .start(hold_delivery(
                    Body::from(bytes),
                    Arc::new(()),
                    Some(transfer.control.clone()),
                ))
                .await;
            transfers.push(transfer);
            assert_eq!(
                budget.snapshot().bytes_used,
                ((n as usize + 1) * size) as u64,
                "body {n}"
            );
        }
        tokio::time::advance(Duration::from_secs(1)).await;
        transfers[7].control.wrote(64 * 1024);
        let mut ninth = Box::pin(budget.reserve(size, budget.deadline()));
        pending(&mut ninth).await;
        transfers[0].exited().await;
        assert_eq!(budget.snapshot().bytes_used, (8 * size) as u64);
        assert!(
            transfers[1..]
                .iter()
                .all(|transfer| transfer.slots.available_permits() == 0)
        );
        drop(held);
        let replacement = ninth.await.unwrap();
        assert_eq!(budget.snapshot().bytes_used, (8 * size) as u64);
        assert_eq!(budget.snapshot().reclaimed, 1);
        drop(replacement);
        assert!(transfers[1].drain().await >= size);
        assert!(transfers[7].drain().await >= size);
    }

    #[tokio::test(start_paused = true)]
    async fn progressing_shared_owners_prevent_reclamation_and_cancelled_waiters_clean_up() {
        use crate::delivery_control::{CLIENT, Client, Connection};
        let budget = Delivery::new(1, 1, Duration::from_secs(2));
        let cache = Verified::default();
        cache.put("shared".into(), &content(b"shared allocation"));
        let slow = Client::new("2001:db8:1:1::1".parse().unwrap(), Connection::new());
        let fast = Client::new("2001:db8:1:2::1".parse().unwrap(), Connection::new());
        slow.connection.enable_reclamation();
        fast.connection.enable_reclamation();
        let mut bodies = Vec::new();
        for client in [&slow, &fast] {
            let served = cache
                .deliver("shared", &budget, budget.deadline())
                .await
                .unwrap()
                .unwrap();
            bodies.push(
                CLIENT
                    .scope(Some(client.clone()), async {
                        budget.track(served.body).unwrap()
                    })
                    .await,
            );
            client.ready();
        }
        tokio::time::advance(Duration::from_secs(1)).await;
        fast.connection.wrote(64 * 1024);
        let mut waiting = Box::pin(budget.reserve(BYTE_UNIT, budget.deadline()));
        pending(&mut waiting).await;
        let cx = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(!slow.connection.poll_aborted(&cx));
        assert!(!fast.connection.poll_aborted(&cx));
        assert_eq!(budget.snapshot().bytes_used, BYTE_UNIT as u64);
        drop(waiting);
        assert_eq!(budget.snapshot().waiting, 0);
        assert!(budget.state.usage.lock().unwrap().pending.is_empty());
        drop(bodies.pop());
        tokio::time::advance(crate::delivery_control::PRESSURE_TICK).await;
        assert!(budget.state.deliveries.reclaim());
        assert!(slow.connection.poll_aborted(&cx));
        assert_eq!(budget.snapshot().bytes_used, BYTE_UNIT as u64);
        drop(bodies);
        assert_eq!(budget.snapshot().bytes_used, 0);
        drop(budget.reserve(BYTE_UNIT, budget.deadline()).await.unwrap());
    }

    #[tokio::test(start_paused = true)]
    async fn unreleased_victims_bound_cancellation_attempts_and_leave_all_bytes_charged() {
        use crate::delivery_control::{CLIENT, Client, Connection};
        let budget = Delivery::new(8, 1, Duration::from_secs(2));
        let cache = Verified::default();
        let mut clients = Vec::new();
        let mut bodies = Vec::new();
        for n in 0..8 {
            let key = n.to_string();
            cache.put(key.clone(), &content(b"one allocation"));
            let client = Client::new(
                format!("2001:db8:{n}::1").parse().unwrap(),
                Connection::new(),
            );
            client.connection.enable_reclamation();
            client.ready();
            let content = cache
                .deliver(&key, &budget, budget.deadline())
                .await
                .unwrap()
                .unwrap();
            bodies.push(
                CLIENT
                    .scope(Some(client.clone()), async {
                        budget.track(content.body).unwrap()
                    })
                    .await,
            );
            clients.push(client);
        }
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(budget.reserve(BYTE_UNIT, budget.deadline()).await.is_err());
        let cx = std::task::Context::from_waker(std::task::Waker::noop());
        assert_eq!(
            clients
                .iter()
                .filter(|client| client.connection.poll_aborted(&cx))
                .count(),
            4
        );
        assert_eq!(budget.snapshot().bytes_used, (8 * BYTE_UNIT) as u64);
        assert_eq!(budget.snapshot().waiting, 0);
        assert!(budget.state.usage.lock().unwrap().pending.is_empty());
        drop(bodies);
        assert_eq!(budget.snapshot().bytes_used, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn many_waiter_notifications_without_victims_scan_only_once_per_tick() {
        use crate::delivery_control::{CLIENT, Client, Connection};
        let budget = Delivery::new(1, 64, Duration::from_secs(2));
        let cache = Verified::default();
        cache.put("held".into(), &content(b"progressing holder"));
        let holder = Client::new("2001:db8:1:1::1".parse().unwrap(), Connection::new());
        holder.connection.enable_reclamation();
        let served = cache
            .deliver("held", &budget, budget.deadline())
            .await
            .unwrap()
            .unwrap();
        let held = CLIENT
            .scope(Some(holder.clone()), async {
                budget.track(served.body).unwrap()
            })
            .await;
        holder.ready();
        let mut waiters: Vec<_> = (0..64)
            .map(|_| Box::pin(budget.reserve(BYTE_UNIT, budget.deadline())))
            .collect();
        for tick in 1..=4 {
            holder.connection.wrote(64 * 1024);
            for _ in 0..10 {
                budget.state.changed.notify_waiters();
                for waiter in &mut waiters {
                    pending(waiter).await;
                }
            }
            assert_eq!(budget.snapshot().reclaim_scans, tick);
            assert_eq!(budget.snapshot().reclaim_no_victim, tick);
            assert_eq!(budget.snapshot().reclaimed, 0);
            tokio::time::advance(crate::delivery_control::PRESSURE_TICK).await;
        }
        drop(waiters);
        assert_eq!(budget.snapshot().waiting, 0);
        assert!(budget.state.usage.lock().unwrap().pending.is_empty());
        let cx = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(!holder.connection.poll_aborted(&cx));
        drop(held);
        assert_eq!(budget.snapshot().bytes_used, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn progressing_reader_projected_past_its_deadline_is_reclaimed_but_fast_reader_is_not() {
        use crate::delivery_control::{CLIENT, Client, Connection, PRESSURE_TICK};
        let size = 4 << 20;
        let budget = Delivery::new(2 * size / BYTE_UNIT, 1, Duration::from_secs(2));
        let cache = Verified::default();
        let mut clients = Vec::new();
        let mut bodies = Vec::new();
        for n in 0..2u8 {
            let key = n.to_string();
            let mut source = content(b"fixture");
            source.body = Bytes::from(vec![n; size]);
            cache.put(key.clone(), &source);
            let client = Client::new(
                format!("2001:db8:{n}::1").parse().unwrap(),
                Connection::new(),
            );
            client.connection.enable_reclamation();
            client
                .connection
                .set_deadline(tokio::time::Instant::now() + Duration::from_secs(20));
            let served = cache
                .deliver(&key, &budget, budget.deadline())
                .await
                .unwrap()
                .unwrap();
            bodies.push(
                CLIENT
                    .scope(Some(client.clone()), async {
                        budget.track(served.body).unwrap()
                    })
                    .await,
            );
            client.ready();
            clients.push(client);
        }
        for _ in 0..8 {
            tokio::time::advance(PRESSURE_TICK).await;
            // 96 KiB/s never stalls but needs about 40 s; 1 MiB/s needs about 2 s.
            clients[0].connection.wrote(24 << 10);
            clients[1].connection.wrote(256 << 10);
        }
        let mut waiting = Box::pin(budget.reserve(BYTE_UNIT, budget.deadline()));
        pending(&mut waiting).await;
        let cx = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(clients[0].connection.poll_aborted(&cx));
        assert!(!clients[1].connection.poll_aborted(&cx));
        assert_eq!(budget.snapshot().reclaimed, 1);
        assert_eq!(budget.snapshot().bytes_used, (2 * size) as u64);
        drop(bodies.remove(0));
        drop(waiting.await.unwrap());
        drop(bodies);
        assert_eq!(budget.snapshot().bytes_used, 0);
    }

    #[test]
    fn only_capacity_failures_are_shared_with_the_waiting_cohort() {
        for message in [
            ADMISSION_EXHAUSTED,
            "local admission queue full",
            "local admission deadline elapsed",
            "local body exceeds byte budget",
        ] {
            assert!(cohort_busy(&busy(message)));
        }
        for message in [
            "local delivery closed",
            "local allocation unavailable",
            "local cache unavailable",
            "cache i/o: failed",
            "local reader failed",
        ] {
            assert!(!cohort_busy(&busy(message)));
        }
        assert!(!cohort_busy(&ContentError::invalid(
            "local admission queue full"
        )));
    }
    use crate::content::Verification;
    use bytes::Bytes;

    fn content(body: &'static [u8]) -> Content {
        Content {
            delivery_accounted: false,
            body: Bytes::from_static(body),
            content_type: None,
            content_encoding: None,
            verification: Verification::IpfsCar,
            directory_index: false,
            from_cache: false,
            etag: "tag".into(),
        }
    }

    #[test]
    fn retains_only_verified_bytes_and_reads_share_the_same_allocation() {
        let cache = Verified::default();
        let mut unverified = content(b"unsafe");
        unverified.verification = Verification::Unverified;
        cache.put("bad".into(), &unverified);
        assert!(cache.get("bad").is_none());
        cache.put("good".into(), &content(b"safe"));
        let first = cache.get("good").unwrap();
        let next = cache.get("good").unwrap();
        assert!(first.from_cache);
        assert_eq!(first.body, "safe");
        assert_eq!(first.body.as_ptr(), next.body.as_ptr());
    }

    #[test]
    fn byte_and_entry_limits_evict_lru_without_invalidating_active_readers() {
        for cache in [Verified::new(8300, 99), Verified::new(100_000, 2)] {
            cache.put("one".into(), &content(b"first"));
            cache.put("two".into(), &content(b"second"));
            let held = cache.get("one").unwrap();
            cache.put("three".into(), &content(b"third"));
            assert!(cache.get("two").is_none());
            assert!(cache.get("one").is_some());
            cache.put("four".into(), &content(b"fourth"));
            assert_eq!(held.body, "first");
            let state = cache.state.lock().unwrap();
            assert!(state.bytes <= cache.max_bytes);
            assert!(state.entries.len() <= cache.max_entries);
        }
    }

    #[test]
    fn byte_limit_batch_eviction_keeps_the_newest_entry() {
        let cache = Verified::new(16_000, 4);
        for key in ["one", "two", "three"] {
            cache.put(key.into(), &content(b"x"));
        }
        assert!(cache.get("one").is_some());

        let mut large = content(b"");
        large.body = Bytes::from(vec![0; 6_000]);
        cache.put("large".into(), &large);

        assert!(cache.get("one").is_some());
        assert!(cache.get("two").is_none());
        assert!(cache.get("three").is_none());
        assert!(cache.get("large").is_some());
        let state = cache.state.lock().unwrap();
        assert!(state.bytes <= cache.max_bytes);
        assert!(state.entries.len() <= cache.max_entries);
    }

    #[test]
    fn retention_drops_larger_backing_allocations_and_their_permits() {
        struct Backing(std::sync::Arc<()>);
        impl AsRef<[u8]> for Backing {
            fn as_ref(&self) -> &[u8] {
                let _ = &self.0;
                b"long backing buffer"
            }
        }
        let backing = std::sync::Arc::new(());
        let mut value = content(b"");
        value.body = Bytes::from_owner(Backing(backing.clone())).slice(..4);
        let cache = Verified::default();
        cache.put("file".into(), &value);
        drop(value);
        assert_eq!(std::sync::Arc::strong_count(&backing), 1);
        assert_eq!(cache.get("file").unwrap().body, "long");
    }
    async fn pending<F: std::future::Future + Unpin>(future: &mut F) {
        std::future::poll_fn(|cx| {
            assert!(std::pin::Pin::new(&mut *future).poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
    }

    #[tokio::test]
    async fn bounded_delivery_waiters_resume_without_exceeding_bytes() {
        let delivery = Delivery::new(2, 2, Duration::from_secs(2));
        let first = delivery
            .reserve(2 * BYTE_UNIT, delivery.deadline())
            .await
            .unwrap();
        let mut visitor_a = Box::pin(delivery.reserve(BYTE_UNIT, delivery.deadline()));
        let mut visitor_b = Box::pin(delivery.reserve(2 * BYTE_UNIT, delivery.deadline()));
        pending(&mut visitor_a).await;
        pending(&mut visitor_b).await;
        assert_eq!(delivery.snapshot().waiting, 2);
        assert_eq!(delivery.snapshot().bytes_used, (2 * BYTE_UNIT) as u64);
        let error = delivery
            .reserve(BYTE_UNIT, delivery.deadline())
            .await
            .unwrap_err();
        assert_eq!(error.retry_after, Some(Duration::from_secs(1)));
        assert_eq!(delivery.snapshot().rejected, 1);
        drop(first);
        let a = visitor_a.await.unwrap();
        pending(&mut visitor_b).await;
        assert_eq!(delivery.snapshot().waiting, 1);
        drop(a);
        let b = visitor_b.await.unwrap();
        assert_eq!(delivery.snapshot().waiting, 0);
        drop(b);
        assert_eq!(delivery.snapshot().bytes_used, 0);
    }

    #[tokio::test]
    async fn cancelled_or_expired_delivery_waiters_release_slots_and_do_not_steal_bytes() {
        let delivery = Delivery::new(1, 1, Duration::from_secs(2));
        let held = delivery
            .reserve(BYTE_UNIT, delivery.deadline())
            .await
            .unwrap();
        let mut cancelled = Box::pin(delivery.reserve(BYTE_UNIT, delivery.deadline()));
        pending(&mut cancelled).await;
        assert_eq!(delivery.snapshot().waiting, 1);
        drop(cancelled);
        assert_eq!(delivery.snapshot().waiting, 0);
        let error = delivery
            .reserve(BYTE_UNIT, tokio::time::Instant::now())
            .await
            .unwrap_err();
        assert_eq!(error.kind, super::super::ErrorKind::Unavailable);
        assert!(AdmissionBudget::exhausted(&error));
        assert_eq!(delivery.snapshot().timeouts, 0);
        assert_eq!(delivery.snapshot().waiting, 0);
        drop(held);
        let recovered = delivery
            .reserve(BYTE_UNIT, delivery.deadline())
            .await
            .unwrap();
        assert_eq!(delivery.snapshot().bytes_used, BYTE_UNIT as u64);
        drop(recovered);
        assert_eq!(delivery.snapshot().bytes_used, 0);
    }

    #[tokio::test]
    async fn oversize_delivery_does_not_wait_or_consume_budget() {
        let delivery = Delivery::new(1, 1, Duration::from_secs(2));
        assert!(
            delivery
                .reserve(BYTE_UNIT + 1, delivery.deadline())
                .await
                .is_err()
        );
        assert_eq!(delivery.snapshot().bytes_used, 0);
        assert_eq!(delivery.snapshot().waiting, 0);
    }

    #[test]
    fn hot_memory_entries_request_at_most_one_disk_touch_per_second() {
        let cache = Verified::default();
        cache.put("key".into(), &content(b"body"));
        assert!(cache.touch_due("key").is_some());
        assert!(cache.touch_due("key").is_none());
        assert!(cache.touch_due("absent").is_none());
        cache
            .state
            .lock()
            .unwrap()
            .entries
            .get_mut("key")
            .unwrap()
            .touched = Some(Instant::now() - Duration::from_secs(2));
        assert!(cache.touch_due("key").is_some());
    }
    #[tokio::test]
    async fn large_waiter_cannot_absorb_free_bytes_a_small_request_can_use() {
        let delivery = Delivery::new(8, 4, Duration::from_secs(2));
        let held = delivery
            .reserve(6 * BYTE_UNIT, delivery.deadline())
            .await
            .unwrap();
        let mut large = Box::pin(delivery.reserve(4 * BYTE_UNIT, delivery.deadline()));
        pending(&mut large).await;
        let small = delivery
            .reserve(BYTE_UNIT, delivery.deadline())
            .await
            .unwrap();
        assert_eq!(delivery.snapshot().bytes_used, (7 * BYTE_UNIT) as u64);
        pending(&mut large).await;
        drop(small);
        drop(held);
        let large = large.await.unwrap();
        drop(large);
        assert_eq!(delivery.snapshot().bytes_used, 0);
    }

    #[tokio::test]
    async fn concurrent_deliveries_share_one_charge_until_the_last_clone_drops() {
        let delivery = Delivery::new(1, 4, Duration::from_secs(2));
        let cache = Verified::default();
        cache.put("shared".into(), &content(b"one allocation"));
        let mut readers = Vec::new();
        for _ in 0..8 {
            readers.push(
                cache
                    .deliver("shared", &delivery, delivery.deadline())
                    .await
                    .unwrap()
                    .unwrap(),
            );
        }
        assert_eq!(delivery.snapshot().bytes_used, BYTE_UNIT as u64);
        assert_eq!(delivery.snapshot().waits, 0);
        let ptr = readers[0].body.as_ptr();
        assert!(readers.iter().all(|reader| reader.body.as_ptr() == ptr));
        let last = readers[0].body.clone();
        drop(readers);
        assert_eq!(delivery.snapshot().bytes_used, BYTE_UNIT as u64);
        drop(last);
        assert_eq!(delivery.snapshot().bytes_used, 0);
        assert!(
            cache
                .deliver("shared", &delivery, delivery.deadline())
                .await
                .unwrap()
                .is_some()
        );
        assert_eq!(delivery.snapshot().bytes_used, 0);
    }

    #[tokio::test]
    async fn evicted_delivered_allocation_stays_charged_and_waiting_keys_hold_no_body() {
        let delivery = Delivery::new(1, 4, Duration::from_secs(2));
        let cache = Verified::new(100_000, 1);
        cache.put("first".into(), &content(b"first"));
        let held = cache
            .deliver("first", &delivery, delivery.deadline())
            .await
            .unwrap()
            .unwrap();
        cache.put("second".into(), &content(b"second"));
        assert!(cache.get("first").is_none());
        assert_eq!(delivery.snapshot().bytes_used, BYTE_UNIT as u64);
        let mut waiting = Box::pin(cache.deliver("second", &delivery, delivery.deadline()));
        pending(&mut waiting).await;
        cache.put("third".into(), &content(b"third"));
        drop(held);
        assert!(waiting.await.unwrap().is_none());
        assert_eq!(delivery.snapshot().bytes_used, 0);
        assert!(
            cache
                .deliver("third", &delivery, delivery.deadline())
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn cancelled_allocation_leader_releases_both_waiter_and_follower() {
        let delivery = Delivery::new(1, 4, Duration::from_secs(2));
        let cache = Verified::default();
        cache.put("shared".into(), &content(b"body"));
        let held = delivery
            .reserve(BYTE_UNIT, delivery.deadline())
            .await
            .unwrap();
        let mut leader = Box::pin(cache.deliver("shared", &delivery, delivery.deadline()));
        let mut follower = Box::pin(cache.deliver("shared", &delivery, delivery.deadline()));
        pending(&mut leader).await;
        pending(&mut follower).await;
        assert_eq!(delivery.snapshot().waiting, 2);
        assert_eq!(delivery.snapshot().leader_waits, 1);
        drop(leader);
        drop(held);
        let recovered = follower.await.unwrap().unwrap();
        assert_eq!(delivery.snapshot().waiting, 0);
        assert_eq!(recovered.body, "body");
        drop(recovered);
        assert_eq!(delivery.snapshot().bytes_used, 0);
    }
    #[tokio::test]
    async fn aged_large_waiter_gets_headroom_and_cancellation_restores_small_admission() {
        let budget = Delivery::new(8, 4, Duration::from_secs(2));
        let held = budget
            .reserve(6 * BYTE_UNIT, budget.deadline())
            .await
            .unwrap();
        let mut large = Box::pin(budget.reserve(4 * BYTE_UNIT, budget.deadline()));
        pending(&mut large).await;
        let young = budget.reserve(BYTE_UNIT, budget.deadline()).await.unwrap();
        drop(young);
        budget
            .state
            .usage
            .lock()
            .unwrap()
            .pending
            .first_entry()
            .unwrap()
            .get_mut()
            .1 = tokio::time::Instant::now() - Duration::from_millis(251);
        let mut small = Box::pin(budget.reserve(BYTE_UNIT, budget.deadline()));
        pending(&mut small).await;
        drop(large);
        let small = small.await.unwrap();
        drop(small);
        drop(held);
        assert_eq!(budget.snapshot().bytes_used, 0);
        assert!(budget.state.usage.lock().unwrap().pending.is_empty());
    }

    #[tokio::test]
    async fn aged_large_waiter_succeeds_first_while_already_shared_hits_stay_free() {
        let budget = Delivery::new(8, 4, Duration::from_secs(2));
        let cache = Verified::default();
        cache.put("hot".into(), &content(b"hot"));
        let hot = cache
            .deliver("hot", &budget, budget.deadline())
            .await
            .unwrap()
            .unwrap();
        let held = budget
            .reserve(6 * BYTE_UNIT, budget.deadline())
            .await
            .unwrap();
        let mut large = Box::pin(budget.reserve(4 * BYTE_UNIT, budget.deadline()));
        pending(&mut large).await;
        budget
            .state
            .usage
            .lock()
            .unwrap()
            .pending
            .first_entry()
            .unwrap()
            .get_mut()
            .1 = tokio::time::Instant::now() - Duration::from_millis(251);
        let another = cache
            .deliver("hot", &budget, budget.deadline())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(hot.body.as_ptr(), another.body.as_ptr());
        assert_eq!(budget.snapshot().bytes_used, 7 * BYTE_UNIT as u64);
        drop(held);
        let large = large.await.unwrap();
        drop((large, hot, another));
        assert_eq!(budget.snapshot().bytes_used, 0);
    }
    #[tokio::test]
    async fn shared_admission_budget_counts_only_waits_and_rejects_late_work() {
        let readers = Delivery::new(0, 2, Duration::from_secs(2));
        let scratch = Delivery::new(1, 2, Duration::from_secs(2));
        let delivery = Delivery::new(1, 2, Duration::from_secs(2));
        let mut admission = AdmissionBudget::new(Duration::from_millis(100));
        readers
            .wait_impl(
                &mut admission,
                tokio::time::sleep(Duration::from_millis(10)),
                false,
            )
            .await
            .unwrap();
        let remaining = admission.remaining;
        assert!(remaining < Duration::from_millis(100));
        assert!(!remaining.is_zero());
        // Work performed between admission stages spends none of the wait budget.
        tokio::time::sleep(Duration::from_millis(110)).await;
        assert_eq!(admission.remaining, remaining);
        assert!(
            scratch
                .wait_impl(&mut admission, std::future::pending::<()>(), false)
                .await
                .is_err()
        );
        assert_eq!(admission.remaining, Duration::ZERO);
        assert_eq!(readers.snapshot().waits, 1);
        assert_eq!(readers.snapshot().timeouts, 0);
        assert_eq!(scratch.snapshot().waits, 1);
        assert_eq!(scratch.snapshot().timeouts, 1);
        let polled = std::sync::atomic::AtomicBool::new(false);
        let error = delivery
            .wait_impl(
                &mut admission,
                async {
                    polled.store(true, Ordering::Relaxed);
                },
                false,
            )
            .await
            .unwrap_err();
        assert!(AdmissionBudget::exhausted(&error));
        assert!(!polled.load(Ordering::Relaxed));
        assert_eq!(delivery.snapshot().waits, 0);
        assert_eq!(delivery.snapshot().timeouts, 0);
        assert_eq!(delivery.snapshot().rejected, 0);
        assert!(delivery.reserve_budget(1, &mut admission).await.is_err());
        assert_eq!(delivery.snapshot().bytes_used, 0);
    }

    #[tokio::test]
    async fn ready_admission_polled_after_budget_exhaustion_drops_its_permit() {
        let delivery = Delivery::new(1, 2, Duration::from_secs(2));
        let semaphore = Arc::new(Semaphore::new(1));
        let mut admission = AdmissionBudget::new(Duration::from_millis(1));
        let error = delivery
            .wait_impl(
                &mut admission,
                async {
                    let permit = semaphore.clone().acquire_owned().await.unwrap();
                    std::thread::sleep(Duration::from_millis(10));
                    permit
                },
                false,
            )
            .await
            .unwrap_err();
        assert!(AdmissionBudget::exhausted(&error));
        assert_eq!(semaphore.available_permits(), 1);
        assert_eq!(delivery.snapshot().timeouts, 0);
        assert_eq!(delivery.snapshot().waiting, 0);
    }

    #[tokio::test]
    async fn leader_wait_rejection_and_timeout_have_separate_counters_and_recover() {
        let delivery = Delivery::new(1, 1, Duration::from_secs(2));
        let cache = Verified::default();
        cache.put("shared".into(), &content(b"body"));
        let (_, allocation) = cache.allocation("shared").unwrap();
        let held = allocation.leader.clone().lock_owned().await;
        let mut waiting = Box::pin(cache.deliver(
            "shared",
            &delivery,
            tokio::time::Instant::now() + Duration::from_millis(20),
        ));
        pending(&mut waiting).await;
        assert_eq!(delivery.snapshot().leader_waits, 1);
        assert!(
            cache
                .deliver("shared", &delivery, delivery.deadline())
                .await
                .is_err()
        );
        assert_eq!(delivery.snapshot().leader_rejected, 1);
        assert!(waiting.await.is_err());
        assert_eq!(delivery.snapshot().leader_timeouts, 1);
        assert_eq!(delivery.snapshot().waiting, 0);
        assert_eq!(delivery.snapshot().waits, 0);
        assert_eq!(delivery.snapshot().timeouts, 0);
        assert_eq!(delivery.snapshot().rejected, 0);
        drop(held);
        let recovered = cache
            .deliver("shared", &delivery, delivery.deadline())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(recovered.body, "body");
        drop(recovered);
        assert_eq!(delivery.snapshot().bytes_used, 0);
    }

    #[test]
    fn evicted_body_is_dropped_after_releasing_the_cache_mutex() {
        struct DropCheck {
            cache: Weak<Verified>,
            unlocked: Arc<std::sync::atomic::AtomicBool>,
        }
        impl AsRef<[u8]> for DropCheck {
            fn as_ref(&self) -> &[u8] {
                b"first"
            }
        }
        impl Drop for DropCheck {
            fn drop(&mut self) {
                let cache = self.cache.upgrade().unwrap();
                self.unlocked
                    .store(cache.state.try_lock().is_ok(), Ordering::Relaxed);
            }
        }
        let cache = Arc::new(Verified::new(100_000, 1));
        cache.put("first".into(), &content(b"first"));
        let unlocked = Arc::new(std::sync::atomic::AtomicBool::new(false));
        cache
            .state
            .lock()
            .unwrap()
            .entries
            .get_mut("first")
            .unwrap()
            .content
            .body = Bytes::from_owner(DropCheck {
            cache: Arc::downgrade(&cache),
            unlocked: unlocked.clone(),
        });
        cache.put("second".into(), &content(b"second"));
        assert!(unlocked.load(Ordering::Relaxed));
        assert_eq!(cache.get("second").unwrap().body, "second");
    }
}
