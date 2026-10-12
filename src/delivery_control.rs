//! Cancellation metadata owns no body bytes and never refunds allocation charges.
use std::{
    collections::{HashMap, HashSet},
    net::{IpAddr, Ipv6Addr},
    sync::{Arc, Mutex, Weak},
    task::{Context, Poll, Waker},
    time::Duration,
};

use bytes::Bytes;
use tokio::time::Instant;

const MAX_RECORDS: usize = 8192;
const MAX_ALLOCATIONS: usize = 2 * MAX_RECORDS;
pub(crate) const PRESSURE_TICK: Duration = Duration::from_millis(250);
const STALLED: Duration = Duration::from_secs(1);
const MAX_VICTIMS: usize = 8;
const RATE_WINDOW: Duration = Duration::from_secs(5);
// One bucket per tick in the window, plus the partial current bucket.
const RATE_BUCKETS: usize =
    RATE_WINDOW.as_millis().div_ceil(PRESSURE_TICK.as_millis()) as usize + 1;

#[derive(Clone, Copy, Default)]
struct RateBucket {
    epoch: u64,
    bytes: u64,
}

#[derive(Clone, Copy)]
struct Demand {
    since: Instant,
    initial_written: u64,
    bytes: u64,
}

#[derive(Clone)]
pub(crate) struct Connection(Arc<Mutex<ConnectionState>>);
struct ConnectionState {
    aborted: bool,
    multiplexed: bool,
    reclaimable: bool,
    waker: Option<Waker>,
    progress: Instant,
    written: usize,
    accepted: Instant,
    deadline: Option<Instant>,
    total_written: u64,
    rate: [RateBucket; RATE_BUCKETS],
}
impl Connection {
    pub(crate) fn new() -> Self {
        Self(Arc::new(Mutex::new(ConnectionState {
            aborted: false,
            multiplexed: false,
            reclaimable: false,
            waker: None,
            progress: Instant::now(),
            written: 0,
            accepted: Instant::now(),
            deadline: None,
            total_written: 0,
            rate: [RateBucket::default(); RATE_BUCKETS],
        })))
    }
    pub(crate) fn poll_aborted(&self, cx: &Context<'_>) -> bool {
        let mut state = self.0.lock().unwrap_or_else(|e| e.into_inner());
        state.waker = Some(cx.waker().clone());
        state.aborted
    }
    #[cfg(test)]
    pub(crate) fn wrote(&self, bytes: usize) {
        let mut state = self.0.lock().unwrap_or_else(|e| e.into_inner());
        state.wrote(bytes);
    }
    pub(crate) fn write(
        &self,
        write: impl FnOnce() -> Poll<std::io::Result<usize>>,
    ) -> Poll<std::io::Result<usize>> {
        let mut state = self.0.lock().unwrap_or_else(|e| e.into_inner());
        if state.aborted {
            return Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionAborted,
                "stalled delivery reclaimed",
            )));
        }
        let result = write();
        if let Poll::Ready(Ok(bytes)) = result {
            state.wrote(bytes);
        }
        result
    }
    pub(crate) fn protect_multiplexed(&self) {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).multiplexed = true;
    }
    pub(crate) fn enable_reclamation(&self) {
        let mut state = self.0.lock().unwrap_or_else(|e| e.into_inner());
        state.reclaimable = true;
        let upper =
            state.accepted + Duration::from_secs(crate::transport::PROXY_WRITE_TIMEOUT_SECONDS);
        state.deadline = Some(state.deadline.map_or(upper, |deadline| deadline.min(upper)));
    }
    pub(crate) fn set_deadline(&self, deadline: Instant) {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).deadline = Some(deadline);
    }
    fn reclaimable(&self, demand: Demand, now: Instant) -> bool {
        self.0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .reclaimable(demand, now)
    }
    #[cfg(test)]
    fn stalled(&self, since: Instant, now: Instant) -> bool {
        self.reclaimable(
            Demand {
                since,
                initial_written: 0,
                bytes: 1 << 20,
            },
            now,
        )
    }
    #[cfg(test)]
    fn abort_stalled(&self, since: Instant, now: Instant) -> bool {
        let demand = Demand {
            since,
            initial_written: 0,
            bytes: 1 << 20,
        };
        let Some((_, wakers)) = abort_group(&[(self.clone(), demand)], now) else {
            return false;
        };
        for waker in wakers {
            waker.wake();
        }
        true
    }
}
impl ConnectionState {
    fn wrote(&mut self, bytes: usize) {
        let now = Instant::now();
        self.total_written = self.total_written.saturating_add(bytes as u64);
        let epoch = self.epoch(now);
        let bucket = &mut self.rate[epoch as usize % RATE_BUCKETS];
        if bucket.epoch != epoch {
            *bucket = RateBucket { epoch, bytes: 0 };
        }
        bucket.bytes = bucket.bytes.saturating_add(bytes as u64);
        self.written = self.written.saturating_add(bytes);
        if self.written >= 64 * 1024 {
            self.written = 0;
            self.progress = now;
        }
    }
    fn epoch(&self, now: Instant) -> u64 {
        u64::try_from(
            now.saturating_duration_since(self.accepted).as_millis() / PRESSURE_TICK.as_millis(),
        )
        .unwrap_or(u64::MAX)
    }
    fn reclaimable(&self, demand: Demand, now: Instant) -> bool {
        if !self.reclaimable || self.aborted || self.multiplexed {
            return false;
        }
        // Transport bytes include headers, so a stall never depends on `remaining`.
        if now.saturating_duration_since(self.progress.max(demand.since)) >= STALLED {
            return true;
        }
        let remaining = demand
            .bytes
            .saturating_sub(self.total_written.saturating_sub(demand.initial_written));
        let Some(deadline) = self.deadline else {
            return false;
        };
        if remaining == 0 || now.saturating_duration_since(demand.since) < STALLED {
            return false;
        }
        // Rate over the last five seconds, or the whole demand when younger, so a
        // short dip does not cancel a healthy download. The oldest counted bucket
        // starts at or before `start`, so the rate is never underestimated.
        let start = now
            .checked_sub(RATE_WINDOW)
            .map_or(demand.since, |start| start.max(demand.since))
            .max(self.accepted);
        let first = self.epoch(start);
        let bytes = self
            .rate
            .iter()
            .filter(|bucket| bucket.epoch >= first)
            .fold(0u64, |sum, bucket| sum.saturating_add(bucket.bytes));
        u128::from(remaining) * now.saturating_duration_since(start).as_nanos()
            > u128::from(bytes) * deadline.saturating_duration_since(now).as_nanos()
    }
}

/// Lock every owner in pointer order: new progress either precedes all
/// cancellations or follows them, including when only a bounded batch is aborted.
fn abort_group(owners: &[(Connection, Demand)], now: Instant) -> Option<(usize, Vec<Waker>)> {
    let mut owners = owners.to_vec();
    owners.sort_by_key(|(connection, _)| Arc::as_ptr(&connection.0) as usize);
    let mut states: Vec<_> = owners
        .iter()
        .map(|(connection, _)| connection.0.lock().unwrap_or_else(|e| e.into_inner()))
        .collect();
    if states
        .iter()
        .zip(&owners)
        .any(|(state, (_, demand))| !state.reclaimable(*demand, now))
    {
        return None;
    }
    Some((
        states.len().min(MAX_VICTIMS),
        states
            .iter_mut()
            .take(MAX_VICTIMS)
            .filter_map(|state| {
                state.aborted = true;
                state.waker.take()
            })
            .collect(),
    ))
}

#[derive(Clone)]
pub(crate) struct Client {
    pub(crate) ip: IpAddr,
    pub(crate) connection: Connection,
    ready: Arc<Mutex<Option<(Instant, u64)>>>,
}
impl Client {
    pub(crate) fn new(ip: IpAddr, connection: Connection) -> Self {
        Self {
            ip,
            connection,
            ready: Arc::new(Mutex::new(None)),
        }
    }
    pub(crate) fn ready(&self) {
        let written = self
            .connection
            .0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .total_written;
        *self.ready.lock().unwrap_or_else(|e| e.into_inner()) = Some((Instant::now(), written));
    }
    fn demand(&self, registered: Instant, bytes: usize) -> Option<Demand> {
        self.ready.lock().ok()?.map(|(ready, written)| Demand {
            since: ready.max(registered),
            initial_written: written,
            bytes: bytes as u64,
        })
    }
}
tokio::task_local! {
    pub(crate) static CLIENT: Option<Client>;
}

#[derive(Clone, Default)]
pub(crate) struct Registry(Arc<Mutex<State>>);
#[derive(Default)]
struct State {
    next: u64,
    allocations: HashMap<u64, Record>,
    pointers: HashMap<usize, u64>,
    streams: HashMap<u64, Stream>,
    last_scan: Option<Instant>,
    counters: ReclaimCounters,
}
#[derive(Clone, Copy, Default)]
pub(crate) struct ReclaimCounters {
    pub(crate) scans: u64,
    pub(crate) reclaimed: u64,
    pub(crate) no_victim: u64,
}
struct Record {
    bytes: usize,
    body_bytes: usize,
    pointer: Option<usize>,
    owner: Option<Weak<dyn Send + Sync>>,
}
struct Stream {
    allocation: u64,
    client: Client,
    since: Instant,
}
pub(crate) struct Allocation {
    registry: Registry,
    id: u64,
}
pub(crate) struct Registration {
    registry: Weak<Mutex<State>>,
    id: u64,
}
impl std::fmt::Debug for Allocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Allocation").field("id", &self.id).finish()
    }
}
impl Registry {
    pub(crate) fn snapshot(&self) -> ReclaimCounters {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).counters
    }
    pub(crate) fn allocate(&self, bytes: usize) -> Option<Allocation> {
        let mut state = self.0.lock().ok()?;
        if state.allocations.len() >= MAX_ALLOCATIONS {
            return None;
        }
        state.next = state.next.checked_add(1)?;
        let id = state.next;
        state.allocations.insert(
            id,
            Record {
                bytes,
                body_bytes: bytes,
                pointer: None,
                owner: None,
            },
        );
        Some(Allocation {
            registry: self.clone(),
            id,
        })
    }
    pub(crate) fn track(&self, bytes: Bytes) -> Result<Bytes, ()> {
        let Some(client) = CLIENT.try_with(Clone::clone).ok().flatten() else {
            return Ok(bytes);
        };
        if bytes.is_empty() {
            return Ok(bytes);
        }
        let mut state = self.0.lock().map_err(|_| ())?;
        let id = *state.pointers.get(&(bytes.as_ptr() as usize)).ok_or(())?;
        let registration = state.register(id, client).ok_or(())?;
        Ok(Bytes::from_owner(TrackedBytes {
            bytes,
            _registration: Registration {
                registry: Arc::downgrade(&self.0),
                id: registration,
            },
        }))
    }
    /// Cancel allocations only when every owner is stalled or projected late.
    /// Each family/prefix counts a shared allocation once, regardless of streams.
    pub(crate) fn reclaim(&self) -> bool {
        let now = Instant::now();
        let Ok(mut state) = self.0.lock() else {
            return false;
        };
        // Unregistered bytes, including scratch, have no connection to cancel.
        if state.streams.is_empty()
            || state
                .last_scan
                .is_some_and(|last| now.duration_since(last) < PRESSURE_TICK)
        {
            return false;
        }
        state.last_scan = Some(now);
        state.counters.scans = state.counters.scans.saturating_add(1);
        let mut owners: HashMap<u64, Vec<&Stream>> = HashMap::new();
        let mut family_bytes: HashMap<IpAddr, usize> = HashMap::new();
        let mut prefix_bytes: HashMap<IpAddr, usize> = HashMap::new();
        let mut seen_family = HashSet::new();
        let mut seen_prefix = HashSet::new();
        for stream in state.streams.values() {
            let Some(record) = state.allocations.get(&stream.allocation) else {
                continue;
            };
            if record
                .owner
                .as_ref()
                .is_some_and(|owner| owner.strong_count() == 0)
            {
                continue;
            }
            owners.entry(stream.allocation).or_default().push(stream);
            let prefix = crate::client_ip::rate_ip(stream.client.ip);
            let family = family(prefix);
            if seen_family.insert((family, stream.allocation)) {
                *family_bytes.entry(family).or_default() += record.bytes;
            }
            if seen_prefix.insert((prefix, stream.allocation)) {
                *prefix_bytes.entry(prefix).or_default() += record.bytes;
            }
        }
        let victim = owners
            .iter()
            .filter(|(id, streams)| {
                let bytes = state.allocations[id].body_bytes;
                streams.iter().all(|stream| {
                    stream
                        .client
                        .demand(stream.since, bytes)
                        .is_some_and(|demand| stream.client.connection.reclaimable(demand, now))
                })
            })
            .max_by_key(|(id, streams)| {
                let weight = streams
                    .iter()
                    .map(|stream| {
                        let prefix = crate::client_ip::rate_ip(stream.client.ip);
                        (family_bytes[&family(prefix)], prefix_bytes[&prefix])
                    })
                    .max()
                    .unwrap_or_default();
                (weight, state.allocations[id].bytes, std::cmp::Reverse(**id))
            })
            .map(|(id, streams)| {
                let bytes = state.allocations[id].body_bytes;
                let mut owners = HashMap::new();
                for stream in streams {
                    let demand = stream.client.demand(stream.since, bytes).unwrap();
                    let owner = owners
                        .entry(Arc::as_ptr(&stream.client.connection.0) as usize)
                        .or_insert_with(|| (stream.client.connection.clone(), demand));
                    owner.1.since = owner.1.since.max(demand.since);
                    owner.1.initial_written = owner.1.initial_written.max(demand.initial_written);
                }
                owners.into_values().collect::<Vec<_>>()
            });
        let Some(victim) = victim else {
            state.counters.no_victim = state.counters.no_victim.saturating_add(1);
            return false;
        };
        let Some((count, wakers)) = abort_group(&victim, now) else {
            state.counters.no_victim = state.counters.no_victim.saturating_add(1);
            return false;
        };
        state.counters.reclaimed = state.counters.reclaimed.saturating_add(count as u64);
        drop(state);
        for waker in wakers {
            waker.wake();
        }
        true
    }
}
fn family(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(ip) => Ipv6Addr::from(u128::from(ip) & (u128::MAX << 80)).into(),
        ip => ip,
    }
}
impl State {
    fn register(&mut self, allocation: u64, client: Client) -> Option<u64> {
        if self.streams.len() >= MAX_RECORDS || !self.allocations.contains_key(&allocation) {
            return None;
        }
        self.next = self.next.checked_add(1)?;
        let id = self.next;
        self.streams.insert(
            id,
            Stream {
                allocation,
                client,
                since: Instant::now(),
            },
        );
        Some(id)
    }
}
impl Allocation {
    #[cfg(test)]
    pub(crate) fn bind(&self, bytes: &Bytes) -> Result<(), ()> {
        self.bind_impl(bytes, None)
    }
    pub(crate) fn bind_owned(&self, bytes: &Bytes, owner: Weak<dyn Send + Sync>) -> Result<(), ()> {
        self.bind_impl(bytes, Some(owner))
    }
    fn bind_impl(&self, bytes: &Bytes, owner: Option<Weak<dyn Send + Sync>>) -> Result<(), ()> {
        if bytes.is_empty() {
            return Ok(());
        }
        let mut state = self.registry.0.lock().map_err(|_| ())?;
        let pointer = bytes.as_ptr() as usize;
        if let Some(previous) = state.pointers.get(&pointer).filter(|id| **id != self.id) {
            // Arc reaches zero before its destructor retires metadata. Do not
            // upgrade here: dropping a final temporary Arc would relock this map.
            let retired = state
                .allocations
                .get(previous)
                .and_then(|record| record.owner.as_ref())
                .is_some_and(|owner| owner.strong_count() == 0);
            if !retired {
                return Err(());
            }
        }
        let record = state.allocations.get_mut(&self.id).ok_or(())?;
        if record.pointer.is_some_and(|previous| previous != pointer) {
            return Err(());
        }
        record.pointer = Some(pointer);
        record.body_bytes = bytes.len();
        record.owner = owner;
        state.pointers.insert(pointer, self.id);
        Ok(())
    }
    pub(crate) fn register(&self, client: Client) -> Result<Registration, ()> {
        let id = self
            .registry
            .0
            .lock()
            .map_err(|_| ())?
            .register(self.id, client)
            .ok_or(())?;
        Ok(Registration {
            registry: Arc::downgrade(&self.registry.0),
            id,
        })
    }
}
impl Drop for Allocation {
    fn drop(&mut self) {
        if let Ok(mut state) = self.registry.0.lock() {
            if let Some(record) = state.allocations.remove(&self.id) {
                if let Some(pointer) = record.pointer {
                    if state.pointers.get(&pointer) == Some(&self.id) {
                        state.pointers.remove(&pointer);
                    }
                }
            }
        }
    }
}
impl Drop for Registration {
    fn drop(&mut self) {
        if let Some(registry) = self.registry.upgrade() {
            if let Ok(mut state) = registry.lock() {
                state.streams.remove(&self.id);
            }
        }
    }
}
struct TrackedBytes {
    bytes: Bytes,
    _registration: Registration,
}
impl AsRef<[u8]> for TrackedBytes {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn client(ip: &str) -> Client {
        let client = Client::new(ip.parse().unwrap(), Connection::new());
        client.connection.enable_reclamation();
        client.ready();
        client
    }
    fn aborted(client: &Client) -> bool {
        client
            .connection
            .poll_aborted(&Context::from_waker(Waker::noop()))
    }
    #[tokio::test(start_paused = true)]
    async fn heaviest_family_then_prefix_is_selected_without_blocking_its_neighbours() {
        let registry = Registry::default();
        let clients = [
            client("2001:db8:1:1::1"),
            client("2001:db8:1:2::1"),
            client("2001:db8:2:1::1"),
            client("2001:db8:1:3::1"),
        ];
        let mut allocations = Vec::new();
        let mut streams = Vec::new();
        for (client, bytes) in clients.iter().zip([200, 100, 250, 1]) {
            let allocation = registry.allocate(bytes).unwrap();
            streams.push(allocation.register(client.clone()).unwrap());
            allocations.push(allocation);
        }
        tokio::time::advance(STALLED).await;
        clients[3].connection.wrote(64 * 1024);
        assert!(registry.reclaim());
        assert!(aborted(&clients[0]));
        assert!(clients[1..].iter().all(|client| !aborted(client)));
        // Concurrent pressure cannot cancel another connection before the cooldown.
        assert!(!registry.reclaim());
        let neighbour = client("2001:db8:1:4::1");
        streams.push(allocations[1].register(neighbour.clone()).unwrap());
        assert!(!aborted(&neighbour));
    }

    #[tokio::test(start_paused = true)]
    async fn shared_allocations_count_once_and_progress_or_multiplexing_prevents_cancellation() {
        let registry = Registry::default();
        let shared = registry.allocate(100).unwrap();
        let distinct = registry.allocate(150).unwrap();
        let shared_client = client("2001:db8:1:1::1");
        let distinct_client = client("2001:db8:2:1::1");
        let mut streams = vec![distinct.register(distinct_client.clone()).unwrap()];
        for _ in 0..8 {
            streams.push(shared.register(shared_client.clone()).unwrap());
        }
        tokio::time::advance(STALLED).await;
        assert!(registry.reclaim());
        assert!(aborted(&distinct_client));
        assert!(!aborted(&shared_client));
        tokio::time::advance(STALLED).await;
        shared_client.connection.wrote(64 * 1024);
        assert!(!registry.reclaim());
        tokio::time::advance(STALLED).await;
        shared_client.connection.protect_multiplexed();
        assert!(!registry.reclaim());
    }

    #[tokio::test(start_paused = true)]
    async fn registration_and_allocation_maps_are_bounded_and_pointer_reuse_removes_old_identity() {
        let registry = Registry::default();
        let bytes = Bytes::from_static(b"same address reused after release");
        let first = registry.allocate(bytes.len()).unwrap();
        first.bind(&bytes).unwrap();
        let old_id = first.id;
        let old_client = client("192.0.2.1");
        let old_stream = first.register(old_client.clone()).unwrap();
        drop(first);
        assert!(registry.0.lock().unwrap().pointers.is_empty());
        let second = registry.allocate(bytes.len()).unwrap();
        second.bind(&bytes).unwrap();
        assert_ne!(old_id, second.id);
        let current = client("192.0.2.2");
        let tracked = CLIENT
            .scope(Some(current.clone()), async {
                registry.track(bytes).unwrap()
            })
            .await;
        tokio::time::advance(STALLED).await;
        assert!(registry.reclaim());
        assert!(aborted(&current));
        assert!(!aborted(&old_client));
        drop((tracked, old_stream, second));
        assert!(registry.0.lock().unwrap().streams.is_empty());
        let allocations: Vec<_> = (0..MAX_ALLOCATIONS)
            .map(|_| registry.allocate(1).unwrap())
            .collect();
        assert!(registry.allocate(1).is_none());
        let streams: Vec<_> = (0..MAX_RECORDS)
            .map(|_| allocations[0].register(current.clone()).unwrap())
            .collect();
        assert!(allocations[0].register(current).is_err());
        drop((streams, allocations));
        assert!(registry.0.lock().unwrap().allocations.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn unfinished_handlers_and_new_progress_are_protected_and_cancellation_is_idempotent() {
        let registry = Registry::default();
        let allocation = registry.allocate(1 << 20).unwrap();
        let client = Client::new("192.0.2.1".parse().unwrap(), Connection::new());
        client.connection.enable_reclamation();
        let _stream = allocation.register(client.clone()).unwrap();
        tokio::time::advance(STALLED * 2).await;
        assert!(!registry.reclaim());
        client.ready();
        assert!(!registry.reclaim());
        tokio::time::advance(STALLED).await;
        client.connection.wrote(64 * 1024);
        assert!(
            !client
                .connection
                .abort_stalled(Instant::now() - STALLED, Instant::now())
        );
        tokio::time::advance(STALLED).await;
        assert!(registry.reclaim());
        assert!(!registry.reclaim());
        assert!(aborted(&client));
    }

    #[tokio::test(start_paused = true)]
    async fn progress_after_selection_atomically_protects_every_shared_owner() {
        let first = client("192.0.2.1");
        let second = client("192.0.2.2");
        let since = Instant::now();
        tokio::time::advance(STALLED).await;
        assert!(first.connection.stalled(since, Instant::now()));
        assert!(second.connection.stalled(since, Instant::now()));
        // Progress arrives after selection and before group cancellation.
        second.connection.wrote(64 * 1024);
        assert!(
            abort_group(
                &[
                    (
                        first.connection.clone(),
                        Demand {
                            since,
                            initial_written: 0,
                            bytes: 1 << 20
                        }
                    ),
                    (
                        second.connection.clone(),
                        Demand {
                            since,
                            initial_written: 0,
                            bytes: 1 << 20
                        }
                    )
                ],
                Instant::now()
            )
            .is_none()
        );
        assert!(!aborted(&first));
        assert!(!aborted(&second));
    }

    #[tokio::test(start_paused = true)]
    async fn direct_connections_are_never_pressure_victims() {
        let registry = Registry::default();
        let client = Client::new("192.0.2.1".parse().unwrap(), Connection::new());
        client.ready();
        let allocation = registry.allocate(1).unwrap();
        let _stream = allocation.register(client.clone()).unwrap();
        tokio::time::advance(STALLED * 2).await;
        assert!(!registry.reclaim());
        assert!(!aborted(&client));
    }

    #[tokio::test(start_paused = true)]
    async fn expired_owner_can_rebind_before_old_destructor_and_stale_drop_preserves_new_identity()
    {
        let registry = Registry::default();
        let bytes = Bytes::from_static(b"shared cached allocation");
        let previous = registry.allocate(bytes.len()).unwrap();
        let old_owner: Arc<dyn Send + Sync> = Arc::new(());
        previous
            .bind_owned(&bytes, Arc::downgrade(&old_owner))
            .unwrap();
        let old_client = client("192.0.2.1");
        let _old_stream = previous.register(old_client.clone()).unwrap();
        let next = registry.allocate(bytes.len()).unwrap();
        let next_owner: Arc<dyn Send + Sync> = Arc::new(());
        assert!(
            next.bind_owned(&bytes, Arc::downgrade(&next_owner))
                .is_err()
        );
        drop(old_owner);
        // The old allocation destructor has not run, although its owner expired.
        assert!(
            registry
                .0
                .lock()
                .unwrap()
                .allocations
                .contains_key(&previous.id)
        );
        next.bind_owned(&bytes, Arc::downgrade(&next_owner))
            .unwrap();
        let next_client = client("192.0.2.2");
        let tracked = CLIENT
            .scope(Some(next_client.clone()), async {
                registry.track(bytes.clone()).unwrap()
            })
            .await;
        drop(previous);
        assert_eq!(
            registry.0.lock().unwrap().pointers[&(bytes.as_ptr() as usize)],
            next.id
        );
        tokio::time::advance(STALLED).await;
        assert!(registry.reclaim());
        assert!(aborted(&next_client));
        assert!(!aborted(&old_client));
        drop((tracked, next));
        assert!(registry.0.lock().unwrap().pointers.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn projected_late_tricklers_leave_fast_shared_and_multiplexed_owners_running() {
        let registry = Registry::default();
        let clients: Vec<_> = (1..=5)
            .map(|n| client(&format!("2001:db8:1:{n}::1")))
            .collect();
        clients[4].connection.protect_multiplexed();
        let direct = Client::new("192.0.2.1".parse().unwrap(), Connection::new());
        direct.ready();
        let allocations: Vec<_> = (0..5)
            .map(|_| registry.allocate(32 << 20).unwrap())
            .collect();
        let streams = [
            allocations[0].register(clients[0].clone()).unwrap(),
            allocations[1].register(clients[1].clone()).unwrap(),
            allocations[2].register(clients[2].clone()).unwrap(),
            allocations[2].register(clients[3].clone()).unwrap(),
            allocations[3].register(clients[4].clone()).unwrap(),
            allocations[4].register(direct.clone()).unwrap(),
        ];
        for _ in 0..16 {
            tokio::time::advance(PRESSURE_TICK).await;
            for (index, client) in clients.iter().enumerate() {
                client.connection.wrote(if matches!(index, 1 | 3) {
                    128 << 10
                } else {
                    32 << 10
                });
            }
        }
        // The slow reader projects beyond180s but before the listener's240s.
        assert!(registry.reclaim());
        assert!(aborted(&clients[0]));
        assert!(clients[1..].iter().all(|client| !aborted(client)));
        assert!(!aborted(&direct));
        assert_eq!(registry.snapshot().reclaimed, 1);
        drop((streams, allocations));
    }

    #[tokio::test(start_paused = true)]
    async fn projection_uses_remaining_bytes_and_the_shorter_transport_deadline() {
        let registry = Registry::default();
        let short = client("192.0.2.1");
        short
            .connection
            .set_deadline(Instant::now() + Duration::from_secs(10));
        let normal = client("192.0.2.2");
        let nearly_done = client("192.0.2.3");
        let allocations = [
            registry.allocate(2 << 20).unwrap(),
            registry.allocate(2 << 20).unwrap(),
            registry.allocate(32 << 20).unwrap(),
        ];
        let streams = [
            allocations[0].register(short.clone()).unwrap(),
            allocations[1].register(normal.clone()).unwrap(),
            allocations[2].register(nearly_done.clone()).unwrap(),
        ];
        nearly_done.connection.wrote(31 << 20);
        for _ in 0..8 {
            tokio::time::advance(PRESSURE_TICK).await;
            short.connection.wrote(32 << 10);
            normal.connection.wrote(32 << 10);
            nearly_done.connection.wrote(20 << 10);
        }
        assert!(registry.reclaim());
        assert!(aborted(&short));
        assert!(!aborted(&normal));
        assert!(!aborted(&nearly_done));
        drop((streams, allocations));
    }

    #[tokio::test(start_paused = true)]
    async fn short_dips_and_young_demands_keep_running_but_persistently_slow_readers_are_reclaimed()
    {
        let registry = Registry::default();
        let dip = client("192.0.2.1");
        let slow = client("192.0.2.2");
        let young = client("192.0.2.3");
        let allocations: Vec<_> = (0..3)
            .map(|_| registry.allocate(32 << 20).unwrap())
            .collect();
        let mut streams = vec![
            allocations[0].register(dip.clone()).unwrap(),
            allocations[1].register(slow.clone()).unwrap(),
        ];
        for tick in 1..=166 {
            if tick == 158 {
                // Starts at 39.25 s; its rate covers only its own 2 s.
                streams.push(allocations[2].register(young.clone()).unwrap());
            }
            tokio::time::advance(PRESSURE_TICK).await;
            // 400 KiB/s finishes at about 83 s; the dip to 80 KiB/s starts at 40 s.
            dip.connection
                .wrote(if tick <= 160 { 100 << 10 } else { 20 << 10 });
            // 100 KiB/s needs about 328 s.
            slow.connection.wrote(25 << 10);
            if tick >= 158 {
                young.connection.wrote(100 << 10);
            }
            if tick == 165 {
                assert!(registry.reclaim());
            }
        }
        assert!(aborted(&slow));
        assert!(!registry.reclaim());
        assert!(!aborted(&dip));
        assert!(!aborted(&young));
        assert_eq!(registry.snapshot().reclaimed, 1);
        drop((streams, allocations));
    }

    #[tokio::test(start_paused = true)]
    async fn stalled_owner_stays_reclaimable_once_transport_bytes_cover_the_body() {
        let registry = Registry::default();
        let allocation = registry.allocate(1024).unwrap();
        let stalled = client("192.0.2.1");
        let _stream = allocation.register(stalled.clone()).unwrap();
        // Headers are counted too; the body's last frame may still be unwritten.
        stalled.connection.wrote(2048);
        tokio::time::advance(STALLED).await;
        assert!(registry.reclaim());
        assert!(aborted(&stalled));
    }

    #[tokio::test(start_paused = true)]
    async fn counters_saturate_and_throttled_or_empty_scans_count_nothing() {
        let registry = Registry::default();
        for _ in 0..10 {
            assert!(!registry.reclaim());
        }
        assert_eq!(registry.snapshot().scans, 0);
        let young = registry.allocate(1 << 20).unwrap();
        let _young = young.register(client("192.0.2.2")).unwrap();
        for _ in 0..100 {
            assert!(!registry.reclaim());
        }
        assert_eq!(registry.snapshot().scans, 1);
        assert_eq!(registry.snapshot().no_victim, 1);
        tokio::time::advance(PRESSURE_TICK).await;
        registry.0.lock().unwrap().counters = ReclaimCounters {
            scans: u64::MAX,
            reclaimed: u64::MAX,
            no_victim: u64::MAX,
        };
        assert!(!registry.reclaim());
        assert_eq!(registry.snapshot().scans, u64::MAX);
        assert_eq!(registry.snapshot().no_victim, u64::MAX);
        let allocation = registry.allocate(1 << 20).unwrap();
        let client = client("192.0.2.1");
        let _stream = allocation.register(client).unwrap();
        tokio::time::advance(STALLED).await;
        assert!(registry.reclaim());
        assert_eq!(registry.snapshot().reclaimed, u64::MAX);
    }
}
