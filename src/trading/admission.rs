use super::{TradingError, valid_actor};
use std::{
    collections::{HashMap, VecDeque},
    net::IpAddr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::oneshot;

const WINDOW: Duration = Duration::from_secs(60);
const ADDRESS_BUDGET: u32 = 120;
const MAX_ADDRESSES: usize = 4096;
const WORKERS: usize = 4;
const WORKERS_PER_FAMILY: usize = 2;
const MAX_WAITING: usize = 128;
const WAIT: Duration = Duration::from_millis(250);

struct Bucket {
    start: Instant,
    touched: Instant,
    used: u32,
    active: bool,
}
impl Bucket {
    fn new(now: Instant) -> Self {
        Self {
            start: now,
            touched: now,
            used: 0,
            active: false,
        }
    }
    fn reset(&mut self, now: Instant) {
        self.touched = now;
        if now.duration_since(self.start) >= WINDOW {
            self.start = now;
            self.used = 0;
        }
    }
}
#[derive(Default)]
struct Aggregate {
    queue: VecDeque<u64>,
    running: usize,
}
#[derive(Default)]
struct Family {
    order: VecDeque<IpAddr>,
    running: usize,
    waiting: usize,
}
fn family(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(ip) => IpAddr::V6((u128::from(ip) & (u128::MAX << 80)).into()),
        ip => ip,
    }
}
struct Job {
    actor: String,
    ip: IpAddr,
    charge: Option<(u32, Instant)>,
    running: bool,
    ready: Option<oneshot::Sender<()>>,
}
#[derive(Default)]
struct Registry {
    addresses: HashMap<String, Bucket>,
    aggregates: HashMap<IpAddr, Aggregate>,
    families: HashMap<IpAddr, Family>,
    jobs: HashMap<u64, Job>,
    order: VecDeque<IpAddr>,
    next_id: u64,
    running: usize,
    waiting: usize,
}
impl Registry {
    fn remove(&mut self, id: u64) {
        let Some(job) = self.jobs.remove(&id) else {
            return;
        };
        if let Some(bucket) = self.addresses.get_mut(&job.actor) {
            bucket.active = false;
            bucket.touched = Instant::now();
            if let Some((weight, start)) = job.charge {
                if bucket.start == start {
                    bucket.used = bucket.used.saturating_sub(weight);
                }
            }
        }
        let network = family(job.ip);
        let family = self.families.get_mut(&network).expect("registered family");
        let aggregate = self.aggregates.get_mut(&job.ip).expect("registered job");
        if job.running {
            self.running -= 1;
            aggregate.running -= 1;
            family.running -= 1;
        } else {
            self.waiting -= 1;
            family.waiting -= 1;
            aggregate.queue.retain(|queued| *queued != id);
        }
        if aggregate.queue.is_empty() {
            family.order.retain(|ip| *ip != job.ip);
            if aggregate.running == 0 {
                self.aggregates.remove(&job.ip);
            }
        }
        if family.order.is_empty() {
            self.order.retain(|ip| *ip != network);
            if family.running == 0 {
                self.families.remove(&network);
            }
        }
    }

    fn schedule(&mut self) {
        // Rotate families, then their prefixes; no family can occupy every worker.
        let mut skipped = 0;
        while self.running < WORKERS && skipped < self.order.len() {
            let Some(network) = self.order.pop_front() else {
                break;
            };
            let family = self.families.get_mut(&network).expect("queued family");
            if family.running >= WORKERS_PER_FAMILY {
                self.order.push_back(network);
                skipped += 1;
                continue;
            }
            let ip = family.order.pop_front().expect("queued prefix");
            let aggregate = self.aggregates.get_mut(&ip).expect("queued aggregate");
            let id = aggregate.queue.pop_front().expect("queued job");
            if !aggregate.queue.is_empty() {
                family.order.push_back(ip);
            }
            if !family.order.is_empty() {
                self.order.push_back(network);
            }
            aggregate.running += 1;
            family.running += 1;
            family.waiting -= 1;
            self.running += 1;
            self.waiting -= 1;
            skipped = 0;
            let job = self.jobs.get_mut(&id).expect("registered job");
            job.running = true;
            if job
                .ready
                .take()
                .expect("pending notification")
                .send(())
                .is_err()
            {
                self.remove(id);
            }
        }
    }

    fn make_room(&mut self, ip: IpAddr) -> Result<(), TradingError> {
        if self.waiting < MAX_WAITING {
            return Ok(());
        }
        let network = family(ip);
        let own_waiting = self.families.get(&network).map_or(0, |f| f.waiting);
        // Preserve singleton families, including those with just one queued prefix.
        let victim = self
            .families
            .iter()
            .filter(|(key, f)| **key != network && f.waiting > own_waiting + 1)
            .filter_map(|(_, f)| {
                f.order
                    .iter()
                    .filter_map(|ip| self.aggregates[ip].queue.back().copied())
                    .max()
                    .map(|newest| (f.waiting, newest))
            })
            .max()
            .map(|(_, newest)| newest)
            .ok_or(TradingError::Busy)?;
        self.remove(victim);
        Ok(())
    }
}

pub(super) struct Admission {
    registry: Arc<Mutex<Registry>>,
}
pub(super) struct Permit {
    registry: Arc<Mutex<Registry>>,
    id: u64,
}
impl Permit {
    /// Commit the reservation immediately before external work begins.
    pub fn start_work(&mut self) {
        let mut registry = self.registry.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(job) = registry.jobs.get_mut(&self.id) {
            job.charge = None;
        }
    }
}
impl Drop for Permit {
    fn drop(&mut self) {
        let mut registry = self.registry.lock().unwrap_or_else(|e| e.into_inner());
        registry.remove(self.id);
        registry.schedule();
    }
}
impl Admission {
    pub fn new() -> Self {
        Self {
            registry: Arc::new(Mutex::new(Registry::default())),
        }
    }
    pub async fn acquire(
        &self,
        actor: &str,
        ip: IpAddr,
        weight: u32,
    ) -> Result<Permit, TradingError> {
        let ip = crate::client_ip::rate_ip(ip);
        if !valid_actor(actor) {
            return Err(TradingError::Unauthorized);
        }
        let (permit, ready) = {
            let now = Instant::now();
            let mut registry = self.registry.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(bucket) = registry.addresses.get_mut(actor) {
                bucket.reset(now);
                if bucket.active {
                    return Err(TradingError::Busy);
                }
                if bucket.used.saturating_add(weight) > ADDRESS_BUDGET {
                    return Err(TradingError::RateLimited);
                }
            } else if weight > ADDRESS_BUDGET {
                return Err(TradingError::RateLimited);
            }
            registry.make_room(ip)?;
            if !registry.addresses.contains_key(actor) && registry.addresses.len() >= MAX_ADDRESSES
            {
                let oldest = registry
                    .addresses
                    .iter()
                    .filter(|(_, b)| !b.active)
                    .min_by_key(|(_, b)| b.touched)
                    .map(|(key, _)| key.clone())
                    .ok_or(TradingError::Busy)?;
                registry.addresses.remove(&oldest);
            }
            let bucket = registry
                .addresses
                .entry(actor.into())
                .or_insert_with(|| Bucket::new(now));
            bucket.used += weight;
            bucket.active = true;
            let charge = Some((weight, bucket.start));
            let id = registry.next_id;
            registry.next_id = registry.next_id.wrapping_add(1);
            let (send, ready) = oneshot::channel();
            registry.jobs.insert(
                id,
                Job {
                    actor: actor.into(),
                    ip,
                    charge,
                    running: false,
                    ready: Some(send),
                },
            );
            let aggregate = registry.aggregates.entry(ip).or_default();
            let newly_queued = aggregate.queue.is_empty();
            aggregate.queue.push_back(id);
            let network = family(ip);
            let family = registry.families.entry(network).or_default();
            let newly_queued_family = family.order.is_empty();
            if newly_queued {
                family.order.push_back(ip);
            }
            family.waiting += 1;
            if newly_queued_family {
                registry.order.push_back(network);
            }
            registry.waiting += 1;
            registry.schedule();
            (
                Permit {
                    registry: self.registry.clone(),
                    id,
                },
                ready,
            )
        };
        tokio::time::timeout(WAIT, ready)
            .await
            .map_err(|_| TradingError::Busy)?
            .map_err(|_| TradingError::Busy)?;
        Ok(permit)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn started(mut permit: Permit) -> Permit {
        permit.start_work();
        permit
    }
    fn actor(n: usize) -> String {
        format!("tz1{:033}", n)
    }
    fn ip(n: u8) -> IpAddr {
        IpAddr::from([127, 0, 0, n])
    }
    fn ipv6(network: usize, prefix: usize) -> IpAddr {
        format!("2001:db8:{network:x}:{prefix:x}::1")
            .parse()
            .unwrap()
    }
    async fn queued(
        gate: &Arc<Admission>,
        n: usize,
        address: IpAddr,
    ) -> tokio::task::JoinHandle<Result<Permit, TradingError>> {
        let clone = gate.clone();
        let task = tokio::spawn(async move { clone.acquire(&actor(n), address, 10).await });
        tokio::time::timeout(Duration::from_secs(1), async {
            while !gate
                .registry
                .lock()
                .unwrap()
                .addresses
                .get(&actor(n))
                .is_some_and(|b| b.active)
            {
                assert!(!task.is_finished(), "request failed before queuing");
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        task
    }
    async fn fill_workers(gate: &Admission) -> Vec<Permit> {
        let mut permits = Vec::new();
        for n in 1..=4 {
            permits.push(
                gate.acquire(&actor(n), ip(if n <= 2 { 1 } else { 2 }), 1)
                    .await
                    .unwrap(),
            );
        }
        permits
    }
    fn empty(gate: &Admission) {
        let r = gate.registry.lock().unwrap();
        assert_eq!((r.running, r.waiting), (0, 0));
        assert!(r.jobs.is_empty());
        assert!(r.aggregates.is_empty());
        assert!(r.families.is_empty());
        assert!(r.order.is_empty());
        assert!(r.addresses.values().all(|b| !b.active));
    }

    #[tokio::test]
    async fn address_budgets_do_not_charge_other_addresses_sharing_an_ip() {
        let gate = Admission::new();
        for _ in 0..12 {
            drop(started(gate.acquire(&actor(1), ip(1), 10).await.unwrap()));
        }
        assert!(matches!(
            gate.acquire(&actor(1), ip(2), 1).await,
            Err(TradingError::RateLimited)
        ));
        for n in 2..=40 {
            drop(started(gate.acquire(&actor(n), ip(1), 10).await.unwrap()));
        }
        empty(&gate);
    }

    #[tokio::test]
    async fn family_worker_shares_cover_ipv6_prefixes_and_mapped_ipv4() {
        for (first, same, separate) in [
            ("2001:db8:1:2::1", "2001:db8:1:2::abcd", "2001:db8:2:3::1"),
            ("192.0.2.1", "::ffff:192.0.2.1", "192.0.2.2"),
        ] {
            let first = first.parse().unwrap();
            let same = same.parse().unwrap();
            let separate = separate.parse().unwrap();
            let gate = Arc::new(Admission::new());
            let a = gate.acquire(&actor(1), first, 10).await.unwrap();
            let b = gate.acquire(&actor(2), same, 10).await.unwrap();
            let pending = queued(&gate, 3, same).await;
            let independent = gate.acquire(&actor(4), separate, 10).await.unwrap();
            assert!(!pending.is_finished());
            assert_eq!(gate.registry.lock().unwrap().running, 3);
            drop(a);
            let next = pending.await.unwrap().unwrap();
            drop((b, independent, next));
            empty(&gate);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn rotating_ipv6_prefixes_share_two_workers_and_leave_two_for_other_families() {
        let gate = Arc::new(Admission::new());
        let first = gate.acquire(&actor(1), ipv6(1, 1), 10).await.unwrap();
        let second = gate.acquire(&actor(2), ipv6(1, 2), 10).await.unwrap();
        let pending = queued(&gate, 3, ipv6(1, 3)).await;
        let neighbour = gate.acquire(&actor(4), ipv6(2, 1), 10).await.unwrap();
        let other = gate.acquire(&actor(5), ipv6(2, 2), 10).await.unwrap();
        {
            let r = gate.registry.lock().unwrap();
            assert_eq!((r.running, r.waiting), (WORKERS, 1));
            assert_eq!(r.families[&family(ipv6(1, 1))].running, 2);
            assert_eq!(r.families[&family(ipv6(2, 1))].running, 2);
        }
        assert!(!pending.is_finished());
        drop(first);
        let admitted = pending.await.unwrap().unwrap();
        drop((second, neighbour, other, admitted));
        empty(&gate);
    }

    #[tokio::test(start_paused = true)]
    async fn queued_work_rotates_families_before_rotating_prefixes_within_each_family() {
        let gate = Arc::new(Admission::new());
        let mut held = fill_workers(&gate).await;
        let first = queued(&gate, 10, ipv6(1, 1)).await;
        let same_prefix = queued(&gate, 11, ipv6(1, 1)).await;
        let next_prefix = queued(&gate, 12, ipv6(1, 2)).await;
        let second_family = queued(&gate, 13, ipv6(2, 1)).await;
        let second_same_prefix = queued(&gate, 14, ipv6(2, 1)).await;
        let second_next_prefix = queued(&gate, 15, ipv6(2, 2)).await;

        drop(held.pop());
        let first = first.await.unwrap().unwrap();
        assert!(!second_family.is_finished());
        assert!(!same_prefix.is_finished());
        drop(held.pop());
        let second_family = second_family.await.unwrap().unwrap();
        assert!(!next_prefix.is_finished());
        drop(held.pop());
        let next_prefix = next_prefix.await.unwrap().unwrap();
        assert!(!same_prefix.is_finished());
        drop(held.pop());
        let second_next_prefix = second_next_prefix.await.unwrap().unwrap();
        assert!(!second_same_prefix.is_finished());
        {
            let r = gate.registry.lock().unwrap();
            assert_eq!((r.running, r.waiting), (WORKERS, 2));
            assert!(r.families.values().all(|f| f.running == 2));
        }
        drop((first, next_prefix, second_family, second_next_prefix));
        drop(same_prefix.await.unwrap().unwrap());
        drop(second_same_prefix.await.unwrap().unwrap());
        empty(&gate);
    }

    #[tokio::test]
    async fn new_aggregate_gets_a_turn_before_a_busy_aggregates_second_waiter() {
        let gate = Arc::new(Admission::new());
        let mut held = fill_workers(&gate).await;
        let a = queued(&gate, 10, ip(1)).await;
        let b = queued(&gate, 11, ip(1)).await;
        let c = queued(&gate, 12, ip(1)).await;
        let independent = queued(&gate, 13, ip(3)).await;
        drop(held.remove(0));
        let a = a.await.unwrap().unwrap();
        assert!(!b.is_finished());
        drop(held.remove(0));
        let independent = independent.await.unwrap().unwrap();
        assert!(!b.is_finished());
        drop((a, independent, held));
        drop(b.await.unwrap().unwrap());
        drop(c.await.unwrap().unwrap());
        empty(&gate);
    }

    #[tokio::test]
    async fn shared_ip_addresses_queue_in_order_instead_of_failing_at_two() {
        let gate = Arc::new(Admission::new());
        let first = gate.acquire(&actor(1), ip(1), 1).await.unwrap();
        let mut second = gate.acquire(&actor(2), ip(1), 1).await.unwrap();
        let mut pending = Vec::new();
        for n in 3..=10 {
            pending.push(queued(&gate, n, ip(1)).await);
        }
        for task in pending {
            drop(second);
            second = task.await.unwrap().unwrap();
        }
        drop((first, second));
        empty(&gate);
    }

    #[tokio::test]
    async fn full_queue_reclaims_a_busy_aggregates_newest_request_for_a_neighbour() {
        let gate = Arc::new(Admission::new());
        let held = fill_workers(&gate).await;
        let mut pending = Vec::new();
        for n in 0..MAX_WAITING {
            pending.push(queued(&gate, n + 10, ip(1)).await);
        }
        assert!(matches!(
            gate.acquire(&actor(1000), ip(1), 1).await,
            Err(TradingError::Busy)
        ));
        let independent = queued(&gate, 1001, ip(3)).await;
        let displaced = pending.pop().unwrap();
        assert!(matches!(displaced.await.unwrap(), Err(TradingError::Busy)));
        {
            let r = gate.registry.lock().unwrap();
            assert_eq!(r.waiting, MAX_WAITING);
            assert_eq!(r.jobs.len(), MAX_WAITING + WORKERS);
            assert_eq!(r.addresses[&actor(MAX_WAITING + 9)].used, 0);
        }
        for task in pending {
            task.abort();
            let _ = task.await;
        }
        drop(held);
        drop(independent.await.unwrap().unwrap());
        empty(&gate);
    }

    #[tokio::test(start_paused = true)]
    async fn full_queue_displaces_newest_rotating_prefix_from_oversized_family() {
        let gate = Arc::new(Admission::new());
        let held = fill_workers(&gate).await;
        let mut pending = Vec::new();
        for n in 0..MAX_WAITING {
            pending.push(queued(&gate, n + 10, ipv6(1, n)).await);
        }
        assert!(matches!(
            gate.acquire(&actor(1000), ipv6(1, MAX_WAITING), 10).await,
            Err(TradingError::Busy)
        ));
        let independent = queued(&gate, 1001, ipv6(2, 1)).await;
        assert!(matches!(
            pending.pop().unwrap().await.unwrap(),
            Err(TradingError::Busy)
        ));
        {
            let r = gate.registry.lock().unwrap();
            assert_eq!(r.waiting, MAX_WAITING);
            assert_eq!(r.families[&family(ipv6(1, 0))].waiting, MAX_WAITING - 1);
            assert_eq!(r.families[&family(ipv6(2, 0))].waiting, 1);
            assert_eq!(r.addresses[&actor(MAX_WAITING + 9)].used, 0);
            assert!(
                !r.aggregates
                    .contains_key(&crate::client_ip::rate_ip(ipv6(1, MAX_WAITING - 1)))
            );
        }
        for task in pending {
            task.abort();
            let _ = task.await;
        }
        assert!(
            !gate
                .registry
                .lock()
                .unwrap()
                .families
                .contains_key(&family(ipv6(1, 0)))
        );
        drop(held);
        drop(independent.await.unwrap().unwrap());
        empty(&gate);
    }

    #[tokio::test(start_paused = true)]
    async fn singleton_ipv6_families_survive_new_arrivals_and_cancel_without_stale_turns() {
        let gate = Arc::new(Admission::new());
        let held = fill_workers(&gate).await;
        let mut pending = Vec::new();
        for n in 0..MAX_WAITING {
            pending.push(queued(&gate, n + 10, ipv6(n, 1)).await);
        }
        assert!(matches!(
            gate.acquire(&actor(1000), ipv6(MAX_WAITING, 1), 10).await,
            Err(TradingError::Busy)
        ));
        assert!(pending.iter().all(|task| !task.is_finished()));
        for task in pending {
            task.abort();
            let _ = task.await;
        }
        {
            let r = gate.registry.lock().unwrap();
            assert_eq!(r.waiting, 0);
            assert_eq!(r.families.len(), 2);
            assert!(r.order.is_empty());
        }
        drop(held);
        empty(&gate);
    }

    #[tokio::test]
    async fn unrelated_singleton_waiters_are_not_displaced_by_new_arrivals() {
        let gate = Arc::new(Admission::new());
        let held = fill_workers(&gate).await;
        let mut tasks = Vec::new();
        for n in 0..MAX_WAITING {
            tasks.push(queued(&gate, n + 10, ip(n as u8 + 3)).await);
        }
        assert!(matches!(
            gate.acquire(&actor(1000), ip(200), 1).await,
            Err(TradingError::Busy)
        ));
        assert_eq!(gate.registry.lock().unwrap().waiting, MAX_WAITING);
        for task in tasks {
            task.abort();
            let _ = task.await;
        }
        drop(held);
        empty(&gate);
    }

    #[tokio::test]
    async fn queue_timeout_refunds_unstarted_work_and_releases_every_reservation() {
        let gate = Admission::new();
        let held = fill_workers(&gate).await;
        assert!(matches!(
            gate.acquire(&actor(5), ip(5), 10).await,
            Err(TradingError::Busy)
        ));
        {
            let r = gate.registry.lock().unwrap();
            assert_eq!(r.addresses[&actor(5)].used, 0);
            assert!(!r.addresses[&actor(5)].active);
            assert_eq!(r.waiting, 0);
        }
        drop(held);
        drop(gate.acquire(&actor(5), ip(5), 10).await.unwrap());
        empty(&gate);
    }

    #[tokio::test]
    async fn cancellation_refunds_pending_and_assigned_but_unstarted_work() {
        let gate = Arc::new(Admission::new());
        let mut held = fill_workers(&gate).await;
        let pending = queued(&gate, 5, ip(5)).await;
        pending.abort();
        let _ = pending.await;
        assert_eq!(gate.registry.lock().unwrap().addresses[&actor(5)].used, 0);
        let assigned = queued(&gate, 6, ip(6)).await;
        drop(held.pop());
        // No yield: delivery is granted, but the acquiring task has not resumed.
        assert_eq!(gate.registry.lock().unwrap().running, WORKERS);
        assigned.abort();
        let _ = assigned.await;
        assert_eq!(gate.registry.lock().unwrap().addresses[&actor(6)].used, 0);
        drop(held);
        empty(&gate);
    }

    #[tokio::test]
    async fn cancellation_never_refunds_a_different_window() {
        let gate = Arc::new(Admission::new());
        let held = fill_workers(&gate).await;
        let task = queued(&gate, 5, ip(5)).await;
        {
            let mut r = gate.registry.lock().unwrap();
            let bucket = r.addresses.get_mut(&actor(5)).unwrap();
            bucket.start += Duration::from_nanos(1);
            bucket.used = 7;
        }
        task.abort();
        let _ = task.await;
        assert_eq!(gate.registry.lock().unwrap().addresses[&actor(5)].used, 7);
        drop(held);
        empty(&gate);
    }

    #[tokio::test]
    async fn full_tracking_map_evicts_idle_addresses_and_preserves_active_work() {
        let gate = Admission::new();
        let held = gate.acquire(&actor(0), ip(0), 10).await.unwrap();
        let now = Instant::now();
        {
            let mut r = gate.registry.lock().unwrap();
            for n in 1..MAX_ADDRESSES {
                let mut bucket = Bucket::new(now);
                if n == 1 {
                    bucket.touched = now - Duration::from_secs(1);
                }
                r.addresses.insert(actor(n), bucket);
            }
        }
        let newcomer = gate
            .acquire(&actor(MAX_ADDRESSES), ip(2), 10)
            .await
            .unwrap();
        {
            let r = gate.registry.lock().unwrap();
            assert_eq!(r.addresses.len(), MAX_ADDRESSES);
            assert!(r.addresses.contains_key(&actor(0)));
            assert!(!r.addresses.contains_key(&actor(1)));
            assert_eq!(r.aggregates.len(), 2);
        }
        drop((held, newcomer));
        empty(&gate);
    }

    #[tokio::test]
    async fn started_work_keeps_its_charge_and_duplicate_actors_cannot_queue() {
        let gate = Admission::new();
        let held = started(gate.acquire(&actor(1), ip(1), 10).await.unwrap());
        assert!(matches!(
            gate.acquire(&actor(1), ip(2), 10).await,
            Err(TradingError::Busy)
        ));
        drop(held);
        assert_eq!(gate.registry.lock().unwrap().addresses[&actor(1)].used, 10);
        empty(&gate);
    }
}
