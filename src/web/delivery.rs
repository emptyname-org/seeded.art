//! Account for retained artwork bytes independently of generator assembly.
use axum::http::StatusCode;
use std::{
    collections::HashMap,
    net::IpAddr,
    sync::{Arc, Mutex, Weak},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use super::ArtworkCounters;
use crate::delivery_control::PRESSURE_TICK;

const UNIT: u64 = 4096;
const MIN_CLIENT_BYTES: u64 = 64 << 20;
const MAX_CLIENTS: usize = 8192;

#[derive(Clone)]
pub(super) struct Budget {
    total: Arc<Semaphore>,
    clients: Arc<Mutex<HashMap<IpAddr, Weak<Semaphore>>>>,
    client_units: u32,
    counters: Arc<ArtworkCounters>,
    deliveries: crate::delivery_control::Registry,
    waiting: Arc<Semaphore>,
    pending_bytes: Arc<Semaphore>,
}
pub(super) struct Reservation {
    _client: OwnedSemaphorePermit,
    _total: Option<OwnedSemaphorePermit>,
    allocation: Option<crate::delivery_control::Allocation>,
    _stream: Option<crate::delivery_control::Registration>,
}
impl Budget {
    pub(super) fn new(object_bytes: u64, counters: Arc<ArtworkCounters>) -> Self {
        // Rendered HTML needs a small envelope beyond the source object.
        let object_units = object_bytes
            .min(256 << 20)
            .saturating_add(1 << 20)
            .div_ceil(UNIT) as u32;
        let client_units = object_units.max(MIN_CLIENT_BYTES.div_ceil(UNIT) as u32);
        Self {
            total: Arc::new(Semaphore::new(client_units as usize * 8)),
            clients: Arc::new(Mutex::new(HashMap::new())),
            client_units,
            deliveries: counters.delivery_reclaim.clone(),
            counters,
            waiting: Arc::new(Semaphore::new(16)),
            // Pending retention adds one configured object plus the HTML envelope
            // to the active limit of eight client envelopes.
            pending_bytes: Arc::new(Semaphore::new(object_units as usize)),
        }
    }
    #[cfg(test)]
    pub(super) fn reserve(&self, ip: IpAddr, bytes: u64) -> Result<Arc<Reservation>, StatusCode> {
        self.reserve_kind(ip, bytes, false)
    }

    #[cfg(test)]
    pub(super) fn reserve_accounted(
        &self,
        ip: IpAddr,
        bytes: u64,
    ) -> Result<Arc<Reservation>, StatusCode> {
        self.reserve_kind(ip, bytes, true)
    }

    #[cfg(test)]
    pub(super) async fn reserve_delivery(
        &self,
        ip: IpAddr,
        bytes: u64,
        accounted: bool,
        connection: Option<crate::delivery_control::Connection>,
    ) -> Result<Arc<Reservation>, StatusCode> {
        self.reserve_delivery_after(ip, bytes, accounted, connection, || {})
            .await
    }

    pub(super) async fn reserve_delivery_after(
        &self,
        ip: IpAddr,
        bytes: u64,
        accounted: bool,
        connection: Option<crate::delivery_control::Connection>,
        release_assembly: impl FnOnce() + Send,
    ) -> Result<Arc<Reservation>, StatusCode> {
        // Hold the client's entire charge while waiting; no partial global admission.
        let mut reservation = self.reserve_kind(ip, bytes, true)?;
        let mut release_assembly = Some(release_assembly);
        if !accounted {
            let units = bytes.div_ceil(UNIT).max(1) as u32;
            let result = async {
                if let Ok(permit) = self.total.clone().try_acquire_many_owned(units) {
                    release_assembly.take().unwrap()();
                    return Ok(permit);
                }
                let _pending = self
                    .pending_bytes
                    .clone()
                    .try_acquire_many_owned(units)
                    .map_err(|_| {
                        self.deliveries.reclaim();
                        StatusCode::SERVICE_UNAVAILABLE
                    })?;
                // The completed body now has a byte charge before any new await.
                release_assembly.take().unwrap()();
                let _waiting = self
                    .waiting
                    .clone()
                    .try_acquire_owned()
                    .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
                let mut cancellations = 0;
                let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
                if self.deliveries.reclaim() {
                    cancellations += 1;
                }
                // Young waiters leave capacity to small arrivals; after 250 ms, FIFO
                // semaphore admission keeps headroom for the oldest large response.
                tokio::time::sleep(PRESSURE_TICK).await;
                let acquire = self.total.clone().acquire_many_owned(units);
                tokio::pin!(acquire);
                loop {
                    let tick = (tokio::time::Instant::now() + PRESSURE_TICK).min(deadline);
                    tokio::select! {
                        biased;
                        permit = &mut acquire => {
                            return permit.map_err(|_| StatusCode::SERVICE_UNAVAILABLE);
                        }
                        () = tokio::time::sleep_until(tick) => {}
                    }
                    if tokio::time::Instant::now() >= deadline {
                        return Err(StatusCode::SERVICE_UNAVAILABLE);
                    }
                    if cancellations < 4 && self.deliveries.reclaim() {
                        cancellations += 1;
                    }
                }
            }
            .await;
            let total = result.inspect_err(|_| self.counters.delivery_global_bytes_full())?;
            let allocation = self
                .deliveries
                .allocate(bytes as usize)
                .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
            let stream = connection
                .map(|connection| {
                    let client = crate::delivery_control::Client::new(ip, connection);
                    client.ready();
                    allocation.register(client)
                })
                .transpose()
                .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
            let reservation = Arc::get_mut(&mut reservation).expect("unshared reservation");
            reservation._total = Some(total);
            reservation.allocation = Some(allocation);
            reservation._stream = stream;
        } else {
            release_assembly.take().unwrap()();
        }
        Ok(reservation)
    }

    fn reserve_kind(
        &self,
        ip: IpAddr,
        bytes: u64,
        accounted: bool,
    ) -> Result<Arc<Reservation>, StatusCode> {
        let units =
            u32::try_from(bytes.div_ceil(UNIT).max(1)).map_err(|_| StatusCode::BAD_GATEWAY)?;
        if units > self.client_units {
            return Err(StatusCode::BAD_GATEWAY);
        }
        let ip = crate::client_ip::rate_ip(ip);
        let client = {
            let mut clients = self
                .clients
                .lock()
                .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
            if let Some(client) = clients.get(&ip).and_then(Weak::upgrade) {
                client
            } else {
                if clients.len() >= MAX_CLIENTS {
                    clients.retain(|_, weak| weak.strong_count() > 0);
                }
                if clients.len() >= MAX_CLIENTS {
                    self.counters.delivery_client_table_full();
                    return Err(StatusCode::SERVICE_UNAVAILABLE);
                }
                let client = Arc::new(Semaphore::new(self.client_units as usize));
                clients.insert(ip, Arc::downgrade(&client));
                client
            }
        };
        // No queued body allocations: a failed reservation drops the assembled body.
        let client = match client.try_acquire_many_owned(units) {
            Ok(permit) => permit,
            Err(error) => {
                if matches!(error, tokio::sync::TryAcquireError::NoPermits) {
                    self.counters.delivery_client_bytes_full();
                }
                return Err(StatusCode::TOO_MANY_REQUESTS);
            }
        };
        let total = if accounted {
            None
        } else {
            Some(match self.total.clone().try_acquire_many_owned(units) {
                Ok(permit) => permit,
                Err(error) => {
                    if matches!(error, tokio::sync::TryAcquireError::NoPermits) {
                        self.counters.delivery_global_bytes_full();
                    }
                    return Err(StatusCode::SERVICE_UNAVAILABLE);
                }
            })
        };
        let allocation = if accounted {
            None
        } else {
            Some(
                self.deliveries
                    .allocate(units as usize * UNIT as usize)
                    .ok_or(StatusCode::SERVICE_UNAVAILABLE)?,
            )
        };
        Ok(Arc::new(Reservation {
            _client: client,
            _total: total,
            allocation,
            _stream: None,
        }))
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn web_pressure_cancels_transport_and_retains_charge_until_final_frame_drop() {
        use crate::{request_limits::hold_delivery, transport::tests::Transfer};
        use axum::body::Body;
        use bytes::Bytes;
        use http_body_util::BodyExt;
        let size = 1024 * 1024;
        let (mut budget, _) = counters(size);
        budget.client_units = (size / UNIT) as u32;
        budget.total = Arc::new(Semaphore::new((8 * size / UNIT) as usize));
        budget.pending_bytes = Arc::new(Semaphore::new((size / UNIT) as usize));
        let mut transfers = Vec::new();
        let mut final_frame = None;
        for n in 0..8 {
            let mut transfer = Transfer::new().await;
            let permit = budget
                .reserve_delivery(ip(n), size, false, Some(transfer.control.clone()))
                .await
                .unwrap();
            let bytes = Bytes::from(vec![n; size as usize]);
            if n == 0 {
                let mut body = hold_delivery(
                    Body::from(bytes.clone()),
                    permit.clone(),
                    Some(transfer.control.clone()),
                );
                while let Some(frame) = body.frame().await {
                    final_frame = Some(frame.unwrap());
                }
                drop(body);
            }
            transfer
                .start(hold_delivery(
                    Body::from(bytes),
                    permit,
                    Some(transfer.control.clone()),
                ))
                .await;
            transfers.push(transfer);
        }
        assert_eq!(budget.total.available_permits(), 0);
        tokio::time::advance(std::time::Duration::from_secs(1)).await;
        transfers[7].control.wrote(64 * 1024);
        let mut ninth = Box::pin(budget.reserve_delivery(ip(9), size, false, None));
        std::future::poll_fn(|cx| {
            assert!(std::future::Future::poll(ninth.as_mut(), cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        transfers[0].exited().await;
        assert_eq!(budget.total.available_permits(), 0);
        drop(final_frame);
        let replacement = ninth.await.unwrap();
        assert_eq!(budget.total.available_permits(), 0);
        assert_eq!(budget.deliveries.snapshot().reclaimed, 1);
        assert!(
            transfers[1..]
                .iter()
                .all(|transfer| transfer.slots.available_permits() == 0)
        );
        drop(replacement);
        assert!(transfers[1].drain().await >= size as usize);
        assert!(transfers[7].drain().await >= size as usize);
    }

    #[tokio::test(start_paused = true)]
    async fn web_pressure_without_a_victim_times_out_and_cancelled_waiters_refund_client_only() {
        let (mut budget, counters) = counters(1);
        budget.client_units = 1;
        budget.total = Arc::new(Semaphore::new(1));
        budget.pending_bytes = Arc::new(Semaphore::new(1));
        let held = budget.reserve(ip(1), UNIT).unwrap();
        let mut waiting = Box::pin(budget.reserve_delivery(ip(2), UNIT, false, None));
        std::future::poll_fn(|cx| {
            assert!(std::future::Future::poll(waiting.as_mut(), cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        assert_eq!(budget.waiting.available_permits(), 15);
        assert_eq!(budget.pending_bytes.available_permits(), 0);
        drop(waiting);
        assert_eq!(budget.waiting.available_permits(), 16);
        assert_eq!(budget.pending_bytes.available_permits(), 1);
        assert_eq!(budget.total.available_permits(), 0);
        assert_eq!(
            budget
                .reserve_delivery(ip(2), UNIT, false, None)
                .await
                .err(),
            Some(StatusCode::SERVICE_UNAVAILABLE)
        );
        assert_eq!(snapshot(counters).delivery_global_bytes_full, 1);
        drop(held);
        drop(
            budget
                .reserve_delivery(ip(2), UNIT, false, None)
                .await
                .unwrap(),
        );
        assert_eq!(budget.total.available_permits(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn web_pressure_reclaims_a_progressing_reader_projected_past_its_deadline() {
        use crate::delivery_control::Connection;
        let size = 4 << 20;
        let (mut budget, _) = counters(size);
        budget.client_units = (size / UNIT) as u32;
        budget.total = Arc::new(Semaphore::new((2 * size / UNIT) as usize));
        let mut connections = Vec::new();
        let mut active = Vec::new();
        for n in 0..2 {
            let connection = Connection::new();
            connection.enable_reclamation();
            connection
                .set_deadline(tokio::time::Instant::now() + std::time::Duration::from_secs(20));
            active.push(
                budget
                    .reserve_delivery(ip(n), size, false, Some(connection.clone()))
                    .await
                    .unwrap(),
            );
            connections.push(connection);
        }
        for _ in 0..8 {
            tokio::time::advance(PRESSURE_TICK).await;
            // 96 KiB/s never stalls but needs about 40 s; 1 MiB/s needs about 2 s.
            connections[0].wrote(24 << 10);
            connections[1].wrote(256 << 10);
        }
        let mut waiting = Box::pin(budget.reserve_delivery(ip(3), UNIT, false, None));
        std::future::poll_fn(|cx| {
            assert!(std::future::Future::poll(waiting.as_mut(), cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        let cx = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(connections[0].poll_aborted(&cx));
        assert!(!connections[1].poll_aborted(&cx));
        assert_eq!(budget.deliveries.snapshot().reclaimed, 1);
        drop(active.remove(0));
        drop(waiting.await.unwrap());
        drop(active);
        assert_eq!(budget.total.available_permits(), (2 * size / UNIT) as usize);
    }

    #[tokio::test(start_paused = true)]
    async fn pending_body_bytes_are_bounded_and_transfer_to_active_before_pending_release() {
        let (mut budget, _) = counters(1);
        budget.client_units = 2;
        budget.total = Arc::new(Semaphore::new(1));
        budget.pending_bytes = Arc::new(Semaphore::new(2));
        let held = budget.reserve(ip(1), UNIT).unwrap();
        let mut first = Box::pin(budget.reserve_delivery(ip(2), UNIT, false, None));
        let mut second = Box::pin(budget.reserve_delivery(ip(3), UNIT, false, None));
        for future in [&mut first, &mut second] {
            std::future::poll_fn(|cx| {
                assert!(std::future::Future::poll(future.as_mut(), cx).is_pending());
                std::task::Poll::Ready(())
            })
            .await;
        }
        assert_eq!(budget.pending_bytes.available_permits(), 0);
        assert_eq!(
            budget
                .reserve_delivery(ip(4), UNIT, false, None)
                .await
                .err(),
            Some(StatusCode::SERVICE_UNAVAILABLE)
        );
        drop(first);
        assert_eq!(budget.pending_bytes.available_permits(), 1);
        assert_eq!(budget.total.available_permits(), 0);
        drop(held);
        let active = second.await.unwrap();
        assert_eq!(budget.pending_bytes.available_permits(), 2);
        assert_eq!(budget.total.available_permits(), 0);
        assert_eq!(budget.waiting.available_permits(), 16);
        drop(active);
        assert_eq!(budget.total.available_permits(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn assembly_is_released_only_after_a_body_charge_and_errors_drop_it_without_waiting() {
        let (mut budget, _) = counters(1);
        budget.client_units = 1;
        budget.total = Arc::new(Semaphore::new(1));
        budget.pending_bytes = Arc::new(Semaphore::new(1));
        let assembly = Arc::new(Semaphore::new(1));
        let permit = assembly.clone().acquire_owned().await.unwrap();
        let active = budget
            .reserve_delivery_after(ip(1), UNIT, false, None, || {
                assert_eq!(budget.total.available_permits(), 0);
                assert_eq!(budget.pending_bytes.available_permits(), 1);
                drop(permit);
            })
            .await
            .unwrap();
        assert_eq!(assembly.available_permits(), 1);
        let permit = assembly.clone().acquire_owned().await.unwrap();
        let mut waiting = Box::pin(budget.reserve_delivery_after(ip(2), UNIT, false, None, || {
            assert_eq!(budget.total.available_permits(), 0);
            assert_eq!(budget.pending_bytes.available_permits(), 0);
            drop(permit);
        }));
        std::future::poll_fn(|cx| {
            assert!(std::future::Future::poll(waiting.as_mut(), cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        assert_eq!(assembly.available_permits(), 1);
        let permit = assembly.clone().acquire_owned().await.unwrap();
        let called = std::sync::atomic::AtomicBool::new(false);
        assert!(
            budget
                .reserve_delivery_after(ip(3), UNIT, false, None, || {
                    called.store(true, std::sync::atomic::Ordering::Relaxed);
                    drop(permit);
                })
                .await
                .is_err()
        );
        assert!(!called.load(std::sync::atomic::Ordering::Relaxed));
        assert_eq!(assembly.available_permits(), 1);
        drop(waiting);
        assert_eq!(budget.pending_bytes.available_permits(), 1);
        assert_eq!(budget.total.available_permits(), 0);
        drop(active);
    }

    #[tokio::test(start_paused = true)]
    async fn aged_large_waiters_keep_headroom_and_cancellation_refunds_partial_admission() {
        let (mut budget, _) = counters(1);
        budget.client_units = 4;
        budget.total = Arc::new(Semaphore::new(4));
        budget.pending_bytes = Arc::new(Semaphore::new(4));
        let held = budget.total.clone().acquire_many_owned(3).await.unwrap();
        let mut waiting = Box::pin(budget.reserve_delivery(ip(1), 4 * UNIT, false, None));
        std::future::poll_fn(|cx| {
            assert!(std::future::Future::poll(waiting.as_mut(), cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        drop(budget.reserve(ip(2), UNIT).unwrap());
        tokio::time::advance(crate::delivery_control::PRESSURE_TICK).await;
        std::future::poll_fn(|cx| {
            assert!(std::future::Future::poll(waiting.as_mut(), cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        assert_eq!(budget.total.available_permits(), 0);
        assert_eq!(
            budget.reserve(ip(2), UNIT).err(),
            Some(StatusCode::SERVICE_UNAVAILABLE)
        );
        drop(waiting);
        assert_eq!(budget.total.available_permits(), 1);
        assert_eq!(budget.pending_bytes.available_permits(), 4);
        assert_eq!(budget.waiting.available_permits(), 16);
        let mut waiting = Box::pin(budget.reserve_delivery(ip(1), 4 * UNIT, false, None));
        std::future::poll_fn(|cx| {
            assert!(std::future::Future::poll(waiting.as_mut(), cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        drop(held);
        let active = waiting.await.unwrap();
        assert_eq!(budget.total.available_permits(), 0);
        assert_eq!(budget.pending_bytes.available_permits(), 4);
        drop(active);
        assert_eq!(budget.total.available_permits(), 4);
    }
    fn ip(n: u8) -> IpAddr {
        format!("2001:db8:1:{n}::1").parse().unwrap()
    }
    fn counters(object_bytes: u64) -> (Budget, Arc<super::super::ArtworkCounters>) {
        let counters = Arc::new(super::super::ArtworkCounters::default());
        (Budget::new(object_bytes, counters.clone()), counters)
    }
    fn snapshot(
        counters: Arc<super::super::ArtworkCounters>,
    ) -> super::super::ArtworkMetricsSnapshot {
        super::super::ArtworkMetricsHandle { counters }.snapshot()
    }
    #[test]
    fn twelve_prefixes_share_a_charged_large_body_without_global_double_counting() {
        let (budget, counters) = counters(96 << 20);
        let size = 68 << 20;
        let held: Vec<_> = (1..=12)
            .map(|n| budget.reserve_accounted(ip(n), size).unwrap())
            .collect();
        assert_eq!(
            budget.total.available_permits(),
            budget.client_units as usize * 8
        );
        assert_eq!(snapshot(counters.clone()).delivery_global_bytes_full, 0);
        assert_eq!(
            budget.reserve_accounted(ip(1), size).err(),
            Some(StatusCode::TOO_MANY_REQUESTS)
        );
        assert_eq!(snapshot(counters).delivery_client_bytes_full, 1);
        drop(held);
        drop(budget.reserve_accounted(ip(1), size).unwrap());
    }

    #[test]
    fn five_stalled_prefixes_leave_capacity_for_complete_artwork() {
        let (budget, _) = counters(32 << 20);
        let mut held = vec![];
        for n in 1..=5 {
            held.push(budget.reserve(ip(n), 32 << 20).unwrap());
            held.push(budget.reserve(ip(n), 32 << 20).unwrap());
            assert_eq!(
                budget.reserve(ip(n), 1).err(),
                Some(StatusCode::TOO_MANY_REQUESTS)
            );
        }
        drop(budget.reserve(ip(6), 32 << 20).unwrap());
        drop(held);
        assert_eq!(
            budget.total.available_permits(),
            (512 << 20) / UNIT as usize
        );
    }
    #[test]
    fn reservations_follow_last_buffer_reference_and_failed_global_admission_refunds() {
        let (budget, _) = counters(32 << 20);
        let held: Vec<_> = (0..8)
            .map(|n| budget.reserve(ip(n), 64 << 20).unwrap())
            .collect();
        assert_eq!(
            budget.reserve(ip(9), 32 << 20).err(),
            Some(StatusCode::SERVICE_UNAVAILABLE)
        );
        let last = held[0].clone();
        drop(held);
        assert_eq!(
            budget.total.available_permits(),
            (448 << 20) / UNIT as usize
        );
        drop(last);
        let recovered = budget.reserve(ip(9), 64 << 20).unwrap();
        drop(recovered);
        assert_eq!(
            budget.total.available_permits(),
            (512 << 20) / UNIT as usize
        );
    }
    #[test]
    fn larger_configured_objects_keep_a_full_object_per_prefix_and_bounded_tables() {
        let (budget, _) = counters(256 << 20);
        drop(budget.reserve(ip(1), 256 << 20).unwrap());
        assert_eq!(
            budget.reserve(ip(1), u64::MAX).err(),
            Some(StatusCode::BAD_GATEWAY)
        );
        for n in 0..MAX_CLIENTS + 1 {
            drop(
                budget
                    .reserve(std::net::Ipv4Addr::from(n as u32).into(), 1)
                    .unwrap(),
            );
        }
        assert!(budget.clients.lock().unwrap().len() <= MAX_CLIENTS);
    }

    #[test]
    fn active_client_table_rejection_counts_once_and_recovers_after_release() {
        let (budget, counters) = counters(32 << 20);
        let address = |n: usize| std::net::Ipv4Addr::from(0x0a00_0000u32 + n as u32).into();
        let mut held: Vec<_> = (0..MAX_CLIENTS)
            .map(|n| budget.reserve(address(n), 1).unwrap())
            .collect();
        assert_eq!(budget.clients.lock().unwrap().len(), MAX_CLIENTS);

        assert_eq!(
            budget.reserve(address(MAX_CLIENTS), 1).err(),
            Some(StatusCode::SERVICE_UNAVAILABLE)
        );
        assert_eq!(snapshot(counters.clone()).delivery_client_table_full, 1);

        let existing = budget.reserve(address(0), 1).unwrap();
        assert_eq!(snapshot(counters.clone()).delivery_client_table_full, 1);
        drop(existing);

        drop(held.pop());
        let new_client = budget.reserve(address(MAX_CLIENTS), 1).unwrap();
        assert_eq!(snapshot(counters.clone()).delivery_client_table_full, 1);
        drop(new_client);
    }

    #[test]
    fn full_delivery_byte_limits_count_the_specific_limiter_and_recover_without_extra_counts() {
        let (budget, counters) = counters(32 << 20);
        let per_client = u64::from(budget.client_units) * UNIT;
        let held = budget.reserve(ip(1), per_client).unwrap();
        assert_eq!(
            budget.reserve(ip(1), 1).err(),
            Some(StatusCode::TOO_MANY_REQUESTS)
        );
        assert_eq!(snapshot(counters.clone()).delivery_client_bytes_full, 1);
        drop(held);
        drop(budget.reserve(ip(1), 1).unwrap());
        assert_eq!(snapshot(counters.clone()).delivery_client_bytes_full, 1);

        let mut held = Vec::new();
        for n in 2..=9 {
            held.push(budget.reserve(ip(n), per_client).unwrap());
        }
        assert_eq!(
            budget.reserve(ip(10), 1).err(),
            Some(StatusCode::SERVICE_UNAVAILABLE)
        );
        let observed = snapshot(counters.clone());
        assert_eq!(observed.delivery_global_bytes_full, 1);
        assert_eq!(observed.delivery_client_bytes_full, 1);
        drop(held);
        drop(budget.reserve(ip(10), 1).unwrap());
        assert_eq!(snapshot(counters).delivery_global_bytes_full, 1);
    }
}
