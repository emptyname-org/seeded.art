use std::sync::atomic::{AtomicU64, Ordering};

use serde::Serialize;

use super::cache::CacheStats;

/// A content-store metrics snapshot. Disk measurements are absent when the
/// disk cache is unavailable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ContentMetrics {
    pub cache: CacheMetrics,
    pub mirror: MirrorMetrics,
    pub local_delivery: LocalDeliveryMetrics,
    pub local_scratch: LocalDeliveryMetrics,
    pub local_reads: LocalReadMetrics,
    pub resolution: ResolutionMetrics,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CacheMetrics {
    /// Successful local lookups, including memory, mirror, and disk reads.
    pub hits: u64,
    /// Validated local lookups that found no body.
    pub misses: u64,
    /// Content HTTP attempts after acquiring a request permit.
    pub upstream_fetches: u64,
    /// Failed content HTTP attempts and non-2xx statuses except 404.
    pub upstream_errors: u64,
    /// Verified upstream bodies not retained because scratch or copy workers were full.
    pub retention_skipped: u64,
    pub evictions: Option<u64>,
    pub bytes_used: Option<u64>,
    pub bytes_quota: Option<u64>,
    pub entries: Option<u64>,
    pub enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MirrorMetrics {
    /// Successful direct reads from the preservation mirror.
    pub hits: u64,
}

/// Byte-pool usage and bounded admission waits.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LocalDeliveryMetrics {
    pub reclaim_scans: u64,
    pub reclaimed: u64,
    pub reclaim_no_victim: u64,
    pub bytes_used: u64,
    pub bytes_quota: u64,
    pub waiting: u64,
    pub wait_capacity: u64,
    pub waits: u64,
    pub timeouts: u64,
    pub rejected: u64,
    pub leader_waits: u64,
    pub leader_timeouts: u64,
    pub leader_rejected: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LocalReadMetrics {
    pub active: u64,
    pub waiting: u64,
    pub wait_capacity: u64,
    pub waits: u64,
    pub timeouts: u64,
    pub rejected: u64,
    pub deadline_expired: u64,
    pub shared_failures: u64,
    pub coalesced: u64,
    pub coordination_rejected: u64,
    pub leader_waiting: u64,
    pub leader_wait_capacity: u64,
    pub leader_waits: u64,
    pub leader_timeouts: u64,
    pub leader_rejected: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ResolutionMetrics {
    pub upstream_failures: u64,
    pub local_failures: u64,
    pub upstream_other_failures: u64,
    pub local_other_failures: u64,
    pub upstream_busy: u64,
    pub upstream_timeouts: u64,
    pub mirror_busy: u64,
}

#[derive(Default)]
pub(super) struct ContentCounters {
    pub cache_hits: AtomicU64,
    pub cache_misses: AtomicU64,
    pub upstream_fetches: AtomicU64,
    pub upstream_errors: AtomicU64,
    pub mirror_hits: AtomicU64,
    pub upstream_failures: AtomicU64,
    pub local_failures: AtomicU64,
    pub upstream_other_failures: AtomicU64,
    pub local_other_failures: AtomicU64,
    pub upstream_busy: AtomicU64,
    pub upstream_timeouts: AtomicU64,
    pub local_deadline_expired: AtomicU64,
    pub local_shared_failures: AtomicU64,
    pub local_leader_deadline_expired: AtomicU64,
    pub local_coalesced: AtomicU64,
    pub local_coordination_rejected: AtomicU64,
    pub retention_skipped: AtomicU64,
}

impl ContentCounters {
    pub(super) fn snapshot(
        &self,
        enabled: bool,
        disk: Option<CacheStats>,
        local_delivery: LocalDeliveryMetrics,
        local_scratch: LocalDeliveryMetrics,
        local_reads: LocalReadMetrics,
        mirror_busy: u64,
    ) -> ContentMetrics {
        ContentMetrics {
            local_delivery,
            local_scratch,
            local_reads,
            resolution: ResolutionMetrics {
                upstream_failures: self.upstream_failures.load(Ordering::Relaxed),
                local_failures: self.local_failures.load(Ordering::Relaxed),
                upstream_other_failures: self.upstream_other_failures.load(Ordering::Relaxed),
                local_other_failures: self.local_other_failures.load(Ordering::Relaxed),
                upstream_busy: self.upstream_busy.load(Ordering::Relaxed),
                upstream_timeouts: self.upstream_timeouts.load(Ordering::Relaxed),
                mirror_busy,
            },
            cache: CacheMetrics {
                hits: self.cache_hits.load(Ordering::Relaxed),
                misses: self.cache_misses.load(Ordering::Relaxed),
                upstream_fetches: self.upstream_fetches.load(Ordering::Relaxed),
                upstream_errors: self.upstream_errors.load(Ordering::Relaxed),
                retention_skipped: self.retention_skipped.load(Ordering::Relaxed),
                evictions: disk.as_ref().map(|stats| stats.evictions),
                bytes_used: disk.as_ref().map(|stats| stats.bytes_used),
                bytes_quota: disk.as_ref().map(|stats| stats.bytes_quota),
                entries: disk
                    .as_ref()
                    .map(|stats| u64::try_from(stats.entries).unwrap_or(u64::MAX)),
                enabled,
            },
            mirror: MirrorMetrics {
                hits: self.mirror_hits.load(Ordering::Relaxed),
            },
        }
    }
}
