#!/usr/bin/env python3
"""Small VPS monitor: per-minute aggregates, alerts and load-test reports.

Stdlib only. Stores aggregates only: no addresses, paths, queries or bodies.
"""

import argparse
import datetime
import json
import math
import os
import re
import signal
import subprocess
import sys
import threading
import time
import urllib.request
from collections import Counter, deque
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field, replace
from pathlib import Path
from types import FrameType
from typing import Any

ENDPOINTS = {
    "app": "http://127.0.0.1:3918/health/metrics",
    "artwork": "http://127.0.0.1:3919/health/metrics",
}
UNITS = (
    "seeded",
    "seeded-app",
    "seeded-content",
    "seeded-proxy",
    "php8.4-fpm",
    "mariadb",
)
# Scheduled one-shot jobs: idle ("inactive") between runs, so only a failed run alerts.
JOB_UNITS = (
    "seeded-sync",
    "seeded-sales",
    "seeded-articles",
    "seeded-profiles",
    "seeded-verify",
    "seeded-backup",
    "seeded-forum-backup",
)
DISK_PATH = "/var/lib/seeded"
CGROUP_ROOT = Path("/sys/fs/cgroup")
HTTP_TIMEOUT = 3.0
SYSTEMCTL_TIMEOUT = 5.0
MAX_BODY = 1 << 20
KEEP_DAYS = 14
MAX_TOTAL_BYTES = 256 * 1024 * 1024
WINDOW_MINUTES = 5
MIN_WINDOW_REQUESTS = 20
DOWN_SECONDS = 120.0
ALERT_INTERVAL = 900.0
SHARE_5XX = 0.02
SHARE_503 = 0.01
P95_LIMIT_MS = 2000.0
MEMORY_RATIO = 0.9
DISK_FREE_MIN_PCT = 10.0
ROUTES = ("page", "api", "static", "render", "content", "preview", "other")
REJECTIONS = (
    "rate_limited",
    "admission_full",
    "upstream_busy",
    "delivery_budget",
    "timeout",
    "db_waiting_full",
)
CACHE_COUNTERS = (
    "hits",
    "misses",
    "upstream_fetches",
    "upstream_errors",
    "evictions",
    "retention_skipped",
)
CACHE_GAUGES = ("bytes_used", "bytes_quota", "entries")
LOCAL_COUNTERS = (
    "waits",
    "timeouts",
    "rejected",
    "leader_waits",
    "leader_timeouts",
    "leader_rejected",
    "reclaim_scans",
    "reclaimed",
    "reclaim_no_victim",
)
READ_COUNTERS = (
    "waits",
    "timeouts",
    "rejected",
    "deadline_expired",
    "coalesced",
    "coordination_rejected",
    "shared_failures",
    "leader_waits",
    "leader_timeouts",
    "leader_rejected",
)
READ_GAUGES = (
    "active",
    "waiting",
    "wait_capacity",
    "leader_waiting",
    "leader_wait_capacity",
)
RESOLUTION = (
    "upstream_failures",
    "local_failures",
    "upstream_other_failures",
    "local_other_failures",
    "upstream_busy",
    "upstream_timeouts",
    "mirror_busy",
)
DATABASE_ADMISSION = (
    "waiting_full",
    "waiting_timeout",
    "query_timeout",
    "query_unavailable",
)
REQUEST_LIMITS = (
    "global_content_wait_full",
    "client_content_wait_full",
    "content_wait_timeout",
    "noncontent_bucket_rejected",
    "family_new_client_rejected",
    "noncontent_stream_timeout",
    "client_table_full",
    "missing_client_ip",
)
LOCAL_GAUGES = ("bytes_used", "bytes_quota", "waiting", "wait_capacity")
# Byte pools with the same schema: metrics key -> counter prefix.
POOLS = {"local_delivery": "local", "local_scratch": "scratch"}
ADMISSION = (
    "metadata_waiting_full",
    "metadata_waiting_timeout",
    "upstream_waiting_full",
    "upstream_waiting_timeout",
    "transform_timeout",
    "delivery_global_bytes_full",
    "delivery_client_bytes_full",
    "metadata_query_timeout",
    "metadata_query_unavailable",
    "handler_timeout",
    "delivery_client_table_full",
    "delivery_reclaim_scans",
    "delivery_reclaimed",
    "delivery_reclaim_no_victim",
)
FILE_NAME = re.compile(r"metrics-([0-9]{8})\.jsonl")
CLASS_KEY = re.compile(r"[1-5]xx")
STATUS_KEY = re.compile(r"[1-5][0-9]{2}")
MAX_BUCKETS = 256
UNSET = 2**64 - 1
PEAK_GAUGES = frozenset(
    {
        "active.requests",
        "db.reads_active",
        "db.reads_waiting",
        "local.bytes_used",
        "local.waiting",
        "scratch.bytes_used",
        "scratch.waiting",
        "reads.active",
        "reads.waiting",
        "reads.leader_waiting",
    }
)


def _count(value: object) -> int:
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return max(value, 0)
    if isinstance(value, float) and math.isfinite(value):
        return max(int(value), 0)
    return 0


def _obj(value: object) -> dict[str, Any]:
    if isinstance(value, dict):
        return {key: item for key, item in value.items() if isinstance(key, str)}
    return {}


@dataclass(frozen=True)
class Sample:
    start: str
    counters: dict[str, int]
    gauges: dict[str, int]
    bounds: tuple[float, ...]
    counts: tuple[int, ...]


def _histogram(raw: object) -> tuple[tuple[float, ...], tuple[int, ...]]:
    latency = _obj(raw)
    bounds_raw = latency.get("buckets")
    counts_raw = latency.get("counts")
    if not isinstance(bounds_raw, list) or not isinstance(counts_raw, list):
        return (), ()
    if not 0 < len(bounds_raw) <= MAX_BUCKETS:
        return (), ()
    bounds: list[float] = []
    for item in bounds_raw:
        if (
            isinstance(item, bool)
            or not isinstance(item, (int, float))
            or not math.isfinite(item)
            or item <= 0
            or (bounds and item <= bounds[-1])
        ):
            return (), ()
        bounds.append(float(item))
    if len(counts_raw) not in (len(bounds), len(bounds) + 1):
        return (), ()
    return tuple(bounds), tuple(_count(item) for item in counts_raw)


def _opt(value: object) -> int | None:
    """A measured non-negative count, or None when missing, null or mistyped."""
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        return value if value >= 0 else None
    if isinstance(value, float) and math.isfinite(value) and value >= 0:
        return int(value)
    return None


def _put(target: dict[str, int], key: str, value: object) -> None:
    number = _opt(value)
    if number is not None:
        target[key] = number


def parse_metrics(raw: bytes) -> Sample | None:
    """Defensive schema v1 parse. Unknown fields are ignored; None means unusable.

    Missing or null fields stay absent from the sample: unknown, never zero.
    """
    try:
        root = json.loads(raw)
    except (ValueError, RecursionError):
        return None
    root = _obj(root)
    if root.get("schema") != 1:
        return None
    start = root.get("process_start")
    counters: dict[str, int] = {}
    gauges: dict[str, int] = {}
    requests = _obj(root.get("requests"))
    _put(counters, "requests.total", requests.get("total"))
    _put(counters, "requests.cancelled", requests.get("cancelled_before_headers"))
    for key, value in list(_obj(requests.get("by_class")).items())[:16]:
        if CLASS_KEY.fullmatch(key):
            _put(counters, f"class.{key}", value)
    for key, value in list(_obj(requests.get("by_status")).items())[:64]:
        if STATUS_KEY.fullmatch(key):
            _put(counters, f"status.{key}", value)
    routes = _obj(requests.get("by_route"))
    for name in ROUTES:
        _put(counters, f"route.{name}", routes.get(name))
    rejections = _obj(root.get("rejections"))
    for name in REJECTIONS:
        _put(counters, f"rej.{name}", rejections.get(name))
    cache = _obj(root.get("cache"))
    for name in CACHE_COUNTERS:
        _put(counters, f"cache.{name}", cache.get(name))
    for name in CACHE_GAUGES:
        _put(gauges, f"cache.{name}", cache.get(name))
    _put(counters, "mirror.hits", _obj(root.get("mirror")).get("hits"))
    for key, prefix in POOLS.items():
        pool = _obj(root.get(key))
        for name in LOCAL_COUNTERS:
            _put(counters, f"{prefix}.{name}", pool.get(name))
        for name in LOCAL_GAUGES:
            _put(gauges, f"{prefix}.{name}", pool.get(name))
    admission = _obj(root.get("artwork_admission"))
    for name in ADMISSION:
        _put(counters, f"adm.{name}", admission.get(name))
    reads = _obj(root.get("local_reads"))
    for name in READ_COUNTERS:
        _put(counters, f"reads.{name}", reads.get(name))
    for name in READ_GAUGES:
        _put(gauges, f"reads.{name}", reads.get(name))
    resolution = _obj(root.get("resolution"))
    for name in RESOLUTION:
        _put(counters, f"res.{name}", resolution.get(name))
    database = _obj(root.get("database_admission"))
    for name in DATABASE_ADMISSION:
        _put(counters, f"dbadm.{name}", database.get(name))
    limits = _obj(root.get("request_limits"))
    for name in REQUEST_LIMITS:
        _put(counters, f"rl.{name}", limits.get(name))
    body = _obj(root.get("body"))
    _put(counters, "body.errors", body.get("errors"))
    _put(counters, "body.bytes_emitted", body.get("bytes_emitted"))
    latency = _obj(root.get("latency_ms"))
    _put(counters, "lat.sum_ms", latency.get("sum_ms"))
    _put(counters, "lat.count", latency.get("count"))
    active = _obj(root.get("active"))
    _put(gauges, "active.requests", active.get("requests"))
    _put(gauges, "active.max_seen", active.get("max_seen"))
    database = _obj(root.get("db"))
    _put(gauges, "db.reads_active", database.get("reads_active"))
    _put(gauges, "db.reads_waiting", database.get("reads_waiting"))
    bounds, counts = _histogram(latency)
    return Sample(
        start if isinstance(start, str) else "", counters, gauges, bounds, counts
    )


@dataclass(frozen=True)
class Delta:
    counters: dict[str, int]
    gauges: dict[str, int]
    bounds: tuple[float, ...]
    counts: tuple[int, ...]
    reset: bool


class Tracker:
    """Per-interval deltas for one endpoint; resets when the process restarts.

    Counters without a baseline (first sample, newly appearing) stay unknown.
    """

    def __init__(self) -> None:
        self.previous: Sample | None = None

    def update(self, sample: Sample) -> Delta:
        previous, self.previous = self.previous, sample
        if previous is None:
            return Delta({}, sample.gauges, sample.bounds, (), False)
        restarted = (
            previous.start != sample.start
            or previous.bounds != sample.bounds
            or len(previous.counts) != len(sample.counts)
            or any(
                value < previous.counters[key]
                for key, value in sample.counters.items()
                if key in previous.counters
            )
            or any(new < old for new, old in zip(sample.counts, previous.counts))
        )
        if restarted:
            return Delta(
                dict(sample.counters), sample.gauges, sample.bounds, sample.counts, True
            )
        counters = {
            key: value - previous.counters[key]
            for key, value in sample.counters.items()
            if key in previous.counters
        }
        counts = tuple(new - old for new, old in zip(sample.counts, previous.counts))
        return Delta(counters, sample.gauges, sample.bounds, counts, False)


def percentile(
    bounds: Iterable[float], counts: Iterable[int], quantile: float
) -> float | None:
    """Estimate by linear interpolation in the bucket. The overflow bucket reports its lower bound."""
    edges = list(bounds)
    values = list(counts)
    total = sum(values)
    if total <= 0 or not edges or len(values) not in (len(edges), len(edges) + 1):
        return None
    rank = quantile * total
    seen = 0
    for index, count in enumerate(values):
        if count <= 0:
            continue
        if seen + count >= rank:
            low = edges[index - 1] if index > 0 else 0.0
            if index >= len(edges):
                return round(low, 1)
            return round(low + (edges[index] - low) * (rank - seen) / count, 1)
        seen += count
    return round(edges[-1], 1)


def fetch_metrics(url: str) -> Sample | None:
    """One GET; any failure is a down sample."""
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(url, timeout=HTTP_TIMEOUT) as response:
            if response.status != 200:
                return None
            return parse_metrics(response.read(MAX_BODY))
    except (OSError, ValueError):
        return None


@dataclass(frozen=True)
class UnitSample:
    state: str
    cpu_ns: int | None
    memory: int | None
    memory_high: int | None
    memory_max: int | None
    file_state: str | None = None
    control_group: str | None = None


# Deliberately off: a stopped alternative (combined vs split units) is not an outage.
OFF_FILE_STATES = frozenset({"disabled", "masked"})


def _unit_number(value: str | None) -> int | None:
    if value is None or not value.isdigit():
        return None
    number = int(value)
    return None if number >= UNSET else number


def parse_units(text: str) -> dict[str, UnitSample]:
    found: dict[str, UnitSample] = {}
    for block in text.split("\n\n"):
        fields = dict(line.split("=", 1) for line in block.splitlines() if "=" in line)
        name = fields.get("Id", "").removesuffix(".service")
        if name not in UNITS + JOB_UNITS or fields.get("LoadState") == "not-found":
            continue
        found[name] = UnitSample(
            fields.get("ActiveState", "unknown"),
            _unit_number(fields.get("CPUUsageNSec")),
            _unit_number(fields.get("MemoryCurrent")),
            _unit_number(fields.get("MemoryHigh")),
            _unit_number(fields.get("MemoryMax")),
            fields.get("UnitFileState") or None,
            fields.get("ControlGroup") or None,
        )
    return found


def working_set(unit: UnitSample, root: Path = CGROUP_ROOT) -> UnitSample:
    """Memory less inactive page cache, which the kernel reclaims first at MemoryHigh."""
    if unit.memory is None or not unit.control_group:
        return unit
    try:
        stat = (root / unit.control_group.lstrip("/") / "memory.stat").read_text()
    except OSError:
        return unit
    for line in stat.splitlines():
        key, _, value = line.partition(" ")
        if key == "inactive_file" and value.isdigit():
            return replace(unit, memory=max(0, unit.memory - int(value)))
    return unit


def sample_units() -> dict[str, UnitSample]:
    properties = (
        "Id,LoadState,ActiveState,UnitFileState,"
        "MemoryCurrent,MemoryHigh,MemoryMax,CPUUsageNSec,ControlGroup"
    )
    try:
        result = subprocess.run(
            ["systemctl", "show", "-p", properties, *UNITS, *JOB_UNITS],
            capture_output=True,
            text=True,
            timeout=SYSTEMCTL_TIMEOUT,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return {}
    if result.returncode != 0:
        return {}
    return {
        name: working_set(unit) for name, unit in parse_units(result.stdout).items()
    }


def disk_free(path: str = DISK_PATH) -> dict[str, float] | None:
    try:
        stat = os.statvfs(path)
    except OSError:
        return None
    total = stat.f_blocks * stat.f_frsize
    free = stat.f_bavail * stat.f_frsize
    return {
        "free_bytes": free,
        "total_bytes": total,
        "free_pct": round(100.0 * free / total, 2) if total else 0.0,
    }


def merge_histograms(
    records: Iterable[Mapping[str, Any]],
) -> tuple[list[float], list[int]]:
    """Sum record histograms that share the newest bucket layout."""
    bounds: list[float] = []
    counts: list[int] = []
    for record in records:
        latency = _obj(record.get("latency_ms"))
        record_bounds = latency.get("bounds")
        record_counts = latency.get("counts")
        if not isinstance(record_bounds, list) or not isinstance(record_counts, list):
            continue
        if record_bounds != bounds:
            bounds, counts = list(record_bounds), [0] * len(record_counts)
        if len(record_counts) != len(counts):
            continue
        counts = [old + _count(new) for old, new in zip(counts, record_counts)]
    return bounds, counts


@dataclass
class OriginAgg:
    up: int = 0
    down: int = 0
    counters: dict[str, int] = field(default_factory=dict)
    bounds: tuple[float, ...] = ()
    counts: list[int] = field(default_factory=list)
    peaks: dict[str, int] = field(default_factory=dict)
    last: dict[str, int] = field(default_factory=dict)

    def add(self, delta: Delta) -> None:
        self.up += 1
        for key, value in delta.counters.items():
            self.counters[key] = self.counters.get(key, 0) + value
        if delta.counts:
            if delta.bounds != self.bounds:
                self.bounds, self.counts = delta.bounds, [0] * len(delta.counts)
            self.counts = [a + b for a, b in zip(self.counts, delta.counts)]
        for key, value in delta.gauges.items():
            if key in PEAK_GAUGES:
                self.peaks[key] = max(self.peaks.get(key, 0), value)
            else:
                self.last[key] = value

    def record(self) -> dict[str, Any]:
        """Unknown values are null or omitted from a group, never zero."""
        counters = self.counters

        def group(
            prefix: str, only: Iterable[str] | None = None
        ) -> dict[str, int] | None:
            found = {
                key[len(prefix) :]: value
                for key, value in sorted(counters.items())
                if key.startswith(prefix)
                and (only is None or key[len(prefix) :] in only)
            }
            return found or None

        def peak(key: str) -> int | None:
            return self.peaks.get(key)

        hits, misses = counters.get("cache.hits"), counters.get("cache.misses")
        cache: dict[str, Any] = {
            name: counters.get(f"cache.{name}") for name in CACHE_COUNTERS
        }
        for name in CACHE_GAUGES:
            cache[name] = self.last.get(f"cache.{name}")
        cache["hit_ratio"] = (
            round(hits / (hits + misses), 4)
            if hits is not None and misses is not None and hits + misses
            else None
        )
        quantiles = {
            f"p{q}": percentile(self.bounds, self.counts, q / 100) for q in (50, 95, 99)
        }

        def pool(prefix: str) -> dict[str, Any] | None:
            found: dict[str, Any] = {
                name: counters.get(f"{prefix}.{name}") for name in LOCAL_COUNTERS
            }
            found["bytes_used_max"] = peak(f"{prefix}.bytes_used")
            found["waiting_max"] = peak(f"{prefix}.waiting")
            found["bytes_quota"] = self.last.get(f"{prefix}.bytes_quota")
            found["wait_capacity"] = self.last.get(f"{prefix}.wait_capacity")
            return found if any(v is not None for v in found.values()) else None

        def local_reads() -> dict[str, Any] | None:
            found: dict[str, Any] = {
                name: counters.get(f"reads.{name}") for name in READ_COUNTERS
            }
            found["active_max"] = peak("reads.active")
            found["waiting_max"] = peak("reads.waiting")
            found["wait_capacity"] = self.last.get("reads.wait_capacity")
            found["leader_waiting_max"] = peak("reads.leader_waiting")
            found["leader_wait_capacity"] = self.last.get("reads.leader_wait_capacity")
            return found if any(v is not None for v in found.values()) else None

        return {
            "up": self.up,
            "down": self.down,
            "requests": counters.get("requests.total"),
            "cancelled": counters.get("requests.cancelled"),
            "by_class": group("class."),
            "by_status": group("status."),
            "by_route": group("route."),
            "rejections": group("rej."),
            "max_active": peak("active.requests"),
            "max_seen": self.last.get("active.max_seen"),
            "latency_ms": {
                "bounds": list(self.bounds),
                "counts": self.counts,
                "count": counters.get("lat.count"),
                "sum_ms": counters.get("lat.sum_ms"),
                **quantiles,
            },
            "body": {
                "errors": counters.get("body.errors"),
                "bytes_emitted": counters.get("body.bytes_emitted"),
            },
            "cache": cache,
            "mirror_hits": counters.get("mirror.hits"),
            **{key: pool(prefix) for key, prefix in POOLS.items()},
            "admission": group("adm."),
            "local_reads": local_reads(),
            "resolution": group("res."),
            "request_limits": group("rl."),
            "database_admission": group("dbadm."),
            "db": {
                "reads_active_max": peak("db.reads_active"),
                "reads_waiting_max": peak("db.reads_waiting"),
            },
        }


@dataclass
class UnitAgg:
    state: str = "unknown"
    file_state: str | None = None
    cpu: list[float] = field(default_factory=list)
    memory_max: int | None = None
    memory_high: int | None = None
    memory_limit: int | None = None

    def record(self) -> dict[str, Any]:
        return {
            "state": self.state,
            "file_state": self.file_state,
            "cpu_avg": round(sum(self.cpu) / len(self.cpu), 2) if self.cpu else None,
            "cpu_max": round(max(self.cpu), 2) if self.cpu else None,
            "mem_max": self.memory_max,
            "mem_high": self.memory_high,
            "mem_limit": self.memory_limit,
        }


def _file_day(text: str) -> datetime.date:
    return datetime.date(int(text[:4]), int(text[4:6]), int(text[6:]))


def _iso(epoch: float) -> str:
    moment = datetime.datetime.fromtimestamp(epoch, datetime.timezone.utc)
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


def _share_window(window: list[dict[str, Any]], origin: str) -> tuple[int, int, int]:
    total = five = busy = 0
    for record in window:
        data = _obj(_obj(record.get("origins")).get(origin))
        total += _count(data.get("requests"))
        five += _count(_obj(data.get("by_class")).get("5xx"))
        busy += _count(_obj(data.get("by_status")).get("503"))
    return total, five, busy


def conditions(
    window: list[dict[str, Any]], down_for: Mapping[str, float]
) -> dict[str, str]:
    """Alert kinds that currently hold, with short reasons."""
    found: dict[str, str] = {}
    origins = {name for record in window for name in _obj(record.get("origins"))}
    for origin in sorted(origins):
        total, five, busy = _share_window(window, origin)
        if total >= MIN_WINDOW_REQUESTS:
            if five / total > SHARE_5XX:
                found[f"5xx:{origin}"] = f"{origin} 5xx share {100 * five / total:.1f}%"
            if busy / total > SHARE_503:
                found[f"503:{origin}"] = f"{origin} 503 share {100 * busy / total:.1f}%"
        records = [_obj(_obj(record.get("origins")).get(origin)) for record in window]
        bounds, counts = merge_histograms(records)
        p95 = percentile(bounds, counts, 0.95)
        # The overflow bucket reports its lower bound, so equality counts as over.
        if (
            p95 is not None
            and p95 >= P95_LIMIT_MS
            and sum(counts) >= MIN_WINDOW_REQUESTS
        ):
            found[f"p95:{origin}"] = f"{origin} p95 {p95:.0f} ms"
    for origin, seconds in sorted(down_for.items()):
        if seconds > DOWN_SECONDS:
            found[f"down:{origin}"] = f"{origin} metrics down {seconds / 60:.0f} min"
    for record in window:
        for name, unit in _obj(record.get("units")).items():
            data = _obj(unit)
            high = _count(data.get("mem_high"))
            if high and _count(data.get("mem_max")) > MEMORY_RATIO * high:
                used = 100 * _count(data.get("mem_max")) / high
                found[f"memory:{name}"] = f"{name} memory {used:.0f}% of high"
    if window:
        latest = window[-1]
        for name, unit in sorted(_obj(latest.get("units")).items()):
            state = _obj(unit).get("state")
            if name in JOB_UNITS:
                if state == "failed":
                    found[f"unit:{name}"] = f"{name} failed"
            elif (
                state != "active"
                and _obj(unit).get("file_state") not in OFF_FILE_STATES
            ):
                found[f"unit:{name}"] = f"{name} {state}"
        free = _obj(latest.get("disk")).get("free_pct")
        if isinstance(free, (int, float)) and free < DISK_FREE_MIN_PCT:
            found["disk"] = f"disk free {free:.1f}%"
    return found


def emit_stderr(line: str) -> None:
    sys.stderr.write(line + "\n")
    sys.stderr.flush()


class Alerter:
    """Rate-limits each kind to one line per 15 minutes and logs recovery once."""

    def __init__(self, emit: Callable[[str], None] = emit_stderr) -> None:
        self.emit = emit
        self.last: dict[str, float] = {}
        self.active: set[str] = set()

    def update(self, found: Mapping[str, str], now: float) -> None:
        for kind, reason in found.items():
            self.active.add(kind)
            last = self.last.get(kind)
            if last is None or now - last >= ALERT_INTERVAL:
                self.last[kind] = now
                self.emit(f"<4>seeded-monitor alert: {reason}")
        for kind in sorted(self.active - set(found)):
            self.active.discard(kind)
            self.emit(f"<5>seeded-monitor recovered: {kind}")


def prune(
    state: Path,
    now: float,
    keep_days: int = KEEP_DAYS,
    max_bytes: int = MAX_TOTAL_BYTES,
) -> None:
    """Delete metrics files older than keep_days, then oldest until under max_bytes."""
    today = datetime.datetime.fromtimestamp(now, datetime.timezone.utc).date()
    cutoff = today - datetime.timedelta(days=keep_days)
    files: list[tuple[str, Path, int]] = []
    try:
        entries = list(state.iterdir())
    except OSError:
        return
    for path in entries:
        match = FILE_NAME.fullmatch(path.name)
        if not match or path.is_symlink():
            continue
        try:
            day = _file_day(match[1])
            if day < cutoff:
                path.unlink()
            else:
                files.append((match[1], path, path.stat().st_size))
        except (OSError, ValueError):
            continue
    files.sort()
    total = sum(size for _, _, size in files)
    while total > max_bytes and len(files) > 1:
        _, path, size = files.pop(0)
        try:
            path.unlink()
        except OSError:
            continue
        total -= size


def append_line(state: Path, epoch: float, record: Mapping[str, Any]) -> None:
    day = datetime.datetime.fromtimestamp(epoch, datetime.timezone.utc)
    path = state / f"metrics-{day:%Y%m%d}.jsonl"
    flags = os.O_WRONLY | os.O_APPEND | os.O_CREAT | os.O_NOFOLLOW
    descriptor = os.open(path, flags, 0o600)
    with os.fdopen(descriptor, "a") as output:
        output.write(json.dumps(record, separators=(",", ":")) + "\n")


class Monitor:
    def __init__(
        self,
        state: Path,
        *,
        fetch: Callable[[str], Sample | None] = fetch_metrics,
        units: Callable[[], dict[str, UnitSample]] = sample_units,
        disk: Callable[[], dict[str, float] | None] = disk_free,
        load: Callable[[], tuple[float, float, float]] = os.getloadavg,
        emit: Callable[[str], None] = emit_stderr,
    ) -> None:
        self.state = state
        self.fetch, self.units, self.disk, self.load = fetch, units, disk, load
        self.alerter = Alerter(emit)
        self.trackers = {name: Tracker() for name in ENDPOINTS}
        self.down_since: dict[str, float] = {}
        self.previous_cpu: dict[str, tuple[int, float]] = {}
        self.window: deque[dict[str, Any]] = deque(maxlen=WINDOW_MINUTES)
        self.minute: int | None = None
        self.origins: dict[str, OriginAgg] = {}
        self.unit_aggs: dict[str, UnitAgg] = {}
        self.disk_now: dict[str, float] | None = None
        self.load_now: tuple[float, float, float] | None = None

    def tick(self, now: float) -> None:
        minute = int(now // 60)
        if self.minute is not None and minute != self.minute:
            self.flush(now)
        self.minute = minute
        for name, url in ENDPOINTS.items():
            agg = self.origins.setdefault(name, OriginAgg())
            sample = self.fetch(url)
            if sample is None:
                agg.down += 1
                self.down_since.setdefault(name, now)
            else:
                self.down_since.pop(name, None)
                agg.add(self.trackers[name].update(sample))
        self.sample_system(now)

    def sample_system(self, now: float) -> None:
        for name, unit in self.units().items():
            agg = self.unit_aggs.setdefault(name, UnitAgg())
            agg.state, agg.file_state = unit.state, unit.file_state
            agg.memory_high, agg.memory_limit = unit.memory_high, unit.memory_max
            if unit.memory is not None:
                agg.memory_max = max(agg.memory_max or 0, unit.memory)
            previous = self.previous_cpu.get(name)
            if unit.cpu_ns is not None:
                self.previous_cpu[name] = (unit.cpu_ns, now)
                if previous and unit.cpu_ns >= previous[0] and now > previous[1]:
                    used = (unit.cpu_ns - previous[0]) / 1e9
                    agg.cpu.append(100.0 * used / (now - previous[1]))
            else:
                self.previous_cpu.pop(name, None)
        self.disk_now = self.disk()
        try:
            self.load_now = self.load()
        except OSError:
            self.load_now = None

    def flush(self, now: float) -> None:
        if self.minute is None or not self.origins:
            return
        start = self.minute * 60
        record: dict[str, Any] = {
            "ts": _iso(start),
            "t": start,
            "origins": {name: agg.record() for name, agg in self.origins.items()},
            "units": {name: agg.record() for name, agg in self.unit_aggs.items()},
        }
        if self.disk_now is not None:
            record["disk"] = self.disk_now
        if self.load_now is not None:
            record["load"] = list(self.load_now)
        self.origins, self.unit_aggs = {}, {}
        try:
            append_line(self.state, start, record)
            prune(self.state, now)
        except OSError as error:
            emit_stderr(f"seeded-monitor error: write failed ({type(error).__name__})")
        self.window.append(record)
        down_for = {name: now - since for name, since in self.down_since.items()}
        self.alerter.update(conditions(list(self.window), down_for), now)


def run(state: Path, interval: float) -> int:
    state.mkdir(mode=0o700, parents=True, exist_ok=True)
    stop = threading.Event()

    def request_stop(_signal: int, _frame: FrameType | None) -> None:
        stop.set()

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    monitor = Monitor(state)
    deadline = time.monotonic()
    while not stop.is_set():
        try:
            monitor.tick(time.time())
        except Exception as error:  # noqa: BLE001 - the monitor must keep running
            emit_stderr(f"seeded-monitor error: {type(error).__name__}")
        deadline += interval
        stop.wait(max(deadline - time.monotonic(), 0.0))
        deadline = max(deadline, time.monotonic() - interval)
    try:
        monitor.flush(time.time())
    except Exception as error:  # noqa: BLE001
        emit_stderr(f"seeded-monitor error: {type(error).__name__}")
    return 0


def parse_time(text: str) -> float:
    moment = datetime.datetime.fromisoformat(text.replace("Z", "+00:00"))
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=datetime.timezone.utc)
    return moment.timestamp()


def read_records(state: Path, since: float, until: float) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    first = datetime.datetime.fromtimestamp(since, datetime.timezone.utc).date()
    for path in sorted(state.glob("metrics-*.jsonl")):
        match = FILE_NAME.fullmatch(path.name)
        if not match:
            continue
        day = _file_day(match[1])
        if day < first or (until != math.inf and _iso(until)[:10] < day.isoformat()):
            continue
        try:
            lines = path.read_text().splitlines()
        except OSError:
            continue
        for line in lines:
            try:
                record = json.loads(line)
            except ValueError:
                continue
            moment = _obj(record).get("t")
            if isinstance(moment, int) and since <= moment < until:
                records.append(record)
    return sorted(records, key=lambda record: record["t"])


def _sum_known(values: Iterable[object]) -> int | None:
    known = [value for value in values if isinstance(value, int)]
    return sum(known) if known else None


def _sum_maps(maps: Iterable[object]) -> dict[str, int] | None:
    totals: Counter[str] = Counter()
    seen = False
    for mapping in maps:
        if isinstance(mapping, dict):
            seen = True
            for key, value in mapping.items():
                totals[key] += _count(value)
    return dict(sorted(totals.items())) if seen else None


def _local_summary(parts: Iterable[object]) -> dict[str, Any] | None:
    return _group_summary(
        parts,
        LOCAL_COUNTERS,
        ("bytes_used_max", "waiting_max"),
        ("bytes_quota", "wait_capacity"),
    )


def _group_summary(
    parts: Iterable[object],
    summed: Iterable[str],
    maxima: Iterable[str],
    latest: Iterable[str],
) -> dict[str, Any] | None:
    """Counters summed, peaks maxed, limits from the latest minute; None if never reported."""
    known = [part for part in parts if isinstance(part, dict)]
    if not known:
        return None

    def numbers(key: str) -> list[int]:
        return [
            value
            for part in known
            if isinstance(value := part.get(key), int) and not isinstance(value, bool)
        ]

    summary: dict[str, Any] = {
        name: _sum_known(part.get(name) for part in known) for name in summed
    }
    for key in maxima:
        found = numbers(key)
        summary[key] = max(found) if found else None
    for key in latest:
        found = numbers(key)
        summary[key] = found[-1] if found else None
    return summary


def summarize(records: list[dict[str, Any]]) -> dict[str, Any]:
    """Window summary. Unknown values stay None; they are never zeros."""
    seconds = 60 * len(records)
    origins: dict[str, Any] = {}
    names = sorted({n for r in records for n in _obj(r.get("origins"))})
    for name in names:
        parts = [_obj(_obj(r.get("origins")).get(name)) for r in records]
        total = _sum_known(p.get("requests") for p in parts)
        classes = _sum_maps(p.get("by_class") for p in parts)
        statuses = _sum_maps(p.get("by_status") for p in parts)
        bounds, counts = merge_histograms(parts)
        actives = [
            p["max_active"] for p in parts if isinstance(p.get("max_active"), int)
        ]
        seen = [p["max_seen"] for p in parts if isinstance(p.get("max_seen"), int)]
        origins[name] = {
            "requests": total,
            "rps": round(total / seconds, 2) if total is not None and seconds else None,
            "classes": {k: round(v / total, 4) for k, v in classes.items()}
            if classes is not None and total
            else None,
            "share_503": round(statuses.get("503", 0) / total, 4)
            if statuses is not None and total
            else None,
            "p95": percentile(bounds, counts, 0.95),
            "p99": percentile(bounds, counts, 0.99),
            "sampled_max_active": max(actives) if actives else None,
            "lifetime_max_seen": seen[-1] if seen else None,
            "rejections": _sum_maps(p.get("rejections") for p in parts),
            **{key: _local_summary(p.get(key) for p in parts) for key in POOLS},
            "admission": _sum_maps(p.get("admission") for p in parts),
            "local_reads": _group_summary(
                (p.get("local_reads") for p in parts),
                READ_COUNTERS,
                ("active_max", "waiting_max", "leader_waiting_max"),
                ("wait_capacity", "leader_wait_capacity"),
            ),
            "cache": _group_summary(
                (p.get("cache") for p in parts),
                CACHE_COUNTERS,
                (),
                CACHE_GAUGES,
            ),
            "resolution": _sum_maps(p.get("resolution") for p in parts),
            "request_limits": _sum_maps(p.get("request_limits") for p in parts),
            "database_admission": _sum_maps(p.get("database_admission") for p in parts),
            "down_samples": sum(_count(p.get("down")) for p in parts),
        }
    units: dict[str, Any] = {}
    for record in records:
        for name, unit in _obj(record.get("units")).items():
            data = _obj(unit)
            peak = units.setdefault(name, {"cpu_max": None, "mem_max": None})
            for key, field_name in (("cpu_max", "cpu_max"), ("mem_max", "mem_max")):
                value = data.get(field_name)
                if isinstance(value, (int, float)) and not isinstance(value, bool):
                    old = peak[key]
                    peak[key] = value if old is None else max(old, value)
    free = [
        float(p)
        for r in records
        if isinstance(p := _obj(r.get("disk")).get("free_pct"), (int, float))
    ]
    return {
        "minutes": len(records),
        "origins": origins,
        "units": units,
        "disk_free_pct_min": min(free) if free else None,
    }


def format_report(summary: Mapping[str, Any]) -> str:
    def text(value: object, spec: str = "g", scale: float = 1.0) -> str:
        if value is None:
            return "n/a"
        return format(value * scale, spec)  # type: ignore[operator]

    lines = [f"minutes {summary['minutes']}", ""]
    lines.append(
        f"{'origin':<9}{'req':>9}{'req/s':>8}{'2xx%':>7}{'4xx%':>7}{'5xx%':>7}"
        f"{'503%':>7}{'p95ms':>8}{'p99ms':>8}{'down':>6}"
    )
    for name, data in summary["origins"].items():
        classes = data["classes"] or {}
        percent = [
            text(classes.get(key, 0.0) if classes else None, ".1f", 100)
            for key in ("2xx", "4xx", "5xx")
        ]
        lines.append(
            f"{name:<9}{text(data['requests']):>9}{text(data['rps'], '.2f'):>8}"
            f"{percent[0]:>7}{percent[1]:>7}{percent[2]:>7}"
            f"{text(data['share_503'], '.2f', 100):>7}{text(data['p95']):>8}"
            f"{text(data['p99']):>8}{data['down_samples']:>6}"
        )
        lines.append(
            f"  sampled max active: {text(data['sampled_max_active'])};"
            f" lifetime max seen (latest snapshot): {text(data['lifetime_max_seen'])}"
        )
        if data["rejections"] is None:
            lines.append("  rejections: n/a")
        else:
            shown = ", ".join(f"{k}={v}" for k, v in data["rejections"].items() if v)
            lines.append(f"  rejections: {shown or 'none'}")
        for key in POOLS:
            local = data.get(key)
            if local is None:
                continue
            mib = 1 / 1048576
            lines.append(
                f"  {key.replace('_', ' ')}: waits {text(local['waits'])},"
                f" timeouts {text(local['timeouts'])},"
                f" rejected {text(local['rejected'])};"
                f" max bytes {text(local['bytes_used_max'], '.0f', mib)}"
                f" of {text(local['bytes_quota'], '.0f', mib)} MiB;"
                f" max waiting {text(local['waiting_max'])}"
                f" of {text(local['wait_capacity'])};"
                f" leader waits {text(local['leader_waits'])},"
                f" timeouts {text(local['leader_timeouts'])},"
                f" rejected {text(local['leader_rejected'])};"
                f" reclaim scans {text(local['reclaim_scans'])},"
                f" reclaimed {text(local['reclaimed'])},"
                f" no victim {text(local['reclaim_no_victim'])}"
            )
        cache = data.get("cache")
        if cache is not None and any(cache.get(k) is not None for k in CACHE_COUNTERS):
            lines.append(
                f"  cache: hits {text(cache['hits'])},"
                f" misses {text(cache['misses'])},"
                f" upstream fetches {text(cache['upstream_fetches'])},"
                f" upstream errors {text(cache['upstream_errors'])},"
                f" evictions {text(cache['evictions'])},"
                f" retention skipped {text(cache['retention_skipped'])}"
            )
        reads = data.get("local_reads")
        if reads is not None:
            lines.append(
                f"  local reads: waits {text(reads['waits'])},"
                f" timeouts {text(reads['timeouts'])},"
                f" rejected {text(reads['rejected'])},"
                f" deadline expired {text(reads['deadline_expired'])},"
                f" coalesced {text(reads['coalesced'])},"
                f" coordination rejected {text(reads['coordination_rejected'])},"
                f" shared failures {text(reads['shared_failures'])};"
                f" max active {text(reads['active_max'])};"
                f" max waiting {text(reads['waiting_max'])}"
                f" of {text(reads['wait_capacity'])};"
                f" followers waits {text(reads['leader_waits'])},"
                f" timeouts {text(reads['leader_timeouts'])},"
                f" rejected {text(reads['leader_rejected'])},"
                f" max waiting {text(reads['leader_waiting_max'])}"
                f" of {text(reads['leader_wait_capacity'])}"
            )
        for key, label in (
            ("admission", "artwork admission"),
            ("resolution", "resolution"),
            ("request_limits", "request limits"),
            ("database_admission", "database admission"),
        ):
            found = data.get(key)
            if found is not None:
                shown = ", ".join(f"{k}={v}" for k, v in found.items() if v)
                lines.append(f"  {label}: {shown or 'none'}")
    lines.append("")
    lines.append(f"{'unit':<16}{'cpu max %':>10}{'mem max MiB':>13}")
    for name, data in sorted(summary["units"].items()):
        lines.append(
            f"{name:<16}{text(data['cpu_max'], '.1f'):>10}"
            f"{text(data['mem_max'], '.1f', 1 / 1048576):>13}"
        )
    lines.append("")
    lines.append(f"disk free min % {text(summary['disk_free_pct_min'])}")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    runner = commands.add_parser("run", help="sample and write per-minute lines")
    runner.add_argument("--state", type=Path, required=True)
    runner.add_argument("--interval", type=float, default=15.0)
    reporter = commands.add_parser("report", help="summarize a time window")
    reporter.add_argument("--state", type=Path, required=True)
    reporter.add_argument("--since", required=True)
    reporter.add_argument("--until")
    args = parser.parse_args()
    if args.command == "run":
        if not 1.0 <= args.interval <= 60.0:
            parser.error("--interval must be 1 to 60 seconds")
        return run(args.state, args.interval)
    try:
        since = parse_time(args.since)
        until = parse_time(args.until) if args.until else math.inf
    except ValueError:
        parser.error("--since and --until must be ISO 8601 times")
    print(format_report(summarize(read_records(args.state, since, until))))
    return 0


if __name__ == "__main__":
    sys.exit(main())
