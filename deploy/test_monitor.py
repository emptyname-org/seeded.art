"""Deterministic monitor checks; no network, systemd or host state."""

import contextlib
import io
import json
import tempfile
import unittest
import unittest.mock
from pathlib import Path
from typing import Any

import configure
import monitor

ROOT = Path(__file__).resolve().parent
DAY = 86400
BOUNDS = [10, 50, 100, 500, 2000]


def payload(
    total: int = 100,
    start: str = "2026-01-01T00:00:00Z",
    counts: list[int] | None = None,
    **extra: Any,
) -> bytes:
    body: dict[str, Any] = {
        "schema": 1,
        "origin": "app",
        "process_start": start,
        "uptime_s": 5,
        "release": "r1",
        "requests": {
            "total": total,
            "by_class": {"2xx": total, "5xx": 0},
            "by_status": {"200": total},
            "by_route": {"page": total},
        },
        "active": {"requests": 3, "max_seen": 7},
        "latency_ms": {
            "buckets": BOUNDS,
            "counts": counts or [total, 0, 0, 0, 0, 0],
            "sum_ms": total,
            "count": total,
        },
        "rejections": {"rate_limited": 0},
        "cache": {"hits": 3, "misses": 1, "bytes_used": 10, "bytes_quota": 100},
        "mirror": {"hits": 2},
        "db": {"reads_active": 1, "reads_waiting": 0},
    }
    body.update(extra)
    return json.dumps(body).encode()


def parsed(*args: Any, **kwargs: Any) -> monitor.Sample:
    sample = monitor.parse_metrics(payload(*args, **kwargs))
    assert sample is not None
    return sample


def record(
    minute: int = 0,
    requests: int = 100,
    by_class: dict[str, int] | None = None,
    by_status: dict[str, int] | None = None,
    counts: list[int] | None = None,
    units: dict[str, Any] | None = None,
    free_pct: float = 50.0,
    origin: str = "app",
) -> dict[str, Any]:
    return {
        "ts": "x",
        "t": minute * 60,
        "origins": {
            origin: {
                "requests": requests,
                "by_class": by_class or {"2xx": requests},
                "by_status": by_status or {},
                "by_route": {},
                "rejections": {"rate_limited": 1},
                "max_active": minute + 1,
                "max_seen": 9 + minute % 3,
                "down": 0,
                "latency_ms": {
                    "bounds": BOUNDS,
                    "counts": counts or [requests, 0, 0, 0, 0, 0],
                },
            }
        },
        "units": units or {},
        "disk": {"free_pct": free_pct},
    }


LOCAL = {
    "bytes_used": 1048576,
    "bytes_quota": 536870912,
    "waiting": 4,
    "wait_capacity": 256,
    "waits": 10,
    "timeouts": 2,
    "rejected": 1,
}


class LocalDeliveryTests(unittest.TestCase):
    def test_local_delivery_counters_and_peaks_are_recorded(self):
        tracker, agg = monitor.Tracker(), monitor.OriginAgg()
        tracker.update(parsed(total=10, local_delivery=LOCAL))
        later = dict(LOCAL, waits=15, timeouts=2, rejected=4, waiting=9)
        agg.add(tracker.update(parsed(total=20, local_delivery=later)))
        local = agg.record()["local_delivery"]
        self.assertEqual(
            (local["waits"], local["timeouts"], local["rejected"]), (5, 0, 3)
        )
        self.assertEqual(local["waiting_max"], 9)
        self.assertEqual(local["bytes_used_max"], 1048576)
        self.assertEqual(local["bytes_quota"], 536870912)
        self.assertEqual(local["wait_capacity"], 256)

    def test_reclaim_counters_are_recorded_and_reported(self):
        tracker, agg = monitor.Tracker(), monitor.OriginAgg()
        admission = {name: 0 for name in monitor.ADMISSION}
        first = {"reclaim_scans": 4, "reclaimed": 1, "reclaim_no_victim": 3}
        tracker.update(
            parsed(
                total=10,
                local_delivery=dict(LOCAL, **first),
                artwork_admission=admission,
            )
        )
        later = {"reclaim_scans": 10, "reclaimed": 3, "reclaim_no_victim": 7}
        web = dict(
            admission,
            delivery_reclaim_scans=5,
            delivery_reclaimed=2,
            delivery_reclaim_no_victim=3,
        )
        agg.add(
            tracker.update(
                parsed(
                    total=20,
                    local_delivery=dict(LOCAL, **later),
                    artwork_admission=web,
                )
            )
        )
        minute = agg.record()
        local = minute["local_delivery"]
        self.assertEqual(
            (local["reclaim_scans"], local["reclaimed"], local["reclaim_no_victim"]),
            (6, 2, 4),
        )
        self.assertEqual(minute["admission"]["delivery_reclaimed"], 2)
        line = record()
        line["origins"]["app"].update(
            local_delivery=local, admission=minute["admission"]
        )
        text = monitor.format_report(monitor.summarize([line]))
        self.assertIn("; reclaim scans 6, reclaimed 2, no victim 4", text)
        for shown in (
            "delivery_reclaim_scans=5",
            "delivery_reclaimed=2",
            "delivery_reclaim_no_victim=3",
        ):
            self.assertIn(shown, text)

    def test_scratch_pool_and_admission_counters_are_recorded_and_reported(self):
        tracker, agg = monitor.Tracker(), monitor.OriginAgg()
        admission = {name: 0 for name in monitor.ADMISSION}
        tracker.update(
            parsed(
                total=10,
                local_scratch=dict(LOCAL, waits=1),
                artwork_admission=admission,
            )
        )
        later = dict(admission, delivery_global_bytes_full=40, upstream_waiting_full=2)
        agg.add(
            tracker.update(
                parsed(
                    total=20,
                    local_scratch=dict(LOCAL, waits=4, waiting=12),
                    artwork_admission=later,
                )
            )
        )
        minute = agg.record()
        self.assertIsNone(minute["local_delivery"])
        self.assertEqual(minute["local_scratch"]["waits"], 3)
        self.assertEqual(minute["local_scratch"]["waiting_max"], 12)
        self.assertEqual(minute["admission"]["delivery_global_bytes_full"], 40)
        self.assertEqual(minute["admission"]["metadata_waiting_full"], 0)
        line = record()
        line["origins"]["app"].update(
            local_scratch=minute["local_scratch"], admission=minute["admission"]
        )
        text = monitor.format_report(monitor.summarize([line]))
        self.assertIn("local scratch: waits 3,", text)
        self.assertIn("leader waits n/a, timeouts n/a, rejected n/a", text)
        self.assertIn(
            "artwork admission: delivery_global_bytes_full=40, upstream_waiting_full=2",
            text,
        )
        self.assertNotIn("local delivery:", text)

    def test_reads_resolution_and_request_limits_are_recorded_and_reported(self):
        tracker, agg = monitor.Tracker(), monitor.OriginAgg()
        reads = {
            "active": 3,
            "waiting": 0,
            "wait_capacity": 256,
            "waits": 1,
            "timeouts": 0,
            "rejected": 0,
            "deadline_expired": 0,
            "coalesced": 0,
            "coordination_rejected": 0,
            "shared_failures": 0,
            "leader_waiting": 0,
            "leader_wait_capacity": 256,
            "leader_waits": 0,
            "leader_timeouts": 0,
            "leader_rejected": 0,
        }
        resolution = {name: 0 for name in monitor.RESOLUTION}
        database = {name: 0 for name in monitor.DATABASE_ADMISSION}
        limits = {name: 0 for name in monitor.REQUEST_LIMITS}
        admission = {name: 0 for name in monitor.ADMISSION}
        tracker.update(
            parsed(
                total=10,
                local_reads=reads,
                resolution=resolution,
                request_limits=limits,
                artwork_admission=admission,
                database_admission=database,
                cache={"hits": 0, "misses": 0, "retention_skipped": 0},
            )
        )
        agg.add(
            tracker.update(
                parsed(
                    total=20,
                    local_reads=dict(
                        reads,
                        active=16,
                        waiting=40,
                        waits=9,
                        timeouts=2,
                        deadline_expired=3,
                        coalesced=11,
                        coordination_rejected=1,
                        shared_failures=5,
                        leader_waiting=12,
                        leader_waits=20,
                        leader_timeouts=2,
                    ),
                    resolution=dict(
                        resolution, mirror_busy=5, upstream_busy=6, upstream_timeouts=2
                    ),
                    cache={"hits": 3, "misses": 1, "retention_skipped": 4},
                    database_admission=dict(database, waiting_timeout=4),
                    request_limits=dict(limits, global_content_wait_full=7),
                    artwork_admission=dict(admission, handler_timeout=1),
                )
            )
        )
        minute = agg.record()
        self.assertEqual(minute["local_reads"]["waits"], 8)
        self.assertEqual(minute["local_reads"]["timeouts"], 2)
        self.assertEqual(minute["local_reads"]["active_max"], 16)
        self.assertEqual(minute["local_reads"]["waiting_max"], 40)
        self.assertEqual(minute["resolution"]["mirror_busy"], 5)
        self.assertEqual(minute["resolution"]["upstream_busy"], 6)
        self.assertEqual(minute["local_reads"]["deadline_expired"], 3)
        self.assertEqual(minute["local_reads"]["coalesced"], 11)
        self.assertEqual(minute["local_reads"]["coordination_rejected"], 1)
        self.assertEqual(minute["local_reads"]["shared_failures"], 5)
        self.assertEqual(minute["local_reads"]["leader_waits"], 20)
        self.assertEqual(minute["local_reads"]["leader_timeouts"], 2)
        self.assertEqual(minute["local_reads"]["leader_waiting_max"], 12)
        self.assertEqual(minute["local_reads"]["leader_wait_capacity"], 256)
        self.assertEqual(minute["resolution"]["upstream_timeouts"], 2)
        self.assertEqual(minute["cache"]["retention_skipped"], 4)
        self.assertEqual(minute["database_admission"]["waiting_timeout"], 4)
        self.assertEqual(minute["request_limits"]["global_content_wait_full"], 7)
        self.assertEqual(minute["admission"]["handler_timeout"], 1)
        line = record()
        line["origins"]["app"].update(
            {
                key: minute[key]
                for key in (
                    "local_reads",
                    "resolution",
                    "request_limits",
                    "admission",
                    "database_admission",
                    "cache",
                )
            }
        )
        text = monitor.format_report(monitor.summarize([line]))
        self.assertIn(
            "local reads: waits 8, timeouts 2, rejected 0, deadline expired 3,"
            " coalesced 11, coordination rejected 1, shared failures 5;"
            " max active 16;"
            " max waiting 40 of 256",
            text,
        )
        self.assertIn(
            "resolution: mirror_busy=5, upstream_busy=6, upstream_timeouts=2", text
        )
        self.assertIn("retention skipped 4", text)
        self.assertIn(
            "followers waits 20, timeouts 2, rejected 0, max waiting 12 of 256", text
        )
        self.assertIn("database admission: waiting_timeout=4", text)
        self.assertIn("request limits: global_content_wait_full=7", text)
        self.assertIn("artwork admission: handler_timeout=1", text)

    def test_missing_admission_stays_unknown(self):
        tracker, agg = monitor.Tracker(), monitor.OriginAgg()
        tracker.update(parsed(total=10))
        agg.add(tracker.update(parsed(total=20)))
        self.assertIsNone(agg.record()["admission"])
        summary = monitor.summarize([record()])
        self.assertIsNone(summary["origins"]["app"]["admission"])
        self.assertNotIn("artwork admission", monitor.format_report(summary))

    def test_origin_without_local_delivery_records_none(self):
        tracker, agg = monitor.Tracker(), monitor.OriginAgg()
        tracker.update(parsed(total=10))
        agg.add(tracker.update(parsed(total=20)))
        self.assertIsNone(agg.record()["local_delivery"])

    def test_report_sums_counters_and_keeps_unknown_as_na(self):
        first, second = record(minute=0), record(minute=1)
        first["origins"]["app"]["local_delivery"] = {
            "waits": 3,
            "timeouts": None,
            "rejected": 1,
            "bytes_used_max": 2097152,
            "waiting_max": 5,
            "bytes_quota": 536870912,
            "wait_capacity": 256,
        }
        second["origins"]["app"]["local_delivery"] = {
            "waits": 4,
            "timeouts": None,
            "rejected": 0,
            "bytes_used_max": 1048576,
            "waiting_max": 7,
            "bytes_quota": 536870912,
            "wait_capacity": 256,
        }
        summary = monitor.summarize([first, second])
        local = summary["origins"]["app"]["local_delivery"]
        self.assertEqual(
            (local["waits"], local["timeouts"], local["rejected"]), (7, None, 1)
        )
        self.assertEqual((local["bytes_used_max"], local["waiting_max"]), (2097152, 7))
        text = monitor.format_report(summary)
        self.assertIn(
            "local delivery: waits 7, timeouts n/a, rejected 1; max bytes 2 of 512 MiB;"
            " max waiting 7 of 256",
            text,
        )
        self.assertIsNone(
            monitor.summarize([record()])["origins"]["app"]["local_delivery"]
        )


class ParseTests(unittest.TestCase):
    def test_valid_payload_is_flattened(self):
        sample = parsed(total=9)
        self.assertEqual(sample.counters["requests.total"], 9)
        self.assertEqual(sample.counters["status.200"], 9)
        self.assertEqual(sample.counters["cache.hits"], 3)
        self.assertEqual(sample.gauges["active.max_seen"], 7)

    def test_malformed_or_wrong_schema_is_unusable(self):
        for raw in (
            b"",
            b"{",
            b"[]",
            b"null",
            b'{"schema":2}',
            b"\xff\xfe",
            b"[" * 5000,
        ):
            with self.subTest(raw=raw[:10]):
                self.assertIsNone(monitor.parse_metrics(raw))

    def test_missing_null_and_mistyped_fields_stay_unknown(self):
        sample = monitor.parse_metrics(
            b'{"schema":1,"requests":{"total":"x","by_class":[],'
            b'"by_status":{"200":-5,"bad":3,"99999":1}},'
            b'"latency_ms":{"buckets":[5,1],"counts":[1]},"cache":7}'
        )
        assert sample is not None
        for key in ("requests.total", "status.200", "rej.timeout", "cache.hits"):
            self.assertNotIn(key, sample.counters)
        self.assertNotIn("cache.bytes_used", sample.gauges)
        self.assertNotIn("status.99999", sample.counters)
        self.assertEqual(sample.bounds, ())
        self.assertEqual(sample.start, "")

    def test_unknown_fields_are_ignored(self):
        sample = parsed(extra_field={"a": 1}, future=[1, 2])
        self.assertEqual(sample.counters["requests.total"], 100)

    def test_down_endpoint_is_a_sample_not_a_crash(self):
        self.assertIsNone(monitor.fetch_metrics("http://127.0.0.1:1/health/metrics"))


class DeltaTests(unittest.TestCase):
    def test_first_sample_has_no_baseline(self):
        delta = monitor.Tracker().update(parsed(total=500))
        self.assertEqual(delta.counters, {})

    def test_deltas_between_samples(self):
        tracker = monitor.Tracker()
        tracker.update(parsed(total=100, counts=[100, 0, 0, 0, 0, 0]))
        delta = tracker.update(parsed(total=130, counts=[100, 30, 0, 0, 0, 0]))
        self.assertEqual(delta.counters["requests.total"], 30)
        self.assertEqual(delta.counters["class.2xx"], 30)
        self.assertEqual(delta.counts, (0, 30, 0, 0, 0, 0))
        self.assertFalse(delta.reset)

    def test_restart_resets_to_current_counts(self):
        tracker = monitor.Tracker()
        tracker.update(parsed(total=1000))
        delta = tracker.update(parsed(total=40, start="2026-01-02T00:00:00Z"))
        self.assertEqual(delta.counters["requests.total"], 40)
        self.assertTrue(delta.reset)
        again = tracker.update(parsed(total=55, start="2026-01-02T00:00:00Z"))
        self.assertEqual(again.counters["requests.total"], 15)

    def test_decreasing_counter_without_new_start_is_a_reset(self):
        tracker = monitor.Tracker()
        tracker.update(parsed(total=1000))
        delta = tracker.update(parsed(total=10))
        self.assertEqual(delta.counters["requests.total"], 10)

    def test_down_sample_keeps_baseline(self):
        monitor_ = monitor.Monitor.__new__(monitor.Monitor)
        tracker = monitor.Tracker()
        tracker.update(parsed(total=100))
        self.assertIsNotNone(monitor_)
        delta = tracker.update(parsed(total=120))
        self.assertEqual(delta.counters["requests.total"], 20)


class PercentileTests(unittest.TestCase):
    def test_interpolates_inside_the_bucket(self):
        counts = [0, 100, 0, 0, 0, 0]
        self.assertEqual(monitor.percentile(BOUNDS, counts, 0.5), 30.0)
        self.assertEqual(monitor.percentile(BOUNDS, counts, 0.95), 48.0)

    def test_spreads_across_buckets(self):
        counts = [90, 0, 0, 10, 0, 0]
        self.assertEqual(monitor.percentile(BOUNDS, counts, 0.5), 5.6)
        p99 = monitor.percentile(BOUNDS, counts, 0.99)
        assert p99 is not None
        self.assertTrue(100 < p99 <= 500)

    def test_overflow_bucket_reports_its_lower_bound(self):
        self.assertEqual(monitor.percentile(BOUNDS, [0, 0, 0, 0, 0, 5], 0.99), 2000.0)

    def test_empty_or_mismatched_is_none(self):
        self.assertIsNone(monitor.percentile(BOUNDS, [0] * 6, 0.5))
        self.assertIsNone(monitor.percentile(BOUNDS, [1, 2], 0.5))
        self.assertIsNone(monitor.percentile([], [], 0.5))


class AlertTests(unittest.TestCase):
    def test_5xx_share_over_two_percent(self):
        window = [record(by_class={"2xx": 90, "5xx": 10})]
        self.assertIn("5xx:app", monitor.conditions(window, {}))
        window = [record(by_class={"2xx": 98, "5xx": 2})]
        self.assertNotIn("5xx:app", monitor.conditions(window, {}))

    def test_503_share_over_one_percent(self):
        window = [record(by_status={"503": 5})]
        self.assertIn("503:app", monitor.conditions(window, {}))
        self.assertNotIn(
            "503:app", monitor.conditions([record(by_status={"503": 1})], {})
        )

    def test_small_samples_do_not_alert_on_shares(self):
        window = [record(requests=5, by_class={"5xx": 5}, by_status={"503": 5})]
        self.assertEqual(monitor.conditions(window, {}), {})

    def test_p95_over_two_seconds_uses_the_whole_window(self):
        slow = [0, 0, 0, 0, 0, 100]
        found = monitor.conditions([record(0), record(1, counts=slow)], {})
        self.assertIn("p95:app", found)
        fast = [monitor.conditions([record(0), record(1)], {})]
        self.assertNotIn("p95:app", fast[0])

    def test_endpoint_down_over_two_minutes(self):
        self.assertIn("down:app", monitor.conditions([record()], {"app": 121.0}))
        self.assertNotIn("down:app", monitor.conditions([record()], {"app": 120.0}))

    def test_memory_over_ninety_percent_of_high(self):
        unit = {"state": "active", "mem_max": 91, "mem_high": 100}
        self.assertIn("memory:u", monitor.conditions([record(units={"u": unit})], {}))
        unit = {"state": "active", "mem_max": 90, "mem_high": 100}
        self.assertEqual(monitor.conditions([record(units={"u": unit})], {}), {})
        unit = {"state": "active", "mem_max": 10**12, "mem_high": None}
        self.assertEqual(monitor.conditions([record(units={"u": unit})], {}), {})

    def test_disk_below_ten_percent_and_inactive_unit(self):
        units = {"mariadb": {"state": "failed"}}
        found = monitor.conditions([record(free_pct=9.5, units=units)], {})
        self.assertIn("disk", found)
        self.assertIn("unit:mariadb", found)

    def test_job_units_alert_only_when_a_run_failed(self):
        units = {
            "seeded-sync": {"state": "inactive", "file_state": "static"},
            "seeded-verify": {"state": "activating", "file_state": "static"},
            "seeded-backup": {"state": "failed", "file_state": "static"},
        }
        found = monitor.conditions([record(units=units)], {})
        self.assertNotIn("unit:seeded-sync", found)
        self.assertNotIn("unit:seeded-verify", found)
        self.assertEqual(found["unit:seeded-backup"], "seeded-backup failed")
        text = (
            "Id=seeded-backup.service\nLoadState=loaded\nActiveState=failed\n"
            "UnitFileState=static\n\n"
            "Id=seeded-forum-backup.service\nLoadState=not-found\nActiveState=inactive\n"
        )
        self.assertEqual(sorted(monitor.parse_units(text)), ["seeded-backup"])

    def test_stopped_disabled_alternative_unit_does_not_alert(self):
        units = {
            "seeded": {"state": "inactive", "file_state": "disabled"},
            "seeded-app": {"state": "active", "file_state": "enabled"},
            "seeded-content": {"state": "failed", "file_state": "enabled"},
        }
        found = monitor.conditions([record(units=units)], {})
        self.assertNotIn("unit:seeded", found)
        self.assertNotIn("unit:seeded-app", found)
        self.assertIn("unit:seeded-content", found)

    def test_alerts_are_rate_limited_and_recovery_logs_once(self):
        lines: list[str] = []
        alerter = monitor.Alerter(lines.append)
        alerter.update({"disk": "disk free 5.0%"}, 0)
        alerter.update({"disk": "disk free 5.0%"}, 600)
        self.assertEqual(len(lines), 1)
        self.assertTrue(lines[0].endswith("seeded-monitor alert: disk free 5.0%"))
        alerter.update({"disk": "disk free 5.0%"}, 900)
        self.assertEqual(len(lines), 2)
        alerter.update({}, 960)
        alerter.update({}, 1020)
        self.assertEqual(len(lines), 3)
        self.assertIn("recovered: disk", lines[2])

    def test_recurrence_inside_the_interval_stays_quiet(self):
        lines: list[str] = []
        alerter = monitor.Alerter(lines.append)
        alerter.update({"disk": "x"}, 0)
        alerter.update({}, 60)
        alerter.update({"disk": "x"}, 120)
        self.assertEqual(
            [line.split(":")[0] for line in lines].count("<4>seeded-monitor alert"), 1
        )


class RetentionTests(unittest.TestCase):
    def setUp(self):
        self.cleanup = tempfile.TemporaryDirectory(dir=ROOT.parent / "tmp")
        self.addCleanup(self.cleanup.cleanup)
        self.state = Path(self.cleanup.name)

    @classmethod
    def setUpClass(cls):
        (ROOT.parent / "tmp").mkdir(exist_ok=True)

    def names(self) -> list[str]:
        return sorted(path.name for path in self.state.iterdir())

    def test_files_older_than_fourteen_days_are_deleted(self):
        now = 1_767_268_800.0  # 2026-01-01T12:00:00Z
        for name in (
            "metrics-20251216.jsonl",
            "metrics-20251218.jsonl",
            "metrics-20260101.jsonl",
            "other.txt",
        ):
            (self.state / name).write_text("x\n")
        monitor.prune(self.state, now)
        self.assertEqual(
            self.names(),
            ["metrics-20251218.jsonl", "metrics-20260101.jsonl", "other.txt"],
        )

    def test_oldest_files_go_first_when_over_the_size_cap(self):
        for day in (28, 29, 30, 31):
            (self.state / f"metrics-202512{day}.jsonl").write_bytes(b"x" * 100)
        monitor.prune(self.state, 1_767_268_800.0, max_bytes=250)
        self.assertEqual(
            self.names(), ["metrics-20251230.jsonl", "metrics-20251231.jsonl"]
        )

    def test_the_newest_file_is_never_deleted_by_size(self):
        (self.state / "metrics-20251231.jsonl").write_bytes(b"x" * 100)
        monitor.prune(self.state, 1_767_268_800.0, max_bytes=10)
        self.assertEqual(self.names(), ["metrics-20251231.jsonl"])

    def test_lines_are_private_and_append_only(self):
        monitor.append_line(self.state, 1_767_268_800.0, {"a": 1})
        monitor.append_line(self.state, 1_767_268_800.0, {"a": 2})
        path = self.state / "metrics-20260101.jsonl"
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        self.assertEqual(len(path.read_text().splitlines()), 2)


class MonitorLoopTests(unittest.TestCase):
    def setUp(self):
        (ROOT.parent / "tmp").mkdir(exist_ok=True)
        self.cleanup = tempfile.TemporaryDirectory(dir=ROOT.parent / "tmp")
        self.addCleanup(self.cleanup.cleanup)
        self.state = Path(self.cleanup.name)

    def test_minute_line_aggregates_samples_units_and_down_endpoints(self):
        totals = iter([100, 160, 200, 260, 300])
        lines: list[str] = []

        def fetch(url: str) -> monitor.Sample | None:
            if ":3919" in url:
                return None
            return parsed(total=next(totals))

        cpu = iter([0, 7_500_000_000, 15_000_000_000, 15_000_000_000, 15_000_000_000])
        unit = lambda: {
            "seeded-app": monitor.UnitSample("active", next(cpu), 50, 100, 200)
        }
        mon = monitor.Monitor(
            self.state,
            fetch=fetch,
            units=unit,
            disk=lambda: {"free_pct": 40.0},
            load=lambda: (0.5, 0.4, 0.3),
            emit=lines.append,
        )
        for second in (0, 15, 30, 45):
            mon.tick(600.0 + second)
        mon.tick(660.0)
        written = (self.state / "metrics-19700101.jsonl").read_text().splitlines()
        self.assertEqual(len(written), 1)
        line = json.loads(written[0])
        app = line["origins"]["app"]
        self.assertEqual(app["requests"], 160)
        self.assertEqual(app["by_class"]["2xx"], 160)
        self.assertEqual(app["up"], 4)
        self.assertEqual(line["origins"]["artwork"]["down"], 4)
        self.assertEqual(line["units"]["seeded-app"]["cpu_max"], 50.0)
        self.assertEqual(line["units"]["seeded-app"]["cpu_avg"], 33.33)
        self.assertEqual(line["load"], [0.5, 0.4, 0.3])
        text = written[0]
        for forbidden in ("127.0.0.1", "http", "/health"):
            self.assertNotIn(forbidden, text)

    def test_down_endpoint_alerts_after_two_minutes(self):
        lines: list[str] = []
        mon = monitor.Monitor(
            self.state,
            fetch=lambda url: None,
            units=dict,
            disk=lambda: None,
            load=lambda: (0.0, 0.0, 0.0),
            emit=lines.append,
        )
        for second in range(0, 300, 15):
            mon.tick(float(second))
        self.assertTrue(any("down" in line for line in lines))
        self.assertTrue(
            all(line.startswith("<4>seeded-monitor alert:") for line in lines)
        )


class UnknownValueTests(unittest.TestCase):
    PRODUCER = (
        b'{"schema":1,"process_start":"s","requests":{"total":10,'
        b'"cancelled_before_headers":1,"by_class":{"2xx":10},'
        b'"by_status":{"200":10},"by_route":{"page":10}},'
        b'"active":{"requests":2,"max_seen":4},'
        b'"body":{"errors":0,"bytes_emitted":99},'
        b'"cache":{"hits":1,"misses":1,"bytes_used":null,"bytes_quota":null}}'
    )

    def test_omitted_groups_and_null_cache_bytes_survive_aggregation(self):
        sample = monitor.parse_metrics(self.PRODUCER)
        assert sample is not None
        self.assertEqual(sample.counters["body.bytes_emitted"], 99)
        self.assertNotIn("cache.bytes_used", sample.gauges)
        tracker = monitor.Tracker()
        agg = monitor.OriginAgg()
        agg.add(tracker.update(sample))
        agg.add(tracker.update(sample))
        out = agg.record()
        self.assertEqual(out["requests"], 0)
        self.assertIsNone(out["rejections"])
        self.assertIsNone(out["cache"]["bytes_used"])
        self.assertIsNone(out["db"]["reads_waiting_max"])
        self.assertEqual(out["max_active"], 2)
        self.assertIsNone(out["latency_ms"]["p95"])
        self.assertIsNone(monitor.OriginAgg().record()["requests"])

    def test_report_shows_na_not_zero_for_unknown(self):
        line = record()
        origin = line["origins"]["app"]
        for key in ("rejections", "max_active", "max_seen", "by_status"):
            origin[key] = None
        line["units"] = {"u": {"state": "active", "cpu_max": None, "mem_max": None}}
        del line["disk"]
        text = monitor.format_report(monitor.summarize([line]))
        self.assertIn("rejections: n/a", text)
        self.assertIn("sampled max active: n/a", text)
        self.assertIn("lifetime max seen (latest snapshot): n/a", text)
        self.assertIn("disk free min % n/a", text)
        self.assertRegex(text, r"u +n/a +n/a")

    def test_unit_with_unknown_memory_is_not_recorded_as_zero(self):
        agg = monitor.UnitAgg()
        self.assertIsNone(agg.record()["mem_max"])


class UnitParsingTests(unittest.TestCase):
    def test_parse_units_skips_missing_and_unset_values(self):
        text = (
            "Id=seeded-app.service\nLoadState=loaded\nActiveState=active\n"
            "MemoryCurrent=1000\nMemoryHigh=infinity\nMemoryMax=2000\n"
            "CPUUsageNSec=[not set]\n\n"
            "Id=mariadb.service\nLoadState=not-found\nActiveState=inactive\n\n"
            "Id=php8.4-fpm.service\nLoadState=loaded\nActiveState=failed\n"
            "UnitFileState=disabled\n"
            "MemoryCurrent=18446744073709551615\n"
        )
        units = monitor.parse_units(text)
        self.assertEqual(sorted(units), ["php8.4-fpm", "seeded-app"])
        self.assertIn("seeded", monitor.UNITS)
        self.assertEqual(units["seeded-app"].memory, 1000)
        self.assertIsNone(units["seeded-app"].memory_high)
        self.assertIsNone(units["seeded-app"].cpu_ns)
        self.assertIsNone(units["php8.4-fpm"].memory)
        self.assertEqual(units["php8.4-fpm"].state, "failed")
        self.assertEqual(units["php8.4-fpm"].file_state, "disabled")
        self.assertIsNone(units["seeded-app"].file_state)

    def test_parse_units_reads_control_group(self):
        text = (
            "Id=seeded-content.service\nLoadState=loaded\nActiveState=active\n"
            "ControlGroup=/system.slice/seeded-content.service\n"
        )
        unit = monitor.parse_units(text)["seeded-content"]
        self.assertEqual(unit.control_group, "/system.slice/seeded-content.service")


class WorkingSetTests(unittest.TestCase):
    def setUp(self):
        (ROOT.parent / "tmp").mkdir(exist_ok=True)
        self.cleanup = tempfile.TemporaryDirectory(dir=ROOT.parent / "tmp")
        self.addCleanup(self.cleanup.cleanup)
        self.root = Path(self.cleanup.name)
        self.group = self.root / "system.slice" / "u.service"
        self.group.mkdir(parents=True)

    def unit(self, memory: int | None = 3000) -> monitor.UnitSample:
        return monitor.UnitSample(
            "active", None, memory, 3000, 4000, None, "/system.slice/u.service"
        )

    def test_page_cache_at_memory_high_does_not_alert(self):
        (self.group / "memory.stat").write_text(
            "anon 1200\nfile 1700\ninactive_file 1650\nactive_file 50\n"
        )
        unit = monitor.working_set(self.unit(), self.root)
        self.assertEqual(unit.memory, 1350)
        record_units = {
            "u": {"state": "active", "mem_max": unit.memory, "mem_high": 3000}
        }
        self.assertNotIn(
            "memory:u", monitor.conditions([record(units=record_units)], {})
        )

    def test_working_set_never_goes_negative(self):
        (self.group / "memory.stat").write_text("inactive_file 5000\n")
        self.assertEqual(monitor.working_set(self.unit(), self.root).memory, 0)

    def test_unreadable_stat_keeps_current_memory(self):
        self.assertEqual(monitor.working_set(self.unit(), self.root).memory, 3000)
        (self.group / "memory.stat").write_text("anon 1200\n")
        self.assertEqual(monitor.working_set(self.unit(), self.root).memory, 3000)

    def test_unknown_memory_or_group_is_unchanged(self):
        (self.group / "memory.stat").write_text("inactive_file 100\n")
        self.assertIsNone(monitor.working_set(self.unit(None), self.root).memory)
        bare = monitor.UnitSample("active", None, 3000, None, None)
        self.assertEqual(monitor.working_set(bare, self.root), bare)


class ReportTests(unittest.TestCase):
    def setUp(self):
        (ROOT.parent / "tmp").mkdir(exist_ok=True)
        self.cleanup = tempfile.TemporaryDirectory(dir=ROOT.parent / "tmp")
        self.addCleanup(self.cleanup.cleanup)
        self.state = Path(self.cleanup.name)

    def fixture(self) -> None:
        base = 1_767_268_800  # 2026-01-01T12:00:00Z
        lines = []
        for index in range(3):
            line = record(
                minute=base // 60 + index,
                requests=600,
                by_class={"2xx": 580, "5xx": 20},
                by_status={"200": 580, "503": 6, "500": 14},
                counts=[0, 0, 600, 0, 0, 0],
                units={
                    "seeded-app": {
                        "state": "active",
                        "cpu_max": 40.0 + index,
                        "mem_max": (index + 1) * 1048576,
                    }
                },
                free_pct=30.0 - index,
            )
            lines.append(json.dumps(line))
        lines.insert(1, "not json")
        (self.state / "metrics-20260101.jsonl").write_text("\n".join(lines) + "\n")

    def test_summary_over_fixture_lines(self):
        self.fixture()
        since = monitor.parse_time("2026-01-01T12:00:00Z")
        records = monitor.read_records(self.state, since, float("inf"))
        summary = monitor.summarize(records)
        app = summary["origins"]["app"]
        self.assertEqual(summary["minutes"], 3)
        self.assertEqual(app["requests"], 1800)
        self.assertEqual(app["rps"], 10.0)
        self.assertAlmostEqual(app["classes"]["5xx"], 60 / 1800, places=4)
        self.assertEqual(app["share_503"], round(18 / 1800, 4))
        self.assertEqual(app["p95"], 97.5)
        self.assertEqual(app["sampled_max_active"], 1_767_268_800 // 60 + 3)
        self.assertEqual(app["lifetime_max_seen"], 9 + (1_767_268_800 // 60 + 2) % 3)
        self.assertEqual(app["rejections"], {"rate_limited": 3})
        self.assertEqual(summary["units"]["seeded-app"]["cpu_max"], 42.0)
        self.assertEqual(summary["units"]["seeded-app"]["mem_max"], 3 * 1048576)
        self.assertEqual(summary["disk_free_pct_min"], 28.0)

    def test_until_excludes_later_minutes(self):
        self.fixture()
        since = monitor.parse_time("2026-01-01T12:00:00Z")
        until = monitor.parse_time("2026-01-01T12:02:00+00:00")
        records = monitor.read_records(self.state, since, until)
        self.assertEqual(len(records), 2)

    def test_cli_prints_a_table(self):
        self.fixture()
        out = io.StringIO()
        argv = [
            "monitor.py",
            "report",
            "--state",
            str(self.state),
            "--since",
            "2026-01-01T12:00:00Z",
        ]
        with (
            contextlib.redirect_stdout(out),
            unittest.mock.patch("sys.argv", argv),
        ):
            self.assertEqual(monitor.main(), 0)
        text = out.getvalue()
        self.assertIn("app", text)
        self.assertIn("req/s", text)
        self.assertIn("seeded-app", text)
        self.assertIn("disk free min %", text)


class UnitRenderTests(unittest.TestCase):
    def test_unit_is_rendered_and_hardened(self):
        self.assertEqual(
            configure.TEMPLATES["seeded-monitor.service.in"], "seeded-monitor.service"
        )
        text = (ROOT / "seeded-monitor.service.in").read_text()
        self.assertNotIn("__", text)
        for line in (
            "Type=simple",
            "User=seeded-monitor",
            "StateDirectory=seeded-monitor",
            "StateDirectoryMode=0700",
            "ProtectSystem=strict",
            "ProtectHome=true",
            "PrivateTmp=true",
            "NoNewPrivileges=true",
            "RestrictAddressFamilies=AF_INET AF_INET6 AF_UNIX",
            "MemoryMax=128M",
            "Restart=always",
            "RestartSec=10",
            (
                "ExecStart=/usr/bin/python3 /opt/seeded/current/deploy/monitor.py run "
                "--state /var/lib/seeded-monitor --interval 15"
            ),
        ):
            self.assertIn(line, text.splitlines())
        # A DynamicUser cannot query systemd over D-Bus without NSS "systemd".
        self.assertNotIn("DynamicUser=yes", text.splitlines())


if __name__ == "__main__":
    unittest.main()
