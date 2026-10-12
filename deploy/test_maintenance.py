"""Deterministic local scheduling checks; no installed services or network."""

import contextlib
import json
import os
import signal
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import maintain

ROOT = Path(__file__).resolve().parent.parent
FAKE = r"""#!/usr/bin/python3
import json, os, pathlib, signal, sqlite3, sys, time
state = pathlib.Path(os.environ["TEST_STATE"])
with (state / "calls").open("a") as log:
    log.write(json.dumps(sys.argv[1:]) + "\n")
if pathlib.Path(sys.argv[0]).name == "seeded-sales" and sys.argv[1] == "sync":
    sales = pathlib.Path(sys.argv[sys.argv.index("--sales-db") + 1])
    if not sales.exists():
        with sqlite3.connect(sales) as database:
            database.execute("CREATE TABLE marker(value TEXT)")
        sales.chmod(0o600)
if os.environ.get("TEST_HANG") == "1":
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    (state / "pid").write_text(str(os.getpid()) + "\n")
    time.sleep(60)
if sys.argv[1] == "backup":
    if pathlib.Path(os.environ["SEEDED_DATABASE"]).name == os.environ.get("TEST_FAIL_SOURCE"):
        sys.exit(75)
    destination = sys.argv[2]
    source_name = pathlib.Path(os.environ["SEEDED_DATABASE"]).name
    (state / f"timeout-{source_name}").write_text(
        os.environ.get("SEEDED_BACKUP_TIMEOUT_SECS", "")
    )
    fd = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    os.close(fd)
    if os.environ.get("TEST_INVALID") == "1":
        pathlib.Path(destination).write_bytes(b"not SQLite")
    else:
        with sqlite3.connect(os.environ["SEEDED_DATABASE"]) as source:
            with sqlite3.connect(destination) as target:
                source.backup(target)
sys.exit(int(os.environ.get("TEST_EXIT_" + sys.argv[1].upper(), "0")))
"""


class MaintenanceTests(unittest.TestCase):
    def test_backup_timer_orders_after_verification_without_requiring_success(self):
        unit = (ROOT / "deploy/seeded-backup.service.in").read_text()
        self.assertIn(
            "After=seeded-profiles.service seeded-verify.service", unit.splitlines()
        )
        for dependency in ("seeded-profiles.service", "seeded-verify.service"):
            self.assertNotIn(f"Requires={dependency}", unit.splitlines())
            self.assertNotIn(f"Wants={dependency}", unit.splitlines())

    def setUp(self):
        (ROOT / "tmp").mkdir(exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(
            prefix="maintenance_", dir=ROOT / "tmp"
        )
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.state = self.directory / "state"
        self.state.mkdir(mode=0o700)
        self.release = self.directory / "release"
        self.release.mkdir()
        self.database = self.state / "archive.sqlite"
        self.make_database(self.database)
        for name in (
            "seeded_archive",
            "seeded-index",
            "seeded-sales",
            "seeded-articles",
        ):
            path = self.release / name
            path.write_text(FAKE)
            path.chmod(0o700)
        environment = patch.dict(
            os.environ,
            {
                "SEEDED_DATABASE": str(self.database),
                "TEST_STATE": str(self.state),
            },
            clear=True,
        )
        environment.start()
        self.addCleanup(environment.stop)

    def make_database(self, path):
        with contextlib.closing(sqlite3.connect(path)) as database, database:
            database.execute("CREATE TABLE marker(value TEXT)")
            database.execute("INSERT INTO marker VALUES('preserved')")
        path.chmod(0o600)

    def run_job(self, job, stop=None):
        return maintain.run(job, self.state, self.release, stop or maintain.Stop())

    def calls(self):
        log = self.state / "calls"
        return (
            [json.loads(line) for line in log.read_text().splitlines()]
            if log.exists()
            else []
        )

    def test_secondary_jobs_use_locked_bounded_dedicated_databases(self):
        self.assertEqual(self.run_job("sales"), 0)
        self.assertEqual(
            self.calls(),
            [
                [
                    "sync",
                    "--sales-db",
                    str(self.state / "sales.sqlite"),
                    "--archive-db",
                    str(self.database),
                ],
                [
                    "volumes",
                    "--db",
                    str(self.database),
                    "--sales-db",
                    str(self.state / "sales.sqlite"),
                ],
            ],
        )
        self.assertEqual(
            self.calls()[0],
            [
                "sync",
                "--sales-db",
                str(self.state / "sales.sqlite"),
                "--archive-db",
                str(self.database),
            ],
        )
        self.assertEqual(self.run_job("articles"), 0)
        self.assertEqual(
            self.calls()[-1],
            [
                "sync",
                "--db",
                str(self.state / "articles.sqlite"),
                "--archive-db",
                str(self.database),
            ],
        )
        with (
            patch.dict(
                os.environ, {"SEEDED_SALES_DATABASE": str(self.directory / "elsewhere")}
            ),
            self.assertRaises(maintain.JobError),
        ):
            self.run_job("sales")

    def test_secondary_jobs_refuse_symlink_databases(self):
        (self.state / "sales.sqlite").symlink_to(self.database)
        with self.assertRaises(maintain.JobError):
            self.run_job("sales")
        self.assertEqual(self.calls(), [])

    def test_sales_volume_refresh_runs_only_after_successful_sync(self):
        for code in (1, 75, 130):
            with patch.dict(os.environ, {"TEST_EXIT_SYNC": str(code)}):
                before = len(self.calls())
                self.assertEqual(self.run_job("sales"), code)
                self.assertEqual(len(self.calls()), before + 1)
                self.assertEqual(self.calls()[-1][0], "sync")

    def test_sales_volume_failure_propagates_for_timer_retry(self):
        with patch.dict(os.environ, {"TEST_EXIT_VOLUMES": "75"}):
            self.assertEqual(self.run_job("sales"), 75)
        self.assertEqual([call[0] for call in self.calls()], ["sync", "volumes"])

    def test_sales_timeout_prevents_volume_launch(self):
        with (
            patch.dict(os.environ, {"TEST_HANG": "1"}),
            patch.dict(maintain.LIMITS, {"sales": 0.15}),
            patch.object(maintain, "STOP_SECONDS", 0.05),
        ):
            self.assertEqual(self.run_job("sales"), 75)
        self.assertEqual([call[0] for call in self.calls()], ["sync"])

    def test_missing_sales_database_after_success_returns_retry(self):
        original = maintain.run_child

        def remove_sales_after_sync(command, *args, **kwargs):
            code = original(command, *args, **kwargs)
            if Path(command[0]).name == "seeded-sales":
                (self.state / "sales.sqlite").unlink()
            return code

        with (
            patch.object(maintain, "run_child", side_effect=remove_sales_after_sync),
            self.assertRaises(maintain.JobError) as error,
        ):
            self.run_job("sales")
        self.assertEqual(error.exception.code, 75)
        self.assertEqual([call[0] for call in self.calls()], ["sync"])

    def test_cancellation_after_sales_sync_prevents_volume_launch(self):
        stop = maintain.Stop()
        original = maintain.run_child

        def cancel_after_sync(command, *args, **kwargs):
            code = original(command, *args, **kwargs)
            if Path(command[0]).name == "seeded-sales":
                stop.requested = True
            return code

        with patch.object(maintain, "run_child", side_effect=cancel_after_sync):
            self.assertEqual(self.run_job("sales", stop), 130)
        self.assertEqual([call[0] for call in self.calls()], ["sync"])

    def test_exhausted_sales_deadline_prevents_volume_launch(self):
        clock = [0.0]
        original = maintain.run_child

        def exhaust_after_sync(command, *args, **kwargs):
            code = original(command, *args, **kwargs)
            if Path(command[0]).name == "seeded-sales":
                clock[0] = args[2] + 1
            return code

        with (
            patch.object(maintain.time, "monotonic", side_effect=lambda: clock[0]),
            patch.object(maintain, "run_child", side_effect=exhaust_after_sync),
        ):
            self.assertEqual(self.run_job("sales"), 75)
        self.assertEqual([call[0] for call in self.calls()], ["sync"])

    def test_sales_symlink_created_by_sync_is_rejected_before_volumes(self):
        original = maintain.run_child

        def replace_with_symlink(command, *args, **kwargs):
            code = original(command, *args, **kwargs)
            if Path(command[0]).name == "seeded-sales":
                (self.state / "sales.sqlite").unlink()
                (self.state / "sales.sqlite").symlink_to(self.database)
            return code

        with (
            patch.object(maintain, "run_child", side_effect=replace_with_symlink),
            self.assertRaises(maintain.JobError),
        ):
            self.run_job("sales")
        self.assertEqual([call[0] for call in self.calls()], ["sync"])

    def test_backup_cli_gets_most_of_each_database_budget(self):
        self.make_database(self.state / "sales.sqlite")
        self.assertEqual(self.run_job("backup"), 0)
        for name in ("archive.sqlite", "sales.sqlite"):
            timeout = int((self.state / f"timeout-{name}").read_text())
            self.assertGreater(timeout, 300, name)
            self.assertLessEqual(timeout, int(maintain.LIMITS["backup"]), name)

    def test_backup_preserves_all_databases_with_separate_retention(self):
        for name in ("sales", "articles", "trading"):
            self.make_database(self.state / f"{name}.sqlite")
        self.assertEqual(self.run_job("backup"), 0)
        for name in (
            "backups",
            "backups-sales",
            "backups-articles",
            "backups-trading",
        ):
            copies = list((self.state / name).glob("scheduled-*.sqlite"))
            self.assertEqual(len(copies), 1)
            with contextlib.closing(sqlite3.connect(copies[0])) as conn:
                self.assertEqual(
                    conn.execute("SELECT value FROM marker").fetchone(), ("preserved",)
                )

    def test_backup_retries_only_failed_database_and_keeps_other_daily_copies(self):
        for name in ("sales", "articles"):
            self.make_database(self.state / f"{name}.sqlite")
        with patch.dict(os.environ, {"TEST_FAIL_SOURCE": "sales.sqlite"}):
            self.assertEqual(self.run_job("backup"), 75)
        first = list((self.state / "backups").glob("scheduled-*.sqlite"))
        last = list((self.state / "backups-articles").glob("scheduled-*.sqlite"))
        self.assertEqual(len(first), 1)
        self.assertEqual(len(last), 1)
        self.assertEqual(len(self.calls()), 3)
        self.assertEqual(self.run_job("backup"), 0)
        self.assertEqual(len(self.calls()), 4)
        self.assertEqual(
            list((self.state / "backups").glob("scheduled-*.sqlite")), first
        )
        self.assertEqual(
            list((self.state / "backups-articles").glob("scheduled-*.sqlite")), last
        )
        self.assertEqual(self.run_job("backup"), 0)
        self.assertEqual(len(self.calls()), 7)
        self.assertFalse((self.state / maintain.RETRY_NAME).exists())

    def test_completed_same_day_run_always_takes_a_fresh_copy(self):
        self.assertEqual(self.run_job("backup"), 0)
        with contextlib.closing(sqlite3.connect(self.database)) as db, db:
            db.execute("UPDATE marker SET value='updated'")
        self.assertEqual(self.run_job("backup"), 0)
        copies = sorted((self.state / "backups").glob("scheduled-*.sqlite"))
        self.assertEqual(len(copies), 2)
        self.assertEqual(len(self.calls()), 2)
        with contextlib.closing(sqlite3.connect(copies[-1])) as db:
            self.assertEqual(
                db.execute("SELECT value FROM marker").fetchone(), ("updated",)
            )

    def test_invalid_retry_copy_is_retained_outside_rotation_and_replaced(self):
        self.make_database(self.state / "sales.sqlite")
        with patch.dict(os.environ, {"TEST_FAIL_SOURCE": "sales.sqlite"}):
            self.assertEqual(self.run_job("backup"), 75)
        copy = next((self.state / "backups").glob("scheduled-*.sqlite"))
        copy.write_bytes(b"invalid for inspection")
        self.assertEqual(self.run_job("backup"), 0)
        preserved = list(copy.parent.glob(".scheduled-invalid-*"))
        self.assertEqual(len(preserved), 1)
        self.assertEqual(preserved[0].read_bytes(), b"invalid for inspection")
        self.assertFalse(copy.exists())
        self.assertEqual(len(list(copy.parent.glob("scheduled-*.sqlite"))), 1)
        self.assertEqual(len(self.calls()), 4)
        self.assertFalse((self.state / maintain.RETRY_NAME).exists())

    def test_previous_day_incomplete_record_does_not_skip_today_backup(self):
        self.make_database(self.state / "sales.sqlite")
        with patch.dict(os.environ, {"TEST_FAIL_SOURCE": "sales.sqlite"}):
            self.assertEqual(self.run_job("backup"), 75)
        path = self.state / maintain.RETRY_NAME
        record = json.loads(path.read_text())
        record["date"] = "20200101"
        path.write_text(json.dumps(record))
        self.assertEqual(self.run_job("backup"), 0)
        self.assertEqual(len(self.calls()), 4)
        self.assertEqual(
            len(list((self.state / "backups").glob("scheduled-*.sqlite"))), 2
        )

    def test_cancelled_run_restarts_from_recorded_success_and_clears_journal(self):
        self.make_database(self.state / "sales.sqlite")
        stop = maintain.Stop()
        original = maintain.backup

        def interrupt(*args):
            code = original(*args)
            stop.requested = True
            return code

        with (
            patch.object(maintain, "backup", side_effect=interrupt),
            self.assertRaises(maintain.JobError) as error,
        ):
            self.run_job("backup", stop)
        self.assertEqual(error.exception.code, 130)
        record = json.loads((self.state / maintain.RETRY_NAME).read_text())
        self.assertEqual(set(record["copies"]), {"backups"})
        self.assertEqual(self.run_job("backup"), 0)
        self.assertEqual(len(self.calls()), 2)
        self.assertFalse((self.state / maintain.RETRY_NAME).exists())

    def test_lost_checkpoint_never_reuses_unrecorded_success(self):
        original = maintain.BackupRetry.record
        with (
            patch.object(maintain.BackupRetry, "record", side_effect=OSError("disk")),
            self.assertRaises(OSError),
        ):
            self.run_job("backup")
        self.assertEqual(len(self.calls()), 1)
        self.assertEqual(
            json.loads((self.state / maintain.RETRY_NAME).read_text())["copies"], {}
        )
        pending = self.state / maintain.RETRY_PENDING
        pending.write_bytes(b"interrupted journal staging")
        pending.chmod(0o600)
        with patch.object(maintain.BackupRetry, "record", original):
            self.assertEqual(self.run_job("backup"), 0)
        self.assertEqual(len(self.calls()), 2)
        self.assertFalse(pending.exists())
        self.assertEqual(
            len(list((self.state / "backups").glob("scheduled-*.sqlite"))), 2
        )

    def test_retry_journal_rejects_symlink_oversize_and_unsafe_names(self):
        record = self.state / maintain.RETRY_NAME
        record.symlink_to(self.database)
        with self.assertRaises(maintain.JobError):
            self.run_job("backup")
        record.unlink()
        for value in [
            b"x" * 1025,
            b'{"date":"20261005","copies":{"backups":"../archive.sqlite"}}',
        ]:
            record.write_bytes(value)
            record.chmod(0o600)
            with self.assertRaises(maintain.JobError):
                self.run_job("backup")
        record.unlink()
        (self.state / maintain.RETRY_PENDING).symlink_to(self.database)
        with self.assertRaises(maintain.JobError):
            self.run_job("backup")
        self.assertEqual(len(self.calls()), 0)

    def test_backup_timeout_leaves_fresh_budgets_for_secondary_databases(self):
        for name in ("sales", "articles"):
            self.make_database(self.state / f"{name}.sqlite")
        clock = [100.0]
        deadlines = []

        def copy(*args):
            deadlines.append(args[5])
            clock[0] += 600.0
            if len(deadlines) == 1:
                raise maintain.JobError("busy source", 75)
            return 0

        with (
            patch.object(maintain.time, "monotonic", side_effect=lambda: clock[0]),
            patch.object(maintain, "backup", side_effect=copy),
            self.assertRaises(maintain.JobError) as error,
        ):
            self.run_job("backup")
        self.assertEqual(error.exception.code, 75)
        self.assertEqual(deadlines, [700.0, 1300.0, 1900.0])

    def test_missing_primary_does_not_skip_existing_secondary_backups(self):
        self.database.unlink()
        for name in ("sales", "articles", "trading"):
            self.make_database(self.state / f"{name}.sqlite")
        with self.assertRaises(FileNotFoundError):
            self.run_job("backup")
        self.assertEqual(len(self.calls()), 3)
        for folder in ("backups-sales", "backups-articles", "backups-trading"):
            self.assertEqual(
                len(list((self.state / folder).glob("scheduled-*.sqlite"))), 1
            )

    def test_trading_backup_is_optional_and_refuses_symlink_source(self):
        self.assertEqual(self.run_job("backup"), 0)
        self.assertFalse((self.state / "backups-trading").exists())

        trading = self.state / "trading.sqlite"
        self.make_database(trading)
        self.assertEqual(self.run_job("backup"), 0)
        copies = list((self.state / "backups-trading").glob("scheduled-*.sqlite"))
        self.assertEqual(len(copies), 1)
        with contextlib.closing(sqlite3.connect(copies[0])) as database:
            self.assertEqual(
                database.execute("SELECT value FROM marker").fetchone(), ("preserved",)
            )

        trading.unlink()
        trading.symlink_to(self.database)
        with self.assertRaises(maintain.JobError):
            self.run_job("backup")

        trading.unlink()
        self.make_database(trading)
        destination = self.directory / "external-trading-backups"
        destination.mkdir(mode=0o700)
        (self.state / "backups-trading").rename(self.state / "saved-backups-trading")
        (self.state / "backups-trading").symlink_to(
            destination, target_is_directory=True
        )
        with self.assertRaises(OSError):
            self.run_job("backup")
        self.assertEqual(list(destination.iterdir()), [])

    def test_permanent_source_failure_does_not_suppress_other_database_retry(self):
        for name in ("sales", "articles"):
            self.make_database(self.state / f"{name}.sqlite")
        with (
            patch.object(
                maintain, "backup", side_effect=[maintain.JobError("invalid", 1), 75, 0]
            ) as copy,
            self.assertRaises(maintain.JobError) as failure,
        ):
            self.run_job("backup")
        self.assertEqual(copy.call_count, 3)
        self.assertEqual(failure.exception.code, 75)

    def test_profile_refresh_reads_only_objkt_because_fxhash_profiles_are_frozen(self):
        self.assertEqual(self.run_job("profiles"), 0)
        self.assertEqual(
            self.calls(),
            [["profiles", "--source", "objkt", "--db", str(self.database)]],
        )

    def old_backups(self, count=8):
        directory = self.state / "backups"
        directory.mkdir(mode=0o700, exist_ok=True)
        names = []
        for number in range(count):
            path = directory / f"scheduled-20200101T000000000000Z-{number:016x}.sqlite"
            self.make_database(path)
            names.append(path)
        return names

    def pid(self):
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            try:
                text = (self.state / "pid").read_text()
                if text.endswith("\n"):
                    return int(text)
            except (OSError, ValueError):
                pass
            time.sleep(0.005)
        self.fail("fixture child did not start")

    def test_sync_and_escrow_share_a_lock_and_transient_progress_retries(self):
        with patch.dict(os.environ, {"TEST_EXIT_SYNC": "75"}):
            self.assertEqual(self.run_job("sync"), 75)
        self.assertEqual(
            [line[0] for line in self.calls()], ["sync", "escrow", "offers"]
        )
        self.assertEqual(self.run_job("sync"), 0)
        self.assertEqual(
            [line[0] for line in self.calls()], ["sync", "escrow", "offers"] * 2
        )
        for code in (1, 64, 130):
            with patch.dict(os.environ, {"TEST_EXIT_SYNC": str(code)}):
                before = len(self.calls())
                self.assertEqual(self.run_job("sync"), code)
                self.assertEqual(len(self.calls()), before + 1)

    def test_daily_verification_uses_500_and_lock_contention_is_transient(self):
        with maintain.state_directory(self.state) as state, maintain.writer_lock(state):
            for job in maintain.LIMITS:
                with self.assertRaises(maintain.JobError) as error:
                    self.run_job(job)
                self.assertEqual(error.exception.code, 75)
        self.assertEqual(self.calls(), [])
        self.assertEqual(self.run_job("verify"), 0)
        self.assertEqual(self.calls()[0][:3], ["verify", "--sample", "500"])

    def test_timeout_kills_and_reaps_the_child_before_releasing_the_lock(self):
        with (
            patch.dict(os.environ, {"TEST_HANG": "1"}),
            patch.dict(maintain.LIMITS, {"verify": 0.15}),
            patch.object(maintain, "STOP_SECONDS", 0.05),
        ):
            started = time.monotonic()
            self.assertEqual(self.run_job("verify"), 75)
        pid = self.pid()
        self.assertFalse(Path(f"/proc/{pid}").exists())
        self.assertLess(time.monotonic() - started, 2)
        self.assertEqual(self.run_job("verify"), 0)

    def test_interruption_kills_and_reaps_the_child_and_preserves_exit_130(self):
        stop = maintain.Stop()

        def interrupt():
            self.pid()
            stop.requested = True

        worker = threading.Thread(target=interrupt)
        worker.start()
        with (
            patch.dict(os.environ, {"TEST_HANG": "1"}),
            patch.object(maintain, "STOP_SECONDS", 0.05),
        ):
            with self.assertRaises(maintain.JobError) as error:
                self.run_job("verify", stop)
            self.assertEqual(error.exception.code, 130)
        worker.join(timeout=3)
        self.assertFalse(worker.is_alive())
        self.assertFalse(Path(f"/proc/{self.pid()}").exists())
        self.assertEqual(self.run_job("verify"), 0)

    def test_child_keeps_writer_lock_if_runner_is_killed(self):
        environment = {**os.environ, "TEST_HANG": "1"}
        runner = subprocess.Popen(
            [
                sys.executable,
                str(ROOT / "deploy/maintain.py"),
                "verify",
                "--state-dir",
                str(self.state),
                "--release-dir",
                str(self.release),
            ],
            env=environment,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        pid = None
        try:
            pid = self.pid()
            runner.kill()
            runner.wait(timeout=3)
            with self.assertRaises(maintain.JobError) as error:
                self.run_job("verify")
            self.assertEqual(error.exception.code, 75)
        finally:
            if runner.poll() is None:
                runner.kill()
                runner.wait(timeout=3)
            if pid is not None:
                os.kill(pid, signal.SIGKILL)
        deadline = time.monotonic() + 3
        while True:
            try:
                self.assertEqual(self.run_job("verify"), 0)
                break
            except maintain.JobError as error:
                if error.code != 75 or time.monotonic() >= deadline:
                    raise
                time.sleep(0.01)

    def test_backup_validates_publishes_atomically_and_keeps_two_own_copies(self):
        old = self.old_backups()
        manual = self.state / "backups/before-v3.sqlite"
        self.make_database(manual)
        link = os.link

        def publish(source, destination, **options):
            directory = options["src_dir_fd"]
            maintain.validate_backup(
                directory, source, time.monotonic() + 3, maintain.Stop()
            )
            self.assertTrue(all(path.exists() for path in old))
            self.assertFalse((manual.parent / destination).exists())
            return link(source, destination, **options)

        with patch.object(maintain.os, "link", side_effect=publish) as publication:
            self.assertEqual(self.run_job("backup"), 0)
        publication.assert_called_once()
        managed = [
            path
            for path in manual.parent.iterdir()
            if maintain.BACKUP_NAME.fullmatch(path.name)
        ]
        self.assertEqual(len(managed), 2)
        self.assertTrue(manual.exists())
        self.assertTrue(old[-1].exists())
        self.assertFalse(any(path.exists() for path in old[:-1]))
        self.assertEqual(list(manual.parent.glob(".scheduled-pending-*")), [])
        for path in managed:
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            with contextlib.closing(sqlite3.connect(path)) as database:
                self.assertEqual(
                    database.execute("SELECT value FROM marker").fetchone(),
                    ("preserved",),
                )

    def test_failed_or_invalid_backup_never_prunes_or_publishes(self):
        old = self.old_backups()
        for environment in (
            {"TEST_EXIT_BACKUP": "75"},
            {"TEST_EXIT_BACKUP": "1"},
            {"TEST_INVALID": "1"},
        ):
            with (
                patch.dict(os.environ, environment),
                patch.object(maintain.os, "link") as publish,
            ):
                if "TEST_INVALID" in environment:
                    with self.assertRaises(maintain.JobError) as error:
                        self.run_job("backup")
                    self.assertEqual(error.exception.code, 1)
                else:
                    self.assertEqual(self.run_job("backup"), 75)
                publish.assert_not_called()
            self.assertTrue(all(path.exists() for path in old))
        self.assertEqual(
            len(list((self.state / "backups").glob(".scheduled-pending-*"))), 3
        )

    def test_publication_collision_and_sync_failure_do_not_prune(self):
        old = self.old_backups()
        with (
            patch.object(maintain.os, "link", side_effect=FileExistsError),
            self.assertRaises(FileExistsError),
        ):
            self.run_job("backup")
        self.assertTrue(all(path.exists() for path in old))
        with (
            patch.object(
                maintain.os, "fsync", side_effect=OSError("fixture sync failure")
            ),
            self.assertRaises(OSError),
        ):
            self.run_job("backup")
        self.assertTrue(all(path.exists() for path in old))

    def test_backup_names_cannot_replace_existing_pending_files(self):
        self.old_backups(0)
        pending = self.state / "backups/.scheduled-pending-aaaaaaaaaaaaaaaa.sqlite"
        pending.write_bytes(b"keep")
        pending.chmod(0o600)
        with patch.object(maintain.secrets, "token_hex", return_value="a" * 16):
            with self.assertRaises(maintain.JobError) as error:
                self.run_job("backup")
            self.assertEqual(error.exception.code, 75)
        self.assertEqual(pending.read_bytes(), b"keep")
        self.assertEqual(self.calls(), [])

    def test_interrupted_publication_recovers_only_a_valid_matching_hardlink_pair(self):
        old = self.old_backups()
        directory = self.state / "backups"
        pending = directory / ".scheduled-pending-aaaaaaaaaaaaaaaa.sqlite"
        final = directory / "scheduled-20250101T000000000000Z-aaaaaaaaaaaaaaaa.sqlite"
        self.make_database(pending)
        os.link(pending, final)
        self.assertEqual(self.run_job("backup"), 0)
        self.assertFalse(pending.exists())
        self.assertTrue(final.exists())
        self.assertEqual(final.stat().st_nlink, 1)
        self.assertEqual(len(list(directory.glob("scheduled-*.sqlite"))), 2)
        self.assertFalse(any(path.exists() for path in old))

    def test_invalid_or_ambiguous_publication_pair_is_retained_without_pruning(self):
        old = self.old_backups()
        directory = self.state / "backups"
        pending = directory / ".scheduled-pending-aaaaaaaaaaaaaaaa.sqlite"
        final = directory / "scheduled-20250101T000000000000Z-aaaaaaaaaaaaaaaa.sqlite"
        pending.write_bytes(b"corrupt")
        pending.chmod(0o600)
        os.link(pending, final)
        with self.assertRaises(maintain.JobError) as error:
            self.run_job("backup")
        self.assertEqual(error.exception.code, 1)
        self.assertTrue(pending.exists())
        self.assertTrue(final.exists())
        self.assertTrue(all(path.exists() for path in old))
        final.rename(directory / "before-v3.sqlite")
        with self.assertRaises(maintain.JobError) as error:
            self.run_job("backup")
        self.assertEqual(error.exception.code, 1)
        self.assertTrue(all(path.exists() for path in old))
        self.assertEqual(self.calls(), [])

    def test_backup_scan_and_failed_staging_have_finite_entry_limits(self):
        directory = self.state / "backups"
        directory.mkdir(mode=0o700)
        for number in range(8):
            path = directory / f".scheduled-pending-{number:016x}.sqlite"
            path.write_bytes(b"retained")
            path.chmod(0o600)
        with self.assertRaises(maintain.JobError):
            self.run_job("backup")
        with (
            patch.object(maintain, "MAX_BACKUP_ENTRIES", 4),
            self.assertRaises(maintain.JobError),
        ):
            self.run_job("backup")
        self.assertEqual(self.calls(), [])
        self.assertEqual(len(list(directory.iterdir())), 8)

    def test_symlinked_state_database_lock_and_backup_directory_are_refused(self):
        link = self.directory / "state-link"
        link.symlink_to(self.state, target_is_directory=True)
        with self.assertRaises((maintain.JobError, OSError)):
            maintain.run("verify", link, self.release, maintain.Stop())
        self.database.rename(self.state / "saved.sqlite")
        self.database.symlink_to(self.state / "saved.sqlite")
        with self.assertRaises(maintain.JobError):
            self.run_job("verify")
        self.database.unlink()
        (self.state / "saved.sqlite").rename(self.database)
        (self.state / "maintenance.lock").unlink()
        (self.state / "maintenance.lock").symlink_to(self.database)
        with self.assertRaises(OSError):
            self.run_job("verify")
        (self.state / "maintenance.lock").unlink()
        other = self.directory / "other"
        other.mkdir(mode=0o700)
        (self.state / "backups").symlink_to(other, target_is_directory=True)
        with self.assertRaises(OSError):
            self.run_job("backup")
        self.assertEqual(self.calls(), [])
        self.assertEqual(list(other.iterdir()), [])

    def test_managed_symlink_or_hardlink_aborts_pruning_without_touching_target(self):
        old = self.old_backups()
        old[0].unlink()
        old[0].symlink_to(self.database)
        with self.assertRaises(maintain.JobError):
            self.run_job("backup")
        self.assertTrue(old[0].is_symlink())
        self.assertTrue(all(path.exists() for path in old))
        old[0].unlink()
        os.link(self.database, old[0])
        with self.assertRaises(maintain.JobError):
            self.run_job("backup")
        self.assertTrue(all(path.exists() for path in old))

    def test_unsafe_state_and_mismatched_database_are_usage_errors(self):
        for path in (Path("/"), Path("/var/lib"), Path("relative"), self.state / ".."):
            with self.assertRaises(maintain.JobError):
                maintain.run("verify", path, self.release, maintain.Stop())
        with patch.dict(
            os.environ, {"SEEDED_DATABASE": str(self.directory / "elsewhere.sqlite")}
        ):
            with self.assertRaises(maintain.JobError) as error:
                self.run_job("verify")
            self.assertEqual(error.exception.code, 64)
        result = subprocess.run(
            [sys.executable, str(ROOT / "deploy/maintain.py"), "unknown"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=3,
            check=False,
        )
        self.assertEqual(result.returncode, 64)

    @unittest.skipUnless(
        (ROOT / "target/debug/seeded_archive").is_file(), "compiled app unavailable"
    )
    def test_real_online_backup_uses_checked_paths_compatible_with_sqlite_nofollow(
        self,
    ):
        self.assertEqual(
            maintain.run("backup", self.state, ROOT / "target/debug", maintain.Stop()),
            0,
        )
        copies = list((self.state / "backups").glob("scheduled-*.sqlite"))
        self.assertEqual(len(copies), 1)
        with contextlib.closing(sqlite3.connect(copies[0])) as database:
            self.assertEqual(
                database.execute("SELECT value FROM marker").fetchone(), ("preserved",)
            )


if __name__ == "__main__":
    unittest.main()
