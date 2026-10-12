"""Synthetic local dumps; no MariaDB server, credentials, or network."""

import contextlib
import datetime
import errno
import fcntl
import io
import json
import os
import signal
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import forum_backup
from maintain import JobError, Stop, state_directory, writer_lock

ROOT = Path(__file__).resolve().parent.parent
GOOD_DUMP = (
    b"/*M!999999\\- enable the sandbox mode */\n"
    b"-- MariaDB dump 10.19  Distrib 11.8.6-MariaDB, for debian-linux-gnu (x86_64)\n"
    b"-- Host: localhost    Database: flarum\n"
    b"CREATE TABLE marker (id int);\n"
    b"-- Dump completed\n"
)
FAKE = r"""
import json, os, pathlib, signal, sys, time
directory = pathlib.Path(__file__).parent
prefix = "query-" if any(arg.startswith("--execute=") for arg in sys.argv) else ""
mode_file = directory / (prefix + "mode")
mode = mode_file.read_text() if mode_file.exists() else "success"
(directory / (prefix + "call")).write_text(json.dumps({"args": sys.argv[1:], "env": dict(os.environ)}))
signal.signal(signal.SIGTERM, signal.SIG_IGN)
(directory / (prefix + "pid")).write_text(str(os.getpid()))
payload_file = directory / (prefix + "payload")
payload = payload_file.read_bytes() if payload_file.exists() else b"0\n"
if mode != "empty":
    sys.stdout.buffer.write(payload)
    sys.stdout.buffer.flush()
if mode == "closed_hang":
    sys.stdout.close()
    os.close(1)
if mode in ("hang", "closed_hang"):
    time.sleep(60)
if mode == "fail":
    sys.stderr.write("PRIVATE_DATABASE_ERROR_SENTINEL\n")
    sys.exit(2)
"""


class ForumBackupTests(unittest.TestCase):
    def test_default_directory_matches_private_service_state_directory(self):
        unit = (ROOT / "deploy/seeded-forum-backup.service.in").read_text()
        state = next(
            line.split("=", 1)[1]
            for line in unit.splitlines()
            if line.startswith("StateDirectory=")
        )
        self.assertEqual(forum_backup.DEFAULT_DIRECTORY, Path("/var/lib") / state)
        self.assertIn(f"WorkingDirectory={forum_backup.DEFAULT_DIRECTORY}\n", unit)
        self.assertNotIn("--directory", unit)

    def setUp(self):
        (ROOT / "tmp").mkdir(exist_ok=True)
        temporary = tempfile.TemporaryDirectory(
            prefix="forum_backup_", dir=ROOT / "tmp"
        )
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.directory = self.root / "backups"
        self.directory.mkdir(mode=0o700)
        self.executable = self.root / "dump"
        self.executable.write_text(f"#!{sys.executable}\n" + FAKE)
        self.executable.chmod(0o700)
        (self.root / "payload").write_bytes(GOOD_DUMP)
        (self.root / "mode").write_text("success")
        executable = patch.object(forum_backup, "DUMP_EXECUTABLE", str(self.executable))
        executable.start()
        self.addCleanup(executable.stop)
        client = patch.object(forum_backup, "CLIENT_EXECUTABLE", str(self.executable))
        client.start()
        self.addCleanup(client.stop)
        grace = patch.object(forum_backup, "STOP_SECONDS", 0.02)
        grace.start()
        self.addCleanup(grace.stop)

    def run_backup(self, stop=None, max_bytes=forum_backup.MAX_BYTES):
        return forum_backup.run(
            self.directory, forum_backup.DEFAULT_SOCKET, stop or Stop(), max_bytes
        )

    def copies(self):
        return sorted(self.directory.glob("forum-*.sql"))

    def staging(self):
        return list(self.directory.glob(".forum-pending-*"))

    def old_copies(self, count=8):
        paths = []
        for number in range(count):
            path = self.directory / f"forum-20260101T000000000000Z-{number:016x}.sql"
            path.write_bytes(GOOD_DUMP)
            path.chmod(0o600)
            paths.append(path)
        return paths

    def assert_reaped(self, prefix=""):
        pid = int((self.root / (prefix + "pid")).read_text())
        with self.assertRaises(ProcessLookupError):
            os.kill(pid, 0)
        with self.assertRaises(ChildProcessError):
            os.waitpid(pid, os.WNOHANG)

    def test_success_publishes_private_complete_dump_and_keeps_seven(self):
        old = self.old_copies()
        self.assertEqual(self.run_backup(), 0)
        self.assertEqual(len(self.copies()), 7)
        self.assertFalse(old[0].exists())
        self.assertFalse(old[1].exists())
        for path in self.copies():
            self.assertEqual(path.read_bytes(), GOOD_DUMP)
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            self.assertEqual(path.stat().st_nlink, 1)
        self.assertEqual(self.staging(), [])
        self.assert_reaped()

    def test_command_uses_only_local_socket_and_dedicated_account(self):
        with patch.dict(
            os.environ, {"MYSQL_PWD": "never-inherit", "MYSQL_HOME": "/private"}
        ):
            self.run_backup()
        call = json.loads((self.root / "call").read_text())
        self.assertEqual(
            call["args"],
            [
                "--no-defaults",
                "--protocol=socket",
                "--socket=/run/mysqld/mysqld.sock",
                "--user=seeded-forum-backup",
                "--single-transaction",
                "--quick",
                "--skip-lock-tables",
                "--skip-add-locks",
                "--no-tablespaces",
                "--routines",
                "--events",
                "--triggers",
                "--hex-blob",
                "--comments",
                "--skip-dump-date",
                "flarum",
            ],
        )
        self.assertNotIn("MYSQL_PWD", call["env"])
        self.assertNotIn("MYSQL_HOME", call["env"])

    def test_failed_dump_never_prunes_or_leaks_stderr(self):
        old = self.old_copies()
        (self.root / "mode").write_text("fail")
        diagnostic = io.StringIO()
        with contextlib.redirect_stderr(diagnostic):
            self.assertEqual(
                forum_backup.main(["--directory", str(self.directory)]), 75
            )
        self.assertIn("exit 2", diagnostic.getvalue())
        self.assertNotIn("PRIVATE_DATABASE_ERROR_SENTINEL", diagnostic.getvalue())
        self.assertNotIn("CREATE TABLE", diagnostic.getvalue())
        self.assertEqual(self.copies(), old)
        self.assertEqual(self.staging(), [])
        self.assert_reaped()

    def test_empty_and_partial_successful_processes_do_not_publish_or_prune(self):
        old = self.old_copies()
        for payload in (
            b"",
            b"-- MariaDB dump 10.19\nSELECT 1;\n",
            b"-- Dump completed\n",
            b"-- MariaDB dump 10.19\n-- Dump completed on 2026-10-09  3:31:07\n",
        ):
            with self.subTest(payload=payload):
                (self.root / "payload").write_bytes(payload)
                with self.assertRaisesRegex(JobError, "incomplete"):
                    self.run_backup()
                self.assertEqual(self.copies(), old)
                self.assertEqual(self.staging(), [])

    def test_byte_bound_prevents_publication_even_for_complete_dump(self):
        old = self.old_copies()
        with self.assertRaisesRegex(JobError, "byte bound"):
            self.run_backup(max_bytes=len(GOOD_DUMP) - 1)
        self.assertEqual(self.copies(), old)
        self.assertEqual(self.staging(), [])
        self.assert_reaped()
        self.assertEqual(self.run_backup(max_bytes=len(GOOD_DUMP)), 0)

    def test_timeout_kills_and_reaps_with_stdout_open_or_closed(self):
        old = self.old_copies()
        for mode in ("hang", "closed_hang"):
            with self.subTest(mode=mode):
                (self.root / "mode").write_text(mode)
                with (
                    patch.object(forum_backup, "SECONDS", 0.5),
                    self.assertRaises(JobError) as error,
                ):
                    self.run_backup()
                self.assertEqual(error.exception.code, 75)
                self.assertEqual(self.copies(), old)
                self.assertEqual(self.staging(), [])
                self.assert_reaped()

    def test_interruption_kills_and_reaps_and_allows_retry(self):
        old = self.old_copies()
        (self.root / "mode").write_text("hang")
        pid_file = self.root / "pid"

        class CancelOnStart(Stop):
            def check(self, deadline):
                self.requested = pid_file.exists()
                super().check(deadline)

        with self.assertRaises(JobError) as error:
            self.run_backup(CancelOnStart())
        self.assertEqual(error.exception.code, 130)
        self.assertEqual(self.copies(), old)
        self.assertEqual(self.staging(), [])
        self.assert_reaped()
        (self.root / "mode").write_text("success")
        self.assertEqual(self.run_backup(), 0)

    def test_lock_contention_preserves_staging_without_starting_dump(self):
        stale = self.directory / ".forum-pending-0000000000000000.sql"
        stale.write_bytes(b"incomplete")
        stale.chmod(0o600)
        with (
            state_directory(self.directory) as directory,
            writer_lock(directory),
            self.assertRaises(JobError) as error,
        ):
            self.run_backup()
        self.assertEqual(error.exception.code, 75)
        self.assertTrue(stale.exists())
        self.assertFalse((self.root / "call").exists())

    def test_child_inherits_the_lock(self):
        original = forum_backup.subprocess.Popen

        def inspect(*args, **kwargs):
            (inherited,) = kwargs["pass_fds"]
            lock_fd = os.open(self.directory / "maintenance.lock", os.O_RDWR)
            try:
                self.assertTrue(os.fstat(inherited).st_ino == os.fstat(lock_fd).st_ino)
                with self.assertRaises(BlockingIOError):
                    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            finally:
                os.close(lock_fd)
            return original(*args, **kwargs)

        with patch.object(forum_backup.subprocess, "Popen", side_effect=inspect):
            self.run_backup()

    def test_stale_staging_and_interrupted_publication_are_recovered(self):
        stale = self.directory / ".forum-pending-ffffffffffffffff.sql"
        stale.write_bytes(b"incomplete")
        stale.chmod(0o600)
        (old,) = self.old_copies(1)
        pending = self.directory / ".forum-pending-0000000000000000.sql"
        pending.hardlink_to(old)
        self.assertEqual(self.run_backup(), 0)
        self.assertEqual(self.staging(), [])
        self.assertEqual(old.stat().st_nlink, 1)
        self.assertEqual(len(self.copies()), 2)

    def test_scan_bound_prevents_cleanup_and_dump(self):
        self.old_copies(2)
        with (
            patch.object(forum_backup, "MAX_ENTRIES", 1),
            self.assertRaisesRegex(JobError, "scan bound"),
        ):
            self.run_backup()
        self.assertEqual(len(self.copies()), 2)
        self.assertFalse((self.root / "call").exists())

    def test_symlinked_directory_or_parent_is_rejected(self):
        alias = self.root / "alias"
        alias.symlink_to(self.directory, target_is_directory=True)
        with self.assertRaises(OSError):
            forum_backup.run(alias, forum_backup.DEFAULT_SOCKET, Stop())
        parent = self.root / "parent"
        parent.symlink_to(self.root, target_is_directory=True)
        with self.assertRaises(OSError):
            forum_backup.run(parent / "backups", forum_backup.DEFAULT_SOCKET, Stop())
        self.assertFalse((self.root / "call").exists())

    def test_symlinked_lock_staging_copy_or_unknown_entry_is_rejected(self):
        target = self.root / "preserved"
        target.write_bytes(b"preserve")
        target.chmod(0o600)
        for name in (
            "maintenance.lock",
            ".forum-pending-ffffffffffffffff.sql",
            "forum-20260101T000000000000Z-ffffffffffffffff.sql",
            "unexpected",
        ):
            with self.subTest(name=name):
                candidate = self.directory / name
                with contextlib.suppress(FileNotFoundError):
                    candidate.unlink()
                candidate.symlink_to(target)
                with self.assertRaises((OSError, JobError)):
                    self.run_backup()
                candidate.unlink()
                self.assertEqual(target.read_bytes(), b"preserve")
        self.assertFalse((self.root / "call").exists())

    def test_insecure_mode_and_unexpected_hardlink_are_rejected(self):
        self.directory.chmod(0o755)
        with self.assertRaises(JobError):
            self.run_backup()
        self.directory.chmod(0o700)
        (old,) = self.old_copies(1)
        (self.root / "outside").hardlink_to(old)
        with self.assertRaisesRegex(JobError, "hard link"):
            self.run_backup()
        self.assertFalse((self.root / "call").exists())

    def test_missing_dump_executable_cleans_staging_without_pruning(self):
        old = self.old_copies()
        self.executable.unlink()
        with self.assertRaises(FileNotFoundError):
            self.run_backup()
        self.assertEqual(self.copies(), old)
        self.assertEqual(self.staging(), [])

    def test_cancellation_before_publication_keeps_old_copies(self):
        old = self.old_copies()
        stop = Stop()
        original = forum_backup.stream_dump

        def cancel(*args, **kwargs):
            original(*args, **kwargs)
            stop.requested = True

        with (
            patch.object(forum_backup, "stream_dump", side_effect=cancel),
            self.assertRaises(JobError),
        ):
            self.run_backup(stop)
        self.assertEqual(self.copies(), old)
        self.assertEqual(self.staging(), [])

    def test_directory_move_during_dump_prevents_publication(self):
        original = forum_backup.stream_dump

        def move(*args, **kwargs):
            original(*args, **kwargs)
            self.directory.rename(self.root / "moved")
            self.directory.mkdir(mode=0o700)

        with (
            patch.object(forum_backup, "stream_dump", side_effect=move),
            self.assertRaisesRegex(JobError, "moved"),
        ):
            self.run_backup()
        self.assertEqual(list((self.root / "moved").glob("*.sql")), [])
        self.assertEqual(self.copies(), [])

    def test_cli_installs_and_restores_signal_handlers(self):
        before = {
            number: signal.getsignal(number)
            for number in (signal.SIGTERM, signal.SIGINT)
        }
        self.assertEqual(forum_backup.main(["--directory", str(self.directory)]), 0)
        self.assertEqual(
            {number: signal.getsignal(number) for number in before}, before
        )

    def test_invalid_bounds_and_relative_socket_fail_before_dump(self):
        with self.assertRaises(JobError):
            self.run_backup(max_bytes=0)
        with self.assertRaises(JobError):
            forum_backup.run(self.directory, Path("relative.sock"), Stop())
        self.assertEqual(self.staging(), [])
        self.assertFalse((self.root / "call").exists())

    def test_engine_preflight_rejects_nontransactional_or_invalid_response(self):
        old = self.old_copies()
        for response in (b"1\n", b"", b"broken\n", b"0\nextra\n", b"0" * 33):
            with self.subTest(response=response):
                (self.root / "query-payload").write_bytes(response)
                with self.assertRaises(JobError):
                    self.run_backup()
                self.assertFalse((self.root / "call").exists())
                self.assertEqual(self.copies(), old)
                self.assertEqual(self.staging(), [])
                self.assert_reaped("query-")

    def test_engine_preflight_failure_and_timeout_do_not_start_dump(self):
        old = self.old_copies()
        for mode in ("fail", "hang"):
            with self.subTest(mode=mode):
                (self.root / "query-mode").write_text(mode)
                with (
                    patch.object(forum_backup, "SECONDS", 0.5),
                    self.assertRaises(JobError) as error,
                ):
                    self.run_backup()
                self.assertEqual(error.exception.code, 75)
                self.assertFalse((self.root / "call").exists())
                self.assertEqual(self.copies(), old)
                self.assert_reaped("query-")

    def test_engine_preflight_uses_same_restricted_socket_account(self):
        self.run_backup()
        arguments = json.loads((self.root / "query-call").read_text())["args"]
        self.assertEqual(
            arguments[:4], forum_backup.connection_options(forum_backup.DEFAULT_SOCKET)
        )
        self.assertIn(
            "TABLE_SCHEMA='flarum' AND TABLE_TYPE='BASE TABLE'", arguments[-1]
        )
        self.assertIn("ENGINE IS NULL OR ENGINE <> 'InnoDB'", arguments[-1])

    def test_engine_preflight_cancellation_reaps_and_preserves_copies(self):
        old = self.old_copies()
        (self.root / "query-mode").write_text("hang")
        pid_file = self.root / "query-pid"

        class CancelOnStart(Stop):
            def check(self, deadline):
                self.requested = pid_file.exists()
                super().check(deadline)

        with self.assertRaises(JobError) as error:
            self.run_backup(CancelOnStart())
        self.assertEqual(error.exception.code, 130)
        self.assertEqual(self.copies(), old)
        self.assertFalse((self.root / "call").exists())
        self.assert_reaped("query-")

    def test_disk_write_failure_reaps_and_does_not_publish_or_prune(self):
        old = self.old_copies()
        diagnostic = io.StringIO()
        with (
            patch.object(
                forum_backup.os,
                "write",
                side_effect=OSError(errno.ENOSPC, "PRIVATE_DATA_SENTINEL"),
            ),
            contextlib.redirect_stderr(diagnostic),
        ):
            self.assertEqual(
                forum_backup.main(["--directory", str(self.directory)]), 75
            )
        self.assertNotIn("PRIVATE_DATA_SENTINEL", diagnostic.getvalue())
        self.assertEqual(self.copies(), old)
        self.assertEqual(self.staging(), [])
        self.assert_reaped()

    def test_publication_failure_cleans_staging_and_does_not_prune(self):
        old = self.old_copies()
        with (
            patch.object(
                forum_backup.os, "link", side_effect=OSError(errno.ENOSPC, "full")
            ),
            self.assertRaises(OSError),
        ):
            self.run_backup()
        self.assertEqual(self.copies(), old)
        self.assertEqual(self.staging(), [])

    def test_publication_collision_never_overwrites_an_existing_copy(self):
        old = self.old_copies()
        stamp = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
        with (
            patch.object(forum_backup.datetime, "datetime") as clock,
            patch.object(
                forum_backup.secrets, "token_hex", return_value="0000000000000000"
            ),
        ):
            clock.now.return_value = stamp
            with self.assertRaises(FileExistsError):
                self.run_backup()
        self.assertEqual(self.copies(), old)
        self.assertEqual(old[0].read_bytes(), GOOD_DUMP)
        self.assertEqual(self.staging(), [])

    def test_retention_keeps_new_copy_after_clock_moves_backwards(self):
        self.old_copies()
        stamp = datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc)
        manual = self.directory / "manual.sql"
        manual.write_bytes(b"keep")
        manual.chmod(0o600)
        with patch.object(forum_backup.datetime, "datetime") as clock:
            clock.now.return_value = stamp
            self.run_backup()
        self.assertEqual(len(self.copies()), 7)
        self.assertEqual(len(list(self.directory.glob("forum-2025*.sql"))), 1)
        self.assertEqual(manual.read_bytes(), b"keep")


if __name__ == "__main__":
    unittest.main()
