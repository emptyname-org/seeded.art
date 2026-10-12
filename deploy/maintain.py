#!/usr/bin/env python3
"""Optional locked archive jobs. Run as seeded; never install or activate services."""

import argparse
import contextlib
import datetime
import fcntl
import json
import os
import re
import secrets
import signal
import sqlite3
import stat
import subprocess
import sys
import time
from collections.abc import Iterator
from pathlib import Path
from types import FrameType
from typing import NoReturn

LIMITS = {
    "sync": 480.0,
    "verify": 1800.0,
    "backup": 600.0,
    "sales": 600.0,
    "articles": 300.0,
    "profiles": 2400.0,
}
STOP_SECONDS = 15.0
KILL_SECONDS = 5.0
MAX_BACKUP_ENTRIES = 4096
MAX_STAGED_BACKUPS = 8
KEEP = 2
BACKUP_NAME = re.compile(r"scheduled-[0-9]{8}T[0-9]{12}Z-[0-9a-f]{16}\.sqlite")
PENDING_NAME = re.compile(r"\.scheduled-pending-([0-9a-f]{16})\.sqlite")
RETRY_NAME = ".backup-retry.json"
RETRY_PENDING = ".backup-retry.pending"
BACKUP_FOLDERS = {
    "backups",
    "backups-sales",
    "backups-articles",
    "backups-trading",
}


class JobError(Exception):
    def __init__(self, message: str, code: int = 64):
        super().__init__(message)
        self.code = code


class Stop:
    requested = False

    def signal(self, _number: int, _frame: FrameType | None) -> None:
        self.requested = True

    def check(self, deadline: float) -> None:
        if self.requested:
            raise JobError("job interrupted; committed progress retained", 130)
        if time.monotonic() >= deadline:
            raise JobError("job deadline reached; retry to resume", 75)


def checked_file(parent: int, name: str, links: int = 1) -> os.stat_result:
    info = os.stat(name, dir_fd=parent, follow_symlinks=False)
    if (
        not stat.S_ISREG(info.st_mode)
        or info.st_uid != os.geteuid()
        or info.st_mode & 0o077
        or info.st_nlink != links
    ):
        raise JobError("unsafe state file type, owner, mode or link count")
    return info


@contextlib.contextmanager
def state_directory(path: Path) -> Iterator[int]:
    if not path.is_absolute() or len(path.parts) < 4 or ".." in path.parts:
        raise JobError("state directory requires a dedicated absolute path")
    current = os.open("/", os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        for part in path.parts[1:]:
            child = os.open(
                part,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                dir_fd=current,
            )
            os.close(current)
            current = child
        info = os.fstat(current)
        if info.st_uid != os.geteuid() or info.st_mode & 0o077:
            raise JobError("state directory must be owned and mode 0700")
        yield current
    finally:
        os.close(current)


@contextlib.contextmanager
def writer_lock(state: int) -> Iterator[int]:
    fd = os.open(
        "maintenance.lock",
        os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC,
        0o600,
        dir_fd=state,
    )
    try:
        checked_file(state, "maintenance.lock")
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise JobError("maintenance writer busy; retry later", 75) from error
        yield fd
    finally:
        os.close(fd)


def executable(release: Path, name: str) -> str:
    # The operator-managed `current` release link is intentional. Writable state
    # paths and final executable files may never be symlinks.
    path = release.resolve(strict=True) / name
    info = path.lstat()
    if (
        not stat.S_ISREG(info.st_mode)
        or info.st_uid not in (0, os.geteuid())
        or info.st_mode & 0o022
        or not os.access(path, os.X_OK)
    ):
        raise JobError("release executable must be a trusted regular file")
    return str(path)


def run_child(
    command: list[str],
    environment: dict[str, str],
    descriptors: tuple[int, ...],
    deadline: float,
    stop: Stop,
) -> int:
    stop.check(deadline)
    child = subprocess.Popen(
        command,
        env=environment,
        stdin=subprocess.DEVNULL,
        pass_fds=descriptors,
        start_new_session=True,
    )
    try:
        while True:
            stop.check(deadline)
            try:
                code = child.wait(
                    timeout=min(0.1, max(0.001, deadline - time.monotonic()))
                )
                if code >= 0:
                    return code
                return 130 if -code in (signal.SIGINT, signal.SIGTERM) else 1
            except subprocess.TimeoutExpired:
                pass
    except BaseException:
        # Signal the whole owned process group; the inherited lock stays held
        # until the child exits, even if this runner is killed abruptly.
        with contextlib.suppress(ProcessLookupError):
            os.killpg(child.pid, signal.SIGTERM)
        try:
            child.wait(timeout=STOP_SECONDS)
        except subprocess.TimeoutExpired:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(child.pid, signal.SIGKILL)
            child.wait(timeout=KILL_SECONDS)
        raise


def backup_directory(state: int, name: str = "backups") -> int:
    try:
        os.mkdir(name, mode=0o700, dir_fd=state)
    except FileExistsError:
        pass
    fd = os.open(
        name,
        os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
        dir_fd=state,
    )
    info = os.fstat(fd)
    if info.st_uid != os.geteuid() or info.st_mode & 0o077:
        os.close(fd)
        raise JobError("backup directory must be owned and mode 0700")
    return fd


def validate_backup(
    directory: int, name: str, deadline: float, stop: Stop, links: int = 1
) -> None:
    info = checked_file(directory, name, links)
    if info.st_size == 0:
        raise JobError("empty backup; older copies retained", 1)
    fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=directory)
    try:
        stop.check(deadline)
        with contextlib.closing(
            sqlite3.connect(
                f"file:/proc/self/fd/{fd}?mode=ro&immutable=1",
                uri=True,
                timeout=0,
            )
        ) as database:
            database.set_progress_handler(
                lambda: int(stop.requested or time.monotonic() >= deadline),
                1000,
            )
            if database.execute("PRAGMA quick_check").fetchone() != ("ok",):
                raise JobError("backup validation failed; older copies retained", 1)
        stop.check(deadline)
    except sqlite3.DatabaseError as error:
        stop.check(deadline)
        raise JobError("backup validation failed; older copies retained", 1) from error
    finally:
        os.close(fd)


def recover_publication(directory: int, deadline: float, stop: Stop) -> None:
    names = []
    with os.scandir(directory) as entries:
        for number, entry in enumerate(entries):
            stop.check(deadline)
            if number >= MAX_BACKUP_ENTRIES:
                raise JobError("backup directory exceeds scan bound", 1)
            names.append(entry.name)
    for pending in names:
        match = PENDING_NAME.fullmatch(pending)
        if not match:
            continue
        info = os.stat(pending, dir_fd=directory, follow_symlinks=False)
        if info.st_nlink != 2:
            checked_file(directory, pending)
            continue
        checked_file(directory, pending, links=2)
        published = [
            name
            for name in names
            if BACKUP_NAME.fullmatch(name) and name.endswith(f"-{match[1]}.sqlite")
        ]
        if len(published) != 1:
            raise JobError(
                "ambiguous backup publication; copies retained for inspection", 1
            )
        final = checked_file(directory, published[0], links=2)
        if (info.st_dev, info.st_ino) != (final.st_dev, final.st_ino):
            raise JobError("backup publication links disagree; copies retained", 1)
        validate_backup(directory, published[0], deadline, stop, links=2)
        os.fsync(directory)
        os.unlink(pending, dir_fd=directory)
        os.fsync(directory)


def check_directory_identity(path: Path, descriptor: int) -> None:
    with state_directory(path) as current:
        first, second = os.fstat(descriptor), os.fstat(current)
        if (first.st_dev, first.st_ino) != (second.st_dev, second.st_ino):
            raise JobError("state directory moved during the job", 1)


def prune(directory: int, newest: str, deadline: float, stop: Stop) -> None:
    copies: list[tuple[str, os.stat_result]] = []
    with os.scandir(directory) as entries:
        for number, entry in enumerate(entries):
            stop.check(deadline)
            if number >= MAX_BACKUP_ENTRIES:
                raise JobError("backup directory exceeds scan bound; nothing pruned", 1)
            if BACKUP_NAME.fullmatch(entry.name):
                copies.append((entry.name, checked_file(directory, entry.name)))
    # Keep the just-published copy even after a clock correction. Manual and
    # interrupted `.scheduled-pending-*` backups are outside this namespace.
    previous = sorted((name, info) for name, info in copies if name != newest)
    for name, before in previous[: max(0, len(previous) - (KEEP - 1))]:
        stop.check(deadline)
        after = checked_file(directory, name)
        if (before.st_dev, before.st_ino) != (after.st_dev, after.st_ino):
            raise JobError("backup changed during retention; stopping", 1)
        os.unlink(name, dir_fd=directory)
    os.fsync(directory)


class BackupRetry:
    """Exact successful copies from one unfinished daily run; never a daily cache."""

    def __init__(self, state: int):
        self.state = state
        self.day = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d")
        self.copies: dict[str, str] = {}
        try:
            info = checked_file(state, RETRY_NAME)
        except FileNotFoundError:
            return
        if info.st_size > 1024:
            raise JobError("backup retry record exceeds size bound", 1)
        fd = os.open(RETRY_NAME, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=state)
        try:
            value = json.loads(os.read(fd, 1025))
        except (ValueError, UnicodeError) as error:
            raise JobError(
                "invalid backup retry record; retained for inspection", 1
            ) from error
        finally:
            os.close(fd)
        if (
            not isinstance(value, dict)
            or set(value) != {"date", "copies"}
            or not isinstance(value["date"], str)
            or not re.fullmatch(r"[0-9]{8}", value["date"])
            or not isinstance(value["copies"], dict)
            or not set(value["copies"]).issubset(BACKUP_FOLDERS)
            or any(
                not isinstance(name, str) or not BACKUP_NAME.fullmatch(name)
                for name in value["copies"].values()
            )
        ):
            raise JobError("invalid backup retry record; retained for inspection", 1)
        if value["date"] == self.day:
            self.copies = value["copies"]

    def save(self) -> None:
        # A killed previous write leaves at most this one staging file.
        with contextlib.suppress(FileNotFoundError):
            checked_file(self.state, RETRY_PENDING)
            os.unlink(RETRY_PENDING, dir_fd=self.state)
        fd = os.open(
            RETRY_PENDING,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
            0o600,
            dir_fd=self.state,
        )
        try:
            payload = json.dumps({"date": self.day, "copies": self.copies}).encode()
            with os.fdopen(fd, "wb", closefd=False) as output:
                output.write(payload)
                output.flush()
                os.fsync(fd)
            with contextlib.suppress(FileNotFoundError):
                checked_file(self.state, RETRY_NAME)
            os.replace(
                RETRY_PENDING, RETRY_NAME, src_dir_fd=self.state, dst_dir_fd=self.state
            )
            os.fsync(self.state)
        finally:
            os.close(fd)
            with contextlib.suppress(FileNotFoundError):
                os.unlink(RETRY_PENDING, dir_fd=self.state)

    def record(self, folder: str, name: str | None) -> None:
        if name is None:
            self.copies.pop(folder, None)
        else:
            self.copies[folder] = name
        self.save()

    def finish(self) -> None:
        checked_file(self.state, RETRY_NAME)
        os.unlink(RETRY_NAME, dir_fd=self.state)
        os.fsync(self.state)


def backup(
    release: Path,
    state_path: Path,
    state: int,
    lock: int,
    environment: dict[str, str],
    deadline: float,
    stop: Stop,
    folder: str = "backups",
    retry: BackupRetry | None = None,
) -> int:
    directory = backup_directory(state, folder)
    try:
        recover_publication(directory, deadline, stop)
        staged = 0
        with os.scandir(directory) as entries:
            for number, entry in enumerate(entries):
                stop.check(deadline)
                if number >= MAX_BACKUP_ENTRIES:
                    raise JobError("backup directory exceeds scan bound", 1)
                if entry.name.startswith(
                    (".scheduled-pending-", ".seeded-backup-", ".scheduled-invalid-")
                ):
                    checked_file(directory, entry.name)
                    staged += 1
        newest = retry.copies.get(folder) if retry else None
        if retry and newest:
            try:
                validate_backup(directory, newest, deadline, stop)
            except FileNotFoundError:
                # Interrupted quarantine or external removal: make a fresh copy.
                retry.record(folder, None)
            except JobError as error:
                if error.code != 1:
                    raise
                checked_file(directory, newest)
                quarantined = f".scheduled-invalid-{newest}"
                try:
                    os.stat(quarantined, dir_fd=directory, follow_symlinks=False)
                except FileNotFoundError:
                    pass
                else:
                    raise JobError(
                        "invalid backup quarantine already exists", 1
                    ) from error
                os.rename(
                    newest, quarantined, src_dir_fd=directory, dst_dir_fd=directory
                )
                os.fsync(directory)
                staged += 1
                retry.record(folder, None)
            else:
                prune(directory, newest, deadline, stop)
                return 0
        if staged >= MAX_STAGED_BACKUPS:
            raise JobError("staged backups require inspection before another backup", 1)
        suffix = secrets.token_hex(8)
        timestamp = datetime.datetime.now(datetime.timezone.utc).strftime(
            "%Y%m%dT%H%M%S%fZ"
        )
        pending = f".scheduled-pending-{suffix}.sqlite"
        final = f"scheduled-{timestamp}-{suffix}.sqlite"
        for name in (pending, final):
            try:
                os.stat(name, dir_fd=directory, follow_symlinks=False)
            except FileNotFoundError:
                continue
            raise JobError("backup name collision; retry later", 75)
        # The CLI's own deadline defaults to 300 s, too short for a large archive;
        # give it most of this database's budget and keep the rest for validation.
        timeout = max(1, min(3600, int((deadline - time.monotonic()) * 0.6)))
        code = run_child(
            [
                executable(release, "seeded_archive"),
                "backup",
                str(state_path / folder / pending),
            ],
            dict(environment, SEEDED_BACKUP_TIMEOUT_SECS=str(timeout)),
            (state, lock, directory),
            deadline,
            stop,
        )
        if code != 0:
            # The backup CLI has only a generic failure code. Retry it boundedly;
            # our own failed integrity validation remains a non-retryable error.
            return 75 if code == 1 else code
        check_directory_identity(state_path, state)
        check_directory_identity(state_path / folder, directory)
        validate_backup(directory, pending, deadline, stop)
        stop.check(deadline)
        os.link(
            pending,
            final,
            src_dir_fd=directory,
            dst_dir_fd=directory,
            follow_symlinks=False,
        )
        os.fsync(directory)
        os.unlink(pending, dir_fd=directory)
        os.fsync(directory)
        if retry:
            retry.record(folder, final)
        prune(directory, final, deadline, stop)
        return 0
    finally:
        # A failed CLI can have published a valid backup before reporting an I/O
        # failure. Keep its unique pending destination for operator inspection.
        os.close(directory)


def run(job: str, state_path: Path, release: Path, stop: Stop) -> int:
    deadline = time.monotonic() + LIMITS[job]
    configured = os.environ.get("SEEDED_DATABASE", str(state_path / "archive.sqlite"))
    if Path(configured) != state_path / "archive.sqlite":
        raise JobError(
            "SEEDED_DATABASE must be archive.sqlite within the state directory"
        )
    with state_directory(state_path) as state, writer_lock(state) as lock:
        environment = dict(os.environ)
        # Native SQLite NOFOLLOW deliberately rejects /proc/self/fd ancestors.
        # Use checked absolute paths; retain descriptors for locking/retention.
        database = str(state_path / "archive.sqlite")
        environment["SEEDED_DATABASE"] = database
        if job == "backup":
            databases = [("archive.sqlite", "backups")]
            for secondary in ("sales", "articles", "trading"):
                filename = f"{secondary}.sqlite"
                expected = os.environ.get(
                    f"SEEDED_{secondary.upper()}_DATABASE", str(state_path / filename)
                )
                if Path(expected) != state_path / filename:
                    raise JobError(
                        "secondary database must be inside the state directory"
                    )
                try:
                    checked_file(state, filename)
                except FileNotFoundError:
                    continue
                databases.append((filename, f"backups-{secondary}"))
            retry = BackupRetry(state)
            retry.save()
            failure: JobError | OSError | None = None
            result = 0
            for filename, folder in databases:
                # Each database gets its own budget; one busy source must not
                # consume the other databases' entire backup window.
                deadline = time.monotonic() + LIMITS[job]
                stop.check(deadline)
                env = dict(environment, SEEDED_DATABASE=str(state_path / filename))
                try:
                    if checked_file(state, filename).st_size == 0:
                        raise JobError("source database is empty", 1)
                    code = backup(
                        release,
                        state_path,
                        state,
                        lock,
                        env,
                        deadline,
                        stop,
                        folder,
                        retry,
                    )
                except JobError as error:
                    if error.code not in (1, 75):
                        raise
                    print(f"{filename}: {error}", file=sys.stderr)
                    failure = failure or error
                    code = error.code
                except OSError as error:
                    print(
                        f"{filename}: backup I/O failure ({error.errno})",
                        file=sys.stderr,
                    )
                    failure = failure or error
                    code = 75
                if code not in (0, 1, 75):
                    return code
                if code != 0:
                    # A permanent problem in one source must not suppress retries
                    # of another source's transient failure.
                    result = 75 if code == 75 or result == 75 else code
            if failure:
                if (
                    result == 75
                    and isinstance(failure, JobError)
                    and failure.code != 75
                ):
                    raise JobError(
                        "some backups need inspection; others need retry", 75
                    ) from failure
                raise failure
            if result == 0:
                retry.finish()
            return result
        if checked_file(state, "archive.sqlite").st_size == 0:
            raise JobError("archive database is empty")
        if job in ("sales", "articles"):
            filename = f"{job}.sqlite"
            secondary_path = state_path / filename
            expected = os.environ.get(
                f"SEEDED_{job.upper()}_DATABASE", str(secondary_path)
            )
            if Path(expected) != secondary_path:
                raise JobError("secondary database must be inside the state directory")
            try:
                checked_file(state, filename)
            except FileNotFoundError:
                pass  # The indexer may initialise a new secondary database.
            arguments = (
                ["sync", "--sales-db", str(secondary_path), "--archive-db", database]
                if job == "sales"
                else [
                    "sync",
                    "--db",
                    str(secondary_path),
                    "--archive-db",
                    database,
                ]
            )
            if job != "sales":
                return run_child(
                    [executable(release, f"seeded-{job}"), *arguments],
                    environment,
                    (state, lock),
                    deadline,
                    stop,
                )
            try:
                code = run_child(
                    [executable(release, f"seeded-{job}"), *arguments],
                    environment,
                    (state, lock),
                    deadline,
                    stop,
                )
            except JobError as error:
                if error.code not in (75, 130):
                    raise
                return error.code
            if code != 0:
                return code
            # A successful sync must leave a database for the volume refresh.
            # Treat a missing file as transient so the timer retries the sync.
            check_directory_identity(state_path, state)
            try:
                refreshed_info = checked_file(state, filename)
            except FileNotFoundError as error:
                raise JobError(
                    "sales database missing after sync; retry later", 75
                ) from error
            checked_file(state, "archive.sqlite")
            try:
                volume_code = run_child(
                    [
                        executable(release, "seeded-index"),
                        "volumes",
                        "--db",
                        database,
                        "--sales-db",
                        str(secondary_path),
                    ],
                    environment,
                    (state, lock),
                    deadline,
                    stop,
                )
            except JobError as error:
                if error.code not in (75, 130):
                    raise
                return error.code
            if volume_code != 0:
                return volume_code
            check_directory_identity(state_path, state)
            current_info = checked_file(state, filename)
            if (current_info.st_dev, current_info.st_ino) != (
                refreshed_info.st_dev,
                refreshed_info.st_ino,
            ):
                raise JobError("sales database changed during volume refresh", 1)
            return volume_code
        binary = executable(release, "seeded-index")
        commands = [(["verify", "--sample", "500"], deadline)]
        if job == "profiles":
            commands = [(["profiles", "--source", "objkt"], deadline)]
        if job == "sync":
            # Reserve time for listings and offers even after a slow token sync.
            commands = [
                (["sync"], time.monotonic() + LIMITS[job] * 0.65),
                (["escrow"], time.monotonic() + LIMITS[job] * 0.87),
                (["offers"], deadline),
            ]
        result = 0
        for arguments, until in commands:
            check_directory_identity(state_path, state)
            checked_file(state, "archive.sqlite")
            try:
                code = run_child(
                    [binary, *arguments, "--db", database],
                    environment,
                    (state, lock),
                    until,
                    stop,
                )
            except JobError as error:
                if error.code != 75:
                    raise
                code = 75
            if code not in (0, 75):
                return code
            result = max(result, code)
        return result


class Parser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        self.exit(64, f"{message}\n")


def main() -> int:
    parser = Parser(description=__doc__)
    parser.add_argument("job", choices=LIMITS)
    parser.add_argument("--state-dir", type=Path, default=Path("/var/lib/seeded"))
    parser.add_argument(
        "--release-dir", type=Path, default=Path(__file__).resolve().parent.parent
    )
    args = parser.parse_args()
    os.umask(0o077)
    stop = Stop()
    for number in (signal.SIGTERM, signal.SIGINT):
        signal.signal(number, stop.signal)
    try:
        return run(args.job, args.state_dir, args.release_dir, stop)
    except JobError as error:
        print(str(error), file=sys.stderr)
        return error.code
    except OSError as error:
        print(
            f"maintenance I/O failure ({error.errno}); no older backup pruned before publication",
            file=sys.stderr,
        )
        return 75


if __name__ == "__main__":
    sys.exit(main())
