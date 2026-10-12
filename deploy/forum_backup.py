#!/usr/bin/env python3
"""Bounded local MariaDB backup; no passwords or forum configuration."""

import argparse
import contextlib
import datetime
import os
import re
import secrets
import selectors
import signal
import subprocess
import sys
import time
from collections.abc import Callable
from pathlib import Path

from maintain import (
    JobError,
    Stop,
    check_directory_identity,
    checked_file,
    state_directory,
    writer_lock,
)

DUMP_EXECUTABLE = "/usr/bin/mariadb-dump"
CLIENT_EXECUTABLE = "/usr/bin/mariadb"
DEFAULT_DIRECTORY = Path("/var/lib/seeded-forum-backup")
DEFAULT_SOCKET = Path("/run/mysqld/mysqld.sock")
MAX_BYTES = 1024**3
MAX_ENTRIES = 4096
SECONDS = 600.0
STOP_SECONDS = 5.0
KILL_SECONDS = 5.0
KEEP = 7
COPY_NAME = re.compile(r"forum-[0-9]{8}T[0-9]{12}Z-([0-9a-f]{16})\.sql")
STAGING_NAME = re.compile(r"\.forum-pending-([0-9a-f]{16})\.sql")
COMPLETION = re.compile(rb"\n-- Dump completed\n\Z")


def scan(directory: int, deadline: float, stop: Stop) -> dict[str, os.stat_result]:
    files = {}
    with os.scandir(directory) as entries:
        for number, entry in enumerate(entries):
            stop.check(deadline)
            if number >= MAX_ENTRIES:
                raise JobError("forum backup directory exceeds scan bound", 1)
            # A crash between link and unlink can leave one publication pair.
            links = os.stat(
                entry.name, dir_fd=directory, follow_symlinks=False
            ).st_nlink
            if links == 2 and (
                COPY_NAME.fullmatch(entry.name) or STAGING_NAME.fullmatch(entry.name)
            ):
                files[entry.name] = checked_file(directory, entry.name, links=2)
            else:
                files[entry.name] = checked_file(directory, entry.name)
    return files


def same_file(before: os.stat_result, after: os.stat_result) -> bool:
    return (before.st_dev, before.st_ino) == (after.st_dev, after.st_ino)


def clean_staging(directory: int, deadline: float, stop: Stop) -> None:
    files = scan(directory, deadline, stop)
    paired: set[str] = set()
    pending = []
    for name, info in files.items():
        match = STAGING_NAME.fullmatch(name)
        if not match:
            continue
        pending.append(name)
        if info.st_nlink == 2:
            copies = [
                other
                for other, candidate in files.items()
                if COPY_NAME.fullmatch(other)
                and other.endswith(f"-{match[1]}.sql")
                and same_file(info, candidate)
            ]
            if len(copies) != 1:
                raise JobError("ambiguous forum backup publication", 1)
            paired.update((name, copies[0]))
    if any(info.st_nlink == 2 and name not in paired for name, info in files.items()):
        raise JobError("unexpected forum backup hard link", 1)
    for name in pending:
        stop.check(deadline)
        if not same_file(
            files[name], checked_file(directory, name, files[name].st_nlink)
        ):
            raise JobError("forum backup staging changed", 1)
        os.unlink(name, dir_fd=directory)
    os.fsync(directory)


def connection_options(socket: Path) -> list[str]:
    if not socket.is_absolute() or ".." in socket.parts:
        raise JobError("MariaDB socket requires an absolute path")
    return [
        "--no-defaults",
        "--protocol=socket",
        f"--socket={socket}",
        "--user=seeded-forum-backup",
    ]


def dump_command(socket: Path) -> list[str]:
    return [
        DUMP_EXECUTABLE,
        *connection_options(socket),
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
    ]


def stream_child(
    command: list[str],
    lock: int,
    max_bytes: int,
    deadline: float,
    stop: Stop,
    consume: Callable[[bytes], None],
) -> None:
    stop.check(deadline)
    child = subprocess.Popen(
        command,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        env={"PATH": "/usr/bin:/bin", "LC_ALL": "C"},
        pass_fds=(lock,),
        start_new_session=True,
    )
    try:
        assert child.stdout is not None
        size = 0
        with selectors.DefaultSelector() as selector:
            os.set_blocking(child.stdout.fileno(), False)
            selector.register(child.stdout, selectors.EVENT_READ)
            while selector.get_map():
                stop.check(deadline)
                if not selector.select(timeout=0.1):
                    continue
                chunk = os.read(child.stdout.fileno(), 64 * 1024)
                if not chunk:
                    selector.unregister(child.stdout)
                    break
                size += len(chunk)
                if size > max_bytes:
                    raise JobError(
                        "MariaDB output exceeds byte bound; older copies retained", 1
                    )
                consume(chunk)
        while child.poll() is None:
            stop.check(deadline)
            try:
                child.wait(timeout=0.1)
            except subprocess.TimeoutExpired:
                pass
        stop.check(deadline)
        if child.returncode != 0:
            raise JobError(
                f"MariaDB command failed (exit {child.returncode}); older copies retained",
                75,
            )
    finally:
        # Retain the inherited lock until the dump process is reaped.
        try:
            if child.poll() is None:
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(child.pid, signal.SIGTERM)
                try:
                    child.wait(timeout=STOP_SECONDS)
                except subprocess.TimeoutExpired:
                    with contextlib.suppress(ProcessLookupError):
                        os.killpg(child.pid, signal.SIGKILL)
                    try:
                        child.wait(timeout=KILL_SECONDS)
                    except subprocess.TimeoutExpired as error:
                        raise JobError("MariaDB dump did not stop", 75) from error
        finally:
            if child.stdout is not None:
                child.stdout.close()


def check_engines(lock: int, socket: Path, deadline: float, stop: Stop) -> None:
    result = bytearray()
    stream_child(
        [
            CLIENT_EXECUTABLE,
            *connection_options(socket),
            "--batch",
            "--raw",
            "--skip-column-names",
            (
                "--execute=SELECT COUNT(*) FROM information_schema.TABLES "
                "WHERE TABLE_SCHEMA='flarum' AND TABLE_TYPE='BASE TABLE' "
                "AND (ENGINE IS NULL OR ENGINE <> 'InnoDB')"
            ),
        ],
        lock,
        32,
        deadline,
        stop,
        result.extend,
    )
    if result != b"0\n":
        raise JobError(
            "forum backup requires InnoDB base tables; older copies retained", 1
        )


def stream_dump(
    output: int, lock: int, socket: Path, max_bytes: int, deadline: float, stop: Stop
) -> None:
    head = b""
    tail = b""

    def write(chunk: bytes) -> None:
        nonlocal head, tail
        head = (head + chunk)[:4096]
        tail = (tail + chunk)[-4096:]
        remaining = memoryview(chunk)
        while remaining:
            stop.check(deadline)
            written = os.write(output, remaining)
            if written == 0:
                raise JobError("forum dump write failed", 1)
            remaining = remaining[written:]

    stream_child(dump_command(socket), lock, max_bytes, deadline, stop, write)
    if b"-- MariaDB dump " not in head or not COMPLETION.search(tail):
        raise JobError("incomplete forum dump; older copies retained", 1)


def prune(directory: int, newest: str, deadline: float, stop: Stop) -> None:
    files = scan(directory, deadline, stop)
    copies = sorted(
        name for name in files if COPY_NAME.fullmatch(name) and name != newest
    )
    for name in copies[: max(0, len(copies) - KEEP + 1)]:
        stop.check(deadline)
        if not same_file(files[name], checked_file(directory, name)):
            raise JobError("forum backup changed during retention", 1)
        os.unlink(name, dir_fd=directory)
    os.fsync(directory)


def run(
    directory_path: Path, socket: Path, stop: Stop, max_bytes: int = MAX_BYTES
) -> int:
    if max_bytes <= 0:
        raise JobError("forum dump byte bound must be positive")
    deadline = time.monotonic() + SECONDS
    with state_directory(directory_path) as directory, writer_lock(directory) as lock:
        clean_staging(directory, deadline, stop)
        check_engines(lock, socket, deadline, stop)
        suffix = secrets.token_hex(8)
        timestamp = datetime.datetime.now(datetime.timezone.utc).strftime(
            "%Y%m%dT%H%M%S%fZ"
        )
        pending = f".forum-pending-{suffix}.sql"
        final = f"forum-{timestamp}-{suffix}.sql"
        fd = os.open(
            pending,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
            0o600,
            dir_fd=directory,
        )
        try:
            stream_dump(fd, lock, socket, max_bytes, deadline, stop)
            os.fsync(fd)
            check_directory_identity(directory_path, directory)
            if not same_file(os.fstat(fd), checked_file(directory, pending)):
                raise JobError("forum backup staging changed", 1)
            stop.check(deadline)
            os.link(
                pending,
                final,
                src_dir_fd=directory,
                dst_dir_fd=directory,
                follow_symlinks=False,
            )
            os.fsync(directory)
        finally:
            os.close(fd)
            os.unlink(pending, dir_fd=directory)
            os.fsync(directory)
        prune(directory, final, deadline, stop)
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, default=DEFAULT_DIRECTORY)
    parser.add_argument("--socket", type=Path, default=DEFAULT_SOCKET)
    parser.add_argument("--max-bytes", type=int, default=MAX_BYTES)
    args = parser.parse_args(argv)
    stop = Stop()
    previous = {
        number: signal.signal(number, stop.signal)
        for number in (signal.SIGINT, signal.SIGTERM)
    }
    try:
        return run(args.directory, args.socket, stop, args.max_bytes)
    except JobError as error:
        print(str(error), file=sys.stderr)
        return error.code
    except OSError as error:
        print(f"forum backup I/O failure ({error.errno})", file=sys.stderr)
        return 75
    finally:
        for number, handler in previous.items():
            signal.signal(number, handler)


if __name__ == "__main__":
    sys.exit(main())
