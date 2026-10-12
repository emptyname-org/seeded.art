#!/usr/bin/env python3
"""Package local build outputs; replace each executable without truncating it."""

import argparse
import hashlib
import json
import os
import re
import selectors
import shutil
import subprocess
import tempfile
import time
from pathlib import Path

BINARIES = ("seeded_archive", "seeded-index", "seeded-sales", "seeded-articles")
MANIFEST = "release-manifest.json"
TREES = ("assets", "deploy", "docs")
GENERATED = (
    "tmp",
    "var",
    "dist",
    "target",
    ".ccb",
    ".claude",
    ".agents",
    ".codex",
    ".aws",
)


def git_read(source: Path, arguments: list[str]) -> tuple[int, str] | None:
    """Read only this repository; cap output at 64 KiB and execution at five seconds."""
    environment = {
        key: value for key, value in os.environ.items() if not key.startswith("GIT_")
    }
    environment.update(
        GIT_OPTIONAL_LOCKS="0", GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL="/dev/null"
    )
    command = [
        "git",
        "-c",
        "core.fsmonitor=false",
        "-c",
        "core.untrackedCache=false",
        "-C",
        str(source),
        *arguments,
    ]
    try:
        with subprocess.Popen(
            command, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, env=environment
        ) as process:
            assert process.stdout is not None
            deadline = time.monotonic() + 5
            output = bytearray()
            try:
                with selectors.DefaultSelector() as selector:
                    selector.register(process.stdout, selectors.EVENT_READ)
                    while True:
                        remaining = deadline - time.monotonic()
                        if remaining <= 0 or not selector.select(remaining):
                            return None
                        block = os.read(process.stdout.fileno(), 4096)
                        if not block:
                            return process.wait(
                                timeout=max(0.01, deadline - time.monotonic())
                            ), output.decode("utf-8", errors="replace")
                        output.extend(block)
                        if len(output) > 65_536:
                            return None
            finally:
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=1)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=1)
    except (OSError, subprocess.TimeoutExpired):
        return None


def source_state(source: Path, generated: tuple[Path, ...]) -> dict[str, str | bool]:
    unknown: dict[str, str | bool] = {
        "revision": "unknown",
        "state": "unknown",
        "dirty": "unknown",
    }
    top = git_read(source, ["rev-parse", "--show-toplevel"])
    if top is None or top[0] != 0 or Path(top[1].strip()).resolve() != source:
        return unknown
    revision = git_read(source, ["rev-parse", "--verify", "--quiet", "HEAD"])
    if revision is None:
        return unknown
    if revision[0] != 0:
        return {"revision": "unknown", "state": "uncommitted", "dirty": True}
    commit = revision[1].strip()
    if not re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", commit):
        return unknown
    excluded = set(GENERATED)
    excluded.update(
        path.relative_to(source).as_posix()
        for path in generated
        if path.is_relative_to(source)
    )
    status = git_read(
        source,
        [
            "status",
            "--porcelain=v1",
            "--untracked-files=normal",
            "--",
            ".",
            *[f":(exclude,literal){name}" for name in sorted(excluded)],
        ],
    )
    dirty: str | bool = (
        bool(status[1]) if status is not None and status[0] == 0 else "unknown"
    )
    return {"revision": commit, "state": "git", "dirty": dirty}


def release_manifest(destination: Path, state: dict[str, str | bool]) -> str:
    files = {}
    pending = [destination]
    visited = 0
    while pending:
        parent = pending.pop()
        for path in parent.iterdir():
            visited += 1
            if visited > 100_000:
                raise ValueError("release exceeds file limit")
            if path.is_symlink():
                raise ValueError("release must not contain symbolic links")
            if path.is_dir():
                if len(path.relative_to(destination).parts) > 64:
                    raise ValueError("release exceeds directory depth")
                pending.append(path)
            elif path.is_file() and path != destination / MANIFEST:
                digest = hashlib.sha256()
                with path.open("rb") as source:
                    while block := source.read(1024 * 1024):
                        digest.update(block)
                files[path.relative_to(destination).as_posix()] = digest.hexdigest()
            elif not path.is_file():
                raise ValueError("release contains a non-regular file")
    return (
        json.dumps(
            {"format": 1, "source": state, "files": files}, sort_keys=True, indent=2
        )
        + "\n"
    )


def package(source: Path, compiled: Path, destination: Path) -> None:
    source, compiled, destination = (
        source.resolve(),
        compiled.resolve(),
        destination.resolve(),
    )
    if (
        source.is_relative_to(destination)
        or compiled.is_relative_to(destination)
        or any(
            destination.is_relative_to(source / name)
            for name in ("assets", "deploy", "docs")
        )
    ):
        raise ValueError("output must not overlap packaged inputs")
    for name in BINARIES:
        if not (compiled / name).is_file() or (compiled / name).is_symlink():
            raise FileNotFoundError(f"missing compiled binary: {name}")
    for name in (*TREES, "README.md"):
        if (source / name).is_symlink():
            raise ValueError("packaged inputs must not be symbolic links")
    state = source_state(source, (compiled, destination))
    destination.mkdir(parents=True, exist_ok=True)
    if any(
        path.is_symlink() or path.name not in (*BINARIES, *TREES, "README.md", MANIFEST)
        for path in destination.iterdir()
    ):
        raise ValueError(
            "output contains unmanaged files; use a fresh release directory"
        )
    staged: dict[str, Path] = {}
    try:
        # Stage both before publishing either. A failed copy leaves both old
        # executables intact; finally removes every unpublished staging file.
        for name in BINARIES:
            with tempfile.NamedTemporaryFile(
                prefix=f".{name}.", dir=destination, delete=False
            ) as output:
                staged[name] = Path(output.name)
            shutil.copyfile(compiled / name, staged[name])
            staged[name].chmod(0o755)
        (destination / MANIFEST).unlink(missing_ok=True)
        for name in TREES:
            path = destination / name
            if path.exists():
                shutil.rmtree(path)
            shutil.copytree(
                source / name,
                path,
                symlinks=True,
                ignore=shutil.ignore_patterns(
                    "__pycache__",
                    "*.pyc",
                    "*.pyo",
                    ".pytest_cache",
                    ".ruff_cache",
                    ".mypy_cache",
                ),
            )
        shutil.copyfile(source / "README.md", destination / "README.md")
        for name, path in staged.items():
            path.replace(destination / name)
        manifest = release_manifest(destination, state)
        with tempfile.NamedTemporaryFile(
            prefix=".release-manifest.",
            dir=destination,
            mode="w",
            encoding="utf-8",
            delete=False,
        ) as output:
            staged[MANIFEST] = Path(output.name)
            output.write(manifest)
        staged[MANIFEST].chmod(0o644)
        staged[MANIFEST].replace(destination / MANIFEST)
    finally:
        for path in staged.values():
            path.unlink(missing_ok=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=Path.cwd())
    parser.add_argument("--compiled", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=Path("dist"))
    args = parser.parse_args()
    try:
        package(args.source, args.compiled, args.output)
    except (OSError, ValueError) as error:
        parser.exit(1, f"Packaging failed: {error}\n")


if __name__ == "__main__":
    main()
