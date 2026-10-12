#!/usr/bin/env python3
"""Read project-local archive sizes; never extrapolate to chain inventory."""

import argparse
import json
import sqlite3
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MAX_ENTRIES = 1000000
TABLES = {
    "archive": ("projects", "tokens", "artists"),
    "sales": ("sales",),
    "articles": ("articles", "objects"),
}


def local_path(value: str) -> Path:
    path = (ROOT / value).resolve()
    if not path.is_relative_to(ROOT):
        raise ValueError("measurement paths must remain inside this project")
    return path


def files(path: Path) -> dict[str, int | str]:
    if not path.exists():
        return {"status": "missing"}
    count = logical = allocated = largest = 0
    pending = [path]
    entries = 1
    deadline = time.monotonic() + 30
    while pending:
        if time.monotonic() >= deadline:
            raise ValueError(
                "file measurement bound reached; no partial sizing report emitted"
            )
        item = pending.pop()
        if item.is_symlink():
            raise ValueError("measurement refuses symbolic links")
        if item.is_dir():
            for child in item.iterdir():
                entries += 1
                if entries > MAX_ENTRIES or time.monotonic() >= deadline:
                    raise ValueError("directory measurement bound reached")
                pending.append(child)
            continue
        metadata = item.stat()
        if not item.is_file():
            raise ValueError("measurement requires regular files")
        count += 1
        logical += metadata.st_size
        allocated += metadata.st_blocks * 512
        largest = max(largest, metadata.st_size)
    return {
        "status": "measured",
        "files": count,
        "logical_bytes": logical,
        "allocated_bytes": allocated,
        "largest_file_bytes": largest,
    }


def database(path: Path, kind: str = "archive") -> dict[str, object]:
    tables = TABLES[kind]
    if path.is_symlink():
        raise ValueError("measurement refuses symbolic links")
    if not path.is_file():
        return {"status": "missing"}
    connection = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=2)
    deadline = time.monotonic() + 5
    connection.set_progress_handler(lambda: int(time.monotonic() > deadline), 1000)
    try:
        connection.execute("PRAGMA query_only=ON")
        connection.execute("BEGIN")
        result: dict[str, object] = {"status": "measured"}
        for pragma in ("page_size", "page_count", "freelist_count", "journal_mode"):
            result[pragma] = connection.execute(f"PRAGMA {pragma}").fetchone()[0]
        result["indexed_rows"] = {
            table: connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            for table in tables
        }
        result["main"] = files(path)
        result["wal"] = files(Path(str(path) + "-wal"))
        result["shm"] = files(Path(str(path) + "-shm"))
        return result
    finally:
        connection.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", default="var/seeded.sqlite")
    parser.add_argument("--sales-database", default="var/sales.sqlite")
    parser.add_argument("--articles-database", default="var/articles.sqlite")
    parser.add_argument("--cache", default="var/cache")
    parser.add_argument("--previews", default="assets/fixtures/previews")
    args = parser.parse_args()
    try:
        result = {
            "scope": "local files only; no full-chain estimate",
            "database": database(local_path(args.database)),
            "sales_database": database(local_path(args.sales_database), "sales"),
            "articles_database": database(
                local_path(args.articles_database), "articles"
            ),
            "cache": files(local_path(args.cache)),
            "previews": files(local_path(args.previews)),
        }
    except (ValueError, OSError, sqlite3.Error) as error:
        parser.exit(1, f"Measurement failed: {error}\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
