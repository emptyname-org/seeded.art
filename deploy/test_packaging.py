"""Distribution checks use local fixtures, never compile or deploy an application."""

import hashlib
import json
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import package_release

ROOT = Path(__file__).resolve().parent.parent


class PackagingTests(unittest.TestCase):
    def setUp(self):
        (ROOT / "tmp").mkdir(exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(
            prefix="package_", dir=ROOT / "tmp"
        )
        self.addCleanup(self.temporary.cleanup)
        self.source = Path(self.temporary.name)
        self.compiled = self.source / "target/release"
        self.compiled.mkdir(parents=True)
        self.destination = self.source / "dist"
        self.destination.mkdir()
        for name in package_release.BINARIES:
            (self.compiled / name).write_bytes(f"new {name}".encode())
            (self.destination / name).write_bytes(f"old {name}".encode())
        for name in ("assets", "deploy", "docs"):
            (self.source / name).mkdir()
            (self.source / name / "current").write_text("current")
            (self.destination / name).mkdir()
            (self.destination / name / "obsolete").write_text("old")
        (self.source / "README.md").write_text("fixture readme")

    def test_make_build_packages_all_binaries_without_truncating_open_files(self):
        shutil.copyfile(ROOT / "Makefile", self.source / "Makefile")
        shutil.copyfile(
            ROOT / "deploy/package_release.py",
            self.source / "deploy/package_release.py",
        )
        compiler = self.source / "fake_cargo"
        compiler.write_text('#!/bin/sh\nprintf "%s\\n" "$@" > cargo_args\n')
        compiler.chmod(0o755)
        (self.source / "package.json").write_text("{}")
        (self.source / "package-lock.json").write_text("{}")
        bundler = self.source / "assets/static/beacon/build.mjs"
        bundler.parent.mkdir(parents=True)
        bundler.write_text(
            "import {writeFileSync} from 'node:fs'; writeFileSync('assets/static/beacon/client.js', 'built fixture');"
        )
        npm = self.source / "fake_npm"
        npm.write_text('#!/bin/sh\nprintf "%s\\n" "$@" > npm_args\n')
        npm.chmod(0o755)
        old_app = (self.destination / "seeded_archive").open("rb")
        old_index = (self.destination / "seeded-index").open("rb")
        with old_app, old_index:
            result = subprocess.run(
                [
                    "make",
                    "build",
                    f"CARGO={compiler}",
                    f"NPM={npm}",
                    f"CARGO_TARGET_DIR={self.compiled.parent}",
                ],
                cwd=self.source,
                capture_output=True,
                text=True,
                timeout=20,
                check=False,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(old_app.read(), b"old seeded_archive")
            self.assertEqual(old_index.read(), b"old seeded-index")
        self.assertEqual(
            (self.source / "cargo_args").read_text().splitlines(),
            ["build", "--release", "--locked", "--bins"],
        )
        self.assertEqual(
            (self.destination / "assets/static/beacon/client.js").read_text(),
            "built fixture",
        )
        self.assertEqual((self.source / "npm_args").read_text().splitlines()[0], "ci")
        for name in package_release.BINARIES:
            self.assertEqual(
                (self.destination / name).read_bytes(), f"new {name}".encode()
            )
            self.assertEqual((self.destination / name).stat().st_mode & 0o777, 0o755)
        for name in ("assets", "deploy", "docs"):
            self.assertFalse((self.destination / name / "obsolete").exists())
            self.assertTrue((self.destination / name / "current").is_file())
        self.assertEqual(list(self.destination.glob(".seeded*")), [])

    def test_missing_indexer_preserves_existing_distribution_binaries(self):
        (self.compiled / "seeded-index").unlink()
        with self.assertRaises(FileNotFoundError):
            package_release.package(self.source, self.compiled, self.destination)
        self.assert_old_binaries()

    def test_release_includes_optional_maintenance_runner_and_all_schedules(self):
        names = [
            "maintain.py",
            *[
                f"seeded-{job}.{kind}.in"
                for job in ("sync", "verify", "backup", "sales", "articles")
                for kind in ("service", "timer")
            ],
        ]
        for name in names:
            shutil.copyfile(ROOT / "deploy" / name, self.source / "deploy" / name)
        package_release.package(self.source, self.compiled, self.destination)
        manifest = json.loads((self.destination / package_release.MANIFEST).read_text())
        for name in names:
            self.assertEqual(
                (self.destination / "deploy" / name).read_bytes(),
                (ROOT / "deploy" / name).read_bytes(),
            )
            self.assertIn(f"deploy/{name}", manifest["files"])

    def test_output_cannot_replace_sources_or_recurse_into_packaged_trees(self):
        for output in (self.source, self.compiled, self.source / "assets/nested"):
            with self.subTest(output=output), self.assertRaises(ValueError):
                package_release.package(self.source, self.compiled, output)
        self.assertTrue((self.source / "assets/current").is_file())
        self.assert_old_binaries()

    def test_failed_copy_preserves_old_binaries_and_cleans_staging(self):
        copy = shutil.copyfile

        def fail_second(source, destination, *args, **kwargs):
            if Path(source).name == "seeded-index":
                Path(destination).write_bytes(b"partial")
                raise OSError("simulated full filesystem")
            return copy(source, destination, *args, **kwargs)

        with (
            patch.object(package_release.shutil, "copyfile", side_effect=fail_second),
            self.assertRaises(OSError),
        ):
            package_release.package(self.source, self.compiled, self.destination)
        self.assert_old_binaries()

    def test_manifest_hashes_every_packaged_file_and_is_reproducible(self):
        (self.source / "assets/release-manifest.json").write_text(
            "asset with same name"
        )
        generated = self.source / "deploy/__pycache__"
        generated.mkdir()
        (generated / "private.pyc").write_bytes(b"not a release input")
        package_release.package(self.source, self.compiled, self.destination)
        first = (self.destination / package_release.MANIFEST).read_bytes()
        manifest = json.loads(first)
        files = {
            path.relative_to(self.destination).as_posix(): hashlib.sha256(
                path.read_bytes()
            ).hexdigest()
            for path in self.destination.rglob("*")
            if path.is_file() and path != self.destination / package_release.MANIFEST
        }
        self.assertEqual(manifest["files"], files)
        self.assertEqual(list(manifest["files"]), sorted(files))
        self.assertNotIn(package_release.MANIFEST, files)
        self.assertFalse((self.destination / "deploy/__pycache__").exists())
        self.assertNotIn(str(self.source), first.decode())
        package_release.package(self.source, self.compiled, self.destination)
        self.assertEqual(
            (self.destination / package_release.MANIFEST).read_bytes(), first
        )
        (self.destination / "seeded_archive").write_bytes(b"modified after packaging")
        self.assertNotEqual(
            hashlib.sha256(
                (self.destination / "seeded_archive").read_bytes()
            ).hexdigest(),
            manifest["files"]["seeded_archive"],
        )

    def test_no_head_is_explicitly_unknown_and_uncommitted(self):
        with patch.object(
            package_release, "git_read", side_effect=[(0, str(self.source)), (1, "")]
        ):
            package_release.package(self.source, self.compiled, self.destination)
        manifest = json.loads((self.destination / package_release.MANIFEST).read_text())
        self.assertEqual(
            manifest["source"],
            {"revision": "unknown", "state": "uncommitted", "dirty": True},
        )

    def test_known_revision_reports_dirty_and_excludes_generated_paths(self):
        for status, dirty in ((" M src/main.rs\n", True), ("", False)):
            with patch.object(
                package_release,
                "git_read",
                side_effect=[(0, str(self.source)), (0, "a" * 40), (0, status)],
            ) as git:
                state = package_release.source_state(
                    self.source, (self.compiled, self.destination)
                )
            self.assertEqual(
                state, {"revision": "a" * 40, "state": "git", "dirty": dirty}
            )
            arguments = git.call_args.args[1]
            for name in (
                "tmp",
                "var",
                "dist",
                "target",
                "target/release",
                ".ccb",
                ".aws",
            ):
                self.assertIn(f":(exclude,literal){name}", arguments)
        with patch.object(
            package_release,
            "git_read",
            side_effect=[(0, str(self.source)), (0, "a" * 40), None],
        ):
            state = package_release.source_state(self.source, ())
        self.assertEqual(state["dirty"], "unknown")

    def test_unmanaged_output_or_linked_inputs_cannot_enter_manifest(self):
        runtime = self.destination / "runtime.sqlite"
        runtime.write_bytes(b"runtime")
        with self.assertRaises(ValueError):
            package_release.package(self.source, self.compiled, self.destination)
        self.assert_old_binaries()
        runtime.unlink()
        (self.source / "assets/linked").symlink_to(self.source / "README.md")
        with self.assertRaises(ValueError):
            package_release.package(self.source, self.compiled, self.destination)
        self.assertFalse((self.destination / package_release.MANIFEST).exists())

    def assert_old_binaries(self):
        for name in package_release.BINARIES:
            self.assertEqual(
                (self.destination / name).read_bytes(), f"old {name}".encode()
            )
        self.assertEqual(list(self.destination.glob(".seeded*")), [])


if __name__ == "__main__":
    unittest.main()
