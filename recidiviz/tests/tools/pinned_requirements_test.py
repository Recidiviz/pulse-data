# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================
"""Tests that every requirements.txt in PINNED_REQUIREMENTS_PATHS pins its
dependencies to exact versions that match uv.lock."""
import os
import unittest

from recidiviz.tools.pinned_dependency_utils import (
    PINNED_REQUIREMENTS_PATHS,
    parse_requirements_txt,
    repo_root,
    uv_lock_path,
    uv_lock_versions,
)


class TestPinnedRequirementsMatchUvLock(unittest.TestCase):
    """Guards the requirements.txt files that are installed separately from the main
    uv-managed environment (the Dataflow flex template workers and the PG ticket
    diagnosis Docker agent) against silently drifting from uv.lock.

    Those files are installed by pip outside of uv, so uv.lock exerts no influence
    over them; without these tests a version could drift arbitrarily far from what's
    tested in CI. The invariant enforced here is that uv.lock is a superset of every
    pinned file, pinned to identical versions. Run
    `uv run python -m recidiviz.tools.deploy.sync_pinned_dependencies` to fix a
    version-mismatch failure; a missing-package failure needs a pyproject.toml change.
    """

    def test_all_files_exist(self) -> None:
        for path in PINNED_REQUIREMENTS_PATHS:
            with self.subTest(path=path):
                self.assertTrue(
                    os.path.exists(path),
                    f"Path [{path}] in PINNED_REQUIREMENTS_PATHS does not exist.",
                )

    def test_all_dependencies_pinned_to_exact_version(self) -> None:
        for path in PINNED_REQUIREMENTS_PATHS:
            relative_path = os.path.relpath(path, repo_root())
            for entry in parse_requirements_txt(path):
                with self.subTest(path=relative_path, package=entry.requirement.name):
                    self.assertIsNotNone(
                        entry.pinned_version,
                        f"Dependency [{entry.requirement}] in [{relative_path}] is "
                        f"not pinned to a single exact version (e.g. 'foo==1.2.3').",
                    )

    def test_uv_lock_contains_every_pinned_package(self) -> None:
        """Asserts uv.lock is a superset of every pinned requirements file.

        Without this, a package dropped from pyproject.toml, a typo'd or upstream-
        renamed package, or a package added here but never added to pyproject.toml
        would all read as "not in the main dependency graph" and be skipped by the
        sync forever, freezing its pin while everything around it moves.
        """
        uv_versions = uv_lock_versions(uv_lock_path())

        for path in PINNED_REQUIREMENTS_PATHS:
            relative_path = os.path.relpath(path, repo_root())
            for entry in parse_requirements_txt(path):
                with self.subTest(path=relative_path, package=entry.requirement.name):
                    if entry.canonical_name not in uv_versions:
                        # Not assertIn: that would dump all of uv.lock into the
                        # failure output, burying the message.
                        self.fail(
                            f"Dependency [{entry.requirement.name}] is pinned in "
                            f"[{relative_path}] but is not in uv.lock, so its pin can "
                            f"never be synced and no test can exercise code that uses "
                            f"it. Add it to pyproject.toml and re-lock, or drop the "
                            f"pin if it is no longer used."
                        )

    def test_pinned_versions_match_uv_lock(self) -> None:
        uv_versions = uv_lock_versions(uv_lock_path())

        for path in PINNED_REQUIREMENTS_PATHS:
            relative_path = os.path.relpath(path, repo_root())
            for entry in parse_requirements_txt(path):
                with self.subTest(path=relative_path, package=entry.requirement.name):
                    pinned_version = entry.pinned_version
                    if pinned_version is None:
                        # Covered by test_all_dependencies_pinned_to_exact_version.
                        continue

                    uv_version = uv_versions.get(entry.canonical_name)
                    if uv_version is None:
                        # Covered by test_uv_lock_contains_every_pinned_package.
                        continue

                    self.assertEqual(
                        uv_version,
                        pinned_version,
                        f"Dependency [{entry.requirement.name}] is pinned to "
                        f"[{pinned_version}] in [{relative_path}] but uv.lock "
                        f"resolves it to [{uv_version}]. Update the pin to match "
                        f"uv.lock.",
                    )
