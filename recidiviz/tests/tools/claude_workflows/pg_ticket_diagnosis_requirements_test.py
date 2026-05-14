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
"""Tests that pg_ticket_diagnosis/requirements.txt versions match uv.lock."""
import os
import tomllib
import unittest

import recidiviz

REQUIREMENTS_PATH = os.path.join(
    os.path.dirname(recidiviz.__file__),
    "tools/claude_workflows/pg_ticket_diagnosis/requirements.txt",
)

UV_LOCK_PATH = os.path.join(
    os.path.dirname(os.path.dirname(recidiviz.__file__)),
    "uv.lock",
)


def _parse_requirements(path: str) -> dict[str, str]:
    """Parse a requirements.txt into {normalized_name: version}."""
    deps: dict[str, str] = {}
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "==" in line:
                name, version = line.split("==", 1)
                deps[name.strip().lower()] = version.strip()
    return deps


def _uv_lock_versions(path: str) -> dict[str, str]:
    """Parse uv.lock into {name: version} for all packages."""
    with open(path, "rb") as f:
        data = tomllib.load(f)
    return {
        pkg["name"]: pkg["version"]
        for pkg in data.get("package", [])
        if "version" in pkg
    }


class TestPgDiagnosisRequirementsMatchUvLock(unittest.TestCase):
    """Validates that pinned versions in pg_ticket_diagnosis/requirements.txt
    match the versions in the top-level uv.lock.

    The pg_ticket_diagnosis Docker container installs from its own
    requirements.txt, separate from the main project's uv-managed deps.
    If the two drift, the container may use a different version than
    what's tested in CI.
    """

    def test_pinned_versions_match_uv_lock(self) -> None:
        requirements = _parse_requirements(REQUIREMENTS_PATH)
        uv_lock = _uv_lock_versions(UV_LOCK_PATH)

        for req_name, req_version in requirements.items():
            with self.subTest(package=req_name):
                self.assertIn(
                    req_name,
                    uv_lock,
                    f"Package {req_name!r} is in requirements.txt but not in "
                    f"uv.lock. Add it to pyproject.toml and run `uv sync --all-extras`.",
                )
                self.assertEqual(
                    req_version,
                    uv_lock[req_name],
                    f"Version mismatch for {req_name!r}: requirements.txt has "
                    f"{req_version} but uv.lock has {uv_lock[req_name]}. "
                    f"Update requirements.txt to match uv.lock.",
                )
