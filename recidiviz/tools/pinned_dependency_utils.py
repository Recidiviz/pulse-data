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
"""Shared helpers for reading resolved dependency versions from uv.lock, and for
parsing the pinned requirements.txt files that must be kept in sync with it.

Each file in PINNED_REQUIREMENTS_PATHS is installed separately from the main
uv-managed environment, and pins every dependency to an exact version so it can't
silently drift from what uv.lock resolves for the main project. uv.lock must be a
superset of every one of those files, so that anything they install is also
importable by tests.

Used by the guard test for those files (pinned_requirements_test.py) and by
recidiviz/tools/deploy/sync_pinned_dependencies.py, which rewrites stale pins.
"""
import os
import tomllib
from typing import NamedTuple

from packaging.requirements import Requirement
from packaging.specifiers import SpecifierSet
from packaging.utils import canonicalize_name

import recidiviz


def repo_root() -> str:
    """Returns the absolute path to the repo root, derived from the recidiviz package."""
    return os.path.dirname(os.path.dirname(recidiviz.__file__))


def uv_lock_path() -> str:
    """Returns the absolute path to the repo's uv.lock."""
    return os.path.join(repo_root(), "uv.lock")


def _recidiviz_path(*relative_parts: str) -> str:
    return os.path.join(os.path.dirname(recidiviz.__file__), *relative_parts)


# Every requirements.txt in the repo whose pins must stay in sync with uv.lock. Add a
# path here to bring a new file under both the guard test and the sync script.
PINNED_REQUIREMENTS_PATHS = [
    # Installed into the Beam SDK harness worker venv on Dataflow workers, via
    # recidiviz/pipelines/dataflow_flex_setup.py's install_requires.
    _recidiviz_path("pipelines", "dataflow_flex_requirements.txt"),
]


def uv_lock_versions(uv_lock_file_path: str) -> dict[str, str]:
    """Returns a map of canonicalized package name to resolved version, from uv.lock."""
    with open(uv_lock_file_path, "rb") as uv_lock_file:
        uv_data = tomllib.load(uv_lock_file)

    return {
        canonicalize_name(package["name"]): package["version"]
        for package in uv_data.get("package", [])
        if package.get("version")
    }


class RequirementsTxtEntry(NamedTuple):
    """One parsed line from a flat pip requirements.txt file."""

    requirement: Requirement
    # 0-indexed position within the file's readlines() list.
    line_index: int

    @property
    def pinned_version(self) -> str | None:
        """Returns the version this entry pins to, or None if it is not pinned to a
        single exact `==` version."""
        specifiers = list(self.requirement.specifier)
        if len(specifiers) != 1 or specifiers[0].operator != "==":
            return None

        return specifiers[0].version

    @property
    def canonical_name(self) -> str:
        """Returns the entry's package name, canonicalized for uv.lock lookups."""
        return canonicalize_name(self.requirement.name)

    def with_pinned_version(self, version: str) -> Requirement:
        """Returns a copy of this entry's requirement pinned to the given version.

        Round-tripping through Requirement carries extras and environment markers
        over untouched; str() on the result re-serializes them (and preserves the
        original name casing, e.g. PyGithub).
        """
        repinned = Requirement(str(self.requirement))
        repinned.specifier = SpecifierSet(f"=={version}")
        return repinned


def parse_requirements_txt_lines(lines: list[str]) -> list[RequirementsTxtEntry]:
    """Parses a flat pip requirements.txt's lines (one requirement per line, blank
    lines and '#' comments ignored) into Requirement objects paired with their line
    index.
    """
    entries = []
    for index, line in enumerate(lines):
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        entries.append(
            RequirementsTxtEntry(requirement=Requirement(stripped), line_index=index)
        )
    return entries


def parse_requirements_txt(requirements_txt_path: str) -> list[RequirementsTxtEntry]:
    """Reads a flat pip requirements.txt file and parses its entries."""
    with open(requirements_txt_path, "r", encoding="utf-8") as requirements_file:
        lines = requirements_file.readlines()

    return parse_requirements_txt_lines(lines)
