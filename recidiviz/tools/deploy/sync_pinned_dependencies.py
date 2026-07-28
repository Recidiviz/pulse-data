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
"""Rewrites stale pins in every requirements.txt listed in PINNED_REQUIREMENTS_PATHS
so they match the versions resolved in uv.lock.

Those files are installed separately from the main uv-managed environment and pin
every dependency to an exact version so they can't silently drift from what uv.lock
resolves for the main project; this script keeps those pins current whenever uv.lock
changes.

uv.lock is required to be a superset of every pinned file -- enforced by
pinned_requirements_test.py, not here. A package absent from uv.lock is left alone
rather than raising, so that one bad entry can't block syncing every other file; the
guard test is what fails on the resulting PR.

Can be run on-demand via:
    $ uv run python -m recidiviz.tools.deploy.sync_pinned_dependencies
"""
import logging
import os

from recidiviz.tools.pinned_dependency_utils import (
    PINNED_REQUIREMENTS_PATHS,
    parse_requirements_txt_lines,
    repo_root,
    uv_lock_path,
    uv_lock_versions,
)


def rewrite_requirements_lines(
    *, lines: list[str], uv_versions: dict[str, str], display_path: str
) -> tuple[list[str], bool]:
    """For each requirements.txt entry pinned with a single `==`, if uv.lock resolves
    that package to a different version, rewrites the line in place, preserving the
    original package-name casing (e.g. PyGithub), any extras and environment markers,
    and the newline style. Entries not pinned with a single `==` (including direct URL
    references, which carry no specifier), or not present in uv.lock, are left
    untouched.

    Returns: The rewritten lines, and whether any line changed.
    """
    new_lines = list(lines)
    changed = False

    for entry in parse_requirements_txt_lines(lines):
        pinned_version = entry.pinned_version
        if pinned_version is None:
            continue

        new_version = uv_versions.get(entry.canonical_name)
        if new_version is None or new_version == pinned_version:
            continue

        line = new_lines[entry.line_index]
        newline_suffix = line[len(line.rstrip("\r\n")) :]
        new_lines[
            entry.line_index
        ] = f"{entry.with_pinned_version(new_version)}{newline_suffix}"
        logging.info(
            "%s: %s %s -> %s",
            display_path,
            entry.requirement.name,
            pinned_version,
            new_version,
        )
        changed = True

    return new_lines, changed


def sync_requirements_file(
    *, requirements_txt_path: str, uv_versions: dict[str, str]
) -> bool:
    """Reads, rewrites, and writes back a pinned requirements.txt in place.

    Returns: True if the file was modified.
    """
    with open(requirements_txt_path, "r", encoding="utf-8") as requirements_file:
        lines = requirements_file.readlines()

    new_lines, changed = rewrite_requirements_lines(
        lines=lines,
        uv_versions=uv_versions,
        display_path=os.path.relpath(requirements_txt_path, repo_root()),
    )
    if changed:
        with open(requirements_txt_path, "w", encoding="utf-8") as requirements_file:
            requirements_file.writelines(new_lines)

    return changed


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    versions = uv_lock_versions(uv_lock_path())
    any_changed = False
    for path in PINNED_REQUIREMENTS_PATHS:
        any_changed |= sync_requirements_file(
            requirements_txt_path=path, uv_versions=versions
        )

    if not any_changed:
        logging.info("All pinned requirements files already match uv.lock.")
