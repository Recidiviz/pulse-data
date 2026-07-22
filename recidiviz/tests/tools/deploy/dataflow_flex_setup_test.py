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
"""Tests the pulse-data/recidiviz/pipelines/dataflow_flex_setup.py file that specifies required packages
for the Dataflow VM workers. """
import ast
import os
import tomllib
import unittest

from packaging.requirements import Requirement
from packaging.utils import canonicalize_name

import recidiviz
from recidiviz.utils.types import assert_type

UV_LOCK_PATH = os.path.join(
    os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
    ),
    "uv.lock",
)

SETUP_PATH = os.path.join(
    os.path.dirname(recidiviz.__file__),
    "pipelines/dataflow_flex_setup.py",
)


class TestSetupFilePinnedDependencies(unittest.TestCase):
    """Tests that every dependency in dataflow_flex_setup.py is pinned to an exact
    version, and that any dependency also present in uv.lock is pinned to the same
    version uv resolves for the main project.

    Dataflow workers install this setup file's dependencies fresh (via the Beam SDK
    harness setup_file mechanism) into a separate venv at worker boot time. An
    unpinned dependency is re-resolved against PyPI at that point, so a new upstream
    release can break every pipeline without any change to this repo.
    """

    def test_all_dependencies_pinned_to_exact_version(self) -> None:
        for requirement in _required_packages():
            specifiers = list(requirement.specifier)
            if len(specifiers) != 1 or specifiers[0].operator != "==":
                raise AssertionError(
                    f"Dependency [{requirement}] in dataflow_flex_setup.py is not "
                    f"pinned to a single exact version (e.g. 'foo==1.2.3')."
                )

    def test_pinned_versions_match_uv_lock(self) -> None:
        uv_lock_versions = _uv_lock_versions()
        for requirement in _required_packages():
            specifiers = list(requirement.specifier)
            if len(specifiers) != 1 or specifiers[0].operator != "==":
                # Covered by test_all_dependencies_pinned_to_exact_version.
                continue

            uv_version = uv_lock_versions.get(canonicalize_name(requirement.name))
            if uv_version is None:
                # Not every dataflow_flex_setup.py dependency is part of the main
                # project's dependency graph in pyproject.toml/uv.lock.
                continue

            pinned_version = specifiers[0].version
            self.assertEqual(
                uv_version,
                pinned_version,
                f"Dependency [{requirement.name}] is pinned to [{pinned_version}] in "
                f"dataflow_flex_setup.py but uv.lock resolves it to [{uv_version}]. "
                "Update the pin in dataflow_flex_setup.py to match uv.lock.",
            )


def _required_packages() -> list[Requirement]:
    """Parses dataflow_flex_setup.py's REQUIRED_PACKAGES list via ast, without
    importing the module (importing it would invoke setuptools.setup()).

    Returns: The list of dependencies as parsed Requirement objects.
    """
    with open(SETUP_PATH, "r", encoding="utf-8") as setup_file:
        tree = ast.parse(setup_file.read(), filename=SETUP_PATH)

    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        if not any(
            isinstance(target, ast.Name) and target.id == "REQUIRED_PACKAGES"
            for target in node.targets
        ):
            continue

        required_packages = assert_type(node.value, ast.List)
        return [
            Requirement(assert_type(assert_type(element, ast.Constant).value, str))
            for element in required_packages.elts
        ]

    raise ValueError(
        f"Could not find a REQUIRED_PACKAGES list assignment in [{SETUP_PATH}]."
    )


def _uv_lock_versions() -> dict[str, str]:
    """Returns: A map of canonicalized package name to resolved version, from uv.lock."""
    with open(UV_LOCK_PATH, "rb") as uv_lock_file:
        uv_data = tomllib.load(uv_lock_file)

    return {
        canonicalize_name(package["name"]): package["version"]
        for package in uv_data.get("package", [])
        if package.get("version")
    }
