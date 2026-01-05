# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
import os
import tomllib
import unittest

import recidiviz

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
    """Tests that dependencies pinned at certain versions are pinned at the version in the uv.lock file."""

    def test_setup_file_pinned_dependencies(self) -> None:
        pinned_dependencies = [
            "protobuf",
            "dill",
            "sqlalchemy",
            "google-cloud-tasks",
            "cloudpickle",
        ]

        for dependency in pinned_dependencies:
            uv_dependency = uv_lock_version_for_dependency(dependency)
            dependency_found = False

            with open(SETUP_PATH, "r", encoding="utf-8") as setup_file:
                for line in setup_file:
                    line = line.lower().strip()
                    if line.startswith(f'"{dependency}'):
                        dependency_found = True
                        # Remove whitespace, quotation marks, and commas
                        dependency_with_version = (
                            line.replace('"', "").replace("'", "").replace(",", "")
                        )

                        if dependency_with_version.startswith("#"):
                            # Skip comments that mention the dependency
                            continue

                        self.assertEqual(
                            uv_dependency,
                            dependency_with_version,
                            "Try verifying the package's version in dataflow_flex_setup.py or running uv sync "
                            "--all-extras before running this test again.",
                        )

            if not dependency_found:
                raise ValueError(f"Dependency {dependency} not found.")

    def test_setup_file_non_pinned_dependency(self) -> None:
        dependency = "cattrs"

        uv_dependency = uv_lock_version_for_dependency(dependency)

        with open(SETUP_PATH, "r", encoding="utf-8") as setup_file:
            for line in setup_file:
                if dependency in line:
                    # Remove whitespace, quotation marks, and commas
                    dependency_with_version = (
                        line.strip().replace('"', "").replace("'", "").replace(",", "")
                    )

                    # This dependency is not pinned at a particular version, so these should not be equal
                    self.assertNotEqual(uv_dependency, dependency_with_version)


def uv_lock_version_for_dependency(dependency: str) -> str:
    """Looks in the uv.lock file for the current version of the given dependency. Returns a string in the format
    'dependency==v.X.X.X'.
    """
    with open(UV_LOCK_PATH, "rb") as uv_lock_file:
        uv_data = tomllib.load(uv_lock_file)

    # uv.lock uses TOML format with [[package]] entries
    packages = uv_data.get("package", [])
    for package in packages:
        if package.get("name") == dependency:
            version = package.get("version")
            if version:
                return f"{dependency}=={version}"

    raise ValueError(
        f"Dataflow pipeline dependent on a package ({dependency}) not in the uv.lock."
    )
