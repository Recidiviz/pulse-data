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
"""Tests the pulse-data/setup.py file that specifies required packages for the Dataflow VM workers."""
import json
import os
import unittest


PIPFILE_LOCK_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.dirname(__file__))))), 'Pipfile.lock')

SETUP_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.dirname(__file__))))), 'setup.py')


class TestSetupFilePinnedDependencies(unittest.TestCase):
    """Tests that dependencies pinned at certain versions are pinned at the version in the Pipfile.lock file."""
    def test_setup_file_pinned_dependencies(self) -> None:
        pinned_dependencies = ['protobuf', 'dill']

        for dependency in pinned_dependencies:
            pipfile_dependency = pipfile_version_for_dependency(dependency)

            with open(SETUP_PATH, "r") as setup_file:
                for line in setup_file:
                    if dependency in line:
                        # Remove whitespace, quotation marks, and commas
                        dependency_with_version = line.strip().replace("'", "").replace(",", "")

                        if dependency_with_version.startswith('#'):
                            # Skip comments that mention the dependency
                            continue

                        self.assertEqual(pipfile_dependency, dependency_with_version,
                                         "Try verifying the package's version in setup.py or running pipenv sync --dev "
                                         "before running this test again.")

    def test_setup_file_non_pinned_dependency(self) -> None:
        dependency = 'cattrs'

        pipfile_dependency = pipfile_version_for_dependency(dependency)

        with open(SETUP_PATH, "r") as setup_file:
            for line in setup_file:
                if dependency in line:
                    # Remove whitespace, quotation marks, and commas
                    dependency_with_version = line.strip().replace("'", "").replace(",", "")

                    # This dependency is not pinned at a particular version, so these should not be equal
                    self.assertNotEqual(pipfile_dependency, dependency_with_version)


def pipfile_version_for_dependency(dependency: str) -> str:
    """Looks in the Pipfile.lock file for the current version of the given dependency. Returns a string in the format
    'dependency==v.X.X.X'.
    """
    pipfile_version = None

    with open(PIPFILE_LOCK_PATH) as pipfile_lock_json:
        pipfile_data = json.load(pipfile_lock_json)

        if pipfile_data:
            default_data = pipfile_data.get('default')
            if default_data:
                dependency_data = default_data.get(dependency)
                if dependency_data:
                    pipfile_version = dependency_data.get('version')

    if not pipfile_version:
        raise ValueError("Dataflow pipeline dependent on a package not in the Pipfile.")

    pipfile_dependency_version = dependency + pipfile_version

    return pipfile_dependency_version
