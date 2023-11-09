#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2023 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""Tests for copu_source_files_to_experimental_composer.py"""
import os
import unittest
from typing import Set

from recidiviz.tools.airflow.copy_source_files_to_experiment_composer import (
    add_file_module_dependencies_to_set,
)

TEST_IMPORTING_FILE_PATH = os.path.join(
    os.path.dirname(__file__), "fixtures/test_importing.py"
)


class DependencyPathsTest(unittest.TestCase):
    def test_get_correct_source_files(self) -> None:
        module_dependencies: Set[str] = set()
        add_file_module_dependencies_to_set(
            TEST_IMPORTING_FILE_PATH, module_dependencies
        )
        source_files = [
            f"recidiviz/{file_split[1]}"
            for file_split in [file.split("recidiviz/") for file in module_dependencies]
        ]
        self.assertEqual(
            set(source_files),
            {
                "recidiviz/tests/tools/airflow/fixtures/a/__init__.py",
                "recidiviz/tests/tools/airflow/fixtures/__init__.py",
                "recidiviz/tests/tools/airflow/fixtures/a/b/__init__.py",
                "recidiviz/tests/tools/airflow/fixtures/a/c.py",
                "recidiviz/tests/tools/airflow/fixtures/a/b/b.py",
                "recidiviz/tests/tools/airflow/fixtures/a/b/e.py",
                "recidiviz/tests/tools/airflow/__init__.py",
                "recidiviz/tests/tools/__init__.py",
                "recidiviz/tests/__init__.py",
                "recidiviz/__init__.py",
            },
        )
