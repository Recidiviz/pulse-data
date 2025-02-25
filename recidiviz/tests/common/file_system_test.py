# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Tests functions in the file_system.py file."""
import os
import tempfile
import unittest
from typing import Set

from recidiviz.common import file_system


class TestFileSystem(unittest.TestCase):
    """Tests various functions in the file_system.py file."""

    def setUp(self) -> None:
        # Make temporary directories for testing
        self.temp_test_dir = os.path.join(
            tempfile.gettempdir(), "recidiviz_tests_file_system_test"
        )
        self.empty_dir = os.path.join(self.temp_test_dir, "empty")
        os.makedirs(self.empty_dir, exist_ok=True)

        self.dir_1 = os.path.join(self.temp_test_dir, "dir_1")
        self.dir_2 = os.path.join(self.dir_1, "dir_2")
        os.makedirs(self.dir_2, exist_ok=True)

        # Make temporary files for testing
        self.file_A = os.path.join(self.dir_1, "file_A")
        with open(self.file_A, mode="w", encoding="utf-8") as temp_file:
            temp_file.close()
        self.file_B = os.path.join(self.dir_1, "file_B")
        with open(self.file_B, mode="w", encoding="utf-8") as temp_file:
            temp_file.close()
        self.file_C = os.path.join(self.dir_2, "file_C")
        with open(self.file_C, mode="w", encoding="utf-8") as temp_file:
            temp_file.close()

        self.all_dirs_ordered = [
            self.dir_2,
            self.dir_1,
            self.empty_dir,
            self.temp_test_dir,
        ]
        self.all_files = [self.file_A, self.file_B, self.file_C]

    def tearDown(self) -> None:
        # Cleans up all temp files
        for file in self.all_files:
            if os.path.exists(file):
                os.remove(file)

        for directory in self.all_dirs_ordered:
            if os.path.exists(directory):
                os.removedirs(directory)

    def test_get_all_files_recursive(self) -> None:
        all_files = file_system.get_all_files_recursive(self.temp_test_dir)

        expected_output: Set[str] = {
            os.path.join(self.temp_test_dir, "dir_1/file_A"),
            os.path.join(self.temp_test_dir, "dir_1/file_B"),
            os.path.join(self.temp_test_dir, "dir_1/dir_2/file_C"),
        }

        self.assertEqual(expected_output, all_files)

    def test_get_all_files_recursive_empty(self) -> None:
        all_files = file_system.get_all_files_recursive(self.empty_dir)

        expected_output: Set[str] = set()

        self.assertEqual(expected_output, all_files)

    def test_get_all_files_recursive_file(self) -> None:
        all_files = file_system.get_all_files_recursive(self.file_C)

        expected_output: Set[str] = set()

        self.assertEqual(expected_output, all_files)

    def test_delete_files(
        self,
    ) -> None:
        files_to_delete = [self.file_A, self.file_C]

        file_system.delete_files(files_to_delete, delete_empty_dirs=False)

        self.assertFalse(os.path.exists(self.file_A))
        self.assertFalse(os.path.exists(self.file_C))

    def test_get_all_files_recursive_delete_dirs(
        self,
    ) -> None:
        files_to_delete = [self.file_A, self.file_C]

        file_system.delete_files(files_to_delete, delete_empty_dirs=True)

        self.assertFalse(os.path.exists(self.file_A))
        self.assertFalse(os.path.exists(self.file_C))
        self.assertFalse(os.path.exists(self.dir_2))

    def test_get_all_files_recursive_delete_dirs_not_empty(
        self,
    ) -> None:
        files_to_delete = [self.file_B]

        file_system.delete_files(files_to_delete, delete_empty_dirs=True)

        self.assertFalse(os.path.exists(self.file_B))
        self.assertTrue(os.path.exists(self.dir_1))
