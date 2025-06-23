# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Tests for script_helpers.py"""
import os
import tempfile
import unittest

from recidiviz.tools.looker.script_helpers import (
    hash_directory,
    remove_lookml_files_from,
)


class LookMLScriptHelpersTest(unittest.TestCase):
    """Tests for script_helpers.py"""

    def test_remove_lookml_files(self) -> None:
        # Test that .lkml files are removed even in subdirectories
        with tempfile.TemporaryDirectory() as tmp_dir:
            top_level_file = os.path.join(tmp_dir, "test.lkml")
            subdir = os.path.join(tmp_dir, "directory")
            subdir_file = os.path.join(subdir, "test.lkml")

            os.mkdir(subdir)
            with open(top_level_file, "x", encoding="UTF-8"), open(
                subdir_file, "x", encoding="UTF-8"
            ):
                remove_lookml_files_from(tmp_dir)
                for _, _, filenames in os.walk(tmp_dir):
                    self.assertFalse(any(file.endswith(".lkml") for file in filenames))

    def test_hash_directory(self) -> None:
        # Test that hash_directory generates consistent hashes
        with tempfile.TemporaryDirectory() as tmp_dir:
            file1 = os.path.join(tmp_dir, "file1.txt")
            file2 = os.path.join(tmp_dir, "file2.txt")

            # Create files with specific content
            with open(file1, "w", encoding="UTF-8") as f:
                f.write("Hello, World!")
            with open(file2, "w", encoding="UTF-8") as f:
                f.write("Another file content.")

            # Generate hash for the directory
            initial_hash = hash_directory(tmp_dir)

            # Ensure the hash remains the same if no changes are made
            self.assertEqual(initial_hash, hash_directory(tmp_dir))

            # Modify one file and check that the hash changes
            with open(file1, "w", encoding="UTF-8") as f:
                f.write("Modified content.")
            modified_hash = hash_directory(tmp_dir)
            self.assertNotEqual(initial_hash, modified_hash)

            # Add a new file and check that the hash changes
            new_file = os.path.join(tmp_dir, "new_file.txt")
            with open(new_file, "w", encoding="UTF-8") as f:
                f.write("New file content.")
            new_hash = hash_directory(tmp_dir)
            self.assertNotEqual(modified_hash, new_hash)

            # Remove a file and check that the hash changes
            os.remove(file2)
            final_hash = hash_directory(tmp_dir)
            self.assertNotEqual(new_hash, final_hash)
