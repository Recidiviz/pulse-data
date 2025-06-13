# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Tests for the copy_with_overwrite function."""
import tempfile
import unittest
from pathlib import Path

from recidiviz.tools.looker.copy_all_lookml import copy_with_overwrite


class TestCopyWithOverwrite(unittest.TestCase):
    """Tests for the copy_with_overwrite function."""

    def test_copy_with_overwrite(self) -> None:
        with tempfile.TemporaryDirectory() as source_root, tempfile.TemporaryDirectory() as target_root:
            source_root_path = Path(source_root)
            target_root_path = Path(target_root)

            (source_root_path / "dir1").mkdir()
            (source_root_path / "dir1" / "file1.lookml").write_text("File 1 content")
            (source_root_path / "dir1" / "subdir1").mkdir()
            (source_root_path / "dir1" / "subdir1" / "file2.lookml").write_text(
                "File 2 content"
            )

            (target_root_path / "dir1").mkdir()
            (target_root_path / "dir1" / "file1.lookml").write_text(
                "Old File 1 content"
            )
            (target_root_path / "dir1" / "old_file.lookml").write_text(
                "Old File content"
            )
            (target_root_path / "dir1" / "README.md").write_text(
                "This should not be deleted"
            )

            copied_files_count = copy_with_overwrite(
                source_root=source_root, target_root=target_root
            )

            assert copied_files_count == 2, "Expected 2 files to be copied"
            assert (target_root_path / "dir1").is_dir()
            # Should overwrite file
            assert (
                target_root_path / "dir1" / "file1.lookml"
            ).read_text() == "File 1 content"
            assert (target_root_path / "dir1" / "subdir1").is_dir()
            assert (
                target_root_path / "dir1" / "subdir1" / "file2.lookml"
            ).read_text() == "File 2 content"

            # Should be deleted
            assert not (target_root_path / "dir1" / "old_file.lookml").exists()
            # Should not be deleted
            assert (target_root_path / "dir1" / "README.md").exists()

    def test_copy_raises_on_unexpected_files(self) -> None:
        with tempfile.TemporaryDirectory() as source_root, tempfile.TemporaryDirectory() as target_root:
            source_root_path = Path(source_root)

            (source_root_path / "dir1").mkdir()
            (source_root_path / "dir1" / "file1.lookml").write_text("File 1 content")
            (source_root_path / "dir1" / "unexpected_file.txt").write_text(
                "Unexpected file content"
            )

            with self.assertRaisesRegex(ValueError, "Unexpected file type"):
                copy_with_overwrite(source_root=source_root, target_root=target_root)
