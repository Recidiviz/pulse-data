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

from recidiviz.tools.looker.script_helpers import remove_lookml_files_from


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
