# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Tests for functionality in fixture_util.py"""
import os
import unittest

from recidiviz.common.file_system import is_non_empty_code_directory, is_valid_code_path
from recidiviz.tests.ingest.direct.fixture_util import (
    DIRECT_INGEST_FIXTURES_ROOT,
    ENUM_PARSING_FIXTURE_SUBDIR,
    DirectIngestTestFixturePath,
)


class TestDirectIngestTestFixturePath(unittest.TestCase):
    def test_all_paths_parse(self) -> None:
        for path, _directory_names, file_names in os.walk(DIRECT_INGEST_FIXTURES_ROOT):
            if (
                ENUM_PARSING_FIXTURE_SUBDIR in path
                or "_ingest_view_results" in path
                or not is_non_empty_code_directory(path)
            ):
                continue
            for file_name in file_names:
                if file_name == "__init__.py" or not is_valid_code_path(file_name):
                    continue
                full_path = os.path.join(path, file_name)
                path_obj = DirectIngestTestFixturePath.from_path(full_path)
                self.assertEqual(full_path, path_obj.full_path())
