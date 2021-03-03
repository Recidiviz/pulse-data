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
"""Tests for DirectIngestFilesGenerator."""
import os
import unittest
from shutil import rmtree
from types import ModuleType
from typing import List

import pytest

from recidiviz.ingest.direct import regions as regions_module
from recidiviz.ingest.direct.direct_ingest_files_generator import (
    DirectIngestFilesGenerator,
)
from recidiviz.ingest.direct.direct_ingest_region_utils import (
    get_existing_region_dir_names,
    get_existing_region_dir_paths,
)
from recidiviz.persistence.entity_matching import state as state_module
from recidiviz.tests.ingest.direct import regions as regions_tests_module
from recidiviz.tests.ingest.direct.regions.direct_ingest_region_structure_test import (
    DirectIngestRegionDirStructureBase,
)


# TODO(#6105): Use a fake FS for these tests so they can be run in parallel without clobbering each other.
@pytest.mark.no_parallel
class DirectIngestFilesGeneratorTest(
    DirectIngestRegionDirStructureBase, unittest.TestCase
):
    """Tests for DirectIngestFilesGenerator."""

    def setUp(self) -> None:
        super().setUp()
        test_region_code = "us_aa"
        self.files_generator = DirectIngestFilesGenerator(test_region_code)
        self.files_generator.generate_all_new_dirs_and_files()

    def tearDown(self) -> None:
        new_region_dir_path = os.path.join(
            os.path.dirname(regions_module.__file__), self.files_generator.region_code
        )
        new_region_persistence_dir_path = os.path.join(
            os.path.dirname(state_module.__file__), self.files_generator.region_code
        )
        new_region_tests_dir_path = os.path.join(
            os.path.dirname(regions_tests_module.__file__),
            self.files_generator.region_code,
        )
        dirs_created = [
            new_region_dir_path,
            new_region_persistence_dir_path,
            new_region_tests_dir_path,
        ]
        for d in dirs_created:
            if os.path.isdir(d):
                rmtree(d)

    @property
    def region_dir_names(self) -> List[str]:
        return get_existing_region_dir_names()

    @property
    def region_dir_paths(self) -> List[str]:
        return get_existing_region_dir_paths()

    @property
    def test(self) -> unittest.TestCase:
        return self

    @property
    def region_module_override(self) -> ModuleType:
        return regions_module

    def test_generate_all_new_files_for_existing_state(self) -> None:
        with self.assertRaises(FileExistsError):
            files_generator = DirectIngestFilesGenerator("us_nd")
            files_generator.generate_all_new_dirs_and_files()
