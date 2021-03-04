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
import tempfile
import unittest
from shutil import copytree, rmtree
from types import ModuleType
from typing import List

from recidiviz.ingest.direct import (
    regions as regions_module,
    templates as ingest_templates_module,
)
from recidiviz.ingest.direct.direct_ingest_files_generator import (
    DirectIngestFilesGenerator,
    REGIONS_DIR_PATH,
    DEFAULT_WORKING_DIR,
)
from recidiviz.ingest.direct.direct_ingest_region_utils import (
    get_existing_region_dir_names,
)
from recidiviz.persistence.entity_matching import (
    templates as persistence_templates_module,
)
from recidiviz.tests.ingest.direct import templates as test_templates_module
from recidiviz.tests.ingest.direct.regions.direct_ingest_region_structure_test import (
    DirectIngestRegionDirStructureBase,
)

GENERIC_REGION_CODE = "us_xx"

INGEST_TEMPLATES_DIR_PATH = os.path.dirname(
    os.path.relpath(ingest_templates_module.__file__, start=DEFAULT_WORKING_DIR)
)
PERSISTENCE_TEMPLATES_DIR_PATH = os.path.dirname(
    os.path.relpath(persistence_templates_module.__file__, start=DEFAULT_WORKING_DIR)
)
INGEST_TEST_TEMPLATES_DIR_PATH = os.path.dirname(
    os.path.relpath(test_templates_module.__file__, start=DEFAULT_WORKING_DIR)
)


class DirectIngestFilesGeneratorTest(
    DirectIngestRegionDirStructureBase, unittest.TestCase
):
    """Tests for DirectIngestFilesGenerator."""

    def setUp(self) -> None:
        # Create a temporary directory so that tests in base class can be run in parallel
        self.temp_dir = tempfile.mkdtemp()
        self.populate_test_directory()

        super().setUp()
        test_region_code = "us_aa"
        self.files_generator = DirectIngestFilesGenerator(
            test_region_code, self.temp_dir
        )
        self.files_generator.generate_all_new_dirs_and_files()

    def populate_test_directory(self) -> None:
        """Populate temporary directory with files needed for DirectIngestFilesGenerator"""
        copytree(
            os.path.join(os.path.dirname(regions_module.__file__)),
            os.path.join(self.temp_dir, REGIONS_DIR_PATH),
        )
        copytree(
            os.path.join(
                os.path.dirname(ingest_templates_module.__file__), GENERIC_REGION_CODE
            ),
            os.path.join(self.temp_dir, INGEST_TEMPLATES_DIR_PATH, GENERIC_REGION_CODE),
        )
        copytree(
            os.path.join(
                os.path.dirname(persistence_templates_module.__file__),
                GENERIC_REGION_CODE,
            ),
            os.path.join(
                self.temp_dir, PERSISTENCE_TEMPLATES_DIR_PATH, GENERIC_REGION_CODE
            ),
        )
        copytree(
            os.path.join(
                os.path.dirname(test_templates_module.__file__), GENERIC_REGION_CODE
            ),
            os.path.join(
                self.temp_dir, INGEST_TEST_TEMPLATES_DIR_PATH, GENERIC_REGION_CODE
            ),
        )

    def tearDown(self) -> None:
        rmtree(self.temp_dir)

    @property
    def region_dir_names(self) -> List[str]:
        return get_existing_region_dir_names()

    @property
    def region_dir_paths(self) -> List[str]:
        return [
            os.path.join(self.temp_dir, REGIONS_DIR_PATH, d)
            for d in get_existing_region_dir_names()
        ]

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
