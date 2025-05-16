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
"""
This module aims to encapsulate common elements for all ingest testing.

It defines some core classes:
  - BaseStateIngestTestCase (has methods needed for testing views and mappings)
  - EnumManifestParsingTestCase (to test parsing Enums from raw data)
"""
import csv
import os
import re
import unittest

from recidiviz.common.constants.states import StateCode
from recidiviz.common.io.local_file_contents_handle import LocalFileContentsHandle
from recidiviz.ingest.direct.direct_ingest_regions import get_direct_ingest_region
from recidiviz.ingest.direct.ingest_mappings.ingest_view_contents_context import (
    IngestViewContentsContext,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest import (
    EntityTreeManifest,
    EnumMappingManifest,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_compiler import (
    IngestViewManifestCompiler,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_compiler_delegate import (
    StateSchemaIngestViewManifestCompilerDelegate,
)
from recidiviz.tests.ingest.direct.fixture_util import (
    INGEST_MAPPING_OUTPUT_SUBDIR,
    enum_parsing_fixture_path,
)
from recidiviz.utils.environment import in_ci


class BaseStateIngestTestCase(unittest.TestCase):
    """Holds methods and functionality needed by multiple types of ingest tests."""

    INGEST_TEST_NAME_REGEX = re.compile(r"^test_(\w+)__for__(\w+)$")

    @classmethod
    def state_code(cls) -> StateCode:
        raise NotImplementedError("Ingest tests must have a StateCode!")

    @classmethod
    def build_ingest_view_manifest_compiler(cls) -> IngestViewManifestCompiler:
        return IngestViewManifestCompiler(
            delegate=StateSchemaIngestViewManifestCompilerDelegate(
                region=get_direct_ingest_region(cls.state_code().value)
            )
        )

    def setUp(self) -> None:
        super().setUp()
        self.ingest_view_manifest_compiler = self.build_ingest_view_manifest_compiler()
        if not in_ci():
            self.maxDiff = None

    @classmethod
    def get_ingest_view_name_and_characteristic(
        cls, test_method_name: str
    ) -> tuple[str, str]:
        if not (matches := cls.INGEST_TEST_NAME_REGEX.match(test_method_name)):
            raise ValueError(
                f"Test method name did not match required format: {test_method_name}. "
                "Test names must be 'test_<ingest view name>__for__<thing you are testing>'"
            )
        ingest_view_name, characteristic = matches.groups()
        return str(ingest_view_name), str(characteristic)

    def create_directory_for_characteristic_level_fixtures(
        self, expected_fixture_path: str, characteristic: str
    ) -> None:
        """
        Creates the directory needed for fixture files named for a characteristic.
        These include:
            - ingest view results: The output of ingest view tests / input of ingest mapping tests
            - ingest mapping results: The output of ingest mapping tests
        """
        ext = "txt" if INGEST_MAPPING_OUTPUT_SUBDIR in expected_fixture_path else "csv"
        directory = expected_fixture_path.strip(f"{characteristic}.{ext}")
        os.makedirs(directory, exist_ok=True)


class EnumManifestParsingTestCase(BaseStateIngestTestCase):
    """This TestCase serves as the base for testing complex parsing of enums from raw text."""

    @classmethod
    def state_code(cls) -> StateCode:
        raise NotImplementedError("Ingest tests must have a StateCode!")

    def parse_manifest(self, ingest_view_name: str) -> EntityTreeManifest:
        manifest = self.ingest_view_manifest_compiler.compile_manifest(
            ingest_view_name=ingest_view_name
        )
        return manifest.output

    def run_enum_manifest_parsing_test(
        self, file_tag: str, enum_parser_manifest: EnumMappingManifest
    ) -> None:
        fixture_path = enum_parsing_fixture_path(self.state_code(), file_tag)
        contents_handle = LocalFileContentsHandle(fixture_path, cleanup_file=False)
        for row in csv.DictReader(contents_handle.get_contents_iterator()):
            _ = enum_parser_manifest.build_from_row(
                row,
                context=IngestViewContentsContext.build_for_tests(),
            )
