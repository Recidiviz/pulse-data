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
  - StateIngestMappingTestCase (to test ingest mappings against input fixtures)
"""
import csv
import io
import os
import re
import unittest

from recidiviz.big_query.big_query_address import BigQueryAddress
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
from recidiviz.persistence.entity.entities_module_context_factory import (
    entities_module_context_for_module,
)
from recidiviz.persistence.entity.entity_utils import print_entity_trees
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.tests.common.constants.state.external_id_types_test import (
    external_id_types_by_state_code,
)
from recidiviz.tests.ingest.direct.fixture_util import (
    INGEST_MAPPING_OUTPUT_SUBDIR,
    enum_parsing_fixture_path,
    fixture_path_for_address,
    ingest_mapping_output_fixture_path,
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


class StateIngestMappingTestCase(BaseStateIngestTestCase):
    """Base class for ingest mapping tests, where we test the parsing of ingest view results."""

    @classmethod
    def state_code(cls) -> StateCode:
        raise NotImplementedError("Ingest tests must have a StateCode!")

    def _read_from_ingest_view_results_fixture(
        self, ingest_view_name: str, characteristic: str
    ) -> list:
        """Reads a fixture file containing ingest view results."""
        input_fixture = fixture_path_for_address(
            self.state_code(),
            BigQueryAddress(
                dataset_id=f"{self.state_code().value.lower()}_ingest_view_results",
                table_id=ingest_view_name,
            ),
            characteristic,
        )
        return list(
            csv.DictReader(
                LocalFileContentsHandle(
                    input_fixture, cleanup_file=False
                ).get_contents_iterator()
            )
        )

    def run_ingest_mapping_test(
        self,
        create_expected_output: bool = False,
    ) -> None:
        """
        This tests that an ingest mapping produces an expected output (entity tree)
        from particular ingest view results.

        Args:
            create_expected_output:
                When True, this argument will write the expected entity tree representation
                to a file. The file name is <ingest view name>.fixture and is simply a text file.
                The path to this file arises from `ingest_view_results_parsing_fixture_path`

        Note that empty (either None or empty list) fields on entities are not written out to the file
        or string buffer for this test.
        """
        if in_ci() and create_expected_output:
            raise ValueError("Cannot have create_expected_output=True in the CI.")

        # self.id() is from unittest.TestCase
        # https://docs.python.org/3/library/unittest.html#unittest.TestCase.id
        *_, test_name = self.id().split(".")

        (
            ingest_view_name,
            characteristic,
        ) = self.get_ingest_view_name_and_characteristic(test_name)

        # Build input by reading in fixture, compiling the mapping, and parsing fixture data.
        ingest_view_results = self._read_from_ingest_view_results_fixture(
            ingest_view_name, characteristic
        )
        manifest = self.ingest_view_manifest_compiler.compile_manifest(
            ingest_view_name=ingest_view_name
        )

        # Check that our mapping has a defined RootEntity with
        # defined external ID types.
        if manifest.root_entity_cls not in {
            state_entities.StatePerson,
            state_entities.StateStaff,
        }:
            raise ValueError(f"Unexpected RootEntity type: {manifest.root_entity_cls}")
        allowed_id_types = external_id_types_by_state_code()[self.state_code()]
        found_id_types = manifest.root_entity_external_id_types
        if unexpected_id_types := found_id_types - allowed_id_types:
            raise ValueError(
                f"Unexpected external ID types for {manifest.root_entity_cls}: {unexpected_id_types}"
            )

        parsed_ingest_view_results = manifest.parse_contents(
            contents_iterator=ingest_view_results,
            context=IngestViewContentsContext.build_for_tests(),
        )

        # Build expected entity trees by loading the representation from a fixture
        expected_fixture_path = ingest_mapping_output_fixture_path(
            self.state_code(), ingest_view_name, characteristic
        )
        state_entity_context = entities_module_context_for_module(state_entities)
        if create_expected_output:
            self.create_directory_for_characteristic_level_fixtures(
                expected_fixture_path, characteristic
            )
            with open(expected_fixture_path, "w", encoding="utf-8") as _output_fixture:
                print_entity_trees(
                    parsed_ingest_view_results,
                    state_entity_context,
                    file_or_buffer=_output_fixture,
                )
        with open(expected_fixture_path, "r", encoding="utf-8") as _output_fixture:
            expected_trees = _output_fixture.read()

        # Actually test the ingest mapping!
        # We write the parsed entity tree representation to a string buffer
        # so that we can compare against the representation in the fixture file.
        with io.StringIO() as tree_buffer:
            print_entity_trees(
                parsed_ingest_view_results,
                state_entity_context,
                file_or_buffer=tree_buffer,
            )
            actual_trees = tree_buffer.getvalue()
        self.assertEqual(actual_trees, expected_trees)


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
