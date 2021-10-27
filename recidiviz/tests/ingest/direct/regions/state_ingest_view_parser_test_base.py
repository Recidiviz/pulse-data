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
"""Base test class for ingest view parser tests."""
import csv
import os
import unittest
from abc import abstractmethod
from typing import Dict, Sequence

from recidiviz.cloud_storage.gcs_file_system import GcsfsFileContentsHandle
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.controllers.direct_ingest_instance import (
    DirectIngestInstance,
)
from recidiviz.ingest.direct.controllers.ingest_view_file_parser import (
    FileFormat,
    IngestViewFileParser,
)
from recidiviz.ingest.direct.controllers.ingest_view_file_parser_delegate import (
    IngestViewFileParserDelegateImpl,
    ingest_view_manifest_dir,
)
from recidiviz.ingest.direct.controllers.ingest_view_manifest import (
    EntityTreeManifest,
    EnumFieldManifest,
)
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entity_utils import print_entity_trees
from recidiviz.tests.ingest.direct.fixture_util import direct_ingest_fixture_path
from recidiviz.utils.environment import in_ci
from recidiviz.utils.regions import Region, get_region


# TODO(#9022): Enforce that every single ingest view manifest has an associated parser
#  test.
class StateIngestViewParserTestBase:
    """Base test class for ingest view parser tests."""

    @classmethod
    @abstractmethod
    def schema_type(cls) -> SchemaType:
        pass

    @classmethod
    @abstractmethod
    def region_code(cls) -> str:
        pass

    @property
    @abstractmethod
    def test(self) -> unittest.TestCase:
        pass

    def _region(self) -> Region:
        return get_region(
            self.region_code(),
            is_direct_ingest=True,
        )

    @classmethod
    def _main_ingest_instance(cls) -> DirectIngestInstance:
        # We assume we're ingesting into the SECONDARY ingest instance, which
        # should always have the latest ingest logic updates released to it.
        return DirectIngestInstance.SECONDARY

    def _build_parser(self) -> IngestViewFileParser:
        region = self._region()
        return IngestViewFileParser(
            delegate=IngestViewFileParserDelegateImpl(
                region, self.schema_type(), self._main_ingest_instance()
            )
        )

    def _parse_manifest(self, file_tag: str) -> EntityTreeManifest:
        parser = self._build_parser()
        manifest_ast, _ = parser.parse_manifest(
            manifest_path=parser.delegate.get_ingest_view_manifest_path(file_tag),
        )
        return manifest_ast

    def _parse_enum_manifest_test(
        self, file_tag: str, enum_parser_manifest: EnumFieldManifest
    ) -> None:
        fixture_path = direct_ingest_fixture_path(
            region_code=self.region_code(),
            file_name=f"{file_tag}.csv",
        )

        contents_handle = GcsfsFileContentsHandle(fixture_path, cleanup_file=False)
        for row in csv.DictReader(contents_handle.get_contents_iterator()):
            _ = enum_parser_manifest.build_from_row(row).parse()

    def _run_parse_ingest_view_test(
        self, file_tag: str, expected_output: Sequence[Entity], debug: bool = False
    ) -> None:
        self._check_test_matches_file_tag(file_tag)

        parser = self._build_parser()
        fixture_path = direct_ingest_fixture_path(
            region_code=self.region_code(),
            file_name=f"{file_tag}.csv",
        )
        parsed_output = parser.parse(
            file_tag=file_tag,
            contents_handle=GcsfsFileContentsHandle(fixture_path, cleanup_file=False),
            file_format=FileFormat.CSV,
        )

        if debug:
            if in_ci():
                self.test.fail(
                    "The |debug| flag should only be used for local debugging."
                )

            print("============== EXPECTED ==============")
            print_entity_trees(expected_output)
            print("============== ACTUAL ==============")
            print_entity_trees(parsed_output)

        self.test.assertEqual(expected_output, parsed_output)

    # TODO(#8905): Change this function to just return manifest paths once all
    # manifests have been migrated to v2 mappings and have
    # test_all_ingest_view_manifests_parse do the parsing.
    def _manifest_path_to_v2_manifest(self) -> Dict[str, EntityTreeManifest]:
        region = self._region()
        manifest_dir = ingest_view_manifest_dir(region)

        parser = self._build_parser()

        result = {}
        for file in os.listdir(manifest_dir):
            if file == "__init__.py":
                continue
            manifest_path = os.path.join(manifest_dir, file)
            try:
                manifest_ast, _ = parser.parse_manifest(manifest_path)
            except KeyError as e:
                if "Expected nonnull [manifest_language] in input" in str(e):
                    # TODO(#8905): Remove this check once all files have been migrated
                    #  to v2 structure.
                    continue
                raise e

            self.test.assertIsInstance(manifest_ast, EntityTreeManifest)
            result[manifest_path] = manifest_ast
        return result

    def test_all_ingest_view_manifests_parse(self) -> None:
        if self.region_code() == StateCode.US_XX.value:
            # Skip template region
            return

        # Make sure building all manifests doesn't crash
        _ = self._manifest_path_to_v2_manifest()

    def test_all_ingest_view_manifests_are_tested(self) -> None:
        if self.region_code() == StateCode.US_XX.value:
            # Skip template region
            return

        for manifest_path in self._manifest_path_to_v2_manifest():
            self._check_manifest_has_parser_test(manifest_path)

    def _check_manifest_has_parser_test(self, manifest_path: str) -> None:
        """Validates that manifest at the given path has an associated test defined in
        this test class.
        """
        file_tag = self._file_tag_for_manifest(manifest_path)
        expected_test_name = self._expected_test_name_for_tag(file_tag)

        all_tests = [t for t in dir(self) if t.startswith("test_")]
        if expected_test_name not in all_tests:
            self.test.fail(
                f"Missing test for [{file_tag}] - expected test with name "
                f"[{expected_test_name}]",
            )

    def _file_tag_for_manifest(self, manifest_path: str) -> str:
        """Parses the ingest view file tag from the ingest view manifest path."""
        _, manifest_file = os.path.split(manifest_path)
        manifest_name, _ = os.path.splitext(manifest_file)

        return manifest_name[len(self.region_code()) + 1 :]

    @staticmethod
    def _expected_test_name_for_tag(file_tag: str) -> str:
        """Returns the name we expect for the parser test for this file tag."""
        return f"test_parse_{file_tag}"

    def _check_test_matches_file_tag(self, file_tag: str) -> None:
        """Validates that the file tag that is being processed is the expected file tag
        given the current test name.
        """
        expected_test_name = self._expected_test_name_for_tag(file_tag)
        actual_test_name = self.test._testMethodName  # pylint: disable=protected-access

        if actual_test_name != expected_test_name:
            self.test.fail(
                f"Unexpected test name [{actual_test_name}] for file_tag "
                f"[{file_tag}]. Expected [{expected_test_name}]."
            )
