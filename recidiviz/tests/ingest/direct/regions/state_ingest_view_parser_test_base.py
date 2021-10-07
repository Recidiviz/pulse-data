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
from typing import Sequence

from recidiviz.cloud_storage.gcs_file_system import GcsfsFileContentsHandle
from recidiviz.common.constants.states import StateCode
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

    def _build_parser(self) -> IngestViewFileParser:
        region = self._region()
        return IngestViewFileParser(
            delegate=IngestViewFileParserDelegateImpl(region, self.schema_type())
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

    def test_all_ingest_view_manifests_parse(self) -> None:
        if self.region_code() == StateCode.US_XX.value:
            # Skip template region
            return

        parser = self._build_parser()

        region = self._region()
        manifest_dir = ingest_view_manifest_dir(region)

        for file in os.listdir(manifest_dir):
            if file == "__init__.py":
                continue
            manifest_path = os.path.join(manifest_dir, file)
            try:
                manifest_ast, _ = parser.parse_manifest(manifest_path)
            except KeyError as e:
                if "Expected nonnull [manifest_language] in input" in str(e):
                    # TODO(#8905): Remove this check once all files have been migrated to
                    #  v2 structure.
                    continue
                raise e

            self.test.assertIsInstance(manifest_ast, EntityTreeManifest)
