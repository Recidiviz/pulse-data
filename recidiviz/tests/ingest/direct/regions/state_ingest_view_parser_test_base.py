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
import unittest
from abc import abstractmethod
from typing import Sequence

from recidiviz.cloud_storage.gcs_file_system import GcsfsFileContentsHandle
from recidiviz.ingest.direct.controllers.ingest_view_file_parser import (
    FileFormat,
    IngestViewFileParser,
    IngestViewFileParserDelegateImpl,
)
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entity_utils import print_entity_trees
from recidiviz.tests.ingest.direct.fixture_util import direct_ingest_fixture_path
from recidiviz.utils.environment import in_ci
from recidiviz.utils.regions import get_region


# TODO(#9022): Enforce that every single ingest view manifest has an associated parser
#  test.
class StateIngestViewParserTestBase(unittest.TestCase):
    """Base test class for ingest view parser tests."""

    @classmethod
    @abstractmethod
    def schema_type(cls) -> SchemaType:
        pass

    @classmethod
    @abstractmethod
    def region_code(cls) -> str:
        pass

    def _run_parse_ingest_view_test(
        self, file_tag: str, expected_output: Sequence[Entity], debug: bool = False
    ) -> None:
        parser = IngestViewFileParser(
            delegate=IngestViewFileParserDelegateImpl(
                get_region(self.region_code(), is_direct_ingest=True),
                self.schema_type(),
            )
        )
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
                self.fail("The |debug| flag should only be used for local debugging.")

            print("============== EXPECTED ==============")
            print_entity_trees(expected_output)
            print("============== ACTUAL ==============")
            print_entity_trees(parsed_output)

        self.assertEqual(expected_output, parsed_output)
