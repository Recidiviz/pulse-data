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
"""Ingest view parser tests for US_XX direct ingest."""
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.tests.ingest.direct.regions.state_ingest_view_parser_test_base import (
    StateIngestViewParserTestBase,
)


class UsXxIngestViewParserTest(StateIngestViewParserTestBase):
    """Parser unit tests for each US_XX ingest view file to be ingested."""

    @classmethod
    def schema_type(cls) -> SchemaType:
        raise NotImplementedError("Choose one of STATE or JAILS")

    @classmethod
    def region_code(cls) -> str:
        return "US_XX"

    # Add parsing tests for new ingest view files here #
