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
import unittest

from recidiviz.common.constants.states import StateCode
from recidiviz.tests.ingest.direct.regions.state_ingest_view_parser_test_base import (
    LegacyStateIngestViewParserTestBase,
)


class UsXxIngestViewParserTest(LegacyStateIngestViewParserTestBase, unittest.TestCase):
    """Parser unit tests for each US_XX ingest view query results to be ingested."""

    @classmethod
    def state_code(cls) -> StateCode:
        return StateCode.US_XX

    @property
    def test(self) -> unittest.TestCase:
        return self

    # ~~ Add parsing tests for new ingest view files here ~~
    # Parser tests must call self._run_parse_ingest_view_test() and follow the naming
    # convention `test_parse_<file_tag>`.
