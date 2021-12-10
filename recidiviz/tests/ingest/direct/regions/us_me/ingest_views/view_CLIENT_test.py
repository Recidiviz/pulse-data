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
"""Tests the US_ME `CLIENT` view logic."""
from unittest.mock import Mock, patch

from recidiviz.common.constants.states import StateCode
from recidiviz.tests.big_query.view_test_util import BaseViewTest


@patch("recidiviz.utils.metadata.project_id", Mock(return_value="t"))
class ClientTest(BaseViewTest):
    """Tests the US_ME `CLIENT` view query functionality."""

    def setUp(self) -> None:
        super().setUp()
        self.region_code = StateCode.US_ME.value
        self.view_builder = self.view_builder_for_tag(self.region_code, "CLIENT")

    def test_client_query_name_formatting(self) -> None:
        fixtures_files_name = "name_formatting.csv"
        self.create_mock_raw_bq_tables_from_fixtures(
            region_code=self.region_code,
            ingest_view_builder=self.view_builder,
            raw_fixtures_name=fixtures_files_name,
        )

        self.run_ingest_view_test(
            region_code=self.region_code,
            view_builder=self.view_builder,
            expected_output_fixture_file_name=fixtures_files_name,
        )
