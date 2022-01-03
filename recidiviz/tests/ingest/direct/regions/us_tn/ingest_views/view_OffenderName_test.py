#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2021 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""Tests the TN `OffenderName` view logic."""

from mock import Mock, patch

from recidiviz.common.constants.states import StateCode
from recidiviz.tests.big_query.view_test_util import BaseViewTest


@patch("recidiviz.utils.metadata.project_id", Mock(return_value="t"))
class OffenderNameTest(BaseViewTest):
    """Tests the TN `OffenderName` query functionality."""

    def setUp(self) -> None:
        super().setUp()
        self.region_code = StateCode.US_TN.value
        self.view_builder = self.view_builder_for_tag(self.region_code, "OffenderName")
        self.data_types = str

    def test_offender_name_simple(self) -> None:
        self.run_ingest_view_test(fixtures_files_name="offender_name_simple.csv")

    def test_offender_name_multiple_rows_for_same_id_collapsed(self) -> None:
        self.run_ingest_view_test(fixtures_files_name="offender_name_multiple_ids.csv")

    def test_offender_name_multiple_rows_for_same_id_collapsed_filter_nickname(
        self,
    ) -> None:
        self.run_ingest_view_test(
            fixtures_files_name="offender_name_filter_nickname.csv"
        )
