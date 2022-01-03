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
"""Tests the PA sci incarceration period logic"""

from mock import Mock, patch

from recidiviz.common.constants.states import StateCode
from recidiviz.tests.big_query.view_test_util import BaseViewTest


@patch("recidiviz.utils.metadata.project_id", Mock(return_value="t"))
class ViewSciIncarcerationPeriodTest(BaseViewTest):
    """Tests the PA external ids query functionality"""

    def setUp(self) -> None:
        super().setUp()
        self.region_code = StateCode.US_PA.value
        self.view_builder = self.view_builder_for_tag(
            self.region_code, "sci_incarceration_period"
        )
        self.data_types = str

    def test_move_facilities(self) -> None:
        self.run_ingest_view_test(
            fixtures_files_name="sci_incarceration_period_movements.csv"
        )
