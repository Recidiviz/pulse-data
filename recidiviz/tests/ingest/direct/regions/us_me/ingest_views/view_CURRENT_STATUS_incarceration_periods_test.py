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
"""Tests the US_ME `CURRENT_STATUS_incarceration_periods` view logic."""
from unittest.mock import Mock, patch

from recidiviz.common.constants.states import StateCode
from recidiviz.tests.big_query.view_test_util import BaseViewTest

STATE_CODE = StateCode.US_ME.value


@patch("recidiviz.utils.metadata.project_id", Mock(return_value="t"))
class CurrentStatusIncarcerationPeriodTest(BaseViewTest):
    """Tests the US_ME `CURRENT_STATUS_incarceration_periods` view query functionality."""

    def setUp(self) -> None:
        super().setUp()
        self.region_code = StateCode.US_ME.value
        self.view_builder = self.view_builder_for_tag(
            self.region_code, "CURRENT_STATUS_incarceration_periods"
        )
        self.data_types = str

    def test_juvenile_locations(self) -> None:
        """Assert that incarceration periods at juvenile locations are not returned in the query."""
        self.run_ingest_view_test(fixtures_files_name="juvenile_locations.csv")

    def test_supervision_status_in_doc_facility(self) -> None:
        """Assert that supervision statuses with a location in a DOC facility is not captured as an incarceration
        period."""
        self.run_ingest_view_test(
            fixtures_files_name="supervision_status_at_doc_facility.csv"
        )

    def test_incarceration_periods_with_furlough(self) -> None:
        """Assert that supervision statuses with a location in a DOC facility is not captured as an incarceration
        period."""
        self.run_ingest_view_test(
            fixtures_files_name="incarceration_periods_with_furlough.csv"
        )

    def test_incarceration_period_ends_with_death_status(self) -> None:
        """Assert that periods followed by a current status of 'DECEASED' are captured correctly."""
        self.run_ingest_view_test(
            fixtures_files_name="incarceration_period_ends_with_death_status.csv"
        )
