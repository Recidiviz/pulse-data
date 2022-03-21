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
"""Tests the US_ME `supervision_periods` view logic."""
from unittest.mock import Mock, patch

from recidiviz.common.constants.states import StateCode
from recidiviz.tests.big_query.view_test_util import BaseViewTest


@patch("recidiviz.utils.metadata.project_id", Mock(return_value="t"))
class SupervisionPeriodsTest(BaseViewTest):
    """Tests the US_ME `supervision_periods` view query functionality."""

    def setUp(self) -> None:
        super().setUp()
        self.region_code = StateCode.US_ME.value
        self.view_builder = self.view_builder_for_tag(
            self.region_code, "supervision_periods"
        )
        self.data_types = {
            "client_id": str,
            "supervision_period_id": int,
            "start_date": str,
            "end_date": str,
            "previous_status": str,
            "current_status": str,
            "next_status": str,
            "supervision_location": str,
            "previous_jurisdiction_location_type": str,
            "current_jurisdiction_location_type": str,
            "next_jurisdiction_location_type": str,
            "transfer_type": str,
            "transfer_reason": str,
            "next_transfer_type": str,
            "next_transfer_reason": str,
            "officer_external_id": str,
            "officer_status": str,
            "officer_first_name": str,
            "officer_middle_name": str,
            "officer_last_name": str,
        }

    def test_multiple_commitments_from_supervision(self) -> None:
        """Assert that supervision periods with multiple re-commitments are captured correctly."""
        self.run_ingest_view_test(
            fixtures_files_name="multiple_commitments_from_supervision.csv"
        )

    def test_choose_supervision_start_over_status(self) -> None:
        """Assert that clients with an earlier status date than supervision start date prioritize the supervision
        start date."""
        self.run_ingest_view_test(
            fixtures_files_name="choose_supervision_start_over_status.csv"
        )
