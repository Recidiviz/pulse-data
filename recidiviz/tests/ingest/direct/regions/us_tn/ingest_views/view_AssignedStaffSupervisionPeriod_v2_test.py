# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Tests the TN `AssignedStaffSupervisionPeriod_v2` view logic."""

from mock import Mock, patch

from recidiviz.common.constants.states import StateCode
from recidiviz.tests.big_query.view_test_util import BaseViewTest


@patch("recidiviz.utils.metadata.project_id", Mock(return_value="t"))
class AssignedStaffSupervisionPeriodPeriodV2Test(BaseViewTest):
    """Tests the TN `AssignedStaffSupervisionPeriod_v2` query functionality."""

    def setUp(self) -> None:
        super().setUp()
        self.region_code = StateCode.US_TN.value
        self.view_builder = self.view_builder_for_tag(
            self.region_code, "AssignedStaffSupervisionPeriod_v2"
        )
        self.data_types = str

    def test_assigned_staff_supervision_period_simple(self) -> None:
        self.run_ingest_view_test(fixtures_files_name="simple.csv")

    def test_assigned_staff_supervision_simple_open_period(self) -> None:
        self.run_ingest_view_test(fixtures_files_name="simple_open_period.csv")

    def test_assigned_staff_supervision_period_noncontiguous(self) -> None:
        self.run_ingest_view_test(
            fixtures_files_name="noncontiguous_supervision_periods.csv"
        )

    def test_assigned_staff_supervision_period_squashes_contiguous_plan_without_changes(
        self,
    ) -> None:
        self.run_ingest_view_test(
            fixtures_files_name="squashes_contiguous_plan_without_changes.csv"
        )

    def test_assigned_staff_supervision_period_changed_supervision_level(self) -> None:
        self.run_ingest_view_test(fixtures_files_name="changed_supervision_level.csv")

    def test_assigned_staff_supervision_period_concurrent_probation_and_cc(
        self,
    ) -> None:
        self.run_ingest_view_test(fixtures_files_name="concurrent_probation_and_cc.csv")

    def test_assigned_staff_supervision_period_multi_officer_one_supervision_left_open(
        self,
    ) -> None:
        self.run_ingest_view_test(
            fixtures_files_name="multi_officer_one_supervision_left_open.csv"
        )

    def test_assigned_staff_supervision_period_supervision_plan_ends_before_period(
        self,
    ) -> None:
        self.run_ingest_view_test(
            fixtures_files_name="supervision_plan_ends_before_period.csv"
        )

    def test_assigned_staff_supervision_period_new_period_type_without_plan(
        self,
    ) -> None:
        self.run_ingest_view_test(
            fixtures_files_name="new_period_type_without_plan.csv"
        )

    def test_assigned_staff_supervision_period_supervision_period_starts_without_plan(
        self,
    ) -> None:
        self.run_ingest_view_test(
            fixtures_files_name="supervision_period_starts_without_plan.csv"
        )

    def test_assigned_staff_supervision_period_gap_in_supervision_plan(self) -> None:
        self.run_ingest_view_test(fixtures_files_name="gap_in_supervision_plan.csv")

    def test_assigned_staff_supervision_period_exclude_discharge_time(self) -> None:
        self.run_ingest_view_test(fixtures_files_name="exclude_discharge_time.csv")
