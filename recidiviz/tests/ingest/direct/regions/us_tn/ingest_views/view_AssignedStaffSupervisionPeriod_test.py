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
"""Tests the TN `AssignedStaffSupervisionPeriod` view logic."""

from typing import Any, List, Tuple

import pandas as pd
from mock import Mock, patch
from more_itertools import one
from pandas.testing import assert_frame_equal

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.controllers.direct_ingest_view_collector import (
    DirectIngestPreProcessedIngestViewCollector,
)
from recidiviz.ingest.direct.query_utils import get_region_raw_file_config
from recidiviz.tests.big_query.view_test_util import BaseViewTest
from recidiviz.utils.regions import get_region

STATE_CODE = StateCode.US_TN.value


@patch("recidiviz.utils.metadata.project_id", Mock(return_value="t"))
class AssignedStaffSupervisionPeriodPeriodTest(BaseViewTest):
    """Tests the TN `AssignedStaffSupervisionPeriod` query functionality."""

    def setUp(self) -> None:
        super().setUp()
        view_builders = DirectIngestPreProcessedIngestViewCollector(
            get_region(STATE_CODE, is_direct_ingest=True), []
        ).collect_view_builders()
        self.view_builder = one(
            view
            for view in view_builders
            if view.file_tag == "AssignedStaffSupervisionPeriod"
        )

        # All columns need to be in lowercase.
        self.expected_result_columns = [
            "offenderid",
            "supervisiontype",
            "startdate",
            "enddate",
            "admissionreason",
            "terminationreason",
            "supervisionofficer",
            "assignmenttype",
            "site",
            "supervisionperiodsequencenumber",
        ]

    def run_test(
        self,
        assigned_staff: List[Tuple[Any, ...]],
        expected_output: List[List[Any]],
    ) -> None:
        """Runs a test that executes the AssignedStaffSupervisionPeriod query given the provided
        input rows.
        """

        # Arrange
        raw_file_configs = get_region_raw_file_config(STATE_CODE).raw_file_configs

        self.create_mock_raw_file(
            region_code=STATE_CODE,
            file_config=raw_file_configs["AssignedStaff"],
            mock_data=assigned_staff,
        )

        # Act
        results = self.query_raw_data_view_for_builder(
            self.view_builder,
            dimensions=self.expected_result_columns,
        )

        # Assert
        expected = pd.DataFrame(expected_output, columns=self.expected_result_columns)
        expected = expected.set_index(self.expected_result_columns)
        print(expected)
        print(results)
        assert_frame_equal(expected, results)

    def test_assigned_staff_supervision_period_simple(self) -> None:
        self.run_test(
            assigned_staff=[
                (
                    "ABCDEF01",  # StaffID
                    "12345678",  # OffenderID
                    "PRO",  # AssignmentType
                    "2020-10-12",  # StartDate
                    "2021-01-04",  # EndDate
                    "PPO",  # CaseType
                    "RENEO",  # AssignmentBeginReason
                    '" "RNO  " "',  # AssignmentEndReason
                    "NULL",  # AssignmentDueDate
                    '"P94F "',  # Site
                    '"ABCDEF01 "',  # LastUpdateUserID
                    "2021-01-04 08:49:43.983368",  # LastUpdateDate
                )
            ],
            expected_output=[
                [
                    "12345678",  # OffenderID
                    "PPO",  # SupervisionType
                    "2020-10-12",  # StartDate
                    "2021-01-04",  # EndDateTime
                    "RENEO",  # AdmissionReason
                    "RNO",  # TerminationReason
                    "ABCDEF01",  # SupervisionOfficer
                    "PRO",  # AssignmentType
                    "P94F",  # Site
                    1,  # SupervisionPeriodSequenceNumber
                ],
            ],
        )
