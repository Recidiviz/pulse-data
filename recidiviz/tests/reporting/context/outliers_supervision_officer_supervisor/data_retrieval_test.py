# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Tests for the Outliers supervisor report data retrieval function"""

from unittest import TestCase
from unittest.mock import MagicMock, patch

from recidiviz.common.constants.states import StateCode
from recidiviz.outliers.constants import (
    ABSCONSIONS_BENCH_WARRANTS,
    INCARCERATION_STARTS,
    INCARCERATION_STARTS_TECHNICAL_VIOLATION,
)
from recidiviz.outliers.types import OfficerSupervisorReportData
from recidiviz.reporting.context.outliers_supervision_officer_supervisor.data_retrieval import (
    retrieve_data_for_outliers_supervision_officer_supervisor,
)
from recidiviz.reporting.context.outliers_supervision_officer_supervisor.fixtures import (
    metric_fixtures,
)
from recidiviz.reporting.context.po_monthly_report.constants import Batch, ReportType


@patch(
    "recidiviz.reporting.context.outliers_supervision_officer_supervisor.data_retrieval.OutliersQuerier.get_officer_level_report_data_for_all_officer_supervisors"
)
class RetrieveDataTest(TestCase):
    def test_exclude_recipients_without_metrics(self, query_mock: MagicMock) -> None:
        report_without_outliers = OfficerSupervisorReportData(
            metrics=[],
            metrics_without_outliers=[
                metric_fixtures[ABSCONSIONS_BENCH_WARRANTS],
                metric_fixtures[INCARCERATION_STARTS],
                metric_fixtures[INCARCERATION_STARTS_TECHNICAL_VIOLATION],
            ],
            recipient_email_address="test@recidiviz.org",
        )
        query_mock.return_value = {"abc": report_without_outliers}

        test_batch = Batch(
            StateCode.US_XX,
            report_type=ReportType.OutliersSupervisionOfficerSupervisor,
            batch_id="20230614123033",
        )

        self.assertEqual(
            retrieve_data_for_outliers_supervision_officer_supervisor(test_batch), {}
        )
