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
"""Tests for email content preparation logic in the OutliersSupervisionOfficerSupervisor report"""
from unittest import TestCase

from recidiviz.common.constants.states import StateCode
from recidiviz.outliers.constants import (
    ABSCONSIONS_BENCH_WARRANTS,
    EARLY_DISCHARGE_REQUESTS,
    INCARCERATION_STARTS_TECHNICAL_VIOLATION,
)
from recidiviz.outliers.querier.querier import OfficerSupervisorReportData
from recidiviz.outliers.types import TargetStatusStrategy
from recidiviz.reporting.context.outliers_supervision_officer_supervisor.context import (
    OutliersSupervisionOfficerSupervisorContext,
)
from recidiviz.reporting.context.outliers_supervision_officer_supervisor.fixtures import (
    create_fixture,
    highlighted_officers_fixture,
    metric_fixtures,
    other_officers_fixture,
    target_fixture,
)
from recidiviz.reporting.context.outliers_supervision_officer_supervisor.types import (
    MetricHighlightDetail,
    MultipleMetricHighlight,
)
from recidiviz.reporting.context.po_monthly_report.constants import Batch, ReportType
from recidiviz.reporting.recipient import Recipient


class OutliersSupervisionOfficerSupervisorTest(TestCase):
    """Tests for email content preparation logic"""

    def setUp(self) -> None:
        self.test_email = "test-outliers-supervisor@recidiviz.org"
        self.batch = Batch(
            state_code=StateCode.US_XX,
            batch_id="20230614123033",
            report_type=ReportType.OutliersSupervisionOfficerSupervisor,
        )

    def _get_prepared_data(self, report: OfficerSupervisorReportData) -> dict:
        recipient = Recipient(
            email_address=report.recipient_email_address,
            state_code=self.batch.state_code,
            data={
                "report": report,
            },
        )
        context = OutliersSupervisionOfficerSupervisorContext(self.batch, recipient)
        return context.get_prepared_data()

    def test_highlight_multiple_metrics(self) -> None:
        actual = self._get_prepared_data(
            OfficerSupervisorReportData(
                metrics=[],
                metrics_without_outliers=[],
                recipient_email_address=self.test_email,
            )
        )
        self.assertIsNone(actual["highlights"].multiple_metrics)

        actual = self._get_prepared_data(
            OfficerSupervisorReportData(
                metrics=[
                    create_fixture(
                        metric_fixtures[INCARCERATION_STARTS_TECHNICAL_VIOLATION],
                        target_fixture,
                        other_officers_fixture,
                        highlighted_officers_fixture[:2],
                    ),
                    create_fixture(
                        metric_fixtures[ABSCONSIONS_BENCH_WARRANTS],
                        target_fixture,
                        other_officers_fixture,
                        highlighted_officers_fixture,
                    ),
                    create_fixture(
                        metric_fixtures[EARLY_DISCHARGE_REQUESTS],
                        target_fixture,
                        other_officers_fixture,
                        highlighted_officers_fixture[:1],
                        TargetStatusStrategy.ZERO_RATE,
                    ),
                ],
                metrics_without_outliers=[],
                recipient_email_address=self.test_email,
            )
        )
        self.assertEqual(
            actual["highlights"].multiple_metrics,
            [
                MultipleMetricHighlight(
                    name=highlighted_officers_fixture[0].name,
                    details=[
                        MetricHighlightDetail(
                            condition="is far from the state average on",
                            metrics="technical incarceration rate and absconsion rate",
                        ),
                        MetricHighlightDetail(
                            condition="has zero",
                            metrics="earned discharge requests",
                        ),
                    ],
                ),
                MultipleMetricHighlight(
                    name=highlighted_officers_fixture[1].name,
                    details=[
                        MetricHighlightDetail(
                            condition="is far from the state average on",
                            metrics="technical incarceration rate and absconsion rate",
                        ),
                    ],
                ),
            ],
        )

    def test_highlight_no_outliers(self) -> None:
        actual = self._get_prepared_data(
            OfficerSupervisorReportData(
                metrics=[],
                metrics_without_outliers=[],
                recipient_email_address=self.test_email,
            )
        )
        self.assertIsNone(actual["highlights"].no_outliers)

        actual = self._get_prepared_data(
            OfficerSupervisorReportData(
                metrics=[],
                metrics_without_outliers=[metric_fixtures[ABSCONSIONS_BENCH_WARRANTS]],
                recipient_email_address=self.test_email,
            )
        )
        self.assertEqual(actual["highlights"].no_outliers, "absconsion rate")

        actual = self._get_prepared_data(
            OfficerSupervisorReportData(
                metrics=[],
                metrics_without_outliers=[
                    metric_fixtures[ABSCONSIONS_BENCH_WARRANTS],
                    metric_fixtures[EARLY_DISCHARGE_REQUESTS],
                ],
                recipient_email_address=self.test_email,
            )
        )
        self.assertEqual(
            actual["highlights"].no_outliers,
            "absconsion rate or earned discharge request rate",
        )

        actual = self._get_prepared_data(
            OfficerSupervisorReportData(
                metrics=[],
                metrics_without_outliers=[
                    metric_fixtures[INCARCERATION_STARTS_TECHNICAL_VIOLATION],
                    metric_fixtures[ABSCONSIONS_BENCH_WARRANTS],
                    metric_fixtures[EARLY_DISCHARGE_REQUESTS],
                ],
                recipient_email_address=self.test_email,
            )
        )
        self.assertEqual(
            actual["highlights"].no_outliers,
            "technical incarceration rate, absconsion rate, or earned discharge request rate",
        )
