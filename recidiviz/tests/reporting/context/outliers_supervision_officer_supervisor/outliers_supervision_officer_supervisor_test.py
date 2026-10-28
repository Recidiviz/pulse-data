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
from datetime import datetime
from unittest import TestCase

from attrs import evolve

from recidiviz.common.constants.states import StateCode
from recidiviz.outliers.constants import (
    ABSCONSIONS_BENCH_WARRANTS,
    EARLY_DISCHARGE_REQUESTS,
    INCARCERATION_STARTS_TECHNICAL_VIOLATION,
)
from recidiviz.outliers.querier.querier import OfficerSupervisorReportData
from recidiviz.outliers.types import OutliersProductConfiguration, TargetStatusStrategy
from recidiviz.reporting.constants import Batch, ReportType
from recidiviz.reporting.context.outliers_supervision_officer_supervisor.context import (
    OutliersSupervisionOfficerSupervisorContext,
)
from recidiviz.reporting.context.outliers_supervision_officer_supervisor.fixtures import (
    create_fixture,
    get_metric_fixtures_for_state,
    highlighted_officers_fixture_adverse,
    highlighted_officers_fixture_favorable_zero,
    other_officers_fixture_adverse,
    other_officers_fixture_favorable_zero,
    target_fixture_adverse,
    target_fixture_favorable_zero,
)
from recidiviz.reporting.context.outliers_supervision_officer_supervisor.types import (
    MetricHighlightDetail,
    MultipleMetricHighlight,
)
from recidiviz.reporting.recipient import Recipient


class OutliersSupervisionOfficerSupervisorTest(TestCase):
    """Tests for email content preparation logic"""

    def setUp(self) -> None:
        self.test_email = "test-outliers-supervisor@recidiviz.org"
        self.metric_fixtures = get_metric_fixtures_for_state(StateCode.US_XX)
        self.batch = Batch(
            state_code=StateCode.US_XX,
            batch_id="20230614123033",
            report_type=ReportType.OutliersSupervisionOfficerSupervisor,
        )
        self.config = OutliersProductConfiguration(
            updated_at=datetime(2024, 1, 1),
            updated_by="alexa@recidiviz.org",
            feature_variant=None,
            supervision_district_label="district",
            supervision_district_manager_label="district manager",
            supervision_jii_label="client",
            supervisor_has_no_outlier_officers_label="Nice! No officers are outliers on any metrics this month.",
            officer_has_no_outlier_metrics_label="Nice! No outlying metrics this month.",
            supervisor_has_no_officers_with_eligible_clients_label="Nice! No outstanding opportunities for now.",
            officer_has_no_eligible_clients_label="Nice! No outstanding opportunities for now.",
            supervision_unit_label="unit",
            supervision_supervisor_label="supervisor",
            metrics=[
                self.metric_fixtures[INCARCERATION_STARTS_TECHNICAL_VIOLATION],
                self.metric_fixtures[ABSCONSIONS_BENCH_WARRANTS],
                self.metric_fixtures[EARLY_DISCHARGE_REQUESTS],
            ],
            supervision_officer_label="officer",
            learn_more_url="https://recidiviz.org",
        )

    def _get_prepared_data(
        self,
        report: OfficerSupervisorReportData,
    ) -> dict:
        """Returns the prepared email data for the given report."""
        recipient = Recipient(
            email_address=report.recipient_email_address,
            state_code=self.batch.state_code,
            data={
                "report": report,
                "config": self.config,
                "review_month": 6,
                "review_year": 2023,
            },
        )
        context = OutliersSupervisionOfficerSupervisorContext(self.batch, recipient)
        return context.get_prepared_data()

    def test_show_section_headings(self) -> None:
        self.assertTrue(
            self._get_prepared_data(
                OfficerSupervisorReportData(
                    metrics=[],
                    metrics_without_outliers=[],
                    recipient_email_address=self.test_email,
                    additional_recipients=[],
                )
            )["show_metric_section_headings"]
        )

        self.config = evolve(self.config, metrics=self.config.metrics[:1])
        self.assertFalse(
            self._get_prepared_data(
                OfficerSupervisorReportData(
                    metrics=[],
                    metrics_without_outliers=[],
                    recipient_email_address=self.test_email,
                    additional_recipients=[],
                )
            )["show_metric_section_headings"]
        )

    def test_metric_context(self) -> None:
        test_report = OfficerSupervisorReportData(
            metrics=[
                create_fixture(
                    self.metric_fixtures[INCARCERATION_STARTS_TECHNICAL_VIOLATION],
                    target_fixture_adverse,
                    other_officers_fixture_adverse,
                    highlighted_officers_fixture_adverse,
                ),
            ],
            metrics_without_outliers=[],
            recipient_email_address=self.test_email,
            additional_recipients=[],
        )

        self.assertEqual(
            self._get_prepared_data(test_report)["adverse_metrics"],
            [
                {
                    "title_display_name": "Technical Incarceration Rate",
                    "body_display_name": "technical incarceration rate",
                }
            ],
        )

    def test_highlight_multiple_metrics(self) -> None:
        actual = self._get_prepared_data(
            OfficerSupervisorReportData(
                metrics=[],
                metrics_without_outliers=[],
                recipient_email_address=self.test_email,
                additional_recipients=[],
            )
        )
        self.assertIsNone(actual["highlights"].multiple_metrics)

        actual = self._get_prepared_data(
            OfficerSupervisorReportData(
                metrics=[
                    create_fixture(
                        self.metric_fixtures[INCARCERATION_STARTS_TECHNICAL_VIOLATION],
                        target_fixture_adverse,
                        other_officers_fixture_adverse,
                        highlighted_officers_fixture_adverse[:2],
                    ),
                    create_fixture(
                        self.metric_fixtures[ABSCONSIONS_BENCH_WARRANTS],
                        target_fixture_adverse,
                        other_officers_fixture_adverse,
                        highlighted_officers_fixture_adverse,
                    ),
                    create_fixture(
                        self.metric_fixtures[EARLY_DISCHARGE_REQUESTS],
                        target_fixture_favorable_zero,
                        other_officers_fixture_favorable_zero,
                        highlighted_officers_fixture_favorable_zero[:1],
                        TargetStatusStrategy.ZERO_RATE,
                    ),
                ],
                metrics_without_outliers=[],
                recipient_email_address=self.test_email,
                additional_recipients=[],
            )
        )
        self.assertEqual(
            actual["highlights"].multiple_metrics,
            [
                MultipleMetricHighlight(
                    name=highlighted_officers_fixture_adverse[
                        0
                    ].name.formatted_first_last,
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
                    name=highlighted_officers_fixture_adverse[
                        1
                    ].name.formatted_first_last,
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
                additional_recipients=[],
            )
        )
        self.assertIsNone(actual["highlights"].no_outliers)

        actual = self._get_prepared_data(
            OfficerSupervisorReportData(
                metrics=[],
                metrics_without_outliers=[
                    self.metric_fixtures[ABSCONSIONS_BENCH_WARRANTS]
                ],
                recipient_email_address=self.test_email,
                additional_recipients=[],
            )
        )
        self.assertEqual(actual["highlights"].no_outliers, "absconsion rate")

        actual = self._get_prepared_data(
            OfficerSupervisorReportData(
                metrics=[],
                metrics_without_outliers=[
                    self.metric_fixtures[ABSCONSIONS_BENCH_WARRANTS],
                    self.metric_fixtures[EARLY_DISCHARGE_REQUESTS],
                ],
                recipient_email_address=self.test_email,
                additional_recipients=[],
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
                    self.metric_fixtures[INCARCERATION_STARTS_TECHNICAL_VIOLATION],
                    self.metric_fixtures[ABSCONSIONS_BENCH_WARRANTS],
                    self.metric_fixtures[EARLY_DISCHARGE_REQUESTS],
                ],
                recipient_email_address=self.test_email,
                additional_recipients=[],
            )
        )
        self.assertEqual(
            actual["highlights"].no_outliers,
            "technical incarceration rate, absconsion rate, or earned discharge request rate",
        )

    def test_faq(self) -> None:
        """Verify static values are provided to renderer"""
        actual = self._get_prepared_data(
            OfficerSupervisorReportData(
                metrics=[],
                metrics_without_outliers=[],
                recipient_email_address=self.test_email,
                additional_recipients=[],
            )
        )

        self.assertEqual(
            actual["feedback_form_url"], "https://forms.gle/c7WaTaC7wMu9CrsB9"
        )

        for q in actual["faq"]:
            self.assertEqual(q.url, self.config.learn_more_url)

    def test_ix_name(self) -> None:
        """Special case handling for temporary Idaho/ATLAS migration object"""
        self.batch.state_code = StateCode.US_IX
        actual = self._get_prepared_data(
            OfficerSupervisorReportData(
                metrics=[],
                metrics_without_outliers=[],
                recipient_email_address=self.test_email,
                additional_recipients=[],
            )
        )
        self.assertEqual("Idaho", actual["state_name"])
