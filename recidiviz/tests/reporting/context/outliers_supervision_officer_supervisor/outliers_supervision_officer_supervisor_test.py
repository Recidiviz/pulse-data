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
from typing import Callable, Iterable, Optional
from unittest import TestCase

import freezegun
import responses

from recidiviz.common.constants.states import StateCode
from recidiviz.outliers.constants import (
    ABSCONSIONS_BENCH_WARRANTS,
    EARLY_DISCHARGE_REQUESTS,
    INCARCERATION_STARTS_TECHNICAL_VIOLATION,
)
from recidiviz.outliers.querier.querier import OfficerSupervisorReportData
from recidiviz.outliers.types import OutliersConfig, TargetStatusStrategy
from recidiviz.reporting.context.outliers_supervision_officer_supervisor.context import (
    OutliersSupervisionOfficerSupervisorContext,
)
from recidiviz.reporting.context.outliers_supervision_officer_supervisor.fixtures import (
    create_fixture,
    highlighted_officers_fixture_adverse,
    highlighted_officers_fixture_favorable_zero,
    metric_fixtures,
    other_officers_fixture_adverse,
    other_officers_fixture_favorable_zero,
    target_fixture_adverse,
    target_fixture_favorable_zero,
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
        self.config = OutliersConfig(
            metrics=[
                metric_fixtures[INCARCERATION_STARTS_TECHNICAL_VIOLATION],
                metric_fixtures[ABSCONSIONS_BENCH_WARRANTS],
                metric_fixtures[EARLY_DISCHARGE_REQUESTS],
            ],
            supervision_officer_label="officer",
        )

    def _get_prepared_data(
        self,
        report: OfficerSupervisorReportData,
        # responses unfortunately does not expose this type alias but it's pretty generic
        request_match: Optional[Iterable[Callable]] = None,
    ) -> dict:
        if request_match is None:
            request_match = []

        recipient = Recipient(
            email_address=report.recipient_email_address,
            state_code=self.batch.state_code,
            data={
                "report": report,
                "config": self.config,
            },
        )
        context = OutliersSupervisionOfficerSupervisorContext(self.batch, recipient)
        with responses.RequestsMock() as rsps:
            if report.metrics:
                rsps.post(
                    "http://asset-generation-test/generate/outliers-supervisor-chart",
                    match=request_match,
                    status=200,
                    json={"url": "/mock/image/url", "height": 200},
                )
            prepared_data = context.get_prepared_data()

        return prepared_data

    @freezegun.freeze_time(datetime(2023, 6, 18, 14, 21, 0, 0))
    def test_metric_periods(self) -> None:
        self.assertEqual(
            {
                "current": "Jun &rsquo;22&ndash;Jun &rsquo;23",
                "previous": "May &rsquo;22&ndash;May &rsquo;23",
            },
            self._get_prepared_data(
                OfficerSupervisorReportData(
                    metrics=[],
                    metrics_without_outliers=[],
                    recipient_email_address=self.test_email,
                )
            )["metric_periods"],
        )

    def test_show_section_headings(self) -> None:
        self.assertTrue(
            self._get_prepared_data(
                OfficerSupervisorReportData(
                    metrics=[],
                    metrics_without_outliers=[],
                    recipient_email_address=self.test_email,
                )
            )["show_metric_section_headings"]
        )

        self.config.metrics = self.config.metrics[:1]
        self.assertFalse(
            self._get_prepared_data(
                OfficerSupervisorReportData(
                    metrics=[],
                    metrics_without_outliers=[],
                    recipient_email_address=self.test_email,
                )
            )["show_metric_section_headings"]
        )

    def test_chart_request(self) -> None:
        test_report = OfficerSupervisorReportData(
            metrics=[
                create_fixture(
                    metric_fixtures[INCARCERATION_STARTS_TECHNICAL_VIOLATION],
                    target_fixture_adverse,
                    other_officers_fixture_adverse,
                    highlighted_officers_fixture_adverse,
                )
            ],
            metrics_without_outliers=[],
            recipient_email_address=self.test_email,
        )
        actual = self._get_prepared_data(
            test_report,
            request_match=[
                responses.matchers.json_params_matcher(
                    {
                        "stateCode": "US_XX",
                        "width": 571,
                        "id": "test-outliers-supervisor@recidiviz.org-incarceration_starts_technical_violation",
                        "data": {
                            "target": 0.05428241659992843,
                            "otherOfficers": {
                                "MET": [
                                    0.013664782299427202,
                                    0,
                                    0,
                                    0.01986070301447383,
                                    0.023395936157938592,
                                ],
                                "NEAR": [
                                    0.05557247259439707,
                                    0.06803989188181564,
                                    0.0880180859080633,
                                ],
                                "FAR": [
                                    0.24142872891632675,
                                    0.2114256751864456,
                                    0.10346978115432588,
                                ],
                            },
                            "highlightedOfficers": [
                                {
                                    "name": "Tatiana Alvarez-Thomas",
                                    "rate": 0.19904024430145054,
                                    "targetStatus": "FAR",
                                    "prevRate": 0.15804024430145053,
                                    "supervisorExternalId": "abc123",
                                    "prevTargetStatus": None,
                                    "supervisionDistrict": "1",
                                },
                                {
                                    "name": "Mario McCarthy",
                                    "rate": 0.10228673915480327,
                                    "targetStatus": "FAR",
                                    "prevRate": 0.08228673915480327,
                                    "supervisorExternalId": "abc123",
                                    "prevTargetStatus": None,
                                    "supervisionDistrict": "1",
                                },
                                {
                                    "name": "Ryan Luna",
                                    "rate": 0.129823,
                                    "targetStatus": "FAR",
                                    "prevRate": 0.121354,
                                    "supervisorExternalId": "abc123",
                                    "prevTargetStatus": None,
                                    "supervisionDistrict": "1",
                                },
                            ],
                        },
                    }
                )
            ],
        )

        self.assertEqual(
            actual["adverse_metrics"][0]["chart"]["url"], "/mock/image/url"
        )

    def test_metric_context(self) -> None:
        test_report = OfficerSupervisorReportData(
            metrics=[
                create_fixture(
                    metric_fixtures[INCARCERATION_STARTS_TECHNICAL_VIOLATION],
                    target_fixture_adverse,
                    other_officers_fixture_adverse,
                    highlighted_officers_fixture_adverse,
                ),
            ],
            metrics_without_outliers=[],
            recipient_email_address=self.test_email,
        )

        self.assertEqual(
            self._get_prepared_data(test_report)["adverse_metrics"],
            [
                {
                    "title_display_name": "Technical Incarceration Rate",
                    "body_display_name": "technical incarceration rate",
                    "legend_zero": False,
                    "far_direction": "above",
                    "event_name": "technical incarcerations",
                    "chart": {
                        "url": "/mock/image/url",
                        "alt_text": (
                            "Swarm plot of all technical incarceration rates in the "
                            "state where Tatiana Alvarez-Thomas, Mario McCarthy, and "
                            "Ryan Luna are far from the state average for the current "
                            "reporting period."
                        ),
                    },
                }
            ],
        )

        # some of the fields produce different output based on logic;
        # no need to test the copy pass-through fields again
        test_report.metrics[0].highlighted_officers = test_report.metrics[
            0
        ].highlighted_officers[:1]
        actual = self._get_prepared_data(test_report)["adverse_metrics"][0]
        self.assertRegex(
            actual["chart"]["alt_text"], "where Tatiana Alvarez-Thomas is far"
        )

        test_report.metrics = [
            create_fixture(
                metric_fixtures[EARLY_DISCHARGE_REQUESTS],
                target_fixture_favorable_zero,
                other_officers_fixture_favorable_zero,
                highlighted_officers_fixture_favorable_zero,
                TargetStatusStrategy.ZERO_RATE,
            ),
        ]
        actual = self._get_prepared_data(test_report)["favorable_metrics"][0]
        self.assertTrue(actual["legend_zero"])
        self.assertEqual(actual["far_direction"], "below")
        self.assertRegex(
            actual["chart"]["alt_text"],
            "where Tatiana Alvarez-Thomas and Samuel Dunn have zero earned discharge requests",
        )

        test_report.metrics[0].highlighted_officers = test_report.metrics[
            0
        ].highlighted_officers[1:]
        actual = self._get_prepared_data(test_report)["favorable_metrics"][0]
        self.assertRegex(
            actual["chart"]["alt_text"],
            "where Samuel Dunn has zero",
        )

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
                        target_fixture_adverse,
                        other_officers_fixture_adverse,
                        highlighted_officers_fixture_adverse[:2],
                    ),
                    create_fixture(
                        metric_fixtures[ABSCONSIONS_BENCH_WARRANTS],
                        target_fixture_adverse,
                        other_officers_fixture_adverse,
                        highlighted_officers_fixture_adverse,
                    ),
                    create_fixture(
                        metric_fixtures[EARLY_DISCHARGE_REQUESTS],
                        target_fixture_favorable_zero,
                        other_officers_fixture_favorable_zero,
                        highlighted_officers_fixture_favorable_zero[:1],
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
                    name=highlighted_officers_fixture_adverse[0].name,
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
                    name=highlighted_officers_fixture_adverse[1].name,
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
