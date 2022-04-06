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
"""This class implements tests for Justice Counts ReportedMetric class."""

from unittest import TestCase

from recidiviz.justice_counts.dimensions.law_enforcement import (
    CallType,
    SheriffBudgetType,
)
from recidiviz.justice_counts.dimensions.person import Gender
from recidiviz.justice_counts.metrics import law_enforcement
from recidiviz.justice_counts.metrics.constants import ContextKey
from recidiviz.justice_counts.metrics.reported_metric import (
    ReportedAggregatedDimension,
    ReportedContext,
    ReportedMetric,
)


class TestJusticeCountsMetricDefinition(TestCase):
    """Implements tests for the Justice Counts ReportedMetric class."""

    def setUp(self) -> None:
        self.reported_budget = ReportedMetric(
            key=law_enforcement.annual_budget.key,
            value=100000,
            contexts=[
                ReportedContext(
                    key=ContextKey.PRIMARY_FUNDING_SOURCE, value="government"
                )
            ],
            aggregated_dimensions=[
                ReportedAggregatedDimension(
                    dimension_to_value={
                        SheriffBudgetType.DETENTION: 50000,
                        SheriffBudgetType.PATROL: 50000,
                    }
                )
            ],
        )
        self.reported_calls_for_service = ReportedMetric(
            key=law_enforcement.calls_for_service.key,
            value=100,
            contexts=[
                ReportedContext(
                    key=ContextKey.ALL_CALLS_OR_CALLS_RESPONDED, value="all calls"
                )
            ],
            aggregated_dimensions=[
                ReportedAggregatedDimension(
                    dimension_to_value={
                        CallType.EMERGENCY: 20,
                        CallType.NON_EMERGENCY: 60,
                        CallType.UNKNOWN: 20,
                    }
                )
            ],
        )

    def test_init(self) -> None:
        self.assertEqual(
            self.reported_budget.metric_definition.display_name, "Annual Budget"
        )

    def test_value_validation(self) -> None:
        with self.assertRaisesRegex(
            ValueError, "Not all dimension instances belong to the same class"
        ):
            ReportedMetric(
                key=law_enforcement.annual_budget.key,
                value=100000,
                aggregated_dimensions=[
                    ReportedAggregatedDimension(
                        dimension_to_value={
                            SheriffBudgetType.DETENTION: 50000,
                            Gender.get("FEMALE"): 100,
                        }
                    )
                ],
            )

        with self.assertRaisesRegex(
            ValueError,
            "Not all members of the dimension enum have a reported value",
        ):
            ReportedMetric(
                key=law_enforcement.annual_budget.key,
                value=100000,
                aggregated_dimensions=[
                    ReportedAggregatedDimension(
                        dimension_to_value={
                            SheriffBudgetType.DETENTION: 50000,
                        }
                    )
                ],
            )

        with self.assertRaisesRegex(
            ValueError,
            "Sums across dimension metric/law_enforcement/budget/type do not equal the total",
        ):
            ReportedMetric(
                key=law_enforcement.annual_budget.key,
                value=100000,
                aggregated_dimensions=[
                    ReportedAggregatedDimension(
                        dimension_to_value={
                            SheriffBudgetType.DETENTION: 50000,
                            SheriffBudgetType.PATROL: 10,
                        }
                    )
                ],
            )

    def test_context_validation(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "The following required contexts are missing: {<ContextKey.ALL_CALLS_OR_CALLS_RESPONDED",
        ):
            ReportedMetric(
                key=law_enforcement.calls_for_service.key,
                value=100000,
                contexts=[],
                aggregated_dimensions=[],
            )

    def test_aggregated_dimensions_validation(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "The following required dimensions are missing: {'metric/law_enforcement/calls_for_service/type'}",
        ):
            ReportedMetric(
                key=law_enforcement.calls_for_service.key,
                value=100000,
                contexts=[
                    ReportedContext(
                        key=ContextKey.ALL_CALLS_OR_CALLS_RESPONDED, value="all calls"
                    )
                ],
                aggregated_dimensions=[],
            )
