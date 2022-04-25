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
"""This class implements tests for Justice Counts ReportMetric class."""

from unittest import TestCase

from recidiviz.justice_counts.dimensions.law_enforcement import SheriffBudgetType
from recidiviz.justice_counts.dimensions.person import Gender
from recidiviz.justice_counts.metrics import law_enforcement
from recidiviz.justice_counts.metrics.constants import ContextKey
from recidiviz.justice_counts.metrics.report_metric import (
    ReportedAggregatedDimension,
    ReportedContext,
    ReportMetric,
)
from recidiviz.tests.justice_counts.utils import JusticeCountsSchemaTestObjects


class TestJusticeCountsReportMetric(TestCase):
    """Implements tests for the Justice Counts ReportMetric class."""

    def setUp(self) -> None:
        self.test_schema_objects = JusticeCountsSchemaTestObjects()
        self.reported_budget = self.test_schema_objects.reported_budget_metric
        self.reported_calls_for_service = (
            self.test_schema_objects.reported_calls_for_service_metric
        )

    def test_init(self) -> None:
        self.assertEqual(
            self.reported_budget.metric_definition.display_name, "Annual Budget"
        )

    def test_value_validation(self) -> None:
        with self.assertRaisesRegex(
            ValueError, "Not all dimension instances belong to the same class"
        ):
            ReportMetric(
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
            ReportMetric(
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
            ReportMetric(
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
            "The required context ContextKey.ALL_CALLS_OR_CALLS_RESPONDED is missing",
        ):
            ReportMetric(
                key=law_enforcement.calls_for_service.key,
                value=100000,
                contexts=[],
                aggregated_dimensions=[],
                enforce_required_fields=True,
            )

        with self.assertRaisesRegex(
            ValueError,
            r"The context ContextKey.ALL_CALLS_OR_CALLS_RESPONDED is reported as a <class 'str'> but typed as a \(<class 'bool'>,\).",
        ):
            ReportMetric(
                key=law_enforcement.calls_for_service.key,
                value=100000,
                contexts=[
                    ReportedContext(
                        key=ContextKey.ALL_CALLS_OR_CALLS_RESPONDED, value="all calls"
                    )
                ],
                aggregated_dimensions=[],
            )

    def test_aggregated_dimensions_validation(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "The following required dimensions are missing: {'metric/law_enforcement/calls_for_service/type'}",
        ):
            ReportMetric(
                key=law_enforcement.calls_for_service.key,
                value=100000,
                contexts=[
                    ReportedContext(
                        key=ContextKey.ALL_CALLS_OR_CALLS_RESPONDED, value=True
                    )
                ],
                aggregated_dimensions=[],
                enforce_required_fields=True,
            )
