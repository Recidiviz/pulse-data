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
"""Tests for all_aggregated_metrics_view_builder.py"""

import re
import unittest

from recidiviz.aggregated_metrics.all_aggregated_metrics_view_builder import (
    generate_all_aggregated_metrics_view_builder,
)
from recidiviz.aggregated_metrics.models.metric_population_type import (
    MetricPopulationType,
)
from recidiviz.aggregated_metrics.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysis,
    MetricUnitOfAnalysisType,
)
from recidiviz.tests.aggregated_metrics.fixture_aggregated_metrics import (
    MY_LOGINS_BY_PRIMARY_WORKFLOWS,
)


class GenerateAggregatedAllMetricsViewBuilderTest(unittest.TestCase):
    """Tests for all_aggregated_metrics_view_builder()"""

    # verify that generate_aggregated_metrics_view_builder called with no metrics
    # raises an error
    def test_no_metrics(self) -> None:
        with self.assertRaisesRegex(
            ValueError, "No metrics found for justice_involved metrics"
        ):
            generate_all_aggregated_metrics_view_builder(
                unit_of_analysis=MetricUnitOfAnalysis.for_type(
                    MetricUnitOfAnalysisType.STATE_CODE
                ),
                population_type=MetricPopulationType.JUSTICE_INVOLVED,
                metrics=[],  # no metrics
                dataset_id_override=None,
                collection_tag=None,
                disaggregate_by_observation_attributes=None,
            )

    def test_with_disaggregation_attributes(self) -> None:
        builder = generate_all_aggregated_metrics_view_builder(
            unit_of_analysis=MetricUnitOfAnalysis.for_type(
                MetricUnitOfAnalysisType.WORKFLOWS_PROVISIONED_USER
            ),
            population_type=MetricPopulationType.JUSTICE_INVOLVED,
            metrics=[
                MY_LOGINS_BY_PRIMARY_WORKFLOWS,
            ],
            dataset_id_override=None,
            collection_tag=None,
            disaggregate_by_observation_attributes=[
                "task_type",
            ],
        )

        select_statement = re.search(
            r"SELECT\s+(.*?)\s+FROM",
            builder.view_query_template,
            re.IGNORECASE | re.DOTALL,
        )
        if not select_statement:
            self.fail("Could not find SELECT statement in view query template")
        else:
            selected_columns_str = select_statement.group(1)
            self.assertIn("task_type", selected_columns_str)
