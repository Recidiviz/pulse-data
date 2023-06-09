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
"""Tests for the aggregated metric view collector"""

import unittest

from recidiviz.aggregated_metrics.aggregated_metric_view_collector import (
    collect_aggregated_metrics_view_builders,
)
from recidiviz.aggregated_metrics.models.aggregated_metric_configurations import (
    ASSIGNMENTS,
    DAYS_AT_LIBERTY_365,
    LSIR_ASSESSMENTS_365,
)
from recidiviz.calculator.query.state.views.analyst_data.models.metric_population_type import (
    MetricPopulationType,
)
from recidiviz.calculator.query.state.views.analyst_data.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysisType,
)


class CollectAggregatedMetricsViewBuilders(unittest.TestCase):
    """
    Tests for the collect_aggregated_metrics_view_builders function
    """

    def test_no_assignments_with_assignment_metric(self) -> None:
        """
        This test verifies that an error is thrown if an event/span assignment metric is
        included without ASSIGNMENTS.
        """
        # assignment span metric without ASSIGNMENTS
        with self.assertRaises(ValueError):
            collect_aggregated_metrics_view_builders(
                metrics_by_population_dict={
                    MetricPopulationType.JUSTICE_INVOLVED: [
                        DAYS_AT_LIBERTY_365,
                    ]
                },
                levels_by_population_dict={
                    MetricPopulationType.JUSTICE_INVOLVED: [
                        MetricUnitOfAnalysisType.STATE_CODE,
                    ]
                },
            )

        # assignment event metric without ASSIGNMENTS
        with self.assertRaises(ValueError):
            collect_aggregated_metrics_view_builders(
                metrics_by_population_dict={
                    MetricPopulationType.JUSTICE_INVOLVED: [
                        LSIR_ASSESSMENTS_365,
                    ]
                },
                levels_by_population_dict={
                    MetricPopulationType.JUSTICE_INVOLVED: [
                        MetricUnitOfAnalysisType.STATE_CODE,
                    ]
                },
            )

    def test_assignments_with_assignment_metric(self) -> None:
        """
        This test verifies that no error is thrown if an event/span assignment metric is
        included with ASSIGNMENTS.
        """
        # assignment span metric with ASSIGNMENTS
        collect_aggregated_metrics_view_builders(
            metrics_by_population_dict={
                MetricPopulationType.JUSTICE_INVOLVED: [
                    ASSIGNMENTS,
                    DAYS_AT_LIBERTY_365,
                ]
            },
            levels_by_population_dict={
                MetricPopulationType.JUSTICE_INVOLVED: [
                    MetricUnitOfAnalysisType.STATE_CODE,
                ]
            },
        )

        # assignment event metric with ASSIGNMENTS
        collect_aggregated_metrics_view_builders(
            metrics_by_population_dict={
                MetricPopulationType.JUSTICE_INVOLVED: [
                    ASSIGNMENTS,
                    LSIR_ASSESSMENTS_365,
                ]
            },
            levels_by_population_dict={
                MetricPopulationType.JUSTICE_INVOLVED: [
                    MetricUnitOfAnalysisType.STATE_CODE,
                ]
            },
        )
