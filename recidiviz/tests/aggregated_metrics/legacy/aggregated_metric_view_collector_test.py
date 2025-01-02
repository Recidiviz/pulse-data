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

from recidiviz.aggregated_metrics.legacy.aggregated_metric_view_collector import (
    collect_legacy_aggregated_metrics_view_builders,
)
from recidiviz.aggregated_metrics.models.aggregated_metric_configurations import (
    ASSIGNMENTS,
    DAYS_AT_LIBERTY_365,
    LSIR_ASSESSMENTS,
    LSIR_ASSESSMENTS_365,
    LSIR_ASSESSMENTS_AVG_SCORE,
)
from recidiviz.aggregated_metrics.models.metric_population_type import (
    MetricPopulationType,
)
from recidiviz.aggregated_metrics.models.metric_unit_of_analysis_type import (
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
            collect_legacy_aggregated_metrics_view_builders(
                metrics_by_population_dict={
                    MetricPopulationType.JUSTICE_INVOLVED: [
                        DAYS_AT_LIBERTY_365,
                    ]
                },
                units_of_analysis_by_population_dict={
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
        collect_legacy_aggregated_metrics_view_builders(
            metrics_by_population_dict={
                MetricPopulationType.JUSTICE_INVOLVED: [
                    ASSIGNMENTS,
                    DAYS_AT_LIBERTY_365,
                ]
            },
            units_of_analysis_by_population_dict={
                MetricPopulationType.JUSTICE_INVOLVED: [
                    MetricUnitOfAnalysisType.STATE_CODE,
                ]
            },
        )

        # assignment event metric with ASSIGNMENTS
        collect_legacy_aggregated_metrics_view_builders(
            metrics_by_population_dict={
                MetricPopulationType.JUSTICE_INVOLVED: [
                    ASSIGNMENTS,
                    LSIR_ASSESSMENTS_365,
                ]
            },
            units_of_analysis_by_population_dict={
                MetricPopulationType.JUSTICE_INVOLVED: [
                    MetricUnitOfAnalysisType.STATE_CODE,
                ]
            },
        )

    def test_no_event_count_metric_with_event_value_metric(self) -> None:
        """
        Test verifies that an error is thrown when a configured EventValueMetric
        has no associated EventCountMetric configured for the same population type.
        """
        with self.assertRaises(ValueError):
            collect_legacy_aggregated_metrics_view_builders(
                metrics_by_population_dict={
                    MetricPopulationType.JUSTICE_INVOLVED: [
                        LSIR_ASSESSMENTS_AVG_SCORE,
                    ]
                },
                units_of_analysis_by_population_dict={
                    MetricPopulationType.JUSTICE_INVOLVED: [
                        MetricUnitOfAnalysisType.STATE_CODE,
                    ]
                },
            )

    def test_event_count_metric_with_event_value_metric(self) -> None:
        """
        Test verifies that no error is thrown when a configured EventValueMetric
        has an associated EventCountMetric configured for the same population type.
        """
        collect_legacy_aggregated_metrics_view_builders(
            metrics_by_population_dict={
                MetricPopulationType.JUSTICE_INVOLVED: [
                    LSIR_ASSESSMENTS_AVG_SCORE,
                    LSIR_ASSESSMENTS,
                ]
            },
            units_of_analysis_by_population_dict={
                MetricPopulationType.JUSTICE_INVOLVED: [
                    MetricUnitOfAnalysisType.STATE_CODE,
                ]
            },
        )
