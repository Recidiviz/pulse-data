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


class GenerateAggregatedAllMetricsViewBuilderTest(unittest.TestCase):
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
            )
