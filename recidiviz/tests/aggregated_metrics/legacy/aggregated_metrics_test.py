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
"""Tests for aggregated_metrics.py"""

import unittest

from recidiviz.aggregated_metrics.legacy.aggregated_metrics import (
    generate_aggregated_metrics_view_builder,
)
from recidiviz.calculator.query.state.views.analyst_data.models.metric_population_type import (
    MetricPopulationType,
)
from recidiviz.calculator.query.state.views.analyst_data.models.metric_unit_of_analysis_type import (
    METRIC_UNITS_OF_ANALYSIS_BY_TYPE,
    MetricUnitOfAnalysisType,
)


class GenerateAggregatedMetricsViewBuilderTest(unittest.TestCase):
    # verify that generate_aggregated_metrics_view_builder called with no metrics
    # raises an error
    def test_no_metrics(self) -> None:
        with self.assertRaises(ValueError):
            generate_aggregated_metrics_view_builder(
                unit_of_analysis=METRIC_UNITS_OF_ANALYSIS_BY_TYPE[
                    MetricUnitOfAnalysisType.STATE_CODE
                ],
                population_type=MetricPopulationType.JUSTICE_INVOLVED,
                metrics=[],  # no metrics
            )
