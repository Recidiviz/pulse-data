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
"""Tests functionality of aggregated metrics lookml generation function"""

import unittest

from recidiviz.aggregated_metrics.aggregated_metric_view_collector import (
    METRICS_BY_POPULATION_TYPE,
)
from recidiviz.aggregated_metrics.models.metric_aggregation_level_type import (
    METRIC_AGGREGATION_LEVELS_BY_TYPE,
    MetricAggregationLevelType,
)
from recidiviz.aggregated_metrics.models.metric_population_type import (
    METRIC_POPULATIONS_BY_TYPE,
    MetricPopulationType,
)
from recidiviz.tools.looker.aggregated_metrics_lookml_generator import (
    get_lookml_view_for_metrics,
)


class LookMLViewTest(unittest.TestCase):
    """Tests function for lookml generation"""

    def test_get_lookml_view_for_metrics(self) -> None:
        # Test passes if this doesn't crash
        _ = get_lookml_view_for_metrics(
            population=METRIC_POPULATIONS_BY_TYPE[MetricPopulationType.SUPERVISION],
            aggregation_level=METRIC_AGGREGATION_LEVELS_BY_TYPE[
                MetricAggregationLevelType.SUPERVISION_DISTRICT
            ],
            metrics=METRICS_BY_POPULATION_TYPE[MetricPopulationType.SUPERVISION],
        )
