# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Tests for aggregated_metric_query_utils.py"""
import unittest

from recidiviz.aggregated_metrics.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysisType,
)
from recidiviz.aggregated_metrics.query_building.aggregated_metric_query_utils import (
    metric_group_by_columns,
)


class TestAggregatedMetriQueryUtils(unittest.TestCase):
    """Tests for aggregated_metric_query_utils.py"""

    def test_metric_group_by_columns(self) -> None:
        self.assertEqual(
            [
                "state_code",
                "facility",
                "metric_period_start_date",
                "metric_period_end_date_exclusive",
                "period",
            ],
            metric_group_by_columns(MetricUnitOfAnalysisType.FACILITY),
        )

        self.assertEqual(
            [
                "state_code",
                "metric_period_start_date",
                "metric_period_end_date_exclusive",
                "period",
            ],
            metric_group_by_columns(MetricUnitOfAnalysisType.STATE_CODE),
        )

        self.assertEqual(
            [
                "state_code",
                "person_id",
                "metric_period_start_date",
                "metric_period_end_date_exclusive",
                "period",
            ],
            metric_group_by_columns(MetricUnitOfAnalysisType.PERSON_ID),
        )
