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
"""Tests for custom aggregated metrics generation functions"""

import unittest
from datetime import datetime

from recidiviz.aggregated_metrics.metric_time_periods import MetricTimePeriod
from recidiviz.aggregated_metrics.models.aggregated_metric_configurations import (
    CONTACTS_ATTEMPTED,
    DAYS_EMPLOYED_365,
)
from recidiviz.calculator.query.state.views.analyst_data.models.metric_population_type import (
    MetricPopulationType,
)
from recidiviz.calculator.query.state.views.analyst_data.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysisType,
)
from recidiviz.tools.analyst.aggregated_metrics_utils import (
    get_custom_aggregated_metrics_query_template,
    get_time_period_cte,
)


class TestAggregatedMetricsUtils(unittest.TestCase):
    """Tests for custom aggregated metrics generation functions"""

    # custom time periods
    def test_get_time_period_cte_custom(self) -> None:
        my_time_periods_cte = get_time_period_cte(
            interval_unit=MetricTimePeriod.WEEK,
            interval_length=19,
            min_date=datetime(2020, 1, 4),
            max_date=datetime(2022, 8, 3),
        )
        expected_time_periods_cte = """
SELECT
    population_start_date,
    DATE_ADD(population_start_date, INTERVAL 19 WEEK) AS population_end_date,
    "CUSTOM" as period,
FROM
    UNNEST(GENERATE_DATE_ARRAY(
        "2020-01-04",
        "2022-08-03",
        INTERVAL 19 WEEK
    )) AS population_start_date
WHERE
    DATE_ADD(population_start_date, INTERVAL 19 WEEK) <= CURRENT_DATE("US/Eastern")
    AND DATE_ADD(population_start_date, INTERVAL 19 WEEK) <= "2022-08-03"
"""
        self.assertEqual(my_time_periods_cte, expected_time_periods_cte)

    # tests detection of a standard time period
    def test_get_time_period_cte_regular_period(self) -> None:
        my_time_periods_cte = get_time_period_cte(
            interval_unit=MetricTimePeriod.QUARTER,
            interval_length=1,
            min_date=datetime(2020, 1, 4),
            max_date=datetime(2022, 8, 3),
        )
        expected_time_periods_cte = """
SELECT
    population_start_date,
    DATE_ADD(population_start_date, INTERVAL 1 QUARTER) AS population_end_date,
    "QUARTER" as period,
FROM
    UNNEST(GENERATE_DATE_ARRAY(
        "2020-01-04",
        "2022-08-03",
        INTERVAL 1 QUARTER
    )) AS population_start_date
WHERE
    DATE_ADD(population_start_date, INTERVAL 1 QUARTER) <= CURRENT_DATE("US/Eastern")
    AND DATE_ADD(population_start_date, INTERVAL 1 QUARTER) <= "2022-08-03"
"""
        self.assertEqual(my_time_periods_cte, expected_time_periods_cte)

    # tests time periods creation when no max date is provided
    def test_get_time_period_cte_no_max_date(self) -> None:
        my_time_periods_cte = get_time_period_cte(
            interval_unit=MetricTimePeriod.QUARTER,
            interval_length=1,
            min_date=datetime(2020, 1, 4),
            max_date=None,
        )
        expected_time_periods_cte = """
SELECT
    population_start_date,
    DATE_ADD(population_start_date, INTERVAL 1 QUARTER) AS population_end_date,
    "QUARTER" as period,
FROM
    UNNEST(GENERATE_DATE_ARRAY(
        "2020-01-04",
        CURRENT_DATE("US/Eastern"),
        INTERVAL 1 QUARTER
    )) AS population_start_date
WHERE
    DATE_ADD(population_start_date, INTERVAL 1 QUARTER) <= CURRENT_DATE("US/Eastern")
    AND DATE_ADD(population_start_date, INTERVAL 1 QUARTER) <= CURRENT_DATE("US/Eastern")
"""
        self.assertEqual(my_time_periods_cte, expected_time_periods_cte)

    # TODO(#26436): Add more rigorous unit testing for query output
    def test_get_custom_aggregated_metrics_query_template(self) -> None:
        # Test passes if this doesn't crash
        _ = get_custom_aggregated_metrics_query_template(
            metrics=[
                CONTACTS_ATTEMPTED,
                DAYS_EMPLOYED_365,
            ],
            unit_of_analysis_type=MetricUnitOfAnalysisType.STATE_CODE,
            population_type=MetricPopulationType.INCARCERATION,
            time_interval_unit=MetricTimePeriod.WEEK,
            time_interval_length=2,
            min_date=datetime(2023, 1, 1),
            max_date=datetime(2023, 5, 1),
        )
