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

import pytz

from recidiviz.aggregated_metrics.legacy.custom_aggregated_metrics_template import (
    _get_time_period_cte,
    get_legacy_custom_aggregated_metrics_query_template,
)
from recidiviz.aggregated_metrics.metric_time_period_config import MetricTimePeriod
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


class TestAggregatedMetricsUtils(unittest.TestCase):
    """Tests for custom aggregated metrics generation functions"""

    # custom time periods
    def test_get_time_period_cte_custom(self) -> None:
        my_time_periods_cte = _get_time_period_cte(
            interval_unit=MetricTimePeriod.WEEK,
            interval_length=19,
            min_end_date=datetime(2020, 1, 4, tzinfo=pytz.timezone("US/Eastern")),
            max_end_date=datetime(2022, 8, 3, tzinfo=pytz.timezone("US/Eastern")),
            rolling_period_unit=MetricTimePeriod.WEEK,
            rolling_period_length=19,
        )
        expected_time_periods_cte = """
SELECT
    metric_period_start_date AS population_start_date,
    metric_period_end_date_exclusive AS population_end_date,
    "CUSTOM" AS period
FROM (
    SELECT
        DATE_SUB(
            metric_period_end_date_exclusive, INTERVAL 19 WEEK
        ) AS metric_period_start_date,
        metric_period_end_date_exclusive,
        "CUSTOM" as period,
    FROM
        UNNEST(GENERATE_DATE_ARRAY(
            "2020-01-04",
            "2022-08-03",
            INTERVAL 19 WEEK
        )) AS metric_period_end_date_exclusive
)
"""
        self.assertEqual(expected_time_periods_cte, my_time_periods_cte)

    # tests time periods creation when no max date is provided
    def test_get_time_period_cte_no_max_date(self) -> None:
        my_time_periods_cte = _get_time_period_cte(
            interval_unit=MetricTimePeriod.QUARTER,
            interval_length=1,
            min_end_date=datetime(2020, 1, 4, tzinfo=pytz.timezone("US/Eastern")),
            max_end_date=None,
            rolling_period_unit=MetricTimePeriod.QUARTER,
            rolling_period_length=1,
        )
        expected_time_periods_cte = """
SELECT
    metric_period_start_date AS population_start_date,
    metric_period_end_date_exclusive AS population_end_date,
    "CUSTOM" AS period
FROM (
    SELECT
        DATE_SUB(
            metric_period_end_date_exclusive, INTERVAL 1 QUARTER
        ) AS metric_period_start_date,
        metric_period_end_date_exclusive,
        "CUSTOM" as period,
    FROM
        UNNEST(GENERATE_DATE_ARRAY(
            "2020-01-04",
            CURRENT_DATE("US/Eastern"),
            INTERVAL 1 QUARTER
        )) AS metric_period_end_date_exclusive
)
"""
        self.assertEqual(expected_time_periods_cte, my_time_periods_cte)

    # tests time periods creation when rolling periods are different than interval periods
    def test_get_time_period_monthly_metrics_yearly_intervals(self) -> None:
        my_time_periods_cte = _get_time_period_cte(
            interval_unit=MetricTimePeriod.MONTH,
            interval_length=1,
            min_end_date=datetime(2020, 1, 4, tzinfo=pytz.timezone("US/Eastern")),
            max_end_date=None,
            rolling_period_unit=MetricTimePeriod.YEAR,
            rolling_period_length=1,
        )
        expected_time_periods_cte = """
SELECT
    metric_period_start_date AS population_start_date,
    metric_period_end_date_exclusive AS population_end_date,
    "CUSTOM" AS period
FROM (
    SELECT
        DATE_SUB(
            metric_period_end_date_exclusive, INTERVAL 1 MONTH
        ) AS metric_period_start_date,
        metric_period_end_date_exclusive,
        "CUSTOM" as period,
    FROM
        UNNEST(GENERATE_DATE_ARRAY(
            "2020-01-04",
            CURRENT_DATE("US/Eastern"),
            INTERVAL 1 YEAR
        )) AS metric_period_end_date_exclusive
)
"""
        self.assertEqual(expected_time_periods_cte, my_time_periods_cte)

    def test_get_time_period_call_without_rolling_params(self) -> None:
        my_time_periods_cte = _get_time_period_cte(
            interval_unit=MetricTimePeriod.MONTH,
            interval_length=1,
            min_end_date=datetime(2020, 1, 4, tzinfo=pytz.timezone("US/Eastern")),
            max_end_date=None,
        )
        expected_time_periods_cte = """
SELECT
    metric_period_start_date AS population_start_date,
    metric_period_end_date_exclusive AS population_end_date,
    "CUSTOM" AS period
FROM (
    SELECT
        DATE_SUB(
            metric_period_end_date_exclusive, INTERVAL 1 MONTH
        ) AS metric_period_start_date,
        metric_period_end_date_exclusive,
        "CUSTOM" as period,
    FROM
        UNNEST(GENERATE_DATE_ARRAY(
            "2020-01-04",
            CURRENT_DATE("US/Eastern"),
            INTERVAL 1 MONTH
        )) AS metric_period_end_date_exclusive
)
"""
        self.assertEqual(expected_time_periods_cte, my_time_periods_cte)

    # TODO(#26436): Add more rigorous unit testing for query output
    def test_get_custom_aggregated_metrics_query_template(self) -> None:
        # Test passes if this doesn't crash
        _ = get_legacy_custom_aggregated_metrics_query_template(
            metrics=[
                CONTACTS_ATTEMPTED,
                DAYS_EMPLOYED_365,
            ],
            unit_of_analysis_type=MetricUnitOfAnalysisType.STATE_CODE,
            population_type=MetricPopulationType.INCARCERATION,
            time_interval_unit=MetricTimePeriod.WEEK,
            time_interval_length=2,
            rolling_period_unit=MetricTimePeriod.WEEK,
            rolling_period_length=2,
            min_end_date=datetime(2023, 1, 1, tzinfo=pytz.timezone("US/Eastern")),
            max_end_date=datetime(2023, 5, 1, tzinfo=pytz.timezone("US/Eastern")),
        )
        _ = get_legacy_custom_aggregated_metrics_query_template(
            metrics=[
                CONTACTS_ATTEMPTED,
                DAYS_EMPLOYED_365,
            ],
            unit_of_analysis_type=MetricUnitOfAnalysisType.STATE_CODE,
            population_type=MetricPopulationType.INCARCERATION,
            time_interval_unit=MetricTimePeriod.WEEK,
            time_interval_length=2,
            min_end_date=datetime(2023, 1, 1, tzinfo=pytz.timezone("US/Eastern")),
            max_end_date=datetime(2023, 5, 1, tzinfo=pytz.timezone("US/Eastern")),
        )
