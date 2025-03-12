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

from recidiviz.aggregated_metrics.metric_time_period_config import (
    MetricTimePeriod,
    MetricTimePeriodConfig,
)
from recidiviz.aggregated_metrics.models.aggregated_metric import (
    AssignmentSpanAggregatedMetric,
    PeriodEventAggregatedMetric,
)
from recidiviz.aggregated_metrics.models.aggregated_metric_configurations import (
    CONTACTS_ATTEMPTED,
    DAYS_EMPLOYED_365,
)
from recidiviz.aggregated_metrics.models.metric_population_type import (
    MetricPopulationType,
)
from recidiviz.aggregated_metrics.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysisType,
)
from recidiviz.aggregated_metrics.query_building.build_aggregated_metric_query import (
    build_aggregated_metric_query_template,
)
from recidiviz.aggregated_metrics.query_building.custom_aggregated_metrics_template import (
    get_custom_aggregated_metrics_query_template,
)
from recidiviz.utils.string_formatting import fix_indent


class TestAggregatedMetricsUtils(unittest.TestCase):
    """Tests for custom aggregated metrics generation functions"""

    def test_get_custom_aggregated_metrics_query_template_run(self) -> None:
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
            rolling_period_unit=MetricTimePeriod.WEEK,
            rolling_period_length=2,
            min_end_date=datetime(2023, 1, 1, tzinfo=pytz.timezone("US/Eastern")),
            max_end_date=datetime(2023, 5, 1, tzinfo=pytz.timezone("US/Eastern")),
        )
        _ = get_custom_aggregated_metrics_query_template(
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

    def test_get_custom_aggregated_metrics_query_template(self) -> None:
        actual_query = get_custom_aggregated_metrics_query_template(
            metrics=[
                CONTACTS_ATTEMPTED,
                DAYS_EMPLOYED_365,
            ],
            unit_of_analysis_type=MetricUnitOfAnalysisType.STATE_CODE,
            population_type=MetricPopulationType.INCARCERATION,
            time_interval_unit=MetricTimePeriod.WEEK,
            time_interval_length=2,
            rolling_period_unit=MetricTimePeriod.MONTH,
            rolling_period_length=3,
            min_end_date=datetime(2023, 1, 1, tzinfo=pytz.timezone("US/Eastern")),
            max_end_date=datetime(2023, 5, 1, tzinfo=pytz.timezone("US/Eastern")),
        )
        expected_query = f"""
WITH period_event_metrics AS (
{fix_indent(
    build_aggregated_metric_query_template(
        population_type=MetricPopulationType.INCARCERATION,
        unit_of_analysis_type=MetricUnitOfAnalysisType.STATE_CODE,
        metric_class=PeriodEventAggregatedMetric,
        metrics=[CONTACTS_ATTEMPTED],
        time_period=MetricTimePeriodConfig(
            interval_unit=MetricTimePeriod.WEEK,
            interval_length=2,
            min_period_end_date=datetime(2023, 1, 1, tzinfo=pytz.timezone("US/Eastern")).date(),
            max_period_end_date=datetime(2023, 5, 1, tzinfo=pytz.timezone("US/Eastern")).date(),
            rolling_period_unit=MetricTimePeriod.MONTH,
            rolling_period_length=3,
            period_name=MetricTimePeriod.CUSTOM.value,
        ),
        read_from_cached_assignments_by_time_period=False,
    ),
    indent_level=4
)}
)
assignment_span_metrics AS (
{fix_indent(
    build_aggregated_metric_query_template(
        population_type=MetricPopulationType.INCARCERATION,
        unit_of_analysis_type=MetricUnitOfAnalysisType.STATE_CODE,
        metric_class=AssignmentSpanAggregatedMetric,
        metrics=[DAYS_EMPLOYED_365],
        time_period=MetricTimePeriodConfig(
            interval_unit=MetricTimePeriod.WEEK,
            interval_length=2,
            min_period_end_date=datetime(2023, 1, 1, tzinfo=pytz.timezone("US/Eastern")).date(),
            max_period_end_date=datetime(2023, 5, 1, tzinfo=pytz.timezone("US/Eastern")).date(),
            rolling_period_unit=MetricTimePeriod.MONTH,
            rolling_period_length=3,
            period_name=MetricTimePeriod.CUSTOM.value,
        ),
        read_from_cached_assignments_by_time_period=False,
    ),
    indent_level=4
)}
)
SELECT
    state_code,
    period,
    start_date,
    end_date,
    IFNULL(contacts_attempted, 0) AS contacts_attempted,
    IFNULL(days_employed_365, 0) AS days_employed_365
FROM
    assignment_span_metrics
FULL OUTER JOIN
    period_event_metrics_metrics
USING
    (state_code, period, start_date, end_date)
"""
        self.assertEqual(expected_query, actual_query)
