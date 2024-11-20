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
"""Tests for MetricTimePeriodConfig"""

import datetime

from freezegun import freeze_time

from recidiviz.aggregated_metrics.metric_time_period_config import (
    MetricTimePeriod,
    MetricTimePeriodConfig,
)
from recidiviz.common.date import current_date_us_eastern
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)


class TestMetricTimePeriodConfig(BigQueryEmulatorTestCase):
    """Tests for MetricTimePeriodConfig"""

    def test_week_periods(self) -> None:
        lookback_weeks = 2

        # Tuesday, Nov 12, 2024 (US/Eastern time)
        with freeze_time("2024-11-12 00:00:00-05:00"):
            self.assertEqual("2024-11-12", current_date_us_eastern().isoformat())

            metric_time_period_config = MetricTimePeriodConfig.week_periods(
                lookback_weeks=lookback_weeks
            )
            query_str = metric_time_period_config.build_query()

        expected_results = [
            {
                "period": "WEEK",
                "metric_period_start_date": datetime.date(2024, 10, 28),
                "metric_period_end_date_exclusive": datetime.date(2024, 11, 4),
            },
            {
                "period": "WEEK",
                "metric_period_start_date": datetime.date(2024, 11, 4),
                "metric_period_end_date_exclusive": datetime.date(2024, 11, 11),
            },
        ]

        results = self.query(query_str).to_dict(orient="records")

        # Should produce the same number of rows as specified in lookback_weeks
        self.assertEqual(lookback_weeks, len(results))
        self.assertEqual(expected_results, results)

    def test_week_periods_last_day_of_week(self) -> None:
        lookback_weeks = 2

        # Sunday, Nov 10, 2024 (US/Eastern time)
        with freeze_time("2024-11-10 00:00:00-05:00"):
            self.assertEqual("2024-11-10", current_date_us_eastern().isoformat())
            metric_time_period_config = MetricTimePeriodConfig.week_periods(
                lookback_weeks=lookback_weeks
            )
            query_str = metric_time_period_config.build_query()

        expected_results = [
            {
                "period": "WEEK",
                "metric_period_start_date": datetime.date(2024, 10, 21),
                "metric_period_end_date_exclusive": datetime.date(2024, 10, 28),
            },
            {
                "period": "WEEK",
                "metric_period_start_date": datetime.date(2024, 10, 28),
                "metric_period_end_date_exclusive": datetime.date(2024, 11, 4),
            },
        ]

        results = self.query(query_str).to_dict(orient="records")

        # Should produce the same number of rows as specified in lookback_weeks
        self.assertEqual(lookback_weeks, len(results))
        self.assertEqual(expected_results, results)

    def test_week_periods_first_day_of_week(self) -> None:
        lookback_weeks = 2

        # Monday, Nov 11, 2024 (US/Eastern time)
        with freeze_time("2024-11-11 00:00:00-05:00"):
            self.assertEqual("2024-11-11", current_date_us_eastern().isoformat())

            metric_time_period_config = MetricTimePeriodConfig.week_periods(
                lookback_weeks=lookback_weeks
            )
            query_str = metric_time_period_config.build_query()

        expected_results = [
            {
                "period": "WEEK",
                "metric_period_start_date": datetime.date(2024, 10, 28),
                "metric_period_end_date_exclusive": datetime.date(2024, 11, 4),
            },
            {
                "period": "WEEK",
                "metric_period_start_date": datetime.date(2024, 11, 4),
                "metric_period_end_date_exclusive": datetime.date(2024, 11, 11),
            },
        ]

        results = self.query(query_str).to_dict(orient="records")

        # Should produce the same number of rows as specified in lookback_weeks
        self.assertEqual(lookback_weeks, len(results))
        self.assertEqual(expected_results, results)

    def test_month_periods(self) -> None:
        lookback_months = 3

        # Tuesday, Nov 12, 2024 (US/Eastern time)
        with freeze_time("2024-11-12 00:00:00-05:00"):
            self.assertEqual("2024-11-12", current_date_us_eastern().isoformat())

            metric_time_period_config = MetricTimePeriodConfig.month_periods(
                lookback_months=lookback_months
            )
            query_str = metric_time_period_config.build_query()

        expected_results = [
            {
                "period": "MONTH",
                "metric_period_start_date": datetime.date(2024, 8, 1),
                "metric_period_end_date_exclusive": datetime.date(2024, 9, 1),
            },
            {
                "period": "MONTH",
                "metric_period_start_date": datetime.date(2024, 9, 1),
                "metric_period_end_date_exclusive": datetime.date(2024, 10, 1),
            },
            {
                "period": "MONTH",
                "metric_period_start_date": datetime.date(2024, 10, 1),
                "metric_period_end_date_exclusive": datetime.date(2024, 11, 1),
            },
        ]

        results = self.query(query_str).to_dict(orient="records")

        # Should produce the same number of rows as specified in lookback_months
        self.assertEqual(lookback_months, len(results))
        self.assertEqual(expected_results, results)

    def test_monthly_quarter_periods(self) -> None:
        lookback_months = 4

        # Tuesday, Nov 12, 2024 (US/Eastern time)
        with freeze_time("2024-11-12 00:00:00-05:00"):
            self.assertEqual("2024-11-12", current_date_us_eastern().isoformat())

            metric_time_period_config = MetricTimePeriodConfig.monthly_quarter_periods(
                lookback_months=lookback_months
            )
            query_str = metric_time_period_config.build_query()

        expected_results = [
            {
                "period": "QUARTER",
                "metric_period_start_date": datetime.date(2024, 7, 1),
                "metric_period_end_date_exclusive": datetime.date(2024, 10, 1),
            },
            {
                "period": "QUARTER",
                "metric_period_start_date": datetime.date(2024, 8, 1),
                "metric_period_end_date_exclusive": datetime.date(2024, 11, 1),
            },
        ]

        results = self.query(query_str).to_dict(orient="records")
        self.assertEqual(expected_results, results)

    def test_monthly_year_periods(self) -> None:
        lookback_months = 14

        # Tuesday, Nov 12, 2024 (US/Eastern time)
        with freeze_time("2024-11-12 00:00:00-05:00"):
            self.assertEqual("2024-11-12", current_date_us_eastern().isoformat())

            metric_time_period_config = MetricTimePeriodConfig.monthly_year_periods(
                lookback_months=lookback_months
            )
            query_str = metric_time_period_config.build_query()

        expected_results = [
            {
                "period": "YEAR",
                "metric_period_start_date": datetime.date(2023, 9, 1),
                "metric_period_end_date_exclusive": datetime.date(2024, 9, 1),
            },
            {
                "period": "YEAR",
                "metric_period_start_date": datetime.date(2023, 10, 1),
                "metric_period_end_date_exclusive": datetime.date(2024, 10, 1),
            },
            {
                "period": "YEAR",
                "metric_period_start_date": datetime.date(2023, 11, 1),
                "metric_period_end_date_exclusive": datetime.date(2024, 11, 1),
            },
        ]

        results = self.query(query_str).to_dict(orient="records")
        self.assertEqual(expected_results, results)

    def test_custom_periods(self) -> None:
        # Tuesday, Nov 12, 2024 (US/Eastern time)
        with freeze_time("2024-11-12 00:00:00-05:00"):
            self.assertEqual("2024-11-12", current_date_us_eastern().isoformat())

            # Config for periods that are 2 months long, with a new period starting
            # every week.
            metric_time_period_config = MetricTimePeriodConfig(
                interval_length=2,
                interval_unit=MetricTimePeriod.MONTH,
                rolling_period_length=1,
                rolling_period_unit=MetricTimePeriod.WEEK,
                min_period_end_date=datetime.date(2024, 1, 30),
                max_period_end_date=datetime.date(2024, 2, 15),
            )
            query_str = metric_time_period_config.build_query()

        expected_results = [
            {
                "period": "CUSTOM",
                "metric_period_start_date": datetime.date(2023, 11, 30),
                "metric_period_end_date_exclusive": datetime.date(2024, 1, 30),
            },
            {
                "period": "CUSTOM",
                "metric_period_start_date": datetime.date(2023, 12, 6),
                "metric_period_end_date_exclusive": datetime.date(2024, 2, 6),
            },
            {
                "period": "CUSTOM",
                "metric_period_start_date": datetime.date(2023, 12, 13),
                "metric_period_end_date_exclusive": datetime.date(2024, 2, 13),
            },
        ]

        results = self.query(query_str).to_dict(orient="records")

        self.assertEqual(expected_results, results)
