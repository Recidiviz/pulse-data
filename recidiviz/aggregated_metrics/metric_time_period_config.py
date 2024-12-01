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
"""Class that can be used to build a query that produces a set of time periods."""

import enum
from datetime import date, datetime

import attr
from dateutil.relativedelta import relativedelta

from recidiviz.common import attr_validators
from recidiviz.common.date import (
    current_date_us_eastern,
    first_day_of_month,
    first_day_of_week,
)
from recidiviz.utils.types import assert_type


class MetricTimePeriod(enum.Enum):
    CUSTOM = "CUSTOM"
    DAY = "DAY"
    WEEK = "WEEK"
    MONTH = "MONTH"
    QUARTER = "QUARTER"
    YEAR = "YEAR"

    def is_valid_input_period(self) -> bool:
        return self != MetricTimePeriod.CUSTOM


@attr.define
class MetricTimePeriodConfig:
    """Class that can be used to build a query that produces a set of time periods."""

    METRIC_TIME_PERIOD_PERIOD_COLUMN = "period"
    METRIC_TIME_PERIOD_START_DATE_COLUMN = "metric_period_start_date"
    METRIC_TIME_PERIOD_END_DATE_EXCLUSIVE_COLUMN = "metric_period_end_date_exclusive"

    # These tell us how long each time period is (i.e. time between
    # metric_period_start_date and metric_period_end_date_exclusive).
    interval_length: int = attr.ib(validator=attr_validators.is_int)
    interval_unit: MetricTimePeriod = attr.ib(
        validator=attr.validators.instance_of(MetricTimePeriod)
    )

    # These tell us on what interval to create a new time period (i.e. time between
    # metric_period_start_date and the metric_period_start_date on the row with the next
    # greatest metric_period_start_date).
    rolling_period_length: int | None = attr.ib(validator=attr_validators.is_opt_int)
    rolling_period_unit: MetricTimePeriod | None = attr.ib(
        validator=attr_validators.is_opt(MetricTimePeriod)
    )

    # The min value for metric_period_end_date_exclusive of any periods generated.
    min_period_end_date: date = attr.ib(validator=attr_validators.is_date)
    # The max value for metric_period_end_date_exclusive of any periods generated.
    max_period_end_date: date | None = attr.ib(validator=attr_validators.is_opt_date)

    # The value for the period column of the output query.
    period_name_type: MetricTimePeriod = attr.ib(default=MetricTimePeriod.CUSTOM)

    def __attrs_post_init__(self) -> None:
        # Both rolling_period_unit and rolling_period_length must be provided together
        if (self.rolling_period_unit and not self.rolling_period_length) or (
            self.rolling_period_length and not self.rolling_period_unit
        ):
            raise ValueError(
                "Both rolling_period_unit and rolling_period_length must be provided "
                "together."
            )

        # Validate interval_unit and rolling_period_unit
        if not self.interval_unit.is_valid_input_period():
            raise ValueError(
                f"Interval type {self.interval_unit.value} is not a valid interval "
                f"type."
            )

        if (
            self.rolling_period_unit
            and not self.rolling_period_unit.is_valid_input_period()
        ):
            raise ValueError(
                f"Rolling period type {self.rolling_period_unit.value} is not a valid "
                f"type."
            )

        if isinstance(self.max_period_end_date, datetime):
            raise ValueError(
                f"Expected date type for max_period_end_date, found datetime: "
                f"{self.max_period_end_date.isoformat()}"
            )

        if isinstance(self.min_period_end_date, datetime):
            raise ValueError(
                f"Expected date type for min_period_end_date, found datetime: "
                f"{self.min_period_end_date.isoformat()}"
            )

        if self.max_period_end_date:
            # TODO(#35618): Revert this comment or decide to remove it entirely
            # current_date_eastern = current_date_us_eastern()
            # if self.max_period_end_date > current_date_eastern:
            #     raise ValueError(
            #         f"Expected max_period_end_date to be less than or equal to current "
            #         f"date [{current_date_eastern.isoformat()}]. Found "
            #         f"[{self.max_period_end_date.isoformat()}] instead."
            #     )

            if self.min_period_end_date > self.max_period_end_date:
                raise ValueError(
                    f"Found max_period_end_date "
                    f"[{self.max_period_end_date.isoformat()}] which is less than "
                    f"min_period_end_date [{self.min_period_end_date.isoformat()}]"
                )

    @classmethod
    def query_output_columns(cls) -> list[str]:
        return [
            cls.METRIC_TIME_PERIOD_START_DATE_COLUMN,
            cls.METRIC_TIME_PERIOD_END_DATE_EXCLUSIVE_COLUMN,
            cls.METRIC_TIME_PERIOD_PERIOD_COLUMN,
        ]

    @staticmethod
    def week_periods(lookback_weeks: int) -> "MetricTimePeriodConfig":
        """Returns a MetricTimePeriodConfig that can be used to generate a query that
        produces week-long time periods that start on Monday, with periods starting up
        to |lookback_weeks| weeks ago.
        """
        period_length = relativedelta(weeks=1)
        if lookback_weeks < period_length.weeks:
            raise ValueError(
                f"Must provide a lookback_weeks of at least [{period_length.weeks}] "
                f"to produce any periods."
            )

        max_period_end_date = first_day_of_week(current_date_us_eastern())
        min_period_end_date = first_day_of_week(
            max_period_end_date - relativedelta(weeks=lookback_weeks) + period_length
        )
        return MetricTimePeriodConfig(
            interval_length=1,
            interval_unit=MetricTimePeriod.WEEK,
            rolling_period_length=1,
            rolling_period_unit=MetricTimePeriod.WEEK,
            min_period_end_date=min_period_end_date,
            max_period_end_date=max_period_end_date,
            period_name_type=MetricTimePeriod.WEEK,
        )

    @staticmethod
    def month_periods(lookback_months: int) -> "MetricTimePeriodConfig":
        """Returns a MetricTimePeriodConfig that can be used to generate a query that
        produces month-long time periods that start on the first of every month, with
        periods starting up to |lookback_months| months ago.
        """
        period_length = relativedelta(months=1)
        if lookback_months < period_length.months:
            raise ValueError(
                f"Must provide a lookback_months of at least [{period_length.months}] "
                f"to produce any periods."
            )

        max_period_end_date = first_day_of_month(current_date_us_eastern())
        min_period_end_date = first_day_of_month(
            max_period_end_date - relativedelta(months=lookback_months) + period_length
        )
        return MetricTimePeriodConfig(
            interval_length=1,
            interval_unit=MetricTimePeriod.MONTH,
            rolling_period_length=1,
            rolling_period_unit=MetricTimePeriod.MONTH,
            min_period_end_date=min_period_end_date,
            max_period_end_date=max_period_end_date,
            period_name_type=MetricTimePeriod.MONTH,
        )

    @staticmethod
    def monthly_quarter_periods(lookback_months: int) -> "MetricTimePeriodConfig":
        """Returns a MetricTimePeriodConfig that can be used to generate a query that
        produces quarter-long time periods that start on the first of every month, with
        periods starting up to |lookback_months| months ago.
        """

        period_length = relativedelta(months=3)
        if lookback_months < period_length.months:
            raise ValueError(
                f"Must provide a lookback_months of at least [{period_length.months}] "
                f"to produce any periods."
            )
        max_period_end_date = first_day_of_month(current_date_us_eastern())
        min_period_end_date = first_day_of_month(
            max_period_end_date - relativedelta(months=lookback_months) + period_length
        )
        return MetricTimePeriodConfig(
            interval_length=3,
            interval_unit=MetricTimePeriod.MONTH,
            rolling_period_length=1,
            rolling_period_unit=MetricTimePeriod.MONTH,
            min_period_end_date=min_period_end_date,
            max_period_end_date=max_period_end_date,
            period_name_type=MetricTimePeriod.QUARTER,
        )

    @staticmethod
    def monthly_year_periods(lookback_months: int) -> "MetricTimePeriodConfig":
        """Returns a MetricTimePeriodConfig that can be used to generate a query that
        produces year-long time periods that start on the first of every month, with
        periods starting up to |lookback_months| months ago.
        """

        period_length = relativedelta(years=1)
        if lookback_months < period_length.months:
            raise ValueError(
                f"Must provide a lookback_months of at least [{period_length.months}] "
                f"to produce any periods."
            )

        max_period_end_date = first_day_of_month(current_date_us_eastern())
        min_period_end_date = first_day_of_month(
            max_period_end_date - relativedelta(months=lookback_months) + period_length
        )
        return MetricTimePeriodConfig(
            interval_length=1,
            interval_unit=MetricTimePeriod.YEAR,
            rolling_period_length=1,
            rolling_period_unit=MetricTimePeriod.MONTH,
            min_period_end_date=min_period_end_date,
            max_period_end_date=max_period_end_date,
            period_name_type=MetricTimePeriod.YEAR,
        )

    def build_query(self) -> str:
        # If any rolling period parameter is not provided, default them to interval_unit
        # and interval_length.
        if not self.rolling_period_length or not self.rolling_period_unit:
            rolling_period_length = self.interval_length
            rolling_period_unit = self.interval_unit
        else:
            rolling_period_length = assert_type(self.rolling_period_length, int)
            rolling_period_unit = assert_type(
                self.rolling_period_unit, MetricTimePeriod
            )

        rolling_interval_str = (
            f"INTERVAL {rolling_period_length} {rolling_period_unit.value}"
        )
        period_str = f'"{self.period_name_type.value}"'
        interval_str = f"INTERVAL {self.interval_length} {self.interval_unit.value}"
        min_date_str = f'"{self.min_period_end_date.isoformat()}"'
        max_date_str = (
            f'"{self.max_period_end_date.isoformat()}"'
            if self.max_period_end_date
            else 'CURRENT_DATE("US/Eastern")'
        )
        return f"""
SELECT
    DATE_SUB(
        {self.METRIC_TIME_PERIOD_END_DATE_EXCLUSIVE_COLUMN}, {interval_str}
    ) AS {self.METRIC_TIME_PERIOD_START_DATE_COLUMN},
    {self.METRIC_TIME_PERIOD_END_DATE_EXCLUSIVE_COLUMN},
    {period_str} as {self.METRIC_TIME_PERIOD_PERIOD_COLUMN},
FROM
    UNNEST(GENERATE_DATE_ARRAY(
        {min_date_str},
        {max_date_str},
        {rolling_interval_str}
    )) AS {self.METRIC_TIME_PERIOD_END_DATE_EXCLUSIVE_COLUMN}
"""
