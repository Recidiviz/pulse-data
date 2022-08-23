# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
""" MetricQueryBuilder for events over time metrics """
from typing import Dict, Union

import attr
from sqlalchemy import Column, Integer, distinct, func, select, text
from sqlalchemy.orm import Query
from sqlalchemy.sql.ddl import DDLElement

from recidiviz.case_triage.pathways.dimensions.dimension import Dimension
from recidiviz.case_triage.pathways.dimensions.time_period import TimePeriod
from recidiviz.case_triage.pathways.metrics.query_builders.metric_query_builder import (
    FetchMetricParams,
    MetricConfigOptionsType,
    MetricQueryBuilder,
)
from recidiviz.persistence.database.schema.pathways.schema import PathwaysBase


@attr.s(auto_attribs=True)
class OverTimeMetricQueryBuilder(MetricQueryBuilder):
    """Metric which counts events over time and includes a running average"""

    date_column: Union[DDLElement, Column] = attr.field()
    counting_function: Union[DDLElement, Column] = attr.field(default=func.count())

    def _build_earliest_date_query(self, time_period: str) -> Query:
        # Selects the earliest date for a `time_period`
        # Heavily utilizes an index on `(time_period, date_column)` for sub-ms execution time
        earliest_date = Query(func.min(self.date_column).label("date")).filter(
            self.model.time_period.in_(TimePeriod.period_range(time_period))
        )

        return earliest_date.cte("earliest_date")

    def build_query(self, params: FetchMetricParams) -> Query:
        new_params = attr.evolve(params, filters=params.filters.copy())
        time_periods_asc = sorted(
            new_params.filters.pop(
                Dimension.TIME_PERIOD, [TimePeriod.MONTHS_25_60.value]
            ),
            key=lambda val: TimePeriod.month_map()[val],
        )
        earliest_date = self._build_earliest_date_query(time_periods_asc[-1])

        event_counts = (
            Query(
                [
                    self.date_column.label("date"),
                    self.counting_function.label("count"),
                ]
            )
            # Select an extra 2 months to initialize running average
            .filter(
                self.date_column
                >= (
                    select(
                        earliest_date.c.date - text("INTERVAL '2 MONTHS'")
                    ).scalar_subquery()
                ),
                *self.build_filter_conditions(new_params),
            )
            .group_by(self.date_column)
            .cte("event_counts")
        )

        # Generate entries for all months in range so we include months with 0 data points in our
        # averages.
        all_months = Query(
            func.generate_series(
                earliest_date.c.date - text("INTERVAL '2 MONTHS'"),
                func.current_date(),
                "1 month",
            ).label("date")
        ).subquery()

        full_counts = (
            Query(
                [
                    func.date_trunc("month", all_months.c.date).label("date"),
                    func.coalesce(event_counts.c.count, 0).label("count"),
                ]
            )
            .outerjoin(
                all_months,
                func.date_trunc("month", all_months.c.date) == event_counts.c.date,
                full=True,
            )
            .cte("full_counts")
        )

        # Calculate running 3-month average
        with_averages = Query(
            [
                full_counts.c.date,
                full_counts.c.count,
                func.cast(
                    func.avg(full_counts.c.count).over(
                        order_by=full_counts.c.date, rows=(-2, 0)
                    ),
                    Integer,
                ).label("avg90day"),
            ]
        ).cte("averages")

        return (
            Query(
                [
                    func.cast(
                        func.extract("year", with_averages.c.date), Integer
                    ).label("year"),
                    func.cast(
                        func.extract("month", with_averages.c.date), Integer
                    ).label("month"),
                    with_averages.c.count,
                    with_averages.c.avg90day,
                ]
            )
            # Drop leading 2 months
            .filter(
                with_averages.c.date >= (select(earliest_date.c.date).scalar_subquery())
            ).order_by(with_averages.c.date)
        )

    @classmethod
    def adapt_config_options(
        cls, model: PathwaysBase, options: MetricConfigOptionsType
    ) -> Dict[str, DDLElement]:
        adapted_options = {}
        if cls.has_valid_option(options, "date_column", instance=str):
            date_column = str(options["date_column"])
            adapted_options["date_column"] = getattr(model, date_column)

        if cls.has_valid_option(options, "counting_column", instance=str):
            counting_column = str(options["counting_column"])

            adapted_options["counting_function"] = func.count(
                distinct(getattr(model, counting_column))
            )

        return adapted_options
