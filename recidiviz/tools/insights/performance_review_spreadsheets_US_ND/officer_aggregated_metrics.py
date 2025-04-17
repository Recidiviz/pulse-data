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
"""Utilities for querying officer aggregated metrics."""

from collections import defaultdict
from datetime import date

import attr

from recidiviz.big_query.big_query_client import BigQueryClient

_OFFICER_AGGREGATED_METRICS_MONTH_QUERY = """
SELECT
    officer_id,
    end_date,
    avg_daily_population,
    violations_absconsion,
    incarceration_starts_most_severe_violation_type_not_absconsion,
    task_completions_early_discharge
FROM `recidiviz-123.aggregated_metrics.supervision_officer_or_previous_if_transitional_aggregated_metrics_materialized`
WHERE
    state_code = "US_ND"
    AND period = "MONTH"
    AND end_date BETWEEN "2024-02-01" AND "2025-04-01"
ORDER BY officer_id, end_date
"""

_OFFICER_AGGREGATED_METRICS_YEAR_QUERY = """
SELECT
    officer_id,
    end_date,
    avg_daily_population,
    violations_absconsion,
    incarceration_starts_most_severe_violation_type_not_absconsion,
    task_completions_early_discharge
FROM `recidiviz-123.aggregated_metrics.supervision_officer_or_previous_if_transitional_aggregated_metrics_materialized`
WHERE
    state_code = "US_ND"
    AND period = "YEAR"
    AND end_date = "2025-01-01"
"""


@attr.define(frozen=True)
class AggregatedMetrics:
    end_date_exclusive: date
    avg_daily_population: float | None
    num_absconsions: int | None
    num_incarcerations: int | None
    task_completions_early_discharge: int | None


@attr.define(frozen=True)
class OfficerAggregatedMetrics:
    """Holds mappings of monthly+yearly aggregated metrics by officer"""

    monthly_data: dict[str, list[AggregatedMetrics]]
    yearly_data: dict[str, AggregatedMetrics]

    @classmethod
    def from_bigquery(cls, bq_client: BigQueryClient) -> "OfficerAggregatedMetrics":
        """Creates an OfficerAggregatedMetrics based on the result of querying BigQuery"""
        monthly_results = bq_client.run_query_async(
            query_str=_OFFICER_AGGREGATED_METRICS_MONTH_QUERY, use_query_cache=True
        )

        monthly_data = defaultdict(list)
        for row in monthly_results:
            metrics = AggregatedMetrics(
                end_date_exclusive=row["end_date"],
                avg_daily_population=row["avg_daily_population"],
                num_absconsions=row["violations_absconsion"],
                num_incarcerations=row[
                    "incarceration_starts_most_severe_violation_type_not_absconsion"
                ],
                task_completions_early_discharge=row[
                    "task_completions_early_discharge"
                ],
            )
            monthly_data[row["officer_id"]].append(metrics)

        yearly_results = bq_client.run_query_async(
            query_str=_OFFICER_AGGREGATED_METRICS_YEAR_QUERY, use_query_cache=True
        )

        yearly_data = {}
        for row in yearly_results:
            metrics = AggregatedMetrics(
                end_date_exclusive=row["end_date"],
                avg_daily_population=row["avg_daily_population"],
                num_absconsions=row["violations_absconsion"],
                num_incarcerations=row[
                    "incarceration_starts_most_severe_violation_type_not_absconsion"
                ],
                task_completions_early_discharge=row[
                    "task_completions_early_discharge"
                ],
            )
            yearly_data[row["officer_id"]] = metrics

        return cls(monthly_data=monthly_data, yearly_data=yearly_data)
