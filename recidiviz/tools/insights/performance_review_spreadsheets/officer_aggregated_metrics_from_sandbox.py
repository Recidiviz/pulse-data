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
"""Utilities for querying officer aggregated metrics from a sandbox dataset.

See branch danawillow/id-perf-sandbox for the changes required to be loaded.
"""

from collections import defaultdict
from datetime import date
from typing import Any

import attr

from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.utils.string import StrictStringFormatter

_OFFICER_AGGREGATED_METRICS_MONTH_QUERY = """
SELECT
    officer_id,
    end_date,
    task_completions_early_discharge,
    task_completions_transfer_to_limited_supervision,
    task_completions_supervision_level_downgrade,
    task_completions_full_term_discharge,
    1 - SAFE_DIVIDE(avg_population_assessment_overdue, avg_population_assessment_required) AS timely_risk_assessment,
    1 - SAFE_DIVIDE(avg_population_contact_overdue, avg_population_contact_required) AS timely_contact,
    1 - SAFE_DIVIDE(avg_population_task_eligible_supervision_level_downgrade, avg_daily_population) AS timely_downgrade
FROM `recidiviz-123.{sandbox_prefix}_aggregated_metrics.supervision_officer_aggregated_metrics_materialized`
WHERE
    state_code = "US_IX"
    AND period = "MONTH"
    AND end_date BETWEEN "2024-02-01" AND "2025-01-01"
ORDER BY officer_id, end_date
"""

_OFFICER_AGGREGATED_METRICS_YEAR_QUERY = """
SELECT
    officer_id,
    end_date,
    task_completions_early_discharge,
    task_completions_transfer_to_limited_supervision,
    task_completions_supervision_level_downgrade,
    task_completions_full_term_discharge,
    1 - SAFE_DIVIDE(avg_population_assessment_overdue, avg_population_assessment_required) AS timely_risk_assessment,
    1 - SAFE_DIVIDE(avg_population_contact_overdue, avg_population_contact_required) AS timely_contact,
    1 - SAFE_DIVIDE(avg_population_task_eligible_supervision_level_downgrade, avg_daily_population) AS timely_downgrade
FROM `recidiviz-123.{sandbox_prefix}_aggregated_metrics.supervision_officer_aggregated_metrics_materialized`
WHERE
    state_code = "US_IX"
    AND period = "YEAR"
    AND end_date = "2024-12-01" -- TODO(#35973): Use 2025-01-01 once we're in Jan
"""


@attr.define(frozen=True)
class AggregatedMetricsFromSandbox:
    end_date_exclusive: date
    task_completions_early_discharge: int | None
    task_completions_transfer_to_limited_supervision: int | None
    task_completions_supervision_level_downgrade: int | None
    task_completions_full_term_discharge: int | None
    timely_risk_assessment: float | None
    timely_contact: float | None
    timely_downgrade: float | None

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "AggregatedMetricsFromSandbox":
        return cls(
            end_date_exclusive=row["end_date"],
            task_completions_early_discharge=row["task_completions_early_discharge"],
            task_completions_transfer_to_limited_supervision=row[
                "task_completions_transfer_to_limited_supervision"
            ],
            task_completions_supervision_level_downgrade=row[
                "task_completions_supervision_level_downgrade"
            ],
            task_completions_full_term_discharge=row[
                "task_completions_full_term_discharge"
            ],
            timely_risk_assessment=row["timely_risk_assessment"],
            timely_contact=row["timely_contact"],
            timely_downgrade=row["timely_downgrade"],
        )


@attr.define(frozen=True)
class OfficerAggregatedMetricsFromSandbox:
    """Holds mappings of monthly+yearly aggregated metrics by officer"""

    monthly_data: dict[str, list[AggregatedMetricsFromSandbox]]
    yearly_data: dict[str, AggregatedMetricsFromSandbox]

    @classmethod
    def from_bigquery(
        cls, bq_client: BigQueryClient, sandbox_prefix: str
    ) -> "OfficerAggregatedMetricsFromSandbox":
        """Creates an OfficerAggregatedMetricsFromSandbox based on the result of querying BigQuery"""
        string_formatter = StrictStringFormatter()
        monthly_results = bq_client.run_query_async(
            query_str=string_formatter.format(
                _OFFICER_AGGREGATED_METRICS_MONTH_QUERY, sandbox_prefix=sandbox_prefix
            ),
            use_query_cache=True,
        )

        monthly_data = defaultdict(list)
        for row in monthly_results:
            metrics = AggregatedMetricsFromSandbox.from_row(row)
            monthly_data[row["officer_id"]].append(metrics)

        yearly_results = bq_client.run_query_async(
            query_str=string_formatter.format(
                _OFFICER_AGGREGATED_METRICS_YEAR_QUERY, sandbox_prefix=sandbox_prefix
            ),
            use_query_cache=True,
        )

        yearly_data = {}
        for row in yearly_results:
            metrics = AggregatedMetricsFromSandbox.from_row(row)
            yearly_data[row["officer_id"]] = metrics

        return cls(monthly_data=monthly_data, yearly_data=yearly_data)
