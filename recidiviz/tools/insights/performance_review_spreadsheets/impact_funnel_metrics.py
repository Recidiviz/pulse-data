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
"""Utilities for querying impact funnel metrics."""

from collections import defaultdict
from datetime import date

import attr

from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause
from recidiviz.calculator.query.sessions_query_fragments import (
    create_intersection_spans,
)

_IMPACT_FUNNEL_QUERY = f"""
WITH impact_funnel_data AS (
    SELECT
        state_code,
        person_id,
        task_type,
        start_date,
        end_date AS end_date_exclusive,
        is_eligible
    FROM `recidiviz-123.analyst_data.workflows_person_impact_funnel_status_sessions_materialized`
    WHERE
        state_code="US_IX"
        AND task_type IN ("EARLY_DISCHARGE", "TRANSFER_TO_LIMITED_SUPERVISION", "SUPERVISION_LEVEL_DOWNGRADE", "FULL_TERM_DISCHARGE")
        AND {nonnull_end_date_exclusive_clause("end_date")} >= "2024-02-01"
)
, officer_sessions AS (
    SELECT
        state_code,
        person_id,
        supervising_officer_external_id AS officer_id,
        start_date,
        end_date_exclusive
    FROM `recidiviz-123.sessions.supervision_officer_sessions_materialized`
    WHERE
        state_code="US_IX"
        AND {nonnull_end_date_exclusive_clause("end_date_exclusive")} >= "2024-02-01"
)
, joined_data AS (
  {create_intersection_spans(
    table_1_name="impact_funnel_data",
    table_2_name="officer_sessions",
    index_columns=["state_code", "person_id"],
    table_1_columns=["task_type", "is_eligible"],
    table_2_columns=["officer_id"])}
)
SELECT
    officer_id,
    date_to_check,
    task_type,
    COUNTIF(is_eligible) AS eligible,
FROM joined_data
CROSS JOIN UNNEST(generate_date_array("2024-02-01", "2025-01-01", INTERVAL 1 MONTH)) AS date_to_check
WHERE
    date_to_check BETWEEN start_date AND {nonnull_end_date_exclusive_clause("end_date_exclusive")}
    AND officer_id IS NOT NULL
GROUP BY 1, 2, 3
"""


@attr.define(frozen=True)
class ImpactFunnelMetric:
    date: date
    task_type: str
    eligible: int


@attr.define(frozen=True)
class ImpactFunnelMetrics:
    """Holds mappings of impact funnel metrics as of the end of each month by officer"""

    data: dict[str, list[ImpactFunnelMetric]]

    @classmethod
    def from_bigquery(cls, bq_client: BigQueryClient) -> "ImpactFunnelMetrics":
        """Creates an ImpactFunnel based on the result of querying BigQuery"""
        results = bq_client.run_query_async(
            query_str=_IMPACT_FUNNEL_QUERY, use_query_cache=True
        )

        data = defaultdict(list)

        for result in results:
            data[result["officer_id"]].append(
                ImpactFunnelMetric(
                    date=result["date_to_check"],
                    task_type=result["task_type"],
                    eligible=result["eligible"],
                )
            )

        return cls(data)
