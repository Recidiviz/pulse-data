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
"""Utilities for querying snooze metrics."""

from collections import defaultdict

import attr

from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause

_SNOOZE_QUERY = f"""
WITH counts AS (
  SELECT
      o.supervising_officer_external_id AS officer_id,
      opportunity_type,
      denial_reason,
      COUNT(*) AS snooze_reason_count
  FROM `recidiviz-123.workflows_views.clients_snooze_spans_materialized` s
  INNER JOIN `recidiviz-123.sessions.supervision_officer_sessions_materialized` o
  ON
    s.person_id = o.person_id
    AND s.start_date BETWEEN o.start_date AND {nonnull_end_date_exclusive_clause("o.end_date_exclusive")}
  CROSS JOIN UNNEST(denial_reasons) AS denial_reason
  WHERE
      s.state_code="US_ND"
      AND opportunity_type IN ("earlyTermination")
      AND s.start_date BETWEEN "2024-01-01" AND "2024-03-31"
  GROUP BY 1, 2, 3
)
SELECT
    officer_id,
    opportunity_type,
    STRING_AGG(denial_reason) AS denial_reasons
FROM counts
GROUP BY officer_id, opportunity_type, snooze_reason_count
QUALIFY RANK() OVER (PARTITION BY officer_id, opportunity_type ORDER BY snooze_reason_count DESC) = 1
"""


@attr.define(frozen=True)
class SnoozeMetrics:
    """Holds snooze metrics for each opportunity by officer"""

    data: dict[str, dict[str, str]]  # [officer_id, [opportunity_type, denial_reasons]]

    @classmethod
    def from_bigquery(cls, bq_client: BigQueryClient) -> "SnoozeMetrics":
        """Creates a SnoozeMetrics based on the result of querying BigQuery"""
        results = bq_client.run_query_async(
            query_str=_SNOOZE_QUERY, use_query_cache=True
        )
        data: dict[str, dict[str, str]] = defaultdict(dict)
        for result in results:
            data[result["officer_id"]][result["opportunity_type"]] = result[
                "denial_reasons"
            ]

        return cls(data)
