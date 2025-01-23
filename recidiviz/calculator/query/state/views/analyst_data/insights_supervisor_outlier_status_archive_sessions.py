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
"""
View representing spans of time during which a unit supervisor had some number
of surfaceable outlier officers in the Insights (Supervisor Homepage) tool 
in a given month, for any metric.
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_NAME = "insights_supervisor_outlier_status_archive_sessions"

_VIEW_DESCRIPTION = """
View representing spans of time during which a unit supervisor had some number
of surfaceable outlier officers in the Insights (Supervisor Homepage) tool 
in a given month, for any metric.
"""

_QUERY_TEMPLATE = f"""
-- Use officer-level outlier statuses to aggregate up to the supervisor level.
WITH surfaceable_outlier_sessions_with_supervisor AS (
    -- Officer outlier status
    SELECT
        state_code,
        officer_id,
        start_date,
        end_date_exclusive,
        is_surfaceable_outlier,
        supervisor_staff_id,
    FROM
        `{{project_id}}.analyst_data.insights_supervision_officer_outlier_status_archive_sessions_materialized`,
        UNNEST(supervisor_staff_id_array) AS supervisor_staff_id
)
,
{create_sub_sessions_with_attributes("surfaceable_outlier_sessions_with_supervisor", index_columns=["state_code", "supervisor_staff_id"], end_date_field_name="end_date_exclusive")}
,
-- Dedup to single spans of time with a count of the number of outlier officers
sub_sessions_dedup AS (
    SELECT
        state_code,
        supervisor_staff_id,
        start_date,
        end_date_exclusive,
        COUNT(DISTINCT officer_id) AS num_officers,
        COUNT(DISTINCT IF(is_surfaceable_outlier, officer_id, NULL)) AS num_outlier_officers,
    FROM
        sub_sessions_with_attributes
    GROUP BY
        1, 2, 3, 4
)
,
aggregated_spans AS (
    {aggregate_adjacent_spans(
        "sub_sessions_dedup", 
        index_columns=["state_code", "supervisor_staff_id"], 
        attribute=["num_outlier_officers", "num_officers"],
        end_date_field_name="end_date_exclusive")
    }
)
SELECT
    aggregated_spans.*,
    LOWER(staff.email) AS email_address,
FROM
    aggregated_spans
INNER JOIN
    `{{project_id}}.normalized_state.state_staff` staff
ON
    aggregated_spans.supervisor_staff_id = staff.staff_id
    AND aggregated_spans.state_code = staff.state_code
"""

INSIGHTS_SUPERVISOR_OUTLIER_STATUS_ARCHIVE_SESSIONS_VIEW_BUILDER = (
    SimpleBigQueryViewBuilder(
        dataset_id=ANALYST_VIEWS_DATASET,
        view_id=_VIEW_NAME,
        description=_VIEW_DESCRIPTION,
        view_query_template=_QUERY_TEMPLATE,
        clustering_fields=["state_code", "supervisor_staff_id"],
        should_materialize=True,
    )
)


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        INSIGHTS_SUPERVISOR_OUTLIER_STATUS_ARCHIVE_SESSIONS_VIEW_BUILDER.build_and_print()
