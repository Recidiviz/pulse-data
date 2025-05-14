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
View representing spans of time during which a supervision officer was surfaceable
in the Insights (Supervisor Homepage) tool as being an outlier in a given month
for a given metric.
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import (
    MAGIC_START_DATE,
    nonnull_end_date_clause,
    nonnull_end_date_exclusive_clause,
    revert_nonnull_end_date_clause,
)
from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_intersection_spans,
)
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_NAME = "insights_supervision_officer_outlier_status_archive_sessions"

_VIEW_DESCRIPTION = """
View representing spans of time during which a supervision officer was surfaceable
in the Insights (Supervisor Homepage) tool as being an outlier in a given month
for a given metric.
"""

_QUERY_TEMPLATE = f"""
WITH officer_outlier_archive_with_backfill AS (
    -- All exports
    SELECT 
        state_code,
        officer_id,
        -- We use the previous full month's aggregated metrics to determine outlier
        -- status in the tool -- use the end date of the last archived metrics
        -- as the start date of the new period.
        end_date AS start_date,
        DATE_ADD(end_date, INTERVAL 1 MONTH) AS end_date_exclusive,
        export_date AS export_start_date,
        LEAD(export_date) OVER (PARTITION BY state_code, officer_id, metric_id, end_date ORDER BY export_date) AS export_end_date,
        metric_id,
        status = "FAR" AS is_surfaceable_outlier,
    FROM
        `{{project_id}}.outliers_views.supervision_officer_outlier_status_archive_materialized`
    WHERE
        is_surfaced_category_type

    UNION ALL

    -- Backfilled exports
    SELECT 
        state_code,
        officer_id,
        -- We use the previous full month's aggregated metrics to determine outlier
        -- status in the tool -- use the end date of the last archived metrics
        -- as the start date of the new period.
        end_date AS start_date,
        DATE_ADD(end_date, INTERVAL 1 MONTH) AS end_date_exclusive,
        DATE("{MAGIC_START_DATE}") AS export_start_date,
        export_date AS export_end_date,
        metric_id,
        status = "FAR" AS is_surfaceable_outlier,
    FROM
        `{{project_id}}.outliers_views.supervision_officer_outlier_status_archive_materialized` 
    WHERE
        is_surfaced_category_type
    QUALIFY
        ROW_NUMBER() OVER (PARTITION BY state_code, officer_id, metric_id, end_date ORDER BY export_date) = 1
)
,
-- Produces intersections of export sessions and metric periods
officer_outlier_status_sessions AS (
    SELECT
        state_code,
        officer_id,
        GREATEST(start_date, export_start_date) AS start_date,
        {revert_nonnull_end_date_clause(f'LEAST(end_date_exclusive, {nonnull_end_date_clause("export_end_date")})')} AS end_date_exclusive,
        metric_id,
        is_surfaceable_outlier,
    FROM
        officer_outlier_archive_with_backfill
    WHERE
        export_start_date BETWEEN start_date AND DATE_SUB(end_date_exclusive, INTERVAL 1 DAY)
        OR start_date BETWEEN export_start_date AND {nonnull_end_date_exclusive_clause("export_end_date")} 
)
,
officer_outlier_status_sessions_aggregated AS (
{aggregate_adjacent_spans(
    table_name="officer_outlier_status_sessions",
    index_columns=["state_code", "officer_id", "metric_id"],
    attribute=["is_surfaceable_outlier"],
    end_date_field_name="end_date_exclusive"
)}
)
,
supervisor_assignments AS (
    SELECT
        state_code,
        officer_id,
        supervisor_staff_id_array,
        start_date,
        end_date_exclusive,
    FROM
        `{{project_id}}.sessions.supervision_staff_attribute_sessions_materialized`
)
{create_intersection_spans(
    table_1_name="officer_outlier_status_sessions_aggregated", 
    table_2_name="supervisor_assignments", 
    index_columns=["state_code", "officer_id"],
    include_zero_day_intersections=False,
    table_1_columns=["metric_id", "is_surfaceable_outlier"],
    table_2_columns=["supervisor_staff_id_array"]
)}
"""

INSIGHTS_SUPERVISION_OFFICER_OUTLIER_STATUS_ARCHIVE_SESSIONS_VIEW_BUILDER = (
    SimpleBigQueryViewBuilder(
        dataset_id=ANALYST_VIEWS_DATASET,
        view_id=_VIEW_NAME,
        description=_VIEW_DESCRIPTION,
        view_query_template=_QUERY_TEMPLATE,
        clustering_fields=["state_code", "officer_id"],
        should_materialize=True,
    )
)


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        INSIGHTS_SUPERVISION_OFFICER_OUTLIER_STATUS_ARCHIVE_SESSIONS_VIEW_BUILDER.build_and_print()
