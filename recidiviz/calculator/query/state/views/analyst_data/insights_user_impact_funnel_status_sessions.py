# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
Sessionized view representing spans of time where a supervisor has surfaceable
officer outlier officers, along with tool usage.
"""

from google.cloud import bigquery

from recidiviz.calculator.query.state.views.analyst_data.funnel_status_sessions_big_query_view_builder import (
    FunnelStatusEventQueryBuilder,
    FunnelStatusSessionsViewBuilder,
    FunnelStatusSpanQueryBuilder,
)
from recidiviz.calculator.query.state.views.analyst_data.insights_segment_events import (
    INSIGHTS_SEGMENT_EVENT_QUERY_CONFIGS,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_NAME = "insights_user_impact_funnel_status_sessions"

_VIEW_DESCRIPTION = """
Sessionized view representing spans of time where a supervisor has surfaceable
officer outlier officers, along with tool usage.
"""

INSIGHTS_USAGE_EVENTS: list[str] = [
    event_config.insights_event_type
    for event_config in INSIGHTS_SEGMENT_EVENT_QUERY_CONFIGS
]

INSIGHTS_USER_IMPACT_FUNNEL_STATUS_SESSIONS_VIEW_BUILDER = FunnelStatusSessionsViewBuilder(
    view_id=_VIEW_NAME,
    description=_VIEW_DESCRIPTION,
    index_cols=["state_code", "email_address"],
    # End dates are always the end of the month
    funnel_reset_dates_sql_source="""
    SELECT DISTINCT
        state_code,
        email AS email_address,
        DATE_ADD(DATE_TRUNC(event_ts, MONTH), INTERVAL 1 MONTH) AS funnel_reset_date,
    FROM
        `{project_id}.analyst_data.insights_segment_events_materialized`
""",
    funnel_status_query_builders=[
        FunnelStatusSpanQueryBuilder(
            sql_source="""
    SELECT
        state_code,
        email_address,
        start_date,
        end_date_exclusive,
        num_outlier_officers > 0 AS has_outlier_officers,
        num_outlier_officers,
    FROM
        `{project_id}.analyst_data.insights_supervisor_outlier_status_archive_sessions_materialized`""",
            start_date_col="start_date",
            end_date_exclusive_col="end_date_exclusive",
            index_cols=["state_code", "email_address"],
            status_cols_by_type=[
                ("has_outlier_officers", bigquery.enums.StandardSqlTypeNames.BOOL),
                ("num_outlier_officers", bigquery.enums.StandardSqlTypeNames.INT64),
            ],
            # We don't need to further truncate these sessions because they already
            # represent accurate spans of time of outlier status as surfaced in the
            # tool.
            truncate_spans_at_reset_dates=False,
        ),
        *[
            FunnelStatusEventQueryBuilder(
                sql_source=f"""
        SELECT
            state_code,
            email AS email_address,
            event_ts AS start_date,
            TRUE AS {event_type.lower()},
        FROM
            `{{project_id}}.analyst_data.insights_segment_events_materialized`
        WHERE
            event = "{event_type}"
            """,
                event_date_col="start_date",
                index_cols=["state_code", "email_address"],
                status_cols_by_type=[
                    (event_type.lower(), bigquery.enums.StandardSqlTypeNames.BOOL)
                ],
            )
            for event_type in INSIGHTS_USAGE_EVENTS
        ],
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        INSIGHTS_USER_IMPACT_FUNNEL_STATUS_SESSIONS_VIEW_BUILDER.build_and_print()
