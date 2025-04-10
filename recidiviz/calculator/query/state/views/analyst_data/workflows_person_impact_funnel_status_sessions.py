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
"""Sessionized view of eligibility and Workflows tool engagement at the person-level"""


from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.state.views.analyst_data.funnel_status_sessions_big_query_view_builder import (
    FunnelStatusEventQueryBuilder,
    FunnelStatusSessionsViewBuilder,
    FunnelStatusSpanQueryBuilder,
)
from recidiviz.calculator.query.state.views.analyst_data.workflows_person_events import (
    USAGE_EVENTS_DICT,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_NAME = "workflows_person_impact_funnel_status_sessions"

_VIEW_DESCRIPTION = (
    "Sessionized view of eligibility and "
    "Workflows tool engagement at the person-level"
)

# Get the subset of usage events that occur as discrete events rather than spans
# This exclude MARKED_INELIGIBLE/snooze span starts, since those can be handled
# directly via client snooze spans.
DISCRETE_USAGE_EVENTS_DICT = {
    event: selector
    for event, selector in USAGE_EVENTS_DICT.items()
    if event not in ("MARKED_INELIGIBLE", "IN_PROGRESS")
}

SYSTEM_SESSIONS_CTE_TEMPLATE = """
WITH distinct_task_types AS (
    SELECT DISTINCT
        state_code,
        task_type,
    FROM
        `{project_id}.analyst_data.all_task_type_eligibility_spans_materialized`
)
# Get spans of justice involvement among states, and unnest across all task types live in a given state
SELECT *, TRUE AS is_justice_involved,
FROM
    `{project_id}.sessions.system_sessions_materialized`
INNER JOIN
    distinct_task_types
USING
    (state_code)
"""

WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSIONS_VIEW_BUILDER = FunnelStatusSessionsViewBuilder(
    view_id=_VIEW_NAME,
    description=_VIEW_DESCRIPTION,
    index_cols=["state_code", "person_id", "task_type"],
    funnel_reset_dates_sql_source=f"""
    # End all usage status spans when someone's eligibility status becomes newly eligible
    SELECT
        state_code,
        person_id,
        task_type,
        start_date AS funnel_reset_date,
    FROM
        `{{project_id}}.analyst_data.all_task_type_eligibility_spans_materialized`
    WHERE
        eligibility_reset
        -- Exclude future eligibility spans
        AND start_date <= CURRENT_DATE("US/Eastern")

    UNION ALL

    # End all usage status spans when someone begins a new session in the criminal justice system
    SELECT
        state_code,
        person_id,
        task_type,
        start_date AS funnel_reset_date,
    FROM
        ({SYSTEM_SESSIONS_CTE_TEMPLATE})
""",
    funnel_status_query_builders=[
        # Justice Involved
        FunnelStatusSpanQueryBuilder(
            sql_source=SYSTEM_SESSIONS_CTE_TEMPLATE,
            start_date_col="start_date",
            end_date_exclusive_col="end_date_exclusive",
            index_cols=["state_code", "person_id", "task_type"],
            status_cols_by_type=[
                ("is_justice_involved", bigquery.enums.StandardSqlTypeNames.BOOL)
            ],
            truncate_spans_at_reset_dates=False,
        ),
        # Eligibility
        FunnelStatusSpanQueryBuilder(
            sql_source="""SELECT
    state_code,
    person_id,
    task_type,
    start_date,
    CASE WHEN end_date > CURRENT_DATE("US/Eastern") THEN NULL ELSE end_date END AS end_date,
    is_eligible,
    is_almost_eligible,
FROM
    `{project_id}.analyst_data.all_task_type_eligibility_spans_materialized`
WHERE start_date <= CURRENT_DATE("US/Eastern")""",
            start_date_col="start_date",
            end_date_exclusive_col="end_date",
            index_cols=["state_code", "person_id", "task_type"],
            status_cols_by_type=[
                ("is_eligible", bigquery.enums.StandardSqlTypeNames.BOOL),
                ("is_almost_eligible", bigquery.enums.StandardSqlTypeNames.BOOL),
            ],
            truncate_spans_at_reset_dates=False,
        ),
        # Eligible >30 days
        FunnelStatusSpanQueryBuilder(
            sql_source="""SELECT
    state_code,
    person_id,
    task_type,
    -- This span starts 30 days after the source eligibility span begins
    DATE_ADD(start_date, INTERVAL 30 DAY) as start_date,
    CASE WHEN end_date > CURRENT_DATE("US/Eastern") THEN NULL ELSE end_date END AS end_date,
    TRUE as is_eligible_past_30_days,
FROM
    `{project_id}.analyst_data.all_task_type_eligibility_spans_materialized`
WHERE start_date <= CURRENT_DATE("US/Eastern") AND is_eligible AND (
    -- Eligibility span has ended
    (end_date <= CURRENT_DATE("US/Eastern") AND DATE_DIFF(end_date, start_date, DAY) > 30) OR
    -- Span is open
    (end_date is NULL AND DATE_DIFF(CURRENT_DATE("US/Eastern"), start_date, DAY) > 30) OR
    -- Span ends in future
    (end_date > CURRENT_DATE("US/Eastern") AND DATE_DIFF(CURRENT_DATE("US/Eastern"), start_date, DAY) > 30)
)
""",
            start_date_col="start_date",
            end_date_exclusive_col="end_date",
            index_cols=["state_code", "person_id", "task_type"],
            status_cols_by_type=[
                ("is_eligible_past_30_days", bigquery.enums.StandardSqlTypeNames.BOOL)
            ],
            truncate_spans_at_reset_dates=False,
        ),
        # Surfaceability
        FunnelStatusSpanQueryBuilder(
            sql_source="""
    SELECT
        state_code,
        person_id,
        b.completion_event_type AS task_type,
        start_date,
        end_date_exclusive,
        TRUE AS is_surfaceable,
    FROM
        `{project_id}.analyst_data.workflows_record_archive_surfaceable_person_sessions_materialized` a
    INNER JOIN
        `{project_id}.reference_views.workflows_opportunity_configs_materialized` b
    USING
        (state_code, opportunity_type)
            """,
            start_date_col="start_date",
            end_date_exclusive_col="end_date_exclusive",
            index_cols=["state_code", "person_id", "task_type"],
            status_cols_by_type=[
                ("is_surfaceable", bigquery.enums.StandardSqlTypeNames.BOOL)
            ],
            truncate_spans_at_reset_dates=False,
        ),
        # Usage
        *[
            FunnelStatusEventQueryBuilder(
                sql_source=f"""SELECT DISTINCT
    state_code,
    person_id,
    task_type,
    start_date,
    TRUE AS {event.lower()},
FROM
    `{{project_id}}.analyst_data.workflows_person_events_materialized`
WHERE
    usage_event_type = "{event}"
    AND start_date <= CURRENT_DATE("US/Eastern")""",
                event_date_col="start_date",
                index_cols=["state_code", "person_id", "task_type"],
                status_cols_by_type=[
                    (event.lower(), bigquery.enums.StandardSqlTypeNames.BOOL)
                ],
            )
            for event in DISCRETE_USAGE_EVENTS_DICT
        ],
        # Marked Ineligible
        FunnelStatusSpanQueryBuilder(
            sql_source=f"""SELECT
    state_code,
    person_id,
    task_type,
    CAST(start_date AS DATE) AS start_date,
    IF(CAST(end_date_exclusive AS DATE) > CURRENT_DATE("US/Eastern"), NULL, CAST(end_date_exclusive AS DATE)) AS end_date_exclusive,
    TRUE AS marked_ineligible,
    denial_reasons,
FROM
    `{{project_id}}.analyst_data.all_task_type_marked_ineligible_spans_materialized`WHERE
    # Drop zero day spans (indicating multiple updates in 1 day) and pick
    # the status as of the end of day
    CAST(start_date AS DATE) != {nonnull_end_date_clause("CAST(end_date_exclusive AS DATE)")}""",
            start_date_col="start_date",
            end_date_exclusive_col="end_date_exclusive",
            index_cols=["state_code", "person_id", "task_type"],
            status_cols_by_type=[
                ("marked_ineligible", bigquery.enums.StandardSqlTypeNames.BOOL),
                ("denial_reasons", bigquery.enums.StandardSqlTypeNames.STRING),
            ],
            truncate_spans_at_reset_dates=False,
        ),
        # Marked Submitted
        FunnelStatusSpanQueryBuilder(
            sql_source=f"""SELECT
    state_code,
    person_id,
    task_type,
    CAST(start_date AS DATE) AS start_date,
    IF(CAST(end_date AS DATE) > CURRENT_DATE("US/Eastern"), NULL, CAST(end_date AS DATE)) AS end_date,
    TRUE AS in_progress,
FROM
    `{{project_id}}.analyst_data.all_task_type_marked_submitted_spans_materialized`
WHERE
    # Drop zero day spans (indicating multiple updates in 1 day) and pick
    # the status as of the end of day
    CAST(start_date AS DATE) != {nonnull_end_date_clause("CAST(end_date AS DATE)")}""",
            start_date_col="start_date",
            end_date_exclusive_col="end_date",
            index_cols=["state_code", "person_id", "task_type"],
            status_cols_by_type=[
                ("in_progress", bigquery.enums.StandardSqlTypeNames.BOOL)
            ],
            truncate_spans_at_reset_dates=False,
        ),
        # Task Completed
        FunnelStatusEventQueryBuilder(
            sql_source="""
    SELECT
        state_code,
        person_id,
        completion_event_type AS task_type,
        completion_event_date,
        TRUE AS task_completed,
    FROM
        `{project_id}.task_eligibility.all_completion_events_materialized`
    WHERE completion_event_date <= CURRENT_DATE("US/Eastern")
""",
            event_date_col="completion_event_date",
            index_cols=["state_code", "person_id", "task_type"],
            status_cols_by_type=[
                ("task_completed", bigquery.enums.StandardSqlTypeNames.BOOL)
            ],
        ),
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSIONS_VIEW_BUILDER.build_and_print()
