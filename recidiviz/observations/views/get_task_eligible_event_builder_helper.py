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
"""Helper that returns an EventObservationBigQueryViewBuilder for the specified
EventType.
"""
from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause
from recidiviz.observations.event_observation_big_query_view_builder import (
    EventObservationBigQueryViewBuilder,
)
from recidiviz.observations.event_type import EventType


def get_task_eligible_event_observation_view_builder(
    event_type: EventType, days_overdue: int
) -> EventObservationBigQueryViewBuilder:
    """
    Returns an EventObservationBigQueryViewBuilder for the specified EventType that
    represents observations with the date when a task-eligible client has been overdue
    for an opportunity by greater than `days_overdue` days. If `days_overdue` is set to
    0, returns an event on the date that eligibility began.
    """
    description_add_on = f" past {days_overdue} days ago" if days_overdue > 0 else ""
    date_condition_query_str = (
        f"""
        AND LEAST(
                IFNULL(a.end_date, "9999-01-01"),
                "9999-01-01"
            ) > DATE_ADD(a.start_date, INTERVAL {days_overdue} DAY)
    """
        if days_overdue > 0
        else ""
    )

    return EventObservationBigQueryViewBuilder(
        event_type=event_type,
        description=f"Opportunity eligibility starts{description_add_on}",
        sql_source=f"""
    SELECT
        a.state_code,
        a.person_id,
        DATE_ADD(a.start_date, INTERVAL {days_overdue} DAY) AS overdue_date,
        a.task_name,
        a.task_type,
        COALESCE(c.surfaced, FALSE) AS after_tool_action,
        -- Flag if person was previously almost eligible before becoming fully eligible
        IFNULL(prev_eligibility_span.is_almost_eligible, FALSE) AS after_almost_eligible,
        d.system_type,
        d.decarceral_impact_type,
        d.is_jii_decarceral_transition,
        d.has_mandatory_due_date,
        launches.first_access_date IS NOT NULL AS task_type_is_live,
        IFNULL(launches.is_fully_launched, FALSE) AS task_type_is_fully_launched,
    FROM
        `{{project_id}}.task_eligibility.all_tasks__collapsed_materialized` a
    -- Get information about impact funnel status.
    -- We convert the eligibility start date to a timestamp having the last time (23:59:59) on that date,
    -- to account for any usage events that may have occurred on the same date.
    LEFT JOIN
        `{{project_id}}.analyst_data.workflows_person_impact_funnel_status_sessions_materialized` c
    ON
        a.person_id = c.person_id
        AND a.task_type = c.task_type
        AND DATETIME_SUB(DATETIME(DATE_ADD(a.start_date, INTERVAL 1 DAY)), INTERVAL 1 SECOND)
            BETWEEN c.start_date AND {nonnull_end_date_exclusive_clause("c.end_date_exclusive")}
    INNER JOIN
        `{{project_id}}.reference_views.completion_event_type_metadata_materialized` d
    ON
        a.task_type = d.completion_event_type
    LEFT JOIN
        `{{project_id}}.analyst_data.workflows_live_completion_event_types_by_state_materialized` launches
    ON
        a.state_code = launches.state_code
        AND d.completion_event_type = launches.completion_event_type
    LEFT JOIN
        `{{project_id}}.task_eligibility.all_tasks__collapsed_materialized` prev_eligibility_span
    ON
        prev_eligibility_span.person_id = a.person_id
        AND prev_eligibility_span.task_type = a.task_type
        AND prev_eligibility_span.end_date = a.start_date
    WHERE
        -- remove any spans that start after the current millennium, e.g.
        -- eligibility following a life sentence
        a.start_date < "3000-01-01"
        AND a.is_eligible{date_condition_query_str}
    """,
        attribute_cols=[
            "task_name",
            "task_type",
            "system_type",
            "decarceral_impact_type",
            "is_jii_decarceral_transition",
            "has_mandatory_due_date",
            "task_type_is_live",
            "task_type_is_fully_launched",
            "after_tool_action",
            "after_almost_eligible",
        ],
        event_date_col="overdue_date",
    )
