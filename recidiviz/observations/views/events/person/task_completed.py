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
"""View with task completion events for all Workflows opportunities"""

from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.observations.event_observation_big_query_view_builder import (
    EventObservationBigQueryViewBuilder,
)
from recidiviz.observations.event_type import EventType
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_DESCRIPTION = "Task completion events for all Workflows opportunities"

_SOURCE_DATA_QUERY_TEMPLATE = f"""
SELECT
    e.state_code,
    e.person_id,
    e.completion_event_date,
    e.completion_event_type AS task_type,
    t.is_eligible,
    IF(t.is_eligible, DATE_DIFF(e.completion_event_date, t.start_date, DAY), NULL) AS days_eligible,
    -- eligible_for_over_7_days is used to calculate metrics on task completions that happen after more than 7 days of eligibility
    IF(t.is_eligible, DATE_DIFF(e.completion_event_date, t.start_date, DAY) > 7, FALSE) as eligible_for_over_7_days,
    t.is_almost_eligible,
    CASE
    -- If no TES spans overlap with this completion event, the reason for ineligibility is that the person was not included in the candidate population
        WHEN (t.is_eligible = False OR t.is_eligible IS NULL) AND a_t.ineligible_criteria IS NULL THEN 'NOT_IN_CANDIDATE_POPULATION'
        ELSE a_t.ineligible_criteria
    END AS ineligible_criteria,
    -- Flag if someone has experienced at least one tool action before task completion
    COALESCE(f.surfaced, FALSE) AS after_tool_action,
    c.system_type,
    c.decarceral_impact_type,
    c.is_jii_decarceral_transition,
    c.has_mandatory_due_date,
    launches.first_access_date IS NOT NULL AS task_type_is_live,
    IFNULL(launches.is_fully_launched, FALSE) AS task_type_is_fully_launched,
    IFNULL(e.completion_event_date >= launches.launch_date, FALSE) AS is_after_full_state_launch,
FROM
    `{{project_id}}.task_eligibility.all_completion_events_materialized` e
INNER JOIN
    `{{project_id}}.reference_views.completion_event_type_metadata_materialized` c
USING
    (completion_event_type)
LEFT JOIN
    `{{project_id}}.analyst_data.workflows_live_completion_event_types_by_state_materialized` launches
USING
    (state_code, completion_event_type)
-- Get information about continuous spans of eligibility
LEFT JOIN
    `{{project_id}}.analyst_data.all_task_type_eligibility_spans_materialized` t
ON
    e.person_id = t.person_id
    AND e.completion_event_type = t.task_type
    AND e.completion_event_date BETWEEN t.start_date AND {nonnull_end_date_clause("t.end_date")}

LEFT JOIN 
    `{{project_id}}.analyst_data.all_task_type_ineligible_criteria_sessions_materialized` a_t
ON
    e.completion_event_type = a_t.completion_event_type
    AND e.state_code = a_t.state_code
    AND e.person_id = a_t.person_id
    AND e.completion_event_date BETWEEN a_t.start_date AND {nonnull_end_date_clause("a_t.end_date")}
-- Get information about impact funnel status.
-- We convert the completion event date to a timestamp having the last time (23:59:59) on that date,
-- to account for any usage events that may have occurred on the same date.
LEFT JOIN
    `{{project_id}}.analyst_data.workflows_person_impact_funnel_status_sessions_materialized` f
ON
    e.person_id = f.person_id
    AND e.completion_event_type = f.task_type
    AND DATETIME_SUB(DATETIME(DATE_ADD(e.completion_event_date, INTERVAL 1 DAY)), INTERVAL 1 SECOND)
        BETWEEN f.start_date AND {nonnull_end_date_clause("f.end_date_exclusive")}
-- If completion event falls exactly on the border between two sessions, take the eligibility
-- attributes associated with the first span (i.e., the span that ended, rather than the span that started).
-- Uses RANK so that if there are multiple completion events for the same task type on the same day,
-- (e.g., multiple assessments), we don't drop these.
QUALIFY
    RANK() OVER (
        PARTITION BY e.person_id, e.completion_event_date, e.completion_event_type
        ORDER BY t.start_date, f.start_date, a_t.start_date
    ) = 1
"""

VIEW_BUILDER: EventObservationBigQueryViewBuilder = EventObservationBigQueryViewBuilder(
    event_type=EventType.TASK_COMPLETED,
    description=_VIEW_DESCRIPTION,
    sql_source=_SOURCE_DATA_QUERY_TEMPLATE,
    attribute_cols=[
        "task_type",
        "system_type",
        "decarceral_impact_type",
        "is_jii_decarceral_transition",
        "has_mandatory_due_date",
        "task_type_is_live",
        "task_type_is_fully_launched",
        "is_after_full_state_launch",
        "is_eligible",
        "days_eligible",
        "eligible_for_over_7_days",
        "is_almost_eligible",
        "after_tool_action",
        "ineligible_criteria",
    ],
    event_date_col="completion_event_date",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
