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
"""View with Workflows usage event tracked at the client level"""
from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause
from recidiviz.calculator.query.state.views.analyst_data.workflows_person_events import (
    USAGE_EVENTS_DICT,
)
from recidiviz.observations.event_observation_big_query_view_builder import (
    EventObservationBigQueryViewBuilder,
)
from recidiviz.observations.event_type import EventType
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_DESCRIPTION = "Workflows usage event tracked at the client level"

_SOURCE_DATA_QUERY_TEMPLATE = f"""
SELECT
    a.state_code,
    a.person_id,
    a.start_date,
    a.task_type,
    usage_event_type,
    -- Flag if this is the first tool action for this person/task within
    -- the impact funnel session, so that funnel resets are properly incorporated
    (
        -- The event must fall on a status session boundary
        b.start_date IS NOT NULL
        -- The previous status session must be FALSE for all usage statuses
        AND NOT {" AND NOT ".join(["prev_status." + usage_event.lower() for usage_event in USAGE_EVENTS_DICT])}
        -- Only 1 event on the first tool action day can be counted as the first action,
        -- with surfaced events prioritized against other event types
        AND ROW_NUMBER() OVER (
            PARTITION BY a.person_id, a.task_type, a.start_date
            ORDER BY a.start_date, IF(usage_event_type = "SURFACED", 0, 1), usage_event_type
        ) = 1
    ) AS is_first_tool_action,
    DATE_DIFF(a.start_date, c.start_date, DAY) days_eligible,
    d.system_type,
    d.decarceral_impact_type,
    d.is_jii_decarceral_transition,
    d.has_mandatory_due_date,
    launches.first_access_date IS NOT NULL AS task_type_is_live,
    IFNULL(launches.is_fully_launched, FALSE) AS task_type_is_fully_launched,
FROM
    `{{project_id}}.analyst_data.workflows_person_events_materialized` a
LEFT JOIN
    `{{project_id}}.analyst_data.workflows_person_impact_funnel_status_sessions_materialized` b
USING
    (person_id, task_type, start_date)
LEFT JOIN
    `{{project_id}}.analyst_data.workflows_person_impact_funnel_status_sessions_materialized` prev_status
ON
    b.state_code = prev_status.state_code
    AND b.person_id = prev_status.person_id
    AND b.start_date = prev_status.end_date
    AND b.task_type = prev_status.task_type
LEFT JOIN
    `{{project_id}}.analyst_data.all_task_type_eligibility_spans_materialized` c
ON
    a.person_id = c.person_id
    AND a.task_type = c.task_type
    AND c.is_eligible
    -- Cast to a date to account for date comparison with timestamps
    AND DATE(a.start_date) BETWEEN c.start_date AND {nonnull_end_date_exclusive_clause("c.end_date")}
INNER JOIN
    `{{project_id}}.reference_views.completion_event_type_metadata_materialized` d
ON
    a.task_type = d.completion_event_type
LEFT JOIN
    `{{project_id}}.analyst_data.workflows_live_completion_event_types_by_state_materialized` launches
ON
    d.completion_event_type = launches.completion_event_type
    AND a.state_code = launches.state_code
"""

VIEW_BUILDER: EventObservationBigQueryViewBuilder = EventObservationBigQueryViewBuilder(
    event_type=EventType.WORKFLOWS_PERSON_USAGE_EVENT,
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
        "usage_event_type",
        "is_first_tool_action",
        "days_eligible",
    ],
    event_date_col="start_date",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
