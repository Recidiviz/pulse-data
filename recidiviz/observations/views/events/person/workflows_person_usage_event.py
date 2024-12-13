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
    -- Flag if this is the first tool action by checking if someone went from un-surfaced to surfaced at this datetime,
    COALESCE(
        b.surfaced AND NOT LAG(surfaced) OVER (
            PARTITION BY b.person_id, b.task_type
            ORDER BY b.start_date
        )
    , FALSE) AS is_first_tool_action,
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
