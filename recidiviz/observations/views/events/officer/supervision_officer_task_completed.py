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
"""View with spans of time over which an officer had at least one client eligible
for an opportunity.
"""
from recidiviz.calculator.query.bq_utils import (
    list_to_query_string,
    nonnull_end_date_clause,
)
from recidiviz.observations.event_observation_big_query_view_builder import (
    EventObservationBigQueryViewBuilder,
)
from recidiviz.observations.event_type import EventType
from recidiviz.observations.views.events.person import task_completed
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# Re-use the task-specific attribute columns from the task eligibility session spans
_EVENT_ATTRIBUTE_COLUMNS = task_completed.VIEW_BUILDER.attribute_cols

_SOURCE_DATA_QUERY_TEMPLATE = f"""
    SELECT
        assignments.state_code,
        supervising_officer_external_id AS officer_id,
        event_date,
        -- Include all of the attributes from the person-level task_completed event
        {list_to_query_string(_EVENT_ATTRIBUTE_COLUMNS)},
        -- Indicate if the client was eligible OR almost eligible when the task was completed
        (is_eligible = "true" OR is_almost_eligible = "true") AS is_eligible_or_almost_eligible,
    FROM
        `{{project_id}}.sessions.supervision_officer_sessions_materialized` assignments
    INNER JOIN
        `{{project_id}}.observations__person_event.task_completed_materialized` events
    ON
        assignments.state_code = events.state_code
        AND assignments.person_id = events.person_id
        -- Join tasks that were completed between the 1st day and the last day (inclusive)
        -- of the officer assignment session
        AND events.event_date
            BETWEEN assignments.start_date AND {nonnull_end_date_clause("assignments.end_date_exclusive")}
"""

VIEW_BUILDER: EventObservationBigQueryViewBuilder = EventObservationBigQueryViewBuilder(
    event_type=EventType.SUPERVISION_OFFICER_TASK_COMPLETED,
    description=__doc__,
    sql_source=_SOURCE_DATA_QUERY_TEMPLATE,
    attribute_cols=_EVENT_ATTRIBUTE_COLUMNS + ["is_eligible_or_almost_eligible"],
    event_date_col="event_date",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
