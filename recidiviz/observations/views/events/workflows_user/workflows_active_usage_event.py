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
"""Event tracking all activity that qualifies a Workflows user as "active" """
from recidiviz.observations.event_observation_big_query_view_builder import (
    EventObservationBigQueryViewBuilder,
)
from recidiviz.observations.event_type import EventType
from recidiviz.observations.views.events.workflows_user.workflows_user_action import (
    VIEW_BUILDER as WORKFLOWS_USER_ACTION_VIEW_BUILDER,
)
from recidiviz.observations.views.events.workflows_user.workflows_user_client_status_update import (
    VIEW_BUILDER as WORKFLOWS_USER_CLIENT_STATUS_UPDATE_VIEW_BUILDER,
)
from recidiviz.observations.views.events.workflows_user.workflows_user_page import (
    VIEW_BUILDER as WORKFLOWS_USER_PAGE_VIEW_BUILDER,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_DESCRIPTION = (
    'Event tracking all activity that qualifies a Workflows user as "active"'
)

# TODO(#34498): Remove all JSON_EXTRACT_SCALAR from this query once single-event views
#  do not package attributes into JSON.
_SOURCE_DATA_QUERY_TEMPLATE = f"""
WITH combined_usage_events AS (
    SELECT
        state_code,
        email_address,
        event_date,
        JSON_EXTRACT_SCALAR(event_attributes, "$.event_type") AS event_type,
        JSON_EXTRACT_SCALAR(event_attributes, "$.task_type") AS task_type,
    FROM `{{project_id}}.{WORKFLOWS_USER_ACTION_VIEW_BUILDER.table_for_query.to_str()}`
    
    UNION ALL
    
    SELECT
        state_code,
        email_address,
        event_date,
        JSON_EXTRACT_SCALAR(event_attributes, "$.event_type") AS event_type,
        JSON_EXTRACT_SCALAR(event_attributes, "$.task_type") AS task_type,
    FROM `{{project_id}}.{WORKFLOWS_USER_CLIENT_STATUS_UPDATE_VIEW_BUILDER.table_for_query.to_str()}`
    
    UNION ALL
    
    SELECT
        state_code,
        email_address,
        event_date,
        JSON_EXTRACT_SCALAR(event_attributes, "$.event_type") AS event_type,
        JSON_EXTRACT_SCALAR(event_attributes, "$.task_type") AS task_type,
    FROM `{{project_id}}.{WORKFLOWS_USER_PAGE_VIEW_BUILDER.table_for_query.to_str()}`
    
)
SELECT
    state_code,
    email_address,
    event_date,
    event_type,
    task_type,
FROM combined_usage_events
"""

VIEW_BUILDER: EventObservationBigQueryViewBuilder = EventObservationBigQueryViewBuilder(
    event_type=EventType.WORKFLOWS_ACTIVE_USAGE_EVENT,
    description=_VIEW_DESCRIPTION,
    sql_source=_SOURCE_DATA_QUERY_TEMPLATE,
    attribute_cols=[
        "event_type",
        "task_type",
    ],
    event_date_col="event_date",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
