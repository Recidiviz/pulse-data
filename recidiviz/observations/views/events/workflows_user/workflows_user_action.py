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
"""View with events where the officer took a specific Workflows action, e.g., FORM_COPIED"""
from recidiviz.observations.event_observation_big_query_view_builder import (
    EventObservationBigQueryViewBuilder,
)
from recidiviz.observations.event_type import EventType
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_DESCRIPTION = (
    "Events where the officer took a specific Workflows action, e.g., FORM_COPIED"
)

_SOURCE_DATA_QUERY_TEMPLATE = f"""
SELECT
    state_code,
    email AS email_address,
    event_type,
    opportunity_type,
    b.completion_event_type AS task_type,
    c.system_type,
    launches.first_access_date IS NOT NULL AS task_type_is_live,
    IFNULL(launches.is_fully_launched, FALSE) AS task_type_is_fully_launched,
    person_external_id,
    new_status,
    event_ts
FROM
    `{{project_id}}.analyst_data.workflows_officer_events_materialized` a
INNER JOIN
    `{{project_id}}.reference_views.workflows_opportunity_configs_materialized` b
USING
    (state_code, opportunity_type)
INNER JOIN
    `{{project_id}}.reference_views.completion_event_type_metadata_materialized` c
USING
    (completion_event_type)
LEFT JOIN
    `{{project_id}}.analyst_data.workflows_live_completion_event_types_by_state_materialized` launches
USING
    (state_code, completion_event_type)
WHERE event = "{EventType.WORKFLOWS_USER_ACTION.value}"
"""

VIEW_BUILDER: EventObservationBigQueryViewBuilder = EventObservationBigQueryViewBuilder(
    event_type=EventType.WORKFLOWS_USER_ACTION,
    description=_VIEW_DESCRIPTION,
    sql_source=_SOURCE_DATA_QUERY_TEMPLATE,
    attribute_cols=[
        "event_type",
        "opportunity_type",
        "task_type",
        "system_type",
        "task_type_is_live",
        "task_type_is_fully_launched",
        "person_external_id",
        "new_status",
    ],
    event_date_col="event_ts",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
