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
"""View with events where a caseload was used in the search bar to surface
eligible clients in the tool
"""
from recidiviz.observations.event_observation_big_query_view_builder import (
    EventObservationBigQueryViewBuilder,
)
from recidiviz.observations.event_type import EventType
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_DESCRIPTION = (
    "View with events where a caseload was used in the search bar to surface "
    "eligible clients in the tool"
)

_SOURCE_DATA_QUERY_TEMPLATE = """
SELECT
    a.state_code,
    b.completion_event_type AS task_type,
    DATETIME(timestamp) AS event_date,
    CASE search_field
        WHEN "officerId" THEN 
            IFNULL(c.external_id, d.id)
        ELSE a.search_id_value
    END AS caseload_id,
FROM
    `{project_id}.workflows_views.clients_surfaced` a
INNER JOIN
    `{project_id}.reference_views.workflows_opportunity_configs_materialized` b
USING
    (state_code, opportunity_type)
-- Join to product staff view to translate caseload id to officer id
-- for supervision tools with officer search
LEFT JOIN
    `{project_id}.reference_views.product_staff_materialized` c
ON
    a.state_code = c.state_code
    AND a.search_id_value = c.pseudonymized_id
    AND search_field = "officerId"
    AND b.person_record_type = "CLIENT"

-- Join to product staff view to translate caseload id to officer id
-- for incarceration tools with officer search

-- TODO(#35053) Remove once roster sync is available for facilities staff
-- and the product_staff view includes non-supervision staff.
LEFT JOIN
    `{project_id}.workflows_views.incarceration_staff_record_materialized` d
ON
    a.state_code = d.state_code
    AND a.search_id_value = d.pseudonymized_id
    AND search_field = "officerId"
    AND b.person_record_type = "RESIDENT"
WHERE
    a.search_id_value IS NOT NULL 
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY state_code, task_type, timestamp, search_id_value
    ORDER BY c.start_date DESC
) = 1
"""

VIEW_BUILDER: EventObservationBigQueryViewBuilder = EventObservationBigQueryViewBuilder(
    event_type=EventType.WORKFLOWS_CASELOAD_SURFACED,
    description=_VIEW_DESCRIPTION,
    sql_source=_SOURCE_DATA_QUERY_TEMPLATE,
    attribute_cols=["task_type"],
    event_date_col="event_date",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
