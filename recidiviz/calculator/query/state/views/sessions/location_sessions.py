# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Sessionized view of each individual. Session defined as continuous time in a given facility or district office"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

LOCATION_SESSIONS_VIEW_NAME = "location_sessions"

LOCATION_SESSIONS_VIEW_DESCRIPTION = """Sessionized view of each individual. Session defined as continuous stay associated with a given location. Location sessions may be overlapping."""

LOCATION_SESSIONS_QUERY_TEMPLATE = """
    /*{description}*/
    WITH sub_sessions_attributes_unnested AS (
    SELECT DISTINCT
        state_code, person_id, 
        session_attributes.compartment_location AS location,
        start_date,
        end_date,
        dataflow_session_id,
    FROM `{project_id}.{sessions_dataset}.dataflow_sessions_materialized`,
    UNNEST(session_attributes) session_attributes
    )
    SELECT state_code, person_id, location, location_session_id, MIN(start_date) start_date,
    CASE WHEN LOGICAL_AND(end_date IS NOT NULL) THEN MAX(end_date) END AS end_date,
    FROM (
    -- If any location in the current session was not present in the preceding session, count a location change
    SELECT *, 
        SUM(CASE WHEN COALESCE(location_changed, 1) = 1 THEN 1 ELSE 0 END) 
            OVER(PARTITION BY person_id ORDER BY start_date) AS location_session_id
    FROM (
        SELECT session.state_code, session.person_id, session.location,
            session.start_date, session.end_date,
            -- Only count a location toward an location change once if location was not present at all in preceding session
            MIN(IF(session_lag.location = session.location, 0, 1)) AS location_changed
        FROM sub_sessions_attributes_unnested session
        LEFT JOIN sub_sessions_attributes_unnested as session_lag
            ON session.state_code = session_lag.state_code
            AND session.person_id = session_lag.person_id
            AND session.dataflow_session_id = session_lag.dataflow_session_id + 1
            AND session.start_date = DATE_ADD(session_lag.end_date, INTERVAL 1 DAY)
        GROUP BY state_code, person_id, location, start_date, end_date
        )
    )
    GROUP BY state_code, person_id, location, location_session_id
    """

LOCATION_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=LOCATION_SESSIONS_VIEW_NAME,
    view_query_template=LOCATION_SESSIONS_QUERY_TEMPLATE,
    description=LOCATION_SESSIONS_VIEW_DESCRIPTION,
    sessions_dataset=SESSIONS_DATASET,
    clustering_fields=["state_code", "person_id"],
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        LOCATION_SESSIONS_VIEW_BUILDER.build_and_print()
