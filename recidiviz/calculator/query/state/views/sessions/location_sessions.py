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
    WITH sub_sessions_attributes_unnested AS 
    (
    SELECT DISTINCT
        state_code,
        person_id, 
        session_attributes.compartment_location AS location,
        session_attributes.facility,
        session_attributes.supervision_office,
        session_attributes.supervision_district,
        start_date,
        end_date,
        dataflow_session_id,
    FROM `{project_id}.{sessions_dataset}.dataflow_sessions_materialized`,
    UNNEST(session_attributes) session_attributes
    )
    ,
    sessionized_cte AS
    (
    SELECT 
        state_code, 
        person_id,
        location,
        facility,
        supervision_office,
        supervision_district,
        location_session_id_unordered,
        MIN(start_date) start_date,
        CASE WHEN LOGICAL_AND(end_date IS NOT NULL) THEN MAX(end_date) END AS end_date,
        FROM
            (
            /*
            This subquery creates an identifier that is used to group together adjacent sessions of the same location type.
            The partition is done on person_id and then ordered by location and then start date. We look across all sessions
            in a given location type and create a new id if the location changes from the preceding session.
            */
            -- If any location in the current session was not present in the preceding session, count a location change
            SELECT 
                *, 
                SUM(CASE WHEN COALESCE(location_changed, 1) = 1 THEN 1 ELSE 0 END) 
                    OVER(PARTITION BY person_id, state_code ORDER BY location, start_date) AS location_session_id_unordered
            FROM 
                (
                /*
                This subquery does a self join to identify cases where a person's location has changed. This is more complicated
                that looking at the preceding session because a person can have multiple locations simultaneously. Therefore,
                we join in sessions based on the end_date being one day prior to the session start_date. Because of the way 
                that dataflow sessions are constructed, this will capture the location that a person was in prior to the current 
                session, and then we check whether or not any of those joining sessions has the same location value. 
                Dataflow sessions maintains non-overlaps and is defined along boundaries of any session attribute changing, which means
                that start date will match the preceding end date + 1 (if there is a preceding session). If we were to instead be constructing
                this with sessions that truly overlapped, then I believe we would sub in the join condition with 
                `AND DATE_SUB(session.start_date, INTERVAL 1 DAY) BETWEEN session_lag.start_date AND COALESCE(session_lag.end_date, '9999-01-01')`
                */
                SELECT 
                    session.state_code, 
                    session.person_id, 
                    session.location,
                    session.facility,
                    session.supervision_office,
                    session.supervision_district,
                    session.start_date,
                    session.end_date,
                    -- Only count a location toward an location change once if location was not present at all in preceding session
                    MIN(IF(session_lag.location = session.location, 0, 1)) AS location_changed
                FROM sub_sessions_attributes_unnested session
                LEFT JOIN sub_sessions_attributes_unnested as session_lag
                    ON session.state_code = session_lag.state_code
                    AND session.person_id = session_lag.person_id
                    AND session.start_date = DATE_ADD(session_lag.end_date, INTERVAL 1 DAY)
                GROUP BY 1,2,3,4,5,6,7,8
                )
            )           
    GROUP BY 1,2,3,4,5,6,7
    )
    /*
    The grouping ID that is created isn't in the order that we want. This last step creates an ID for each session within each 
    person_id based on start_date, then end_date, and then sorted location name.
    */
    SELECT 
        * EXCEPT(location_session_id_unordered),
        ROW_NUMBER() OVER(PARTITION BY person_id, state_code ORDER BY start_date, COALESCE(end_date,'9999-01-01'), location) AS location_session_id
    FROM sessionized_cte 
    ORDER BY location_session_id
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
