# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Sessionized view of each individual on supervision.
Session defined as continuous time on caseload of a given supervising officer
in the same office."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_OFFICER_OFFICE_SESSIONS_VIEW_NAME = "supervision_officer_office_sessions"

SUPERVISION_OFFICER_OFFICE_SESSIONS_VIEW_DESCRIPTION = """
Sessionized view of each individual. Session defined as continuous stay on supervision 
associated with a given officer in the same office. Office sessions may be overlapping.
"""

SUPERVISION_OFFICER_OFFICE_SESSIONS_QUERY_TEMPLATE = """
/*{description}*/

WITH sub_sessions_attributes_unnested AS (
SELECT DISTINCT
    state_code,
    person_id, 
    dataflow_session_id,
    start_date,
    end_date,
    session_attributes.supervising_officer_external_id,
    session_attributes.supervision_office AS office,
    session_attributes.supervision_district AS district,
FROM
    `{project_id}.{sessions_dataset}.dataflow_sessions_materialized`,
    UNNEST(session_attributes) session_attributes
WHERE
    compartment_level_1 = "SUPERVISION"
    OR compartment_level_1 = "SUPERVISION_OUT_OF_STATE"
)

SELECT 
    state_code, 
    person_id,
    officer_office_session_id,
    supervising_officer_external_id,
    district,
    office,
    MIN(start_date) start_date,
    CASE WHEN LOGICAL_AND(end_date IS NOT NULL) THEN MAX(end_date) END AS end_date,
    MIN(dataflow_session_id) AS dataflow_session_id_start,
    MAX(dataflow_session_id) AS dataflow_session_id_end,
FROM (
    -- If any officer-office in the current session was not present in the preceding 
    -- session, count an officer-office change
    SELECT *, 
        SUM(
            CASE WHEN COALESCE(officer_office_changed, 1) = 1 THEN 1 ELSE 0 END) 
            OVER(PARTITION BY person_id ORDER BY start_date) 
            AS officer_office_session_id,
    FROM (
        SELECT
            session.state_code,
            session.person_id, 
            session.dataflow_session_id,
            session.start_date,
            session.end_date,
            session.supervising_officer_external_id,
            session.district,
            session.office,
            -- Only count an officer-office toward a change once if officer-office 
            -- was not present in preceding session
            MIN(IF(
                session_lag.supervising_officer_external_id = session.supervising_officer_external_id
                AND session_lag.district = session.district
                AND session_lag.office = session.office,
                0, 1)
            ) AS officer_office_changed
        FROM
            sub_sessions_attributes_unnested session
        LEFT JOIN 
            sub_sessions_attributes_unnested session_lag
        ON
            session.state_code = session_lag.state_code
            AND session.person_id = session_lag.person_id
            AND session.start_date = DATE_ADD(session_lag.end_date, INTERVAL 1 DAY)
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
    )
)
GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY 1, 2, 3
"""

SUPERVISION_OFFICER_OFFICE_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=SUPERVISION_OFFICER_OFFICE_SESSIONS_VIEW_NAME,
    view_query_template=SUPERVISION_OFFICER_OFFICE_SESSIONS_QUERY_TEMPLATE,
    description=SUPERVISION_OFFICER_OFFICE_SESSIONS_VIEW_DESCRIPTION,
    sessions_dataset=SESSIONS_DATASET,
    clustering_fields=["state_code", "person_id"],
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_OFFICER_OFFICE_SESSIONS_VIEW_BUILDER.build_and_print()
