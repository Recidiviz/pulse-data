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
"""Sessionized view of each client's associations with a supervision district. District
sessions may be overlapping if a client has more than one concurrent supervision
sentence."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

DISTRICT_SESSIONS_VIEW_NAME = "district_sessions"

DISTRICT_SESSIONS_VIEW_DESCRIPTION = """
Sessionized view of each client's associations with a supervision district. District
sessions may be overlapping if a client has more than one concurrent supervision
sentence.

Each row marks the start of a new district session. Sessions end when the client
is no longer serving any supervision sentence associated with a given district
for at least one day. Order of sessions is determined first by start date, then
end date, then alphabetical order of the district-office.
"""


DISTRICT_SESSIONS_QUERY_TEMPLATE = """
/*{description}*/
WITH sub_sessions_attributes_unnested AS 
(
SELECT DISTINCT
    state_code,
    person_id, 
    session_attributes.supervision_district AS district,
    start_date,
    end_date,
    dataflow_session_id,
FROM `{project_id}.{sessions_dataset}.dataflow_sessions_materialized`,
UNNEST(session_attributes) session_attributes
WHERE session_attributes.supervision_district IS NOT NULL
    AND session_attributes.compartment_level_1 != 'INCARCERATION_NOT_INCLUDED_IN_STATE'
)
,
sessionized_cte AS
(
SELECT 
    state_code, 
    person_id,
    district,
    district_session_id_unordered,
    MIN(start_date) start_date,
    CASE WHEN LOGICAL_AND(end_date IS NOT NULL) THEN MAX(end_date) END AS end_date,
    MIN(dataflow_session_id) AS dataflow_session_id_start,
    MAX(dataflow_session_id) AS dataflow_session_id_end,
    FROM
        (
        SELECT 
            *, 
            SUM(CASE WHEN COALESCE(district_changed, 1) = 1 THEN 1 ELSE 0 END) 
                OVER(PARTITION BY person_id, state_code ORDER BY district, start_date) AS district_session_id_unordered
        FROM 
            (
            SELECT 
                session.state_code, 
                session.person_id, 
                session.district,
                session.start_date, 
                session.end_date,
                -- Only count a district toward an district change once if district was not present at all in preceding session,
                session.dataflow_session_id,
                MIN(IF(session_lag.district = session.district, 0, 1)) AS district_changed
            FROM sub_sessions_attributes_unnested session
            LEFT JOIN sub_sessions_attributes_unnested as session_lag
                ON session.state_code = session_lag.state_code
                AND session.person_id = session_lag.person_id
                AND session.start_date = DATE_ADD(session_lag.end_date, INTERVAL 1 DAY)
            GROUP BY 1,2,3,4,5,6
            )
        )           
GROUP BY 1,2,3,4
)
SELECT 
    *  EXCEPT(district_session_id_unordered),
    ROW_NUMBER() OVER(PARTITION BY person_id, state_code ORDER BY start_date, COALESCE(end_date,'9999-01-01'), district) AS district_session_id
FROM sessionized_cte 
ORDER BY district_session_id
"""
DISTRICT_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=DISTRICT_SESSIONS_VIEW_NAME,
    view_query_template=DISTRICT_SESSIONS_QUERY_TEMPLATE,
    description=DISTRICT_SESSIONS_VIEW_DESCRIPTION,
    sessions_dataset=SESSIONS_DATASET,
    clustering_fields=["state_code", "person_id"],
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        DISTRICT_SESSIONS_VIEW_BUILDER.build_and_print()
