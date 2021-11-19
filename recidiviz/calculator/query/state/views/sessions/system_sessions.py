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
"""An aggregated view of sessions that shows continuous stays within the system (supervision or incarceration). A
 session in a RELEASE compartment triggers the start of a new system session."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SYSTEM_SESSIONS_VIEW_NAME = "system_sessions"

SYSTEM_SESSIONS_VIEW_DESCRIPTION = """
## Overview

This view is an aggregated view of sessions that represent a continuous stay within the 
system. A `RELEASE` session triggers the start of a new system session. This view is 
unique on person and system session id.

As an example, say a person had the following compartment sessions:

1. `INCARCERATION - GENERAL`
2. `SUPERVISION - PAROLE`
3. `INCARCERATION - GENERAL`
4. `SUPERVISION - PAROLE`
5. `RELEASE`
6. `SUPERVISION - PROBATION`
7. `RELEASE`

This person would have two system sessions - one that encompasses compartment sessions 
1 - 4 and one that encompasses compartment session 6. 

## Field Definitions

|	Field	|	Description	|
|	--------------------	|	--------------------	|
|	state_code	|	State code of the person	|
|	person_id	|	Unique person identifier	|
|	system_session_id	|	System session identifier. A new ID is triggered based on a RELEASE compartment session	|
|	start_date	|	System session start date	|
|	end_date	|	System session end date	|
|	incarceration_days	|	Days of system session spent in incarceration	|
|	supervision_days	|	Days of system session spent under supervision	|
|	session_length_days	|	Total length of system session	|
|	session_id_start	|	Compartment session id associated with the start of the system session	|
|	session_id_end	|	Compartment session id associated with the end of the system session	|

"""

SYSTEM_SESSIONS_QUERY_TEMPLATE = """
    /*{description}*/
    SELECT
        state_code,
        person_id,
        system_session_id,
        MIN(start_date) AS start_date,
        CASE WHEN LOGICAL_AND(end_date IS NOT NULL) THEN MAX(end_date) END AS end_date,
        SUM(CASE WHEN compartment_level_1 = 'INCARCERATION' 
            THEN session_length_days ELSE 0 END) incarceration_days,
        SUM(CASE WHEN compartment_level_1 = 'SUPERVISION' 
            THEN session_length_days ELSE 0 END) supervision_days,
        SUM(session_length_days) AS session_length_days,
        MIN(session_id) AS session_id_start,
        MAX(session_id) AS session_id_end,
    FROM  
        (
        SELECT 
            *,
            SUM(CASE WHEN compartment_level_1 = 'RELEASE' THEN 1 ELSE 0 END) 
                OVER(PARTITION BY state_code, person_id ORDER BY start_date) + 1 
                AS system_session_id
        FROM `{project_id}.{sessions_dataset}.compartment_sessions_materialized` 
        )
    WHERE compartment_level_1 != 'RELEASE'
    GROUP BY 1, 2, 3
    """

SYSTEM_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=SYSTEM_SESSIONS_VIEW_NAME,
    view_query_template=SYSTEM_SESSIONS_QUERY_TEMPLATE,
    description=SYSTEM_SESSIONS_VIEW_DESCRIPTION,
    sessions_dataset=SESSIONS_DATASET,
    clustering_fields=["state_code", "person_id"],
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SYSTEM_SESSIONS_VIEW_BUILDER.build_and_print()
