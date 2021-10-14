# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""View that aggregates across sessions with matching adjacent compartment_level_1 values in compartment_sessions"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

COMPARTMENT_LEVEL_1_SUPER_SESSIONS_VIEW_NAME = "compartment_level_1_super_sessions"

COMPARTMENT_LEVEL_1_SUPER_SESSIONS_VIEW_DESCRIPTION = """
## Overview

This view is unique on `person_id` and `compartment_level_1_super_session_id` and aggregates across adjacent `compartment_level_1` values. This means that incarceration sessions of different legal status that are adjacent to each other (`PAROLE_BOARD_HOLD` and `GENERAL`, for example) would be aggregated into the same `compartment_level_1` super session. Similarly, adjacent supervision sessions of different legal status would be grouped together as well. 

This view does not aggregate across in-state and out-of-state incarceration or supervision sessions.


## Field Definitions
|	Field	|	Description	|
|	--------------------	|	--------------------	|
|	person_id	|	Unique person identifier	|
|	compartment_level_1_super_session_id	|	Session id for the super session. Aggregates across compartment_level_1 values	|
|	compartment_level_1	|	Level 1 Compartment. Possible values are: <br>-`INCARCERATION`<br>-`INCARCERATION_OUT_OF_STATE` (inferred from location)<br>-`SUPERVISION`<br>-`SUPERVISION_OUT_OF_STATE`<br>-`RELEASE` (inferred from gap in data)<br>-`INTERNAL_UNKNOWN` (inferred from gap in data), <br>-`PENDING_CUSTODY` (inferred from gap in data)<br>-`PENDING_SUPERVISION` (inferred from gap in data)<br>-`SUSPENSION`<br>-`ERRONEOUS_RELEASE` (inferred from gap in data)	|
|	start_date	|	Super-session start date	|
|	end_date	|	Super-session end date	|
|	state_code	|	State	|
|	session_length_days	|	Difference between session start date and session end date. For active sessions the session start date is differenced from the last day of data	|
|	session_id_start	|	Compartment session id associated with the start of the super-session. This field and the following field are used to join sessions and super-sessions	|
|	session_id_end	|	Compartment session id associated with the end of the super-session. This field and the preceding field are used to join sessions and super-sessions	|
|	start_reason	|	Start reason associated with the start of a super-session. This is pulled from the compartment session represented by `session_id_start`	|
|	start_sub_reason	|	Start sub reason associated with the start of a super-session. This is pulled from the compartment session represented by `session_id_start`	|
|	end_reason	|	End associated with the start of a super-session. This is pulled from the compartment session represented by `session_id_end`	|
|	inflow_from_level_1	|	Compartment level 1 value of the preceding compartment session	|
|	inflow_from_level_2	|	Compartment level 2 value of the preceding compartment session	|
|	outflow_to_level_1	|	Compartment level 1 value of the subsequent compartment session	|
|	outflow_to_level_2	|	Compartment level 2 value of the subsequent compartment session	|
|	last_day_of_data	|	The last day for which we have data, specific to a state. This is pulled from `compartment_sessions`	|
"""

COMPARTMENT_LEVEL_1_SUPER_SESSIONS_QUERY_TEMPLATE = """
    /*{description}*/
    WITH super_session_lookup AS 
    (
    SELECT
        person_id,
        session_id,
        compartment_level_1,
        SUM(CASE WHEN change_compartment THEN 1 ELSE 0 END) 
            OVER(PARTITION BY person_id ORDER BY start_date) + 1 AS compartment_level_1_super_session_id
    FROM 
        (
        /*
        Identifies compartments where the compartment_level_1 value changes from the previous compartment_level_1
        value.
        */
        SELECT
            *,
            COALESCE(compartment_level_1 != LAG(compartment_level_1) OVER(PARTITION BY person_id ORDER BY start_date),FALSE) change_compartment,
        FROM `{project_id}.{analyst_dataset}.compartment_sessions_materialized`
        )
    )
    ,
    super_session_agg AS
    (
    /*
    Use the super-session IDs created above to aggregate sessions to incarceration super-sessions
    */
    SELECT
        person_id,
        compartment_level_1_super_session_id,
        state_code,
        s.compartment_level_1,
        last_day_of_data,
        MIN(start_date) AS start_date,
        --this is done to ensure we take a null end date if present instead of the max
        CASE WHEN LOGICAL_AND(end_date IS NOT NULL) THEN MAX(end_date) END AS end_date,        
        --store the session ids at start and end for easy joining
        MIN(session_id) AS session_id_start,
        MAX(session_id) AS session_id_end,
    FROM super_session_lookup s
    JOIN `{project_id}.{analyst_dataset}.compartment_sessions_materialized`
        USING(person_id, session_id)
    GROUP BY 1,2,3,4,5
    )
    /*
    Pull in session characteristics at start and end of super-session
    */
    SELECT 
        s.person_id,
        s.compartment_level_1_super_session_id,
        s.compartment_level_1,
        s.start_date,
        s.end_date,
        s.state_code,
        DATE_DIFF(COALESCE(s.end_date, s.last_day_of_data), s.start_date, DAY) + 1 AS session_length_days,
        s.session_id_start,
        s.session_id_end,
        first.start_reason,
        first.start_sub_reason,
        last.end_reason,
        first.inflow_from_level_1,
        first.inflow_from_level_2,
        last.outflow_to_level_1,
        last.outflow_to_level_2,
        s.last_day_of_data
    FROM super_session_agg s
    LEFT JOIN `{project_id}.{analyst_dataset}.compartment_sessions_materialized` first
        ON first.person_id = s.person_id
        AND first.session_id = s.session_id_start
    LEFT JOIN `{project_id}.{analyst_dataset}.compartment_sessions_materialized` last
        ON last.person_id = s.person_id
        AND last.session_id = s.session_id_end
    """

COMPARTMENT_LEVEL_1_SUPER_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=COMPARTMENT_LEVEL_1_SUPER_SESSIONS_VIEW_NAME,
    view_query_template=COMPARTMENT_LEVEL_1_SUPER_SESSIONS_QUERY_TEMPLATE,
    description=COMPARTMENT_LEVEL_1_SUPER_SESSIONS_VIEW_DESCRIPTION,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        COMPARTMENT_LEVEL_1_SUPER_SESSIONS_VIEW_BUILDER.build_and_print()
