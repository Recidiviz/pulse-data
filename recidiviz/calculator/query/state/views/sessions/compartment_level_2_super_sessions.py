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
"""View that aggregates across sessions with matching adjacent compartment_level_0 or compartment_level_2 values in compartment_sessions"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

COMPARTMENT_LEVEL_2_SUPER_SESSIONS_VIEW_NAME = "compartment_level_2_super_sessions"

COMPARTMENT_LEVEL_2_SUPER_SESSIONS_VIEW_DESCRIPTION = """
## Overview
This view is unique on `person_id` and `compartment_level_2_super_session_id` and aggregates across adjacent `compartment_level_2` values. This mainly is done to capture time spent under a particular legal status regardless of whether the person is in-state or out-of-state. 

A new compartment level 2 super session is triggered by a change either in the `compartment_level_2` value __or__ a change in the `compartment_level_0` value. We check for a change in `compartment_level_0` as well because it is possible for a person to have the same `compartment_level_2` value but with different `compartment_level_0` values. At this point, this mainly happens with a `compartment_level_2` value of `INTERNAL_UNKNOWN`. A person can, for example, go from `INCARCERATION - INTERNAL_UNKNOWN` to `SUPERVISION - INTERNAL_UNKNOWN`. Rather than aggregate those adjacent sessions together, it is more useful to have those as separate sessions and only aggregate across in-state and out-of-state changes.

## Field Definitions
|	Field	|	Description	|
|	--------------------	|	--------------------	|
|	person_id	|	Unique person identifier	|
|	compartment_level_2_super_session_id	|	Session id for the super session. Aggregates across compartment_level_0 values	|
|	compartment_level_0	|	Identical to `compartment_level_1` with the exception of in-state and out-of-state aggregation for both incarceration and supervision	|
|	compartment_level_2	|	Level 2 compartment value	
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

COMPARTMENT_LEVEL_2_SUPER_SESSIONS_QUERY_TEMPLATE = """
    /*{description}*/
    WITH super_session_lookup AS 
    (
    SELECT
        person_id,
        session_id,
        compartment_level_0,
        compartment_level_2,
        SUM(CASE WHEN change_compartment THEN 1 ELSE 0 END) 
            OVER(PARTITION BY person_id ORDER BY start_date) + 1 AS compartment_level_2_super_session_id
    FROM 
        (
        /*
        Identifies compartments where the compartment level 0 or level 2 values change from the previous level_0 and level 2 values
        values.
        */
        SELECT
        *,
        COALESCE(compartment_level_0 != LAG(compartment_level_0) OVER(PARTITION BY person_id ORDER BY start_date)
            OR compartment_level_2 != LAG(compartment_level_2) OVER(PARTITION BY person_id ORDER BY start_date),FALSE) change_compartment,
        FROM 
            (
            SELECT
                *,
                CASE WHEN compartment_level_1 LIKE 'INCARCERATION%' THEN 'INCARCERATION'
                    WHEN compartment_level_1 LIKE 'SUPERVISION%' THEN 'SUPERVISION'
                    ELSE compartment_level_1 END AS compartment_level_0
                FROM `{project_id}.{sessions_dataset}.compartment_sessions_materialized`
            )
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
        compartment_level_2_super_session_id,
        state_code,
        s.compartment_level_0,
        s.compartment_level_2,
        last_day_of_data,
        MIN(start_date) AS start_date,
        --this is done to ensure we take a null end date if present instead of the max
        CASE WHEN LOGICAL_AND(end_date IS NOT NULL) THEN MAX(end_date) END AS end_date,        
        --store the session ids at start and end for easy joining
        MIN(session_id) AS session_id_start,
        MAX(session_id) AS session_id_end,
    FROM super_session_lookup s
    JOIN `{project_id}.{sessions_dataset}.compartment_sessions_materialized`
        USING(person_id, session_id)
    GROUP BY 1,2,3,4,5,6
    )
    /*
    Pull in session characteristics at start and end of super-session
    */
    SELECT 
        s.person_id,
        s.compartment_level_2_super_session_id,
        s.compartment_level_0,
        s.compartment_level_2,
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
    LEFT JOIN `{project_id}.{sessions_dataset}.compartment_sessions_materialized` first
        ON first.person_id = s.person_id
        AND first.session_id = s.session_id_start
    LEFT JOIN `{project_id}.{sessions_dataset}.compartment_sessions_materialized` last
        ON last.person_id = s.person_id
        AND last.session_id = s.session_id_end
    """

COMPARTMENT_LEVEL_2_SUPER_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=COMPARTMENT_LEVEL_2_SUPER_SESSIONS_VIEW_NAME,
    view_query_template=COMPARTMENT_LEVEL_2_SUPER_SESSIONS_QUERY_TEMPLATE,
    description=COMPARTMENT_LEVEL_2_SUPER_SESSIONS_VIEW_DESCRIPTION,
    sessions_dataset=SESSIONS_DATASET,
    clustering_fields=["state_code", "person_id"],
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        COMPARTMENT_LEVEL_2_SUPER_SESSIONS_VIEW_BUILDER.build_and_print()
