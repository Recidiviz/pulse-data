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
"""Incarceration super-sessions for each individual. Super-session defined as continuous stay within an incarceration
 compartment_level_2, aggregating across in-state and out-of-state incarceration"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

INCARCERATION_SUPER_SESSIONS_VIEW_NAME = "incarceration_super_sessions"

INCARCERATION_SUPER_SESSIONS_VIEW_DESCRIPTION = """

## Overview

This view is unique on `person_id` and `incarceration_super_session_id`. Like supervision super-sessions, incarceration super-sessions aggregate across compartment sessions. However, the methodology is slightly different. Incarceration super-sessions only aggregates across incarceration `compartment_level_1` values _within_ a `compartment_level_2`. For example, `INCARCERATION - GENERAL` --> `INCARCERATION_OUT_OF_STATE - GENERAL` would become one incarceration super-session, but `INCARCERATION - TREATMENT_IN_PRISON` --> `INCARCERATION - GENERAL` would not. 

This was done mainly for the specific use-case of calculating LOS within a given legal status. If someone on a general term transfers back and forth between in-state and out-of-state, we would want to measure that as a continous stay on a general term.

## Field Definitions
|	Field	|	Description	|
|	--------------------	|	--------------------	|
|	person_id	|	Unique person identifier	|
|	incarceration_super_session_id	|	Session id for the incarceration super session. Aggregates across in-state and out-of-state incarceration within a `compartment_level_2` value	|
|	start_date	|	Incarceration super-session start date	|
|	end_date	|	Incarceration super-session end date	|
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

INCARCERATION_SUPER_SESSIONS_QUERY_TEMPLATE = """
    /*{description}*/
    WITH incarceration_super_session_lookup AS 
    (
    SELECT
        person_id,
        session_id,
        SUM(CASE WHEN change_compartment_level_2 OR non_continuous_session THEN 1 ELSE 0 END) 
            OVER(PARTITION BY person_id ORDER BY start_date) + 1 AS incarceration_super_session_id
    FROM 
        (
        /*
        Subquery subsets for those with a compartment_level_1 value of INCARCERATION or INCARCERATION_OUT_OF_STATE
        and then identifies compartments where (a) the compartment_level_2 value changes from the previous compartment_level_2
        value and (b) sessions are non-continuous. If either of these conditions are met than a new incarceration super-session
        should be created. 
        */
        SELECT
            *,
            COALESCE(compartment_level_2 != LAG(compartment_level_2) OVER(PARTITION BY person_id ORDER BY start_date),FALSE) change_compartment_level_2,
            COALESCE(session_id != LAG(session_id+1) OVER(PARTITION BY person_id ORDER BY start_date), FALSE)  non_continuous_session,
        FROM `{project_id}.{sessions_dataset}.compartment_sessions_materialized`
        WHERE compartment_level_1 IN ('INCARCERATION','INCARCERATION_OUT_OF_STATE')
        )
    )
    ,
    incarceration_super_session_agg AS
    (
    /*
    Use the super-session IDs created above to aggregate sessions to incarceration super-sessions
    */
    SELECT
        person_id,
        incarceration_super_session_id,
        state_code,
        last_day_of_data,
        MIN(start_date) AS start_date,
        --this is done to ensure we take a null end date if present instead of the max
        CASE WHEN LOGICAL_AND(end_date IS NOT NULL) THEN MAX(end_date) END AS end_date,        
        --store the session ids at start and end for easy joining
        MIN(session_id) AS session_id_start,
        MAX(session_id) AS session_id_end,
    FROM incarceration_super_session_lookup
    JOIN `{project_id}.{sessions_dataset}.compartment_sessions_materialized`
        USING(person_id, session_id)
    GROUP BY 1,2,3,4
    )
    /*
    Pull in session characteristics at start and end of incarceration super-session
    */
    SELECT 
        s.person_id,
        s.incarceration_super_session_id,
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
    FROM incarceration_super_session_agg s
    LEFT JOIN `{project_id}.{sessions_dataset}.compartment_sessions_materialized` first
        ON first.person_id = s.person_id
        AND first.session_id = s.session_id_start
    LEFT JOIN `{project_id}.{sessions_dataset}.compartment_sessions_materialized` last
        ON last.person_id = s.person_id
        AND last.session_id = s.session_id_end
    """

INCARCERATION_SUPER_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=INCARCERATION_SUPER_SESSIONS_VIEW_NAME,
    view_query_template=INCARCERATION_SUPER_SESSIONS_QUERY_TEMPLATE,
    description=INCARCERATION_SUPER_SESSIONS_VIEW_DESCRIPTION,
    sessions_dataset=SESSIONS_DATASET,
    clustering_fields=["state_code", "person_id"],
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        INCARCERATION_SUPER_SESSIONS_VIEW_BUILDER.build_and_print()
