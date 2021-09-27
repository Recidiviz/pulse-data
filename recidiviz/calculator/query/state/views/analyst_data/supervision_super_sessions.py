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
"""Supervision super-sessions for each individual"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_SUPER_SESSIONS_VIEW_NAME = "supervision_super_sessions"

SUPERVISION_SUPER_SESSIONS_VIEW_DESCRIPTION = """
## Overview

This view has a record for each person and each supervision super-session. A supervision super-session groups together compartment sessions that represent a continuous stay on supervision despite a person's legal status changing. The most frequent examples of this are (1) parole board holds where a person can either be revoked or returned to parole, and (2) bench warrants where a person often transitions from probation, to a bench warrant, and back to probation. 

The view has a couple of important uses:

1. Supervision LOS is probably more accurately calculated using this view because otherwise LOS will be artificially skewed down by transitions such as PAROLE --> PBH --> PAROLE or PROBATION --> BENCH_WARRANT --> PROBATION if we were to calculate LOS on those individual supervision sessions.
2. This view is used as the denominator for revocation rate calculations as well as the starting point to determine when a revocation occurs relative to the supervision start. The views `revocation_sessions` and `revocation_cohort_sessions` are based on a `supervision_super_session` as the unit of analysis from which a person can be revoked.

## Field Definitions

|	Field	|	Description	|
|	--------------------	|	--------------------	|
|	person_id	|	Unique person identifier	|
|	supervision_super_session_id	|	Super session identifier	|
|	start_date	|	Super session start date	|
|	end_date	|	Super session end date	|
|	state_code	|	State	|
|	incarceration_days	|	Number of days of the super session that are spent incarcerated. Someone spends part of their supervision super-session incarcerated because of parole board holds and shock incarceration 	|
|	session_length_days	|	Difference between session start date and session end date. For active sessions the session start date is differenced from the last day of data	|
|	session_id_start	|	Compartment session id associated with the start of the super session. This field and the following field are used to join sessions and super-sessions	|
|	session_id_end	|	Compartment session id associated with the end of the super session. This field and the preceding field are used to join sessions and super-sessions	|
|	start_reason	|	Start reason associated with the start of a super session. This is pulled from the compartment session represented by `session_id_start`	|
|	start_sub_reason	|	Start sub reason associated with the start of a super session. This is pulled from the compartment session represented by `session_id_start`	|
|	end_reason	|	End associated with the start of a super session. This is pulled from the compartment session represented by `session_id_end`	|
|	inflow_from_level_1	|	Compartment level 1 value of the preceding compartment session	|
|	inflow_from_level_2	|	Compartment level 2 value of the preceding compartment session	|
|	outflow_to_level_1	|	Compartment level 1 value of the subsequent compartment session	|
|	outflow_to_level_2	|	Compartment level 2 value of the subsequent compartment session	|
|	last_day_of_data	|	The last day for which we have data, specific to a state. This is pulled from `compartment_sessions`	|

## Methodology

A supervision super session aggregates together sessions of the following types:

1. Any session with `compartment_level_1` in one of the following:

    1. `SUPERVISION` (this includes bench warrants and absconsions)
    2. `SUPERVISION_OUT_OF_STATE`
    
2. Any session with `compartment_level_2` in one of the following:

    1. `PAROLE_BOARD_HOLD`
    2. `PENDING_CUSTODY`
    3. `TEMPORARY_CUSTODY`
    4. `SUSPENSION`
    5. `SHOCK_INCARCERATION`
"""

SUPERVISION_SUPER_SESSIONS_QUERY_TEMPLATE = """
    /*{description}*/
    WITH supervision_super_session_lookup AS
    /*
    Identify and create supervision super-session ids from sessions.
    */
    (
    SELECT 
        person_id,
        session_id,
        supervision_super_session_id,
    FROM 
        (
        SELECT 
            *,
            --Create a new ID when a session is in the supervison super-compartment (supervision, parole board hold, or pending custody) and the previous session is not
            SUM(CASE WHEN supervision_super_compartment AND NOT COALESCE(lag_supervision_super_compartment, FALSE) THEN 1 ELSE 0 END) 
                OVER(PARTITION BY person_id ORDER BY start_date) AS supervision_super_session_id
        FROM 
            (
            SELECT
                *,
                --Identify sessions that are in the supervision super-compartment as well as whether the prior session is
                -- The compartment_level_2 list here is meant to capture folks that are in temporary holds of some sort,
                -- where their supervision status doesnt not change, so that we don't count transitions to those compartments
                -- as revocations
                compartment_level_1 IN ('SUPERVISION','SUPERVISION_OUT_OF_STATE') 
                    OR compartment_level_2 IN ('PAROLE_BOARD_HOLD', 'PENDING_CUSTODY', 'TEMPORARY_CUSTODY', 'SUSPENSION','SHOCK_INCARCERATION') AS supervision_super_compartment,
                LAG(compartment_level_1 IN ('SUPERVISION', 'SUPERVISION_OUT_OF_STATE')
                    OR compartment_level_2 IN ('PAROLE_BOARD_HOLD','PENDING_CUSTODY', 'TEMPORARY_CUSTODY', 'SUSPENSION','SHOCK_INCARCERATION'))
                    OVER(PARTITION BY person_id ORDER BY start_date) AS lag_supervision_super_compartment
            FROM `{project_id}.{analyst_dataset}.compartment_sessions_materialized`
            )
      )
    --Subset the data at the end to only include supervision super-sessions and not other incarceration sessions
    WHERE supervision_super_compartment
    )
    ,
    supervision_super_session_agg AS
    (
    /*
    Use the super-session IDs created above to aggregate sessions to supervision super-sessions
    */
    SELECT
        person_id,
        supervision_super_session_id,
        state_code,
        last_day_of_data,
        MIN(start_date) AS start_date,
        --this is done to ensure we take a null end date if present instead of the max
        CASE WHEN LOGICAL_AND(end_date IS NOT NULL) THEN MAX(end_date) END AS end_date,        
        --keep track of the number of days within a supervision super-session that a person is incarcerated (parole board holds)
        SUM(CASE WHEN compartment_level_1 = 'INCARCERATION' THEN session_length_days ELSE 0 END) AS incarceration_days,
        --store the session ids at start and end for easy joining
        MIN(session_id) AS session_id_start,
        MAX(session_id) AS session_id_end,
    FROM supervision_super_session_lookup
    JOIN `{project_id}.{analyst_dataset}.compartment_sessions_materialized`
        USING(person_id, session_id)
    GROUP BY 1,2,3,4
    )
    /*
    Pull in session characteristics at start and end of supervision super-session
    */
    SELECT 
        s.person_id,
        s.supervision_super_session_id,
        s.start_date,
        s.end_date,
        s.state_code,
        s.incarceration_days,
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
    FROM supervision_super_session_agg s
    LEFT JOIN `{project_id}.{analyst_dataset}.compartment_sessions_materialized` first
        ON first.person_id = s.person_id
        AND first.session_id = s.session_id_start
    LEFT JOIN `{project_id}.{analyst_dataset}.compartment_sessions_materialized` last
        ON last.person_id = s.person_id
        AND last.session_id = s.session_id_end
    """

SUPERVISION_SUPER_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=SUPERVISION_SUPER_SESSIONS_VIEW_NAME,
    view_query_template=SUPERVISION_SUPER_SESSIONS_QUERY_TEMPLATE,
    description=SUPERVISION_SUPER_SESSIONS_VIEW_DESCRIPTION,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_SUPER_SESSIONS_VIEW_BUILDER.build_and_print()
