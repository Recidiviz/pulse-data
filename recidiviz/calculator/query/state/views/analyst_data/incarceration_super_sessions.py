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
"""Incarceration super-sessions for each individual. Super-session defined as continuous stay within an incarceration
 compartment_level_2, aggregating across in-state and out-of-state incarceration"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

INCARCERATION_SUPER_SESSIONS_VIEW_NAME = "incarceration_super_sessions"

INCARCERATION_SUPER_SESSIONS_VIEW_DESCRIPTION = """Incarceration super-sessions for each individual. Super-session defined as continuous stay within an incarceration
 compartment_level_2, aggregating across in-state and out-of-state incarceration."""

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
        FROM `{project_id}.{analyst_dataset}.compartment_sessions_materialized`
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
    JOIN `{project_id}.{analyst_dataset}.compartment_sessions_materialized`
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
    LEFT JOIN `{project_id}.{analyst_dataset}.compartment_sessions_materialized` first
        ON first.person_id = s.person_id
        AND first.session_id = s.session_id_start
    LEFT JOIN `{project_id}.{analyst_dataset}.compartment_sessions_materialized` last
        ON last.person_id = s.person_id
        AND last.session_id = s.session_id_end
    """

INCARCERATION_SUPER_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=INCARCERATION_SUPER_SESSIONS_VIEW_NAME,
    view_query_template=INCARCERATION_SUPER_SESSIONS_QUERY_TEMPLATE,
    description=INCARCERATION_SUPER_SESSIONS_VIEW_DESCRIPTION,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        INCARCERATION_SUPER_SESSIONS_VIEW_BUILDER.build_and_print()
