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
"""Sessionized view of each individual merged onto an array of dates at daily intervals,
 used to calculate person-based metrics such as population"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

COMPARTMENT_SESSIONS_UNNESTED_VIEW_NAME = "compartment_sessions_unnested"

COMPARTMENT_SESSIONS_UNNESTED_VIEW_DESCRIPTION = """
Sessionized view of each individual merged onto an array of dates at daily intervals, 
used to calculate person-based metrics such as population
"""

COMPARTMENT_SESSIONS_UNNESTED_QUERY_TEMPLATE = """
    /*{description}*/
    WITH cte AS 
    (
    SELECT
        sessions.state_code,
        sessions.person_id,
        sessions.start_date as session_start,
        ss_level_0.start_date AS compartment_level_0_super_session_start,
        ss_level_1.start_date AS compartment_level_1_super_session_start,
        ss_level_2.start_date AS compartment_level_2_super_session_start,
        ss_supervision.start_date AS supervision_super_session_start,
        system.start_date AS system_session_start,
        sessions.end_date,
        sessions.session_id,
        sessions.dataflow_session_id_start,
        sessions.dataflow_session_id_end,
        CASE WHEN sessions.compartment_level_1 LIKE 'INCARCERATION%' THEN 'INCARCERATION'
            WHEN sessions.compartment_level_1 LIKE 'SUPERVISION%' THEN 'SUPERVISION'
            ELSE sessions.compartment_level_1 END AS compartment_level_0,
        sessions.compartment_level_1,
        sessions.compartment_level_2,
        sessions.session_length_days as session_length_days,
        population_date,
        system.session_id_start AS session_id_start_system_session,
        DATE_DIFF(population_date, system.start_date, DAY) + 1 AS time_served_in_system,
        DATE_DIFF(population_date, ss_level_0.start_date, DAY) + 1 AS time_served_in_compartment_level_0,
        DATE_DIFF(population_date, ss_level_1.start_date, DAY) + 1 AS time_served_in_compartment_level_1,
        DATE_DIFF(population_date, ss_level_2.start_date, DAY) + 1 AS time_served_in_compartment_level_2,
        DATE_DIFF(population_date, sessions.start_date, DAY) + 1 AS time_served_in_compartment,
        DATE_DIFF(population_date, ss_supervision.start_date, DAY) + 1 AS time_served_in_supervision_super_session,
    FROM `{project_id}.{sessions_dataset}.compartment_sessions_materialized` sessions
    JOIN `{project_id}.{sessions_dataset}.compartment_level_0_super_sessions_materialized` ss_level_0
        ON sessions.person_id = ss_level_0.person_id
        AND sessions.session_id BETWEEN ss_level_0.session_id_start AND ss_level_0.session_id_end
    JOIN `{project_id}.{sessions_dataset}.compartment_level_1_super_sessions_materialized` ss_level_1
        ON sessions.person_id = ss_level_1.person_id
        AND sessions.session_id BETWEEN ss_level_1.session_id_start AND ss_level_1.session_id_end
    JOIN`{project_id}.{sessions_dataset}.compartment_level_2_super_sessions_materialized` ss_level_2
        ON sessions.person_id = ss_level_2.person_id
        AND sessions.session_id BETWEEN ss_level_2.session_id_start AND ss_level_2.session_id_end
    LEFT JOIN `{project_id}.{sessions_dataset}.system_sessions_materialized` system
        ON sessions.person_id = system.person_id
        AND sessions.session_id BETWEEN system.session_id_start AND system.session_id_end
    LEFT JOIN `{project_id}.{sessions_dataset}.supervision_super_sessions_materialized` ss_supervision
        ON sessions.person_id = ss_supervision.person_id
        AND sessions.session_id BETWEEN ss_supervision.session_id_start AND ss_supervision.session_id_end
    , UNNEST(GENERATE_DATE_ARRAY(DATE_SUB(CURRENT_DATE, INTERVAL 9 YEAR), CURRENT_DATE, INTERVAL 1 DAY)) AS population_date
    WHERE population_date BETWEEN sessions.start_date AND COALESCE(sessions.end_date, '9999-01-01')
    )
    /*
    The above CTE is left joined back to compartment sessions to pull in information associated with all of the preceding sessions
    within the person's system session. This allows us to create four additional fields that calculate the cumulative time spent in 
    that compartment, compartment level 0, compartment level 1, etc as opposed to the previously calculated fields which represent 
    continuous time spent. The left join creates duplicate records, but the new fields are calculated with window functions that 
    apply to all of the duplicates, so taking a "distinct" brings the table back to a deduped form with one record per person per day.
    */
    SELECT DISTINCT
        cte.* EXCEPT(session_id_start_system_session),
        --Sum up the session lengths of preceding sessions that match the current compartment and add that to the time served in the active compartment
        SUM(IF(cte.compartment_level_1 = preceding_sessions.compartment_level_1, COALESCE(preceding_sessions.session_length_days,0),0)) 
            OVER(PARTITION BY cte.person_id, population_date) + cte.time_served_in_compartment AS cumulative_time_served_in_compartment_level_1,
        SUM(IF(cte.compartment_level_1 = preceding_sessions.compartment_level_1 AND cte.compartment_level_2 = preceding_sessions.compartment_level_2, COALESCE(preceding_sessions.session_length_days,0),0)) 
            OVER(PARTITION BY cte.person_id, population_date) + cte.time_served_in_compartment AS cumulative_time_served_in_compartment,
        SUM(IF(cte.compartment_level_2 = preceding_sessions.compartment_level_2, COALESCE(preceding_sessions.session_length_days,0),0)) 
            OVER(PARTITION BY cte.person_id, population_date) + cte.time_served_in_compartment AS cumulative_time_served_in_compartment_level_2,
        SUM(IF(cte.compartment_level_0 = preceding_sessions.compartment_level_0, COALESCE(preceding_sessions.session_length_days,0),0)) 
            OVER(PARTITION BY cte.person_id, population_date) + cte.time_served_in_compartment AS cumulative_time_served_in_compartment_level_0,
    FROM cte
    LEFT JOIN 
    (
    SELECT 
        *,
        CASE WHEN compartment_level_1 LIKE 'INCARCERATION%' THEN 'INCARCERATION'
            WHEN compartment_level_1 LIKE 'SUPERVISION%' THEN 'SUPERVISION'
            ELSE compartment_level_1 END AS compartment_level_0,
    FROM `{project_id}.{sessions_dataset}.compartment_sessions_materialized`
    ) preceding_sessions 
        ON preceding_sessions.person_id = cte.person_id
        AND preceding_sessions.session_id < cte.session_id
        AND preceding_sessions.session_id>=cte.session_id_start_system_session
    """

COMPARTMENT_SESSIONS_UNNESTED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=COMPARTMENT_SESSIONS_UNNESTED_VIEW_NAME,
    view_query_template=COMPARTMENT_SESSIONS_UNNESTED_QUERY_TEMPLATE,
    description=COMPARTMENT_SESSIONS_UNNESTED_VIEW_DESCRIPTION,
    sessions_dataset=SESSIONS_DATASET,
    clustering_fields=["state_code", "person_id"],
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        COMPARTMENT_SESSIONS_UNNESTED_VIEW_BUILDER.build_and_print()
