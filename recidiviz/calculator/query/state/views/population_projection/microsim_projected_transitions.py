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
"""Microsimulation projection output"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

MICROSIM_PROJECTED_TRANSITIONS_VIEW_NAME = "microsim_projected_transitions"

MICROSIM_PROJECTED_TRANSITIONS_VIEW_DESCRIPTION = (
    """"The projected transitions for the micro simulation"""
)

TRANSITIONS_START_DATE = "2016-06-01"

MICROSIM_PROJECTION_QUERY_TEMPLATE = """
    /* {description} */
    WITH historical_transitions AS (
        SELECT
            state_code,
            DATE_TRUNC(start_date, MONTH) AS simulation_date,
            gender,
            CASE
                WHEN sessions.inflow_from = 'INCARCERATION - GENERAL' AND prev_session.previously_incarcerated 
                    THEN 'INCARCERATION - RE-INCARCERATION'
                ELSE sessions.inflow_from
            END AS compartment,
            CASE
              WHEN sessions.compartment = 'INCARCERATION - GENERAL' 
                    AND (sessions.previously_incarcerated OR sessions.inflow_from = 'SUPERVISION - PAROLE')
                THEN 'INCARCERATION - RE-INCARCERATION'
              ELSE sessions.compartment
            END AS outflow_to,
            COUNT(*) AS total_population
        FROM `{project_id}.{population_projection_dataset}.population_projection_sessions_materialized` sessions
        LEFT JOIN (
            SELECT state_code, person_id, session_id + 1 AS session_id, previously_incarcerated
            FROM `{project_id}.{population_projection_dataset}.population_projection_sessions_materialized`
        ) prev_session
        USING (state_code, person_id, session_id)
        WHERE state_code = 'US_ID'
            AND start_date BETWEEN '{transitions_start_date}' AND LAST_DAY(DATE_SUB(CURRENT_DATE, INTERVAL 1 MONTH))
            AND gender IN ('FEMALE', 'MALE')
        GROUP BY state_code, simulation_date, gender, compartment, outflow_to
    ),
    most_recent_results AS (
      SELECT
        state_code, MAX(date_created) AS date_created
      FROM `{project_id}.{population_projection_output_dataset}.microsim_projected_outflows_raw`
      GROUP BY state_code
    )
    SELECT
      state_code,
      "PROJECTED" AS simulation_tag,
      date_created,
      simulation_date,
      -- Split the compartment and legal status into separate columns
      SPLIT(compartment, ' ')[OFFSET(0)] AS compartment,
      SPLIT(compartment, ' ')[SAFE_OFFSET(2)] AS compartment_legal_status,
      SPLIT(outflow_to, ' ')[OFFSET(0)] AS outflow_compartment,
      SPLIT(outflow_to, ' ')[SAFE_OFFSET(2)] AS outflow_legal_status,
      simulation_group AS gender,
      total_population
    FROM `{project_id}.{population_projection_output_dataset}.microsim_projected_outflows_raw`
    INNER JOIN most_recent_results
    USING (state_code, date_created)
    WHERE simulation_date >= DATE_TRUNC(CURRENT_DATE, MONTH)
    UNION ALL
    SELECT
        state_code,
        "HISTORICAL" AS simulation_tag,
        NULL AS date_created,
        simulation_date,
        -- Split the compartment and legal status into separate columns
        SPLIT(compartment, ' ')[OFFSET(0)] AS compartment,
        SPLIT(compartment, ' ')[SAFE_OFFSET(2)] AS compartment_legal_status,
        SPLIT(outflow_to, ' ')[OFFSET(0)] AS outflow_compartment,
        SPLIT(outflow_to, ' ')[SAFE_OFFSET(2)] AS outflow_legal_status,
        gender,
        total_population
    FROM historical_transitions
    ORDER BY state_code, gender, compartment, compartment_legal_status, simulation_date
    """

MICROSIM_PROJECTED_TRANSITIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.POPULATION_PROJECTION_DATASET,
    view_id=MICROSIM_PROJECTED_TRANSITIONS_VIEW_NAME,
    view_query_template=MICROSIM_PROJECTION_QUERY_TEMPLATE,
    description=MICROSIM_PROJECTED_TRANSITIONS_VIEW_DESCRIPTION,
    population_projection_dataset=dataset_config.POPULATION_PROJECTION_DATASET,
    population_projection_output_dataset=dataset_config.POPULATION_PROJECTION_OUTPUT_DATASET,
    transitions_start_date=TRANSITIONS_START_DATE,
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        MICROSIM_PROJECTED_TRANSITIONS_VIEW_BUILDER.build_and_print()
