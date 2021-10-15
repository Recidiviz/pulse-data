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
"""Historical total US_ID parole board hold population by outflow compartment, and compartment duration (months)"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_ID_PAROLE_BOARD_HOLD_POPULATION_TRANSITIONS_VIEW_NAME = (
    "us_id_parole_board_hold_population_transitions"
)

US_ID_PAROLE_BOARD_HOLD_POPULATION_TRANSITIONS_VIEW_DESCRIPTION = """"Historical US_ID Board Hold total population
by outflow compartment, and compartment duration (months)"""

US_ID_PAROLE_BOARD_HOLD_POPULATION_TRANSITIONS_QUERY_TEMPLATE = """
    /*{description}*/
    WITH cohorts_per_run_date AS (
      SELECT
        state_code,
        run_dates.run_date,
        compartment,
        outflow_to,
        person_id,
        gender,
        FLOOR(DATE_DIFF(end_date, start_date, DAY)/30) AS compartment_duration
      FROM `{project_id}.{population_projection_dataset}.population_projection_sessions_materialized` sessions
      JOIN `{project_id}.{population_projection_dataset}.simulation_run_dates` run_dates
        -- Use sessions that were completed before the run date
        -- TODO(#4867): count un-finished sessions someway instead of dropping
        ON sessions.end_date < run_dates.run_date
      WHERE state_code = 'US_ID'
        AND gender IN ('MALE', 'FEMALE')
        -- Only take data from the 3 years prior to the run date to match short-term behavior better
        AND DATE_DIFF(run_dates.run_date, sessions.start_date, year) <= 3
        -- Only include outflows that are supported by the IDOC
        AND compartment = 'INCARCERATION - PAROLE_BOARD_HOLD'
    ),
    fully_connected_graph AS (
      -- Create rows for every compartment duration and outflow
      SELECT
        state_code,
        run_date,
        gender,
        compartment,
        outflow_to,
        compartment_duration,
        cohort_size
      FROM (
        -- Get the max duration per compartment/gender/run date
        SELECT
          state_code,
          run_date,
          compartment,
          gender,
          MAX(compartment_duration) AS max_duration,
          -- Calculate the cohort size for each run date to use as the transition denominator below
          COUNT(*) AS cohort_size
        FROM cohorts_per_run_date
        GROUP BY state_code, run_date, compartment, gender
      ),
      UNNEST(GENERATE_ARRAY(1, max_duration)) AS compartment_duration,
      UNNEST(['SUPERVISION - PAROLE', 'INCARCERATION - GENERAL',
        'INCARCERATION - TREATMENT_IN_PRISON', 'SUPERVISION_OUT_OF_STATE - PAROLE']) AS outflow_to
    )
    SELECT
      state_code,
      run_date,
      gender,
      compartment,
      -- Revocations from parole board holds are an outflow to re-incarceration
      IF(outflow_to = 'INCARCERATION - GENERAL', 'INCARCERATION - RE-INCARCERATION', outflow_to) AS outflow_to,
      compartment_duration,
      -- Sum up the non-NULL entries per compartment/outflow/duration, will be 0 if they are all NULL
      COUNT(cohorts_per_run_date.person_id)/cohort_size AS total_population
    FROM fully_connected_graph
    LEFT JOIN cohorts_per_run_date
      USING (state_code, run_date, gender, compartment, outflow_to, compartment_duration)
    GROUP BY state_code, run_date, gender, compartment, outflow_to, compartment_duration, cohort_size
    """

US_ID_PAROLE_BOARD_HOLD_POPULATION_TRANSITIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.POPULATION_PROJECTION_DATASET,
    view_id=US_ID_PAROLE_BOARD_HOLD_POPULATION_TRANSITIONS_VIEW_NAME,
    view_query_template=US_ID_PAROLE_BOARD_HOLD_POPULATION_TRANSITIONS_QUERY_TEMPLATE,
    description=US_ID_PAROLE_BOARD_HOLD_POPULATION_TRANSITIONS_VIEW_DESCRIPTION,
    population_projection_dataset=dataset_config.POPULATION_PROJECTION_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ID_PAROLE_BOARD_HOLD_POPULATION_TRANSITIONS_VIEW_BUILDER.build_and_print()
