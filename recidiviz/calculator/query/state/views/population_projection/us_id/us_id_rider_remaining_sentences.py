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
"""US_ID Rider remaining sentences by outflow compartment, and projected compartment duration (months)"""
# pylint: disable=trailing-whitespace, line-too-long
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_ID_RIDER_REMAINING_SENTENCES_VIEW_NAME = 'us_id_rider_pbh_remaining_sentences'

US_ID_RIDER_REMAINING_SENTENCES_VIEW_DESCRIPTION = \
    """"US_ID Rider remaining sentences by outflow compartment, and compartment duration (months) projected using the
    historical transition probabilities and the time served so far to make up the remaining compartment duration."""

US_ID_RIDER_REMAINING_SENTENCES_QUERY_TEMPLATE = \
    """
    /*{description}*/
    
    /* 
    High level idea: treat the rider population transitions like a survival curve and select a subset of that
    distribution for each open rider session based on the time served before the run date. Shift this distribution
    down to month 1 and normalize it to equal 1 across all future durations for each session/person.
    */
    WITH historical_transitions AS (
      SELECT
        state_code, run_date, compartment, gender,
        compartment_duration, outflow_to, total_population
      FROM `{project_id}.{population_projection_dataset}.us_id_rider_population_transitions`
      UNION ALL
      SELECT
        state_code, run_date, compartment, gender,
        compartment_duration, outflow_to, total_population
      FROM `{project_id}.{population_projection_dataset}.us_id_parole_board_hold_population_transitions`
    ),
    cohorts_per_run_date AS (
      -- Grab all the open rider sessions per run date
      SELECT
        state_code,
        run_dates.run_date,
        compartment,
        person_id,
        gender,
        GREATEST(DATE_DIFF(run_dates.run_date, start_date, MONTH), 1) AS months_served,
        transitions.compartment_duration - GREATEST(DATE_DIFF(run_dates.run_date, start_date, MONTH), 1) AS remaining_compartment_duration,
        transitions.outflow_to,
        transitions.total_population
      FROM `{project_id}.{population_projection_dataset}.population_projection_sessions_materialized` sessions
      JOIN `{project_id}.{population_projection_dataset}.simulation_run_dates` run_dates
        -- Use sessions that were open on the run date
        ON run_dates.run_date BETWEEN sessions.start_date AND COALESCE(sessions.end_date, '9999-01-01')
      JOIN historical_transitions transitions
        USING (state_code, run_date, compartment, gender)
      WHERE state_code = 'US_ID'
        AND compartment IN ('INCARCERATION - TREATMENT_IN_PRISON', 'INCARCERATION - PAROLE_BOARD_HOLD')
        AND gender IN ('MALE', 'FEMALE')
        -- Select only the "tail" of the duration distribution
        AND GREATEST(DATE_DIFF(run_dates.run_date, start_date, MONTH), 1) < transitions.compartment_duration
        -- TODO(#4868): filter unwanted transitions
    ),
    normalization_cte AS (
      -- Compute the normalization denominator per person/run date
      SELECT
        state_code,
        run_date,
        person_id,
        compartment,
        SUM(total_population) AS person_level_normalization_constant
      FROM cohorts_per_run_date
      GROUP BY state_code, run_date, person_id, compartment
    )
    SELECT
      state_code,
      run_date,
      compartment,
      gender,
      months_served,
      remaining_compartment_duration AS compartment_duration,
      outflow_to,
      total_population/person_level_normalization_constant AS total_population
    FROM cohorts_per_run_date
    JOIN normalization_cte
      USING (state_code, run_date, person_id, compartment)
    """

US_ID_RIDER_REMAINING_SENTENCES_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.POPULATION_PROJECTION_DATASET,
    view_id=US_ID_RIDER_REMAINING_SENTENCES_VIEW_NAME,
    view_query_template=US_ID_RIDER_REMAINING_SENTENCES_QUERY_TEMPLATE,
    description=US_ID_RIDER_REMAINING_SENTENCES_VIEW_DESCRIPTION,
    analyst_dataset=dataset_config.ANALYST_VIEWS_DATASET,
    population_projection_dataset=dataset_config.POPULATION_PROJECTION_DATASET,
    should_materialize=False
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ID_RIDER_REMAINING_SENTENCES_VIEW_BUILDER.build_and_print()
