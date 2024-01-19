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
"""Current total population by compartment, outflow compartment, and months until transition will be made"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    POPULATION_PROJECTION_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

REMAINING_SENTENCES_VIEW_NAME = "supervision_remaining_sentences"

REMAINING_SENTENCES_VIEW_DESCRIPTION = """"Current supervised total population by compartment, outflow compartment, and months until transition will be made"""

REMAINING_SENTENCES_QUERY_TEMPLATE = """
    WITH supervision_cte AS (
      SELECT
        sessions.state_code,
        run_date,
        gender,
        -- Re-categorize repeat incarceration sessions as re-incarceration
        CASE
            WHEN sessions.compartment = 'INCARCERATION - GENERAL' AND sessions.previously_incarcerated
                THEN 'INCARCERATION - RE-INCARCERATION'
            ELSE sessions.compartment
        END AS compartment,
        transitions.outflow_to,
        CASE
            -- Use the projected release date for the outflow to liberty for this person_id/run_date if available.
            -- Would be helpful to include the likelihood of early release (similar to parole vs liberty in
            -- incarceration remaining sentences)
            WHEN sentences.projected_release_date IS NOT NULL
                 AND transitions.outflow_to = 'LIBERTY - LIBERTY_REPEAT_IN_SYSTEM'
                THEN CEIL(DATE_DIFF(sentences.projected_release_date, run_dates.run_date, DAY)/30)
            -- Handle sentences that are really long (4+ years) and are unlikely to end soon
            -- Otherwise this will predict a large chunk of people will have 0 remaining duration
            WHEN CEIL(DATE_DIFF(run_dates.run_date, sessions.start_date, DAY)/30) + 1 >= 180
                THEN 120
            WHEN CEIL(DATE_DIFF(run_dates.run_date, sessions.start_date, DAY)/30) + 1 >= 150
                THEN 180 - CEIL(DATE_DIFF(run_dates.run_date, sessions.start_date, DAY)/30) + 1
            WHEN CEIL(DATE_DIFF(run_dates.run_date, sessions.start_date, DAY)/30) + 1 >= 120
                THEN 150 - CEIL(DATE_DIFF(run_dates.run_date, sessions.start_date, DAY)/30) + 1
            WHEN CEIL(DATE_DIFF(run_dates.run_date, sessions.start_date, DAY)/30) + 1 >= 100
                THEN 120 - CEIL(DATE_DIFF(run_dates.run_date, sessions.start_date, DAY)/30) + 1
            WHEN CEIL(DATE_DIFF(run_dates.run_date, sessions.start_date, DAY)/30) + 1 >= 50
                THEN 100 - CEIL(DATE_DIFF(run_dates.run_date, sessions.start_date, DAY)/30) + 1
            ELSE transitions.compartment_duration - CEIL(DATE_DIFF(run_dates.run_date, sessions.start_date, DAY)/30)
         END AS remaining_compartment_duration,

        -- Normalize the transition "cohort_portion" sum at the person-level so that each individual only counts
        -- towards 1 total transition per run-date (likely split across multiple outflows and compartment durations)
        transitions.cohort_portion / SUM(transitions.cohort_portion) OVER (
            PARTITION BY state_code, run_date, person_id
        ) AS total_population,  -- RENAME?
      FROM `{project_id}.{population_projection_dataset}.population_projection_sessions_materialized` sessions
      JOIN `{project_id}.{population_projection_dataset}.simulation_run_dates` run_dates
        -- Only include sessions that were open on the run date
        ON run_dates.run_date BETWEEN sessions.start_date AND COALESCE(sessions.end_date, '9999-01-01')
      JOIN `{project_id}.{population_projection_dataset}.population_transitions_materialized` transitions
        USING (state_code, compartment, run_date)
      LEFT JOIN `{project_id}.{population_projection_dataset}.supervision_projected_release_dates_materialized` sentences
        USING (state_code, person_id, run_date)
      WHERE
        gender in ('MALE', 'FEMALE')
        -- Replace `USING gender` clause in population_transitions_materialized JOIN 
        AND sessions.gender = transitions.simulation_group
        -- Only include transition durations that are longer than the time already served on supervision
        AND CEIL(DATE_DIFF(run_dates.run_date, sessions.start_date, DAY)/30) <= transitions.compartment_duration
        -- Drop sessions in long lasting compartments that are over 10 years old to limit the amount
        -- of sessions projected with 0 remaining sentence length
        AND (
            NOT sessions.long_lasting_compartment
            OR CEIL(DATE_DIFF(run_dates.run_date, sessions.start_date, DAY)/30) <= 120
        )
    )
    SELECT
      state_code,
      run_date,
      compartment,
      gender as simulation_group,
      remaining_compartment_duration AS compartment_duration,
      outflow_to,
      SUM(total_population) AS cohort_portion
    FROM supervision_cte
    GROUP BY 1,2,3,4,5,6
    """

SUPERVISION_REMAINING_SENTENCES_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=POPULATION_PROJECTION_DATASET,
    view_id=REMAINING_SENTENCES_VIEW_NAME,
    view_query_template=REMAINING_SENTENCES_QUERY_TEMPLATE,
    description=REMAINING_SENTENCES_VIEW_DESCRIPTION,
    population_projection_dataset=POPULATION_PROJECTION_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_REMAINING_SENTENCES_VIEW_BUILDER.build_and_print()
