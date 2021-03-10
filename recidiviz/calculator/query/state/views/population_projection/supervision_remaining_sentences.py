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
# pylint: disable=trailing-whitespace
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
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
        compartment,
        gender,
        sessions.person_id,
        transitions.compartment_duration - GREATEST(DATE_DIFF(run_dates.run_date, sessions.start_date, MONTH), 1) AS remaining_compartment_duration,
    --    sentences.projected_completion_date_max,
        transitions.outflow_to,
        transitions.total_population
      FROM `{project_id}.{population_projection_dataset}.population_projection_sessions_materialized` sessions
      JOIN `{project_id}.{population_projection_dataset}.simulation_run_dates` run_dates
        -- Use sessions that were open on the run date
        ON run_dates.run_date BETWEEN sessions.start_date AND COALESCE(sessions.end_date, '9999-01-01')
      JOIN  `{project_id}.{population_projection_dataset}.population_transitions_materialized` transitions
        USING (state_code, run_date, compartment, gender)
    --  LEFT JOIN `{project_id}.{analyst_dataset}.compartment_sentences_materialized` sentences
    --    ON sentences.person_id = sessions.person_id
    --    AND sentences.session_id = sessions.session_id
      WHERE sessions.state_code IN ('US_ID', 'US_ND')
        AND compartment LIKE 'SUPERVISION%'
        AND gender in ('MALE', 'FEMALE')
        AND GREATEST(DATE_DIFF(run_dates.run_date, sessions.start_date, MONTH), 1) < transitions.compartment_duration
    ),
    supervision_normalization_cte AS (
      SELECT
        state_code,
        run_date,
        person_id,
        SUM(total_population) AS person_level_normalization_constant
      FROM supervision_cte 
      GROUP BY 1,2,3
    )
    
    SELECT
      state_code,
      run_date,
      compartment,
      gender,
    --  CASE
    --    WHEN (outflow_to = 'RELEASE - RELEASE') AND (CEILING(DATE_DIFF(projected_completion_date_max, run_date, DAY)/30) > 0)
    --        THEN CEILING(DATE_DIFF(projected_completion_date_max, run_date, DAY)/30)
    --    ELSE remaining_compartment_duration 
    --  END AS compartment_duration,
      remaining_compartment_duration as compartment_duration,
      outflow_to,
      CAST(ROUND(SUM(total_population/person_level_normalization_constant), 0) AS INT64) AS total_population
    FROM supervision_cte
    JOIN supervision_normalization_cte
      USING (state_code, run_date, person_id)
    GROUP BY 1,2,3,4,5,6
    ORDER BY 1,2,3,4,5,6
    """

SUPERVISION_REMAINING_SENTENCES_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=POPULATION_PROJECTION_DATASET,
    view_id=REMAINING_SENTENCES_VIEW_NAME,
    view_query_template=REMAINING_SENTENCES_QUERY_TEMPLATE,
    description=REMAINING_SENTENCES_VIEW_DESCRIPTION,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    population_projection_dataset=POPULATION_PROJECTION_DATASET,
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_REMAINING_SENTENCES_VIEW_BUILDER.build_and_print()
