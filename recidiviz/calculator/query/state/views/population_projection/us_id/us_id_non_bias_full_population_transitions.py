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
"""Historical total US_ID rider/parole board hold population by outflow compartment and compartment duration (months)"""
# pylint: disable=trailing-whitespace, line-too-long
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_ID_PAROLE_BOARD_HOLD_FULL_TRANSITIONS_VIEW_NAME = 'us_id_non_bias_full_transitions'

US_ID_PAROLE_BOARD_HOLD_POPULATION_TRANSITIONS_VIEW_DESCRIPTION = \
    """"Historical US_ID Board Hold total population by outflow compartment, and compartment duration (months)"""

US_ID_PAROLE_BOARD_HOLD_POPULATION_TRANSITIONS_QUERY_TEMPLATE = \
    """
    /*{description}*/
    WITH remaining_sentences AS (
        -- Count the projected remaining sentences as part of the transition distribution so that the dist isn't bias towards short sentences
        SELECT
            compartment,
            gender,
            state_code,
            outflow_to,
            months_served + compartment_duration AS compartment_duration,
            run_date,
            CAST(ROUND(SUM(total_population)) AS INT64) AS total_population
        FROM `{project_id}.{population_projection_dataset}.us_id_rider_pbh_remaining_sentences`
        GROUP BY state_code, run_date, compartment, gender, compartment_duration, outflow_to
    ),
    parole_board_hold_union AS (
        SELECT
          compartment,
          gender,
          state_code,
          outflow_to,
          compartment_duration,
          run_date,
          total_population
        FROM `{project_id}.{population_projection_dataset}.us_id_parole_board_hold_population_transitions`

        UNION ALL

        SELECT
          compartment,
          gender,
          state_code,
          outflow_to,
          compartment_duration,
          run_date,
          total_population
        FROM `{project_id}.{population_projection_dataset}.us_id_rider_population_transitions`
        UNION ALL
        -- Count the projected remaining sentences as part of the transition distribution so that the dist isn't bias towards short sentences
        SELECT
          compartment,
          gender,
          state_code,
          outflow_to,
          compartment_duration,
          run_date,
          SUM(total_population)/total_pop AS total_population
        FROM remaining_sentences
        JOIN (
            SELECT
                compartment, gender, state_code, run_date,
                SUM(total_population) AS total_pop
            FROM remaining_sentences
            GROUP BY compartment, gender, state_code, run_date
        )
          USING (compartment, gender, state_code, run_date)
        GROUP BY compartment, gender, state_code, outflow_to, compartment_duration, run_date, total_pop
    )

    SELECT
      compartment,
      gender,
      state_code,
      outflow_to,
      compartment_duration,
      run_date,
      SUM(total_population)/total_pop AS total_population
    FROM parole_board_hold_union
    JOIN (
        SELECT
          compartment, gender, state_code, run_date,
          SUM(total_population) AS total_pop
        FROM parole_board_hold_union
        GROUP BY compartment, gender, state_code, run_date
    )
      USING (compartment, gender, state_code, run_date)
    GROUP BY compartment, gender, state_code, outflow_to, compartment_duration, run_date, total_pop
    """

US_ID_PAROLE_BOARD_HOLD_FULL_TRANSITIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.POPULATION_PROJECTION_DATASET,
    view_id=US_ID_PAROLE_BOARD_HOLD_FULL_TRANSITIONS_VIEW_NAME,
    view_query_template=US_ID_PAROLE_BOARD_HOLD_POPULATION_TRANSITIONS_QUERY_TEMPLATE,
    description=US_ID_PAROLE_BOARD_HOLD_POPULATION_TRANSITIONS_VIEW_DESCRIPTION,
    analyst_dataset=dataset_config.ANALYST_VIEWS_DATASET,
    population_projection_dataset=dataset_config.POPULATION_PROJECTION_DATASET,
    should_materialize=True
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ID_PAROLE_BOARD_HOLD_FULL_TRANSITIONS_VIEW_BUILDER.build_and_print()
