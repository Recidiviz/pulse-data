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

REMAINING_SENTENCES_VIEW_NAME = "remaining_sentences"

REMAINING_SENTENCES_VIEW_DESCRIPTION = """"Current total population by compartment, outflow compartment, and months until transition will be made"""

REMAINING_SENTENCES_QUERY_TEMPLATE = """
    /* {description} */

    SELECT
      state_code,
      run_date,
      compartment,
      outflow_to,
      compartment_duration,
      gender,
      total_population
    FROM `{project_id}.{population_projection_dataset}.supervision_remaining_sentences`
    
    UNION ALL
    
    SELECT
        state_code,
        run_date,
        compartment,
        outflow_to,
        compartment_duration,
        gender,
        total_population
    FROM `{project_id}.{population_projection_dataset}.incarceration_remaining_sentences`

    UNION ALL

    SELECT
        state_code,
        run_date,
        compartment,
        outflow_to,
        compartment_duration,
        gender,
        SUM(total_population) AS total_population
    FROM `{project_id}.{population_projection_dataset}.us_id_rider_pbh_remaining_sentences`
    GROUP BY state_code, run_date, compartment, outflow_to, compartment_duration, gender
    """

REMAINING_SENTENCES_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=POPULATION_PROJECTION_DATASET,
    view_id=REMAINING_SENTENCES_VIEW_NAME,
    view_query_template=REMAINING_SENTENCES_QUERY_TEMPLATE,
    description=REMAINING_SENTENCES_VIEW_DESCRIPTION,
    population_projection_dataset=POPULATION_PROJECTION_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        REMAINING_SENTENCES_VIEW_BUILDER.build_and_print()
