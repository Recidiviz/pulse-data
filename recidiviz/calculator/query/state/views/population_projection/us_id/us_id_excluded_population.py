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
"""Historical population that should be excluded from population projection inputs and outputs"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_ID_EXCLUDED_POPULATION_VIEW_NAME = "us_id_excluded_population"

US_ID_EXCLUDED_POPULATION_VIEW_DESCRIPTION = """"Historical population to exclude from population projection inputs and outputs per year and month"""

US_ID_EXCLUDED_POPULATION_QUERY_TEMPLATE = """
    SELECT
      state_code,
      compartment,
      run_date,
      time_step,
      SUM(total_population) AS total_population,
    FROM `{project_id}.{population_projection_dataset}.us_id_total_jail_population`,
    UNNEST(['INCARCERATION - ALL', compartment]) AS compartment
    GROUP BY state_code, compartment, run_date, time_step
    """

US_ID_EXCLUDED_POPULATION_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.POPULATION_PROJECTION_DATASET,
    view_id=US_ID_EXCLUDED_POPULATION_VIEW_NAME,
    view_query_template=US_ID_EXCLUDED_POPULATION_QUERY_TEMPLATE,
    description=US_ID_EXCLUDED_POPULATION_VIEW_DESCRIPTION,
    population_projection_dataset=dataset_config.POPULATION_PROJECTION_DATASET,
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ID_EXCLUDED_POPULATION_VIEW_BUILDER.build_and_print()
