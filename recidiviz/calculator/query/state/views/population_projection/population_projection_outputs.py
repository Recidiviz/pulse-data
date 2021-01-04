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
"""The population projection simulation output views that select the latest run for each simulation_tag"""
from dataclasses import dataclass
from typing import List

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


@dataclass
class PopulationProjectionOutputView:
    output_name: str
    output_description: str
    output_columns: List[str]


POPULATION_PROJECTION_OUTPUT_VIEWS = [
    PopulationProjectionOutputView(output_name='cost_avoidance_estimate',
                                   output_description='The projected cumulative cost saved by the simulated policy',
                                   output_columns=['total_cost']),
    PopulationProjectionOutputView(output_name='cost_avoidance_non_cumulative_estimate',
                                   output_description='The projected cost saved by the simulated policy per '
                                                      'year/month (non-cumulative',
                                   output_columns=['total_cost']),
    PopulationProjectionOutputView(output_name='life_years_estimate',
                                   output_description='The projected cumulative life years saved by the simulated '
                                                      'policy',
                                   output_columns=['life_years']),
    PopulationProjectionOutputView(output_name='population_estimate',
                                   output_description='The projected population for the simulated policy and the '
                                                      'baseline',
                                   output_columns=['scenario', 'population']),
]

POPULATION_PROJECTION_OUTPUT_QUERY_TEMPLATE = \
    """
    /*{description}*/
    WITH most_recent_results AS (
      SELECT
        simulation_tag, MAX(date_created) AS date_created
      FROM `{project_id}.{population_projection_dataset}.{output_name}_raw`
      GROUP BY 1
    )
    SELECT
      simulation_tag,
      date_created,
      EXTRACT(YEAR FROM simulation_date) AS year,
      EXTRACT(MONTH FROM simulation_date) AS month,
      compartment,
      {output_columns}
    FROM `{project_id}.{population_projection_dataset}.{output_name}_raw`
    INNER JOIN most_recent_results
    USING (simulation_tag, date_created)
    """

POPULATION_PROJECTION_OUTPUT_VIEW_BUILDERS = [
    SimpleBigQueryViewBuilder(dataset_id=dataset_config.POPULATION_PROJECTION_DATASET,
                              view_id=output.output_name,
                              view_query_template=POPULATION_PROJECTION_OUTPUT_QUERY_TEMPLATE,
                              description=output.output_description,
                              population_projection_dataset=dataset_config.POPULATION_PROJECTION_DATASET,
                              output_name=output.output_name,
                              output_columns=', '.join(output.output_columns),
                              should_materialize=False
                              )
    for output in POPULATION_PROJECTION_OUTPUT_VIEWS
]

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        for pop_projection_output_view_builder in POPULATION_PROJECTION_OUTPUT_VIEW_BUILDERS:
            pop_projection_output_view_builder.build_and_print()
