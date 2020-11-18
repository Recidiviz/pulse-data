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
"""The projected cost saved by the simulated policy"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

COST_AVOIDANCE_ESTIMATE_VIEW_NAME = 'cost_avoidance_estimate'

COST_AVOIDANCE_ESTIMATE_VIEW_DESCRIPTION = \
    """"The projected cost saved by the simulated policy"""

COST_AVOIDANCE_ESTIMATE_QUERY_TEMPLATE = \
    """
    WITH most_recent_results AS (
      SELECT
        simulation_tag, MAX(date_created) AS date_created
      FROM `{project_id}.{population_projection_dataset}.cost_avoidance_estimate_raw`
      GROUP BY 1
    )
    SELECT
      simulation_tag,
      date_created,
      EXTRACT(YEAR FROM simulation_date) AS year,
      EXTRACT(MONTH FROM simulation_date) AS month,
      compartment,
      total_cost
    FROM `{project_id}.{population_projection_dataset}.cost_avoidance_estimate_raw`
    INNER JOIN most_recent_results
    USING (simulation_tag, date_created)
    """

COST_AVOIDANCE_ESTIMATE_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.POPULATION_PROJECTION_DATASET,
    view_id=COST_AVOIDANCE_ESTIMATE_VIEW_NAME,
    view_query_template=COST_AVOIDANCE_ESTIMATE_QUERY_TEMPLATE,
    description=COST_AVOIDANCE_ESTIMATE_VIEW_DESCRIPTION,
    population_projection_dataset=dataset_config.POPULATION_PROJECTION_DATASET,
    should_materialize=False
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        COST_AVOIDANCE_ESTIMATE_VIEW_BUILDER.build_and_print()
