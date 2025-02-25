# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Takes only the most recent population data for each spark simulation tag"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

POPULATION_VIEW_NAME = "population_estimate_most_recent"

POPULATION_VIEW_DESCRIPTION = (
    """Spark population data, only most recent data for each simulation tag"""
)

POPULATION_QUERY_TEMPLATE = """
    SELECT *
    FROM `{project_id}.{spark_output_dataset}.population_estimate_raw`
    QUALIFY RANK() OVER (PARTITION BY simulation_tag ORDER BY date_created DESC) = 1
"""

SPARK_POPULATION_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.SPARK_OUTPUT_DATASET_MOST_RECENT,
    view_id=POPULATION_VIEW_NAME,
    view_query_template=POPULATION_QUERY_TEMPLATE,
    description=POPULATION_VIEW_DESCRIPTION,
    projects_to_deploy={GCP_PROJECT_STAGING},
    spark_output_dataset=dataset_config.SPARK_OUTPUT_DATASET,
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SPARK_POPULATION_VIEW_BUILDER.build_and_print()
