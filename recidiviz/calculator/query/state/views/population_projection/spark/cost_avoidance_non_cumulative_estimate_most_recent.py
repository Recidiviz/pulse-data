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
"""Takes only the most recent non-cumulative cost avoidance data for each spark simulation tag"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

COST_AVOIDANCE_NON_CUMULATIVE_VIEW_NAME = (
    "cost_avoidance_non_cumulative_estimate_most_recent"
)

COST_AVOIDANCE_NON_CUMULATIVE_VIEW_DESCRIPTION = """Spark non-cumulative cost avoidance data, only most recent data for each simulation tag"""

COST_AVOIDANCE_NON_CUMULATIVE_QUERY_TEMPLATE = """
    WITH most_recent_uploads as (
      SELECT simulation_tag, MAX(date_created) as latest_run
      FROM `{project_id}.{spark_output_dataset}.cost_avoidance_non_cumulative_estimate_raw`
      GROUP BY simulation_tag
    )
    SELECT data.*
    FROM `{project_id}.{spark_output_dataset}.cost_avoidance_non_cumulative_estimate_raw` data
    JOIN most_recent_uploads ON data.simulation_tag = most_recent_uploads.simulation_tag
      AND data.date_created = most_recent_uploads.latest_run
"""

SPARK_COST_AVOIDANCE_NON_CUMULATIVE_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.SPARK_OUTPUT_DATASET_MOST_RECENT,
    view_id=COST_AVOIDANCE_NON_CUMULATIVE_VIEW_NAME,
    view_query_template=COST_AVOIDANCE_NON_CUMULATIVE_QUERY_TEMPLATE,
    description=COST_AVOIDANCE_NON_CUMULATIVE_VIEW_DESCRIPTION,
    spark_output_dataset=dataset_config.SPARK_OUTPUT_DATASET,
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SPARK_COST_AVOIDANCE_NON_CUMULATIVE_VIEW_BUILDER.build_and_print()
