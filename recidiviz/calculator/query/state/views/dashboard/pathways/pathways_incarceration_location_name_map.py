#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2022 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""Pathways helper view to map from location ID to aggregating location id and name"""
from recidiviz.calculator.query.pathways_bq_utils import filter_to_pathways_states
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.state_specific_query_strings import (
    PATHWAYS_LEVEL_2_INCARCERATION_LOCATION_OPTIONS,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.pathways_metric_big_query_view import (
    PathwaysMetricBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

PATHWAYS_INCARCERATION_LOCATION_NAME_MAP_VIEW_NAME = (
    "pathways_incarceration_location_name_map"
)

PATHWAYS_INCARCERATION_LOCATION_NAME_MAP_DESCRIPTION = "Map by state from location id to the name and aggregating id of the location it will be aggregated by"

PATHWAYS_INCARCERATION_LOCATION_NAME_MAP_QUERY_TEMPLATE = """
    WITH data AS (
        SELECT
            state_code,
            level_1_incarceration_location_external_id AS location_id,
            CASE
                WHEN state_code IN {pathways_level_2_incarceration_state_codes}
                    THEN level_2_incarceration_location_external_id
                WHEN state_code = 'US_ND'
                    THEN IF(level_1_incarceration_location_external_id = 'TRCC', 'TRC', level_1_incarceration_location_external_id)
                ELSE level_1_incarceration_location_external_id
            END AS aggregating_location_id,
            level_1_incarceration_location_name AS location_name,
        FROM `{project_id}.{reference_views_dataset}.incarceration_location_ids_to_names`
        {filter_to_pathways_states}
    )
    SELECT
        {dimensions_clause},
        aggregating_location_id
    FROM data
"""

PATHWAYS_INCARCERATION_LOCATION_NAME_MAP_VIEW_BUILDER = PathwaysMetricBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=PATHWAYS_INCARCERATION_LOCATION_NAME_MAP_VIEW_NAME,
    view_query_template=PATHWAYS_INCARCERATION_LOCATION_NAME_MAP_QUERY_TEMPLATE,
    description=PATHWAYS_INCARCERATION_LOCATION_NAME_MAP_DESCRIPTION,
    dimensions=("state_code", "location_id", "location_name"),
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    filter_to_pathways_states=filter_to_pathways_states(state_code_column="state_code"),
    pathways_level_2_incarceration_state_codes=PATHWAYS_LEVEL_2_INCARCERATION_LOCATION_OPTIONS,
)


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        PATHWAYS_INCARCERATION_LOCATION_NAME_MAP_VIEW_BUILDER.build_and_print()
