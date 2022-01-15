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
"""Pathways helper view to map from location ID to aggregating location name."""
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.dashboard.pathways.pathways_enabled_states import (
    ENABLED_STATES,
)
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

ENABLED_STATE_CODES = ", ".join(f'"{state}"' for state in ENABLED_STATES)

PATHWAYS_INCARCERATION_LOCATION_NAME_MAP_VIEW_NAME = (
    "pathways_incarceration_location_name_map"
)

PATHWAYS_INCARCERATION_LOCATION_NAME_MAP_DESCRIPTION = (
    "Map by state from location id to the name of the location it will be aggregated by"
)

PATHWAYS_INCARCERATION_LOCATION_NAME_MAP_QUERY_TEMPLATE = """
    /*{description}*/
    SELECT
        state_code,
        level_1_incarceration_location_external_id AS location_id,
        level_1_incarceration_location_name AS location_name,
    FROM `{project_id}.{reference_views_dataset}.incarceration_location_ids_to_names`
    WHERE state_code IN ({enabled_states})
"""

PATHWAYS_INCARCERATION_LOCATION_NAME_MAP_VIEW_BUILDER = MetricBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=PATHWAYS_INCARCERATION_LOCATION_NAME_MAP_VIEW_NAME,
    view_query_template=PATHWAYS_INCARCERATION_LOCATION_NAME_MAP_QUERY_TEMPLATE,
    description=PATHWAYS_INCARCERATION_LOCATION_NAME_MAP_DESCRIPTION,
    dimensions=("state_code", "location_id", "location_name"),
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    enabled_states=ENABLED_STATE_CODES,
)


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        PATHWAYS_INCARCERATION_LOCATION_NAME_MAP_VIEW_BUILDER.build_and_print()
