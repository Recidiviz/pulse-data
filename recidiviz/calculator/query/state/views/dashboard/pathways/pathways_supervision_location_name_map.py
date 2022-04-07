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
"""Pathways helper view to map from location ID to aggregating office name."""
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.dashboard.pathways.pathways_enabled_states import (
    ENABLED_STATES,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.pathways_metric_big_query_view import (
    PathwaysMetricBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

ENABLED_STATE_CODES = ", ".join(f'"{state}"' for state in ENABLED_STATES)

PATHWAYS_SUPERVISION_LOCATION_NAME_MAP_VIEW_NAME = (
    "pathways_supervision_location_name_map"
)

PATHWAYS_SUPERVISION_LOCATION_NAME_MAP_DESCRIPTION = (
    "Map by state from district id to the name of the location it will be aggregated by"
)

PATHWAYS_SUPERVISION_LOCATION_NAME_MAP_QUERY_TEMPLATE = """
    /*{description}*/
    SELECT
        state_code,
        level_1_supervision_location_external_id AS location_id,
        CASE state_code
            WHEN "US_ID" THEN level_2_supervision_location_name
            WHEN "US_ME" THEN INITCAP(level_1_supervision_location_name)
            WHEN "US_ND" THEN INITCAP(level_1_supervision_location_name)
            WHEN "US_TN" THEN 
                CASE
                    WHEN level_2_supervision_location_name = "NOT_APPLICABLE" THEN "OTHER"
                    WHEN level_2_supervision_location_name = "INTERNAL_UNKNOWN" THEN "INTERNAL_UNKNOWN"
                    ELSE INITCAP(level_2_supervision_location_name)
                END
            ELSE INITCAP(level_1_supervision_location_name)
        END AS location_name,
    FROM `{project_id}.{reference_views_dataset}.supervision_location_ids_to_names`
    WHERE state_code IN ({enabled_states})
"""

PATHWAYS_SUPERVISION_LOCATION_NAME_MAP_VIEW_BUILDER = PathwaysMetricBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=PATHWAYS_SUPERVISION_LOCATION_NAME_MAP_VIEW_NAME,
    view_query_template=PATHWAYS_SUPERVISION_LOCATION_NAME_MAP_QUERY_TEMPLATE,
    description=PATHWAYS_SUPERVISION_LOCATION_NAME_MAP_DESCRIPTION,
    dimensions=("state_code",),
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    enabled_states=ENABLED_STATE_CODES,
)


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        PATHWAYS_SUPERVISION_LOCATION_NAME_MAP_VIEW_BUILDER.build_and_print()
