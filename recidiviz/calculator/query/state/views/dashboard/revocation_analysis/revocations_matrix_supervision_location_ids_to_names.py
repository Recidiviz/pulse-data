# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Revocations by month."""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

REVOCATIONS_MATRIX_SUPERVISION_LOCATION_IDS_TO_NAMES_VIEW_NAME = (
    "revocations_matrix_supervision_location_ids_to_names"
)

REVOCATIONS_MATRIX_SUPERVISION_LOCATION_IDS_TO_NAMES_DESCRIPTION = """ Mapping of supervision locations to view names only for locations present in revocations matrix data."""

REVOCATIONS_MATRIX_SUPERVISION_LOCATION_IDS_TO_NAMES_QUERY_TEMPLATE = """
    /*{description}*/
    SELECT
      names.state_code,
      names.level_3_supervision_location_external_id,
      names.level_3_supervision_location_name,
      names.level_2_supervision_location_external_id,
      CASE WHEN names.state_code = 'US_MO' THEN UPPER(level_2_supervision_location_name)
           WHEN names.state_code = 'US_PA' THEN FORMAT("%s - %s", UPPER(TRIM(REPLACE(level_2_supervision_location_name, 'DO', ''))), level_2_supervision_location_external_id)
      END AS level_2_supervision_location_name,
      names.level_1_supervision_location_external_id,
      CASE WHEN names.state_code = 'US_MO' THEN UPPER(level_1_supervision_location_external_id)
           WHEN names.state_code = 'US_PA' THEN COALESCE(
               FORMAT("%s - %s", UPPER(TRIM(SPLIT(level_1_supervision_location_name, '-')[SAFE_OFFSET(1)])), level_2_supervision_location_external_id), 
               level_1_supervision_location_name
           )
      END AS level_1_supervision_location_name
    FROM `{project_id}.{reference_views_dataset}.supervision_location_ids_to_names` names
    JOIN (
      SELECT 
        DISTINCT state_code, level_1_supervision_location, level_2_supervision_location
      FROM `{project_id}.{shared_metric_views_dataset}.supervision_matrix_by_person_materialized`
    ) seen_locations
    ON names.state_code = seen_locations.state_code
        AND names.level_1_supervision_location_external_id = seen_locations.level_1_supervision_location
        AND CASE
            -- TODO(#3829): MO does not have level 2 values ingested, so level_2_supervision_location values are all
            -- 'ALL'. Once we do start ingesting MO region information, this state-specific clause can be removed.
            WHEN names.state_code = 'US_MO' THEN true
            ELSE names.level_2_supervision_location_external_id = seen_locations.level_2_supervision_location
        END
    ORDER BY 
        state_code,
        level_3_supervision_location_external_id,
        level_2_supervision_location_external_id,
        level_1_supervision_location_external_id;"""

REVOCATIONS_MATRIX_SUPERVISION_LOCATION_IDS_TO_NAMES_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=REVOCATIONS_MATRIX_SUPERVISION_LOCATION_IDS_TO_NAMES_VIEW_NAME,
    view_query_template=REVOCATIONS_MATRIX_SUPERVISION_LOCATION_IDS_TO_NAMES_QUERY_TEMPLATE,
    description=REVOCATIONS_MATRIX_SUPERVISION_LOCATION_IDS_TO_NAMES_DESCRIPTION,
    shared_metric_views_dataset=dataset_config.SHARED_METRIC_VIEWS_DATASET,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        REVOCATIONS_MATRIX_SUPERVISION_LOCATION_IDS_TO_NAMES_VIEW_BUILDER.build_and_print()
