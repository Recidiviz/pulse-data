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
"""Reference table for supervision location names."""

# pylint: disable=trailing-whitespace
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_LOCATION_IDS_TO_NAMES_VIEW_NAME = "supervision_location_ids_to_names"

SUPERVISION_LOCATION_IDS_TO_NAMES_DESCRIPTION = (
    """Reference table for supervision location names"""
)

SUPERVISION_LOCATION_IDS_TO_NAMES_QUERY_TEMPLATE = """
    /*{description}*/
    WITH
    mo_location_names AS (        
        SELECT
            DISTINCT
                'US_MO' AS state_code,
                'NOT_APPLICABLE' AS level_3_supervision_location_external_id,
                'NOT_APPLICABLE' AS level_3_supervision_location_name,
                level_2_supervision_location_external_id,
                level_2_supervision_location_external_id  AS level_2_supervision_location_name,
                level_1_supervision_location_external_id,
                level_1_supervision_location_name,
        FROM (
            SELECT DISTINCT CE_PLN AS level_1_supervision_location_external_id
            FROM `{project_id}.us_mo_raw_data_up_to_date_views.LBAKRDTA_TAK034_latest`
        )
        LEFT OUTER JOIN
            `{project_id}.us_mo_raw_data_up_to_date_views.RECIDIVIZ_REFERENCE_supervision_district_to_name_latest`
        USING (level_1_supervision_location_external_id)
        LEFT OUTER JOIN
            `{project_id}.us_mo_raw_data_up_to_date_views.RECIDIVIZ_REFERENCE_supervision_district_to_region_latest`
        USING (level_1_supervision_location_external_id)
    ),
    nd_location_names AS (
        SELECT
            DISTINCT
                'US_ND' AS state_code,
                'NOT_APPLICABLE' AS level_3_supervision_location_external_id,
                'NOT_APPLICABLE' AS level_3_supervision_location_name,
                'NOT_APPLICABLE' AS level_2_supervision_location_external_id,
                'NOT_APPLICABLE' AS level_2_supervision_location_name,
                supervising_district_external_id as level_1_supervision_location_external_id,
                supervising_district_name as level_1_supervision_location_name,
        FROM
            `{project_id}.us_nd_raw_data_up_to_date_views.RECIDIVIZ_REFERENCE_supervision_district_id_to_name_latest`
    ),
    pa_location_names AS (
        SELECT 
            DISTINCT 'US_PA' AS state_code,
            level_3_supervision_location_external_id,
            level_3_supervision_location_name,
            level_2_supervision_location_external_id,
            level_2_supervision_location_name,
            UPPER(level_1_supervision_location_external_id) AS level_1_supervision_location_external_id,
            level_1_supervision_location_name,
        FROM `{project_id}.us_pa_raw_data_up_to_date_views.RECIDIVIZ_REFERENCE_supervision_location_ids_latest`
    )
    SELECT * FROM mo_location_names
    UNION ALL
    SELECT * FROM nd_location_names
    UNION ALL
    SELECT * FROM pa_location_names;    
    """

SUPERVISION_LOCATION_IDS_TO_NAMES_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_VIEWS_DATASET,
    view_id=SUPERVISION_LOCATION_IDS_TO_NAMES_VIEW_NAME,
    view_query_template=SUPERVISION_LOCATION_IDS_TO_NAMES_QUERY_TEMPLATE,
    description=SUPERVISION_LOCATION_IDS_TO_NAMES_DESCRIPTION,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_LOCATION_IDS_TO_NAMES_VIEW_BUILDER.build_and_print()
