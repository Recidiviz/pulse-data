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

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.common.constants.states import StateCode
from recidiviz.datasets.static_data.config import EXTERNAL_REFERENCE_DATASET
from recidiviz.ingest.direct.raw_data.dataset_config import (
    raw_latest_views_dataset_for_region,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_LOCATION_IDS_TO_NAMES_VIEW_NAME = "supervision_location_ids_to_names"

SUPERVISION_LOCATION_IDS_TO_NAMES_DESCRIPTION = (
    """Reference table for supervision location names"""
)

SUPERVISION_LOCATION_IDS_TO_NAMES_QUERY_TEMPLATE = """
    /*{description}*/
    WITH
    me_location_names AS (
        SELECT
            DISTINCT 'US_ME' AS state_code,
            'NOT_APPLICABLE' AS level_3_supervision_location_external_id,
            'NOT_APPLICABLE' AS level_3_supervision_location_name,
            Cis_9009_Region_Cd AS level_2_supervision_location_external_id,
            CASE
                WHEN Cis_9009_Region_Cd = '1' THEN 'Region 1 Portland'
                WHEN Cis_9009_Region_Cd = '2' THEN 'Region 2 Auburn'
                WHEN Cis_9009_Region_Cd = '3' THEN 'Region 3 Bangor'
                WHEN Cis_9009_Region_Cd = '4' THEN 'Region 4'
                WHEN Cis_9009_Region_Cd = '5' THEN 'Central Office'
            ELSE NULL
            END AS level_2_supervision_location_name,
            -- TODO(#11167): Update this to use the external ID instead of location name once it is hydrated in the
            -- entities.
            UPPER(Location_Name) AS level_1_supervision_location_external_id,
            TRIM(
                REGEXP_REPLACE(
                REGEXP_REPLACE(Location_Name, r',|Adult1|Adult|\\(Main Office\\)', ''), 
                r'\\(Region (\\d)\\)', 
                r'(R\\1)'
                )
            ) AS level_1_supervision_location_name
        FROM `{project_id}.{us_me_raw_data_up_to_date_dataset}.CIS_908_CCS_LOCATION_latest`
        -- Adult Supervision Office Location Type
        WHERE Cis_9080_Ccs_Location_Type_Cd = '4'
    ),
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
            FROM `{project_id}.{us_mo_raw_data_up_to_date_dataset}.LBAKRDTA_TAK034_latest`
        )
        LEFT OUTER JOIN
            `{project_id}.{us_mo_raw_data_up_to_date_dataset}.RECIDIVIZ_REFERENCE_supervision_district_to_name_latest`
        USING (level_1_supervision_location_external_id)
        LEFT OUTER JOIN
            `{project_id}.{us_mo_raw_data_up_to_date_dataset}.RECIDIVIZ_REFERENCE_supervision_district_to_region_latest`
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
            `{project_id}.{us_nd_raw_data_up_to_date_dataset}.RECIDIVIZ_REFERENCE_supervision_district_id_to_name_latest`
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
        FROM `{project_id}.{us_pa_raw_data_up_to_date_dataset}.RECIDIVIZ_REFERENCE_supervision_location_ids_latest`
    ),
    tn_location_names AS (
        SELECT
            DISTINCT 'US_TN' AS state_code,
            division AS level_3_supervision_location_external_id,
            CASE
                WHEN division = 'NOT_APPLICABLE' THEN division
                WHEN division = 'INTERNAL_UNKNOWN' THEN division
                ELSE INITCAP(division)
            END AS level_3_supervision_location_name,
            district AS level_2_supervision_location_external_id,
            CASE
                WHEN district = 'NOT_APPLICABLE' THEN district
                WHEN district = 'INTERNAL_UNKNOWN' THEN district
                ELSE IF(CONTAINS_SUBSTR(district, ','),
                    CONCAT('Districts ',
                        SUBSTR(district, 0, STRPOS(district, ',') - 1),
                        ' and ',
                        SUBSTR(district, STRPOS(district, ',') + 1)),
                    CONCAT('District ', district))
            END AS level_2_supervision_location_name,
            site_code AS level_1_supervision_location_external_id,
            site_name AS level_1_supervision_location_name
        FROM `{project_id}.{external_reference_dataset}.us_tn_supervision_locations`
    ),
    id_location_names AS (
        SELECT
            DISTINCT
                'US_ID' AS state_code,
                'NOT_APPLICABLE' AS level_3_supervision_location_external_id,
                'NOT_APPLICABLE' AS level_3_supervision_location_name,
                level_2_supervision_location_external_id,
                level_2_supervision_location_name,
                level_1_supervision_location_external_id,
                INITCAP(level_1_supervision_location_external_id) AS level_1_supervision_location_name,
        FROM `{project_id}.{external_reference_dataset}.us_id_supervision_unit_to_district_map`
        LEFT OUTER JOIN
            `{project_id}.{external_reference_dataset}.us_id_supervision_district_names`
        USING (level_2_supervision_location_external_id)
    )
    SELECT * FROM me_location_names
    UNION ALL
    SELECT * FROM mo_location_names
    UNION ALL
    SELECT * FROM nd_location_names
    UNION ALL
    SELECT * FROM pa_location_names
    UNION ALL
    SELECT * FROM tn_location_names
    UNION ALL
    SELECT * FROM id_location_names;
    """

SUPERVISION_LOCATION_IDS_TO_NAMES_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_VIEWS_DATASET,
    view_id=SUPERVISION_LOCATION_IDS_TO_NAMES_VIEW_NAME,
    view_query_template=SUPERVISION_LOCATION_IDS_TO_NAMES_QUERY_TEMPLATE,
    description=SUPERVISION_LOCATION_IDS_TO_NAMES_DESCRIPTION,
    external_reference_dataset=EXTERNAL_REFERENCE_DATASET,
    us_mo_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        StateCode.US_MO.value
    ),
    us_nd_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        StateCode.US_ND.value
    ),
    us_pa_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        StateCode.US_PA.value
    ),
    us_me_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        StateCode.US_ME.value
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_LOCATION_IDS_TO_NAMES_VIEW_BUILDER.build_and_print()
