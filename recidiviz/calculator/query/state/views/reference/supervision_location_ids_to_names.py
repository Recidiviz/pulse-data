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
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_LOCATION_IDS_TO_NAMES_VIEW_NAME = "supervision_location_ids_to_names"

SUPERVISION_LOCATION_IDS_TO_NAMES_DESCRIPTION = (
    """Reference table for supervision location names"""
)

SUPERVISION_LOCATION_IDS_TO_NAMES_QUERY_TEMPLATE = """
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
    ),
    ix_location_names AS (
        SELECT
            DISTINCT
                'US_IX' AS state_code,
                'NOT_APPLICABLE' AS level_3_supervision_location_external_id,
                'NOT_APPLICABLE' AS level_3_supervision_location_name,
                level_2_supervision_location_external_id,
                level_2_supervision_location_name,
                level_1_supervision_location_external_id,
                INITCAP(level_1_supervision_location_external_id) AS level_1_supervision_location_name,
        FROM `{project_id}.{us_ix_raw_data_up_to_date_dataset}.RECIDIVIZ_REFERENCE_supervision_location_districts_map_latest`
    ),
    mi_location_names AS (        
        SELECT
            DISTINCT
                'US_MI' AS state_code,
                'NOT_APPLICABLE' AS level_3_supervision_location_external_id,
                'NOT_APPLICABLE' AS level_3_supervision_location_name,
                REGION_ID AS level_2_supervision_location_external_id,
                REGION_NAME AS level_2_supervision_location_name,
                SUPERVISION_SITE AS level_1_supervision_location_external_id,
                SUPERVISION_SITE AS level_1_supervision_location_name,
        FROM `{project_id}.{us_mi_raw_data_up_to_date_dataset}.RECIDIVIZ_REFERENCE_supervision_location_ids_latest`
    )
    # TODO(#19319): Delete this clause / logic when ME supervision locations added to location_metadata
    SELECT * FROM me_location_names
    UNION ALL
    # TODO(#19340): Delete this clause / logic when MO supervision locations added to location_metadata
    SELECT * FROM mo_location_names
    UNION ALL
    SELECT * FROM id_location_names
    UNION ALL
    # TODO(#19317): Delete this clause / logic when IX supervision locations added to location_metadata
    SELECT * FROM ix_location_names
    UNION ALL
    # TODO(#19316): Delete this clause / logic when MI supervision locations added to location_metadata
    SELECT * FROM mi_location_names
    UNION ALL

    # TODO(#19343): Eventually update the output columns to be the "nice" names in the 
    # `location_metadata` table (e.g. 'supervision_region_id', 'supervision_district_id',
    # etc instead of level_1/level_2...).
    SELECT DISTINCT
        state_code, 
        COALESCE(
            JSON_EXTRACT_SCALAR(location_metadata, '$.supervision_region_id'),
            'NOT_APPLICABLE'
        ) AS level_3_supervision_location_external_id,
        COALESCE(
            JSON_EXTRACT_SCALAR(location_metadata, '$.supervision_region_name'),
            'NOT_APPLICABLE'
        ) AS level_3_supervision_location_name,
        JSON_EXTRACT_SCALAR(location_metadata, '$.supervision_district_id') AS level_2_supervision_location_external_id,
        JSON_EXTRACT_SCALAR(location_metadata, '$.supervision_district_name') AS level_2_supervision_location_name,
        JSON_EXTRACT_SCALAR(location_metadata, '$.supervision_office_id') AS level_1_supervision_location_external_id,
        JSON_EXTRACT_SCALAR(location_metadata, '$.supervision_office_name') AS level_1_supervision_location_name
    FROM `{project_id}.{reference_views_dataset}.location_metadata_materialized`
    WHERE location_type = 'SUPERVISION_LOCATION' and JSON_EXTRACT_SCALAR(location_metadata, '$.supervision_office_id') IS NOT NULL;
    """

SUPERVISION_LOCATION_IDS_TO_NAMES_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_VIEWS_DATASET,
    view_id=SUPERVISION_LOCATION_IDS_TO_NAMES_VIEW_NAME,
    view_query_template=SUPERVISION_LOCATION_IDS_TO_NAMES_QUERY_TEMPLATE,
    description=SUPERVISION_LOCATION_IDS_TO_NAMES_DESCRIPTION,
    external_reference_dataset=EXTERNAL_REFERENCE_DATASET,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    us_mo_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_MO, instance=DirectIngestInstance.PRIMARY
    ),
    us_me_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_ME, instance=DirectIngestInstance.PRIMARY
    ),
    us_mi_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_MI, instance=DirectIngestInstance.PRIMARY
    ),
    us_ix_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_IX, instance=DirectIngestInstance.PRIMARY
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_LOCATION_IDS_TO_NAMES_VIEW_BUILDER.build_and_print()
