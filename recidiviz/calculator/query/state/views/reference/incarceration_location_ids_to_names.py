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
"""Reference table for incarceration location names."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import REFERENCE_VIEWS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

INCARCERATION_LOCATION_IDS_TO_NAMES_VIEW_NAME = "incarceration_location_ids_to_names"

INCARCERATION_LOCATION_IDS_TO_NAMES_DESCRIPTION = (
    """Reference table for incarceration location ids and names"""
)

INCARCERATION_LOCATION_IDS_TO_NAMES_QUERY_TEMPLATE = """
    WITH
    me_location_names AS (
        SELECT 
            DISTINCT 'US_ME' AS state_code,
            'NOT_APPLICABLE' AS level_3_incarceration_location_external_id,
            'NOT_APPLICABLE' AS level_3_incarceration_location_name,
            Cis_9009_Region_Cd AS level_2_incarceration_location_external_id,
            CASE 
                WHEN Cis_9009_Region_Cd = '1' THEN 'Region 1 Portland'
                WHEN Cis_9009_Region_Cd = '2' THEN 'Region 2 Auburn'
                WHEN Cis_9009_Region_Cd = '3' THEN 'Region 3 Bangor'
                WHEN Cis_9009_Region_Cd = '4' THEN 'Region 4'
                WHEN Cis_9009_Region_Cd = '5' THEN 'Central Office'
            ELSE NULL
            END AS level_2_incarceration_location_name,
            -- TODO(#11167): Update this to use the external ID instead of location name once it is hydrated in the
            -- entities.
            CASE 
                WHEN Location_Name IN ('Mountain View Adult Center', 'Charleston Correctional Facility')
                    THEN 'MOUNTAIN VIEW CORRECTIONAL FACILITY' 
                WHEN Location_Name = 'Southern Maine Pre-Release'
                    THEN "SOUTHERN MAINE WOMEN'S REENTRY CENTER"
                ELSE UPPER(Location_Name)
            END AS level_1_incarceration_location_external_id,
            CASE 
                WHEN Location_Name IN ('Mountain View Adult Center', 'Charleston Correctional Facility')
                THEN 'Mountain View Correctional Facility' 
                WHEN Location_Name = 'Southern Maine Pre-Release'
                THEN "Southern Maine Women's ReEntry Center"
                ELSE Location_Name 
            END AS level_1_incarceration_location_name,
            facility_code AS level_1_incarceration_location_alias
        FROM `{project_id}.{us_me_raw_data_up_to_date_dataset}.CIS_908_CCS_LOCATION_latest` raw
        LEFT JOIN `{project_id}.gcs_backed_tables.us_me_incarceration_facility_names` map
            ON raw.Location_Name = map.facility_name
        -- Filter to adult facilities and re-entry centers
        WHERE Cis_9080_Ccs_Location_Type_Cd IN (
            '2', -- Adult DOC Facilities
            '7', -- Adult Pre-Release Centers
            '16' -- Re-Entry Centers
        )
    ),
    id_location_names AS (
        SELECT
            DISTINCT
                'US_ID' AS state_code,
                'NOT_APPLICABLE' AS level_3_incarceration_location_external_id,
                'NOT_APPLICABLE' AS level_3_incarceration_location_name,
                facility_code,
                INITCAP(facility_name),
                level_1_incarceration_location_external_id,
                INITCAP(level_1_incarceration_location_external_id) AS level_1_incarceration_location_name,
                level_1_incarceration_location_external_id AS level_1_incarceration_location_alias
        FROM `{project_id}.gcs_backed_tables.us_id_incarceration_facility_map`
        LEFT OUTER JOIN
            `{project_id}.gcs_backed_tables.us_id_incarceration_facility_names`
        ON level_2_incarceration_location_external_id = facility_code
    ),
    ix_location_names AS (
        SELECT
            DISTINCT
                'US_IX' AS state_code,
                'NOT_APPLICABLE' AS level_3_incarceration_location_external_id,
                'NOT_APPLICABLE' AS level_3_incarceration_location_name,
                level_2_incarceration_location_external_id,
                level_2_incarceration_location_name,
                level_1_incarceration_location_external_id,
                INITCAP(level_1_incarceration_location_external_id) AS level_1_incarceration_location_name,
                level_1_incarceration_location_external_id AS level_1_incarceration_location_alias
        FROM `{project_id}.{us_ix_raw_data_up_to_date_dataset}.RECIDIVIZ_REFERENCE_incarceration_location_names_map_latest`
    ),
    mi_location_names AS (
        SELECT
            DISTINCT
                'US_MI' AS state_code,
                'NOT_APPLICABLE' AS level_3_incarceration_location_external_id,
                'NOT_APPLICABLE' AS level_3_incarceration_location_name,
                'NOT_APPLICABLE' AS level_2_incarceration_location_external_id,
                'NOT_APPLICABLE' AS level_2_incarceration_location_name,
                facility_code AS level_1_incarceration_location_external_id,
                facility_name AS level_1_incarceration_location_name,
                facility_code AS level_1_incarceration_location_alias
        FROM `{project_id}.gcs_backed_tables.us_mi_incarceration_facility_names`
    ),
    co_location_names AS (
        WITH level_2_names AS (
            SELECT
                facility_code as level_2_id,
                facility_name as level_2_name
            FROM `{project_id}.gcs_backed_tables.us_co_incarceration_facility_names`
            -- Exclude the Pueblo YOS level 1 facility to remove duplicates
            WHERE facility_name != "Youthful Offender System-Pueblo"
        ),
        level_3_names AS (
            SELECT
                facility_code as level_3_id,
                facility_name as level_3_name
            FROM `{project_id}.gcs_backed_tables.us_co_incarceration_facility_names`
        )
        SELECT
            DISTINCT
                'US_CO' AS state_code,
                level_3_incarceration_location_external_id AS level_3_incarceration_location_external_id,
                level_3_name AS level_3_incarceration_location_name,
                level_2_incarceration_location_external_id,
                level_2_name as level_2_incarceration_location_name,
                facility_code AS level_1_incarceration_location_external_id,
                facility_name AS level_1_incarceration_location_name,
                facility_code AS level_1_incarceration_location_alias
        FROM `{project_id}.gcs_backed_tables.us_co_incarceration_facility_map`
        LEFT JOIN
            `{project_id}.gcs_backed_tables.us_co_incarceration_facility_names`
        ON level_1_incarceration_location_external_id = facility_code
        LEFT JOIN level_2_names
        ON level_2_incarceration_location_external_id = level_2_id
        LEFT JOIN level_3_names
        ON level_3_incarceration_location_external_id = level_3_id
        -- Exclude the YOS level 2 facility to remove duplicates
        WHERE facility_name != "YOS Facility"
    ),
    mo_location_names AS (
        SELECT 
             DISTINCT
                'US_MO' AS state_code,
                'NOT_APPLICABLE' AS level_3_incarceration_location_external_id,
                'NOT_APPLICABLE' AS level_3_incarceration_location_name,
                'NOT_APPLICABLE' AS level_2_incarceration_location_external_id,
                'NOT_APPLICABLE' AS level_2_incarceration_location_name,
                incarceration_location_level_1_external_id AS level_1_incarceration_location_external_id,
                incarceration_location_level_1_name AS level_1_incarceration_location_name,
                incarceration_location_level_1_external_id AS level_1_incarceration_location_alias
        FROM `{project_id}.gcs_backed_tables.us_mo_incarceration_facility_names`
    )
    # TODO(#19319): Delete this clause / logic when ME incarceration locations added to location_metadata
    SELECT * FROM me_location_names
    UNION ALL
    SELECT * FROM id_location_names
    UNION ALL
    # TODO(#19317): Delete this clause / logic when IX incarceration locations added to location_metadata
    SELECT * FROM ix_location_names
    UNION ALL
    # TODO(#19316): Delete this clause / logic when MI incarceration locations added to location_metadata
    SELECT * FROM mi_location_names
    UNION ALL
    # TODO(#20339): Delete this clause / logic when CO incarceration locations added to location_metadata
    SELECT * FROM co_location_names
    UNION ALL
    # TODO(#19340): Delete this clause / logic when MO incarceration locations added to location_metadata
    SELECT * FROM mo_location_names
    
    UNION ALL

    # TODO(#19343): Eventually update the output columns to be the "nice" names in the 
    # `location_metadata` table (e.g. 'supervision_region_id', 'supervision_district_id',
    # etc instead of level_1/level_2...).
    SELECT DISTINCT
        state_code, 
        'NOT_APPLICABLE' AS level_3_incarceration_location_external_id,
        'NOT_APPLICABLE' AS level_3_incarceration_location_name,
        COALESCE(
            JSON_EXTRACT_SCALAR(location_metadata, '$.facility_group_external_id'), 
            'NOT_APPLICABLE'
        ) AS level_2_incarceration_location_external_id,
        COALESCE(
            JSON_EXTRACT_SCALAR(location_metadata, '$.facility_group_name'), 
            'NOT_APPLICABLE'
        ) AS level_2_incarceration_location_name,
        location_external_id AS level_1_incarceration_location_external_id,
        location_name AS level_1_incarceration_location_name,
        JSON_EXTRACT_SCALAR(location_metadata, '$.location_acronym') AS level_1_incarceration_location_alias
    FROM `{project_id}.{reference_views_dataset}.location_metadata_materialized`
    WHERE location_type IN (
        'STATE_PRISON',
        'COUNTY_JAIL',
        'FEDERAL_PRISON',
        'RESIDENTIAL_PROGRAM',
        'OUT_OF_STATE'
    );
    """

INCARCERATION_LOCATION_IDS_TO_NAMES_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_VIEWS_DATASET,
    view_id=INCARCERATION_LOCATION_IDS_TO_NAMES_VIEW_NAME,
    view_query_template=INCARCERATION_LOCATION_IDS_TO_NAMES_QUERY_TEMPLATE,
    description=INCARCERATION_LOCATION_IDS_TO_NAMES_DESCRIPTION,
    reference_views_dataset=REFERENCE_VIEWS_DATASET,
    us_me_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_ME, instance=DirectIngestInstance.PRIMARY
    ),
    us_ix_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_IX, instance=DirectIngestInstance.PRIMARY
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        INCARCERATION_LOCATION_IDS_TO_NAMES_VIEW_BUILDER.build_and_print()
