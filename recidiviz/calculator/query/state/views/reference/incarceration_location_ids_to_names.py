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
from recidiviz.datasets.static_data.config import EXTERNAL_REFERENCE_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

INCARCERATION_LOCATION_IDS_TO_NAMES_VIEW_NAME = "incarceration_location_ids_to_names"

INCARCERATION_LOCATION_IDS_TO_NAMES_DESCRIPTION = (
    """Reference table for incarceration location ids and names"""
)

INCARCERATION_LOCATION_IDS_TO_NAMES_QUERY_TEMPLATE = """
    /*{description}*/
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
            Ccs_Location_Id AS level_1_incarceration_location_external_id,
            CASE 
                WHEN Location_Name IN ('Mountain View Adult Center', 'Charleston Correctional Facility')
                THEN 'Mountain View Correctional Facility' 
                WHEN Location_Name = 'Southern Maine Pre-Release'
                THEN "Southern Maine Women's ReEntry Center"
                ELSE Location_Name 
            END AS level_1_incarceration_location_name
        -- TODO(#10636): Replace this with CIS_908_CCS_LOCATION_latest once US_ME raw data is available in production.
        FROM `{project_id}.static_reference_tables.us_me_cis_908_ccs_location`
        -- Filter to adult facilities and re-entry centers
        WHERE Cis_9080_Ccs_Location_Type_Cd IN ('2', '7', '16')
    ), nd_location_names AS (
        SELECT
            'US_ND' AS state_code,
            'NOT_APPLICABLE' AS level_3_incarceration_location_external_id,
            'NOT_APPLICABLE' AS level_3_incarceration_location_name,
            'NOT_APPLICABLE' AS level_2_incarceration_location_external_id,
            'NOT_APPLICABLE' AS level_2_incarceration_location_name,
            facility_code AS level_1_incarceration_location_external_id,
            facility_name AS level_1_incarceration_location_name,
        FROM `{project_id}.{external_reference_dataset}.us_nd_incarceration_facility_names`
    )
    SELECT * FROM me_location_names
    UNION ALL
    SELECT * FROM nd_location_names
    """

INCARCERATION_LOCATION_IDS_TO_NAMES_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_VIEWS_DATASET,
    view_id=INCARCERATION_LOCATION_IDS_TO_NAMES_VIEW_NAME,
    view_query_template=INCARCERATION_LOCATION_IDS_TO_NAMES_QUERY_TEMPLATE,
    description=INCARCERATION_LOCATION_IDS_TO_NAMES_DESCRIPTION,
    external_reference_dataset=EXTERNAL_REFERENCE_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        INCARCERATION_LOCATION_IDS_TO_NAMES_VIEW_BUILDER.build_and_print()
