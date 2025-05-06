# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Reference table with metadata about specific locations in TN that can be associated
with a person or staff member.
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.reference.location_metadata.location_metadata_key import (
    LocationMetadataKey,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_TN_LOCATION_METADATA_VIEW_NAME = "us_tn_location_metadata"

# TODO(#23114): Revisit SiteType mappings and add additional metadata information when
# required for supervision or facilities staff.
US_TN_LOCATION_METADATA_QUERY_TEMPLATE = f"""
SELECT 
    'US_TN' AS state_code,
    SiteID AS location_external_id,
    SiteName AS location_name,
    CASE
        WHEN SiteType IN ('CV', 'AP', 'PC', 'OA') THEN 'ADMINISTRATIVE'
        WHEN SiteType IN ('JA') THEN 'COUNTY_JAIL'
        WHEN SiteType IN ('CK') THEN 'COURT'
        WHEN SiteType IN ('CA', 'DA') THEN 'LAW_ENFORCEMENT'
        WHEN SiteType IN ('BP') THEN 'PAROLE_VIOLATOR_FACILITY'
        WHEN SiteType IN ('BC', 'YC') THEN 'RESIDENTIAL_PROGRAM'
        WHEN SiteType IN ('IJ', 'IN', 'DI', 'PS') THEN 'STATE_PRISON'
        WHEN SiteType IN ('CC', 'PA', 'PR', 'PX', 'EI', 'TC') THEN 'SUPERVISION_LOCATION'
        END AS location_type,
    CASE
        -- SiteType codes for all locations mapped to 'SUPERVISION_LOCATION'
        WHEN SiteType IN ('CC', 'PA', 'PR', 'PX', 'EI', 'TC') AND sup_ref.site_code IS NOT NULL THEN 
            TO_JSON(
                STRUCT(
                    sup_ref.site_code AS {LocationMetadataKey.SUPERVISION_OFFICE_ID.value},
                    sup_ref.site_name AS {LocationMetadataKey.SUPERVISION_OFFICE_NAME.value},
                    sup_ref.district AS {LocationMetadataKey.SUPERVISION_DISTRICT_ID.value},
                    CASE
                        WHEN sup_ref.district = 'NOT_APPLICABLE' THEN sup_ref.district
                        WHEN sup_ref.district = 'INTERNAL_UNKNOWN' THEN sup_ref.district
                        ELSE IF(
                            CONTAINS_SUBSTR(sup_ref.district, ','),
                            CONCAT(
                                'Districts ',
                                SUBSTR(sup_ref.district, 0, STRPOS(sup_ref.district, ',') - 1),
                                ' and ',
                                SUBSTR(sup_ref.district, STRPOS(sup_ref.district, ',') + 1)
                            ),
                            CONCAT('District ', sup_ref.district)
                        )
                        END AS {LocationMetadataKey.SUPERVISION_DISTRICT_NAME.value},
                    UPPER(sup_ref.division) AS {LocationMetadataKey.SUPERVISION_REGION_ID.value},
                    IF(sup_ref.division IN ('NOT_APPLICABLE', 'INTERNAL_UNKNOWN'), sup_ref.division, INITCAP(sup_ref.division)) AS {LocationMetadataKey.SUPERVISION_REGION_NAME.value}
                )
            )
        -- SiteType codes for all locations mapped to 'COUNTY_JAIL', 'RESIDENTIAL_PROGRAM', 'STATE_PRISON'
        WHEN SiteType IN ('JA', 'BC', 'YC', 'IJ', 'IN', 'DI', 'PS') THEN 
            TO_JSON(
                STRUCT(
                    IF(Status = 'I', 'INACTIVE', COALESCE(inc_ref.level_2_incarceration_location_external_id, SiteID)) AS {LocationMetadataKey.FACILITY_GROUP_EXTERNAL_ID.value},
                    CASE 
                        WHEN Status = 'I' THEN 'Inactive'
                        WHEN inc_ref.level_2_incarceration_location_external_id = 'CJ' THEN 'County Jail'
                        WHEN inc_ref.level_2_incarceration_location_external_id = 'WH' THEN 'Workhouse'
                        WHEN inc_ref.level_2_incarceration_location_external_id = 'GENERAL' THEN 'General'
                        ELSE COALESCE(inc_ref.level_2_incarceration_location_external_id, SiteID)
                        END AS {LocationMetadataKey.FACILITY_GROUP_NAME.value},
                    SiteID AS {LocationMetadataKey.LOCATION_ACRONYM.value}
                )
            )
        ELSE NULL
        END AS location_metadata
FROM `{{project_id}}.{{us_tn_raw_data_up_to_date_dataset}}.Site_latest` site
LEFT JOIN `{{project_id}}.{{us_tn_raw_data_up_to_date_dataset}}.RECIDIVIZ_REFERENCE_supervision_locations_latest` sup_ref
    ON site_code = SiteID
LEFT JOIN `{{project_id}}.gcs_backed_tables.us_tn_incarceration_facility_map` inc_ref
    ON level_1_incarceration_location_external_id = SiteID
-- checking for location-to-district mappings that are still active
WHERE SiteID IS NOT NULL AND sup_ref.end_date IS NULL
"""

US_TN_LOCATION_METADATA_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_VIEWS_DATASET,
    view_id=US_TN_LOCATION_METADATA_VIEW_NAME,
    view_query_template=US_TN_LOCATION_METADATA_QUERY_TEMPLATE,
    description=__doc__,
    us_tn_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_TN,
        instance=DirectIngestInstance.PRIMARY,
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_LOCATION_METADATA_VIEW_BUILDER.build_and_print()
