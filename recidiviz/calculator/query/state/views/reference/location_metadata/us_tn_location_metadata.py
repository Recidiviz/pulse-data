# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Reference table with metadata about specific
locations in TN that can be associated with a person or staff member."""

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

US_TN_LOCATION_METADATA_DESCRIPTION = """Reference table with metadata about specific
locations in TN that can be associated with a person or staff member.
"""

US_TN_LOCATION_METADATA_QUERY_TEMPLATE = f"""
SELECT 
    'US_TN' AS state_code,
    SiteID AS location_external_id,
    SiteName AS location_name,
    CASE 
        WHEN SiteType IN ('CV', 'TC', 'AP', 'PC') THEN 'ADMINISTRATIVE' 
        WHEN SiteType IN ('CA') THEN 'CITY_COUNTY' 
        WHEN SiteType IN ('JA') THEN 'COUNTY_JAIL' 
        WHEN SiteType IN ('CK') THEN 'COURT' 
        WHEN SiteType IN ('DA') THEN 'LAW_ENFORCEMENT'
        WHEN SiteType IN ('OA') THEN 'OUT_OF_STATE'
        WHEN SiteType IN ('BP') THEN 'PAROLE_VIOLATOR_FACILITY'   
        WHEN SiteType IN ('BC', 'YC') THEN 'RESIDENTIAL_PROGRAM' 
        WHEN SiteType IN ('IJ', 'IN', 'DI', 'PS') THEN 'STATE_PRISON' 
        WHEN SiteType IN ('CC', 'PA', 'PR', 'PX', 'EI') THEN 'SUPERVISION_LOCATION' 
     END AS location_type,
    # TODO(#23114) - Add additional metadata information when required for supervision or facilities staff
    CASE WHEN SiteType IN ('CC', 'PA', 'PR', 'PX', 'EI') THEN
        TO_JSON(
          STRUCT(
            ref.site_code AS {LocationMetadataKey.SUPERVISION_OFFICE_ID.value},
            ref.site_name AS {LocationMetadataKey.SUPERVISION_OFFICE_NAME.value},
            CASE
                WHEN ref.district = 'NOT_APPLICABLE' THEN ref.district
                WHEN ref.district = 'INTERNAL_UNKNOWN' THEN ref.district
                ELSE IF(CONTAINS_SUBSTR(ref.district, ','),
                    CONCAT('Districts ',
                        SUBSTR(ref.district, 0, STRPOS(ref.district, ',') - 1),
                        ' and ',
                        SUBSTR(ref.district, STRPOS(ref.district, ',') + 1)),
                    CONCAT('District ', ref.district)) END AS {LocationMetadataKey.SUPERVISION_DISTRICT_ID.value},
              CASE
                WHEN ref.district = 'NOT_APPLICABLE' THEN ref.district
                WHEN ref.district = 'INTERNAL_UNKNOWN' THEN ref.district
                ELSE IF(CONTAINS_SUBSTR(ref.district, ','),
                    CONCAT('Districts ',
                        SUBSTR(ref.district, 0, STRPOS(ref.district, ',') - 1),
                        ' and ',
                        SUBSTR(ref.district, STRPOS(ref.district, ',') + 1)),
                    CONCAT('District ', ref.district)) END AS {LocationMetadataKey.SUPERVISION_DISTRICT_NAME.value},
            INITCAP(ref.division) AS {LocationMetadataKey.SUPERVISION_REGION_ID.value},
            INITCAP(ref.division) AS {LocationMetadataKey.SUPERVISION_REGION_NAME.value}
          )
        ) ELSE NULL END AS location_metadata
FROM `{{project_id}}.{{us_tn_raw_data_up_to_date_dataset}}.Site_latest` site
LEFT JOIN `{{project_id}}.{{us_tn_raw_data_up_to_date_dataset}}.RECIDIVIZ_REFERENCE_supervision_locations_latest` ref ON site_code = SiteID
WHERE SiteID IS NOT NULL
"""

US_TN_LOCATION_METADATA_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_VIEWS_DATASET,
    view_id=US_TN_LOCATION_METADATA_VIEW_NAME,
    view_query_template=US_TN_LOCATION_METADATA_QUERY_TEMPLATE,
    description=US_TN_LOCATION_METADATA_DESCRIPTION,
    us_tn_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_TN, instance=DirectIngestInstance.PRIMARY
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_LOCATION_METADATA_VIEW_BUILDER.build_and_print()
