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
locations in IX that can be associated with a person or staff member."""

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

US_IX_LOCATION_METADATA_VIEW_NAME = "us_ix_location_metadata"

US_IX_LOCATION_METADATA_DESCRIPTION = """Reference table with metadata about specific
locations in IX that can be associated with a person or staff member.
"""

US_IX_LOCATION_METADATA_QUERY_TEMPLATE = f"""
-- Pull information from the Atlas location reference table
SELECT 
    'US_IX' AS state_code,
    CONCAT('ATLAS-', LocationId) AS location_external_id,
    UPPER(LocationName) AS location_name,
    CASE
        WHEN LocationTypeId in ('20', '15') THEN 'ADMINISTRATIVE'
        WHEN LocationTypeId in ('9', '5') THEN 'MEDICAL_FACILITY'
        WHEN LocationTypeId in ('14', '8') THEN 'OUT_OF_STATE'
        WHEN LocationTypeId in ('6') THEN 'SUPERVISION_LOCATION'
        WHEN LocationTypeId in ('16') THEN 'FEDERAL_PRISON'
        WHEN LocationTypeId in ('13') THEN 'CITY_COUNTY'
        WHEN LocationTypeId in ('3') THEN 'COURT'
        WHEN LocationTypeId in ('26') THEN 'LAW_ENFORCEMENT'
        WHEN LocationTypeId in ('7') THEN 'COUNTY_JAIL'
        WHEN LocationTypeId in ('4', '17') THEN 'STATE_PRISON'
        WHEN LocationTypeId in ('11') THEN 'RESIDENTIAL_PROGRAM'
        ELSE NULL
      END AS location_type,
    CASE WHEN LocationTypeId = '6' THEN
      TO_JSON(
        STRUCT(
          LocationId AS {LocationMetadataKey.SUPERVISION_OFFICE_ID.value},
          UPPER(LocationName) AS {LocationMetadataKey.SUPERVISION_OFFICE_NAME.value},
          CASE 
            WHEN UPPER(LocationName) like 'DISTRICT %' AND UPPER(LocationName) like '%-%' THEN REPLACE(SPLIT(UPPER(LocationName), ' - ')[OFFSET(0)], 'OFFICE ', '')
            WHEN UPPER(LocationName) like 'DISTRICT %' AND UPPER(LocationName) like '%,%' THEN REPLACE(SPLIT(UPPER(LocationName), ',')[OFFSET(0)], 'OFFICE ', '') 
            WHEN UPPER(LocationName) = 'LOW SUPERVISION UNIT' THEN 'DISTRICT 4'
            ELSE NULL
          END AS {LocationMetadataKey.SUPERVISION_DISTRICT_ID.value},
          CASE 
            WHEN UPPER(LocationName) like 'DISTRICT %' AND UPPER(LocationName) like '%-%' THEN REPLACE(SPLIT(UPPER(LocationName), ' - ')[OFFSET(0)], 'OFFICE ', '')
            WHEN UPPER(LocationName) like 'DISTRICT %' AND UPPER(LocationName) like '%,%' THEN REPLACE(SPLIT(UPPER(LocationName), ',')[OFFSET(0)], 'OFFICE ', '') 
            WHEN UPPER(LocationName) = 'LOW SUPERVISION UNIT' THEN 'DISTRICT 4'
            ELSE NULL
          END AS {LocationMetadataKey.SUPERVISION_DISTRICT_NAME.value}
        )
      )
    WHEN LocationTypeId = '13' THEN
      TO_JSON(
        STRUCT(
          UPPER(map.district) AS {LocationMetadataKey.SUPERVISION_DISTRICT_ID.value},
          UPPER(map.district) AS {LocationMetadataKey.SUPERVISION_DISTRICT_NAME.value}
        )
      )
    ELSE NULL
    END AS location_metadata,

FROM `{{project_id}}.{{us_ix_raw_data_up_to_date_dataset}}.ref_Location_latest` ll
LEFT JOIN `{{project_id}}.{{us_ix_raw_data_up_to_date_dataset}}.RECIDIVIZ_REFERENCE_county_to_district_mapping_latest` map
ON map.countyName = ll.LocationName

UNION ALL

-- Pull information from the CIS location reference table
SELECT 
    'US_IX' AS state_code,
    CONCAT('CIS-', fac_cd) AS location_external_id,
    UPPER(fac_ldesc) AS location_name,
    CASE
      WHEN fac_typ = 'I' THEN 'STATE_PRISON'
      WHEN fac_typ = 'P' THEN 'SUPERVISION_LOCATION'
      WHEN fac_cd = 'HQ' THEN 'ADMINISTRATIVE'
      ELSE NULL
    END AS location_type,
    CASE
      WHEN fac_typ = 'P' THEN 
        TO_JSON(
          STRUCT(
            fac_cd AS {LocationMetadataKey.SUPERVISION_OFFICE_ID.value},
            UPPER(fac_ldesc) AS {LocationMetadataKey.SUPERVISION_OFFICE_NAME.value},
            CASE 
              WHEN UPPER(fac_ldesc) like 'DISTRICT%' and UPPER(fac_ldesc) like '%-%' THEN REPLACE(SPLIT(UPPER(fac_ldesc), '-')[OFFSET(0)], 'OFFICE ', '')
              WHEN UPPER(fac_ldesc) like 'DISTRICT%' and UPPER(fac_ldesc) like '%,%' THEN REPLACE(SPLIT(UPPER(fac_ldesc), ',')[OFFSET(0)], 'OFFICE ', '')
              ELSE NULL
            END AS {LocationMetadataKey.SUPERVISION_DISTRICT_ID.value},
            CASE 
              WHEN UPPER(fac_ldesc) like 'DISTRICT%' and UPPER(fac_ldesc) like '%-%' THEN REPLACE(SPLIT(UPPER(fac_ldesc), '-')[OFFSET(0)], 'OFFICE ', '')
              WHEN UPPER(fac_ldesc) like 'DISTRICT%' and UPPER(fac_ldesc) like '%,%' THEN REPLACE(SPLIT(UPPER(fac_ldesc), ',')[OFFSET(0)], 'OFFICE ', '')
              ELSE NULL
            END AS {LocationMetadataKey.SUPERVISION_DISTRICT_NAME.value}
          )
        ) 
      ELSE NULL
    END AS location_metadata,
FROM `{{project_id}}.{{us_ix_raw_data_up_to_date_dataset}}.facility_latest`
"""

US_IX_LOCATION_METADATA_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_VIEWS_DATASET,
    view_id=US_IX_LOCATION_METADATA_VIEW_NAME,
    view_query_template=US_IX_LOCATION_METADATA_QUERY_TEMPLATE,
    description=US_IX_LOCATION_METADATA_DESCRIPTION,
    us_ix_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_IX, instance=DirectIngestInstance.PRIMARY
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_IX_LOCATION_METADATA_VIEW_BUILDER.build_and_print()
