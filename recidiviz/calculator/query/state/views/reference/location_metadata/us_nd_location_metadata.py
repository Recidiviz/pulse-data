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
locations in PA that can be associated with a person or staff member."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.reference.location_metadata.location_metadata_key import (
    LocationMetadataKey,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.datasets.static_data.config import EXTERNAL_REFERENCE_DATASET
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_ND_LOCATION_METADATA_VIEW_NAME = "us_nd_location_metadata"

US_ND_LOCATION_METADATA_DESCRIPTION = """Reference table with metadata about specific
locations in ND that can be associated with a person or staff member.
"""

US_ND_LOCATION_METADATA_QUERY_TEMPLATE = f"""
SELECT 
    'US_ND' AS state_code,
    level_1_supervision_location_external_id AS location_external_id,
    level_1_supervision_location_name AS location_name,
    'SUPERVISION_LOCATION' AS location_type,
    TO_JSON(
      STRUCT(
        level_1_supervision_location_external_id AS {LocationMetadataKey.SUPERVISION_OFFICE_ID.value},
        level_1_supervision_location_name AS {LocationMetadataKey.SUPERVISION_OFFICE_NAME.value},
        level_2_supervision_location_external_id AS {LocationMetadataKey.SUPERVISION_DISTRICT_ID.value},
        level_2_supervision_location_name AS {LocationMetadataKey.SUPERVISION_DISTRICT_NAME.value}
      )
    ) AS location_metadata,
FROM `{{project_id}}.{{us_nd_raw_data_up_to_date_dataset}}.RECIDIVIZ_REFERENCE_supervision_location_ids_latest`

UNION ALL

SELECT 
    'US_ND' AS state_code,
    facility_code AS location_external_id,
    facility_name AS location_name,
    -- TODO(#20370): Refine these mappings - there are some regions that are being
    -- mapped to STATE_PRISON which probably aren't prisons (e.g. 'Lake Region 
    -- Residential Reentry Center').
    CASE
        WHEN facility_code = 'OOS' THEN 'OUT_OF_STATE'
        WHEN facility_code = 'CJ' THEN 'COUNTY_JAIL'
        WHEN facility_code = 'CPP' THEN 'RESIDENTIAL_PROGRAM'
        ELSE 'STATE_PRISON'
    END AS location_type,
    TO_JSON(
      STRUCT(
        facility_code AS {LocationMetadataKey.LOCATION_ACRONYM.value}
      )
    ) AS location_metadata,
FROM `{{project_id}}.{{external_reference_dataset}}.us_nd_incarceration_facility_names`

UNION ALL

SELECT
  'US_ND' AS state_code,
  CONCAT('COUNTY-', countyDistrictId) AS location_external_id,
  countyName AS location_name,
  'CITY_COUNTY' AS location_type,
  TO_JSON( STRUCT( district AS supervision_district_id,
      district AS supervision_district_name ) ) AS location_metadata,
FROM `{{project_id}}.{{us_nd_raw_data_up_to_date_dataset}}.RECIDIVIZ_REFERENCE_county_to_district_mapping_latest`
"""

US_ND_LOCATION_METADATA_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_VIEWS_DATASET,
    view_id=US_ND_LOCATION_METADATA_VIEW_NAME,
    view_query_template=US_ND_LOCATION_METADATA_QUERY_TEMPLATE,
    description=US_ND_LOCATION_METADATA_DESCRIPTION,
    us_nd_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_ND, instance=DirectIngestInstance.PRIMARY
    ),
    external_reference_dataset=EXTERNAL_REFERENCE_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ND_LOCATION_METADATA_VIEW_BUILDER.build_and_print()
