# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
locations in AZ that can be associated with a person or staff member."""

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

US_AZ_LOCATION_METADATA_VIEW_NAME = "us_az_location_metadata"

US_AZ_LOCATION_METADATA_DESCRIPTION = """Reference table with metadata about specific
locations in AZ that can be associated with a person or staff member.
"""

US_AZ_LOCATION_METADATA_QUERY_TEMPLATE = f"""
SELECT 
  'US_AZ' AS state_code, 
  LOCATION_NAME AS location_external_id,
  LOCATION_NAME AS location_name,
  'SUPERVISION_LOCATION' AS location_type,
  TO_JSON(
    STRUCT(
      OFFICE_LOCATION_ID AS {LocationMetadataKey.SUPERVISION_OFFICE_ID.value},
      UPPER(LOCATION_NAME) AS {LocationMetadataKey.SUPERVISION_OFFICE_NAME.value},
      CASE 
        WHEN UPPER(LOCATION_NAME) IN ('ELECTRONIC MONITORING UNIT', 'SEX OFFENDER COORDINATION UNIT', 'WARRANT SERVICE AND HEARINGS UNIT', 'SPECIAL SUPERVISION UNIT', 'CENTRAL OFFICE', 'INTERSTATE COMPACT UNIT', '801 NO CC SUPERVISION','ADULT ADMINISTRATOR OFFICE') THEN 'CENTRAL OFFICE'
        WHEN UPPER(LOCATION_NAME) = 'TUCSON ELECTRONIC MONITORING UNIT' THEN 'TUCSON REGIONAL PAROLE OFFICE'
        ELSE LOCATION_NAME
      END AS {LocationMetadataKey.SUPERVISION_DISTRICT_ID.value},
      CASE 
        WHEN UPPER(LOCATION_NAME) IN ('ELECTRONIC MONITORING UNIT', 'SEX OFFENDER COORDINATION UNIT', 'WARRANT SERVICE AND HEARINGS UNIT', 'SPECIAL SUPERVISION UNIT', 'CENTRAL OFFICE', 'INTERSTATE COMPACT UNIT', '801 NO CC SUPERVISION','ADULT ADMINISTRATOR OFFICE') THEN 'CENTRAL OFFICE'
        WHEN UPPER(LOCATION_NAME) = 'TUCSON ELECTRONIC MONITORING UNIT' THEN 'TUCSON REGIONAL PAROLE OFFICE'
        ELSE LOCATION_NAME
      END AS {LocationMetadataKey.SUPERVISION_DISTRICT_NAME.value}
    )
  ) AS location_metadata
FROM  `{{project_id}}.{{us_az_raw_data_up_to_date_dataset}}.DPP_OFFICE_LOCATION_latest`

UNION ALL 

SELECT 
  'US_AZ' AS state_code,
  PRISON_NAME AS location_external_id,
  PRISON_NAME AS location_name,
  'STATE_PRISON' AS location_type,
  TO_JSON(
    STRUCT(
      -- Denotes whether this prison is state-run or private. 
      lookups.DESCRIPTION AS {LocationMetadataKey.LOCATION_SUBTYPE.value}
    )
  ) AS location_metadata
FROM `{{project_id}}.{{us_az_raw_data_up_to_date_dataset}}.AZ_DOC_PRISON_latest`
LEFT JOIN`{{project_id}}.{{us_az_raw_data_up_to_date_dataset}}.LOOKUPS_latest` lookups
ON(PRISON_TYPE_ID = LOOKUP_ID)
"""

US_AZ_LOCATION_METADATA_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_VIEWS_DATASET,
    view_id=US_AZ_LOCATION_METADATA_VIEW_NAME,
    view_query_template=US_AZ_LOCATION_METADATA_QUERY_TEMPLATE,
    description=US_AZ_LOCATION_METADATA_DESCRIPTION,
    us_az_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_AZ, instance=DirectIngestInstance.PRIMARY
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_AZ_LOCATION_METADATA_VIEW_BUILDER.build_and_print()
