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
"""Reference table with metadata about specific
locations in UT that can be associated with a person or staff member."""

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

US_UT_LOCATION_METADATA_VIEW_NAME = "us_ut_location_metadata"

US_UT_LOCATION_METADATA_DESCRIPTION = """Reference table with metadata about specific
locations in UT that can be associated with a person or staff member.
"""

US_UT_LOCATION_METADATA_QUERY_TEMPLATE = f"""
WITH base as (
SELECT 
  'US_UT' AS state_code, 
  agcy_id AS location_external_id,
  agcy_desc AS location_name,
  CASE
    WHEN agcy_desc LIKE "%A.P.& P.%" OR agcy_desc = 'BOARD OF PARDONS' OR agcy_desc LIKE "% OFFICE" OR agcy_desc LIKE "%T.R.C%" THEN "SUPERVISION_LOCATION" 
    WHEN agcy_desc LIKE "%DPO%" OR agcy_desc LIKE "%USP%" OR agcy_desc LIKE "%UCI%" OR agcy_desc LIKE "%CIRT%" OR agcy_desc IN('CIRT CUCF', 'REENTRY-CUCF') THEN "STATE_PRISON"
    WHEN agcy_desc LIKE "%C.T.C.%" OR agcy_desc LIKE "%CTC%" OR agcy_desc LIKE "%C.C.C.%" OR agcy_desc LIKE "%ROAD HOME%" OR agcy_desc LIKE "%TRC%" OR agcy_desc IN ("FORTITUDE TREATMENT CNTR","SLC TRANSITION FACILITY","GAIL MILLER RESOURCE CENTER-CS","RESCUE MISSION OF SLC","IRON COUNTY CARE & SHARE","LANTERN HOUSE", "SWITCHPOINT") THEN "RESIDENTIAL_PROGRAM"
    WHEN agcy_desc IN ("UDC ADMINISTRATION", "UDC BIT", "UDC INVESTIGATIONS", "UDC PLANNING & RESEARCH", "UDC PROGRAMMING") THEN "ADMINISTRATIVE"
    WHEN agcy_desc LIKE "%MEDICAL%" THEN "MEDICAL_FACILITY"
    WHEN agcy_desc LIKE "%PD%" OR agcy_desc IN ("MILLER POST ACADEMY", "FRED HOUSE TRAINING ACADE")THEN "LAW_ENFORCEMENT"
    ELSE "UNKNOWN"
  END AS location_type
FROM  `{{project_id}}.{{us_ut_raw_data_up_to_date_dataset}}.agcy_loc_latest`
WHERE agcy_desc NOT IN ("DISCHARGED", "TEST123")
)
, base_plus_metadata as (
  SELECT * ,
  CASE WHEN location_type = 'SUPERVISION_LOCATION' THEN
  TO_JSON(
    STRUCT(
      location_external_id AS {LocationMetadataKey.SUPERVISION_OFFICE_ID.value},
      UPPER(location_name) AS {LocationMetadataKey.SUPERVISION_OFFICE_NAME.value}
    )
  ) END AS location_metadata
  FROM base
)  
SELECT *
from base_plus_metadata
"""

US_UT_LOCATION_METADATA_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_VIEWS_DATASET,
    view_id=US_UT_LOCATION_METADATA_VIEW_NAME,
    view_query_template=US_UT_LOCATION_METADATA_QUERY_TEMPLATE,
    description=US_UT_LOCATION_METADATA_DESCRIPTION,
    us_ut_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_UT, instance=DirectIngestInstance.PRIMARY
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_UT_LOCATION_METADATA_VIEW_BUILDER.build_and_print()
