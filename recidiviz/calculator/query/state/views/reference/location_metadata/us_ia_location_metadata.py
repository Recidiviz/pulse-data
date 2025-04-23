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
locations in IA that can be associated with a person or staff member."""

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

US_IA_LOCATION_METADATA_VIEW_NAME = "us_ia_location_metadata"

US_IA_LOCATION_METADATA_DESCRIPTION = """Reference table with metadata about specific
locations in IA that can be associated with a person or staff member.
"""

US_IA_LOCATION_METADATA_QUERY_TEMPLATE = f"""
-- Pull information from the ICON region reference table and the staff work unit table
SELECT
    'US_IA' AS state_code,
    WorkUnitId AS location_external_id,
    WorkUnitNm AS location_name,
    CASE RegionType
        WHEN 'C' THEN 'SUPERVISION_LOCATION'
        WHEN 'CO' THEN 'ADMINISTRATIVE'
        WHEN 'I' THEN 'STATE_PRISON'
        ELSE NULL
      END AS location_type,
    CASE 
      WHEN RegionType = 'C' 
      THEN
        TO_JSON(
          STRUCT(
            WorkUnitId AS {LocationMetadataKey.SUPERVISION_OFFICE_ID.value},
            UPPER(WorkUnitNm) AS {LocationMetadataKey.SUPERVISION_OFFICE_NAME.value},
            RegionId AS {LocationMetadataKey.SUPERVISION_DISTRICT_ID.value},
            UPPER(RegionNm) AS {LocationMetadataKey.SUPERVISION_DISTRICT_NAME.value}
          )
        )
      END AS location_metadata
FROM (
  SELECT DISTINCT WorkUnitId, WorkUnitNm, RegionId, RegionNm, RegionType
  FROM `{{project_id}}.{{us_ia_raw_data_up_to_date_dataset}}.IA_DOC_MAINT_StaffWorkUnits_latest` 
  LEFT JOIN `{{project_id}}.{{us_ia_raw_data_up_to_date_dataset}}.IA_DOC_MAINT_Regions_latest` 
    USING(RegionId)
  WHERE WorkUnitId IS NOT NULL
)
"""

US_IA_LOCATION_METADATA_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_VIEWS_DATASET,
    view_id=US_IA_LOCATION_METADATA_VIEW_NAME,
    view_query_template=US_IA_LOCATION_METADATA_QUERY_TEMPLATE,
    description=US_IA_LOCATION_METADATA_DESCRIPTION,
    us_ia_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_IA, instance=DirectIngestInstance.PRIMARY
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_IA_LOCATION_METADATA_VIEW_BUILDER.build_and_print()
