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
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_PA_LOCATION_METADATA_VIEW_NAME = "us_pa_location_metadata"

US_PA_LOCATION_METADATA_DESCRIPTION = """Reference table with metadata about specific
locations in PA that can be associated with a person or staff member.
"""

# TODO(#19341): Update this to pull in other types of locations (e.g. facilities)
US_PA_LOCATION_METADATA_QUERY_TEMPLATE = f"""
SELECT 
    'US_PA' AS state_code,
    Org_cd AS location_external_id,
    Org_Name AS location_name,
    'SUPERVISION_LOCATION' AS location_type,
    TO_JSON(
      STRUCT(
        IF(Org_Name LIKE '%UNIT%' OR Org_Name LIKE "% UNT%", Org_cd, NULL) 
            AS {LocationMetadataKey.SUPERVISION_UNIT_ID.value},
        IF(Org_Name LIKE '%UNIT%' OR Org_Name LIKE "% UNT%", Org_Name, NULL) 
            AS {LocationMetadataKey.SUPERVISION_UNIT_NAME.value},
        level_1_supervision_location_external_id AS {LocationMetadataKey.SUPERVISION_OFFICE_ID.value},
        level_1_supervision_location_name AS {LocationMetadataKey.SUPERVISION_OFFICE_NAME.value},
        level_2_supervision_location_external_id AS {LocationMetadataKey.SUPERVISION_DISTRICT_ID.value},
        level_2_supervision_location_name AS {LocationMetadataKey.SUPERVISION_DISTRICT_NAME.value},
        level_3_supervision_location_external_id AS {LocationMetadataKey.SUPERVISION_REGION_ID.value},
        level_3_supervision_location_name AS {LocationMetadataKey.SUPERVISION_REGION_NAME.value}
      )
    ) AS location_metadata,
FROM `{{project_id}}.{{us_pa_raw_data_up_to_date_dataset}}.RECIDIVIZ_REFERENCE_supervision_location_ids_latest`
-- There are a few of organization codes associated with the central office across
-- regions other than the CENTRAL region (CR). We filter these out to make downstream 
-- deduplication easier.
WHERE level_2_supervision_location_external_id != 'CO' 
    OR level_3_supervision_location_external_id = 'CR'
"""

US_PA_LOCATION_METADATA_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_VIEWS_DATASET,
    view_id=US_PA_LOCATION_METADATA_VIEW_NAME,
    view_query_template=US_PA_LOCATION_METADATA_QUERY_TEMPLATE,
    description=US_PA_LOCATION_METADATA_DESCRIPTION,
    us_pa_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_PA, instance=DirectIngestInstance.PRIMARY
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_PA_LOCATION_METADATA_VIEW_BUILDER.build_and_print()
