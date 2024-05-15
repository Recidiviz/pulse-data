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
locations in CA that can be associated with a person or staff member."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.reference.location_metadata.location_metadata_key import (
    LocationMetadataKey,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_tables_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_CA_LOCATION_METADATA_VIEW_NAME = "us_ca_location_metadata"

US_CA_LOCATION_METADATA_DESCRIPTION = """Reference table with metadata about specific
locations in CA that can be associated with a person or staff member.
"""


US_CA_LOCATION_METADATA_QUERY_TEMPLATE = f"""
WITH all_units AS (
  -- `all_units` unions all units from AgentParole and PersonParole. For each unit, we
  -- use whatever the most recently used ParoleDistrict and ParoleRegion are. If for the
  -- same unit within the most recent transfer there entries with different
  -- ParoleDistrict and ParoleRegion information, we prioritize non-null information.
  -- This only occurs once in AgentParole in 2023, and never occurs in PersonParole.
  SELECT * FROM (
    SELECT DISTINCT
      ParoleUnit,
      ParoleDistrict,
      ParoleRegion,
      update_datetime
    FROM `{{project_id}}.{{us_ca_raw_data}}.PersonParole`

    UNION DISTINCT

    SELECT DISTINCT
      ParoleUnit,
      ParoleDistrict,
      ParoleRegion,
      update_datetime
    FROM `{{project_id}}.{{us_ca_raw_data}}.AgentParole`
  )
  QUALIFY ROW_NUMBER() OVER (PARTITION BY ParoleUnit ORDER BY update_datetime DESC, ParoleDistrict NULLS LAST, ParoleRegion NULLS LAST) = 1
),
all_units_nulls_as_string AS (
  -- `all_units_nulls_as_string` replaces nulls with the string "NULL". This avoids validation
  -- errors down the line.
  SELECT
    IFNULL(ParoleUnit, 'NULL') AS ParoleUnit,
    IFNULL(ParoleDistrict, 'NULL') AS ParoleDistrict,
    IFNULL(ParoleRegion, 'NULL') AS ParoleRegion
  FROM all_units
)
-- Finally, we build a JSON object for the location metadata out of the ParoleUnit, ParoleDistrict, and ParoleRegion.
SELECT 
  'US_CA' AS state_code, 
  UPPER(ParoleUnit) as location_external_id,
  UPPER(ParoleUnit) as location_name,
  TO_JSON(
    STRUCT(
      -- ParoleUnit is the lowest level of location in CA, but is analogous to the
      -- Recidiviz term "office". If any of these fields are ints, we replace them with
      -- null to avoid breaking the `location_metadata_human_readable_metadata_name`
      -- validation; see #27374. 
      CASE WHEN SAFE_CAST(ParoleUnit AS INT64) IS NULL THEN UPPER(ParoleUnit) ELSE NULL END AS {LocationMetadataKey.SUPERVISION_OFFICE_NAME.value},
      CASE WHEN SAFE_CAST(ParoleDistrict AS INT64) IS NULL THEN UPPER(ParoleDistrict) ELSE NULL END AS {LocationMetadataKey.SUPERVISION_DISTRICT_NAME.value},
      CASE WHEN SAFE_CAST(ParoleRegion AS INT64) IS NULL THEN UPPER(ParoleRegion) ELSE NULL END AS {LocationMetadataKey.SUPERVISION_REGION_NAME.value},
      UPPER(ParoleUnit) AS {LocationMetadataKey.SUPERVISION_OFFICE_ID.value},
      UPPER(ParoleDistrict) AS {LocationMetadataKey.SUPERVISION_DISTRICT_ID.value},
      UPPER(ParoleRegion) AS {LocationMetadataKey.SUPERVISION_REGION_ID.value}
    )
  ) AS location_metadata,
  'SUPERVISION_LOCATION' AS location_type
FROM all_units_nulls_as_string;
"""

US_CA_LOCATION_METADATA_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_VIEWS_DATASET,
    view_id=US_CA_LOCATION_METADATA_VIEW_NAME,
    view_query_template=US_CA_LOCATION_METADATA_QUERY_TEMPLATE,
    description=US_CA_LOCATION_METADATA_DESCRIPTION,
    us_ca_raw_data=raw_tables_dataset_for_region(
        state_code=StateCode.US_CA, instance=DirectIngestInstance.PRIMARY
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_CA_LOCATION_METADATA_VIEW_BUILDER.build_and_print()
