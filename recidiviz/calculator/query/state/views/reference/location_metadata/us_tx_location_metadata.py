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
"""
Reference table for TX locations.
This is currently only:
  - location external ID (region - district)
  - office name

# TODO(#41400): Include office types when they are known
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.reference.location_metadata.location_metadata_key import (
    LocationMetadataKey,
)
from recidiviz.calculator.query.state.views.reference.location_metadata.state_location_type import (
    StateLocationType,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_TX_LOCATION_METADATA_VIEW_NAME = "us_tx_location_metadata"

# TODO(#41400): Define OFFC_TYPEs when they are known.
US_TX_LOCATION_METADATA_QUERY_TEMPLATE = f"""
SELECT
    'US_TX' AS state_code,
    CONCAT(LPAD(OFFC_REGION, 2, '0'), '-', LPAD(OFFC_DISTRICT, 2, '0')) AS location_external_id,
    OFFC_NAME AS location_name,
   '{StateLocationType.INTERNAL_UNKNOWN.value}' AS location_type,
    TO_JSON(STRUCT(
        OFFC_CNTY_CODE AS {LocationMetadataKey.COUNTY_ID.value}
    )) AS location_metadata
FROM 
    `{{project_id}}.{{us_tx_raw_data_up_to_date_dataset}}.OfficeDescription_latest`
"""

US_TX_LOCATION_METADATA_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_VIEWS_DATASET,
    view_id=US_TX_LOCATION_METADATA_VIEW_NAME,
    view_query_template=US_TX_LOCATION_METADATA_QUERY_TEMPLATE,
    description=__doc__,
    us_tx_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_TX, instance=DirectIngestInstance.PRIMARY
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TX_LOCATION_METADATA_VIEW_BUILDER.build_and_print()
