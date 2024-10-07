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

"""View that identifies location_metadata rows with an invalid key in the
location_metadata JSON.
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import REFERENCE_VIEWS_DATASET
from recidiviz.calculator.query.state.views.reference.location_metadata.location_metadata_key import (
    LocationMetadataKey,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config


def _get_valid_metadata_keys_str() -> str:
    return ",\n  ".join(f"'{k.value}'" for k in LocationMetadataKey)


LOCATION_METADATA_INVALID_METADATA_KEY_VIEW_NAME = (
    "location_metadata_invalid_metadata_key"
)

LOCATION_METADATA_INVALID_METADATA_KEY_DESCRIPTION = (
    "Identifies location_metadata rows with an invalid key in the "
    "location_metadata JSON."
)

LOCATION_METADATA_INVALID_METADATA_KEY_QUERY_TEMPLATE = """
SELECT
  state_code,
  state_code AS region_code,
  metadata_key AS unexpected_metadata_key,
  location_external_id,
  location_metadata
FROM
  `{project_id}.{reference_views_dataset}.location_metadata_materialized`,
  UNNEST(
    -- UDF (user-defined function) documented here 
    -- https://github.com/GoogleCloudPlatform/bigquery-utils/blob/master/udfs/community/README.md#json_extract_keys
    bqutil.fn.json_extract_keys(TO_JSON_STRING(location_metadata))
  ) AS metadata_key
WHERE metadata_key NOT IN (
  {valid_metadata_keys_str}
);
"""

LOCATION_METADATA_INVALID_METADATA_KEY_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=LOCATION_METADATA_INVALID_METADATA_KEY_VIEW_NAME,
    view_query_template=LOCATION_METADATA_INVALID_METADATA_KEY_QUERY_TEMPLATE,
    description=LOCATION_METADATA_INVALID_METADATA_KEY_DESCRIPTION,
    reference_views_dataset=REFERENCE_VIEWS_DATASET,
    valid_metadata_keys_str=_get_valid_metadata_keys_str(),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        LOCATION_METADATA_INVALID_METADATA_KEY_VIEW_BUILDER.build_and_print()
