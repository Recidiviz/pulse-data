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

"""View that identifies location_metadata rows with an invalid location_type."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import REFERENCE_VIEWS_DATASET
from recidiviz.calculator.query.state.views.reference.location_metadata.state_location_type import (
    StateLocationType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config


def _get_valid_location_types_str() -> str:
    return ",\n  ".join(f"'{t.value}'" for t in StateLocationType)


LOCATION_METADATA_INVALID_LOCATION_TYPE_VIEW_NAME = (
    "location_metadata_invalid_location_type"
)

LOCATION_METADATA_INVALID_LOCATION_TYPE_DESCRIPTION = (
    "Identifies location_metadata rows with an invalid location_type."
)

LOCATION_METADATA_INVALID_LOCATION_TYPE_QUERY_TEMPLATE = """
SELECT state_code AS region_code, *
FROM `{project_id}.{reference_views_dataset}.location_metadata_materialized`
WHERE location_type NOT IN (
  {valid_location_types_str}
);
"""

LOCATION_METADATA_INVALID_LOCATION_TYPE_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=LOCATION_METADATA_INVALID_LOCATION_TYPE_VIEW_NAME,
    view_query_template=LOCATION_METADATA_INVALID_LOCATION_TYPE_QUERY_TEMPLATE,
    description=LOCATION_METADATA_INVALID_LOCATION_TYPE_DESCRIPTION,
    reference_views_dataset=REFERENCE_VIEWS_DATASET,
    valid_location_types_str=_get_valid_location_types_str(),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        LOCATION_METADATA_INVALID_LOCATION_TYPE_VIEW_BUILDER.build_and_print()
