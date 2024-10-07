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

"""A view revealing when locations in the location_metadata view have a non-human-readable
location_name (i.e. parses as an int).
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import REFERENCE_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

LOCATION_METADATA_HUMAN_READABLE_LOCATION_NAME_VIEW_NAME = (
    "location_metadata_human_readable_location_name"
)

LOCATION_METADATA_HUMAN_READABLE_LOCATION_NAME_DESCRIPTION = (
    "Locations in the location_metadata view with location_names that are not "
    "human-readable (i.e. they parse as an int)."
)

LOCATION_METADATA_HUMAN_READABLE_LOCATION_NAME_QUERY_TEMPLATE = """
SELECT state_code, state_code AS region_code, location_external_id, location_name
FROM `{project_id}.{reference_views_dataset}.location_metadata_materialized`
WHERE SAFE_CAST(location_name AS INT64) IS NOT NULL
"""

LOCATION_METADATA_HUMAN_READABLE_LOCATION_NAME_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=LOCATION_METADATA_HUMAN_READABLE_LOCATION_NAME_VIEW_NAME,
    view_query_template=LOCATION_METADATA_HUMAN_READABLE_LOCATION_NAME_QUERY_TEMPLATE,
    description=LOCATION_METADATA_HUMAN_READABLE_LOCATION_NAME_DESCRIPTION,
    reference_views_dataset=REFERENCE_VIEWS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        LOCATION_METADATA_HUMAN_READABLE_LOCATION_NAME_VIEW_BUILDER.build_and_print()
