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
"""A view revealing when the location ids within the incarceration/supervision location
to names mapping reference views contains duplicate id values.
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import REFERENCE_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

LOCATION_IDS_TO_NAMES_UNIQUE_IDS_VIEW_NAME = "location_ids_to_names_unique_ids"

LOCATION_IDS_TO_NAMES_UNIQUE_IDS_DESCRIPTION = """Reveals when the location ids within
the incarceration/supervision location to names mapping reference views contains
duplicate id values."""

LOCATION_IDS_TO_NAMES_UNIQUE_IDS_QUERY_TEMPLATE = """
SELECT
    state_code,
    state_code AS region_code,
    level_1_incarceration_location_external_id AS location_id,
    "incarceration" AS location_type,
    COUNT(*) AS total_rows
FROM `{project_id}.{reference_views_dataset}.incarceration_location_ids_to_names`
WHERE level_1_incarceration_location_external_id IS NOT NULL
GROUP BY region_code, location_id
HAVING total_rows > 1

UNION ALL

SELECT
    state_code,
    state_code AS region_code,
    CONCAT(
        level_1_supervision_location_external_id,
        "|",
        COALESCE(level_2_supervision_location_external_id, 'NOT_APPLICABLE')
    ) AS location_id,
    "supervision" AS location_type,
    COUNT(*) AS total_rows
FROM `{project_id}.{reference_views_dataset}.supervision_location_ids_to_names_materialized`
WHERE level_1_supervision_location_external_id IS NOT NULL
GROUP BY region_code, location_id
HAVING total_rows > 1
"""

LOCATION_IDS_TO_NAMES_UNIQUE_IDS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=LOCATION_IDS_TO_NAMES_UNIQUE_IDS_VIEW_NAME,
    view_query_template=LOCATION_IDS_TO_NAMES_UNIQUE_IDS_QUERY_TEMPLATE,
    reference_views_dataset=REFERENCE_VIEWS_DATASET,
    description=LOCATION_IDS_TO_NAMES_UNIQUE_IDS_DESCRIPTION,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        LOCATION_IDS_TO_NAMES_UNIQUE_IDS_VIEW_BUILDER.build_and_print()
