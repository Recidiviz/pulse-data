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

"""A view revealing when locations in the state_staff_location_period view
that do not have corresponding rows in the location_metadata view.
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import REFERENCE_VIEWS_DATASET
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

LOCATION_METADATA_MISSING_PERSON_STAFF_RELATIONSHIP_PERIOD_LOCATIONS_VIEW_NAME = (
    "location_metadata_missing_person_staff_relationship_locations"
)

LOCATION_METADATA_MISSING_PERSON_STAFF_RELATIONSHIP_PERIOD_LOCATIONS_DESCRIPTION = (
    "Locations in the state_person_staff_relationship table that do not "
    "have corresponding rows in the location_metadata view."
)

LOCATION_METADATA_MISSING_PERSON_STAFF_RELATIONSHIP_PERIOD_LOCATIONS_QUERY_TEMPLATE = """
SELECT
  state_code,
  state_code AS region_code,
  p.location_external_id,
  COUNT(*) AS num_periods_with_this_location,
  ANY_VALUE(person_staff_relationship_period_id) AS example_period
FROM 
  `{project_id}.{state_dataset}.state_person_staff_relationship_period` p 
LEFT JOIN
  `{project_id}.{reference_views_dataset}.location_metadata_materialized` m
USING (state_code, location_external_id)
WHERE
    p.location_external_id IS NOT NULL
    AND m.location_external_id IS NULL
GROUP BY 1, 2, 3
"""

LOCATION_METADATA_MISSING_PERSON_STAFF_RELATIONSHIP_PERIOD_LOCATIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=LOCATION_METADATA_MISSING_PERSON_STAFF_RELATIONSHIP_PERIOD_LOCATIONS_VIEW_NAME,
    view_query_template=LOCATION_METADATA_MISSING_PERSON_STAFF_RELATIONSHIP_PERIOD_LOCATIONS_QUERY_TEMPLATE,
    description=LOCATION_METADATA_MISSING_PERSON_STAFF_RELATIONSHIP_PERIOD_LOCATIONS_DESCRIPTION,
    state_dataset=NORMALIZED_STATE_DATASET,
    reference_views_dataset=REFERENCE_VIEWS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        LOCATION_METADATA_MISSING_PERSON_STAFF_RELATIONSHIP_PERIOD_LOCATIONS_VIEW_BUILDER.build_and_print()
