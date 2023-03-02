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
"""View listing all current supervision staff with their current location."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    US_PA_RAW_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

CURRENT_STAFF_SUPERVISION_LOCATIONS_VIEW_NAME = "current_staff_supervision_locations"

CURRENT_STAFF_SUPERVISION_LOCATIONS_VIEW_DESCRIPTION = """View listing all current supervision staff with their
current location."""

CURRENT_STAFF_SUPERVISION_LOCATIONS_QUERY_TEMPLATE = """
--TODO(#18093): Deprecate in favor of using state_staff_location_period to get current staff location
SELECT 
      "US_PA" AS state_code, 
      roster.Employ_Num AS officer_id,
      CONCAT(ORG_ID,'-',ORG_LONG_NAME) AS location_raw_text,
      UPPER(ref.level_1_supervision_location_external_id) AS level_1_supervision_location_external_id,
      UPPER(ref.level_1_supervision_location_name) AS level_1_supervision_location_name,
      UPPER(ref.level_2_supervision_location_external_id) AS level_2_supervision_location_external_id,
      UPPER(ref.level_2_supervision_location_name) AS level_2_supervision_location_name,
      UPPER(ref.level_3_supervision_location_external_id) AS level_3_supervision_location_external_id,
      UPPER(ref.level_3_supervision_location_name) AS level_3_supervision_location_name,
FROM 
    `{project_id}.{raw_dataset}.RECIDIVIZ_REFERENCE_agent_districts_latest` roster
LEFT JOIN 
    `{project_id}.{raw_dataset}.RECIDIVIZ_REFERENCE_supervision_location_ids_latest` ref
  ON 
    roster.ORG_ID = ref.ORG_cd

"""

CURRENT_STAFF_SUPERVISION_LOCATIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=CURRENT_STAFF_SUPERVISION_LOCATIONS_VIEW_NAME,
    view_query_template=CURRENT_STAFF_SUPERVISION_LOCATIONS_QUERY_TEMPLATE,
    description=CURRENT_STAFF_SUPERVISION_LOCATIONS_VIEW_DESCRIPTION,
    raw_dataset=US_PA_RAW_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        CURRENT_STAFF_SUPERVISION_LOCATIONS_VIEW_BUILDER.build_and_print()
