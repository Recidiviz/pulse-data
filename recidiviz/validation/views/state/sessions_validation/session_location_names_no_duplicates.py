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
"""Flag any rows within `session_location_names` that have the same primary keys
(state_code, facility, supervision_office, supervision_district)"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

SESSION_LOCATION_NAMES_NO_DUPLICATES_VIEW_NAME = "session_location_names_no_duplicates"

SESSION_LOCATION_NAMES_NO_DUPLICATES_QUERY_TEMPLATE = """
    SELECT
      state_code AS region_code,
      state_code,
      facility,
      supervision_office,
      supervision_district,
      COUNT(*) AS total_rows,
    FROM `{project_id}.sessions.session_location_names_materialized`
    GROUP BY 1, 2, 3, 4, 5
    HAVING total_rows > 1
    ORDER BY total_rows DESC
"""


SESSION_LOCATION_NAMES_NO_DUPLICATES_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=SESSION_LOCATION_NAMES_NO_DUPLICATES_VIEW_NAME,
    view_query_template=SESSION_LOCATION_NAMES_NO_DUPLICATES_QUERY_TEMPLATE,
    description=__doc__,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SESSION_LOCATION_NAMES_NO_DUPLICATES_VIEW_BUILDER.build_and_print()
