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
"""View that parses and formats state agent names in various ways for use downstream."""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

STATE_STAFF_AND_MOST_RECENT_SUPERVISOR_WITH_NAMES_VIEW_NAME = (
    "state_staff_and_most_recent_supervisor_with_names"
)

STATE_STAFF_AND_MOST_RECENT_SUPERVISOR_WITH_NAMES_VIEW_DESCRIPTION = (
    """View that matches staff to most recent supervisor and includes names."""
)

STATE_STAFF_AND_MOST_RECENT_SUPERVISOR_WITH_NAMES_QUERY_TEMPLATE = """
WITH most_recent_supervisor_by_staff_id AS (
  -- Choose the most recent supervisor for each staff member (by most recent start date)
  SELECT 
    p.state_code, 
    p.staff_id AS officer_staff_id, 
    supervisor_ids.staff_id AS most_recent_supervisor_staff_id
  FROM `{project_id}.normalized_state.state_staff_supervisor_period` p
  LEFT JOIN `{project_id}.normalized_state.state_staff_external_id` supervisor_ids
  ON supervisor_staff_external_id = supervisor_ids.external_id AND supervisor_staff_external_id_type = supervisor_ids.id_type
  QUALIFY ROW_NUMBER() OVER (PARTITION BY p.staff_id ORDER BY start_date DESC) = 1
)
SELECT
    supervisors.state_code,
    
    # Officer information
    supervisors.officer_staff_id,
    officer_names.legacy_supervising_officer_external_id AS officer_staff_external_id,
    officer_names.full_name_clean AS officer_name,
    officer_names.email AS officer_email,
    # Supervisor information
    supervisors.most_recent_supervisor_staff_id,
    supervisor_names.legacy_supervising_officer_external_id AS most_recent_supervisor_staff_external_id,
    supervisor_names.full_name_clean AS most_recent_supervisor_name,
    officer_names.email AS most_recent_supervisor_email,
FROM most_recent_supervisor_by_staff_id supervisors
LEFT JOIN
  `{project_id}.reference_views.state_staff_with_names` officer_names
ON supervisors.officer_staff_id = officer_names.staff_id
LEFT JOIN
  `{project_id}.reference_views.state_staff_with_names` supervisor_names
ON supervisors.most_recent_supervisor_staff_id = officer_names.staff_id
"""

STATE_STAFF_AND_MOST_RECENT_SUPERVISOR_WITH_NAMES_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_VIEWS_DATASET,
    view_id=STATE_STAFF_AND_MOST_RECENT_SUPERVISOR_WITH_NAMES_VIEW_NAME,
    view_query_template=STATE_STAFF_AND_MOST_RECENT_SUPERVISOR_WITH_NAMES_QUERY_TEMPLATE,
    description=STATE_STAFF_AND_MOST_RECENT_SUPERVISOR_WITH_NAMES_VIEW_DESCRIPTION,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        STATE_STAFF_AND_MOST_RECENT_SUPERVISOR_WITH_NAMES_VIEW_BUILDER.build_and_print()
