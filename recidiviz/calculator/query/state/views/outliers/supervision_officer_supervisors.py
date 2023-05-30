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
"""A parole and/or probation officer/agent who serves in a manager or supervisor role and doesn't supervise a typical caseload"""
from recidiviz.big_query.selected_columns_big_query_view import (
    SelectedColumnsBigQueryViewBuilder,
)
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.outliers.staff_query_template import (
    staff_query_template,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_OFFICER_SUPERVISORS_VIEW_NAME = "supervision_officer_supervisors"

SUPERVISION_OFFICER_SUPERVISORS_DESCRIPTION = """A parole and/or probation officer/agent who serves in a manager or supervisor role and doesn't supervise a typical caseload"""


SUPERVISION_OFFICER_SUPERVISORS_QUERY_TEMPLATE = f"""
WITH 
supervision_officer_supervisors AS (
    {staff_query_template(role="SUPERVISION_OFFICER_SUPERVISOR")}
)
-- TODO(#21119): Remove US_PA logic when role_subtype=SUPERVISION_OFFICER_SUPERVISOR is hydrated
, current_us_pa_supervisor_ids AS (
  SELECT
    supervisor.state_code,
    supervisor_external_id.staff_id AS staff_id,
    supervisor.supervisor_staff_external_id AS external_id,
  FROM `{{project_id}}.{{normalized_state_dataset}}.state_staff_supervisor_period` supervisor
  INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_staff_external_id` supervisor_external_id
    ON supervisor.state_code = supervisor_external_id.state_code
    AND supervisor.supervisor_staff_external_id = supervisor_external_id.external_id
    AND supervisor.supervisor_staff_external_id_type = supervisor_external_id.id_type
  INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_staff_external_id` staff_external_id
    ON supervisor.staff_id = staff_external_id.staff_id
  WHERE supervisor.state_code = 'US_PA'
    AND CURRENT_DATE('US/Pacific') BETWEEN supervisor.start_date AND IFNULL(supervisor.end_date, '9999-01-01')
  GROUP BY 1, 2, 3
)
, us_pa_supervision_officer_supervisors AS (
  SELECT
    current_us_pa_supervisor_ids.state_code,
    current_us_pa_supervisor_ids.external_id,
    current_us_pa_supervisor_ids.staff_id,
    TRIM(CONCAT(COALESCE(JSON_EXTRACT_SCALAR(full_name, '$.given_names'), ''), ' ', COALESCE(JSON_EXTRACT_SCALAR(full_name, '$.surname'), ''))) AS full_name,
    staff.email,
    location.location_external_id
  FROM current_us_pa_supervisor_ids
  INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_staff` staff 
    USING (state_code, staff_id)
  INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_staff_location_period` location
    USING (state_code, staff_id)
  WHERE CURRENT_DATE("US/Pacific") BETWEEN location.start_date AND IFNULL(location.end_date, '9999-01-01')
)

SELECT 
    {{columns}}
FROM us_pa_supervision_officer_supervisors

UNION ALL

SELECT 
    {{columns}}
FROM supervision_officer_supervisors
"""

SUPERVISION_OFFICER_SUPERVISORS_VIEW_BUILDER = SelectedColumnsBigQueryViewBuilder(
    dataset_id=dataset_config.OUTLIERS_VIEWS_DATASET,
    view_id=SUPERVISION_OFFICER_SUPERVISORS_VIEW_NAME,
    view_query_template=SUPERVISION_OFFICER_SUPERVISORS_QUERY_TEMPLATE,
    description=SUPERVISION_OFFICER_SUPERVISORS_DESCRIPTION,
    normalized_state_dataset=dataset_config.NORMALIZED_STATE_DATASET,
    should_materialize=True,
    columns=[
        "state_code",
        "external_id",
        "staff_id",
        "full_name",
        "email",
        "location_external_id",
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_OFFICER_SUPERVISORS_VIEW_BUILDER.build_and_print()
