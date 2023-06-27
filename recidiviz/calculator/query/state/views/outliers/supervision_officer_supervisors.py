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
from recidiviz.calculator.query.bq_utils import (
    today_between_start_date_and_nullable_end_date_exclusive_clause,
)
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.outliers.staff_query_template import (
    staff_query_template,
)
from recidiviz.calculator.query.state.views.outliers.utils import (
    get_state_specific_config_exclusions,
)
from recidiviz.common.constants.states import StateCode
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
  WHERE supervisor.state_code = 'US_PA'
    AND {today_between_start_date_and_nullable_end_date_exclusive_clause("supervisor.start_date", "supervisor.end_date")}
  GROUP BY 1, 2, 3
)
, us_pa_supervision_officer_supervisors AS (
  SELECT
    current_us_pa_supervisor_ids.state_code,
    current_us_pa_supervisor_ids.external_id,
    current_us_pa_supervisor_ids.staff_id,
    full_name,
    staff.email,
    attrs.supervisor_staff_external_id AS supervisor_external_id,
    attrs.supervision_district,
    attrs.supervision_unit
  FROM current_us_pa_supervisor_ids
  INNER JOIN `{{project_id}}.{{sessions_dataset}}.supervision_officer_attribute_sessions_materialized` attrs 
    USING (staff_id, state_code)
  INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_staff` staff 
    USING (state_code, staff_id)
  WHERE 
    {today_between_start_date_and_nullable_end_date_exclusive_clause("attrs.start_date", "attrs.end_date_exclusive")}
    {f'AND {" AND ".join(get_state_specific_config_exclusions(StateCode.US_PA))}' if get_state_specific_config_exclusions(StateCode.US_PA) else ""}
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
    sessions_dataset=dataset_config.SESSIONS_DATASET,
    should_materialize=True,
    columns=[
        "state_code",
        "external_id",
        "staff_id",
        "full_name",
        "email",
        "supervisor_external_id",
        "supervision_district",
        "supervision_unit",
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_OFFICER_SUPERVISORS_VIEW_BUILDER.build_and_print()
