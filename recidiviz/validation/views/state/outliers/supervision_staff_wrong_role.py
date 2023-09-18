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
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.    See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.    If not, see <https://www.gnu.org/licenses/>.
# =============================================================================
"""
View that returns information for supervision officers who have an open StateStaff 
supervisor period, but do not have an open StateStaff role period with role subtype 
SUPERVISION_OFFICER, and supervision officers who are listed as supervisors in StateStaff 
supervisor period but do not have an open StateStaff role period with role subtype 
SUPERVISION_OFFICER_SUPERVISOR.
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import (
    today_between_start_date_and_nullable_end_date_clause,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views.dataset_config import VIEWS_DATASET

_VIEW_NAME = "supervision_staff_wrong_role"

_VIEW_DESCRIPTION = (
    "View that returns information for supervision officers who have an open StateStaff "
    "supervisor period, but do not have an open StateStaff role period with role subtype "
    "SUPERVISION_OFFICER, and supervision officers who are listed as supervisors in StateStaff "
    "supervisor period but do not have an open StateStaff role period with role subtype "
    "SUPERVISION_OFFICER_SUPERVISOR."
)

_QUERY_TEMPLATE = f"""
    WITH 
    current_officers AS (
      -- staff IDs of all employees with open supervisor periods
    SELECT 
        state_code,
        staff_id,
    FROM `{{project_id}}.normalized_state.state_staff_supervisor_period`
    WHERE 
        {today_between_start_date_and_nullable_end_date_clause("start_date", "end_date")}
    ),
    current_supervisors AS (
      -- staff IDs of all employees listed as a supervisor in an open supervisor period
        SELECT DISTINCT
            sp.state_code,
            e.staff_id,
        FROM `{{project_id}}.normalized_state.state_staff_supervisor_period` sp
        LEFT JOIN `{{project_id}}.normalized_state.state_staff_external_id` e
        ON(sp.supervisor_staff_external_id = e.external_id) AND
        (sp.state_code != 'US_IX' or e.id_type = 'US_IX_EMPLOYEE') 
    ),
    officers_wrong_role AS (    
      -- staff IDs and role subtypes of employees who have a supervisor
    SELECT
        current_officers.state_code as region_code,
        current_officers.staff_id,
        ARRAY_AGG(role_period.role_subtype) AS role_subtypes,
        'SUPERVISION_OFFICER' AS expected_subtype
    FROM current_officers
    LEFT JOIN `{{project_id}}.normalized_state.state_staff_role_period` role_period
    USING(staff_id)
    WHERE {today_between_start_date_and_nullable_end_date_clause("start_date", "end_date")}
    GROUP BY 1,2
    ),
    supervisors_wrong_role AS (
      -- staff IDs and role subtypes of employees who supervise an officer
    SELECT
        current_supervisors.state_code as region_code,
        current_supervisors.staff_id,
        ARRAY_AGG(role_period.role_subtype) AS role_subtypes,
        'SUPERVISION_OFFICER_SUPERVISOR' AS expected_subtype
    FROM current_supervisors 
    LEFT JOIN `{{project_id}}.normalized_state.state_staff_role_period` role_period
    USING(staff_id)
    WHERE {today_between_start_date_and_nullable_end_date_clause("start_date", "end_date")}
    GROUP BY 1,2
    )

-- All staff_ids, subtypes, and missing but expected subtypes of employees
-- This includes employees who have a supervisor in an open supervisor period, but do not have a row in role period with role subtype SUPERVISION_OFFICER
-- AND employees who supervise an employee in an open supervisor period, but do not have a rwo in role period with role subtype SUPERVISION_OFFICER_SUPERVISOR
    SELECT  
      region_code,
      staff_id,
      role_subtypes,
      expected_subtype
    FROM officers_wrong_role
    WHERE 'SUPERVISION_OFFICER' NOT IN UNNEST(role_subtypes)
    UNION ALL
    SELECT 
      region_code,
      staff_id,
      role_subtypes,
      expected_subtype
    FROM supervisors_wrong_role
    WHERE 'SUPERVISION_OFFICER_SUPERVISOR' NOT IN UNNEST(role_subtypes)
"""

SUPERVISION_STAFF_WRONG_ROLE_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=VIEWS_DATASET,
    view_id=_VIEW_NAME,
    view_query_template=_QUERY_TEMPLATE,
    description=_VIEW_DESCRIPTION,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_STAFF_WRONG_ROLE_VIEW_BUILDER.build_and_print()
