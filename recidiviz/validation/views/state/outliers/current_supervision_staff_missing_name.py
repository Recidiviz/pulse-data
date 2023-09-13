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
View that returns a list of ingested StateStaff individuals that have an open role period with subtype 
SUPERVISION_OFFICER or SUPERVISION_OFFICER_SUPERVISOR but don't have a StateStaff record
with valued name information
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import (
    today_between_start_date_and_nullable_end_date_clause,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views.dataset_config import VIEWS_DATASET

_VIEW_NAME = "current_supervision_staff_missing_name"

_VIEW_DESCRIPTION = (
    "View that returns a list of ingested StateStaff individuals that have an open role period with role type  "
    "SUPERVISION_OFFICER but don't have a StateStaff record with valued name information. "
    "Missing names for supervisors and officers is launch blocking for both the Outliers email tool and web tool."
)

_QUERY_TEMPLATE = f"""
    WITH 
    current_supervision_staff AS (
    SELECT 
        state_code,
        staff_id,
        role_subtype
    FROM `{{project_id}}.normalized_state.state_staff_role_period`
    WHERE 
        role_type = 'SUPERVISION_OFFICER' AND 
        {today_between_start_date_and_nullable_end_date_clause("start_date", "end_date")}
    )

    SELECT DISTINCT
        current_supervision_staff.state_code AS region_code,
        current_supervision_staff.staff_id,
        current_supervision_staff.role_subtype,
        full_name
    FROM current_supervision_staff
    LEFT JOIN `{{project_id}}.normalized_state.state_staff` USING(staff_id, state_code)
    WHERE 
        full_name IS NULL OR
        JSON_EXTRACT_SCALAR(full_name, '$.given_names') IS NULL OR
        JSON_EXTRACT_SCALAR(full_name, '$.surname') IS NULL

"""

CURRENT_SUPERVISION_STAFF_MISSING_NAME_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=VIEWS_DATASET,
    view_id=_VIEW_NAME,
    view_query_template=_QUERY_TEMPLATE,
    description=_VIEW_DESCRIPTION,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        CURRENT_SUPERVISION_STAFF_MISSING_NAME_VIEW_BUILDER.build_and_print()
