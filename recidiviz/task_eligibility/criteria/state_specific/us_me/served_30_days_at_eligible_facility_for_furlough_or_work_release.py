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

"""
Defines a criteria view that shows spans of time for
which clients have served 30 days at an eligible facility
"""
from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import (
    nonnull_end_date_clause,
    nonnull_start_date_clause,
)
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = (
    "US_ME_SERVED_30_DAYS_AT_ELIGIBLE_FACILITY_FOR_FURLOUGH_OR_WORK_RELEASE"
)

_DESCRIPTION = """Defines a criteria view that shows spans of time for
which clients have served 30 days at an eligible facility
"""

_ELIGIBLE_FACILITIES = """  "MOUNTAIN VIEW CORRECTIONAL FACILITY",
                    "MAINE CORRECTIONAL CENTER",
                    "MID-COAST REENTRY (WALDO)",
                    "SOUTHERN MAINE WOMEN'S REENTRY CENTER",
                    "CHARLESTON CORRECTIONAL FACILITY",
                    "DOWNEAST CORRECTIONAL FACILITY",
                    "BOLDUC CORRECTIONAL FACILITY" """

_QUERY_TEMPLATE = f"""
WITH inc_period_with_eligible_facilities AS (
    SELECT 
        person_id, 
        state_code,
        admission_date AS start_date,
        DATE_ADD(admission_date, INTERVAL 30 DAY) AS critical_date,
        release_date AS end_date,
        facility,
    FROM `{{project_id}}.normalized_state.state_incarceration_period` 
    WHERE state_code = 'US_ME'
        AND facility IN ({_ELIGIBLE_FACILITIES})
        AND {nonnull_start_date_clause('admission_date')} != {nonnull_end_date_clause('release_date')}
),

{create_sub_sessions_with_attributes('inc_period_with_eligible_facilities')},

critical_date_spans AS (
    SELECT 
    person_id,
    state_code,
    start_date AS start_datetime,
    end_date AS end_datetime,
    MIN(critical_date) AS critical_date,
    ANY_VALUE(facility) AS facility
    FROM sub_sessions_with_attributes
    GROUP BY 1,2,3,4
), 

{critical_date_has_passed_spans_cte()}

SELECT
    state_code,
    person_id,
    start_date,
    end_date,     
    critical_date_has_passed AS meets_criteria,
    TO_JSON(STRUCT(critical_date AS eligible_date)) AS reason,
    critical_date AS eligible_date,
FROM critical_date_has_passed_spans cd
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        state_code=StateCode.US_ME,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        reasons_fields=[
            ReasonsField(
                name="eligible_date",
                type=bigquery.enums.SqlTypeNames.DATE,
                description="#TODO(#29059): Add reasons field description",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
