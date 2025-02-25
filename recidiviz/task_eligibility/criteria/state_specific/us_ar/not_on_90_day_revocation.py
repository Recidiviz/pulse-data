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
# ============================================================================
"""Spans of time when someone is not on 90-day revocation status in Arkansas.
"""
from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_AR_NOT_ON_90_DAY_REVOCATION"

_DESCRIPTION = (
    """Spans of time when someone is not on 90-day revocation status in Arkansas."""
)

_QUERY_TEMPLATE = f"""
    WITH ninety_day_revocation_ips AS (
        SELECT
            state_code,
            person_id,
            admission_date AS start_date,
            release_date AS end_date,
            TRUE AS is_90_day_revocation,
        FROM `{{project_id}}.normalized_state.state_incarceration_period`
        WHERE 
            state_code = 'US_AR' 
            AND specialized_purpose_for_incarceration_raw_text = '90_DAY'

    )
    ,
    -- TODO(#33097): Update once we can identify 90-day revocations in sessions
    {create_sub_sessions_with_attributes(
        table_name="ninety_day_revocation_ips",
    )}
    SELECT
        state_code,
        person_id,
        start_date,
        end_date,
        FALSE AS meets_criteria,
        is_90_day_revocation,
        TO_JSON(STRUCT(is_90_day_revocation)) AS reason
    FROM sub_sessions_with_attributes
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        reasons_fields=[
            ReasonsField(
                name="is_90_day_revocation",
                type=bigquery.enums.StandardSqlTypeNames.BOOL,
                description="Whether the person is on 90-day revocation status.",
            )
        ],
        state_code=StateCode.US_AR,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        meets_criteria_default=True,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
