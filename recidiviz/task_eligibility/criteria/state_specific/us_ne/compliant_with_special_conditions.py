# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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

"""Defines a criterion that shows spans of time when supervision clients in NE
are considered compliant with special conditions.
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

_CRITERIA_NAME = "US_NE_COMPLIANT_WITH_SPECIAL_CONDITIONS"

_QUERY_TEMPLATE = f"""
-- Identify all spans when someone was marked as non-compliant with a special condition
WITH special_condition_noncompliance_sessions AS (
    SELECT
        state_code,
        person_id,
        start_date,
        end_date_exclusive,
        special_condition_type,
        compliance,
    FROM `{{project_id}}.sessions.us_ne_special_condition_compliance_sessions_materialized`
    WHERE compliance = "No"
)
,
-- Create sub-sessions for each special condition non-compliance session
{create_sub_sessions_with_attributes(
    table_name="special_condition_noncompliance_sessions",
    end_date_field_name="end_date_exclusive",
    index_columns=["person_id", "state_code"],
)}
,
-- Aggregate the list of special conditions for which the client was marked as non-compliant
all_conditions AS (
    SELECT
        state_code,
        person_id,
        start_date,
        end_date_exclusive,
        ARRAY_AGG(special_condition_type ORDER BY special_condition_type) AS non_compliant_conditions,
    FROM sub_sessions_with_attributes
    GROUP BY 1, 2, 3, 4
)
SELECT
    state_code,
    person_id,
    start_date,
    end_date_exclusive AS end_date,
    FALSE AS meets_criteria,
    non_compliant_conditions,
    start_date AS last_case_plan_date,
    TO_JSON(STRUCT(
        non_compliant_conditions AS non_compliant_conditions,
        start_date AS last_case_plan_date
    )) AS reason,
FROM all_conditions
"""

_REASONS_FIELDS = [
    ReasonsField(
        name="non_compliant_conditions",
        type=bigquery.enums.StandardSqlTypeNames.ARRAY,
        description="List of conditions for which the client is marked as non-compliant",
    ),
    ReasonsField(
        name="last_case_plan_date",
        type=bigquery.enums.StandardSqlTypeNames.DATE,
        description="Date of last case plan, when the client was marked as non-compliant",
    ),
]

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=__doc__,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        reasons_fields=_REASONS_FIELDS,
        state_code=StateCode.US_NE,
        meets_criteria_default=True,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
