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
# ============================================================================
"""Describes the spans of time when a TN client either has a low fines/fees balance, has a permanent exemption, or
has made regular payments."""
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.criteria.general.has_fines_fees_balance_below_500 import (
    VIEW_BUILDER as has_fines_fees_balance_below_500_builder,
)
from recidiviz.task_eligibility.criteria.general.has_payments_3_consecutive_months import (
    VIEW_BUILDER as has_payments_3_consecutive_months_builder,
)
from recidiviz.task_eligibility.criteria.general.has_permanent_fines_fees_exemption import (
    VIEW_BUILDER as has_permanent_fines_fees_exemption_builder,
)
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_TN_FINES_FEES_ELIGIBLE"

_DESCRIPTION = """Describes the spans of time when a TN client either has a low fines/fees balance, has a permanent
exemption, or has made regular payments."""

_QUERY_TEMPLATE = f"""
    WITH combine_views AS (
        SELECT 
            state_code,
            person_id,
            start_date,
            end_date,
            meets_criteria,
            TO_JSON(STRUCT('{has_fines_fees_balance_below_500_builder.criteria_name}' AS criteria_name, reason AS reason))
            reason,
        FROM
            `{{project_id}}.{{criteria_dataset}}.{has_fines_fees_balance_below_500_builder.view_id}_materialized`
        WHERE
            state_code = "US_TN"

        UNION ALL

        SELECT 
            state_code,
            person_id,
            start_date,
            end_date,
            meets_criteria,
            TO_JSON(STRUCT('{has_payments_3_consecutive_months_builder.criteria_name}' AS criteria_name, reason AS reason))
        FROM
            `{{project_id}}.{{criteria_dataset}}.{has_payments_3_consecutive_months_builder.view_id}_materialized`
        WHERE
            state_code = "US_TN"

        UNION ALL

        SELECT 
            state_code,
            person_id,
            start_date,
            end_date,
            meets_criteria,
            TO_JSON(STRUCT('{has_permanent_fines_fees_exemption_builder.criteria_name}' AS criteria_name, reason AS reason))
        FROM
            `{{project_id}}.{{criteria_dataset}}.{has_permanent_fines_fees_exemption_builder.view_id}_materialized`
        WHERE
            state_code = "US_TN"
    ),
    {create_sub_sessions_with_attributes('combine_views')}
    SELECT
        state_code,
        person_id,
        start_date,
        end_date,
        LOGICAL_OR(meets_criteria) AS meets_criteria,
        TO_JSON(ARRAY_AGG(
            reason
        )) AS reason,
    FROM sub_sessions_with_attributes
    GROUP BY
        1,2,3,4
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        state_code=StateCode.US_TN,
        criteria_name=_CRITERIA_NAME,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        description=_DESCRIPTION,
        criteria_dataset=has_fines_fees_balance_below_500_builder.dataset_id,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
