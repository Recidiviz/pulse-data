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
# ============================================================================
"""Spans of time when a client has been on supervision for 2 years (according to the
`ON_SUPERVISION_AT_LEAST_2_YEARS` criterion) and has had an assessed risk level of 'LOW'
since starting supervision and for at least 2 years (according to the
`ASSESSED_RISK_LOW_WHILE_ON_SUPERVISION_AT_LEAST_2_YEARS` criterion).
"""

from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import (
    MAGIC_END_DATE,
    nonnull_end_date_clause,
    revert_nonnull_end_date_clause,
)
from recidiviz.task_eligibility.criteria.general import (
    assessed_risk_low_while_on_supervision_at_least_2_years,
    on_supervision_at_least_2_years,
)
from recidiviz.task_eligibility.dataset_config import TASK_ELIGIBILITY_CRITERIA_GENERAL
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.state_dataset_query_fragments import (
    combining_several_criteria_into_one,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "ON_SUPERVISION_AT_LEAST_2_YEARS_AND_ASSESSED_RISK_LOW_WHILE_ON_SUPERVISION_AT_LEAST_2_YEARS"

_CRITERIA_QUERY_1 = f"""
    SELECT
        state_code,
        person_id,
        start_date,
        end_date,
        meets_criteria,
        SAFE_CAST(JSON_VALUE(reason_v2, '$.minimum_time_served_date') AS DATE) AS minimum_time_served_date,
        NULL AS eligible_date,
        /* We'll end up using the field below to create an overall eligibility date when
        we combine this date-based subcriterion into the aggregate criterion. */
        {nonnull_end_date_clause(
            "SAFE_CAST(JSON_VALUE(reason_v2, '$.minimum_time_served_date') AS DATE)"
        )} AS combined_eligible_date,
    FROM `{{project_id}}.{{task_eligibility_criteria_general_dataset}}.{on_supervision_at_least_2_years.VIEW_BUILDER.view_id}_materialized`
"""

_CRITERIA_QUERY_2 = f"""
    SELECT
        state_code,
        person_id,
        start_date,
        end_date,
        meets_criteria,
        NULL AS minimum_time_served_date,
        SAFE_CAST(JSON_VALUE(reason_v2, '$.eligible_date') AS DATE) AS eligible_date,
        /* We'll end up using the field below to create an overall eligibility date when
        we combine this date-based subcriterion into the aggregate criterion. */
        {nonnull_end_date_clause(
            "SAFE_CAST(JSON_VALUE(reason_v2, '$.eligible_date') AS DATE)"
        )} AS combined_eligible_date,
    FROM `{{project_id}}.{{task_eligibility_criteria_general_dataset}}.{assessed_risk_low_while_on_supervision_at_least_2_years.VIEW_BUILDER.view_id}_materialized`
"""

# Someone can only meet this aggregate criterion if they meet both of the underlying
# criteria. The `num_criteria=2` condition ensures that we have rows for both criteria
# that are being combined. Both of the underlying criteria use
# `meets_criteria_default=False`, and so anyone without a row in a criterion wouldn't
# meet that criterion anyways. If someone has a row for each criterion and meets both,
# then they meet the overall aggregate criterion.
_MEETS_CRITERIA_CONDITION = "LOGICAL_AND(meets_criteria AND num_criteria=2)"

# The first two reasons fields should be identical to the `reason_v2` fields from the
# underlying criteria (as the MAX function will just take the non-null date if it
# exists, and that non-null date would come from only one of the criteria). The third
# reasons field combines the dates to identify an aggregate eligibility date. We only
# have a combined eligibility date if there are non-null/non-'9999-12-31' dates for both
# of the underlying criteria. In other words, we can only set an eligibility date when
# the person has actual dates set for both criteria.
_REASONS_CONTENT = f"""MAX(minimum_time_served_date) AS minimum_time_served_date,
MAX(eligible_date) AS eligible_date,
{revert_nonnull_end_date_clause(f"IF(ANY_VALUE(num_criteria)=2, MAX(combined_eligible_date), '{MAGIC_END_DATE}')")} AS combined_eligible_date"""

_QUERY_TEMPLATE = f"""
    WITH combined_criteria AS (
        {combining_several_criteria_into_one(
            select_statements_for_criteria_lst=[
                _CRITERIA_QUERY_1,
                _CRITERIA_QUERY_2,
            ],
            meets_criteria=_MEETS_CRITERIA_CONDITION,
            json_content=_REASONS_CONTENT,
        )}
    )
    SELECT
        *,
        JSON_EXTRACT(reason, "$.minimum_time_served_date") AS minimum_time_served_date,
        JSON_EXTRACT(reason, "$.eligible_date") AS eligible_date,
        JSON_EXTRACT(reason, "$.combined_eligible_date") AS combined_eligible_date,
    FROM combined_criteria
"""

VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = StateAgnosticTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    description=__doc__,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    meets_criteria_default=False,
    task_eligibility_criteria_general_dataset=TASK_ELIGIBILITY_CRITERIA_GENERAL,
    reasons_fields=[
        ReasonsField(
            name="minimum_time_served_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="Date when the client has served the time required",
        ),
        ReasonsField(
            name="eligible_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="Date when individual has been at a qualifying risk level for the required time period",
        ),
        ReasonsField(
            name="combined_eligible_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="Date when the client has met both the time-served and time-at-risk-level criteria",
        ),
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
