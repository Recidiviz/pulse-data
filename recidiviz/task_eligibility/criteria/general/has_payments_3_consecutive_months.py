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
"""Describes the spans of time when a client has a balance below $2000 and has made consecutive payments for 3 months"""

from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.task_eligibility.criteria.general.has_fines_fees_balance_below_500 import (
    VIEW_BUILDER as has_fines_fees_balance_below_500_builder,
)
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "HAS_PAYMENTS_3_CONSECUTIVE_MONTHS"

_QUERY_TEMPLATE = f"""
    WITH balance_and_payments AS (
        SELECT 
            state_code,
            person_id,
            start_date,
            end_date,
            CAST(JSON_EXTRACT_SCALAR(reason, '$.amount_owed') AS FLOAT64) AS amount_owed,
            NULL AS number_consecutive_monthly_payment
        FROM
            `{{project_id}}.{{criteria_dataset}}.{has_fines_fees_balance_below_500_builder.view_id}_materialized` ff

        UNION ALL

        SELECT
          state_code,
          person_id,
          start_date,
          end_date_exclusive AS end_date,
          NULL AS amount_owed,
          number_consecutive_monthly_payment
        FROM
            `{{project_id}}.{{analyst_dataset}}.consecutive_payments_preprocessed_materialized` 
    ),
    {create_sub_sessions_with_attributes('balance_and_payments')},
    grouped_dates AS (
        SELECT
            DISTINCT
            state_code,
            person_id,
            start_date,
            end_date,
            FIRST_VALUE(amount_owed) OVER(amount_owed_window) AS amount_owed,
            FIRST_VALUE(number_consecutive_monthly_payment) OVER(consecutive_window) AS number_consecutive_monthly_payment,            
        FROM sub_sessions_with_attributes
        WINDOW amount_owed_window AS (
          PARTITION BY state_code, person_id, start_date, end_date
            ORDER BY CASE WHEN amount_owed IS NOT NULL THEN 0
                          ELSE 1 END
        ),
        consecutive_window AS (
          PARTITION BY state_code, person_id, start_date, end_date
            ORDER BY CASE WHEN number_consecutive_monthly_payment IS NOT NULL THEN 0 
                          ELSE 1 END
        )    
    )
    SELECT
        state_code,
        person_id,
        start_date,
        end_date,
        CASE 
            WHEN amount_owed <= 2000 AND number_consecutive_monthly_payment >= 3 THEN TRUE 
            ELSE FALSE END AS meets_criteria,
        TO_JSON(STRUCT(
            amount_owed AS amount_owed,
            number_consecutive_monthly_payment AS consecutive_monthly_payments
        )) AS reason,
        amount_owed,
        number_consecutive_monthly_payment AS consecutive_monthly_payments
    FROM
        grouped_dates
"""

VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = StateAgnosticTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    description=__doc__,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    criteria_dataset=has_fines_fees_balance_below_500_builder.dataset_id,
    reasons_fields=[
        ReasonsField(
            name="amount_owed",
            type=bigquery.enums.StandardSqlTypeNames.FLOAT64,
            description="Amount that a client owes in fines/fees",
        ),
        ReasonsField(
            name="consecutive_monthly_payments",
            type=bigquery.enums.StandardSqlTypeNames.INT64,
            description="Number of consecutive monthly payments that a client has made",
        ),
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
