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
"""Preprocessed fines and fees sessions view for TN.
Unique on state code, person_id, compartment_level_0_super_session_id, fee_type, start_date, end_date"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_TN_FINES_FEES_SESSIONS_PREPROCESSED_VIEW_NAME = (
    "us_tn_fines_fees_sessions_preprocessed"
)

US_TN_FINES_FEES_SESSIONS_PREPROCESSED_VIEW_DESCRIPTION = """Preprocessed fines and fees sessions view for TN.
Unique on state code, person_id, compartment_level_0_super_session_id, fee_type, start_date, end_date"""

US_TN_FINES_FEES_SESSIONS_PREPROCESSED_QUERY_TEMPLATE = f"""
    /* The following CTE unions together different date boundaries: session starts, session ends, payments, invoices.
     Invoices and payments (positive amounts for invoices and negative amounts for payments) allow us to sum together
     overall activity for money owed and money paid to compute a running balance. Bringing in session boundaries
     allows us to have "full coverage" over someone's time in the system, even if there are periods where there is
     no invoice or payment activity
    */
    WITH population_change_dates AS (
        SELECT 
            state_code,
            person_id,
            start_date AS change_date,
            0 AS invoice_amount,
            0 AS activity_amount,
            '' AS transaction_type,
        FROM
            `{{project_id}}.{{sessions_dataset}}.compartment_sessions_materialized` cs
        WHERE
            state_code = 'US_TN'
        
        UNION ALL
        
        SELECT 
            state_code,
            person_id,
            end_date AS change_date,
            0 AS invoice_amount,
            0 AS activity_amount,
            '' AS transaction_type,
        FROM
            `{{project_id}}.{{sessions_dataset}}.compartment_sessions_materialized` cs
        WHERE 
            end_date IS NOT NULL
            AND state_code = 'US_TN'
        
        UNION ALL
          
        SELECT 
            state_code,
            person_id,
            invoice_date AS change_date,
            invoice_amount_adjusted AS invoice_amount,
            invoice_amount_adjusted AS activity_amount,
            'INVOICE' AS transaction_type,
        FROM
            `{{project_id}}.{{analyst_dataset}}.invoices_preprocessed_materialized`
        WHERE
            state_code = 'US_TN'
        
        UNION ALL
        
        SELECT
            state_code,
            person_id,
            payment_date AS change_date,
            0 AS invoice_amount,
            -1 * payment_amount AS activity_amount,
            'PAYMENT' AS transaction_type
        FROM
            `{{project_id}}.{{analyst_dataset}}.payments_preprocessed_materialized`
        WHERE
            state_code = 'US_TN'
    ),
    population_change_dates_agg AS (
        SELECT 
            state_code,
            person_id,
            change_date,
            SUM(invoice_amount) AS invoice_amount,
            SUM(activity_amount) AS activity_amount,
            STRING_AGG(transaction_type) AS transaction_type
        FROM
            population_change_dates
        GROUP BY
            1,2,3
    ),
    time_agg_join AS (
        SELECT 
            p.state_code,
            p.person_id,
            p.change_date AS start_date,
            p.transaction_type,
            compartment_level_0_super_session_id,
            LEAD(change_date) OVER (PARTITION BY p.state_code,
                                               p.person_id
                                  ORDER BY change_date) AS end_date,
            SUM(activity_amount) OVER (PARTITION BY p.state_code, 
                                                    p.person_id
                                    ORDER BY change_date
                                    ) AS unpaid_balance,
            SUM(activity_amount) OVER (PARTITION BY p.state_code, 
                                                    p.person_id,
                                                    cs.compartment_level_0_super_session_id
                                    ORDER BY change_date
            ) AS compartment_level_0_unpaid_balance,
        FROM
            population_change_dates_agg p
        LEFT JOIN
            `{{project_id}}.{{sessions_dataset}}.compartment_level_0_super_sessions_materialized` cs
        ON 
            p.person_id = cs.person_id
        AND 
            p.change_date BETWEEN cs.start_date AND {nonnull_end_date_clause('cs.end_date')}
    )
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date,
        compartment_level_0_super_session_id,
        "SUPERVISION_FEES" AS fee_type,
        transaction_type,
        unpaid_balance,
        compartment_level_0_unpaid_balance,
    FROM
        time_agg_join
"""

US_TN_FINES_FEES_SESSIONS_PREPROCESSED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_TN_FINES_FEES_SESSIONS_PREPROCESSED_VIEW_NAME,
    sessions_dataset=SESSIONS_DATASET,
    description=US_TN_FINES_FEES_SESSIONS_PREPROCESSED_VIEW_DESCRIPTION,
    view_query_template=US_TN_FINES_FEES_SESSIONS_PREPROCESSED_QUERY_TEMPLATE,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_FINES_FEES_SESSIONS_PREPROCESSED_VIEW_BUILDER.build_and_print()
