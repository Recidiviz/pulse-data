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
"""Preprocessed view showing spans of time when a person made consecutive monthly payments."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

CONSECUTIVE_PAYMENTS_PREPROCESSED_VIEW_NAME = "consecutive_payments_preprocessed"

CONSECUTIVE_PAYMENTS_PREPROCESSED_VIEW_DESCRIPTION = """Preprocessed view showing spans of time when a person made
consecutive monthly payments."""

CONSECUTIVE_PAYMENTS_PREPROCESSED_QUERY_TEMPLATE = f"""
    WITH payments_preprocessed AS (
        SELECT 
            state_code,
            person_id,
            external_id,
            payment_date AS start_date,
            next_payment_date AS end_date_exclusive,
            MAX(payment_date) OVER(PARTITION BY person_id, external_id) as latest_payment,
            CASE WHEN 
                DATE_DIFF(
                    DATE_TRUNC(next_payment_date, MONTH), 
                    DATE_TRUNC(payment_date, MONTH), 
                    MONTH
                    ) <= 1 
                THEN TRUE
                /* 
                If someone made a payment in the current or previous month, we don't want to start a new "span"
                that resets the number of consecutive payments
                */  
                WHEN next_payment_date IS NULL
                    AND DATE_DIFF(
                            DATE_TRUNC(CURRENT_DATE('US/Eastern'), MONTH),
                            DATE_TRUNC(payment_date, MONTH),
                            MONTH
                        ) <= 1
                THEN TRUE
                ELSE FALSE END AS consecutive_payment
        FROM (
            SELECT *, 
                LEAD(payment_date) 
                    OVER(partition by person_id, external_id ORDER BY payment_date ASC) AS next_payment_date
            FROM 
                `{{project_id}}.{{analyst_dataset}}.payments_preprocessed_materialized`
            WHERE payment_amount > 0
        )
    ),
    sessionized_cte AS
    (
    {aggregate_adjacent_spans(table_name='payments_preprocessed',
                       attribute=['consecutive_payment','latest_payment','external_id'],
                       session_id_output_name='consecutive_payment_session_id',
                       end_date_field_name='end_date_exclusive')}
    )
    SELECT *,
            /*
            If there are consecutive payments, then we use the span start/end date to figure out how many months
            of consecutive payments there have been. When end_date_exclusive is null (i.e. there are no next
            payments), we use latest_payment to calculate # of consecutive months until now. 
            
            We add 1 to the count of months to count inclusively - i.e. if someone made payments on Jan, Feb, March,
            their span will be 1/20XX - 3/20XX and we want to count that as 3 months of consecutive payments. 
            */
            CASE WHEN consecutive_payment 
            THEN DATE_DIFF(
                        DATE_TRUNC(COALESCE(end_date_exclusive,latest_payment),MONTH),
                        DATE_TRUNC(start_date,MONTH), 
                        MONTH
                    ) + 1
            ELSE 0
            END AS number_consecutive_monthly_payment
    FROM sessionized_cte

"""

CONSECUTIVE_PAYMENTS_PREPROCESSED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=CONSECUTIVE_PAYMENTS_PREPROCESSED_VIEW_NAME,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    description=CONSECUTIVE_PAYMENTS_PREPROCESSED_VIEW_DESCRIPTION,
    view_query_template=CONSECUTIVE_PAYMENTS_PREPROCESSED_QUERY_TEMPLATE,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        CONSECUTIVE_PAYMENTS_PREPROCESSED_VIEW_BUILDER.build_and_print()
