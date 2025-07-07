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
"""Helper SQL fragments that do standard queries against pre-processed views.
"""

from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)

# TODO(#20230): Deprecate this view and move functions to state_data_query_fragments when relevant data is ingested
# TODO(#20231): Ingest drug screens data into state_drug_screen
# TODO(#38834): Clean up query fragments vs. criteria builders


def has_at_least_x_negative_tests_in_time_interval(
    number_of_negative_tests: int = 1,
    date_interval: int = 12,
    date_part: str = "MONTH",
) -> str:
    """
    Args:
        number_of_negative_tests: Number of negative tests needed within time interval
        date_interval (int): Number of <date_part> when the negative drug screen
            will be counted as valid. Defaults to 12 (e.g. it could be 12 months).
        date_part (str): Supports any of the BigQuery date_part values:
            "DAY", "WEEK","MONTH","QUARTER","YEAR". Defaults to "MONTH".
    Returns:
        f-string: Spans of time where the criteria is met
    """

    return f"""
    WITH screens AS (
        SELECT
            state_code,
            person_id,
            drug_screen_date AS start_date,
            DATE_ADD(drug_screen_date, INTERVAL {date_interval} {date_part}) AS end_date,
            drug_screen_date,
            result_raw_text_primary
        FROM
            `{{project_id}}.{{sessions_dataset}}.drug_screens_preprocessed_materialized`
        WHERE
            NOT is_positive_result
    ),
    {create_sub_sessions_with_attributes('screens')},
    grouped AS (
        SELECT
            state_code,
            person_id,
            start_date,
            end_date,
            count(*) AS num_screens_within_time_interval,
            TO_JSON(
                ARRAY_AGG(
                    STRUCT(
                        drug_screen_date AS negative_screen_date,
                        result_raw_text_primary AS negative_screen_result
                    ) ORDER BY drug_screen_date
                )
            ) AS reason,
            ARRAY_AGG(
                STRUCT(
                    drug_screen_date AS negative_screen_date,
                    result_raw_text_primary AS negative_screen_result
                ) ORDER BY drug_screen_date
            ) AS negative_drug_screen_history_array,
        FROM
            sub_sessions_with_attributes
        GROUP BY
            1,2,3,4
    )
    SELECT
        state_code,
        person_id,
        start_date,
        end_date,
        CASE WHEN num_screens_within_time_interval >= {number_of_negative_tests} THEN TRUE ELSE FALSE END AS meets_criteria,
        reason,
        negative_drug_screen_history_array,
    FROM
        grouped
    """


def client_specific_fines_fees_balance(
    unpaid_balance_field: str,
) -> str:
    """
    Args:
        unpaid_balance_field (str, optional): Specifies which field should be used to track unpaid balance.

    Returns:
        f-string: Spans of time deduplicated to a given client and fee type showing their balance
    """

    return f"""
    WITH fines_fees AS (
        SELECT
            state_code,
            person_id,
            payment_account_external_id,
            fee_type,
            transaction_type,
            start_date,
            end_date,
            {unpaid_balance_field} AS current_balance,
        FROM
            `{{project_id}}.{{analyst_dataset}}.fines_fees_sessions_materialized`

    ),
    {create_sub_sessions_with_attributes('fines_fees')}
        SELECT 
            state_code,
            person_id,
            start_date,
            end_date,
            fee_type, 
            SUM(current_balance) AS current_balance,
        FROM sub_sessions_with_attributes
        WHERE start_date != {nonnull_end_date_clause('end_date')}
        GROUP BY 1,2,3,4,5
    """


def has_unpaid_fines_fees_balance(
    fee_type: str,
    unpaid_balance_criteria: str,
    unpaid_balance_field: str,
) -> str:
    """
    Args:
        fee_type (str, optional): Specifies the fee-type (e.g. Restitution, Supervision Fees) since there might be multiple within
         a state.
        unpaid_balance_criteria (str, optional): Specifies the criteria on unpaid balance.
        unpaid_balance_field (str, optional): Specifies which field should be used to track unpaid balance.

    Returns:
        f-string: Spans of time where the unpaid balance condition was met
    """

    return f"""
    WITH aggregated_fines_fees_per_client AS (
        {client_specific_fines_fees_balance(unpaid_balance_field=unpaid_balance_field)}
    )

    SELECT 
        state_code,
        person_id,
        start_date,
        end_date,
        current_balance {unpaid_balance_criteria} AS meets_criteria,
        TO_JSON(STRUCT(current_balance AS amount_owed)) AS reason,
        current_balance AS amount_owed,
    FROM aggregated_fines_fees_per_client
    WHERE fee_type = "{fee_type}"
    """
