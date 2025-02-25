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
    aggregate_adjacent_spans,
    create_sub_sessions_with_attributes,
)

# TODO(#20230): Deprecate this view and move functions to state_data_query_fragments when relevant data is ingested
# TODO(#20231): Ingest drug screens data into state_drug_screen


def at_least_X_time_since_positive_drug_screen(
    date_interval: int = 12,
    date_part: str = "MONTH",
) -> str:
    """
    Args:
        date_interval (int): Number of <date_part> when the positive drug screen
            will be counted as valid. Defaults to 12 (e.g. it could be 12 months).
        date_part (str): Supports any of the BigQuery date_part values:
            "DAY", "WEEK","MONTH","QUARTER","YEAR". Defaults to "MONTH".
    Returns:
        f-string: Spans of time where the criteria is met
    """

    return f"""
    WITH positive_drug_test_sessions_cte AS
    (
        SELECT
            state_code,
            person_id,
            drug_screen_date AS start_date,
            DATE_ADD(drug_screen_date, INTERVAL {date_interval} {date_part}) AS end_date,
            FALSE AS meets_criteria,
            drug_screen_date AS latest_drug_screen_date,
        FROM
            `{{project_id}}.{{sessions_dataset}}.drug_screens_preprocessed_materialized`
        WHERE
            is_positive_result
    )
    ,
    /*
    If a person has more than 1 positive test in an X month period, they will have overlapping sessions
    created in the above CTE. Therefore we use `create_sub_sessions_with_attributes` to break these up
    */
    {create_sub_sessions_with_attributes('positive_drug_test_sessions_cte')}
    ,
    dedup_cte AS
    /*
    If a person has more than 1 positive test in an X month period, they will have duplicate sub-sessions for 
    the period of time where there were more than 1 tests. For example, if a person has a test on Jan 1 and March 1
    there would be duplicate sessions for the period March 1 - Dec 31 because both tests are relevant at that time.
    We deduplicate below so that we surface the most-recent test that is relevant at each time. 
    */
    (
    SELECT
        *,
    FROM sub_sessions_with_attributes
    QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id, state_code, start_date, end_date 
        ORDER BY latest_drug_screen_date DESC) = 1
    )
    ,
    sessionized_cte AS 
    /*
    Sessionize so that we have continuous periods of time for which a person is not eligible due to a positive test. A
    new session exists either when a person becomes eligible, or if a person has an additional test within a 12-month
    period which changes the "latest_drug_screen_date" value.
    */
    (
    {aggregate_adjacent_spans(table_name='dedup_cte',
                       attribute=['latest_drug_screen_date','meets_criteria'],
                       end_date_field_name='end_date')}
    )
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date,
        meets_criteria,
        TO_JSON(STRUCT(latest_drug_screen_date AS most_recent_positive_test_date)) AS reason,
        latest_drug_screen_date AS most_recent_positive_test_date,
    FROM sessionized_cte
    """


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
            external_id,
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
