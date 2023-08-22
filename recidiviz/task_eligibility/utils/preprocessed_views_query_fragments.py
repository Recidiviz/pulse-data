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

from typing import List, Optional

from recidiviz.calculator.query.bq_utils import (
    nonnull_end_date_clause,
    nonnull_end_date_exclusive_clause,
    revert_nonnull_end_date_clause,
)
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
        TO_JSON(STRUCT(latest_drug_screen_date AS most_recent_positive_test_date)) AS reason
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
            TO_JSON(STRUCT(ARRAY_AGG(drug_screen_date ORDER BY drug_screen_date) AS latest_negative_screen_dates,
                    ARRAY_AGG(result_raw_text_primary ORDER BY drug_screen_date) AS latest_negative_screen_results)) AS reason
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
        reason
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
    FROM aggregated_fines_fees_per_client
    WHERE fee_type = "{fee_type}"
    """


def time_difference_from_client_record_case_notes(
    state_code: str,
    criteria_str: str = "Time remaining on supervision",
    event_date_str: str = "NULL",
    note_title_str: str = "NULL",
    latest_date: str = "expiration_date",
    earliest_date: str = "CURRENT_DATE('US/Pacific')",
) -> str:
    """
    This generates a view that calculates the time difference between two dates present in
    the client record (e.g. time remaining on supervision) and formats it to be consistent
    with workflows case notes.

    Args:
        state_code (str): String state code (e.g. 'US_MI')
        criteria_str (str, optional): Criteria name as a string. Defaults to "Time remaining on supervision".
        event_date_str (str, optional): Event date. Defaults to "NULL".
        note_title_str (str, optional): Note title. Defaults to "NULL".
        latest_date (str, optional): Date to be used to calculate the difference. This
            date should come later than earliest_date. Defaults to "expiration_date".
        earliest_date (str, optional): Date to be used to calculate the difference. This
            date should come before than latest_date. Defaults to "CURRENT_DATE('US/Pacific')".

    Returns:
        str: SQL query
    """

    return f"""
    SELECT 
        external_id,
        {note_title_str} AS note_title,
        CASE 
            WHEN ABS(years_remaining) > 0 
            THEN CONCAT(CAST(years_remaining AS string), " years and ", CAST(MOD(months_remaining,12) AS string), " months")
            WHEN ABS(years_remaining) = 0 AND ABS(months_remaining)>=1
            THEN CONCAT(CAST(months_remaining AS string), " months")
            WHEN ABS(years_remaining) = 0 AND ABS(months_remaining)=0
            THEN CONCAT(CAST(days_remaining AS string), " days")
        END AS note_body,
        {event_date_str} as event_date,
        "{criteria_str}" AS criteria,
    FROM (
        SELECT 
            person_external_id AS external_id,
            DATE_DIFF({latest_date}, {earliest_date}, YEAR) AS years_remaining,
            DATE_DIFF({latest_date}, {earliest_date}, MONTH) AS months_remaining,
            DATE_DIFF({latest_date}, {earliest_date}, DAY) AS days_remaining,
            {latest_date},
        FROM `{{project_id}}.{{workflows_dataset}}.client_record_materialized` 
        WHERE state_code = "{state_code}"
        # We drop cases where earliest_date comes after latest_date
            AND {latest_date} > {earliest_date}
        )
    """


def compartment_level_1_super_sessions_without_me_sccp() -> str:
    """
    Compartment level 1 super sessions after partitioning with ME SCCP sesssions.
    """

    return f"""cl1_super_sessions_wsccp AS (
        SELECT 
            state_code, 
            person_id,
            start_date, 
            end_date_exclusive AS end_date,
            compartment_level_1,
            NULL AS compartment_level_2,
        FROM `{{project_id}}.{{sessions_dataset}}.compartment_level_1_super_sessions_materialized`

        UNION ALL

        SELECT 
            state_code, 
            person_id,
            start_date, 
            end_date_exclusive AS end_date,
            compartment_level_1,
            compartment_level_2,
            FROM `{{project_id}}.{{sessions_dataset}}.compartment_sessions_materialized`
        WHERE state_code = 'US_ME'
            AND compartment_level_2 = 'COMMUNITY_CONFINEMENT'
            AND compartment_level_1 = 'SUPERVISION'
    ),

    {create_sub_sessions_with_attributes('cl1_super_sessions_wsccp')},

    partitioning_compartment_l1_ss_with_sccp AS (
        SELECT 
            *,
            DATE_ADD(end_date, INTERVAL 1 DAY) AS end_date_exclusive
        FROM sub_sessions_with_attributes
        # For repeated subsessions, keep only the one with a value compartment_level_2
        QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id, state_code, start_date, end_date ORDER BY compartment_level_2 DESC) = 1
    )
    """


def join_current_task_eligibility_spans_with_external_id(
    state_code: str,
    tes_task_query_view: str,
    id_type: str,
    additional_columns: str = "",
) -> str:
    """
    It joins a task eligibility span view with the state_person_external_id to retrieve external ids.
    It also filters out spans of time that aren't current.

    Returns:
        state_code (str): State code. The final statement will filter out all other states.
        tes_task_query_view (str): The task query view that we're interested in querying.
            E.g. 'work_release_materialized'.
    """
    return f"""    SELECT
        pei.external_id,
        tes.person_id,
        tes.state_code,
        tes.reasons,
        tes.ineligible_criteria,
        tes.is_eligible,
        {additional_columns}
    FROM `{{project_id}}.{{task_eligibility_dataset}}.{tes_task_query_view}` tes
    LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
        USING(person_id)
    WHERE 
      CURRENT_DATE('US/Pacific') BETWEEN tes.start_date AND 
                                         {nonnull_end_date_exclusive_clause('tes.end_date')}
      AND tes.state_code = {state_code}
      AND pei.id_type = {id_type}
    """


def array_agg_case_notes_by_external_id(
    from_cte: str = "eligible_and_almost_eligible",
    left_join_cte: str = "case_notes_cte",
) -> str:
    """
    Aggregates all case notes into one array within a JSON by external_id.

    Args:
        from_cte (str, optional): Usually the view that contains eligible and almost
            eligible client list. Defaults to "eligible_and_almost_eligible".
        left_join_cte (str, optional): Usually the CTE containing all the case notes.
            This CTE should contain the following columns:
                - note_title
                - note_body
                - event_date
                - criteria
            Defaults to "case_notes_cte".
    """

    return f"""    SELECT
      external_id,
      -- Group all notes into an array within a JSON
      TO_JSON(ARRAY_AGG( STRUCT(note_title, note_body, event_date, criteria))) AS case_notes,
  FROM {from_cte}
  LEFT JOIN {left_join_cte}
    USING(external_id)
  WHERE criteria IS NOT NULL
  GROUP BY 1 """


def opportunity_query_final_select_with_case_notes(
    from_cte: str = "eligible_and_almost_eligible",
    left_join_cte: str = "array_case_notes_cte",
) -> str:
    """The final CTE usually found in opportunity/form queries.

    Args:
        from_cte (str, optional): Usually the view that contains eligible and almost
            eligible client list. Defaults to "eligible_and_almost_eligible".
        left_join_cte (str, optional): Usually the CTE containing all the case notes aggregated
            in a JSON. Defaults to "array_case_notes_cte".
    """
    return f"""    SELECT
        external_id,
        state_code,
        reasons,
        ineligible_criteria,
        case_notes,
    FROM {from_cte}
    LEFT JOIN {left_join_cte}
    USING(external_id)
  """


def spans_within_x_and_y_months_of_start_date(
    x_months: int,
    y_months: int,
    start_date_plus_x_months_name_in_reason_blob: str,
    table_view: str,
    dataset: str,
    project_id: str = "project_id",
    where_clause_additions: Optional[List[str]] = None,
) -> str:
    """
    Returns a SQL query that returns spans of time where someone is between |x_months| and
    |y_months| of the start_date of a True (meets_criteria=True) span from the |table_view|.

    Args:
        x_months (int): Number of months to add to the start_date.
        y_months (int): Number of months to add to the start_date. If None, start_date_plus_y_months
            will be set to '9999-12-31'.
        start_date_plus_x_months_name_in_reason_blob (str): Name of the start_date_plus_x_months field in the reason blob.
        table_view (str): Name of the table or view to query.
        dataset (str): BigQuery dataset.
        project_id (str): Project id. Defaults to 'project_id'
        where_clause_additions Optional[List[str]]: Optional additional WHERE clauses to add to the first CTE.

    Returns:
        str: SQL query as a string.

    Example usage:
        query = spans_within_x_and_y_months_of_start_date(3, 6, "start_date_plus_3_months", "my_table_view")
    """

    if y_months is None:
        start_date_plus_y_months = "CAST('9999-12-31' AS DATE)"
    else:
        start_date_plus_y_months = f"DATE_ADD(start_date, INTERVAL {y_months} MONTH)"

    where_clause_additions_sql: str = ""
    if where_clause_additions:
        where_clause_additions_sql = "AND " + " AND ".join(list(where_clause_additions))

    return f"""cte AS (
        SELECT
            state_code,
            person_id,
            DATE_ADD(start_date, INTERVAL {x_months} MONTH) AS start_date_plus_x_months,
            {start_date_plus_y_months} AS start_date_plus_y_months,
            {nonnull_end_date_clause('end_date')} AS end_date,
        FROM `{{{project_id}}}.{{{dataset}}}.{table_view}`
        WHERE meets_criteria=True
            {where_clause_additions_sql}
    )

SELECT
    state_code,
    person_id,
    start_date_plus_x_months AS start_date,
    {revert_nonnull_end_date_clause('LEAST(start_date_plus_y_months, end_date)')} AS end_date,
    TRUE AS meets_criteria,
    TO_JSON(STRUCT(start_date_plus_x_months AS {start_date_plus_x_months_name_in_reason_blob})) AS reason
FROM cte
WHERE cte.end_date > start_date_plus_x_months"""


def spans_within_x_and_y_months_of_end_date(
    x_months: int,
    y_months: int,
    end_date_plus_x_months_name_in_reason_blob: str,
    table_view: str,
    dataset: str,
    project_id: str = "project_id",
) -> str:
    """
    Returns a SQL query that returns spans of time where someone is between |x_months| and
    |y_months| of the end_date of a False (meets_criteria=False) span from the |table_view|.

    Args:
        x_months (int): Number of months to add to the end_date.
        y_months (int): Number of months to add to the end_date.
        end_date_plus_x_months_name_in_reason_blob (str): Name of the end_date_plus_x_months field in the reason blob.
        table_view (str): Name of the table or view to query.
        dataset (str): BigQuery dataset.
        project_id (str): Project id. Defaults to 'project_id'

    Returns:
        str: SQL query as a string.

    Example usage:
        query = spans_within_x_and_y_months_of_end_date(3, 6, "end_date_plus_3_months", "my_table_view")
    """

    return f"""cte AS (
        SELECT
            state_code,
            person_id,
            LEAD(start_date) OVER(PARTITION BY state_code, person_id 
                                  ORDER BY {nonnull_end_date_clause('end_date')})
                AS lead_start_date,
            # TODO(#23420): Use end_date exclusive instead of end_date
            DATE_ADD({nonnull_end_date_clause('end_date')}, INTERVAL {x_months} MONTH) AS end_date_plus_x_months,
            DATE_ADD({nonnull_end_date_clause('end_date')}, INTERVAL {y_months} MONTH) AS end_date_plus_y_months,
        FROM `{{{project_id}}}.{{{dataset}}}.{table_view}`
        WHERE meets_criteria=False
    )

SELECT
    state_code,
    person_id,
    end_date_plus_x_months AS start_date,
    LEAST(end_date_plus_y_months,
          {nonnull_end_date_clause('lead_start_date')}) AS end_date,
    TRUE AS meets_criteria,
    TO_JSON(STRUCT(end_date_plus_x_months AS {end_date_plus_x_months_name_in_reason_blob})) AS reason
FROM cte
WHERE {nonnull_end_date_clause('lead_start_date')} > end_date_plus_x_months"""
