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
"""Helper SQL fragments that do standard queries against tables in the
`normalized_state` dataset.
"""

from typing import List, Optional

from recidiviz.calculator.query.bq_utils import (
    date_diff_in_full_months,
    nonnull_end_date_clause,
    nonnull_end_date_exclusive_clause,
    revert_nonnull_end_date_clause,
)
from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_sub_sessions_with_attributes,
)
from recidiviz.common.constants.state.state_task_deadline import StateTaskType
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
)


def task_deadline_critical_date_update_datetimes_cte(
    task_type: StateTaskType,
    critical_date_column: str,
    additional_where_clause: str = "",
    round_up_datetime: Optional[bool] = True,
) -> str:
    """Returns a CTE that selects all StateTaskDeadline rows with the provided
    |task_type] and renames the |critical_date_column| to `critical_date` for standard
    processing.
    """
    if critical_date_column not in {"eligible_date", "due_date"}:
        raise ValueError(f"Unsupported critical date column {critical_date_column}")

    if round_up_datetime:
        datetime_str = (
            "CAST(DATE_ADD(update_datetime, INTERVAL 1 DAY) AS DATE) AS update_datetime"
        )
    else:
        datetime_str = "CAST(update_datetime AS DATE) AS update_datetime"

    return f"""critical_date_update_datetimes AS (
        SELECT
            state_code,
            person_id,
            {critical_date_column} AS critical_date,
            {datetime_str}
        FROM `{{project_id}}.{{normalized_state_dataset}}.state_task_deadline`
        WHERE task_type = '{task_type.value}' {additional_where_clause}
    )"""


def has_at_least_x_incarceration_incidents_in_time_interval(
    number_of_incidents: int = 1,
    date_interval: int = 12,
    date_part: str = "MONTH",
) -> str:
    """
    Determines if individuals have at least a specified number of incarceration incidents
    within a given time interval.

    Args:
        number_of_incidents: Number of incidents tests needed within time interval
        date_interval (int, optional): Number of <date_part> when the negative drug screen
            will be counted as valid. Defaults to 12 (e.g. it could be 12 months).
        date_part (str, optional): Supports any of the BigQuery date_part values:
            "DAY", "WEEK","MONTH","QUARTER","YEAR". Defaults to "MONTH".
        where_clause (str, optional): Optional clause that does some state-specific filtering. Defaults to ''.
    Returns:
        f-string: Spans of time where the criteria is met
    """

    return f"""
    WITH incident_sessions AS (
        SELECT
            state_code,
            person_id,
            incident_date AS start_date,
            DATE_ADD(incident_date, INTERVAL {date_interval} {date_part}) AS end_date,
            incident_date AS latest_incident_date,
            FALSE AS meets_criteria,
        FROM 
            `{{project_id}}.{{analyst_dataset}}.incarceration_incidents_preprocessed_materialized`
    )
    ,
    {create_sub_sessions_with_attributes('incident_sessions')},
    grouped AS (
        SELECT
            state_code,
            person_id,
            start_date,
            end_date,
            count(*) AS num_incidents_within_timeframe,
            MAX(latest_incident_date) AS latest_incident_date
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
        num_incidents_within_timeframe >= {number_of_incidents} AS meets_criteria,
        TO_JSON(STRUCT(latest_incident_date AS latest_incarceration_incident_date)) AS reason
    FROM
        grouped
    """


def get_sentences_current_span(in_projected_completion_array: bool = True) -> str:
    """
    Pull information on sentences from the current sentence span.

    Args:
        in_projected_completion_array (bool, optional): Determines whether we keep the
            sentences in the intersection of
            `sentences_preprocessed_id_array_actual_completion` +
            `sentences_preprocessed_id_array_projected_completion` vs. just sentences
            from `sentences_preprocessed_id_array_actual_completion`. Defaults to True,
            such that only sentences in the intersection are included.

    Returns:
        str: SQL query as a string.
    """

    where_clause = ""
    if in_projected_completion_array:
        where_clause = "WHERE sentences_preprocessed_id IN UNNEST(sentences_preprocessed_id_array_projected_completion)"

    return f"""
    SELECT
        s.person_id,
        s.state_code,
        s.start_date,
        sentences.county_code AS conviction_county,
        JSON_EXTRACT_SCALAR(sentences.sentence_metadata, '$.CASE_NUMBER') AS docket_number,
        sentences.description AS offense,
        sentences.judicial_district,
        sentences.life_sentence,
        sentences.date_imposed AS sentence_start_date,
        sentences.status,
        sentences.projected_completion_date_max AS expiration_date,
    FROM (
        SELECT *
        FROM `{{project_id}}.{{sessions_dataset}}.sentence_spans_materialized`
        WHERE CURRENT_DATE('US/Pacific') BETWEEN start_date AND {nonnull_end_date_exclusive_clause('end_date_exclusive')}
    ) s,
    UNNEST(sentences_preprocessed_id_array_actual_completion) AS sentences_preprocessed_id
    INNER JOIN `{{project_id}}.{{sessions_dataset}}.sentences_preprocessed_materialized` sentences
        USING (person_id, state_code, sentences_preprocessed_id)
    {where_clause}
    """


def x_time_from_ineligible_offense(
    statutes_list: list,
    date_part: str,
    date_interval: int,
    additional_where_clause: str = "",
    start_date_column: str = "effective_date",
    statute_date_name: str = "most_recent_statute_date",
) -> str:
    """
    Generate a BigQuery SQL query to identify time spans where individuals
    are ineligible for an opportunity due to ineligible charges/statutes.

    Args:
        statutes_list (list): A list of statutes to check for ineligibility.
        date_part (str): The unit of time for the date interval, e.g., 'DAY', 'MONTH', 'YEAR'.
        date_interval (int): The duration of the interval to add to start_date.
        additional_where_clause (str): An optional clause to add to the WHERE clause of
            the query.


    Returns:
        str: A formatted BigQuery SQL query that identifies time spans without ineligible statutes.

    Example usage:
        query = x_time_without_ineligible_statute(statutes_list=['statute1', 'statute2'],
                                                  date_part='DAY', date_interval=30)
    """

    statutes_string = "('" + "', '".join(statutes_list) + "')"

    return f"""WITH ineligible_sentences AS (
    SELECT 
        state_code,
        person_id, 
        {start_date_column} AS start_date,
        DATE_ADD({start_date_column}, INTERVAL {date_interval} {date_part}) AS end_date,
        statute,
        {start_date_column},
        description
    FROM `{{project_id}}.sentence_sessions.sentences_and_charges_materialized`
    WHERE statute IS NOT NULL
        AND statute IN {statutes_string}
        AND {start_date_column} IS NOT NULL
        AND {start_date_column} NOT IN ('9999-12-31', '9999-12-20')
        {additional_where_clause}
),

{create_sub_sessions_with_attributes('ineligible_sentences')}

SELECT 
    state_code, 
    person_id,
    start_date, 
    end_date,
    FALSE AS meets_criteria,
    TO_JSON(STRUCT(
        ARRAY_AGG(DISTINCT statute ORDER BY statute) AS ineligible_offenses,
        ARRAY_AGG(DISTINCT description ORDER BY description) AS ineligible_offenses_descriptions,
        MAX({start_date_column}) AS {statute_date_name}
    )) AS reason,
    ARRAY_AGG(DISTINCT statute ORDER BY statute) AS ineligible_offenses,
    ARRAY_AGG(DISTINCT description ORDER BY description) AS ineligible_offenses_descriptions,
    MAX({start_date_column}) AS {statute_date_name},
FROM sub_sessions_with_attributes
GROUP BY 1,2,3,4,5"""


def extract_object_from_json(
    object_column: str, object_type: str, json_column: str = "reason"
) -> str:
    """
    Extracts an object from a JSON column. E.g. this is useful for extracting dates from
    the reason column of a task eligibility view.

    Args:
        object_column (str): The name of the column in the BigQuery table to extract.
        object_type (str): The type of the object we want as output. E.g. 'STRING', 'DATE'.
            TODO(#31856) switch the |object_type| argument type from `str` to `StandardSqlTypeNames`
        json_column (str): The name of the JSON column to extract from. Defaults to
            "reason".

    Returns:
        str: A formatted BigQuery SQL query that extracts an object from a JSON column.
    """
    if object_type == "ARRAY":
        return f"""JSON_VALUE_ARRAY({json_column}, '$.{object_column}')"""

    return f"""SAFE_CAST(JSON_VALUE({json_column}, '$.{object_column}') AS {object_type})"""


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
    TO_JSON(STRUCT(start_date_plus_x_months AS {start_date_plus_x_months_name_in_reason_blob})) AS reason,
    start_date_plus_x_months AS {start_date_plus_x_months_name_in_reason_blob},
FROM cte
WHERE cte.end_date > start_date_plus_x_months"""


def no_supervision_violation_within_x_to_y_months_of_start(
    x_months: int, y_months: int
) -> str:
    """
    Defines a criteria span view that shows spans of time during which there
    is no supervision violation within x to y months. If x_months is 6 and y_months is 8,
    then the criteria is met if there is no violation within 6 to 8 months of
    the probation/parole start date (or the latest violation).
    """

    return f"""
WITH violations AS (
  -- All violations and violation responses
  SELECT
      vr.state_code,
      vr.person_id,
      COALESCE(v.violation_date, vr.response_date) AS violation_date,
  FROM `{{project_id}}.{{normalized_state_dataset}}.state_supervision_violation_response` vr
  LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_supervision_violation` v
      ON vr.supervision_violation_id = v.supervision_violation_id
      AND vr.person_id = v.person_id
      AND vr.state_code = v.state_code
),
probation_and_parole_sessions AS (
  -- Compartment sessions for probation, parole or DUAL
  SELECT 
    state_code,
    person_id,
    start_date,
    end_date,
    compartment_level_1,
    compartment_level_2,
  FROM `{{project_id}}.{{sessions_dataset}}.compartment_sessions_materialized`
  WHERE compartment_level_1 = 'SUPERVISION'
    AND compartment_level_2 IN ('PAROLE', 'PROBATION', 'DUAL')
  ORDER BY 1,2,3
),
probation_and_parole_sessions_agg AS (
    -- Aggregate adjacent probation and parole sessions. This means if a probation 
    --      session ends and it is immediately followed by a parole session, we don't 
    --      restart the clock for violations.
    SELECT *,
    FROM ({aggregate_adjacent_spans(table_name='probation_and_parole_sessions',
                                    end_date_field_name="end_date")})
),
critical_date_spans AS (
    -- Combine previous CTEs and calculate critical date as x months after start date
    --      or violation date, whichever is later.
    SELECT 
        pps.state_code,
        pps.person_id,
        pps.start_date AS start_datetime,
        {nonnull_end_date_clause('pps.end_date')} AS end_datetime,
        v.violation_date AS violation_date,
        DATE_ADD(IFNULL(v.violation_date, pps.start_date),
                INTERVAL {x_months} MONTH) AS critical_date,
    FROM probation_and_parole_sessions_agg pps
    LEFT JOIN violations v
        ON v.state_code = pps.state_code
            AND v.person_id = pps.person_id
            AND v.violation_date BETWEEN pps.start_date AND {nonnull_end_date_clause('pps.end_date')}
),
{critical_date_has_passed_spans_cte(attributes = ['violation_date'])},

{create_sub_sessions_with_attributes(
    table_name="critical_date_has_passed_spans",  
)},

deduped_sub_sessions_with_attributes AS (
    -- Dedupe sub-sessions with attributes. If a person has multiple sub-sessions,
    --      only if all of them have critical_date_has_passed = True, then the person
    --      meets the criteria. We also store the last violation date for the reason blob.
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date,
        LOGICAL_AND(critical_date_has_passed) AS meets_criteria,
        MAX(violation_date) AS last_violation_date,
    FROM sub_sessions_with_attributes
    GROUP BY state_code, person_id, start_date, end_date
)
-- Only surface folks as eligible for Y months after X months of the last violation date
--      or start date.
SELECT 
    * EXCEPT(end_date, last_violation_date),
    LEAST(
        DATE_ADD(start_date, INTERVAL {y_months-x_months} MONTH),
        {nonnull_end_date_clause('end_date')}
    ) AS end_date,
    TO_JSON(STRUCT(last_violation_date AS last_violation_date)) AS reason,
    last_violation_date,
FROM deduped_sub_sessions_with_attributes
WHERE meets_criteria
"""


# TODO(#35405): Turn this into a general criteria builder, rather than string output
def no_supervision_sanction_within_time_interval(
    *,
    date_interval: int,
    date_part: str,
    supervision_sanctions_cte: str,
) -> str:
    """Identify spans of time during which individuals in TN have NOT had a supervision sanction
    within a specified time interval (e.g., within the past 2 years).

    Args:
        date_interval (int): Number of <date_part> when a sanction will
        remain valid/relevant.
        date_part (str): Supports any of the BigQuery `date_part` values: "DAY", "WEEK",
            "MONTH", "QUARTER", or "YEAR".
        supervision_sanctions_cte: Any state/context specific configuration or filtering of sanctions.
            Expected output should have state_code, person_id, and sanction_date
    Returns:
        str: SQL query as a string.
    """

    return f"""
    WITH supervision_sanctions AS (
        SELECT
            state_code,
            person_id,
            sanction_date,
            sanction_date AS start_date,            
            /* For our selected violation responses, we create spans of *ineligibility*
            by setting span start and end dates here, covering the periods of time
            during which someone will not meet this criterion because the response in
            question remains relevant/valid. */
            DATE_ADD(sanction_date, INTERVAL {date_interval} {date_part}) AS sanction_expiration_date,
            DATE_ADD(sanction_date, INTERVAL {date_interval} {date_part}) AS end_date,
            FALSE AS meets_criteria,
        FROM (
            {supervision_sanctions_cte}
        )
    ),
    /* Sub-sessionize in case there are overlapping spans (i.e., if someone has multiple
    still-relevant sanctions at once). */
    {create_sub_sessions_with_attributes('supervision_sanctions')}
    SELECT
        state_code,
        person_id,
        start_date,
        end_date,
        LOGICAL_AND(meets_criteria) AS meets_criteria,
        TO_JSON(STRUCT(
            ARRAY_AGG(sanction_date IGNORE NULLS ORDER BY sanction_date DESC) AS sanction_dates,
            MAX(sanction_expiration_date) AS latest_sanction_expiration_date
        )) AS reason,
        ARRAY_AGG(sanction_date IGNORE NULLS ORDER BY sanction_date DESC) AS sanction_dates,
        MAX(sanction_expiration_date) AS latest_sanction_expiration_date,
    FROM sub_sessions_with_attributes
    GROUP BY 1, 2, 3, 4
    """


def sentence_attributes() -> str:
    """
    Get time span and other critical attributes for each sentence.

    Returns:
        str: SQL query as a string.
    """

    return f"""
    SELECT DISTINCT
        state_code,
        person_id,
        sentence_id,
        external_id,
        sentence_type,
        charge_id,
        offense_date,
        date_imposed,
        statute,
        classification_type,
        county_code,
        sentence_metadata,
        max_sentence_length_days_calculated,
        effective_date AS start_date,
        IFNULL(completion_date, {nonnull_end_date_clause('projected_completion_date_max')}) AS end_date,
    FROM `{{project_id}}.{{sessions_dataset}}.sentences_preprocessed_materialized`
    -- drop zero-day sentences (which, at least in OR, come from underlying raw data)
    WHERE effective_date<IFNULL(completion_date, {nonnull_end_date_clause('projected_completion_date_max')})
    """


def participated_in_programming_for_X_to_Y_months(x_months: int, y_months: int) -> str:
    """
    Returns a query which returns spans of time where someone has participated in any
    program for X to Y months. Example: if x_months is 6 and y_months is 8, then the
    criteria is met if there is a span of time where someone has participated in any
    program for 6 to 8 months.

    Args:
        x_months (int): Number of months to add to the start_date.
        y_months (int): Number of months to add to the start_date.

    Returns:
        str: SQL query as a string.
    """
    return f"""
WITH program_assignments_ongoing_for_X_months AS (
  SELECT
      pa.state_code,
      pa.person_id,
      pa.start_date as program_start_date,
      pa.discharge_date as program_end_date,
      pa.program_id
  FROM `{{project_id}}.{{normalized_state_dataset}}.state_program_assignment` pa
  WHERE {date_diff_in_full_months("IFNULL(pa.discharge_date, CURRENT_DATE('US/Pacific'))", 'pa.start_date')} BETWEEN {x_months} AND {y_months}
),
probation_and_parole_sessions AS (
  -- Compartment sessions for probation, parole or DUAL
  SELECT 
    state_code,
    person_id,
    start_date,
    end_date,
    compartment_level_1,
    compartment_level_2,
  FROM `{{project_id}}.{{sessions_dataset}}.compartment_sessions_materialized`
  WHERE compartment_level_1 = 'SUPERVISION'
    AND compartment_level_2 IN ('PAROLE', 'PROBATION', 'DUAL')
  ORDER BY 1,2,3
),
probation_and_parole_sessions_agg AS (
    -- Aggregate adjacent probation and parole sessions. This means if a probation 
    --      session ends and it is immediately followed by a parole session, we don't 
    --      restart the clock for violations.
    SELECT *,
    FROM ({aggregate_adjacent_spans(table_name='probation_and_parole_sessions',
                                    end_date_field_name="end_date")})
),
critical_date_spans AS (
    -- Combine previous CTEs and calculate critical date as x months after start date
    --      or violation date, whichever is later.
    SELECT 
        pps.state_code,
        pps.person_id,
        pps.start_date AS start_datetime,
        LEAST(
            {nonnull_end_date_clause('pps.end_date')},
            {nonnull_end_date_clause('pa.program_end_date')}
        ) AS end_datetime,
        DATE_ADD(pa.program_start_date, INTERVAL {x_months} MONTH) AS critical_date,
        pa.program_start_date,
        pa.program_end_date,
        pa.program_id
    FROM probation_and_parole_sessions_agg pps
    -- This join ensures we box the programming days within known periods of
    -- supervision.  This means if someone is returned to incarceration, their
    -- programming will be ended as far as this milestone goes. There is an open UXR
    -- question about if this is a correct assumption.
    LEFT JOIN program_assignments_ongoing_for_X_months pa
        ON pa.state_code = pps.state_code
            AND pa.person_id = pps.person_id
            AND pa.program_start_date BETWEEN pps.start_date AND {nonnull_end_date_clause('pps.end_date')}
            -- Join in any programs that are on-going or completed during a valid period of supervision
            AND (pa.program_end_date IS NULL OR pa.program_end_date BETWEEN pps.start_date AND {nonnull_end_date_clause('pps.end_date')})
),
{critical_date_has_passed_spans_cte(attributes = ['program_start_date', 'program_end_date', 'program_id'])},

-- We only need true spans -- if someone is participating in multiple programs,
-- this person meets this criteria as long they have participated in any of those
-- programs for X to Y months
only_true_critical_date_spans AS (
    SELECT * 
    FROM critical_date_has_passed_spans
    WHERE critical_date_has_passed
),

{create_sub_sessions_with_attributes(
    table_name="only_true_critical_date_spans",
)},

deduped_sub_sessions_with_attributes AS (
    -- Dedupe sub-sessions with attributes. If a person has multiple sub-sessions,
    --      only if all of them have critical_date_has_passed = True, then the person
    --      meets the criteria. We also store the last violation date for the reason blob.
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date,
        true AS meets_criteria, -- See only_true_critical_date_spans CTE.
        ARRAY_AGG(
            STRUCT(program_id, program_start_date, program_end_date)
            ORDER BY program_id, program_start_date, program_end_date
        ) AS programs
    FROM sub_sessions_with_attributes
    GROUP BY state_code, person_id, start_date, end_date
)
-- Only surface folks as eligible for Y months after X months of the last violation date
--      or start date.
SELECT 
    * EXCEPT(end_date, programs),
    LEAST(
        DATE_ADD(start_date, INTERVAL {y_months-x_months} MONTH),
        {nonnull_end_date_clause('end_date')}
    ) AS end_date,
    TO_JSON(programs) AS reason,
    programs,
FROM deduped_sub_sessions_with_attributes
    """


def combining_several_criteria_into_one(
    select_statements_for_criteria_lst: List[str],
    meets_criteria: str,
    json_content: str,
) -> str:
    """
    Returns a SQL query that combines several criteria into one view builder.

    Args:
        select_statements_for_criteria (List[str]): List of select statements for each criteria.

    Returns:
        str: SQL query as a string.
    """
    reformatted_select_statements_for_criteria = "\n\n    UNION ALL \n".join(
        select_statements_for_criteria_lst
    )

    query = f"""WITH x_criteria_cte AS ({reformatted_select_statements_for_criteria}
    ),

    {create_sub_sessions_with_attributes('x_criteria_cte')},

sub_sessions_with_count AS (
    SELECT 
        *,
        COUNT(*) OVER (PARTITION BY state_code, person_id, start_date, end_date) AS num_criteria,
    FROM sub_sessions_with_attributes
)

SELECT 
    state_code, 
    person_id,
    start_date, 
    end_date,
    {meets_criteria} AS meets_criteria,
    TO_JSON(STRUCT({json_content})) AS reason,
FROM sub_sessions_with_count
GROUP BY 1,2,3,4
    """

    return query


def supervision_violations_cte(
    violation_type: str = "",
    where_clause_addition: str = "",
    exclude_violation_unfounded_decisions: bool = False,
    use_response_date: bool = False,
) -> str:
    """
    Combine state_supervision_violation and state_supervision_violation_response to select violations that
    we want to count for supervision violation related criteria. Violations are filtered based on any
    specified violation-level attributes and any aggregated attributes of violation responses
    (since there can be multiple responses per violation).

    Args:
        exclude_violation_unfounded_decisions (bool): Only violations where the LATEST violation response
        DOES NOT contain a VIOLATION_UNFOUNDED decision, indicating that the violation is unfounded
        use_response_date (bool, optional): Whether to use the report date rather than the violation date when determining
            eligibility. Defaults to False

    Returns:
        str: SQL query as a string.
    """

    violation_type_join = f"""
        INNER JOIN `{{project_id}}.normalized_state.state_supervision_violation_type_entry` vt
            ON v.supervision_violation_id = vt.supervision_violation_id
            AND v.person_id = vt.person_id
            AND v.state_code = vt.state_code
            {violation_type}
        """

    violation_unfounded_decision_join = f"""
        INNER JOIN (
            {get_supervision_violations_sans_unfounded()}
            )
            USING(supervision_violation_id, person_id, state_code)
    """

    return f"""
        # TODO(#35354): Account for violation decisions when considering which violations
        # should disqualify someone from eligibility.    
        SELECT
            v.state_code,
            v.person_id,
            v.supervision_violation_id,
            /* If use_response_date is true, set the critical date as the response date.
            Otherwise, use the following logic: 
            If there is no `violation_date` value, we treat the earliest date of any
            response(s) as the violation date. (We take the MIN of `violation_date`
            below just because we have to aggregate the `violation_date` field in this
            expression, but note that because the violation-to-response ratio can be
            one-to-many, every row should have the same `violation_date`, and this
            therefore will just return the original `violation_date` value if there is
            one.) */
            IF({use_response_date}, MIN(vr.response_date), COALESCE(MIN(v.violation_date), MIN(vr.response_date))) AS violation_date,
        FROM `{{project_id}}.normalized_state.state_supervision_violation` v
        {violation_type_join if violation_type else ""}
        {violation_unfounded_decision_join if exclude_violation_unfounded_decisions else ""}
        /* NB: there can be multiple responses per violation, so this LEFT JOIN can
        create multiple rows per violation (where each row is a unique violation
        response). */
        LEFT JOIN `{{project_id}}.normalized_state.state_supervision_violation_response` vr
            ON v.supervision_violation_id = vr.supervision_violation_id
            AND v.person_id = vr.person_id
            AND v.state_code = vr.state_code
        /* NB: the WHERE clause is typically evaluated after the FROM clause but before
        GROUP BY and aggregation, per BigQuery documentation. This means that any
        filtering applied in this WHERE clause will apply to the pre-grouped data. */
        WHERE
            CASE v.state_code
                /* In ME, only violations with a `response_type` of 'PERMANENT_DECISION'
                or 'VIOLATION_REPORT' are considered to be violations that would ever
                impact a client's eligibility for any opportunity. From the set of ME
                violations & responses, then, we therefore drop rows without one of
                these specified response types. Note that this means that if a violation
                in ME did not have any response with one of these types, it will get
                entirely dropped (and will therefore not disqualify a client from
                eligibility). */
                WHEN 'US_ME' THEN vr.response_type IN ('PERMANENT_DECISION', 'VIOLATION_REPORT')
                ELSE TRUE
                END
            {where_clause_addition}
        /* Group by violation, so that we get one row per violation coming out of this
        CTE. */
        GROUP BY 1, 2, 3
    """


def get_supervision_violations_sans_unfounded() -> str:
    """
    Returns a query fragment that only contains violations where the LATEST violation response DOES NOT contain a
    VIOLATION_UNFOUNDED decision, indicating that the violation is unfounded.

    Returns:
        str: SQL query as a string.
    """
    return """
        WITH responses_at_latest AS (
        SELECT
          vr.state_code,
          vr.person_id,
          vr.supervision_violation_id,
          vr.supervision_violation_response_id
        FROM `{project_id}.normalized_state.state_supervision_violation_response` AS vr
        QUALIFY vr.response_date = MAX(vr.response_date)
            OVER (PARTITION BY vr.state_code, vr.person_id, vr.supervision_violation_id)
      ),
      decisions_for_latest AS (
        SELECT
          ra.state_code,
          ra.person_id,
          ra.supervision_violation_id,
          ARRAY_AGG(CONCAT(COALESCE(de.decision, ''), COALESCE(de.decision_raw_text, '')) ORDER BY CONCAT(COALESCE(de.decision, ''), COALESCE(de.decision_raw_text, ''))) AS latest_decisions
        FROM
          responses_at_latest AS ra
        JOIN `{project_id}.normalized_state.state_supervision_violation_response_decision_entry` AS de
            USING(state_code, person_id, supervision_violation_response_id)
        GROUP BY
          ra.state_code,
          ra.person_id,
          ra.supervision_violation_id
      )
        SELECT
          v.state_code,
          v.person_id,
          v.supervision_violation_id
        FROM `{project_id}.normalized_state.state_supervision_violation` AS v
        LEFT JOIN decisions_for_latest AS dfl
            USING(state_code, person_id, supervision_violation_id)
        WHERE
          NOT EXISTS (
            SELECT 1 FROM UNNEST(dfl.latest_decisions) AS dec
            /* for Iowa, use state-specific logic to define which decisions are considered unfounded. otherwise, 
               take all where the decision is VIOLATION_UNFOUNDED */ 
            WHERE CASE WHEN state_code = 'US_IA' THEN dec LIKE '%REINSTATE%' OR dec LIKE '%DISMISSED%'
              ELSE dec LIKE '%VIOLATION_UNFOUNDED%' END
          )
        GROUP BY
          v.state_code,
          v.person_id,
          v.supervision_violation_id
"""
