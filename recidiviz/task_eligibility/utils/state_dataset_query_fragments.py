# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
normalized_state dataset.
"""
from typing import List, Optional
from recidiviz.calculator.query.bq_utils import (
    nonnull_end_date_clause,
    nonnull_end_date_exclusive_clause,
    revert_nonnull_end_date_clause,
)
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.common.constants.state.state_task_deadline import StateTaskType

VIOLATIONS_FOUND_WHERE_CLAUSE = """WHERE (v.state_code != 'US_ME' OR
       # In ME, convictions are only relevant if their outcome is VIOLATION FOUND
       response_type IN ("VIOLATION_REPORT", "PERMANENT_DECISION"))
"""


def task_deadline_critical_date_update_datetimes_cte(
    task_type: StateTaskType, critical_date_column: str
) -> str:
    """Returns a CTE that selects all StateTaskDeadline rows with the provided
    |task_type] and renames the |critical_date_column| to `critical_date` for standard
    processing.
    """
    if critical_date_column not in {"eligible_date", "due_date"}:
        raise ValueError(f"Unsupported critical date column {critical_date_column}")
    return f"""critical_date_update_datetimes AS (
        SELECT
            state_code,
            person_id,
            {critical_date_column} AS critical_date,
            update_datetime
        FROM `{{project_id}}.{{normalized_state_dataset}}.state_task_deadline`
        WHERE task_type = '{task_type.value}'
    )"""


def violations_within_time_interval_cte(
    violation_type: str = "",
    where_clause: str = "",
    bool_column: str = "False AS meets_criteria,",
    date_interval: int = 12,
    date_part: str = "MONTH",
) -> str:
    """
    Args:
        violation_type (str, optional): Specifies the violation types that should be
            counted towards the criteria. Should only include values inside of the
            StateSupervisionViolationType enum. Example: "AND vt.violation_type = 'FELONY' "
            Defaults to ''.
        where_clause (str, optional): _description_. Defaults to ''.
        bool_column (str, optional): _description_. Defaults to "False AS meets_criteria,".
        date_interval (int, optional): Number of <date_part> when the violation
            will be counted as valid. Defaults to 12 (e.g. it could be 12 months).
        date_part (str, optional): Supports any of the BigQuery date_part values:
            "DAY", "WEEK","MONTH","QUARTER","YEAR". Defaults to "MONTH".

    Returns:
        f-string: CTE query that shows the spans of time where the violations that meet
            certain conditions set by the user (<violaiton_type> and <where clause>).
            The span of time for the validity of each violation starts at violation_date
            and ends after a period specified by the user (in <date_interval> and <date_part>)
    """

    violation_type_join = f"""
    INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_supervision_violation_type_entry` vt
        ON vr.supervision_violation_id = vt.supervision_violation_id
        AND vr.person_id = vt.person_id
        AND vr.state_code = vt.state_code
        {violation_type}
    """

    return f"""
    SELECT
        vr.state_code,
        vr.person_id,
        COALESCE(v.violation_date, vr.response_date) AS start_date,
        DATE_ADD(COALESCE(v.violation_date, vr.response_date), INTERVAL {date_interval} {date_part}) AS end_date,
        COALESCE(v.violation_date, vr.response_date) AS violation_date,
        {bool_column}
    FROM `{{project_id}}.{{normalized_state_dataset}}.state_supervision_violation_response` vr
    {violation_type_join if violation_type else ""}
    LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_supervision_violation` v
        ON vr.supervision_violation_id = v.supervision_violation_id
        AND vr.person_id = v.person_id
        AND vr.state_code = v.state_code
    {where_clause}
    """


def has_at_least_x_incarceration_incidents_in_time_interval(
    number_of_incidents: int = 1,
    date_interval: int = 12,
    date_part: str = "MONTH",
) -> str:
    """
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
    Returns: CTE that pulls information on sentences from the current sentence span. The where clause determines
     whether we keep the intersection of sentences in sentences_preprocessed_id_array_actual_completion AND
     sentences_preprocessed_id_array_projected_completion or only in  sentences_preprocessed_id_array_actual_completion.
    """

    where_clause = ""
    if in_projected_completion_array:
        where_clause = "WHERE sentences_preprocessed_id in UNNEST(sentences_preprocessed_id_array_projected_completion)"

    return f"""
    SELECT
          s.person_id,
          s.state_code,
          s.start_date,
          sentences.county_code AS conviction_county,
          JSON_EXTRACT_SCALAR(sentences.sentence_metadata, '$.CASE_NUMBER') AS docket_number,
          sentences.description AS offense,
          sentences.judicial_district,
          sentences.date_imposed AS sentence_start_date,
          sentences.status,
          sentences.projected_completion_date_max AS expiration_date,
      FROM (
        SELECT *
        FROM `{{project_id}}.{{sessions_dataset}}.sentence_spans_materialized`
        WHERE CURRENT_DATE('US/Pacific') BETWEEN start_date AND {nonnull_end_date_exclusive_clause('end_date')}
      ) s,
      UNNEST(sentences_preprocessed_id_array_actual_completion) as sentences_preprocessed_id
      INNER JOIN `{{project_id}}.{{sessions_dataset}}.sentences_preprocessed_materialized` sentences
        USING(person_id, state_code, sentences_preprocessed_id)
      {where_clause}
    """


def x_time_from_ineligible_offense(
    statutes_list: list,
    date_part: str,
    date_interval: int,
    additional_where_clause: str = "",
    start_date_column: str = "effective_date",
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

    return f"""ineligible_sentences AS (
    SELECT 
        state_code,
        person_id, 
        {start_date_column} AS start_date,
        DATE_ADD({start_date_column}, INTERVAL {date_interval} {date_part}) AS end_date,
        statute,
        {start_date_column},
        description
    FROM `{{project_id}}.{{sessions_dataset}}.sentences_preprocessed_materialized` 
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
  TO_JSON(STRUCT(ARRAY_AGG(DISTINCT statute) AS ineligible_offenses,
                 ARRAY_AGG(DISTINCT description) AS ineligible_offenses_descriptions,
                 MAX({start_date_column}) AS most_recent_statute_date)) AS reason,
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
        json_column (str): The name of the JSON column to extract from. Defaults to
            "reason".

    Returns:
        str: A formatted BigQuery SQL query that extracts an object from a JSON column.
    """

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
