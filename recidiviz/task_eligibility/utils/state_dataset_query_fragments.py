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
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.common.constants.state.state_task_deadline import StateTaskType

VIOLATIONS_FOUND_WHERE_CLAUSE = """WHERE (v.state_code != 'US_ME' OR
       # In ME, convictions are only relevant if their outcome is VIOLATION FOUND
       response_type IN ("VIOLATION_REPORT", "PERMANENT_DECISION"))
"""

INCARCERATION_INCIDENTS_FOUND_WHERE_CLAUSE = """WHERE state_code !='US_TN' OR
        # In TN, we only want to count incidents where disciplinary class is not null or they have a pending disposition
        ((incident_class !="" OR hearing_date IS NULL) AND IncidentID IS NOT NULL)
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

    return f"""
    SELECT
        vr.state_code,
        vr.person_id,
        COALESCE(v.violation_date, vr.response_date) AS start_date,
        DATE_ADD(COALESCE(v.violation_date, vr.response_date), INTERVAL {date_interval} {date_part}) AS end_date,
        COALESCE(v.violation_date, vr.response_date) AS violation_date,
        {bool_column}
    FROM `{{project_id}}.{{normalized_state_dataset}}.state_supervision_violation_response` vr
    INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_supervision_violation_type_entry` vt
        ON vr.supervision_violation_id = vt.supervision_violation_id
        AND vr.person_id = vt.person_id
        AND vr.state_code = vt.state_code
        {violation_type}
    LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_supervision_violation` v
        ON vr.supervision_violation_id = v.supervision_violation_id
        AND vr.person_id = v.person_id
        AND vr.state_code = v.state_code
    {where_clause}
    """


def incident_sessions(
    date_interval: int = 12,
    date_part: str = "MONTH",
    where_clause: str = "",
) -> str:
    """
    <<<<<<< HEAD
        Args:
            date_interval (int, optional): Number of <date_part> when the incident
                will be counted as valid. Defaults to 6 (e.g. it could be 6 months).
            date_part (str, optional): Supports any of the BigQuery date_part values:
                "DAY", "WEEK","MONTH","QUARTER","YEAR". Defaults to "MONTH".
            where_clause (str, optional): Optional clause that does some state-specific filtering. Defaults to ''.
        Returns:
            f-string: CTE used in later functions
    """
    return f"""
        SELECT
            state_code,
            person_id,
            incident_date AS start_date,
            DATE_ADD(incident_date, INTERVAL {date_interval} {date_part}) AS end_date,
            incident_date AS latest_incident_date,
            FALSE AS meets_criteria,
        FROM (
          SELECT inc.person_id,
                inc.state_code,
                inc.incident_date,
                inc_outcome.hearing_date,
                JSON_EXTRACT_SCALAR(inc.incident_metadata, "$.Class") AS incident_class,
                IncidentID,
          FROM `{{project_id}}.{{normalized_state_dataset}}.state_incarceration_incident` inc
          LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_incarceration_incident_outcome` inc_outcome
            USING(incarceration_incident_id)
          INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
            ON inc.person_id = pei.person_id
            AND inc.state_code = pei.state_code
          -- TODO(#20693): Remove hack when entity deletion exists
          LEFT JOIN `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.Disciplinary_latest` disc
            ON pei.external_id = disc.OffenderID
            AND SPLIT(inc.external_id,'-')[SAFE_OFFSET(1)] = disc.IncidentID
        )
        {where_clause}
    """


def has_at_least_x_incarceration_incidents_in_time_interval(
    number_of_incidents: int = 1,
    date_interval: int = 12,
    date_part: str = "MONTH",
    where_clause: str = "",
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
        {incident_sessions(date_interval=date_interval, date_part=date_part, where_clause=where_clause)}
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


def get_current_offenses() -> str:
    """
    Returns: CTE that pulls information on currently active sentences
    """

    return """
    SELECT
          s.person_id,
          s.state_code,
          s.start_date,
          sentences.county_code AS conviction_county,
          JSON_EXTRACT_SCALAR(sentences.sentence_metadata, '$.CASE_NUMBER') AS docket_number,
          sentences.description AS offense
      FROM (
        SELECT *
        FROM `{project_id}.{sessions_dataset}.sentence_spans_materialized`
        QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY start_date DESC) = 1
      ) s,
      UNNEST(sentences_preprocessed_id_array) as sentences_preprocessed_id
      LEFT JOIN `{project_id}.{sessions_dataset}.sentences_preprocessed_materialized` sentences
        USING(person_id, state_code, sentences_preprocessed_id)
    """
