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
"""Helper methods that return criteria view builders with similar logic that
can be parameterized.
"""
from typing import List, Optional

from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import revert_nonnull_end_date_clause
from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_sub_sessions_with_attributes,
    join_sentence_spans_to_compartment_sessions,
)
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
    StateSpecificTaskCriteriaBigQueryViewBuilder,
    TaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
)


def raise_error_if_invalid_compartment_level_1_filter(
    compartment_level_1_filter: str,
) -> None:
    """Raises a ValueError if the compartment_level_1_filter is not valid"""

    compartment_level_1 = compartment_level_1_filter.upper()

    if compartment_level_1 not in ("SUPERVISION", "INCARCERATION"):
        raise ValueError(
            "'compartment_level_1_filter' only accepts two values: `SUPERVISION` or `INCARCERATION`"
        )


def get_ineligible_offense_type_criteria(
    criteria_name: str,
    compartment_level_1: str,
    description: str,
    where_clause: str = "",
    additional_json_fields: Optional[List[str]] = None,
) -> StateAgnosticTaskCriteriaBigQueryViewBuilder:
    """Returns a state-agnostic criteria view builder indicating the spans of time when a person is
    serving a sentence of a particular type.
    """
    additional_json_fields_str = ""
    if additional_json_fields:
        additional_json_fields_str = ", " + ", ".join(additional_json_fields)
    criteria_query = f"""
    SELECT
        span.state_code,
        span.person_id,
        span.start_date,
        span.end_date,
        FALSE AS meets_criteria,
        TO_JSON(STRUCT(ARRAY_AGG(DISTINCT statute) AS ineligible_offenses{additional_json_fields_str})) AS reason,
        ARRAY_AGG(DISTINCT statute) AS ineligible_offenses{additional_json_fields_str}
    {join_sentence_spans_to_compartment_sessions(compartment_level_1_to_overlap=compartment_level_1)}
    {where_clause}
    GROUP BY 1,2,3,4,5
    """

    return StateAgnosticTaskCriteriaBigQueryViewBuilder(
        criteria_name=criteria_name,
        criteria_spans_query_template=criteria_query,
        description=description,
        sessions_dataset=SESSIONS_DATASET,
        meets_criteria_default=True,
        reasons_fields=[
            ReasonsField(
                name="ineligible_offenses",
                type=bigquery.enums.StandardSqlTypeNames.ARRAY,
                description="List of offenses that make this person ineligible",
            ),
        ],
    )


def get_minimum_age_criteria(
    minimum_age: int,
) -> StateAgnosticTaskCriteriaBigQueryViewBuilder:
    """Returns a state agnostic criteria view builder indicating the spans of time when a person is
    |minimum_age| years or older
    """
    criteria_name = f"AGE_{minimum_age}_YEARS_OR_OLDER"

    criteria_description = f"""Defines a criteria span view that shows spans of time during which someone
     is {minimum_age} years old or older"""

    criteria_query = f"""
    SELECT
        state_code,
        person_id,
        DATE_ADD(birthdate, INTERVAL {minimum_age} YEAR) AS start_date,
        CAST(NULL AS DATE) AS end_date,
        TRUE AS meets_criteria,
        TO_JSON(STRUCT(
            DATE_ADD(birthdate, INTERVAL {minimum_age} YEAR) AS eligible_date
        )) AS reason,
        birthdate,
        DATE_ADD(birthdate, INTERVAL {minimum_age} YEAR) AS age_eligible_date,
    FROM `{{project_id}}.{{sessions_dataset}}.person_demographics_materialized`
    -- Drop any erroneous birthdate values
    WHERE birthdate <= CURRENT_DATE("US/Eastern")
    """

    return StateAgnosticTaskCriteriaBigQueryViewBuilder(
        criteria_name=criteria_name,
        description=criteria_description,
        criteria_spans_query_template=criteria_query,
        reasons_fields=[
            ReasonsField(
                name="birthdate",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Client's birth date",
            ),
            ReasonsField(
                name="age_eligible_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Date when the client becomes eligible based on their age",
            ),
        ],
        sessions_dataset=SESSIONS_DATASET,
    )


def get_minimum_time_served_criteria_query(
    criteria_name: str,
    description: str,
    minimum_time_served: int,
    time_served_interval: str = "YEAR",
    compartment_level_0: str = "INCARCERATION",
    compartment_level_1_types: Optional[List[str]] = None,
    compartment_level_2_types: Optional[List[str]] = None,
    housing_unit_types: Optional[List[str]] = None,
    supervision_level_types: Optional[List[str]] = None,
) -> StateAgnosticTaskCriteriaBigQueryViewBuilder:
    """Returns a state agnostic criteria view builder indicating spans of time when a person has served
    |minimum_time_served| years or more. The compartment level filters can be used to restrict the type of session
    that counts towards the time served.

    Parameters:
    -----------
    criteria_name : str
        The name of the criteria view.

    description : str
        A brief description of the criteria view.

    minimum_time_served : int
        The minimum amount of time served required to meet the criteria.

    time_served_interval : str, optional
        The interval type for the time served (e.g., "YEAR", "MONTH"). Defaults to "YEAR".

    compartment_level_0 : str, optional
        The primary compartment level to filter sessions by. Defaults to "INCARCERATION". If "SUPERVISION" then
        `prioritized_supervision_sessions` is used instead of `compartment_level_1` or `compartment_sessions`

    compartment_level_1_types : Optional[List[str]], optional
        A list of compartment level 1 types to filter sessions by, e.g., "SUPERVISION",
        "INCARCERATION". Defaults to None.

    compartment_level_2_types : Optional[List[str]], optional
        A list of compartment level 2 types to filter sessions by. If provided, sessions
        will be filtered based on this level. Defaults to None.

    housing_unit_types : Optional[List[str]], optional
        A list of housing unit types to filter sessions by. Defaults to None.

    supervision_level_types : Optional[List[str]], optional
        A list of supervision level types to filter sessions by. Defaults to None.
    """

    # Default to `system_sessions` if no compartment type is specified
    sessions_table = "system_sessions_materialized"
    sessions_conditions = []
    attribute = ""  # Initialize attribute to a default value

    if compartment_level_1_types:
        attribute = "compartment_level_1"
        if (
            "SUPERVISION" in compartment_level_1_types
            or "SUPERVISION_OUT_OF_STATE" in compartment_level_1_types
        ):
            sessions_table = "prioritized_supervision_sessions_materialized"
            formatted_values = "', '".join(compartment_level_1_types)
            sessions_conditions.append(f"compartment_level_1 IN ('{formatted_values}')")

        if "INCARCERATION" in compartment_level_1_types:
            sessions_table = "compartment_level_1_super_sessions_materialized"
            formatted_values = "', '".join(compartment_level_1_types)
            sessions_conditions.append(f"compartment_level_1 IN ('{formatted_values}')")

    if compartment_level_2_types:
        attribute = "compartment_level_2"
        if compartment_level_0 == "SUPERVISION":
            sessions_table = "prioritized_supervision_sessions_materialized"
            formatted_values = "', '".join(compartment_level_2_types)
            sessions_conditions.append(f"compartment_level_2 IN ('{formatted_values}')")
        else:
            sessions_table = "compartment_sessions_materialized"
            formatted_values = "', '".join(compartment_level_2_types)
            sessions_conditions.append(f"compartment_level_2 IN ('{formatted_values}')")

    if supervision_level_types:
        if compartment_level_1_types or compartment_level_2_types:
            raise ValueError(
                "Compartment level 1 and 2 values are not supported in supervision level sessions"
            )
        sessions_table = "supervision_level_sessions_materialized"
        formatted_values = "', '".join(supervision_level_types)
        sessions_conditions.append(f"supervision_level IN ('{formatted_values}')")
        attribute = "supervision_level"

    if housing_unit_types:
        sessions_table = "housing_unit_type_sessions_materialized"
        formatted_values = "', '".join(housing_unit_types)
        sessions_conditions.append(f"housing_unit_type IN ('{formatted_values}')")
        attribute = "housing_unit_type"

    if len(sessions_conditions) > 0:
        condition_string = "WHERE " + "\n\t\tAND ".join(sessions_conditions)
    else:
        condition_string = ""

    criteria_query = f"""
    WITH filtered_spans AS (
      SELECT
        state_code,
        person_id,
        start_date,
        end_date_exclusive,
        {attribute},
      FROM `{{project_id}}.{{sessions_dataset}}.{sessions_table}`
      {condition_string}
    ),
    critical_date_spans AS (
      SELECT
        state_code,
        person_id,
        start_date AS start_datetime,
        end_date_exclusive AS end_datetime,
        DATE_ADD(start_date, INTERVAL {minimum_time_served} {time_served_interval}) AS critical_date,
      FROM ({aggregate_adjacent_spans(
                table_name='filtered_spans',
                end_date_field_name="end_date_exclusive",
            )})
    ),
    {critical_date_has_passed_spans_cte()}
    SELECT
        cd.state_code,
        cd.person_id,
        cd.start_date,
        cd.end_date,
        cd.critical_date_has_passed AS meets_criteria,
        TO_JSON(STRUCT(
            cd.critical_date AS eligible_date
        )) AS reason,
        cd.critical_date AS minimum_time_served_date,
    FROM critical_date_has_passed_spans cd
    """

    return StateAgnosticTaskCriteriaBigQueryViewBuilder(
        criteria_name=criteria_name,
        description=description,
        criteria_spans_query_template=criteria_query,
        sessions_dataset=SESSIONS_DATASET,
        reasons_fields=[
            ReasonsField(
                name="minimum_time_served_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Date when the client has served the time required",
            ),
        ],
    )


def custody_or_supervision_level_criteria_builder(
    criteria_name: str,
    description: str,
    levels_lst: list,
    reasons_columns: str,
    reasons_fields: List[ReasonsField],
    level_meets_criteria: bool = True,
    compartment_level_1_filter: str = "SUPERVISION",
) -> StateAgnosticTaskCriteriaBigQueryViewBuilder:
    """
    Args:
        criteria_name (str): Criteria query name
        description (str): Criteria query description
        levels_lst (list): List of supervision/custody levels to include in the criteria
        reasons_columns (str): SQL snippet to use for the criteria reasons, typically includes something for the
            custody or supervision level and the level start date.
        reasons_fields (list): ReasonFields used to aggregate the reason columns into the reason JSON
        level_meets_criteria (bool, optional): Value to use for the meets_criteria
            column. Defaults to True.
        compartment_level_1_filter (str, optional): Either 'SUPERVISION' OR
            'INCARCERATION'. Defaults to "SUPERVISION".
    Returns:
        StateAgnosticTaskCriteriaBigQueryViewBuilder: Returns a state agnostic criteria
        view builder indicating spans of time when a person is (or not) in a certain
        supervision_level or custody_level as tracked by our
        `supervision/custody_level_sessions` table
    """

    raise_error_if_invalid_compartment_level_1_filter(compartment_level_1_filter)
    #
    if compartment_level_1_filter.upper() == "INCARCERATION":
        level_type = "custody"
    elif compartment_level_1_filter.upper() == "SUPERVISION":
        level_type = "supervision"
    else:
        raise ValueError(
            f"Unexpected compartment_level_1_filter [{compartment_level_1_filter}]"
        )

    # Transform list of levels to a string to be used in the query
    levels_str = "('" + "', '".join(levels_lst) + "')"

    criteria_query = f"""
    SELECT
        state_code,
        person_id,
        start_date,
        end_date_exclusive AS end_date,
        {level_meets_criteria} AS meets_criteria,
        TO_JSON(STRUCT({reasons_columns})) AS reason,
        {reasons_columns},
    FROM `{{project_id}}.{{sessions_dataset}}.{level_type}_level_raw_text_sessions_materialized`
    WHERE {level_type}_level IN {levels_str}
    """

    # If meets criteria is always true, then the default view builder should be false
    meets_criteria_default_view_builder = not level_meets_criteria

    return StateAgnosticTaskCriteriaBigQueryViewBuilder(
        criteria_name=criteria_name,
        description=description,
        criteria_spans_query_template=criteria_query,
        sessions_dataset=SESSIONS_DATASET,
        meets_criteria_default=meets_criteria_default_view_builder,
        reasons_fields=reasons_fields,
    )


def custody_level_compared_to_recommended(
    criteria: str,
) -> str:
    """
    Args:
        criteria (str): The criteria for comparing current custody level to recommended level
    Returns:
        f-string: Spans of time where a given criteria comparing current and recommended custody level is met
    """

    return f"""
    WITH critical_dates AS (
      SELECT 
        state_code,
        person_id,
        start_date,
        end_date_exclusive,
        custody_level,
        CAST(NULL AS STRING) AS recommended_custody_level,
      FROM `{{project_id}}.{{sessions_dataset}}.custody_level_sessions_materialized`

      UNION ALL

      SELECT 
        state_code, 
        person_id, 
        start_date,
        end_date_exclusive,
        CAST(NULL AS STRING) AS custody_level,
        recommended_custody_level,
      FROM `{{project_id}}.{{analyst_dataset}}.recommended_custody_level_spans_materialized`

    ),
    {create_sub_sessions_with_attributes(table_name='critical_dates',end_date_field_name="end_date_exclusive")}
    , 
    dedup_cte AS (
        SELECT
            person_id,
            state_code,
            start_date,
            end_date_exclusive,
            -- Take non-null values if there are any
            MAX(custody_level) AS custody_level,
            MAX(recommended_custody_level) AS recommended_custody_level,
        FROM
            sub_sessions_with_attributes
        GROUP BY
            1,2,3,4
    )
    SELECT
        state_code,
        person_id,
        start_date,
        end_date_exclusive AS end_date,
        {criteria} AS meets_criteria,
        TO_JSON(STRUCT(
            recommended_custody_level AS recommended_custody_level,
            dedup_cte.custody_level AS custody_level
        )) AS reason,
        recommended_custody_level,
        dedup_cte.custody_level,
    FROM dedup_cte
    LEFT JOIN `{{project_id}}.{{sessions_dataset}}.custody_level_dedup_priority` current_cl
        ON dedup_cte.custody_level = current_cl.custody_level
    LEFT JOIN `{{project_id}}.{{sessions_dataset}}.custody_level_dedup_priority` recommended_cl
        ON recommended_custody_level = recommended_cl.custody_level
    WHERE start_date <= CURRENT_DATE('US/Pacific')
    """


VIOLATIONS_FOUND_WHERE_CLAUSE = """
    WHERE 
        CASE v.state_code
            # In ME, convictions are only relevant if their outcome is VIOLATION FOUND
            WHEN 'US_ME' THEN response_type IN ("VIOLATION_REPORT", "PERMANENT_DECISION")
            -- TODO(#26878): Update this if we revise violation ingest mappings in Oregon
            WHEN 'US_OR' THEN JSON_EXTRACT_SCALAR(vr.violation_response_metadata, '$.SANCTION_OR_INTERVENTION') = 'S'
            ELSE TRUE
            END
"""


def num_events_within_time_interval_spans(
    events_cte: str,
    date_interval: int,
    date_part: str,
) -> str:
    """
    Creates a CTE with spans of time for the number of events within a given time interval.
    Args:
        events_cte (str): Specifies the events that should be counted towards
            the spans.
        date_interval (int): Number of <date_part> over which the events will be counted.
        date_part (str): Supports any of the BigQuery date_part values:
            "DAY", "WEEK","MONTH","QUARTER","YEAR".
    """
    return f"""event_spans AS (
        SELECT
            state_code,
            person_id,
            event_date AS start_date,
            DATE_ADD(event_date, INTERVAL {date_interval} {date_part}) AS end_date,
            event_date,
        FROM {events_cte}
        WHERE event_date IS NOT NULL
    )
    ,
    -- We create sub-sessions to find overlapping periods where an event happened during
    -- some interval, allowing us to count the number of events that have recently occurred 
    -- during that period
    {create_sub_sessions_with_attributes('event_spans')}
    ,
    event_count_spans AS (
        SELECT 
            state_code,
            person_id,
            start_date,
            end_date,
            COUNT(event_date) AS event_count,
            ARRAY_AGG(event_date ORDER BY event_date DESC) AS event_dates,
        FROM sub_sessions_with_attributes
        GROUP BY 1,2,3,4
    )
    """


def violations_within_time_interval_criteria_builder(
    *,
    criteria_name: str,
    description: str,
    violation_type: str = "",
    where_clause: str = "",
    bool_column: str = "False AS meets_criteria,",
    date_interval: int = 12,
    date_part: str = "MONTH",
    violation_date_name_in_reason_blob: str = "latest_convictions",
    display_single_violation_date: bool = False,
    state_code: Optional[StateCode] = None,
) -> TaskCriteriaBigQueryViewBuilder:
    """
    Returns a criteria query that has spans of time where the violations that meet
    certain conditions set by the user (<violaiton_type> and <where clause>) occured.
    Args:
        criteria_name (str): Name of the criteria
        description (str): Description of the criteria
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
        violation_date_name_in_reason_blob (str, optional): Name of the violation_date
            field in the reason blob. Defaults to "latest_convictions".
        display_single_violation_date (bool, optional): Show only the latest violation in
            the reason blob. Defaults to False, showing all violations in the given time
            period.
        state_code (str, optional): State code for which to return a state-specific view
            builder. Defaults to None, returning a state-agnostic view builder.
    Returns:
        TaskCriteriaBigQueryViewBuilder: CTE query that shows the spans of
            time where the violations that meet certain conditions set by the user
            (<violaiton_type> and <where clause>) occured. The span of time for the validity of
            each violation starts at violation_date and ends after a period specified by
            the user (in <date_interval> and <date_part>)
    """

    violation_type_join = f"""
    INNER JOIN `{{project_id}}.normalized_state.state_supervision_violation_type_entry` vt
        ON vr.supervision_violation_id = vt.supervision_violation_id
        AND vr.person_id = vt.person_id
        AND vr.state_code = vt.state_code
        {violation_type}
    """

    violation_date_content_in_reason_blob = (
        "ARRAY_AGG(violation_date IGNORE NULLS ORDER BY violation_date DESC)"
    )
    if display_single_violation_date:
        violation_date_content_in_reason_blob += "[OFFSET(0)]"

    criteria_query = f"""WITH supervision_violations AS (
        SELECT
            vr.state_code,
            vr.person_id,
            COALESCE(v.violation_date, vr.response_date) AS start_date,
            DATE_ADD(COALESCE(v.violation_date, vr.response_date), INTERVAL {date_interval} {date_part}) AS end_date,
            DATE_ADD(COALESCE(v.violation_date, vr.response_date), INTERVAL {date_interval} {date_part}) AS violation_expiration_date,
            COALESCE(v.violation_date, vr.response_date) AS violation_date,
            {bool_column}
        FROM `{{project_id}}.normalized_state.state_supervision_violation_response` vr
        {violation_type_join if violation_type else ""}
        LEFT JOIN `{{project_id}}.normalized_state.state_supervision_violation` v
            ON vr.supervision_violation_id = v.supervision_violation_id
            AND vr.person_id = v.person_id
            AND vr.state_code = v.state_code
        {where_clause}
    ), 
    {create_sub_sessions_with_attributes('supervision_violations')}

    SELECT 
        state_code,
        person_id,
        start_date,
        end_date,
        LOGICAL_AND(meets_criteria) AS meets_criteria,
        TO_JSON(STRUCT(
            {violation_date_content_in_reason_blob} AS {violation_date_name_in_reason_blob},
            MAX(violation_expiration_date) AS violation_expiration_date
        )) AS reason,
        {violation_date_content_in_reason_blob} AS {violation_date_name_in_reason_blob},
        MAX(violation_expiration_date) AS violation_expiration_date,
    FROM sub_sessions_with_attributes
    GROUP BY 1,2,3,4
    """

    if state_code:
        # TODO(#26803): Remove this once Oregon fits within state-agnostic logic
        return StateSpecificTaskCriteriaBigQueryViewBuilder(
            criteria_name=criteria_name,
            description=description,
            criteria_spans_query_template=criteria_query,
            state_code=state_code,
            meets_criteria_default=True,
            reasons_fields=[
                ReasonsField(
                    name=violation_date_name_in_reason_blob,
                    type=bigquery.enums.StandardSqlTypeNames.DATE,
                    description="Date when the violation occurred",
                ),
                ReasonsField(
                    name="violation_expiration_date",
                    type=bigquery.enums.StandardSqlTypeNames.DATE,
                    description="Date when the violations will age out of the time interval",
                ),
            ],
        )

    return StateAgnosticTaskCriteriaBigQueryViewBuilder(
        criteria_name=criteria_name,
        description=description,
        criteria_spans_query_template=criteria_query,
        meets_criteria_default=True,
        reasons_fields=[
            ReasonsField(
                name=violation_date_name_in_reason_blob,
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Date when the violation occurred",
            ),
            ReasonsField(
                name="violation_expiration_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Date when the violations will age out of the time interval",
            ),
        ],
    )


def incarceration_violations_within_time_interval_criteria_builder(
    *,
    criteria_name: str,
    description: str,
    incident_type: str = "",
    where_clause: str = "",
    bool_column: str = "False AS meets_criteria,",
    date_interval: int = 12,
    date_part: str = "MONTH",
    violation_date_name_in_reason_blob: str = "latest_violations",
    display_single_violation_date: bool = False,
    state_code: Optional[StateCode] = None,
) -> TaskCriteriaBigQueryViewBuilder:
    """
    Returns a criteria query that has spans of time when incarceration violations that meet
    certain conditions set by the user (<violation_type> and <where clause>) occurred.
    Args:
        criteria_name (str): Name of the criteria
        description (str): Description of the criteria
        incident_type (str, optional): Specifies the violation types that should be
            counted towards the criteria. Should only include values inside the
            StateIncarcerationIncidentType enum. Example: "AND sii.incident_type = 'VIOLENCE' "
            Defaults to ''.
        where_clause (str, optional): _description_. Defaults to ''.
        bool_column (str, optional): _description_. Defaults to "False AS meets_criteria,".
        date_interval (int, optional): Number of <date_part> when the violation
            will be counted as valid. Defaults to 12 (e.g. it could be 12 months).
        date_part (str, optional): Supports any of the BigQuery date_part values:
            "DAY", "WEEK","MONTH","QUARTER","YEAR". Defaults to "MONTH".
        violation_date_name_in_reason_blob (str, optional): Name of the violation_date
            field in the reason blob. Defaults to "latest_convictions".
        display_single_violation_date (bool, optional): Show only the latest violation in
            the reason blob. Defaults to False, showing all violations in the given time
            period.
        state_code (str, optional): State code for which to return a state-specific view
            builder. Defaults to None, returning a state-agnostic view builder.
    Returns:
        TaskCriteriaBigQueryViewBuilder: CTE query that shows the spans of
            time when the violations that meet certain conditions set by the user
            (<incident_type> and <where clause>) occurred. The span of time for the validity of
            each violation starts at incident_date and ends after a period specified by
            the user (in <date_interval> and <date_part>)
    """

    violation_date_content_in_reason_blob = (
        "ARRAY_AGG(violation_date IGNORE NULLS ORDER BY violation_date DESC)"
    )
    if display_single_violation_date:
        violation_date_content_in_reason_blob += "[OFFSET(0)]"

    criteria_query = f"""
    WITH incarceration_violations AS (
        SELECT
            sii.state_code,
            sii.person_id,
            incident_date AS start_date,
            DATE_ADD(incident_date, INTERVAL {date_interval} {date_part}) AS end_date,
            incident_date AS violation_date,
            {bool_column}
        FROM `{{project_id}}.normalized_state.state_incarceration_incident` sii
        {incident_type}
        {where_clause}
    ), 
    {create_sub_sessions_with_attributes('incarceration_violations')}
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date,
        LOGICAL_AND(meets_criteria) AS meets_criteria,
        TO_JSON(STRUCT({violation_date_content_in_reason_blob} AS {violation_date_name_in_reason_blob})) AS reason,
        {violation_date_content_in_reason_blob} AS {violation_date_name_in_reason_blob},
    FROM sub_sessions_with_attributes
    GROUP BY 1,2,3,4
    """

    if state_code:
        # TODO(#26803): Remove this once Oregon fits within state-agnostic logic
        return StateSpecificTaskCriteriaBigQueryViewBuilder(
            criteria_name=criteria_name,
            description=description,
            criteria_spans_query_template=criteria_query,
            state_code=state_code,
            meets_criteria_default=True,
            reasons_fields=[
                ReasonsField(
                    name=violation_date_name_in_reason_blob,
                    type=bigquery.enums.StandardSqlTypeNames.DATE,
                    description="Date when the violation occurred",
                ),
            ],
        )

    return StateAgnosticTaskCriteriaBigQueryViewBuilder(
        criteria_name=criteria_name,
        description=description,
        criteria_spans_query_template=criteria_query,
        meets_criteria_default=True,
        reasons_fields=[
            ReasonsField(
                name=violation_date_name_in_reason_blob,
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Date when the violation occurred",
            ),
        ],
    )


def is_past_completion_date_criteria_builder(
    criteria_name: str,
    description: str,
    meets_criteria_leading_window_time: int = 0,
    compartment_level_1_filter: str = "SUPERVISION",
    date_part: str = "DAY",
    critical_date_name_in_reason: str = "eligible_date",
    critical_date_column: str = "projected_completion_date_max",
    negate_critical_date_has_passed: bool = False,
) -> StateAgnosticTaskCriteriaBigQueryViewBuilder:
    """
    Returns a criteria query that has spans of time when the projected completion date
    has passed or is coming up while someone is on supervision or incarceration. This is
    a standalone function that can be called when creating criteria queries.
    Args:
        criteria_name (str): Criteria query name
        description (str): Criteria query description
        meets_criteria_leading_window_time (int, optional): Modifier to move the start_date
            by a constant value to account, for example, for time before the critical date
            where some criteria is met. Defaults to 0. This is passed to the
            `critical_date_has_passed_spans_cte` function.
        compartment_level_1_filter (str, optional): Either 'SUPERVISION' OR
            'INCARCERATION'. Defaults to "SUPERVISION".
        date_part (str, optional): Supports any of the BigQuery date_part values:
            "DAY", "WEEK","MONTH","QUARTER","YEAR". Defaults to "MONTH".
        critical_date_name_in_reason (str, optional): The name of the critical date in
            the reason column. Defaults to "eligible_date".
        critical_date_column (str, optional): The name of the column that contains the
            critical date. Defaults to "projected_completion_date_max".
        negate_critical_date_has_passed (bool, optional): If True, the critical date has
            passed will be negated. This means the periods where this date has passed
            will become False. Defaults to False.
    Raises:
        ValueError: if compartment_level_1_filter is different from "supervision" or
            "incarceration".
    Returns:
        StateAgnosticTaskCriteriaBigQueryViewBuilder: criteria query that has spans of
            time when the projected completion date has passed or is coming up while
            someone is on supervision or incarceration
    """
    raise_error_if_invalid_compartment_level_1_filter(compartment_level_1_filter)

    # Transform compartment_level_1_filter to a string to be used in the query
    compartment_level_1 = compartment_level_1_filter.lower()

    criteria_query = f"""
    WITH critical_date_spans AS (
        SELECT
            state_code,
            person_id,
            start_date AS start_datetime,
            end_date AS end_datetime,
            {revert_nonnull_end_date_clause(critical_date_column)} AS critical_date
        FROM `{{project_id}}.{{sessions_dataset}}.{compartment_level_1}_projected_completion_date_spans_materialized`
    ),
    {critical_date_has_passed_spans_cte(meets_criteria_leading_window_time = meets_criteria_leading_window_time,
                                        date_part=date_part)}
    SELECT
        state_code,
        person_id,
        start_date,
        end_date,
        {'NOT' if negate_critical_date_has_passed else ''} critical_date_has_passed AS meets_criteria,
        TO_JSON(STRUCT(critical_date AS {critical_date_name_in_reason})) AS reason,
        critical_date AS {critical_date_name_in_reason},
    FROM critical_date_has_passed_spans
    """

    return StateAgnosticTaskCriteriaBigQueryViewBuilder(
        criteria_name=criteria_name,
        criteria_spans_query_template=criteria_query,
        description=description,
        sessions_dataset=SESSIONS_DATASET,
        reasons_fields=[
            ReasonsField(
                name=critical_date_name_in_reason,
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Date when the critical date has passed",
            ),
        ],
    )
