# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
from typing import Dict, List, Optional, Union

from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import (
    list_to_query_string,
    nonnull_end_date_exclusive_clause,
    revert_nonnull_end_date_clause,
)
from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_sub_sessions_with_attributes,
    join_sentence_serving_periods_to_compartment_sessions,
)
from recidiviz.calculator.query.state.dataset_config import (
    SENTENCE_SESSIONS_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
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
        span.end_date_exclusive AS end_date,
        FALSE AS meets_criteria,
        TO_JSON(STRUCT(ARRAY_AGG(DISTINCT statute ORDER BY statute) AS ineligible_offenses{additional_json_fields_str})) AS reason,
        ARRAY_AGG(DISTINCT statute ORDER BY statute) AS ineligible_offenses{additional_json_fields_str}
    {join_sentence_serving_periods_to_compartment_sessions(compartment_level_1_to_overlap=compartment_level_1)}
    {where_clause}
    GROUP BY 1,2,3,4,5
    """

    return StateAgnosticTaskCriteriaBigQueryViewBuilder(
        criteria_name=criteria_name,
        criteria_spans_query_template=criteria_query,
        description=description,
        sessions_dataset=SESSIONS_DATASET,
        sentence_sessions_dataset=SENTENCE_SESSIONS_DATASET,
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
    *,
    criteria_name: str,
    description: str,
    minimum_time_served: int,
    time_served_interval: str = "YEAR",
    compartment_level_1_types: Optional[List[str]] = None,
    compartment_level_2_types: Optional[List[str]] = None,
    supervision_levels: Optional[List[str]] = None,
    custody_levels: Optional[List[str]] = None,
    housing_unit_types: Optional[List[str]] = None,
    custodial_authority_types: Optional[List[str]] = None,
) -> StateAgnosticTaskCriteriaBigQueryViewBuilder:
    """Returns a state-agnostic criterion view builder indicating spans of time when a
    person has served at least some minimum period of time, as determined by
    |minimum_time_served| and |time_served_interval|. The filters for compartment
    levels, supervision levels, and housing unit types can be used to restrict the type
    of session that counts as time served.

    NB: to meet this criterion, the person must have served the minimum period of time
    continuously. Any departure from the system or from specified compartment level(s),
    supervision level(s), or housing unit type(s) will "reset the clock" for a person.

    Parameters:
    -----------
    criteria_name : str
        The name of the criterion view.

    description : str
        A brief description of the criterion view.

    minimum_time_served : int
        The minimum amount of time served (e.g., 2) required to meet the criterion.

    time_served_interval : str, optional
        The interval type for the time served. Supports any of the BigQuery `date_part`
        values: "DAY", "WEEK", "MONTH", "QUARTER", or "YEAR". Defaults to "YEAR".

    compartment_level_1_types : Optional[List[str]], optional
        A list of `compartment_level_1` types by which to filter sessions (e.g.,
        "SUPERVISION", "INCARCERATION"). Defaults to None.

    compartment_level_2_types : Optional[List[str]], optional
        A list of `compartment_level_2` types by which to filter sessions (e.g.,
        "PAROLE", "DUAL"). Defaults to None.

    supervision_levels : Optional[List[str]], optional
        A list of supervision levels by which to filter sessions. Defaults to None.

    custody_levels: Optional[List[str]], optional
        A list of custody levels by which to filter sessions. Defaults to None.

    housing_unit_types : Optional[List[str]], optional
        A list of housing-unit types by which to filter sessions. Defaults to None.

    custodial_authority_types : Optional[List[str]], optional
        A list of custodial authority types by which to filter sessions. Defaults to None.
    """

    # Default to `system_sessions` if no filters are specified
    sessions_table = "system_sessions_materialized"
    sessions_conditions = []

    # Check validity of input arguments
    if supervision_levels is not None or housing_unit_types is not None:
        if (
            compartment_level_1_types is not None
            or compartment_level_2_types is not None
        ):
            raise ValueError(
                "Compartment-level values are not supported alongside supervision levels or housing-unit types."
            )
        if supervision_levels is not None and housing_unit_types is not None:
            raise ValueError(
                "Supervision levels and housing-unit types are not simultaneously supported."
            )

    if compartment_level_1_types is not None or compartment_level_2_types is not None:
        sessions_table = "compartment_sessions_materialized"
        if compartment_level_1_types is not None:
            if all(
                compartment in ["SUPERVISION", "SUPERVISION_OUT_OF_STATE"]
                for compartment in compartment_level_1_types
            ):
                sessions_table = "prioritized_supervision_sessions_materialized"
            elif all(
                compartment in ["INCARCERATION", "INCARCERATION_OUT_OF_STATE"]
                for compartment in compartment_level_1_types
            ):
                sessions_table = "compartment_level_1_super_sessions_materialized"
            formatted_values = "', '".join(compartment_level_1_types)
            sessions_conditions.append(f"compartment_level_1 IN ('{formatted_values}')")
        if compartment_level_2_types is not None:
            formatted_values = "', '".join(compartment_level_2_types)
            sessions_conditions.append(f"compartment_level_2 IN ('{formatted_values}')")

    if supervision_levels is not None:
        sessions_table = "supervision_level_sessions_materialized"
        formatted_values = "', '".join(supervision_levels)
        sessions_conditions.append(f"supervision_level IN ('{formatted_values}')")

    if custody_levels is not None:
        sessions_table = "custody_level_sessions_materialized"
        formatted_values = "', '".join(custody_levels)
        sessions_conditions.append(f"custody_level IN ('{formatted_values}')")

    if housing_unit_types is not None:
        sessions_table = "housing_unit_type_sessions_materialized"
        formatted_values = "', '".join(housing_unit_types)
        sessions_conditions.append(f"housing_unit_type IN ('{formatted_values}')")

    if custodial_authority_types is not None:
        sessions_table = "custodial_authority_sessions_materialized"
        formatted_values = "', '".join(custodial_authority_types)
        sessions_conditions.append(f"custodial_authority IN ('{formatted_values}')")

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
        state_code,
        person_id,
        start_date,
        end_date,
        critical_date_has_passed AS meets_criteria,
        TO_JSON(STRUCT(
            critical_date AS eligible_date
        )) AS reason,
        critical_date AS minimum_time_served_date,
    FROM critical_date_has_passed_spans
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
    ),
    meets_criteria_spans AS (
        SELECT 
            dedup_cte.*,
            {criteria} AS meets_criteria,
        FROM dedup_cte
        LEFT JOIN `{{project_id}}.{{sessions_dataset}}.custody_level_dedup_priority` current_cl
            ON dedup_cte.custody_level = current_cl.custody_level
        LEFT JOIN `{{project_id}}.{{sessions_dataset}}.custody_level_dedup_priority` recommended_cl
            ON recommended_custody_level = recommended_cl.custody_level
    ),
    next_eligibility_spans AS (
    /* This CTE aggregates meets_critera_spans for rows where custody_level, recommended_custody_level, 
    and meets_criteria have the same value so that we can set the upcoming_eligibility_date as the start date for that
    row if meets_criteria, and the start_date for the upcoming row, if the next row meets_criteria. In this way we 
    get the date at which someone becomes eligible for rows where clients are eligible, or the next date at which
    the client will become eligible */
    SELECT 
        *,
        CASE
            WHEN LEAD(meets_criteria) OVER (PARTITION BY person_id ORDER BY start_date) 
                THEN LEAD(start_date) OVER (PARTITION BY person_id ORDER BY start_date)
            WHEN meets_criteria THEN start_date
            ELSE NULL
        END AS upcoming_eligibility_date
    FROM ({aggregate_adjacent_spans(table_name = 'meets_criteria_spans',
                                      attribute=['custody_level', 'recommended_custody_level', 'meets_criteria'],
                                    end_date_field_name='end_date_exclusive')})
    )
    SELECT
        state_code,
        person_id,
        start_date,
        end_date_exclusive AS end_date,
        meets_criteria,
        TO_JSON(STRUCT(
            recommended_custody_level AS recommended_custody_level,
            n.custody_level AS custody_level,
            n.upcoming_eligibility_date
        )) AS reason,
        recommended_custody_level,
        n.custody_level,
        n.upcoming_eligibility_date
    FROM next_eligibility_spans n
    LEFT JOIN `{{project_id}}.{{sessions_dataset}}.custody_level_dedup_priority` current_cl
        ON n.custody_level = current_cl.custody_level
    LEFT JOIN `{{project_id}}.{{sessions_dataset}}.custody_level_dedup_priority` recommended_cl
        ON recommended_custody_level = recommended_cl.custody_level
    WHERE start_date <= CURRENT_DATE('US/Pacific')
    """


def num_events_within_time_interval_spans(
    events_cte: str,
    date_interval: Optional[int] = None,
    date_part: Optional[str] = None,
) -> str:
    """
    Creates a CTE with spans of time for the number of events within a given time interval.
    Args:
        events_cte (str): Specifies the events that should be counted towards
            the spans.
        date_interval (int, optional): Number of <date_part> over which the events will be counted.
            If not provided, the events will be counted into the indefinite future.
        date_part (str, optional): Supports any of the BigQuery date_part values:
            "DAY", "WEEK", "MONTH", "QUARTER", "YEAR".
            Must be provided if date_interval is provided.
    """
    if date_interval:
        end_date_clause = f"DATE_ADD(event_date, INTERVAL {date_interval} {date_part})"
    else:
        end_date_clause = "CAST(NULL AS DATE)"

    return f"""event_spans AS (
        SELECT
            state_code,
            person_id,
            event_date AS start_date,
            {end_date_clause} AS end_date,
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


def supervision_violations_within_time_interval_criteria_builder(
    *,
    criteria_name: str,
    description: str,
    date_interval: int,
    date_part: str,
    violation_type: str = "",
    where_clause_addition: str = "",
    violation_date_name_in_reason_blob: str = "latest_violations",
    return_view_builder: bool = True,
) -> Dict | StateAgnosticTaskCriteriaBigQueryViewBuilder:
    """
    Returns a TES criterion view builder that has spans of time where violations that
    meet certain conditions set by the user have occurred within some specified window
    of time (e.g., within the past 6 months).
    Args:
        criteria_name (str): Name of the criterion.
        description (str): Description of the criterion.
        date_interval (int): Number of <date_part> when the violation will be counted as
            valid/relevant.
        date_part (str): Supports any of the BigQuery date_part values: "DAY", "WEEK",
            "MONTH", "QUARTER", or "YEAR".
        violation_type (str, optional): Specifies the violation types that should be
            counted towards the criterion. Should only include values inside of the
            StateSupervisionViolationType enum. Example: "AND vt.violation_type='FELONY'".
            Defaults to "".
        where_clause_addition (str, optional): Any additional WHERE-clause filters for
            selecting violations. Example: "AND vr.response_type='VIOLATION_REPORT'".
            Defaults to "".
        violation_date_name_in_reason_blob (str, optional): Name of the `violation_date`
            field in the reason blob. Defaults to "latest_violations".
        return_view_builder (bool, optional): Whether to return a view builder or just the
            query string + reasons_fields in a dictionary. Defaults to True.
    Returns:
        Union[str, StateAgnosticTaskCriteriaBigQueryViewBuilder]: Either a TES criterion
        view builder that shows the spans of time where the violations that meet any
        condition(s) set by the user have occurred (<violation_type> and <where_clause_addition>),
        or a dictionary with the query string and reasons_fields. The span of time for
        the validity of each violation starts at `violation_date` and ends after a
        period specified by the user (<date_interval> and <date_part>).
    """

    violation_type_join = f"""
    INNER JOIN `{{project_id}}.normalized_state.state_supervision_violation_type_entry` vt
        ON v.supervision_violation_id = vt.supervision_violation_id
        AND v.person_id = vt.person_id
        AND v.state_code = vt.state_code
        {violation_type}
    """

    # TODO(#35354): Account for violation decisions when considering which violations
    # should disqualify someone from eligibility.
    criteria_query = f"""
    WITH supervision_violations AS (
        /* Select violations that we want to count for this criterion. Violations are
        filtered based on any specified violation-level attributes and any aggregated
        attributes of violation responses (since there can be multiple responses per
        violation). */
        SELECT
            v.state_code,
            v.person_id,
            v.supervision_violation_id,
            /* If there is no `violation_date` value, we treat the earliest date of any
            response(s) as the violation date. (We take the MIN of `violation_date`
            below just because we have to aggregate the `violation_date` field in this
            expression, but note that because the violation-to-response ratio can be
            one-to-many, every row should have the same `violation_date`, and this
            therefore will just return the original `violation_date` value if there is
            one.) */
            COALESCE(MIN(v.violation_date), MIN(vr.response_date)) AS violation_date,
        FROM `{{project_id}}.normalized_state.state_supervision_violation` v
        {violation_type_join if violation_type else ""}
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
    ),
    supervision_violation_ineligibility_spans AS (
        /* With our selected violations, we create spans of *ineligibility*, covering
        the periods of time during which someone will not meet this criterion due to
        still-relevant violations. */
        SELECT
            state_code,
            person_id,
            violation_date AS start_date,
            DATE_ADD(violation_date, INTERVAL {date_interval} {date_part}) AS end_date,
            violation_date,
            DATE_ADD(violation_date, INTERVAL {date_interval} {date_part}) AS violation_expiration_date,
            FALSE AS meets_criteria,
        FROM supervision_violations
    ),
    /* We sub-sessionize to handle overlapping spans, which arise when clients have
    more than one violation within the given time period of violation relevance (as
    specified by <date_interval> and <date_part>). */
    {create_sub_sessions_with_attributes('supervision_violation_ineligibility_spans')}
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date,
        LOGICAL_AND(meets_criteria) AS meets_criteria,
        TO_JSON(STRUCT(
            ARRAY_AGG(violation_date IGNORE NULLS ORDER BY violation_date DESC) AS {violation_date_name_in_reason_blob},
            MAX(violation_expiration_date) AS violation_expiration_date
        )) AS reason,
        ARRAY_AGG(violation_date IGNORE NULLS ORDER BY violation_date DESC) AS {violation_date_name_in_reason_blob},
        MAX(violation_expiration_date) AS violation_expiration_date,
    FROM sub_sessions_with_attributes
    GROUP BY 1, 2, 3, 4
    """
    reasons_fields = [
        ReasonsField(
            name=violation_date_name_in_reason_blob,
            type=bigquery.enums.StandardSqlTypeNames.ARRAY,
            description="Date(s) when the violation(s) occurred",
        ),
        ReasonsField(
            name="violation_expiration_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="Date when the most recent violation(s) will age out of the time interval",
        ),
    ]
    if return_view_builder:
        return StateAgnosticTaskCriteriaBigQueryViewBuilder(
            criteria_name=criteria_name,
            description=description,
            criteria_spans_query_template=criteria_query,
            meets_criteria_default=True,
            reasons_fields=reasons_fields,
        )
    return {"criteria_query": criteria_query, "reasons_fields": reasons_fields}


def incarceration_incidents_within_time_interval_criteria_builder(
    *,
    criteria_name: str,
    description: str,
    date_interval: int,
    date_part: str,
    where_clause_addition: Optional[str] = None,
    incident_date_name_in_reason_blob: str = "latest_incidents",
) -> StateAgnosticTaskCriteriaBigQueryViewBuilder:
    """
    Returns a TES criterion view builder that has spans of time where incidents that
    meet certain conditions set by the user have occurred within some specified window
    of time (e.g., within the past 6 months).

    Args:
        criteria_name (str): Name of the criterion.
        description (str): Description of the criterion.
        date_interval (int): Number of <date_part> when the incident will be counted as
            valid.
        date_part (str): Supports any of the BigQuery <date_part> values: "DAY", "WEEK",
            "MONTH", "QUARTER", or "YEAR".
        where_clause_addition (str, optional): Any additional WHERE-clause filters for
            selecting incidents. Example: "AND sii.incident_type='VIOLENCE'". Defaults
            to None.
        incident_date_name_in_reason_blob (str, optional): Name of the `incident_date`
            field in the reason blob. Defaults to "latest_incidents".

    Returns:
        StateAgnosticTaskCriteriaBigQueryViewBuilder: View builder for a state-agnostic
            TES criterion view that shows the spans of time where the incidents that
            meet any condition(s) set by the user (<where_clause_addition>) occurred.
            The span of time for the validity of each incident starts at `incident_date`
            and ends after a period specified by the user (<date_interval> and
            <date_part>).
    """

    # check validity of input
    if date_part not in ["DAY", "WEEK", "MONTH", "QUARTER", "YEAR"]:
        raise ValueError("Invalid value specified for `date_part`.")
    if where_clause_addition is not None and not where_clause_addition.startswith(
        "AND "
    ):
        raise ValueError(
            "Additional WHERE-clause filter(s) in `where_clause_addition` must start with 'AND '."
        )

    criteria_query = f"""
    WITH incarceration_incidents AS (
        /* Select incidents that we want to count for this criterion. Incidents are
        filtered based on any specified incident-level attributes and any aggregated
        attributes of incident outcomes (since there can be multiple outcomes per
        incident). */
        SELECT
            state_code,
            person_id,
            incarceration_incident_id,
            sii.incident_date,
        FROM `{{project_id}}.normalized_state.state_incarceration_incident` sii
        /* NB: there can be multiple outcomes per incident, so this LEFT JOIN can create
        multiple rows per incident (where each row is a unique incident outcome). */
        LEFT JOIN `{{project_id}}.normalized_state.state_incarceration_incident_outcome` siio
            USING (state_code, person_id, incarceration_incident_id)
        /* NB: the WHERE clause is typically evaluated after the FROM clause but before
        GROUP BY and aggregation, per BigQuery documentation. This means that any
        filtering applied in this WHERE clause will apply to the pre-grouped data. */
        /* By default, we'll exclude from consideration any incidents with an
        `incident_type` of 'POSITIVE', as these incidents reflect reports of good
        behavior; as such, we don't want these to count as disqualifying incidents in
        this criterion. */
        WHERE COALESCE(sii.incident_type, 'NO_INCIDENT_TYPE') NOT IN ('POSITIVE')
            {where_clause_addition if where_clause_addition else ""}
        /* Group by incident, so that we get one row per incident coming out of this
        CTE. */
        GROUP BY 1, 2, 3, 4
        /* Drop incidents with excluded `outcome_type` values. Note that not all
        incidents necessarily have outcomes (and even those that do may have a NULL
        `outcome_type`). We exclude any incidents where every non-null `outcome_type` is
        either 'DISMISSED' or 'NOT_GUILTY'. (Note that any incidents with at least one
        other `outcome_type` will therefore be considered disqualifying for a resident.)
        We use COALESCE in the statement below to ensure that we don't accidentally drop
        incidents that have no recorded outcome(s) (type[s]), as the LOGICAL_AND will
        return NULL if `outcome_type` is NULL for every row going into aggregation. */
        HAVING (NOT COALESCE(LOGICAL_AND(siio.outcome_type IN ('DISMISSED', 'NOT_GUILTY')), FALSE))
    ),
    incarceration_incident_ineligibility_spans AS (
        /* With our selected incidents, we create spans of *ineligibility*, covering the
        periods of time during which someone will not meet this criterion due to
        still-relevant incidents. */
        SELECT
            state_code,
            person_id,
            incident_date AS start_date,
            DATE_ADD(incident_date, INTERVAL {date_interval} {date_part}) AS end_date,
            incident_date,
            FALSE AS meets_criteria,
        FROM incarceration_incidents
    ),
    /* We sub-sessionize to handle overlapping spans, which arise when residents have
    more than one incident within the given time period of incident relevance (as
    specified by <date_interval> and <date_part>). */
    {create_sub_sessions_with_attributes('incarceration_incident_ineligibility_spans')}
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date,
        LOGICAL_AND(meets_criteria) AS meets_criteria,
        TO_JSON(STRUCT(
            ARRAY_AGG(incident_date IGNORE NULLS ORDER BY incident_date DESC) AS {incident_date_name_in_reason_blob}
        )) AS reason,
        ARRAY_AGG(incident_date IGNORE NULLS ORDER BY incident_date DESC) AS {incident_date_name_in_reason_blob},
    FROM sub_sessions_with_attributes
    GROUP BY 1, 2, 3, 4
    """

    return StateAgnosticTaskCriteriaBigQueryViewBuilder(
        criteria_name=criteria_name,
        description=description,
        criteria_spans_query_template=criteria_query,
        meets_criteria_default=True,
        reasons_fields=[
            ReasonsField(
                name=incident_date_name_in_reason_blob,
                type=bigquery.enums.StandardSqlTypeNames.ARRAY,
                description="Date(s) when the incident(s) occurred",
            ),
        ],
    )


def incarceration_sanctions_within_time_interval_criteria_builder(
    criteria_name: str,
    description: str,
    date_interval: int,
    date_part: str,
    additional_excluded_outcome_types: Optional[Union[str, List[str]]] = None,
    incident_severity: Optional[Union[str, List[str]]] = None,
) -> TaskCriteriaBigQueryViewBuilder:
    """
    Returns a criteria query with spans of time when someone has not recently had an
    incarceration sanction.
    Args:
        criteria_name (str): Name of the criterion
        description (str): Description of the criterion
        date_interval (int): Number of <date_part> representing the amount of time
            the sanction will be disqualifying for the criterion.
        date_part (str): Supports any of the BigQuery date_part values:
            "DAY", "WEEK", "MONTH", "QUARTER", "YEAR". Defaults to "MONTH".
        additional_excluded_outcome_types (str or List[str], optional): Specifies any additional outcome types
            that should be excluded when considering someone's eligibility, in addition
            to the default list of excluded types: ["DISMISSED", "NOT_GUILTY"].
        incident_severity (str or List[str], optional): Specifies the incident severity types that should be
            counted.
    Returns:
        TaskCriteriaBigQueryViewBuilder: View builder for spans of time when someone has
            not recently had an incarceration sanction.
    """
    if incident_severity:
        if isinstance(incident_severity, str):
            incident_severity = [incident_severity]

        incident_severity_bq_list = list_to_query_string(incident_severity, quoted=True)
        incident_severity_filter = (
            f"AND incident.incident_severity IN ({incident_severity_bq_list})"
        )
    else:
        incident_severity_filter = ""

    excluded_outcome_types = ["DISMISSED", "NOT_GUILTY"]
    if additional_excluded_outcome_types:
        if isinstance(additional_excluded_outcome_types, str):
            additional_excluded_outcome_types = [additional_excluded_outcome_types]
        excluded_outcome_types.extend(additional_excluded_outcome_types)

    excluded_outcome_types_bq_list = list_to_query_string(
        excluded_outcome_types, quoted=True
    )

    criteria_query = f"""
        WITH incarceration_sanction_dates AS (
            SELECT
                outcome.person_id,
                outcome.state_code,
                outcome.date_effective AS event_date
            FROM
                `{{project_id}}.normalized_state.state_incarceration_incident_outcome` outcome
            LEFT JOIN `{{project_id}}.normalized_state.state_incarceration_incident` incident
            USING(state_code, person_id, incarceration_incident_id)
            WHERE
                -- We only want to count sanctions with an actual effective date
                outcome.date_effective <= '3000-01-01'
                AND outcome.outcome_type NOT IN ({excluded_outcome_types_bq_list})
                {incident_severity_filter}
        ),
        {num_events_within_time_interval_spans(
            events_cte="incarceration_sanction_dates",
            date_interval=date_interval,
            date_part=date_part,
        )}
        SELECT
            person_id,
            state_code,
            start_date,
            end_date,
            event_count = 0 as meets_criteria,
            TO_JSON(STRUCT(event_dates)) AS reason,
            event_dates,
        FROM event_count_spans
    """

    return StateAgnosticTaskCriteriaBigQueryViewBuilder(
        criteria_name=criteria_name,
        description=description,
        criteria_spans_query_template=criteria_query,
        meets_criteria_default=True,
        reasons_fields=[
            ReasonsField(
                name="event_dates",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Date(s) when the sanction occurred",
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
            "DAY", "WEEK", "MONTH", "QUARTER", "YEAR". Defaults to "MONTH".
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


def no_absconsion_within_time_interval_criteria_builder(
    criteria_name: str,
    description: str,
    date_interval: int,
    date_part: str,
) -> StateAgnosticTaskCriteriaBigQueryViewBuilder:
    """
    Returns a criteria query builder that has spans of time when someone has not absconded
    within a given time interval.
    Args:
        criteria_name (str): Criteria query name
        description (str): Criteria query description
        date_interval (int): Number of <date_part> when the absconsion will be counted as
            valid.
        date_part (str): Supports any of the BigQuery date_part values:
            "DAY", "WEEK", "MONTH", "QUARTER", or "YEAR".
    """

    # TODO(#36981): This could be generalized further so it works with any CL2 or CL1

    criteria_query = f"""WITH absconded_sessions AS (
    SELECT 
        state_code,
        person_id,
        start_date,
        DATE_ADD(start_date, INTERVAL {date_interval} {date_part}) AS end_date,
        FALSE AS meets_criteria,
        start_date AS absconded_date,
    FROM `{{project_id}}.{{sessions_dataset}}.compartment_sessions_materialized`
    WHERE compartment_level_2 = 'ABSCONSION'
    ),

    {create_sub_sessions_with_attributes('absconded_sessions')}

    SELECT 
        state_code,
        person_id,
        start_date,
        end_date,
        LOGICAL_OR(meets_criteria) AS meets_criteria,
        TO_JSON(STRUCT(MAX(absconded_date) AS most_recent_absconded_date)) AS reason,
        MAX(absconded_date) AS most_recent_absconded_date,
    FROM sub_sessions_with_attributes
    GROUP BY 1,2,3,4"""

    return StateAgnosticTaskCriteriaBigQueryViewBuilder(
        criteria_name=criteria_name,
        description=description,
        criteria_spans_query_template=criteria_query,
        sessions_dataset=SESSIONS_DATASET,
        meets_criteria_default=True,
        reasons_fields=[
            ReasonsField(
                name="most_recent_absconded_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Start date of most recent absconsion",
            ),
        ],
    )


def custom_tuple(input_list: list) -> str:
    """
    Converts a list to a string that looks like a tuple. If the list has only one element,
    returns a single-element string tuple. If the list has more than one element, returns a
    string tuple with all elements.

    Args:
        input_list (list): The input list.

    Returns:
        str: The tuple as a string.
    """
    if len(input_list) == 1:
        return "('" + input_list[0] + "')"
    return str(tuple(input_list))


def employed_for_at_least_x_time_criteria_builder(
    criteria_name: str,
    description: str,
    employment_status_values: List[str],
    date_interval: int = 6,
    date_part: str = "MONTH",
) -> StateAgnosticTaskCriteriaBigQueryViewBuilder:
    """
    Returns a criteria query builder that has spans of time when someone has been employed
    for at least a given amount of time.

    Args:
        criteria_name (str): Criteria query name
        description (str): Criteria query description
        employment_status_values (List[str], optional): List of employment statuses to include in the criteria.
            Example: ["EMPLOYED_UNKNOWN_AMOUNT", "EMPLOYED_FULL_TIME", "EMPLOYED_PART_TIME"].
        date_interval (int, optional): Number of <date_part> when the employment will be counted as
            valid. Defaults to 6.
        date_part (str, optional): Supports any of the BigQuery date_part values:
            "DAY", "WEEK", "MONTH", "QUARTER", or "YEAR". Defaults to "MONTH".
    """
    query_template = status_for_at_least_x_time_criteria_query(
        table_name="{project_id}.normalized_state.state_employment_period",
        additional_where_clause=f"""AND employment_status IN {custom_tuple(employment_status_values)}""",
        date_interval=date_interval,
        date_part=date_part,
        start_date="start_date",
        end_date="DATE_ADD(end_date, INTERVAL 1 DAY)",
        additional_column="employment_status",
    )

    return StateAgnosticTaskCriteriaBigQueryViewBuilder(
        criteria_name=criteria_name,
        criteria_spans_query_template=query_template,
        description=description,
        reasons_fields=[
            ReasonsField(
                name="employment_status",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="employment_status",
            ),
        ],
    )


def housed_for_at_least_x_time_criteria_builder(
    criteria_name: str,
    description: str,
    housing_status_values: List[str],
    date_interval: int = 6,
    date_part: str = "MONTH",
) -> StateAgnosticTaskCriteriaBigQueryViewBuilder:
    """
    Returns a criteria query builder that has spans of time when someone has been housed
    for at least a given amount of time.

    Args:
        criteria_name (str): Criteria query name
        description (str): Criteria query description
        housing_status_values (List[str], optional): List of housing statuses to include in the criteria.
            Example: ["PERMANENT_RESIDENCE", "TEMPORARY_OR_SUPPORTIVE_HOUSING"].
        date_interval (int, optional): Number of <date_part> when the employment will be counted as
            valid. Defaults to 6.
        date_part (str, optional): Supports any of the BigQuery date_part values:
            "DAY", "WEEK", "MONTH", "QUARTER", or "YEAR". Defaults to "MONTH".
    """
    query_template = status_for_at_least_x_time_criteria_query(
        table_name="{project_id}.normalized_state.state_person_housing_status_period",
        additional_where_clause=f"""AND housing_status_type IN {custom_tuple(housing_status_values)}""",
        date_interval=date_interval,
        date_part=date_part,
        start_date="housing_status_start_date",
        end_date="DATE_ADD(housing_status_end_date, INTERVAL 1 DAY)",
        additional_column="housing_status_type",
    )

    return StateAgnosticTaskCriteriaBigQueryViewBuilder(
        criteria_name=criteria_name,
        criteria_spans_query_template=query_template,
        description=description,
        reasons_fields=[
            ReasonsField(
                name="housing_status_type",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="Housing status type",
            ),
        ],
    )


def status_for_at_least_x_time_criteria_query(
    start_date: str,
    end_date: str,
    table_name: str,
    date_interval: int,
    date_part: str = "MONTH",
    additional_where_clause: str = "",
    additional_column: str = "",
) -> str:

    """
    Returns a criteria query builder that has spans of time when someone has been in a
    certain status for at least a given amount of time.

    Args:
        start_date (str): The name of the start date field in the table.
        end_date (str): The name of the end date field in the table.
        table_name (str): The name of the table that contains the status information.
        date_interval (int, optional): Number of <date_part> when the status will be counted as
            valid.
        date_part (str, optional): Supports any of the BigQuery date_part values:
            "DAY", "WEEK", "MONTH", "QUARTER", or "YEAR". Defaults to "MONTH".
        additional_where_clause (str, optional): Any additional WHERE-clause filters for
            selecting statuses. Defaults to "".
        additional_column (str, optional): The name of the column that contains the
    """
    return f"""WITH spans AS (
        -- Spans that are relevant for the criteria
        SELECT *
        FROM (
            SELECT 
                state_code,
                person_id,
                {start_date} AS start_date,
                {end_date} AS end_date,
                state_code AS column_placeholder,
                {additional_column}
            FROM `{table_name}`
            WHERE {start_date} < '9999-01-01'
                {additional_where_clause}
        )
        WHERE start_date < {nonnull_end_date_exclusive_clause('end_date')}
    ),

    {create_sub_sessions_with_attributes('spans')},

    sessions AS (
        -- Aggregate the spans to get non-overlapping sessions
        SELECT 
            state_code,
            person_id,
            start_date,
            end_date,
            {f"STRING_AGG(DISTINCT {additional_column}, ', ' ORDER BY {additional_column}) AS {additional_column}," if additional_column != '' else ''}
        FROM sub_sessions_with_attributes
        GROUP BY 1,2,3,4
    ),
    critical_date_spans AS (
        -- Aggregate adjacent spans and calculate the critical date
        SELECT 
            state_code,
            person_id,
            start_date AS start_datetime,
            end_date AS end_datetime,
            DATE_ADD(start_date, INTERVAL {date_interval} {date_part}) AS critical_date,
            {additional_column}
        FROM ({aggregate_adjacent_spans(
            table_name='sessions',
            end_date_field_name="end_date",
            index_columns=["state_code", "person_id", additional_column]
            )})
    ),
    {critical_date_has_passed_spans_cte(attributes = [additional_column])}
    SELECT
        state_code,
        person_id,
        start_date,
        end_date,
        critical_date_has_passed AS meets_criteria,
        TO_JSON(STRUCT({additional_column} AS {additional_column})) AS reason,
        {additional_column},
    FROM critical_date_has_passed_spans"""
