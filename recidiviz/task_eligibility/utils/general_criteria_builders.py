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
"""Helper methods that return criteria view builders with similar logic that
can be parameterized.
"""
from typing import List, Optional, Union

from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import (
    list_to_query_string,
    nonnull_end_date_clause,
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
from recidiviz.calculator.query.state.views.sessions.state_sentence_configurations import (
    STATES_WITH_NO_INCARCERATION_SENTENCES_ON_SUPERVISION,
    STATES_WITH_NO_INFERRED_OPEN_SPANS,
)
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
    StateSpecificTaskCriteriaBigQueryViewBuilder,
    TaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
)
from recidiviz.task_eligibility.utils.state_dataset_query_fragments import (
    supervision_violations_cte,
)


def at_least_X_time_since_drug_screen(
    criteria_name: str,
    date_interval: int,
    date_part: str = "MONTH",
    where_clause: str = "",
    meets_criteria_during_time_frame: bool = False,
) -> StateAgnosticTaskCriteriaBigQueryViewBuilder:
    """
    The function is designed to help identify and manage periods where individuals meet
    or do not meet certain criteria based on their drug screen history.

    Args:
        criteria_name (str): Name of the criteria
        date_interval (int): Number of <date_part> when the drug screen
            will be counted as valid.
        date_part (str): Supports any of the BigQuery date_part values:
            "DAY", "WEEK","MONTH","QUARTER","YEAR". Defaults to "MONTH".
        where_clause (str): Additional WHERE clause to filter the drug screens. e.g. "WHERE is_positive_result"
            to filter on positive results only or "WHERE not is_positive_result" to filter on negative results only
        meets_criteria_during_time_frame (bool): If True, the criteria is met when the
            person has a recent drug screen within the time frame. If False, the criteria is
            met when the person does not have a recent drug screen within the time frame.
    Returns:
        StateAgnosticTaskCriteriaBigQueryViewBuilder: A builder object for the criteria view
    """

    query_template = f"""WITH drug_test_sessions_cte AS
    (
        SELECT
            state_code,
            person_id,
            drug_screen_date AS start_date,
            DATE_ADD(drug_screen_date, INTERVAL {date_interval} {date_part}) AS end_date,
            {meets_criteria_during_time_frame} AS meets_criteria,
            drug_screen_date AS latest_drug_screen_date,
        FROM
            `{{project_id}}.{{sessions_dataset}}.drug_screens_preprocessed_materialized`
        {where_clause}
    )
    ,
    /*
    If a person has more than 1 positive test in an X month period, they will have overlapping sessions
    created in the above CTE. Therefore we use `create_sub_sessions_with_attributes` to break these up
    */
    {create_sub_sessions_with_attributes('drug_test_sessions_cte')}
    ,
    dedup_cte AS
    /*
    If a person has more than 1 relevant test in an X month period, they will have duplicate sub-sessions for 
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
    Sessionize so that we have continuous periods of time for which a person is/is not eligible due to a relevant test. A
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
        -- Note, this criteria builder actually works for any specified relevant test - positive or negative - 
        -- so this reason name is not 100% accurate. 
        -- TODO(#39094): update reasons blob and ensure downstream references don't break
        TO_JSON(STRUCT(latest_drug_screen_date AS most_recent_positive_test_date)) AS reason,
        latest_drug_screen_date AS most_recent_positive_test_date,
    FROM sessionized_cte
    """

    return StateAgnosticTaskCriteriaBigQueryViewBuilder(
        criteria_name=criteria_name,
        criteria_spans_query_template=query_template,
        description=__doc__,
        sessions_dataset=SESSIONS_DATASET,
        meets_criteria_default=not meets_criteria_during_time_frame,
        reasons_fields=[
            ReasonsField(
                name="most_recent_positive_test_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Date of most recent positive drug test",
            )
        ],
    )


def latest_drug_test_is_negative(
    criteria_name: str,
    description: str,
    meets_criteria_default: bool = False,
) -> StateAgnosticTaskCriteriaBigQueryViewBuilder:
    """
    Returns a criteria query builder that has spans of time when a client's latest drug screen is negative.
    The logic looks at all drug screens over all time, so someone could return to supervision after a period on liberty
    and if their last drug test on a prior period was positive, they wouldn't meet this criteria.

    Args:
        criteria_name (str): Criteria query name
        description (str): Criteria query description
        meets_criteria_default (bool): Determines whether people who don't have a drug screen are considered eligible
            or not
    """

    criteria_query = """
        WITH screens AS (
        SELECT
            state_code,
            person_id,
            drug_screen_date AS start_date,
            LEAD(drug_screen_date) OVER(PARTITION BY person_id ORDER BY drug_screen_date ASC) AS end_date,
            NOT is_positive_result AS meets_criteria,
            result_raw_text_primary AS latest_drug_screen_result,
            drug_screen_date AS latest_drug_screen_date,
        FROM
            (
                SELECT *
                FROM `{project_id}.{sessions_dataset}.drug_screens_preprocessed_materialized`
                -- The preprocessed view can have multiple tests per person-day if there are different sample types.
                -- For the purposes of this criteria we just want to keep 1 test per person-day and prioritize positive
                -- results
                QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id, drug_screen_date ORDER BY is_positive_result DESC,
                                                                                            sample_type) = 1
            )
    )
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date,
        meets_criteria,
        TO_JSON(STRUCT(
            latest_drug_screen_result AS latest_drug_screen_result,
            latest_drug_screen_date AS latest_drug_screen_date
        )) AS reason,
        latest_drug_screen_result,
        latest_drug_screen_date,
    FROM screens
    """

    return StateAgnosticTaskCriteriaBigQueryViewBuilder(
        criteria_name=criteria_name,
        description=description,
        criteria_spans_query_template=criteria_query,
        meets_criteria_default=meets_criteria_default,
        sessions_dataset=SESSIONS_DATASET,
        reasons_fields=[
            ReasonsField(
                name="latest_drug_screen_result",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="Result of latest drug screen",
            ),
            ReasonsField(
                name="latest_drug_screen_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Date of latest drug screen",
            ),
        ],
    )


def raise_error_if_invalid_compartment_level_1_filter(
    compartment_level_1_filter: None | str,
) -> None:
    """Raises a ValueError if the compartment_level_1_filter is not valid"""

    if compartment_level_1_filter:
        compartment_level_1 = compartment_level_1_filter.upper()

        if compartment_level_1 not in ("SUPERVISION", "INCARCERATION"):
            raise ValueError(
                "'compartment_level_1_filter' only accepts two values: `SUPERVISION` or `INCARCERATION`"
            )


def get_ineligible_offense_type_criteria(
    criteria_name: str,
    compartment_level_1: Union[str, List[str]],
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
    state_code: StateCode | None = None,
    description: str,
    date_interval: int,
    date_part: str,
    violation_type: str = "",
    where_clause_addition: str = "",
    violation_date_name_in_reason_blob: str = "latest_violations",
    exclude_violation_unfounded_decisions: bool = False,
    use_response_date: bool = False,
) -> (
    StateAgnosticTaskCriteriaBigQueryViewBuilder
    | StateSpecificTaskCriteriaBigQueryViewBuilder
):
    """
    Returns a TES criterion view builder that has spans of time where violations that
    meet certain conditions set by the user have occurred within some specified window
    of time (e.g., within the past 6 months).
    Args:
        criteria_name (str): Name of the criterion.
        state_code (StateCode): The state code for this criterion, if it contains
            state-specific logic.
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
        exclude_violation_unfounded_decisions (bool, optional): Whether to exclude violations where the LATEST
            violation response DOES NOT contain a VIOLATION_UNFOUNDED decision, indicating that the violation is unfounded
        use_response_date (bool, optional): Whether to use the response date rather than the violation date when determining
            eligibility. Defaults to False
    Returns:
        Either a state-specific or state-agnostic TES criterion view builder that shows
        the spans of time where the violations that meet any condition(s) set by the
        user have occurred (<violation_type> and <where_clause_addition>). The span of
        time for the validity of each violation starts at `violation_date` and ends
        after a period specified by the user (<date_interval> and <date_part>).
    """

    # TODO(#35354): Account for violation decisions when considering which violations
    # should disqualify someone from eligibility.
    criteria_query = f"""
    WITH supervision_violations AS (
        {supervision_violations_cte(
        violation_type,
        where_clause_addition,
        exclude_violation_unfounded_decisions,
        use_response_date
        )}
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
            ARRAY_AGG(DISTINCT violation_date IGNORE NULLS ORDER BY violation_date DESC) AS {violation_date_name_in_reason_blob},
            MAX(violation_expiration_date) AS violation_expiration_date
        )) AS reason,
        ARRAY_AGG(DISTINCT violation_date IGNORE NULLS ORDER BY violation_date DESC) AS {violation_date_name_in_reason_blob},
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
    if state_code:
        return StateSpecificTaskCriteriaBigQueryViewBuilder(
            criteria_name=criteria_name,
            state_code=state_code,
            description=description,
            criteria_spans_query_template=criteria_query,
            meets_criteria_default=True,
            reasons_fields=reasons_fields,
        )

    return StateAgnosticTaskCriteriaBigQueryViewBuilder(
        criteria_name=criteria_name,
        description=description,
        criteria_spans_query_template=criteria_query,
        meets_criteria_default=True,
        reasons_fields=reasons_fields,
    )


def incarceration_incidents_within_time_interval_criteria_builder(
    *,
    criteria_name: str,
    state_code: Optional[StateCode] = None,
    description: str,
    date_interval: int,
    date_part: str,
    where_clause_addition: Optional[str] = None,
    incident_date_name_in_reason_blob: str = "latest_incidents",
) -> (
    StateAgnosticTaskCriteriaBigQueryViewBuilder
    | StateSpecificTaskCriteriaBigQueryViewBuilder
):
    """
    Returns a TES criterion view builder that has spans of time where incidents that
    meet certain conditions set by the user have occurred within some specified window
    of time (e.g., within the past 6 months).

    Args:
        criteria_name (str): Name of the criterion.
        state_code (StateCode): The state code for this criterion, if it contains state-
            specific logic.
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
        Either a state-specific or state-agnostic TES criterion view builder that shows
        the spans of time where the incidents that meet any condition(s) set by the user
        (<where_clause_addition>) occurred. The span of time for the validity of each
        incident starts at `incident_date` and ends after a period specified by the user
        (<date_interval> and <date_part>).
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
            {where_clause_addition if where_clause_addition is not None else ""}
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

    reasons_fields = [
        ReasonsField(
            name=incident_date_name_in_reason_blob,
            type=bigquery.enums.StandardSqlTypeNames.ARRAY,
            description="Date(s) when the incident(s) occurred",
        ),
    ]

    if state_code:
        return StateSpecificTaskCriteriaBigQueryViewBuilder(
            criteria_name=criteria_name,
            state_code=state_code,
            description=description,
            criteria_spans_query_template=criteria_query,
            meets_criteria_default=True,
            reasons_fields=reasons_fields,
        )

    return StateAgnosticTaskCriteriaBigQueryViewBuilder(
        criteria_name=criteria_name,
        description=description,
        criteria_spans_query_template=criteria_query,
        meets_criteria_default=True,
        reasons_fields=reasons_fields,
    )


def incarceration_sanctions_or_incidents_within_time_interval_criteria_builder(
    criteria_name: str,
    description: str,
    date_interval: int,
    date_part: str,
    additional_excluded_outcome_types: Optional[Union[str, List[str]]] = None,
    incident_severity: Optional[Union[str, List[str]]] = None,
    event_column: str = "outcome.date_effective",
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
        event_column (str, optional): Specifies which field defines the event date—typically outcome.date_effective
            (when the sanction was assigned) or incident.incident_date (when the incident occurred). Defaults to
            outcome.date_effective.
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
                {event_column} as event_date,
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


# TODO(#46236): Revisit how this builder handles sentences of different types depending
# on the `compartment_level_1_filter` parameter.
def is_past_completion_date_criteria_builder(
    *,
    criteria_name: str,
    description: str,
    meets_criteria_leading_window_time: int = 0,
    compartment_level_1_filter: Optional[str] = None,
    date_part: str = "DAY",
    critical_date_name_in_reason: str = "eligible_date",
    critical_date_column: str = "sentence_projected_full_term_release_date_max",
    negate_critical_date_has_passed: bool = False,
    leave_last_sentence_span_open: bool = False,
    sentence_sessions_dataset: str = "sentence_sessions",
    allow_past_critical_date: bool = True,
    null_magic_future_date: bool = False,
    meets_criteria_default: bool = False,
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
            critical date. Defaults to "sentence_projected_full_term_release_date_max".
        negate_critical_date_has_passed (bool, optional): If True, the critical date has
            passed will be negated. This means the periods where this date has passed
            will become False. Defaults to False.
        leave_last_sentence_span_open (bool, optional): If True, the most recent
            sentence spans are left open in order to cover cases where an open incarceration
            or supervision session has no overlapping sentence span. The projected dates from
            the latest (non-overlapping) sentence span will be applied. Defaults to False.
        sentence_sessions_dataset (str, optional): The dataset that contains the sentence
            sessions. Defaults to "sentence_sessions", but can be set to "sentence_sessions_v2_all"
            for states that want to use the new sentence sessions but have yet to fully migrate.
        allow_past_critical_date (bool, optional): If True, the critical date can be in the past.
            Defaults to True. If False, spans are cropped so that the end_date of a span is the
            LEAST of the critical date and the sentence session end_date.
        null_magic_future_date (bool, optional): If True, critical_dates in the year 2999 and beyond will be set to NULL.
            This is useful for when we are looking for spans of time where the critical date
            has passed, but we do not want to include spans where the critical date is a magic future date.
        meets_criteria_default (bool, optional): Default value for the meets_criteria column.
            Defaults to False.
    Raises:
        ValueError: if compartment_level_1_filter is different from "supervision" or
            "incarceration".
    Returns:
        StateAgnosticTaskCriteriaBigQueryViewBuilder: criteria query that has spans of
            time when the projected completion date has passed or is coming up while
            someone is on supervision or incarceration
    """
    raise_error_if_invalid_compartment_level_1_filter(compartment_level_1_filter)

    if compartment_level_1_filter and ("group" in critical_date_column):
        raise ValueError(
            f"Compartment filter {compartment_level_1_filter} cannot be set for the group-level sentence date {critical_date_column}"
        )

    excluded_incarceration_states = list_to_query_string(
        string_list=STATES_WITH_NO_INCARCERATION_SENTENCES_ON_SUPERVISION,
        quoted=True,
    )
    no_inferred_open_span_states = list_to_query_string(
        string_list=STATES_WITH_NO_INFERRED_OPEN_SPANS,
        quoted=True,
    )
    # Transform compartment_level_1_filter to a string to be used in the query
    if compartment_level_1_filter == "INCARCERATION":
        query_where_clause_str = "WHERE sentence_type IN ('STATE_PRISON')"
    elif compartment_level_1_filter == "SUPERVISION":
        query_where_clause_str = f"""
        WHERE (state_code NOT IN ({excluded_incarceration_states})
        OR
        sentence_type IN ('PAROLE','PROBATION'))
        """
    else:
        query_where_clause_str = ""

    if leave_last_sentence_span_open:
        end_date_str = f"""
                IF(
                    state_code NOT IN ({no_inferred_open_span_states}) 
                    AND start_date = MAX(start_date) OVER (PARTITION BY state_code, person_id)
                    AND {nonnull_end_date_exclusive_clause('end_date_exclusive')}<=CURRENT_DATE('US/Eastern'),
                    NULL,
                    end_date_exclusive
                ) AS end_date_exclusive
                """
    else:
        end_date_str = "end_date_exclusive"

    # TODO(#28370) revert once normalization fix is in
    if null_magic_future_date:
        # If magic future date is allowed, we can use the critical date column directly
        critical_date_column_preprocessed = f"IF(EXTRACT (YEAR FROM {critical_date_column})=9999, NULL, {critical_date_column}) AS {critical_date_column}"
    else:
        critical_date_column_preprocessed = critical_date_column

    # TODO(#41848): move this to a separate helper and/or refactor the critical date helper
    if allow_past_critical_date:
        final_end_date_condition = "end_date"
    else:
        # If past critical dates are not allowed and the span started before
        # the critical date, then crop the criteria span end on the critical date
        final_end_date_condition = f"""
            CASE WHEN start_date <= {nonnull_end_date_clause('critical_date')}
                THEN LEAST({nonnull_end_date_clause('end_date')}, {nonnull_end_date_clause('critical_date')})
                ELSE end_date
            END"""

    meets_criteria_condition = "critical_date_has_passed"
    if negate_critical_date_has_passed:
        meets_criteria_condition = "NOT " + meets_criteria_condition
    if not allow_past_critical_date:
        meets_criteria_condition = (
            meets_criteria_condition
            + f" AND start_date < {nonnull_end_date_clause('critical_date')}"
        )

    criteria_query = f"""
    WITH sentences AS
    (
    SELECT
        state_code,
        person_id,
        start_date,
        {end_date_str},
        {critical_date_column_preprocessed},
        is_life,
        FROM `{{project_id}}.{sentence_sessions_dataset}.person_projected_date_sessions_materialized`,
        UNNEST(sentence_array)
        JOIN `{{project_id}}.{sentence_sessions_dataset}.sentences_and_charges_materialized`
            USING(state_code, person_id, sentence_id)
        {query_where_clause_str}
    )
    ,
    critical_date_spans AS 
    (
    SELECT
        state_code,
        person_id,
        start_date AS start_datetime,
        end_date_exclusive AS end_datetime,
        {revert_nonnull_end_date_clause(f"MAX(IF(is_life, {nonnull_end_date_clause(critical_date_column)}, {critical_date_column}))")} AS critical_date,
        FROM sentences
        GROUP BY 1,2,3,4
    ),
    {critical_date_has_passed_spans_cte(meets_criteria_leading_window_time = meets_criteria_leading_window_time,
                                        date_part=date_part)}
    SELECT
        state_code,
        person_id,
        start_date,
        {final_end_date_condition} AS end_date,
        {meets_criteria_condition} AS meets_criteria,
        TO_JSON(STRUCT(critical_date AS {critical_date_name_in_reason})) AS reason,
        critical_date AS {critical_date_name_in_reason},
    FROM critical_date_has_passed_spans
    -- Drop zero day spans
    WHERE start_date != {nonnull_end_date_clause(final_end_date_condition)}
    """

    return StateAgnosticTaskCriteriaBigQueryViewBuilder(
        criteria_name=criteria_name,
        criteria_spans_query_template=criteria_query,
        description=description,
        meets_criteria_default=meets_criteria_default,
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
    compartment_level_1_filter: str = "",
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
        compartment_level_1_filter (str): The compartment level 1 filter to apply to the
            absconsion sessions. Defaults to "".
    """
    if compartment_level_1_filter != "":
        raise_error_if_invalid_compartment_level_1_filter(compartment_level_1_filter)

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
        {"AND compartment_level_1 = '" + compartment_level_1_filter + "'" if compartment_level_1_filter else ""}
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
        # TODO(#38963): Remove the end_date < '3000-01-01' once we are enforcing that
        #  employment period end dates are reasonable and all exemptions have been
        #  resolved. This filter was added to avoid date overflow when adding time to
        #  dates close to the max date 9999-12-31.
        additional_where_clause=f"""
            AND employment_status IN {custom_tuple(employment_status_values)} 
            # If end_date is more than 3000-01-01, drop period. Don't drop NULL end_dates
            AND (end_date IS NULL OR end_date < '3000-01-01')""",
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
    left_join_statement = (
        f"""LEFT JOIN sessions ses ON adj.state_code = ses.state_code
                AND adj.person_id = ses.person_id
                AND adj.start_date < {nonnull_end_date_clause('ses.end_date')}
                AND ses.start_date < {nonnull_end_date_clause('adj.end_date')}"""
        if additional_column != ""
        else ""
    )

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
            adj.state_code,
            adj.person_id,
            adj.start_date AS start_datetime,
            adj.end_date AS end_datetime,
            DATE_ADD(adj.start_date, INTERVAL {date_interval} {date_part}) AS critical_date,
            {f"STRING_AGG(DISTINCT ses.{additional_column}, ', ' ORDER BY ses.{additional_column}) AS {additional_column}," if additional_column != '' else ''}
        FROM ({aggregate_adjacent_spans(
            table_name='sessions',
            end_date_field_name="end_date",
            )}) AS adj
        {left_join_statement if additional_column != '' else ''}

        GROUP BY 1,2,3,4
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


def get_reason_json_fields_query_template_for_criteria(
    criteria_builder: StateSpecificTaskCriteriaBigQueryViewBuilder
    | StateAgnosticTaskCriteriaBigQueryViewBuilder,
) -> str:
    """Returns a query template that extracts all json fields from a criteria builder"""
    return ",\n".join(
        [
            f"JSON_EXTRACT_SCALAR(reason_v2, '$.{field.name}') AS {field.name}"
            for field in criteria_builder.reasons_fields
        ]
    )


def on_negative_drug_screen_streak(
    criteria_name: str,
    date_interval: int,
    date_part: str = "MONTH",
) -> StateAgnosticTaskCriteriaBigQueryViewBuilder:
    """
    Creates spans showing when someone has had entirely negative drug screens for at
    least X time, and has had a negative drug screen at least every year.

    The logic tracks periods when:
    1. Someone has had a negative drug screen
    2. At least X time has passed since the streak of negative drug screens began
    3. There has been at least one negative drug test

    This creates historical spans that can start and end based on drug screen patterns.
    For example: positive in Jan, negative in Feb -> eligible span starts in April.
    If positive again in June -> span ends in June. If negative in July -> new span starts in September.

    Args:
        criteria_name (str): Name of the criteria
        date_interval (int): Number of <date_part> that must pass since first negative
        date_part (str): Supports any of the BigQuery date_part values:
            "DAY", "WEEK","MONTH","QUARTER","YEAR". Defaults to "MONTH".
    Returns:
        StateAgnosticTaskCriteriaBigQueryViewBuilder: A builder object for the criteria view
    """

    query_template = f"""
    WITH
    -- This CTE collapses all drug screens on a given date together and flags if any
    -- were positive
    any_positive_drug_screens AS (
        SELECT
            state_code,
            person_id,
            drug_screen_date,
            LOGICAL_OR(is_positive_result) AS is_positive_result
        FROM `{{project_id}}.{{sessions_dataset}}.drug_screens_preprocessed_materialized`
        GROUP BY state_code, person_id, drug_screen_date
    ),
    -- This CTE adds an end date which is either a year out, or at the next drug screen
    -- date, whatever is closer. This effectively means your streak can only continue if
    -- you are getting at least one negative drug screen each year
    test_result_periods AS (
        SELECT
            state_code,
            person_id,
            drug_screen_date AS start_date,
            -- This period ends at the next test date OR a year later
            LEAST({nonnull_end_date_clause('LEAD(drug_screen_date) OVER (PARTITION BY person_id ORDER BY drug_screen_date ASC)')}, drug_screen_date + INTERVAL 1 YEAR) AS potential_end_date,
            is_positive_result
        FROM any_positive_drug_screens
    ),
    sessionized AS (
        {aggregate_adjacent_spans(
            table_name='test_result_periods',
            attribute=['is_positive_result'],
            end_date_field_name='potential_end_date'
        )}
    ),
    -- Adjust start times in case we want to wait a few months before a streak is
    -- considered valid, and drop periods which no longer are valid (start date after
    -- end date -- these will be autopopulated with spans of meets_criteria = <default>
    -- in the final query)
    streaks AS (
        SELECT 
            * EXCEPT (start_date), 
            start_date AS drug_test_date,
            NOT is_positive_result AS meets_criteria,
        CASE WHEN
            NOT is_positive_result THEN start_date + INTERVAL {date_interval} {date_part}
            ELSE start_date 
        END AS start_date,
        FROM sessionized
        WHERE start_date + INTERVAL {date_interval} {date_part} < {nonnull_end_date_clause('potential_end_date')}
    ),
    -- At this point we have the test_result_periods which indicate periods during which
    -- the most recent test was positive or negative, and we have streaks which define
    -- continuous periods of negative drug screens. Now we will create periods during
    -- which someone is on a streak, and include information about when the streak
    -- began, and also when the last negative drug test was.

    -- It's possible (likely) that a streak starts on a date where there wasn't actually
    -- a test, so we union in an extra record per streak which will account for the
    -- beginning span of the streak.
    trp_and_streak_starts AS (
        SELECT state_code, person_id, start_date, potential_end_date, FALSE AS streak_start_record
        FROM test_result_periods

        UNION ALL

        SELECT state_code, person_id, start_date, NULL AS end_date, TRUE AS streak_start_record
        FROM streaks s
    ),
    -- Join in the information from the streaks back into the periods
    joined_streaks AS (
        SELECT DISTINCT
            trpss.state_code,
            trpss.person_id,
            trpss.streak_start_record,
            trpss.start_date,
            trpss.potential_end_date,
            meets_criteria,
            IF(
                trpss.streak_start_record, 
                LAG(trpss.start_date) OVER (PARTITION BY trpss.person_id ORDER BY trpss.start_date, streak_start_record),
                trpss.start_date
            ) AS most_recent_test_date,
            s.start_date - INTERVAL {date_interval} {date_part} AS first_negative_test_in_streak
        FROM trp_and_streak_starts trpss
        LEFT JOIN streaks s ON 
            trpss.person_id = s.person_id AND 
            trpss.start_date >= s.start_date AND 
            trpss.start_date < COALESCE(s.potential_end_date, DATE('9999-01-01'))
        ORDER BY trpss.start_date DESC
    ),
    -- Earlier, when we joined in the streak_start_record's, we introduced a period with
    -- an open end date. Here, we set the end date of the start record to either the
    -- next test date (if there is one), or a year after the most recent test date.
    joined_streaks_with_end_dates as (
        SELECT 
            * EXCEPT (potential_end_date), 
            IFNULL(LEAD(start_date) OVER (PARTITION BY person_id ORDER BY start_date, streak_start_record), most_recent_test_date + INTERVAL 1 YEAR) AS end_date
        FROM joined_streaks
    )
    SELECT *, TO_JSON(STRUCT(first_negative_test_in_streak, most_recent_test_date)) AS reason
    FROM joined_streaks_with_end_dates
    """

    return StateAgnosticTaskCriteriaBigQueryViewBuilder(
        criteria_name=criteria_name,
        criteria_spans_query_template=query_template,
        description="Creates spans showing when someone has had entirely negative drug screens for at least X time",
        sessions_dataset=SESSIONS_DATASET,
        meets_criteria_default=False,
        reasons_fields=[
            ReasonsField(
                name="first_negative_test_in_streak",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Date of the first negative result in this streak",
            ),
            ReasonsField(
                name="most_recent_test_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="The most recent negative result at this point in time",
            ),
        ],
    )


def not_on_specific_supervision_case_type(
    case_type: StateSupervisionCaseType,
) -> StateAgnosticTaskCriteriaBigQueryViewBuilder:
    """
    Creates criteria with spans of time when a client is supervised on a specific case
    type in order to support "SUPERVISION_CASE_TYPE_IS_NOT_XX" style criteria.
    """

    vb_description = f"""Defines a criteria span view that shows spans of time during which clients does not have
    a supervision case type of "{case_type.name}"."""

    query_template = f"""
    WITH case_type_spans AS (
    /* pull spans of time where a client has a sex offense case type */
        SELECT sp.state_code,
          sp.person_id,
          sp.start_date,
          sp.termination_date AS end_date,
          sc.case_type_raw_text,
        FROM `{{project_id}}.{{normalized_state_dataset}}.state_supervision_period` sp
        INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_supervision_case_type_entry` sc
          USING(person_id, supervision_period_id)
        WHERE sc.case_type = '{case_type.name}'
          AND start_date IS DISTINCT FROM termination_date -- exclude 0-day spans
    ),
    /* sub-sessionize and aggregate for cases where a client has multiple case types at once */
    {create_sub_sessions_with_attributes('case_type_spans')}
    SELECT state_code,
      person_id,
      start_date,
      end_date,
      FALSE AS meets_criteria,
      ARRAY_AGG(DISTINCT case_type_raw_text ORDER BY case_type_raw_text) AS raw_{case_type.name.lower()}_case_types,
      TO_JSON(STRUCT(
        ARRAY_AGG(DISTINCT case_type_raw_text ORDER BY case_type_raw_text) AS raw_{case_type.name.lower()}_case_types
      )) AS reason,
    FROM sub_sessions_with_attributes
    GROUP BY 1, 2, 3, 4
    """

    return StateAgnosticTaskCriteriaBigQueryViewBuilder(
        criteria_name=f"SUPERVISION_CASE_TYPE_IS_NOT_{case_type.name}",
        description=vb_description,
        criteria_spans_query_template=query_template,
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
        meets_criteria_default=True,
        reasons_fields=[
            ReasonsField(
                name=f"raw_{case_type.name.lower()}_case_types",
                type=bigquery.enums.StandardSqlTypeNames.ARRAY,
                description=f"Array of raw case type values for all {case_type.name} case types that a client is assigned during a given period.",
            ),
        ],
    )
