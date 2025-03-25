#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2023 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
" Helper SQL fragments that can be re-used for several opportunity queries."

from recidiviz.calculator.query.bq_utils import (
    nonnull_end_date_exclusive_clause,
    today_between_start_date_and_nullable_end_date_exclusive_clause,
)


def join_current_task_eligibility_spans_with_external_id(
    state_code: str,
    tes_task_query_view: str,
    id_type: str,
    additional_columns: str = "",
    eligible_only: bool = False,
    eligible_and_almost_eligible_only: bool = False,
    almost_eligible_only: bool = False,
) -> str:
    """
    It joins a task eligibility span view with the state_person_external_id to retrieve external ids.
    It also filters out spans of time that aren't current.

    If |eligible_only| is True return only the clients marked currently eligible in the task eligibility span. If
    |eligible_and_almost_eligible_only| is True return only the clients currently marked eligible OR almost eligible.
    Only one argument can be True and an error is thrown if they are both set to True.

    Returns:
        state_code (str): State code. The final statement will filter out all other states.
        tes_task_query_view (str): The task query view that we're interested in querying.
            E.g. 'work_release_materialized'.
    """
    if (
        sum([eligible_only, eligible_and_almost_eligible_only, almost_eligible_only])
        > 1
    ):
        raise ValueError(
            f"Only one of |eligible_only|, |eligible_and_almost_eligible_only|, or |almost_eligible_only| can be True for [{tes_task_query_view}]"
        )
    eligible_condition = ""
    if eligible_only:
        eligible_condition = "AND tes.is_eligible"
    elif eligible_and_almost_eligible_only:
        eligible_condition = "AND (tes.is_eligible OR tes.is_almost_eligible)"
    elif almost_eligible_only:
        eligible_condition = "AND tes.is_almost_eligible"

    return f"""SELECT
        pei.external_id,
        tes.person_id,
        tes.state_code,
        tes.reasons,
        tes.ineligible_criteria,
        tes.is_eligible,
        tes.is_almost_eligible,
        {additional_columns}
    FROM `{{project_id}}.{{task_eligibility_dataset}}.{tes_task_query_view}` tes
    LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
        USING(person_id)
    WHERE 
      CURRENT_DATE('US/Pacific') BETWEEN tes.start_date AND 
                                         {nonnull_end_date_exclusive_clause('tes.end_date')}
      AND tes.state_code = {state_code}
      AND pei.id_type = {id_type}
      {eligible_condition}
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
            TO_JSON(ARRAY_AGG(
                STRUCT(note_title, note_body, event_date, criteria)
                ORDER BY event_date, note_title, note_body, criteria
            )) AS case_notes,
        FROM {from_cte}
        LEFT JOIN {left_join_cte}
            USING(external_id)
        WHERE criteria IS NOT NULL
        GROUP BY 1"""


def opportunity_query_final_select_with_case_notes(
    from_cte: str = "eligible_and_almost_eligible",
    left_join_cte: str = "array_case_notes_cte",
    additional_columns: str = "",
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
        is_eligible,
        is_almost_eligible,
        ineligible_criteria,
        case_notes, {additional_columns}
    FROM {from_cte}
    LEFT JOIN {left_join_cte}
        USING(external_id)
  """


def current_employment_case_notes(state_code: str) -> str:
    """Returns a CTE containing all current employment periods as case notes.

    Args:
        state_code (str): State code. The final statement will filter out all other states.
    """

    return f"""    SELECT
            external_id,
            "Current Employment" AS criteria,
            employment_status_raw_text AS note_title,
            CONCAT("Employer: ", 
                    employer_name,
                    ' - ',
                    "Job title: ",
                    job_title) AS note_body,
            start_date AS event_date,
        FROM `{{project_id}}.{{normalized_state_dataset}}.state_employment_period`
        WHERE state_code = '{state_code}'
            AND {today_between_start_date_and_nullable_end_date_exclusive_clause('start_date', 
                                                                                 'end_date')}
"""


def current_violent_statutes_being_served(state_code: str) -> str:
    """Returns a CTE containing all current violent statutes being served ready to be
        displayed as case notes.

    Args:
        state_code (str): State code. The final statement will filter out all other states.
    """

    return f"""
        WITH statutes_with_descriptions AS (
            SELECT 
                DISTINCT
                state_code,
                statute,
                description
            FROM `{{project_id}}.{{normalized_state_dataset}}.state_charge` 
            WHERE state_code = '{state_code}'
            )

        SELECT 
            pei.external_id,
            'Violent offenses currently serving' AS criteria,
            statute AS note_title,
            description AS note_body,
            vo.start_date AS event_date,
        FROM `{{project_id}}.{{task_eligibility_criteria_dataset}}.not_serving_for_violent_offense_materialized` vo,
        UNNEST(JSON_VALUE_ARRAY(reason.ineligible_offenses)) AS statute
        LEFT JOIN statutes_with_descriptions
            USING(state_code, statute)
        LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
            USING(person_id)
        WHERE vo.state_code = '{state_code}'
            AND CURRENT_DATE('US/Eastern') BETWEEN start_date AND {nonnull_end_date_exclusive_clause('end_date')}"""


def current_snooze(
    state_code: str,
    opportunity_type: str,
) -> str:
    """Returns a CTE containing the most recent snooze data per person for specified opportunity.

    Args:
        state_code (str): State code. The final statement will filter out all other states.
        opportunity_type (str): Opportunity for which we are gathering snooze data, defined in case note metadata.
    """

    return f"""    SELECT
                person_id,
                SAFE.PARSE_JSON(note) AS metadata_denial,
                person_external_id as external_id,
                Note_Date as contact_date
            FROM `{{project_id}}.{{supplemental_dataset}}.us_me_snoozed_opportunities`
            WHERE state_code = '{state_code}'
                AND is_valid_snooze_note
                AND JSON_VALUE(note, '$.opportunity_type') = '{opportunity_type}'
            QUALIFY 
                ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY Note_Date DESC) = 1
    """
