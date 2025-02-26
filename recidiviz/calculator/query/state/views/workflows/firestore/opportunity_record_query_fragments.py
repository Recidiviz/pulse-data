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
) -> str:
    """
    It joins a task eligibility span view with the state_person_external_id to retrieve external ids.
    It also filters out spans of time that aren't current.

    Returns:
        state_code (str): State code. The final statement will filter out all other states.
        tes_task_query_view (str): The task query view that we're interested in querying.
            E.g. 'work_release_materialized'.
    """
    return f"""SELECT
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
        GROUP BY 1"""


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
