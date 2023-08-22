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
"""
Helper SQL queries for Idaho
"""
from typing import List

from recidiviz.calculator.query.bq_utils import (
    nonnull_end_date_clause,
    nonnull_start_date_clause,
)
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
)

IX_CRC_FACILITIES = [
    "Nampa Community Reentry Center",
    "East Boise Community Reentry Center",
    "Idaho Falls Community Reentry Center",
    "Twin Falls Community Work Center",
    "Treasure Valley Community Reentry Center",
]

DETAINER_TYPE_LST = ["59"]
HOLD_TYPE_LST = ["84", "112", "85", "61", "87", "86", "62", "88", "67", "83", "113"]


def date_within_time_span(
    meets_criteria_leading_window_days: int = 0, critical_date_column: str = ""
) -> str:
    """
    Generates a BigQuery SQL query that uses <critical_date_has_passed_spans> to
    determine if a critical date has passed within the specified leading window.
    This is all done using the <us_ix_parole_dates_spans_preprocessing> table.

    Args:
        meets_criteria_leading_window_days (int, optional): The leading window of days
            used to determine if a critical date falls within. Default is 0.
        critical_date_column (str, optional): The column representing the critical date
            to be checked within the time spans. Default is an empty string.

    Returns:
        str: A formatted BigQuery SQL query that selects time spans and determines if the
        critical date has passed within the specified leading window.

    Example usage:
        query = date_within_time_span(meets_criteria_leading_window_days=5, critical_date_column="custom_date")
        # Execute the generated query using your preferred method
    """
    return f"""
    WITH
      critical_date_spans AS (
          SELECT
            state_code,
            person_id,
            {nonnull_start_date_clause('start_date')} as start_datetime,
            end_date as end_datetime,
            {critical_date_column} AS critical_date
          FROM
            `{{project_id}}.{{analyst_dataset}}.us_ix_parole_dates_spans_preprocessing_materialized`
          WHERE {critical_date_column} IS NOT NULL 
            ),
      {critical_date_has_passed_spans_cte(meets_criteria_leading_window_days)},
      {create_sub_sessions_with_attributes('critical_date_has_passed_spans')}
    SELECT
      state_code,
      person_id,
      start_date,
      end_date,
      LOGICAL_AND(critical_date_has_passed) AS meets_criteria,
      TO_JSON(STRUCT(MAX(critical_date) AS {critical_date_column})) AS reason,
    FROM sub_sessions_with_attributes
    GROUP BY 1,2,3,4
    """


def ix_crc_facilities_in_location_sessions(
    crc_facilities_list: list,
    additional_columns: str = "",
) -> str:
    """
    Returns a SQL query that returns spans of time where someone is in a CRC facility in Idaho.

    Args:
        crc_facilities (str): String of CRC facilities to query.
        additional_columns (str): Additional columns to select. Defaults to ''.

    Returns:
        str: SQL query as a string.
    """

    crc_facilities_str = "('" + "', '".join(crc_facilities_list) + "')"

    return f"""SELECT
    state_code,
    person_id,
    start_date,
    end_date_exclusive AS end_date,
    {additional_columns if additional_columns else ''}
FROM `{{project_id}}.{{sessions_dataset}}.location_sessions_materialized` 
WHERE state_code = 'US_IX'
    AND facility_name IN {crc_facilities_str}"""


def detainer_span(types_to_include_lst: List[str]) -> str:
    """
    Retrieves detainer spans information based on the specified detainer type IDs.

    Args:
        types_to_include_lst (List[str]): A list of detainer type IDs to include in the query.

    Returns:
        str: SQL query string for retrieving detainer spans with the specified detainer types.

    Example:
        types_to_include = ['73', '23']
        query = detainer_span(types_to_include)
    """
    reformatted_types_to_include_lst = "('" + "', '".join(types_to_include_lst) + "')"
    return f"""WITH
    detainer_cte AS (
        SELECT *
        FROM `{{project_id}}.{{analyst_dataset}}.us_ix_detainer_spans_materialized`
        WHERE DetainerTypeId IN {reformatted_types_to_include_lst}),
          {create_sub_sessions_with_attributes('detainer_cte')}
SELECT
    state_code,
    person_id,
    start_date,
    end_date,
    False as meets_criteria,
    TO_JSON(STRUCT(start_date AS latest_detainer_start_date,
        DetainerTypeDesc AS latest_detainer_type,
        DetainerStatusDesc AS latest_detainer_status)) AS reason,
FROM
    sub_sessions_with_attributes
QUALIFY ROW_NUMBER() OVER(PARTITION BY state_code, person_id, start_date, end_date ORDER BY start_date DESC)=1
    """


def ix_offender_alerts_case_notes(
    date_part: str = "MONTH", date_interval: str = "6", where_clause: str = ""
) -> str:
    """
    Returns a SQL query that returns case notes for offender alerts in Idaho.
    Args:
        date_part (str): Date part to use for the date interval. Defaults to 'MONTH'.
        date_interval (str): Date interval to use for the date part. Defaults to '6'.
        where_clause (str): Where clause to use in the query. Defaults to ''.
    Returns:
        str: SQL query as a string.
    """
    return f"""    SELECT
            OffenderId AS external_id,
            "Alerts" AS criteria,
            AlertDesc AS note_title,
            Notes AS note_body,
            start_date AS event_date,
        FROM (
            SELECT  
                *,
                CAST(LEFT(StartDate, 10) AS DATE) AS start_date,
                {nonnull_end_date_clause('CAST(LEFT(EndDate, 10) AS DATE)')} AS end_date,
            FROM `{{project_id}}.{{us_ix_raw_data_up_to_date_dataset}}.ind_Offender_Alert_latest` oa
            INNER JOIN `{{project_id}}.{{us_ix_raw_data_up_to_date_dataset}}.ind_Alert_latest` ia
                USING (AlertId)
            {where_clause})
        WHERE end_date > DATE_SUB(CURRENT_DATE, INTERVAL {date_interval} {date_part})"""


INSTITUTIONAL_BEHAVIOR_NOTES_STR = "Institutional Behavior Notes (in the past 6 months)"


def ix_general_case_notes(
    criteria_str: str,
    where_clause_addition: str = "",
    in_the_past_x_months: int = 6,
) -> str:
    """
    Returns a SQL query that returns case notes for a specified contact mode in Idaho.

    Args:
        criteria_str (str): Criteria to use for the criteria column.
        in_the_past_x_months (int): Number of months to use for the date interval.
            Defaults to 6.
        where_clause_addition (str): Where clause to use in the query. Defaults to ''.
    Returns:
        str: SQL query as a string.
    """

    return f"""    SELECT *
        FROM (
            SELECT
                info.OffenderId AS external_id,
                '{criteria_str}' AS criteria,
                note_type_cm.ContactModeDesc AS note_title,
                note.Details AS note_body,
                SAFE_CAST(LEFT(info.NoteDate, 10) AS DATE) AS event_date,
            FROM `{{project_id}}.{{us_ix_raw_data_up_to_date_dataset}}.ind_OffenderNote_latest` note
            LEFT JOIN `{{project_id}}.{{us_ix_raw_data_up_to_date_dataset}}.ind_OffenderNoteInfo_latest` info
                USING (OffenderNoteInfoId)
            LEFT JOIN `{{project_id}}.{{us_ix_raw_data_up_to_date_dataset}}.ref_NoteType_latest` note_type
                USING (NoteTypeId)
            LEFT JOIN `{{project_id}}.{{us_ix_raw_data_up_to_date_dataset}}.ind_OffenderNoteInfo_ContactMode_latest` info_cm
                USING (OffenderNoteInfoId)
            LEFT JOIN `{{project_id}}.{{us_ix_raw_data_up_to_date_dataset}}.ind_ContactMode_latest` note_type_cm
                USING (ContactModeId)
            WHERE OffenderNoteStatusId = '2' -- Posted
                {where_clause_addition}
            GROUP BY 1,2,3,4,5
        )
        WHERE (DATE_DIFF(CURRENT_DATE('US/Pacific'), event_date, MONTH)
                        - IF(EXTRACT(DAY FROM event_date) > EXTRACT(DAY FROM CURRENT_DATE('US/Pacific')),
                            1, 0)) <= {in_the_past_x_months}"""


NOTE_TITLE_REGEX = "r'^{{note_title:(.*?)}}'"
NOTE_BODY_REGEX = " r'{{note:((?s:.*))}}'"


def ix_fuzzy_matched_case_notes(where_clause: str = "") -> str:
    """
    Returns a SQL query that returns fuzzy matched case notes filtered by type in Idaho

    Args:
        where_clause (str): Where clause to use in the query. Defaults to ''.
    """
    return f"""    SELECT *
        FROM (
            SELECT 
                person_external_id AS external_id,
                'New criminal activity check' AS criteria,
                COALESCE(REGEXP_EXTRACT(Details, {{note_title_regex}}),
                         '') AS note_title,
                COALESCE(REGEXP_EXTRACT(REGEXP_REPLACE(Details, '<[^>]*>', ''), {{note_body_regex}}), 
                         REGEXP_REPLACE(Details, '<[^>]*>', '')) AS note_body,
                NoteDate AS event_date,
            FROM `{{project_id}}.{{supplemental_dataset}}.us_ix_case_note_matched_entities`
            {where_clause}
        )
        WHERE (DATE_DIFF(CURRENT_DATE('US/Pacific'), event_date, MONTH)
                    - IF(EXTRACT(DAY FROM event_date) > EXTRACT(DAY FROM CURRENT_DATE('US/Pacific')),
                        1, 0)) <= 3"""
