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
"""Helper SQL fragments for ME
"""

from typing import Optional

from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause


def cis_319_after_csswa(table: str = "sub_sessions_with_attributes") -> str:
    """Helper method to drop repeated subsessions and incorrect
    final spans. Meant to be used after create_sub_sessions_with_attributes (csswa).
    """

    return f"""
    -- Drop repeated subsessions and drop completed and end_date=NULL
    SELECT * EXCEPT(null_end_date_and_completed)
    FROM (SELECT 
             * EXCEPT(start_date, end_date),
             start_date AS start_datetime,
             end_date AS end_datetime,
             -- Completed terms that are the last subsession of one resident
             IF((end_date IS NULL AND status = '2'),
                True,
                False)
             AS null_end_date_and_completed
         FROM {table}
         -- Drop subsessions when repeated on the basis of status (drop 'Completed' first)
         -- and critical date (drop more recent ones first)
         QUALIFY ROW_NUMBER() 
                 OVER(PARTITION BY state_code, person_id, start_date, end_date 
                 ORDER BY status, {nonnull_end_date_clause('critical_date')} DESC) = 1)
    WHERE null_end_date_and_completed IS False
    """


def cis_204_notes_cte(criteria_name: str) -> str:

    """Helper method that returns a query that pulls
    up case notes with the format needed to be aggregated

    Args:
        criteria_name (str): Criteria name for the notes
    """
    return f"""SELECT
        n.Cis_100_Client_Id AS external_id,
        "{criteria_name}" AS criteria,
        n.Short_Note_Tx AS note_title,
        n.Note_Tx AS note_body,
        DATE(SAFE_CAST(n.Note_Date AS DATETIME)) AS event_date,
    FROM `{{project_id}}.{{us_me_raw_data_up_to_date_dataset}}.CIS_204_GEN_NOTE_latest` n
    INNER JOIN `{{project_id}}.{{us_me_raw_data_up_to_date_dataset}}.CIS_2041_NOTE_TYPE_latest` ncd
        ON ncd.Note_Type_Cd = n.Cis_2041_Note_Type_Cd
    INNER JOIN `{{project_id}}.{{us_me_raw_data_up_to_date_dataset}}.CIS_2040_CONTACT_MODE_latest` cncd
        ON n.Cis_2040_Contact_Mode_Cd = cncd.Contact_Mode_Cd"""


def cis_408_violations_notes_cte(
    violation_type: str,
    violation_type_for_criteria: str,
    note_title: str,
    time_interval: int = 6,
    date_part: str = "MONTH",
    violation_finding_cd: str = "1",
    violation_date: str = "v.Toll_Start_Date",
) -> str:
    """

    Args:
        violation_type (str): Violation types that will be kept in the query. Supported
            types: "TECHNICAL", "MISDEMEANOR", "FELONY" and "ANY".
        violation_type_for_criteria (str): String that will be shown in the criteria
            column of the case notes.
        note_title (str): String that will be shown as the note_title column of the
            case notes
        time_interval (int, optional): Number of <date_part> when the violation
            will be counted as valid. Defaults to 6 (e.g. 6 months).
        date_part (str, optional): Supports any of the BigQuery date_part values:
            "DAY", "WEEK","MONTH","QUARTER","YEAR". Defaults to "MONTH".
        violation_finding_cd (str, optional): Violation finding type code. All other
            violation finding types will be filtered out. Defaults to "1", which refers
            to 'Violation found'.
        violation_date (str, optional): This date will be the one filtered to
            consider the most recent violations only. Defaults to v.Toll_Start_Date.

    Returns:
        str: Query that surfaces all violations of the type passed <violation_type>
            that have the finding type "VIOLATION FOUND" and happened within the
            specified <time_interval> <date_part> e.g. (6 MONTHS)
    """

    if violation_type.upper() == "TECHNICAL":
        vtype = " '63' "
    elif violation_type.upper() == "MISDEMEANOR":
        vtype = " '64' "
    elif violation_type.upper() == "FELONY":
        vtype = " '65' "
    elif violation_type.upper() == "ANY":
        vtype = " '65','64','63' "

    return f"""
    SELECT 
        csp.external_id,
        "{violation_type_for_criteria}" AS criteria,
        COALESCE(viol.note_title, "No violations found") AS note_title,
        COALESCE(note_body, "") AS note_body,
        event_date AS event_date
    FROM current_supervision_pop_cte csp
    LEFT JOIN (
        SELECT 
            v.Cis_100_Client_Id AS external_id,
            {note_title} AS note_title,
            Violation_Descr_Tx AS note_body,
            SAFE_CAST(LEFT(v.Toll_Start_Date, 10) AS DATE) AS event_date,
        FROM `{{project_id}}.{{us_me_raw_data_up_to_date_dataset}}.CIS_480_VIOLATION_latest` v
        LEFT JOIN `{{project_id}}.{{us_me_raw_data_up_to_date_dataset}}.CIS_4800_VIOLATION_FINDING_latest` vf
            ON v.Cis_4800_Violation_Finding_Cd = vf.Violation_Finding_Cd
        LEFT JOIN `{{project_id}}.us_me_raw_data_up_to_date_views.CIS_4009_SENT_CALC_SYS_latest` scs
            ON scs.Sent_Calc_Sys_Cd = v.Cis_4009_Violation_Type_Cd
        WHERE Cis_4009_Violation_Type_Cd IN ({vtype})
            AND Cis_4800_Violation_Finding_Cd = '{violation_finding_cd}'
            AND DATE_ADD(SAFE_CAST(LEFT({violation_date}, 10) AS DATE), INTERVAL {time_interval} {date_part}) > CURRENT_DATE('US/Eastern')
            -- Remove cases flagged for deletion
            AND v.Logical_Delete_Ind = 'N'
    ) viol
    USING(external_id)"""


def cis_425_program_enrollment_notes(
    where_clause: Optional[str] = "",
    additional_joins: Optional[str] = "",
    criteria: Optional[str] = "'Program enrollment'",
    note_title: Optional[str] = "CONCAT(st.E_STAT_TYPE_DESC ,' - ', pr.NAME_TX)",
    note_body: Optional[str] = "ps.Comments_Tx",
) -> str:
    """
    Formats program enrollment data as contact notes for Workflows tools

    Args:
        where_clause (str, optional): Additional where clause filters (needs to start
            with 'AND'). Defaults to ''.
        additional_joins (Optional[str], optional): Additional joins. Defaults to "".
        criteria (Optional[str], optional): Add what we want to display as criteria.
            Defaults to "".
        note_title (Optional[str], optional): Add what we want to display as note_title.
            Defaults to "".
        note_body (Optional[str], optional): Add what we want to display as note_body.
            Defaults to "".

    Returns:
        str: SQL query
    """

    return f"""
    -- Program enrollment data as notes
    SELECT 
        mp.CIS_100_CLIENT_ID AS external_id,
        {criteria} AS criteria,
        {note_title} AS note_title,
        {note_body} AS note_body,
        -- TODO(#17587) remove LEFT once the YAML file is updated
        SAFE_CAST(LEFT(mp.MODIFIED_ON_DATE, 10) AS DATE) AS event_date,
    FROM `{{project_id}}.{{us_me_raw_data_up_to_date_dataset}}.CIS_425_MAIN_PROG_latest` mp
    INNER JOIN `{{project_id}}.{{us_me_raw_data_up_to_date_dataset}}.CIS_420_PROGRAMS_latest` pr
        ON mp.CIS_420_PROGRAM_ID = pr.PROGRAM_ID
    -- Comments_Tx/Note_body could be NULL, which happens when the record does not contain free text 
    LEFT JOIN `{{project_id}}.{{us_me_raw_data_up_to_date_dataset}}.CIS_426_PROG_STATUS_latest`  ps
        ON mp.ENROLL_ID = ps.Cis_425_Enroll_Id
    INNER JOIN `{{project_id}}.{{us_me_raw_data_up_to_date_dataset}}.CIS_9900_STATUS_TYPE_latest` st
        ON ps.Cis_9900_Stat_Type_Cd = st.STAT_TYPE_CD
    {additional_joins}
    WHERE pr.NAME_TX IS NOT NULL {where_clause}
    QUALIFY ROW_NUMBER() OVER(PARTITION BY mp.ENROLL_ID ORDER BY Effct_Datetime DESC) = 1
    """
