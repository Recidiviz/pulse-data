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
"""Helper SQL fragments that import raw tables for ME
"""

from typing import Optional

from recidiviz.calculator.query.bq_utils import (
    date_diff_in_full_months,
    nonnull_end_date_clause,
    revert_nonnull_end_date_clause,
)

from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
)
from recidiviz.calculator.query.state.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)

PROGRAM_ENROLLMENT_NOTE_TX_REGEX = "|".join(
    [
        "COMPLET[A-Z]*",
        "CERTIFICAT[A-Z]",
        "PARTICIPATED",
        "ATTENDED",
        "EDUC[A-Z]*",
        "CAT ",
        "CRIMINAL ADDICTIVE THINKING",
        "ANGER MANAGEMENT",
        "NEW FREEDOM",
        "T4C",
        "THINKING FOR A CHANGE",
        "SAFE",
        "STOPPING ABUSE FOR EVERYONE",
        "CBI-IPV",
        "SUD ",
        "HISET",
        "PROBLEM SEXUAL BEHAVIOR",
    ]
)


def cis_319_after_csswa(table: str = "sub_sessions_with_attributes") -> str:
    """
    Clean CIS_319 table after create_sub_sessions_with_attributes.

    Helper method to drop repeated subsessions:
     - Prioritize dropping status = 'Completed'
     - Keep older end_dates/critical_dates
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
        COALESCE(viol.note_title, "None") AS note_title,
        COALESCE(note_body, "No violations found") AS note_body,
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


def cis_112_custody_level_criteria(
    criteria_name: str,
    description: str,
    custody_levels: str = """ "Community", "Minimum" """,
) -> StateSpecificTaskCriteriaBigQueryViewBuilder:
    """
    Creates a state-specific criteria builder that surfaces as eligible those clients
        who match the custody_levels.

    Args:
        criteria_name (str): Criteria name. This should match the criteria file name.
        description (str): Criteria description.
        custody_levels (str, optional): Custody levels who are eligible for said criteria.
            Defaults to "Community", "Minimum".

    Returns:
        StateSpecificTaskCriteriaBigQueryViewBuilder: State specific criteria builder.
    """

    _QUERY_TEMPLATE = f"""
    WITH custody_levels AS (
        -- Grab custody levels, transform datetime to date, 
        -- merge to recidiviz ids, only keep distinct values
        SELECT
            state_code,
            person_id,
            CLIENT_SYS_DESC AS custody_level,
            SAFE_CAST(LEFT(CUSTODY_DATE, 10) AS DATE) AS start_date,
            SAFE_CAST(CUSTODY_DATE AS DATETIME) AS start_datetime,
        #TODO(#16722): pull custody level from ingested data once it is hydrated in our schema
        FROM `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.CIS_112_CUSTODY_LEVEL_latest`
        INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id`
            ON CIS_100_CLIENT_ID = external_id
            AND id_type = 'US_ME_DOC'
        INNER JOIN `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.CIS_1017_CLIENT_SYS_latest` cl_sys
            ON CIS_1017_CLIENT_SYS_CD = CLIENT_SYS_CD
    ),
    custody_level_spans AS (
        SELECT
            state_code,
            person_id,
            custody_level,
            start_date,
            -- If two custody_level_spans have the same start_date, we'll make the highest
            -- priority of all have the span that matters
            LEAD(start_date) OVER (PARTITION BY state_code, person_id ORDER BY start_date, start_datetime, custody_level_priority) AS end_date
        FROM (
            SELECT
                * EXCEPT(custody_level_priority),
                -- Any custody level not in the LEFT JOIN below will have a priority of 999 (highest priority).
                --    This way we, if a client is assigned to two or more custody levels, 
                --    we will keep give priority to the ones not in ["Unclassified", {custody_levels}].
                COALESCE(custody_level_priority, 999) AS custody_level_priority,
            FROM custody_levels
            LEFT JOIN (
                -- Set the custody level priority from lowest to highest, where "Unclassified" is
                -- lowest so that it is dropped if there is any other overlapping value. All other
                -- levels not listed here will be inferred as the highest priority.
                SELECT *
                FROM UNNEST(
                    ["Unclassified", {custody_levels}]
                ) AS custody_level
                WITH OFFSET custody_level_priority
            ) priority
                USING (custody_level)
            )
    )
    SELECT
        state_code,
        person_id,
        start_date,
        end_date,
        custody_level IN ({custody_levels}) AS meets_criteria,
        TO_JSON(STRUCT(custody_level AS custody_level)) AS reason
    FROM custody_level_spans
    WHERE start_date != {nonnull_end_date_clause('end_date')}
    """

    return StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=criteria_name,
        description=description,
        state_code=StateCode.US_ME,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
            state_code=StateCode.US_ME, instance=DirectIngestInstance.PRIMARY
        ),
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
    )


def cis_201_case_plan_case_notes() -> str:
    """
    Returns:
        str: Query that surfaces case plan goals for incarcerated individuals in ME.
    """

    _FINAL_SELECT = """SELECT
        Cis_200_Cis_100_Client_Id AS external_id,
        "Case Plan Goals" AS criteria,
        CONCAT(Domain_Goal_Desc,' - ', Goal_Status_Desc) AS note_title,
        IF(E_Goal_Type_Desc = 'Other',
            CONCAT(E_Goal_Type_Desc, " - " ,Other),
            E_Goal_Type_Desc)
        AS note_body,
        DATE(SAFE.PARSE_DATETIME("%m/%d/%Y %I:%M:%S %p", Open_Date)) AS event_date,
    FROM
    """

    return f"""
    {_FINAL_SELECT} (SELECT 
                *
            FROM `{{project_id}}.{{us_me_raw_data_up_to_date_dataset}}.CIS_201_GOALS_latest` gl
            INNER JOIN `{{project_id}}.{{us_me_raw_data_up_to_date_dataset}}.CIS_2012_GOAL_STATUS_TYPE_latest` gs
                ON gl.Cis_2012_Goal_Status_Cd = gs.Goal_Status_Cd
            INNER JOIN `{{project_id}}.{{us_me_raw_data_up_to_date_dataset}}.CIS_2010_GOAL_TYPE_latest` gt
                ON gl.Cis_2010_Goal_Type_Cd = gt.Goal_Type_Cd
            INNER JOIN `{{project_id}}.{{us_me_raw_data_up_to_date_dataset}}.CIS_2011_DOMAIN_GOAL_TYPE_latest` dg
                ON gl.Cis_2011_Dmn_Goal_Cd = dg.Domain_Goal_Cd
            WHERE gs.Goal_Status_Cd IN ('1','2')) 
    """


def me_time_left_in_sentence_in_categories() -> str:
    """
    Returns:
        str: Query that surfaces the time left in sentence in categories for ME.
    """

    return f"""
        SELECT
        *,
        CASE 
            WHEN month_diff <= 36 AND month_diff >= 6 THEN "3y to 6mo"
            WHEN month_diff < 6 AND day_diff >= 30 THEN "6mo to 30days"
            WHEN day_diff < 30 THEN "30days or less"
        END AS time_left
        FROM (
            -- Grab the day difference between today and the expected release date
            SELECT 
                *,
                {date_diff_in_full_months(date_column='expected_release_date', time_zone='US/Eastern')} AS month_diff,
                DATE_DIFF(expected_release_date, CURRENT_DATE('US/Eastern'), DAY) day_diff
            FROM (
                -- Grab the expected release date
                SELECT 
                    state_code, 
                    person_id,
                    MAX(end_date) AS expected_release_date,
                FROM `{{project_id}}.{{analyst_dataset}}.us_me_sentence_term_materialized` 
                WHERE CURRENT_DATE('US/Eastern') < end_date
                -- Only keep active terms
                AND status = '1'
                GROUP BY 1,2
            )
        )
        INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id`
            USING(person_id)
        """


def cis_300_relevant_property_case_notes(
    criteria_name: str = "Relevant property",
) -> str:
    """
    Returns:
        str: Query that surfaces relevant property for ME.
    """

    return f"""
    SELECT 
        pp.CIS_100_CLIENT_ID AS external_id,
        '{criteria_name}' AS criteria,
        ppit.E_PP_ITEM_TYPE_DESC AS note_title,
        pp.DESCRIPTION_TX AS note_body,
        SAFE_CAST(LEFT(pp.ENTRY_DATE, 10) AS DATE) AS event_date,
    FROM `{{project_id}}.{{us_me_raw_data_up_to_date_dataset}}.CIS_300_Personal_Property_latest` pp
    LEFT JOIN `{{project_id}}.{{us_me_raw_data_up_to_date_dataset}}.CIS_3030_PP_Item_Type_latest` ppit
        ON CIS_3030_PP_ITEM_TYPE_CD = PP_ITEM_TYPE_CD
    -- Inner join to the latest admission ID
    INNER JOIN (
        SELECT 
            CIS_100_CLIENT_ID,
            CIS_301_ADMISSION_ID,
            MIN(ENTRY_DATE) AS entry_date
        FROM `{{project_id}}.{{us_me_raw_data_up_to_date_dataset}}.CIS_300_Personal_Property_latest`
        WHERE CIS_3030_PP_ITEM_TYPE_CD IN ('18', '172')
        GROUP BY 1,2
        QUALIFY ROW_NUMBER() OVER(PARTITION BY CIS_100_CLIENT_ID ORDER BY entry_date DESC) =1 
        ORDER BY 1,2
    )
        USING (CIS_100_CLIENT_ID, CIS_301_ADMISSION_ID)
    WHERE E_PP_ITEM_TYPE_DESC IN ("Legal Documents", "ID's", "Personal ID's", "Legal Materials")
        AND LOGICAL_DELETE_IND != 'Y'
        AND ADULT_IND = 'Y'"""


def cis_900_employee_to_supervisor_match() -> str:
    """
    Returns:
        str: Query that surfaces the mapping between employees and supervisors for ME.
    """

    return """    -- Mapping every staff with his/her supervisor
    #TODO(#24426): use ingested entity data instead of CIS_900_EMPLOYEE_latest
    SELECT DISTINCT
        cis900.Employee_Id AS officer_id,
        "US_ME" AS state_code,
        INITCAP(cis900.First_Name) || " " || INITCAP(cis900.Last_Name) AS officer_name,
        LOWER(cis900.Email_Tx) AS officer_email,
        cis900.Cis_900_Employee_Supervisor_Id AS supervisor_id,
    FROM `{project_id}.{us_me_raw_data_up_to_date_dataset}.CIS_900_EMPLOYEE_latest` cis900
    GROUP BY 1,2,3,4,5"""


def x_years_remaining_on_sentence(x: int) -> str:
    """
    Returns:
        str: Query that surfaces spans of time for which clients are x years away from expected release date.
    """
    return f"""
    WITH term_with_critical_date AS (
    -- Combine case load with term data
        SELECT 
            state_code,
            person_id,
            start_date,
            end_date,
            DATE_SUB(end_date, INTERVAL {x} YEAR) AS critical_date,
            status,
            term_id,
        FROM `{{project_id}}.{{analyst_dataset}}.us_me_sentence_term_materialized` tc
    ),
    
    -- Create sub-sessions w/attributes
    {create_sub_sessions_with_attributes('term_with_critical_date')},
    
    critical_date_spans AS (
        -- Drop additional repeated subsessions: if concurrent keep the longest one, drop
        --   completed sessions over active ones
        {cis_319_after_csswa()}
    ),
    
    -- Critical date has passed
    {critical_date_has_passed_spans_cte()}
    SELECT
        cd.state_code,
        cd.person_id,
        CASE
            WHEN (start_date IS NULL) AND (critical_date_has_passed) THEN cd.critical_date
                                    -- When there was no intake date in us_me_sentence_term,
                                    -- start_date of our subsession is NULL for the
                                    -- period for which the criteria is met. But
                                    -- we know the end date and we can calculate the
                                    -- start_date (eligible_date)
            ELSE start_date
        END start_date,
            -- if the most recent subsession is True, then end_date should be NULL
        IF((ROW_NUMBER() OVER (PARTITION BY cd.person_id, cd.state_code
                               ORDER BY start_date DESC) =  1)
                AND (critical_date_has_passed),
            NULL,
            end_date) AS end_date,
        critical_date_has_passed AS meets_criteria,
        TO_JSON(STRUCT(critical_date AS eligible_date)) AS reason,
    FROM critical_date_has_passed_spans cd
    """


def no_violations_for_x_time(
    x: int, date_part: str = "DAY", violation_classes: tuple[str, ...] = tuple()
) -> str:
    """
    Input:
        x: How many days to look for violations during
        date_part (str, optional): Supports any of the BigQuery date_part values:
            "DAY", "WEEK","MONTH","QUARTER","YEAR". Defaults to "DAY".
        violation_classes (optional): specific violations to filter for (i.e. class A, class B, etc)
    Returns:
        str: Query that surfaces spans of time for which clients are x years away from expected release date.
    """
    assert isinstance(x, int), "x must be of type int"
    assert isinstance(date_part, str), "date_part must be of type string"
    assert date_part in ["DAY", "WEEK", "MONTH", "QUARTER", "YEAR"], (
        "date_part must be one of the following: "
        '"DAY", "WEEK","MONTH","QUARTER","YEAR"'
    )
    assert isinstance(
        violation_classes, tuple
    ), "Violation_classes object must be of type tuple"
    assert all(
        (isinstance(i, str) for i in violation_classes)
    ), "Each violation class must be of type string"

    return f"""
    WITH disciplinary_cases_cte AS (
      SELECT 
          "US_ME" AS state_code,
          ei.person_id,
          IF(vd.Cis_1813_Disposition_Outcome_Type_Cd IS NULL,
             CONCAT('Pending since ', SAFE_CAST(LEFT(dc.CREATED_ON_DATE, 10) AS STRING)),
             vdt.E_Violation_Disposition_Type_Desc) 
                AS disp_type,
          vdc.E_Violation_Disposition_Class_Desc AS disp_class,
          vd.Cis_1813_Disposition_Outcome_Type_Cd AS disp_outcome,
          SAFE_CAST(LEFT(dc.HEARING_ACTUALLY_HELD_DATE, 10) AS DATE) AS start_date,
          SAFE_CAST(LEFT(dc.CREATED_ON_DATE, 10) AS DATE) AS pending_violation_start_date,
          dc.CIS_462_CLIENTS_INVOLVED_ID,
          ci.Cis_100_Client_Id,
      FROM `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.CIS_181_VIOLATION_DISPOSITION_latest`  vd
      LEFT JOIN `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.CIS_180_DISCIPLINARY_CASE_latest` dc
        ON vd.Cis_180_Disciplinary_Case_Id = dc.DISCIPLINARY_CASE_ID
      LEFT JOIN `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.CIS_1811_VIOLATION_DISPOSITION_TYPE_latest` vdt
        ON vd.Cis_1811_Violation_Type_Cd = vdt.Violation_Disposition_Type_Cd
      LEFT JOIN `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.CIS_1810_VIOLATION_DISPOSITION_CLASS_latest` vdc
        ON vd.Cis_1810_Violation_Class_Cd = vdc.Violation_Disposition_Class_Cd
      LEFT JOIN `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.CIS_462_CLIENTS_INVOLVED_latest` ci
        ON ci.Clients_Involved_Id = dc.CIS_462_CLIENTS_INVOLVED_ID
      INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` ei
          ON ci.Cis_100_Client_Id = external_id
          AND id_type = 'US_ME_DOC'
      WHERE
          {'vdc.E_Violation_Disposition_Class_Desc in ' + str(violation_classes) + ' AND' if violation_classes else ''}
          # Drop if logical delete = yes
          COALESCE(dc.LOGICAL_DELETE_IND, 'N') != 'Y'
          AND COALESCE(vd.Logical_Delete_Ind , 'N') != 'Y'
          # Whenever a disciplinary sanction has informal sanctions taken, it does not affect SCCP eligibility.
          # TODO(#25437): Does it affect Medium Trustee eligibility?
          AND COALESCE(dc.DISCIPLINARY_ACTION_FORMAL_IND, 'Y') != 'N'
          
    ),
    cases_wstart_end_cte AS (
      -- Resolved disciplines
      SELECT
            * EXCEPT (start_date, pending_violation_start_date),
            start_date,
            DATE_ADD(start_date, INTERVAL {x} {date_part}) AS end_date,
            -- Keep another date so it doesn't get lost in a sub-session later
            DATE_ADD(start_date, INTERVAL {x} {date_part}) AS eligible_date,
      FROM disciplinary_cases_cte
      WHERE disp_outcome IS NOT NULL
    
      UNION ALL
    
      -- Pending disciplines
      SELECT
      -- Create an open span on the pending start date if the violation disposition outcome is not set
            * EXCEPT (start_date, pending_violation_start_date),
            pending_violation_start_date AS start_date,
            NULL AS end_date,
            -- Keep another date so it doesn't get lost in a sub-session later
            NULL AS eligible_date,
      FROM disciplinary_cases_cte
      WHERE disp_outcome IS NULL
    ),
    {create_sub_sessions_with_attributes(table_name='cases_wstart_end_cte')},
    no_dup_subsessions_cte AS (
      -- Drop duplicate sub-sessions by keeping the class with the highest priority
      -- but keep the highest end_date as an eligible_date
      SELECT * EXCEPT(disp_type, eligible_date),
          -- If more than 1 violation at the time, state that there are 'More than 1'
          IF(SUM(1) OVER(PARTITION BY person_id, state_code, start_date, end_date) > 1,
            'More than 1',
            disp_type) AS disp_type_wmorethan1,
          -- When 2 duplicates subsessions are present, we keep the highest eligible date
          MAX({nonnull_end_date_clause('eligible_date')}) OVER(PARTITION BY person_id, state_code, start_date, end_date)
            AS eligible_date,
      FROM sub_sessions_with_attributes
      -- Drop cases where resident was 'Found Not Guilty' or 'Dismissed (Technical)'
      WHERE disp_outcome NOT IN ('5', '10', '3', '8') 
            OR disp_outcome IS NULL
      -- Keep the violaiton with the highest priority
      QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id, state_code, start_date, end_date
                                ORDER BY disp_class) = 1
    )
    
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date,
        False AS meets_criteria,
        TO_JSON(STRUCT(disp_class AS highest_class_viol,
                       disp_type_wmorethan1 AS viol_type,
                       {revert_nonnull_end_date_clause('eligible_date')} AS eligible_date)) AS reason
    FROM no_dup_subsessions_cte
    """


def compartment_level_1_super_sessions_without_me_sccp() -> str:
    """
    Compartment level 1 super sessions after partitioning with ME SCCP sesssions.
    """

    return f"""cl1_super_sessions_wsccp AS (
        SELECT 
            state_code, 
            person_id,
            start_date, 
            end_date_exclusive AS end_date,
            compartment_level_1,
            NULL AS compartment_level_2,
        FROM `{{project_id}}.{{sessions_dataset}}.compartment_level_1_super_sessions_materialized`

        UNION ALL

        SELECT 
            state_code, 
            person_id,
            start_date, 
            end_date_exclusive AS end_date,
            compartment_level_1,
            compartment_level_2,
            FROM `{{project_id}}.{{sessions_dataset}}.compartment_sessions_materialized`
        WHERE state_code = 'US_ME'
            AND compartment_level_2 = 'COMMUNITY_CONFINEMENT'
            AND compartment_level_1 = 'SUPERVISION'
    ),

    {create_sub_sessions_with_attributes('cl1_super_sessions_wsccp')},

    partitioning_compartment_l1_ss_with_sccp AS (
        SELECT 
            *,
            DATE_ADD(end_date, INTERVAL 1 DAY) AS end_date_exclusive
        FROM sub_sessions_with_attributes
        # For repeated subsessions, keep only the one with a value compartment_level_2
        QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id, state_code, start_date, end_date ORDER BY compartment_level_2 DESC) = 1
    )
    """
