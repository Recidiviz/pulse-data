# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
from typing import List, Optional

from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import (
    nonnull_end_date_clause,
    nonnull_end_date_exclusive_clause,
)
from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.views.workflows.firestore.opportunity_record_query_fragments import (
    current_violent_statutes_being_served,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.observations.span_observation_big_query_view_builder import fix_indent
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_candidate_population_big_query_view_builder import (
    StateSpecificTaskCandidatePopulationBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
)
from recidiviz.task_eligibility.utils.state_dataset_query_fragments import (
    extract_object_from_json,
)

IX_CRC_FACILITIES = [
    "Nampa Community Reentry Center",
    "East Boise Community Reentry Center",
    "Idaho Falls Community Reentry Center",
    "Twin Falls Community Work Center",
    "Twin Falls Community Reentry Center",
    "Treasure Valley Community Reentry Center",
]

DETAINER_TYPE_LST_CRC = ["59"]
HOLD_TYPE_LST_CRC = ["84", "112", "85", "61", "87", "86", "62", "88", "67", "83", "113"]
# detainer types related to felonies and immigration (https://docs.google.com/spreadsheets/d/1sV6BLWQQiQ4mrtzWJiooaHzO_sgk-bNkCMdwolQ8aVk/edit?usp=sharing)
DETAINER_TYPE_LST_CLD = [
    "2",
    "59",
    "60",
    "76",
    "78",
    "84",
    "85",
    "87",
    "98",
    "99",
    "101",
    "102",
    "108",
    "112",
    "115",
    "117",
    "121",
]

IX_STATE_CODE_WHERE_CLAUSE = "WHERE state_code = 'US_IX'"


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
    TRUE as meets_criteria,
    TO_JSON(STRUCT(start_date AS latest_detainer_start_date,
        DetainerTypeDesc AS latest_detainer_type,
        DetainerStatusDesc AS latest_detainer_status)) AS reason,
    start_date AS latest_detainer_start_date,
    DetainerTypeDesc AS latest_detainer_type,
    DetainerStatusDesc AS latest_detainer_status,
FROM
    sub_sessions_with_attributes
QUALIFY ROW_NUMBER() OVER(PARTITION BY state_code, person_id, start_date, end_date ORDER BY start_date DESC)=1
    """


# This pattern should match text enclosed within angle brackets < >,
#   curly braces { }, or the pattern &nbsp
ESCAPE_CURLY_AND_ANGLE_BRACKETS_REGEX = "[<{{]([^>}}]*)[>}}]|&nbsp"


def ix_offender_alerts_case_notes(
    date_part: str = "MONTH",
    date_interval: str = "6",
    where_clause: str = "",
    indent_level: int = 8,
) -> str:
    """
    Returns a SQL query that returns case notes for offender alerts in Idaho.
    Args:
        date_part (str): Date part to use for the date interval. Defaults to 'MONTH'.
        date_interval (str): Date interval to use for the date part. Defaults to '6'.
        where_clause (str): Where clause to use in the query. Defaults to ''.
        indent_level (int): Indent level for the query. Defaults to 2.
    Returns:
        str: SQL query as a string.
    """
    query = f"""
        SELECT
            OffenderId AS external_id,
            "Alerts" AS criteria,
            AlertDesc AS note_title,
            REGEXP_REPLACE(Notes, r'{ESCAPE_CURLY_AND_ANGLE_BRACKETS_REGEX}', '') AS note_body,
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

    return fix_indent(query, indent_level=indent_level)


INSTITUTIONAL_BEHAVIOR_NOTES_STR = "Institutional Behavior Notes (in the past 6 months)"
I9_NOTES_STR = "I-9 Documents Notes"
WORK_HISTORY_STR = "Work History (in the past 5 years)"
MEDICAL_CLEARANCE_STR = "Medical Clearance (in the past 6 months)"
RELEASE_INFORMATION_STR = "Release District Information"
# TODO(#30190) - add new contact mode types
RELEASE_INFORMATION_CONTACT_MODES = """(
                    "CRC Request - D1 Release",
                    "CRC Request - D2 Release",
                    "CRC Request - D3 Release",
                    "CRC Request - D4 Release",
                    "CRC Request - D5 Release",
                    "CRC Request - D6 Release",
                    "CRC Request - D7 Release",
                    "CRC Request - ISC Release")"""
CRC_INFORMATION_STR = "Additional CRC Information (in the past 6 months)"
CRC_INFORMATION_CONTACT_MODES = """('CRC Termer Approved',
                    'CRC Termer Denied', 
                    'CRC DIV Approved', 
                    'CRC DIV Denied')"""
I9_NOTE_TX_REGEX = "|".join(
    ["I9", "I-9", "I- 9", "I - 9", "I -9", "I - 9", "I- 9", "I -9"]
)
MEDICAL_CLEARANCE_TX_REGEX = "|".join(["MEDICALLY CLEAR", "MEDICAL CLEAR"])


def ix_general_case_notes(
    criteria_str: str,
    where_clause_addition: str = "",
    in_the_past_x_months: Optional[int] = None,
    indent_level: int = 8,
) -> str:
    """
    Returns a SQL query that returns case notes for a specified contact mode in Idaho.

    Args:
        criteria_str (str): Criteria to use for the criteria column.
        in_the_past_x_months (int): Number of months to use for the date interval.
            Defaults to 6.
        where_clause_addition (str): Where clause to use in the query. Defaults to ''.
        indent_level (int): Indent level for the query. Defaults to 2.
    Returns:
        str: SQL query as a string.
    """
    date_filter_clause = ""
    if in_the_past_x_months is not None:
        date_filter_clause = f"""
            WHERE DATE_DIFF(CURRENT_DATE('US/Pacific'), event_date, MONTH)
            - IF(EXTRACT(DAY FROM event_date) > EXTRACT(DAY FROM CURRENT_DATE('US/Pacific')), 1, 0)
            <= {in_the_past_x_months}"""
    query = f"""
        SELECT *
        FROM (
            SELECT
                info.OffenderId AS external_id,
                '{criteria_str}' AS criteria,
                note_type_cm.ContactModeDesc AS note_title,
                REGEXP_REPLACE(note.Details, r'{ESCAPE_CURLY_AND_ANGLE_BRACKETS_REGEX}', '') AS note_body,
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
        {date_filter_clause}"""

    return fix_indent(query, indent_level=indent_level)


NOTE_TITLE_REGEX = "r'^{{note_title:(.*?)}}'"
NOTE_BODY_REGEX = " r'{{note:((?s:.*))}}'"


def ix_fuzzy_matched_case_notes(where_clause: str = "", indent_level: int = 8) -> str:
    """
    Returns a SQL query that returns fuzzy matched case notes filtered by type in Idaho

    Args:
        where_clause (str): Where clause to use in the query. Defaults to ''.
        indent_level (int): Indent level for the query. Defaults to 8.
    """
    query = f"""
        SELECT *
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

    return fix_indent(query, indent_level=indent_level)


def escape_absconsion_or_eluding_police_case_notes(
    criteria_column_str: str = "Escape, Absconsion or Eluding Police history (in the past 10 years)",
    indent_level: int = 8,
) -> str:
    """
    Returns a SQL query that returns case notes for escape, absconsion or eluding police in Idaho.

    Args:
        criteria_column_str (str): Criteria to use for the criteria column. Defaults to
            'Escape, Absconsion or Eluding Police history (in the past 10 years').
        indent_level (int): Indent level for the query. Defaults to 8.

    Returns:
        str: SQL query as a string.
    """

    query = f"""
    SELECT
        pei.external_id,
        '{criteria_column_str}' AS criteria,
        description AS note_title,
        statute AS note_body,
        {extract_object_from_json(object_column = 'most_recent_statute_date', 
                                object_type='DATE')} AS event_date,
    FROM `{{project_id}}.{{task_eligibility_criteria_us_ix_dataset}}.no_escape_offense_within_10_years_materialized` ne,
    UNNEST(JSON_VALUE_ARRAY(reason.ineligible_offenses)) AS statute,
    UNNEST(JSON_VALUE_ARRAY(reason.ineligible_offenses_descriptions)) AS description
    LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
        ON ne.person_id = pei.person_id
         AND ne.state_code = pei.state_code
         AND pei.id_type = 'US_IX_IDOC'
    WHERE CURRENT_DATE BETWEEN start_date AND {nonnull_end_date_clause('end_date')}
        AND meets_criteria IS FALSE
    GROUP BY 1,2,3,4,5

    UNION ALL 

    SELECT 
        pei.external_id,
        '{criteria_column_str}' AS criteria,
        '' AS note_title,
        'ABSCONSION' AS note_body,
        {extract_object_from_json(object_column = 'most_recent_absconded_date', 
                                object_type='DATE')} AS event_date,
    FROM `{{project_id}}.{{task_eligibility_criteria_dataset}}.no_absconsion_within_10_years_materialized` na
    LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
        ON na.person_id = pei.person_id
         AND na.state_code = pei.state_code
         AND pei.id_type = 'US_ID_DOC'
    WHERE pei.state_code = 'US_IX'
        AND CURRENT_DATE BETWEEN start_date AND {nonnull_end_date_clause('end_date')}
            AND meets_criteria IS FALSE

    UNION ALL

    SELECT
        pei.external_id,
        '{criteria_column_str}' AS criteria,
        description AS note_title,
        statute AS note_body,
        {extract_object_from_json(object_column = 'most_recent_statute_date', 
                                object_type='DATE')} AS event_date,
    FROM `{{project_id}}.{{task_eligibility_criteria_us_ix_dataset}}.no_eluding_police_offense_within_10_years_materialized` ne,
    UNNEST(JSON_VALUE_ARRAY(reason.ineligible_offenses)) AS statute,
    UNNEST(JSON_VALUE_ARRAY(reason.ineligible_offenses_descriptions)) AS description
    LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
        ON ne.person_id = pei.person_id
            AND ne.state_code = pei.state_code
            AND pei.id_type = 'US_IX_IDOC'
    WHERE CURRENT_DATE BETWEEN start_date AND {nonnull_end_date_clause('end_date')}
        AND meets_criteria IS FALSE
    GROUP BY 1,2,3,4,5"""

    return fix_indent(query, indent_level=indent_level)


def detainer_case_notes(
    criteria_column: str = "Detainers", indent_level: int = 8
) -> str:
    """
    Returns a SQL query that returns case notes for detainers in Idaho.
    Args:

        criteria_column (str): Criteria to use for the criteria column. Defaults to 'Detainers'.
        indent_level (int): Indent level for the query. Defaults to 8.
    """

    query = f"""
SELECT
    pei.external_id,
    '{criteria_column}' AS criteria,
    CONCAT(DetainerTypeDesc, ' - ', DetainerStatusDesc) AS note_title,
    Comments AS note_body,
    start_date AS event_date,
FROM `{{project_id}}.{{analyst_dataset}}.us_ix_detainer_spans_materialized` det
LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
    ON det.person_id = pei.person_id
        AND det.state_code = pei.state_code
        AND pei.id_type = 'US_IX_DOC'"""

    return fix_indent(query, indent_level=indent_level)


def lsir_spans() -> str:
    """
    Returns a SQL query that returns LSIR spans in Idaho.

    Note: Confirmed on 12/8/25 that Idaho uses gender (not sex) to classify individual LSIR scores.
    """

    return f"""LSIR_level_gender AS(
  /* This CTE creates a view of LSIR-score level by gender according to updated
  Idaho Supervision Categories (07/21/2020) */
  SELECT
      score.person_id,
      score.state_code,
      score.assessment_date AS score_start_date,
      {nonnull_end_date_clause('score.score_end_date_exclusive')} AS score_end_date,
      ses.start_date AS supervision_start_date,
      ses.end_date_exclusive AS supervision_end_date,
      CASE
          WHEN ((gender != "MALE" OR gender IS NULL) AND assessment_score <=22) THEN "LOW"
          WHEN ((gender != "MALE" OR gender IS NULL)
                                AND (assessment_score BETWEEN 23 AND 30)) THEN "MODERATE"
          WHEN (gender = "MALE" AND assessment_score <=20) THEN "LOW"
          WHEN (gender = "MALE" AND (assessment_score BETWEEN 21 AND 28)) THEN "MODERATE"
          ELSE "HIGH"
          END AS lsir_level
  FROM `{{project_id}}.{{sessions_dataset}}.assessment_score_sessions_materialized` score
  INNER JOIN `{{project_id}}.{{sessions_dataset}}.supervision_super_sessions_materialized`ses
    ON score.state_code = ses.state_code
    AND score.person_id = ses.person_id
    --only consider scores relevant in the supervision session during which they occur 
    AND ses.start_date < {nonnull_end_date_clause('score.score_end_date_exclusive')}
    AND score.assessment_date < {nonnull_end_date_clause('ses.end_date_exclusive')}
     LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person` info
      ON score.state_code = info.state_code
      AND score.person_id = info.person_id
  WHERE score.state_code = 'US_IX' 
      AND assessment_type = 'LSIR'
),
grp_starts as (
   /* This CTE identifies when the LSI-R level changes within a supervision session. 
   grp_start is 1 at the beginning of each new level span and 0 for consecutive periods with the same LSI-R score.*/
  SELECT 
      state_code,
      person_id,
      lsir_level, 
      supervision_start_date,
      supervision_end_date,
      score_start_date,
      score_end_date,
      IF(lsir_level = LAG(lsir_level) OVER supervision_window, 0, 1) AS grp_start
  FROM LSIR_level_gender
  WINDOW supervision_window AS (
    PARTITION BY person_id, supervision_start_date ORDER BY score_start_date
  )
),
grps as (
    /* This CTE sums grp_starts to group together adjacent periods where the LSI-R score is the same */
    SELECT
        state_code,
        person_id,
        lsir_level, 
        supervision_start_date,
        supervision_end_date,
        score_start_date,
        score_end_date,
        SUM(grp_start) OVER(ORDER BY person_id, supervision_start_date, score_start_date) AS grp
    FROM grp_starts
),
lsir_spans AS (
    /* This CTE actually combines adjacent periods where the LSI-R score is the same by 
    choosing min start and max end date.*/
    SELECT 
        state_code, 
        person_id,
        lsir_level,
        MIN(score_start_date) AS score_span_start_date,
        --end span date on supervision session end date, or score end date, whichever comes first
        MAX(LEAST(score_end_date,{nonnull_end_date_clause('supervision_end_date')})) AS score_span_end_date
    FROM grps
    GROUP BY grp, state_code, person_id, lsir_level)"""


DOR_CASE_NOTES_COLUMNS = """
        dac.OffenderId AS external_id,
        'Disciplinary Offense Reports (in the past 6 months)' AS criteria,
        CONCAT('Class ', SUBSTR(dot.DorOffenseCode, 1, 1), ': ', DorOffenseTypeName) AS note_title,
        COALESCE(REGEXP_EXTRACT(dac.OffenseDesc, r'"offense_desc": "([^"]+)"'), dac.OffenseDesc) AS note_body,
        SAFE_CAST(LEFT(dac.OffenseDateTime, 10) AS DATE) AS event_date,"""

DOR_CRITERIA_COLUMNS = """
        dac.OffenderId AS external_id,
        SAFE_CAST(LEFT(COALESCE(dac.AuditHearingDate, dac.OffenseDateTime), 10) AS DATE) AS start_date,
        SUBSTR(dot.DorOffenseCode, 1, 1) AS dor_class"""


def dor_query(columns_str: str, classes_to_include: list, indent_level: int = 8) -> str:
    """
    Returns a SQL query that returns all the Disciplinary Offense Reports in Idaho
    that match the classes requested.

    Args:
        columns_str (str): Columns to use for the select statement. Defaults to
            DOR_CASE_NOTES_COLUMNS.
        classes_to_include (list): List of classes to include in the query. E.g. ['A', 'B'].
        additional_where_clause (str): Additional where clause to use in the query.
            Defaults to ''.
        indent_level (int): Indent level for the query. Defaults to 8.
    """

    query = f"""
    SELECT *
    FROM (
        SELECT {columns_str}
        # Procedure data contains offense type for each case (class)
        FROM `{{project_id}}.{{us_ix_raw_data_up_to_date_dataset}}.dsc_DAProcedure_latest` dap
        # Details of each case
        LEFT JOIN `{{project_id}}.{{us_ix_raw_data_up_to_date_dataset}}.dsc_DACase_latest` dac
            USING (OffenderId, DACaseId)
        # Offense types
        INNER JOIN `{{project_id}}.{{us_ix_raw_data_up_to_date_dataset}}.scl_DorOffenseType_latest` dot
            USING (DorOffenseTypeId)
        WHERE SUBSTR(dot.DorOffenseCode, 1, 1) IN {str(tuple(classes_to_include))}
        -- Remove dismissed DORs
            AND dac.DAProcedureStatusId NOT IN ('3','7','15','18','21','22','25')
            -- According to ID, affirmed DORs are (2,6,11,17,20,23,26)
        #Only keep the latest record for each offender-case
        QUALIFY ROW_NUMBER() OVER(PARTITION BY dap.DACaseId, dap.OffenderId ORDER BY dap.InsertDate DESC) = 1
    )"""

    return fix_indent(query, indent_level=indent_level)


def program_enrollment_query(indent_level: int = 8) -> str:
    """
    Returns a SQL query that selects information about program enrollments. Some facts:
    - The query includes the offender ID, criteria, note title, note body, and event date.
    - Only records with an enrollment status of 'Complete', 'Enrolled', or 'Waitlisted'
    are included.
    - Only programs that were started at current incarceration periods are included
    """
    query = """
    SELECT 
        ce.OffenderId AS external_id,
        "Program enrollment" AS criteria,
        CourseNameName AS note_title,
        CONCAT(ces.OfdEnrollmentStatusDesc, ' - ', CourseSectionNameName) AS note_body,
        event_date
    FROM (
        SELECT 
            *,
            COALESCE( CAST(LEFT(ce.EndDate, 10) AS DATE),
                CAST(LEFT(ce.StartDate, 10) AS DATE),
                CAST(LEFT(ce.PendingEnrollmentDate, 10) AS DATE)) AS event_date
        FROM `{project_id}.{us_ix_raw_data_up_to_date_dataset}.crs_OfdCourseEnrollment_latest` ce  
    ) ce
    LEFT JOIN 
        `{project_id}.{us_ix_raw_data_up_to_date_dataset}.crs_OfdEnrollmentStatus_latest` ces
    ON 
        ce.OfdEnrollmentStatusId = ces.OfdEnrollmentStatusId
    LEFT JOIN 
        `{project_id}.{us_ix_raw_data_up_to_date_dataset}.crs_Course_latest` co
    ON 
        ce.CourseId = co.CourseId
    LEFT JOIN 
        `{project_id}.{us_ix_raw_data_up_to_date_dataset}.crs_CourseName_latest` con
    ON 
        co.CourseNameId = con.CourseNameId
    LEFT JOIN 
        `{project_id}.{us_ix_raw_data_up_to_date_dataset}.crs_CourseSection_latest` cs
    ON 
        cs.CourseSectionId = ce.CourseSectionId
    LEFT JOIN 
        `{project_id}.{us_ix_raw_data_up_to_date_dataset}.crs_CourseSectionName_latest` csn
    ON 
        cs.CourseSectionNameId = csn.CourseSectionNameId
    INNER JOIN `{project_id}.{normalized_state_dataset}.state_person_external_id` pei
        ON ce.OffenderId = pei.external_id
         AND pei.state_code = 'US_IX'
         AND pei.id_type = 'US_IX_DOC'
    -- Only inlcude enrollments that started during the current incarceration period
    INNER JOIN (
        SELECT person_id, start_date
        FROM `{project_id}.{sessions_dataset}.compartment_level_0_super_sessions_materialized` 
        WHERE state_code = 'US_IX'
            AND compartment_level_0 = 'INCARCERATION'
            AND CURRENT_DATE('US/Pacific') < IFNULL(end_date, '9999-12-31')
    ) ss
    ON pei.person_id = ss.person_id
        AND ss.start_date < event_date
    WHERE 
        ce.OfdEnrollmentStatusId IN ('1', '2', '3')
    """

    return fix_indent(query, indent_level=indent_level)


def victim_alert_notes(indent_level: int = 8) -> str:
    """
    Returns a SQL query that selects information about victim alerts.
    Args:
        indent_level (int): Indent level for the query. Defaults to 8.
    """

    query = """
    SELECT
        OffenderId AS external_id,
        'Victim Alerts' AS criteria,
        'Attention' AS note_title,
        Notes AS note_body,
        SAFE_CAST(LEFT(StartDate, 10) AS DATE) AS event_date
    FROM `{project_id}.{us_ix_raw_data_up_to_date_dataset}.ind_Offender_Alert_latest`
    -- Victim alerts
    WHERE AlertId = '133'"""

    return fix_indent(query, indent_level=indent_level)


def supervision_level_criteria_query(
    excluded_levels: List[str],
) -> str:
    return f"""
#TODO(#22511) refactor to build off of a general criteria view builder
WITH so_spans AS (
SELECT
        state_code,
        person_id,
        start_date,
        end_date_exclusive AS end_date,
    #TODO(#20035) replace with supervision level raw text sessions once views agree
    FROM `{{project_id}}.{{sessions_dataset}}.compartment_sub_sessions_materialized`
    WHERE compartment_level_1 = 'SUPERVISION'
    AND correctional_level_raw_text IN {tuple(excluded_levels)}
    AND state_code = 'US_IX'
)
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date,
        FALSE AS meets_criteria,
    TO_JSON(STRUCT(TRUE AS supervision_level_is_so)) AS reason,
    TRUE AS supervision_level_is_so,
    FROM ({aggregate_adjacent_spans(table_name='so_spans')})
"""


def eprd_cte() -> str:
    """
    Defines a CTE that calculates the Earliest Possible Release Date (EPRD) for individuals in Idaho.

    The EPRD is calculated as specified below:
        EPRD = TPD, if the TPD exists
        EPRD = FTRD, if both TPD and PHD do not exist
        EPRD = PED, if TPD does not exist, the PHD is prior to the PED, and the PED after today.
        EPRD = PHD, if TPD does not exist and PHD is after PED.
    """

    return f"""ped_tpd_ftrd_spans AS (
SELECT
    span.state_code,
    span.person_id,
    span.start_date,
    span.end_date_exclusive AS end_date,
    span.group_parole_eligibility_date AS parole_eligibility_date,
    span.group_projected_parole_release_date AS tentative_parole_date,
    span.group_projected_full_term_release_date_max AS full_term_release_date
FROM `{{project_id}}.{{sentence_sessions_v2_dataset}}.person_projected_date_sessions_materialized` span
WHERE span.state_code = 'US_IX'
),
ped_tpd_phd_ftrd_spans AS (
SELECT
    state_code,
    person_id,
    start_date,
    -- Only retain PEDs in the future
    LEAST({nonnull_end_date_clause('end_date')}, parole_eligibility_date) AS end_date,
    parole_eligibility_date,
    NULL AS tentative_parole_date,
    NULL AS parole_hearing_date,
    NULL AS full_term_release_date
FROM ped_tpd_ftrd_spans
WHERE parole_eligibility_date IS NOT NULL
  -- Avoid zero-day spans when parole_eligibility_date <= start_date
  AND parole_eligibility_date > start_date

UNION ALL

SELECT 
    state_code,
    person_id,
    start_date,
    end_date,
    NULL AS parole_eligibility_date,
    tentative_parole_date,
    NULL AS parole_hearing_date,
    full_term_release_date
FROM ped_tpd_ftrd_spans ped

UNION ALL

SELECT 
    state_code,
    person_id,
    start_date,
    end_date_exclusive AS end_date,
    NULL AS parole_eligibility_date,
    NULL AS tentative_parole_date,
    pds.initial_parole_hearing_date AS parole_hearing_date,
    NULL AS full_term_release_date
FROM `{{project_id}}.{{analyst_dataset}}.us_ix_parole_dates_spans_preprocessing_materialized` pds

UNION ALL

SELECT 
    pds.state_code,
    pds.person_id,
    start_date,
    end_date_exclusive AS end_date,
    NULL AS parole_eligibility_date,
    NULL AS tentative_parole_date,
    pds.next_parole_hearing_date AS parole_hearing_date,
    NULL AS full_term_release_date
FROM `{{project_id}}.{{analyst_dataset}}.us_ix_parole_dates_spans_preprocessing_materialized` pds
),
{create_sub_sessions_with_attributes(
table_name="ped_tpd_phd_ftrd_spans"
)},
grouped_sub_sessions AS (
SELECT
    state_code,
    person_id,
    start_date,
    end_date,
    MIN(parole_eligibility_date) AS parole_eligibility_date, 
    MIN(tentative_parole_date) AS tentative_parole_date, 
    --take the MIN of all future parole hearing dates, or if none exist, the MAX of all past parole hearing dates
    --this only works based on the subsessionizing done in us_ix_parole_dates_spans_preprocessing_materialized
    COALESCE(
        MIN(IF(parole_hearing_date > start_date, parole_hearing_date, NULL)),
        MAX(parole_hearing_date)
    ) AS parole_hearing_date,
    MIN(full_term_release_date) AS full_term_release_date,
FROM sub_sessions_with_attributes
GROUP BY 1,2,3,4
),
eprd_sessions AS (
SELECT
    state_code,
    person_id,
    start_date,
    end_date,
    parole_eligibility_date,
    tentative_parole_date,
    parole_hearing_date,
    full_term_release_date,
    CASE
        WHEN tentative_parole_date IS NOT NULL THEN tentative_parole_date
        WHEN parole_hearing_date IS NULL THEN full_term_release_date
        WHEN parole_eligibility_date > parole_hearing_date THEN parole_eligibility_date
        WHEN (parole_hearing_date >= parole_eligibility_date OR parole_eligibility_date IS NULL) THEN parole_hearing_date
        ELSE NULL
    END AS earliest_possible_release_date
FROM grouped_sub_sessions
)"""


def eprd_within_x_months_query(
    num_months: int,
    meets_criteria_bool: bool = True,
    indent_level: int = 8,
) -> str:
    """
    Defines a criteria span view that shows periods of time during which someone is
    within x months of their Earliest Possible Release Date (EPRD) and has not yet
    passed that date.

    Args:
        num_months (int): Number of months to use for the leading window time.
        meets_criteria_bool (bool): Whether to return spans that meet or do not meet
            the criteria. Defaults to True.
        indent_level (int): Indent level for the query. Defaults to 8.
    """
    meets_criteria_condition = "" if meets_criteria_bool else "NOT"
    query = f"""
    WITH {eprd_cte()},
    critical_date_spans AS (
        SELECT
            state_code,
            person_id,
            start_date AS start_datetime,
            --crop spans so that EPRDs are always upcoming
            LEAST({nonnull_end_date_clause('end_date')}, earliest_possible_release_date) AS end_datetime,
            earliest_possible_release_date AS critical_date,
            parole_eligibility_date,
            tentative_parole_date,
            parole_hearing_date,
            full_term_release_date,
            earliest_possible_release_date
        FROM eprd_sessions
        --only include spans where EPRD is upcoming
        WHERE earliest_possible_release_date > start_date
    ),
    {critical_date_has_passed_spans_cte(meets_criteria_leading_window_time=num_months,
                                        attributes=["parole_eligibility_date",
                                                    "tentative_parole_date",
                                                    "parole_hearing_date",
                                                    "full_term_release_date",
                                                    "earliest_possible_release_date"],
                                        date_part="MONTH")}
    SELECT
        state_code,
        person_id,
        start_date,
        end_date,
        {meets_criteria_condition} critical_date_has_passed AS meets_criteria,
        TO_JSON(STRUCT(parole_eligibility_date,
                    tentative_parole_date,
                    parole_hearing_date,
                    full_term_release_date,
                    earliest_possible_release_date)) AS reason,
        parole_eligibility_date,
        tentative_parole_date,
        parole_hearing_date,
        full_term_release_date,
        earliest_possible_release_date
    FROM critical_date_has_passed_spans
    WHERE start_date IS DISTINCT FROM end_date"""

    return fix_indent(query, indent_level=indent_level)


def us_ix_active_supervision_population_view_builder(
    population_name: str,
    description: str,
    case_types: List[str],
    supervision_levels: List[str],
    additional_where_clause: Optional[str] = None,
    additional_cte: Optional[str] = None,
) -> StateSpecificTaskCandidatePopulationBigQueryViewBuilder:
    """
    Creates a VIEW_BUILDER for Idaho active supervision population with specified filters.

    This includes people on PROBATION, PAROLE, DUAL, or COMMUNITY_CONFINEMENT
    with specified case types and supervision levels.

    Args:
        population_name: The name of the population (e.g., 'US_IX_ACTIVE_SUPERVISION_POPULATION_FOR_TASKS')
        description: The description/docstring for the population
        case_types: List of case types to include (e.g., ['GENERAL', 'SEX_OFFENSE'])
        supervision_levels: List of supervision levels to include (e.g., ['MINIMUM', 'MEDIUM', 'HIGH'])
        additional_where_clause: Optional additional WHERE clause to apply to compartment_sub_sessions.
            Should start with 'AND'. Example: "AND css.sex = 'MALE'"
        additional_cte: Optional additional CTE to intersect with the population. Should be a CTE
            name that selects (state_code, person_id, start_date, end_date). When provided, the
            population will only include spans where all criteria overlap. Example:
            "able_to_work AS (SELECT state_code, person_id, start_date, end_date FROM ...)"
    """
    case_types_str = "('" + "', '".join(case_types) + "')"
    supervision_levels_str = "('" + "', '".join(supervision_levels) + "')"

    # Build additional where clause if specified
    where_clause_addition = additional_where_clause or ""

    # Build additional CTE and union if specified
    additional_cte_definition = ""
    additional_union = ""
    # Base count is 2 (active_supervision_population + supervision_case_and_level)
    required_count = 2
    if additional_cte:
        # Extract the CTE name from the definition (assumes format "cte_name AS (...)")
        cte_name = additional_cte.split(" AS ")[0].strip()
        additional_cte_definition = f"""{additional_cte},"""
        additional_union = f"""UNION ALL

    SELECT
        state_code,
        person_id,
        start_date,
        end_date,
        SAFE_CAST(NULL AS STRING) AS compartment_level_1,
        SAFE_CAST(NULL AS STRING) AS compartment_level_2,
        SAFE_CAST(NULL AS STRING) AS case_type,
        SAFE_CAST(NULL AS STRING) AS supervision_level
    FROM {cte_name}"""
        required_count = 3

    query_template = f"""WITH active_supervision_population AS (
    -- Active supervision population on PROBATION, PAROLE, or DUAL supervision
    SELECT
        css.state_code,
        css.person_id,
        css.start_date,
        css.end_date_exclusive AS end_date,
        css.compartment_level_1,
        css.compartment_level_2,
        SAFE_CAST(NULL AS STRING) AS case_type,
        SAFE_CAST(NULL AS STRING) AS supervision_level
    FROM
        `{{project_id}}.sessions.compartment_sub_sessions_materialized` css
    WHERE css.compartment_level_1 IN ('SUPERVISION')
        AND css.metric_source != "INFERRED"
        AND css.compartment_level_2 IN ('PROBATION', 'PAROLE', 'DUAL', 'COMMUNITY_CONFINEMENT')
        AND css.correctional_level NOT IN ('IN_CUSTODY','WARRANT','ABSCONDED','ABSCONSION','EXTERNAL_UNKNOWN')
        AND css.start_date >= '1900-01-01'
        {where_clause_addition}
),

supervision_case_and_level AS (
    -- Filter by case types and supervision levels
    SELECT
        ctsl.state_code,
        ctsl.person_id,
        ctsl.start_date,
        ctsl.end_date,
        SAFE_CAST(NULL AS STRING) AS compartment_level_1,
        SAFE_CAST(NULL AS STRING) AS compartment_level_2,
        ctsl.case_type,
        ctsl.supervision_level,
    FROM `{{project_id}}.tasks_views.us_ix_case_type_supervision_level_spans_materialized` ctsl
        WHERE ctsl.case_type IN {case_types_str}
            AND ctsl.supervision_level IN {supervision_levels_str}
),

{additional_cte_definition}

combine AS (
    SELECT *
    FROM active_supervision_population

    UNION ALL

    SELECT *
    FROM supervision_case_and_level
    
    {additional_union}
),

{create_sub_sessions_with_attributes(table_name="combine")}

SELECT
    state_code,
    person_id,
    start_date,
    end_date,
FROM sub_sessions_with_attributes
WHERE start_date != {nonnull_end_date_clause('end_date')}
GROUP BY 1,2,3,4
-- We only want spans where all criteria are met
HAVING COUNT(*) >= {required_count}
"""

    return StateSpecificTaskCandidatePopulationBigQueryViewBuilder(
        state_code=StateCode.US_IX,
        population_name=population_name,
        population_spans_query_template=query_template,
        description=description,
    )


def us_ix_supervision_start_cte() -> str:
    """
    Returns a SQL CTE that selects the start of supervision sessions in Idaho.
    This is used for initial assessment triggers.
    """
    return f"""
supervision_start AS (
    -- Get the first compartment_level_1_super_session for each supervision_super_session
    SELECT
        css.state_code,
        css.person_id,
        sss.end_date_exclusive AS end_datetime,
        MIN(css.start_date) AS start_of_supervision_date,
        MIN(css.start_date) AS start_datetime,
    FROM `{{project_id}}.sessions.compartment_level_1_super_sessions_materialized` css
    LEFT JOIN `{{project_id}}.sessions.supervision_super_sessions_materialized` sss
        ON css.state_code = sss.state_code
            AND css.person_id = sss.person_id
            AND css.start_date BETWEEN sss.start_date AND {nonnull_end_date_clause('sss.end_date')}
    WHERE css.state_code = 'US_IX'
        AND css.start_reason IN ('COURT_SENTENCE', 'RELEASE_FROM_INCARCERATION')
        AND css.compartment_level_1 = 'SUPERVISION'
    GROUP BY 1,2,3
)"""


def us_ix_annual_assessment_criteria_view_builder(
    criteria_name: str,
    description: str,
    assessment_type: str,
    assessment_class: str,
) -> StateSpecificTaskCriteriaBigQueryViewBuilder:
    """
    Creates a VIEW_BUILDER for annual assessment criteria in Idaho.

    This criteria shows spans of time for which supervision clients need an assessment
    reassessment. This should happen every 365 days after the last assessment, and is
    triggered 30 days before that date.

    Args:
        criteria_name: The name of the criteria (e.g., 'US_IX_IS_MISSING_ANNUAL_LSIR_ASSESSMENT')
        description: The description/docstring for the criteria
        assessment_type: The assessment type to filter (e.g., 'LSIR', 'STABLE')
        assessment_class: The assessment class to filter (e.g., 'RISK', 'SEX_OFFENSE')
    """
    query_template = f"""
WITH supervision_sessions AS (
    SELECT
        state_code,
        person_id,
        start_date,
        end_date_exclusive AS end_date,
    FROM `{{project_id}}.sessions.supervision_super_sessions_materialized` css
    WHERE css.state_code = 'US_IX'
),

assessments AS (
    SELECT DISTINCT
        state_code,
        person_id,
        assessment_date,
    FROM `{{project_id}}.sessions.assessment_score_sessions_materialized`
    WHERE state_code = 'US_IX'
        AND assessment_type = '{assessment_type}'
        AND assessment_class = '{assessment_class}'
),

critical_date_spans AS (
    SELECT
        ss.state_code,
        ss.person_id,
        IFNULL(a.assessment_date, ss.start_date) AS start_datetime,
        -- The end_datetime is either the next assessment date (if it exists) or the end of the
        -- supervision session.
        IF(
            a.assessment_date IS NOT NULL,
            -- next_assessment date OR end_date, whichever is earlier
            LEAST(
                IFNULL(
                    LEAD(a.assessment_date) OVER (
                        PARTITION BY ss.state_code, ss.person_id
                        ORDER BY a.assessment_date
                    ),
                    '9999-12-31'
                ),
                {nonnull_end_date_clause('ss.end_date')}
            ),
            ss.end_date
        ) AS end_datetime,
        DATE_ADD(a.assessment_date, INTERVAL 365 DAY) AS critical_date,
        a.assessment_date AS last_assessment_date,
    FROM supervision_sessions ss
    LEFT JOIN assessments a
        ON ss.person_id = a.person_id
            AND ss.state_code = a.state_code
            AND a.assessment_date BETWEEN ss.start_date AND {nonnull_end_date_exclusive_clause('ss.end_date')}
),

{critical_date_has_passed_spans_cte(
    meets_criteria_leading_window_time=30,
    attributes=['last_assessment_date'])}

SELECT
    state_code,
    person_id,
    start_date,
    end_date,
    critical_date_has_passed AS meets_criteria,
    TO_JSON(
        STRUCT(
            critical_date AS assessment_due_date,
            last_assessment_date AS last_assessment_date,
            critical_date_has_passed AS is_missing_annual_assessment,
            '1 EVERY 365 DAYS' AS contact_cadence,
            '{assessment_type}' AS assessment_type,
            '{assessment_class}' AS assessment_class
    )) AS reason,
    critical_date AS assessment_due_date,
    last_assessment_date,
    critical_date_has_passed AS is_missing_annual_assessment,
    '1 EVERY 365 DAYS' AS contact_cadence,
    '{assessment_type}' AS assessment_type,
    '{assessment_class}' AS assessment_class,
FROM critical_date_has_passed_spans
"""

    return StateSpecificTaskCriteriaBigQueryViewBuilder(
        state_code=StateCode.US_IX,
        criteria_name=criteria_name,
        criteria_spans_query_template=query_template,
        description=description,
        reasons_fields=[
            ReasonsField(
                name="assessment_due_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Due date of the assessment.",
            ),
            ReasonsField(
                name="last_assessment_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Date of the last assessment.",
            ),
            ReasonsField(
                name="is_missing_annual_assessment",
                type=bigquery.enums.StandardSqlTypeNames.BOOL,
                description="Whether the client is missing an annual assessment.",
            ),
            ReasonsField(
                name="contact_cadence",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="The required cadence for assessments.",
            ),
            ReasonsField(
                name="assessment_type",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="The type of assessment.",
            ),
            ReasonsField(
                name="assessment_class",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="The class of assessment.",
            ),
        ],
    )


def crc_case_notes_cte(
    crc_denied_months: int = 6,
    include_work_history: bool = False,
) -> str:
    """
    Returns a SQL query string for case notes relevant to CRC (Community
    Reentry Center) eligibility in Idaho, with all queries joined by UNION ALL.

    Args:
        crc_denied_months: Number of months to look back for CRC denied case notes.
            Defaults to 6 months.
        include_work_history: Whether to include work history case notes.
            Defaults to False.

    The case notes include:
    - Offender alerts (excluding victims)
    - Institutional behavior notes (corrective action and positive)
    - Release information
    - CRC information and denial notes
    - I-9 documents
    - Work history (optional)
    - Medical clearance
    - Violent charges being served
    - NCIC/ILETS checks
    - Escape, absconsion, or eluding police history
    - Detainers
    - DORs (Disciplinary Offense Reports)
    - Program enrollment
    - Victim alerts
    """
    union_separator = "\n\n        UNION ALL\n\n"

    queries = [
        # Offender alerts (excluding victims)
        "--Offender alerts (excluding victims)\n"
        + ix_offender_alerts_case_notes(where_clause="WHERE AlertId != '133'"),
        # Institutional Behavior Notes - Corrective Action
        "        --Institutional Behavior Notes - Corrective Action\n"
        + ix_general_case_notes(
            where_clause_addition="AND ContactModeDesc = 'Corrective Action'",
            criteria_str=INSTITUTIONAL_BEHAVIOR_NOTES_STR,
            in_the_past_x_months=6,
        ),
        # Positive behavior notes
        "        --Positive [behavior notes]\n"
        + ix_general_case_notes(
            where_clause_addition="AND ContactModeDesc = 'Positive'",
            criteria_str=INSTITUTIONAL_BEHAVIOR_NOTES_STR,
            in_the_past_x_months=6,
        ),
        # Release information (in the past 3 years)
        "        --Release information (in the past 3 years)\n"
        + ix_general_case_notes(
            where_clause_addition=f"AND ContactModeDesc IN {RELEASE_INFORMATION_CONTACT_MODES}",
            criteria_str=RELEASE_INFORMATION_STR,
            in_the_past_x_months=36,
        ),
        # Additional CRC info (in the past 6 months)
        "        --Additional CRC info (in the past 6 months)\n"
        + ix_general_case_notes(
            where_clause_addition=f"AND ContactModeDesc IN {CRC_INFORMATION_CONTACT_MODES}",
            criteria_str=CRC_INFORMATION_STR,
            in_the_past_x_months=6,
        ),
        # CRC denied case notes
        f"        --CRC denied case notes (in the past {crc_denied_months} months)\n"
        + ix_general_case_notes(
            where_clause_addition="AND ContactModeDesc = 'CRC Termer Denied'",
            criteria_str="CRC Denial Case Notes",
            in_the_past_x_months=crc_denied_months,
        ),
        # I-9 Documents
        "        --I-9 Documents\n"
        + ix_general_case_notes(
            where_clause_addition=f"AND REGEXP_CONTAINS(UPPER(note.Details), r'{I9_NOTE_TX_REGEX}')",
            criteria_str=I9_NOTES_STR,
            in_the_past_x_months=60,
        ),
        # Medical clearance
        "        --Medical clearance\n"
        + ix_general_case_notes(
            where_clause_addition=f"AND REGEXP_CONTAINS(UPPER(note.Details), r'{MEDICAL_CLEARANCE_TX_REGEX}')",
            criteria_str=MEDICAL_CLEARANCE_STR,
            in_the_past_x_months=6,
        ),
        # Violent charges being served
        "        --Violent charges being served\n"
        + "("
        + current_violent_statutes_being_served(state_code="US_IX")
        + ")",
        # NCIC/ILETS
        "        --NCIC/ILETS\n"
        + ix_fuzzy_matched_case_notes(where_clause="WHERE ncic_ilets_nco_check"),
        # Recent escape, absconsion or eluding police
        "        --Recent escape, absconsion or eluding police\n"
        + escape_absconsion_or_eluding_police_case_notes(),
        # Detainers
        "        --Detainers\n" + detainer_case_notes(),
        # DORs
        "        --DORs\n"
        + dor_query(
            columns_str=DOR_CASE_NOTES_COLUMNS, classes_to_include=["A", "B", "C"]
        )
        + "\nWHERE event_date > DATE_SUB(CURRENT_DATE('US/Eastern'), INTERVAL 6 MONTH)",
        # Program Enrollment
        "        --Program Enrollment\n" + program_enrollment_query(),
        # Victim alerts
        "        --Victim alerts\n" + victim_alert_notes(),
    ]

    # Work history (optional, for resident worker)
    if include_work_history:
        queries.append(
            "        --Work History\n"
            + ix_general_case_notes(
                where_clause_addition="AND ContactModeDesc = 'Work History'",
                criteria_str=WORK_HISTORY_STR,
                in_the_past_x_months=60,
            )
        )

    return union_separator.join(queries)


def release_district_criteria_query(release_districts: list[str]) -> str:
    """
    Returns a SQL query template for release district criteria.

    This query identifies spans of time during which someone has a CRC release
    district case note matching the specified districts.

    Args:
        release_districts: List of release districts to match (e.g., ['1', '2', 'ISC']
            or ['3', '4', '5', '6', '7']).

    Returns:
        str: SQL query template as a string.
    """
    release_districts_str = "('" + "', '".join(release_districts) + "')"

    return f"""
WITH case_notes AS ({ix_general_case_notes(
    where_clause_addition="AND ContactModeDesc LIKE '%CRC Request%'",
    criteria_str="CRC Release District case note")}
    --choose the most recent note
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY external_id, event_date
        -- If there's multiple notes on the same date for a person,
        -- pick one deterministically
        ORDER BY note_title
    ) = 1
),
case_notes_plus_sentinel AS (
    -- Add a sentinel date
    SELECT external_id, event_date, note_title
    FROM case_notes

    UNION ALL

    SELECT DISTINCT external_id, SAFE_CAST('1900-01-01' AS DATE) AS event_date, '' AS note_title
    FROM case_notes
),
super_sessions_with_case_notes AS (
    -- Join incarceration sessions to case notes to get spans with release districts
    SELECT
        sess.state_code,
        sess.person_id,
        GREATEST(sess.start_date, c.event_date) AS start_date,
        LEAST({nonnull_end_date_exclusive_clause("sess.end_date_exclusive")},
            IFNULL(LEAD(c.event_date) OVER (
                PARTITION BY sess.state_code, sess.person_id ORDER BY c.event_date),
            '9999-12-31')
        ) AS end_date,
        REGEXP_EXTRACT(c.note_title, r'ISC|\\d+') AS release_district
    FROM `{{project_id}}.sessions.compartment_level_1_super_sessions_materialized` sess
    INNER JOIN `{{project_id}}.reference_views.product_stable_person_external_ids_materialized` pei
        USING (state_code, person_id)
    INNER JOIN case_notes c
        ON c.event_date BETWEEN sess.start_date AND {nonnull_end_date_exclusive_clause("sess.end_date_exclusive")}
            AND c.external_id = pei.stable_person_external_id
            AND pei.system_type = 'INCARCERATION'
            AND pei.state_code='US_IX'
    WHERE sess.compartment_level_1 = 'INCARCERATION'
)

SELECT
    state_code,
    person_id,
    start_date,
    end_date,
    release_district IN {release_districts_str} AS meets_criteria,
    TO_JSON(STRUCT(release_district AS release_district)) AS reason,
    release_district,
FROM super_sessions_with_case_notes
"""
