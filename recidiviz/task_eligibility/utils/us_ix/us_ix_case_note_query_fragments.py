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
"""Helper SQL queries that build Idaho case notes for opportunity records, one row
per note with external_id, criteria, note_title, note_body, and event_date."""

from typing import Optional

from recidiviz.calculator.query.bq_utils import (
    nonnull_end_date_clause,
    today_between_start_date_and_nullable_end_date_exclusive_clause,
)
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.calculator.query.state.views.analyst_data.us_ix.us_ix_release_district_spans import (
    ADDRESS_RELEASE_DISTRICT_COLUMN_NAME,
    PHYSICAL_ADDRESS_COLUMN_NAME,
    RELEASE_DISTRICT_SOURCE_COLUMN_NAME,
    SENTENCING_COUNTY_NAME_COLUMN_NAME,
    SENTENCING_COUNTY_RELEASE_DISTRICT_COLUMN_NAME,
    US_IX_RELEASE_DISTRICT_SPANS_VIEW_NAME,
    UsIxReleaseDistrictSource,
    release_district_from_crc_request_note_title,
)
from recidiviz.calculator.query.state.views.workflows.firestore.opportunity_record_query_fragments import (
    current_violent_statutes_being_served,
)
from recidiviz.observations.span_observation_big_query_view_builder import fix_indent
from recidiviz.task_eligibility.utils.state_dataset_query_fragments import (
    extract_object_from_json,
)
from recidiviz.task_eligibility.utils.us_ix.us_ix_dor_query_fragments import (
    us_ix_dors_query,
)

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
            FROM `{{project_id}}.{{us_ix_raw_data_up_to_date_dataset}}.ind_Offender_Alert_latest_materialized` oa
            INNER JOIN `{{project_id}}.{{us_ix_raw_data_up_to_date_dataset}}.ind_Alert_latest_materialized` ia
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
    *,
    include_note_type: bool = False,
    include_note_id_and_contact_mode_code: bool = False,
) -> str:
    """
    Returns a SQL query that returns case notes for a specified contact mode in Idaho.

    Args:
        criteria_str (str): Criteria to use for the criteria column.
        in_the_past_x_months (int): Number of months to use for the date interval.
            Defaults to 6.
        where_clause_addition (str): Where clause to use in the query. Defaults to ''.
        indent_level (int): Indent level for the query. Defaults to 2.
        include_note_type (bool): Whether to add a note_type column holding the
            note's Atlas note type (e.g. Facility Notes, Supervision Notes). Off by
            default, so callers that union this with other case note queries keep
            the same columns.
        include_note_id_and_contact_mode_code (bool): Whether to add a
            note_external_id column holding the note's OffenderNoteId and a
            note_contact_mode_code column holding the contact mode's abbreviation
            (e.g. POS). Off by default for the same reason.
    Returns:
        str: SQL query as a string.
    """
    optional_columns = []
    if include_note_type:
        optional_columns.append("note_type.NoteTypeDesc AS note_type,")
    if include_note_id_and_contact_mode_code:
        optional_columns.append("note.OffenderNoteId AS note_external_id,")
        optional_columns.append("note_type_cm.Abbreviation AS note_contact_mode_code,")
    optional_columns_sql = "".join(
        f"\n                {column}" for column in optional_columns
    )
    group_by_columns = ",".join(str(i) for i in range(1, 6 + len(optional_columns)))
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
                SAFE_CAST(LEFT(info.NoteDate, 10) AS DATE) AS event_date,{optional_columns_sql}
            FROM `{{project_id}}.{{us_ix_raw_data_up_to_date_dataset}}.ind_OffenderNote_latest_materialized` note
            LEFT JOIN `{{project_id}}.{{us_ix_raw_data_up_to_date_dataset}}.ind_OffenderNoteInfo_latest_materialized` info
                USING (OffenderNoteInfoId)
            LEFT JOIN `{{project_id}}.{{us_ix_raw_data_up_to_date_dataset}}.ref_NoteType_latest_materialized` note_type
                USING (NoteTypeId)
            LEFT JOIN `{{project_id}}.{{us_ix_raw_data_up_to_date_dataset}}.ind_OffenderNoteInfo_ContactMode_latest_materialized` info_cm
                USING (OffenderNoteInfoId)
            LEFT JOIN `{{project_id}}.{{us_ix_raw_data_up_to_date_dataset}}.ind_ContactMode_latest_materialized` note_type_cm
                USING (ContactModeId)
            WHERE OffenderNoteStatusId = '2' -- Posted
                {where_clause_addition}
            GROUP BY {group_by_columns}
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
         AND pei.id_type = 'US_IX_DOC'
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
         AND pei.id_type = 'US_IX_DOC'
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
            AND pei.id_type = 'US_IX_DOC'
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


def dor_case_notes_query(*, in_the_past_x_months: int, indent_level: int) -> str:
    """Returns a SQL query for the class A, B and C DORs from the past
    in_the_past_x_months months, as opportunity case notes keyed on the resident's
    current US_IX_DOC display id."""
    query = f"""
    SELECT
        pei.external_id,
        'Disciplinary Offense Reports (in the past {in_the_past_x_months} months)' AS criteria,
        CONCAT('Class ', dor.dor_class, ': ', dor.offense_type_name) AS note_title,
        dor.dor_description AS note_body,
        dor.incident_date AS event_date,
    FROM (
{fix_indent(us_ix_dors_query(dor_classes=["A", "B", "C"]), indent_level=8)}
    ) dor
    INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
        ON pei.person_id = dor.person_id
        AND pei.id_type = 'US_IX_DOC'
        AND pei.is_current_display_id_for_type
    WHERE dor.incident_date > DATE_SUB(CURRENT_DATE('US/Eastern'), INTERVAL {in_the_past_x_months} MONTH)"""
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
        FROM `{project_id}.{us_ix_raw_data_up_to_date_dataset}.crs_OfdCourseEnrollment_latest_materialized` ce  
    ) ce
    LEFT JOIN 
        `{project_id}.{us_ix_raw_data_up_to_date_dataset}.crs_OfdEnrollmentStatus_latest_materialized` ces
    ON 
        ce.OfdEnrollmentStatusId = ces.OfdEnrollmentStatusId
    LEFT JOIN 
        `{project_id}.{us_ix_raw_data_up_to_date_dataset}.crs_Course_latest_materialized` co
    ON 
        ce.CourseId = co.CourseId
    LEFT JOIN 
        `{project_id}.{us_ix_raw_data_up_to_date_dataset}.crs_CourseName_latest_materialized` con
    ON 
        co.CourseNameId = con.CourseNameId
    LEFT JOIN 
        `{project_id}.{us_ix_raw_data_up_to_date_dataset}.crs_CourseSection_latest_materialized` cs
    ON 
        cs.CourseSectionId = ce.CourseSectionId
    LEFT JOIN 
        `{project_id}.{us_ix_raw_data_up_to_date_dataset}.crs_CourseSectionName_latest_materialized` csn
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
    FROM `{project_id}.{us_ix_raw_data_up_to_date_dataset}.ind_Offender_Alert_latest_materialized`
    -- Victim alerts
    WHERE AlertId = '133'"""

    return fix_indent(query, indent_level=indent_level)


def release_district_case_notes() -> str:
    """Returns the case notes that show where a resident's release district comes
    from. A resident with CRC Request notes in the current incarceration gets those
    notes. Otherwise they get one note explaining the district from their home address
    or, without an Idaho one, from the county of their latest sentence, matching
    release_district_source in us_ix_release_district_spans.

    For example, a resident with a CRC Request note this incarceration gets the first
    row below, and one with no such note but an Ada County home address gets the
    second (columns abridged):

        note_title,note_body
        Release district from CRC Request note: DISTRICT 3,"CRC Request - D3 Release: Plans to live with family in Nampa."
        Release district from home address: DISTRICT 4,"No CRC Request note this incarceration. Home address on file: 123 MAIN ST, BOISE ID 83702 (Ada County), which is in DISTRICT 4."
    """
    return f"""
        SELECT
            crc_notes.external_id,
            crc_notes.criteria,
            CONCAT(
                'Release district from CRC Request note: ',
                {release_district_from_crc_request_note_title("crc_notes.note_title")}
            ) AS note_title,
            CONCAT(crc_notes.note_title, ': ', IFNULL(crc_notes.note_body, '')) AS note_body,
            crc_notes.event_date,
        FROM (
{ix_general_case_notes(
    where_clause_addition=f"AND ContactModeDesc IN {RELEASE_INFORMATION_CONTACT_MODES}",
    criteria_str=RELEASE_INFORMATION_STR,
    indent_level=12,
)}
        ) crc_notes
        INNER JOIN `{{project_id}}.reference_views.product_stable_person_external_ids_materialized` pei
            ON crc_notes.external_id = pei.stable_person_external_id
            AND pei.state_code = 'US_IX'
            AND pei.system_type = 'INCARCERATION'
        INNER JOIN `{{project_id}}.sessions.compartment_level_1_super_sessions_materialized` sess
            ON pei.person_id = sess.person_id
            AND sess.compartment_level_1 = 'INCARCERATION'
            AND {today_between_start_date_and_nullable_end_date_exclusive_clause(
                start_date_column="sess.start_date",
                end_date_column="sess.end_date_exclusive",
            )}
        WHERE crc_notes.event_date >= sess.start_date

        UNION ALL

        SELECT
            pei.stable_person_external_id AS external_id,
            '{RELEASE_INFORMATION_STR}' AS criteria,
            CASE rds.{RELEASE_DISTRICT_SOURCE_COLUMN_NAME}
                WHEN '{UsIxReleaseDistrictSource.ADDRESS.value}' THEN CONCAT(
                    'Release district from home address: ',
                    rds.{ADDRESS_RELEASE_DISTRICT_COLUMN_NAME}
                )
                WHEN '{UsIxReleaseDistrictSource.SENTENCING_COUNTY.value}' THEN CONCAT(
                    'Release district from sentencing county: ',
                    rds.{SENTENCING_COUNTY_RELEASE_DISTRICT_COLUMN_NAME}
                )
            END AS note_title,
            CASE rds.{RELEASE_DISTRICT_SOURCE_COLUMN_NAME}
                WHEN '{UsIxReleaseDistrictSource.ADDRESS.value}' THEN CONCAT(
                    'No CRC Request note this incarceration. Home address on file: ',
                    rds.{PHYSICAL_ADDRESS_COLUMN_NAME},
                    ', which is in ',
                    rds.{ADDRESS_RELEASE_DISTRICT_COLUMN_NAME},
                    '.'
                )
                WHEN '{UsIxReleaseDistrictSource.SENTENCING_COUNTY.value}' THEN CONCAT(
                    'No CRC Request note this incarceration and no Idaho home address ',
                    'on file. Most recent sentence was imposed in ',
                    INITCAP(rds.{SENTENCING_COUNTY_NAME_COLUMN_NAME}),
                    ' County, which is in ',
                    rds.{SENTENCING_COUNTY_RELEASE_DISTRICT_COLUMN_NAME},
                    '.'
                )
            END AS note_body,
            rds.start_date AS event_date,
        FROM `{{project_id}}.{ANALYST_VIEWS_DATASET}.{US_IX_RELEASE_DISTRICT_SPANS_VIEW_NAME}_materialized` rds
        INNER JOIN `{{project_id}}.reference_views.product_stable_person_external_ids_materialized` pei
            ON rds.person_id = pei.person_id
            AND pei.state_code = 'US_IX'
            AND pei.system_type = 'INCARCERATION'
        WHERE {today_between_start_date_and_nullable_end_date_exclusive_clause(
                start_date_column="rds.start_date",
                end_date_column="rds.end_date_exclusive",
            )}
            AND rds.{RELEASE_DISTRICT_SOURCE_COLUMN_NAME} IN (
                '{UsIxReleaseDistrictSource.ADDRESS.value}',
                '{UsIxReleaseDistrictSource.SENTENCING_COUNTY.value}'
            )"""


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
        "        --Release district information\n" + release_district_case_notes(),
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
        + dor_case_notes_query(in_the_past_x_months=6, indent_level=8),
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
