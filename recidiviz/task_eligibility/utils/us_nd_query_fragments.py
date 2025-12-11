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
"""
Helper SQL queries for North Dakota
"""
from typing import Optional

from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import (
    nonnull_end_date_clause,
    nonnull_end_date_exclusive_clause,
    today_between_start_date_and_nullable_end_date_exclusive_clause,
)
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import (
    raw_latest_views_dataset_for_region,
    raw_tables_dataset_for_region,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
)

MINIMUM_SECURITY_FACILITIES = [
    "JRCC",  # James River Correctional Center,
    "MRCC",  # Missouri River Correctional Center
    "HRCC",  # Heart River Correctional Center
    "DWCRC",  # Dakota Women's Correctional and Rehabilitation Center
]

ATP_FACILITIES = [
    "FTPFAR",  # Centre Fargo - Female (ATP)
    "MTPFAR",  # Centre Fargo - Male (ATP)
    "GFC",  # Centre Grand Forks (ATP)
    "FTPMND",  # Centre Mandan - Female (ATP)
    "MTPMND",  # Centre Mandan - Male (ATP)
    "BTC",  # Bismarck Transition Center (ATP)
    "SWMCCC",  # SW Multi-County Correctional Center - Work Release (ATP)
    "WCJWRP",  # Ward County Jail - Work Release Program (ATP)
]

# Where clause needed to identify folks who are on minimum security facilities or units
MINIMUM_SECURITY_FACILITIES_WHERE_CLAUSE = f"""
    AND facility IN {tuple(MINIMUM_SECURITY_FACILITIES)}
    -- Only folks on JRMU in JRCC are minimum security
    AND (facility != 'JRCC' OR REGEXP_CONTAINS(housing_unit, r'JRMU'))
    -- Only folks on Haven in DWCRC are minimum security
    AND (facility != 'DWCRC' OR REGEXP_CONTAINS(housing_unit, r'HVN'))"""


def parole_review_dates_query() -> str:
    """
    Returns a SQL query that retrieves the parole review dates for North Dakota.
    """
    # TODO(#31967): ingest parole review dates
    return """
        SELECT 
            peid.state_code,
            peid.person_id,
            IFNULL(
                SAFE_CAST(LEFT(ms.MEDICAL_DATE, 10) AS DATE),
                SAFE_CAST(SAFE.PARSE_DATETIME('%m/%d/%Y  %H:%M:%S%p', ms.MEDICAL_DATE) AS DATE)
            ) AS parole_review_date,
        FROM `{project_id}.{raw_data_dataset}.elite_offender_medical_screenings_6i` ms
        LEFT JOIN `{project_id}.{normalized_state_dataset}.state_person_external_id` peid
            ON peid.external_id = REPLACE(REPLACE(ms.OFFENDER_BOOK_ID,',',''), '.00', '')
            AND peid.id_type = 'US_ND_ELITE_BOOKING'
            AND peid.state_code = 'US_ND'
            AND ms.MEDICAL_QUESTIONAIRE_CODE = 'PAR'
"""


def incarceration_within_parole_review_date_criteria_builder(
    criteria_name: str,
    description: str,
    date_part: str = "YEAR",
    date_interval: int = 1,
    negate_criteria: bool = False,
) -> StateSpecificTaskCriteriaBigQueryViewBuilder:
    """
    Returns a view builder for North Dakota that checks if someone is within certain
    time of the parole_review_date.

    Args:
        criteria_name: The name of the criteria
        description: The description of the criteria
        date_part: The date part to use for the interval
        date_interval: The interval to use for the date part

    Returns:
        StateSpecificTaskCriteriaBigQueryViewBuilder
    """
    meets_criteria_default = False
    if negate_criteria:
        meets_criteria_default = True

    _QUERY_TEMPLATE = f"""
        WITH medical_screening AS (
            {parole_review_dates_query()}
        ),

        critical_date_spans AS (
            SELECT 
                iss.state_code,
                iss.person_id,
                iss.start_date AS start_datetime,
                iss.end_date AS end_datetime,
                DATE_SUB(MAX(ms.parole_review_date), INTERVAL {date_interval} {date_part}) AS critical_date,
                MAX(ms.parole_review_date) AS parole_review_date
            FROM `{{project_id}}.{{sessions_dataset}}.incarceration_super_sessions_materialized` iss
            LEFT JOIN medical_screening ms
                ON iss.state_code = ms.state_code
                AND iss.person_id = ms.person_id
                AND ms.parole_review_date BETWEEN iss.start_date AND {nonnull_end_date_exclusive_clause('iss.end_date')}
            WHERE iss.state_code = 'US_ND'
            GROUP BY 1,2,3,4
        ),
        {critical_date_has_passed_spans_cte(attributes=['parole_review_date'])}

        SELECT 
            state_code,
            person_id,
            start_date,
            end_date,
            {'NOT' if negate_criteria else ''} critical_date_has_passed AS meets_criteria,
            TO_JSON(STRUCT(parole_review_date AS parole_review_date)) AS reason,
            parole_review_date,
        FROM critical_date_has_passed_spans
        WHERE start_date != {nonnull_end_date_clause('end_date')}
    """
    return StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=criteria_name,
        description=description,
        state_code=StateCode.US_ND,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        raw_data_dataset=raw_tables_dataset_for_region(
            state_code=StateCode.US_ND, instance=DirectIngestInstance.PRIMARY
        ),
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
        sessions_dataset=SESSIONS_DATASET,
        meets_criteria_default=meets_criteria_default,
        reasons_fields=[
            ReasonsField(
                name="parole_review_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Parole Review Date: The date of the next parole review is scheduled",
            ),
        ],
    )


def reformat_ids(column_name: str) -> str:
    """
    Generates a SQL expression to reformat the IDs in a specified column.

    This function constructs a SQL expression that removes commas and the
    ".00" suffix from the IDs in the given column name. It is useful for
    cleaning up numeric ID representations stored as strings in databases.

    Args:
        column_name (str): The name of the column containing the IDs to be reformatted.

    Returns:
        str: A SQL expression that replaces commas and ".00" in the specified column.
    """

    return f"""REPLACE(REPLACE({column_name},',',''), '.00', '')"""


def get_positive_behavior_reports_as_case_notes() -> str:
    """
    Returns a SQL query that retrieves positive behavior reports as side panel notes.
    """
    return """
    SELECT 
        person_id,
        "Positive Behavior Reports (in the past year)" AS criteria,
        facility AS note_title, 
        incident_details AS note_body,
        sic.incident_date AS event_date,
    FROM `{project_id}.us_nd_normalized_state.state_incarceration_incident` sic
    WHERE sic.incident_type = 'POSITIVE'
        AND sic.incident_date > DATE_SUB(CURRENT_DATE, INTERVAL 1 YEAR)
    """


TRAINING_PROGRAMMING_NOTE_TEXT_REGEX_FOR_CRITERIA = "|".join(
    [
        "CONFLICT RESOLUTION",
        "NPHR",
        "SEX OFFENDER TREATMENT PROGRAM",
        "CBISA",
    ]
)

TRAINING_PROGRAMMING_NOTE_TEXT_REGEX = "|".join(
    [
        "THINKING FOR CHANGE",
        "THINKING FOR A CHANGE",
        "RESTORING PROMISE",
        "PEER SUPPORT",
        "BEYOND TRAUMA",
        "ADDICTION AFTERCARE",
        "TED TALKS",
        "TRAINING",
        "PROGRAM",
        "CLASS",
        "COURSE",
        "EDUCATION",
        "SKILLS",
        "CAREER",
        "CONFLICT RESOLUTION",
        "NPHR",
        "SEX OFFENDER TREATMENT PROGRAM",
        "CBISA",
    ]
)

WORK_NOTE_TEXT_REGEX = "|".join(
    [
        "FOOD SERVICES",
        "SHOP",
        "HELPER",
        "MAIN KITCHEN",
        "WORKER",
        "JANITOR",
        "ASST",
        "JOB",
        "EMPLOYMENT",
        "WORK",
    ]
)

HEALTH_NOTE_TEXT_REGEX = "|".join(
    [
        "MEDICAL",
        "HEALTH",
        "PSYCH",
        "MEDICATED",
        "MEDICATION",
    ]
)


def get_program_assignments_as_case_notes(
    criteria: str = "Assignments", additional_where_clause: Optional[str] = None
) -> str:
    """
    Returns a SQL query that retrieves program assignments from the current
    incarceration as side panel notes.
    """

    base_query = f"""
    SELECT 
        spa.person_id,
        '{criteria}' AS criteria,
        CASE 
            WHEN spa.participation_status = 'IN_PROGRESS' THEN 'In Progress'
            WHEN spa.participation_status = 'PENDING' THEN 'Pending'
            WHEN spa.participation_status IN ('DISCHARGED', 
                                            'DISCHARGED_SUCCESSFUL', 
                                            'DISCHARGED_UNSUCCESSFUL', 
                                            'DISCHARGED_SUCCESSFUL_WITH_DISCRETION', 
                                            'DISCHARGED_OTHER', 
                                            'DISCHARGED_UNKNOWN') THEN 'Discharged'
            WHEN spa.participation_status = 'DECEASED' THEN 'Deceased'
            ELSE 'Unknown Status'
        END AS note_title,
        CONCAT(
            spa.program_location_id,
            " - ",
            SPLIT(spa.program_id, '@@')[SAFE_OFFSET(0)],
            " - Description: ",
            SPLIT(spa.program_id, '@@')[SAFE_OFFSET(1)]
        ) AS note_body,
        COALESCE(spa.discharge_date, spa.start_date, spa.referral_date) AS event_date
    FROM `{{project_id}}.us_nd_normalized_state.state_program_assignment` spa
    -- Only include program assignments from the current incarceration span
    INNER JOIN `{{project_id}}.{{sessions_dataset}}.incarceration_super_sessions_materialized` iss
        ON spa.state_code = iss.state_code
        AND spa.person_id = iss.person_id
        AND spa.start_date BETWEEN iss.start_date AND {nonnull_end_date_exclusive_clause('iss.end_date')}
        AND COALESCE(spa.discharge_date, spa.start_date, spa.referral_date) BETWEEN iss.start_date AND IFNULL(iss.end_date, '9999-12-31')
        AND {today_between_start_date_and_nullable_end_date_exclusive_clause(
            start_date_column="iss.start_date",
            end_date_column="iss.end_date"
        )}
    WHERE spa.program_id IS NOT NULL
        AND spa.participation_status IN ('IN_PROGRESS', 
                                            'PENDING', 
                                            'DISCHARGED', 
                                            'DISCHARGED_SUCCESSFUL',
                                            'DISCHARGED_UNSUCCESSFUL',
                                            'DISCHARGED_SUCCESSFUL_WITH_DISCRETION',
                                            'DISCHARGED_OTHER',
                                            'DISCHARGED_UNKNOWN',
                                            'DECEASED')
        -- Don't surface case manager assignments
        AND NOT REGEXP_CONTAINS(spa.program_id, r'ASSIGNED CASE MANAGER')
    """

    if additional_where_clause:
        base_query += f" AND {additional_where_clause}"

    base_query += """
    GROUP BY 1,2,3,4,5
    HAVING note_body IS NOT NULL"""

    return base_query


def get_infractions_as_case_notes() -> str:
    """
    Returns a SQL query that retrieves infractions in the past 6 months as side panel.
    """
    # TODO(#41410): Update to use ingested data

    return f"""
    WITH infractions AS (
    SELECT
        peid.person_id,
        "Infractions (in the past 6 months)" AS criteria,
        SAFE_CAST(LEFT(e.incident_date, 10) AS DATE) AS start_date,
        DATE_ADD(SAFE_CAST(LEFT(e.incident_date, 10) AS DATE), INTERVAL 6 MONTH) AS end_date,
        CONCAT(e.OIC_HEARING_TYPE_DESC, ' - ', e.INCIDENT_TYPE_DESC) AS note_title,
        CONCAT(e.FINDING_DESCRIPTION, ' - ', e.INCIDENT_DETAILS) AS note_body,
        SAFE_CAST(LEFT(e.incident_date, 10) AS DATE) AS event_date,
    FROM `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.elite_offense_in_custody_and_pos_report_data_latest` e
    INNER JOIN `{{project_id}}.us_nd_normalized_state.state_person_external_id` peid
        ON peid.external_id = e.ROOT_OFFENDER_ID
        AND peid.id_type = 'US_ND_ELITE'
    WHERE FINDING_DESCRIPTION = 'GUILTY'
        AND RESULT_OIC_OFFENCE_CATEGORY IN ('LVL2', 'LVL3', 'LVL2E', 'LVL3R')
    )
    SELECT * EXCEPT(start_date, end_date)
    FROM infractions
    WHERE {today_between_start_date_and_nullable_end_date_exclusive_clause(
            start_date_column="start_date",
            end_date_column="end_date")}"""


MINIMUM_HOUSING_REFERRAL_QUERY = f"""
    SELECT DISTINCT
    'US_ND' AS state_code,
    peid.person_id,
    SAFE_CAST(REGEXP_REPLACE(assess3.parent_assessment_id, r',', '')  AS NUMERIC) AS assess_type,
    assess4.description as assess_type_desc,
    CASE 
        WHEN EVALUATION_RESULT_CODE = 'APP' THEN 'Approved'
        WHEN EVALUATION_RESULT_CODE = 'RESCIND' THEN 'Rescinded'
        WHEN EVALUATION_RESULT_CODE = 'REFER' THEN 'Referred'
        WHEN EVALUATION_RESULT_CODE = 'NOTAPP' THEN 'Not Approved'
        WHEN EVALUATION_RESULT_CODE = 'NOREF' THEN 'Not Referred'
        WHEN EVALUATION_RESULT_CODE = 'DEFERRED' THEN 'Deferred'
        ELSE 'Unknown'
    END AS evaluation_result,
    reoa.ASSESS_COMMENT_TEXT AS assess_comment_text,
    reoa.COMMITTE_COMMENT_TEXT AS committee_comment_text,
    SAFE_CAST(LEFT(COALESCE(reoa.EVALUATION_DATE, reoa.ASSESSMENT_DATE), 10) AS DATE) AS evaluation_date,
    SAFE_CAST(LEFT(reoa.NEXT_REVIEW_DATE, 10) AS DATE) AS next_review_date,
    -- When there is a final reviewed facility, use that. When there is not, use the facility
    -- included in the initial request.
    COALESCE(reoa.REVIEW_PLACE_AGY_LOC_ID, assess1.ASSESSMENT_CODE) AS referral_facility,
FROM `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.elite_OffenderAssessmentItems_latest` items
-- The elite_OffenderAssessmentItems table includes an entry with a description that is only the referral facility. 
-- To deduce that the facility listed in that description is the target of a minimum housing request, we have to trace
-- back through PARENT_ASSESSMENT_ID values to the ultimate "MINIMUM HOUSING REQUEST" parent assessment.
-- This requires stepping back through 4 parent assessments with the following descriptions, to be absolutely sure
-- this is the type of minimum housing request we want to track: 
-- "[Facility Name]" -> "Facility Referral" -> "Minimum Housing Request" -> "Minimum Housing Request". 
JOIN (
    SELECT * FROM `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.recidiviz_elite_Assessments_latest` 
    -- These are the only facilities that can be referred. This filters out other items
    -- related to this assessment and keeps only the referral facility, where there is one.
    WHERE ASSESSMENT_CODE IN ('MTP', 'MRCC', 'JRMU')) assess1
    ON(items.assessment_id = assess1.assessment_id)
JOIN (
    SELECT * FROM `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.recidiviz_elite_Assessments_latest` 
    WHERE description = 'Facility Referral') assess2
    ON(assess1.parent_assessment_id = assess2.assessment_id )
JOIN (
    SELECT * FROM  `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.recidiviz_elite_Assessments_latest` 
    WHERE description = '{'MINIMUM HOUSING REQUEST'}') assess3
    ON(assess2.parent_assessment_id = assess3.assessment_id)
JOIN (
    SELECT * FROM `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.recidiviz_elite_Assessments_latest` 
    WHERE description = '{'MINIMUM HOUSING REQUEST'}') assess4
    ON(assess3.parent_assessment_id = assess4.assessment_id)
JOIN `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.recidiviz_elite_OffenderAssessments_latest` reoa
    ON(reoa.ASSESSMENT_TYPE_ID = assess4.ASSESSMENT_ID AND items.OFFENDER_BOOK_ID = reoa.OFFENDER_BOOK_ID)
JOIN `{{project_id}}.us_nd_normalized_state.state_person_external_id` peid
ON {reformat_ids('reoa.OFFENDER_BOOK_ID')} = peid.external_id
    AND peid.id_type = 'US_ND_ELITE_BOOKING'
"""


def get_minimum_housing_referals_query(
    join_with_completion_events: bool = True,
    keep_latest_referral_only: bool = False,
    evaluation_result: str = "Approved",
) -> str:
    """
    Returns a SQL query that retrieves minimum housing referrals data.

    Args:
        join_with_completion_events (bool): Whether to join with completion events or not.
        keep_latest_referral_only (bool): Whether to keep only the latest referral information.
    """

    _COMPLETION_EVENTS_JOIN_QUERY = f""",
        completion_events AS (
            -- Completion events for both ATP and transfer to min
            SELECT *
            FROM `{{project_id}}.{{general_task_eligibility_completion_events_dataset}}.granted_work_release_materialized`

            UNION ALL

            SELECT *
            FROM `{{project_id}}.{{us_nd_task_eligibility_completion_events_dataset}}.transfer_to_minimum_facility_materialized`
        ),

        min_referrals_filtered_with_ce AS (
            # We join the referrals with the closest completion event date for those who were approved
            SELECT 
                mr.person_id,
                mr.state_code,
                mr.assess_type,
                mr.assess_type_desc,
                mr.evaluation_result,
                mr.assess_comment_text,
                mr.committee_comment_text,
                mr.evaluation_date,
                mr.start_date,
                mr.next_review_date,
                -- We use the next_review_date as the end date if it exists
                IFNULL(mr.next_review_date,
                    CASE
                        -- If not, in cases where the evaluation was approved, we either use the 
                        -- completion event or the evaluation date + 6 months
                        WHEN mr.evaluation_result = '{evaluation_result}' THEN IFNULL(MIN(ce.completion_event_date), DATE_ADD(mr.start_date, INTERVAL 6 MONTH))
                        -- For people who's evaluation was not approved, end_date = start_date + 6 months 
                        -- after the evaluation/start date
                        ELSE DATE_ADD(mr.start_date, INTERVAL 6 MONTH)
                    END) AS end_date
            FROM min_referrals_filtered mr
            LEFT JOIN completion_events ce
                ON mr.person_id = ce.person_id
                    AND mr.start_date <= ce.completion_event_date
                    AND mr.evaluation_result = '{evaluation_result}'
                    AND mr.state_code = ce.state_code
            GROUP BY 1,2,3,4,5,6,7,8,9,10
        )"""

    return f"""WITH min_referrals AS (
    {MINIMUM_HOUSING_REFERRAL_QUERY}
),

min_referrals_filtered AS (
    SELECT 
        mr.person_id,
        mr.state_code,
        mr.assess_type,
        mr.assess_type_desc,
        mr.evaluation_result,
        mr.assess_comment_text,
        mr.committee_comment_text,
        mr.evaluation_date AS start_date,
        mr.evaluation_date,
        mr.next_review_date,
        mr.referral_facility,
    FROM min_referrals mr
    {"QUALIFY ROW_NUMBER() OVER(PARTITION BY state_code, person_id ORDER BY start_date DESC) = 1" if keep_latest_referral_only else ''}
)
{_COMPLETION_EVENTS_JOIN_QUERY if join_with_completion_events else ''}"""


def get_recent_denials_query() -> str:
    """
    Returns a SQL query that retrieves minimum housing referrals data.
    """
    return """
    WITH recent_denials AS (
      SELECT 
          sn.state_code,
          sn.person_id,
          SAFE_CAST(sn.start_date AS DATE) AS marked_ineligible_date,
          SAFE_CAST(sn.end_date_scheduled AS DATE) AS snoozed_until_date,
          sn.snoozed_by AS denied_by_email,
          STRING_AGG(denial_reason, ' - ' ORDER BY denial_reason) AS denied_reasons,
      FROM `{project_id}.workflows_views.clients_snooze_spans_materialized` sn,
      UNNEST(denial_reasons) AS denial_reason
      WHERE sn.state_code = 'US_ND'
          AND sn.opportunity_type = 'usNdTransferToMinFacility'
          AND CURRENT_DATE('US/Eastern') BETWEEN SAFE_CAST(sn.start_date AS DATE) AND SAFE_CAST(sn.end_date_scheduled AS DATE)
      GROUP BY 1,2,3,4,5
      QUALIFY ROW_NUMBER() OVER(PARTITION BY sn.state_code, sn.person_id ORDER BY marked_ineligible_date DESC) = 1
)"""


def get_recent_referrals_query(evaluation_result: str) -> str:
    """
    Returns a SQL query that retrieves minimum housing referrals data based on the given evaluation_result.
    Raises a ValueError if evaluation_result is not provided.
    """
    if not evaluation_result:
        raise ValueError("evaluation_result must be provided.")

    return f"""
    recent_referrals AS (
        SELECT *
        FROM min_referrals_filtered
        WHERE evaluation_result = '{evaluation_result}'
    )

    SELECT 
        rr.state_code,
        elite_nos.elite_no,
        CONCAT(
            JSON_EXTRACT_SCALAR(rr.person_name, '$.given_names'), ' ',
            JSON_EXTRACT_SCALAR(rr.person_name, '$.surname')
        ) AS person_name,
        rr.facility_id,
        rr.unit_id AS living_unit,
        rr.release_date,
        DATE_DIFF(CURRENT_DATE("US/Pacific"), sp.birthdate, YEAR) AS age_in_years,
        rr.custody_level,
        sr.officer_name,
        r_ref.start_date AS referral_date, 
        r_ref.referral_facility
    FROM `{{project_id}}.{{task_eligibility_dataset}}.transfer_to_minimum_facility_form_materialized` tes
    INNER JOIN recent_referrals r_ref
        USING(person_id, state_code)
    LEFT JOIN `{{project_id}}.workflows_views.resident_record_materialized` rr
        USING(person_id, state_code)
    LEFT JOIN (
        SELECT id AS officer_external_id, CONCAT(sr.given_names, ' ', sr.surname) AS officer_name
        FROM `{{project_id}}.workflows_views.incarceration_staff_record_materialized` sr
        WHERE state_code = 'US_ND'
    ) sr
        ON sr.officer_external_id = rr.officer_id
    LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person` sp
        USING(state_code, person_id)
    LEFT JOIN (
        SELECT person_id, external_id AS elite_no
        FROM `{{project_id}}.us_nd_normalized_state.state_person_external_id`
        WHERE id_type = 'US_ND_ELITE' AND state_code = 'US_ND'
            AND is_current_display_id_for_type
    ) elite_nos
        USING (person_id)
    WHERE CURRENT_DATE("US/Pacific") BETWEEN tes.start_date AND IFNULL(tes.end_date, '9999-12-31')
        -- Not in a minimum security facility
        AND JSON_EXTRACT_SCALAR(
                (SELECT item FROM UNNEST(JSON_EXTRACT_ARRAY(reasons, "$")) AS item
                WHERE JSON_EXTRACT_SCALAR(item, "$.criteria_name") = "US_ND_NOT_IN_MINIMUM_SECURITY_FACILITY"), 
                "$.reason.minimum_facility") IS NULL
        -- Not in work-release already
        AND JSON_EXTRACT_SCALAR(
            (SELECT item FROM UNNEST(JSON_EXTRACT_ARRAY(reasons, "$")) AS item
            WHERE JSON_EXTRACT_SCALAR(item, "$.criteria_name") = "NOT_IN_WORK_RELEASE"), 
            "$.reason.most_recent_work_release_start_date") IS NULL
        AND custody_level = 'MINIMUM'
    ORDER BY facility_id, officer_name, release_date DESC"""


_SSI_NOTE_TEXT_REGEX = "|".join(
    [
        "SSI ",
        " SSI",
        "SOCIAL SECURITY DISABILITY",
        "DISABILITY BENEFITS",
        "SOCIAL DISABILITY INSURANCE",
        "DISABILITY INSURANCE",
    ]
)
SSI_NOTE_WHERE_CLAUSE = (
    f"WHERE REGEXP_CONTAINS(UPPER(ocn.CASE_NOTE_TEXT), r'{_SSI_NOTE_TEXT_REGEX}')"
)


def get_offender_case_notes(
    criteria: str, additional_where_clause: Optional[str] = None
) -> str:
    """
    Returns a SQL query that retrieves case notes from OffenderCaseNotes and formats
    them as side panel notes
    """

    return f"""
    SELECT 
        person_id,
        "{criteria}" AS criteria,
        ocn.CASE_NOTE_TYPE AS note_title,
        ocn.CASE_NOTE_TEXT AS note_body,
        EXTRACT(DATE FROM CAST(CONTACT_TIME AS DATETIME)) AS event_date,
    FROM (
        SELECT person_id, CASE_NOTE_TYPE, CASE_NOTE_TEXT, CONTACT_TIME
        FROM `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.elite_OffenderCaseNotes_latest` ocn
        INNER JOIN `{{project_id}}.us_nd_normalized_state.state_person_external_id` peid
            ON SAFE_CAST(REGEXP_REPLACE(ocn.OFFENDER_BOOK_ID, r',|.00', '') AS STRING) = peid.external_id
                AND peid.state_code = 'US_ND'
                AND peid.id_type = 'US_ND_ELITE_BOOKING'
    ) ocn
    -- Only include case notes from the current incarceration span
    INNER JOIN (
        SELECT person_id, start_date, end_date
        FROM `{{project_id}}.{{sessions_dataset}}.incarceration_super_sessions_materialized` iss
        WHERE {today_between_start_date_and_nullable_end_date_exclusive_clause(
            start_date_column="iss.start_date",
            end_date_column="iss.end_date"
        )} AND iss.state_code='US_ND'
    ) iss
    USING (person_id)
    {additional_where_clause}
    """


def get_ids_as_case_notes() -> str:
    """
    Returns a SQL query that retrieves ID's available as side panel notes.
    """
    return f"""
SELECT 
    person_id,
    "Available IDs" AS criteria,
    CASE 
        WHEN CHECK_LIST_CODE = 'BC' THEN 'Birth Certificate'
        WHEN CHECK_LIST_CODE = 'BIA' THEN 'Bureau of Indian Affairs ID'
        WHEN CHECK_LIST_CODE = 'SG' THEN 'State Government Photo ID'
        WHEN CHECK_LIST_CODE = 'PASS' THEN 'Passport or VISA'
        WHEN CHECK_LIST_CODE = 'MIL' THEN 'Military ID'
        WHEN CHECK_LIST_CODE = 'SSN' THEN 'Social Security Number'
        WHEN CHECK_LIST_CODE = 'VEH' THEN 'Vehicle Title'
        WHEN CHECK_LIST_CODE = 'NAT' THEN 'Naturalization Paper'
        WHEN CHECK_LIST_CODE = 'SS' THEN 'Selective Service Card'
        WHEN CHECK_LIST_CODE = 'PHOTOC' THEN 'Photocopy of ID'
        WHEN CHECK_LIST_CODE = 'DEBIT' THEN 'Debit Card'
        ELSE 'Unknown ID'
    END AS note_title,
    'Confirmed' AS note_body,
    SAFE_CAST(LEFT(MODIFY_DATETIME, 10) AS DATE) AS event_date,
FROM (
    SELECT person_id, CHECK_LIST_CODE, CHECK_LIST_CATEGORY, CONFIRMED_FLAG, MODIFY_DATETIME
    FROM `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.elite_offender_chk_list_details_latest` chk
    INNER JOIN `{{project_id}}.us_nd_normalized_state.state_person_external_id` peid
    ON peid.external_id = chk.OFFENDER_BOOK_ID
        AND peid.id_type ='US_ND_ELITE_BOOKING'
        AND peid.state_code = 'US_ND'
) chk
-- Filter to currently incarcerated people
INNER JOIN (
    SELECT person_id
    FROM `{{project_id}}.{{sessions_dataset}}.incarceration_super_sessions_materialized` iss
    WHERE iss.state_code='US_ND'
    AND {today_between_start_date_and_nullable_end_date_exclusive_clause(
        start_date_column="iss.start_date",
        end_date_column="iss.end_date"
    )}
)
USING (person_id)
WHERE chk.CHECK_LIST_CATEGORY IN ("ID'S")
    AND chk.CONFIRMED_FLAG = 'Y'
"""


def eligible_and_almost_eligible_minus_referrals(
    remove_recent_referrals: bool = True,
) -> str:
    """
    Returns a SQL query that retrieves the eligible and almost eligible individuals.
    Conditionally removes individuals who have had recent referrals to minimum housing
    based on the value of the `remove_recent_referrals` parameter.

    Args:
        remove_recent_referrals (bool): If True, removes individuals who have had
            recent referrals to minimum housing. If False, includes all individuals
            regardless of recent referrals.
    """
    INELIGIBLE_EVALUATION_RESULTS = ("NOT REFERRED", "NOT APPROVED", "RESCINDED")

    return f"""
    SELECT 
        eae.* EXCEPT (reasons),
        TO_JSON(ARRAY_CONCAT(
            JSON_QUERY_ARRAY(eae.reasons),
            [TO_JSON(STRUCT(
                "US_ND_NO_RECENT_REFERRALS_TO_MINIMUM_HOUSING" AS criteria_name,
                nrr.reason AS reason
            ))]
        )) AS reasons,
        CASE
            WHEN nrr.meets_criteria IS FALSE AND UPPER(JSON_EXTRACT_SCALAR(nrr.reason, '$.evaluation_result')) IN {INELIGIBLE_EVALUATION_RESULTS} THEN 'MARKED_INELIGIBLE'
            WHEN nrr.meets_criteria IS FALSE AND UPPER(JSON_EXTRACT_SCALAR(nrr.reason, '$.evaluation_result')) NOT IN {INELIGIBLE_EVALUATION_RESULTS} THEN 'REFERRAL_SUBMITTED'
            ELSE NULL
        END AS metadata_tab_name,
    FROM eligible_and_almost_eligible eae
    LEFT JOIN (
        SELECT * 
        FROM `{{project_id}}.{{task_eligibility_criteria_dataset}}.no_recent_referrals_to_minimum_housing_materialized`
        WHERE CURRENT_DATE("US/Pacific") BETWEEN start_date AND {nonnull_end_date_exclusive_clause('end_date')}
    ) nrr
    USING (person_id, state_code)
    {'WHERE IFNULL(nrr.meets_criteria, True)' if remove_recent_referrals else ''}
    """


def get_warrants_and_detainers_query(
    order_types: list,
    criteria_name: str,
    description: str,
    remove_misdemeanor_detainers: bool = False,
) -> StateSpecificTaskCriteriaBigQueryViewBuilder:
    """
    Returns a SQL query that creates a warrants and detainers criteria.

    Args:
        order_types: A list of order types to include in the query.
        criteria_name: The name of the criteria.
        description: The description of the criteria.
        remove_misdemeanor_detainers: Whether to remove misdemeanor detainers from the query.
    """
    query = f"""WITH warrants_and_detainers AS (
    SELECT 
        peid.state_code,
        peid.person_id,
        SAFE_CAST(LEFT(e.OFFENSE_DATE, 10) AS DATE) AS start_date,
        SAFE_CAST(LEFT(e.OFFENSE_DATE, 10) AS DATE) AS start_date_warrant_or_detainer,
        iss.end_date AS end_date,
        FALSE AS meets_criteria,
        e.ORDER_TYPE,
        e.OFFENSE_STATUS,
        e.OFFENSE_DESC,
    FROM `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.elite_offender_offences_latest` e
    INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` peid
    ON peid.external_id = {reformat_ids('e.OFFENDER_BOOK_ID')}
        AND peid.state_code = 'US_ND'
        AND peid.id_type = 'US_ND_ELITE_BOOKING'
    -- If there was a warrant or detainer, treat it as active through the end of the 
    -- overlapping incarceration super session, or the next super session if it doesn't overlap 
    -- with one.
    LEFT JOIN `{{project_id}}.sessions.incarceration_super_sessions_materialized` iss
    ON peid.state_code = iss.state_code
        AND peid.person_id = iss.person_id
        AND SAFE_CAST(LEFT(e.OFFENSE_DATE, 10) AS DATE) < {nonnull_end_date_exclusive_clause('iss.end_date')}
    WHERE e.ORDER_TYPE IN {tuple(order_types)}
        AND e.OFFENSE_STATUS != 'C'
        -- Removing misdemeanor detainers if specified
        {"AND e.OFFENSE_DESC NOT LIKE '%(M)%' AND e.OFFENSE_DESC NOT LIKE '%(MisA)%'" if remove_misdemeanor_detainers else ""}
    QUALIFY ROW_NUMBER() OVER(PARTITION BY peid.state_code, peid.person_id, e.OFFENSE_DATE ORDER BY {nonnull_end_date_exclusive_clause('iss.end_date')} ASC) = 1
    
),
{create_sub_sessions_with_attributes(table_name='warrants_and_detainers')}
SELECT 
    state_code,
    person_id,
    start_date,
    end_date,
    meets_criteria,
    TO_JSON(STRUCT(
        STRING_AGG(DISTINCT OFFENSE_DESC, ', ' ORDER BY OFFENSE_DESC) AS offenses_descriptions,
        STRING_AGG(DISTINCT ORDER_TYPE, ', ' ORDER BY ORDER_TYPE) AS order_types,
        MAX(start_date_warrant_or_detainer) AS most_recent_warrant_or_detainer_date
    )) AS reason,
    STRING_AGG(DISTINCT OFFENSE_DESC, ', ' ORDER BY OFFENSE_DESC) AS offenses_descriptions,
    STRING_AGG(DISTINCT ORDER_TYPE, ', ' ORDER BY ORDER_TYPE) AS order_types,
    MAX(start_date_warrant_or_detainer) AS most_recent_warrant_or_detainer_date,
FROM sub_sessions_with_attributes
GROUP BY 1,2,3,4,5
"""
    return StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=criteria_name,
        description=description,
        state_code=StateCode.US_ND,
        criteria_spans_query_template=query,
        raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
            state_code=StateCode.US_ND, instance=DirectIngestInstance.PRIMARY
        ),
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
        meets_criteria_default=True,
        reasons_fields=[
            ReasonsField(
                name="offenses_descriptions",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="Descriptions of the offenses that led to the warrants or detainers.",
            ),
            ReasonsField(
                name="order_types",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="Types of the warrants or detainers.",
            ),
            ReasonsField(
                name="most_recent_warrant_or_detainer_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Date of the most recent warrant or detainer.",
            ),
        ],
    )
