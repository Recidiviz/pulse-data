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
        peid.external_id,
        "Positive Behavior Reports (in the past year)" AS criteria,
        facility AS note_title, 
        incident_details AS note_body,
        sic.incident_date AS event_date,
    FROM `{project_id}.{normalized_state_dataset}.state_incarceration_incident` sic
    INNER JOIN `{project_id}.{normalized_state_dataset}.state_person_external_id` peid
        USING (person_id)
    WHERE sic.state_code= 'US_ND'
        AND sic.incident_type = 'POSITIVE'
        AND sic.incident_date > DATE_SUB(CURRENT_DATE, INTERVAL 1 YEAR)
        AND peid.id_type = 'US_ND_ELITE'"""


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
        peid.external_id,
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
    FROM `{{project_id}}.{{normalized_state_dataset}}.state_program_assignment` spa
    INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` peid
        USING (person_id)
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
    WHERE spa.state_code = 'US_ND'
        AND spa.program_id IS NOT NULL
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
        AND peid.id_type = 'US_ND_ELITE'
    """

    if additional_where_clause:
        base_query += f" AND {additional_where_clause}"

    base_query += """
    GROUP BY 1,2,3,4,5
    HAVING note_body IS NOT NULL"""

    return base_query


def get_infractions_query(
    date_interval: int, date_part: str, additional_columns: str = ""
) -> str:
    """
    Returns a SQL query that retrieves infractions for North Dakota.
    """
    # TODO(#33675): Update to use ingested data

    return f"""
    SELECT 
        peid.state_code,
        peid.person_id,
        SAFE_CAST(LEFT(e.incident_date, 10) AS DATE) AS start_date,
        DATE_ADD(SAFE_CAST(LEFT(e.incident_date, 10) AS DATE), INTERVAL {date_interval} {date_part}) AS end_date,
        FALSE AS meets_criteria,
        RESULT_OIC_OFFENCE_CATEGORY AS infraction_category,
        SAFE_CAST(LEFT(e.incident_date, 10) AS DATE) AS start_date_infraction,
        {additional_columns}
    FROM `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.elite_offense_in_custody_and_pos_report_data_latest` e
    INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` peid
        ON peid.external_id = e.ROOT_OFFENDER_ID 
        AND peid.id_type = 'US_ND_ELITE'
    WHERE FINDING_DESCRIPTION = 'GUILTY'
        AND RESULT_OIC_OFFENCE_CATEGORY IN ('LVL2', 'LVL3', 'LVL2E', 'LVL3R')"""


def get_infractions_as_case_notes() -> str:
    """
    Returns a SQL query that retrieves infractions in the past 6 months as side panel.
    """

    return f"""
    WITH infractions AS (
        {get_infractions_query(
            date_interval = 6,
            date_part = 'MONTH',
            additional_columns = "e.INCIDENT_DETAILS, e.OIC_HEARING_TYPE_DESC, e.FINDING_DESCRIPTION, e.INCIDENT_TYPE_DESC, peid.external_id")}
    )
    SELECT 
        external_id,
        "Infractions (in the past 6 months)" AS criteria,
        CONCAT(OIC_HEARING_TYPE_DESC, ' - ', INCIDENT_TYPE_DESC) AS note_title,
        CONCAT(FINDING_DESCRIPTION, ' - ', INCIDENT_DETAILS) AS note_body,
        start_date_infraction AS event_date
    FROM infractions i
    WHERE {today_between_start_date_and_nullable_end_date_exclusive_clause(
            start_date_column="i.start_date",
            end_date_column="i.end_date")}"""


def get_minimum_housing_referals_query() -> str:
    """
    Returns a SQL query that retrieves minimum housing referrals data.
    """
    return """
WITH min_referrals AS (
    SELECT 
        SAFE_CAST(REGEXP_REPLACE(OFFENDER_BOOK_ID, r',|.00', '') AS STRING) AS external_id,
        SAFE_CAST(REGEXP_REPLACE(ASSESSMENT_TYPE_ID, r',', '')  AS NUMERIC) AS assess_type,
        rea.DESCRIPTION AS assess_type_desc,
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
    FROM `{project_id}.{raw_data_up_to_date_views_dataset}.recidiviz_elite_OffenderAssessments_latest` reoa
    LEFT JOIN `{project_id}.{raw_data_up_to_date_views_dataset}.recidiviz_elite_Assessments_latest` rea
    ON reoa.ASSESSMENT_TYPE_ID = rea.ASSESSMENT_ID
    -- We only care about MINIMUM HOUSING REQUESTS
    WHERE rea.DESCRIPTION = 'MINIMUM HOUSING REQUEST'
),

min_referrals_with_external_id AS (
    # We join the referrals with the external id
    SELECT 
        peid.person_id,
        peid.state_code,
        mr.assess_type,
        mr.assess_type_desc,
        mr.evaluation_result,
        mr.assess_comment_text,
        mr.committee_comment_text,
        mr.evaluation_date AS start_date,
        mr.next_review_date,
    FROM min_referrals mr
    INNER JOIN `{project_id}.{normalized_state_dataset}.state_person_external_id` peid
        ON mr.external_id = peid.external_id
            AND peid.state_code = 'US_ND'
            AND peid.id_type = 'US_ND_ELITE_BOOKING'
),

completion_events AS (
    -- Completion events for both ATP and transfer to min
    SELECT *
    FROM `{project_id}.{general_task_eligibility_completion_events_dataset}.granted_work_release_materialized`

    UNION ALL

    SELECT *
    FROM `{project_id}.{us_nd_task_eligibility_completion_events_dataset}.transfer_to_minimum_facility_materialized`
),

min_referrals_with_external_id_and_ce AS (
    # We join the referrals with the closest completion event date for those who were approved
    SELECT 
        mr.person_id,
        mr.state_code,
        mr.assess_type,
        mr.assess_type_desc,
        mr.evaluation_result,
        mr.assess_comment_text,
        mr.committee_comment_text,
        mr.start_date,
        mr.next_review_date,
        -- We use the next_review_date as the end date if it exists
        IFNULL(mr.next_review_date,
            CASE
                -- If not, in cases where the evaluation was approved, we either use the 
                -- completion event or the evaluation date + 6 months
                WHEN mr.evaluation_result = 'Approved' THEN IFNULL(MIN(ce.completion_event_date), DATE_ADD(mr.start_date, INTERVAL 6 MONTH))
                -- For people who's evaluation was not approved, end_date = start_date + 6 months 
                -- after the evaluation/start date
                ELSE DATE_ADD(mr.start_date, INTERVAL 6 MONTH)
            END) AS end_date
    FROM min_referrals_with_external_id mr
    LEFT JOIN completion_events ce
        ON mr.person_id = ce.person_id
            AND mr.start_date <= ce.completion_event_date
            AND mr.evaluation_result = 'Approved'
            AND mr.state_code = ce.state_code
    GROUP BY 1,2,3,4,5,6,7,8,9
)"""


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
        peid2.external_id,
        "{criteria}" AS criteria,
        ocn.CASE_NOTE_TYPE AS note_title,
        ocn.CASE_NOTE_TEXT AS note_body,
        EXTRACT(DATE FROM CAST(CONTACT_TIME AS DATETIME)) AS event_date,
    FROM `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.elite_OffenderCaseNotes_latest` ocn
    INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` peid
        ON SAFE_CAST(REGEXP_REPLACE(ocn.OFFENDER_BOOK_ID, r',|.00', '') AS STRING) = peid.external_id
            AND peid.state_code = 'US_ND'
            AND peid.id_type = 'US_ND_ELITE_BOOKING'
    INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` peid2
        ON peid.person_id = peid2.person_id
            AND peid.state_code = peid2.state_code
            AND peid2.id_type = 'US_ND_ELITE'
    -- Only include case notes from the current incarceration span
    INNER JOIN `{{project_id}}.{{sessions_dataset}}.incarceration_super_sessions_materialized` iss
        ON iss.state_code='US_ND'
        AND peid.person_id = iss.person_id
        AND {today_between_start_date_and_nullable_end_date_exclusive_clause(
            start_date_column="iss.start_date",
            end_date_column="iss.end_date"
        )}
    {additional_where_clause}
    """


def get_ids_as_case_notes() -> str:
    """
    Returns a SQL query that retrieves ID's available as side panel notes.
    """
    return f"""
SELECT 
    peid2.external_id,
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
FROM `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.elite_offender_chk_list_details_latest` chk
INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` peid
    ON peid.external_id = chk.OFFENDER_BOOK_ID
        AND peid.id_type ='US_ND_ELITE_BOOKING'
        AND peid.state_code = 'US_ND'
INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` peid2
    USING(person_id, state_code)
INNER JOIN `{{project_id}}.{{sessions_dataset}}.incarceration_super_sessions_materialized` iss
    ON iss.state_code='US_ND'
    AND peid.person_id = iss.person_id
    AND {today_between_start_date_and_nullable_end_date_exclusive_clause(
        start_date_column="iss.start_date",
        end_date_column="iss.end_date"
    )}
WHERE chk.CHECK_LIST_CATEGORY IN ("ID'S")
    AND chk.CONFIRMED_FLAG = 'Y'
    AND peid2.id_type = 'US_ND_ELITE'
"""


def eligible_and_almost_eligible_minus_referrals() -> str:
    """
    Returns a SQL query that retrieves the eligible and almost eligible individuals
    and removes those that have had referrals to minimum housing.
    """
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
    FROM eligible_and_almost_eligible eae
    LEFT JOIN `{{project_id}}.{{task_eligibility_criteria_dataset}}.no_recent_referrals_to_minimum_housing_materialized` nrr
        ON eae.person_id = nrr.person_id
            AND eae.state_code = nrr.state_code
            AND CURRENT_DATE("US/Pacific") BETWEEN nrr.start_date AND {nonnull_end_date_exclusive_clause('nrr.end_date')}
    WHERE IFNULL(nrr.meets_criteria, True)"""


def get_infractions_criteria_builder(
    date_interval: int, date_part: str, criteria_name: str, description: str
) -> StateSpecificTaskCriteriaBigQueryViewBuilder:
    criteria_query = f"""WITH infractions AS (
    {get_infractions_query(date_interval = date_interval, 
                           date_part = date_part,)}
                           ),
    {create_sub_sessions_with_attributes(table_name='infractions')}
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date,
        meets_criteria,
        TO_JSON(STRUCT(
            STRING_AGG(DISTINCT infraction_category, ', ' ORDER BY infraction_category) AS infraction_categories,
            MAX(start_date_infraction) AS most_recent_infraction_date
        )) AS reason,
        STRING_AGG(DISTINCT infraction_category, ', ' ORDER BY infraction_category) AS infraction_categories,
        MAX(start_date_infraction) AS most_recent_infraction_date,
    FROM sub_sessions_with_attributes
    GROUP BY 1,2,3,4,5"""

    return StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=criteria_name,
        description=description,
        state_code=StateCode.US_ND,
        criteria_spans_query_template=criteria_query,
        raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
            state_code=StateCode.US_ND, instance=DirectIngestInstance.PRIMARY
        ),
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
        meets_criteria_default=True,
        reasons_fields=[
            ReasonsField(
                name="infraction_categories",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="Categories of the infractions that led to the level 2 or 3 infraction.",
            ),
            ReasonsField(
                name="most_recent_infraction_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Date of the most recent level 2 or 3 infraction.",
            ),
        ],
    )
