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
from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause
from recidiviz.calculator.query.state.dataset_config import (
    NORMALIZED_STATE_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_tables_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
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


def parole_review_date_criteria_builder(
    criteria_name: str,
    description: str,
    date_part: str = "YEAR",
    date_interval: int = 1,
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

    _QUERY_TEMPLATE = f"""
        WITH medical_screening AS (
        SELECT 
            peid.state_code,
            peid.person_id,
            SAFE_CAST(SAFE.PARSE_DATETIME('%m/%d/%Y  %H:%M:%S%p', ms.MEDICAL_DATE) AS DATE) AS parole_review_date,
        FROM `{{project_id}}.{{raw_data_dataset}}.elite_offender_medical_screenings_6i` ms
        LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` peid
            ON peid.external_id = REPLACE(REPLACE(ms.OFFENDER_BOOK_ID,',',''), '.00', '')
            AND peid.id_type = 'US_ND_ELITE_BOOKING'
            AND peid.state_code = 'US_ND'
            AND ms.MEDICAL_QUESTIONAIRE_CODE = 'PAR'
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
            critical_date_has_passed AS meets_criteria,
            TO_JSON(STRUCT(parole_review_date AS parole_review_date)) AS reason,
            parole_review_date,
        FROM critical_date_has_passed_spans
        WHERE start_date != end_date
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
        meets_criteria_default=False,
        reasons_fields=[
            ReasonsField(
                name="parole_review_date",
                type=bigquery.enums.SqlTypeNames.DATE,
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
    Returns a SQL query that retrieves positive behavior reports as case notes.
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
        AND sic.incident_date > DATE_SUB(CURRENT_DATE, INTERVAL 1 YEAR)"""


def get_program_assignments_as_case_notes() -> str:
    """
    Returns a SQL query that retrieves program assignments as case notes.
    """

    return """
    SELECT 
        peid.external_id,
        "Assignments" AS criteria,
        spa.participation_status AS note_title,
        CONCAT(
            spa.program_location_id,
            " - Service: ",
            SPLIT(spa.program_id, '@@')[SAFE_OFFSET(0)],
            " - Activity Description: ",
            SPLIT(spa.program_id, '@@')[SAFE_OFFSET(1)]
        ) AS note_body,
        COALESCE(spa.discharge_date, spa.start_date, spa.referral_date) AS event_date,
    FROM `{project_id}.{normalized_state_dataset}.state_program_assignment` spa
    INNER JOIN `{project_id}.{normalized_state_dataset}.state_person_external_id` peid
        USING (person_id)
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
    GROUP BY 1,2,3,4,5
    HAVING note_body IS NOT NULL
    """
