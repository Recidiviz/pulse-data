# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Helper SQL fragments that import raw tables for NC
"""
from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import revert_nonnull_end_date_clause
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.utils.general_criteria_builders import (
    create_sub_sessions_with_attributes,
)
from recidiviz.task_eligibility.utils.state_dataset_query_fragments import (
    nonnull_end_date_exclusive_clause,
)


def prs_spans_cte() -> str:
    """Returns a CTE for Post-Release Supervision (PRS) spans in NC.

    PRS is represented as COMMUNITY_CONFINEMENT within SUPERVISION in compartment sessions.
    """
    return """
    prs_spans AS (
        SELECT
            state_code,
            person_id,
            start_date,
            end_date_exclusive,
        FROM `{project_id}.sessions.compartment_sessions_materialized`
        WHERE compartment_level_1 IN ('SUPERVISION')
            AND compartment_level_2 IN ('COMMUNITY_CONFINEMENT')
            AND state_code = 'US_NC'
    )"""


def sex_offender_programs_query(
    meets_criteria_column: str,
    include_program_ids: bool = True,
    include_participation_statuses: bool = True,
    include_discharge_dates: bool = False,
) -> str:
    """Returns the full criteria query for sex offender program assignments
    overlapping with PRS spans.

    Args:
        meets_criteria_column: SQL column name applied via LOGICAL_AND to determine
            meets_criteria (e.g. "has_no_so_programs_assigned" or "all_so_programs_are_completed").
        include_program_ids: Include sex_offender_program_ids in reason output.
        include_participation_statuses: Include participation_statuses in reason output.
        include_discharge_dates: Include discharge_dates in reason output.
    """
    reason_fields = [
        f"LOGICAL_AND({meets_criteria_column}) AS {meets_criteria_column}",
    ]
    if include_program_ids:
        reason_fields.append(
            "ARRAY_AGG(program_id ORDER BY program_start_date) AS sex_offender_program_ids"
        )
    if include_participation_statuses:
        reason_fields.append(
            "ARRAY_AGG(participation_status_raw_text ORDER BY program_start_date) AS participation_statuses"
        )
    if include_discharge_dates:
        reason_fields.append(
            "ARRAY_AGG(discharge_date ORDER BY program_start_date) AS discharge_dates"
        )

    reason_fields_str = ",\n        ".join(reason_fields)
    # Outside TO_JSON, same fields without alias prefix
    outer_fields_str = ",\n    ".join(reason_fields)

    return f"""
{prs_spans_cte()},
sex_offender_programs AS (
    SELECT
        state_code,
        person_id,
        start_date AS program_start_date,
        discharge_date,
        program_id,
        participation_status_raw_text,
        participation_status_raw_text = 'ACHIEVED' AS all_so_programs_are_completed
    FROM `{{project_id}}.us_nc_normalized_state.state_program_assignment`
    WHERE state_code = 'US_NC'
        AND program_id LIKE 'SEX_OFFENDER%'
),
prs_spans_with_sex_offender_programs AS (
    SELECT
        sop.state_code,
        sop.person_id,
        IFNULL(sop.discharge_date, sop.program_start_date) AS start_date,
        GREATEST({nonnull_end_date_exclusive_clause('sop.discharge_date')}, {nonnull_end_date_exclusive_clause('prs.end_date_exclusive')}) AS end_date,
        sop.program_id,
        sop.program_start_date,
        sop.discharge_date,
        sop.participation_status_raw_text,
        sop.all_so_programs_are_completed,
        sop.program_id IS NULL AS has_no_so_programs_assigned,
    FROM sex_offender_programs sop
    -- Keep programs that started OR ended during someone's PRS period
    INNER JOIN prs_spans prs
    ON sop.person_id = prs.person_id
        AND (
            sop.program_start_date BETWEEN prs.start_date AND {nonnull_end_date_exclusive_clause('prs.end_date_exclusive')}
            OR {nonnull_end_date_exclusive_clause('sop.discharge_date')} BETWEEN prs.start_date AND {nonnull_end_date_exclusive_clause('prs.end_date_exclusive')}
        )
    -- Keep only one distinct program id within a session, prioritize the latest one
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY prs.person_id, prs.start_date, prs.end_date_exclusive, sop.program_id
        ORDER BY sop.program_start_date DESC
    ) = 1
),
{create_sub_sessions_with_attributes('prs_spans_with_sex_offender_programs')}

SELECT
    state_code,
    person_id,
    start_date,
    {revert_nonnull_end_date_clause('end_date')} AS end_date,
    LOGICAL_AND({meets_criteria_column}) AS meets_criteria,
    TO_JSON(STRUCT(
        {reason_fields_str}
    )) AS reason,
    {outer_fields_str}
FROM sub_sessions_with_attributes
WHERE start_date != {nonnull_end_date_exclusive_clause('end_date')}
GROUP BY 1,2,3,4"""


def sex_offender_programs_reasons_fields(
    meets_criteria_column: str,
    include_program_ids: bool = True,
    include_participation_statuses: bool = True,
    include_discharge_dates: bool = False,
) -> list[ReasonsField]:
    """Returns the ReasonsField list matching the columns produced by
    sex_offender_programs_query with the same flags."""
    fields: list[ReasonsField] = [
        ReasonsField(
            name=meets_criteria_column,
            type=bigquery.enums.StandardSqlTypeNames.BOOL,
            description=f"Whether the criteria ({meets_criteria_column}) is met",
        ),
    ]
    if include_program_ids:
        fields.append(
            ReasonsField(
                name="sex_offender_program_ids",
                type=bigquery.enums.StandardSqlTypeNames.ARRAY,
                description="Names of the sex offender programs the person is assigned to",
            )
        )
    if include_participation_statuses:
        fields.append(
            ReasonsField(
                name="participation_statuses",
                type=bigquery.enums.StandardSqlTypeNames.ARRAY,
                description="Participation statuses for each program ordered by program start date",
            )
        )
    if include_discharge_dates:
        fields.append(
            ReasonsField(
                name="discharge_dates",
                type=bigquery.enums.StandardSqlTypeNames.ARRAY,
                description="Discharge dates for each program ordered by program start date",
            )
        )
    return fields


FACILITY_PROGRAM_ID_VALUES = [
    # Substance abuse
    "SUBSTANCE ABUSE-COMPLETE RECOMMENDED SUBSTANCE ABUSE TREATMENT.",
    "SUBSTANCE ABUSE-PARTICIPATE AND COMPLETE BLACK MOUNTAIN.",  # residential treatment program
    "SUBSTANCE ABUSE-PARTICIPATE IN TROSA PROGRAM.",  # Triangle Residential Options for Substance Abusers
    "SUBSTANCE ABUSE-PARTICIPATE IN DART PROGRAM.",  # Drug Abuse Residential Treatment
    "SUBSTANCE ABUSE-COMPLETE DRUG EDUCATION CLASSES.",
    "SUBSTANCE ABUSE-PARTICIPATE AND COMPLETE TWO SESSIONS PER WEEK DWI.",
    # Mental health
    "MENTAL HEALTH-COMPLETE RECOMMENDED MENTAL HEALTH TREATMENT.",
    "MENTAL HEALTH-PARTICIPATE IN PSYCHOLOGICAL COUNSELING.",
    "MENTAL HEALTH-PARTICIPATE IN PSYCHIATRIC COUNSELING.",
    "MENTAL HEALTH-PARTICIPATE IN AND COMPLETE MENTAL HEALTH TREATMENT COURT.",
    "MENTAL HEALTH-PARTICIPATE IN MEDICAL/PSYCHOLOGICAL TREATMENT.",
]
