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
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.    See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.    If not, see <https://www.gnu.org/licenses/>.
# =============================================================================
"""Sessionized view of parole board hearings off of pre-processed raw IX data"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_NAME = "us_ix_parole_board_hearing_sessions"

_VIEW_DESCRIPTION = "Sessionized view of parole board hearings constructed off of pre-processed raw IX data"

_QUERY_TEMPLATE = """

WITH employee_table AS (
    SELECT
        prb_PBCase_Employee.EmployeeId,
        prb_PBCase_Employee.PBCaseId,
        employee.StaffId AS recommendation_officer_external_id
    FROM
        `{project_id}.us_ix_raw_data_up_to_date_views.prb_PBCase_Employee_latest` prb_PBCase_Employee
    LEFT JOIN
        `{project_id}.us_ix_raw_data_up_to_date_views.ref_Employee_latest` employee
    ON
        employee.EmployeeId = prb_PBCase_Employee.EmployeeId
    INNER JOIN
        `{project_id}.us_ix_raw_data_up_to_date_views.ref_EmployeeType_latest` employee_type
    ON
        employee.EmployeeTypeId = employee_type.EmployeeTypeId
    WHERE
        EmployeeTypeName = 'Parole Commission Parole Hearing Investigator'
)

SELECT
    sessions.state_code,
    person.person_id,
    sessions.compartment_level_1_super_session_id,
    sessions.start_date AS incarceration_start_date,
    sessions.end_date_exclusive AS incarceration_end_date_exclusive,
    ROW_NUMBER() OVER w AS hearing_number,
    IFNULL(LAG(DATE(hearings.HearingStartDate)) OVER w, start_date) AS start_date,
    DATE(hearings.HearingStartDate) AS hearing_date,
    DATE(hearings.DecisionSubmitDate) AS decision_date,
    CAST(NULL AS STRING) AS recommendation_officer_external_id,
    CAST(NULL AS STRING) AS recommended_decision,
    CAST(NULL AS STRING) AS recommended_decision_raw,
    CASE
        WHEN vote_value.BOPVoteValueName IN (
            "Deny", "Deny Parole", "Deny Parole & Pass to FTRD", "Reject Request"
        ) THEN "DENIED"
        WHEN vote_value.BOPVoteValueName IN (
            "Grant", "Grant Parole", "Grant/Retain", "Reinstate Parole"
        ) THEN "APPROVED"
        WHEN vote_value.BOPVoteValueName IN (
            "Continue", "Continue See in Person", "Continue to Full Commission", "Schedule Hearing"
        ) THEN "CONTINUED"
        WHEN vote_value.BOPVoteValueName IS NULL THEN "EXTERNAL_UNKNOWN"
        ELSE "INTERNAL_UNKNOWN" 
    END AS decision,
    vote_value.BOPVoteValueName AS decision_raw,
    CAST(NULL AS STRING) AS decision_reasons_raw,
    DATE(hearings.TentativeParoleDate) AS tentative_parole_date,
    DATE_DIFF(
        DATE(hearings.HearingStartDate),
        start_date,
        DAY
    ) AS days_since_incarceration_start,
    DATE_DIFF(
        DATE(hearings.HearingStartDate),
        LAG(DATE(hearings.HearingStartDate)) OVER w, DAY
    ) AS days_since_prior_hearing,
FROM
    `{project_id}.us_ix_raw_data_up_to_date_views.bop_BOPHearing_latest` hearings
INNER JOIN
    `{project_id}.us_ix_raw_data_up_to_date_views.prb_PBCase_latest` cases
ON
    hearings.PBCaseId = cases.PBCaseId
INNER JOIN
    `{project_id}.normalized_state.state_person_external_id` person
ON
    person.state_code = "US_IX" AND cases.OffenderId = person.external_id
LEFT JOIN
    `{project_id}.us_ix_raw_data_up_to_date_views.bop_BOPVoteResult_latest` vote_result
ON
    hearings.PBCaseId = vote_result.PBCaseId
LEFT JOIN
    `{project_id}.us_ix_raw_data_up_to_date_views.bop_BOPVoteValue_latest` vote_value
ON
    vote_result.BOPVoteValueId = vote_value.BOPVoteValueId
LEFT JOIN
    `{project_id}.us_ix_raw_data.prb_PBCaseSubType` subtypes
ON
    cases.PBCaseSubtypeId = subtypes.PBCaseSubtypeId
INNER JOIN
    `{project_id}.sessions.compartment_level_1_super_sessions_materialized` sessions
ON
    person.person_id = sessions.person_id
    AND DATE(hearings.HearingStartDate) BETWEEN sessions.start_date AND IFNULL(sessions.end_date, "9999-01-01")
LEFT JOIN
    employee_table
ON
    employee_table.PBCaseId = cases.PBCaseId

-- Select parole board case types that correspond with a parole board hearing
-- HOR = hearing officer review
-- SIPR = self-initiated parole hearing
WHERE
    subtypes.PBCaseSubTypeDesc IN ("Regular", "HOR", "SIPR", "Commutation", "Pardon", "Medical")
    AND sessions.compartment_level_1 = "INCARCERATION"

-- Note that this deduplicates and prioritizes decisions in alphabetical order of decision
-- (APPROVED > CONTINUED > DENIED > EXTERNAL_UNKNOWN > INTERNAL_UNKNOWN)
QUALIFY
    ROW_NUMBER() OVER (PARTITION BY person_id, hearings.HearingStartDate ORDER BY decision ASC) = 1
WINDOW
    w AS (PARTITION BY person.person_id, sessions.start_date ORDER BY hearings.HearingStartDate ASC)
"""

US_IX_PAROLE_BOARD_HEARING_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=_VIEW_NAME,
    view_query_template=_QUERY_TEMPLATE,
    description=_VIEW_DESCRIPTION,
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_IX_PAROLE_BOARD_HEARING_SESSIONS_VIEW_BUILDER.build_and_print()
