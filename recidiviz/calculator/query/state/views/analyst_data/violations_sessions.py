# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Sessionized view of violations data - joins violations reports to sessions view
to allow analysis of violations that are associated with a given compartment"""
# pylint: disable=trailing-whitespace
# pylint: disable=line-too-long
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIOLATIONS_SESSIONS_VIEW_NAME = "violations_sessions"

VIOLATIONS_SESSIONS_VIEW_DESCRIPTION = """Sessionized view of violations data - joins violations reports to sessions view
    to allow analysis of violations that are associated with a given compartment"""

US_PA_TECHNICAL_VIOLATION_SPECIAL_SUBTYPE = ("H03", "H12", "L02", "L08", "M03", "M14")

VIOLATIONS_SESSIONS_SUPPORTED_STATES = ("US_ID", "US_PA")

VIOLATIONS_SESSIONS_QUERY_TEMPLATE = """
    /*{description}*/
    /* TODO(#7387): Incorporate the output of the calc pipeline to create this view once #6240 is merged */
    
    WITH violations_cte AS (
        SELECT
            violation_response.supervision_violation_response_id,
            violation_response.state_code,
            violation_response.person_id,
            type.violation_type,
            decision.decision as response_decision,
            violation.is_violent,
            violation.is_sex_offense,
            CASE
                 WHEN state_code = "US_PA" AND type.violation_type = 'TECHNICAL' AND type.violation_type_raw_text LIKE 'H%' AND type.violation_type_raw_text NOT IN ('{US_PA_special_subtypes}') THEN "HIGH_TECH"
                 WHEN state_code = "US_PA" AND type.violation_type = 'TECHNICAL' AND type.violation_type_raw_text LIKE 'M%' AND type.violation_type_raw_text NOT IN ('{US_PA_special_subtypes}') THEN "MED_TECH"
                 WHEN state_code = "US_PA" AND type.violation_type = 'TECHNICAL' AND type.violation_type_raw_text LIKE 'L%' AND type.violation_type_raw_text NOT IN ('{US_PA_special_subtypes}') THEN "LOW_TECH"
                 WHEN state_code = "US_PA" AND type.violation_type = 'TECHNICAL' AND type.violation_type_raw_text IN ('{US_PA_special_subtypes}') THEN "SUBSTANCE_ABUSE"
                 WHEN state_code = "US_PA" AND type.violation_type = 'TECHNICAL' AND type.violation_type_raw_text IN ('M16') AND type.violation_type_raw_text NOT IN ('{US_PA_special_subtypes}') THEN "ELEC_MONITORING"
                 ELSE "NONE"
            END AS violation_sub_type,
            violation_response.response_date,
            violation_response.supervision_violation_id
        FROM `{project_id}.{state_dataset}.state_supervision_violation_response` violation_response
        LEFT JOIN `{project_id}.{state_dataset}.state_supervision_violation_response_decision_entry` decision
            USING (supervision_violation_response_id, person_id, state_code)
        LEFT JOIN `{project_id}.{state_dataset}.state_supervision_violation_type_entry` type
            USING (supervision_violation_id, person_id, state_code)
        LEFT JOIN `{project_id}.{state_dataset}.state_supervision_violation` violation
            USING (supervision_violation_id, person_id, state_code)
        
        /*  This exclusion is to avoid double counting violations in PA. All violations - tracked
        in raw table dbo_SanctionTracking - have response_type = 'VIOLATION_REPORT'. Some of those violations will also
        have subsequent actions captured in dbo_BoardAction, where response_type = 'PERMANENT_DECISION' in our schema.
        Counting both would double count certain violations, so we remove the board actions */
        
        WHERE violation_response.response_type != 'PERMANENT_DECISION'
        
    ) , /* Violations are de-duplicated to keep most serious violation type for a single response date, but we also keep track of 
        distinct violations per day */
    violations_dedup_cte AS (
        SELECT *,
        FROM
        (
            SELECT *,
                COUNT(DISTINCT  supervision_violation_id) OVER(PARTITION BY person_id, state_code, response_date) AS distinct_violations_per_day,
                /* Ordering logic:
                    - Deduplicates by person-day
                    - orders first by state-specific violation type prioritization
                    - when violation types are the same we might have different response_decisions. This ordering is the same
                    as the one used in the calc pipeline to choose the most severe response decision */
                ROW_NUMBER() OVER(PARTITION BY person_id, state_code, response_date ORDER BY COALESCE(priority,9999), 
                CASE
                    WHEN response_decision IN ('REVOCATION') THEN 1
                    WHEN response_decision IN ('SHOCK_INCARCERATION') THEN 2 
                    WHEN response_decision IN ('TREATMENT_IN_PRISON') THEN 3
                    WHEN response_decision IN ('WARRANT_ISSUED') THEN 4
                    WHEN response_decision IN ('PRIVILEGES_REVOKED') THEN 5
                    WHEN response_decision IN ('NEW_CONDITIONS') THEN 6
                    WHEN response_decision IN ('EXTENSION') THEN 7
                    WHEN response_decision IN ('SPECIALIZED_COURT') THEN 8
                    WHEN response_decision IN ('SUSPENSION') THEN 9
                    WHEN response_decision IN ('SERVICE_TERMINATION') THEN 10
                    WHEN response_decision IN ('TREATMENT_IN_FIELD') THEN 11
                    WHEN response_decision IN ('COMMUNITY_SERVICE') THEN 12
                    WHEN response_decision IN ('DELAYED_ACTION') THEN 13
                    WHEN response_decision IN ('OTHER') THEN 14
                    WHEN response_decision IN ('INTERNAL_UNKNOWN') THEN 15
                    WHEN response_decision IN ('WARNING') THEN 16
                    WHEN response_decision IN ('CONTINUANCE') THEN 17
                    ELSE 18 END ) as violation_priority,
            FROM violations_cte
            LEFT JOIN `{project_id}.{analyst_dataset}.violation_type_dedup_priority`
                USING(state_code, violation_type, violation_sub_type)
        )
        WHERE violation_priority = 1
    ) 
    SELECT
        violations.state_code,
        violations.person_id,
        violations.supervision_violation_id ,
        violations.supervision_violation_response_id,
        violations.violation_type as most_serious_violation_type,
        violations.violation_sub_type as most_serious_violation_sub_type,
        violations.response_date,
        violations.response_decision,
        violations.distinct_violations_per_day,
        violations.is_violent,
        violations.is_sex_offense,
        sessions.session_id,
        super_sessions.supervision_super_session_id,
        CASE WHEN ROW_NUMBER() OVER(PARTITION BY revocations.person_id, revocations.revocation_session_id 
            ORDER BY CASE WHEN priority IS null THEN 1 ELSE 0 END, priority ASC, response_date DESC) = 1 THEN revocation_session_id END AS revocation_session_id
    FROM violations_dedup_cte violations
    JOIN `{project_id}.{analyst_dataset}.compartment_sessions_materialized` sessions
        ON sessions.person_id = violations.person_id
        AND violations.response_date BETWEEN sessions.start_date AND COALESCE(sessions.end_date, '9999-01-01')
    LEFT JOIN `{project_id}.{analyst_dataset}.supervision_super_sessions_materialized` super_sessions
        ON sessions.person_id = super_sessions.person_id
        AND sessions.session_id BETWEEN super_sessions.session_id_start AND super_sessions.session_id_end
    LEFT JOIN `{project_id}.{analyst_dataset}.revocation_sessions_materialized` revocations
      ON revocations.person_id = violations.person_id
      AND revocations.revocation_date BETWEEN violations.response_date AND DATE_ADD(violations.response_date, INTERVAL 365 DAY)
      
    WHERE violations.state_code IN ('{supported_states}')
  
    """

VIOLATIONS_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.ANALYST_VIEWS_DATASET,
    view_id=VIOLATIONS_SESSIONS_VIEW_NAME,
    view_query_template=VIOLATIONS_SESSIONS_QUERY_TEMPLATE,
    description=VIOLATIONS_SESSIONS_VIEW_DESCRIPTION,
    analyst_dataset=dataset_config.ANALYST_VIEWS_DATASET,
    state_dataset=dataset_config.STATE_BASE_DATASET,
    US_PA_special_subtypes="', '".join(US_PA_TECHNICAL_VIOLATION_SPECIAL_SUBTYPE),
    supported_states="', '".join(VIOLATIONS_SESSIONS_SUPPORTED_STATES),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIOLATIONS_SESSIONS_VIEW_BUILDER.build_and_print()
