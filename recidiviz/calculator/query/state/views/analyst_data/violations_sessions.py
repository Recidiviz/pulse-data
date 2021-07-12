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

us_nd_raw_data_up_to_date_views = "us_nd_raw_data_up_to_date_views"

VIOLATIONS_SESSIONS_VIEW_DESCRIPTION = """Takes the output of the dataflow violations pipeline and integrates it with
other sessions views including compartment_sessions, revocation_sessions, and supervision_super_sessions. This view
also deduplicates to one violation per person per day by doing the following:
- keeping the most severe violation type for a given person_id, violation_id
- keeping the most severe violation type across all violation_ids for a given person_id and response_date
- keeping the most severe response decision across all violation_ids for a given person_id and response_date if they
have the same most_severe_violation_type and most_severe_violation_type_subtype"""

VIOLATIONS_SESSIONS_QUERY_TEMPLATE = """
    /*{description}*/
    WITH violations_cte AS (
        SELECT 
        violations.state_code,
        violations.person_id,
        violations.supervision_violation_id,
        violations.violation_type as most_serious_violation_type,
        violations.violation_type_subtype as most_serious_violation_sub_type,
        violations.response_date,
        violations.most_severe_response_decision,
        violations.is_violent,
        violations.is_sex_offense,
        priority,  
        COUNT(DISTINCT  supervision_violation_id) OVER(PARTITION BY person_id, state_code, response_date) AS distinct_violations_per_day,
        /* Ordering logic:
            - Deduplicates to person-day by keeping most severe violation type across violations on the same day using 
            state-specific violation type prioritization
            - When we have the same person_id, response_date, and most serious violation type / sub-type, 
            we might have different response_decisions. The ordering used below is the same as the one used in 
            the calc pipeline to choose the most severe response decision. See DECISION_SEVERITY_ORDER in 
            recidiviz/calculator/pipeline/utils/violation_response_utils.py 
        */
        ROW_NUMBER() OVER(PARTITION BY person_id, state_code, response_date ORDER BY COALESCE(priority,9999), 
        # TODO(#8187): pull logic into separate dedup view
        CASE
            WHEN most_severe_response_decision IN ('REVOCATION') THEN 1
            WHEN most_severe_response_decision IN ('SHOCK_INCARCERATION') THEN 2 
            WHEN most_severe_response_decision IN ('TREATMENT_IN_PRISON') THEN 3
            WHEN most_severe_response_decision IN ('WARRANT_ISSUED') THEN 4
            WHEN most_severe_response_decision IN ('PRIVILEGES_REVOKED') THEN 5
            WHEN most_severe_response_decision IN ('NEW_CONDITIONS') THEN 6
            WHEN most_severe_response_decision IN ('EXTENSION') THEN 7
            WHEN most_severe_response_decision IN ('SPECIALIZED_COURT') THEN 8
            WHEN most_severe_response_decision IN ('SUSPENSION') THEN 9
            WHEN most_severe_response_decision IN ('SERVICE_TERMINATION') THEN 10
            WHEN most_severe_response_decision IN ('TREATMENT_IN_FIELD') THEN 11
            WHEN most_severe_response_decision IN ('COMMUNITY_SERVICE') THEN 12
            WHEN most_severe_response_decision IN ('DELAYED_ACTION') THEN 13
            WHEN most_severe_response_decision IN ('OTHER') THEN 14
            WHEN most_severe_response_decision IN ('INTERNAL_UNKNOWN') THEN 15
            WHEN most_severe_response_decision IN ('WARNING') THEN 16
            WHEN most_severe_response_decision IN ('CONTINUANCE') THEN 17
            ELSE 18 END ) as violation_priority,
        FROM `{project_id}.{dataflow_dataset}.most_recent_violation_with_response_metrics_materialized` violations
        LEFT JOIN `{project_id}.{analyst_dataset}.violation_type_dedup_priority`
                USING(state_code, violation_type, violation_type_subtype)
        /* This keeps most severe violation type associated with a given supervision_violation_id. There may still be 
        multiple supervision_violation_id's on the same response_date, so there are duplicates on person_id, 
        response_date after this step which are dealt with above in the ordering logic */ 
        WHERE is_most_severe_violation_type
    ), 
    violations_dedup_cte AS (
        SELECT *
        FROM violations_cte 
        WHERE violation_priority = 1
    ),
    us_nd_contacts_violations as (
        SELECT
            person_id,
            'US_ND' AS state_code,
            EXTRACT(date FROM PARSE_TIMESTAMP('%m/%d/%Y %I:%M:%S%p',CONTACT_DATE) ) AS response_date_ND
        FROM
            `{project_id}.us_nd_raw_data_up_to_date_views.docstars_contacts_latest`
        LEFT JOIN
            `{project_id}.{state_dataset}.state_person_external_id`
            ON
            SID = external_id
            AND state_code = "US_ND"
            AND id_type = 'US_ND_SID'
        WHERE
            CONTACT_CODE = 'VI'
            OR C2 = 'VI'
            OR C3 = 'VI'
            OR C4 = 'VI'
            OR C5 = 'VI'
            OR C6 = 'VI'
    ), 
    violations_combine AS (
        SELECT * EXCEPT(response_date, response_date_ND), 
        COALESCE(response_date_ND,response_date) AS response_date
        FROM violations_dedup_cte 
        FULL OUTER JOIN us_nd_contacts_violations USING(person_id,state_code)
    ),
    violations_sessions_info AS (
        SELECT violations.*,
        COALESCE(sessions.session_id,1) as session_id,
        revocations.revocation_session_id as revocation_session_id_temp,
        revocations.revocation_date,
        ROW_NUMBER() OVER(PARTITION BY violations.person_id, violations.response_date 
            ORDER BY revocations.revocation_date ASC) as same_violation_multiple_revocations_rn,
        super_sessions.supervision_super_session_id,
        CASE WHEN ROW_NUMBER() OVER(PARTITION BY violations.person_id, revocations.revocation_session_id 
            ORDER BY CASE WHEN priority IS null THEN 1 ELSE 0 END, priority ASC, response_date DESC) = 1 THEN revocation_session_id END AS revocation_session_id
        FROM violations_combine violations
        /* left join because there is a very small number of observations that dont get a session_id - if the violation
        is recorded before they show up in population metrics and therefore in sessions */
        LEFT JOIN `{project_id}.{analyst_dataset}.compartment_sessions_materialized` sessions
            ON sessions.person_id = violations.person_id
            AND violations.response_date BETWEEN sessions.start_date AND COALESCE(sessions.end_date, '9999-01-01')
        LEFT JOIN `{project_id}.{analyst_dataset}.supervision_super_sessions_materialized` super_sessions
            ON sessions.person_id = super_sessions.person_id
            AND sessions.session_id BETWEEN super_sessions.session_id_start AND super_sessions.session_id_end
        LEFT JOIN `{project_id}.{analyst_dataset}.revocation_sessions_materialized` revocations
        ON revocations.person_id = violations.person_id
        AND revocations.revocation_date BETWEEN violations.response_date AND DATE_ADD(violations.response_date, INTERVAL 365 DAY)        
    )   
    SELECT *
    FROM violations_sessions_info 
    WHERE same_violation_multiple_revocations_rn = 1
    """

VIOLATIONS_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.ANALYST_VIEWS_DATASET,
    view_id=VIOLATIONS_SESSIONS_VIEW_NAME,
    view_query_template=VIOLATIONS_SESSIONS_QUERY_TEMPLATE,
    description=VIOLATIONS_SESSIONS_VIEW_DESCRIPTION,
    analyst_dataset=dataset_config.ANALYST_VIEWS_DATASET,
    dataflow_dataset=dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
    state_dataset=dataset_config.STATE_BASE_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIOLATIONS_SESSIONS_VIEW_BUILDER.build_and_print()
