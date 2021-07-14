# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""View tracking characteristics/composition of the supervision population for each supervision officer at monthly cadence"""
# pylint: disable=trailing-whitespace
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_OFFICER_CASELOAD_HEALTH_METRICS_VIEW_NAME = (
    "supervision_officer_caseload_health_metrics"
)

SUPERVISION_OFFICER_CASELOAD_HEALTH_METRICS_VIEW_DESCRIPTION = "Metrics capturing caseload composition, evolution of officer punitivity, and decarceral actions received by clients on supervision"

SUPERVISION_OFFICER_CASELOAD_HEALTH_METRICS_QUERY_TEMPLATE = """
    /*{description}*/
        WITH officers_unnested AS (
        SELECT DISTINCT 
            supervision_date,
            officers.state_code,
            officers.start_date,
            COALESCE(officers.end_date, CURRENT_DATE()) AS end_date,
            officers.person_id,
            supervising_officer_external_id,
            MIN(officers.start_date) OVER (PARTITION BY officers.state_code, supervising_officer_external_id) as officer_tenure_start_date
        FROM `{project_id}.{analyst_dataset}.supervision_officer_sessions_materialized` officers,
          UNNEST(GENERATE_DATE_ARRAY(DATE_SUB(CURRENT_DATE(), INTERVAL 5 YEAR), CURRENT_DATE(), INTERVAL 1 MONTH)) supervision_date 
        WHERE supervision_date BETWEEN officers.start_date AND COALESCE(officers.end_date, '9999-01-01')
    ),
    client_cumulative_counts AS (
     /* Number of violations, downgrades, and upgrades experienced by everyone currently on an officer's caseload, with that officer, during ongoing supervision session. */
        SELECT supervision_date, officers.state_code, supervising_officer_external_id,
            DATE_DIFF(supervision_date, ANY_VALUE(officer_tenure_start_date), DAY) officer_tenure_days,
            COUNT(DISTINCT officers.person_id) as caseload_size,
            COUNT(DISTINCT IF(violations.response_date BETWEEN officers.start_date AND supervision_date, CONCAT(officers.person_id, response_date), NULL)) as client_violations,
            COUNT(DISTINCT IF(levels.supervision_downgrade > 0 AND levels.start_date BETWEEN officers.start_date AND supervision_date, CONCAT(officers.person_id, levels.start_date), NULL)) as client_downgrades,
            /* Count of supervision downgrades that were the result of PO discretion alone, by excluding all downgrades occurring on July 23, 2020, when Idaho put in place a policy that automatically downgraded clients in the system based on their score. */
            COUNT(DISTINCT IF(levels.supervision_downgrade > 0 AND (officers.state_code != 'US_ID' OR levels.start_date != '2020-07-23') AND levels.start_date BETWEEN officers.start_date AND supervision_date, CONCAT(officers.person_id, levels.start_date), NULL)) as client_downgrades_discretionary,
            COUNT(DISTINCT IF(levels.supervision_upgrade > 0 AND levels.start_date BETWEEN officers.start_date AND supervision_date, CONCAT(officers.person_id, levels.start_date), NULL)) as client_upgrades,
            COUNT(DISTINCT IF(ed.decision_status != 'INVALID' AND ed.request_date BETWEEN officers.start_date AND supervision_date, ed.person_id, NULL)) as client_earned_discharge_requests,
        FROM officers_unnested officers 
        LEFT JOIN `{project_id}.{analyst_dataset}.violations_sessions_materialized` violations
            USING (state_code, person_id)
        LEFT JOIN `{project_id}.{analyst_dataset}.supervision_level_sessions_materialized` levels
            USING (state_code, person_id)
        LEFT JOIN `{project_id}.{base_dataset}.state_early_discharge` ed
            USING (state_code, person_id)
        GROUP BY 1,2,3
    ),
    officer_inflows AS (
        /* Calculate total number of revocations over the entire course of an officer's tenure */
        SELECT officers.state_code, supervision_date, supervising_officer_external_id, 
            COUNT(revocation_date) as lifetime_revocation_count,
        FROM `{project_id}.{analyst_dataset}.supervision_officer_sessions_materialized` officers,
            UNNEST(GENERATE_DATE_ARRAY(DATE_SUB(CURRENT_DATE(), INTERVAL 5 YEAR), CURRENT_DATE(), INTERVAL 1 MONTH)) supervision_date 
        LEFT JOIN `{project_id}.{analyst_dataset}.revocation_sessions_materialized` revocations
            ON officers.state_code = revocations.state_code
            AND officers.person_id = revocations.person_id
            AND DATE_SUB(revocations.revocation_date, INTERVAL 1 DAY) BETWEEN officers.start_date AND LEAST(COALESCE(officers.end_date, CURRENT_DATE()), supervision_date)
        WHERE officers.start_date <= supervision_date
        GROUP BY 1,2,3
    ),
    officer_outflows AS (
        /* Calculate total number of successful supervision completions over the entire course of an officer's tenure */
        SELECT officers.state_code, supervision_date, supervising_officer_external_id, 
            COUNT(sessions.end_date) as lifetime_release_count,
        FROM `{project_id}.{analyst_dataset}.supervision_officer_sessions_materialized` officers,
            UNNEST(GENERATE_DATE_ARRAY(DATE_SUB(CURRENT_DATE(), INTERVAL 5 YEAR), CURRENT_DATE(), INTERVAL 1 MONTH)) supervision_date 
        LEFT JOIN `{project_id}.{analyst_dataset}.compartment_sessions_materialized` sessions
            ON officers.state_code = sessions.state_code
            AND officers.person_id = sessions.person_id
            AND outflow_to_level_1 = 'RELEASE'
            AND sessions.end_date BETWEEN officers.start_date AND LEAST(COALESCE(officers.end_date, CURRENT_DATE()), supervision_date)
        WHERE officers.start_date <= supervision_date
        GROUP BY 1,2,3
    ),
    
    caseload_characteristics AS (
        /* Calculates attributes of a caseload's composition on a single day snapshot*/
        SELECT officers.state_code, supervision_date, officers.supervising_officer_external_id,
            COUNTIF(dataflow.case_type = 'GENERAL')/COUNT(1) AS prop_general_caseload,
            COUNTIF(dataflow.case_type = 'SEX_OFFENSE')/COUNT(1) AS prop_sex_offense_caseload,
            COUNTIF(demographics.gender = 'FEMALE')/COUNT(1) AS prop_female_caseload,
            COUNTIF(demographics.prioritized_race_or_ethnicity = 'WHITE')/COUNT(1) AS prop_white_caseload,
            COUNTIF(demographics.prioritized_race_or_ethnicity = 'BLACK')/COUNT(1) AS prop_black_caseload,
            COUNTIF(dataflow.correctional_level = 'MINIMUM')/COUNT(1) AS prop_low_risk_caseload,
            AVG(assessment.assessment_score) AS avg_risk_score,
            COUNTIF(recommended_supervision_downgrade_level IS NOT NULL) AS supervision_level_mismatch_count
    
        FROM officers_unnested officers
        LEFT JOIN `{project_id}.{analyst_dataset}.person_demographics_materialized` demographics
            USING (state_code, person_id)
        LEFT JOIN `{project_id}.{analyst_dataset}.dataflow_sessions_materialized` dataflow
            ON officers.state_code = dataflow.state_code
            AND officers.person_id = dataflow.person_id
            AND supervision_date BETWEEN dataflow.start_date AND COALESCE(dataflow.end_date, CURRENT_DATE()) 
        LEFT JOIN `{project_id}.{analyst_dataset}.assessment_score_sessions_materialized` assessment
            ON officers.state_code = assessment.state_code
            AND officers.person_id = assessment.person_id
            AND supervision_date BETWEEN assessment.assessment_date AND COALESCE(assessment.score_end_date, CURRENT_DATE())
        LEFT JOIN `{project_id}.{materialized_metrics_dataset}.most_recent_supervision_case_compliance_metrics_materialized` risk_compliance
            ON officers.state_code = risk_compliance.state_code
            AND officers.person_id = risk_compliance.person_id
            AND supervision_date = risk_compliance.date_of_evaluation
        GROUP BY 1,2,3
    )
    SELECT *, 
        lifetime_revocation_count - lifetime_release_count AS lifetime_system_inflows
    FROM client_cumulative_counts
    JOIN caseload_characteristics 
        USING (supervision_date, state_code, supervising_officer_external_id)
    JOIN officer_inflows 
        USING (supervision_date, state_code, supervising_officer_external_id)
    JOIN officer_outflows 
        USING (supervision_date, state_code, supervising_officer_external_id)
    WHERE supervising_officer_external_id IS NOT NULL
    """

SUPERVISION_OFFICER_CASELOAD_HEALTH_METRICS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.ANALYST_VIEWS_DATASET,
    view_id=SUPERVISION_OFFICER_CASELOAD_HEALTH_METRICS_VIEW_NAME,
    view_query_template=SUPERVISION_OFFICER_CASELOAD_HEALTH_METRICS_QUERY_TEMPLATE,
    description=SUPERVISION_OFFICER_CASELOAD_HEALTH_METRICS_VIEW_DESCRIPTION,
    analyst_dataset=dataset_config.ANALYST_VIEWS_DATASET,
    base_dataset=dataset_config.STATE_BASE_DATASET,
    materialized_metrics_dataset=dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_OFFICER_CASELOAD_HEALTH_METRICS_VIEW_BUILDER.build_and_print()
