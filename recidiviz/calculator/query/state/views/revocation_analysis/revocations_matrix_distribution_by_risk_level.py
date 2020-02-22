# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Revocations Matrix Distribution by Risk Level."""
# pylint: disable=trailing-whitespace, line-too-long

from recidiviz.calculator.query import bqview
from recidiviz.calculator.query.state import view_config
from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
METRICS_DATASET = view_config.DATAFLOW_METRICS_DATASET
VIEWS_DATASET = view_config.DASHBOARD_VIEWS_DATASET

REVOCATIONS_MATRIX_DISTRIBUTION_BY_RISK_LEVEL_VIEW_NAME = \
    'revocations_matrix_distribution_by_risk_level'

REVOCATIONS_MATRIX_DISTRIBUTION_BY_RISK_LEVEL_DESCRIPTION = """
 Revocations matrix of violation response count and most severe violation by risk level, and metric period month.
 This counts all individuals admitted to prison for a revocation of probation or parole, broken down by number of 
 violations leading up to the revocation, the most severe violation, risk level and the metric period. 
 """

# TODO(2853): Handle unset violation type in the calc step
REVOCATIONS_MATRIX_DISTRIBUTION_BY_RISK_LEVEL_QUERY = \
    """
    /*{description}*/
 
    SELECT
      state_code,
      violation_type,
      reported_violations,
      IFNULL(population_count, 0) AS population_count,
      total_supervision_count,
      risk_level,
      supervision_type,
      charge_category,
      district,
      metric_period_months
    FROM (
      SELECT
        state_code, 
        IFNULL(violation_type, 'NO_VIOLATIONS') AS violation_type,
        reported_violations,
        SUM(CASE WHEN reported_violations > 0 AND violation_type IS NULL 
                 THEN count - IFNULL(response_with_violations, 0)
                 ELSE count END) AS total_supervision_count,
        risk_level,
        supervision_type,
        charge_category,
        district,
        metric_period_months    
      FROM (
        SELECT
          state_code,
          CASE WHEN most_severe_violation_type = 'TECHNICAL' AND most_severe_violation_type_subtype = 'SUBSTANCE_ABUSE' 
            THEN most_severe_violation_type_subtype 
            ELSE most_severe_violation_type END as violation_type,
          IF(response_count > 8, 8, response_count) as reported_violations,
          count, 
          IFNULL(assessment_score_bucket, 'OVERALL') as risk_level, 
          supervision_type, 
          IFNULL(case_type, 'ALL') AS charge_category, 
          IFNULL(supervising_district_external_id, 'ALL') as district,
          metric_period_months 
        FROM `{project_id}.{metrics_dataset}.supervision_population_metrics`
        JOIN `{project_id}.{views_dataset}.most_recent_job_id_by_metric_and_state_code` job
          USING (state_code, job_id, year, month, metric_period_months)
        WHERE methodology = 'PERSON'
          AND supervision_type IS NOT NULL
          AND month IS NOT NULL
          AND response_count IS NOT NULL
          AND (most_severe_violation_type IS NOT NULL OR most_severe_violation_type_subtype = 'UNSET')
          AND most_severe_violation_type_subtype IS NOT NULL
          AND assessment_score_bucket IS NOT NULL
          AND assessment_type IS NULL
          AND supervising_officer_external_id IS NULL
          AND age_bucket IS NULL
          AND race IS NULL
          AND ethnicity IS NULL
          AND gender IS NULL
          AND year = EXTRACT(YEAR FROM CURRENT_DATE('US/Pacific'))
          AND month = EXTRACT(MONTH FROM CURRENT_DATE('US/Pacific'))
          AND job.metric_type = 'SUPERVISION_POPULATION'
      )
      LEFT JOIN (
        SELECT
          state_code,
          IF(response_count > 8, 8, response_count) as reported_violations,
          IFNULL(assessment_score_bucket, 'OVERALL') as risk_level, 
          supervision_type, 
          IFNULL(case_type, 'ALL') AS charge_category, 
          IFNULL(supervising_district_external_id, 'ALL') as district,
          metric_period_months,
          SUM(count) AS response_with_violations
        FROM `{project_id}.{metrics_dataset}.supervision_population_metrics`
        JOIN `{project_id}.{views_dataset}.most_recent_job_id_by_metric_and_state_code` job
          USING (state_code, job_id, year, month, metric_period_months)
        WHERE methodology = 'PERSON'
          AND supervision_type IS NOT NULL
          AND month IS NOT NULL
          AND response_count IS NOT NULL
          AND most_severe_violation_type IS NOT NULL
          AND most_severe_violation_type_subtype = 'UNSET'
          AND assessment_score_bucket IS NOT NULL
          AND assessment_type IS NULL
          AND supervising_officer_external_id IS NULL
          AND age_bucket IS NULL
          AND race IS NULL
          AND ethnicity IS NULL
          AND gender IS NULL
          AND year = EXTRACT(YEAR FROM CURRENT_DATE('US/Pacific'))
          AND month = EXTRACT(MONTH FROM CURRENT_DATE('US/Pacific'))
          AND job.metric_type = 'SUPERVISION_POPULATION'
        GROUP BY 1,2,3,4,5,6,7
      ) supervision_response_with_violation
      USING (state_code, metric_period_months, reported_violations, risk_level, supervision_type, charge_category, district)
      GROUP BY state_code, violation_type, reported_violations, risk_level, supervision_type, 
        charge_category, district, metric_period_months 
    ) pop
    LEFT JOIN (
      SELECT
        state_code,
        IFNULL(violation_type, 'NO_VIOLATIONS') AS violation_type,
        reported_violations,
        SUM(CASE WHEN reported_violations > 0 AND violation_type IS NULL 
                 THEN total_revocations - IFNULL(response_with_violations, 0)
                 ELSE total_revocations END) as population_count,
        risk_level,
        supervision_type,
        charge_category,
        district,
        metric_period_months
      FROM (
        SELECT
          state_code,
          CASE WHEN most_severe_violation_type = 'TECHNICAL' AND most_severe_violation_type_subtype = 'SUBSTANCE_ABUSE' 
            THEN most_severe_violation_type_subtype 
            ELSE most_severe_violation_type END as violation_type,
          IF(response_count > 8, 8, response_count) as reported_violations,
          count as total_revocations,
          IFNULL(assessment_score_bucket, 'OVERALL') as risk_level,
          supervision_type,
          IFNULL(case_type, 'ALL') AS charge_category,
          IFNULL(supervising_district_external_id, 'ALL') as district,
          metric_period_months
        FROM `{project_id}.{metrics_dataset}.supervision_revocation_analysis_metrics`
        JOIN `{project_id}.{views_dataset}.most_recent_job_id_by_metric_and_state_code` job
          USING (state_code, job_id, year, month, metric_period_months)
        WHERE methodology = 'PERSON'
          AND supervision_type IS NOT NULL
          AND month IS NOT NULL
          AND response_count IS NOT NULL
          AND (most_severe_violation_type IS NOT NULL OR most_severe_violation_type_subtype = 'UNSET')
          AND most_severe_violation_type_subtype IS NOT NULL
          AND most_severe_response_decision IS NULL
          AND assessment_score_bucket IS NOT NULL
          AND assessment_type IS NULL
          AND revocation_type = 'REINCARCERATION'
          AND source_violation_type IS NULL
          AND supervising_officer_external_id IS NULL
          AND age_bucket IS NULL
          AND race IS NULL
          AND ethnicity IS NULL
          AND gender IS NULL
          AND person_external_id IS NULL
          AND year = EXTRACT(YEAR FROM CURRENT_DATE('US/Pacific'))
          AND month = EXTRACT(MONTH FROM CURRENT_DATE('US/Pacific'))
          AND job.metric_type = 'SUPERVISION_REVOCATION_ANALYSIS'
      )
      LEFT JOIN (
        SELECT
          state_code,
          IF(response_count > 8, 8, response_count) as reported_violations,
          IFNULL(assessment_score_bucket, 'OVERALL') as risk_level,
          supervision_type,
          IFNULL(case_type, 'ALL') AS charge_category,
          IFNULL(supervising_district_external_id, 'ALL') as district,
          metric_period_months,
          SUM(count) as response_with_violations
        FROM `{project_id}.{metrics_dataset}.supervision_revocation_analysis_metrics`
        JOIN `{project_id}.{views_dataset}.most_recent_job_id_by_metric_and_state_code` job
          USING (state_code, job_id, year, month, metric_period_months)
        WHERE methodology = 'PERSON'
          AND supervision_type IS NOT NULL
          AND month IS NOT NULL
          AND response_count IS NOT NULL
          AND most_severe_violation_type IS NOT NULL
          AND most_severe_violation_type_subtype = 'UNSET'
          AND most_severe_response_decision IS NULL
          AND assessment_score_bucket IS NOT NULL
          AND assessment_type IS NULL
          AND revocation_type = 'REINCARCERATION'
          AND source_violation_type IS NULL
          AND supervising_officer_external_id IS NULL
          AND age_bucket IS NULL
          AND race IS NULL
          AND ethnicity IS NULL
          AND gender IS NULL
          AND person_external_id IS NULL
          AND year = EXTRACT(YEAR FROM CURRENT_DATE('US/Pacific'))
          AND month = EXTRACT(MONTH FROM CURRENT_DATE('US/Pacific'))
          AND job.metric_type = 'SUPERVISION_REVOCATION_ANALYSIS'
        GROUP BY 1,2,3,4,5,6,7
      ) revocation_response_with_violation
      USING (state_code, metric_period_months, reported_violations, risk_level, supervision_type, charge_category, district)
      GROUP BY state_code, violation_type, reported_violations, risk_level, supervision_type, 
        charge_category, district, metric_period_months
    ) rev
    USING (state_code, violation_type, reported_violations, risk_level, supervision_type, 
      charge_category, district, metric_period_months)
    WHERE supervision_type IN ('ALL', 'PAROLE', 'PROBATION')
      AND total_supervision_count > 0
    ORDER BY state_code, district, supervision_type, risk_level, metric_period_months, violation_type, reported_violations 
    """.format(
        description=REVOCATIONS_MATRIX_DISTRIBUTION_BY_RISK_LEVEL_DESCRIPTION,
        project_id=PROJECT_ID,
        metrics_dataset=METRICS_DATASET,
        views_dataset=VIEWS_DATASET,
        )

REVOCATIONS_MATRIX_DISTRIBUTION_BY_RISK_LEVEL_VIEW = bqview.BigQueryView(
    view_id=REVOCATIONS_MATRIX_DISTRIBUTION_BY_RISK_LEVEL_VIEW_NAME,
    view_query=REVOCATIONS_MATRIX_DISTRIBUTION_BY_RISK_LEVEL_QUERY
)

if __name__ == '__main__':
    print(REVOCATIONS_MATRIX_DISTRIBUTION_BY_RISK_LEVEL_VIEW.view_id)
    print(REVOCATIONS_MATRIX_DISTRIBUTION_BY_RISK_LEVEL_VIEW.view_query)
