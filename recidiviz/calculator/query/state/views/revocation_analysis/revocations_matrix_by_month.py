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
"""Revocations Matrix by month."""
# pylint: disable=trailing-whitespace, line-too-long

from recidiviz.calculator.query import bqview
from recidiviz.calculator.query.state import view_config
from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
METRICS_DATASET = view_config.DATAFLOW_METRICS_DATASET
VIEWS_DATASET = view_config.DASHBOARD_VIEWS_DATASET

REVOCATIONS_MATRIX_BY_MONTH_VIEW_NAME = 'revocations_matrix_by_month'

REVOCATIONS_MATRIX_BY_MONTH_DESCRIPTION = """
 Revocations matrix of violation response count and most severe violation by month.
 This counts all individuals admitted to prison for a revocation of probation or parole, broken down by number of 
 violations leading up to the revocation and the most severe violation. 
 """

# TODO(2853): Handle unset violation type in the calc step
REVOCATIONS_MATRIX_BY_MONTH_QUERY = \
    """
    /*{description}*/
    SELECT *
    FROM (
        SELECT 
          state_code, year, month, 
          IFNULL(violation_type, 'NO_VIOLATIONS') as violation_type, 
          reported_violations, 
          CASE WHEN reported_violations > 0 AND violation_type IS NULL
               THEN total_revocations - IFNULL(response_with_violations, 0)
               ELSE total_revocations END as total_revocations,
          supervision_type, 
          charge_category, 
          district 
        FROM (
          SELECT 
            state_code, year, month, 
            CASE WHEN most_severe_violation_type = 'TECHNICAL' AND most_severe_violation_type_subtype = 'SUBSTANCE_ABUSE' 
                  THEN most_severe_violation_type_subtype 
                  ELSE most_severe_violation_type END as violation_type, 
            IF(response_count > 8, 8, response_count) as reported_violations,
            supervision_type, 
            IFNULL(case_type, 'ALL') AS charge_category,
            IFNULL(supervising_district_external_id, 'ALL') as district,
            SUM(count) as total_revocations
          FROM `{project_id}.{metrics_dataset}.supervision_revocation_analysis_metrics`
          JOIN `{project_id}.{views_dataset}.most_recent_job_id_by_metric_and_state_code` job
            USING (state_code, job_id, year, month, metric_period_months)
          WHERE methodology = 'PERSON'
            AND month IS NOT NULL
            AND metric_period_months = 1
            AND supervision_type IS NOT NULL
            AND response_count IS NOT NULL
            AND most_severe_violation_type_subtype IS NOT NULL
            AND (most_severe_violation_type IS NOT NULL OR most_severe_violation_type_subtype = 'UNSET')
            AND most_severe_response_decision IS NULL
            AND assessment_score_bucket IS NULL
            AND assessment_type IS NULL
            AND revocation_type = 'REINCARCERATION'
            AND source_violation_type IS NULL
            AND supervising_officer_external_id IS NULL
            AND age_bucket IS NULL
            AND race IS NULL
            AND ethnicity IS NULL
            AND gender IS NULL
            AND person_id IS NULL
            AND person_external_id IS NULL
            AND year >= EXTRACT(YEAR FROM DATE_ADD(CURRENT_DATE(), INTERVAL -3 YEAR))
            AND job.metric_type = 'SUPERVISION_REVOCATION_ANALYSIS'
          GROUP BY state_code, year, month, violation_type, reported_violations, supervision_type, charge_category,
            district
        )
        LEFT JOIN (
          SELECT
            state_code, year, month,
            IF(response_count > 8, 8, response_count) as reported_violations,
            supervision_type, 
            IFNULL(case_type, 'ALL') AS charge_category,
            IFNULL(supervising_district_external_id, 'ALL') as district,
            SUM(count) AS response_with_violations
          FROM `{project_id}.{metrics_dataset}.supervision_revocation_analysis_metrics`
          JOIN `{project_id}.{views_dataset}.most_recent_job_id_by_metric_and_state_code` job
            USING (state_code, job_id, year, month, metric_period_months)
          WHERE methodology = 'PERSON'
            AND month IS NOT NULL
            AND metric_period_months = 1
            AND supervision_type IS NOT NULL
            -- SUM the total number of people with a response and violations per category
            AND response_count IS NOT NULL
            AND most_severe_violation_type IS NOT NULL 
            AND most_severe_violation_type_subtype = 'UNSET'
            AND most_severe_response_decision IS NULL
            AND assessment_score_bucket IS NULL
            AND assessment_type IS NULL
            AND revocation_type = 'REINCARCERATION'
            AND source_violation_type IS NULL
            AND supervising_officer_external_id IS NULL
            AND age_bucket IS NULL
            AND race IS NULL
            AND ethnicity IS NULL
            AND gender IS NULL
            AND person_id IS NULL
            AND person_external_id IS NULL
            AND year >= EXTRACT(YEAR FROM DATE_ADD(CURRENT_DATE(), INTERVAL - 3 YEAR))
            AND job.metric_type = 'SUPERVISION_REVOCATION_ANALYSIS'
          GROUP BY state_code, year, month, reported_violations, supervision_type, charge_category, district
        ) revocation_response_with_violation
        USING (state_code, year, month, reported_violations, supervision_type, charge_category, district)
    )
    -- Only include rows that have revocations
    WHERE total_revocations > 0
    ORDER BY state_code, year, month, district, supervision_type, violation_type, reported_violations
    """.format(
        description=REVOCATIONS_MATRIX_BY_MONTH_DESCRIPTION,
        project_id=PROJECT_ID,
        metrics_dataset=METRICS_DATASET,
        views_dataset=VIEWS_DATASET,
        )

REVOCATIONS_MATRIX_BY_MONTH_VIEW = bqview.BigQueryView(
    view_id=REVOCATIONS_MATRIX_BY_MONTH_VIEW_NAME,
    view_query=REVOCATIONS_MATRIX_BY_MONTH_QUERY
)

if __name__ == '__main__':
    print(REVOCATIONS_MATRIX_BY_MONTH_VIEW.view_id)
    print(REVOCATIONS_MATRIX_BY_MONTH_VIEW.view_query)
