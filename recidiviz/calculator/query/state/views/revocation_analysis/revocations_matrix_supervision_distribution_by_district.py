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
"""Revocations Matrix Supervision Distribution by District."""
# pylint: disable=trailing-whitespace, line-too-long

from recidiviz.calculator.query import bqview
from recidiviz.calculator.query.state import view_config
from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
METRICS_DATASET = view_config.DATAFLOW_METRICS_DATASET
VIEWS_DATASET = view_config.DASHBOARD_VIEWS_DATASET

REVOCATIONS_MATRIX_SUPERVISION_DISTRIBUTION_BY_DISTRICT_VIEW_NAME = \
    'revocations_matrix_supervision_distribution_by_district'

REVOCATIONS_MATRIX_SUPERVISION_DISTRIBUTION_BY_DISTRICT_DESCRIPTION = """
 Supervision matrix of violation response count and most severe violation by district and metric period month.
 This counts all individuals on supervision, broken down by number of violations during the last 12 months on
 supervision, the most severe violation, the district, and the metric period. 
 """

# TODO(2853): Handle unset violation type in the calc step
REVOCATIONS_MATRIX_SUPERVISION_DISTRIBUTION_BY_DISTRICT_QUERY = \
    """
    /*{description}*/
    SELECT
      state_code,
      district,
      supervision_type,
      SUM(CASE WHEN reported_violations > 0 AND violation_type IS NULL
               THEN total_population - IFNULL(response_with_violations, 0)
               ELSE total_population END) as total_population,
      charge_category,
      IFNULL(violation_type, 'NO_VIOLATIONS') as violation_type,
      reported_violations,
      metric_period_months
    FROM (
      SELECT
        state_code,
        supervising_district_external_id as district,
        supervision_type,
        count as total_population,
        IFNULL(case_type, 'ALL') as charge_category,
        CASE WHEN most_severe_violation_type = 'TECHNICAL' AND most_severe_violation_type_subtype = 'SUBSTANCE_ABUSE'
             THEN most_severe_violation_type_subtype
             ELSE most_severe_violation_type END AS violation_type,
        IF(response_count > 8, 8, response_count) AS reported_violations,
        metric_period_months
      FROM `{project_id}.{metrics_dataset}.supervision_population_metrics`
      JOIN `{project_id}.{views_dataset}.most_recent_job_id_by_metric_and_state_code` job
        USING (state_code, job_id, year, month, metric_period_months)
      WHERE methodology = 'PERSON'
        AND month IS NOT NULL
        AND supervision_type IS NOT NULL
        AND (most_severe_violation_type IS NOT NULL OR most_severe_violation_type_subtype = 'UNSET')
        AND most_severe_violation_type_subtype IS NOT NULL
        AND response_count IS NOT NULL
        AND supervising_district_external_id IS NOT NULL
        AND supervising_officer_external_id IS NULL
        AND assessment_score_bucket IS NULL
        AND assessment_type IS NULL
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
          supervising_district_external_id as district,
          supervision_type,
          IFNULL(case_type, 'ALL') as charge_category,
          IF(response_count > 8, 8, response_count) AS reported_violations,
          metric_period_months,
          SUM(count) as response_with_violations,
        FROM `{project_id}.{metrics_dataset}.supervision_population_metrics`
        JOIN `{project_id}.{views_dataset}.most_recent_job_id_by_metric_and_state_code` job
          USING (state_code, job_id, year, month, metric_period_months)
      WHERE methodology = 'PERSON'
          AND month IS NOT NULL
          AND supervision_type IS NOT NULL
          AND response_count IS NOT NULL
          AND most_severe_violation_type IS NOT NULL
          AND most_severe_violation_type_subtype = 'UNSET'
          AND supervising_district_external_id IS NOT NULL
          AND supervising_officer_external_id IS NULL
          AND assessment_score_bucket IS NULL
          AND assessment_type IS NULL
          AND age_bucket IS NULL
          AND race IS NULL
          AND ethnicity IS NULL
          AND gender IS NULL
          AND year = EXTRACT(YEAR FROM CURRENT_DATE('US/Pacific'))
          AND month = EXTRACT(MONTH FROM CURRENT_DATE('US/Pacific'))
          AND job.metric_type = 'SUPERVISION_POPULATION'
      GROUP BY 1,2,3,4,5,6
    ) supervision_responses_with_violations
    USING (state_code, district, supervision_type, charge_category, reported_violations, metric_period_months)
    GROUP BY state_code, district, supervision_type, charge_category, violation_type, reported_violations, metric_period_months
    ORDER BY state_code, metric_period_months, district, supervision_type, reported_violations, charge_category
    """.format(
        description=REVOCATIONS_MATRIX_SUPERVISION_DISTRIBUTION_BY_DISTRICT_DESCRIPTION,
        project_id=PROJECT_ID,
        metrics_dataset=METRICS_DATASET,
        views_dataset=VIEWS_DATASET,
        )

REVOCATIONS_MATRIX_SUPERVISION_DISTRIBUTION_BY_DISTRICT_VIEW = bqview.BigQueryView(
    view_id=REVOCATIONS_MATRIX_SUPERVISION_DISTRIBUTION_BY_DISTRICT_VIEW_NAME,
    view_query=REVOCATIONS_MATRIX_SUPERVISION_DISTRIBUTION_BY_DISTRICT_QUERY
)

if __name__ == '__main__':
    print(REVOCATIONS_MATRIX_SUPERVISION_DISTRIBUTION_BY_DISTRICT_VIEW.view_id)
    print(REVOCATIONS_MATRIX_SUPERVISION_DISTRIBUTION_BY_DISTRICT_VIEW.view_query)
