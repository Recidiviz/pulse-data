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

from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

REVOCATIONS_MATRIX_DISTRIBUTION_BY_RISK_LEVEL_VIEW_NAME = 'revocations_matrix_distribution_by_risk_level'

REVOCATIONS_MATRIX_DISTRIBUTION_BY_RISK_LEVEL_DESCRIPTION = """
 Revocations matrix of violation response count and most severe violation by risk level, and metric period month.
 This counts all individuals admitted to prison for a revocation of supervision, broken down by number of
 violations leading up to the revocation, the most severe violation, risk level and the metric period.
 """

REVOCATIONS_MATRIX_DISTRIBUTION_BY_RISK_LEVEL_QUERY_TEMPLATE = \
    """
    /*{description}*/
    WITH supervision_counts AS (
        SELECT
          state_code, 
          violation_type,
          reported_violations,
          COUNT(DISTINCT person_id) AS supervision_population_count,
          risk_level,
          supervision_type,
          supervision_level,
          charge_category,
          level_1_supervision_location,
          level_2_supervision_location,
          metric_period_months    
        FROM `{project_id}.{reference_views_dataset}.supervision_matrix_by_person_materialized`
        GROUP BY state_code, violation_type, reported_violations, risk_level, supervision_type, supervision_level,
          charge_category, level_1_supervision_location, level_2_supervision_location, metric_period_months
      ), termination_counts AS (
         SELECT
          state_code, 
          violation_type,
          reported_violations,
          COUNT(DISTINCT person_id) AS termination_count,
          risk_level,
          supervision_type,
          supervision_level,
          charge_category,
          level_1_supervision_location,
          level_2_supervision_location,
          metric_period_months    
        FROM `{project_id}.{reference_views_dataset}.supervision_termination_matrix_by_person_materialized` 
        GROUP BY state_code, violation_type, reported_violations, risk_level, supervision_type, supervision_level,
        charge_category, level_1_supervision_location, level_2_supervision_location, metric_period_months
      ), revocation_counts AS (
        SELECT
          state_code,
          violation_type,
          reported_violations,
          COUNT(DISTINCT person_id) AS revocation_count,
          risk_level,
          supervision_type,
          supervision_level,
          charge_category,
          level_1_supervision_location,
          level_2_supervision_location,
          metric_period_months
        FROM `{project_id}.{reference_views_dataset}.revocations_matrix_by_person_materialized`
        GROUP BY state_code, violation_type, reported_violations, risk_level, supervision_type, supervision_level,
            charge_category, level_1_supervision_location, level_2_supervision_location, metric_period_months
    )
 
 
    SELECT
      state_code,
      violation_type,
      reported_violations,
      IFNULL(revocation_count, 0) AS revocation_count,
      IFNULL(termination_count, 0) AS exit_count,
      supervision_population_count,
      risk_level,
      supervision_type,
      supervision_level,
      charge_category,
      level_1_supervision_location,
      level_2_supervision_location,
      metric_period_months
    FROM
      supervision_counts
    LEFT JOIN
      revocation_counts
    USING (state_code, violation_type, reported_violations, risk_level, supervision_type, supervision_level,
        charge_category, level_1_supervision_location, level_2_supervision_location, metric_period_months)
    LEFT JOIN
      termination_counts
    USING (state_code, violation_type, reported_violations, risk_level, supervision_type, supervision_level,
        charge_category, level_1_supervision_location, level_2_supervision_location, metric_period_months)
    """

REVOCATIONS_MATRIX_DISTRIBUTION_BY_RISK_LEVEL_VIEW_BUILDER = MetricBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=REVOCATIONS_MATRIX_DISTRIBUTION_BY_RISK_LEVEL_VIEW_NAME,
    view_query_template=REVOCATIONS_MATRIX_DISTRIBUTION_BY_RISK_LEVEL_QUERY_TEMPLATE,
    dimensions=['state_code', 'metric_period_months', 'level_1_supervision_location',
                'level_2_supervision_location', 'supervision_type', 'violation_type', 'reported_violations',
                'charge_category', 'risk_level', 'supervision_level'],
    description=REVOCATIONS_MATRIX_DISTRIBUTION_BY_RISK_LEVEL_DESCRIPTION,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        REVOCATIONS_MATRIX_DISTRIBUTION_BY_RISK_LEVEL_VIEW_BUILDER.build_and_print()
