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
"""Revocations Matrix Distribution by Gender."""
# pylint: disable=trailing-whitespace, line-too-long

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GAE_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

REVOCATIONS_MATRIX_DISTRIBUTION_BY_GENDER_VIEW_NAME = 'revocations_matrix_distribution_by_gender'

REVOCATIONS_MATRIX_DISTRIBUTION_BY_GENDER_DESCRIPTION = """
 Revocations matrix of violation response count and most severe violation by gender, risk level, and metric period month.
 This counts all individuals admitted to prison for a revocation of supervision, broken down by number of
 violations leading up to the revocation, the most severe violation, gender, and the metric period months.
 """

REVOCATIONS_MATRIX_DISTRIBUTION_BY_GENDER_QUERY_TEMPLATE = \
    """
    /*{description}*/
    WITH supervision_counts AS (
    SELECT
      state_code, 
      violation_type,
      reported_violations,
      COUNT(DISTINCT person_id) AS total_supervision_count,
      gender,
      risk_level,
      supervision_type,
      charge_category,
      district,
      metric_period_months    
    FROM `{project_id}.{reference_dataset}.supervision_matrix_by_person`
    GROUP BY state_code, violation_type, reported_violations, gender, risk_level, supervision_type, charge_category,
      district, metric_period_months
  ), termination_counts AS (
     SELECT
      state_code, 
      violation_type,
      reported_violations,
      COUNT(DISTINCT person_id) AS termination_count,
      gender,
      risk_level,
      supervision_type,
      charge_category,
      district,
      metric_period_months    
    FROM `{project_id}.{reference_dataset}.supervision_termination_matrix_by_person` 
    GROUP BY state_code, violation_type, reported_violations, gender, risk_level, supervision_type, charge_category,
      district, metric_period_months
  ), revocation_counts AS (
    SELECT
      state_code,
      violation_type,
      reported_violations,
      COUNT(DISTINCT person_id) AS population_count,
      gender,
      risk_level,
      supervision_type,
      charge_category,
      district,
      metric_period_months
    FROM `{project_id}.{reference_dataset}.revocations_matrix_by_person`
    GROUP BY state_code, violation_type, reported_violations, gender, risk_level, supervision_type, charge_category,
      district, metric_period_months
  )
 
 
    SELECT
      state_code,
      violation_type,
      reported_violations,
      IFNULL(population_count, 0) AS population_count, -- Revocation count
      IFNULL(termination_count, 0) AS total_exit_count,
      total_supervision_count,
      gender,
      risk_level,
      supervision_type,
      charge_category,
      district,
      metric_period_months
    FROM
      supervision_counts
    LEFT JOIN
      revocation_counts
    USING (state_code, violation_type, reported_violations, gender, risk_level, supervision_type, charge_category,
      district, metric_period_months)
    LEFT JOIN
      termination_counts
    USING (state_code, violation_type, reported_violations, gender, risk_level, supervision_type, charge_category,
      district, metric_period_months)
    ORDER BY state_code, metric_period_months, district, supervision_type, gender, risk_level, violation_type,
      reported_violations, charge_category
    """

REVOCATIONS_MATRIX_DISTRIBUTION_BY_GENDER_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=REVOCATIONS_MATRIX_DISTRIBUTION_BY_GENDER_VIEW_NAME,
    view_query_template=REVOCATIONS_MATRIX_DISTRIBUTION_BY_GENDER_QUERY_TEMPLATE,
    description=REVOCATIONS_MATRIX_DISTRIBUTION_BY_GENDER_DESCRIPTION,
    reference_dataset=dataset_config.REFERENCE_TABLES_DATASET,
)


if __name__ == '__main__':
    with local_project_id_override(GAE_PROJECT_STAGING):
        REVOCATIONS_MATRIX_DISTRIBUTION_BY_GENDER_VIEW_BUILDER.build_and_print()
