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
"""Revocations Matrix Distribution by Race."""
# pylint: disable=trailing-whitespace, line-too-long

from recidiviz.big_query.big_query_view import BigQueryView
from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state import dataset_config

REVOCATIONS_MATRIX_DISTRIBUTION_BY_RACE_VIEW_NAME = 'revocations_matrix_distribution_by_race'

REVOCATIONS_MATRIX_DISTRIBUTION_BY_RACE_DESCRIPTION = """
 Revocations matrix of violation response count and most severe violation by race, risk level, and metric period month.
 This counts all individuals admitted to prison for a revocation of supervision, broken down by number of
 violations leading up to the revocation, the most severe violation, race, and the metric period.
 """

REVOCATIONS_MATRIX_DISTRIBUTION_BY_RACE_QUERY_TEMPLATE = \
    """
    /*{description}*/
    SELECT
      state_code,
      violation_type,
      reported_violations,
      IFNULL(population_count, 0) AS population_count,
      total_supervision_count,
      race,
      risk_level,
      supervision_type,
      charge_category,
      district,
      metric_period_months
    FROM (
      SELECT
        state_code, 
        violation_type,
        reported_violations,
        COUNT(DISTINCT person_id) AS total_supervision_count,
        race_or_ethnicity AS race,
        risk_level,
        supervision_type,
        charge_category,
        district,
        metric_period_months    
      FROM `{project_id}.{reference_dataset}.supervision_matrix_by_person`,
      {race_ethnicity_dimension}
      WHERE current_month
      GROUP BY state_code, violation_type, reported_violations, race, risk_level, supervision_type, charge_category,
        district, metric_period_months
    ) pop
    LEFT JOIN (
      SELECT
        state_code,
        violation_type,
        reported_violations,
        COUNT(DISTINCT person_id) AS population_count,
        race_or_ethnicity AS race,
        risk_level,
        supervision_type,
        charge_category,
        district,
        metric_period_months
      FROM `{project_id}.{reference_dataset}.revocations_matrix_by_person`,
      {race_ethnicity_dimension}
      WHERE current_month
      GROUP BY state_code, violation_type, reported_violations, race, risk_level, supervision_type, charge_category,
        district, metric_period_months
    ) rev
    USING (state_code, violation_type, reported_violations, race, risk_level, supervision_type, charge_category,
      district, metric_period_months)
    WHERE race NOT IN ('EXTERNAL_UNKNOWN', 'NOT_HISPANIC')
    ORDER BY state_code, district, supervision_type, race, risk_level, metric_period_months, violation_type,
      reported_violations, charge_category
    """

REVOCATIONS_MATRIX_DISTRIBUTION_BY_RACE_VIEW = BigQueryView(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=REVOCATIONS_MATRIX_DISTRIBUTION_BY_RACE_VIEW_NAME,
    view_query_template=REVOCATIONS_MATRIX_DISTRIBUTION_BY_RACE_QUERY_TEMPLATE,
    description=REVOCATIONS_MATRIX_DISTRIBUTION_BY_RACE_DESCRIPTION,
    reference_dataset=dataset_config.REFERENCE_TABLES_DATASET,
    race_ethnicity_dimension=bq_utils.unnest_race_and_ethnicity(),
)

if __name__ == '__main__':
    print(REVOCATIONS_MATRIX_DISTRIBUTION_BY_RACE_VIEW.view_id)
    print(REVOCATIONS_MATRIX_DISTRIBUTION_BY_RACE_VIEW.view_query)
