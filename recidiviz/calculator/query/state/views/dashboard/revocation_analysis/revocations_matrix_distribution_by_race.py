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

from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

REVOCATIONS_MATRIX_DISTRIBUTION_BY_RACE_VIEW_NAME = 'revocations_matrix_distribution_by_race'

REVOCATIONS_MATRIX_DISTRIBUTION_BY_RACE_DESCRIPTION = """
 Revocations matrix of violation response count and most severe violation by race, risk level, and metric period month.
 This counts all individuals admitted to prison for a revocation of supervision, broken down by number of
 violations leading up to the revocation, the most severe violation, race, and the metric period.
 """

REVOCATIONS_MATRIX_DISTRIBUTION_BY_RACE_QUERY_TEMPLATE = \
    """
    /*{description}*/
    WITH supervision_counts AS (
    SELECT
      state_code, 
      violation_type,
      reported_violations,
      COUNT(DISTINCT person_id) AS total_supervision_count,
      race_or_ethnicity as race,
      risk_level,
      supervision_type,
      supervision_level,
      charge_category,
      district,
      metric_period_months    
    FROM `{project_id}.{reference_views_dataset}.supervision_matrix_by_person`,
    {race_ethnicity_dimension}
    GROUP BY state_code, violation_type, reported_violations, race, risk_level, supervision_type, supervision_level, charge_category, 
      district, metric_period_months
  ), termination_counts AS (
     SELECT
      state_code, 
      violation_type,
      reported_violations,
      COUNT(DISTINCT person_id) AS termination_count,
      race_or_ethnicity as race,
      risk_level,
      supervision_type,
      supervision_level,
      charge_category,
      district,
      metric_period_months    
    FROM `{project_id}.{reference_views_dataset}.supervision_termination_matrix_by_person`,
    {race_ethnicity_dimension}
    GROUP BY state_code, violation_type, reported_violations, race, risk_level, supervision_type, supervision_level, charge_category,
      district, metric_period_months
  ), revocation_counts AS (
    SELECT
      state_code,
      violation_type,
      reported_violations,
      COUNT(DISTINCT person_id) AS population_count,
      race_or_ethnicity as race,
      risk_level,
      supervision_type,
      supervision_level,
      charge_category,
      district,
      metric_period_months
    FROM `{project_id}.{reference_views_dataset}.revocations_matrix_by_person`,
    {race_ethnicity_dimension}
    GROUP BY state_code, violation_type, reported_violations, race, risk_level, supervision_type, supervision_level, charge_category,
      district, metric_period_months
  )
 
    SELECT
      state_code,
      violation_type,
      reported_violations,
      IFNULL(population_count, 0) AS population_count, -- Revocation count
      IFNULL(termination_count, 0) AS total_exit_count,
      total_supervision_count,
      race,
      risk_level,
      supervision_type,
      supervision_level,
      charge_category,
      district,
      metric_period_months
    FROM
      supervision_counts
    LEFT JOIN
      revocation_counts
    USING (state_code, violation_type, reported_violations, race, risk_level, supervision_type, supervision_level, charge_category,
      district, metric_period_months)
    LEFT JOIN
      termination_counts
    USING (state_code, violation_type, reported_violations, race, risk_level, supervision_type, supervision_level, charge_category,
      district, metric_period_months)
    ORDER BY state_code, metric_period_months, district, supervision_type, supervision_level, race, risk_level, violation_type,
      reported_violations, charge_category
    """

REVOCATIONS_MATRIX_DISTRIBUTION_BY_RACE_VIEW_BUILDER = MetricBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=REVOCATIONS_MATRIX_DISTRIBUTION_BY_RACE_VIEW_NAME,
    view_query_template=REVOCATIONS_MATRIX_DISTRIBUTION_BY_RACE_QUERY_TEMPLATE,
    dimensions=['state_code', 'metric_period_months', 'district', 'supervision_type', 'supervision_level',
                'violation_type', 'reported_violations', 'charge_category', 'race', 'risk_level'],
    description=REVOCATIONS_MATRIX_DISTRIBUTION_BY_RACE_DESCRIPTION,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    race_ethnicity_dimension=bq_utils.unnest_race_and_ethnicity(),
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        REVOCATIONS_MATRIX_DISTRIBUTION_BY_RACE_VIEW_BUILDER.build_and_print()
