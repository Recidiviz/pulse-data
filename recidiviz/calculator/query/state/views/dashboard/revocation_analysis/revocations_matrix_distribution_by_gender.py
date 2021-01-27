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
from recidiviz.calculator.query import bq_utils
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

REVOCATIONS_MATRIX_DISTRIBUTION_BY_GENDER_VIEW_NAME = 'revocations_matrix_distribution_by_gender'

REVOCATIONS_MATRIX_DISTRIBUTION_BY_GENDER_DESCRIPTION = """
 Revocations matrix of violation response count and most severe violation by gender, risk level, and metric period month.
 This counts all individuals admitted to prison for a revocation of supervision, broken down by number of
 violations leading up to the revocation, the most severe violation, gender, and the metric period months.
 """

# TODO(#5473): Remove risk level and other deprecated columns
REVOCATIONS_MATRIX_DISTRIBUTION_BY_GENDER_QUERY_TEMPLATE = \
    """
    /*{description}*/

    WITH supervision_counts AS (
    SELECT
      state_code, 
      violation_type,
      reported_violations,
      COUNT(DISTINCT person_id) AS supervision_population_count,
      gender,
      risk_level,
      supervision_type,
      supervision_level,
      charge_category,
      level_1_supervision_location,
      level_2_supervision_location,
      metric_period_months    
    FROM `{project_id}.{reference_views_dataset}.supervision_matrix_by_person_materialized`,
    {gender_dimension}
    GROUP BY state_code, violation_type, reported_violations, gender, risk_level, supervision_type, supervision_level, charge_category, 
      level_1_supervision_location, level_2_supervision_location, metric_period_months
  ), termination_counts AS (
     SELECT
      state_code, 
      violation_type,
      reported_violations,
      COUNT(DISTINCT person_id) AS termination_count,
      gender,
      risk_level,
      supervision_type,
      supervision_level,
      charge_category,
      level_1_supervision_location,
      level_2_supervision_location,
      metric_period_months    
    FROM `{project_id}.{reference_views_dataset}.supervision_termination_matrix_by_person_materialized`
    GROUP BY state_code, violation_type, reported_violations, gender, risk_level, supervision_type, supervision_level, charge_category,
      level_1_supervision_location, level_2_supervision_location, metric_period_months
  ), revocation_counts AS (
    SELECT
      state_code,
      violation_type,
      reported_violations,
      COUNT(DISTINCT person_id) AS revocation_count,
      gender,
      risk_level,
      supervision_type,
      supervision_level,
      charge_category,
      level_1_supervision_location,
      level_2_supervision_location,
      metric_period_months
    FROM `{project_id}.{reference_views_dataset}.revocations_matrix_by_person_materialized`,
    {gender_dimension}
    GROUP BY state_code, violation_type, reported_violations, gender, risk_level, supervision_type, supervision_level, charge_category,
      level_1_supervision_location, level_2_supervision_location, metric_period_months
  )
  
    SELECT
      state_code,
      violation_type,
      reported_violations,
      -- TODO(#5473): Delete this column
      IFNULL(gender_rev.revocation_count, 0) AS population_count, -- [DEPRECATED] Gender-specific revocation count
      IFNULL(gender_rev.revocation_count, 0) AS revocation_count, -- Gender-specific revocation count
      IFNULL(gender_term.termination_count, 0) AS exit_count, -- Gender-specific termination count
      IFNULL(gender_sup.supervision_population_count, 0) AS supervision_population_count, -- Gender-specific supervision pop count
      -- TODO(#5473): Delete this column
      IFNULL(gender_sup.supervision_population_count, 0) AS total_supervision_count, -- [DEPRECATED] Gender-specific supervision pop count
      IFNULL(revocation_count_all, 0) AS revocation_count_all, -- Total revocation count, all genders
      supervision_count_all, -- Total supervision count, all genders
      gender,
      risk_level,
      supervision_type,
      supervision_level,
      charge_category,
      -- TODO(#4709): Remove this field once it is no-longer used on the frontend
      CASE
        WHEN state_code = 'US_MO' THEN level_1_supervision_location
        WHEN state_code = 'US_PA' THEN level_2_supervision_location
        ELSE level_1_supervision_location
      END AS district,
      level_1_supervision_location,
      level_2_supervision_location,
      metric_period_months
    FROM
      (SELECT * EXCEPT(gender, supervision_population_count, risk_level), SUM(supervision_population_count) AS supervision_count_all
       FROM supervision_counts WHERE gender = 'ALL'
       GROUP BY state_code, violation_type, reported_violations, supervision_type, supervision_level, charge_category,
      level_1_supervision_location, level_2_supervision_location, metric_period_months) total_pop
    LEFT JOIN
      (SELECT * EXCEPT(gender, revocation_count, risk_level), SUM(revocation_count) AS revocation_count_all
       FROM revocation_counts WHERE gender = 'ALL'
       GROUP BY state_code, violation_type, reported_violations, supervision_type, supervision_level, charge_category,
      level_1_supervision_location, level_2_supervision_location, metric_period_months) total_rev
    USING (state_code, violation_type, reported_violations, supervision_type, supervision_level, charge_category,
      level_1_supervision_location, level_2_supervision_location, metric_period_months)
    LEFT JOIN
      (SELECT * FROM supervision_counts WHERE gender != 'ALL') gender_sup
    USING (state_code, violation_type, reported_violations, supervision_type, supervision_level, charge_category,
      level_1_supervision_location, level_2_supervision_location, metric_period_months)  
    LEFT JOIN
      (SELECT * FROM revocation_counts WHERE gender != 'ALL') gender_rev
    USING (state_code, violation_type, reported_violations, gender, supervision_type, supervision_level, charge_category,
      level_1_supervision_location, level_2_supervision_location, metric_period_months, risk_level)
    LEFT JOIN
      (SELECT * FROM termination_counts WHERE gender != 'ALL') gender_term
    USING (state_code, violation_type, reported_violations, gender, supervision_type, supervision_level, charge_category,
      level_1_supervision_location, level_2_supervision_location, metric_period_months, risk_level)
    """

REVOCATIONS_MATRIX_DISTRIBUTION_BY_GENDER_VIEW_BUILDER = MetricBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=REVOCATIONS_MATRIX_DISTRIBUTION_BY_GENDER_VIEW_NAME,
    view_query_template=REVOCATIONS_MATRIX_DISTRIBUTION_BY_GENDER_QUERY_TEMPLATE,
    dimensions=['state_code', 'metric_period_months', 'district', 'level_1_supervision_location', 
                'level_2_supervision_location', 'supervision_type', 'supervision_level',
                'violation_type', 'reported_violations', 'charge_category', 'gender', 'risk_level'],
    description=REVOCATIONS_MATRIX_DISTRIBUTION_BY_GENDER_DESCRIPTION,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    gender_dimension=bq_utils.unnest_column('gender', 'gender')
)


if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        REVOCATIONS_MATRIX_DISTRIBUTION_BY_GENDER_VIEW_BUILDER.build_and_print()
