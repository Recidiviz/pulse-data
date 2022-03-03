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
from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state import dataset_config
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

REVOCATIONS_MATRIX_DISTRIBUTION_BY_GENDER_VIEW_NAME = (
    "revocations_matrix_distribution_by_gender"
)

REVOCATIONS_MATRIX_DISTRIBUTION_BY_GENDER_DESCRIPTION = """
 Revocations matrix of violation response count and most severe violation by gender and metric period month.
 This counts all individuals admitted to prison for a revocation of supervision, broken down by number of
 violations leading up to the revocation, the most severe violation, gender, and the metric period months.
 """

SUPPORTED_GENDER_VALUES = "'FEMALE', 'MALE', 'OTHER'"

REVOCATIONS_MATRIX_DISTRIBUTION_BY_GENDER_QUERY_TEMPLATE = """
    /*{description}*/

    WITH supervision_counts AS (
    SELECT
      state_code, 
      admission_type,
      violation_type,
      reported_violations,
      COUNT(DISTINCT person_id) AS supervision_population_count,
      COUNT(DISTINCT IF(recommended_for_revocation, person_id, NULL)) AS recommended_for_revocation_count,
      IF(gender IN ({supported_gender_values}, 'ALL'), gender, 'OTHER') AS gender,
      supervision_type,
      supervision_level,
      charge_category,
      level_1_supervision_location,
      level_2_supervision_location,
      metric_period_months    
    FROM `{project_id}.{shared_metric_views_dataset}.admission_types_per_state_for_matrix_materialized`
    LEFT JOIN
        `{project_id}.{shared_metric_views_dataset}.supervision_matrix_by_person_materialized`
    USING (state_code),
        {gender_dimension}
    GROUP BY state_code, violation_type, reported_violations, gender, supervision_type, supervision_level, charge_category, 
      level_1_supervision_location, level_2_supervision_location, metric_period_months, admission_type
  ), termination_counts AS (
     SELECT
      state_code, 
      admission_type,
      violation_type,
      reported_violations,
      COUNT(DISTINCT person_id) AS termination_count,
      IF(gender IN ({supported_gender_values}, 'ALL'), gender, 'OTHER') AS gender,
      supervision_type,
      supervision_level,
      charge_category,
      level_1_supervision_location,
      level_2_supervision_location,
      metric_period_months
    FROM `{project_id}.{shared_metric_views_dataset}.admission_types_per_state_for_matrix_materialized`
    LEFT JOIN
        `{project_id}.{shared_metric_views_dataset}.supervision_termination_matrix_by_person_materialized`
    USING (state_code)
    GROUP BY state_code, violation_type, reported_violations, gender, supervision_type, supervision_level, charge_category,
      level_1_supervision_location, level_2_supervision_location, metric_period_months, admission_type
  ), revocation_counts AS (
    SELECT
      state_code,
      admission_type,
      violation_type,
      reported_violations,
      COUNT(DISTINCT person_id) AS revocation_count,
      IF(gender IN ({supported_gender_values}, 'ALL'), gender, 'OTHER') AS gender,
      supervision_type,
      supervision_level,
      charge_category,
      level_1_supervision_location,
      level_2_supervision_location,
      metric_period_months
    FROM `{project_id}.{shared_metric_views_dataset}.revocations_matrix_by_person_materialized`,
    {gender_dimension}
    GROUP BY state_code, violation_type, reported_violations, gender, supervision_type, supervision_level, charge_category,
      level_1_supervision_location, level_2_supervision_location, metric_period_months, admission_type
  )
  
    SELECT
      state_code,
      admission_type,
      violation_type,
      reported_violations,
      IFNULL(gender_rev.revocation_count, 0) AS revocation_count, -- Gender-specific revocation count
      IFNULL(gender_term.termination_count, 0) AS exit_count, -- Gender-specific termination count
      IFNULL(gender_sup.supervision_population_count, 0) AS supervision_population_count, -- Gender-specific supervision pop count
      IFNULL(gender_sup.recommended_for_revocation_count, 0) as recommended_for_revocation_count, -- Gender-specific recommended for revocation count
      IFNULL(revocation_count_all, 0) AS revocation_count_all, -- Total revocation count, all genders
      IFNULL(recommended_for_revocation_count_all, 0) AS recommended_for_revocation_count_all, -- Total recommended for revocation, all genders
      supervision_count_all, -- Total supervision count, all genders
      gender,
      supervision_type,
      supervision_level,
      charge_category,
      level_1_supervision_location,
      level_2_supervision_location,
      metric_period_months
    FROM
      (SELECT * FROM    
      (SELECT * EXCEPT(gender, supervision_population_count, recommended_for_revocation_count),
       SUM(supervision_population_count) AS supervision_count_all,
       SUM(recommended_for_revocation_count) AS recommended_for_revocation_count_all
         FROM supervision_counts
         WHERE gender = 'ALL'
         GROUP BY state_code, violation_type, reported_violations, supervision_type, supervision_level, charge_category,
        level_1_supervision_location, level_2_supervision_location, metric_period_months, admission_type),
      -- Create one row per gender supported on the FE and per non-empty dimension in the supervision counts --
      UNNEST([{supported_gender_values}]) as gender) total_pop
    LEFT JOIN
      (SELECT * FROM    
        (SELECT * EXCEPT(gender, revocation_count), SUM(revocation_count) AS revocation_count_all
         FROM revocation_counts WHERE gender = 'ALL'
         GROUP BY state_code, violation_type, reported_violations, supervision_type, supervision_level, charge_category,
        level_1_supervision_location, level_2_supervision_location, metric_period_months, admission_type),
      -- Create one row per gender supported on the FE and per non-empty dimension in the revocation counts --
      UNNEST([{supported_gender_values}]) as gender) total_rev
    USING (state_code, violation_type, gender, reported_violations,  supervision_type, supervision_level, charge_category,
      level_1_supervision_location, level_2_supervision_location, metric_period_months, admission_type)
    LEFT JOIN
      (SELECT * FROM supervision_counts WHERE gender != 'ALL') gender_sup
    USING (state_code, violation_type, gender, reported_violations, supervision_type, supervision_level, charge_category,
      level_1_supervision_location, level_2_supervision_location, metric_period_months, admission_type)  
    LEFT JOIN
      (SELECT * FROM revocation_counts WHERE gender != 'ALL') gender_rev
    USING (state_code, violation_type, reported_violations, gender, supervision_type, supervision_level, charge_category,
      level_1_supervision_location, level_2_supervision_location, metric_period_months, admission_type)
    LEFT JOIN
      (SELECT * FROM termination_counts WHERE gender != 'ALL') gender_term
    USING (state_code, violation_type, reported_violations, gender, supervision_type, supervision_level, charge_category,
      level_1_supervision_location, level_2_supervision_location, metric_period_months, admission_type)
    -- Filter out any rows that don't have a specified violation_type
    WHERE violation_type != 'NO_VIOLATION_TYPE'
    """

REVOCATIONS_MATRIX_DISTRIBUTION_BY_GENDER_VIEW_BUILDER = MetricBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=REVOCATIONS_MATRIX_DISTRIBUTION_BY_GENDER_VIEW_NAME,
    view_query_template=REVOCATIONS_MATRIX_DISTRIBUTION_BY_GENDER_QUERY_TEMPLATE,
    dimensions=(
        "state_code",
        "metric_period_months",
        "level_1_supervision_location",
        "level_2_supervision_location",
        "supervision_type",
        "supervision_level",
        "violation_type",
        "reported_violations",
        "admission_type",
        "charge_category",
        "gender",
    ),
    description=REVOCATIONS_MATRIX_DISTRIBUTION_BY_GENDER_DESCRIPTION,
    shared_metric_views_dataset=dataset_config.SHARED_METRIC_VIEWS_DATASET,
    gender_dimension=bq_utils.unnest_column("gender", "gender"),
    supported_gender_values=SUPPORTED_GENDER_VALUES,
)


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        REVOCATIONS_MATRIX_DISTRIBUTION_BY_GENDER_VIEW_BUILDER.build_and_print()
