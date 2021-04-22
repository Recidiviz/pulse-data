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
# pylint: disable=trailing-whitespace
from recidiviz.calculator.query import bq_utils
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

REVOCATIONS_MATRIX_DISTRIBUTION_BY_RACE_VIEW_NAME = (
    "revocations_matrix_distribution_by_race"
)

REVOCATIONS_MATRIX_DISTRIBUTION_BY_RACE_DESCRIPTION = """
 Revocations matrix of violation response count and most severe violation by race and metric period month.
 This counts all individuals admitted to prison for a revocation of supervision, broken down by number of
 violations leading up to the revocation, the most severe violation, race, and the metric period.
 """

SUPPORTED_RACE_VALUES = "'AMERICAN_INDIAN_ALASKAN_NATIVE', 'ASIAN', 'BLACK', 'EXTERNAL_UNKNOWN', 'HISPANIC', 'OTHER', 'WHITE'"
US_PA_SUPPORTED_RACE_VALUES = "'BLACK', 'HISPANIC', 'OTHER', 'WHITE'"


def generate_state_specific_racial_groupings(field: str) -> str:
    return f"""
    CASE
      WHEN state_code = 'US_PA'
        THEN IF({field} IN ({US_PA_SUPPORTED_RACE_VALUES}, 'ALL'), {field}, 'OTHER')
      ELSE IF({field} IN ({SUPPORTED_RACE_VALUES}, 'ALL'), {field}, 'OTHER')
    END AS race
    """


STATE_SPECIFIC_SUPPORTED_RACE_VALUES_LIST = f"""
    (CASE WHEN state_code = 'US_PA' THEN [{US_PA_SUPPORTED_RACE_VALUES}]
          ELSE [{SUPPORTED_RACE_VALUES}]
    END)"""


REVOCATIONS_MATRIX_DISTRIBUTION_BY_RACE_QUERY_TEMPLATE = f"""
    /*{{description}}*/
    
    WITH supervision_counts AS (
    SELECT
      state_code, 
      admission_type,
      violation_type,
      reported_violations,
      COUNT(DISTINCT person_id) AS supervision_population_count,
      COUNT(DISTINCT IF(recommended_for_revocation, person_id, NULL)) AS recommended_for_revocation_count,
      {generate_state_specific_racial_groupings('race')},
      supervision_type,
      supervision_level,
      charge_category,
      level_1_supervision_location,
      level_2_supervision_location,
      metric_period_months    
    FROM `{{project_id}}.{{reference_views_dataset}}.admission_types_per_state_for_matrix_materialized`
    LEFT JOIN
        `{{project_id}}.{{reference_views_dataset}}.supervision_matrix_by_person_materialized`
    USING (state_code),
        {{race_dimension}}
    GROUP BY state_code, violation_type, reported_violations, race, supervision_type, supervision_level, charge_category, 
      level_1_supervision_location, level_2_supervision_location, metric_period_months, admission_type
  ), termination_counts AS (
     SELECT
      state_code,
      admission_type, 
      violation_type,
      reported_violations,
      COUNT(DISTINCT person_id) AS termination_count,
      {generate_state_specific_racial_groupings('prioritized_race_or_ethnicity')},
      supervision_type,
      supervision_level,
      charge_category,
      level_1_supervision_location,
      level_2_supervision_location,
      metric_period_months
    FROM `{{project_id}}.{{reference_views_dataset}}.admission_types_per_state_for_matrix_materialized`
    LEFT JOIN  
        `{{project_id}}.{{reference_views_dataset}}.supervision_termination_matrix_by_person_materialized`
    USING (state_code)
    GROUP BY state_code, violation_type, reported_violations, race, supervision_type, supervision_level, charge_category,
      level_1_supervision_location, level_2_supervision_location, metric_period_months, admission_type
  ), revocation_counts AS (
    SELECT
      state_code,
      admission_type,
      violation_type,
      reported_violations,
      COUNT(DISTINCT person_id) AS revocation_count,
      {generate_state_specific_racial_groupings('race')},
      supervision_type,
      supervision_level,
      charge_category,
      level_1_supervision_location,
      level_2_supervision_location,
      metric_period_months
    FROM `{{project_id}}.{{reference_views_dataset}}.revocations_matrix_by_person_materialized`,
    {{race_dimension}}
    GROUP BY state_code, violation_type, reported_violations, race, supervision_type, supervision_level, charge_category,
      level_1_supervision_location, level_2_supervision_location, metric_period_months, admission_type
  )
  
    SELECT
      state_code,
      admission_type,
      violation_type,
      reported_violations,
      IFNULL(race_rev.revocation_count, 0) AS revocation_count, -- Race-specific revocation count
      IFNULL(race_term.termination_count, 0) AS exit_count, -- Race-specific termination count
      IFNULL(race_sup.supervision_population_count, 0) AS supervision_population_count, -- Race-specific supervision pop count,
      IFNULL(race_sup.recommended_for_revocation_count, 0) as recommended_for_revocation_count, -- Race-specific recommended for revocation count
      IFNULL(revocation_count_all, 0) AS revocation_count_all, -- Total revocation count, all races
      IFNULL(recommended_for_revocation_count_all, 0) AS recommended_for_revocation_count_all, -- Total recommended for revocation, all races
      supervision_count_all, -- Total supervision count, all races
      race,
      supervision_type,
      supervision_level,
      charge_category,
      level_1_supervision_location,
      level_2_supervision_location,
      metric_period_months
    FROM
      (SELECT * FROM
      (SELECT * EXCEPT(race, supervision_population_count, recommended_for_revocation_count),
       SUM(supervision_population_count) AS supervision_count_all,
       SUM(recommended_for_revocation_count) AS recommended_for_revocation_count_all
         FROM supervision_counts WHERE race = 'ALL'
         GROUP BY state_code, violation_type, reported_violations, supervision_type, supervision_level, charge_category,
        level_1_supervision_location, level_2_supervision_location, metric_period_months, admission_type),
      -- Create one row per race supported on the FE and per non-empty dimension in the supervision counts --
      UNNEST({STATE_SPECIFIC_SUPPORTED_RACE_VALUES_LIST}) as race) total_pop
    LEFT JOIN
      (SELECT * FROM
        (SELECT * EXCEPT(race, revocation_count), SUM(revocation_count) AS revocation_count_all
         FROM revocation_counts WHERE race = 'ALL'
         GROUP BY state_code, violation_type, reported_violations, supervision_type, supervision_level, charge_category,
        level_1_supervision_location, level_2_supervision_location, metric_period_months, admission_type),
      -- Create one row per race supported on the FE and per non-empty dimension in the revocation counts --
      UNNEST({STATE_SPECIFIC_SUPPORTED_RACE_VALUES_LIST}) as race) total_rev
    USING (state_code, violation_type, reported_violations, race, supervision_type, supervision_level, charge_category,
      level_1_supervision_location, level_2_supervision_location, metric_period_months, admission_type)
    LEFT JOIN
      (SELECT * FROM supervision_counts WHERE race != 'ALL') race_sup
    USING (state_code, violation_type, reported_violations, race, supervision_type, supervision_level, charge_category,
      level_1_supervision_location, level_2_supervision_location, metric_period_months, admission_type)  
    LEFT JOIN
      (SELECT * FROM revocation_counts WHERE race != 'ALL') race_rev
    USING (state_code, violation_type, reported_violations, race, supervision_type, supervision_level, charge_category,
      level_1_supervision_location, level_2_supervision_location, metric_period_months, admission_type)
    LEFT JOIN
      (SELECT * FROM termination_counts WHERE race != 'ALL') race_term
    USING (state_code, violation_type, reported_violations, race, supervision_type, supervision_level, charge_category,
      level_1_supervision_location, level_2_supervision_location, metric_period_months, admission_type)
    -- Filter out any rows that don't have a specified violation_type
    WHERE violation_type != 'NO_VIOLATION_TYPE'
    """

REVOCATIONS_MATRIX_DISTRIBUTION_BY_RACE_VIEW_BUILDER = MetricBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=REVOCATIONS_MATRIX_DISTRIBUTION_BY_RACE_VIEW_NAME,
    view_query_template=REVOCATIONS_MATRIX_DISTRIBUTION_BY_RACE_QUERY_TEMPLATE,
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
        "race",
    ),
    description=REVOCATIONS_MATRIX_DISTRIBUTION_BY_RACE_DESCRIPTION,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    race_dimension=bq_utils.unnest_column("prioritized_race_or_ethnicity", "race"),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        REVOCATIONS_MATRIX_DISTRIBUTION_BY_RACE_VIEW_BUILDER.build_and_print()
