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
"""Revocations Matrix Distribution by Officer."""
# pylint: disable=trailing-whitespace

from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

REVOCATIONS_MATRIX_DISTRIBUTION_BY_OFFICER_VIEW_NAME = (
    "revocations_matrix_distribution_by_officer"
)

REVOCATIONS_MATRIX_DISTRIBUTION_BY_OFFICER_DESCRIPTION = """
 Revocations matrix of violation response count and by officer and metric period month.
 This counts all individuals admitted to prison for a revocation of supervision, broken down by officer,
 and the metric period months.
 """

REVOCATIONS_MATRIX_DISTRIBUTION_BY_OFFICER_QUERY_TEMPLATE = """
    /*{description}*/
    WITH supervision_counts AS (
    SELECT
      state_code, 
      admission_type,
      violation_type,
      reported_violations,
      COUNT(DISTINCT person_id) AS supervision_population_count,
      officer,
      supervision_type,
      supervision_level,
      charge_category,
      level_1_supervision_location,
      level_2_supervision_location,
      metric_period_months    
    FROM `{project_id}.{reference_views_dataset}.supervision_matrix_by_person_materialized`
    FULL OUTER JOIN
         `{project_id}.{reference_views_dataset}.admission_types_per_state_for_matrix_materialized`
    USING (state_code)
    GROUP BY state_code, violation_type, reported_violations, officer, supervision_type, supervision_level, charge_category,
      level_1_supervision_location, level_2_supervision_location, metric_period_months, admission_type
  ), termination_counts AS (
     SELECT
      state_code, 
      admission_type,
      violation_type,
      reported_violations,
      COUNT(DISTINCT person_id) AS termination_count,
      officer,
      supervision_type,
      supervision_level,
      charge_category,
      level_1_supervision_location,
      level_2_supervision_location,
      metric_period_months    
    FROM `{project_id}.{reference_views_dataset}.supervision_termination_matrix_by_person_materialized`
    FULL OUTER JOIN
         `{project_id}.{reference_views_dataset}.admission_types_per_state_for_matrix_materialized`
    USING (state_code) 
    GROUP BY state_code, violation_type, reported_violations, officer, supervision_type, supervision_level, charge_category,
      level_1_supervision_location, level_2_supervision_location, metric_period_months, admission_type
  ), revocation_counts AS (
    SELECT
      state_code,
      admission_type,
      violation_type,
      reported_violations,
      COUNT(DISTINCT person_id) AS revocation_count,
      IFNULL(officer, 'EXTERNAL_UNKNOWN') as officer,
      supervision_type,
      supervision_level,
      charge_category,
      level_1_supervision_location,
      level_2_supervision_location,
      metric_period_months
    FROM `{project_id}.{reference_views_dataset}.revocations_matrix_by_person_materialized`
    GROUP BY state_code, violation_type, reported_violations, officer, supervision_type, supervision_level, charge_category,
      level_1_supervision_location, level_2_supervision_location, metric_period_months, admission_type
  ), officers_to_locations AS (
    -- Determine all supervision locations an officer was associated with in each time period --
    SELECT DISTINCT
      state_code,
      metric_period_months,
      officer,
      IFNULL(CASE WHEN state_code = 'US_MO' THEN level_1_supervision_location
                  WHEN state_code = 'US_PA' THEN level_2_supervision_location
                  END,
             'UNKNOWN') AS district
    FROM `{project_id}.{reference_views_dataset}.supervision_matrix_by_person_materialized`
    WHERE officer IS NOT NULL
      AND ((state_code = 'US_PA' AND level_2_supervision_location != 'ALL')
      OR (state_code = 'US_MO' AND level_1_supervision_location != 'ALL'))
  ), officer_labels AS (
    SELECT
        state_code, 
        officer,
        metric_period_months,
        -- List all locations an officer has supervised in during the time period, with the officer name
        -- E.g. If officer "555: JAMES SMITH" has supervised in locations 07 and 09 in the time period, then this
        -- would produce a label in the format "07, 09 - JAMES SMITH" --
        CONCAT(STRING_AGG(DISTINCT district, ' & ' ORDER BY district), ' - ',
               IFNULL(SPLIT(officer, ':')[SAFE_OFFSET(1)], officer)) AS officer_label    
      FROM officers_to_locations
      GROUP BY state_code, officer, metric_period_months 
  )

    SELECT
      state_code,
      admission_type,
      violation_type,
      reported_violations,
      IFNULL(revocation_count, 0) AS revocation_count,
      IFNULL(termination_count, 0) AS exit_count,
      supervision_population_count,
      officer,
      supervision_type,
      supervision_level,
      charge_category,
      level_1_supervision_location,
      level_2_supervision_location,
      metric_period_months,
      officer_label
    FROM
      supervision_counts
    LEFT JOIN
      revocation_counts
    USING (state_code, violation_type, reported_violations, officer, supervision_type, supervision_level, charge_category,
      level_1_supervision_location, level_2_supervision_location, metric_period_months, admission_type)
    LEFT JOIN
      termination_counts
    USING (state_code, violation_type, reported_violations, officer, supervision_type, supervision_level, charge_category,
      level_1_supervision_location, level_2_supervision_location, metric_period_months, admission_type)
    LEFT JOIN
      officer_labels
    USING (state_code, metric_period_months, officer) 
    WHERE revocation_count > 0
        AND officer != 'EXTERNAL_UNKNOWN'
        -- Filter out any rows that don't have a specified violation_type
        AND violation_type != 'NO_VIOLATION_TYPE'
    """

REVOCATIONS_MATRIX_DISTRIBUTION_BY_OFFICER_VIEW_BUILDER = MetricBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=REVOCATIONS_MATRIX_DISTRIBUTION_BY_OFFICER_VIEW_NAME,
    view_query_template=REVOCATIONS_MATRIX_DISTRIBUTION_BY_OFFICER_QUERY_TEMPLATE,
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
        "officer",
    ),
    description=REVOCATIONS_MATRIX_DISTRIBUTION_BY_OFFICER_DESCRIPTION,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        REVOCATIONS_MATRIX_DISTRIBUTION_BY_OFFICER_VIEW_BUILDER.build_and_print()
