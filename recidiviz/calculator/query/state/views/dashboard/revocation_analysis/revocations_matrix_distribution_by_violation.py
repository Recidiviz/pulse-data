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
"""Revocations Matrix Distribution by Violation."""
from recidiviz.calculator.query.state import (
    dataset_config,
    state_specific_query_strings,
)
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

REVOCATIONS_MATRIX_DISTRIBUTION_BY_VIOLATION_VIEW_NAME = (
    "revocations_matrix_distribution_by_violation"
)

REVOCATIONS_MATRIX_DISTRIBUTION_BY_VIOLATION_DESCRIPTION = """
 Relative frequency of each type of violation and condition violated for people who were revoked to prison. This is
 calculated as the total number of times each type of violation and condition violated was reported on all violations
 filed during a period of 12 months leading up to revocation, divided by the total number of notices of citation and
 violation reports filed during that period. 
 """

REVOCATIONS_MATRIX_DISTRIBUTION_BY_VIOLATION_QUERY_TEMPLATE = """
    /*{description}*/

    /*
 Relative frequency of each type of violation and condition violated for people who were revoked to prison. This is
 calculated as the total number of times each type of violation and condition violated was reported on all violations
 filed during a period of 12 months leading up to revocation, divided by the total number of notices of citation and
 violation reports filed during that period.
 */

    WITH violation_counts AS (
      SELECT
          state_code,
          admission_type,
          metric_period_months,
          supervision_type,
          supervision_level,
          charge_category,
          violation_type,
          level_1_supervision_location,
          level_2_supervision_location,
          reported_violations,
          response_count AS violation_count,
          -- Rows that count towards the number of violations don't count towards any violation type entry counts --
          '' as violation_type_entry
        FROM `{project_id}.{reference_views_dataset}.revocations_matrix_by_person_materialized`
    ) , unnested_violation_type_entries AS (
      SELECT
        state_code,
        admission_type,
        metric_period_months,
        supervision_type,
        supervision_level,
        charge_category,
        violation_type,
        level_1_supervision_location,
        level_2_supervision_location,
        reported_violations,
        -- Rows with violation type entries don't count towards the number of violations --
        0 as violation_count,
        TRIM(violation_type_entry) as violation_type_entry
      FROM `{project_id}.{reference_views_dataset}.revocations_matrix_by_person_materialized`,
        -- Unnest each violation type listed on each violation --
        UNNEST(
            -- Translates a string like "[TECHNICAL, FELONY],[ABSCONSION, MISDEMEANOR],[MISDEMEANOR]" to
            -- "TECHNICAL,FELONY,ABSCONSION,MISDEMEANOR,MISDEMEANOR"
            SPLIT(REPLACE(REPLACE(violation_type_frequency_counter,'[', ''), ']', ''), ',')
        ) as violation_type_entry
    ), state_specific_violation_type_entries AS (
      SELECT
        * EXCEPT (violation_type_entry),
        {state_specific_violation_type_entry}
      FROM unnested_violation_type_entries
    ), violation_counts_with_type_entries AS (
      SELECT * FROM violation_counts 
      UNION ALL
      SELECT * FROM state_specific_violation_type_entries
    )
    
    
    SELECT
      state_code,
      admission_type,
      metric_period_months,
      supervision_type,
      supervision_level,
      charge_category,
      level_1_supervision_location,
      level_2_supervision_location,
      reported_violations,
      violation_type,
      -- Shared violation categories --
      COUNTIF(violation_type_entry = 'ABSCONDED') AS absconded_count,
      COUNTIF(violation_type_entry = 'FELONY') AS felony_count,
      COUNTIF(violation_type_entry = 'MUNICIPAL') AS municipal_count,
      COUNTIF(violation_type_entry = 'MISDEMEANOR') AS misdemeanor_count,
      COUNTIF(violation_type_entry = 'SUBSTANCE_ABUSE') AS substance_count,
      COUNTIF(violation_type_entry = 'LAW') AS law_count,
      -- State-specific violation categories --
      {state_specific_violation_type_entry_categories}
      -- Overall violation count --
      SUM(violation_count) AS violation_count
    FROM
      violation_counts_with_type_entries
    WHERE {state_specific_supervision_location_optimization_filter}
        -- Filter out any rows that don't have a specified violation_type
        AND violation_type != 'NO_VIOLATION_TYPE'
    GROUP BY state_code, admission_type, metric_period_months, supervision_type, supervision_level, charge_category,
      level_1_supervision_location, level_2_supervision_location, reported_violations, violation_type
"""

REVOCATIONS_MATRIX_DISTRIBUTION_BY_VIOLATION_VIEW_BUILDER = MetricBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=REVOCATIONS_MATRIX_DISTRIBUTION_BY_VIOLATION_VIEW_NAME,
    view_query_template=REVOCATIONS_MATRIX_DISTRIBUTION_BY_VIOLATION_QUERY_TEMPLATE,
    dimensions=(
        "state_code",
        "metric_period_months",
        "level_1_supervision_location",
        "level_2_supervision_location",
        "admission_type",
        "supervision_type",
        "supervision_level",
        "violation_type",
        "reported_violations",
        "charge_category",
    ),
    description=REVOCATIONS_MATRIX_DISTRIBUTION_BY_VIOLATION_DESCRIPTION,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    state_specific_violation_type_entry=state_specific_query_strings.state_specific_violation_type_entry(),
    state_specific_supervision_location_optimization_filter=state_specific_query_strings.state_specific_supervision_location_optimization_filter(),
    state_specific_violation_type_entry_categories=state_specific_query_strings.state_specific_violation_type_entry_categories(),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        REVOCATIONS_MATRIX_DISTRIBUTION_BY_VIOLATION_VIEW_BUILDER.build_and_print()
