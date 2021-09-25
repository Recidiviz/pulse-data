# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Event-based supervision by person, where a person is included in the supervision
population on the day of their commitment from supervision.

This is necessary whenever we do commitment rate calculations within the supervision
population. This view ensures that all individuals with commitment admissions are
included in the supervision populations for rate calculations where the supervision
population is the denominator and the numerator is some subset of commitments.

For example, if we want to calculate the percent of an officer's caseload
that had a treatment sanction commitment admission in a given month, we need to
ensure that the individuals with the commitment admission are counted as on the
officer's caseload in that month.
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

EVENT_BASED_SUPERVISION_POPULATIONS_WITH_COMMITMENTS_FOR_RATE_DENOMINATORS_VIEW_NAME = (
    "event_based_supervision_populations_with_commitments_for_rate_denominators"
)

EVENT_BASED_SUPERVISION_POPULATIONS_WITH_COMMITMENTS_FOR_RATE_DENOMINATORS_DESCRIPTION = """
 Event-based supervision by person, where a person is included in the supervision
 population on the day of their commitment from supervision.

 This is necessary whenever we do commitment rate calculations within the supervision
 population. This view ensures that all individuals with commitment admissions are
 included in the supervision populations for rate calculations where the supervision
 population is the denominator and the numerator is some subset of commitments.

 For example, if we want to calculate the percent of an officer's caseload
 that had a treatment sanction commitment admission in a given month, we need to
 ensure that the individuals with the commitment admission are counted as on the
 officer's caseload in that month.

 Expanded Dimensions: district, supervision_type
 """

EVENT_BASED_SUPERVISION_POPULATIONS_WITH_COMMITMENTS_FOR_RATE_DENOMINATORS_QUERY_TEMPLATE = """
    /*{description}*/
    
    WITH supervision_pop AS (
        SELECT
          state_code,
          person_id,
          year, month, date_of_supervision,
          supervision_type,
          supervising_district_external_id,
          supervising_officer_external_id AS officer_external_id,
          prioritized_race_or_ethnicity as race_or_ethnicity,
          gender, {age_bucket}, assessment_score_bucket
        FROM `{project_id}.{materialized_metrics_dataset}.most_recent_supervision_population_metrics_materialized`
    ), commitments_from_supervision AS (
        SELECT
          state_code,
          person_id,
          year,
          month,
          admission_date AS date_of_supervision,
          supervision_type,
          supervising_district_external_id,
          supervising_officer_external_id AS officer_external_id,
          prioritized_race_or_ethnicity as race_or_ethnicity,
          gender, {age_bucket}, assessment_score_bucket
        FROM `{project_id}.{materialized_metrics_dataset}.most_recent_incarceration_commitment_from_supervision_metrics_included_in_state_population_materialized`
    ), all_pop AS (
        SELECT * FROM supervision_pop
        UNION ALL
        SELECT * FROM commitments_from_supervision
    )
    
    SELECT
      state_code,
      person_id,
      year, month, date_of_supervision,
      supervision_type,
      district,
      officer_external_id,
      race_or_ethnicity,
      gender, age_bucket, assessment_score_bucket
    FROM
        all_pop,
    {district_dimension},
    {supervision_type_dimension}
    WHERE district IS NOT NULL
      AND {thirty_six_month_filter}
    """

EVENT_BASED_SUPERVISION_POPULATIONS_WITH_COMMITMENTS_FOR_RATE_DENOMINATORS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_VIEWS_DATASET,
    view_id=EVENT_BASED_SUPERVISION_POPULATIONS_WITH_COMMITMENTS_FOR_RATE_DENOMINATORS_VIEW_NAME,
    view_query_template=EVENT_BASED_SUPERVISION_POPULATIONS_WITH_COMMITMENTS_FOR_RATE_DENOMINATORS_QUERY_TEMPLATE,
    description=EVENT_BASED_SUPERVISION_POPULATIONS_WITH_COMMITMENTS_FOR_RATE_DENOMINATORS_DESCRIPTION,
    materialized_metrics_dataset=dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
    district_dimension=bq_utils.unnest_district(),
    supervision_type_dimension=bq_utils.unnest_supervision_type(),
    thirty_six_month_filter=bq_utils.thirty_six_month_filter(),
    age_bucket=bq_utils.age_bucket_grouping(),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        EVENT_BASED_SUPERVISION_POPULATIONS_WITH_COMMITMENTS_FOR_RATE_DENOMINATORS_VIEW_BUILDER.build_and_print()
